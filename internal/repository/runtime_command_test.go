package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
)

func TestTimescaleRepositoryCreateRuntimeCommandIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeCommands(ctx, db); err != nil {
		t.Fatalf("create temp runtime_commands: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	repo := &TimescaleRepository{db: db}
	cmd := domain.RuntimeCommand{
		CommandID:      "cmd-1",
		UserID:         42,
		RuntimeID:      "rt-1",
		SessionID:      "sess-1",
		IdempotencyKey: "idem-1",
		CommandType:    domain.RuntimeCommandTypeStartSession,
		DeadlineAt:     now.Add(time.Minute),
		Payload:        []byte(`{"account_id":7}`),
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	first, reused, err := repo.CreateRuntimeCommand(ctx, cmd)
	if err != nil {
		t.Fatalf("CreateRuntimeCommand first: %v", err)
	}
	if reused || first.CommandID != "cmd-1" || first.Status != domain.RuntimeCommandStatusQueued {
		t.Fatalf("first command = %+v reused=%v, want queued cmd-1 reused=false", first, reused)
	}

	cmd.CommandID = "cmd-2"
	cmd.Payload = []byte(`{"account_id":999}`)
	second, reused, err := repo.CreateRuntimeCommand(ctx, cmd)
	if err != nil {
		t.Fatalf("CreateRuntimeCommand second: %v", err)
	}
	if !reused || second.CommandID != "cmd-1" || string(second.Payload) != `{"account_id":7}` {
		t.Fatalf("second command = %+v reused=%v, want original cmd-1 payload", second, reused)
	}
}

func TestTimescaleRepositoryClaimRuntimeCommandRequiresConnectionOwnerAndInFlightCapacity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}
	if err := createTempRuntimeCommands(ctx, db); err != nil {
		t.Fatalf("create temp runtime_commands: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	repo := &TimescaleRepository{db: db}
	if err := repo.CreateRuntime(ctx, domain.Runtime{
		RuntimeID:       "rt-claim",
		UserID:          42,
		Name:            "claim-runtime",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleExecutor,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}
	if err := repo.RecordRuntimeConnectionOwner(ctx, "rt-claim", "cp-owner", now); err != nil {
		t.Fatalf("RecordRuntimeConnectionOwner: %v", err)
	}
	for _, cmd := range []domain.RuntimeCommand{
		{CommandID: "cmd-inflight", UserID: 42, RuntimeID: "rt-claim", CommandType: domain.RuntimeCommandTypeStatusPatch, Status: domain.RuntimeCommandStatusRunning, DeadlineAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now},
		{CommandID: "cmd-queued", UserID: 42, RuntimeID: "rt-claim", CommandType: domain.RuntimeCommandTypeStopSession, DeadlineAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now},
	} {
		if _, _, err := repo.CreateRuntimeCommand(ctx, cmd); err != nil {
			t.Fatalf("CreateRuntimeCommand(%s): %v", cmd.CommandID, err)
		}
		if cmd.Status == domain.RuntimeCommandStatusRunning {
			if _, err := db.ExecContext(ctx, `UPDATE runtime_commands SET status = 'running' WHERE command_id = $1`, cmd.CommandID); err != nil {
				t.Fatalf("force running: %v", err)
			}
		}
	}

	_, ok, err := repo.ClaimNextRuntimeCommand(ctx, "rt-claim", "cp-stale", now, 5)
	if err != nil {
		t.Fatalf("Claim stale owner: %v", err)
	}
	if ok {
		t.Fatal("stale owner claimed a command")
	}
	_, ok, err = repo.ClaimNextRuntimeCommand(ctx, "rt-claim", "cp-owner", now, 1)
	if err != nil {
		t.Fatalf("Claim over capacity: %v", err)
	}
	if ok {
		t.Fatal("claim succeeded despite in-flight limit")
	}
	claimed, ok, err := repo.ClaimNextRuntimeCommand(ctx, "rt-claim", "cp-owner", now, 2)
	if err != nil {
		t.Fatalf("Claim with capacity: %v", err)
	}
	if !ok || claimed.CommandID != "cmd-queued" || claimed.Status != domain.RuntimeCommandStatusSent || claimed.AttemptCount != 1 || claimed.SentAt == nil {
		t.Fatalf("claimed = %+v ok=%v, want sent cmd-queued attempt=1", claimed, ok)
	}
}

func TestTimescaleRepositoryRuntimeCommandAckCompleteAndTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeCommands(ctx, db); err != nil {
		t.Fatalf("create temp runtime_commands: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	repo := &TimescaleRepository{db: db}
	if _, _, err := repo.CreateRuntimeCommand(ctx, domain.RuntimeCommand{
		CommandID:   "cmd-ack",
		UserID:      42,
		RuntimeID:   "rt-1",
		CommandType: domain.RuntimeCommandTypeStopSession,
		DeadlineAt:  now.Add(time.Minute),
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateRuntimeCommand ack: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runtime_commands SET status = 'sent' WHERE command_id = 'cmd-ack'`); err != nil {
		t.Fatalf("force sent: %v", err)
	}
	acked, err := repo.AcknowledgeRuntimeCommand(ctx, "cmd-ack", now.Add(time.Second))
	if err != nil {
		t.Fatalf("AcknowledgeRuntimeCommand: %v", err)
	}
	if acked.Status != domain.RuntimeCommandStatusAcked || acked.AckedAt == nil {
		t.Fatalf("acked = %+v, want acked_at", acked)
	}
	done, err := repo.CompleteRuntimeCommand(ctx, "cmd-ack", domain.RuntimeCommandStatusSucceeded, []byte(`{"ok":true}`), "", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("CompleteRuntimeCommand: %v", err)
	}
	if done.Status != domain.RuntimeCommandStatusSucceeded || done.CompletedAt == nil || string(done.Result) != `{"ok":true}` {
		t.Fatalf("done = %+v, want succeeded result", done)
	}

	if _, _, err := repo.CreateRuntimeCommand(ctx, domain.RuntimeCommand{
		CommandID:   "cmd-timeout",
		UserID:      42,
		RuntimeID:   "rt-1",
		CommandType: domain.RuntimeCommandTypeStopSession,
		DeadlineAt:  now.Add(-time.Second),
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateRuntimeCommand timeout: %v", err)
	}
	timedOut, err := repo.TimeoutRuntimeCommands(ctx, now, "deadline exceeded")
	if err != nil {
		t.Fatalf("TimeoutRuntimeCommands: %v", err)
	}
	if len(timedOut) != 1 || timedOut[0].CommandID != "cmd-timeout" || timedOut[0].Status != domain.RuntimeCommandStatusTimedOut {
		t.Fatalf("timed out = %+v, want cmd-timeout timed_out", timedOut)
	}
}

func TestTimescaleRepositoryRuntimeCommandCircuitOpensAfterRepeatedFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeCommands(ctx, db); err != nil {
		t.Fatalf("create temp runtime_commands: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	repo := &TimescaleRepository{db: db}
	for _, cmd := range []domain.RuntimeCommand{
		{CommandID: "cmd-failed-1", UserID: 42, RuntimeID: "rt-circuit", CommandType: domain.RuntimeCommandTypeStopSession, DeadlineAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now},
		{CommandID: "cmd-failed-2", UserID: 42, RuntimeID: "rt-circuit", CommandType: domain.RuntimeCommandTypeStopSession, DeadlineAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now},
		{CommandID: "cmd-other-runtime", UserID: 42, RuntimeID: "rt-other", CommandType: domain.RuntimeCommandTypeStopSession, DeadlineAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now},
	} {
		if _, _, err := repo.CreateRuntimeCommand(ctx, cmd); err != nil {
			t.Fatalf("CreateRuntimeCommand(%s): %v", cmd.CommandID, err)
		}
	}
	if _, err := repo.CompleteRuntimeCommand(ctx, "cmd-failed-1", domain.RuntimeCommandStatusFailed, nil, "runtime disconnected", now.Add(time.Second)); err != nil {
		t.Fatalf("complete failed 1: %v", err)
	}
	if _, err := repo.CompleteRuntimeCommand(ctx, "cmd-failed-2", domain.RuntimeCommandStatusTimedOut, nil, "deadline exceeded", now.Add(2*time.Second)); err != nil {
		t.Fatalf("complete failed 2: %v", err)
	}
	if _, err := repo.CompleteRuntimeCommand(ctx, "cmd-other-runtime", domain.RuntimeCommandStatusFailed, nil, "runtime disconnected", now.Add(time.Second)); err != nil {
		t.Fatalf("complete other runtime: %v", err)
	}

	open, failures, err := repo.RuntimeCommandCircuitOpen(ctx, "rt-circuit", now.Add(-time.Minute), 2)
	if err != nil {
		t.Fatalf("RuntimeCommandCircuitOpen: %v", err)
	}
	if !open || failures != 2 {
		t.Fatalf("circuit open/failures = %v/%d, want true/2", open, failures)
	}
	open, failures, err = repo.RuntimeCommandCircuitOpen(ctx, "rt-circuit", now.Add(-time.Minute), 3)
	if err != nil {
		t.Fatalf("RuntimeCommandCircuitOpen threshold 3: %v", err)
	}
	if open || failures != 2 {
		t.Fatalf("circuit open/failures = %v/%d, want false/2", open, failures)
	}
}

func createTempRuntimeCommands(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE runtime_commands (
			command_id TEXT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			runtime_id TEXT NOT NULL,
			session_id TEXT NOT NULL DEFAULT '',
			idempotency_key TEXT NOT NULL DEFAULT '',
			command_type TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'queued',
			deadline_at TIMESTAMPTZ NOT NULL,
			sent_at TIMESTAMPTZ,
			acked_at TIMESTAMPTZ,
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			cancelled_at TIMESTAMPTZ,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			result JSONB NOT NULL DEFAULT '{}'::jsonb,
			failure_reason TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		) ON COMMIT PRESERVE ROWS;
		CREATE UNIQUE INDEX uq_runtime_commands_runtime_idempotency
			ON runtime_commands (runtime_id, idempotency_key)
			WHERE idempotency_key <> '';
	`)
	return err
}
