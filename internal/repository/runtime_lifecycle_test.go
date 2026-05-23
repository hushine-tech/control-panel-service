package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
)

func TestTimescaleRepositoryEndDeadRuntimesOnlyEndsUnhealthyCandidates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * time.Minute)
	fresh := now.Add(-time.Minute)
	repo := &TimescaleRepository{db: db}
	for _, rt := range []domain.Runtime{
		{RuntimeID: "rt_active", UserID: 42, Name: "active", Source: domain.RuntimeSourceHosted, Status: domain.RuntimeStatusActive, UpdatedAt: stale, CreatedAt: stale},
		{RuntimeID: "rt_paired", UserID: 42, Name: "paired", Source: domain.RuntimeSourceHosted, Status: domain.RuntimeStatusPaired, UpdatedAt: stale, CreatedAt: stale},
		{RuntimeID: "rt_unhealthy", UserID: 42, Name: "unhealthy", Source: domain.RuntimeSourceHosted, Status: domain.RuntimeStatusUnhealthy, UpdatedAt: stale, CreatedAt: stale},
		{RuntimeID: "rt_unhealthy_fresh", UserID: 42, Name: "unhealthy-fresh", Source: domain.RuntimeSourceHosted, Status: domain.RuntimeStatusUnhealthy, UpdatedAt: fresh, CreatedAt: fresh},
	} {
		if err := repo.CreateRuntime(ctx, rt); err != nil {
			t.Fatalf("CreateRuntime(%s): %v", rt.RuntimeID, err)
		}
	}

	ended, err := repo.EndDeadRuntimes(ctx, now.Add(-5*time.Minute), domain.RuntimeEndedReasonHeartbeatStale, now)
	if err != nil {
		t.Fatalf("EndDeadRuntimes: %v", err)
	}
	if len(ended) != 1 || ended[0].RuntimeID != "rt_unhealthy" {
		t.Fatalf("ended = %+v, want only rt_unhealthy", ended)
	}
	for _, id := range []string{"rt_active", "rt_paired", "rt_unhealthy_fresh"} {
		got, err := repo.GetRuntime(ctx, id)
		if err != nil {
			t.Fatalf("GetRuntime(%s): %v", id, err)
		}
		if domain.IsRuntimeTerminalStatus(got.Status) {
			t.Fatalf("%s was ended unexpectedly", id)
		}
	}
	got, err := repo.GetRuntime(ctx, "rt_unhealthy")
	if err != nil {
		t.Fatalf("GetRuntime(rt_unhealthy): %v", err)
	}
	if got.Status != domain.RuntimeStatusHeartbeatStale || got.EndedReason != domain.RuntimeEndedReasonHeartbeatStale || got.EndedAt == nil {
		t.Fatalf("rt_unhealthy = %+v, want heartbeat_stale terminal state", got)
	}
}

func TestTimescaleRepositoryCreateSelfHostedRuntimeConsumesDownloadedCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}
	if err := createTempRuntimeCredentials(ctx, db); err != nil {
		t.Fatalf("create temp runtime_credentials: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	_, err := db.ExecContext(ctx, `
		INSERT INTO runtime_credentials (
			key_id, user_id, public_key_pem, label, role, status, created_at, downloaded_at
		) VALUES (
			'key-debugger', 42, 'public-key', 'debugger', 'debugger', 'downloaded', $1, $1
		)`, now)
	if err != nil {
		t.Fatalf("insert credential: %v", err)
	}

	repo := &TimescaleRepository{db: db}
	if err := repo.CreateOrReplaceSelfHostedRuntime(ctx, domain.Runtime{
		RuntimeID:       "runtime-debugger",
		CredentialKeyID: "key-debugger",
		UserID:          42,
		Name:            "debugger-desk",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleDebugger,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatalf("CreateOrReplaceSelfHostedRuntime: %v", err)
	}

	var status, consumedRuntimeID, role string
	var consumedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `
		SELECT status, consumed_runtime_id, consumed_at
		FROM runtime_credentials WHERE key_id = 'key-debugger'`,
	).Scan(&status, &consumedRuntimeID, &consumedAt); err != nil {
		t.Fatalf("query credential: %v", err)
	}
	if status != string(domain.CredentialStatusConsumed) || consumedRuntimeID != "runtime-debugger" || !consumedAt.Valid {
		t.Fatalf("credential = status:%q consumed_runtime_id:%q consumed_at:%v, want consumed/runtime-debugger/timestamp", status, consumedRuntimeID, consumedAt)
	}
	if err := db.QueryRowContext(ctx, `SELECT role FROM runtime_registry WHERE runtime_id = 'runtime-debugger'`).Scan(&role); err != nil {
		t.Fatalf("query runtime role: %v", err)
	}
	if role != string(domain.CredentialRoleDebugger) {
		t.Fatalf("runtime role = %q, want debugger", role)
	}
}

func TestTimescaleRepositoryCreateSelfHostedRuntimeRejectsSecondNonTerminalDebugger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}
	if err := createTempRuntimeCredentials(ctx, db); err != nil {
		t.Fatalf("create temp runtime_credentials: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	_, err := db.ExecContext(ctx, `
		INSERT INTO runtime_credentials (
			key_id, user_id, public_key_pem, label, role, status, created_at, downloaded_at
		) VALUES
			('key-debugger-1', 42, 'public-key', 'debugger-1', 'debugger', 'downloaded', $1, $1),
			('key-debugger-2', 42, 'public-key', 'debugger-2', 'debugger', 'downloaded', $1, $1)`, now)
	if err != nil {
		t.Fatalf("insert credentials: %v", err)
	}

	repo := &TimescaleRepository{db: db}
	first := domain.Runtime{
		RuntimeID:       "runtime-debugger-1",
		CredentialKeyID: "key-debugger-1",
		UserID:          42,
		Name:            "debugger-one",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleDebugger,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := repo.CreateOrReplaceSelfHostedRuntime(ctx, first); err != nil {
		t.Fatalf("first CreateOrReplaceSelfHostedRuntime: %v", err)
	}
	err = repo.CreateOrReplaceSelfHostedRuntime(ctx, domain.Runtime{
		RuntimeID:       "runtime-debugger-2",
		CredentialKeyID: "key-debugger-2",
		UserID:          42,
		Name:            "debugger-two",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleDebugger,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second debugger err = %v, want ErrConflict", err)
	}

	if _, err := repo.EndRuntime(ctx, "runtime-debugger-1", domain.RuntimeEndedReasonUserCancelled, now.Add(time.Minute)); err != nil {
		t.Fatalf("end first debugger: %v", err)
	}
	if err := repo.CreateOrReplaceSelfHostedRuntime(ctx, domain.Runtime{
		RuntimeID:       "runtime-debugger-2",
		CredentialKeyID: "key-debugger-2",
		UserID:          42,
		Name:            "debugger-two",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleDebugger,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       now.Add(2 * time.Minute),
		UpdatedAt:       now.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("second debugger after terminal first: %v", err)
	}
}

func TestTimescaleRepositoryCreateSelfHostedRuntimeRejectsConsumedCredentialForNewRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}
	if err := createTempRuntimeCredentials(ctx, db); err != nil {
		t.Fatalf("create temp runtime_credentials: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	_, err := db.ExecContext(ctx, `
		INSERT INTO runtime_credentials (
			key_id, user_id, public_key_pem, label, role, status, created_at,
			downloaded_at, consumed_at, consumed_runtime_id
		) VALUES (
			'key-used', 42, 'public-key', 'used', 'executor', 'consumed', $1, $1, $1, 'runtime-old'
		)`, now)
	if err != nil {
		t.Fatalf("insert credential: %v", err)
	}

	repo := &TimescaleRepository{db: db}
	err = repo.CreateOrReplaceSelfHostedRuntime(ctx, domain.Runtime{
		RuntimeID:       "runtime-new",
		CredentialKeyID: "key-used",
		UserID:          42,
		Name:            "new-runtime",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleExecutor,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	var runtimeCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_registry`).Scan(&runtimeCount); err != nil {
		t.Fatalf("count runtimes: %v", err)
	}
	if runtimeCount != 0 {
		t.Fatalf("runtime rows = %d, want 0", runtimeCount)
	}
}

func TestTimescaleRepositoryCreateSelfHostedRuntimeRejectsConsumedCredentialForSameRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}
	if err := createTempRuntimeCredentials(ctx, db); err != nil {
		t.Fatalf("create temp runtime_credentials: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	_, err := db.ExecContext(ctx, `
		INSERT INTO runtime_credentials (
			key_id, user_id, public_key_pem, label, role, status, created_at,
			downloaded_at, consumed_at, consumed_runtime_id
		) VALUES (
			'key-used', 42, 'public-key', 'used', 'executor', 'consumed', $1, $1, $1, 'runtime-used'
		)`, now)
	if err != nil {
		t.Fatalf("insert credential: %v", err)
	}

	repo := &TimescaleRepository{db: db}
	err = repo.CreateOrReplaceSelfHostedRuntime(ctx, domain.Runtime{
		RuntimeID:       "runtime-used",
		CredentialKeyID: "key-used",
		UserID:          42,
		Name:            "same-runtime",
		Source:          domain.RuntimeSourceSelfHosted,
		Role:            domain.CredentialRoleExecutor,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

func TestTimescaleRepositoryListCredentialsIncludesConsumedByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeCredentials(ctx, db); err != nil {
		t.Fatalf("create temp runtime_credentials: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	_, err := db.ExecContext(ctx, `
		INSERT INTO runtime_credentials (
			key_id, user_id, public_key_pem, label, role, status, created_at,
			downloaded_at, consumed_at, consumed_runtime_id, revoked_at
		) VALUES
			('key-consumed', 42, 'public-key', 'consumed', 'executor', 'consumed', $1, $1, $1, 'runtime-consumer', NULL),
			('key-revoked', 42, 'public-key', 'revoked', 'executor', 'revoked', $1, NULL, NULL, '', $1)`,
		now)
	if err != nil {
		t.Fatalf("insert credentials: %v", err)
	}

	repo := &TimescaleRepository{db: db}
	creds, err := repo.ListRuntimeCredentialsByUser(ctx, 42, false)
	if err != nil {
		t.Fatalf("ListRuntimeCredentialsByUser: %v", err)
	}
	if len(creds) != 1 || creds[0].KeyID != "key-consumed" || creds[0].ConsumedRuntimeID != "runtime-consumer" {
		t.Fatalf("creds = %+v, want consumed only", creds)
	}
}

func TestTimescaleRepositoryRuntimeChannelLeaseAndAdmissionFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeChannelLeases(ctx, db); err != nil {
		t.Fatalf("create temp runtime_channel_leases: %v", err)
	}
	if err := createTempRuntimeAdmissionFailures(ctx, db); err != nil {
		t.Fatalf("create temp runtime_admission_failures: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	repo := &TimescaleRepository{db: db}
	if err := repo.CreateRuntimeChannelLease(ctx, domain.RuntimeChannelLease{
		RuntimeID:       "runtime-1",
		UserID:          42,
		CredentialKeyID: "key-1",
		LeaseHash:       "lease-hash",
		IssuedAt:        now,
		ExpiresAt:       now.Add(time.Hour),
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatalf("CreateRuntimeChannelLease: %v", err)
	}
	lease, err := repo.GetRuntimeChannelLeaseByHash(ctx, "lease-hash")
	if err != nil {
		t.Fatalf("GetRuntimeChannelLeaseByHash: %v", err)
	}
	if lease.RuntimeID != "runtime-1" || lease.UserID != 42 {
		t.Fatalf("lease = %+v", lease)
	}
	if err := repo.TouchRuntimeChannelLease(ctx, "runtime-1", "lease-hash", now.Add(time.Minute)); err != nil {
		t.Fatalf("TouchRuntimeChannelLease: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := repo.RecordRuntimeAdmissionFailure(ctx, domain.RuntimeAdmissionFailure{
			UserID:             42,
			CredentialKeyID:    "key-1",
			RequestedRuntimeID: "runtime-1",
			RequestedName:      "desk",
			Source:             domain.RuntimeSourceSelfHosted,
			Role:               domain.CredentialRoleExecutor,
			FailureCode:        "permission_denied",
			Reason:             "credential consumed by runtime runtime-1",
			ConsumedRuntimeID:  "runtime-1",
			FirstSeenAt:        now,
			LastSeenAt:         now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("RecordRuntimeAdmissionFailure #%d: %v", i+1, err)
		}
	}
	failures, err := repo.ListRuntimeAdmissionFailuresByUser(ctx, 42, 20)
	if err != nil {
		t.Fatalf("ListRuntimeAdmissionFailuresByUser: %v", err)
	}
	if len(failures) != 1 || failures[0].AttemptCount != 2 || failures[0].ConsumedRuntimeID != "runtime-1" {
		t.Fatalf("failures = %+v, want one rolled-up failure with attempt_count=2", failures)
	}
}

func TestTimescaleRepositoryCreateHostedRuntimeRejectsConsumedCredentialReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}
	if err := createTempRuntimeCredentials(ctx, db); err != nil {
		t.Fatalf("create temp runtime_credentials: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	_, err := db.ExecContext(ctx, `
		INSERT INTO runtime_credentials (
			key_id, user_id, public_key_pem, label, role, status, created_at,
			downloaded_at, consumed_at, consumed_runtime_id, hosted_internal
		) VALUES (
			'hosted-key', 42, 'public-key', 'hosted', 'executor', 'consumed',
			$1, $1, $1, 'rt-hosted', TRUE
		)`, now)
	if err != nil {
		t.Fatalf("insert credential: %v", err)
	}

	repo := &TimescaleRepository{db: db}
	firstHeartbeat := now
	if err := repo.CreateRuntime(ctx, domain.Runtime{
		RuntimeID:       "rt-hosted",
		CredentialKeyID: "hosted-key",
		UserID:          42,
		Name:            "hosted-one",
		Source:          domain.RuntimeSourceHosted,
		Role:            domain.CredentialRoleExecutor,
		EndpointHost:    "127.0.0.1",
		GRPCPort:        50106,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusUnhealthy,
		HeartbeatAt:     &firstHeartbeat,
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatalf("CreateRuntime: %v", err)
	}

	reconnectAt := now.Add(2 * time.Minute)
	err = repo.CreateOrReplaceHostedRuntime(ctx, domain.Runtime{
		RuntimeID:       "rt-hosted",
		CredentialKeyID: "hosted-key",
		UserID:          42,
		Name:            "hosted-one",
		Source:          domain.RuntimeSourceHosted,
		Role:            domain.CredentialRoleExecutor,
		EndpointHost:    "127.0.0.1",
		GRPCPort:        50106,
		ResourceProfile: "small",
		Status:          domain.RuntimeStatusActive,
		HeartbeatAt:     &reconnectAt,
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       reconnectAt,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("CreateOrReplaceHostedRuntime reconnect err = %v, want ErrConflict", err)
	}
}

func TestTimescaleRepositoryConnectionOwnerMoveDoesNotLetStaleOwnerClear(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openRepositoryTestDB(t, ctx)
	defer db.Close()
	if err := createTempRuntimeRegistry(ctx, db); err != nil {
		t.Fatalf("create temp runtime_registry: %v", err)
	}

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	repo := &TimescaleRepository{db: db}
	if err := repo.CreateRuntime(ctx, domain.Runtime{
		RuntimeID:       "runtime-owner",
		UserID:          42,
		Name:            "owner-runtime",
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
	if err := repo.RecordRuntimeConnectionOwner(ctx, "runtime-owner", "cp-old", now); err != nil {
		t.Fatalf("record old owner: %v", err)
	}
	if err := repo.RecordRuntimeConnectionOwner(ctx, "runtime-owner", "cp-new", now.Add(time.Second)); err != nil {
		t.Fatalf("record new owner: %v", err)
	}
	if err := repo.ClearRuntimeConnectionOwner(ctx, "runtime-owner", "cp-old"); err != nil {
		t.Fatalf("clear old owner: %v", err)
	}
	var owner string
	if err := db.QueryRowContext(ctx, `SELECT connection_owner_instance_id FROM runtime_registry WHERE runtime_id = 'runtime-owner'`).Scan(&owner); err != nil {
		t.Fatalf("query owner after stale clear: %v", err)
	}
	if owner != "cp-new" {
		t.Fatalf("owner after stale clear = %q, want cp-new", owner)
	}
	if err := repo.ClearRuntimeConnectionOwner(ctx, "runtime-owner", "cp-new"); err != nil {
		t.Fatalf("clear new owner: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT connection_owner_instance_id FROM runtime_registry WHERE runtime_id = 'runtime-owner'`).Scan(&owner); err != nil {
		t.Fatalf("query owner after current clear: %v", err)
	}
	if owner != "" {
		t.Fatalf("owner after current clear = %q, want empty", owner)
	}
}

func openRepositoryTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CONTROL_PANEL_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("TIMESCALEDB_DSN")
	}
	if dsn == "" {
		t.Skip("CONTROL_PANEL_TEST_DSN or TIMESCALEDB_DSN is required for TimescaleRepository DB-backed tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("postgres unavailable for TimescaleRepository DB-backed tests: %v", err)
	}
	return db
}

func createTempRuntimeRegistry(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE runtime_registry (
			runtime_id TEXT PRIMARY KEY,
			user_id BIGINT,
			name TEXT NOT NULL,
			source TEXT NOT NULL,
			role TEXT NOT NULL DEFAULT 'executor',
			endpoint_host TEXT NOT NULL DEFAULT '',
			grpc_port INT NOT NULL DEFAULT 0,
			debug_port INT,
			capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
			resource_profile TEXT NOT NULL DEFAULT '',
			version TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			token_hash TEXT NOT NULL DEFAULT '',
			paired_at TIMESTAMPTZ,
			started_at TIMESTAMPTZ,
			ended_at TIMESTAMPTZ,
			ended_reason TEXT NOT NULL DEFAULT '',
			cleanup_status TEXT NOT NULL DEFAULT '',
			cleanup_reason TEXT NOT NULL DEFAULT '',
			cleanup_at TIMESTAMPTZ,
			heartbeat_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			credential_key_id TEXT,
			connection_owner_instance_id TEXT NOT NULL DEFAULT '',
			connection_owner_acquired_at TIMESTAMPTZ,
			connection_owner_heartbeat_at TIMESTAMPTZ
		) ON COMMIT PRESERVE ROWS`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func createTempRuntimeCredentials(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE runtime_credentials (
			key_id TEXT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			public_key_pem TEXT NOT NULL,
			label TEXT NOT NULL DEFAULT '',
			role TEXT NOT NULL DEFAULT 'executor',
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL,
			downloaded_at TIMESTAMPTZ,
			consumed_at TIMESTAMPTZ,
			consumed_runtime_id TEXT NOT NULL DEFAULT '',
			expires_at TIMESTAMPTZ,
			last_used_at TIMESTAMPTZ,
			revoked_at TIMESTAMPTZ,
			hosted_internal BOOLEAN NOT NULL DEFAULT FALSE
		) ON COMMIT PRESERVE ROWS`)
	if err != nil {
		return fmt.Errorf("create runtime_credentials: %w", err)
	}
	return nil
}

func createTempRuntimeChannelLeases(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE runtime_channel_leases (
			runtime_id TEXT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			credential_key_id TEXT NOT NULL,
			lease_hash TEXT NOT NULL,
			issued_at TIMESTAMPTZ NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			last_used_at TIMESTAMPTZ,
			revoked_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		) ON COMMIT PRESERVE ROWS;
		CREATE UNIQUE INDEX uq_runtime_channel_leases_hash
			ON runtime_channel_leases (lease_hash)
			WHERE revoked_at IS NULL`)
	if err != nil {
		return fmt.Errorf("create runtime_channel_leases: %w", err)
	}
	return nil
}

func createTempRuntimeAdmissionFailures(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE runtime_admission_failures (
			admission_failure_id BIGSERIAL PRIMARY KEY,
			user_id BIGINT NOT NULL DEFAULT 0,
			credential_key_id TEXT NOT NULL DEFAULT '',
			requested_runtime_id TEXT NOT NULL DEFAULT '',
			requested_name TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '',
			role TEXT NOT NULL DEFAULT '',
			failure_code TEXT NOT NULL DEFAULT '',
			reason TEXT NOT NULL,
			consumed_runtime_id TEXT NOT NULL DEFAULT '',
			first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			attempt_count INTEGER NOT NULL DEFAULT 1
		) ON COMMIT PRESERVE ROWS;
		CREATE UNIQUE INDEX uq_runtime_admission_failures_rollup
			ON runtime_admission_failures (
				user_id,
				credential_key_id,
				requested_runtime_id,
				requested_name,
				failure_code,
				consumed_runtime_id
			)`)
	if err != nil {
		return fmt.Errorf("create runtime_admission_failures: %w", err)
	}
	return nil
}
