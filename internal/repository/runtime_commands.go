package repository

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
)

const runtimeCommandSelectColumns = `
	command_id, user_id, runtime_id, session_id, idempotency_key,
	command_type, status, deadline_at, sent_at, acked_at, started_at,
	completed_at, cancelled_at, attempt_count, payload::text, result::text,
	failure_reason, created_at, updated_at`

func (r *TimescaleRepository) CreateRuntimeCommand(ctx context.Context, cmd domain.RuntimeCommand) (domain.RuntimeCommand, bool, error) {
	cmd = normalizeRuntimeCommandForWrite(cmd)
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO runtime_commands (
			command_id, user_id, runtime_id, session_id, idempotency_key,
			command_type, status, deadline_at, payload, result,
			failure_reason, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9::jsonb, $10::jsonb,
			$11, $12, $13
		)
		ON CONFLICT (runtime_id, idempotency_key)
		WHERE idempotency_key <> ''
		DO NOTHING
		RETURNING `+runtimeCommandSelectColumns,
		cmd.CommandID, cmd.UserID, cmd.RuntimeID, cmd.SessionID, cmd.IdempotencyKey,
		cmd.CommandType, cmd.Status, cmd.DeadlineAt.UTC(), cmd.Payload, cmd.Result,
		cmd.FailureReason, cmd.CreatedAt.UTC(), cmd.UpdatedAt.UTC(),
	)
	inserted, err := scanRuntimeCommand(row)
	if err == nil {
		return inserted, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		if isUniqueViolation(err) {
			return domain.RuntimeCommand{}, false, ErrConflict
		}
		return domain.RuntimeCommand{}, false, err
	}
	if cmd.IdempotencyKey == "" {
		return domain.RuntimeCommand{}, false, ErrConflict
	}
	existing, err := r.GetRuntimeCommandByIdempotencyKey(ctx, cmd.RuntimeID, cmd.IdempotencyKey)
	if err != nil {
		return domain.RuntimeCommand{}, false, err
	}
	return existing, true, nil
}

func (r *TimescaleRepository) GetRuntimeCommand(ctx context.Context, commandID string) (domain.RuntimeCommand, error) {
	row := r.db.QueryRowContext(ctx, `SELECT `+runtimeCommandSelectColumns+` FROM runtime_commands WHERE command_id = $1`, commandID)
	return scanRuntimeCommand(row)
}

func (r *TimescaleRepository) GetRuntimeCommandByIdempotencyKey(ctx context.Context, runtimeID, idempotencyKey string) (domain.RuntimeCommand, error) {
	row := r.db.QueryRowContext(ctx, `SELECT `+runtimeCommandSelectColumns+` FROM runtime_commands WHERE runtime_id = $1 AND idempotency_key = $2`, runtimeID, idempotencyKey)
	return scanRuntimeCommand(row)
}

func (r *TimescaleRepository) ClaimNextRuntimeCommand(ctx context.Context, runtimeID, ownerInstanceID string, at time.Time, inFlightLimit int) (domain.RuntimeCommand, bool, error) {
	if runtimeID == "" || ownerInstanceID == "" {
		return domain.RuntimeCommand{}, false, ErrNotFound
	}
	if inFlightLimit <= 0 {
		inFlightLimit = 1
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RuntimeCommand{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	var owner string
	if err := tx.QueryRowContext(ctx, `SELECT connection_owner_instance_id FROM runtime_registry WHERE runtime_id = $1`, runtimeID).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.RuntimeCommand{}, false, ErrNotFound
		}
		return domain.RuntimeCommand{}, false, err
	}
	if owner != ownerInstanceID {
		return domain.RuntimeCommand{}, false, nil
	}

	var inFlight int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_commands
		WHERE runtime_id = $1
		  AND status IN ('sent', 'acked', 'running')`, runtimeID).Scan(&inFlight); err != nil {
		return domain.RuntimeCommand{}, false, err
	}
	if inFlight >= inFlightLimit {
		return domain.RuntimeCommand{}, false, nil
	}

	row := tx.QueryRowContext(ctx, `
		UPDATE runtime_commands
		SET status = 'sent',
		    sent_at = $2,
		    attempt_count = attempt_count + 1,
		    updated_at = $2
		WHERE command_id = (
			SELECT command_id
			FROM runtime_commands
			WHERE runtime_id = $1
			  AND status = 'queued'
			ORDER BY created_at ASC, command_id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+runtimeCommandSelectColumns,
		runtimeID, at.UTC(),
	)
	cmd, err := scanRuntimeCommand(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return domain.RuntimeCommand{}, false, nil
		}
		return domain.RuntimeCommand{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.RuntimeCommand{}, false, err
	}
	return cmd, true, nil
}

func (r *TimescaleRepository) AcknowledgeRuntimeCommand(ctx context.Context, commandID string, at time.Time) (domain.RuntimeCommand, error) {
	row := r.db.QueryRowContext(ctx, `
		UPDATE runtime_commands
		SET status = 'acked',
		    acked_at = COALESCE(acked_at, $2),
		    updated_at = $2
		WHERE command_id = $1
		  AND status IN ('sent', 'acked')
		RETURNING `+runtimeCommandSelectColumns,
		commandID, at.UTC(),
	)
	return scanRuntimeCommand(row)
}

func (r *TimescaleRepository) MarkRuntimeCommandRunning(ctx context.Context, commandID string, at time.Time) (domain.RuntimeCommand, error) {
	row := r.db.QueryRowContext(ctx, `
		UPDATE runtime_commands
		SET status = 'running',
		    started_at = COALESCE(started_at, $2),
		    updated_at = $2
		WHERE command_id = $1
		  AND status IN ('sent', 'acked', 'running')
		RETURNING `+runtimeCommandSelectColumns,
		commandID, at.UTC(),
	)
	return scanRuntimeCommand(row)
}

func (r *TimescaleRepository) CompleteRuntimeCommand(ctx context.Context, commandID, status string, result []byte, failureReason string, at time.Time) (domain.RuntimeCommand, error) {
	if !domain.IsRuntimeCommandTerminalStatus(status) {
		return domain.RuntimeCommand{}, fmt.Errorf("%w: invalid terminal command status %q", ErrConflict, status)
	}
	if len(result) == 0 {
		result = []byte("{}")
	}
	result = compactJSONOrDefault(result, []byte("{}"))
	row := r.db.QueryRowContext(ctx, `
		UPDATE runtime_commands
		SET status = $2,
		    result = $3::jsonb,
		    failure_reason = $4,
		    completed_at = CASE WHEN $2 <> 'cancelled' THEN COALESCE(completed_at, $5) ELSE completed_at END,
		    cancelled_at = CASE WHEN $2 = 'cancelled' THEN COALESCE(cancelled_at, $5) ELSE cancelled_at END,
		    updated_at = $5
		WHERE command_id = $1
		  AND status NOT IN ('succeeded', 'failed', 'timed_out', 'cancelled')
		RETURNING `+runtimeCommandSelectColumns,
		commandID, status, result, failureReason, at.UTC(),
	)
	return scanRuntimeCommand(row)
}

func (r *TimescaleRepository) TimeoutRuntimeCommands(ctx context.Context, at time.Time, failureReason string) ([]domain.RuntimeCommand, error) {
	rows, err := r.db.QueryContext(ctx, `
		UPDATE runtime_commands
		SET status = 'timed_out',
		    failure_reason = $2,
		    completed_at = COALESCE(completed_at, $1),
		    updated_at = $1
		WHERE deadline_at <= $1
		  AND status IN ('queued', 'sent', 'acked', 'running')
		RETURNING `+runtimeCommandSelectColumns,
		at.UTC(), failureReason,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.RuntimeCommand
	for rows.Next() {
		cmd, err := scanRuntimeCommand(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cmd)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) CountRecentRuntimeCommandFailures(ctx context.Context, runtimeID string, since time.Time) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_commands
		WHERE runtime_id = $1
		  AND status IN ('failed', 'timed_out')
		  AND updated_at >= $2`,
		runtimeID, since.UTC(),
	).Scan(&count)
	return count, err
}

func (r *TimescaleRepository) RuntimeCommandCircuitOpen(ctx context.Context, runtimeID string, since time.Time, threshold int64) (bool, int64, error) {
	if threshold <= 0 {
		threshold = 1
	}
	failures, err := r.CountRecentRuntimeCommandFailures(ctx, runtimeID, since)
	if err != nil {
		return false, 0, err
	}
	return failures >= threshold, failures, nil
}

func normalizeRuntimeCommandForWrite(cmd domain.RuntimeCommand) domain.RuntimeCommand {
	if cmd.Status == "" {
		cmd.Status = domain.RuntimeCommandStatusQueued
	}
	if len(cmd.Payload) == 0 {
		cmd.Payload = []byte("{}")
	}
	if len(cmd.Result) == 0 {
		cmd.Result = []byte("{}")
	}
	cmd.Payload = compactJSONOrDefault(cmd.Payload, []byte("{}"))
	cmd.Result = compactJSONOrDefault(cmd.Result, []byte("{}"))
	if cmd.CreatedAt.IsZero() {
		cmd.CreatedAt = time.Now().UTC()
	}
	if cmd.UpdatedAt.IsZero() {
		cmd.UpdatedAt = cmd.CreatedAt
	}
	return cmd
}

func scanRuntimeCommand(s interface{ Scan(...any) error }) (domain.RuntimeCommand, error) {
	var cmd domain.RuntimeCommand
	var sentAt, ackedAt, startedAt, completedAt, cancelledAt sql.NullTime
	var payloadText, resultText string
	err := s.Scan(
		&cmd.CommandID, &cmd.UserID, &cmd.RuntimeID, &cmd.SessionID, &cmd.IdempotencyKey,
		&cmd.CommandType, &cmd.Status, &cmd.DeadlineAt, &sentAt, &ackedAt, &startedAt,
		&completedAt, &cancelledAt, &cmd.AttemptCount, &payloadText, &resultText,
		&cmd.FailureReason, &cmd.CreatedAt, &cmd.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.RuntimeCommand{}, ErrNotFound
		}
		return domain.RuntimeCommand{}, err
	}
	cmd.Payload = compactJSONOrDefault([]byte(payloadText), []byte("{}"))
	cmd.Result = compactJSONOrDefault([]byte(resultText), []byte("{}"))
	if sentAt.Valid {
		cmd.SentAt = &sentAt.Time
	}
	if ackedAt.Valid {
		cmd.AckedAt = &ackedAt.Time
	}
	if startedAt.Valid {
		cmd.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		cmd.CompletedAt = &completedAt.Time
	}
	if cancelledAt.Valid {
		cmd.CancelledAt = &cancelledAt.Time
	}
	return cmd, nil
}

func compactJSONOrDefault(raw []byte, fallback []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return append([]byte(nil), fallback...)
	}
	return buf.Bytes()
}
