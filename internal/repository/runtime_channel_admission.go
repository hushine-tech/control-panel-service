package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
)

const runtimeChannelLeaseSelectColumns = `
	runtime_id, user_id, credential_key_id, lease_hash, issued_at, expires_at,
	last_used_at, revoked_at, created_at, updated_at`

func (r *TimescaleRepository) CreateRuntimeChannelLease(ctx context.Context, lease domain.RuntimeChannelLease) error {
	if lease.RuntimeID == "" || lease.UserID <= 0 || lease.CredentialKeyID == "" || lease.LeaseHash == "" {
		return ErrConflict
	}
	if lease.IssuedAt.IsZero() {
		lease.IssuedAt = time.Now().UTC()
	}
	if lease.ExpiresAt.IsZero() {
		lease.ExpiresAt = lease.IssuedAt.Add(24 * time.Hour)
	}
	if lease.CreatedAt.IsZero() {
		lease.CreatedAt = lease.IssuedAt
	}
	lease.UpdatedAt = lease.IssuedAt
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO runtime_channel_leases (
			runtime_id, user_id, credential_key_id, lease_hash, issued_at,
			expires_at, last_used_at, revoked_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10
		)
		ON CONFLICT (runtime_id) DO UPDATE SET
			user_id = EXCLUDED.user_id,
			credential_key_id = EXCLUDED.credential_key_id,
			lease_hash = EXCLUDED.lease_hash,
			issued_at = EXCLUDED.issued_at,
			expires_at = EXCLUDED.expires_at,
			last_used_at = EXCLUDED.last_used_at,
			revoked_at = EXCLUDED.revoked_at,
			updated_at = EXCLUDED.updated_at`,
		lease.RuntimeID, lease.UserID, lease.CredentialKeyID, lease.LeaseHash,
		lease.IssuedAt.UTC(), lease.ExpiresAt.UTC(), nullableTime(lease.LastUsedAt),
		nullableTime(lease.RevokedAt), lease.CreatedAt.UTC(), lease.UpdatedAt.UTC(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	return nil
}

func (r *TimescaleRepository) GetRuntimeChannelLeaseByHash(ctx context.Context, leaseHash string) (domain.RuntimeChannelLease, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT `+runtimeChannelLeaseSelectColumns+`
		FROM runtime_channel_leases
		WHERE lease_hash = $1
		  AND revoked_at IS NULL`,
		leaseHash,
	)
	lease, err := scanRuntimeChannelLease(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RuntimeChannelLease{}, ErrNotFound
	}
	return lease, err
}

func (r *TimescaleRepository) TouchRuntimeChannelLease(ctx context.Context, runtimeID, leaseHash string, at time.Time) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_channel_leases
		SET last_used_at = $3,
		    updated_at = $3
		WHERE runtime_id = $1
		  AND lease_hash = $2
		  AND revoked_at IS NULL
		  AND expires_at > $3`,
		runtimeID, leaseHash, at.UTC(),
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) RotateRuntimeChannelLease(ctx context.Context, runtimeID, oldLeaseHash, newLeaseHash string, expiresAt, at time.Time) error {
	if runtimeID == "" || oldLeaseHash == "" || newLeaseHash == "" || expiresAt.IsZero() {
		return ErrConflict
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_channel_leases
		SET lease_hash = $3,
		    expires_at = $4,
		    last_used_at = $5,
		    updated_at = $5
		WHERE runtime_id = $1
		  AND lease_hash = $2
		  AND revoked_at IS NULL
		  AND expires_at > $5`,
		runtimeID, oldLeaseHash, newLeaseHash, expiresAt.UTC(), at.UTC(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

const runtimeAdmissionFailureSelectColumns = `
	admission_failure_id, user_id, credential_key_id, requested_runtime_id,
	requested_name, source, role, failure_code, reason, consumed_runtime_id,
	first_seen_at, last_seen_at, attempt_count`

func (r *TimescaleRepository) RecordRuntimeAdmissionFailure(ctx context.Context, failure domain.RuntimeAdmissionFailure) error {
	if failure.Reason == "" {
		failure.Reason = "runtime admission failed"
	}
	if failure.LastSeenAt.IsZero() {
		failure.LastSeenAt = time.Now().UTC()
	}
	if failure.FirstSeenAt.IsZero() {
		failure.FirstSeenAt = failure.LastSeenAt
	}
	if failure.AttemptCount <= 0 {
		failure.AttemptCount = 1
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO runtime_admission_failures (
			user_id, credential_key_id, requested_runtime_id, requested_name,
			source, role, failure_code, reason, consumed_runtime_id,
			first_seen_at, last_seen_at, attempt_count
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9,
			$10, $11, $12
		)
		ON CONFLICT (
			user_id, credential_key_id, requested_runtime_id, requested_name,
			failure_code, consumed_runtime_id
		) DO UPDATE SET
			source = EXCLUDED.source,
			role = EXCLUDED.role,
			reason = EXCLUDED.reason,
			last_seen_at = EXCLUDED.last_seen_at,
			attempt_count = runtime_admission_failures.attempt_count + 1`,
		failure.UserID, failure.CredentialKeyID, failure.RequestedRuntimeID, failure.RequestedName,
		failure.Source, string(failure.Role), failure.FailureCode, failure.Reason, failure.ConsumedRuntimeID,
		failure.FirstSeenAt.UTC(), failure.LastSeenAt.UTC(), failure.AttemptCount,
	)
	return err
}

func (r *TimescaleRepository) ListRuntimeAdmissionFailuresByUser(ctx context.Context, userID int64, limit int) ([]domain.RuntimeAdmissionFailure, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT `+runtimeAdmissionFailureSelectColumns+`
		FROM runtime_admission_failures
		WHERE user_id = $1
		ORDER BY last_seen_at DESC, admission_failure_id DESC
		LIMIT $2`,
		userID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.RuntimeAdmissionFailure
	for rows.Next() {
		failure, err := scanRuntimeAdmissionFailure(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, failure)
	}
	return out, rows.Err()
}

type runtimeChannelLeaseScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeChannelLease(row runtimeChannelLeaseScanner) (domain.RuntimeChannelLease, error) {
	var lease domain.RuntimeChannelLease
	var lastUsedAt, revokedAt sql.NullTime
	if err := row.Scan(
		&lease.RuntimeID, &lease.UserID, &lease.CredentialKeyID, &lease.LeaseHash,
		&lease.IssuedAt, &lease.ExpiresAt, &lastUsedAt, &revokedAt,
		&lease.CreatedAt, &lease.UpdatedAt,
	); err != nil {
		return domain.RuntimeChannelLease{}, err
	}
	if lastUsedAt.Valid {
		t := lastUsedAt.Time
		lease.LastUsedAt = &t
	}
	if revokedAt.Valid {
		t := revokedAt.Time
		lease.RevokedAt = &t
	}
	return lease, nil
}

type runtimeAdmissionFailureScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeAdmissionFailure(row runtimeAdmissionFailureScanner) (domain.RuntimeAdmissionFailure, error) {
	var failure domain.RuntimeAdmissionFailure
	var role string
	if err := row.Scan(
		&failure.AdmissionFailureID, &failure.UserID, &failure.CredentialKeyID,
		&failure.RequestedRuntimeID, &failure.RequestedName, &failure.Source,
		&role, &failure.FailureCode, &failure.Reason, &failure.ConsumedRuntimeID,
		&failure.FirstSeenAt, &failure.LastSeenAt, &failure.AttemptCount,
	); err != nil {
		return domain.RuntimeAdmissionFailure{}, err
	}
	failure.Role = domain.CredentialRole(role)
	return failure, nil
}
