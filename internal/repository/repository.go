package repository

import (
	"context"
	"errors"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
)

var (
	// ErrNotFound is returned for misses on Get/Find lookups.
	ErrNotFound = errors.New("not found")

	// ErrConflict is returned when a uniqueness invariant blocks an insert
	// (e.g. another runtime already owns this user's runtime name,
	// a credential is already bound, or a runtime_id collides with an
	// existing row).
	ErrConflict = errors.New("conflict")
)

// Repository is the persistence interface for control-panel-service.
//
// Mutations spanning related rows are expressed as one repository method per
// logical action so the SQL layer can wrap them in a single transaction.
type Repository interface {
	Close() error

	// ── Runtime registry ────────────────────────────────────────────────────

	CreateRuntime(ctx context.Context, r domain.Runtime) error

	// CreateOrReplaceHostedRuntime inserts a hosted runtime only when the
	// (r.UserID, r.Name) slot has no owner across any source/status.
	// Existing occupants are never mutated; callers must choose a fresh
	// runtime name because ended rows retain identity for audit.
	// MUST only be called when r.Source == "hosted" and r.UserID > 0.
	CreateOrReplaceHostedRuntime(ctx context.Context, r domain.Runtime) error

	// CreateOrReplaceSelfHostedRuntime is the RuntimeChannel HELLO variant.
	// It binds the signed credential's owning user to the self-hosted runtime
	// immediately, rejects reuse of the same credential by another
	// non-ended runtime, allows runtime_id reconnect only for the same
	// user/credential, and inserts or refreshes the active runtime row without
	// ending other runtimes.
	CreateOrReplaceSelfHostedRuntime(ctx context.Context, r domain.Runtime) error

	// GetRuntime fetches by primary key. Returns ErrNotFound on miss.
	GetRuntime(ctx context.Context, runtimeID string) (domain.Runtime, error)

	// ListRuntimes returns runtimes for a user with optional status/source
	// filters. items has up to ``limit`` rows; ``hasMore`` follows the
	// limit+1 sentinel pattern; ``total`` is a separate COUNT(*) used by
	// pager First/Last/jump controls.
	ListRuntimes(ctx context.Context, userID int64, statusFilter, sourceFilter string, limit, offset int) (items []domain.Runtime, total int64, hasMore bool, err error)

	// UpdateRuntimeStatus changes status only; updated_at is bumped.
	UpdateRuntimeStatus(ctx context.Context, runtimeID, status string) error

	// MarkStaleRuntimesUnhealthy flips active/paired runtimes whose last
	// heartbeat/update is older than cutoff to unhealthy and returns the
	// affected rows. The service layer uses the returned runtime_id values
	// to stop sessions bound to dead runtimes.
	MarkStaleRuntimesUnhealthy(ctx context.Context, cutoff time.Time) ([]domain.Runtime, error)

	// UpdateRuntimeHeartbeat sets heartbeat_at to ``at``; if status is
	// 'paired' or 'unhealthy' it also flips status to 'active'. updated_at
	// is bumped.
	UpdateRuntimeHeartbeat(ctx context.Context, runtimeID string, at time.Time) error

	// RecordRuntimeConnectionOwner records the control-panel instance that
	// currently owns the RuntimeChannel stream for this runtime.
	RecordRuntimeConnectionOwner(ctx context.Context, runtimeID, instanceID string, at time.Time) error

	// ClearRuntimeConnectionOwner clears ownership only when the stored owner
	// still matches instanceID, so a stale disconnect cannot erase a newer
	// reconnect handled by another control-panel instance.
	ClearRuntimeConnectionOwner(ctx context.Context, runtimeID, instanceID string) error

	// CountRuntimesByUser counts non-ended runtimes by source for a user.
	// Used during quota checks.
	CountRuntimesByUser(ctx context.Context, userID int64) (domain.RuntimeUsageCounts, error)

	// EndRuntime terminally ends a runtime row while preserving identity for
	// audit. Returns the ended row.
	EndRuntime(ctx context.Context, runtimeID, reason string, endedAt time.Time) (domain.Runtime, error)

	// EndRuntimesByCredentialKey ends every non-ended runtime row authenticated
	// by the given RuntimeChannel credential. Used by credential revocation so
	// disconnected self-hosted runtimes cannot remain routable after their key
	// is revoked.
	EndRuntimesByCredentialKey(ctx context.Context, keyID, reason string, endedAt time.Time) (int64, error)

	// EndDeadRuntimes terminally ends active/paired/unhealthy runtimes older
	// than cutoff and returns the affected rows.
	EndDeadRuntimes(ctx context.Context, cutoff time.Time, reason string, endedAt time.Time) ([]domain.Runtime, error)

	// UpdateRuntimeCleanupState persists hosted deprovision/self-hosted
	// cleanup ownership state on the runtime row. reason must be non-secret
	// operator/user guidance.
	UpdateRuntimeCleanupState(ctx context.Context, runtimeID, status, reason string, at time.Time) error

	// ── Runtime credentials (Phase D3) ──────────────────────────────────────

	// CreateRuntimeCredential persists a new credential row. The caller is
	// responsible for generating the keypair, deriving key_id, and
	// PEM-encoding both the public and private halves; this method only
	// stores the public half. The private half MUST NOT be persisted
	// anywhere on the platform.
	CreateRuntimeCredential(ctx context.Context, c domain.RuntimeCredential) error

	// GetRuntimeCredential fetches by key_id (regardless of status) and
	// returns ErrNotFound on miss. Used by the HELLO verification path
	// to look up the public key.
	GetRuntimeCredential(ctx context.Context, keyID string) (domain.RuntimeCredential, error)

	// ListRuntimeCredentialsByUser returns the credentials owned by a user.
	// If includeInactive=false, active/downloaded/consumed rows are returned;
	// revoked/expired rows are hidden by default.
	// Ordered by created_at DESC.
	ListRuntimeCredentialsByUser(ctx context.Context, userID int64, includeInactive bool) ([]domain.RuntimeCredential, error)

	// RevokeRuntimeCredential flips status to 'revoked' and stamps
	// revoked_at=NOW(). userID is cross-checked against the credential's
	// owner — mismatch returns ErrConflict (caller maps to PermissionDenied).
	// Returns ErrNotFound if the credential does not exist; idempotent if
	// the credential is already revoked (returns the existing revoked row
	// unchanged).
	RevokeRuntimeCredential(ctx context.Context, keyID string, userID int64) (domain.RuntimeCredential, error)

	// TouchRuntimeCredentialUsed bumps last_used_at to ``at``. Best-effort,
	// non-transactional; failures are logged but do not block HELLO
	// verification.
	TouchRuntimeCredentialUsed(ctx context.Context, keyID string, at time.Time) error

	// ── RuntimeChannel leases and admission failures ───────────────────────

	CreateRuntimeChannelLease(ctx context.Context, lease domain.RuntimeChannelLease) error
	GetRuntimeChannelLeaseByHash(ctx context.Context, leaseHash string) (domain.RuntimeChannelLease, error)
	TouchRuntimeChannelLease(ctx context.Context, runtimeID, leaseHash string, at time.Time) error
	RotateRuntimeChannelLease(ctx context.Context, runtimeID, oldLeaseHash, newLeaseHash string, expiresAt, at time.Time) error
	RecordRuntimeAdmissionFailure(ctx context.Context, failure domain.RuntimeAdmissionFailure) error
	ListRuntimeAdmissionFailuresByUser(ctx context.Context, userID int64, limit int) ([]domain.RuntimeAdmissionFailure, error)
}
