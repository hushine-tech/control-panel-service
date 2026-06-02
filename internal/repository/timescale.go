package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

// TimescaleRepository implements Repository against PostgreSQL/TimescaleDB.
type TimescaleRepository struct {
	db     *sql.DB
	logger elog.Logger
}

// NewTimescaleRepository opens a connection and pings. Migrations are applied
// out-of-band by “cmd/ensure-control-panel-db“; this constructor does NOT
// run migrations on every service boot (mirrors the convention; account-
// service runs migrations on boot but that is the older pattern).
func NewTimescaleRepository(dsn string, logger elog.Logger) (*TimescaleRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open timescaledb: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping timescaledb: %w", err)
	}
	return &TimescaleRepository{db: db, logger: logger}, nil
}

func (r *TimescaleRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// DB exposes the underlying connection pool so a sibling subdomain (e.g.
// marketdata) can share the pool without opening a second connection.
// Only call from within control-panel-service; do not export across
// service boundaries.
func (r *TimescaleRepository) DB() *sql.DB {
	if r == nil {
		return nil
	}
	return r.db
}

// ── Runtime registry ────────────────────────────────────────────────────────

func (r *TimescaleRepository) CreateRuntime(ctx context.Context, rt domain.Runtime) error {
	rt = normalizeRuntimeForWrite(rt)
	caps, err := marshalCapabilities(rt.Capabilities)
	if err != nil {
		return fmt.Errorf("marshal capabilities: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO runtime_registry (
			runtime_id, user_id, name, source, role, endpoint_host, grpc_port,
			debug_port, capabilities, resource_profile, version, status,
			token_hash, paired_at, started_at, ended_at, ended_reason,
			heartbeat_at, created_at, updated_at, credential_key_id
		) VALUES (
			$1, NULLIF($2, 0)::BIGINT, $3, $4, $5, $6, $7,
			NULLIF($8, 0)::INT, $9::JSONB, $10, $11, $12,
			$13, $14, $15, $16, $17,
			$18, $19, $20, NULLIF($21, '')
		)`,
		rt.RuntimeID, rt.UserID, rt.Name, rt.Source, string(rt.Role), rt.EndpointHost, rt.GRPCPort,
		rt.DebugPort, string(caps), rt.ResourceProfile, rt.Version, rt.Status,
		rt.TokenHash, nullableTime(rt.PairedAt), nullableTime(rt.StartedAt),
		nullableTime(rt.EndedAt), rt.EndedReason, nullableTime(rt.HeartbeatAt),
		rt.CreatedAt, rt.UpdatedAt, rt.CredentialKeyID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	return nil
}

// CreateOrReplaceHostedRuntime inserts a hosted runtime or refreshes the same
// non-terminal runtime during RuntimeChannel reconnect. Other rows keep the
// (user_id, name) slot occupied across every source/status.
func (r *TimescaleRepository) CreateOrReplaceHostedRuntime(ctx context.Context, rt domain.Runtime) error {
	if rt.Source != domain.RuntimeSourceHosted {
		return fmt.Errorf("CreateOrReplaceHostedRuntime: source must be hosted, got %q", rt.Source)
	}
	if rt.UserID <= 0 {
		return fmt.Errorf("CreateOrReplaceHostedRuntime: user_id must be > 0")
	}
	rt = normalizeRuntimeForWrite(rt)
	caps, err := marshalCapabilities(rt.Capabilities)
	if err != nil {
		return fmt.Errorf("marshal capabilities: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existingRuntimeID string
	err = tx.QueryRowContext(ctx, `
		SELECT runtime_id
		FROM runtime_registry
		WHERE user_id = $1
		  AND name = $2
		  AND runtime_id <> $3
		ORDER BY updated_at DESC
		LIMIT 1
		FOR UPDATE`,
		rt.UserID, rt.Name, rt.RuntimeID,
	).Scan(&existingRuntimeID)
	if err == nil {
		return fmt.Errorf("%w: hosted runtime slot already occupied by %s", ErrConflict, existingRuntimeID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check hosted runtime slot: %w", err)
	}
	if err := consumeRuntimeCredentialForRuntimeTx(ctx, tx, rt, rt.UpdatedAt); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_registry (
			runtime_id, user_id, name, source, role, endpoint_host, grpc_port,
			debug_port, capabilities, resource_profile, version, status,
			token_hash, paired_at, started_at, ended_at, ended_reason,
			heartbeat_at, created_at, updated_at, credential_key_id
		) VALUES (
			$1, NULLIF($2, 0)::BIGINT, $3, $4, $5, $6, $7,
			NULLIF($8, 0)::INT, $9::JSONB, $10, $11, $12,
			$13, $14, $15, $16, $17,
			$18, $19, $20, NULLIF($21, '')
		)
		ON CONFLICT (runtime_id) DO UPDATE SET
			user_id = EXCLUDED.user_id,
			name = EXCLUDED.name,
			source = EXCLUDED.source,
			role = EXCLUDED.role,
			endpoint_host = EXCLUDED.endpoint_host,
			grpc_port = EXCLUDED.grpc_port,
			debug_port = EXCLUDED.debug_port,
			capabilities = EXCLUDED.capabilities,
			resource_profile = EXCLUDED.resource_profile,
			version = EXCLUDED.version,
			status = EXCLUDED.status,
			token_hash = EXCLUDED.token_hash,
			paired_at = EXCLUDED.paired_at,
			started_at = EXCLUDED.started_at,
			ended_at = EXCLUDED.ended_at,
			ended_reason = EXCLUDED.ended_reason,
			heartbeat_at = EXCLUDED.heartbeat_at,
			credential_key_id = EXCLUDED.credential_key_id,
			updated_at = EXCLUDED.updated_at
		WHERE runtime_registry.user_id = EXCLUDED.user_id
		  AND runtime_registry.source = EXCLUDED.source
		  AND COALESCE(runtime_registry.credential_key_id, '') = COALESCE(EXCLUDED.credential_key_id, '')
		  AND runtime_registry.status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')`,
		rt.RuntimeID, rt.UserID, rt.Name, rt.Source, string(rt.Role), rt.EndpointHost, rt.GRPCPort,
		rt.DebugPort, string(caps), rt.ResourceProfile, rt.Version, rt.Status,
		rt.TokenHash, nullableTime(rt.PairedAt), nullableTime(rt.StartedAt),
		nullableTime(rt.EndedAt), rt.EndedReason, nullableTime(rt.HeartbeatAt),
		rt.CreatedAt, rt.UpdatedAt, rt.CredentialKeyID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrConflict
	}
	return tx.Commit()
}

// CreateOrReplaceSelfHostedRuntime is the RuntimeChannel HELLO admission path.
// The row is bound to the credential owner at HELLO verification time;
// endpoint_host/grpc_port may be empty/zero because handler traffic reaches the
// runtime through RuntimeChannel, not direct dial. It never cancels hosted
// runtimes just because identity names differ.
func (r *TimescaleRepository) CreateOrReplaceSelfHostedRuntime(ctx context.Context, rt domain.Runtime) error {
	if rt.Source != domain.RuntimeSourceSelfHosted {
		return fmt.Errorf("CreateOrReplaceSelfHostedRuntime: source must be self_hosted, got %q", rt.Source)
	}
	if rt.UserID <= 0 {
		return fmt.Errorf("CreateOrReplaceSelfHostedRuntime: user_id must be > 0")
	}
	if strings.TrimSpace(rt.CredentialKeyID) == "" {
		return fmt.Errorf("CreateOrReplaceSelfHostedRuntime: credential_key_id is required")
	}
	rt = normalizeRuntimeForWrite(rt)
	caps, err := marshalCapabilities(rt.Capabilities)
	if err != nil {
		return fmt.Errorf("marshal capabilities: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existingRuntimeID string
	err = tx.QueryRowContext(ctx, `
		SELECT runtime_id
		FROM runtime_registry
		WHERE credential_key_id = $1
		  AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')
		  AND runtime_id <> $2
		ORDER BY updated_at DESC
		LIMIT 1
		FOR UPDATE`,
		rt.CredentialKeyID, rt.RuntimeID,
	).Scan(&existingRuntimeID)
	if err == nil {
		return fmt.Errorf("%w: credential already bound to runtime %s", ErrConflict, existingRuntimeID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check credential runtime binding: %w", err)
	}

	if rt.Role == domain.CredentialRoleDebugger {
		err = tx.QueryRowContext(ctx, `
			SELECT runtime_id
			FROM runtime_registry
			WHERE user_id = $1
			  AND role = 'debugger'
			  AND runtime_id <> $2
			  AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')
			ORDER BY updated_at DESC
			LIMIT 1
			FOR UPDATE`,
			rt.UserID, rt.RuntimeID,
		).Scan(&existingRuntimeID)
		if err == nil {
			return fmt.Errorf("%w: debugger runtime already active for user %d: %s", ErrConflict, rt.UserID, existingRuntimeID)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check active debugger runtime: %w", err)
		}
	}

	err = tx.QueryRowContext(ctx, `
		SELECT runtime_id
		FROM runtime_registry
		WHERE user_id = $1
		  AND name = $2
		  AND runtime_id <> $3
		ORDER BY updated_at DESC
		LIMIT 1
		FOR UPDATE`,
		rt.UserID, rt.Name, rt.RuntimeID,
	).Scan(&existingRuntimeID)
	if err == nil {
		return fmt.Errorf("%w: runtime name already occupied by %s", ErrConflict, existingRuntimeID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check self-hosted runtime name: %w", err)
	}
	if err := consumeRuntimeCredentialForRuntimeTx(ctx, tx, rt, rt.UpdatedAt); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_registry (
			runtime_id, user_id, name, source, role, endpoint_host, grpc_port,
			debug_port, capabilities, resource_profile, version, status,
			token_hash, paired_at, started_at, ended_at, ended_reason,
			heartbeat_at, created_at, updated_at, credential_key_id
		) VALUES (
			$1, NULLIF($2, 0)::BIGINT, $3, $4, $5, $6, $7,
			NULLIF($8, 0)::INT, $9::JSONB, $10, $11, $12,
			$13, $14, $15, $16, $17,
			$18, $19, $20, NULLIF($21, '')
		)
		ON CONFLICT (runtime_id) DO UPDATE SET
			user_id = EXCLUDED.user_id,
			name = EXCLUDED.name,
			source = EXCLUDED.source,
			role = EXCLUDED.role,
			endpoint_host = EXCLUDED.endpoint_host,
			grpc_port = EXCLUDED.grpc_port,
			debug_port = EXCLUDED.debug_port,
			capabilities = EXCLUDED.capabilities,
			resource_profile = EXCLUDED.resource_profile,
			version = EXCLUDED.version,
			status = EXCLUDED.status,
			token_hash = EXCLUDED.token_hash,
			paired_at = EXCLUDED.paired_at,
			started_at = EXCLUDED.started_at,
			ended_at = EXCLUDED.ended_at,
			ended_reason = EXCLUDED.ended_reason,
			heartbeat_at = EXCLUDED.heartbeat_at,
			credential_key_id = EXCLUDED.credential_key_id,
			updated_at = EXCLUDED.updated_at
		WHERE runtime_registry.user_id = EXCLUDED.user_id
		  AND runtime_registry.source = EXCLUDED.source
		  AND COALESCE(runtime_registry.credential_key_id, '') = COALESCE(EXCLUDED.credential_key_id, '')
		  AND runtime_registry.status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')`,
		rt.RuntimeID, rt.UserID, rt.Name, rt.Source, string(rt.Role), rt.EndpointHost, rt.GRPCPort,
		rt.DebugPort, string(caps), rt.ResourceProfile, rt.Version, rt.Status,
		rt.TokenHash, nullableTime(rt.PairedAt), nullableTime(rt.StartedAt),
		nullableTime(rt.EndedAt), rt.EndedReason, nullableTime(rt.HeartbeatAt),
		rt.CreatedAt, rt.UpdatedAt, rt.CredentialKeyID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("check self-hosted runtime upsert result: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: runtime_id already belongs to another runtime credential", ErrConflict)
	}
	return tx.Commit()
}

func consumeRuntimeCredentialForRuntimeTx(ctx context.Context, tx *sql.Tx, rt domain.Runtime, at time.Time) error {
	keyID := strings.TrimSpace(rt.CredentialKeyID)
	if keyID == "" {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC()

	var ownerID int64
	var role, status, consumedRuntimeID string
	var expiresAt sql.NullTime
	var hostedInternal bool
	err := tx.QueryRowContext(ctx, `
		SELECT user_id, role, status, consumed_runtime_id, expires_at, hosted_internal
		FROM runtime_credentials
		WHERE key_id = $1
		FOR UPDATE`,
		keyID,
	).Scan(&ownerID, &role, &status, &consumedRuntimeID, &expiresAt, &hostedInternal)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: runtime credential not found", ErrConflict)
	}
	if err != nil {
		return fmt.Errorf("lock runtime credential: %w", err)
	}
	if ownerID != rt.UserID {
		return fmt.Errorf("%w: runtime credential belongs to another user", ErrConflict)
	}
	if domain.CredentialRole(role) != rt.Role {
		return fmt.Errorf("%w: runtime credential role %q does not match runtime role %q", ErrConflict, role, rt.Role)
	}
	expectedSource := domain.RuntimeSourceSelfHosted
	if hostedInternal {
		expectedSource = domain.RuntimeSourceHosted
	}
	if rt.Source != expectedSource {
		return fmt.Errorf("%w: runtime credential source mismatch; expected %s", ErrConflict, expectedSource)
	}
	if expiresAt.Valid && !expiresAt.Time.After(at) && (status == string(domain.CredentialStatusActive) || status == string(domain.CredentialStatusDownloaded)) {
		if _, err := tx.ExecContext(ctx, `
			UPDATE runtime_credentials
			SET status = 'expired'
			WHERE key_id = $1`,
			keyID,
		); err != nil {
			return fmt.Errorf("expire runtime credential: %w", err)
		}
		return fmt.Errorf("%w: runtime credential expired; stop retrying with this credential", ErrConflict)
	}

	switch domain.CredentialStatus(status) {
	case domain.CredentialStatusActive, domain.CredentialStatusDownloaded:
		_, err := tx.ExecContext(ctx, `
			UPDATE runtime_credentials
			SET status = 'consumed',
			    consumed_at = $2,
			    consumed_runtime_id = $3,
			    last_used_at = $2
			WHERE key_id = $1`,
			keyID, at, rt.RuntimeID,
		)
		if err != nil {
			return fmt.Errorf("consume runtime credential: %w", err)
		}
		return nil
	case domain.CredentialStatusConsumed:
		return fmt.Errorf("%w: runtime credential consumed by runtime %s; stop retrying with this credential", ErrConflict, consumedRuntimeID)
	case domain.CredentialStatusRevoked:
		return fmt.Errorf("%w: runtime credential revoked; stop retrying with this credential", ErrConflict)
	case domain.CredentialStatusExpired:
		return fmt.Errorf("%w: runtime credential expired; stop retrying with this credential", ErrConflict)
	default:
		return fmt.Errorf("%w: runtime credential status %q is not usable", ErrConflict, status)
	}
}

func (r *TimescaleRepository) GetRuntime(ctx context.Context, runtimeID string) (domain.Runtime, error) {
	row := r.db.QueryRowContext(ctx, runtimeSelectColumns+` FROM runtime_registry WHERE runtime_id = $1`, runtimeID)
	rt, err := scanRuntime(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Runtime{}, ErrNotFound
		}
		return domain.Runtime{}, err
	}
	return rt, nil
}

func (r *TimescaleRepository) ListRuntimes(ctx context.Context, userID int64, statusFilter, sourceFilter string, limit, offset int) ([]domain.Runtime, int64, bool, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	args := []any{userID}
	whereClauses := []string{"user_id = $1"}
	if statusFilter != "" {
		args = append(args, statusFilter)
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", len(args)))
	}
	if sourceFilter != "" {
		args = append(args, sourceFilter)
		whereClauses = append(whereClauses, fmt.Sprintf("source = $%d", len(args)))
	}
	where := strings.Join(whereClauses, " AND ")

	// Page query: limit+1 sentinel, separate COUNT for pager total.
	pageArgs := append(append([]any{}, args...), limit+1, offset)
	q := fmt.Sprintf(
		runtimeSelectColumns+` FROM runtime_registry WHERE %s ORDER BY COALESCE(started_at, created_at) DESC, runtime_id DESC LIMIT $%d OFFSET $%d`,
		where, len(args)+1, len(args)+2,
	)
	rows, err := r.db.QueryContext(ctx, q, pageArgs...)
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()

	items := make([]domain.Runtime, 0, limit+1)
	for rows.Next() {
		rt, err := scanRuntime(rows.Scan)
		if err != nil {
			return nil, 0, false, err
		}
		items = append(items, rt)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, err
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	var total int64
	cq := fmt.Sprintf(`SELECT COUNT(*) FROM runtime_registry WHERE %s`, where)
	if err := r.db.QueryRowContext(ctx, cq, args...).Scan(&total); err != nil {
		return nil, 0, false, err
	}
	return items, total, hasMore, nil
}

func (r *TimescaleRepository) UpdateRuntimeStatus(ctx context.Context, runtimeID, status string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE runtime_registry SET status = $2, updated_at = NOW() WHERE runtime_id = $1`,
		runtimeID, status,
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

func (r *TimescaleRepository) MarkStaleRuntimesUnhealthy(ctx context.Context, cutoff time.Time) ([]domain.Runtime, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH updated AS (
			UPDATE runtime_registry
			SET status = 'unhealthy', updated_at = NOW()
			WHERE status IN ('active', 'starting', 'paired')
			  AND COALESCE(heartbeat_at, updated_at, created_at) < $1
			RETURNING runtime_id
		)
		`+runtimeSelectColumns+`
		FROM runtime_registry
		WHERE runtime_id IN (SELECT runtime_id FROM updated)
		ORDER BY updated_at DESC`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Runtime
	for rows.Next() {
		rt, err := scanRuntime(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, rt)
	}
	return result, rows.Err()
}

func (r *TimescaleRepository) UpdateRuntimeHeartbeat(ctx context.Context, runtimeID string, at time.Time) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_registry
		SET heartbeat_at = $2,
		    status = CASE
		        WHEN status IN ('starting','paired','unhealthy') THEN 'active'
		        ELSE status
		    END,
		    started_at = CASE
		        WHEN status IN ('active','starting','paired','unhealthy') THEN COALESCE(started_at, $2)
		        ELSE started_at
		    END,
		    updated_at = NOW()
		WHERE runtime_id = $1
		  AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')`,
		runtimeID, at.UTC(),
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

func (r *TimescaleRepository) RecordRuntimeConnectionOwner(ctx context.Context, runtimeID, instanceID string, at time.Time) error {
	if strings.TrimSpace(runtimeID) == "" || strings.TrimSpace(instanceID) == "" {
		return ErrConflict
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_registry
		SET connection_owner_acquired_at = CASE
		        WHEN connection_owner_instance_id = $2 THEN COALESCE(connection_owner_acquired_at, $3)
		        ELSE $3
		    END,
		    connection_owner_instance_id = $2,
		    connection_owner_heartbeat_at = $3,
		    updated_at = NOW()
		WHERE runtime_id = $1
		  AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')`,
		runtimeID, instanceID, at.UTC(),
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

func (r *TimescaleRepository) ClearRuntimeConnectionOwner(ctx context.Context, runtimeID, instanceID string) error {
	if strings.TrimSpace(runtimeID) == "" || strings.TrimSpace(instanceID) == "" {
		return ErrConflict
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE runtime_registry
		SET connection_owner_instance_id = '',
		    connection_owner_acquired_at = NULL,
		    connection_owner_heartbeat_at = NULL,
		    updated_at = NOW()
		WHERE runtime_id = $1
		  AND connection_owner_instance_id = $2`,
		runtimeID, instanceID,
	)
	return err
}

func (r *TimescaleRepository) CountRuntimesByUser(ctx context.Context, userID int64) (domain.RuntimeUsageCounts, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
		    COUNT(*) FILTER (WHERE source = 'hosted')      AS hosted,
		    COUNT(*) FILTER (WHERE source = 'self_hosted') AS self_hosted
		FROM runtime_registry
		WHERE user_id = $1
		  AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')`,
		userID,
	)
	var counts domain.RuntimeUsageCounts
	if err := row.Scan(&counts.Hosted, &counts.SelfHosted); err != nil {
		return domain.RuntimeUsageCounts{}, err
	}
	return counts, nil
}

func (r *TimescaleRepository) EndRuntime(ctx context.Context, runtimeID, reason string, endedAt time.Time) (domain.Runtime, error) {
	terminalStatus := domain.RuntimeTerminalStatusForReason(reason)
	row := r.db.QueryRowContext(ctx, `
		WITH updated AS (
			UPDATE runtime_registry
			SET status = $4,
			    ended_at = $2,
			    ended_reason = $3,
			    updated_at = NOW()
			WHERE runtime_id = $1
			  AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')
			RETURNING runtime_id
		)
		`+runtimeSelectColumns+`
		FROM runtime_registry
		WHERE runtime_id IN (SELECT runtime_id FROM updated)`,
		runtimeID, endedAt.UTC(), reason, terminalStatus,
	)
	rt, err := scanRuntime(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			existing, getErr := r.GetRuntime(ctx, runtimeID)
			if getErr != nil {
				return domain.Runtime{}, getErr
			}
			if domain.IsRuntimeTerminalStatus(existing.Status) {
				return existing, nil
			}
			return domain.Runtime{}, ErrConflict
		}
		return domain.Runtime{}, err
	}
	return rt, nil
}

func (r *TimescaleRepository) EndRuntimesByCredentialKey(ctx context.Context, keyID, reason string, endedAt time.Time) (int64, error) {
	terminalStatus := domain.RuntimeTerminalStatusForReason(reason)
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_registry
		SET status = $4,
		    ended_at = $2,
		    ended_reason = $3,
		    updated_at = NOW()
		WHERE credential_key_id = $1
		  AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale')`,
		keyID, endedAt.UTC(), reason, terminalStatus,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (r *TimescaleRepository) EndDeadRuntimes(ctx context.Context, cutoff time.Time, reason string, endedAt time.Time) ([]domain.Runtime, error) {
	terminalStatus := domain.RuntimeTerminalStatusForReason(reason)
	rows, err := r.db.QueryContext(ctx, `
		WITH updated AS (
			UPDATE runtime_registry
			SET status = $4,
			    ended_at = $2,
			    ended_reason = $3,
			    updated_at = NOW()
				WHERE status = 'unhealthy'
				  AND COALESCE(heartbeat_at, updated_at, created_at) < $1
			RETURNING runtime_id
		)
		`+runtimeSelectColumns+`
		FROM runtime_registry
		WHERE runtime_id IN (SELECT runtime_id FROM updated)
		ORDER BY updated_at DESC`, cutoff, endedAt.UTC(), reason, terminalStatus)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Runtime
	for rows.Next() {
		rt, err := scanRuntime(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, rt)
	}
	return result, rows.Err()
}

func (r *TimescaleRepository) UpdateRuntimeCleanupState(ctx context.Context, runtimeID, status, reason string, at time.Time) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_registry
		SET cleanup_status = $2,
		    cleanup_reason = $3,
		    cleanup_at = $4,
		    updated_at = NOW()
		WHERE runtime_id = $1`,
		runtimeID, status, reason, at.UTC(),
	)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) SaveDebugWorkspaceState(ctx context.Context, runtimeID string, state domain.DebugWorkspaceState) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_registry
		SET debug_workspace_host_path = $2,
		    debug_workspace_container_path = $3,
		    debug_template_path = $4,
		    debug_archived_template_path = $5,
		    debug_vscode_launch_created = $6,
		    debug_vscode_launch_preserved = $7,
		    debug_pycharm_doc_created = $8,
		    debug_pycharm_doc_preserved = $9,
		    debug_workspace_prepared_at = $10,
		    debug_workspace_last_error = $11,
		    updated_at = NOW()
		WHERE runtime_id = $1`,
		runtimeID,
		state.HostPath,
		nonEmpty(state.ContainerPath, "/workspace"),
		state.TemplatePath,
		state.ArchivedTemplatePath,
		state.VSCodeLaunchCreated,
		state.VSCodeLaunchPreserved,
		state.PyCharmDocCreated,
		state.PyCharmDocPreserved,
		nullableTime(state.PreparedAt),
		state.LastError,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) MarkDebugWorkspaceError(ctx context.Context, runtimeID string, errText string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE runtime_registry
		SET debug_workspace_last_error = $2,
		    updated_at = NOW()
		WHERE runtime_id = $1`,
		runtimeID,
		errText,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) ReplaceActiveDebugDataset(ctx context.Context, state domain.DebugDatasetState) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `
		UPDATE runtime_debug_datasets
		SET state = 'lost',
		    last_error = CASE WHEN last_error = '' THEN 'replaced by newer debug dataset' ELSE last_error END,
		    updated_at = NOW()
		WHERE runtime_id = $1
		  AND state = 'active'`,
		state.RuntimeID,
	)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO runtime_debug_datasets (
			dataset_id, user_id, account_id, runtime_id, market, symbol, interval,
			start_at, end_at, bar_count, coverage_status, state, last_error, loaded_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13, $14, $14
		)`,
		state.DatasetID,
		state.UserID,
		state.AccountID,
		state.RuntimeID,
		state.Market,
		state.Symbol,
		state.Interval,
		state.StartAt,
		state.EndAt,
		state.BarCount,
		state.CoverageStatus,
		state.State,
		state.LastError,
		state.LoadedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	return tx.Commit()
}

func (r *TimescaleRepository) GetLatestDebugDataset(ctx context.Context, userID int64, runtimeID string) (domain.DebugDatasetState, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT dataset_id, user_id, account_id, runtime_id, market, symbol, interval,
		       start_at, end_at, bar_count, coverage_status, loaded_at, state, last_error
		FROM runtime_debug_datasets
		WHERE user_id = $1
		  AND runtime_id = $2
		ORDER BY loaded_at DESC
		LIMIT 1`,
		userID,
		runtimeID,
	)
	return scanDebugDataset(row.Scan)
}

// ── helpers ─────────────────────────────────────────────────────────────────

const runtimeSelectColumns = `
SELECT runtime_id, COALESCE(user_id, 0), name, source, role, endpoint_host,
       grpc_port, COALESCE(debug_port, 0), capabilities, resource_profile,
       version, status, token_hash, paired_at, started_at, ended_at,
       ended_reason, cleanup_status, cleanup_reason, cleanup_at,
       heartbeat_at, created_at, updated_at,
       COALESCE(credential_key_id, ''), connection_owner_instance_id,
       connection_owner_acquired_at, connection_owner_heartbeat_at,
       debug_workspace_host_path, debug_workspace_container_path,
       debug_template_path, debug_archived_template_path,
       debug_vscode_launch_created, debug_vscode_launch_preserved,
       debug_pycharm_doc_created, debug_pycharm_doc_preserved,
       debug_workspace_prepared_at, debug_workspace_last_error`

func scanRuntime(scan func(...any) error) (domain.Runtime, error) {
	var rt domain.Runtime
	var role string
	var capsBytes []byte
	var paired, started, ended, cleanup, heart, ownerAcquired, ownerHeartbeat, debugPrepared sql.NullTime
	if err := scan(
		&rt.RuntimeID, &rt.UserID, &rt.Name, &rt.Source, &role, &rt.EndpointHost,
		&rt.GRPCPort, &rt.DebugPort, &capsBytes, &rt.ResourceProfile,
		&rt.Version, &rt.Status, &rt.TokenHash, &paired, &started, &ended,
		&rt.EndedReason, &rt.CleanupStatus, &rt.CleanupReason, &cleanup,
		&heart, &rt.CreatedAt, &rt.UpdatedAt, &rt.CredentialKeyID,
		&rt.ConnectionOwnerInstanceID, &ownerAcquired, &ownerHeartbeat,
		&rt.DebugWorkspace.HostPath, &rt.DebugWorkspace.ContainerPath,
		&rt.DebugWorkspace.TemplatePath, &rt.DebugWorkspace.ArchivedTemplatePath,
		&rt.DebugWorkspace.VSCodeLaunchCreated, &rt.DebugWorkspace.VSCodeLaunchPreserved,
		&rt.DebugWorkspace.PyCharmDocCreated, &rt.DebugWorkspace.PyCharmDocPreserved,
		&debugPrepared, &rt.DebugWorkspace.LastError,
	); err != nil {
		return domain.Runtime{}, err
	}
	rt.Role = domain.CredentialRole(role)
	if len(capsBytes) > 0 {
		if err := json.Unmarshal(capsBytes, &rt.Capabilities); err != nil {
			return domain.Runtime{}, fmt.Errorf("decode capabilities: %w", err)
		}
	}
	if paired.Valid {
		t := paired.Time
		rt.PairedAt = &t
	}
	if started.Valid {
		t := started.Time
		rt.StartedAt = &t
	}
	if ended.Valid {
		t := ended.Time
		rt.EndedAt = &t
	}
	if cleanup.Valid {
		t := cleanup.Time
		rt.CleanupAt = &t
	}
	if heart.Valid {
		t := heart.Time
		rt.HeartbeatAt = &t
	}
	if ownerAcquired.Valid {
		t := ownerAcquired.Time
		rt.ConnectionOwnerAcquiredAt = &t
	}
	if ownerHeartbeat.Valid {
		t := ownerHeartbeat.Time
		rt.ConnectionOwnerHeartbeatAt = &t
	}
	if debugPrepared.Valid {
		t := debugPrepared.Time
		rt.DebugWorkspace.PreparedAt = &t
	}
	return rt, nil
}

func normalizeRuntimeForWrite(rt domain.Runtime) domain.Runtime {
	if rt.Role == "" {
		rt.Role = domain.CredentialRoleExecutor
	}
	if rt.Status != domain.RuntimeStatusActive || rt.StartedAt != nil {
		return rt
	}
	started := firstRuntimeTime(rt.HeartbeatAt, rt.PairedAt)
	if started == nil && !rt.UpdatedAt.IsZero() {
		t := rt.UpdatedAt
		started = &t
	}
	if started == nil && !rt.CreatedAt.IsZero() {
		t := rt.CreatedAt
		started = &t
	}
	if started == nil {
		t := time.Now().UTC()
		started = &t
	}
	t := started.UTC()
	rt.StartedAt = &t
	return rt
}

func firstRuntimeTime(candidates ...*time.Time) *time.Time {
	for _, candidate := range candidates {
		if candidate == nil || candidate.IsZero() {
			continue
		}
		t := *candidate
		return &t
	}
	return nil
}

func marshalCapabilities(caps []string) ([]byte, error) {
	if len(caps) == 0 {
		return []byte("[]"), nil
	}
	return json.Marshal(caps)
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

func nonEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func scanDebugDataset(scan func(...any) error) (domain.DebugDatasetState, error) {
	var state domain.DebugDatasetState
	if err := scan(
		&state.DatasetID,
		&state.UserID,
		&state.AccountID,
		&state.RuntimeID,
		&state.Market,
		&state.Symbol,
		&state.Interval,
		&state.StartAt,
		&state.EndAt,
		&state.BarCount,
		&state.CoverageStatus,
		&state.LoadedAt,
		&state.State,
		&state.LastError,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.DebugDatasetState{}, ErrNotFound
		}
		return domain.DebugDatasetState{}, err
	}
	return state, nil
}

// isUniqueViolation detects PostgreSQL unique_violation (23505) without
// pulling in pq's typed error directly — keeps the dep surface small.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "duplicate key value") ||
		strings.Contains(msg, "unique constraint")
}

// ── Runtime credentials (Phase D3) ──────────────────────────────────────────

func (r *TimescaleRepository) CreateRuntimeCredential(ctx context.Context, c domain.RuntimeCredential) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO runtime_credentials (
			key_id, user_id, public_key_pem, label, role, status, created_at, downloaded_at, hosted_internal
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		c.KeyID, c.UserID, c.PublicKeyPEM, c.Label, string(c.Role), string(c.Status), c.CreatedAt.UTC(), nullableTime(c.DownloadedAt), c.HostedInternal,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	return nil
}

func (r *TimescaleRepository) GetRuntimeCredential(ctx context.Context, keyID string) (domain.RuntimeCredential, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT key_id, user_id, public_key_pem, label, role, status,
		       created_at, downloaded_at, consumed_at, consumed_runtime_id,
		       expires_at, last_used_at, revoked_at, hosted_internal
		FROM runtime_credentials
		WHERE key_id = $1`,
		keyID,
	)
	c, err := scanCredential(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RuntimeCredential{}, ErrNotFound
	}
	return c, err
}

func (r *TimescaleRepository) ListRuntimeCredentialsByUser(ctx context.Context, userID int64, includeInactive bool) ([]domain.RuntimeCredential, error) {
	q := `SELECT key_id, user_id, public_key_pem, label, role, status,
	             created_at, downloaded_at, consumed_at, consumed_runtime_id,
	             expires_at, last_used_at, revoked_at, hosted_internal
	      FROM runtime_credentials
	      WHERE user_id = $1
	        AND hosted_internal = FALSE`
	if !includeInactive {
		q += ` AND status IN ('active', 'downloaded', 'consumed')`
	}
	q += ` ORDER BY created_at DESC`
	rows, err := r.db.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.RuntimeCredential
	for rows.Next() {
		c, err := scanCredential(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) ListRuntimeCredentialsByUserPage(ctx context.Context, userID int64, includeInactive bool, limit, offset int) ([]domain.RuntimeCredential, int64, bool, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	where := ` WHERE user_id = $1 AND hosted_internal = FALSE`
	args := []any{userID}
	if !includeInactive {
		where += ` AND status IN ('active', 'downloaded', 'consumed')`
	}
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_credentials`+where, args...).Scan(&total); err != nil {
		return nil, 0, false, err
	}
	listArgs := append([]any{}, args...)
	listArgs = append(listArgs, limit+1, offset)
	q := `SELECT key_id, user_id, public_key_pem, label, role, status,
	             created_at, downloaded_at, consumed_at, consumed_runtime_id,
	             expires_at, last_used_at, revoked_at, hosted_internal
	      FROM runtime_credentials` + where +
		fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, len(listArgs)-1, len(listArgs))
	rows, err := r.db.QueryContext(ctx, q, listArgs...)
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()
	var out []domain.RuntimeCredential
	for rows.Next() {
		c, err := scanCredential(rows)
		if err != nil {
			return nil, 0, false, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, err
	}
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	return out, total, hasMore, nil
}

func (r *TimescaleRepository) RevokeRuntimeCredential(ctx context.Context, keyID string, userID int64) (domain.RuntimeCredential, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RuntimeCredential{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Lock the row + cross-check ownership + check current status.
	var ownerID int64
	var status string
	err = tx.QueryRowContext(ctx, `
		SELECT user_id, status FROM runtime_credentials
		WHERE key_id = $1 FOR UPDATE`,
		keyID,
	).Scan(&ownerID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RuntimeCredential{}, ErrNotFound
	}
	if err != nil {
		return domain.RuntimeCredential{}, err
	}
	if ownerID != userID {
		// Caller maps to PermissionDenied. Use ErrConflict because that's
		// what the rest of this layer uses for "you can't do this even
		// though the row exists".
		return domain.RuntimeCredential{}, ErrConflict
	}

	// Idempotent: if already revoked, return the existing row unchanged.
	if status != string(domain.CredentialStatusRevoked) {
		_, err = tx.ExecContext(ctx, `
			UPDATE runtime_credentials
			SET status = 'revoked', revoked_at = NOW()
			WHERE key_id = $1`,
			keyID,
		)
		if err != nil {
			return domain.RuntimeCredential{}, err
		}
	}

	// Re-read and return.
	row := tx.QueryRowContext(ctx, `
		SELECT key_id, user_id, public_key_pem, label, role, status,
		       created_at, downloaded_at, consumed_at, consumed_runtime_id,
		       expires_at, last_used_at, revoked_at, hosted_internal
		FROM runtime_credentials WHERE key_id = $1`,
		keyID,
	)
	c, err := scanCredential(row)
	if err != nil {
		return domain.RuntimeCredential{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.RuntimeCredential{}, err
	}
	return c, nil
}

func (r *TimescaleRepository) TouchRuntimeCredentialUsed(ctx context.Context, keyID string, at time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE runtime_credentials SET last_used_at = $2 WHERE key_id = $1`,
		keyID, at.UTC(),
	)
	return err
}

type credentialRowScanner interface {
	Scan(dest ...any) error
}

func scanCredential(row credentialRowScanner) (domain.RuntimeCredential, error) {
	var c domain.RuntimeCredential
	var status string
	var role string
	var downloadedAt, consumedAt, expiresAt, lastUsed, revokedAt sql.NullTime
	if err := row.Scan(
		&c.KeyID, &c.UserID, &c.PublicKeyPEM, &c.Label, &role, &status,
		&c.CreatedAt, &downloadedAt, &consumedAt, &c.ConsumedRuntimeID,
		&expiresAt, &lastUsed, &revokedAt, &c.HostedInternal,
	); err != nil {
		return domain.RuntimeCredential{}, err
	}
	c.Role = domain.CredentialRole(role)
	c.Status = domain.CredentialStatus(status)
	if downloadedAt.Valid {
		t := downloadedAt.Time
		c.DownloadedAt = &t
	}
	if consumedAt.Valid {
		t := consumedAt.Time
		c.ConsumedAt = &t
	}
	if expiresAt.Valid {
		t := expiresAt.Time
		c.ExpiresAt = &t
	}
	if lastUsed.Valid {
		t := lastUsed.Time
		c.LastUsedAt = &t
	}
	if revokedAt.Valid {
		t := revokedAt.Time
		c.RevokedAt = &t
	}
	return c, nil
}
