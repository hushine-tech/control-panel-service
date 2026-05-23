package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/hushine-tech/control-panel-service/internal/domain"
)

// TimescaleRepository implements Repository against the control_panel
// database. Phase D2: SQL queries were ported verbatim from
// account-service (sources `market_data_control_plane.go` and
// `market_data_history.go` were deleted in the same change); behaviour
// is unchanged. Schema lives in migrations 0003-0006.
//
// The connection pool is SHARED with the runtime control-plane
// repository (see internal/repository/timescale.go.DB() accessor).
// Both packages talk to the same `control_panel` database.
type TimescaleRepository struct {
	db *sql.DB
}

// NewTimescaleRepository wraps a pre-opened *sql.DB. main.go opens the
// pool once via the runtime repository's NewTimescaleRepository(dsn)
// and passes its DB() to this constructor so we don't double-pool.
func NewTimescaleRepository(db *sql.DB) *TimescaleRepository {
	return &TimescaleRepository{db: db}
}

// ── market_data_streams ─────────────────────────────────────────────────

func (r *TimescaleRepository) UpsertMarketDataStream(ctx context.Context, key domain.StreamKey) (domain.MarketDataStream, error) {
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO market_data_streams
			(exchange, market, kind, symbol, interval)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (exchange, market, kind, symbol, interval) DO UPDATE
			SET updated_at = NOW()
		RETURNING stream_id, exchange, market, kind, symbol, interval,
			desired_state, actual_state, effective_live_delivery,
			last_data_at, COALESCE(last_error, ''), last_reconciled_at,
			created_at, updated_at`,
		key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
	)
	return scanStream(row)
}

func recomputeMarketDataStreamAggregate(ctx context.Context, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, streamID int64) error {
	res, err := exec.ExecContext(ctx, `
		UPDATE market_data_streams
		SET desired_state = CASE
				WHEN EXISTS (
					SELECT 1 FROM market_data_requests
					WHERE stream_id = $1 AND status != 'cancelled'
				) THEN 'running'
				ELSE 'stopped'
			END,
			effective_live_delivery = CASE
				WHEN EXISTS (
					SELECT 1 FROM market_data_requests
					WHERE stream_id = $1 AND status != 'cancelled' AND needs_live_delivery = TRUE
				) OR EXISTS (
					SELECT 1 FROM market_data_leases
					WHERE stream_id = $1
					  AND released_at IS NULL
					  AND expires_at > NOW()
				) THEN TRUE
				ELSE FALSE
			END,
			updated_at = NOW()
		WHERE stream_id = $1`,
		streamID,
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

func (r *TimescaleRepository) GetMarketDataStream(ctx context.Context, streamID int64) (domain.MarketDataStream, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT stream_id, exchange, market, kind, symbol, interval,
			desired_state, actual_state, effective_live_delivery,
			last_data_at, COALESCE(last_error, ''), last_reconciled_at,
			created_at, updated_at
		FROM market_data_streams
		WHERE stream_id = $1`,
		streamID,
	)
	s, err := scanStream(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataStream{}, ErrNotFound
	}
	return s, err
}

func (r *TimescaleRepository) GetMarketDataStreamByKey(ctx context.Context, key domain.StreamKey) (domain.MarketDataStream, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT stream_id, exchange, market, kind, symbol, interval,
			desired_state, actual_state, effective_live_delivery,
			last_data_at, COALESCE(last_error, ''), last_reconciled_at,
			created_at, updated_at
		FROM market_data_streams
		WHERE exchange = $1 AND market = $2 AND kind = $3
		  AND symbol = $4 AND interval = $5`,
		key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
	)
	s, err := scanStream(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataStream{}, ErrNotFound
	}
	return s, err
}

func (r *TimescaleRepository) ListMarketDataStreams(ctx context.Context) ([]domain.MarketDataStream, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT stream_id, exchange, market, kind, symbol, interval,
			desired_state, actual_state, effective_live_delivery,
			last_data_at, COALESCE(last_error, ''), last_reconciled_at,
			created_at, updated_at
		FROM market_data_streams
		ORDER BY stream_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MarketDataStream
	for rows.Next() {
		s, err := scanStream(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) UpdateMarketDataStreamActualState(
	ctx context.Context,
	streamID int64,
	state domain.StreamActualState,
	lastData *time.Time,
	lastErr string,
) error {
	q := `UPDATE market_data_streams SET
			actual_state = $2,
			last_reconciled_at = NOW(),
			updated_at = NOW()`
	args := []any{streamID, string(state)}
	if lastData != nil {
		q += ", last_data_at = $" + itoa(len(args)+1)
		args = append(args, *lastData)
	}
	if lastErr != "" {
		q += ", last_error = $" + itoa(len(args)+1)
		args = append(args, lastErr)
	}
	q += " WHERE stream_id = $1"

	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ── market_data_requests ────────────────────────────────────────────────

func (r *TimescaleRepository) UpsertMarketDataRequest(
	ctx context.Context,
	userID int64,
	accountID *int64,
	key domain.StreamKey,
	needsLive bool,
) (domain.MarketDataRequest, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.MarketDataRequest{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	var streamID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO market_data_streams
			(exchange, market, kind, symbol, interval)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (exchange, market, kind, symbol, interval) DO UPDATE
			SET updated_at = NOW()
		RETURNING stream_id`,
		key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
	).Scan(&streamID); err != nil {
		return domain.MarketDataRequest{}, fmt.Errorf("upsert stream: %w", err)
	}

	var existingID int64
	err = tx.QueryRowContext(ctx, `
		SELECT request_id FROM market_data_requests
		WHERE user_id = $1 AND exchange = $2 AND market = $3
		  AND kind = $4 AND symbol = $5 AND interval = $6
		  AND status != 'cancelled'
		LIMIT 1`,
		userID, key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
	).Scan(&existingID)

	if err == nil {
		if _, err := tx.ExecContext(ctx, `
			UPDATE market_data_requests
			SET needs_live_delivery = $2,
			    account_id = COALESCE($3, account_id),
			    status = 'active',
			    updated_at = NOW()
			WHERE request_id = $1`,
			existingID, needsLive, accountID,
		); err != nil {
			return domain.MarketDataRequest{}, fmt.Errorf("update request: %w", err)
		}
		if err := recomputeMarketDataStreamAggregate(ctx, tx, streamID); err != nil {
			return domain.MarketDataRequest{}, fmt.Errorf("recompute stream aggregate: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.MarketDataRequest{}, err
		}
		return r.GetMarketDataRequest(ctx, existingID, userID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataRequest{}, fmt.Errorf("lookup existing request: %w", err)
	}

	var newID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO market_data_requests
			(user_id, account_id, exchange, market, kind, symbol, interval,
			 needs_live_delivery, status, stream_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9)
		RETURNING request_id`,
		userID, accountID,
		key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
		needsLive, streamID,
	).Scan(&newID); err != nil {
		return domain.MarketDataRequest{}, fmt.Errorf("insert request: %w", err)
	}
	if err := recomputeMarketDataStreamAggregate(ctx, tx, streamID); err != nil {
		return domain.MarketDataRequest{}, fmt.Errorf("recompute stream aggregate: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.MarketDataRequest{}, err
	}
	return r.GetMarketDataRequest(ctx, newID, userID)
}

func (r *TimescaleRepository) CancelMarketDataRequest(ctx context.Context, requestID, userID int64) error {
	var ownerID int64
	var streamID int64
	err := r.db.QueryRowContext(ctx, `
		SELECT user_id, stream_id FROM market_data_requests WHERE request_id = $1`,
		requestID,
	).Scan(&ownerID, &streamID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if ownerID != userID {
		return ErrPermissionDenied
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	_, err = tx.ExecContext(ctx, `
		UPDATE market_data_requests
		SET status = 'cancelled', cancelled_at = NOW(), updated_at = NOW()
		WHERE request_id = $1 AND status != 'cancelled'`,
		requestID,
	)
	if err != nil {
		return err
	}
	if err := recomputeMarketDataStreamAggregate(ctx, tx, streamID); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *TimescaleRepository) GetMarketDataRequest(ctx context.Context, requestID, userID int64) (domain.MarketDataRequest, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT request_id, user_id, account_id, stream_id,
			exchange, market, kind, symbol, interval,
			needs_live_delivery, status, created_at, updated_at, cancelled_at
		FROM market_data_requests
		WHERE request_id = $1`,
		requestID,
	)
	req, err := scanRequest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataRequest{}, ErrNotFound
	}
	if err != nil {
		return domain.MarketDataRequest{}, err
	}
	if req.UserID != userID {
		return domain.MarketDataRequest{}, ErrPermissionDenied
	}
	return req, nil
}

func (r *TimescaleRepository) ListMarketDataRequestsByUser(ctx context.Context, userID int64) ([]domain.MarketDataRequest, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT request_id, user_id, account_id, stream_id,
			exchange, market, kind, symbol, interval,
			needs_live_delivery, status, created_at, updated_at, cancelled_at
		FROM market_data_requests
		WHERE user_id = $1
		  AND status != 'cancelled'
		ORDER BY request_id DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MarketDataRequest
	for rows.Next() {
		req, err := scanRequest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, req)
	}
	return out, rows.Err()
}

// ── market_data_leases ──────────────────────────────────────────────────

func (r *TimescaleRepository) CreateOrRenewLease(
	ctx context.Context,
	sessionID string,
	strategyID, accountID *int64,
	streamID int64,
	ttl time.Duration,
) (domain.MarketDataLease, error) {
	expiresAt := time.Now().Add(ttl).UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.MarketDataLease{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	row := tx.QueryRowContext(ctx, `
		INSERT INTO market_data_leases
			(session_id, strategy_id, account_id, stream_id,
			 expires_at, last_heartbeat_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (session_id, stream_id) DO UPDATE SET
			expires_at = EXCLUDED.expires_at,
			last_heartbeat_at = NOW(),
			released_at = NULL,
			strategy_id = COALESCE(EXCLUDED.strategy_id, market_data_leases.strategy_id),
			account_id = COALESCE(EXCLUDED.account_id, market_data_leases.account_id)
		RETURNING lease_id, session_id, strategy_id, account_id,
			stream_id, expires_at, last_heartbeat_at, created_at, released_at`,
		sessionID, strategyID, accountID, streamID, expiresAt,
	)
	lease, err := scanLease(row)
	if err != nil {
		return domain.MarketDataLease{}, err
	}
	if err := recomputeMarketDataStreamAggregate(ctx, tx, streamID); err != nil {
		return domain.MarketDataLease{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.MarketDataLease{}, err
	}
	return lease, nil
}

func (r *TimescaleRepository) ReleaseLease(ctx context.Context, sessionID string, streamID int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	_, err = tx.ExecContext(ctx, `
		UPDATE market_data_leases
		SET released_at = NOW()
		WHERE session_id = $1 AND stream_id = $2 AND released_at IS NULL`,
		sessionID, streamID,
	)
	if err != nil {
		return err
	}
	if err := recomputeMarketDataStreamAggregate(ctx, tx, streamID); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *TimescaleRepository) CountActiveLeasesForStream(ctx context.Context, streamID int64) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM market_data_leases
		WHERE stream_id = $1
		  AND released_at IS NULL
		  AND expires_at > NOW()`,
		streamID,
	).Scan(&n)
	return n, err
}

func (r *TimescaleRepository) CountActiveLeasesByStream(ctx context.Context) (map[int64]int, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT stream_id, COUNT(*)
		FROM market_data_leases
		WHERE released_at IS NULL
		  AND expires_at > NOW()
		GROUP BY stream_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64]int)
	for rows.Next() {
		var streamID int64
		var count int
		if err := rows.Scan(&streamID, &count); err != nil {
			return nil, err
		}
		out[streamID] = count
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) ExpireStaleLeases(ctx context.Context, now time.Time) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.QueryContext(ctx, `
		UPDATE market_data_leases
		SET released_at = $1
		WHERE released_at IS NULL AND expires_at <= $1
		RETURNING stream_id`,
		now.UTC(),
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	streamIDs := map[int64]struct{}{}
	n := 0
	for rows.Next() {
		var streamID int64
		if err := rows.Scan(&streamID); err != nil {
			return 0, err
		}
		n++
		streamIDs[streamID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for streamID := range streamIDs {
		if err := recomputeMarketDataStreamAggregate(ctx, tx, streamID); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

func (r *TimescaleRepository) ListActiveLeasesForStream(ctx context.Context, streamID int64) ([]domain.MarketDataLease, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT lease_id, session_id, strategy_id, account_id,
			stream_id, expires_at, last_heartbeat_at, created_at, released_at
		FROM market_data_leases
		WHERE stream_id = $1
		  AND released_at IS NULL
		  AND expires_at > NOW()
		ORDER BY expires_at DESC`,
		streamID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MarketDataLease
	for rows.Next() {
		l, err := scanLease(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ── RuntimeChannel delivery subscriptions ───────────────────────────────

func (r *TimescaleRepository) UpsertSessionMarketDataSubscriptions(
	ctx context.Context,
	userID int64,
	sessionID string,
	runtimeID string,
	mode int32,
	keys []domain.StreamKey,
) ([]domain.SessionMarketDataSubscription, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	out := make([]domain.SessionMarketDataSubscription, 0, len(keys))
	for _, key := range keys {
		row := tx.QueryRowContext(ctx, `
			INSERT INTO session_market_data_subscriptions
				(user_id, session_id, runtime_id, exchange, market, kind, symbol, interval, mode, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'active')
			ON CONFLICT (session_id, exchange, market, kind, symbol, interval, mode)
				WHERE status = 'active'
			DO UPDATE SET
				user_id = EXCLUDED.user_id,
				runtime_id = EXCLUDED.runtime_id,
				updated_at = NOW()
			RETURNING subscription_id, user_id, session_id, runtime_id,
				exchange, market, kind, symbol, interval, mode, status,
				created_at, updated_at, released_at`,
			userID,
			sessionID,
			runtimeID,
			key.Exchange,
			key.Market,
			key.Kind,
			key.Symbol,
			key.Interval,
			mode,
		)
		sub, err := scanSessionSubscription(row)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *TimescaleRepository) ReleaseSessionMarketDataSubscriptions(ctx context.Context, sessionID, runtimeID string) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		UPDATE session_market_data_subscriptions
		SET status = 'released',
		    released_at = NOW(),
		    updated_at = NOW()
		WHERE session_id = $1
		  AND ($2 = '' OR runtime_id = $2)
		  AND status = 'active'`,
		sessionID,
		runtimeID,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (r *TimescaleRepository) ListActiveSessionMarketDataSubscriptions(ctx context.Context) ([]domain.SessionMarketDataSubscription, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT s.subscription_id, s.user_id, s.session_id, s.runtime_id,
			s.exchange, s.market, s.kind, s.symbol, s.interval, s.mode, s.status,
			s.created_at, s.updated_at, s.released_at
		FROM session_market_data_subscriptions s
		JOIN runtime_registry r ON r.runtime_id = s.runtime_id
			AND r.user_id = s.user_id
		WHERE s.status = 'active'
		  AND r.status NOT IN ('heartbeat_stale', 'ended', 'cancelled', 'failed')
		ORDER BY s.subscription_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.SessionMarketDataSubscription
	for rows.Next() {
		sub, err := scanSessionSubscription(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) CreateOrRenewStreamDeliveryLease(
	ctx context.Context,
	subscriptionID int64,
	ownerInstanceID string,
	ttl time.Duration,
) (domain.StreamDeliveryLease, error) {
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	leaseID := "sdl-" + uuid.NewString()
	expiresAt := time.Now().Add(ttl).UTC()
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO stream_delivery_leases
			(lease_id, subscription_id, owner_instance_id, status,
			 acquired_at, last_heartbeat_at, expires_at)
		VALUES ($1, $2, $3, 'active', NOW(), NOW(), $4)
		ON CONFLICT (subscription_id) WHERE status = 'active'
		DO UPDATE SET
			owner_instance_id = EXCLUDED.owner_instance_id,
			acquired_at = CASE
				WHEN stream_delivery_leases.owner_instance_id = EXCLUDED.owner_instance_id
					THEN stream_delivery_leases.acquired_at
				ELSE NOW()
			END,
			last_heartbeat_at = NOW(),
			expires_at = EXCLUDED.expires_at,
			updated_at = NOW()
		WHERE stream_delivery_leases.owner_instance_id = EXCLUDED.owner_instance_id
		   OR stream_delivery_leases.expires_at <= NOW()
		RETURNING lease_id, subscription_id, owner_instance_id, status,
			acquired_at, last_heartbeat_at, expires_at,
			last_delivery_at, last_topic, last_partition, last_offset,
			released_at, created_at, updated_at`,
		leaseID,
		subscriptionID,
		ownerInstanceID,
		expiresAt,
	)
	lease, err := scanStreamDeliveryLease(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.StreamDeliveryLease{}, ErrPermissionDenied
	}
	return lease, err
}

func (r *TimescaleRepository) RecordStreamDeliveryProgress(
	ctx context.Context,
	subscriptionID int64,
	ownerInstanceID string,
	topic string,
	partition int32,
	offset int64,
	at time.Time,
) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE stream_delivery_leases
		SET last_delivery_at = $3,
		    last_topic = $4,
		    last_partition = $5,
		    last_offset = $6,
		    last_heartbeat_at = $3,
		    updated_at = NOW()
		WHERE subscription_id = $1
		  AND owner_instance_id = $2
		  AND status = 'active'`,
		subscriptionID, ownerInstanceID, at.UTC(), topic, partition, offset,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) RecordStreamDeliveryFailure(ctx context.Context, failure domain.StreamDeliveryFailure) error {
	if failure.Reason == "" {
		failure.Reason = "stream delivery failed"
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
		INSERT INTO stream_delivery_failures (
			subscription_id, owner_instance_id, topic, stream_key,
			failure_code, reason, first_seen_at, last_seen_at, attempt_count
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9
		)
		ON CONFLICT (
			subscription_id, owner_instance_id, topic, stream_key, failure_code
		) DO UPDATE SET
			reason = EXCLUDED.reason,
			last_seen_at = EXCLUDED.last_seen_at,
			attempt_count = stream_delivery_failures.attempt_count + 1`,
		failure.SubscriptionID, failure.OwnerInstanceID, failure.Topic, failure.StreamKey,
		failure.FailureCode, failure.Reason, failure.FirstSeenAt.UTC(), failure.LastSeenAt.UTC(), failure.AttemptCount,
	)
	return err
}

func (r *TimescaleRepository) ListSessionDeliveryHealth(ctx context.Context, userID int64, sessionID, runtimeID string) ([]domain.SessionDeliveryHealth, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			sub.subscription_id, sub.user_id, sub.session_id, sub.runtime_id,
			sub.exchange, sub.market, sub.kind, sub.symbol, sub.interval, sub.mode, sub.status,
			sub.created_at, sub.updated_at, sub.released_at,
			l.lease_id, l.subscription_id, l.owner_instance_id, l.status,
			l.acquired_at, l.last_heartbeat_at, l.expires_at,
			l.last_delivery_at, l.last_topic, l.last_partition, l.last_offset,
			l.released_at, l.created_at, l.updated_at,
			f.failure_id, f.subscription_id, f.owner_instance_id, f.topic, f.stream_key,
			f.failure_code, f.reason, f.first_seen_at, f.last_seen_at, f.attempt_count
		FROM session_market_data_subscriptions sub
		LEFT JOIN LATERAL (
			SELECT *
			FROM stream_delivery_leases
			WHERE subscription_id = sub.subscription_id
			  AND status = 'active'
			ORDER BY updated_at DESC
			LIMIT 1
		) l ON TRUE
		LEFT JOIN LATERAL (
			SELECT *
			FROM stream_delivery_failures
			WHERE subscription_id = sub.subscription_id
			ORDER BY last_seen_at DESC
			LIMIT 1
		) f ON TRUE
		WHERE sub.user_id = $1
		  AND sub.status = 'active'
		  AND ($2 = '' OR sub.session_id = $2)
		  AND ($3 = '' OR sub.runtime_id = $3)
		ORDER BY sub.updated_at DESC, sub.subscription_id DESC`,
		userID, sessionID, runtimeID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.SessionDeliveryHealth
	for rows.Next() {
		health, err := scanSessionDeliveryHealth(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, health)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) ReleaseStreamDeliveryLease(ctx context.Context, leaseID, ownerInstanceID string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE stream_delivery_leases
		SET status = 'released',
		    released_at = NOW(),
		    updated_at = NOW()
		WHERE lease_id = $1
		  AND owner_instance_id = $2
		  AND status = 'active'`,
		leaseID,
		ownerInstanceID,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) CreateOrRenewMarketDataWriterLease(
	ctx context.Context,
	key domain.StreamKey,
	year int32,
	ownerInstanceID string,
	scraperInstanceID string,
	collectorID string,
	ttl time.Duration,
) (domain.MarketDataWriterLease, error) {
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	leaseID := "mdwl-" + uuid.NewString()
	expiresAt := time.Now().Add(ttl).UTC()
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO market_data_writer_leases
			(lease_id, exchange, market, kind, symbol, interval, year,
			 owner_instance_id, scraper_instance_id, collector_id, status,
			 acquired_at, last_heartbeat_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, 'active', NOW(), NOW(), $11)
		ON CONFLICT (exchange, market, kind, symbol, interval, year) WHERE status = 'active'
		DO UPDATE SET
			lease_id = CASE
				WHEN market_data_writer_leases.owner_instance_id = EXCLUDED.owner_instance_id
					THEN market_data_writer_leases.lease_id
				ELSE EXCLUDED.lease_id
			END,
			owner_instance_id = EXCLUDED.owner_instance_id,
			scraper_instance_id = EXCLUDED.scraper_instance_id,
			collector_id = EXCLUDED.collector_id,
			acquired_at = CASE
				WHEN market_data_writer_leases.owner_instance_id = EXCLUDED.owner_instance_id
					THEN market_data_writer_leases.acquired_at
				ELSE NOW()
			END,
			last_heartbeat_at = NOW(),
			expires_at = EXCLUDED.expires_at,
			released_at = NULL,
			updated_at = NOW()
		WHERE market_data_writer_leases.owner_instance_id = EXCLUDED.owner_instance_id
		   OR market_data_writer_leases.expires_at <= NOW()
		RETURNING lease_id, exchange, market, kind, symbol, interval, year,
			owner_instance_id, scraper_instance_id, collector_id, status,
			acquired_at, last_heartbeat_at, expires_at, released_at, created_at, updated_at`,
		leaseID,
		key.Exchange,
		key.Market,
		key.Kind,
		key.Symbol,
		key.Interval,
		year,
		ownerInstanceID,
		scraperInstanceID,
		collectorID,
		expiresAt,
	)
	lease, err := scanMarketDataWriterLease(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataWriterLease{}, ErrPermissionDenied
	}
	return lease, err
}

func (r *TimescaleRepository) ReleaseMarketDataWriterLease(ctx context.Context, leaseID, ownerInstanceID string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE market_data_writer_leases
		SET status = 'released',
		    released_at = NOW(),
		    updated_at = NOW()
		WHERE lease_id = $1
		  AND owner_instance_id = $2
		  AND status = 'active'`,
		leaseID,
		ownerInstanceID,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// ── market_data_history_requests ────────────────────────────────────────

func (r *TimescaleRepository) UpsertMarketDataHistoryRequest(
	ctx context.Context,
	userID int64,
	accountID *int64,
	key domain.StreamKey,
	startAt, endAt time.Time,
) (domain.MarketDataHistoryRequest, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.MarketDataHistoryRequest{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	var existingID int64
	err = tx.QueryRowContext(ctx, `
		SELECT request_id FROM market_data_history_requests
		WHERE user_id = $1 AND exchange = $2 AND market = $3
		  AND kind = $4 AND symbol = $5 AND interval = $6
		  AND requested_start_at = $7 AND requested_end_at = $8
		  AND status != 'cancelled'
		LIMIT 1`,
		userID, key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
		startAt.UTC(), endAt.UTC(),
	).Scan(&existingID)

	if err == nil {
		if _, err := tx.ExecContext(ctx, `
			UPDATE market_data_history_requests
			SET account_id = COALESCE($2, account_id),
			    status = CASE WHEN status = 'cancelled' THEN 'pending' ELSE status END,
			    updated_at = NOW()
			WHERE request_id = $1`,
			existingID, accountID,
		); err != nil {
			return domain.MarketDataHistoryRequest{}, fmt.Errorf("update history request: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.MarketDataHistoryRequest{}, err
		}
		return r.getMarketDataHistoryRequest(ctx, existingID, userID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataHistoryRequest{}, fmt.Errorf("lookup existing history request: %w", err)
	}

	var newID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO market_data_history_requests
			(user_id, account_id, exchange, market, kind, symbol, interval,
			 requested_start_at, requested_end_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending')
		RETURNING request_id`,
		userID, accountID,
		key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
		startAt.UTC(), endAt.UTC(),
	).Scan(&newID); err != nil {
		return domain.MarketDataHistoryRequest{}, fmt.Errorf("insert history request: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.MarketDataHistoryRequest{}, err
	}
	return r.getMarketDataHistoryRequest(ctx, newID, userID)
}

// getMarketDataHistoryRequest is the internal helper. The public
// version (used by the service for lookups) is GetMarketDataHistoryRequest
// below; this private name avoids a self-recursion subtlety in the
// upsert flow above.
func (r *TimescaleRepository) getMarketDataHistoryRequest(ctx context.Context, requestID, userID int64) (domain.MarketDataHistoryRequest, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT request_id, user_id, account_id,
			exchange, market, kind, symbol, interval,
			status, requested_start_at, requested_end_at,
			covered_start_at, covered_end_at, COALESCE(last_error, ''),
			created_at, updated_at, cancelled_at
		FROM market_data_history_requests
		WHERE request_id = $1`,
		requestID,
	)
	req, err := scanHistoryRequest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataHistoryRequest{}, ErrNotFound
	}
	if err != nil {
		return domain.MarketDataHistoryRequest{}, err
	}
	if req.UserID != userID {
		return domain.MarketDataHistoryRequest{}, ErrPermissionDenied
	}
	return req, nil
}

func (r *TimescaleRepository) CancelMarketDataHistoryRequest(ctx context.Context, requestID, userID int64) error {
	var ownerID int64
	err := r.db.QueryRowContext(ctx, `
		SELECT user_id FROM market_data_history_requests WHERE request_id = $1`,
		requestID,
	).Scan(&ownerID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if ownerID != userID {
		return ErrPermissionDenied
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE market_data_history_requests
		SET status = 'cancelled', cancelled_at = NOW(), updated_at = NOW()
		WHERE request_id = $1 AND status != 'cancelled'`,
		requestID,
	)
	return err
}

func (r *TimescaleRepository) ListMarketDataHistoryRequestsByUser(ctx context.Context, userID int64) ([]domain.MarketDataHistoryRequest, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT request_id, user_id, account_id,
			exchange, market, kind, symbol, interval,
			status, requested_start_at, requested_end_at,
			covered_start_at, covered_end_at, COALESCE(last_error, ''),
			created_at, updated_at, cancelled_at
		FROM market_data_history_requests
		WHERE user_id = $1
		  AND status NOT IN ('ready', 'cancelled')
		ORDER BY request_id DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MarketDataHistoryRequest
	for rows.Next() {
		req, err := scanHistoryRequest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, req)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) ListMarketDataHistoryRequests(ctx context.Context, includeTerminal bool) ([]domain.MarketDataHistoryRequest, error) {
	query := `
		SELECT request_id, user_id, account_id,
			exchange, market, kind, symbol, interval,
			status, requested_start_at, requested_end_at,
			covered_start_at, covered_end_at, COALESCE(last_error, ''),
			created_at, updated_at, cancelled_at
		FROM market_data_history_requests`
	var rows *sql.Rows
	var err error
	if includeTerminal {
		rows, err = r.db.QueryContext(ctx, query+` ORDER BY request_id DESC`)
	} else {
		rows, err = r.db.QueryContext(ctx, query+` WHERE status NOT IN ('ready', 'cancelled') ORDER BY request_id DESC`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MarketDataHistoryRequest
	for rows.Next() {
		req, err := scanHistoryRequest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, req)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) UpdateMarketDataHistoryRequestState(
	ctx context.Context,
	requestID int64,
	status domain.MarketDataHistoryRequestStatus,
	coveredStartAt, coveredEndAt *time.Time,
	lastError string,
) (domain.MarketDataHistoryRequest, error) {
	var ownerID int64
	err := r.db.QueryRowContext(ctx, `
		SELECT user_id FROM market_data_history_requests WHERE request_id = $1`,
		requestID,
	).Scan(&ownerID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MarketDataHistoryRequest{}, ErrNotFound
	}
	if err != nil {
		return domain.MarketDataHistoryRequest{}, err
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE market_data_history_requests
		SET status = $2,
		    covered_start_at = $3,
		    covered_end_at = $4,
		    last_error = $5,
		    updated_at = NOW()
		WHERE request_id = $1`,
		requestID,
		string(status),
		nullableUTCTime(coveredStartAt),
		nullableUTCTime(coveredEndAt),
		lastError,
	)
	if err != nil {
		return domain.MarketDataHistoryRequest{}, err
	}
	return r.getMarketDataHistoryRequest(ctx, requestID, ownerID)
}

// ── historical coverage ────────────────────────────────────────────────

func (r *TimescaleRepository) QueryMarketDataCoverage(ctx context.Context, key domain.StreamKey, startAt, endAt time.Time) ([]domain.MarketDataCoverageSegment, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT segment_id,
			exchange, market, kind, symbol, interval,
			year, segment_start_at, segment_end_at, row_count, source, updated_at
		FROM market_data_coverage_segments
		WHERE exchange = $1 AND market = $2 AND kind = $3
		  AND symbol = $4 AND interval = $5
		  AND segment_end_at > $6 AND segment_start_at < $7
		ORDER BY segment_start_at, segment_end_at`,
		key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval,
		startAt.UTC(), endAt.UTC(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MarketDataCoverageSegment
	for rows.Next() {
		seg, err := scanCoverageSegment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, seg)
	}
	return out, rows.Err()
}

func (r *TimescaleRepository) MergeMarketDataCoverageSegments(ctx context.Context, segments []domain.MarketDataCoverageSegment) ([]domain.MarketDataCoverageSegment, error) {
	out := make([]domain.MarketDataCoverageSegment, 0, len(segments))
	for _, seg := range segments {
		merged, err := r.mergeMarketDataCoverageSegment(ctx, seg)
		if err != nil {
			return nil, err
		}
		out = append(out, merged)
	}
	return out, nil
}

func (r *TimescaleRepository) mergeMarketDataCoverageSegment(ctx context.Context, incoming domain.MarketDataCoverageSegment) (domain.MarketDataCoverageSegment, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.MarketDataCoverageSegment{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, marketDataCoverageLockKey(incoming.Key, incoming.Year)); err != nil {
		return domain.MarketDataCoverageSegment{}, err
	}

	mergedStart := incoming.StartAt.UTC()
	mergedEnd := incoming.EndAt.UTC()
	candidates := map[int64]domain.MarketDataCoverageSegment{}
	for {
		rows, err := tx.QueryContext(ctx, `
			SELECT segment_id,
				exchange, market, kind, symbol, interval,
				year, segment_start_at, segment_end_at, row_count, source, updated_at
			FROM market_data_coverage_segments
			WHERE exchange = $1 AND market = $2 AND kind = $3
			  AND symbol = $4 AND interval = $5 AND year = $6
			  AND segment_end_at >= $7 AND segment_start_at <= $8
			ORDER BY segment_start_at, segment_end_at
			FOR UPDATE`,
			incoming.Key.Exchange,
			incoming.Key.Market,
			incoming.Key.Kind,
			incoming.Key.Symbol,
			incoming.Key.Interval,
			incoming.Year,
			mergedStart,
			mergedEnd,
		)
		if err != nil {
			return domain.MarketDataCoverageSegment{}, err
		}
		foundNew := false
		for rows.Next() {
			seg, err := scanCoverageSegment(rows)
			if err != nil {
				rows.Close() //nolint:errcheck
				return domain.MarketDataCoverageSegment{}, err
			}
			if _, ok := candidates[seg.SegmentID]; ok {
				continue
			}
			candidates[seg.SegmentID] = seg
			if seg.StartAt.Before(mergedStart) {
				mergedStart = seg.StartAt
			}
			if seg.EndAt.After(mergedEnd) {
				mergedEnd = seg.EndAt
			}
			foundNew = true
		}
		if err := rows.Err(); err != nil {
			rows.Close() //nolint:errcheck
			return domain.MarketDataCoverageSegment{}, err
		}
		rows.Close() //nolint:errcheck
		if !foundNew {
			break
		}
	}

	for id := range candidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM market_data_coverage_segments WHERE segment_id = $1`, id); err != nil {
			return domain.MarketDataCoverageSegment{}, err
		}
	}

	rowCount, err := coverageExpectedCount(mergedStart, mergedEnd, incoming.Key.Interval)
	if err != nil {
		return domain.MarketDataCoverageSegment{}, err
	}
	row := tx.QueryRowContext(ctx, `
		INSERT INTO market_data_coverage_segments
			(exchange, market, kind, symbol, interval, year,
			 segment_start_at, segment_end_at, row_count, source, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
		ON CONFLICT (exchange, market, kind, symbol, interval, year, segment_start_at, segment_end_at)
		DO UPDATE SET row_count = EXCLUDED.row_count,
			source = EXCLUDED.source,
			updated_at = NOW()
		RETURNING segment_id,
			exchange, market, kind, symbol, interval,
			year, segment_start_at, segment_end_at, row_count, source, updated_at`,
		incoming.Key.Exchange,
		incoming.Key.Market,
		incoming.Key.Kind,
		incoming.Key.Symbol,
		incoming.Key.Interval,
		incoming.Year,
		mergedStart,
		mergedEnd,
		rowCount,
		incoming.Source,
	)
	merged, err := scanCoverageSegment(row)
	if err != nil {
		return domain.MarketDataCoverageSegment{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.MarketDataCoverageSegment{}, err
	}
	return merged, nil
}

func marketDataCoverageLockKey(key domain.StreamKey, year int32) int64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%s|%s|%s|%s|%s|%d", key.Exchange, key.Market, key.Kind, key.Symbol, key.Interval, year)
	return int64(h.Sum64())
}

func coverageExpectedCount(startAt, endAt time.Time, interval string) (int64, error) {
	startAt = startAt.UTC()
	endAt = endAt.UTC()
	if !endAt.After(startAt) {
		return 0, fmt.Errorf("end_at must be after start_at")
	}
	d, err := coverageIntervalDuration(interval)
	if err != nil {
		return 0, err
	}
	diff := endAt.Sub(startAt)
	if !isAlignedToCoverageInterval(startAt, d) || !isAlignedToCoverageInterval(endAt, d) {
		return 0, fmt.Errorf("start_at and end_at must align to interval %q", interval)
	}
	if diff%d != 0 {
		return 0, fmt.Errorf("range must be an exact multiple of interval %q", interval)
	}
	return int64(diff / d), nil
}

func coverageIntervalDuration(interval string) (time.Duration, error) {
	if len(interval) < 2 {
		return 0, fmt.Errorf("interval %q is invalid", interval)
	}
	n, err := strconv.Atoi(interval[:len(interval)-1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("interval %q is invalid", interval)
	}
	switch interval[len(interval)-1] {
	case 's':
		return time.Duration(n) * time.Second, nil
	case 'm':
		return time.Duration(n) * time.Minute, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("interval %q is not supported for coverage; supported units: s/m/h/d", interval)
	}
}

func isAlignedToCoverageInterval(t time.Time, d time.Duration) bool {
	n := int64(d)
	if n <= 0 {
		return false
	}
	return t.UTC().UnixNano()%n == 0
}

// ── helpers / scanners ──────────────────────────────────────────────────

type rowScanner interface {
	Scan(dest ...any) error
}

func scanStream(row rowScanner) (domain.MarketDataStream, error) {
	var s domain.MarketDataStream
	var desired, actual string
	var lastData sql.NullTime
	var lastReconciled sql.NullTime
	if err := row.Scan(
		&s.StreamID,
		&s.Key.Exchange, &s.Key.Market, &s.Key.Kind, &s.Key.Symbol, &s.Key.Interval,
		&desired, &actual, &s.EffectiveLiveDelivery,
		&lastData, &s.LastError, &lastReconciled,
		&s.CreatedAt, &s.UpdatedAt,
	); err != nil {
		return domain.MarketDataStream{}, err
	}
	s.DesiredState = domain.StreamDesiredState(desired)
	s.ActualState = domain.StreamActualState(actual)
	if lastData.Valid {
		t := lastData.Time
		s.LastDataAt = &t
	}
	if lastReconciled.Valid {
		t := lastReconciled.Time
		s.LastReconciledAt = &t
	}
	return s, nil
}

func scanRequest(row rowScanner) (domain.MarketDataRequest, error) {
	var r domain.MarketDataRequest
	var status string
	var acct sql.NullInt64
	var cancelledAt sql.NullTime
	if err := row.Scan(
		&r.RequestID, &r.UserID, &acct, &r.StreamID,
		&r.Key.Exchange, &r.Key.Market, &r.Key.Kind, &r.Key.Symbol, &r.Key.Interval,
		&r.NeedsLiveDelivery, &status, &r.CreatedAt, &r.UpdatedAt, &cancelledAt,
	); err != nil {
		return domain.MarketDataRequest{}, err
	}
	r.Status = domain.MarketDataRequestStatus(status)
	if acct.Valid {
		v := acct.Int64
		r.AccountID = &v
	}
	if cancelledAt.Valid {
		t := cancelledAt.Time
		r.CancelledAt = &t
	}
	return r, nil
}

func scanLease(row rowScanner) (domain.MarketDataLease, error) {
	var l domain.MarketDataLease
	var strat, acct sql.NullInt64
	var released sql.NullTime
	if err := row.Scan(
		&l.LeaseID, &l.SessionID, &strat, &acct,
		&l.StreamID, &l.ExpiresAt, &l.LastHeartbeatAt, &l.CreatedAt, &released,
	); err != nil {
		return domain.MarketDataLease{}, err
	}
	if strat.Valid {
		v := strat.Int64
		l.StrategyID = &v
	}
	if acct.Valid {
		v := acct.Int64
		l.AccountID = &v
	}
	if released.Valid {
		t := released.Time
		l.ReleasedAt = &t
	}
	return l, nil
}

func scanSessionSubscription(row rowScanner) (domain.SessionMarketDataSubscription, error) {
	var s domain.SessionMarketDataSubscription
	var released sql.NullTime
	if err := row.Scan(
		&s.SubscriptionID,
		&s.UserID,
		&s.SessionID,
		&s.RuntimeID,
		&s.Key.Exchange, &s.Key.Market, &s.Key.Kind, &s.Key.Symbol, &s.Key.Interval,
		&s.Mode,
		&s.Status,
		&s.CreatedAt,
		&s.UpdatedAt,
		&released,
	); err != nil {
		return domain.SessionMarketDataSubscription{}, err
	}
	if released.Valid {
		t := released.Time
		s.ReleasedAt = &t
	}
	return s, nil
}

func scanStreamDeliveryLease(row rowScanner) (domain.StreamDeliveryLease, error) {
	var l domain.StreamDeliveryLease
	var released, lastDelivery sql.NullTime
	if err := row.Scan(
		&l.LeaseID,
		&l.SubscriptionID,
		&l.OwnerInstanceID,
		&l.Status,
		&l.AcquiredAt,
		&l.LastHeartbeatAt,
		&l.ExpiresAt,
		&lastDelivery,
		&l.LastTopic,
		&l.LastPartition,
		&l.LastOffset,
		&released,
		&l.CreatedAt,
		&l.UpdatedAt,
	); err != nil {
		return domain.StreamDeliveryLease{}, err
	}
	if lastDelivery.Valid {
		t := lastDelivery.Time
		l.LastDeliveryAt = &t
	}
	if released.Valid {
		t := released.Time
		l.ReleasedAt = &t
	}
	return l, nil
}

func scanSessionDeliveryHealth(row rowScanner) (domain.SessionDeliveryHealth, error) {
	var h domain.SessionDeliveryHealth
	var subReleased sql.NullTime
	var leaseID, leaseOwner, leaseStatus, leaseTopic sql.NullString
	var leaseSubID, leaseOffset sql.NullInt64
	var leasePartition sql.NullInt32
	var leaseAcquired, leaseHeartbeat, leaseExpires, leaseLastDelivery, leaseReleased, leaseCreated, leaseUpdated sql.NullTime
	var failureID, failureSubID, failureAttempts sql.NullInt64
	var failureOwner, failureTopic, failureStreamKey, failureCode, failureReason sql.NullString
	var failureFirst, failureLast sql.NullTime

	if err := row.Scan(
		&h.Subscription.SubscriptionID,
		&h.Subscription.UserID,
		&h.Subscription.SessionID,
		&h.Subscription.RuntimeID,
		&h.Subscription.Key.Exchange, &h.Subscription.Key.Market, &h.Subscription.Key.Kind, &h.Subscription.Key.Symbol, &h.Subscription.Key.Interval,
		&h.Subscription.Mode,
		&h.Subscription.Status,
		&h.Subscription.CreatedAt,
		&h.Subscription.UpdatedAt,
		&subReleased,
		&leaseID, &leaseSubID, &leaseOwner, &leaseStatus,
		&leaseAcquired, &leaseHeartbeat, &leaseExpires,
		&leaseLastDelivery, &leaseTopic, &leasePartition, &leaseOffset,
		&leaseReleased, &leaseCreated, &leaseUpdated,
		&failureID, &failureSubID, &failureOwner, &failureTopic, &failureStreamKey,
		&failureCode, &failureReason, &failureFirst, &failureLast, &failureAttempts,
	); err != nil {
		return domain.SessionDeliveryHealth{}, err
	}
	if subReleased.Valid {
		t := subReleased.Time
		h.Subscription.ReleasedAt = &t
	}
	if leaseID.Valid {
		lease := domain.StreamDeliveryLease{
			LeaseID:         leaseID.String,
			SubscriptionID:  leaseSubID.Int64,
			OwnerInstanceID: leaseOwner.String,
			Status:          leaseStatus.String,
			LastTopic:       leaseTopic.String,
			LastPartition:   int32(leasePartition.Int32),
			LastOffset:      leaseOffset.Int64,
		}
		if leaseAcquired.Valid {
			lease.AcquiredAt = leaseAcquired.Time
		}
		if leaseHeartbeat.Valid {
			lease.LastHeartbeatAt = leaseHeartbeat.Time
		}
		if leaseExpires.Valid {
			lease.ExpiresAt = leaseExpires.Time
		}
		if leaseLastDelivery.Valid {
			t := leaseLastDelivery.Time
			lease.LastDeliveryAt = &t
		}
		if leaseReleased.Valid {
			t := leaseReleased.Time
			lease.ReleasedAt = &t
		}
		if leaseCreated.Valid {
			lease.CreatedAt = leaseCreated.Time
		}
		if leaseUpdated.Valid {
			lease.UpdatedAt = leaseUpdated.Time
		}
		h.Lease = &lease
	}
	if failureID.Valid {
		failure := domain.StreamDeliveryFailure{
			FailureID:       failureID.Int64,
			SubscriptionID:  failureSubID.Int64,
			OwnerInstanceID: failureOwner.String,
			Topic:           failureTopic.String,
			StreamKey:       failureStreamKey.String,
			FailureCode:     failureCode.String,
			Reason:          failureReason.String,
			AttemptCount:    int(failureAttempts.Int64),
		}
		if failureFirst.Valid {
			failure.FirstSeenAt = failureFirst.Time
		}
		if failureLast.Valid {
			failure.LastSeenAt = failureLast.Time
		}
		h.LatestFailure = &failure
	}
	return h, nil
}

func scanMarketDataWriterLease(row rowScanner) (domain.MarketDataWriterLease, error) {
	var l domain.MarketDataWriterLease
	var released sql.NullTime
	if err := row.Scan(
		&l.LeaseID,
		&l.Key.Exchange,
		&l.Key.Market,
		&l.Key.Kind,
		&l.Key.Symbol,
		&l.Key.Interval,
		&l.Year,
		&l.OwnerInstanceID,
		&l.ScraperInstanceID,
		&l.CollectorID,
		&l.Status,
		&l.AcquiredAt,
		&l.LastHeartbeatAt,
		&l.ExpiresAt,
		&released,
		&l.CreatedAt,
		&l.UpdatedAt,
	); err != nil {
		return domain.MarketDataWriterLease{}, err
	}
	if released.Valid {
		t := released.Time
		l.ReleasedAt = &t
	}
	return l, nil
}

func scanCoverageSegment(row rowScanner) (domain.MarketDataCoverageSegment, error) {
	var seg domain.MarketDataCoverageSegment
	if err := row.Scan(
		&seg.SegmentID,
		&seg.Key.Exchange,
		&seg.Key.Market,
		&seg.Key.Kind,
		&seg.Key.Symbol,
		&seg.Key.Interval,
		&seg.Year,
		&seg.StartAt,
		&seg.EndAt,
		&seg.RowCount,
		&seg.Source,
		&seg.UpdatedAt,
	); err != nil {
		return domain.MarketDataCoverageSegment{}, err
	}
	return seg, nil
}

func scanHistoryRequest(row rowScanner) (domain.MarketDataHistoryRequest, error) {
	var r domain.MarketDataHistoryRequest
	var status string
	var acct sql.NullInt64
	var coveredStart sql.NullTime
	var coveredEnd sql.NullTime
	var cancelledAt sql.NullTime
	if err := row.Scan(
		&r.RequestID, &r.UserID, &acct,
		&r.Key.Exchange, &r.Key.Market, &r.Key.Kind, &r.Key.Symbol, &r.Key.Interval,
		&status, &r.RequestedStartAt, &r.RequestedEndAt,
		&coveredStart, &coveredEnd, &r.LastError,
		&r.CreatedAt, &r.UpdatedAt, &cancelledAt,
	); err != nil {
		return domain.MarketDataHistoryRequest{}, err
	}
	r.Status = domain.MarketDataHistoryRequestStatus(status)
	if acct.Valid {
		v := acct.Int64
		r.AccountID = &v
	}
	if coveredStart.Valid {
		t := coveredStart.Time
		r.CoveredStartAt = &t
	}
	if coveredEnd.Valid {
		t := coveredEnd.Time
		r.CoveredEndAt = &t
	}
	if cancelledAt.Valid {
		t := cancelledAt.Time
		r.CancelledAt = &t
	}
	return r, nil
}

func nullableUTCTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

// itoa avoids importing strconv just for one-digit placeholders (SQL $N
// placeholder construction in UpdateMarketDataStreamActualState).
func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return fmt.Sprintf("%d", n)
}
