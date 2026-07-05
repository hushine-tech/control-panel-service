-- Phase D2: market-data control plane moves into control-panel-service.
-- Mirror of core-service 0009_create_market_data_control_plane.sql
-- (requests table portion). Cross-database FKs to users/portfolios are
-- dropped because Postgres cannot enforce FKs across databases.
--
-- ── Behavioural delta from the source schema ───────────────────────────────
-- Source had:
--   user_id  REFERENCES users(id)            ON DELETE CASCADE
--   portfolio_id REFERENCES portfolios(...)      ON DELETE SET NULL
-- After D2, these constraints no longer exist. Consequences:
--   * Deleting a user/portfolio in core-service does NOT cascade-delete
--     the user's market_data_* rows in control_panel; they ORPHAN.
--   * No service-layer code currently runs the equivalent cleanup — the
--     "validated at the service layer" framing is a future placeholder,
--     not present behaviour.
-- This is acceptable for the current product because the platform does
-- not delete users or portfolios in normal operation (single-tenant dev,
-- multi-tenant later). If/when a real user-deletion path lands,
-- control-panel-service must add an `OnUserDeleted` / `OnPortfolioDeleted`
-- RPC (called by core-service inside its own delete transaction) and
-- this comment block must be revisited.
--
-- User-owned demand. Upsert key:
-- (user_id, exchange, market, kind, symbol, interval) while not
-- cancelled — same user can re-open after cancelling, can't accumulate
-- unbounded active duplicates.
CREATE TABLE IF NOT EXISTS market_data_requests (
    request_id             BIGSERIAL   PRIMARY KEY,
    user_id                BIGINT      NOT NULL,           -- logical FK to portfolio.users(id)
    portfolio_id             BIGINT      NULL,               -- logical FK to portfolio.portfolios(portfolio_id)
    exchange               TEXT        NOT NULL,
    market                 TEXT        NOT NULL,
    kind                   TEXT        NOT NULL,
    symbol                 TEXT        NOT NULL,
    interval               TEXT        NOT NULL,
    needs_live_delivery    BOOLEAN     NOT NULL DEFAULT true,
    status                 TEXT        NOT NULL DEFAULT 'active'
        CHECK (status IN ('pending', 'active', 'cancelled')),
    stream_id              BIGINT      NOT NULL REFERENCES market_data_streams(stream_id) ON DELETE CASCADE,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cancelled_at           TIMESTAMPTZ NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_market_data_requests_active_per_user_stream
    ON market_data_requests (user_id, exchange, market, kind, symbol, interval)
    WHERE status != 'cancelled';

CREATE INDEX IF NOT EXISTS idx_market_data_requests_user
    ON market_data_requests (user_id, status);

CREATE INDEX IF NOT EXISTS idx_market_data_requests_stream
    ON market_data_requests (stream_id)
    WHERE status != 'cancelled';
