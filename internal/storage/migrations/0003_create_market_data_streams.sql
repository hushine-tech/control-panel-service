-- Phase D2: market-data control plane moves into control-panel-service.
--
-- This table mirrors the schema originally introduced in
-- account-service/internal/storage/migrations/0009_create_market_data_control_plane.sql
-- but lives in the control_panel database. Cross-database foreign-key
-- constraints are NOT possible, so any references to account.users(id)
-- or account.accounts(account_id) become logical (validated at the
-- service layer via account-service.GetUser etc.) rather than enforced
-- by the database.
--
-- Aggregated flow-level state. Key: (exchange, market, kind, symbol, interval).
-- One row per physical stream. Created lazily when the first valid
-- request references this key. Never owned by a single user.
CREATE TABLE IF NOT EXISTS market_data_streams (
    stream_id                BIGSERIAL   PRIMARY KEY,
    exchange                 TEXT        NOT NULL,
    market                   TEXT        NOT NULL,   -- 'spot' | 'futures'
    kind                     TEXT        NOT NULL,   -- v1 only 'kline'
    symbol                   TEXT        NOT NULL,   -- canonical upper-case, e.g. BTCUSDT
    interval                 TEXT        NOT NULL,   -- e.g. '1m', '5m'
    desired_state            TEXT        NOT NULL DEFAULT 'running'
        CHECK (desired_state IN ('running', 'stopped')),
    actual_state             TEXT        NOT NULL DEFAULT 'pending'
        CHECK (actual_state IN ('pending', 'starting', 'running', 'draining', 'stopped', 'error')),
    effective_live_delivery  BOOLEAN     NOT NULL DEFAULT false,
    last_data_at             TIMESTAMPTZ NULL,
    last_error               TEXT        NULL,
    last_reconciled_at       TIMESTAMPTZ NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (exchange, market, kind, symbol, interval)
);

CREATE INDEX IF NOT EXISTS idx_market_data_streams_actual_state
    ON market_data_streams (actual_state);
