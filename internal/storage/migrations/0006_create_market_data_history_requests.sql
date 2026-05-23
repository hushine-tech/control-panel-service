-- Phase D2: market-data control plane moves into control-panel-service.
-- Mirror of account-service 0010_create_market_data_history_requests.sql.
-- Cross-database FKs to users/accounts dropped because Postgres cannot
-- enforce FKs across databases.
--
-- See migration 0004 for the full discussion of the orphan-on-delete
-- behaviour change. The same caveat applies here: deleting a user in
-- account-service no longer cascade-deletes their history requests.
--
-- The original migration shared the request_id sequence with
-- market_data_requests. We preserve the same trick by using the
-- sequence created in 0004 — DDL-level ordering 0003 → 0004 → 0006
-- ensures the sequence exists before history_requests references it.
--
-- Historical market-data requests represent finite backfill windows
-- and are intentionally separate from market_data_streams /
-- market_data_leases (which track continuously running shared live
-- collectors).
CREATE TABLE IF NOT EXISTS market_data_history_requests (
    request_id            BIGINT      PRIMARY KEY
        DEFAULT nextval('market_data_requests_request_id_seq'::regclass),
    user_id               BIGINT      NOT NULL,           -- logical FK to account.users(id)
    account_id            BIGINT      NULL,               -- logical FK to account.accounts(account_id)
    exchange              TEXT        NOT NULL,
    market                TEXT        NOT NULL,
    kind                  TEXT        NOT NULL,
    symbol                TEXT        NOT NULL,
    interval              TEXT        NOT NULL,
    requested_start_at    TIMESTAMPTZ NOT NULL,
    requested_end_at      TIMESTAMPTZ NOT NULL,
    covered_start_at      TIMESTAMPTZ NULL,
    covered_end_at        TIMESTAMPTZ NULL,
    last_error            TEXT        NULL,
    status                TEXT        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'verifying', 'ready', 'error', 'cancelled')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cancelled_at          TIMESTAMPTZ NULL,
    CHECK (requested_end_at > requested_start_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_market_data_history_requests_active_per_user_window
    ON market_data_history_requests (
        user_id, exchange, market, kind, symbol, interval, requested_start_at, requested_end_at
    )
    WHERE status != 'cancelled';

CREATE INDEX IF NOT EXISTS idx_market_data_history_requests_user
    ON market_data_history_requests (user_id, status);
