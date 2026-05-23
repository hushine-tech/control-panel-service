-- Phase D2: market-data control plane moves into control-panel-service.
-- Mirror of account-service 0009_create_market_data_control_plane.sql
-- (leases table portion). Cross-database FK to accounts dropped because
-- Postgres cannot enforce FKs across databases.
--
-- See migration 0004 for the full discussion of the orphan-on-delete
-- behaviour change. Short version: deleting an account in account-service
-- no longer NULLs the account_id on this table; rows orphan instead.
-- Acceptable because the platform does not delete accounts in normal
-- operation. Revisit if/when an account-delete path lands.
--
-- Session-scoped expiring claims. One row per (session_id, stream_id).
-- Created by strategy-service when a mode=2 session enters live
-- execution; renewed on heartbeat; expires automatically if heartbeat
-- stops. Scraper treats an unexpired lease as effective demand even if
-- the owning request was cancelled.
CREATE TABLE IF NOT EXISTS market_data_leases (
    lease_id           BIGSERIAL   PRIMARY KEY,
    session_id         TEXT        NOT NULL,
    strategy_id        BIGINT      NULL,
    account_id         BIGINT      NULL,                 -- logical FK to account.accounts(account_id)
    stream_id          BIGINT      NOT NULL REFERENCES market_data_streams(stream_id) ON DELETE CASCADE,
    expires_at         TIMESTAMPTZ NOT NULL,
    last_heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at        TIMESTAMPTZ NULL,
    UNIQUE (session_id, stream_id)
);

CREATE INDEX IF NOT EXISTS idx_market_data_leases_stream_active
    ON market_data_leases (stream_id, expires_at)
    WHERE released_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_market_data_leases_expires_at
    ON market_data_leases (expires_at)
    WHERE released_at IS NULL;
