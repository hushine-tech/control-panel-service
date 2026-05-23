-- Market-data writer ownership.
--
-- Scraper collectors must hold one active lease for each write domain before
-- writing to a year database. The domain is:
-- (exchange, market, kind, symbol, interval, year).

CREATE TABLE IF NOT EXISTS market_data_writer_leases (
    lease_id            TEXT        PRIMARY KEY,
    exchange            TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    kind                TEXT        NOT NULL,
    symbol              TEXT        NOT NULL,
    interval            TEXT        NOT NULL DEFAULT '',
    year                INTEGER     NOT NULL,
    owner_instance_id   TEXT        NOT NULL,
    scraper_instance_id TEXT        NOT NULL DEFAULT '',
    collector_id        TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'active',
    acquired_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at          TIMESTAMPTZ NOT NULL,
    released_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (exchange IN ('binance', 'okx')),
    CHECK (market IN ('spot', 'futures')),
    CHECK (kind IN ('kline', 'funding_rate', 'open_interest', 'orderbook')),
    CHECK (year >= 1970),
    CHECK (owner_instance_id <> ''),
    CHECK (collector_id <> ''),
    CHECK (status IN ('active', 'released', 'expired', 'stolen'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_data_writer_leases_active_domain
    ON market_data_writer_leases (exchange, market, kind, symbol, interval, year)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_market_data_writer_leases_owner
    ON market_data_writer_leases (owner_instance_id, status, expires_at);

CREATE INDEX IF NOT EXISTS idx_market_data_writer_leases_expiry
    ON market_data_writer_leases (expires_at)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_market_data_writer_leases_collector
    ON market_data_writer_leases (scraper_instance_id, collector_id, status, updated_at DESC);
