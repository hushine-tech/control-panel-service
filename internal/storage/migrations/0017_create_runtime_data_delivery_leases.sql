-- Runtime data delivery ownership.
--
-- session_market_data_subscriptions records the authorized streams a session
-- may receive. stream_delivery_leases records which control-panel/delivery
-- instance currently owns delivery work for a subscription.

CREATE TABLE IF NOT EXISTS session_market_data_subscriptions (
    subscription_id BIGSERIAL   PRIMARY KEY,
    user_id         BIGINT      NOT NULL,
    session_id      TEXT        NOT NULL,
    runtime_id      TEXT        NOT NULL,
    exchange        TEXT        NOT NULL DEFAULT 'binance',
    market          TEXT        NOT NULL,
    kind            TEXT        NOT NULL DEFAULT 'kline',
    symbol          TEXT        NOT NULL,
    interval        TEXT        NOT NULL DEFAULT '',
    mode            INTEGER     NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at     TIMESTAMPTZ,
    CHECK (market IN ('spot', 'futures')),
    CHECK (kind IN ('kline', 'funding_rate', 'open_interest', 'orderbook')),
    CHECK (status IN ('active', 'released', 'failed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_session_market_data_subscriptions_active
    ON session_market_data_subscriptions (
        session_id, exchange, market, kind, symbol, interval, mode
    )
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_session_market_data_subscriptions_runtime
    ON session_market_data_subscriptions (runtime_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_market_data_subscriptions_user
    ON session_market_data_subscriptions (user_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS stream_delivery_leases (
    lease_id          TEXT        PRIMARY KEY,
    subscription_id   BIGINT      NOT NULL REFERENCES session_market_data_subscriptions(subscription_id) ON DELETE CASCADE,
    owner_instance_id TEXT        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'active',
    acquired_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at        TIMESTAMPTZ NOT NULL,
    released_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (owner_instance_id <> ''),
    CHECK (status IN ('active', 'released', 'expired', 'stolen'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_stream_delivery_leases_active_subscription
    ON stream_delivery_leases (subscription_id)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_stream_delivery_leases_owner
    ON stream_delivery_leases (owner_instance_id, status, expires_at);

CREATE INDEX IF NOT EXISTS idx_stream_delivery_leases_expiry
    ON stream_delivery_leases (expires_at)
    WHERE status = 'active';
