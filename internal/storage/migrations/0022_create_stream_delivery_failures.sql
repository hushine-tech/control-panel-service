-- RuntimeChannel live-delivery diagnostics.
--
-- Stores non-secret delivery worker failures so Runtime Management/session
-- detail can explain why a demo session is not receiving live bars.

CREATE TABLE IF NOT EXISTS stream_delivery_failures (
    failure_id        BIGSERIAL   PRIMARY KEY,
    subscription_id   BIGINT      NOT NULL DEFAULT 0,
    owner_instance_id TEXT        NOT NULL DEFAULT '',
    topic             TEXT        NOT NULL DEFAULT '',
    stream_key        TEXT        NOT NULL DEFAULT '',
    failure_code      TEXT        NOT NULL DEFAULT '',
    reason            TEXT        NOT NULL,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt_count     INTEGER     NOT NULL DEFAULT 1,
    CHECK (reason <> ''),
    CHECK (attempt_count > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_stream_delivery_failures_rollup
    ON stream_delivery_failures (
        subscription_id,
        owner_instance_id,
        topic,
        stream_key,
        failure_code
    );

CREATE INDEX IF NOT EXISTS idx_stream_delivery_failures_subscription
    ON stream_delivery_failures (subscription_id, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_stream_delivery_failures_topic
    ON stream_delivery_failures (topic, last_seen_at DESC)
    WHERE topic <> '';
