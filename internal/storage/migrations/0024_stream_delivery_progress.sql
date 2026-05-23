-- RuntimeChannel live market-data delivery progress.
--
-- The lease row already proves that a control-panel instance owns a
-- subscription shard. These fields record whether that owner has delivered
-- any Kafka message to the runtime, and the latest Kafka position observed.

ALTER TABLE stream_delivery_leases
    ADD COLUMN IF NOT EXISTS last_delivery_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_topic TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS last_partition INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_offset BIGINT NOT NULL DEFAULT 0;
