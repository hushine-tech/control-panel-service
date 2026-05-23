-- Durable runtime command records.
--
-- API handlers create command rows, dispatchers send them over RuntimeChannel,
-- and status APIs read the persisted state instead of blocking on user code.

CREATE TABLE IF NOT EXISTS runtime_commands (
    command_id       TEXT        PRIMARY KEY,
    user_id          BIGINT      NOT NULL,
    runtime_id       TEXT        NOT NULL,
    session_id       TEXT        NOT NULL DEFAULT '',
    idempotency_key  TEXT        NOT NULL DEFAULT '',
    command_type     TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'queued',
    deadline_at      TIMESTAMPTZ NOT NULL,
    sent_at          TIMESTAMPTZ,
    acked_at         TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    cancelled_at     TIMESTAMPTZ,
    attempt_count    INTEGER     NOT NULL DEFAULT 0,
    payload          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    result           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    failure_reason   TEXT        NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (status IN (
        'queued',
        'sent',
        'acked',
        'running',
        'succeeded',
        'failed',
        'timed_out',
        'cancelled'
    )),
    CHECK (command_type IN (
        'start_session',
        'stop_session',
        'finish_session',
        'shutdown_runtime',
        'status_patch'
    ))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_commands_runtime_idempotency
    ON runtime_commands (runtime_id, idempotency_key)
    WHERE idempotency_key <> '';

CREATE INDEX IF NOT EXISTS idx_runtime_commands_runtime_status
    ON runtime_commands (runtime_id, status, created_at);

CREATE INDEX IF NOT EXISTS idx_runtime_commands_deadline
    ON runtime_commands (deadline_at)
    WHERE status IN ('queued', 'sent', 'acked', 'running');

CREATE INDEX IF NOT EXISTS idx_runtime_commands_session
    ON runtime_commands (session_id, created_at DESC)
    WHERE session_id <> '';
