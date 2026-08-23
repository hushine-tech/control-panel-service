CREATE TABLE IF NOT EXISTS runtime_session_cleanup_outbox (
    runtime_id text PRIMARY KEY
        REFERENCES runtime_registry(runtime_id) ON DELETE CASCADE,
    error_message text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    last_error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_runtime_session_cleanup_outbox_runtime
        CHECK (btrim(runtime_id) <> ''),
    CONSTRAINT chk_runtime_session_cleanup_outbox_error
        CHECK (btrim(error_message) <> ''),
    CONSTRAINT chk_runtime_session_cleanup_outbox_attempts
        CHECK (attempt_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_runtime_session_cleanup_outbox_due
    ON runtime_session_cleanup_outbox (next_attempt_at, created_at, runtime_id);

-- Existing terminal runtimes may have crossed the old one-shot handoff before
-- this migration. Re-enqueueing them is safe because the core RPC is
-- idempotent and closes the crash window during upgrade.
INSERT INTO runtime_session_cleanup_outbox (
    runtime_id, error_message, next_attempt_at, created_at, updated_at
)
SELECT runtime_id,
       format('runtime %s ended: %s; session cleanup pending', runtime_id, ended_reason),
       now(), now(), now()
FROM runtime_registry
WHERE status IN ('heartbeat_stale', 'ended', 'cancelled', 'failed')
ON CONFLICT (runtime_id) DO NOTHING;
