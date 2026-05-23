-- Runtime identity unification baseline.
--
-- Role is credential-derived in later service code. Existing runtime rows are
-- executor runtimes unless a future credential migration rewrites them.
--
-- Connection owner fields are nullable/empty for disconnected runtimes. They
-- let command and data dispatch later target the control-panel instance that
-- currently owns the RuntimeChannel.

ALTER TABLE runtime_registry
    ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'executor',
    ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS connection_owner_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS connection_owner_acquired_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS connection_owner_heartbeat_at TIMESTAMPTZ;

UPDATE runtime_registry
SET last_heartbeat_at = heartbeat_at
WHERE last_heartbeat_at IS NULL
  AND heartbeat_at IS NOT NULL;

ALTER TABLE runtime_registry
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_source,
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_role,
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_connection_owner;

ALTER TABLE runtime_registry
    ADD CONSTRAINT ck_runtime_registry_source
        CHECK (source IN ('hosted', 'self_hosted')),
    ADD CONSTRAINT ck_runtime_registry_role
        CHECK (role IN ('executor', 'debugger')),
    ADD CONSTRAINT ck_runtime_registry_connection_owner
        CHECK (
            (
                connection_owner_instance_id = ''
                AND connection_owner_acquired_at IS NULL
                AND connection_owner_heartbeat_at IS NULL
            )
            OR (
                connection_owner_instance_id <> ''
                AND connection_owner_acquired_at IS NOT NULL
                AND connection_owner_heartbeat_at IS NOT NULL
            )
        );

CREATE INDEX IF NOT EXISTS idx_runtime_registry_connection_owner
    ON runtime_registry (connection_owner_instance_id)
    WHERE connection_owner_instance_id <> ''
      AND status <> 'ended';
