-- Runtime lifecycle status normalization for unified control-plane.
--
-- New writes use `starting` instead of legacy `paired`. Terminal statuses are
-- explicit so users can distinguish cancellation, failure, and heartbeat
-- timeout without overloading a single `ended` bucket.

ALTER TABLE runtime_registry
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_status,
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_lifecycle;

ALTER TABLE runtime_registry
    ADD CONSTRAINT ck_runtime_registry_status
        CHECK (status IN (
            'starting',
            'paired',
            'active',
            'unhealthy',
            'heartbeat_stale',
            'ended',
            'cancelled',
            'failed'
        )),
    ADD CONSTRAINT ck_runtime_registry_lifecycle
        CHECK (
            (
                status IN ('starting', 'paired', 'active', 'unhealthy')
                AND ended_at IS NULL
                AND ended_reason = ''
                AND (
                    status <> 'active'
                    OR started_at IS NOT NULL
                )
            )
            OR (
                status IN ('heartbeat_stale', 'ended', 'cancelled', 'failed')
                AND ended_at IS NOT NULL
                AND ended_reason <> ''
            )
        );

DROP INDEX IF EXISTS idx_runtime_registry_status_heartbeat;
DROP INDEX IF EXISTS idx_runtime_registry_credential_key_id;
DROP INDEX IF EXISTS idx_runtime_registry_connection_owner;
DROP INDEX IF EXISTS uq_runtime_registry_active_credential_key;

CREATE INDEX IF NOT EXISTS idx_runtime_registry_status_heartbeat
    ON runtime_registry (status, heartbeat_at);

CREATE INDEX IF NOT EXISTS idx_runtime_registry_credential_key_id
    ON runtime_registry (credential_key_id)
    WHERE credential_key_id IS NOT NULL
      AND status NOT IN ('heartbeat_stale', 'ended', 'cancelled', 'failed');

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_active_credential_key
    ON runtime_registry (credential_key_id)
    WHERE credential_key_id IS NOT NULL
      AND status NOT IN ('heartbeat_stale', 'ended', 'cancelled', 'failed');

CREATE INDEX IF NOT EXISTS idx_runtime_registry_connection_owner
    ON runtime_registry (connection_owner_instance_id)
    WHERE connection_owner_instance_id <> ''
      AND status NOT IN ('heartbeat_stale', 'ended', 'cancelled', 'failed');
