-- A runtime credential is a runtime identity, not a reusable shared secret.
-- Keep only the newest non-cancelled row if older development databases
-- already contain duplicate credential bindings, then enforce one live
-- runtime_registry row per credential_key_id.
WITH ranked AS (
    SELECT
        runtime_id,
        ROW_NUMBER() OVER (
            PARTITION BY credential_key_id
            ORDER BY updated_at DESC, created_at DESC, runtime_id DESC
        ) AS rn
    FROM runtime_registry
    WHERE credential_key_id IS NOT NULL
      AND credential_key_id <> ''
      AND status <> 'cancelled'
)
UPDATE runtime_registry
SET status = 'cancelled', updated_at = NOW()
WHERE runtime_id IN (
    SELECT runtime_id
    FROM ranked
    WHERE rn > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_active_credential_key
    ON runtime_registry (credential_key_id)
    WHERE credential_key_id IS NOT NULL
      AND credential_key_id <> ''
      AND status <> 'cancelled';
