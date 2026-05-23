-- Hosted runtime admission is explicit-cancel-first: a user can have at most
-- one non-cancelled hosted runtime per service_name. Self-hosted runtimes are
-- selected by runtime_id / credential and may share the display service_name.

WITH ranked AS (
    SELECT
        runtime_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, service_name
            ORDER BY updated_at DESC, created_at DESC, runtime_id DESC
        ) AS rn
    FROM runtime_registry
    WHERE user_id IS NOT NULL
      AND source = 'hosted'
      AND status <> 'cancelled'
)
UPDATE runtime_registry
SET status = 'cancelled', updated_at = NOW()
WHERE runtime_id IN (
    SELECT runtime_id
    FROM ranked
    WHERE rn > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_active_hosted_slot
    ON runtime_registry (user_id, service_name)
    WHERE user_id IS NOT NULL
      AND source = 'hosted'
      AND status <> 'cancelled';
