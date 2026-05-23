-- Runtime identity lifecycle:
--   paired -> active <-> unhealthy -> ended
--
-- This migration is intentionally data-preserving. Existing cancelled rows are
-- retained as ended rows with an explicit terminal timestamp/reason.

DROP INDEX IF EXISTS uq_runtime_registry_user_service_name;
DROP INDEX IF EXISTS idx_runtime_registry_user_service;
DROP INDEX IF EXISTS idx_runtime_registry_user_source_service;
DROP INDEX IF EXISTS uq_runtime_registry_active_hosted_slot;
DROP INDEX IF EXISTS uq_runtime_registry_active_credential_key;
DROP INDEX IF EXISTS idx_runtime_registry_credential_key_id;
DROP INDEX IF EXISTS idx_runtime_registry_status_heartbeat;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'runtime_registry'
          AND column_name = 'service_name'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'runtime_registry'
          AND column_name = 'name'
    ) THEN
        ALTER TABLE runtime_registry RENAME COLUMN service_name TO name;
    END IF;
END $$;

ALTER TABLE runtime_registry
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS ended_reason TEXT NOT NULL DEFAULT '';

UPDATE runtime_registry
SET status = 'ended',
    ended_at = COALESCE(ended_at, updated_at, NOW()),
    ended_reason = CASE
        WHEN ended_reason = '' THEN 'user_cancelled'
        ELSE ended_reason
    END,
    updated_at = NOW()
WHERE status = 'cancelled';

UPDATE runtime_registry
SET status = 'paired',
    updated_at = NOW()
WHERE status = 'unpaired';

UPDATE runtime_registry
SET started_at = COALESCE(started_at, heartbeat_at, paired_at, updated_at, created_at, NOW()),
    updated_at = NOW()
WHERE status = 'active'
  AND started_at IS NULL;

DO $$
DECLARE
    rec RECORD;
    base_name TEXT;
    source_name TEXT;
    rn_suffix TEXT;
    runtime_suffix TEXT;
    suffix TEXT;
    candidate TEXT;
    counter INT;
BEGIN
    FOR rec IN
        WITH ranked AS (
            SELECT
                runtime_id,
                user_id,
                name,
                source,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, name
                    ORDER BY updated_at DESC, created_at DESC, runtime_id DESC
                ) AS rn
            FROM runtime_registry
            WHERE user_id IS NOT NULL
        )
        SELECT *
        FROM ranked
        WHERE rn > 1
        ORDER BY user_id, name, rn
    LOOP
        base_name := REGEXP_REPLACE(rec.name, '[^a-zA-Z0-9._-]', '-', 'g');
        base_name := REGEXP_REPLACE(base_name, '^[^a-zA-Z0-9]+', '');
        base_name := REGEXP_REPLACE(base_name, '-+$', '');
        IF base_name = '' THEN
            base_name := 'runtime';
        END IF;

        source_name := LEFT(REGEXP_REPLACE(rec.source, '[^a-zA-Z0-9._-]', '-', 'g'), 12);
        IF source_name = '' THEN
            source_name := 'runtime';
        END IF;

        rn_suffix := RIGHT(rec.rn::TEXT, 6);
        runtime_suffix := LEFT(REGEXP_REPLACE(rec.runtime_id, '[^a-zA-Z0-9]', '', 'g'), 8);
        IF runtime_suffix = '' THEN
            runtime_suffix := 'runtime';
        END IF;

        suffix := '-' || source_name || '-' || rn_suffix || '-' || runtime_suffix;
        candidate := LEFT(base_name, GREATEST(1, 48 - LENGTH(suffix))) || suffix;
        counter := 2;

        WHILE EXISTS (
            SELECT 1
            FROM runtime_registry
            WHERE user_id = rec.user_id
              AND name = candidate
              AND runtime_id <> rec.runtime_id
        ) LOOP
            suffix := '-' || source_name || '-' || rn_suffix || '-' || runtime_suffix || '-' || counter::TEXT;
            candidate := LEFT(base_name, GREATEST(1, 48 - LENGTH(suffix))) || suffix;
            counter := counter + 1;
        END LOOP;

        UPDATE runtime_registry
        SET name = candidate,
            updated_at = NOW()
        WHERE runtime_id = rec.runtime_id;
    END LOOP;
END $$;

ALTER TABLE runtime_registry
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_status,
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_name,
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_lifecycle,
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_ended_reason;

ALTER TABLE runtime_registry
    ADD CONSTRAINT ck_runtime_registry_status
        CHECK (status IN ('paired', 'active', 'unhealthy', 'ended')),
    ADD CONSTRAINT ck_runtime_registry_name
        CHECK (name ~ '^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$'),
    ADD CONSTRAINT ck_runtime_registry_ended_reason
        CHECK (
            ended_reason = ''
            OR ended_reason IN (
                'user_cancelled',
                'runtime_exited',
                'heartbeat_stale',
                'provision_failed',
                'auth_failed',
                'control_panel_shutdown'
            )
        ),
    ADD CONSTRAINT ck_runtime_registry_lifecycle
        CHECK (
            (
                status <> 'ended'
                AND ended_at IS NULL
                AND ended_reason = ''
                AND (
                    status <> 'active'
                    OR started_at IS NOT NULL
                )
            )
            OR (
                status = 'ended'
                AND ended_at IS NOT NULL
                AND ended_reason <> ''
            )
        );

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_user_name
    ON runtime_registry (user_id, name)
    WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_registry_user_runtime
    ON runtime_registry (user_id, runtime_id)
    WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_registry_status_heartbeat
    ON runtime_registry (status, heartbeat_at);

CREATE INDEX IF NOT EXISTS idx_runtime_registry_credential_key_id
    ON runtime_registry (credential_key_id)
    WHERE credential_key_id IS NOT NULL
      AND credential_key_id <> ''
      AND status <> 'ended';

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_active_credential_key
    ON runtime_registry (credential_key_id)
    WHERE credential_key_id IS NOT NULL
      AND credential_key_id <> ''
      AND status <> 'ended';
