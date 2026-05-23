-- Runtime credential lifecycle unification.
--
-- Existing credentials were self-hosted executor credentials. This migration
-- keeps that interpretation while adding the fields needed for one-time
-- consumption, debugger credentials, expiry, and hosted-internal credentials.

ALTER TABLE runtime_credentials
    ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'executor',
    ADD COLUMN IF NOT EXISTS downloaded_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS consumed_runtime_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS hosted_internal BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE runtime_credentials
    DROP CONSTRAINT IF EXISTS runtime_credentials_status_check,
    DROP CONSTRAINT IF EXISTS ck_runtime_credentials_status,
    DROP CONSTRAINT IF EXISTS ck_runtime_credentials_role,
    DROP CONSTRAINT IF EXISTS ck_runtime_credentials_lifecycle;

ALTER TABLE runtime_credentials
    ADD CONSTRAINT ck_runtime_credentials_status
        CHECK (status IN ('active', 'downloaded', 'consumed', 'revoked', 'expired')),
    ADD CONSTRAINT ck_runtime_credentials_role
        CHECK (role IN ('executor', 'debugger')),
    ADD CONSTRAINT ck_runtime_credentials_lifecycle
        CHECK (
            (
                status = 'active'
                AND downloaded_at IS NULL
                AND consumed_at IS NULL
                AND consumed_runtime_id = ''
            )
            OR (
                status = 'downloaded'
                AND downloaded_at IS NOT NULL
                AND consumed_at IS NULL
                AND consumed_runtime_id = ''
            )
            OR (
                status = 'consumed'
                AND consumed_at IS NOT NULL
                AND consumed_runtime_id <> ''
            )
            OR (
                status = 'revoked'
                AND revoked_at IS NOT NULL
                AND (
                    (
                        consumed_at IS NULL
                        AND consumed_runtime_id = ''
                    )
                    OR (
                        consumed_at IS NOT NULL
                        AND consumed_runtime_id <> ''
                    )
                )
            )
            OR (
                status = 'expired'
                AND expires_at IS NOT NULL
                AND consumed_at IS NULL
                AND consumed_runtime_id = ''
            )
        );

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_user_status
    ON runtime_credentials (user_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_expiry
    ON runtime_credentials (expires_at)
    WHERE expires_at IS NOT NULL
      AND status IN ('active', 'downloaded');

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_credentials_consumed_runtime
    ON runtime_credentials (consumed_runtime_id)
    WHERE consumed_runtime_id <> '';
