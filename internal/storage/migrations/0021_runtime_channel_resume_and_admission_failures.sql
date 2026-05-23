-- RuntimeChannel resume + failed admission visibility.
--
-- runtime_channel_leases stores only token hashes. Raw resume tokens are sent
-- to the running runtime process over RuntimeChannel and are never exposed via
-- management APIs.

CREATE TABLE IF NOT EXISTS runtime_channel_leases (
    runtime_id        TEXT        PRIMARY KEY,
    user_id           BIGINT      NOT NULL,
    credential_key_id TEXT        NOT NULL,
    lease_hash        TEXT        NOT NULL,
    issued_at         TIMESTAMPTZ NOT NULL,
    expires_at        TIMESTAMPTZ NOT NULL,
    last_used_at      TIMESTAMPTZ,
    revoked_at        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (runtime_id <> ''),
    CHECK (credential_key_id <> ''),
    CHECK (lease_hash <> ''),
    CHECK (expires_at > issued_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_channel_leases_hash
    ON runtime_channel_leases (lease_hash)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_channel_leases_user
    ON runtime_channel_leases (user_id, expires_at DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_channel_leases_expiry
    ON runtime_channel_leases (expires_at)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS runtime_admission_failures (
    admission_failure_id BIGSERIAL   PRIMARY KEY,
    user_id              BIGINT      NOT NULL DEFAULT 0,
    credential_key_id     TEXT        NOT NULL DEFAULT '',
    requested_runtime_id  TEXT        NOT NULL DEFAULT '',
    requested_name        TEXT        NOT NULL DEFAULT '',
    source                TEXT        NOT NULL DEFAULT '',
    role                  TEXT        NOT NULL DEFAULT '',
    failure_code          TEXT        NOT NULL DEFAULT '',
    reason                TEXT        NOT NULL,
    consumed_runtime_id   TEXT        NOT NULL DEFAULT '',
    first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt_count         INTEGER     NOT NULL DEFAULT 1,
    CHECK (reason <> ''),
    CHECK (attempt_count > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_admission_failures_rollup
    ON runtime_admission_failures (
        user_id,
        credential_key_id,
        requested_runtime_id,
        requested_name,
        failure_code,
        consumed_runtime_id
    );

CREATE INDEX IF NOT EXISTS idx_runtime_admission_failures_user
    ON runtime_admission_failures (user_id, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_admission_failures_credential
    ON runtime_admission_failures (credential_key_id, last_seen_at DESC)
    WHERE credential_key_id <> '';
