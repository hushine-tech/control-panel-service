-- runtime_credentials: Ed25519 keypairs issued to users for self-hosted
-- strategy-runtime registration. Phase D3 introduces this table; it
-- replaces the pairing-code mechanism scaffolded in D1
-- (`runtime_pairings`, removed in D3 task 7.1) for the only flow that
-- ever used pairing — self-hosted onboarding.
--
-- Threat model summary:
--   * Server-side Ed25519 keypair generation. Public key is persisted;
--     private key is returned to the browser exactly once for download
--     and is never stored on the platform.
--   * Each runtime container mounts the credential file and signs its
--     `RuntimeChannel` HELLO. The control-panel verifies the signature
--     against `public_key_pem` looked up by `key_id`.
--   * Revocation is immediate and irreversible (Decision 8): set
--     `status='revoked'`, close all streams keyed by `key_id`, mark
--     associated runtime_registry rows `cancelled`.
--
-- Cross-DB note: `user_id` is a logical reference to `account.users(id)`.
-- Same convention as Phase D2 market-data tables — Postgres does not
-- enforce FKs across databases. Validation lives in the credential
-- service layer (issue path checks the user exists via
-- `account-service.GetUser`).
CREATE TABLE IF NOT EXISTS runtime_credentials (
    -- key_id is the stable opaque identifier that the runtime sends in
    -- every signed HELLO. Generated server-side as base64url-encoded
    -- random bytes (16 bytes → 22 chars after b64url, no padding).
    key_id           TEXT        PRIMARY KEY,

    -- The user this credential belongs to. The HELLO signature, after
    -- it verifies, binds the resulting runtime_registry row to this
    -- user_id immediately (no separate pairing step).
    user_id          BIGINT      NOT NULL,           -- logical FK to account.users(id)

    -- Ed25519 public key in PEM. The private key is never persisted.
    public_key_pem   TEXT        NOT NULL,

    -- Optional human-friendly label set at issue time, shown in the UI
    -- list view. Empty string when not set. Not used for routing or
    -- auth — purely a memory aid for the user ("home VPS", "laptop").
    label            TEXT        NOT NULL DEFAULT '',

    -- Lifecycle: only 'active' or 'revoked'. Revocation is one-way —
    -- to "reactivate" a credential, the user generates a new one.
    status           TEXT        NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'revoked')),

    -- Audit trail. created_at is the moment the keypair was issued;
    -- revoked_at is set when status flips to 'revoked'; last_used_at
    -- is bumped on every successful HELLO verification (best-effort,
    -- not part of any auth invariant — UI-display metadata).
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at       TIMESTAMPTZ NULL,
    last_used_at     TIMESTAMPTZ NULL
);

-- List-by-user is the dominant query (UI "Settings → Runtime Credentials").
CREATE INDEX IF NOT EXISTS idx_runtime_credentials_user
    ON runtime_credentials (user_id, status)
    WHERE status = 'active';

-- Revocation sweep / audit query: find credentials a user revoked recently.
CREATE INDEX IF NOT EXISTS idx_runtime_credentials_revoked
    ON runtime_credentials (revoked_at)
    WHERE revoked_at IS NOT NULL;
