-- runtime_pairings: pairing-code records used to bind an unpaired self-hosted
-- runtime to an authenticated user.
--
-- Spec invariants (from runtime-control-plane spec):
--   * pairing codes MUST be hashed at rest (we store code_hash, never plaintext)
--   * pairing codes MUST expire (expires_at)
--   * pairing codes MUST be attempt-limited (attempts vs max_attempts)
--   * after successful pairing, the runtime is bound to the user; the pairing
--     row is marked consumed and may not be reused.
CREATE TABLE IF NOT EXISTS runtime_pairings (
    id            BIGSERIAL PRIMARY KEY,
    runtime_id    TEXT NOT NULL REFERENCES runtime_registry(runtime_id) ON DELETE CASCADE,
    code_hash     TEXT NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    attempts      INT NOT NULL DEFAULT 0,
    max_attempts  INT NOT NULL DEFAULT 5,
    consumed_at   TIMESTAMPTZ,
    consumed_by   BIGINT,                                  -- user_id that consumed the code, if any
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- At most one unconsumed pairing per runtime. Re-issuing a code requires the
-- caller to delete the previous unconsumed row first (audit-by-log; the
-- consumed history stays as long as it is actually consumed).
CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_pairings_runtime_active
    ON runtime_pairings (runtime_id)
    WHERE consumed_at IS NULL;

-- PairRuntime hashes the user-supplied code and looks the row up by
-- code_hash; uniqueness over the unconsumed set also guards against the
-- vanishingly small but non-zero hash collision over a re-issue.
CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_pairings_code_hash_active
    ON runtime_pairings (code_hash)
    WHERE consumed_at IS NULL;

-- Sweep expired/unused codes.
CREATE INDEX IF NOT EXISTS idx_runtime_pairings_expires
    ON runtime_pairings (expires_at)
    WHERE consumed_at IS NULL;
