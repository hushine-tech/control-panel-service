-- Phase D3: bind self-hosted runtime_registry rows to the Ed25519
-- credential that authenticated their RuntimeChannel HELLO. This lets
-- credential revocation cancel offline rows, not just currently connected
-- streams held in memory.
ALTER TABLE runtime_registry
    ADD COLUMN IF NOT EXISTS credential_key_id TEXT;

CREATE INDEX IF NOT EXISTS idx_runtime_registry_credential_key_id
    ON runtime_registry (credential_key_id)
    WHERE credential_key_id IS NOT NULL AND status <> 'cancelled';
