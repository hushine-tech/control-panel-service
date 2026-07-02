ALTER TABLE runtime_credentials
    ADD COLUMN IF NOT EXISTS client_cert_pem TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS client_cert_fingerprint TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS client_cert_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS issuer TEXT NOT NULL DEFAULT 'user';

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_client_cert_fingerprint
    ON runtime_credentials (client_cert_fingerprint)
    WHERE client_cert_fingerprint <> '';

ALTER TABLE runtime_credentials
    DROP CONSTRAINT IF EXISTS ck_runtime_credentials_issuer;

ALTER TABLE runtime_credentials
    ADD CONSTRAINT ck_runtime_credentials_issuer
    CHECK (issuer IN ('user', 'hosted_internal', 'bare_debug'));
