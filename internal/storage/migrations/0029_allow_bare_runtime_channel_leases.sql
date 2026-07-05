-- Bare debug runtimes do not use RuntimeChannel credentials, but they still
-- need resume/fingerprint leases after HELLO succeeds.
ALTER TABLE runtime_channel_leases
    DROP CONSTRAINT IF EXISTS runtime_channel_leases_credential_key_id_check;
