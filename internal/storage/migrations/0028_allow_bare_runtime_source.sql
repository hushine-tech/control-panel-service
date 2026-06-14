-- Allow debug-only bare runtimes to connect through RuntimeChannel.

ALTER TABLE runtime_registry
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_source;

ALTER TABLE runtime_registry
    ADD CONSTRAINT ck_runtime_registry_source
        CHECK (source IN ('hosted', 'self_hosted', 'bare'));
