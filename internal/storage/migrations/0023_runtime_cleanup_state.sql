-- Hosted runtime cleanup/deprovision state.
--
-- Platform-owned hosted containers are removed by control-panel. Self-hosted
-- containers are user-owned, so control-panel can close the RuntimeChannel but
-- cannot remove the user's local Docker container. These fields make that
-- ownership boundary visible to the API/UI and preserve Docker cleanup errors
-- for operator debugging.

ALTER TABLE runtime_registry
    ADD COLUMN IF NOT EXISTS cleanup_status TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS cleanup_reason TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS cleanup_at TIMESTAMPTZ;

ALTER TABLE runtime_registry
    DROP CONSTRAINT IF EXISTS ck_runtime_registry_cleanup_status;

ALTER TABLE runtime_registry
    ADD CONSTRAINT ck_runtime_registry_cleanup_status
        CHECK (
            cleanup_status = ''
            OR cleanup_status IN ('succeeded', 'failed', 'user_owned')
        );
