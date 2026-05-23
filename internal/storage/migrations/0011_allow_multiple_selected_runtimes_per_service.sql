-- Runtime selection is now runtime_id-based. service_name remains a label /
-- legacy lookup key, not a global routing slot. A hosted runtime and a
-- self-hosted runtime can both be named "default" and must coexist until the
-- user explicitly cancels one from Runtime Management.
DROP INDEX IF EXISTS uq_runtime_registry_user_service_name;

CREATE INDEX IF NOT EXISTS idx_runtime_registry_user_source_service
    ON runtime_registry (user_id, source, service_name)
    WHERE user_id IS NOT NULL AND status <> 'cancelled';
