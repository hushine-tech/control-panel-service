-- runtime_registry: every strategy-runtime instance the control plane knows about.
-- D1 ownership boundary: this table lives in the control-panel-service database;
-- account-service owns users/accounts; control-panel-service owns runtime and
-- market-data control-plane state.
--
-- Lifecycle:
--   paired -> active <-> unhealthy -> cancelled
-- Hosted runtimes provisioned by the platform are created directly with
-- user_id set and start as 'paired'. D3 self-hosted runtimes are admitted by a
-- signed RuntimeChannel HELLO and are written as active rows bound to
-- credential_key_id (added in 0008). 'active' means the runtime has a fresh
-- heartbeat/channel; 'unhealthy' means heartbeat has aged past the configured
-- deadline.
CREATE TABLE IF NOT EXISTS runtime_registry (
    runtime_id        TEXT PRIMARY KEY,
    user_id           BIGINT,                            -- NULL until paired
    service_name      TEXT NOT NULL,                     -- user-visible label, e.g. 'default'
    source            TEXT NOT NULL,                     -- 'hosted' | 'self_hosted'
    endpoint_host     TEXT NOT NULL,
    grpc_port         INT  NOT NULL,
    debug_port        INT,                               -- optional IDE debug port
    capabilities      JSONB NOT NULL DEFAULT '[]'::jsonb,
    resource_profile  TEXT NOT NULL,                     -- 'small' | 'medium' | 'large' | etc.
    version           TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL,                     -- 'unpaired'|'paired'|'active'|'unhealthy'|'cancelled'
    token_hash        TEXT NOT NULL DEFAULT '',          -- hash of current registration/session token
    paired_at         TIMESTAMPTZ,
    heartbeat_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Lookup by user — primary route-resolution path.
CREATE INDEX IF NOT EXISTS idx_runtime_registry_user_service
    ON runtime_registry (user_id, service_name)
    WHERE user_id IS NOT NULL AND status <> 'cancelled';

-- Heartbeat sweep / health check.
CREATE INDEX IF NOT EXISTS idx_runtime_registry_status_heartbeat
    ON runtime_registry (status, heartbeat_at);

-- Per-user uniqueness on service_name once the runtime is bound to a user
-- and not cancelled. Cancelling frees the service_name slot.
CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_user_service_name
    ON runtime_registry (user_id, service_name)
    WHERE user_id IS NOT NULL AND status <> 'cancelled';
