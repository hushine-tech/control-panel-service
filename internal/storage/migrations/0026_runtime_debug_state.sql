-- Debugger runtime workspace metadata and active debug dataset metadata.
-- Full K-line data stays in the self-hosted runtime agent memory.
ALTER TABLE runtime_registry
    ADD COLUMN IF NOT EXISTS debug_workspace_host_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS debug_workspace_container_path TEXT NOT NULL DEFAULT '/workspace',
    ADD COLUMN IF NOT EXISTS debug_template_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS debug_archived_template_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS debug_vscode_launch_created BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS debug_vscode_launch_preserved BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS debug_pycharm_doc_created BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS debug_pycharm_doc_preserved BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS debug_workspace_prepared_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS debug_workspace_last_error TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS runtime_debug_datasets (
    dataset_id      TEXT PRIMARY KEY,
    user_id         BIGINT      NOT NULL,
    account_id      BIGINT      NOT NULL,
    runtime_id      TEXT        NOT NULL REFERENCES runtime_registry(runtime_id) ON DELETE CASCADE,
    market          TEXT        NOT NULL,
    symbol          TEXT        NOT NULL,
    interval        TEXT        NOT NULL,
    start_at        TIMESTAMPTZ NOT NULL,
    end_at          TIMESTAMPTZ NOT NULL,
    bar_count       BIGINT      NOT NULL,
    coverage_status TEXT        NOT NULL,
    state           TEXT        NOT NULL,
    last_error      TEXT        NOT NULL DEFAULT '',
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (market IN ('spot', 'futures')),
    CHECK (symbol <> ''),
    CHECK (interval <> ''),
    CHECK (bar_count >= 0),
    CHECK (coverage_status <> ''),
    CHECK (state IN ('active', 'lost', 'failed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_debug_datasets_active_runtime
    ON runtime_debug_datasets(runtime_id)
    WHERE state = 'active';

CREATE INDEX IF NOT EXISTS idx_runtime_debug_datasets_user_runtime_loaded
    ON runtime_debug_datasets(user_id, runtime_id, loaded_at DESC);
