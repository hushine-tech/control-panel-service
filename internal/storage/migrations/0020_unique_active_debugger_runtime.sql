-- A user may have at most one non-terminal debugger runtime. Ended/cancelled
-- debugger runtimes remain in history but free the debugger slot.

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_one_active_debugger_per_user
    ON runtime_registry (user_id)
    WHERE role = 'debugger'
      AND status NOT IN ('ended', 'cancelled', 'failed', 'heartbeat_stale');
