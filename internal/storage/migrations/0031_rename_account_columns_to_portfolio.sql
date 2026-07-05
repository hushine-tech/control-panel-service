-- Account -> portfolio hard cut compatibility.
--
-- Existing local control_panel databases may already have 0004/0005/0006/0026
-- recorded in schema_migrations from before the rename. Editing those old
-- migration files updates fresh installs, but it does not change applied
-- schemas, so the runtime code can see "column portfolio_id does not exist".
-- Rename the historical columns in-place while staying harmless on fresh DBs.

DO $$
BEGIN
    IF to_regclass('market_data_requests') IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'market_data_requests'
             AND column_name = 'account_id'
       )
       AND NOT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'market_data_requests'
             AND column_name = 'portfolio_id'
       ) THEN
        ALTER TABLE market_data_requests
            RENAME COLUMN account_id TO portfolio_id;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('market_data_leases') IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'market_data_leases'
             AND column_name = 'account_id'
       )
       AND NOT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'market_data_leases'
             AND column_name = 'portfolio_id'
       ) THEN
        ALTER TABLE market_data_leases
            RENAME COLUMN account_id TO portfolio_id;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('market_data_history_requests') IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'market_data_history_requests'
             AND column_name = 'account_id'
       )
       AND NOT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'market_data_history_requests'
             AND column_name = 'portfolio_id'
       ) THEN
        ALTER TABLE market_data_history_requests
            RENAME COLUMN account_id TO portfolio_id;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('runtime_debug_datasets') IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'runtime_debug_datasets'
             AND column_name = 'account_id'
       )
       AND NOT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'runtime_debug_datasets'
             AND column_name = 'portfolio_id'
       ) THEN
        ALTER TABLE runtime_debug_datasets
            RENAME COLUMN account_id TO portfolio_id;
    END IF;
END $$;
