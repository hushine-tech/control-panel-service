DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'session_market_data_subscriptions'
          AND column_name = 'mode'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'session_market_data_subscriptions'
          AND column_name = 'environment'
    ) THEN
        ALTER TABLE session_market_data_subscriptions
            RENAME COLUMN mode TO environment;
    END IF;
END $$;

ALTER INDEX IF EXISTS uq_session_market_data_subscriptions_active
    RENAME TO uq_session_market_data_subscriptions_active_environment;
