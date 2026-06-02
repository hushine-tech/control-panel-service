ALTER TABLE session_market_data_subscriptions
    RENAME COLUMN mode TO environment;

ALTER INDEX IF EXISTS uq_session_market_data_subscriptions_active
    RENAME TO uq_session_market_data_subscriptions_active_environment;
