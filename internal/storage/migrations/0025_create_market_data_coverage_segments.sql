CREATE TABLE IF NOT EXISTS market_data_coverage_segments (
    segment_id          BIGSERIAL PRIMARY KEY,
    exchange            TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    kind                TEXT        NOT NULL,
    symbol              TEXT        NOT NULL,
    interval            TEXT        NOT NULL,
    year                INT         NOT NULL,
    segment_start_at    TIMESTAMPTZ NOT NULL,
    segment_end_at      TIMESTAMPTZ NOT NULL,
    row_count           BIGINT      NOT NULL,
    source              TEXT        NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (exchange IN ('binance', 'okx')),
    CHECK (market IN ('spot', 'futures')),
    CHECK (kind IN ('kline')),
    CHECK (year >= 1970),
    CHECK (symbol <> ''),
    CHECK (interval <> ''),
    CHECK (source <> ''),
    CHECK (segment_end_at > segment_start_at),
    CHECK (row_count > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_data_coverage_segments_exact
    ON market_data_coverage_segments (
        exchange, market, kind, symbol, interval, year, segment_start_at, segment_end_at
    );

CREATE INDEX IF NOT EXISTS idx_market_data_coverage_segments_start
    ON market_data_coverage_segments (
        exchange, market, kind, symbol, interval, year, segment_start_at
    );

CREATE INDEX IF NOT EXISTS idx_market_data_coverage_segments_end
    ON market_data_coverage_segments (
        exchange, market, kind, symbol, interval, year, segment_end_at
    );
