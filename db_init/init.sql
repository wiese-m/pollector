CREATE
    DATABASE IF NOT EXISTS market_data;

CREATE TABLE market_data.trades
(
    ts_recv       DateTime64(9, 'UTC') CODEC (Delta, ZSTD(1)),
    exchange      LowCardinality(String) CODEC (ZSTD(1)),
    symbol        LowCardinality(String) CODEC (ZSTD(1)),
    price         Float64 CODEC (Delta, ZSTD(1)),
    qty           Float64 CODEC (ZSTD(1)),
    is_taker_sell Bool CODEC (T64, ZSTD(1)),
    id            String CODEC (ZSTD(1)),
    ts_execution  DateTime64(9, 'UTC') CODEC (Delta, ZSTD(1)),
    ts_event      DateTime64(9, 'UTC') CODEC (Delta, ZSTD(1))
) ENGINE = MergeTree()
      ORDER BY (ts_recv, exchange, symbol);
