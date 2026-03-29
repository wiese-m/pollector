CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE market_data.trades
(
    timestamp DateTime64(9, 'UTC'),
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    price Float64,
    qty Float64,
    is_taker_sell Bool,
    id String
)
ENGINE = MergeTree()
ORDER BY (exchange, symbol, timestamp);
