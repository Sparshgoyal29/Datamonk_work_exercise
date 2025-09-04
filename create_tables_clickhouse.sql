CREATE DATABASE IF NOT EXISTS lab;

CREATE TABLE IF NOT EXISTS lab.table_with_codec
(
  id UInt64,
  ts DateTime,
  value Float64,
  tag String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (id)
SETTINGS index_granularity = 8192;
