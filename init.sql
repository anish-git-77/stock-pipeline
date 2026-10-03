CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS stock_prices (
    ts          TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    volume      BIGINT,
    pct_change  DOUBLE PRECISION,
    moving_avg  DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
);
SELECT create_hypertable('stock_prices', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS alerts (
    ts          TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    pct_change  DOUBLE PRECISION NOT NULL,
    message     TEXT,
    PRIMARY KEY (symbol, ts)
);
