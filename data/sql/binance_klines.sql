CREATE TABLE nimbus.binance_klines (
    timestamp BIGINT,
    open VARCHAR(50),
    high VARCHAR(50),
    low VARCHAR(50),
    close VARCHAR(50),
    volume VARCHAR(50),
    close_time BIGINT,
    quote_volume VARCHAR(50),
    count BIGINT,
    taker_buy_volume VARCHAR(50),
    taker_buy_quote_volume VARCHAR(50),
    ignore VARCHAR(50),
    symbol VARCHAR(50)
);