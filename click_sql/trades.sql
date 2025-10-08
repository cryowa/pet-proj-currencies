CREATE TABLE trades(
    date DateTime64,
    exchange FixedString(6),
    tradeId Int32,
    instance String,
    price Decimal64(4),
    size Decimal64(16),
    side String ,
    type String
) ENGINE = MergeTree()
ORDER BY date;
