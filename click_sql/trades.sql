CREATE TABLE trades(
    date DateTime64,
    exchange FixedString(6),
    tradeId Int32,
    instance String,
    price Decimal64(4),
    size Decimal128(32),
    side String,
    type String
) ENGINE = MergeTree()
ORDER BY date;

ALTER TABLE trades MODIFY COLUMN date DateTime64(3, 'UTC');


truncate table trades;

select count(*) from trades;


