CREATE STREAM trades_stream (
    ts BIGINT,
    exchange VARCHAR,
    instId VARCHAR,
    px DOUBLE,
    sz DOUBLE,
    side VARCHAR
) WITH (
    KAFKA_TOPIC='BTC-USDT-trades',
    VALUE_FORMAT='JSON'
);