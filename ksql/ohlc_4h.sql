CREATE TABLE ohlc_4h AS
SELECT
    instId,
    WINDOWSTART() AS ws,
    MAX(px) AS high,
    MIN(px) AS low,
    FIRST_BY_OFFSET(px) AS open,
    LAST_BY_OFFSET(px) AS close,
    SUM(sz) AS volume
FROM trades_stream
WINDOW HOPPING (SIZE 240 MINUTES, ADVANCE BY 10 SECONDS)
GROUP BY instId
EMIT CHANGES;