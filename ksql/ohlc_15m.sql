CREATE TABLE ohlc_15m AS
SELECT
    instId,
    WINDOWSTART() AS ws,
    MAX(px) AS high,
    MIN(px) AS low,
    FIRST_BY_OFFSET(px) AS open,
    LAST_BY_OFFSET(px) AS close,
    SUM(sz) AS volume
FROM trades_stream
WINDOW HOPPING (SIZE 15 MINUTES, ADVANCE BY 1 SECONDS)
GROUP BY instId
EMIT CHANGES;