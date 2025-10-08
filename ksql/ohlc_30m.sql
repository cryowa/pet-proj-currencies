CREATE TABLE ohlc_30m AS
SELECT
    instId,
    WINDOWSTART() AS ws,
    MAX(px) AS high,
    MIN(px) AS low,
    FIRST_BY_OFFSET(px) AS open,
    LAST_BY_OFFSET(px) AS close,
    SUM(sz) AS volume
FROM trades_stream
WINDOW HOPPING (SIZE 30 MINUTES, ADVANCE BY 1 SECONDS)
GROUP BY instId
EMIT CHANGES;