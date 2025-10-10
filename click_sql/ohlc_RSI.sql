
drop view ETH_USDT_OHLC_1M;

CREATE VIEW ETH_USDT_OHLC_1M AS
WITH ohlc_data AS (
    SELECT
        toStartOfInterval(date, toIntervalMinute(1)) AS time_interval_start,
        argMin(price, date) AS open,
        max(price) AS high,
        min(price) AS low,
        argMax(price, date) AS close,
        sum(size) AS volume,
        sumIf(size, side = 'buy') AS total_buy,
        sumIf(size, side = 'sell') AS total_sell,
        round(((toFloat64(sumIf(size, side = 'buy')) - toFloat64(sumIf(size, side = 'sell')))
             / toFloat64(sum(size))) * 100, 2) AS buy_vs_sell_percent
    FROM trades
    WHERE instance = 'ETH-USDT'
    GROUP BY time_interval_start
),
ohlc_with_prev AS (
    SELECT
        *,
        any(close) OVER (ORDER BY time_interval_start ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_close
    FROM ohlc_data
),
price_changes AS (
    SELECT
        *,
        close - prev_close AS price_change
    FROM ohlc_with_prev
),
gains_losses AS (
    SELECT
        *,
        if(price_change > 0, price_change, 0) AS gain,
        if(price_change < 0, abs(price_change), 0) AS loss
    FROM price_changes
),
rsi_data AS (
    SELECT
        *,
        -- Скользящие средние для gains и losses
        avg(gain) OVER (ORDER BY time_interval_start ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        avg(loss) OVER (ORDER BY time_interval_start ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
)
SELECT
    time_interval_start,
    open,
    high,
    low,
    close,
    volume,
    total_buy,
    total_sell,
    buy_vs_sell_percent,
    -- Final RSI calculation
    CASE 
        WHEN avg_loss = 0 THEN 100
        ELSE round(100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))), 2)
    END AS rsi_14
FROM rsi_data
ORDER BY time_interval_start ASC;


select * from ETH_USDT_OHLC_1M
where time_interval_start > '2025-10-09 13:00:00';