CREATE VIEW ETH_USDT_RSI_ANALYSIS AS
WITH base_data AS (
    SELECT
        time_interval_start,
        close,
        rsi_14,
        CASE 
            WHEN rsi_14 > 70 THEN 'overbought'
            WHEN rsi_14 < 30 THEN 'oversold'
            ELSE 'neutral'
        END AS signal_type,
        high,
        low,
        volume,
        buy_vs_sell_percent,
        rowNumberInAllBlocks() as row_num
    FROM ETH_USDT_OHLC_1M
    WHERE rsi_14 IS NOT NULL
),
future_data AS (
    SELECT
        *,
        -- Эмуляция lead с помощью any() OVER
        any(close) OVER (ORDER BY row_num ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) as next_1_close,
        any(close) OVER (ORDER BY row_num ASC ROWS BETWEEN 3 FOLLOWING AND 3 FOLLOWING) as next_3_close,
        any(close) OVER (ORDER BY row_num ASC ROWS BETWEEN 5 FOLLOWING AND 5 FOLLOWING) as next_5_close,
        any(close) OVER (ORDER BY row_num ASC ROWS BETWEEN 10 FOLLOWING AND 10 FOLLOWING) as next_10_close,
        any(rsi_14) OVER (ORDER BY row_num ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) as next_1_rsi,
        any(rsi_14) OVER (ORDER BY row_num ASC ROWS BETWEEN 3 FOLLOWING AND 3 FOLLOWING) as next_3_rsi,
        any(rsi_14) OVER (ORDER BY row_num ASC ROWS BETWEEN 5 FOLLOWING AND 5 FOLLOWING) as next_5_rsi,
        any(rsi_14) OVER (ORDER BY row_num ASC ROWS BETWEEN 10 FOLLOWING AND 10 FOLLOWING) as next_10_rsi
    FROM base_data
)
SELECT
    time_interval_start as signal_time,
    close as signal_price,
    rsi_14 as signal_rsi,
    signal_type,
    high,
    low,
    volume,
    buy_vs_sell_percent,
    
    -- Будущие цены
    next_1_close,
    next_3_close,
    next_5_close,
    next_10_close,
    
    -- Процентные изменения цены
    round(((next_1_close - close) / close) * 100, 2) as change_1_period_pct,
    round(((next_3_close - close) / close) * 100, 2) as change_3_periods_pct,
    round(((next_5_close - close) / close) * 100, 2) as change_5_periods_pct,
    round(((next_10_close - close) / close) * 100, 2) as change_10_periods_pct,
    
    -- Будущие RSI
    next_1_rsi,
    next_3_rsi,
    next_5_rsi,
    next_10_rsi,
    
    -- Изменения RSI
    round(next_1_rsi - rsi_14, 2) as rsi_change_1_period,
    round(next_5_rsi - rsi_14, 2) as rsi_change_5_periods,
    
    -- Успешность сигналов
    CASE 
        WHEN signal_type = 'overbought' AND next_1_close < close THEN 'success'
        WHEN signal_type = 'oversold' AND next_1_close > close THEN 'success'
        WHEN next_1_close IS NULL THEN 'unknown'
        ELSE 'failure'
    END as success_1_period,
    
    CASE 
        WHEN signal_type = 'overbought' AND next_3_close < close THEN 'success'
        WHEN signal_type = 'oversold' AND next_3_close > close THEN 'success'
        WHEN next_3_close IS NULL THEN 'unknown'
        ELSE 'failure'
    END as success_3_periods,
    
    CASE 
        WHEN signal_type = 'overbought' AND next_5_close < close THEN 'success'
        WHEN signal_type = 'oversold' AND next_5_close > close THEN 'success'
        WHEN next_5_close IS NULL THEN 'unknown'
        ELSE 'failure'
    END as success_5_periods,
    
    -- Время до возврата к RSI 50
    CASE 
        WHEN signal_type = 'overbought' AND next_1_rsi <= 50 THEN '1_period'
        WHEN signal_type = 'overbought' AND next_3_rsi <= 50 THEN '3_periods'
        WHEN signal_type = 'overbought' AND next_5_rsi <= 50 THEN '5_periods'
        WHEN signal_type = 'overbought' AND next_10_rsi <= 50 THEN '10_periods'
        WHEN signal_type = 'oversold' AND next_1_rsi >= 50 THEN '1_period'
        WHEN signal_type = 'oversold' AND next_3_rsi >= 50 THEN '3_periods'
        WHEN signal_type = 'oversold' AND next_5_rsi >= 50 THEN '5_periods'
        WHEN signal_type = 'oversold' AND next_10_rsi >= 50 THEN '10_periods'
        ELSE 'more_than_10'
    END as time_to_revert_60_40

FROM future_data
WHERE signal_type IN ('overbought', 'oversold')
ORDER BY time_interval_start;

drop view ETH_USDT_RSI_ANALYSIS;

select * from ETH_USDT_RSI_ANALYSIS
where signal_time > '2025-10-09 13:00:00';