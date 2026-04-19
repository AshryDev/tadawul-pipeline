{{
    config(
        materialized         = 'incremental',
        unique_key           = ['symbol', 'date'],
        on_schema_change     = 'sync_all_columns',
        incremental_strategy = 'delete+insert',
        schema               = 'gold',
    )
}}

/*
  gold_technical_rating
  ──────────────────────
  TradingView-style technical rating per symbol per day.

  Eight indicator signals each vote +1 (buy), 0 (neutral), or -1 (sell).
  The net score determines the label:

    Signal                    Buy condition           Sell condition
    ──────────────────────    ────────────────────    ──────────────────────
    1. Price vs SMA10         close > sma10           close < sma10
    2. Price vs SMA20         close > sma20           close < sma20
    3. Price vs SMA50         close > sma50           close < sma50
    4. Price vs SMA200        close > sma200          close < sma200
    5. SMA10 × SMA20 cross    sma10 > sma20           sma10 < sma20
    6. RSI(14)                rsi < 30 (oversold)     rsi > 70 (overbought)
    7. Bollinger Band pos.    close < bb_lower        close > bb_upper
    8. MACD proxy (12/26)     sma12 > sma26           sma12 < sma26

  Score → rating mapping:
     5 to  8 → Strong Buy
     3 to  4 → Buy
    -2 to  2 → Neutral
    -4 to -3 → Sell
    -8 to -5 → Strong Sell
*/

WITH
base AS (
    SELECT
        o.symbol,
        o.date,
        o.close,
        o.high,
        o.low,
        o.volume,
        s.company_name,
        s.sector,
        s.market_cap_tier
    FROM {{ ref('silver_ohlcv') }}   AS o
    JOIN {{ ref('silver_symbols') }} AS s ON o.symbol = s.symbol

    {% if is_incremental() %}
    -- Pull 210 days before the incremental cutoff so SMA200 has sufficient history.
    WHERE o.date > (
        SELECT DATE_ADD('day', -210, MAX(date)) FROM {{ this }}
    )
    {% endif %}
),

-- Step 1: lag close to compute daily gain/loss for RSI
with_lag AS (
    SELECT
        *,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM base
),

with_changes AS (
    SELECT
        *,
        CASE WHEN close > prev_close THEN close - prev_close ELSE 0.0 END AS gain,
        CASE WHEN close < prev_close THEN prev_close - close ELSE 0.0 END AS loss
    FROM with_lag
),

-- Step 2: compute moving averages, Bollinger components, RSI components
with_ma AS (
    SELECT
        symbol,
        date,
        company_name,
        sector,
        market_cap_tier,
        close,
        volume,

        AVG(close) OVER w10  AS sma10,
        AVG(close) OVER w20  AS sma20,
        AVG(close) OVER w50  AS sma50,
        AVG(close) OVER w200 AS sma200,
        AVG(close) OVER w12  AS sma12,
        AVG(close) OVER w26  AS sma26,

        STDDEV(close) OVER w20       AS stddev20,

        AVG(gain) OVER w14           AS avg_gain_14,
        AVG(loss) OVER w14           AS avg_loss_14,

        AVG(volume) OVER w20         AS avg_volume_20d

    FROM with_changes

    WINDOW
        w10  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9   PRECEDING AND CURRENT ROW),
        w12  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 11  PRECEDING AND CURRENT ROW),
        w14  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13  PRECEDING AND CURRENT ROW),
        w20  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19  PRECEDING AND CURRENT ROW),
        w26  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 25  PRECEDING AND CURRENT ROW),
        w50  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 49  PRECEDING AND CURRENT ROW),
        w200 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
),

-- Step 3: derive composite indicators from the moving averages
with_indicators AS (
    SELECT
        *,
        sma20 - 2 * stddev20  AS bb_lower,
        sma20 + 2 * stddev20  AS bb_upper,
        CASE
            WHEN avg_loss_14 = 0 OR avg_loss_14 IS NULL THEN 100.0
            ELSE ROUND(100.0 - (100.0 / (1.0 + avg_gain_14 / NULLIF(avg_loss_14, 0))), 2)
        END AS rsi14
    FROM with_ma
),

-- Step 4: each indicator votes +1 buy / 0 neutral / -1 sell
signals AS (
    SELECT
        symbol,
        date,
        company_name,
        sector,
        market_cap_tier,
        close,
        volume,
        avg_volume_20d,

        ROUND(sma10,   2) AS sma10,
        ROUND(sma20,   2) AS sma20,
        ROUND(sma50,   2) AS sma50,
        ROUND(sma200,  2) AS sma200,
        ROUND(bb_lower,2) AS bb_lower,
        ROUND(bb_upper,2) AS bb_upper,
        ROUND(rsi14,   2) AS rsi14,

        -- 1. price vs sma10
        CASE WHEN close > sma10  THEN  1
             WHEN close < sma10  THEN -1
             ELSE 0 END AS sig_price_sma10,

        -- 2. price vs sma20
        CASE WHEN close > sma20  THEN  1
             WHEN close < sma20  THEN -1
             ELSE 0 END AS sig_price_sma20,

        -- 3. price vs sma50
        CASE WHEN close > sma50  THEN  1
             WHEN close < sma50  THEN -1
             ELSE 0 END AS sig_price_sma50,

        -- 4. price vs sma200
        CASE WHEN close > sma200 THEN  1
             WHEN close < sma200 THEN -1
             ELSE 0 END AS sig_price_sma200,

        -- 5. sma10 / sma20 golden-cross
        CASE WHEN sma10 > sma20  THEN  1
             WHEN sma10 < sma20  THEN -1
             ELSE 0 END AS sig_ma_cross,

        -- 6. RSI(14): oversold = buy, overbought = sell
        CASE WHEN rsi14 < 30     THEN  1
             WHEN rsi14 > 70     THEN -1
             ELSE 0 END AS sig_rsi,

        -- 7. Bollinger Band position
        CASE WHEN close < bb_lower THEN  1
             WHEN close > bb_upper THEN -1
             ELSE 0 END AS sig_bb,

        -- 8. MACD proxy: SMA12 vs SMA26
        CASE WHEN sma12 > sma26  THEN  1
             WHEN sma12 < sma26  THEN -1
             ELSE 0 END AS sig_macd_proxy

    FROM with_indicators
),

scored AS (
    SELECT
        *,
        sig_price_sma10 + sig_price_sma20 + sig_price_sma50 + sig_price_sma200
            + sig_ma_cross + sig_rsi + sig_bb + sig_macd_proxy AS signal_score,

        -- buy / neutral / sell counts for transparency
        (CASE WHEN sig_price_sma10  = 1 THEN 1 ELSE 0 END
         + CASE WHEN sig_price_sma20  = 1 THEN 1 ELSE 0 END
         + CASE WHEN sig_price_sma50  = 1 THEN 1 ELSE 0 END
         + CASE WHEN sig_price_sma200 = 1 THEN 1 ELSE 0 END
         + CASE WHEN sig_ma_cross     = 1 THEN 1 ELSE 0 END
         + CASE WHEN sig_rsi          = 1 THEN 1 ELSE 0 END
         + CASE WHEN sig_bb           = 1 THEN 1 ELSE 0 END
         + CASE WHEN sig_macd_proxy   = 1 THEN 1 ELSE 0 END) AS buy_signals,

        (CASE WHEN sig_price_sma10  = -1 THEN 1 ELSE 0 END
         + CASE WHEN sig_price_sma20  = -1 THEN 1 ELSE 0 END
         + CASE WHEN sig_price_sma50  = -1 THEN 1 ELSE 0 END
         + CASE WHEN sig_price_sma200 = -1 THEN 1 ELSE 0 END
         + CASE WHEN sig_ma_cross     = -1 THEN 1 ELSE 0 END
         + CASE WHEN sig_rsi          = -1 THEN 1 ELSE 0 END
         + CASE WHEN sig_bb           = -1 THEN 1 ELSE 0 END
         + CASE WHEN sig_macd_proxy   = -1 THEN 1 ELSE 0 END) AS sell_signals
    FROM signals
)

SELECT
    symbol,
    company_name,
    sector,
    market_cap_tier,
    date,
    close,
    volume,
    ROUND(avg_volume_20d, 0) AS avg_volume_20d,

    -- Indicators
    sma10,
    sma20,
    sma50,
    sma200,
    bb_lower,
    bb_upper,
    rsi14,

    -- Individual signal votes
    sig_price_sma10,
    sig_price_sma20,
    sig_price_sma50,
    sig_price_sma200,
    sig_ma_cross,
    sig_rsi,
    sig_bb,
    sig_macd_proxy,

    -- Aggregated score
    buy_signals,
    sell_signals,
    8 - buy_signals - sell_signals  AS neutral_signals,
    signal_score,

    -- Final rating label
    CASE
        WHEN signal_score >=  5 THEN 'Strong Buy'
        WHEN signal_score >=  3 THEN 'Buy'
        WHEN signal_score >= -2 THEN 'Neutral'
        WHEN signal_score >= -4 THEN 'Sell'
        ELSE                         'Strong Sell'
    END AS rating,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM scored

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
