{{
    config(
        materialized    = 'incremental',
        unique_key      = ['symbol', 'date'],
        on_schema_change = 'sync_all_columns',
        incremental_strategy = 'append',
    )
}}

/*
  gold_volatility_index
  ──────────────────────
  Rolling 20-day historical volatility per symbol, annualised.

  Methodology:
    1. Compute daily log-return: ln(close_t / close_{t-1})
    2. Compute rolling 20-day standard deviation of log-returns
    3. Annualise: vol_20d × SQRT(252)    [252 trading days/year]

  Also includes the 5-day and 10-day rolling volatility for shorter
  time-frame comparison.
*/

WITH
ohlcv AS (
    SELECT
        o.symbol,
        o.date,
        o.close,
        o.volume,
        o.high,
        o.low,
        s.sector,
        s.company_name
    FROM {{ ref('silver_ohlcv') }}   AS o
    JOIN {{ ref('silver_symbols') }} AS s
        ON o.symbol = s.symbol
),

with_returns AS (
    SELECT
        symbol,
        date,
        close,
        volume,
        high,
        low,
        sector,
        company_name,
        LAG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS prev_close
    FROM ohlcv
),

with_log_return AS (
    SELECT
        symbol,
        date,
        close,
        volume,
        high,
        low,
        sector,
        company_name,
        prev_close,
        CASE
            WHEN prev_close > 0 AND close > 0
            THEN LN(close / prev_close)
            ELSE NULL
        END AS log_return
    FROM with_returns

    {% if is_incremental() %}
    -- Pull extra rows before the incremental cutoff to allow window functions
    -- to have sufficient look-back data (20 trading days)
    WHERE date > (
        SELECT DATE_ADD('day', -25, MAX(date)) FROM {{ this }}
    )
    {% endif %}
),

with_volatility AS (
    SELECT
        symbol,
        date,
        close,
        volume,
        high,
        low,
        sector,
        company_name,
        log_return,
        -- 5-day volatility
        STDDEV(log_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_5d,
        -- 10-day volatility
        STDDEV(log_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS vol_10d,
        -- 20-day volatility (the primary metric)
        STDDEV(log_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_20d
    FROM with_log_return
)

SELECT
    symbol,
    company_name,
    sector,
    date,
    close,
    volume,
    high,
    low,
    ROUND(log_return,                    6)  AS log_return,
    ROUND(vol_5d,                        6)  AS vol_5d,
    ROUND(vol_10d,                       6)  AS vol_10d,
    ROUND(vol_20d,                       6)  AS vol_20d,
    -- Annualised volatilities (multiply daily vol by √252)
    ROUND(vol_5d  * SQRT(252),           4)  AS annualized_vol_5d,
    ROUND(vol_10d * SQRT(252),           4)  AS annualized_vol_10d,
    ROUND(vol_20d * SQRT(252),           4)  AS annualized_vol,
    CURRENT_TIMESTAMP                        AS dbt_updated_at

FROM with_volatility

{% if is_incremental() %}
-- Only emit rows for dates after the current max in the table
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
