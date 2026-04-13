{{
    config(
        materialized    = 'incremental',
        unique_key      = ['symbol', 'date'],
        on_schema_change = 'sync_all_columns',
        incremental_strategy = 'append',
    )
}}

/*
  gold_52w_levels
  ────────────────
  52-week high / low proximity analysis for each symbol.

  Metrics:
    high_52w       — rolling max of daily high over the past 252 trading days
    low_52w        — rolling min of daily low  over the past 252 trading days
    pct_from_high  — (close - high_52w) / high_52w  (negative = below high)
    pct_from_low   — (close - low_52w)  / low_52w   (positive = above low)
    at_52w_high    — true when close is within 2% of the 52-week high
    at_52w_low     — true when close is within 2% of the 52-week low

  This model is used in Superset dashboards to highlight near-breakout symbols.
*/

WITH
ohlcv AS (
    SELECT
        o.symbol,
        o.date,
        o.open,
        o.high,
        o.low,
        o.close,
        o.volume,
        s.company_name,
        s.sector,
        s.market_cap_tier
    FROM {{ ref('silver_ohlcv') }}   AS o
    JOIN {{ ref('silver_symbols') }} AS s
        ON o.symbol = s.symbol

    {% if is_incremental() %}
    -- Pull extra rows to populate the 252-day window for new rows
    WHERE o.date > (
        SELECT DATE_ADD('day', -260, MAX(date)) FROM {{ this }}
    )
    {% endif %}
),

with_rolling AS (
    SELECT
        symbol,
        company_name,
        sector,
        market_cap_tier,
        date,
        open,
        high,
        low,
        close,
        volume,
        -- 52-week (252 trading days) high and low
        MAX(high) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS high_52w,
        MIN(low) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS low_52w
    FROM ohlcv
)

SELECT
    symbol,
    company_name,
    sector,
    market_cap_tier,
    date,
    open,
    high,
    low,
    close,
    volume,
    ROUND(high_52w, 4)                                           AS high_52w,
    ROUND(low_52w,  4)                                           AS low_52w,
    -- Proximity percentages (negative means below high; positive means above low)
    ROUND((close - high_52w) / NULLIF(high_52w, 0), 4)         AS pct_from_high,
    ROUND((close - low_52w)  / NULLIF(low_52w,  0), 4)         AS pct_from_low,
    -- Boolean signals
    ((close - high_52w) / NULLIF(high_52w, 0)) > -0.02         AS at_52w_high,
    ((close - low_52w)  / NULLIF(low_52w,  0)) <  0.02         AS at_52w_low,
    CURRENT_TIMESTAMP                                            AS dbt_updated_at

FROM with_rolling

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
