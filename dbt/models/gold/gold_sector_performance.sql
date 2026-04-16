{{
    config(
        materialized         = 'incremental',
        unique_key           = ['sector', 'date'],
        on_schema_change     = 'sync_all_columns',
        incremental_strategy = 'delete+insert',
        schema               = 'gold',
    )
}}

/*
  gold_sector_performance
  ────────────────────────
  Daily sector rotation index: average return, return range, and constituent
  count per sector per trading day. Useful for identifying which sectors are
  leading or lagging the market.

  daily_return = (close_today - close_yesterday) / close_yesterday
*/

WITH
ohlcv_with_sector AS (
    SELECT
        o.symbol,
        o.date,
        o.close,
        o.volume,
        s.sector,
        s.market_cap_tier
    FROM {{ ref('silver_ohlcv') }}   AS o
    JOIN {{ ref('silver_symbols') }} AS s
        ON o.symbol = s.symbol
),

with_lag AS (
    SELECT
        symbol,
        date,
        close,
        volume,
        sector,
        market_cap_tier,
        LAG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS prev_close
    FROM ohlcv_with_sector
),

with_return AS (
    SELECT
        symbol,
        date,
        close,
        volume,
        sector,
        market_cap_tier,
        CASE
            WHEN prev_close IS NOT NULL AND prev_close > 0
            THEN ROUND((close - prev_close) / prev_close, 6)
            ELSE NULL
        END AS daily_return
    FROM with_lag
    WHERE close IS NOT NULL

    {% if is_incremental() %}
        AND date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
)

SELECT
    sector,
    date,
    COUNT(DISTINCT symbol)                                              AS symbol_count,
    ROUND(AVG(daily_return),    6)                                     AS avg_daily_return,
    ROUND(MAX(daily_return),    6)                                     AS max_daily_return,
    ROUND(MIN(daily_return),    6)                                     AS min_daily_return,
    ROUND(STDDEV(daily_return), 6)                                     AS stddev_daily_return,
    SUM(CAST(volume AS BIGINT))                                        AS total_sector_volume,
    CAST(
        SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END) AS DOUBLE
    ) / NULLIF(COUNT(DISTINCT symbol), 0)                              AS advance_ratio,
    CURRENT_TIMESTAMP                                                  AS dbt_updated_at
FROM with_return
WHERE daily_return IS NOT NULL
GROUP BY sector, date
