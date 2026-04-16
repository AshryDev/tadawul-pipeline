{{
    config(
        materialized         = 'incremental',
        unique_key           = ['symbol', 'session_date'],
        on_schema_change     = 'sync_all_columns',
        incremental_strategy = 'delete+insert',
        schema               = 'gold',
    )
}}

/*
  gold_intraday_vwap
  ───────────────────
  Volume-Weighted Average Price per symbol per trading session, along with
  intraday open / high / low / close values derived from tick data.

  VWAP = SUM(price × volume) / SUM(volume)

  Joined with silver_symbols for sector attribution.
*/

WITH
ticks AS (
    SELECT
        t.symbol,
        t.event_date                            AS session_date,
        t.price,
        t.volume,
        t.event_time,
        s.company_name,
        s.sector,
        s.market_cap_tier
    FROM {{ ref('silver_ticks_cleaned') }} AS t
    JOIN {{ ref('silver_symbols') }}       AS s
        ON t.symbol = s.symbol

    {% if is_incremental() %}
        WHERE t.event_date > (SELECT MAX(session_date) FROM {{ this }})
    {% endif %}
),

session_agg AS (
    SELECT
        symbol,
        company_name,
        sector,
        market_cap_tier,
        session_date,
        -- VWAP
        CAST(
            SUM(CAST(price AS DOUBLE) * CAST(volume AS DOUBLE))
            / NULLIF(SUM(CAST(volume AS DOUBLE)), 0)
        AS DOUBLE)                              AS vwap,
        -- Session OHLC from tick data
        CAST(MIN_BY(price, event_time) AS DOUBLE) AS session_open,
        CAST(MAX(price)                AS DOUBLE) AS session_high,
        CAST(MIN(price)                AS DOUBLE) AS session_low,
        CAST(MAX_BY(price, event_time) AS DOUBLE) AS session_close,
        SUM(CAST(volume AS BIGINT))               AS total_volume,
        COUNT(*)                                  AS tick_count,
        MIN(event_time)                           AS session_start,
        MAX(event_time)                           AS session_end
    FROM ticks
    GROUP BY
        symbol,
        company_name,
        sector,
        market_cap_tier,
        session_date
)

SELECT
    symbol,
    company_name,
    sector,
    market_cap_tier,
    session_date,
    ROUND(vwap,          4)  AS vwap,
    ROUND(session_open,  4)  AS session_open,
    ROUND(session_high,  4)  AS session_high,
    ROUND(session_low,   4)  AS session_low,
    ROUND(session_close, 4)  AS session_close,
    total_volume,
    tick_count,
    session_start,
    session_end,
    CURRENT_TIMESTAMP        AS dbt_updated_at
FROM session_agg
