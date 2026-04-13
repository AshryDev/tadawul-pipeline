{{
    config(
        materialized    = 'incremental',
        unique_key      = ['symbol', 'date'],
        on_schema_change = 'sync_all_columns',
        incremental_strategy = 'append',
    )
}}

/*
  silver_ohlcv
  ─────────────
  Cleaned, typed daily OHLCV bars sourced from bronze_daily_ohlcv.
  - Deduplicates on (symbol, date) keeping the latest ingestion
  - Filters zero-volume days (holidays / market halts)
  - Ensures price columns are positive
*/

WITH
source AS (
    SELECT
        symbol,
        CAST(date   AS DATE)    AS date,
        CAST(open   AS DOUBLE)  AS open,
        CAST(high   AS DOUBLE)  AS high,
        CAST(low    AS DOUBLE)  AS low,
        CAST(close  AS DOUBLE)  AS close,
        CAST(volume AS BIGINT)  AS volume,
        CAST(vwap   AS DOUBLE)  AS vwap,
        CAST(transactions AS BIGINT) AS transactions,
        ingestion_time
    FROM {{ source('bronze', 'bronze_daily_ohlcv') }}
    WHERE
        symbol    IS NOT NULL
        AND date  IS NOT NULL
        AND close IS NOT NULL
        AND close  > 0
        AND volume IS NOT NULL
        AND volume > 0
        AND open  > 0
        AND high  > 0
        AND low   > 0

    {% if is_incremental() %}
        AND CAST(date AS DATE) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM source
)

SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    transactions,
    ingestion_time,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM deduplicated
WHERE rn = 1
