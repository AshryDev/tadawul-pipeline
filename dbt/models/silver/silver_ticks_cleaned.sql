{{
    config(
        materialized         = 'incremental',
        unique_key           = ['symbol', 'event_time'],
        on_schema_change     = 'sync_all_columns',
        incremental_strategy = 'append',
        schema               = 'silver',
    )
}}

/*
  silver_ticks_cleaned
  ─────────────────────
  Deduplicates and type-validates raw tick data from bronze_ticks.
  Each row is guaranteed to have:
    - A non-null symbol, price > 0, volume > 0, valid event_time
  Timestamps are stored in UTC. No timezone conversion occurs here;
  display conversion (UTC → Asia/Riyadh) is done at the dashboard layer.
*/

WITH
source AS (
    SELECT
        symbol,
        CAST(price      AS DOUBLE)      AS price,
        CAST(volume     AS BIGINT)      AS volume,
        CAST(bid        AS DOUBLE)      AS bid,
        CAST(ask        AS DOUBLE)      AS ask,
        CAST(event_time AS TIMESTAMP)   AS event_time,
        CAST(event_date AS DATE)        AS event_date,
        session_status,
        ingestion_time
    FROM {{ source('bronze', 'bronze_ticks') }}
    WHERE
        symbol         IS NOT NULL
        AND price      IS NOT NULL
        AND price       > 0
        AND volume     IS NOT NULL
        AND volume      > 0
        AND event_time IS NOT NULL

    {% if is_incremental() %}
        AND event_time > (SELECT MAX(event_time) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        symbol,
        price,
        volume,
        bid,
        ask,
        event_time,
        event_date,
        session_status,
        ingestion_time,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, event_time
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM source
)

SELECT
    symbol,
    price,
    volume,
    bid,
    ask,
    event_time,
    event_date,
    session_status,
    ingestion_time,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM deduplicated
WHERE rn = 1
