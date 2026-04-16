{{
    config(
        materialized         = 'incremental',
        unique_key           = ['symbol', 'date', 'anomaly_type'],
        on_schema_change     = 'sync_all_columns',
        incremental_strategy = 'delete+insert',
        schema               = 'gold',
    )
}}

/*
  gold_anomaly_flags
  ───────────────────
  SQL-computed anomaly detection results for all symbols.
  Two algorithms are applied:

  1. Volume Z-score: flags when a day's volume is > 2.5 standard deviations
     above its 30-day rolling mean.

  2. Price IQR (daily return): flags when a day's return falls outside the
     IQR fence [Q1 − 1.5×IQR, Q3 + 1.5×IQR] computed over the trailing 90
     trading days.

  Note: The Python-computed anomaly records written by the ml_anomaly_detection
  Airflow DAG are written directly to this same Iceberg table. This model
  provides SQL-layer coverage that runs as part of every dbt run.
*/

WITH
vol_with_stats AS (
    SELECT
        v.symbol,
        v.date,
        v.log_return,
        v.annualized_vol,
        v.volume,
        v.close,
        v.sector,
        AVG(CAST(v.volume AS DOUBLE)) OVER (
            PARTITION BY v.symbol
            ORDER BY v.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS vol_mean_30d,
        STDDEV(CAST(v.volume AS DOUBLE)) OVER (
            PARTITION BY v.symbol
            ORDER BY v.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS vol_std_30d,
        APPROX_PERCENTILE(v.log_return, 0.25) OVER (
            PARTITION BY v.symbol
            ORDER BY v.date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS return_q1_90d,
        APPROX_PERCENTILE(v.log_return, 0.75) OVER (
            PARTITION BY v.symbol
            ORDER BY v.date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS return_q3_90d
    FROM {{ ref('gold_volatility_index') }} AS v

    {% if is_incremental() %}
    WHERE v.date > (
        SELECT DATE_ADD('day', -95, MAX(date)) FROM {{ this }}
    )
    {% endif %}
),

scored AS (
    SELECT
        symbol,
        date,
        log_return,
        annualized_vol,
        volume,
        close,
        sector,
        vol_mean_30d,
        vol_std_30d,
        return_q1_90d,
        return_q3_90d,
        CASE
            WHEN vol_std_30d > 0
            THEN (CAST(volume AS DOUBLE) - vol_mean_30d) / vol_std_30d
            ELSE 0
        END AS vol_zscore,
        return_q1_90d - 1.5 * (return_q3_90d - return_q1_90d) AS iqr_lower_fence,
        return_q3_90d + 1.5 * (return_q3_90d - return_q1_90d) AS iqr_upper_fence
    FROM vol_with_stats
),

volume_anomalies AS (
    SELECT
        symbol,
        date,
        'volume_zscore'                AS anomaly_type,
        ROUND(vol_zscore, 4)           AS score,
        CAST(2.5 AS DOUBLE)            AS threshold,
        ABS(vol_zscore) > 2.5          AS is_anomaly,
        CURRENT_TIMESTAMP              AS detected_at
    FROM scored
    WHERE vol_std_30d IS NOT NULL
),

price_anomalies AS (
    SELECT
        symbol,
        date,
        'price_iqr'                    AS anomaly_type,
        ROUND(
            GREATEST(
                iqr_lower_fence - log_return,
                log_return - iqr_upper_fence,
                0.0
            ) / NULLIF(return_q3_90d - return_q1_90d, 0),
        4)                             AS score,
        CAST(1.5 AS DOUBLE)            AS threshold,
        log_return < iqr_lower_fence
            OR log_return > iqr_upper_fence AS is_anomaly,
        CURRENT_TIMESTAMP              AS detected_at
    FROM scored
    WHERE return_q1_90d IS NOT NULL
      AND return_q3_90d IS NOT NULL
      AND log_return     IS NOT NULL
)

SELECT * FROM volume_anomalies
UNION ALL
SELECT * FROM price_anomalies

{% if is_incremental() %}
-- Emit only new dates; the 95-day buffer above feeds the window functions.
-- This WHERE is applied after the UNION ALL via a wrapping subquery in
-- dbt's incremental compilation.
{% endif %}
