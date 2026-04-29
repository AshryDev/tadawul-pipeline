{{
    config(
        materialized         = 'incremental',
        unique_key           = ['symbol', 'date'],
        on_schema_change     = 'sync_all_columns',
        incremental_strategy = 'delete+insert',
        schema               = 'decision',
    )
}}

/*
  decision_metarule_flags
  ─────────────────────────
  Evaluates metarules per symbol per day. Metarules are higher-order rules that
  control how inference rules are applied (Lecture 4 §4.2 — Metarules).

  Three metarules / gates are evaluated:

  M01 — Market-wide anomaly metarule:
        If > 30% of all symbols have a price-IQR anomaly today, the market is
        in an unstable regime. Tighten BUY threshold from signal_score ≥ 3
        to signal_score ≥ 6 to require stronger conviction.

  M02 — Consecutive down-day metarule:
        If a symbol has 3 consecutive days with log_return < −2%, sustained
        selling pressure is present. Block BUY regardless of rating.

  G03 — Extreme volatility gate:
        If annualised 20-day volatility exceeds 80%, position risk is too high.
        Block BUY to protect against runaway adverse moves.
*/

WITH
-- ── M01: Market-wide price anomaly rate ──────────────────────────────────────
market_anomaly AS (
    SELECT
        date,
        CAST(
            SUM(CASE WHEN anomaly_type = 'price_iqr' AND is_anomaly THEN 1 ELSE 0 END)
            AS DOUBLE
        ) / NULLIF(COUNT(DISTINCT symbol), 0) AS market_anomaly_rate
    FROM {{ ref('gold_anomaly_flags') }}
    GROUP BY date
),

-- ── M02 + G03 per symbol ──────────────────────────────────────────────────────
vol_data AS (
    SELECT
        symbol,
        date,
        log_return,
        annualized_vol,
        SUM(CASE WHEN log_return < -0.02 THEN 1 ELSE 0 END) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS down_days_3d
    FROM {{ ref('gold_volatility_index') }}

    {% if is_incremental() %}
    -- Pull 5 extra days to populate the 3-day rolling window for new rows.
    WHERE date > (SELECT DATE_ADD('day', -5, MAX(date)) FROM {{ this }})
    {% endif %}
),

joined AS (
    SELECT
        v.symbol,
        v.date,
        v.annualized_vol,
        v.down_days_3d,
        COALESCE(m.market_anomaly_rate, 0.0) AS market_anomaly_rate
    FROM vol_data          AS v
    LEFT JOIN market_anomaly AS m ON v.date = m.date
)

SELECT
    symbol,
    date,

    -- ── Metarule evaluation ───────────────────────────────────────────────────
    market_anomaly_rate > 0.30                              AS m01_tighten_buy,
    down_days_3d = 3                                        AS m02_consecutive_down,
    COALESCE(annualized_vol, 0.0) > 0.80                    AS g03_extreme_volatility,

    -- Human-readable list of fired metarule IDs
    TRIM(CONCAT(
        CASE WHEN market_anomaly_rate > 0.30                   THEN 'M01 ' ELSE '' END,
        CASE WHEN down_days_3d = 3                             THEN 'M02 ' ELSE '' END,
        CASE WHEN COALESCE(annualized_vol, 0.0) > 0.80         THEN 'G03'  ELSE '' END
    ))                                                      AS active_metarules,

    -- BUY requires signal_score ≥ this value (3 normally; 6 when M01 fires)
    CASE WHEN market_anomaly_rate > 0.30 THEN 6 ELSE 3 END  AS required_score_for_buy,

    ROUND(market_anomaly_rate, 4)                           AS market_anomaly_rate,
    down_days_3d,
    ROUND(COALESCE(annualized_vol, 0.0), 4)                 AS annualized_vol,
    CURRENT_TIMESTAMP                                       AS dbt_updated_at

FROM joined

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
