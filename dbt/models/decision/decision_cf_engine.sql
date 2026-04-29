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
  decision_cf_engine
  ───────────────────
  Applies the Certainty Factor (CF) combination formula to all eight technical
  indicator votes, producing a single combined_cf per (symbol, date).
  Implements Lecture 4 §4.10 — Certainty Factors.

  Each of the 8 inference rules in knowledge_rules.csv has a certainty_factor
  (CF) between 0 and 1. A rule that votes BUY contributes +CF; a rule that votes
  SELL contributes −CF; a neutral vote contributes 0.

  Combination formula applied iteratively across all 8 rules:

    CF(A, B):
      A ≥ 0 AND B ≥ 0  →  A + B × (1 − A)           [reinforcing positive evidence]
      A ≤ 0 AND B ≤ 0  →  A + B × (1 + A)           [reinforcing negative evidence]
      mixed signs       →  (A + B) / (1 − MIN(|A|,|B|))  [conflicting evidence]

  Final combined_cf ranges from −1.0 (maximum sell conviction) to +1.0 (maximum
  buy conviction).
*/

WITH
rule_cfs AS (
    SELECT rule_id, certainty_factor
    FROM {{ ref('knowledge_rules') }}
    WHERE category = 'inference'
      AND is_active
),

tech AS (
    SELECT
        symbol, date,
        sig_price_sma10,
        sig_price_sma20,
        sig_price_sma50,
        sig_price_sma200,
        sig_ma_cross,
        sig_rsi,
        sig_bb,
        sig_macd_proxy
    FROM {{ ref('gold_technical_rating') }}

    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Signed CF per rule: vote × certainty_factor  (+CF = buy, −CF = sell, 0 = neutral)
individual_votes AS (
    SELECT
        t.symbol,
        t.date,
        CAST(t.sig_price_sma10  AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R01'), 0.30) AS cf_r01,
        CAST(t.sig_price_sma20  AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R02'), 0.40) AS cf_r02,
        CAST(t.sig_price_sma50  AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R03'), 0.50) AS cf_r03,
        CAST(t.sig_price_sma200 AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R04'), 0.60) AS cf_r04,
        CAST(t.sig_ma_cross     AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R05'), 0.50) AS cf_r05,
        CAST(t.sig_rsi          AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R06'), 0.70) AS cf_r06,
        CAST(t.sig_bb           AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R07'), 0.60) AS cf_r07,
        CAST(t.sig_macd_proxy   AS DOUBLE)
            * COALESCE((SELECT certainty_factor FROM rule_cfs WHERE rule_id = 'R08'), 0.40) AS cf_r08
    FROM tech t
),

-- ── Iterative CF combination (8 steps) ───────────────────────────────────────

combine_r01_r02 AS (
    SELECT *,
        CASE
            WHEN cf_r01 >= 0 AND cf_r02 >= 0
                THEN cf_r01 + cf_r02 * (1.0 - cf_r01)
            WHEN cf_r01 <= 0 AND cf_r02 <= 0
                THEN cf_r01 + cf_r02 * (1.0 + cf_r01)
            ELSE (cf_r01 + cf_r02) / NULLIF(1.0 - LEAST(ABS(cf_r01), ABS(cf_r02)), 0.0)
        END AS cf_12
    FROM individual_votes
),

combine_12_r03 AS (
    SELECT *,
        CASE
            WHEN cf_12 >= 0 AND cf_r03 >= 0
                THEN cf_12 + cf_r03 * (1.0 - cf_12)
            WHEN cf_12 <= 0 AND cf_r03 <= 0
                THEN cf_12 + cf_r03 * (1.0 + cf_12)
            ELSE (cf_12 + cf_r03) / NULLIF(1.0 - LEAST(ABS(cf_12), ABS(cf_r03)), 0.0)
        END AS cf_123
    FROM combine_r01_r02
),

combine_123_r04 AS (
    SELECT *,
        CASE
            WHEN cf_123 >= 0 AND cf_r04 >= 0
                THEN cf_123 + cf_r04 * (1.0 - cf_123)
            WHEN cf_123 <= 0 AND cf_r04 <= 0
                THEN cf_123 + cf_r04 * (1.0 + cf_123)
            ELSE (cf_123 + cf_r04) / NULLIF(1.0 - LEAST(ABS(cf_123), ABS(cf_r04)), 0.0)
        END AS cf_1234
    FROM combine_12_r03
),

combine_1234_r05 AS (
    SELECT *,
        CASE
            WHEN cf_1234 >= 0 AND cf_r05 >= 0
                THEN cf_1234 + cf_r05 * (1.0 - cf_1234)
            WHEN cf_1234 <= 0 AND cf_r05 <= 0
                THEN cf_1234 + cf_r05 * (1.0 + cf_1234)
            ELSE (cf_1234 + cf_r05) / NULLIF(1.0 - LEAST(ABS(cf_1234), ABS(cf_r05)), 0.0)
        END AS cf_12345
    FROM combine_123_r04
),

combine_12345_r06 AS (
    SELECT *,
        CASE
            WHEN cf_12345 >= 0 AND cf_r06 >= 0
                THEN cf_12345 + cf_r06 * (1.0 - cf_12345)
            WHEN cf_12345 <= 0 AND cf_r06 <= 0
                THEN cf_12345 + cf_r06 * (1.0 + cf_12345)
            ELSE (cf_12345 + cf_r06) / NULLIF(1.0 - LEAST(ABS(cf_12345), ABS(cf_r06)), 0.0)
        END AS cf_123456
    FROM combine_1234_r05
),

combine_123456_r07 AS (
    SELECT *,
        CASE
            WHEN cf_123456 >= 0 AND cf_r07 >= 0
                THEN cf_123456 + cf_r07 * (1.0 - cf_123456)
            WHEN cf_123456 <= 0 AND cf_r07 <= 0
                THEN cf_123456 + cf_r07 * (1.0 + cf_123456)
            ELSE (cf_123456 + cf_r07) / NULLIF(1.0 - LEAST(ABS(cf_123456), ABS(cf_r07)), 0.0)
        END AS cf_1234567
    FROM combine_12345_r06
),

combine_1234567_r08 AS (
    SELECT *,
        CASE
            WHEN cf_1234567 >= 0 AND cf_r08 >= 0
                THEN cf_1234567 + cf_r08 * (1.0 - cf_1234567)
            WHEN cf_1234567 <= 0 AND cf_r08 <= 0
                THEN cf_1234567 + cf_r08 * (1.0 + cf_1234567)
            ELSE (cf_1234567 + cf_r08) / NULLIF(1.0 - LEAST(ABS(cf_1234567), ABS(cf_r08)), 0.0)
        END AS combined_cf
    FROM combine_123456_r07
)

SELECT
    symbol,
    date,

    -- Individual signed CFs (transparency for explanation facility)
    ROUND(cf_r01, 4) AS cf_r01,
    ROUND(cf_r02, 4) AS cf_r02,
    ROUND(cf_r03, 4) AS cf_r03,
    ROUND(cf_r04, 4) AS cf_r04,
    ROUND(cf_r05, 4) AS cf_r05,
    ROUND(cf_r06, 4) AS cf_r06,
    ROUND(cf_r07, 4) AS cf_r07,
    ROUND(cf_r08, 4) AS cf_r08,

    -- Combined certainty factor (−1.0 to +1.0)
    ROUND(combined_cf, 4) AS combined_cf,

    -- Confidence tier derived from combined CF magnitude
    CASE
        WHEN ABS(combined_cf) >= 0.65 THEN 'HIGH'
        WHEN ABS(combined_cf) >= 0.40 THEN 'MEDIUM'
        ELSE                               'LOW'
    END AS cf_confidence,

    CASE WHEN combined_cf >= 0.40  THEN ROUND(combined_cf,      4) ELSE 0.0 END AS cf_buy_strength,
    CASE WHEN combined_cf <= -0.40 THEN ROUND(ABS(combined_cf), 4) ELSE 0.0 END AS cf_sell_strength,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM combine_1234567_r08

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
