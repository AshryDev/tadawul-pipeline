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
  decision_signals — unified KBS decision layer
  ─────────────────────────────────────────────
  Single output table for the entire decision layer. Reads all six gold models
  directly and applies the decision table seed to produce BUY / SELL / HOLD
  signals with a formal certainty factor (§4.10) on a −1.0 to +1.0 scale.

  Knowledge representation used (Lecture 4):
    §4.1  Production rules    — each row of decision_table.csv is an IF-THEN rule
    §4.7  Decision tree       — signal_score → rating classification (5-branch CASE)
    §4.7  Decision table      — dbt/seeds/decision_table.csv (the explicit KB)
    §4.10 Certainty Factors   — signal_cf column (−1.0 to +1.0) on every row

  Inference mechanism:
    1. Classify each (symbol, date) into categorical dimensions
       (rating, vol_level, at_52w_pos, sector_ok, has_anomaly)
    2. Join against decision_table seed — most specific matching row wins
       (specificity_score = count of non-'any' conditions)
    3. Apply anomaly SELL override: price anomaly + signal_score < 0 → SELL (CF=−0.50)
    4. Generate explanation facility columns (§4.8 Why / Why Not / How)

  Knowledge Acquisition interface (§6.9, §7.4):
    Edit dbt/seeds/decision_table.csv and run:
    docker exec dbt dbt seed --select decision_table --project-dir /usr/dbt --profiles-dir /root/.dbt
    docker exec dbt dbt run  --select decision_signals --project-dir /usr/dbt --profiles-dir /root/.dbt
*/

WITH

-- ── 1. Gold layer inputs ──────────────────────────────────────────────────────

tech AS (
    SELECT
        symbol, company_name, sector, market_cap_tier,
        date, close, volume,
        signal_score, buy_signals, sell_signals, neutral_signals,
        sma50, sma200, rsi14, bb_lower, bb_upper
    FROM {{ ref('gold_technical_rating') }}
    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

vol AS (
    SELECT symbol, date, annualized_vol, log_return
    FROM {{ ref('gold_volatility_index') }}
),

anom AS (
    SELECT
        symbol, date,
        MAX(CASE WHEN anomaly_type = 'price_iqr'     AND is_anomaly THEN TRUE ELSE FALSE END) AS has_price_anomaly,
        MAX(CASE WHEN anomaly_type = 'volume_zscore' AND is_anomaly THEN TRUE ELSE FALSE END) AS has_volume_anomaly
    FROM {{ ref('gold_anomaly_flags') }}
    GROUP BY symbol, date
),

lev AS (
    SELECT symbol, date, high_52w, low_52w, pct_from_high, pct_from_low,
           at_52w_high, at_52w_low
    FROM {{ ref('gold_52w_levels') }}
),

sect AS (
    SELECT sector, date, advance_ratio AS sector_advance_ratio
    FROM {{ ref('gold_sector_performance') }}
),

vwap AS (
    SELECT symbol, date, session_vwap
    FROM {{ ref('gold_intraday_vwap') }}
),

-- ── 2. Classify inputs for decision table lookup ──────────────────────────────

classified AS (
    SELECT
        t.symbol, t.company_name, t.sector, t.market_cap_tier,
        t.date, t.close, t.volume,
        t.signal_score, t.buy_signals, t.sell_signals, t.neutral_signals,
        t.sma50, t.sma200, t.rsi14, t.bb_lower, t.bb_upper,

        COALESCE(v.annualized_vol,       0.0)   AS annualized_vol,
        COALESCE(v.log_return,           0.0)   AS log_return,
        COALESCE(a.has_price_anomaly,    FALSE)  AS has_price_anomaly,
        COALESCE(a.has_volume_anomaly,   FALSE)  AS has_volume_anomaly,

        l.high_52w, l.low_52w,
        l.pct_from_high, l.pct_from_low,
        COALESCE(l.at_52w_high, FALSE)           AS at_52w_high,
        COALESCE(l.at_52w_low,  FALSE)           AS at_52w_low,

        COALESCE(s.sector_advance_ratio, 0.5)   AS sector_advance_ratio,
        w.session_vwap,

        -- ── Decision tree: signal_score → rating (§4.7 Formal Logic) ─────────
        -- Five branches mapping the raw score to a categorical label
        CASE
            WHEN t.signal_score >=  5 THEN 'Strong Buy'
            WHEN t.signal_score >=  3 THEN 'Buy'
            WHEN t.signal_score <= -5 THEN 'Strong Sell'
            WHEN t.signal_score <= -3 THEN 'Sell'
            ELSE                           'Neutral'
        END AS rating,

        -- ── Categorical dimension: volatility level ───────────────────────────
        CASE
            WHEN COALESCE(v.annualized_vol, 0.0) > 0.80 THEN 'extreme'
            WHEN COALESCE(v.annualized_vol, 0.0) > 0.50 THEN 'high'
            WHEN COALESCE(v.annualized_vol, 0.0) > 0.20 THEN 'normal'
            ELSE                                              'low'
        END AS vol_level,

        -- ── Categorical dimension: 52-week position ───────────────────────────
        CASE
            WHEN COALESCE(l.at_52w_low,  FALSE) THEN 'near_low'
            WHEN COALESCE(l.at_52w_high, FALSE) THEN 'near_high'
            ELSE                                     'neutral'
        END AS at_52w_pos,

        -- ── Boolean: sector advancing ─────────────────────────────────────────
        (COALESCE(s.sector_advance_ratio, 0.5) >= 0.5) AS sector_ok

    FROM tech             AS t
    LEFT JOIN vol         AS v  ON t.symbol  = v.symbol  AND t.date = v.date
    LEFT JOIN anom        AS a  ON t.symbol  = a.symbol  AND t.date = a.date
    LEFT JOIN lev         AS l  ON t.symbol  = l.symbol  AND t.date = l.date
    LEFT JOIN sect        AS s  ON t.sector  = s.sector  AND t.date = s.date
    LEFT JOIN vwap        AS w  ON t.symbol  = w.symbol  AND t.date = w.date
),

-- ── 3. Classify inputs into a single condition_group (row dimension) ─────────
-- Priority order: anomaly/vol risk first, then sector context, then 52W position

classified_with_group AS (
    SELECT
        *,
        CASE
            WHEN has_price_anomaly OR vol_level IN ('high', 'extreme') THEN 'anomaly_highvol'
            WHEN NOT sector_ok                                          THEN 'sector_weak'
            WHEN at_52w_pos = 'near_low'                               THEN 'near_52w_low'
            WHEN at_52w_pos = 'near_high'                              THEN 'near_52w_high'
            WHEN vol_level = 'low'                                     THEN 'sector_ok_lowvol'
            WHEN vol_level = 'normal'                                  THEN 'mid_medvol'
            ELSE                                                            'fallback'
        END AS condition_group
    FROM classified
),

-- ── 4. Decision table (§4.7 Formal Logic + §4.10 Certainty Factors) ──────────
-- 7 × 5 symmetric grid — exact 2-key join on (condition_group, rating_group)

dt AS (
    SELECT * FROM {{ ref('decision_table') }}
),

matched AS (
    SELECT
        c.*,
        d.state_id,
        d.signal        AS dt_signal,
        d.signal_cf     AS dt_signal_cf,
        d.confidence    AS dt_confidence,
        d.description   AS dt_description
    FROM classified_with_group AS c
    LEFT JOIN dt AS d
        ON d.condition_group = c.condition_group
       AND d.rating_group    = c.rating
),

-- ── 5. Apply anomaly SELL override ────────────────────────────────────────────
-- Price-IQR anomaly firing alongside a negative signal_score overrides the
-- decision table and forces SELL (CF = −0.50) — anomaly path from §4.1.

with_signal AS (
    SELECT
        *,
        CASE
            WHEN has_price_anomaly AND rating = 'Neutral' AND signal_score < 0 THEN 'SELL'
            ELSE dt_signal
        END AS signal,

        CASE
            WHEN has_price_anomaly AND rating = 'Neutral' AND signal_score < 0 THEN -0.50
            ELSE dt_signal_cf
        END AS signal_cf,

        CASE
            WHEN has_price_anomaly AND rating = 'Neutral' AND signal_score < 0 THEN 'MEDIUM'
            ELSE dt_confidence
        END AS confidence
    FROM matched
),

-- ── 6. Explanation facility (§4.8 Why / Why Not / How) ───────────────────────

with_explanation AS (
    SELECT
        *,

        -- WHY: why this signal was produced (Dynamic explanation §4.9)
        CONCAT(
            signal,
            ' (CF=', CAST(ROUND(signal_cf, 2) AS VARCHAR),
            ', ', confidence, '): ',
            'state=', COALESCE(state_id, 'override'),
            ' — ',
            CASE
                WHEN has_price_anomaly AND rating = 'Neutral' AND signal_score < 0
                THEN 'price-IQR anomaly with negative signal score (' || CAST(signal_score AS VARCHAR) || ') — Neutral rating breakdown'
                ELSE COALESCE(dt_description, 'no description')
            END
        ) AS why_signal,

        -- WHY NOT BUY: first condition that blocked BUY (NULL when signal = BUY)
        CASE
            WHEN signal = 'BUY' THEN NULL
            WHEN rating NOT IN ('Strong Buy', 'Buy')
            THEN CONCAT(
                'Rating is ''', rating,
                ''' (score=', CAST(signal_score AS VARCHAR), '/8) — not eligible for BUY'
            )
            WHEN has_price_anomaly
            THEN CONCAT(
                'Price-IQR anomaly detected — spike entry risk (state ', COALESCE(state_id, '?'), ')'
            )
            WHEN NOT sector_ok
            THEN CONCAT(
                'Sector advancing ', CAST(ROUND(sector_advance_ratio * 100, 0) AS VARCHAR),
                '% < 50% threshold'
            )
            WHEN vol_level IN ('extreme', 'high')
            THEN CONCAT(
                vol_level, ' volatility (',
                CAST(ROUND(annualized_vol * 100, 1) AS VARCHAR), '% ann.) — position risk'
            )
            ELSE CONCAT(
                'Decision table state ', COALESCE(state_id, '?'), ' maps to ', signal
            )
        END AS why_not_buy,

        -- WHY NOT SELL (NULL when signal = SELL)
        CASE
            WHEN signal = 'SELL' THEN NULL
            WHEN NOT (rating IN ('Sell', 'Strong Sell'))
             AND NOT (has_price_anomaly AND rating = 'Neutral' AND signal_score < 0)
            THEN CONCAT('Rating is ''', rating, ''' and no triggering anomaly')
            ELSE 'SELL not triggered'
        END AS why_not_sell,

        -- HOW: numbered step-by-step inference trace (Tracing §4.9)
        CONCAT(
            '1. Inputs classified: rating=', rating,
            ', score=', CAST(signal_score AS VARCHAR), '/8',
            ', vol=', vol_level,
            ', 52W=', at_52w_pos,
            ', anomaly=', CAST(has_price_anomaly AS VARCHAR),
            ', sector_ok=', CAST(sector_ok AS VARCHAR), '. ',
            '2. Decision table matched: state=', COALESCE(state_id, 'fallback'),
            ' (condition_group=', COALESCE(condition_group, '?'), '). ',
            '3. Signal=', signal,
            ', CF=', CAST(ROUND(signal_cf, 2) AS VARCHAR),
            ' (', confidence, ').',
            CASE
                WHEN has_price_anomaly AND rating = 'Neutral' AND signal_score < 0
                THEN ' [anomaly SELL override applied]'
                ELSE ''
            END
        ) AS reasoning_trace

    FROM with_signal
)

SELECT
    -- ── Identity ──────────────────────────────────────────────────────────────
    symbol, company_name, sector, market_cap_tier, date, close, volume,

    -- ── Signal output (§4.10 uncertainty on −1.0 to +1.0 scale) ─────────────
    signal,
    ROUND(signal_cf, 4)         AS signal_cf,
    confidence,
    state_id,

    -- ── Explanation facility (§4.8) ───────────────────────────────────────────
    why_signal,
    why_not_buy,
    why_not_sell,
    reasoning_trace,

    -- ── Technical rating ──────────────────────────────────────────────────────
    rating,
    signal_score,
    buy_signals, sell_signals, neutral_signals,
    ROUND(rsi14,    2)          AS rsi14,
    ROUND(sma50,    4)          AS sma50,
    ROUND(sma200,   4)          AS sma200,
    ROUND(bb_lower, 4)          AS bb_lower,
    ROUND(bb_upper, 4)          AS bb_upper,

    -- ── Volatility ────────────────────────────────────────────────────────────
    ROUND(annualized_vol, 4)    AS annualized_vol,
    vol_level,

    -- ── 52-week levels ────────────────────────────────────────────────────────
    high_52w, low_52w,
    ROUND(pct_from_high, 4)     AS pct_from_high,
    ROUND(pct_from_low,  4)     AS pct_from_low,
    at_52w_high, at_52w_low, at_52w_pos,

    -- ── Anomaly flags ─────────────────────────────────────────────────────────
    has_price_anomaly,
    has_volume_anomaly,

    -- ── Sector & VWAP ─────────────────────────────────────────────────────────
    ROUND(sector_advance_ratio, 4) AS sector_advance_ratio,
    sector_ok,
    session_vwap,

    CURRENT_TIMESTAMP               AS dbt_updated_at

FROM with_explanation

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
