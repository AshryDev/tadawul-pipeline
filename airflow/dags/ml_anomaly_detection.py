"""
Airflow DAG: ml_anomaly_detection
===================================
Applies ML and statistical anomaly detection to Tadawul stock data, writing
results into iceberg.gold.gold_anomaly_flags alongside the dbt SQL-layer
records. Each algorithm is distinguished by anomaly_type:

  - isolation_forest  — sklearn IsolationForest on [log_return, vol_20d,
                        volume] per symbol; multi-dimensional, captures
                        joint outliers the univariate methods miss.
  - volume_zscore     — (volume − 30d_mean) / 30d_std > 2.5
  - price_iqr         — daily return outside Q1 − 1.5×IQR … Q3 + 1.5×IQR
                        over trailing 90 days

The statistical algorithms (volume_zscore, price_iqr) mirror the dbt model
but run on a per-batch Python path for auditability and back-fill testing.

Schedule
────────
"0 0 1 1,7 *" — Jan 1 and Jul 1, aligned with backfill_ohlcv so
gold_volatility_index is always fresh before this DAG scores it.
start_date is 2021-07-01 so the first run has 12 months of lookback data.

Idempotency
───────────
Each write deletes existing rows for (date, anomaly_type) before appending,
so dbt-written volume_zscore / price_iqr records are never touched.
"""

from __future__ import annotations

import logging
import os
import warnings
from datetime import datetime, timedelta, date as date_type, timezone
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
import pyarrow as pa
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook

warnings.filterwarnings("ignore", category=FutureWarning)

log = logging.getLogger(__name__)

TRINO_CONN_ID: str = os.getenv("TRINO_CONN_ID", "trino_conn")

# Months scored per run — matches backfill_ohlcv
BATCH_MONTHS: int = int(os.getenv("ML_BATCH_MONTHS", "6"))

# History fed to IsolationForest before the scoring window
LOOKBACK_MONTHS: int = int(os.getenv("ML_LOOKBACK_MONTHS", "12"))

# IsolationForest: expected fraction of anomalies
CONTAMINATION: float = float(os.getenv("ML_CONTAMINATION", "0.05"))

# Minimum clean rows required to fit a model for a symbol
MIN_ROWS: int = int(os.getenv("ML_MIN_ROWS", "60"))

# Statistical thresholds (volume_zscore and price_iqr)
ZSCORE_THRESHOLD: float = 2.5
IQR_MULTIPLIER: float   = 1.5
ROLLING_VOL_DAYS: int   = 30
ROLLING_RET_DAYS: int   = 90

# PyArrow schema matching iceberg.gold.gold_anomaly_flags.
# date is pa.date32() because silver_ohlcv casts the bronze string to DATE,
# and that type propagates through gold_volatility_index → gold_anomaly_flags.
ANOMALY_SCHEMA = pa.schema([
    pa.field("symbol",       pa.string(),  nullable=False),
    pa.field("date",         pa.date32(),  nullable=False),
    pa.field("anomaly_type", pa.string(),  nullable=False),
    pa.field("score",        pa.float64()),
    pa.field("threshold",    pa.float64()),
    pa.field("is_anomaly",   pa.bool_()),
    pa.field("detected_at",  pa.timestamp("us", tz="UTC")),
])


def _get_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "nessie",
        **{
            "type":                 "rest",
            "uri":                  os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/"),
            "warehouse":            os.environ.get("NESSIE_WAREHOUSE", "stocks"),
            "s3.endpoint":          os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id":     os.environ.get("MINIO_ACCESS_KEY", "admin"),
            "s3.secret-access-key": os.environ.get("MINIO_SECRET_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region":            os.environ.get("MINIO_REGION", "eu-south-1"),
        },
    )


@dag(
    dag_id="ml_anomaly_detection",
    description=(
        "IsolationForest + statistical anomaly detection on "
        "gold_volatility_index → gold_anomaly_flags"
    ),
    schedule="0 0 1 1,7 *",
    start_date=datetime(2021, 7, 1),
    catchup=True,
    max_active_runs=2,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
    },
    tags=["tadawul", "ml", "anomaly", "gold"],
)
def ml_anomaly_detection_dag():

    @task()
    def fetch_features(execution_date=None) -> list[dict]:
        """
        Pull gold_volatility_index rows covering LOOKBACK_MONTHS of model
        training history plus the BATCH_MONTHS scoring window.

        Dates are cast to VARCHAR in Trino and returned as ISO strings so
        Airflow can JSON-serialise the XCom payload.
        """
        lookback_start = execution_date - relativedelta(months=LOOKBACK_MONTHS)
        window_end     = execution_date + relativedelta(months=BATCH_MONTHS)

        lb_str  = lookback_start.strftime("%Y-%m-%d")
        end_str = window_end.strftime("%Y-%m-%d")

        log.info(
            "Fetching gold_volatility_index %s – %s "
            "(%d-month lookback + %d-month window).",
            lb_str, end_str, LOOKBACK_MONTHS, BATCH_MONTHS,
        )

        hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)

        exists = hook.get_records(
            "SELECT COUNT(*) FROM iceberg.information_schema.tables "
            "WHERE table_schema = 'gold' "
            "AND table_name = 'gold_volatility_index'"
        )
        if not exists or exists[0][0] == 0:
            raise RuntimeError(
                "iceberg.gold.gold_volatility_index not found. "
                "Run backfill_ohlcv (which triggers dbt) first."
            )

        rows = hook.get_records(
            f"SELECT symbol, CAST(date AS VARCHAR), log_return, "  # noqa: S608
            f"       vol_5d, vol_10d, vol_20d, annualized_vol, volume, close "
            f"FROM iceberg.gold.gold_volatility_index "
            f"WHERE date >= DATE '{lb_str}' AND date < DATE '{end_str}' "
            f"  AND log_return IS NOT NULL "
            f"ORDER BY symbol, date"
        )

        columns = [
            "symbol", "date", "log_return", "vol_5d", "vol_10d",
            "vol_20d", "annualized_vol", "volume", "close",
        ]
        records = [dict(zip(columns, row)) for row in rows]
        log.info("Fetched %d feature rows.", len(records))
        return records

    @task()
    def detect_anomalies(records: list[dict], execution_date=None) -> list[dict]:
        """
        Run all three anomaly algorithms over the scoring window:

        1. IsolationForest — fits on full lookback, scores scoring window.
           Features: log_return, vol_20d, volume (StandardScaler per symbol).

        2. Volume Z-score  — rolling 30-day mean/std on volume.

        3. Price IQR       — daily return vs 90-day IQR fence.

        Only rows within [execution_date, execution_date + BATCH_MONTHS) are
        emitted; the lookback rows are used for model context only.
        """
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler

        if not records:
            log.warning("No feature records — skipping detection.")
            return []

        window_start_str = execution_date.strftime("%Y-%m-%d")
        window_end_str   = (
            execution_date + relativedelta(months=BATCH_MONTHS)
        ).strftime("%Y-%m-%d")
        detected_at = datetime.now(timezone.utc).isoformat()

        df = pd.DataFrame(records)
        df["date"] = df["date"].astype(str)

        iso_features  = ["log_return", "vol_20d", "volume"]
        anomaly_records: list[dict] = []

        for symbol, sdf in df.groupby("symbol"):
            sdf = sdf.sort_values("date").reset_index(drop=True)

            # ── 1. Isolation Forest ───────────────────────────────────────────
            sdf_clean = sdf.dropna(subset=iso_features)
            if len(sdf_clean) >= MIN_ROWS:
                X = sdf_clean[iso_features].values.astype(float)
                X_scaled = StandardScaler().fit_transform(X)

                clf = IsolationForest(
                    contamination=CONTAMINATION,
                    n_estimators=100,
                    random_state=42,
                )
                clf.fit(X_scaled)

                raw_scores  = clf.score_samples(X_scaled)  # more negative = more anomalous
                predictions = clf.predict(X_scaled)         # -1 = anomaly

                lo, hi  = raw_scores.min(), raw_scores.max()
                denom   = hi - lo if hi > lo else 1.0
                norm_sc = (hi - raw_scores) / denom          # 1.0 = most anomalous

                sdf_clean = sdf_clean.copy()
                sdf_clean["_iso_score"]   = norm_sc
                sdf_clean["_iso_anomaly"] = predictions == -1

                in_window = sdf_clean[
                    (sdf_clean["date"] >= window_start_str) &
                    (sdf_clean["date"] <  window_end_str)
                ]
                for _, row in in_window.iterrows():
                    anomaly_records.append({
                        "symbol":       symbol,
                        "date":         row["date"],
                        "anomaly_type": "isolation_forest",
                        "score":        round(float(row["_iso_score"]), 4),
                        "threshold":    CONTAMINATION,
                        "is_anomaly":   bool(row["_iso_anomaly"]),
                        "detected_at":  detected_at,
                    })
            else:
                log.warning(
                    "%s: %d clean rows for IsolationForest (need ≥ %d) — skipping.",
                    symbol, len(sdf_clean), MIN_ROWS,
                )

            # Limit statistical algorithms to scoring window only
            sdf_win = sdf[
                (sdf["date"] >= window_start_str) &
                (sdf["date"] <  window_end_str)
            ].copy()

            if sdf_win.empty:
                continue

            # ── 2. Volume Z-score ─────────────────────────────────────────────
            vol_mean = sdf["volume"].rolling(ROLLING_VOL_DAYS, min_periods=5).mean()
            vol_std  = sdf["volume"].rolling(ROLLING_VOL_DAYS, min_periods=5).std()
            sdf = sdf.copy()
            sdf["_vol_z"] = (sdf["volume"] - vol_mean) / vol_std.replace(0, np.nan)

            sdf_win = sdf[
                (sdf["date"] >= window_start_str) &
                (sdf["date"] <  window_end_str)
            ].copy()

            for _, row in sdf_win.iterrows():
                vol_z = float(row["_vol_z"]) if pd.notna(row["_vol_z"]) else 0.0
                anomaly_records.append({
                    "symbol":       symbol,
                    "date":         row["date"],
                    "anomaly_type": "volume_zscore",
                    "score":        round(vol_z, 4),
                    "threshold":    ZSCORE_THRESHOLD,
                    "is_anomaly":   abs(vol_z) > ZSCORE_THRESHOLD,
                    "detected_at":  detected_at,
                })

            # ── 3. Price IQR ──────────────────────────────────────────────────
            sdf = sdf.copy()
            sdf["_ret"] = sdf["close"].pct_change()

            returns = sdf["_ret"].dropna()
            if len(returns) >= 4:
                q1, q3   = float(returns.quantile(0.25)), float(returns.quantile(0.75))
                iqr      = q3 - q1
                lo_fence = q1 - IQR_MULTIPLIER * iqr
                hi_fence = q3 + IQR_MULTIPLIER * iqr

                sdf_win2 = sdf[
                    (sdf["date"] >= window_start_str) &
                    (sdf["date"] <  window_end_str)
                ].copy()

                for _, row in sdf_win2.iterrows():
                    ret   = float(row["_ret"]) if pd.notna(row["_ret"]) else 0.0
                    flag  = ret < lo_fence or ret > hi_fence
                    dist  = max(lo_fence - ret, ret - hi_fence, 0.0)
                    score = dist / max(iqr, 1e-9)
                    anomaly_records.append({
                        "symbol":       symbol,
                        "date":         row["date"],
                        "anomaly_type": "price_iqr",
                        "score":        round(score, 4),
                        "threshold":    IQR_MULTIPLIER,
                        "is_anomaly":   flag,
                        "detected_at":  detected_at,
                    })

        flagged = sum(1 for r in anomaly_records if r["is_anomaly"])
        log.info(
            "Detection complete: %d records across 3 algorithms, %d flagged (%.1f%%).",
            len(anomaly_records), flagged,
            100 * flagged / len(anomaly_records) if anomaly_records else 0,
        )
        return anomaly_records

    @task()
    def write_anomalies(anomaly_records: list[dict]) -> None:
        """
        Write anomaly records to iceberg.gold.gold_anomaly_flags.

        Idempotent: deletes (date, anomaly_type) pairs before re-writing so
        dbt-computed records for the same date are never disturbed.
        """
        from pyiceberg.expressions import And, EqualTo

        if not anomaly_records:
            log.warning("No anomaly records to write — skipping.")
            return

        catalog = _get_catalog()
        table   = catalog.load_table("gold.gold_anomaly_flags")

        # Group by (date, anomaly_type) for targeted deletes
        by_key: dict[tuple, list[dict]] = {}
        for rec in anomaly_records:
            by_key.setdefault((rec["date"], rec["anomaly_type"]), []).append(rec)

        log.info(
            "Writing %d records across %d (date × anomaly_type) partitions.",
            len(anomaly_records), len(by_key),
        )

        for (date_str, atype), recs in sorted(by_key.items()):
            try:
                table.delete(
                    And(EqualTo("date", date_str), EqualTo("anomaly_type", atype))
                )
            except Exception as exc:
                log.debug("Delete skipped for (%s, %s): %s", date_str, atype, exc)

            converted = [
                {
                    **r,
                    "date":       date_type.fromisoformat(r["date"]),
                    "detected_at": datetime.fromisoformat(r["detected_at"]).astimezone(timezone.utc),
                }
                for r in recs
            ]

            table.append(pa.Table.from_pylist(converted, schema=ANOMALY_SCHEMA))
            log.info("Wrote %d %s records for %s.", len(recs), atype, date_str)

        log.info("Anomaly write complete.")

    # ── Task dependencies ─────────────────────────────────────────────────────
    records         = fetch_features()
    anomaly_records = detect_anomalies(records)
    write_anomalies(anomaly_records)


ml_anomaly_detection_dag()
