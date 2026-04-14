"""
Airflow DAG: ml_anomaly_detection
===================================
Daily ML pipeline that reads gold-layer metrics from Trino, runs two
anomaly detection algorithms in Python, then writes results to the
Iceberg gold_anomaly_flags table (queryable via Trino).

Algorithms:
  - Volume Z-score: flag when (volume - 30d_mean) / 30d_std > 2.5
  - Price IQR:      flag when daily_return falls outside Q1 - 1.5*IQR
                    or Q3 + 1.5*IQR computed over the trailing 90 days

This DAG is intended to run after backfill_ohlcv completes for the day.
"""

from __future__ import annotations

import logging
import os
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import trino
from airflow.decorators import dag, task

warnings.filterwarnings("ignore", category=FutureWarning)

log = logging.getLogger(__name__)

SYMBOLS: list[str] = [
    s.strip()
    for s in os.getenv(
        "SYMBOLS", "2222,1010,2010,7010,1120,4280,2380,8010,4003,2060"
    ).split(",")
    if s.strip()
]

ZSCORE_THRESHOLD: float = 2.5
IQR_MULTIPLIER: float = 1.5
ROLLING_VOLUME_DAYS: int = 30
ROLLING_RETURN_DAYS: int = 90


# ── Trino connection ──────────────────────────────────────────────────────────
def _get_trino_conn():
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", 8080)),
        user="airflow",
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        schema="gold",
        http_scheme="http",
    )


# ── PyIceberg catalog ─────────────────────────────────────────────────────────
def _get_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/"),
            "warehouse": os.environ.get("MINIO_WAREHOUSE", "s3a://stocks/"),
            "s3.endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("MINIO_ACCESS_KEY", "admin"),
            "s3.secret-access-key": os.environ.get("MINIO_SECRET_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region": os.environ.get("MINIO_REGION", "eu-south-1"),
        },
    )


def _ensure_anomaly_flags_table(catalog) -> Any:
    """Create gold_anomaly_flags Iceberg table if it doesn't exist."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BooleanType,
        DoubleType,
        NestedField,
        StringType,
        TimestampType,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    try:
        catalog.create_namespace("gold")
    except Exception:
        pass

    try:
        return catalog.load_table("gold.gold_anomaly_flags")
    except Exception:
        pass

    schema = Schema(
        NestedField(1, "symbol", StringType(), required=True),
        NestedField(2, "date", StringType(), required=True),
        NestedField(3, "anomaly_type", StringType(), required=True),
        NestedField(4, "score", DoubleType()),
        NestedField(5, "threshold", DoubleType()),
        NestedField(6, "is_anomaly", BooleanType()),
        NestedField(7, "detected_at", TimestampType()),
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=2, field_id=1000, transform=IdentityTransform(), name="date"
        )
    )

    return catalog.create_table(
        identifier="gold.gold_anomaly_flags",
        schema=schema,
        partition_spec=partition_spec,
    )


# ── DAG tasks ─────────────────────────────────────────────────────────────────
@dag(
    dag_id="ml_anomaly_detection",
    description="Daily anomaly detection on gold-layer metrics → Iceberg",
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=True,
    max_active_runs=1,
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
    def read_gold_data(execution_date=None) -> dict:
        """
        Query Trino for gold_volatility_index and silver_ohlcv data.
        Returns a dict of serialisable lists (not DataFrames, for XCom compat).
        """
        conn = _get_trino_conn()
        cursor = conn.cursor()

        # Fetch volatility + volume data (trailing 90 days from exec date)
        exec_date_str = execution_date.strftime("%Y-%m-%d")
        cutoff_str = (execution_date - timedelta(days=ROLLING_RETURN_DAYS)).strftime("%Y-%m-%d")

        volatility_sql = f"""
            SELECT
                v.symbol,
                v.date,
                v.annualized_vol,
                o.volume,
                o.close,
                o.open
            FROM iceberg.gold.gold_volatility_index v
            JOIN iceberg.silver.silver_ohlcv o
                ON v.symbol = o.symbol AND v.date = o.date
            WHERE v.date BETWEEN DATE '{cutoff_str}' AND DATE '{exec_date_str}'
              AND v.symbol IN ({','.join(f"'{s}'" for s in SYMBOLS)})
            ORDER BY v.symbol, v.date
        """

        log.info("Querying Trino for volatility data (%s → %s) …", cutoff_str, exec_date_str)
        cursor.execute(volatility_sql)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        records = [dict(zip(cols, row)) for row in rows]
        log.info("Fetched %d rows from Trino.", len(records))

        if not records:
            log.warning("No gold data found for period %s → %s.", cutoff_str, exec_date_str)

        return {"volatility": records, "execution_date": exec_date_str}

    @task()
    def compute_anomalies(gold_data: dict) -> list[dict]:
        """
        Run Z-score (volume) and IQR (price return) anomaly detection.

        Returns a list of anomaly records for the execution date.
        """
        records = gold_data["volatility"]
        exec_date_str = gold_data["execution_date"]

        if not records:
            log.warning("No input data — returning empty anomaly list.")
            return []

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        # Compute daily return
        df["daily_return"] = df.groupby("symbol")["close"].pct_change()

        anomalies: list[dict] = []
        exec_date = pd.Timestamp(exec_date_str)

        for symbol, sym_df in df.groupby("symbol"):
            sym_df = sym_df.copy().reset_index(drop=True)

            # ── Volume Z-score ────────────────────────────────────────────────
            vol_mean = (
                sym_df["volume"]
                .rolling(ROLLING_VOLUME_DAYS, min_periods=5)
                .mean()
            )
            vol_std = (
                sym_df["volume"]
                .rolling(ROLLING_VOLUME_DAYS, min_periods=5)
                .std()
            )
            sym_df["vol_zscore"] = (sym_df["volume"] - vol_mean) / vol_std.replace(0, np.nan)

            # ── Price return IQR ──────────────────────────────────────────────
            returns = sym_df["daily_return"].dropna()
            if len(returns) >= 4:
                q1 = float(returns.quantile(0.25))
                q3 = float(returns.quantile(0.75))
                iqr = q3 - q1
                lower_fence = q1 - IQR_MULTIPLIER * iqr
                upper_fence = q3 + IQR_MULTIPLIER * iqr
            else:
                lower_fence = upper_fence = None

            # Emit anomaly records for execution date only
            today_rows = sym_df[sym_df["date"].dt.date == exec_date.date()]
            if today_rows.empty:
                continue

            row = today_rows.iloc[-1]
            date_str = row["date"].strftime("%Y-%m-%d")
            detected_at = datetime.now(timezone.utc)

            # Volume anomaly
            vol_z = float(row["vol_zscore"]) if pd.notna(row["vol_zscore"]) else 0.0
            anomalies.append(
                {
                    "symbol": str(symbol),
                    "date": date_str,
                    "anomaly_type": "volume_zscore",
                    "score": round(vol_z, 4),
                    "threshold": ZSCORE_THRESHOLD,
                    "is_anomaly": abs(vol_z) > ZSCORE_THRESHOLD,
                    "detected_at": detected_at.isoformat(),
                }
            )

            # Price IQR anomaly
            daily_ret = float(row["daily_return"]) if pd.notna(row["daily_return"]) else 0.0
            if lower_fence is not None:
                iqr_flag = daily_ret < lower_fence or daily_ret > upper_fence
                # Score as distance from nearest fence, normalised by IQR
                distance = max(lower_fence - daily_ret, daily_ret - upper_fence, 0.0)
                iqr_score = distance / max(iqr, 1e-9)
                anomalies.append(
                    {
                        "symbol": str(symbol),
                        "date": date_str,
                        "anomaly_type": "price_iqr",
                        "score": round(iqr_score, 4),
                        "threshold": IQR_MULTIPLIER,
                        "is_anomaly": iqr_flag,
                        "detected_at": detected_at.isoformat(),
                    }
                )

        flagged = sum(1 for a in anomalies if a["is_anomaly"])
        log.info(
            "Computed %d anomaly records for %s (%d flagged).",
            len(anomalies),
            exec_date_str,
            flagged,
        )
        return anomalies

    @task()
    def write_anomaly_flags(anomalies: list[dict]) -> None:
        """Append anomaly records to Iceberg gold_anomaly_flags."""
        import pyarrow as pa
        from pyiceberg.expressions import EqualTo

        if not anomalies:
            log.info("No anomalies to write.")
            return

        catalog = _get_catalog()
        table = _ensure_anomaly_flags_table(catalog)

        date_str = anomalies[0]["date"]

        # Delete existing records for this date (idempotency)
        try:
            table.delete(EqualTo("date", date_str))
        except Exception as exc:
            log.debug("Delete partition skipped: %s", exc)

        schema = pa.schema(
            [
                pa.field("symbol", pa.string()),
                pa.field("date", pa.string()),
                pa.field("anomaly_type", pa.string()),
                pa.field("score", pa.float64()),
                pa.field("threshold", pa.float64()),
                pa.field("is_anomaly", pa.bool_()),
                pa.field("detected_at", pa.timestamp("us", tz="UTC")),
            ]
        )

        # Parse detected_at back to datetime for PyArrow
        rows = [
            {**a, "detected_at": datetime.fromisoformat(a["detected_at"])}
            for a in anomalies
        ]

        arrow_table = pa.Table.from_pylist(rows, schema=schema)
        table.append(arrow_table)

        log.info("Wrote %d anomaly records to gold_anomaly_flags.", len(anomalies))

    # ── Task dependencies ─────────────────────────────────────────────────────
    gold_data = read_gold_data()
    anomalies = compute_anomalies(gold_data)
    write_anomaly_flags(anomalies)


ml_anomaly_detection_dag()
