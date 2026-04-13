"""
Airflow DAG: backfill_ohlcv
============================
Daily batch DAG that fetches daily OHLCV data from Polygon.io for each
Tadawul symbol and writes it to the Iceberg bronze_daily_ohlcv table on
AWS S3 via the AWS Glue catalog.

After ingestion, triggers dbt silver + gold model refresh.

Idempotency: Deletes the partition for the execution date before re-writing,
so re-running for the same date is always safe.

Note: Polygon.io free tier returns ~2 years of historical data. Requests for
older dates will return empty results; the DAG logs a warning and continues
rather than failing.
"""

from __future__ import annotations

import logging
import os
import subprocess
import shlex
from datetime import datetime, timedelta
from typing import Any

import pyarrow as pa
import requests
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# ── Symbols ───────────────────────────────────────────────────────────────────
SYMBOLS: list[str] = [
    s.strip()
    for s in os.getenv(
        "SYMBOLS", "2222,1010,2010,7010,1120,4280,2380,8010,4003,2060"
    ).split(",")
    if s.strip()
]

POLYGON_BASE = "https://api.polygon.io/v2/aggs/ticker"

# ── PyArrow schema for bronze_daily_ohlcv ────────────────────────────────────
BRONZE_OHLCV_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("date", pa.string()),          # ISO-8601 date string
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("vwap", pa.float64()),
        pa.field("transactions", pa.int64()),
        pa.field("ingestion_time", pa.timestamp("us", tz="UTC")),
    ]
)


# ── Polygon helpers ───────────────────────────────────────────────────────────
def _fetch_ohlcv_for_symbol(symbol: str, date_str: str) -> dict | None:
    """
    Fetch one day of OHLCV data from Polygon for a Tadawul symbol.
    Returns a single record dict or None if data is unavailable.
    """
    api_key = os.environ["POLYGON_API_KEY"]
    url = f"{POLYGON_BASE}/X:{symbol}/range/1/day/{date_str}/{date_str}"
    params = {"adjusted": "true", "sort": "asc", "apiKey": api_key}

    retry_delay = 2.0
    while True:
        try:
            resp = requests.get(url, params=params, timeout=15)
        except requests.RequestException as exc:
            log.warning("Network error fetching %s on %s: %s", symbol, date_str, exc)
            return None

        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if not results:
                log.warning(
                    "No data from Polygon for %s on %s (free-tier limit or market closed).",
                    symbol,
                    date_str,
                )
                return None
            bar = results[0]
            return {
                "symbol": symbol,
                "date": date_str,
                "open": float(bar.get("o", 0.0)),
                "high": float(bar.get("h", 0.0)),
                "low": float(bar.get("l", 0.0)),
                "close": float(bar.get("c", 0.0)),
                "volume": int(bar.get("v", 0)),
                "vwap": float(bar.get("vw", 0.0)),
                "transactions": int(bar.get("n", 0)),
                "ingestion_time": datetime.utcnow(),
            }

        if resp.status_code == 429:
            log.warning("Rate-limited by Polygon. Retrying in %.1fs …", retry_delay)
            import time
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2.0, 60.0)
            continue

        log.warning(
            "Polygon returned %d for %s on %s — skipping.",
            resp.status_code,
            symbol,
            date_str,
        )
        return None


# ── PyIceberg catalog helper ──────────────────────────────────────────────────
def _get_catalog():
    """Return a PyIceberg GlueCatalog instance using env-var credentials."""
    from pyiceberg.catalog.glue import GlueCatalog

    return GlueCatalog(
        name="glue",
        **{
            "region_name": os.environ["AWS_REGION"],
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "warehouse": f"s3://{os.environ['S3_BUCKET_NAME']}/warehouse",
        },
    )


def _ensure_bronze_ohlcv_table(catalog) -> Any:
    """Create the bronze_daily_ohlcv Iceberg table if it doesn't exist."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DoubleType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    try:
        catalog.create_namespace("bronze")
    except Exception:
        pass  # namespace already exists

    try:
        return catalog.load_table("bronze.bronze_daily_ohlcv")
    except Exception:
        pass

    schema = Schema(
        NestedField(1, "symbol", StringType(), required=True),
        NestedField(2, "date", StringType(), required=True),
        NestedField(3, "open", DoubleType()),
        NestedField(4, "high", DoubleType()),
        NestedField(5, "low", DoubleType()),
        NestedField(6, "close", DoubleType()),
        NestedField(7, "volume", LongType()),
        NestedField(8, "vwap", DoubleType()),
        NestedField(9, "transactions", LongType()),
        NestedField(10, "ingestion_time", TimestampType()),
    )

    # Partition by date (identity transform on string date)
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="date")
    )

    return catalog.create_table(
        identifier="bronze.bronze_daily_ohlcv",
        schema=schema,
        partition_spec=partition_spec,
    )


# ── DAG definition ────────────────────────────────────────────────────────────
@dag(
    dag_id="backfill_ohlcv",
    description="Daily OHLCV ingestion from Polygon.io → Iceberg bronze_daily_ohlcv → dbt silver/gold",
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=True,
    max_active_runs=3,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["tadawul", "bronze", "batch", "polygon"],
)
def backfill_ohlcv_dag():

    @task()
    def fetch_ohlcv(execution_date=None) -> list[dict]:
        """Fetch OHLCV for all symbols for the DAG execution date."""
        # execution_date is the start of the schedule interval (previous day)
        date_str = (execution_date + timedelta(days=0)).strftime("%Y-%m-%d")
        log.info("Fetching OHLCV for date %s across %d symbols.", date_str, len(SYMBOLS))

        records: list[dict] = []
        for symbol in SYMBOLS:
            record = _fetch_ohlcv_for_symbol(symbol, date_str)
            if record:
                records.append(record)

        log.info("Fetched %d records for %s.", len(records), date_str)
        return records

    @task()
    def write_bronze(records: list[dict], execution_date=None) -> None:
        """
        Write records to Iceberg bronze_daily_ohlcv.

        Idempotent: deletes the partition for this date before appending,
        so re-running for the same date produces correct results.
        """
        from pyiceberg.expressions import EqualTo

        if not records:
            log.warning("No records to write — skipping bronze write.")
            return

        date_str = records[0]["date"]
        log.info("Writing %d records for %s to bronze_daily_ohlcv.", len(records), date_str)

        catalog = _get_catalog()
        table = _ensure_bronze_ohlcv_table(catalog)

        # Delete existing partition for this date (idempotency)
        try:
            table.delete(EqualTo("date", date_str))
            log.info("Deleted existing partition for %s.", date_str)
        except Exception as exc:
            log.debug("Delete partition skipped (may not exist): %s", exc)

        # Convert to PyArrow table and append
        arrow_table = pa.Table.from_pylist(records, schema=BRONZE_OHLCV_SCHEMA)
        table.append(arrow_table)

        log.info("Successfully appended %d rows to bronze_daily_ohlcv.", len(records))

    @task()
    def run_dbt() -> None:
        """
        Run dbt silver and gold models.

        Invokes dbt as a subprocess so it runs in the same Python environment
        where dbt-trino is installed (inside the Airflow container).
        """
        cmd = (
            "dbt run "
            "--select silver+ gold+ "
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt"
        )
        log.info("Running: %s", cmd)

        result = subprocess.run(
            shlex.split(cmd),
            capture_output=True,
            text=True,
            cwd="/opt/airflow/dbt",
        )

        if result.stdout:
            log.info("dbt stdout:\n%s", result.stdout)
        if result.stderr:
            log.warning("dbt stderr:\n%s", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"dbt run failed with exit code {result.returncode}.\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )

        log.info("dbt run completed successfully.")

    # ── Task dependencies ─────────────────────────────────────────────────────
    records = fetch_ohlcv()
    write_bronze(records)
    run_dbt()


backfill_ohlcv_dag()
