"""
Bulk historical backfill for bronze_daily_ohlcv.

Fetches ALL symbols in one yf.download() call (no looping over dates),
then writes everything as a single Arrow table append — replacing the
DAG's per-date delete+append pattern that causes thousands of Iceberg
round-trips.

Usage (run from inside the Airflow container):
    python /opt/airflow/scripts/bulk_backfill.py [--start 2021-01-01] [--end 2026-04-19]

Or from host (with correct env vars):
    docker exec airflow-webserver python /opt/airflow/scripts/bulk_backfill.py
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

SYMBOLS: list[str] = [
    s.strip()
    for s in os.getenv(
        "SYMBOLS", "2222,1010,2010,7010,1120,4280,2380,8010,4003,2060"
    ).split(",")
    if s.strip()
]

BRONZE_OHLCV_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("date", pa.string(), nullable=False),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("vwap", pa.float64()),
        pa.field("transactions", pa.int64()),
        pa.field("ingestion_time", pa.timestamp("us")),
    ]
)


def fetch_all(start: str, end: str) -> list[dict]:
    yf_tickers = [f"{s}.SR" for s in SYMBOLS]
    ingestion_time = datetime.now(tz=timezone.utc).replace(tzinfo=None)

    log.info("Downloading %d symbols: %s → %s", len(yf_tickers), start, end)

    raw: pd.DataFrame = yf.download(
        tickers=yf_tickers,
        start=start,
        end=end,
        auto_adjust=True,
        group_by="ticker",
        threads=True,
        progress=True,
    )

    if raw.empty:
        log.warning("yf.download() returned no data.")
        return []

    records: list[dict] = []
    for symbol in SYMBOLS:
        yf_ticker = f"{symbol}.SR"
        try:
            df = raw[yf_ticker] if len(yf_tickers) > 1 else raw
        except KeyError:
            log.warning("No column for %s — skipping.", yf_ticker)
            continue

        if df.empty:
            log.warning("Empty data for %s — skipping.", yf_ticker)
            continue

        for ts, row in df.iterrows():
            if pd.isna(row.get("Open")):
                continue
            o = float(row["Open"])
            h = float(row["High"])
            l = float(row["Low"])
            c = float(row["Close"])
            v = int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
            records.append(
                {
                    "symbol": symbol,
                    "date": ts.strftime("%Y-%m-%d"),
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                    "vwap": round((o + h + l + c) / 4, 4),
                    "transactions": 0,
                    "ingestion_time": ingestion_time,
                }
            )

    log.info("Collected %d records total.", len(records))
    return records


def _get_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/"),
            "warehouse": os.environ.get("NESSIE_WAREHOUSE", "stocks"),
            "s3.endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("MINIO_ACCESS_KEY", "admin"),
            "s3.secret-access-key": os.environ.get("MINIO_SECRET_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region": os.environ.get("MINIO_REGION", "eu-south-1"),
        },
    )


def _ensure_table(catalog):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DoubleType, LongType, NestedField, StringType, TimestampType,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    try:
        catalog.create_namespace("bronze")
    except Exception:
        pass

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
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="date")
    )
    return catalog.create_table(
        identifier="bronze.bronze_daily_ohlcv",
        schema=schema,
        partition_spec=partition_spec,
    )


def write_bulk(records: list[dict], truncate: bool) -> None:
    if not records:
        log.warning("No records to write.")
        return

    catalog = _get_catalog()
    table = _ensure_table(catalog)

    if truncate:
        log.info("Truncating existing bronze_daily_ohlcv data.")
        # Delete all rows — faster than per-date deletes
        try:
            table.delete("1=1")
        except Exception as exc:
            log.warning("Truncate via delete('1=1') failed (%s); continuing anyway.", exc)

    arrow_table = pa.Table.from_pylist(records, schema=BRONZE_OHLCV_SCHEMA)
    log.info("Appending %d rows in one shot…", len(arrow_table))
    table.append(arrow_table)
    log.info("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk-backfill bronze_daily_ohlcv")
    parser.add_argument("--start", default="2021-01-01", help="Start date (inclusive), YYYY-MM-DD")
    parser.add_argument(
        "--end",
        default=(datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d"),
        help="End date (exclusive for yfinance), YYYY-MM-DD",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Skip truncating existing data (append-only mode)",
    )
    args = parser.parse_args()

    records = fetch_all(args.start, args.end)
    write_bulk(records, truncate=not args.no_truncate)


if __name__ == "__main__":
    main()
