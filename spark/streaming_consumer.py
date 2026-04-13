"""
Tadawul Spark Structured Streaming Consumer
============================================
Reads raw tick data from the Kafka topic `tadawul.ticks`, applies schema
validation and watermarking, then writes micro-batches to the Iceberg
bronze_ticks table on AWS S3 via the AWS Glue catalog.

Run inside the Spark container:
    spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/bitnami/spark/jars/extra/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,\
/opt/bitnami/spark/jars/extra/iceberg-aws-bundle-1.4.3.jar \
        /opt/spark-app/streaming_consumer.py

Required environment variables:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION
    S3_BUCKET_NAME
    KAFKA_BOOTSTRAP_SERVERS   (default: kafka:29092)
"""

from __future__ import annotations

import logging
import os
import signal
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("tadawul.spark.consumer")

# ── Configuration ─────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID: str = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY: str = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION: str = os.environ.get("AWS_REGION", "me-south-1")
S3_BUCKET: str = os.environ["S3_BUCKET_NAME"]
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC_TICKS", "tadawul.ticks")
CHECKPOINT_DIR: str = "/tmp/spark-checkpoints/bronze_ticks"
TRIGGER_INTERVAL: str = "10 seconds"
WATERMARK_DELAY: str = "10 seconds"

WAREHOUSE_PATH: str = f"s3://{S3_BUCKET}/warehouse"

# ── Tick schema (matches producer output) ─────────────────────────────────────
TICK_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        # timestamp arrives as ISO-8601 string; we cast to TIMESTAMP below
        StructField("timestamp", StringType(), True),
        StructField("session_status", StringType(), True),
    ]
)

# ── Iceberg table DDL ─────────────────────────────────────────────────────────
CREATE_BRONZE_TICKS_SQL = """
    CREATE TABLE IF NOT EXISTS glue_catalog.bronze.bronze_ticks (
        symbol         STRING        COMMENT 'Tadawul stock code (e.g. 2222)',
        price          DOUBLE        COMMENT 'Last trade price in SAR',
        volume         BIGINT        COMMENT 'Volume for the tick period',
        bid            DOUBLE        COMMENT 'Best bid price',
        ask            DOUBLE        COMMENT 'Best ask price',
        event_time     TIMESTAMP     COMMENT 'Event time in UTC',
        event_date     DATE          COMMENT 'Partition date (derived from event_time)',
        session_status STRING        COMMENT 'live | simulated | pre-market',
        ingestion_time TIMESTAMP     COMMENT 'Wall-clock time written to Iceberg'
    )
    USING iceberg
    PARTITIONED BY (symbol, days(event_time))
    TBLPROPERTIES (
        'write.merge.mode'          = 'merge-on-read',
        'write.update.mode'         = 'merge-on-read',
        'write.parquet.compression-codec' = 'snappy'
    )
"""


# ── SparkSession ──────────────────────────────────────────────────────────────
def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("tadawul-bronze-streaming")
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Glue catalog registration
        .config(
            "spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE_PATH)
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        # ── Iceberg S3FileIO credentials (AWS SDK v2 path) ────────────────────
        .config(
            "spark.sql.catalog.glue_catalog.s3.access-key-id",
            AWS_ACCESS_KEY_ID,
        )
        .config(
            "spark.sql.catalog.glue_catalog.s3.secret-access-key",
            AWS_SECRET_ACCESS_KEY,
        )
        .config("spark.sql.catalog.glue_catalog.s3.region", AWS_REGION)
        # ── Hadoop S3A credentials (for checkpoint writes via hadoop-aws) ──────
        # Both credential sets must be configured independently; one does not
        # satisfy the other.
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # Iceberg Glue region
        .config("spark.sql.catalog.glue_catalog.glue.region", AWS_REGION)
        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


# ── foreachBatch writer ───────────────────────────────────────────────────────
def write_to_bronze(batch_df, batch_id: int) -> None:
    """
    Write a micro-batch to Iceberg bronze_ticks.

    Uses CREATE TABLE IF NOT EXISTS for idempotent schema init, then appends.
    Iceberg append is atomic at the file level; the streaming checkpoint
    provides exactly-once delivery guarantees end-to-end.
    """
    if batch_df.isEmpty():
        log.debug("Batch %d is empty, skipping.", batch_id)
        return

    log.info("Writing batch %d (%d rows) to bronze_ticks …", batch_id, batch_df.count())

    # Ensure the Glue database exists
    batch_df.sparkSession.sql(
        "CREATE DATABASE IF NOT EXISTS glue_catalog.bronze"
    )

    # Idempotent table creation
    batch_df.sparkSession.sql(CREATE_BRONZE_TICKS_SQL)

    # Append with ingestion timestamp, drop the raw string timestamp column
    (
        batch_df.withColumn("ingestion_time", current_timestamp())
        .drop("timestamp")
        .writeTo("glue_catalog.bronze.bronze_ticks")
        .option("mergeSchema", "true")
        .append()
    )

    log.info("Batch %d committed to Iceberg.", batch_id)


# ── Stream setup ──────────────────────────────────────────────────────────────
def build_stream(spark: SparkSession):
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-bronze-consumer")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df.select(
            from_json(col("value").cast("string"), TICK_SCHEMA).alias("d")
        )
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("event_date", to_date(col("event_time")))
        .withWatermark("event_time", WATERMARK_DELAY)
        # Drop rows that failed JSON parsing (all fields null)
        .filter(col("symbol").isNotNull() & col("price").isNotNull())
    )

    return (
        parsed_df.writeStream.foreachBatch(write_to_bronze)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
    )


# ── Graceful shutdown ─────────────────────────────────────────────────────────
_query = None


def _shutdown_handler(sig, frame) -> None:
    log.info("Received signal %d — stopping streaming query …", sig)
    if _query is not None:
        _query.stop()
    sys.exit(0)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    global _query

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    log.info("Building SparkSession …")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "Starting stream | Kafka=%s Topic=%s Checkpoint=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        CHECKPOINT_DIR,
    )

    stream = build_stream(spark)
    _query = stream.start()

    log.info("Stream started. Awaiting termination …")
    _query.awaitTermination()


if __name__ == "__main__":
    main()
