"""
Tadawul Spark Structured Streaming Consumer
============================================
Reads raw tick data from the Kafka topic `tadawul.ticks`, applies schema
validation and watermarking, then writes micro-batches to the Iceberg
bronze_ticks table on MinIO via the Nessie catalog.

Run inside the Spark container:
    spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/spark/jars/extra/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,\
/opt/spark/jars/extra/iceberg-aws-bundle-1.4.3.jar \
        /opt/spark-app/streaming_consumer.py

Environment variables (all have sensible defaults for the Docker Compose setup):
    MINIO_ENDPOINT            (default: http://minio:9000)
    MINIO_ACCESS_KEY          (default: admin)
    MINIO_SECRET_KEY          (default: password)
    MINIO_REGION              (default: eu-south-1)
    NESSIE_URI                (default: http://nessie:19120/iceberg/)
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
MINIO_ENDPOINT: str = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY: str = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY: str = os.environ.get("MINIO_SECRET_KEY", "password")
MINIO_REGION: str = os.environ.get("MINIO_REGION", "eu-south-1")
NESSIE_URI: str = os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/")
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC_TICKS", "tadawul.ticks")
CHECKPOINT_DIR: str = "/tmp/spark-checkpoints/bronze_ticks"
TRIGGER_INTERVAL: str = "10 seconds"
WATERMARK_DELAY: str = "10 seconds"

WAREHOUSE_PATH: str = "s3a://stocks/"

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
    CREATE TABLE IF NOT EXISTS nessie.bronze.bronze_ticks (
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
        # ── Nessie REST catalog ───────────────────────────────────────────────
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.rest.RESTCatalog",
        )
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE_PATH)
        .config(
            "spark.sql.catalog.nessie.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        # ── Iceberg S3FileIO → MinIO ──────────────────────────────────────────
        .config("spark.sql.catalog.nessie.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.region", MINIO_REGION)
        # ── Hadoop S3A → MinIO (for checkpoint writes) ────────────────────────
        # Must be configured independently from Iceberg S3FileIO.
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
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

    # Ensure the namespace exists
    batch_df.sparkSession.sql(
        "CREATE NAMESPACE IF NOT EXISTS nessie.bronze"
    )

    # Idempotent table creation
    batch_df.sparkSession.sql(CREATE_BRONZE_TICKS_SQL)

    # Append with ingestion timestamp, drop the raw string timestamp column
    (
        batch_df.withColumn("ingestion_time", current_timestamp())
        .drop("timestamp")
        .writeTo("nessie.bronze.bronze_ticks")
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
