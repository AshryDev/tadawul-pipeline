# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dual-pipeline financial analytics platform for Saudi Arabia's Tadawul stock exchange:
- **Real-time**: Kafka Producer → Kafka → Spark Structured Streaming → Iceberg `bronze_ticks` (MinIO / Nessie)
- **Batch**: Airflow DAG → Yahoo Finance (yfinance, `.SR` suffix) → Iceberg `bronze_daily_ohlcv` (MinIO / Nessie) → dbt silver/gold models

Both pipelines land in the same Iceberg lakehouse on MinIO, catalogued by Nessie. Trino is the unified query engine. Gold-layer results are synced to Amazon S3.

## Common Commands

### Start infrastructure
```bash
docker compose up -d zookeeper kafka postgres
docker compose up kafka-setup          # creates 4 Kafka topics with 6 partitions each
docker compose up -d                   # starts all remaining services
```

### Run the Kafka producer
```bash
cd producer && pip install -r requirements.txt
python kafka_producer.py
```

### Start the Spark streaming consumer
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/extra/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,/opt/spark/jars/extra/iceberg-aws-bundle-1.4.3.jar \
  /opt/spark-app/streaming_consumer.py
```

### dbt (run inside the dbt container)
```bash
docker exec dbt dbt run --project-dir /usr/dbt --profiles-dir /root/.dbt
docker exec dbt dbt test --project-dir /usr/dbt --profiles-dir /root/.dbt
docker exec dbt dbt source freshness --project-dir /usr/dbt --profiles-dir /root/.dbt
# Run a single model and its dependents
docker exec dbt dbt run --select gold_volatility_index+ --project-dir /usr/dbt --profiles-dir /root/.dbt
```

### Query via Trino
```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.bronze_ticks"
docker exec trino trino --execute "SELECT * FROM iceberg.gold.gold_anomaly_flags WHERE is_anomaly = true LIMIT 10"
```

### Download Spark JARs (required before first run)
```bash
curl -L -o spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar
curl -L -o spark/jars/iceberg-aws-bundle-1.4.3.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar
```

## Architecture & Key Integration Points

### Iceberg catalog: Nessie
All Iceberg tables use `nessie` as the Spark catalog name. The three namespaces are `bronze`, `silver`, `gold`. Trino accesses them under the `iceberg` catalog (configured in `trino/catalog/iceberg.properties`). Nessie stores its catalog state in MongoDB (`nessie` database).

### Object storage: MinIO
MinIO is the local S3-compatible object store. The bucket is `stocks` (`s3a://stocks/`). All services connect with credentials `admin` / `password` (hardcoded in `docker-compose.yml` for local dev). Path-style access must be enabled (`s3.path-style-access=true`) because MinIO doesn't support virtual-hosted-style URLs.

### Spark requires two separate MinIO credential configs
`spark/streaming_consumer.py` must set **both**:
- `spark.hadoop.fs.s3a.*` — Hadoop S3A filesystem (used for checkpoint writes)
- `spark.sql.catalog.nessie.s3.*` — Iceberg S3FileIO (used for table data writes)

Setting only one causes "Access Denied" errors. These are independent credential paths.

### PyIceberg catalog: Nessie REST
Airflow DAGs connect to Nessie via the PyIceberg REST catalog (`type=rest`). The Nessie Iceberg REST endpoint is `http://nessie:19120/iceberg/`. S3 credentials for MinIO are passed as `s3.*` properties inside the catalog config dict.

### PyIceberg idempotency pattern (PyIceberg ≥ 0.6)
Airflow DAGs use delete-then-append for partition-level idempotency:
```python
table.delete(EqualTo("date", date_str))   # remove existing partition
table.append(arrow_table)                  # re-write
```
`table.overwrite()` was removed in PyIceberg 0.6 — do not use it.

### dbt schema separation requires a custom macro
`dbt/macros/generate_schema_name.sql` overrides dbt-trino's default behaviour of prefixing custom schemas with the target schema. Without it, `+schema: gold` becomes `silver_gold` instead of `gold`. This macro must not be removed.

### Trino catalog — Nessie + MinIO (no env var substitution)
`trino/catalog/iceberg.properties` uses hardcoded MinIO credentials (`admin` / `password`) and the Nessie URI directly — credentials are not sensitive for local dev. The catalog type is `nessie` and uses `fs.native-s3.enabled=true` with `s3.*` properties for MinIO access.

### Kafka internal vs external listeners
- Internal (container-to-container): `kafka:29092`
- External (host): `localhost:9092`

The producer running locally uses `localhost:9092`. Services inside Docker (Spark, Airflow) use `kafka:29092`. The `KAFKA_BOOTSTRAP_SERVERS` env var defaults to `kafka:29092` for in-container use.

### Airflow DAG patterns
Both DAGs use the TaskFlow API (`@task` decorator). The `backfill_ohlcv` DAG has `catchup=True` and `start_date=datetime(2021,1,1)` — enabling 3-year historical backfill. The dbt step runs as a `subprocess.run()` call (not BashOperator) so it inherits the container's Python environment where dbt-trino is installed.

### Market hours simulation
The producer treats Tadawul as open **Sunday–Thursday, 10:00–15:00 Riyadh time (UTC+3)**. Python `weekday()` returns `{6,0,1,2,3}` for Sun–Thu. Outside these hours, all ticks use the random-walk simulator with per-symbol base prices seeded in `_BASE_PRICES`.

### dbt model dependency chain
```
sources (bronze_ticks, bronze_daily_ohlcv)
  └── silver_ticks_cleaned, silver_ohlcv, silver_symbols
        └── gold_intraday_vwap, gold_sector_performance
        └── gold_volatility_index
              └── gold_anomaly_flags, gold_52w_levels
```
`gold_volatility_index` pulls extra look-back rows before the incremental cutoff (via `DATE_ADD('day', -25, ...)`) so window functions have sufficient history. `gold_anomaly_flags` does the same with a 95-day buffer.

### Yahoo Finance data availability
Yahoo Finance covers Tadawul stocks using the `.SR` suffix (e.g. `2222.SR`). Data availability varies by symbol and date. The `backfill_ohlcv` DAG handles empty results gracefully (logs a warning, continues) — it does not raise on missing data for older dates or market holidays.

## Environment Setup

Copy `.env.example` to `.env` and populate at minimum:
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET_NAME` — for gold-layer cloud sync to Amazon S3
- `AIRFLOW__CORE__FERNET_KEY` — generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- `AIRFLOW__WEBSERVER__SECRET_KEY`

MinIO, Nessie, and MongoDB credentials are hardcoded in `docker-compose.yml` (`admin`/`password`) and mirrored as defaults in `.env`. No changes needed for local development.

## Service Port Reference

| Service | Port |
|---------|------|
| Airflow UI | 9093 |
| Spark Master UI | 8081 |
| Trino UI | 8080 |
| MinIO Console | 9091 |
| MinIO API | 9000 |
| Nessie API | 19120 |
| MongoDB | 27017 |
| PostgreSQL | 5432 |
| Kafka (external) | 9092 |
