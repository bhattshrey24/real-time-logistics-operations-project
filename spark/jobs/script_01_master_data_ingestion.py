# ─────────────────────────────────────────────────────────────────────────────
# Script 1 — Master Data Ingestion  (Batch / On-Demand)
#
# Reads all available messages from the fulfillment_centers, delivery_stations
# and vehicles Kafka topics and writes each record as immutable raw JSON to
# MinIO. Running this script multiple times is safe — each run appends a new
# dated partition, preserving the full history and enabling replay.
#
# Raw layer path:
#   s3a://logistics/raw/master/<entity>/ingested_at=YYYY-MM-DD/
#
# Prerequisites:
#   1. Kafka topics must contain messages (run batch_producer.py first).
#   2. The MinIO bucket "logistics" must exist (create via http://localhost:9001).
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_01_master_data_ingestion.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

# Make the common/ package importable whether run locally or on the cluster.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))

import logging
from pyspark.sql.functions import col, current_date

import config
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ── CORE FUNCTION ─────────────────────────────────────────────────────────────

def ingest_topic(spark, topic: str, output_path: str) -> None:
    """
    Reads every message currently in `topic` (earliest → latest, one-shot batch)
    and appends raw JSON files to `output_path`, partitioned by ingested_at date.

    Stored columns:
      json_payload     — the raw Kafka message value (the producer's JSON string)
      kafka_topic      — source topic name
      kafka_partition  — Kafka partition number
      kafka_offset     — Kafka offset (for exact replay)
      kafka_timestamp  — message timestamp set by the broker
      ingested_at      — date this script ran (used as partition key)
    """
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    raw_df = df.select(
        col("value").cast("string").alias("json_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_date().alias("ingested_at"),
    )

    count = raw_df.count()

    (
        raw_df.write
        .mode("append")
        .partitionBy("ingested_at")
        .json(output_path)
    )

    logger.info(f"Ingested {count} record(s) | topic={topic} → {output_path}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script01_MasterDataIngestion") # Different app names gives each script its own name in Spark's Web UI and History Server, making it easy to distinguish jobs when monitoring.

    logger.info("=" * 60)
    logger.info("Script 1 — Master Data Ingestion started")
    logger.info("=" * 60)

    ingest_topic(spark, config.TOPIC_FULFILLMENT_CENTERS, config.raw_path("master/fulfillment_centers"))
    ingest_topic(spark, config.TOPIC_DELIVERY_STATIONS,   config.raw_path("master/delivery_stations"))
    ingest_topic(spark, config.TOPIC_VEHICLES,            config.raw_path("master/vehicles"))

    logger.info("=" * 60)
    logger.info("Script 1 — Master Data Ingestion completed")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
