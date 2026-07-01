# ─────────────────────────────────────────────────────────────────────────────
# Script 5 — Event Ingestion  (Continuous)
#
# Continuously consumes the orders and shipment_events Kafka topics and writes
# every event as immutable raw JSON to MinIO. This is the audit trail and
# replay source for the batch pipeline (Scripts 6, 7, 8).
#
# This is the streaming counterpart of Script 1 — same raw storage format,
# same column structure, but uses readStream instead of read so it runs
# forever and captures events as they arrive.
#
# startingOffsets=earliest ensures no events are missed on the first run.
# On restart, the checkpoint resumes from where it left off automatically.
#
# Raw layer paths:
#   s3a://logistics/raw/events/orders/ingested_at=YYYY-MM-DD/
#   s3a://logistics/raw/events/shipment_events/ingested_at=YYYY-MM-DD/
#
# Prerequisites:
#   - Stream producer must be running so events flow into Kafka.
#   - MinIO bucket "logistics" must exist (create via http://localhost:9001).
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_05_event_ingestion.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, current_date

import config
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ── CORE FUNCTION ─────────────────────────────────────────────────────────────

def ingest_stream(spark: SparkSession, topic: str, output_path: str, checkpoint_path: str) -> StreamingQuery:
    """
    Reads `topic` as a continuous stream and appends raw JSON files to
    `output_path`, partitioned by ingested_at date.

    Stored columns (identical to Script 1 for consistency):
      json_payload     — raw Kafka message value (the producer's JSON string)
      kafka_topic      — source topic name
      kafka_partition  — Kafka partition number
      kafka_offset     — Kafka offset (for exact replay)
      kafka_timestamp  — message timestamp set by the broker
      ingested_at      — date the event was ingested (partition key)
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")   # capture all events including pre-existing ones on first run
        .load()
    )

    stream_df = raw.select(
        col("value").cast("string").alias("json_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_date().alias("ingested_at"),
    )

    return (
        stream_df.writeStream
        .format("json")
        .partitionBy("ingested_at")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")   # write to MinIO every 30 seconds
        .start()
    )


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script05_EventIngestion")

    logger.info("=" * 60)
    logger.info("Script 5 — Event Ingestion started")
    logger.info("=" * 60)

    # Start both streams in parallel — each writes independently to its own path
    orders_query = ingest_stream(
        spark,
        topic           = config.TOPIC_ORDERS,
        output_path     = config.raw_path("events/orders"),
        checkpoint_path = "/tmp/checkpoints/script05_orders",
    )

    events_query = ingest_stream(
        spark,
        topic           = config.TOPIC_SHIPMENT_EVENTS,
        output_path     = config.raw_path("events/shipment_events"),
        checkpoint_path = "/tmp/checkpoints/script05_shipment_events",
    )

    logger.info("Both streams running. Awaiting termination...")

    # Block until both queries stop (or one fails)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
