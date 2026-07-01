# ─────────────────────────────────────────────────────────────────────────────
# Script 7 — Event Silver Processing  (Batch / On-Demand)
#
# Reads Bronze Delta tables for orders and shipment_events, applies cleaning,
# deduplication, and type casting, and writes Silver Delta tables.
#
# Silver events are the clean, trusted source used by Script 8 (Gold / BI).
#
# Transformations applied:
#   Orders:
#     - Drop records missing order_id (primary key).
#     - Deduplicate per order_id: keep the record with the latest event_timestamp.
#       (The stream producer emits order updates — Silver retains only the latest.)
#     - Cast event_timestamp and promised_delivery_time to proper TIMESTAMP columns.
#     - Validate: drop records with order_value ≤ 0 (data quality guard).
#
#   Shipment Events:
#     - Drop records missing shipment_id (primary key).
#     - Events are NOT deduplicated — every event is a distinct fact in the
#       timeline. Script 8 uses the full event history for trend analysis.
#     - Cast timestamp columns to proper TIMESTAMP types.
#
# This script is idempotent: overwrites Silver Delta tables on every run.
#
# Silver layer paths:
#   s3a://logistics/silver/events/orders/
#   s3a://logistics/silver/events/shipment_events/
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_07_event_silver.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, row_number, to_timestamp
from pyspark.sql.window import Window

import config
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ISO-8601 format produced by the stream producer
_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


# ── SHARED HELPERS ────────────────────────────────────────────────────────────

def deduplicate(df: DataFrame, id_col: str, order_col: str) -> DataFrame:
    """
    Keeps the single latest record per `id_col` ordered by `order_col` DESC.
    Reuses the same Window dedup pattern as Script 3.
    """
    window = Window.partitionBy(id_col).orderBy(col(order_col).desc())
    return (
        df.withColumn("_rn", row_number().over(window))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )


def drop_bronze_metadata(df: DataFrame) -> DataFrame:
    """Removes Bronze infrastructure columns before writing to Silver."""
    return df.drop("kafka_timestamp", "kafka_offset", "kafka_partition",
                   "ingested_at", "bronze_processed_at")


# ── ENTITY PROCESSORS ─────────────────────────────────────────────────────────

def process_orders(spark: SparkSession) -> None:
    """
    Cleans and deduplicates the orders Bronze table.

    Deduplication keeps the latest event_timestamp per order_id because the
    stream producer can emit multiple updates for the same order (e.g. when
    an order status changes). Silver should reflect the most recent state.
    """
    bronze_df = spark.read.format("delta").load(config.bronze_path("events/orders"))

    silver_df = (
        bronze_df
        .filter(col("order_id").isNotNull())           # drop records missing PK
        .filter(col("order_value") > 0)                # data quality: revenue must be positive
        .transform(lambda df: deduplicate(df, "order_id", "event_timestamp"))
        .transform(drop_bronze_metadata)
        # Cast ISO string timestamps to proper TIMESTAMP type
        .withColumn("event_timestamp",        to_timestamp(col("event_timestamp"),        _TS_FORMAT))
        .withColumn("promised_delivery_time", to_timestamp(col("promised_delivery_time"), _TS_FORMAT))
        .withColumn("silver_processed_at", current_timestamp())
    )

    count = silver_df.count()
    silver_df.write.format("delta").mode("overwrite").save(
        config.silver_path("events/orders")
    )
    logger.info(f"Silver orders: wrote {count} record(s)")


def process_shipment_events(spark: SparkSession) -> None:
    """
    Cleans the shipment_events Bronze table — without deduplication.

    Unlike orders, shipment events are immutable facts in the shipment
    timeline (ORDER_ALLOCATED_TO_FC → PICKED_AND_PACKED → SHIPPED → ...).
    All events are preserved so Script 8 can compute stage durations,
    transition counts, and delivery trend analysis across the full history.
    """
    bronze_df = spark.read.format("delta").load(config.bronze_path("events/shipment_events"))

    silver_df = (
        bronze_df
        .filter(col("shipment_id").isNotNull())        # drop records missing PK
        .transform(drop_bronze_metadata)
        # Cast ISO string timestamps to proper TIMESTAMP type
        .withColumn("event_timestamp",          to_timestamp(col("event_timestamp"),          _TS_FORMAT))
        .withColumn("estimated_delivery_time",  to_timestamp(col("estimated_delivery_time"),  _TS_FORMAT))
        .withColumn("promised_delivery_time",   to_timestamp(col("promised_delivery_time"),   _TS_FORMAT))
        .withColumn("silver_processed_at", current_timestamp())
    )

    count = silver_df.count()
    silver_df.write.format("delta").mode("overwrite").save(
        config.silver_path("events/shipment_events")
    )
    logger.info(f"Silver shipment_events: wrote {count} record(s)")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script07_EventSilver")

    logger.info("=" * 60)
    logger.info("Script 7 — Event Silver Processing started")
    logger.info("=" * 60)

    process_orders(spark)
    process_shipment_events(spark)

    logger.info("=" * 60)
    logger.info("Script 7 — Event Silver Processing completed")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
