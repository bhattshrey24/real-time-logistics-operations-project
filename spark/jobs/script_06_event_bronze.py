# ─────────────────────────────────────────────────────────────────────────────
# Script 6 — Event Bronze Processing  (Batch / On-Demand)
#
# Reads the raw JSON files written continuously by Script 5, parses each record
# against its entity schema, and writes Bronze Delta tables for orders and
# shipment_events.
#
# This is the direct batch counterpart of Script 2 — same pattern, same column
# structure, but targeting the events sub-tree of the storage layer.
#
# No business logic here: Bronze = parsed raw. Cleaning, deduplication, and
# enrichment are handled in Script 7 (Silver).
#
# This script is idempotent: re-reading the full raw layer and overwriting
# Bronze Delta tables is always safe.
#
# Bronze layer paths:
#   s3a://logistics/bronze/events/orders/
#   s3a://logistics/bronze/events/shipment_events/
#
# Prerequisites:
#   - Script 5 must have been running long enough to write raw files to MinIO.
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_06_event_bronze.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType

import config
from spark_session import get_spark
from schemas import ORDER_SCHEMA, SHIPMENT_EVENT_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ── CORE FUNCTION ─────────────────────────────────────────────────────────────

def process_to_bronze(
    spark: SparkSession,
    raw_input_path: str,
    bronze_output_path: str,
    schema: StructType,
    entity_name: str,
) -> None:
    """
    Reads all raw JSON partitions from `raw_input_path`, parses the json_payload
    column using `schema`, and writes a Bronze Delta table to `bronze_output_path`.

    Records that fail JSON parsing (malformed payload → null struct) are dropped.
    Kafka metadata columns are kept for lineage tracing.

    Identical pattern to Script 2's process_to_bronze() — same columns, same logic.
    """
    raw_df = spark.read.json(raw_input_path)

    parsed_df = raw_df.withColumn("parsed", from_json(col("json_payload"), schema))

    failed_count = parsed_df.filter(col("parsed").isNull()).count()
    if failed_count > 0:
        logger.warning(f"{entity_name}: {failed_count} record(s) failed JSON parsing — skipped")

    bronze_df = (
        parsed_df
        .filter(col("parsed").isNotNull())
        .select(
            col("parsed.*"),                                  # all business fields
            col("kafka_timestamp"),                           # broker timestamp
            col("kafka_offset"),                              # exact Kafka position for replay
            col("kafka_partition"),
            col("ingested_at"),                               # date Script 5 wrote this file
            current_timestamp().alias("bronze_processed_at"),
        )
    )

    count = bronze_df.count()

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .save(bronze_output_path)
    )

    logger.info(f"Bronze {entity_name}: wrote {count} record(s) → {bronze_output_path}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script06_EventBronze")

    logger.info("=" * 60)
    logger.info("Script 6 — Event Bronze Processing started")
    logger.info("=" * 60)

    process_to_bronze(
        spark,
        raw_input_path     = config.raw_path("events/orders"),
        bronze_output_path = config.bronze_path("events/orders"),
        schema             = ORDER_SCHEMA,
        entity_name        = "orders",
    )

    process_to_bronze(
        spark,
        raw_input_path     = config.raw_path("events/shipment_events"),
        bronze_output_path = config.bronze_path("events/shipment_events"),
        schema             = SHIPMENT_EVENT_SCHEMA,
        entity_name        = "shipment_events",
    )

    logger.info("=" * 60)
    logger.info("Script 6 — Event Bronze Processing completed")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
