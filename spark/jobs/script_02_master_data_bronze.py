# ─────────────────────────────────────────────────────────────────────────────
# Script 2 — Master Data Bronze Processing  (Batch / On-Demand)
#
# Reads the raw JSON files written by Script 1, parses each record against its
# entity schema, and writes Bronze Delta tables. Bronze = parsed raw — no
# business logic, no deduplication. Script 3 handles cleaning and dedup.
#
# This script is idempotent: it always reads the full raw layer and overwrites
# the Bronze Delta tables, so re-running it is safe.
#
# Bronze layer paths:
#   s3a://logistics/bronze/master/fulfillment_centers/
#   s3a://logistics/bronze/master/delivery_stations/
#   s3a://logistics/bronze/master/vehicles/
#
# Submit command:
#   spark-submit \
#     --master spark://spark-master:7077 \
#     --packages <delta>,<hadoop-aws>,<aws-sdk> \
#     /opt/spark/jobs/script_02_master_data_bronze.py
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
from schemas import FULFILLMENT_CENTER_SCHEMA, DELIVERY_STATION_SCHEMA, VEHICLE_SCHEMA

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
    Reads all raw JSON files from `raw_input_path`, parses the `json_payload`
    column using `schema`, and writes a Bronze Delta table to `bronze_output_path`.

    Records that fail JSON parsing (malformed payload → null struct) are dropped
    and logged. Kafka metadata columns are kept for lineage tracing.
    """
    # Read all raw files (all ingested_at partitions)
    raw_df = spark.read.json(raw_input_path)

    # Parse the nested JSON payload string into typed columns
    parsed_df = raw_df.withColumn("parsed", from_json(col("json_payload"), schema)) # It creates one new column "parsed" of type struct, and that struct contains all the fields from the json_payload.

    # Rows where parsing failed produce a null struct — count and drop them
    failed_count = parsed_df.filter(col("parsed").isNull()).count()
    if failed_count > 0:
        logger.warning(f"{entity_name}: {failed_count} record(s) failed JSON parsing — skipped")

    bronze_df = (
        parsed_df
        .filter(col("parsed").isNotNull())
        .select(
            col("parsed.*"),                               # all business fields. "parsed.*" simply breaks the parsed column into multiple columns
            col("kafka_timestamp"),                        # when the producer sent this message
            col("kafka_offset"),                           # exact position in Kafka for replay
            col("kafka_partition"),
            col("ingested_at"),                            # date Script 1 ran
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
    spark = get_spark("Script02_MasterDataBronze")

    logger.info("=" * 60)
    logger.info("Script 2 — Master Data Bronze Processing started")
    logger.info("=" * 60)

    process_to_bronze(
        spark,
        raw_input_path    = config.raw_path("master/fulfillment_centers"),
        bronze_output_path= config.bronze_path("master/fulfillment_centers"),
        schema            = FULFILLMENT_CENTER_SCHEMA,
        entity_name       = "fulfillment_centers",
    )

    process_to_bronze(
        spark,
        raw_input_path    = config.raw_path("master/delivery_stations"),
        bronze_output_path= config.bronze_path("master/delivery_stations"),
        schema            = DELIVERY_STATION_SCHEMA,
        entity_name       = "delivery_stations",
    )

    process_to_bronze(
        spark,
        raw_input_path    = config.raw_path("master/vehicles"),
        bronze_output_path= config.bronze_path("master/vehicles"),
        schema            = VEHICLE_SCHEMA,
        entity_name       = "vehicles",
    )

    logger.info("=" * 60)
    logger.info("Script 2 — Master Data Bronze Processing completed")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
