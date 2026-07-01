# ─────────────────────────────────────────────────────────────────────────────
# Script 3 — Master Data Silver Processing  (Batch / On-Demand)
#
# Reads Bronze Delta tables, applies cleaning, validation and deduplication,
# and writes Silver Delta lookup tables. Silver tables are the source of truth
# used by Script 4 (real-time enrichment) and Script 8 (Gold / BI pipeline).
#
# Transformations applied:
#   - Drop records with null primary key.
#   - Deduplicate: per entity ID, keep the record with the latest updated_at.
#     (The batch producer varies daily_capacity on each run to simulate SCD —
#      this ensures Silver always reflects the most recent values.)
#   - Validate: drop records that fail business rules (e.g. capacity ≤ 0,
#     invalid vehicle status).
#   - Keep only clean business columns (drop Kafka metadata from Bronze).
#
# This script is idempotent: overwrites Silver Delta tables on every run.
#
# Silver layer paths:
#   s3a://logistics/silver/master/fulfillment_centers/
#   s3a://logistics/silver/master/delivery_stations/
#   s3a://logistics/silver/master/vehicles/
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_03_master_data_silver.py
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

# Vehicle statuses considered valid by this system
VALID_VEHICLE_STATUSES = {"AVAILABLE", "IN_USE", "MAINTENANCE"}


# ── SHARED HELPERS ────────────────────────────────────────────────────────────

def deduplicate(df: DataFrame, id_col: str) -> DataFrame:
    """
    Keeps the single latest record per `id_col` based on updated_at.
    This handles the case where the batch producer was run more than once,
    producing multiple versions of the same entity in the raw/bronze layers.
    """
    window = Window.partitionBy(id_col).orderBy(col("updated_at").desc())
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

def process_fulfillment_centers(spark: SparkSession) -> None:
    bronze_df = spark.read.format("delta").load(config.bronze_path("master/fulfillment_centers"))

    silver_df = (
        bronze_df
        .filter(col("fulfillment_center_id").isNotNull())   # drop records missing PK
        .filter(col("daily_capacity") > 0)                  # capacity must be positive
        .transform(lambda df: deduplicate(df, "fulfillment_center_id"))
        .transform(drop_bronze_metadata)
        .withColumn("silver_processed_at", current_timestamp())
    )

    count = silver_df.count()
    silver_df.write.format("delta").mode("overwrite").save(
        config.silver_path("master/fulfillment_centers")
    )
    logger.info(f"Silver fulfillment_centers: wrote {count} record(s)")


def process_delivery_stations(spark: SparkSession) -> None:
    bronze_df = spark.read.format("delta").load(config.bronze_path("master/delivery_stations"))

    silver_df = (
        bronze_df
        .filter(col("delivery_station_id").isNotNull())
        .filter(col("daily_capacity") > 0)
        .transform(lambda df: deduplicate(df, "delivery_station_id"))
        .transform(drop_bronze_metadata)
        .withColumn("silver_processed_at", current_timestamp())
    )

    count = silver_df.count()
    silver_df.write.format("delta").mode("overwrite").save(
        config.silver_path("master/delivery_stations")
    )
    logger.info(f"Silver delivery_stations: wrote {count} record(s)")


def process_vehicles(spark: SparkSession) -> None:
    bronze_df = spark.read.format("delta").load(config.bronze_path("master/vehicles"))

    silver_df = (
        bronze_df
        .filter(col("vehicle_id").isNotNull())
        # Only keep records with a recognised status value
        .filter(col("status").isin(list(VALID_VEHICLE_STATUSES)))
        .transform(lambda df: deduplicate(df, "vehicle_id"))
        .transform(drop_bronze_metadata)
        .withColumn("silver_processed_at", current_timestamp())
    )

    count = silver_df.count()
    silver_df.write.format("delta").mode("overwrite").save(
        config.silver_path("master/vehicles")
    )
    logger.info(f"Silver vehicles: wrote {count} record(s)")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script03_MasterDataSilver")

    logger.info("=" * 60)
    logger.info("Script 3 — Master Data Silver Processing started")
    logger.info("=" * 60)

    process_fulfillment_centers(spark)
    process_delivery_stations(spark)
    process_vehicles(spark)

    logger.info("=" * 60)
    logger.info("Script 3 — Master Data Silver Processing completed")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
