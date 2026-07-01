# ─────────────────────────────────────────────────────────────────────────────
# Script 8 — Gold Processing  (Batch / On-Demand)
#
# Builds the Gold analytical layer from Silver tables. Produces:
#   Dimensions (SCD2):
#     dim_date               — date attributes for trend analysis (no SCD2)
#     dim_fulfillment_center — FC attributes; tracks daily_capacity changes
#     dim_delivery_station   — DS attributes; tracks daily_capacity changes
#     dim_vehicle            — Vehicle attributes; tracks status changes
#     dim_customer           — Customer tier; tracks STANDARD → PRIME upgrades
#
#   Fact (overwrite every run):
#     fact_shipment          — one row per shipment with all measures
#
# fact_shipment measures:
#   lead_time_hours     — end-to-end time from FC receipt to delivery
#   fc_time_minutes     — time spent processing inside the Fulfillment Center
#   ds_time_minutes     — time spent processing inside the Delivery Station
#   sla_breached        — True if delivered after promised_delivery_time
#   transportation_cost — estimated flat cost by vehicle type
#   sla_penalty_amount  — delay_minutes × rate (SAME_DAY=2.0, STANDARD=0.5)
#   refund_amount       — % of order_value refunded when SLA breached
#
# Run order: Scripts 1-3 (master Silver), Scripts 6-7 (event Silver) must run first.
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_08_gold_processing.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))

import logging
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, md5, concat_ws, to_date, current_timestamp, current_date,
    row_number, when, unix_timestamp,
    date_format, dayofmonth, month, quarter, year, weekofyear, dayofweek,
    round as spark_round,
    min as spark_min,
    max as spark_max,
)
from pyspark.sql.window import Window

import config
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ── COST / PENALTY CONSTANTS ──────────────────────────────────────────────────
# Used to derive financial measures in fact_shipment.
# These are estimates — replace with real rate tables if available.

_TRANSPORT_COST = {"TRUCK": 100.0, "VAN": 50.0, "BIKE": 20.0}   # flat cost per shipment
_PENALTY_RATE   = {"SAME_DAY": 2.0, "STANDARD": 0.5}             # $ per minute of delay
_REFUND_RATE    = {"SAME_DAY": 0.15, "STANDARD": 0.05}           # % of order_value refunded on SLA breach


# ── SCD2 SHARED HELPER ────────────────────────────────────────────────────────

def build_scd2_dim(
    spark: SparkSession,
    incoming_df: DataFrame,
    natural_key: str,
    tracked_cols: list,
    output_path: str,
    entity_name: str,
) -> None:
    """
    Writes or updates a Gold SCD2 dimension table.

    Adds these control columns to incoming_df:
      surrogate_key — deterministic MD5 PK = hash(natural_key + effective_from)
      row_hash      — MD5 of tracked_cols; used to detect whether attributes changed
      effective_from — date this version became active
      effective_to   — 9999-12-31 while active; closed to today when superseded
      is_current     — True for the single active version per natural key

    First run: all records inserted as current.
    Subsequent runs:
      - New records                  → inserted as current
      - Records with changed values  → old row closed, new row inserted
      - Unchanged records            → left untouched (no write)
    """
    today_str = date.today().isoformat()

    staged = (
        incoming_df
        .withColumn("row_hash",
            md5(concat_ws("|", *[col(c).cast("string") for c in tracked_cols])))
        .withColumn("effective_from", to_date(lit(today_str)))
        .withColumn("effective_to",   to_date(lit("9999-12-31")))
        .withColumn("is_current",     lit(True))
        # Deterministic surrogate key: unique per (entity, version date)
        .withColumn("surrogate_key",
            md5(concat_ws("|", col(natural_key), lit(today_str))))
    )

    # Check whether the Gold dim table already exists
    try:
        existing = spark.read.format("delta").load(output_path)
        gold_exists = True
    except Exception:
        gold_exists = False

    if not gold_exists:
        # First run — write everything as current
        count = staged.count()
        staged.write.format("delta").mode("overwrite").save(output_path)
        logger.info(f"Gold {entity_name}: created — {count} record(s)")
        return

    # ── SCD2 update logic (pure read-transform-write, no MERGE needed) ─────────
    # Split existing rows into three groups based on whether they changed:
    #   untouched        — natural key not in the incoming data change set
    #   changed_history  — older (already-closed) versions of changed keys; keep as-is
    #   changed_to_close — current version of changed keys; mark as closed today

    current_gold  = existing.filter(col("is_current"))
    changed_keys  = (
        staged
        .join(current_gold.select(natural_key, col("row_hash").alias("old_hash")), natural_key)
        .filter(col("row_hash") != col("old_hash"))
        .select(natural_key)
    )

    untouched        = existing.join(changed_keys, natural_key, "left_anti")
    changed_history  = existing.join(changed_keys, natural_key, "inner").filter(~col("is_current"))
    changed_to_close = (
        existing.join(changed_keys, natural_key, "inner")
        .filter(col("is_current"))
        .withColumn("is_current",   lit(False))
        .withColumn("effective_to", to_date(lit(today_str)))
    )

    # New records (natural key unseen before) + new versions of changed records
    to_insert = staged.join(current_gold.select(natural_key), natural_key, "left_anti")
    count     = to_insert.count()

    # Rebuild the full dimension: existing history + closed rows + new versions
    (
        untouched
        .unionByName(changed_history)
        .unionByName(changed_to_close)
        .unionByName(to_insert)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )

    logger.info(f"Gold {entity_name}: {count} new/updated record(s)")


# ── DIM BUILDERS ──────────────────────────────────────────────────────────────

def build_dim_date(spark: SparkSession) -> None:
    """
    Generates a static date dimension covering 2024-01-01 → 2030-12-31.
    date_key is YYYYMMDD integer — used as the FK in fact_shipment.
    No SCD2 needed since dates never change.
    """
    dates = spark.sql("""
        SELECT explode(
            sequence(to_date('2024-01-01'), to_date('2030-12-31'), interval 1 day)
        ) AS full_date
    """)

    dim_date = (
        dates
        .withColumn("date_key",   date_format("full_date", "yyyyMMdd").cast("integer"))
        .withColumn("day",        dayofmonth("full_date"))
        .withColumn("month",      month("full_date"))
        .withColumn("quarter",    quarter("full_date"))
        .withColumn("year",       year("full_date"))
        .withColumn("week",       weekofyear("full_date"))
        .withColumn("day_name",   date_format("full_date", "EEEE"))
        .withColumn("month_name", date_format("full_date", "MMMM"))
        .withColumn("is_weekend", dayofweek("full_date").isin(1, 7))
    )

    dim_date.write.format("delta").mode("overwrite").save(config.gold_path("dim_date"))
    logger.info(f"Gold dim_date: written (2024-01-01 → 2030-12-31)")


def build_dim_fulfillment_center(spark: SparkSession) -> None:
    """SCD2 dim for Fulfillment Centers. Tracks changes to daily_capacity."""
    fc_df = spark.read.format("delta").load(config.silver_path("master/fulfillment_centers"))

    build_scd2_dim(
        spark, fc_df,
        natural_key  = "fulfillment_center_id",
        tracked_cols = ["fulfillment_center_name", "city", "region", "state", "daily_capacity"],
        output_path  = config.gold_path("dim_fulfillment_center"),
        entity_name  = "dim_fulfillment_center",
    )


def build_dim_delivery_station(spark: SparkSession) -> None:
    """SCD2 dim for Delivery Stations. Tracks changes to daily_capacity."""
    ds_df = spark.read.format("delta").load(config.silver_path("master/delivery_stations"))

    build_scd2_dim(
        spark, ds_df,
        natural_key  = "delivery_station_id",
        tracked_cols = ["delivery_station_name", "city", "region", "state", "daily_capacity"],
        output_path  = config.gold_path("dim_delivery_station"),
        entity_name  = "dim_delivery_station",
    )


def build_dim_vehicle(spark: SparkSession) -> None:
    """SCD2 dim for Vehicles. Tracks status changes (AVAILABLE → IN_USE → MAINTENANCE)."""
    vehicle_df = spark.read.format("delta").load(config.silver_path("master/vehicles"))

    build_scd2_dim(
        spark, vehicle_df,
        natural_key  = "vehicle_id",
        tracked_cols = ["vehicle_type", "capacity", "home_facility_id", "home_facility_type", "status"],
        output_path  = config.gold_path("dim_vehicle"),
        entity_name  = "dim_vehicle",
    )


def build_dim_customer(spark: SparkSession) -> None:
    """
    SCD2 dim for Customers. Derived from Silver orders.
    Keeps the most recently observed customer_tier per customer_id.
    Tracks tier upgrades (STANDARD → PRIME) so historical facts reference
    the correct tier at the time of the shipment.
    """
    orders = spark.read.format("delta").load(config.silver_path("events/orders"))

    # One customer may have many orders — take the tier from their latest order
    w = Window.partitionBy("customer_id").orderBy(col("event_timestamp").desc())
    customers = (
        orders
        .filter(col("customer_id").isNotNull())
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("customer_id", "customer_tier")
    )

    build_scd2_dim(
        spark, customers,
        natural_key  = "customer_id",
        tracked_cols = ["customer_tier"],
        output_path  = config.gold_path("dim_customer"),
        entity_name  = "dim_customer",
    )


# ── FACT BUILDER ──────────────────────────────────────────────────────────────

def build_fact_shipment(spark: SparkSession) -> None:
    """
    Builds fact_shipment — one row per shipment — from Silver events and orders.

    The build is split into three phases:
      Phase 1  — Aggregate shipment_events into per-shipment stage timestamps
                 using conditional aggregation (avoids self-joins).
      Phase 2  — Join orders + vehicles for financial and vehicle data.
      Phase 3  — Compute derived measures and resolve dim surrogate keys.

    The fact table is fully rebuilt (overwrite) on every run because Silver
    is the source of truth and a clean rebuild is simpler than incremental merges.
    """
    events = spark.read.format("delta").load(config.silver_path("events/shipment_events"))
    orders = spark.read.format("delta").load(config.silver_path("events/orders"))

    # ── Phase 1a: Key stage timestamps per shipment ────────────────────────────
    # Each status transition gives us a timestamp. Conditional aggregation
    # collapses all events for a shipment into a single row with named timestamps.
    stage_ts = (
        events.groupBy("shipment_id")
        .agg(
            spark_min(when(col("shipment_status") == "ORDER_ALLOCATED_TO_FC", col("event_timestamp"))).alias("fc_received_at"),
            spark_max(when(col("shipment_status") == "DISPATCHED_FROM_FC",    col("event_timestamp"))).alias("dispatched_from_fc_at"),
            spark_min(when(col("shipment_status") == "RECEIVED_AT_DS",        col("event_timestamp"))).alias("received_at_ds_at"),
            spark_max(when(col("shipment_status") == "OUT_FOR_DELIVERY",      col("event_timestamp"))).alias("out_for_delivery_at"),
            spark_max(when(col("shipment_status") == "DELIVERED",             col("event_timestamp"))).alias("actual_delivery_time"),
            # Facility IDs needed to join to FC/DS dims
            spark_max(when(col("facility_type") == "FULFILLMENT_CENTER", col("facility_id"))).alias("fc_id"),
            spark_max(when(col("facility_type") == "DELIVERY_STATION",   col("facility_id"))).alias("ds_id"),
        )
    )

    # ── Phase 1b: Latest event state per shipment ─────────────────────────────
    # We need the current status, vehicle, and delay info from the most recent event.
    w = Window.partitionBy("shipment_id").orderBy(col("event_timestamp").desc())
    latest = (
        events
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(
            "shipment_id", "order_id", "shipment_status",
            "vehicle_id", "is_delayed", "delay_minutes",
            "customer_tier", "delivery_type", "promised_delivery_time",
        )
    )

    # ── Phase 2: Join supporting tables ───────────────────────────────────────
    vehicles = (
        spark.read.format("delta")
        .load(config.silver_path("master/vehicles"))
        .select("vehicle_id", "vehicle_type")
    )

    fact = (
        latest
        .join(stage_ts, "shipment_id", "left")
        .join(orders.select("shipment_id", "customer_id", "order_value"), "shipment_id", "left")
        .join(vehicles, "vehicle_id", "left")
    )

    # ── Phase 3a: Computed measures ────────────────────────────────────────────
    fact = (
        fact
        # End-to-end lead time — the primary delivery speed metric
        .withColumn("lead_time_hours",
            when(col("actual_delivery_time").isNotNull() & col("fc_received_at").isNotNull(),
                spark_round(
                    (unix_timestamp("actual_delivery_time") - unix_timestamp("fc_received_at")) / 3600.0, 2
                )
            )
        )
        # FC processing time — used for Warehouse Performance metric
        .withColumn("fc_time_minutes",
            when(col("dispatched_from_fc_at").isNotNull() & col("fc_received_at").isNotNull(),
                spark_round(
                    (unix_timestamp("dispatched_from_fc_at") - unix_timestamp("fc_received_at")) / 60.0, 1
                )
            )
        )
        # DS processing time — used for Delivery Station Performance metric
        .withColumn("ds_time_minutes",
            when(col("out_for_delivery_at").isNotNull() & col("received_at_ds_at").isNotNull(),
                spark_round(
                    (unix_timestamp("out_for_delivery_at") - unix_timestamp("received_at_ds_at")) / 60.0, 1
                )
            )
        )
        # SLA breached: True only for shipments that have actually been delivered late
        .withColumn("sla_breached",
            when(col("actual_delivery_time").isNotNull(),
                col("actual_delivery_time") > col("promised_delivery_time")
            ).otherwise(lit(False))
        )
        # Transportation cost estimated by vehicle type
        .withColumn("transportation_cost",
            when(col("vehicle_type") == "TRUCK", lit(_TRANSPORT_COST["TRUCK"]))
            .when(col("vehicle_type") == "VAN",  lit(_TRANSPORT_COST["VAN"]))
            .when(col("vehicle_type") == "BIKE", lit(_TRANSPORT_COST["BIKE"]))
            .otherwise(lit(0.0))
        )
        # SLA penalty: accrues per minute of delay
        .withColumn("sla_penalty_amount",
            when(col("delay_minutes") > 0,
                when(col("delivery_type") == "SAME_DAY",
                    col("delay_minutes").cast("double") * lit(_PENALTY_RATE["SAME_DAY"]))
                .when(col("delivery_type") == "STANDARD",
                    col("delay_minutes").cast("double") * lit(_PENALTY_RATE["STANDARD"]))
                .otherwise(lit(0.0))
            ).otherwise(lit(0.0))
        )
        # Refund: issued as a fixed % of order value when SLA is breached
        .withColumn("refund_amount",
            when(col("sla_breached"),
                when(col("delivery_type") == "SAME_DAY",
                    col("order_value") * lit(_REFUND_RATE["SAME_DAY"]))
                .when(col("delivery_type") == "STANDARD",
                    col("order_value") * lit(_REFUND_RATE["STANDARD"]))
                .otherwise(lit(0.0))
            ).otherwise(lit(0.0))
        )
    )

    # ── Phase 3b: Resolve dim surrogate keys ───────────────────────────────────
    # Load only the current (is_current=True) version of each SCD2 dim
    # to get the surrogate key for joining from the fact table.
    dim_fc = (
        spark.read.format("delta").load(config.gold_path("dim_fulfillment_center"))
        .filter(col("is_current"))
        .select(col("fulfillment_center_id"), col("surrogate_key").alias("fc_sk"))
    )
    dim_ds = (
        spark.read.format("delta").load(config.gold_path("dim_delivery_station"))
        .filter(col("is_current"))
        .select(col("delivery_station_id"), col("surrogate_key").alias("ds_sk"))
    )
    dim_veh = (
        spark.read.format("delta").load(config.gold_path("dim_vehicle"))
        .filter(col("is_current"))
        .select(col("vehicle_id").alias("v_id"), col("surrogate_key").alias("vehicle_sk"))
    )
    dim_cust = (
        spark.read.format("delta").load(config.gold_path("dim_customer"))
        .filter(col("is_current"))
        .select(col("customer_id"), col("surrogate_key").alias("customer_sk"))
    )
    dim_date = (
        spark.read.format("delta").load(config.gold_path("dim_date"))
        .select(col("date_key"), col("full_date"))
    )

    fact = (
        fact
        .join(dim_fc,   col("fc_id")     == col("fulfillment_center_id"), "left").drop("fulfillment_center_id")
        .join(dim_ds,   col("ds_id")     == col("delivery_station_id"),   "left").drop("delivery_station_id")
        .join(dim_veh,  col("vehicle_id") == col("v_id"),                  "left").drop("v_id")
        .join(dim_cust, "customer_id",                                      "left")
        # date_key resolved from the date the shipment first arrived at the FC
        .withColumn("order_date",
            to_date(when(col("fc_received_at").isNotNull(), col("fc_received_at")).otherwise(current_date()))
        )
        .join(dim_date, col("order_date") == col("full_date"), "left")
        .drop("full_date", "order_date")
        .withColumn("gold_processed_at", current_timestamp())
    )

    count = fact.count()
    (
        fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")   # safe to evolve schema between runs
        .save(config.gold_path("fact_shipment"))
    )
    logger.info(f"Gold fact_shipment: wrote {count} row(s)")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script08_GoldProcessing")

    logger.info("=" * 60)
    logger.info("Script 8 — Gold Processing started")
    logger.info("=" * 60)

    # Dimensions must be built before the fact table
    # because build_fact_shipment joins Gold dims to resolve surrogate keys
    logger.info("Building dimensions...")
    build_dim_date(spark)
    build_dim_fulfillment_center(spark)
    build_dim_delivery_station(spark)
    build_dim_vehicle(spark)
    build_dim_customer(spark)

    logger.info("Building fact table...")
    build_fact_shipment(spark)

    logger.info("=" * 60)
    logger.info("Script 8 — Gold Processing completed")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
