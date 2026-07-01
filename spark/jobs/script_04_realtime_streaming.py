# ─────────────────────────────────────────────────────────────────────────────
# Script 4 — Real-Time Streaming Pipeline  (Continuous)
#
# Continuously consumes the shipment_events Kafka topic, enriches each event
# with Silver lookup tables (FC, DS, Vehicle), computes real-time KPI columns,
# writes the enriched state to PostgreSQL (queried by Grafana RT dashboard),
# and triggers email alerts when thresholds are breached.
#
# Data flow:
#   Kafka (shipment_events)
#     → parse JSON
#     → broadcast join with Silver FC / DS / Vehicle
#     → compute KPI columns (is_sla_at_risk, is_priority)
#     → foreachBatch → upsert to PostgreSQL + send alerts
#
# PostgreSQL table (shipment_live):
#   One row per shipment, always reflecting the latest event.
#   Grafana queries this table for the RT dashboard panels.
#
# Prerequisites:
#   - Scripts 1–3 must have run so Silver tables exist in MinIO.
#   - Stream producer must be running so shipment_events flow into Kafka.
#   - psycopg2-binary installed in Spark container:
#       docker exec spark-master pip install psycopg2-binary
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_04_realtime_streaming.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, broadcast,
    to_timestamp, current_timestamp, lit,
)

import config
import postgres
import alerts
from spark_session import get_spark
from schemas import SHIPMENT_EVENT_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ISO-8601 format produced by the stream producer (e.g. "2026-06-30T11:00:00Z")
_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

# Stages where a shipment is still sitting inside a Fulfillment Center
_FC_BACKLOG_STATUSES = {"ORDER_ALLOCATED_TO_FC", "PICKED_AND_PACKED"}

# Stages where a shipment is waiting at a Delivery Station
_DS_BACKLOG_STATUSES = {"RECEIVED_AT_DS", "ASSIGNED_TO_DRIVER"}


# ── SILVER LOOKUP LOADING ─────────────────────────────────────────────────────

def load_silver_lookups(spark: SparkSession):
    """
    Reads Silver master data as static DataFrames for broadcast joins.
    Only the columns needed for enrichment are selected to keep joins cheap.
    """
    fc_df = (
        spark.read.format("delta")
        .load(config.silver_path("master/fulfillment_centers"))
        .select(
            col("fulfillment_center_id"),
            col("fulfillment_center_name").alias("fc_name"),
            col("region").alias("fc_region"),
        )
    )

    ds_df = (
        spark.read.format("delta")
        .load(config.silver_path("master/delivery_stations"))
        .select(
            col("delivery_station_id"),
            col("delivery_station_name").alias("ds_name"),
            col("region").alias("ds_region"),
        )
    )

    vehicle_df = (
        spark.read.format("delta")
        .load(config.silver_path("master/vehicles"))
        .select("vehicle_id", "vehicle_type")
    )

    return fc_df, ds_df, vehicle_df


# ── STREAM SOURCE ─────────────────────────────────────────────────────────────

def build_events_stream(spark: SparkSession) -> DataFrame:
    """
    Reads shipment_events from Kafka as a streaming DataFrame.
    startingOffsets=latest means we only process events that arrive after the job starts.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.TOPIC_SHIPMENT_EVENTS)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse the JSON value into typed columns using the schema
    return (
        raw.select(
            from_json(col("value").cast("string"), SHIPMENT_EVENT_SCHEMA).alias("e") # casts the raw Kafka bytes to a string, then parses it as JSON using the predefined schema.
        )
        .select("e.*")
        .filter(col("shipment_id").isNotNull())
    )


# ── ENRICHMENT ────────────────────────────────────────────────────────────────

def enrich(events_df: DataFrame, fc_df: DataFrame, ds_df: DataFrame, vehicle_df: DataFrame) -> DataFrame:
    """
    Joins each shipment event with Silver lookup tables to add:
      - fc_name, fc_region   (when the shipment is at a Fulfillment Center)
      - ds_name, ds_region   (when the shipment is at a Delivery Station)
      - vehicle_type         (when a vehicle is assigned)

    Two separate left joins for FC and DS are needed because facility_id can
    point to either an FC or a DS depending on the current shipment stage.
    """
    return (
        events_df

        # Join with FC lookup on facility_id when the facility is an FC
        .join(
            broadcast(fc_df),
            (col("facility_id") == col("fulfillment_center_id")) &
            (col("facility_type") == lit("FULFILLMENT_CENTER")),
            "left",
        )
        .drop("fulfillment_center_id")

        # Join with DS lookup on facility_id when the facility is a DS
        .join(
            broadcast(ds_df),
            (col("facility_id") == col("delivery_station_id")) &
            (col("facility_type") == lit("DELIVERY_STATION")),
            "left",
        )
        .drop("delivery_station_id")

        # Join with Vehicle lookup on vehicle_id
        .join(broadcast(vehicle_df), "vehicle_id", "left")
    )


# ── KPI COLUMNS ───────────────────────────────────────────────────────────────
# Converts timestamp strings to real TIMESTAMP types and computes the two boolean KPI flags used by the dashboard and alerting logic.
def add_kpi_columns(df: DataFrame) -> DataFrame:
    """
    Adds computed columns used by the Grafana RT dashboard:
      is_sla_at_risk — ETA is later than the promised delivery time and not yet delivered.
      is_priority    — True for PRIME customer tier (used for priority filtering).
      estimated_delivery_time / promised_delivery_time cast to proper timestamps
        so PostgreSQL receives TIMESTAMP values, not strings.
    """
    return (
        df
        .withColumn("eta_ts",       to_timestamp(col("estimated_delivery_time"), _TS_FORMAT))
        .withColumn("promised_ts",  to_timestamp(col("promised_delivery_time"),  _TS_FORMAT))
        .withColumn("event_ts",     to_timestamp(col("event_timestamp"),         _TS_FORMAT))
        .withColumn("is_sla_at_risk",
            (col("eta_ts") > col("promised_ts")) &
            (col("shipment_status") != lit("DELIVERED"))
        )
        .withColumn("is_priority", col("customer_tier") == lit("PRIME"))
        # Replace the original string columns with the parsed timestamp columns
        .drop("estimated_delivery_time", "promised_delivery_time", "event_timestamp")
        .withColumnRenamed("eta_ts",      "estimated_delivery_time")
        .withColumnRenamed("promised_ts", "promised_delivery_time")
        .withColumnRenamed("event_ts",    "event_timestamp")
    )


# ── BATCH PROCESSOR ───────────────────────────────────────────────────────────
# The foreachBatch handler — runs once per micro-batch (every 10 seconds), persisting the batch to Postgres and triggering all alert checks. This is where streaming meets a regular (non-streaming) Python/SQL workflow.
def process_batch(batch_df: DataFrame, batch_id: int, sent_alerts: dict) -> None:
    """
    foreachBatch handler — called once per micro-batch.

    Steps:
      1. Fetch previous ETAs from PostgreSQL (for ETA change detection).
      2. Upsert the batch to PostgreSQL (shipment_live table).
      3. Run per-shipment alert checks.
      4. Run facility-level alert checks (backlog + stale).
    """
    if batch_df.isEmpty():
        return

    rows         = batch_df.collect()
    shipment_ids = [r.shipment_id for r in rows if r.shipment_id]

    conn = postgres.get_connection()
    try:
        postgres.ensure_table(conn)

        previous_etas = postgres.fetch_previous_etas(shipment_ids, conn) # grabs old ETA values 
        # before they get overwritten, so alerts.check_per_shipment_alerts can detect significant ETA increases.

        # Upsert all events in this batch
        postgres.upsert_batch(rows, conn)
        conn.commit()

        # Per-shipment alerts
        for row in rows:
            if row.shipment_id:
                alerts.check_per_shipment_alerts(
                    row,
                    previous_etas.get(row.shipment_id),
                    sent_alerts,
                )

        # Facility-level alerts (query the full table state from PostgreSQL)
        fc_backlog = postgres.fetch_fc_backlog(conn)
        ds_backlog = postgres.fetch_ds_backlog(conn)
        alerts.check_backlog_alerts(fc_backlog, ds_backlog, sent_alerts)

        stale_rows = postgres.fetch_stale_shipments(conn)
        alerts.check_stale_alerts(stale_rows, sent_alerts)

    except Exception as e:
        conn.rollback()
        logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)
    finally:
        conn.close()

    logger.info(f"Batch {batch_id}: processed {len(rows)} event(s)")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script04_RealTimeStreaming")

    logger.info("=" * 60)
    logger.info("Script 4 — Real-Time Streaming Pipeline started")
    logger.info("=" * 60)

    # Load Silver lookup tables once — broadcast to all executors
    fc_df, ds_df, vehicle_df = load_silver_lookups(spark)

    # Build the enriched streaming DataFrame
    events_stream   = build_events_stream(spark)
    enriched_stream = enrich(events_stream, fc_df, ds_df, vehicle_df)
    enriched_stream = add_kpi_columns(enriched_stream)

    # In-memory alert dedup state, persists for the life of this streaming job
    sent_alerts: dict = {}

    query = (
        enriched_stream.writeStream
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, sent_alerts))
        .option("checkpointLocation", "/tmp/checkpoints/script04")
        .trigger(processingTime="10 seconds")   # micro-batch every 10 seconds
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
