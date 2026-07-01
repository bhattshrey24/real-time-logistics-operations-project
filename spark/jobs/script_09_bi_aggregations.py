# ─────────────────────────────────────────────────────────────────────────────
# Script 9 — BI Aggregations  (Batch / On-Demand)
#
# Reads Gold Delta tables from MinIO, computes the 6 pre-aggregated BI tables,
# and writes them to PostgreSQL so Grafana can query them with a plain SELECT.
#
# All 6 tables are fully rebuilt on every run (TRUNCATE + INSERT) — this keeps
# Grafana queries instant and the logic here simple.
#
# PostgreSQL tables written (all in logistics_rt database):
#   bi_warehouse_performance     — per-FC: volume, avg dispatch time, delayed %
#   bi_delivery_station_perf     — per-DS: volume, avg process time, delayed %
#   bi_logistics_kpis            — overall: on-time %, avg delay, SLA breach %
#   bi_monthly_trends            — month-by-month: volume, on-time %, avg delay
#   bi_cost_penalty              — per delivery type: costs, penalties, refunds
#   bi_lead_time                 — per delivery type: avg/min/max lead time hrs
#
# Prerequisites:
#   - Script 8 must have run so Gold Delta tables exist in MinIO.
#   - logistics_rt PostgreSQL database must exist (created by Script 4).
#
# Submit command:
#   docker exec -it spark-master \
#     /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     /opt/spark/jobs/script_09_bi_aggregations.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))

import logging
import psycopg2.extras
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, current_timestamp,
    count, avg,
    sum  as spark_sum,
    min  as spark_min,
    max  as spark_max,
    round as spark_round,
)

import config
import postgres
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ── POSTGRES WRITE HELPER ─────────────────────────────────────────────────────

def refresh_table(conn, create_sql: str, table_name: str, insert_sql: str, rows: list) -> None:
    """
    Creates the table if it doesn't exist, truncates it, then bulk-inserts
    all rows in a single transaction. Called once per BI aggregation table.
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"TRUNCATE TABLE {table_name}")
        if rows:
            psycopg2.extras.execute_values(cur, insert_sql, rows)
    conn.commit()
    logger.info(f"{table_name}: {len(rows)} row(s) written")


# ── AGGREGATION FUNCTIONS ─────────────────────────────────────────────────────
# Each function takes pre-loaded DataFrames and returns a Spark DataFrame.
# Keeping Gold reads outside these functions avoids re-reading the same files.

def compute_warehouse_performance(fact: DataFrame, dim_fc: DataFrame) -> DataFrame:
    """
    Per Fulfillment Center: shipment volume, average FC dispatch time, delayed %.
    Only includes shipments that passed through a known FC (fc_sk not null).
    """
    return (
        fact
        .filter(col("fc_sk").isNotNull())
        .join(
            dim_fc.select(
                col("surrogate_key"),
                col("fulfillment_center_name").alias("fc_name"),
                "region",
            ),
            col("fc_sk") == col("surrogate_key"),
            "left",
        )
        .drop("surrogate_key")
        .groupBy("fc_id", "fc_name", "region")
        .agg(
            count("shipment_id").alias("shipment_volume"),
            spark_round(avg("fc_time_minutes"), 1).alias("avg_fc_time_min"),
            count(when(col("is_delayed"), True)).alias("delayed_count"),
        )
        .withColumn("delayed_pct",
            when(col("shipment_volume") > 0,
                spark_round(col("delayed_count") / col("shipment_volume") * 100, 1)
            ).otherwise(lit(0.0))
        )
        .withColumn("refreshed_at", current_timestamp())
    )


def compute_ds_performance(fact: DataFrame, dim_ds: DataFrame) -> DataFrame:
    """
    Per Delivery Station: shipment volume, average DS processing time, delayed %.
    Only includes shipments that passed through a known DS (ds_sk not null).
    """
    return (
        fact
        .filter(col("ds_sk").isNotNull())
        .join(
            dim_ds.select(
                col("surrogate_key"),
                col("delivery_station_name").alias("ds_name"),
                "region",
            ),
            col("ds_sk") == col("surrogate_key"),
            "left",
        )
        .drop("surrogate_key")
        .groupBy("ds_id", "ds_name", "region")
        .agg(
            count("shipment_id").alias("shipment_volume"),
            spark_round(avg("ds_time_minutes"), 1).alias("avg_ds_time_min"),
            count(when(col("is_delayed"), True)).alias("delayed_count"),
        )
        .withColumn("delayed_pct",
            when(col("shipment_volume") > 0,
                spark_round(col("delayed_count") / col("shipment_volume") * 100, 1)
            ).otherwise(lit(0.0))
        )
        .withColumn("refreshed_at", current_timestamp())
    )


def compute_logistics_kpis(fact: DataFrame) -> DataFrame:
    """
    Single-row overall summary: on-time delivery %, avg delay, SLA breach %.
    On-time = DELIVERED and sla_breached is False.
    Rates are computed only over delivered shipments to avoid skewing with in-transit.
    """
    return (
        fact
        .agg(
            count("shipment_id").alias("total_shipments"),
            count(when(col("shipment_status") == "DELIVERED", True)).alias("delivered_count"),
            count(when(
                (col("shipment_status") == "DELIVERED") & (col("sla_breached") == False), True
            )).alias("on_time_count"),
            spark_round(avg("delay_minutes"), 1).alias("avg_delay_min"),
            count(when(col("sla_breached") == True, True)).alias("sla_breach_count"),
        )
        .withColumn("on_time_pct",
            when(col("delivered_count") > 0,
                spark_round(col("on_time_count") / col("delivered_count") * 100, 1)
            ).otherwise(lit(0.0))
        )
        .withColumn("sla_breach_pct",
            when(col("delivered_count") > 0,
                spark_round(col("sla_breach_count") / col("delivered_count") * 100, 1)
            ).otherwise(lit(0.0))
        )
        .withColumn("refreshed_at", current_timestamp())
    )


def compute_monthly_trends(fact: DataFrame, dim_date: DataFrame) -> DataFrame:
    """
    Month-by-month rollup: shipment volume, on-time delivery %, average delay.
    date_key in fact_shipment links to dim_date for year/month/month_name attributes.
    """
    return (
        fact
        .join(dim_date.select("date_key", "year", "month", "month_name"), "date_key", "left")
        .groupBy("year", "month", "month_name")
        .agg(
            count("shipment_id").alias("shipment_volume"),
            count(when(col("shipment_status") == "DELIVERED", True)).alias("delivered_count"),
            count(when(
                (col("shipment_status") == "DELIVERED") & (col("sla_breached") == False), True
            )).alias("on_time_count"),
            spark_round(avg("delay_minutes"), 1).alias("avg_delay_min"),
        )
        .withColumn("on_time_pct",
            when(col("delivered_count") > 0,
                spark_round(col("on_time_count") / col("delivered_count") * 100, 1)
            ).otherwise(lit(0.0))
        )
        .withColumn("refreshed_at", current_timestamp())
        .orderBy("year", "month")
    )


def compute_cost_penalty(fact: DataFrame) -> DataFrame:
    """
    Per delivery type: total transportation cost, SLA penalties, customer refunds.
    All cost columns are estimated values derived in Script 8.
    """
    return (
        fact
        .groupBy("delivery_type")
        .agg(
            count("shipment_id").alias("total_shipments"),
            spark_round(spark_sum("transportation_cost"), 2).alias("total_transport_cost"),
            spark_round(spark_sum("sla_penalty_amount"),  2).alias("total_sla_penalty"),
            spark_round(spark_sum("refund_amount"),        2).alias("total_refunds"),
            spark_round(avg("order_value"),                2).alias("avg_order_value"),
        )
        .withColumn("refreshed_at", current_timestamp())
    )


def compute_lead_time(fact: DataFrame) -> DataFrame:
    """
    Per delivery type: avg/min/max lead time for shipments with a complete
    FC-to-delivery journey. lead_time_hours is null for undelivered shipments
    so filtering on isNotNull naturally limits this to completed deliveries.
    """
    return (
        fact
        .filter(col("lead_time_hours").isNotNull())
        .groupBy("delivery_type")
        .agg(
            count("shipment_id").alias("shipment_count"),
            spark_round(avg("lead_time_hours"),       2).alias("avg_lead_time_hrs"),
            spark_round(spark_min("lead_time_hours"), 2).alias("min_lead_time_hrs"),
            spark_round(spark_max("lead_time_hours"), 2).alias("max_lead_time_hrs"),
        )
        .withColumn("refreshed_at", current_timestamp())
    )


# ── DDL AND INSERT SQL ────────────────────────────────────────────────────────
# Each pair defines the table shape and the parameterised INSERT used by
# refresh_table(). Columns must match the aggregation function output exactly.

_WAREHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS bi_warehouse_performance (
    fc_id           VARCHAR,
    fc_name         VARCHAR,
    region          VARCHAR,
    shipment_volume INTEGER,
    avg_fc_time_min DOUBLE PRECISION,
    delayed_count   INTEGER,
    delayed_pct     DOUBLE PRECISION,
    refreshed_at    TIMESTAMP
)"""

_WAREHOUSE_INSERT = """
INSERT INTO bi_warehouse_performance
    (fc_id, fc_name, region, shipment_volume, avg_fc_time_min, delayed_count, delayed_pct, refreshed_at)
VALUES %s"""

# ─────────────────────────────────────────────────────────────────────────────

_DS_DDL = """
CREATE TABLE IF NOT EXISTS bi_delivery_station_perf (
    ds_id           VARCHAR,
    ds_name         VARCHAR,
    region          VARCHAR,
    shipment_volume INTEGER,
    avg_ds_time_min DOUBLE PRECISION,
    delayed_count   INTEGER,
    delayed_pct     DOUBLE PRECISION,
    refreshed_at    TIMESTAMP
)"""

_DS_INSERT = """
INSERT INTO bi_delivery_station_perf
    (ds_id, ds_name, region, shipment_volume, avg_ds_time_min, delayed_count, delayed_pct, refreshed_at)
VALUES %s"""

# ─────────────────────────────────────────────────────────────────────────────

_KPIS_DDL = """
CREATE TABLE IF NOT EXISTS bi_logistics_kpis (
    total_shipments  INTEGER,
    delivered_count  INTEGER,
    on_time_count    INTEGER,
    on_time_pct      DOUBLE PRECISION,
    avg_delay_min    DOUBLE PRECISION,
    sla_breach_count INTEGER,
    sla_breach_pct   DOUBLE PRECISION,
    refreshed_at     TIMESTAMP
)"""

_KPIS_INSERT = """
INSERT INTO bi_logistics_kpis
    (total_shipments, delivered_count, on_time_count, on_time_pct,
     avg_delay_min, sla_breach_count, sla_breach_pct, refreshed_at)
VALUES %s"""

# ─────────────────────────────────────────────────────────────────────────────

_MONTHLY_DDL = """
CREATE TABLE IF NOT EXISTS bi_monthly_trends (
    year             INTEGER,
    month            INTEGER,
    month_name       VARCHAR,
    shipment_volume  INTEGER,
    delivered_count  INTEGER,
    on_time_count    INTEGER,
    on_time_pct      DOUBLE PRECISION,
    avg_delay_min    DOUBLE PRECISION,
    refreshed_at     TIMESTAMP
)"""

_MONTHLY_INSERT = """
INSERT INTO bi_monthly_trends
    (year, month, month_name, shipment_volume, delivered_count,
     on_time_count, on_time_pct, avg_delay_min, refreshed_at)
VALUES %s"""

# ─────────────────────────────────────────────────────────────────────────────

_COST_DDL = """
CREATE TABLE IF NOT EXISTS bi_cost_penalty (
    delivery_type        VARCHAR,
    total_shipments      INTEGER,
    total_transport_cost DOUBLE PRECISION,
    total_sla_penalty    DOUBLE PRECISION,
    total_refunds        DOUBLE PRECISION,
    avg_order_value      DOUBLE PRECISION,
    refreshed_at         TIMESTAMP
)"""

_COST_INSERT = """
INSERT INTO bi_cost_penalty
    (delivery_type, total_shipments, total_transport_cost,
     total_sla_penalty, total_refunds, avg_order_value, refreshed_at)
VALUES %s"""

# ─────────────────────────────────────────────────────────────────────────────

_LEAD_DDL = """
CREATE TABLE IF NOT EXISTS bi_lead_time (
    delivery_type     VARCHAR,
    shipment_count    INTEGER,
    avg_lead_time_hrs DOUBLE PRECISION,
    min_lead_time_hrs DOUBLE PRECISION,
    max_lead_time_hrs DOUBLE PRECISION,
    refreshed_at      TIMESTAMP
)"""

_LEAD_INSERT = """
INSERT INTO bi_lead_time
    (delivery_type, shipment_count, avg_lead_time_hrs, min_lead_time_hrs, max_lead_time_hrs, refreshed_at)
VALUES %s"""


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("Script09_BIAggregations")

    logger.info("=" * 60)
    logger.info("Script 9 — BI Aggregations started")
    logger.info("=" * 60)

    # Load Gold tables once — passed into each aggregation function to avoid
    # re-reading the same MinIO paths multiple times
    logger.info("Loading Gold Delta tables...")
    fact     = spark.read.format("delta").load(config.gold_path("fact_shipment"))
    dim_fc   = spark.read.format("delta").load(config.gold_path("dim_fulfillment_center")).filter(col("is_current"))
    dim_ds   = spark.read.format("delta").load(config.gold_path("dim_delivery_station")).filter(col("is_current"))
    dim_date = spark.read.format("delta").load(config.gold_path("dim_date"))

    # Compute all 6 aggregations in Spark before touching Postgres
    logger.info("Computing aggregations...")
    warehouse_rows = [
        (r.fc_id, r.fc_name, r.region, r.shipment_volume, r.avg_fc_time_min, r.delayed_count, r.delayed_pct, r.refreshed_at)
        for r in compute_warehouse_performance(fact, dim_fc).collect()
    ]
    ds_rows = [
        (r.ds_id, r.ds_name, r.region, r.shipment_volume, r.avg_ds_time_min, r.delayed_count, r.delayed_pct, r.refreshed_at)
        for r in compute_ds_performance(fact, dim_ds).collect()
    ]
    kpi_rows = [
        (r.total_shipments, r.delivered_count, r.on_time_count, r.on_time_pct, r.avg_delay_min, r.sla_breach_count, r.sla_breach_pct, r.refreshed_at)
        for r in compute_logistics_kpis(fact).collect()
    ]
    monthly_rows = [
        (r.year, r.month, r.month_name, r.shipment_volume, r.delivered_count, r.on_time_count, r.on_time_pct, r.avg_delay_min, r.refreshed_at)
        for r in compute_monthly_trends(fact, dim_date).collect()
    ]
    cost_rows = [
        (r.delivery_type, r.total_shipments, r.total_transport_cost, r.total_sla_penalty, r.total_refunds, r.avg_order_value, r.refreshed_at)
        for r in compute_cost_penalty(fact).collect()
    ]
    lead_rows = [
        (r.delivery_type, r.shipment_count, r.avg_lead_time_hrs, r.min_lead_time_hrs, r.max_lead_time_hrs, r.refreshed_at)
        for r in compute_lead_time(fact).collect()
    ]

    # Write all tables to PostgreSQL in one connection
    logger.info("Writing to PostgreSQL...")
    conn = postgres.get_connection()
    try:
        refresh_table(conn, _WAREHOUSE_DDL, "bi_warehouse_performance",  _WAREHOUSE_INSERT, warehouse_rows)
        refresh_table(conn, _DS_DDL,        "bi_delivery_station_perf",  _DS_INSERT,        ds_rows)
        refresh_table(conn, _KPIS_DDL,      "bi_logistics_kpis",         _KPIS_INSERT,      kpi_rows)
        refresh_table(conn, _MONTHLY_DDL,   "bi_monthly_trends",         _MONTHLY_INSERT,   monthly_rows)
        refresh_table(conn, _COST_DDL,      "bi_cost_penalty",           _COST_INSERT,      cost_rows)
        refresh_table(conn, _LEAD_DDL,      "bi_lead_time",              _LEAD_INSERT,      lead_rows)
    except Exception as e:
        conn.rollback()
        logger.error(f"PostgreSQL write failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()

    logger.info("=" * 60)
    logger.info("Script 9 — BI Aggregations completed")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
