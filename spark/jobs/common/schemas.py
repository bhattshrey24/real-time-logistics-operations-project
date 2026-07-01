# ─────────────────────────────────────────────────────────────────────────────
# PySpark schemas for all entities produced by the Kafka producers.
# Master data schemas → used by Scripts 2 & 3 (Bronze/Silver).
# Event schemas       → used by Scripts 4, 5 & 6 (streaming + batch events).
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
)

# ── FULFILLMENT CENTER ────────────────────────────────────────────────────────
FULFILLMENT_CENTER_SCHEMA = StructType([
    StructField("fulfillment_center_id",   StringType(),  nullable=False),
    StructField("fulfillment_center_name", StringType(),  nullable=True),
    StructField("city",                    StringType(),  nullable=True),
    StructField("region",                  StringType(),  nullable=True),
    StructField("state",                   StringType(),  nullable=True),
    StructField("daily_capacity",          IntegerType(), nullable=True),
    StructField("latitude",                DoubleType(),  nullable=True),
    StructField("longitude",               DoubleType(),  nullable=True),
    StructField("updated_at",              StringType(),  nullable=True),
    StructField("batch_date",              StringType(),  nullable=True),
])

# ── DELIVERY STATION ──────────────────────────────────────────────────────────
DELIVERY_STATION_SCHEMA = StructType([
    StructField("delivery_station_id",   StringType(),  nullable=False),
    StructField("delivery_station_name", StringType(),  nullable=True),
    StructField("city",                  StringType(),  nullable=True),
    StructField("region",                StringType(),  nullable=True),
    StructField("state",                 StringType(),  nullable=True),
    StructField("daily_capacity",        IntegerType(), nullable=True),
    StructField("latitude",              DoubleType(),  nullable=True),
    StructField("longitude",             DoubleType(),  nullable=True),
    StructField("updated_at",            StringType(),  nullable=True),
    StructField("batch_date",            StringType(),  nullable=True),
])

# ── VEHICLE ───────────────────────────────────────────────────────────────────
VEHICLE_SCHEMA = StructType([
    StructField("vehicle_id",          StringType(),  nullable=False),
    StructField("vehicle_type",        StringType(),  nullable=True),
    StructField("home_facility_id",    StringType(),  nullable=True),
    StructField("home_facility_type",  StringType(),  nullable=True),
    StructField("capacity",            IntegerType(), nullable=True),
    StructField("status",              StringType(),  nullable=True),
    StructField("updated_at",          StringType(),  nullable=True),
    StructField("batch_date",          StringType(),  nullable=True),
])


# ── SHIPMENT EVENT ────────────────────────────────────────────────────────────
# Mirrors the payload built by producer/stream/event_builder.py → build_shipment_event()
SHIPMENT_EVENT_SCHEMA = StructType([
    StructField("event_id",                StringType(),  nullable=True),
    StructField("event_timestamp",         StringType(),  nullable=True),
    StructField("shipment_id",             StringType(),  nullable=False),
    StructField("order_id",                StringType(),  nullable=True),
    StructField("event_type",              StringType(),  nullable=True),
    StructField("shipment_status",         StringType(),  nullable=True),
    StructField("facility_id",             StringType(),  nullable=True),
    StructField("facility_type",           StringType(),  nullable=True),
    StructField("vehicle_id",              StringType(),  nullable=True),
    StructField("estimated_delivery_time", StringType(),  nullable=True),
    StructField("promised_delivery_time",  StringType(),  nullable=True),
    StructField("is_delayed",              BooleanType(), nullable=True),
    StructField("delay_minutes",           IntegerType(), nullable=True),
    StructField("customer_tier",           StringType(),  nullable=True),
    StructField("delivery_type",           StringType(),  nullable=True),
    StructField("latitude",                DoubleType(),  nullable=True),
    StructField("longitude",               DoubleType(),  nullable=True),
])

# ── ORDER ─────────────────────────────────────────────────────────────────────
# Mirrors the payload built by producer/stream/event_builder.py → build_order_event()
ORDER_SCHEMA = StructType([
    StructField("order_id",               StringType(), nullable=False),
    StructField("event_timestamp",        StringType(), nullable=True),
    StructField("customer_id",            StringType(), nullable=True),
    StructField("customer_tier",          StringType(), nullable=True),
    StructField("delivery_type",          StringType(), nullable=True),
    StructField("order_value",            DoubleType(), nullable=True),
    StructField("fulfillment_center_id",  StringType(), nullable=True),
    StructField("delivery_station_id",    StringType(), nullable=True),
    StructField("promised_delivery_time", StringType(), nullable=True),
    StructField("shipment_id",            StringType(), nullable=True),
    StructField("order_status",           StringType(), nullable=True),
])
