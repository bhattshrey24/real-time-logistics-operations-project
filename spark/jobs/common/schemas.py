# ─────────────────────────────────────────────────────────────────────────────
# PySpark schemas for all master data entities.
# These mirror the payloads produced by producer/batch/batch_builder.py.
# Used in Script 2 (Bronze) to parse the raw JSON stored in MinIO.
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
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
