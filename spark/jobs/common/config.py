# ─────────────────────────────────────────────────────────────────────────────
# Shared configuration for all Spark jobs.
# Reads credentials from .env; provides path helpers for every storage layer.
# ─────────────────────────────────────────────────────────────────────────────

import os

# ── KAFKA ─────────────────────────────────────────────────────────────────────
# Spark runs inside Docker, so it uses the internal broker addresses.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"
)

# ── MINIO (S3-compatible storage) ─────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "logistics")

# ── KAFKA TOPIC NAMES ─────────────────────────────────────────────────────────
TOPIC_FULFILLMENT_CENTERS = "fulfillment_centers"
TOPIC_DELIVERY_STATIONS   = "delivery_stations"
TOPIC_VEHICLES            = "vehicles"
TOPIC_ORDERS              = "orders"
TOPIC_SHIPMENT_EVENTS     = "shipment_events"

# ── STORAGE LAYER PATH BUILDERS ───────────────────────────────────────────────
# All scripts use these helpers so paths are consistent across the pipeline.

def raw_path(entity: str) -> str:
    return f"s3a://{MINIO_BUCKET}/raw/{entity}"

def bronze_path(entity: str) -> str:
    return f"s3a://{MINIO_BUCKET}/bronze/{entity}"

def silver_path(entity: str) -> str:
    return f"s3a://{MINIO_BUCKET}/silver/{entity}"

def gold_path(entity: str) -> str:
    return f"s3a://{MINIO_BUCKET}/gold/{entity}"
