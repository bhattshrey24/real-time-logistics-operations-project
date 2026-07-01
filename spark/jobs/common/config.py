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

# ── POSTGRESQL ────────────────────────────────────────────────────────────────
# Used by Script 4 to write enriched events for the Grafana RT dashboard.
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
LOGISTICS_DB      = os.getenv("LOGISTICS_DB",      "logistics_rt")  # separate from Airflow's DB

# ── SMTP (alert emails) ───────────────────────────────────────────────────────
SMTP_HOST     = os.getenv("SMTP_HOST",     "")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER",     "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")

# ── ALERT THRESHOLDS ──────────────────────────────────────────────────────────
# Kept here so all scripts share the same values (mirrors producer/config.py).
DELAY_ALERT_THRESHOLD_MINUTES = {"SAME_DAY": 30,  "STANDARD": 120}
ETA_CHANGE_ALERT_MINUTES      = {"SAME_DAY": 30,  "STANDARD": 120}
FC_BACKLOG_THRESHOLD          = 10   # Alert if FC backlog exceeds this
DS_BACKLOG_THRESHOLD          = 8    # Alert if DS backlog exceeds this
FC_STALE_THRESHOLD_HOURS      = 4    # Shipment stuck at FC for > 4 hours
DS_STALE_THRESHOLD_HOURS      = 1    # Shipment stuck at DS for > 1 hour

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
