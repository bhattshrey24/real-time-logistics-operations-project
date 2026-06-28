# ─────────────────────────────────────────────────────────────────────────────
# Entry point for the batch (master data) producer.
# Runs ONCE, publishes all static master data to Kafka, then exits cleanly.
#
# How to run (from the project root):
#   python producers/batch/batch_producer.py
#
# Topics published:
#   fulfillment_centers  → 2 records  (one per FC)
#   delivery_stations    → 4 records  (two per FC)
#   vehicles             → 10 records (2 trucks + 8 bikes)
#
# Run this once at startup before starting the stream producer,
# so downstream consumers have the master data available for enrichment.
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

# ── PATH FIX ─────────────────────────────────────────────────────────────────
# Adds `producers/` to sys.path so sibling modules (config, master_data, utils)
# and the batch package are importable from this subdirectory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging

from confluent_kafka import Producer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_FULFILLMENT_CENTERS,
    TOPIC_DELIVERY_STATIONS,
    TOPIC_VEHICLES,
)
from batch.batch_builder import build_fc_events, build_ds_events, build_vehicle_events


# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt = "%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── KAFKA HELPERS ─────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    """
    Callback fired after each message is delivered or fails.
    For batch data, failures are logged as errors since every record matters.
    """
    if err:
        logger.error(f"Delivery failed | topic={msg.topic()} | key={msg.key()} | error={err}")
    else:
        logger.debug(f"Delivered | topic={msg.topic()} | key={msg.key()} | offset={msg.offset()}")


def publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    """
    Serializes a payload dict to JSON and produces it to the Kafka topic.
    Key is the entity's primary ID for consistent partition routing.
    """
    producer.produce(
        topic    = topic,
        key      = key,
        value    = json.dumps(payload),
        callback = delivery_report,
    )
    producer.poll(0)  # Trigger delivery callbacks for already-sent messages


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    logger.info("=" * 60)
    logger.info("Batch producer started.")
    logger.info(f"  Bootstrap servers : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info("=" * 60)

    try:
        # ── FULFILLMENT CENTERS ───────────────────────────────────────────────
        fc_events = build_fc_events()
        for fc in fc_events:
            publish(producer, TOPIC_FULFILLMENT_CENTERS, fc["fulfillment_center_id"], fc)
        logger.info(f"Published {len(fc_events)} record(s) → topic: {TOPIC_FULFILLMENT_CENTERS}")

        # ── DELIVERY STATIONS ─────────────────────────────────────────────────
        ds_events = build_ds_events()
        for ds in ds_events:
            publish(producer, TOPIC_DELIVERY_STATIONS, ds["delivery_station_id"], ds)
        logger.info(f"Published {len(ds_events)} record(s) → topic: {TOPIC_DELIVERY_STATIONS}")

        # ── VEHICLES ──────────────────────────────────────────────────────────
        vehicle_events = build_vehicle_events()
        for vehicle in vehicle_events:
            publish(producer, TOPIC_VEHICLES, vehicle["vehicle_id"], vehicle)
        logger.info(f"Published {len(vehicle_events)} record(s) → topic: {TOPIC_VEHICLES}")

        # Flush ensures every message is delivered before the process exits
        producer.flush(timeout=10)

        logger.info("=" * 60)
        logger.info("Batch producer completed successfully.")
        logger.info(
            f"  Summary: "
            f"{len(fc_events)} FCs | "
            f"{len(ds_events)} DSs | "
            f"{len(vehicle_events)} Vehicles"
        )
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Batch producer failed: {e}", exc_info=True)
        producer.flush()  # Best-effort drain before exit on error
        sys.exit(1)


if __name__ == "__main__":
    main()