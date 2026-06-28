# stream_producer.py
# ─────────────────────────────────────────────────────────────────────────────
# Entry point for the streaming data generator.
# Runs a tick-based loop every second that either creates a new shipment
# or advances an existing one, then publishes events to Kafka.
#
# How to run (from the project root):
#   python producers/stream/stream_producer.py
#
# Tick logic (every second):
#   - No active shipments         → always create
#   - Active shipments at MAX     → always advance
#   - Otherwise                   → 30% create / 70% advance
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os

# ── PATH FIX ─────────────────────────────────────────────────────────────────
# Adds `producers/` to sys.path so sibling modules (config, models, utils, etc.)
# and the stream package are importable regardless of where this file is run from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import random
import logging

from confluent_kafka import Producer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDERS,
    TOPIC_SHIPMENT_EVENTS,
    TICK_INTERVAL_SECONDS,
    MAX_ACTIVE_SHIPMENTS,
    NEW_SHIPMENT_PROBABILITY,
)
from stream.state_manager import (
    create_shipment,
    advance_shipment,
    remove_shipment,
    pick_shipment_to_advance,
    get_active_count,
)
from stream.event_builder import build_order_event, build_shipment_event


# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt = "%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── KAFKA HELPERS ─────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    """
    Callback fired by Confluent Kafka after each message is delivered or fails.
    Runs asynchronously — does not block the main loop.
    """
    if err:
        logger.error(f"Delivery failed | topic={msg.topic()} | error={err}")
    else:
        logger.debug(
            f"Delivered | topic={msg.topic()} "
            f"| partition={msg.partition()} | offset={msg.offset()}"
        )


def publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    """
    Serializes a payload dict to JSON and produces it to the given Kafka topic.

    Key is the shipment_id or order_id — ensures all events for the same
    shipment land on the same partition (preserving ordering per shipment).

    producer.poll(0) is non-blocking: it only triggers already-ready callbacks.
    The final flush() at the end of each tick ensures nothing is left buffered.
    """
    producer.produce(
        topic    = topic,
        key      = key,
        value    = json.dumps(payload),
        callback = delivery_report,
    )
    producer.poll(0)  # Trigger delivery callbacks for messages already sent


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────

def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    logger.info("=" * 60)
    logger.info("Stream producer started.")
    logger.info(f"  Bootstrap servers : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Max active ships  : {MAX_ACTIVE_SHIPMENTS}")
    logger.info(f"  Tick interval     : {TICK_INTERVAL_SECONDS}s")
    logger.info(f"  New ship prob     : {NEW_SHIPMENT_PROBABILITY * 100:.0f}%")
    logger.info("=" * 60)

    while True:
        try:
            active_count = get_active_count()

            # ── DECIDE: CREATE NEW OR ADVANCE EXISTING ────────────────────────
            if active_count == 0:
                # Pool is empty — must create to have anything to advance
                action = "create"
            elif active_count >= MAX_ACTIVE_SHIPMENTS:
                # At capacity — advance only to keep the pool from growing
                action = "advance"
            else:
                # Mixed: randomly choose based on configured probability
                action = "create" if random.random() < NEW_SHIPMENT_PROBABILITY else "advance"

            # ── CREATE NEW SHIPMENT ───────────────────────────────────────────
            if action == "create":
                shipment = create_shipment()

                # Publish to `orders` topic — only at ORDER_ALLOCATED_TO_FC
                order_payload = build_order_event(shipment)
                publish(producer, TOPIC_ORDERS, shipment.order_id, order_payload)

                # Publish to `shipment_events` topic for this initial stage
                shipment_payload = build_shipment_event(shipment)
                publish(producer, TOPIC_SHIPMENT_EVENTS, shipment.shipment_id, shipment_payload)

                logger.info(
                    f"[NEW]     {shipment.shipment_id} "
                    f"| status={shipment.current_status} "
                    f"| tier={shipment.customer_tier} "
                    f"| type={shipment.delivery_type} "
                    f"| delayed={shipment.is_delayed} "
                    f"| active={get_active_count()}"
                )

            # ── ADVANCE EXISTING SHIPMENT ─────────────────────────────────────
            else:
                shipment_id = pick_shipment_to_advance()
                if shipment_id is None:
                    # Shouldn't happen since we check active_count, but guard anyway
                    time.sleep(TICK_INTERVAL_SECONDS)
                    continue

                shipment = advance_shipment(shipment_id)
                if shipment is None:
                    # advance_shipment returns None when a bike isn't available.
                    # Skip this tick — the bike will free up when another shipment delivers.
                    logger.warning(f"[SKIP]    {shipment_id} | Could not advance — retrying next tick.")
                    time.sleep(TICK_INTERVAL_SECONDS)
                    continue

                # Publish the shipment event for this new stage
                shipment_payload = build_shipment_event(shipment)
                publish(producer, TOPIC_SHIPMENT_EVENTS, shipment.shipment_id, shipment_payload)

                logger.info(
                    f"[ADVANCE] {shipment.shipment_id} "
                    f"| status={shipment.current_status} "
                    f"| eta={shipment.eta} "
                    f"| active={get_active_count()}"
                )

                # ── POST-DELIVERY CLEANUP ─────────────────────────────────────
                # Remove AFTER publishing so the DELIVERED event still gets sent.
                # remove_shipment() also frees the assigned bike back to AVAILABLE.
                if shipment.current_status == "DELIVERED":
                    remove_shipment(shipment.shipment_id)
                    logger.info(
                        f"[DONE]    {shipment.shipment_id} "
                        f"| DELIVERED — removed from pool | active={get_active_count()}"
                    )

            # Flush ensures all buffered messages are sent before the next tick
            producer.flush(timeout=5)
            time.sleep(TICK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Stream producer stopped by user (KeyboardInterrupt).")
            producer.flush()  # Drain any remaining messages before exit
            break

        except Exception as e:
            # Log and keep running — a single bad event shouldn't kill the producer
            logger.error(f"Unexpected error on tick: {e}", exc_info=True)
            time.sleep(TICK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()