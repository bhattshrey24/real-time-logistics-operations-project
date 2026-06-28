# event_builder.py
# ─────────────────────────────────────────────────────────────────────────────
# Builds Kafka-ready payload dicts from a Shipment object.
# These functions know nothing about Kafka — they just return Python dicts.
# All field names match the agreed payload specs exactly.
#
# Two public functions:
#   build_order_event(shipment)    → payload for `orders` topic
#   build_shipment_event(shipment) → payload for `shipment_events` topic
# ─────────────────────────────────────────────────────────────────────────────

from models import Shipment
from config import (
    STATUS_EVENT_MAP,
    STAGE_TO_FACILITY_TYPE,
    FACILITY_TYPE_FC,
)
from master_data import FULFILLMENT_CENTERS, DELIVERY_STATIONS
from utils import now_utc, to_iso, next_event_id, jitter_coords


def build_order_event(shipment: Shipment) -> dict:
    """
    Builds the payload for the `orders` Kafka topic.

    Published ONCE per shipment — only at ORDER_ALLOCATED_TO_FC stage.
    Represents an order coming in from the OMS (Order Management System).

    Note: order_status is always "ALLOCATED_TO_FC" here because orders
    are only published to this topic at the moment of FC allocation.
    """
    return {
        "order_id":               shipment.order_id,
        "event_timestamp":        to_iso(now_utc()),
        "customer_id":            shipment.customer_id,
        "customer_tier":          shipment.customer_tier,         # STANDARD or PRIME
        "delivery_type":          shipment.delivery_type,         # STANDARD or SAME_DAY
        "order_value":            shipment.order_value,
        "fulfillment_center_id":  shipment.fc_id,
        "delivery_station_id":    shipment.ds_id,
        "promised_delivery_time": shipment.promised_delivery_time,
        "shipment_id":            shipment.shipment_id,
        "order_status":           "ALLOCATED_TO_FC",              # Fixed for the orders topic
    }


def build_shipment_event(shipment: Shipment) -> dict:
    """
    Builds the payload for the `shipment_events` Kafka topic.

    Published at EVERY lifecycle stage transition.
    Represents a tracking event from the TMS (Transportation Management System).

    Facility logic:
        FC stages (ORDER_ALLOCATED_TO_FC → DISPATCHED_FROM_FC) → facility = FC
        DS stages (RECEIVED_AT_DS → DELIVERED)                 → facility = DS

    Vehicle logic:
        ORDER_ALLOCATED_TO_FC, PICKED_AND_PACKED → no vehicle yet         (None)
        DISPATCHED_FROM_FC, RECEIVED_AT_DS       → truck (FC → DS leg)
        ASSIGNED_TO_DRIVER, OUT_FOR_DELIVERY, DELIVERED → bike (last-mile)

    Extra fields beyond the base spec (useful for downstream dashboards):
        promised_delivery_time → needed to calculate SLA breach
        customer_tier, delivery_type → needed to flag Prime / Same-Day shipments
        is_delayed, delay_minutes → pre-computed delay signals for alert consumers
        latitude, longitude → jittered GPS near the current facility
    """
    status        = shipment.current_status
    facility_type = STAGE_TO_FACILITY_TYPE[status]

    # Resolve facility and its base coordinates
    if facility_type == FACILITY_TYPE_FC:
        facility_id = shipment.fc_id
        facility    = FULFILLMENT_CENTERS[facility_id]
    else:
        facility_id = shipment.ds_id
        facility    = DELIVERY_STATIONS[facility_id]

    # GPS: small random offset around the facility (~1 km radius)
    latitude, longitude = jitter_coords(facility["latitude"], facility["longitude"])

    # event_type and shipment_status come from the central map in config.py
    event_meta = STATUS_EVENT_MAP[status]

    return {
        # ── Core identifiers ──────────────────────────────────────────────────
        "event_id":               next_event_id(),
        "event_timestamp":        to_iso(now_utc()),
        "shipment_id":            shipment.shipment_id,
        "order_id":               shipment.order_id,

        # ── Status fields ─────────────────────────────────────────────────────
        "event_type":             event_meta["event_type"],       # What happened (transition)
        "shipment_status":        event_meta["shipment_status"],  # Resulting state

        # ── Facility context ──────────────────────────────────────────────────
        "facility_id":            facility_id,
        "facility_type":          facility_type,

        # ── Vehicle context ───────────────────────────────────────────────────
        "vehicle_id":             _resolve_vehicle_id(shipment),  # Truck, bike, or None

        # ── Timing ───────────────────────────────────────────────────────────
        "estimated_delivery_time":  shipment.eta,
        "promised_delivery_time":   shipment.promised_delivery_time,

        # ── Delay signals (for real-time alert consumers) ─────────────────────
        "is_delayed":             shipment.is_delayed,
        "delay_minutes":          shipment.delay_minutes,

        # ── Shipment attributes (for dashboard filtering) ─────────────────────
        "customer_tier":          shipment.customer_tier,
        "delivery_type":          shipment.delivery_type,

        # ── GPS location ──────────────────────────────────────────────────────
        "latitude":               latitude,
        "longitude":              longitude,
    }


# ── PRIVATE HELPERS ───────────────────────────────────────────────────────────

def _resolve_vehicle_id(shipment: Shipment):
    """
    Determines which vehicle_id to include in the shipment event
    based on the current lifecycle stage.

    Returns:
        truck_id  → at DISPATCHED_FROM_FC and RECEIVED_AT_DS (FC → DS leg)
        bike_id   → at ASSIGNED_TO_DRIVER, OUT_FOR_DELIVERY, DELIVERED (last-mile)
        None      → before any vehicle is assigned (ORDER_ALLOCATED_TO_FC, PICKED_AND_PACKED)
    """
    status = shipment.current_status

    if status in ("DISPATCHED_FROM_FC", "RECEIVED_AT_DS"):
        return shipment.assigned_truck_id   # Truck carried it from FC to DS

    if status in ("ASSIGNED_TO_DRIVER", "OUT_FOR_DELIVERY", "DELIVERED"):
        return shipment.assigned_bike_id    # Bike handling last-mile delivery

    return None  # No vehicle assigned at ORDER_ALLOCATED_TO_FC or PICKED_AND_PACKED