# ─────────────────────────────────────────────────────────────────────────────
# Builds Kafka-ready payload dicts from a Shipment object.
#
# Two public functions:
#   build_order_event(shipment)    → payload for `orders` topic
#   build_shipment_event(shipment) → payload for `shipment_events` topic
# ─────────────────────────────────────────────────────────────────────────────

from models import Shipment
from config import (
    STAGE_TO_FACILITY_TYPE,
    FACILITY_TYPE_FC,
    FACILITY_TYPE_DS,
)
from master_data import FULFILLMENT_CENTERS, DELIVERY_STATIONS
from utils import now_utc, to_iso, next_event_id, jitter_coords


def build_order_event(shipment: Shipment) -> dict:
    """Builds the `orders` topic payload. Published once per shipment at ORDER_ALLOCATED_TO_FC."""
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
    Builds the `shipment_events` topic payload. Published at every lifecycle stage transition.
    Resolves facility and vehicle based on current stage, and attaches jittered GPS coordinates.
    """
    status        = shipment.current_status
    facility_type = STAGE_TO_FACILITY_TYPE[status]

    # Resolve facility id and GPS base coordinates
    if facility_type == FACILITY_TYPE_FC:
        facility_id     = shipment.fc_id
        facility_coords = FULFILLMENT_CENTERS[facility_id]
    elif facility_type == FACILITY_TYPE_DS:
        facility_id     = shipment.ds_id
        facility_coords = DELIVERY_STATIONS[facility_id]
    else:
        # OUT_FOR_DELIVERY / DELIVERED: package is with the driver, no facility.
        # Use DS coords as the GPS anchor since that's the last known fixed location.
        facility_id     = None
        facility_coords = DELIVERY_STATIONS[shipment.ds_id]

    # GPS: small random offset around the base location (~1 km radius)
    latitude, longitude = jitter_coords(facility_coords["latitude"], facility_coords["longitude"])

    return {
        # ── Core identifiers ──────────────────────────────────────────────────
        "event_id":               next_event_id(),
        "event_timestamp":        to_iso(now_utc()),
        "shipment_id":            shipment.shipment_id,
        "order_id":               shipment.order_id,

        # ── Status fields ─────────────────────────────────────────────────────
        "event_type":             status,
        "shipment_status":        status,

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
    """Returns the truck_id (FC→DS leg), bike_id (last-mile), or None (no vehicle assigned yet)."""
    status = shipment.current_status

    if status in ("DISPATCHED_FROM_FC", "RECEIVED_AT_DS"):
        return shipment.assigned_truck_id   # Truck carried it from FC to DS

    if status in ("ASSIGNED_TO_DRIVER", "OUT_FOR_DELIVERY", "DELIVERED"):
        return shipment.assigned_bike_id    # Bike handling last-mile delivery

    return None  # No vehicle assigned at ORDER_ALLOCATED_TO_FC or PICKED_AND_PACKED