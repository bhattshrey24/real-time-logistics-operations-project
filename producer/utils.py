# ─────────────────────────────────────────────────────────────────────────────
# Pure helper functions — no state, no side effects.
# Every function takes inputs and returns a result. Nothing is mutated here.
# ─────────────────────────────────────────────────────────────────────────────

import random
from datetime import datetime, timezone, timedelta

from config import (
    SLA_HOURS,
    ORDER_VALUE_RANGE,
    CUSTOMER_ID_RANGE,
    CUSTOMER_TIERS,
    DELIVERY_TYPES,
    CUSTOMER_TIER_WEIGHTS,
    PRIME_DELIVERY_TYPE_WEIGHTS,
    STANDARD_DELIVERY_TYPE_WEIGHTS,
    DELAY_RANGE_MINUTES,
)


# ── SEQUENTIAL ID GENERATORS ──────────────────────────────────────────────────
# Simple global counters that increment on every call.
# Produces clean sequential IDs: ORD_100001, SHP_900001, EVT_100001

_order_counter    = 100_000
_shipment_counter = 900_000
_event_counter    = 100_000


def next_order_id() -> str:
    global _order_counter
    _order_counter += 1
    return f"ORD_{_order_counter}"


def next_shipment_id() -> str:
    global _shipment_counter
    _shipment_counter += 1
    return f"SHP_{_shipment_counter}"


def next_event_id() -> str:
    global _event_counter
    _event_counter += 1
    return f"EVT_{_event_counter}"


# ── TIMESTAMP HELPERS ─────────────────────────────────────────────────────────
def now_utc() -> datetime:
    """Returns the current moment as a UTC datetime object."""
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str: # Observe its return type is string because When you send 
    # data to Kafka, JSON, Spark, or another system, you cannot send a Python datetime object 
    # directly. So we convert it to a standard string format called ISO 8601 because then at 
    # recieving end you can easily convert it back.
    """
    Formats a datetime as an ISO-8601 UTC string.
    Example output: 2026-07-01T10:15:20Z
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def calc_promised_time(order_time: datetime, delivery_type: str) -> str:
    """
    Calculates the promised delivery deadline from order creation time.
    SAME_DAY  → order_time + 6 hours
    STANDARD  → order_time + 48 hours
    Returns an ISO-8601 UTC string.
    """
    hours = SLA_HOURS[delivery_type]
    return to_iso(order_time + timedelta(hours=hours))


def calc_eta(promised_delivery_time: str, delay_minutes: int = 0) -> str:
    """
    Computes ETA from the promised delivery time plus any delay.

    ETA is fixed at creation and only changes if the shipment is delayed.
    Stage transitions alone do not affect ETA.

    Returns an ISO-8601 UTC string.
    """
    promised_dt = datetime.strptime(promised_delivery_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return to_iso(promised_dt + timedelta(minutes=delay_minutes))


# ── GPS HELPERS ───────────────────────────────────────────────────────────────
def jitter_coords(base_lat: float, base_lon: float) -> tuple[float, float]:
    """
    Adds a small random offset to a facility's base coordinates.
    Simulates the shipment being somewhere near the facility, not exactly at it.
    Offset range: ±0.01 degrees ≈ ±1 km radius.
    Returns (latitude, longitude) rounded to 6 decimal places.
    """
    lat = round(base_lat + random.uniform(-0.01, 0.01), 6)
    lon = round(base_lon + random.uniform(-0.01, 0.01), 6)
    return lat, lon


# ── ORDER & CUSTOMER HELPERS ──────────────────────────────────────────────────
def random_order_value() -> float:
    """
    Returns a realistic random order value in INR.
    """
    return round(random.uniform(*ORDER_VALUE_RANGE), 2)


def random_customer_id() -> str:
    """
    Picks a random customer ID from the configured pool.
    Customers are pre-existing entities — we sample, not create in this project.
    Example output: CUST_42817
    """
    return f"CUST_{random.randint(*CUSTOMER_ID_RANGE)}"


def random_customer_tier() -> str:
    """
    Returns a weighted random customer tier.
    70% STANDARD, 30% PRIME (weights from config).
    """
    return random.choices(CUSTOMER_TIERS, weights=CUSTOMER_TIER_WEIGHTS, k=1)[0]


def random_delivery_type(customer_tier: str) -> str:
    """
    Returns a delivery type weighted by customer tier.
    PRIME     → 50% STANDARD, 50% SAME_DAY  (Prime customers are granted same-day more)
    STANDARD  → 90% STANDARD, 10% SAME_DAY  (Most standard orders use standard delivery)
    """
    if customer_tier == "PRIME":
        weights = PRIME_DELIVERY_TYPE_WEIGHTS
    else:
        weights = STANDARD_DELIVERY_TYPE_WEIGHTS
    return random.choices(DELIVERY_TYPES, weights=weights, k=1)[0]


def sample_delay_minutes(delivery_type: str) -> int:
    """
    Returns a random delay duration in minutes for the given delivery type.
    SAME_DAY: 15–90 min.  STANDARD: 2–8 hours.
    """
    return random.randint(*DELAY_RANGE_MINUTES[delivery_type])