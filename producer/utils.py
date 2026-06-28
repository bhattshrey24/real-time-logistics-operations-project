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
# Global counters producing clean sequential IDs: ORD_100001, SHP_900001, EVT_100001

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
    """Returns the current UTC datetime."""
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    """Formats a datetime as an ISO-8601 UTC string (e.g. 2026-07-01T10:15:20Z)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def calc_promised_time(order_time: datetime, delivery_type: str) -> str:
    """Returns the promised delivery deadline — order time + SLA hours for the given delivery type."""
    hours = SLA_HOURS[delivery_type]
    return to_iso(order_time + timedelta(hours=hours))


def calc_eta(promised_delivery_time: str, delay_minutes: int = 0) -> str:
    """Returns ETA as promised delivery time + any delay. Fixed at creation; only changes if a delay is applied."""
    promised_dt = datetime.strptime(promised_delivery_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return to_iso(promised_dt + timedelta(minutes=delay_minutes))


# ── GPS HELPERS ───────────────────────────────────────────────────────────────

def jitter_coords(base_lat: float, base_lon: float) -> tuple[float, float]:
    """Adds a ±0.01° random offset (~1 km) to facility coordinates to simulate package location."""
    lat = round(base_lat + random.uniform(-0.01, 0.01), 6)
    lon = round(base_lon + random.uniform(-0.01, 0.01), 6)
    return lat, lon


# ── ORDER & CUSTOMER HELPERS ──────────────────────────────────────────────────

def random_order_value() -> float:
    """Returns a random order value in INR within the configured range."""
    return round(random.uniform(*ORDER_VALUE_RANGE), 2)


def random_customer_id() -> str:
    """Samples a customer ID from the pre-existing pool (e.g. CUST_42817)."""
    return f"CUST_{random.randint(*CUSTOMER_ID_RANGE)}"


def random_customer_tier() -> str:
    """Returns a weighted random tier — 70% STANDARD, 30% PRIME."""
    return random.choices(CUSTOMER_TIERS, weights=CUSTOMER_TIER_WEIGHTS, k=1)[0]


def random_delivery_type(customer_tier: str) -> str:
    """Returns a delivery type weighted by tier — PRIME gets 50% SAME_DAY, STANDARD gets 10%."""
    weights = PRIME_DELIVERY_TYPE_WEIGHTS if customer_tier == "PRIME" else STANDARD_DELIVERY_TYPE_WEIGHTS
    return random.choices(DELIVERY_TYPES, weights=weights, k=1)[0]


def random_delay_minutes(delivery_type: str) -> int:
    """Returns a random delay in minutes for the given delivery type (SAME_DAY: 15–90 min, STANDARD: 2–8 hrs)."""
    return random.randint(*DELAY_RANGE_MINUTES[delivery_type])