"""
Common utility functions used throughout the Real-Time Logistics
Operations Platform.
"""

import random
from datetime import datetime, timedelta, timezone
from faker import Faker
from config import FAKER_LOCALE

# ============================================================
# Faker
# ============================================================

fake = Faker(FAKER_LOCALE)

# ============================================================
# Timestamp Utilities
# ============================================================


def utc_now() -> datetime:
    """
    Returns the current UTC datetime.
    """
    return datetime.now(timezone.utc) # Observe this still is a python object


def to_iso8601(dt: datetime) -> str: # # Observe its return type is string because When you send 
    # data to Kafka, JSON, Spark, or another system, you cannot send a Python datetime object 
    # directly. So we convert it to a standard string format called ISO 8601 because then at 
    # recieving end you can easily convert it back.
    """
    Converts a datetime object to ISO-8601 format.
    Example:
    2026-07-01T10:15:32Z
    """
    return (
        dt.replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def generate_event_timestamp() -> str:
    """
    Generates the current event timestamp.
    """
    return to_iso8601(utc_now())


def generate_estimated_delivery_time(
    min_hours: int,
    max_hours: int,
    delay_minutes: int = 0,
) -> str:
    """
    Generates a realistic ETA.

    Example:
    Current Time + 6 hours + 25 minute delay. Note : delay is optional 
    """
    eta = utc_now() + timedelta(
        hours=random.randint(min_hours, max_hours),
        minutes=delay_minutes,
    )
    return to_iso8601(eta)

# ============================================================
# ID Generators
# ============================================================

_event_counter = 100000 # Starting point for event id
_order_counter = 90000 # Starting point for event id
_shipment_counter = 50000 # Starting point for event id


def generate_event_id() -> str:
    """
    Generates sequential event IDs.

    Example:
    EVT_100001
    """

    global _event_counter

    _event_counter += 1

    return f"EVT_{_event_counter}"


def generate_order_id() -> str:
    """
    Generates sequential order IDs.

    Example:
    ORD_90001
    """

    global _order_counter

    _order_counter += 1

    return f"ORD_{_order_counter}"


def generate_shipment_id() -> str:
    """
    Generates sequential shipment IDs.

    Example:
    SHP_50001
    """

    global _shipment_counter

    _shipment_counter += 1

    return f"SHP_{_shipment_counter}"


# ============================================================
# Random Helpers
# ============================================================


def random_choice(items):
    """
    Returns a random element from a list.
    """
    return random.choice(items)


def probability(chance: float) -> bool:
    """
    Returns True with the supplied probability.

    Example:

    probability(0.30)
    -> 30% chance of returning True
    """
    return random.random() <= chance # random.random() generates a random decimal in range [0,1]


def random_delay(
    min_minutes: int,
    max_minutes: int,
) -> int:
    """
    Generates a random shipment delay in minutes.
    """

    return random.randint(min_minutes, max_minutes)


# ============================================================
# Faker Helpers
# ============================================================


def generate_uuid() -> str:
    """
    Generates a UUID.
    Useful for future extensions.
    """
    return fake.uuid4()


def random_latitude(
    center_lat: float,
    variation: float = 0.01,
) -> float:
    """
    Generates a latitude slightly offset from
    a given location.
    """

    return round(
        center_lat + random.uniform(-variation, variation), 
        6,
    ) # random.uniform generates random float in the given range which is here [-v,+v]


def random_longitude(
    center_lon: float,
    variation: float = 0.01,
) -> float:
    """
    Generates a longitude slightly offset from
    a given location.
    """

    return round(
        center_lon + random.uniform(-variation, variation),
        6,
    )