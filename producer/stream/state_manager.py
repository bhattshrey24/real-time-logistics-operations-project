# ─────────────────────────────────────────────────────────────────────────────
# Manages the in-memory lifecycle of all active shipments. All state changes happen here.
#
# Public API (used by stream_producer.py):
#   create_shipment()           → creates a new shipment at stage 0
#   advance_shipment(id)        → moves a shipment to the next stage
#   remove_shipment(id)         → removes a delivered shipment, frees its bike
#   pick_shipment_to_advance()  → returns a random active shipment_id
#   get_active_count()          → returns number of in-flight shipments
# ─────────────────────────────────────────────────────────────────────────────

import random
import logging
from typing import Optional

from models import Shipment
from config import SHIPMENT_STATUSES, DELAY_PROBABILITY
from master_data import (
    FULFILLMENT_CENTERS,
    FC_TO_DS_MAP,
    FC_TO_TRUCK_MAP,
    DS_TO_BIKES_MAP,
    VEHICLES,
)
from utils import (
    next_order_id,
    next_shipment_id,
    now_utc,
    calc_promised_time,
    calc_eta,
    random_customer_id,
    random_customer_tier,
    random_delivery_type,
    random_order_value,
    sample_delay_minutes,
)

logger = logging.getLogger(__name__)


# ── IN-MEMORY STATE ───────────────────────────────────────────────────────────
# All active (in-flight) shipments keyed by shipment_id.
# A shipment enters this dict at ORDER_ALLOCATED_TO_FC.
# It is removed after the DELIVERED event is published.
_active_shipments: dict[str, Shipment] = {}


# ── PUBLIC API ────────────────────────────────────────────────────────────────

def get_active_count() -> int:
    """Returns the number of currently in-flight shipments."""
    return len(_active_shipments)


def pick_shipment_to_advance() -> Optional[str]:
    """
    Picks a random shipment_id from the active pool.
    Returns None if no shipments are currently active.
    """
    if not _active_shipments:
        return None
    return random.choice(list(_active_shipments.keys()))


def create_shipment() -> Shipment:
    """
    Creates a new shipment at the ORDER_ALLOCATED_TO_FC stage.

    Steps:
    1. Randomly picks a Fulfillment Center and a Delivery Station under it.
    2. Generates IDs and randomizes customer attributes.
    3. Calculates promised delivery time based on SLA.
    4. Decides upfront if this shipment will be delayed (once, at creation).
    5. Calculates initial ETA including any delay.
    6. Adds shipment to the active pool and returns it.
    """
    # Pick a random FC, then a DS that belongs to it
    fc_id = random.choice(list(FULFILLMENT_CENTERS.keys()))
    ds_id = random.choice(FC_TO_DS_MAP[fc_id])

    # Customer and order attributes
    customer_tier = random_customer_tier()
    delivery_type = random_delivery_type(customer_tier)

    order_time             = now_utc()
    promised_delivery_time = calc_promised_time(order_time, delivery_type)

    # Delay is decided once at creation — it persists through all stages
    is_delayed    = random.random() < DELAY_PROBABILITY
    delay_minutes = sample_delay_minutes(delivery_type) if is_delayed else 0

    shipment = Shipment(
        shipment_id            = next_shipment_id(),
        order_id               = next_order_id(),
        customer_id            = random_customer_id(),
        customer_tier          = customer_tier,
        delivery_type          = delivery_type,
        order_value            = random_order_value(),
        fc_id                  = fc_id,
        ds_id                  = ds_id,
        current_status         = "ORDER_ALLOCATED_TO_FC",
        promised_delivery_time = promised_delivery_time,
        eta                    = calc_eta(promised_delivery_time, delay_minutes),
        is_delayed             = is_delayed,
        delay_minutes          = delay_minutes,
    )

    _active_shipments[shipment.shipment_id] = shipment
    return shipment


def advance_shipment(shipment_id: str) -> Optional[Shipment]:
    """
    Moves an active shipment to the next lifecycle stage.

    Stage-specific logic:
    - DISPATCHED_FROM_FC  → assigns the FC's truck to the shipment
    - ASSIGNED_TO_DRIVER  → finds an available bike at the DS, marks it IN_USE.
                            Returns None if no bike is available (both busy).

    For all stages:
    - Updates current_status to the next stage.
    - Recalculates ETA from now, carrying forward any delay.

    Returns the updated Shipment, or None if the advance could not be completed.
    """
    shipment = _active_shipments.get(shipment_id)
    if shipment is None:
        logger.warning(f"advance_shipment: {shipment_id} not found in active shipments.")
        return None

    # Determine next stage using list index — keeps ordering in one place (config.py)
    current_index = SHIPMENT_STATUSES.index(shipment.current_status)
    next_index    = current_index + 1

    if next_index >= len(SHIPMENT_STATUSES):
        # Already at DELIVERED — should never be in the active pool
        logger.warning(f"advance_shipment: {shipment_id} is already at terminal stage.")
        return None

    next_status = SHIPMENT_STATUSES[next_index]

    # ── STAGE-SPECIFIC VEHICLE LOGIC ──────────────────────────────────────────
    if next_status == "DISPATCHED_FROM_FC":
        # Assign the truck registered to this FC (truck carries multiple shipments,
        # so we don't toggle its status — just record the assignment on the shipment)
        shipment.assigned_truck_id = FC_TO_TRUCK_MAP[shipment.fc_id]

    elif next_status == "ASSIGNED_TO_DRIVER":
        # Find the first available bike at this shipment's Delivery Station
        bike_ids        = DS_TO_BIKES_MAP[shipment.ds_id]
        available_bike  = next(
            (bid for bid in bike_ids if VEHICLES[bid].status == "AVAILABLE"),
            None,
        )
        if available_bike is None:
            # Both bikes are currently IN_USE — skip this shipment for now
            logger.warning(
                f"No available bike at {shipment.ds_id} for {shipment_id}. "
                f"Will retry on next tick."
            )
            return None

        # Claim the bike — it will be released in remove_shipment() after DELIVERED
        VEHICLES[available_bike].status = "IN_USE"
        shipment.assigned_bike_id       = available_bike

    # ── COMMON UPDATE FOR ALL STAGES ──────────────────────────────────────────
    shipment.current_status = next_status
    # ETA does not change on stage transitions — it was fixed at creation.
    # It would only be updated here if a new delay were detected mid-journey.

    return shipment


def remove_shipment(shipment_id: str) -> None:
    """
    Removes a delivered shipment from the active pool and frees its assigned bike.
    Called by stream_producer AFTER the DELIVERED event has been published.

    Order matters:
    1. Pop the shipment (stop it from being picked for further advancement).
    2. Free the bike (make it available for the next shipment at this DS).
    """
    shipment = _active_shipments.pop(shipment_id, None)
    if shipment is None:
        return

    if shipment.assigned_bike_id:
        VEHICLES[shipment.assigned_bike_id].status = "AVAILABLE"
        logger.info(
            f"Bike {shipment.assigned_bike_id} freed after delivery of {shipment_id}."
        )