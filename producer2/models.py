# ─────────────────────────────────────────────────────────────────────────────
# Dataclasses for stateful, mutable entities that travel across multiple modules.
# Only entities whose state changes at runtime belong here.
# Static entities (FCs, DSs) remain as plain dicts in master_data.py.
# ─────────────────────────────────────────────────────────────────────────────

from dataclasses import dataclass
from typing import Optional

@dataclass
class Shipment:
    """
    Tracks an active in-flight shipment through its full lifecycle.
    Created at ORDER_ALLOCATED_TO_FC, removed from memory once DELIVERED.
    
    Fields never change after creation:
        shipment_id, order_id, customer_id, customer_tier,
        delivery_type, order_value, fc_id, ds_id, promised_delivery_time

    Fields updated at each stage transition:
        current_status, eta, is_delayed, delay_minutes,
        assigned_truck_id, assigned_bike_id
    """
    shipment_id:             str
    order_id:                str
    customer_id:             str
    customer_tier:           str            # STANDARD or PRIME
    delivery_type:           str            # STANDARD or SAME_DAY
    order_value:             float
    fc_id:                   str            # Fulfillment Center assigned to this shipment
    ds_id:                   str            # Delivery Station for last-mile delivery
    current_status:          str            # Current stage from config.SHIPMENT_STATUSES
    promised_delivery_time:  str            # ISO-8601 UTC — set once at order creation, never changes
    eta:                     str            # ISO-8601 UTC — recalculated at each stage transition

    # Mutable fields — updated as the shipment progresses
    is_delayed:              bool           = False  # True if shipment is running behind ETA
    delay_minutes:           int            = 0      # Minutes added due to delay
    assigned_truck_id:       Optional[str]  = None   # Populated at DISPATCHED_FROM_FC
    assigned_bike_id:        Optional[str]  = None   # Populated at ASSIGNED_TO_DRIVER


@dataclass
class Vehicle:
    """
    Represents a truck or bike with a mutable availability status.
    Trucks handle the FC → DS leg (capacity: 200 shipments).
    Bikes handle the DS → Customer last-mile leg (capacity: 10 shipments).

    Only bike status is actively managed in the stream simulation —
    bikes are marked IN_USE at ASSIGNED_TO_DRIVER and AVAILABLE again after DELIVERED.
    Trucks are referenced by ID in events but their status is not toggled per shipment
    since a single truck carries many shipments simultaneously.
    """
    vehicle_id:         str
    vehicle_type:       str    # TRUCK or BIKE
    home_facility_id:   str    # FC for trucks, DS for bikes
    home_facility_type: str    # FULFILLMENT_CENTER or DELIVERY_STATION
    capacity:           int    # Number of shipments the vehicle can carry
    status:             str    # AVAILABLE, IN_USE, MAINTENANCE