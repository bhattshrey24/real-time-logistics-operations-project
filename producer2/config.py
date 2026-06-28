# ─────────────────────────────────────────────────────────────────────────────
# Single source of truth for all constants, Kafka config, and tuning settings.
# ─────────────────────────────────────────────────────────────────────────────

# ── KAFKA ─────────────────────────────────────────────────────────────────────
# Internal: used when producers run inside the Docker network
KAFKA_BOOTSTRAP_SERVERS_INTERNAL = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
# External: used when producers run from host machine (e.g. PyCharm / terminal)
KAFKA_BOOTSTRAP_SERVERS_EXTERNAL = "localhost:9092,localhost:9093,localhost:9094"
# ← Switch this based on where you are running the producer
KAFKA_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS_INTERNAL


# ── TOPIC NAMES ───────────────────────────────────────────────────────────────
TOPIC_ORDERS              = "orders"
TOPIC_SHIPMENT_EVENTS     = "shipment_events"
TOPIC_FULFILLMENT_CENTERS = "fulfillment_centers"
TOPIC_DELIVERY_STATIONS   = "delivery_stations"
TOPIC_VEHICLES            = "vehicles"


# ── STREAM GENERATOR SETTINGS ─────────────────────────────────────────────────
TICK_INTERVAL_SECONDS    = 1      # Main loop runs every 1 second
MAX_ACTIVE_SHIPMENTS     = 20     # Since we are keeping state of each shipment in memory till it gets delivered therefore 
# this capping makes sure we dont crash our PC. this means at a time only 20 active shipments can exist in memory
NEW_SHIPMENT_PROBABILITY = 0.30   # 30% chance of creating new shipment vs advancing an existing one. Its 30% because this is what usually also happens, most events are just updates of delivery status

# ── SHIPMENT LIFECYCLE ────────────────────────────────────────────────────────
# Ordered list — position determines the next stage in the state machine
SHIPMENT_STATUSES = [
    "ORDER_ALLOCATED_TO_FC",
    "PICKED_AND_PACKED",
    "DISPATCHED_FROM_FC",
    "RECEIVED_AT_DS",
    "ASSIGNED_TO_DRIVER",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
]

# Maps each lifecycle stage to the exact field values for the shipment_events payload.
# event_type = what happened (the transition / action)
# shipment_status = resulting state after the event
# Note: RECEIVED_AT_DS is the only stage where these two differ (per payload spec)
STATUS_EVENT_MAP = {
    "ORDER_ALLOCATED_TO_FC": {
        "event_type":      "ORDER_ALLOCATED_TO_FC",
        "shipment_status": "ORDER_ALLOCATED_TO_FC",
    },
    "PICKED_AND_PACKED": {
        "event_type":      "PICKED_AND_PACKED",
        "shipment_status": "PICKED_AND_PACKED",
    },
    "DISPATCHED_FROM_FC": {
        "event_type":      "DISPATCHED_FROM_FC",
        "shipment_status": "DISPATCHED_FROM_FC",
    },
    "RECEIVED_AT_DS": {
        "event_type":      "RECEIVED_AT_DS",
        "shipment_status": "AT_DELIVERY_STATION",   # State name differs from event name
    },
    "ASSIGNED_TO_DRIVER": {
        "event_type":      "ASSIGNED_TO_DRIVER",
        "shipment_status": "ASSIGNED_TO_DRIVER",
    },
    "OUT_FOR_DELIVERY": {
        "event_type":      "OUT_FOR_DELIVERY",
        "shipment_status": "OUT_FOR_DELIVERY",
    },
    "DELIVERED": {
        "event_type":      "DELIVERED",
        "shipment_status": "DELIVERED",
    },
}


# ── FACILITY TYPES ────────────────────────────────────────────────────────────
FACILITY_TYPE_FC = "FULFILLMENT_CENTER"
FACILITY_TYPE_DS = "DELIVERY_STATION"

# Maps each lifecycle stage to the facility type where the shipment currently sits.
# Used in event_builder to populate the facility_type field consistently.
STAGE_TO_FACILITY_TYPE = {
    "ORDER_ALLOCATED_TO_FC": FACILITY_TYPE_FC,
    "PICKED_AND_PACKED":     FACILITY_TYPE_FC,
    "DISPATCHED_FROM_FC":    FACILITY_TYPE_FC,
    "RECEIVED_AT_DS":        FACILITY_TYPE_DS,
    "ASSIGNED_TO_DRIVER":    FACILITY_TYPE_DS,
    "OUT_FOR_DELIVERY":      FACILITY_TYPE_DS,
    "DELIVERED":             FACILITY_TYPE_DS,
}


# ── CUSTOMER & DELIVERY CONFIG ────────────────────────────────────────────────
CUSTOMER_TIERS = ["STANDARD", "PRIME"]
DELIVERY_TYPES = ["STANDARD", "SAME_DAY"]

# Weighted probabilities for customer tier selection
CUSTOMER_TIER_WEIGHTS = [0.70, 0.30]   # 70% Standard, 30% Prime

# Delivery type weights are tier-dependent (defined in utils.py):
#   PRIME customers    → 50% STANDARD, 50% SAME_DAY
#   STANDARD customers → 90% STANDARD, 10% SAME_DAY
PRIME_DELIVERY_TYPE_WEIGHTS    = [0.50, 0.50]
STANDARD_DELIVERY_TYPE_WEIGHTS = [0.90, 0.10]

# Promised delivery SLA from order creation time
SLA_HOURS = {
    "SAME_DAY": 6,
    "STANDARD": 48,
}


# ── ETA OFFSETS PER STAGE ─────────────────────────────────────────────────────
# Approximate minutes remaining to delivery when a shipment is at each stage.
# Used by calc_eta() in utils.py to generate realistic estimated delivery times.
ETA_OFFSET_MINUTES = {
    "ORDER_ALLOCATED_TO_FC": 300,   # ~5 hours remaining
    "PICKED_AND_PACKED":     240,   # ~4 hours remaining
    "DISPATCHED_FROM_FC":    180,   # ~3 hours remaining
    "RECEIVED_AT_DS":        120,   # ~2 hours remaining
    "ASSIGNED_TO_DRIVER":     60,   # ~1 hour remaining
    "OUT_FOR_DELIVERY":       30,   # ~30 minutes remaining
    "DELIVERED":               0,
}


# ── DELAY SIMULATION ──────────────────────────────────────────────────────────
DELAY_PROBABILITY   = 0.20        # 20% of shipments will experience a delay
DELAY_RANGE_MINUTES = (15, 90)    # Delay randomly picked between 15 and 90 minutes

# ── ALERT THRESHOLDS ──────────────────────────────────────────────────────────
# Defined here so both producers and downstream consumers use the same values.
DELAY_ALERT_THRESHOLD_MINUTES = 30   # Alert if shipment delay exceeds 30 minutes
FC_BACKLOG_THRESHOLD          = 10   # Alert if FC backlog exceeds 10 shipments
DS_BACKLOG_THRESHOLD          = 8    # Alert if DS backlog exceeds 8 shipments
FC_STALE_THRESHOLD_HOURS      = 4    # Alert if shipment hasn't moved from FC in 4 hours
DS_STALE_THRESHOLD_HOURS      = 1    # Alert if shipment hasn't moved from DS in 1 hour
ETA_CHANGE_ALERT_MINUTES      = 30   # Alert if ETA increases by more than 30 minutes

# ── ORDER SETTINGS ────────────────────────────────────────────────────────────
ORDER_VALUE_RANGE = (199.0, 9999.0)   # Realistic INR order value range
CUSTOMER_ID_RANGE = (10000, 99999)    # Pool of existing customer IDs to sample from