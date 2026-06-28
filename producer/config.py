# ─────────────────────────────────────────────────────────────────────────────
# Single source of truth for all constants, Kafka config, and tuning settings.
# ─────────────────────────────────────────────────────────────────────────────

# ── KAFKA ─────────────────────────────────────────────────────────────────────
# Internal: used when producers run inside the Docker network
KAFKA_BOOTSTRAP_SERVERS_INTERNAL = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
# External: used when producers run from host machine (e.g. PyCharm / terminal)
KAFKA_BOOTSTRAP_SERVERS_EXTERNAL = "localhost:9092,localhost:9093,localhost:9094"
# ← Switch this based on where you are running the producer
KAFKA_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS_EXTERNAL


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


# ── FACILITY TYPES ────────────────────────────────────────────────────────────
FACILITY_TYPE_FC = "FULFILLMENT_CENTER"
FACILITY_TYPE_DS = "DELIVERY_STATION"

# Maps each lifecycle stage to the facility type where the shipment currently sits.
# OUT_FOR_DELIVERY and DELIVERED have no facility — the package is with the driver.
STAGE_TO_FACILITY_TYPE = {
    "ORDER_ALLOCATED_TO_FC": FACILITY_TYPE_FC,
    "PICKED_AND_PACKED":     FACILITY_TYPE_FC,
    "DISPATCHED_FROM_FC":    FACILITY_TYPE_FC,
    "RECEIVED_AT_DS":        FACILITY_TYPE_DS,
    "ASSIGNED_TO_DRIVER":    FACILITY_TYPE_DS,
    "OUT_FOR_DELIVERY":      None,
    "DELIVERED":             None,
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


# ── DELAY SIMULATION ──────────────────────────────────────────────────────────
DELAY_PROBABILITY = 0.20   # 20% of shipments will experience a delay

# Delay magnitude varies by delivery type — on a 48hr window, minutes are noise.
DELAY_RANGE_MINUTES = {
    "SAME_DAY": (15, 90),      # 15–90 minutes: meaningful against a 6hr SLA
    "STANDARD": (120, 480),    # 2–8 hours: missed dispatch window, wrong route, etc.
}

# ── ALERT THRESHOLDS ──────────────────────────────────────────────────────────
# Defined here so both producers and downstream consumers use the same values.
# Delay thresholds are split by delivery type — 30 min is meaningful for SAME_DAY
# but noise for STANDARD where delays start at 2 hours.
DELAY_ALERT_THRESHOLD_MINUTES = {
    "SAME_DAY": 30,    # Alert if SAME_DAY shipment is delayed >30 minutes
    "STANDARD": 120,   # Alert if STANDARD shipment is delayed >2 hours
}
FC_BACKLOG_THRESHOLD          = 10   # Alert if FC backlog exceeds 10 shipments
DS_BACKLOG_THRESHOLD          = 8    # Alert if DS backlog exceeds 8 shipments
FC_STALE_THRESHOLD_HOURS      = 4    # Alert if shipment hasn't moved from FC in 4 hours
DS_STALE_THRESHOLD_HOURS      = 1    # Alert if shipment hasn't moved from DS in 1 hour
ETA_CHANGE_ALERT_MINUTES      = {
    "SAME_DAY": 30,    # Alert if SAME_DAY ETA increases by more than 30 minutes
    "STANDARD": 120,   # Alert if STANDARD ETA increases by more than 2 hours
}

# ── ORDER SETTINGS ────────────────────────────────────────────────────────────
ORDER_VALUE_RANGE = (199.0, 9999.0)   # Realistic INR order value range
CUSTOMER_ID_RANGE = (10000, 99999)    # Pool of existing customer IDs to sample from