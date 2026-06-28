"""
Central configuration for the Real-Time Logistics Operations Platform.

All configurable values should live here so the rest of the application
doesn't contain hardcoded values.
"""
# ============================================================
# Kafka Configuration
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]
KAFKA_TOPIC = "shipment-events"
KAFKA_ACKS = "all" # this is producer acknowledgement level and "all" = Leader waits for all in-sync replicas

# ============================================================
# Producer Speed Configuration
# ============================================================

EVENT_INTERVAL_SECONDS = 1 # Generate one event every second

# ============================================================
# Shipment Generation
# ============================================================

MAX_ACTIVE_SHIPMENTS = 500 # Since we are keeping state of each shipment in memory till it gets delivered therefore 
# this capping makes sure we dont crash our PC. this means at a time only 500 active shipments can exist in memory

# This is Probability of a new event :-
# 30% chances of it being a new Event
# 70% chances of it being an update to an old event (because this is what usually also happens, most events are just updates of delivery status)
NEW_SHIPMENT_PROBABILITY = 0.30

# ============================================================
# Delivery ETA
# ============================================================

# Estimated delivery window after dispatch
MIN_DELIVERY_HOURS = 4
MAX_DELIVERY_HOURS = 12

# ============================================================
# Random Delay Simulation
# ============================================================

# Percentage of shipments that become delayed
DELAY_PROBABILITY = 0.10

# Delay duration (minutes)
MIN_DELAY_MINUTES = 15
MAX_DELAY_MINUTES = 120

# ============================================================
# Shipment Event Flow
# ============================================================

SHIPMENT_FLOW = [
    {
        "event_type": "DISPATCHED_FROM_FULFILLMENT_CENTER",
        "shipment_status": "IN_TRANSIT",
        "facility_type": "FULFILLMENT_CENTER",
    },
    {
        "event_type": "ARRIVED_AT_DELIVERY_STATION",
        "shipment_status": "AT_DELIVERY_STATION",
        "facility_type": "DELIVERY_STATION",
    },
    {
        "event_type": "OUT_FOR_DELIVERY",
        "shipment_status": "OUT_FOR_DELIVERY",
        "facility_type": "DELIVERY_STATION",
    },
    {
        "event_type": "DELIVERED",
        "shipment_status": "DELIVERED",
        "facility_type": None,
    },
]

# ============================================================
# Faker
# ============================================================

# Generate realistic Indian data
FAKER_LOCALE = "en_IN"