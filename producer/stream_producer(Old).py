import json
import random
import time
from datetime import datetime, timedelta, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# ----------------------------------------
# Kafka
# ----------------------------------------

producer = Producer({
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094"
})

TOPIC = "shipment-events"

# ----------------------------------------
# Static Master Data
# ----------------------------------------

FULFILLMENT_CENTERS = [
    {
        "id": "FC_DELHI",
        "lat": 28.6139,
        "lon": 77.2090
    },
    {
        "id": "FC_MUMBAI",
        "lat": 19.0760,
        "lon": 72.8777
    }
]

DELIVERY_STATIONS = [
    {
        "id": "DS_DELHI_NORTH",
        "lat": 28.7041,
        "lon": 77.1025
    },
    {
        "id": "DS_DELHI_SOUTH",
        "lat": 28.5355,
        "lon": 77.3910
    }
]

FLOW = [
    (
        "DISPATCHED_FROM_FULFILLMENT_CENTER",
        "IN_TRANSIT",
        "FULFILLMENT_CENTER"
    ),
    (
        "ARRIVED_AT_DELIVERY_STATION",
        "AT_DELIVERY_STATION",
        "DELIVERY_STATION"
    ),
    (
        "OUT_FOR_DELIVERY",
        "OUT_FOR_DELIVERY",
        "DELIVERY_STATION"
    ),
    (
        "DELIVERED",
        "DELIVERED",
        None
    )
]

active_shipments = {}

shipment_counter = 1
event_counter = 1


def utc_now():
    return datetime.now(timezone.utc)


def iso(ts):
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def create_new_shipment():
    global shipment_counter

    shipment_id = f"SHP_{shipment_counter:06d}"
    order_id = f"ORD_{shipment_counter:06d}"

    fc = random.choice(FULFILLMENT_CENTERS)
    ds = random.choice(DELIVERY_STATIONS)

    eta = utc_now() + timedelta(hours=random.randint(4, 10))

    active_shipments[shipment_id] = {
        "order_id": order_id,
        "fc": fc,
        "ds": ds,
        "eta": eta,
        "state": 0
    }

    shipment_counter += 1

    return shipment_id


def next_event(shipment_id):
    global event_counter

    shipment = active_shipments[shipment_id]

    idx = shipment["state"]

    event_type, status, facility_type = FLOW[idx]

    if facility_type == "FULFILLMENT_CENTER":
        facility = shipment["fc"]
    elif facility_type == "DELIVERY_STATION":
        facility = shipment["ds"]
    else:
        facility = {
            "id": None,
            "lat": shipment["ds"]["lat"],
            "lon": shipment["ds"]["lon"]
        }

    payload = {
        "event_id": f"EVT_{event_counter:08d}",
        "event_timestamp": iso(utc_now()),
        "shipment_id": shipment_id,
        "order_id": shipment["order_id"],
        "event_type": event_type,
        "shipment_status": status,
        "facility_type": facility_type,
        "facility_id": facility["id"],
        "estimated_delivery_time": iso(shipment["eta"]),
        "latitude": facility["lat"],
        "longitude": facility["lon"]
    }

    shipment["state"] += 1

    if shipment["state"] >= len(FLOW):
        del active_shipments[shipment_id]

    event_counter += 1

    return payload


def delivery_callback(err, msg):
    if err:
        print(err)


print("Starting Shipment Producer...\n")

while True:

    if len(active_shipments) < 10 or random.random() < 0.30:
        shipment = create_new_shipment()
    else:
        shipment = random.choice(list(active_shipments.keys()))

    event = next_event(shipment)

    producer.produce(
        TOPIC,
        key=event["shipment_id"],
        value=json.dumps(event),
        callback=delivery_callback
    )

    producer.poll(0)

    print(json.dumps(event, indent=2))

    time.sleep(1)