"""

Main entry point for the Real-Time Logistics Operations Platform.

Responsibilities:
- Create Kafka Producer
- Generate one shipment event every second
- Publish event to Kafka
"""

import json
import signal
import sys
import time
from dataclasses import asdict

from confluent_kafka import Producer

from config import (
    EVENT_INTERVAL_SECONDS,
    KAFKA_ACKS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)
from shipment_generator import ShipmentGenerator

# ============================================================
# Delivery Callback
# ============================================================

def delivery_report(err, msg):
    """
    Callback executed after Kafka acknowledges a message.
    """
    if err is not None:
        print(f"Delivery failed: {err}")
        return
    print(
        f"Sent | "
        f"Partition={msg.partition()} "
        f"Offset={msg.offset()}"
    )

# ============================================================
# Kafka Producer
# ============================================================

producer = Producer(
    {
        "bootstrap.servers": ",".join(KAFKA_BOOTSTRAP_SERVERS),
        "acks": KAFKA_ACKS,
    }
)

# ============================================================
# Shutdown Producer
# ============================================================

def shutdown(sig, frame):
    global running
    print("\nShutting down producer...")
    running = False
    producer.flush() # produce() does not immediately send the message to Kafka. It first stores it in an internal memory buffer and sends them asynchronously in background. Flush tells kafka "Don't exit until every buffered message has either been delivered or failed."
    print("Producer stopped successfully.")
    sys.exit(0)

# "signal" lets python program catch operating system signals such as pressing Ctrl+C or docker container stopping
signal.signal(signal.SIGINT, shutdown)  # SIGINT → Tells to execute the function when user presses Ctrl+C in the terminal
signal.signal(signal.SIGTERM, shutdown) # SIGTERM -> Tells to execute the function when docker container stops 

# ============================================================
# Main Loop
# ============================================================

generator = ShipmentGenerator()
running = True

print("=" * 60)
print("Real-Time Logistics Shipment Producer Started")
print(f"Topic   : {KAFKA_TOPIC}")
print(f"Brokers : {', '.join(KAFKA_BOOTSTRAP_SERVERS)}")
print("=" * 60)

while running:

    # Generate next shipment event
    shipment_event = generator.generate_event()

    # Convert dataclass -> dict -> JSON
    payload = json.dumps(asdict(shipment_event)) # Converts Python dictionary into string

    producer.produce(
        topic=KAFKA_TOPIC,
        key=shipment_event.shipment_id,
        value=payload,
        callback=delivery_report,
    )
    producer.poll(0) # Trigger delivery callbacks i.e. tells kafka to run the callback when current message gets delivered and we get acknowledgement from broker
    print(json.dumps(asdict(shipment_event), indent=4))
    time.sleep(EVENT_INTERVAL_SECONDS)

producer.flush()