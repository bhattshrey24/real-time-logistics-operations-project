
"""
Generates realistic shipment lifecycle events for the Real-Time Logistics
Operations Platform.
"""
from dataclasses import asdict

from config import (
    DELAY_PROBABILITY,
    MAX_DELAY_MINUTES,
    MIN_DELAY_MINUTES,
    MIN_DELIVERY_HOURS,
    MAX_DELIVERY_HOURS,
    MAX_ACTIVE_SHIPMENTS,
    NEW_SHIPMENT_PROBABILITY,
    SHIPMENT_FLOW,
)

from master_data import (
    FULFILLMENT_CENTERS,
    DELIVERY_STATIONS,
    ROUTES,
    VEHICLES,
)

from models import ShipmentEvent, ActiveShipment

from utils import (
    generate_event_id,
    generate_order_id,
    generate_shipment_id,
    generate_event_timestamp,
    generate_estimated_delivery_time,
    probability,
    random_choice,
    random_delay,
    random_latitude,
    random_longitude,
)


class ShipmentGenerator:

    def __init__(self):
        self.active_shipments = {} # initializing with empty dictionary, active_shipments dictionary will have "key" as id of event and value as the "active shipment object" itself

    def generate_event(self) -> ShipmentEvent:
        if (
            len(self.active_shipments) == 0
            or probability(NEW_SHIPMENT_PROBABILITY)
        ):
            shipment = self._create_new_shipment()
        else:
            shipment = random_choice(list(self.active_shipments.values())) # this randomly picks one fo the active shipments 

        event = self._create_event(shipment)
        self._advance_state(shipment)
        return event

    def _create_new_shipment(self) -> ActiveShipment:

        route = random_choice(ROUTES) # Randomly selects a route

        warehouse = next( # spits out next value of iterator
            fc for fc in FULFILLMENT_CENTERS # iterator
            if fc["warehouse_id"] == route["warehouse_id"] # Finding which warehouse was randomly selected 
        )

        station = next(
            ds for ds in DELIVERY_STATIONS
            if ds["delivery_station_id"] == route["delivery_station_id"] # Finding which delivery station was randomly selected 
        )

        station_vehicles = [
            v for v in VEHICLES
            if v["delivery_station_id"] == station["delivery_station_id"]
        ]

        vehicle = random_choice(station_vehicles) # Since each DS has 2 vehicle therefore randomly selecting 1 out of those 2

        delayed = probability(DELAY_PROBABILITY) # DELAY_PROBABILITY = 10 so this function will return true 10% of times

        delay_minutes = (
            random_delay(MIN_DELAY_MINUTES, MAX_DELAY_MINUTES)
            if delayed else 0
        )

        shipment = ActiveShipment(
            shipment_id=generate_shipment_id(),
            order_id=generate_order_id(),
            warehouse_id=warehouse["warehouse_id"],
            delivery_station_id=station["delivery_station_id"],
            route_id=route["route_id"],
            vehicle_id=vehicle["vehicle_id"],
            estimated_delivery_time=generate_estimated_delivery_time(
                MIN_DELIVERY_HOURS,
                MAX_DELIVERY_HOURS,
                delay_minutes,
            ),
            current_step=0, # since its new event therefore it starts at step 0
            latitude=warehouse["latitude"], # step 0 is warehouse therefore we are taking warehous's lat and lon
            longitude=warehouse["longitude"],
            is_delayed=delayed,
            delay_minutes=delay_minutes,
        )

        self.active_shipments[shipment.shipment_id] = shipment
        return shipment

    def _create_event(self, shipment: ActiveShipment) -> ShipmentEvent:

        flow = SHIPMENT_FLOW[shipment.current_step] # This tells on which step is the current shipment

        if flow["facility_type"] == "FULFILLMENT_CENTER":
            facility_id = shipment.warehouse_id

        elif flow["facility_type"] == "DELIVERY_STATION":
            facility_id = shipment.delivery_station_id

        else:
            facility_id = None # Vehicle is not a facility

        if flow["facility_type"] == "FULFILLMENT_CENTER":
            fc = next(
                x for x in FULFILLMENT_CENTERS
                if x["warehouse_id"] == shipment.warehouse_id
            )

            lat = random_latitude(fc["latitude"])
            lon = random_longitude(fc["longitude"])

        else:
            ds = next(
                x for x in DELIVERY_STATIONS
                if x["delivery_station_id"] == shipment.delivery_station_id
            )

            lat = random_latitude(ds["latitude"])
            lon = random_longitude(ds["longitude"])

        return ShipmentEvent(
            event_id=generate_event_id(),
            event_timestamp=generate_event_timestamp(),
            shipment_id=shipment.shipment_id,
            order_id=shipment.order_id,
            event_type=flow["event_type"],
            shipment_status=flow["shipment_status"],
            facility_type=flow["facility_type"],
            facility_id=facility_id,
            estimated_delivery_time=shipment.estimated_delivery_time,
            latitude=lat,
            longitude=lon,
        )

    def _advance_state(self, shipment: ActiveShipment):

        shipment.current_step += 1

        if shipment.current_step >= len(SHIPMENT_FLOW):
            self.active_shipments.pop(shipment.shipment_id, None)

    def active_count(self) -> int:
        return len(self.active_shipments)

    def active_shipments_snapshot(self): # snapshot because shipment state keeps changing and this whenever called simply takes a snapshot of all current active shipment state
        return [asdict(s) for s in self.active_shipments.values()]
