# ─────────────────────────────────────────────────────────────────────────────
# Static master data for all facilities and vehicles.
# Loaded once at startup — FCs and DSs never change at runtime.
# Vehicle status (specifically bikes) is mutated during stream simulation.
# ─────────────────────────────────────────────────────────────────────────────

from models import Vehicle
from config import FACILITY_TYPE_FC, FACILITY_TYPE_DS


# ── FULFILLMENT CENTERS ───────────────────────────────────────────────────────
# Plain dicts — static config, never mutated.
FULFILLMENT_CENTERS = {
    "FC_DELHI": {
        "fulfillment_center_id":   "FC_DELHI",
        "fulfillment_center_name": "Delhi Fulfillment Center",
        "city":                    "Delhi",
        "region":                  "North",
        "state":                   "Delhi",
        "daily_capacity":          5000,
        "latitude":                28.6139,
        "longitude":               77.2090,
    },
    "FC_BENGALURU": {
        "fulfillment_center_id":   "FC_BENGALURU",
        "fulfillment_center_name": "Bengaluru Fulfillment Center",
        "city":                    "Bengaluru",
        "region":                  "South",
        "state":                   "Karnataka",
        "daily_capacity":          5000,
        "latitude":                12.9716,
        "longitude":               77.5946,
    },
}


# ── DELIVERY STATIONS ─────────────────────────────────────────────────────────
# Plain dicts — static config, never mutated.
DELIVERY_STATIONS = {
    "DS_DELHI_NORTH": {
        "delivery_station_id":   "DS_DELHI_NORTH",
        "delivery_station_name": "North Delhi Delivery Station",
        "city":                  "Delhi",
        "region":                "North",
        "state":                 "Delhi",
        "daily_capacity":        2500,
        "latitude":              28.7041,
        "longitude":             77.1025,
    },
    "DS_DELHI_SOUTH": {
        "delivery_station_id":   "DS_DELHI_SOUTH",
        "delivery_station_name": "South Delhi Delivery Station",
        "city":                  "Delhi",
        "region":                "South",
        "state":                 "Delhi",
        "daily_capacity":        2500,
        "latitude":              28.5244,
        "longitude":             77.1855,
    },
    "DS_BLR_NORTH": {
        "delivery_station_id":   "DS_BLR_NORTH",
        "delivery_station_name": "North Bengaluru Delivery Station",
        "city":                  "Bengaluru",
        "region":                "North",
        "state":                 "Karnataka",
        "daily_capacity":        2500,
        "latitude":              13.0358,
        "longitude":             77.5970,
    },
    "DS_BLR_SOUTH": {
        "delivery_station_id":   "DS_BLR_SOUTH",
        "delivery_station_name": "South Bengaluru Delivery Station",
        "city":                  "Bengaluru",
        "region":                "South",
        "state":                 "Karnataka",
        "daily_capacity":        2500,
        "latitude":              12.9141,
        "longitude":             77.6101,
    },
}


# ── FC → DS MAPPING ───────────────────────────────────────────────────────────
# Defines which Delivery Stations belong to each Fulfillment Center.
# Used by state_manager when creating a new shipment to pick the correct DS.
FC_TO_DS_MAP = {
    "FC_DELHI":     ["DS_DELHI_NORTH", "DS_DELHI_SOUTH"],
    "FC_BENGALURU": ["DS_BLR_NORTH",   "DS_BLR_SOUTH"],
}


# ── VEHICLES ──────────────────────────────────────────────────────────────────
# Vehicle dataclass instances — status field is mutable for bikes.
# Trucks: 1 per FC — FC → DS leg. Status not toggled per shipment (bulk carrier).
# Bikes:  2 per DS — DS → Customer leg. Status toggled per delivery assignment.
VEHICLES = {

    # ── TRUCKS ────────────────────────────────────────────────────────────────
    "TRUCK_DELHI": Vehicle(
        vehicle_id         = "TRUCK_DELHI",
        vehicle_type       = "TRUCK",
        home_facility_id   = "FC_DELHI",
        home_facility_type = FACILITY_TYPE_FC,
        capacity           = 200,
        status             = "AVAILABLE",
    ),
    "TRUCK_BLR": Vehicle(
        vehicle_id         = "TRUCK_BLR",
        vehicle_type       = "TRUCK",
        home_facility_id   = "FC_BENGALURU",
        home_facility_type = FACILITY_TYPE_FC,
        capacity           = 200,
        status             = "AVAILABLE",
    ),

    # ── BIKES — DS_DELHI_NORTH ────────────────────────────────────────────────
    "BIKE_DS_DELHI_NORTH_1": Vehicle(
        vehicle_id         = "BIKE_DS_DELHI_NORTH_1",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_DELHI_NORTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),
    "BIKE_DS_DELHI_NORTH_2": Vehicle(
        vehicle_id         = "BIKE_DS_DELHI_NORTH_2",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_DELHI_NORTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),

    # ── BIKES — DS_DELHI_SOUTH ────────────────────────────────────────────────
    "BIKE_DS_DELHI_SOUTH_1": Vehicle(
        vehicle_id         = "BIKE_DS_DELHI_SOUTH_1",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_DELHI_SOUTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),
    "BIKE_DS_DELHI_SOUTH_2": Vehicle(
        vehicle_id         = "BIKE_DS_DELHI_SOUTH_2",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_DELHI_SOUTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),

    # ── BIKES — DS_BLR_NORTH ──────────────────────────────────────────────────
    "BIKE_DS_BLR_NORTH_1": Vehicle(
        vehicle_id         = "BIKE_DS_BLR_NORTH_1",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_BLR_NORTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),
    "BIKE_DS_BLR_NORTH_2": Vehicle(
        vehicle_id         = "BIKE_DS_BLR_NORTH_2",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_BLR_NORTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),

    # ── BIKES — DS_BLR_SOUTH ──────────────────────────────────────────────────
    "BIKE_DS_BLR_SOUTH_1": Vehicle(
        vehicle_id         = "BIKE_DS_BLR_SOUTH_1",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_BLR_SOUTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),
    "BIKE_DS_BLR_SOUTH_2": Vehicle(
        vehicle_id         = "BIKE_DS_BLR_SOUTH_2",
        vehicle_type       = "BIKE",
        home_facility_id   = "DS_BLR_SOUTH",
        home_facility_type = FACILITY_TYPE_DS,
        capacity           = 10,
        status             = "AVAILABLE",
    ),
}


# ── CONVENIENCE LOOKUPS ───────────────────────────────────────────────────────
# DS → bike vehicle IDs assigned to that station.
# Used by state_manager to find and assign an available bike.
DS_TO_BIKES_MAP = {
    "DS_DELHI_NORTH": ["BIKE_DS_DELHI_NORTH_1", "BIKE_DS_DELHI_NORTH_2"],
    "DS_DELHI_SOUTH": ["BIKE_DS_DELHI_SOUTH_1", "BIKE_DS_DELHI_SOUTH_2"],
    "DS_BLR_NORTH":   ["BIKE_DS_BLR_NORTH_1",   "BIKE_DS_BLR_NORTH_2"],
    "DS_BLR_SOUTH":   ["BIKE_DS_BLR_SOUTH_1",   "BIKE_DS_BLR_SOUTH_2"],
}

# FC → truck vehicle ID for that FC.
# Used by state_manager to populate truck assignment at DISPATCHED_FROM_FC.
FC_TO_TRUCK_MAP = {
    "FC_DELHI":     "TRUCK_DELHI",
    "FC_BENGALURU": "TRUCK_BLR",
}