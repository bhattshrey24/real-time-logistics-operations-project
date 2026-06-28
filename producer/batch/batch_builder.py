# ─────────────────────────────────────────────────────────────────────────────
# Builds Kafka-ready payload dicts for all master data entities.
# Reads from master_data.py.
#
# Three public functions:
#   build_fc_events()       → list of FC payloads for `fulfillment_centers` topic
#   build_ds_events()       → list of DS payloads for `delivery_stations` topic
#   build_vehicle_events()  → list of vehicle payloads for `vehicles` topic
#
# SCD simulation:
#   Each run of build_fc_events() / build_ds_events() applies a random ±20%
#   capacity change to each facility, simulating real-world capacity adjustments
#   (seasonal scaling, maintenance windows, etc.). Running the batch producer
#   manually triggers a new set of changes — downstream consumers detect the
#   diff and apply SCD logic. Identity and geo fields are never mutated.
# ─────────────────────────────────────────────────────────────────────────────

import random
from dataclasses import asdict

from master_data import FULFILLMENT_CENTERS, DELIVERY_STATIONS, VEHICLES
from utils import now_utc, to_iso


def _now() -> str:
    return to_iso(now_utc())

def _today() -> str:
    return now_utc().strftime("%Y-%m-%d")

def _mutate_capacity(base_capacity: int) -> int:
    """Applies a random ±20% change to a capacity value, rounded to nearest 50."""
    factor = random.uniform(0.80, 1.20)
    return round(base_capacity * factor / 50) * 50


def build_fc_events() -> list[dict]:
    """
    Builds one payload per Fulfillment Center for the `fulfillment_centers` topic.
    daily_capacity is randomly varied ±20% on each call to simulate SCD changes.
    Identity and geo fields (id, name, city, region, state, lat/lon) are never mutated.
    """
    batch_date = _today()
    updated_at = _now()
    events = []

    for fc in FULFILLMENT_CENTERS.values():
        events.append({
            "fulfillment_center_id":   fc["fulfillment_center_id"],
            "fulfillment_center_name": fc["fulfillment_center_name"],
            "city":                    fc["city"],
            "region":                  fc["region"],
            "state":                   fc["state"],
            "daily_capacity":          _mutate_capacity(fc["daily_capacity"]),
            "latitude":                fc["latitude"],
            "longitude":               fc["longitude"],
            "updated_at":              updated_at,
            "batch_date":              batch_date,
        })

    return events


def build_ds_events() -> list[dict]:
    """
    Builds one payload per Delivery Station for the `delivery_stations` topic.
    daily_capacity is randomly varied ±20% on each call to simulate SCD changes.
    Identity and geo fields are never mutated.
    """
    batch_date = _today()
    updated_at = _now()
    events = []

    for ds in DELIVERY_STATIONS.values():
        events.append({
            "delivery_station_id":   ds["delivery_station_id"],
            "delivery_station_name": ds["delivery_station_name"],
            "city":                  ds["city"],
            "region":                ds["region"],
            "state":                 ds["state"],
            "daily_capacity":        _mutate_capacity(ds["daily_capacity"]),
            "latitude":              ds["latitude"],
            "longitude":             ds["longitude"],
            "updated_at":            updated_at,
            "batch_date":            batch_date,
        })

    return events


def build_vehicle_events() -> list[dict]:
    """
    Builds one payload per Vehicle for the `vehicles` topic.
    Published as a static initial load — no mutations applied.
    Vehicle status changes during streaming are tracked in-memory only.
    """
    batch_date = _today()
    updated_at = _now()
    events = []

    for vehicle in VEHICLES.values():
        vehicle_dict = asdict(vehicle)
        vehicle_dict["updated_at"]  = updated_at
        vehicle_dict["batch_date"]  = batch_date
        events.append(vehicle_dict)

    return events