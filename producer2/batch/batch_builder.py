# ─────────────────────────────────────────────────────────────────────────────
# Builds Kafka-ready payload dicts for all master data entities.
# Reads from master_data.py.
#
# Three public functions:
#   build_fc_events()       → list of FC payloads for `fulfillment_centers` topic
#   build_ds_events()       → list of DS payloads for `delivery_stations` topic
#   build_vehicle_events()  → list of vehicle payloads for `vehicles` topic
# ─────────────────────────────────────────────────────────────────────────────

from dataclasses import asdict

from master_data import FULFILLMENT_CENTERS, DELIVERY_STATIONS, VEHICLES
from utils import now_utc


def _today() -> str:
    """Returns today's date as YYYY-MM-DD. Used as batch_date on all records."""
    return now_utc().strftime("%Y-%m-%d")


def build_fc_events() -> list[dict]:
    """
    Builds one payload per Fulfillment Center for the `fulfillment_centers` topic.

    Includes all fields from master_data plus:
        batch_date  → date this record was published (for partitioning in the data lake)
        latitude/longitude → already in master_data; useful for geographic BI dashboards

    Returns a list of dicts, one per FC.
    """
    batch_date = _today()
    events = []

    for fc in FULFILLMENT_CENTERS.values():
        events.append({
            "fulfillment_center_id":   fc["fulfillment_center_id"],
            "fulfillment_center_name": fc["fulfillment_center_name"],
            "city":                    fc["city"],
            "region":                  fc["region"],
            "state":                   fc["state"],
            "daily_capacity":          fc["daily_capacity"],
            "latitude":                fc["latitude"],
            "longitude":               fc["longitude"],
            "batch_date":              batch_date,
        })

    return events


def build_ds_events() -> list[dict]:
    """
    Builds one payload per Delivery Station for the `delivery_stations` topic.

    Mirrors the FC structure — same extra fields (batch_date, lat/lon) for
    consistency across all master data payloads.

    Returns a list of dicts, one per DS.
    """
    batch_date = _today()
    events = []

    for ds in DELIVERY_STATIONS.values():
        events.append({
            "delivery_station_id":   ds["delivery_station_id"],
            "delivery_station_name": ds["delivery_station_name"],
            "city":                  ds["city"],
            "region":                ds["region"],
            "state":                 ds["state"],
            "daily_capacity":        ds["daily_capacity"],
            "latitude":              ds["latitude"],
            "longitude":             ds["longitude"],
            "batch_date":            batch_date,
        })

    return events


def build_vehicle_events() -> list[dict]:
    """
    Builds one payload per Vehicle for the `vehicles` topic.

    Uses dataclasses.asdict() to convert the Vehicle dataclass to a dict —
    this automatically includes all fields and stays in sync if the model grows.

    Status published here reflects the initial state (all AVAILABLE at startup).
    Vehicle status changes during streaming are tracked in-memory only —
    they are not re-published to this topic during the run.

    Returns a list of dicts, one per vehicle (trucks + bikes).
    """
    batch_date = _today()
    events = []

    for vehicle in VEHICLES.values():
        vehicle_dict = asdict(vehicle)          # Converts Vehicle dataclass → plain dict
        vehicle_dict["batch_date"] = batch_date # Attach the batch date
        events.append(vehicle_dict)

    return events