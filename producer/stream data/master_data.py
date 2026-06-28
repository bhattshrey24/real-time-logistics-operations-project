"""
Static reference (master) data used by the shipment generator.

In a real company, this data would come from systems like:
- Warehouse Management System (WMS)
- Transportation Management System (TMS)
- Fleet Management System (FMS)

For this project, we keep it in memory to simulate those systems.
"""

# ============================================================
# Fulfillment Centers (Warehouses)
# ============================================================

FULFILLMENT_CENTERS = [
    {
        "warehouse_id": "FC_DELHI",
        "warehouse_name": "Delhi Fulfillment Center",
        "city": "Delhi",
        "region": "North",
        "capacity": 100000,
        "latitude": 28.6139,
        "longitude": 77.2090,
    },
    {
        "warehouse_id": "FC_MUMBAI",
        "warehouse_name": "Mumbai Fulfillment Center",
        "city": "Mumbai",
        "region": "West",
        "capacity": 95000,
        "latitude": 19.0760,
        "longitude": 72.8777,
    },
    {
        "warehouse_id": "FC_BANGALORE",
        "warehouse_name": "Bangalore Fulfillment Center",
        "city": "Bengaluru",
        "region": "South",
        "capacity": 110000,
        "latitude": 12.9716,
        "longitude": 77.5946,
    },
    {
        "warehouse_id": "FC_KOLKATA",
        "warehouse_name": "Kolkata Fulfillment Center",
        "city": "Kolkata",
        "region": "East",
        "capacity": 85000,
        "latitude": 22.5726,
        "longitude": 88.3639,
    },
]

# ============================================================
# Delivery Stations (for simplicity only keeping 2 DS for each city)
# ============================================================

DELIVERY_STATIONS = [
    {
        "delivery_station_id": "DS_DELHI_NORTH",
        "station_name": "North Delhi Delivery Station",
        "city": "Delhi",
        "region": "North",
        "daily_capacity": 5000,
        "latitude": 28.7041,
        "longitude": 77.1025,
    },
    {
        "delivery_station_id": "DS_DELHI_SOUTH",
        "station_name": "South Delhi Delivery Station",
        "city": "Delhi",
        "region": "North",
        "daily_capacity": 4500,
        "latitude": 28.5355,
        "longitude": 77.3910,
    },
    {
        "delivery_station_id": "DS_MUMBAI_NORTH",
        "station_name": "North Mumbai Delivery Station",
        "city": "Mumbai",
        "region": "West",
        "daily_capacity": 5200,
        "latitude": 19.1726,
        "longitude": 72.9425,
    },
    {
        "delivery_station_id": "DS_MUMBAI_SOUTH",
        "station_name": "South Mumbai Delivery Station",
        "city": "Mumbai",
        "region": "West",
        "daily_capacity": 5000,
        "latitude": 18.9440,
        "longitude": 72.8235,
    },
    {
        "delivery_station_id": "DS_BANGALORE_NORTH",
        "station_name": "North Bangalore Delivery Station",
        "city": "Bengaluru",
        "region": "South",
        "daily_capacity": 5800,
        "latitude": 13.0358,
        "longitude": 77.5970,
    },
    {
        "delivery_station_id": "DS_BANGALORE_SOUTH",
        "station_name": "South Bangalore Delivery Station",
        "city": "Bengaluru",
        "region": "South",
        "daily_capacity": 5600,
        "latitude": 12.9081,
        "longitude": 77.6476,
    },
    {
        "delivery_station_id": "DS_KOLKATA_NORTH",
        "station_name": "North Kolkata Delivery Station",
        "city": "Kolkata",
        "region": "East",
        "daily_capacity": 4300,
        "latitude": 22.6574,
        "longitude": 88.3702,
    },
    {
        "delivery_station_id": "DS_KOLKATA_SOUTH",
        "station_name": "South Kolkata Delivery Station",
        "city": "Kolkata",
        "region": "East",
        "daily_capacity": 4100,
        "latitude": 22.4950,
        "longitude": 88.3468,
    },
]

# ============================================================
# Routes (we have 16 routes i.e. 2 routes for each DS and there are 2 DS in each city and total 4 cities)
# ============================================================

ROUTES = [
    {
        "route_id": "RT_DELHI_NORTH_01",
        "warehouse_id": "FC_DELHI",
        "delivery_station_id": "DS_DELHI_NORTH",
        "distance_km": 28,
        "expected_travel_minutes": 50,
    },
    {
        "route_id": "RT_DELHI_NORTH_02",
        "warehouse_id": "FC_DELHI",
        "delivery_station_id": "DS_DELHI_NORTH",
        "distance_km": 31,
        "expected_travel_minutes": 55,
    },
    {
        "route_id": "RT_DELHI_SOUTH_01",
        "warehouse_id": "FC_DELHI",
        "delivery_station_id": "DS_DELHI_SOUTH",
        "distance_km": 34,
        "expected_travel_minutes": 65,
    },
    {
        "route_id": "RT_DELHI_SOUTH_02",
        "warehouse_id": "FC_DELHI",
        "delivery_station_id": "DS_DELHI_SOUTH",
        "distance_km": 37,
        "expected_travel_minutes": 70,
    },
    {
        "route_id": "RT_MUMBAI_NORTH_01",
        "warehouse_id": "FC_MUMBAI",
        "delivery_station_id": "DS_MUMBAI_NORTH",
        "distance_km": 22,
        "expected_travel_minutes": 45,
    },
    {
        "route_id": "RT_MUMBAI_NORTH_02",
        "warehouse_id": "FC_MUMBAI",
        "delivery_station_id": "DS_MUMBAI_NORTH",
        "distance_km": 25,
        "expected_travel_minutes": 50,
    },
    {
        "route_id": "RT_MUMBAI_SOUTH_01",
        "warehouse_id": "FC_MUMBAI",
        "delivery_station_id": "DS_MUMBAI_SOUTH",
        "distance_km": 27,
        "expected_travel_minutes": 55,
    },
    {
        "route_id": "RT_MUMBAI_SOUTH_02",
        "warehouse_id": "FC_MUMBAI",
        "delivery_station_id": "DS_MUMBAI_SOUTH",
        "distance_km": 30,
        "expected_travel_minutes": 60,
    },
    {
        "route_id": "RT_BANGALORE_NORTH_01",
        "warehouse_id": "FC_BANGALORE",
        "delivery_station_id": "DS_BANGALORE_NORTH",
        "distance_km": 24,
        "expected_travel_minutes": 50,
    },
    {
        "route_id": "RT_BANGALORE_NORTH_02",
        "warehouse_id": "FC_BANGALORE",
        "delivery_station_id": "DS_BANGALORE_NORTH",
        "distance_km": 27,
        "expected_travel_minutes": 55,
    },
    {
        "route_id": "RT_BANGALORE_SOUTH_01",
        "warehouse_id": "FC_BANGALORE",
        "delivery_station_id": "DS_BANGALORE_SOUTH",
        "distance_km": 29,
        "expected_travel_minutes": 60,
    },
    {
        "route_id": "RT_BANGALORE_SOUTH_02",
        "warehouse_id": "FC_BANGALORE",
        "delivery_station_id": "DS_BANGALORE_SOUTH",
        "distance_km": 32,
        "expected_travel_minutes": 65,
    },
    {
        "route_id": "RT_KOLKATA_NORTH_01",
        "warehouse_id": "FC_KOLKATA",
        "delivery_station_id": "DS_KOLKATA_NORTH",
        "distance_km": 20,
        "expected_travel_minutes": 40,
    },
    {
        "route_id": "RT_KOLKATA_NORTH_02",
        "warehouse_id": "FC_KOLKATA",
        "delivery_station_id": "DS_KOLKATA_NORTH",
        "distance_km": 23,
        "expected_travel_minutes": 45,
    },
    {
        "route_id": "RT_KOLKATA_SOUTH_01",
        "warehouse_id": "FC_KOLKATA",
        "delivery_station_id": "DS_KOLKATA_SOUTH",
        "distance_km": 25,
        "expected_travel_minutes": 50,
    },
    {
        "route_id": "RT_KOLKATA_SOUTH_02",
        "warehouse_id": "FC_KOLKATA",
        "delivery_station_id": "DS_KOLKATA_SOUTH",
        "distance_km": 28,
        "expected_travel_minutes": 55,
    },
]

# ============================================================
# Vehicles
# ============================================================

VEHICLES = [
    {
        "vehicle_id": "VH_DELHI_NORTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_DELHI_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_DELHI_NORTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_DELHI_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_DELHI_SOUTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_DELHI_SOUTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_DELHI_SOUTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_DELHI_SOUTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_MUMBAI_NORTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_MUMBAI_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_MUMBAI_NORTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_MUMBAI_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_MUMBAI_SOUTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_MUMBAI_SOUTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_MUMBAI_SOUTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_MUMBAI_SOUTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_BANGALORE_NORTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_BANGALORE_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_BANGALORE_NORTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_BANGALORE_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_BANGALORE_SOUTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_BANGALORE_SOUTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_BANGALORE_SOUTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_BANGALORE_SOUTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_KOLKATA_NORTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_KOLKATA_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_KOLKATA_NORTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_KOLKATA_NORTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_KOLKATA_SOUTH_01",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_KOLKATA_SOUTH",
        "capacity": 30,
    },
    {
        "vehicle_id": "VH_KOLKATA_SOUTH_02",
        "vehicle_type": "Bike",
        "delivery_station_id": "DS_KOLKATA_SOUTH",
        "capacity": 30,
    },
]