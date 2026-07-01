-- ─────────────────────────────────────────────────────────────────────────────
-- Grafana BI Dashboard — SQL Queries Reference
--
-- All queries read from the 6 pre-aggregated tables written by Script 9.
-- These tables live in the logistics_rt PostgreSQL database (public schema).
-- Run Script 9 to refresh the data before opening this dashboard.
-- ─────────────────────────────────────────────────────────────────────────────


-- ── Logistics KPIs (4 stat panels) ───────────────────────────────────────────

-- Total Shipments
SELECT total_shipments AS value FROM bi_logistics_kpis;

-- On-Time Delivery %
SELECT on_time_pct AS value FROM bi_logistics_kpis;

-- Average Delay (minutes)
SELECT avg_delay_min AS value FROM bi_logistics_kpis;

-- SLA Breach %
SELECT sla_breach_pct AS value FROM bi_logistics_kpis;


-- ── Warehouse Performance ─────────────────────────────────────────────────────

SELECT
    fc_name             AS "FC Name",
    region              AS "Region",
    shipment_volume     AS "Volume",
    avg_fc_time_min     AS "Avg Dispatch (min)",
    delayed_count       AS "Delayed",
    delayed_pct         AS "Delayed %"
FROM bi_warehouse_performance
ORDER BY shipment_volume DESC;


-- ── Delivery Station Performance ──────────────────────────────────────────────

SELECT
    ds_name             AS "DS Name",
    region              AS "Region",
    shipment_volume     AS "Volume",
    avg_ds_time_min     AS "Avg Process (min)",
    delayed_count       AS "Delayed",
    delayed_pct         AS "Delayed %"
FROM bi_delivery_station_perf
ORDER BY shipment_volume DESC;


-- ── Monthly Trends (3 panels — same table, different columns) ─────────────────

-- Shipment Volume by Month
SELECT month_name AS "Month", shipment_volume AS "Shipments"
FROM bi_monthly_trends
ORDER BY year, month;

-- On-Time % by Month
SELECT month_name AS "Month", on_time_pct AS "On-Time %"
FROM bi_monthly_trends
ORDER BY year, month;

-- Avg Delay by Month
SELECT month_name AS "Month", avg_delay_min AS "Avg Delay (min)"
FROM bi_monthly_trends
ORDER BY year, month;


-- ── Cost & Penalty Summary ────────────────────────────────────────────────────

SELECT
    delivery_type           AS "Delivery Type",
    total_shipments         AS "Shipments",
    total_transport_cost    AS "Transport Cost ($)",
    total_sla_penalty       AS "SLA Penalty ($)",
    total_refunds           AS "Refunds ($)",
    avg_order_value         AS "Avg Order ($)"
FROM bi_cost_penalty
ORDER BY total_shipments DESC;


-- ── Delivery Lead Time ────────────────────────────────────────────────────────

SELECT
    delivery_type       AS "Delivery Type",
    shipment_count      AS "Shipments",
    avg_lead_time_hrs   AS "Avg (hrs)",
    min_lead_time_hrs   AS "Min (hrs)",
    max_lead_time_hrs   AS "Max (hrs)"
FROM bi_lead_time;
