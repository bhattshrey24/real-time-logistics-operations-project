# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL helpers for Script 4.
# Handles table creation, batch upserts, and alert-related queries.
# ─────────────────────────────────────────────────────────────────────────────

import logging
import psycopg2
import psycopg2.extras

import config

logger = logging.getLogger(__name__)


def get_connection():
    """Opens and returns a psycopg2 connection using config vars"""
    return psycopg2.connect(
        host     = config.POSTGRES_HOST,
        port     = config.POSTGRES_PORT,
        dbname   = config.LOGISTICS_DB,
        user     = config.POSTGRES_USER,
        password = config.POSTGRES_PASSWORD,
    )

# shipment_live is the single source of truth for everything.
# It holds one row per shipment (always latest state). All KPIs — 
# active shipments, delayed, SLA risk, FC/DS backlog, stale shipments — are 
# computed by querying this table. Grafana also reads this same table for the RT dashboard panels.
def ensure_table(conn) -> None:
    """Creates the shipment_live table if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shipment_live (
                shipment_id             VARCHAR PRIMARY KEY,
                order_id                VARCHAR,
                shipment_status         VARCHAR,
                event_timestamp         TIMESTAMP,
                customer_tier           VARCHAR,
                delivery_type           VARCHAR,
                is_delayed              BOOLEAN,
                delay_minutes           INTEGER,
                facility_id             VARCHAR,
                facility_type           VARCHAR,
                fc_name                 VARCHAR,
                ds_name                 VARCHAR,
                fc_region               VARCHAR,
                ds_region               VARCHAR,
                vehicle_id              VARCHAR,
                estimated_delivery_time TIMESTAMP,
                promised_delivery_time  TIMESTAMP,
                is_sla_at_risk          BOOLEAN,
                is_priority             BOOLEAN,
                latitude                DOUBLE PRECISION,
                longitude               DOUBLE PRECISION,
                stage_entered_at        TIMESTAMP,
                last_updated            TIMESTAMP
            )
        """)
    conn.commit()

# Looks up the currently stored ETA for a batch of shipment IDs 
# before they get overwritten, so that check_per_shipment_alerts can detect ETA jumps
def fetch_previous_etas(shipment_ids: list, conn) -> dict:
    """
    Returns the currently stored ETA for each shipment ID.
    Used to detect significant ETA changes before the upsert overwrites the value.
    """
    if not shipment_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT shipment_id, estimated_delivery_time "
            "FROM shipment_live WHERE shipment_id = ANY(%s)",
            (shipment_ids,)
        )
        return {row[0]: row[1] for row in cur.fetchall()} #  converts the result rows into a {shipment_id: eta} dict for fast lookup.

# Inserts new shipments or updates existing ones in shipment_live, 
# with special logic to preserve stage_entered_at unless status 
# actually changed
def upsert_batch(rows: list, conn) -> None:
    """
    Upserts a list of Spark Row objects into shipment_live.
    On conflict (same shipment_id):
      - All fields are updated to the latest values.
      - stage_entered_at is only reset when the status actually changes,
        which is what powers stale-shipment detection.
    """
    sql = """
        INSERT INTO shipment_live (
            shipment_id, order_id, shipment_status, event_timestamp,
            customer_tier, delivery_type, is_delayed, delay_minutes,
            facility_id, facility_type, fc_name, ds_name, fc_region, ds_region,
            vehicle_id, estimated_delivery_time, promised_delivery_time,
            is_sla_at_risk, is_priority, latitude, longitude,
            stage_entered_at, last_updated
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            NOW(), NOW()
        )
        ON CONFLICT (shipment_id) DO UPDATE SET
            order_id                = EXCLUDED.order_id,
            event_timestamp         = EXCLUDED.event_timestamp,
            customer_tier           = EXCLUDED.customer_tier,
            delivery_type           = EXCLUDED.delivery_type,
            is_delayed              = EXCLUDED.is_delayed,
            delay_minutes           = EXCLUDED.delay_minutes,
            facility_id             = EXCLUDED.facility_id,
            facility_type           = EXCLUDED.facility_type,
            fc_name                 = EXCLUDED.fc_name,
            ds_name                 = EXCLUDED.ds_name,
            fc_region               = EXCLUDED.fc_region,
            ds_region               = EXCLUDED.ds_region,
            vehicle_id              = EXCLUDED.vehicle_id,
            estimated_delivery_time = EXCLUDED.estimated_delivery_time,
            promised_delivery_time  = EXCLUDED.promised_delivery_time,
            is_sla_at_risk          = EXCLUDED.is_sla_at_risk,
            is_priority             = EXCLUDED.is_priority,
            latitude                = EXCLUDED.latitude,
            longitude               = EXCLUDED.longitude,
            stage_entered_at        = CASE
                WHEN shipment_live.shipment_status != EXCLUDED.shipment_status
                THEN NOW()
                ELSE shipment_live.stage_entered_at
            END,
            shipment_status         = EXCLUDED.shipment_status,
            last_updated            = NOW()
    """

    records = [
        (
            row.shipment_id, row.order_id, row.shipment_status, row.event_timestamp,
            row.customer_tier, row.delivery_type, row.is_delayed, row.delay_minutes,
            row.facility_id, row.facility_type, row.fc_name, row.ds_name,
            row.fc_region, row.ds_region,
            row.vehicle_id, row.estimated_delivery_time, row.promised_delivery_time,
            row.is_sla_at_risk, row.is_priority, row.latitude, row.longitude,
        )
        for row in rows if row.shipment_id
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records)


def fetch_fc_backlog(conn) -> list:
    """Returns (facility_id, count) for FC facilities whose backlog exceeds the threshold."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT facility_id, COUNT(*) as cnt
            FROM shipment_live
            WHERE shipment_status IN ('ORDER_ALLOCATED_TO_FC', 'PICKED_AND_PACKED')
            GROUP BY facility_id
            HAVING COUNT(*  ) > %s
        """, (config.FC_BACKLOG_THRESHOLD,))
        return cur.fetchall()


def fetch_ds_backlog(conn) -> list:
    """Returns (facility_id, count) for DS facilities whose backlog exceeds the threshold."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT facility_id, COUNT(*) as cnt
            FROM shipment_live
            WHERE shipment_status IN ('RECEIVED_AT_DS', 'ASSIGNED_TO_DRIVER')
            GROUP BY facility_id
            HAVING COUNT(*) > %s
        """, (config.DS_BACKLOG_THRESHOLD,))
        return cur.fetchall() #  returns the result as a list of (facility_id, count) tuples.

# Finds shipments that have been sitting at their current 
# facility/stage too long without progressing.
def fetch_stale_shipments(conn) -> list:
    """
    Returns rows for shipments that haven't moved from their current facility
    beyond the configured stale thresholds (FC: 4h, DS: 1h).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT shipment_id, shipment_status, facility_id, facility_type, stage_entered_at
            FROM shipment_live
            WHERE
                (facility_type = 'FULFILLMENT_CENTER'
                    AND stage_entered_at < NOW() - INTERVAL '%s hours'
                    AND shipment_status NOT IN ('DISPATCHED_FROM_FC', 'DELIVERED'))
                OR
                (facility_type = 'DELIVERY_STATION'
                    AND stage_entered_at < NOW() - INTERVAL '%s hours'
                    AND shipment_status NOT IN ('OUT_FOR_DELIVERY', 'DELIVERED'))
        """, (config.FC_STALE_THRESHOLD_HOURS, config.DS_STALE_THRESHOLD_HOURS))
        cols = ["shipment_id", "shipment_status", "facility_id", "facility_type", "stage_entered_at"]
        return [dict(zip(cols, row)) for row in cur.fetchall()] # zips each 
    # result tuple with column names to produce a list of dicts (so callers like check_stale_alerts can use row['shipment_id'] instead of positional indexing)
