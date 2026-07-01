# ─────────────────────────────────────────────────────────────────────────────
# Alert detection and email delivery for Script 4.
#
# Alert types (matching the RT dashboard spec):
#   1. Prime SLA at risk          — per shipment, sent once
#   2. Delay threshold exceeded   — per shipment, sent once
#   3. ETA increased significantly— per shipment, sent once
#   4. FC backlog exceeded        — per facility, cooldown 1 hour
#   5. DS backlog exceeded        — per facility, cooldown 1 hour
#   6. Stale shipment             — per shipment, cooldown 1 hour
#
# sent_alerts: dict[str, datetime] — maps alert_key to when it was last sent.
#   Persists in the driver's memory for the lifetime of the streaming job.
# ─────────────────────────────────────────────────────────────────────────────

import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime

import config

logger = logging.getLogger(__name__)

# Facility-level and stale alerts re-fire after this cooldown
_COOLDOWN_SECONDS = 3600


# ── EMAIL ─────────────────────────────────────────────────────────────────────
# Builds and sends a plain-text alert email via SMTP; skips silently with a warning log if SMTP config is incomplete.
def send_email(subject: str, body: str) -> None:
    """Sends a plain-text alert email. Logs a warning if SMTP is not configured."""
    if not all([config.SMTP_HOST, config.SMTP_USER, config.SMTP_PASSWORD, config.ALERT_EMAIL_TO]) : # checks that all required SMTP config values are present.
        logger.warning(f"SMTP not configured — skipping email: {subject}")
        return

    msg            = MIMEText(body) 
    msg["Subject"] = f"[Logistics Alert] {subject}"
    msg["From"]    = config.SMTP_USER
    msg["To"]      = config.ALERT_EMAIL_TO

    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server: #  opens an SMTP connection to the mail server
            server.ehlo() # greets the SMTP server, identifying the client (initiates the SMTP handshake).
            server.starttls() # upgrades the connection to an encrypted TLS connection
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.sendmail(config.SMTP_USER, config.ALERT_EMAIL_TO, msg.as_string())
        logger.info(f"Alert sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send alert email '{subject}': {e}")


# ── DEDUPLICATION HELPERS ─────────────────────────────────────────────────────
# Determines whether an alert identified by key should be (re)sent based on prior send history.
def _should_send(key: str, sent_alerts: dict, use_cooldown: bool = False) -> bool:
    """
    Returns True if this alert should be fired.
    Per-shipment alerts (use_cooldown=False): send only once per job run.
    Facility/stale alerts (use_cooldown=True): re-send after _COOLDOWN_SECONDS.
    """
    last_sent = sent_alerts.get(key)
    if last_sent is None:
        return True
    if use_cooldown:
        return (datetime.utcnow() - last_sent).total_seconds() > _COOLDOWN_SECONDS
    return False

# Records that an alert was just sent.
def _mark_sent(key: str, sent_alerts: dict) -> None:
    sent_alerts[key] = datetime.utcnow()


# ── PER-SHIPMENT ALERTS ───────────────────────────────────────────────────────
# Runs alert checks for SLA risk, delay and ETA increase for a 
# single shipment row, sending each at most once per job run.
def check_per_shipment_alerts(row, previous_eta, sent_alerts: dict) -> None:
    """
    Checks all per-shipment alert conditions for a single enriched event row.
    `previous_eta` is the ETA stored in PostgreSQL before this batch's upsert
    (a datetime object or None if the shipment is new).
    """
    sid           = row.shipment_id
    delivery_type = row.delivery_type or "STANDARD"

    # 1. Prime shipment predicted to miss SLA
    if row.customer_tier == "PRIME" and row.is_sla_at_risk:
        key = f"{sid}:PRIME_SLA"
        if _should_send(key, sent_alerts):
            send_email(
                f"Prime SLA Risk — {sid}",
                f"Shipment {sid} (PRIME / {delivery_type}) is predicted to miss its SLA.\n"
                f"  Status   : {row.shipment_status}\n"
                f"  ETA      : {row.estimated_delivery_time}\n"
                f"  Promised : {row.promised_delivery_time}\n"
                f"  Delay    : {row.delay_minutes} min",
            )
            _mark_sent(key, sent_alerts) # Updates the sent_alerts dictionary to record that this alert has been sent, preventing duplicate alerts for the same shipment in the same job run before cooldown period.

    # 2. Delay threshold exceeded
    threshold = config.DELAY_ALERT_THRESHOLD_MINUTES.get(delivery_type, 30) # 30 is default threshold if delivery_type is not found in the config dictionary.
    if row.is_delayed and (row.delay_minutes or 0) > threshold:
        key = f"{sid}:DELAY" # Observe we are using same sid but suffixed with DELAY to create a unique key for this alert type in same sent_alerts dictionary
        if _should_send(key, sent_alerts):
            send_email(
                f"Delay Threshold Exceeded — {sid}",
                f"Shipment {sid} ({delivery_type}) is delayed by {row.delay_minutes} min "
                f"(threshold: {threshold} min).\n"
                f"  Status : {row.shipment_status}",
            )
            _mark_sent(key, sent_alerts)

    # 3. ETA increased significantly
    if previous_eta and row.estimated_delivery_time: # Checks if both previous and estimated_delivery_time exists
        try:
            new_eta        = row.estimated_delivery_time  # already a datetime from Spark
            change_minutes = (new_eta - previous_eta).total_seconds() / 60
            eta_threshold  = config.ETA_CHANGE_ALERT_MINUTES.get(delivery_type, 30) 
            if change_minutes > eta_threshold:
                key = f"{sid}:ETA_CHANGE"
                if _should_send(key, sent_alerts):
                    send_email(
                        f"ETA Increased Significantly — {sid}",
                        f"Shipment {sid} ETA increased by {change_minutes:.0f} min.\n"
                        f"  Old ETA : {previous_eta}\n"
                        f"  New ETA : {new_eta}",
                    )
                    _mark_sent(key, sent_alerts)
        except Exception as e:
            logger.warning(f"ETA change check failed for {sid}: {e}")


# ── FACILITY-LEVEL ALERTS ─────────────────────────────────────────────────────
# Sends backlog alerts for fulfillment centers and delivery stations exceeding their thresholds, using cooldown-based resending.
def check_backlog_alerts(fc_backlog: list, ds_backlog: list, sent_alerts: dict) -> None:
    """
    Sends backlog alerts for facilities whose shipment count exceeds the threshold.
    fc_backlog / ds_backlog: list of (facility_id, count) tuples from postgres.py.
    Uses cooldown so alerts don't fire every micro-batch indefinitely.
    """
    for facility_id, count in fc_backlog: # loops through the list of fulfillment center backlogs, where each item is a tuple containing the facility ID and the count of shipments in backlog.
        key = f"{facility_id}:FC_BACKLOG"
        if _should_send(key, sent_alerts, use_cooldown=True):
            send_email(
                f"FC Backlog Alert — {facility_id}",
                f"Fulfillment Center {facility_id} has {count} shipments in backlog "
                f"(threshold: {config.FC_BACKLOG_THRESHOLD}).",
            )
            _mark_sent(key, sent_alerts)

    for facility_id, count in ds_backlog:
        key = f"{facility_id}:DS_BACKLOG"
        if _should_send(key, sent_alerts, use_cooldown=True):
            send_email(
                f"DS Backlog Alert — {facility_id}",
                f"Delivery Station {facility_id} has {count} shipments in backlog "
                f"(threshold: {config.DS_BACKLOG_THRESHOLD}).",
            )
            _mark_sent(key, sent_alerts)

# Sends alerts for shipments stuck at a facility beyond the stale-time threshold, 
# with cooldown-based resending.
def check_stale_alerts(stale_rows: list, sent_alerts: dict) -> None:
    """
    Sends stale-shipment alerts for shipments that haven't moved from their
    current facility beyond the configured time threshold.
    stale_rows: list of dicts from postgres.fetch_stale_shipments().
    """
    for row in stale_rows:
        key = f"{row['shipment_id']}:STALE"
        if _should_send(key, sent_alerts, use_cooldown=True):
            send_email(
                f"Stale Shipment — {row['shipment_id']}",
                f"Shipment {row['shipment_id']} has not moved from "
                f"{row['facility_type']} {row['facility_id']}.\n"
                f"  Status       : {row['shipment_status']}\n"
                f"  Stage since  : {row['stage_entered_at']}",
            )
            _mark_sent(key, sent_alerts)
