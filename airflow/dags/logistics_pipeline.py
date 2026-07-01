# ─────────────────────────────────────────────────────────────────────────────
# Logistics Batch Pipeline DAG
#
# Wires Scripts 2, 3, 6, 7, 8, 9 into an ordered pipeline.
# schedule=None → manual-only: trigger via the Airflow UI (▶ Trigger DAG)
# or CLI: airflow dags trigger logistics_batch_pipeline
#
# Task dependency graph:
#
#   bronze_master (02) → silver_master (03) ─┐
#                                             ├→ gold_processing (08) → bi_aggregations (09)
#   bronze_events (06) → silver_events (07) ─┘
#
# Each task runs spark-submit inside the spark-master container via docker exec.
# JARs are pre-baked into the spark image so no --packages flag is needed.
# ─────────────────────────────────────────────────────────────────────────────

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Spark submit command template ─────────────────────────────────────────────
# Runs inside spark-master (where the JARs live) via docker exec.
# Event logs are written to /opt/spark/logs so they appear in the History Server.
_SPARK_SUBMIT = (
    "docker exec spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.eventLog.enabled=true "
    "--conf spark.eventLog.dir=file:/opt/spark/logs "
    "/opt/spark/jobs/{script}"
)


def spark_task(task_id: str, script: str) -> BashOperator:
    """Return a BashOperator that submits a single Spark script."""
    return BashOperator(
        task_id=task_id,
        bash_command=_SPARK_SUBMIT.format(script=script),
    )


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="logistics_batch_pipeline",
    description="Bronze → Silver → Gold → BI aggregations (manual trigger only)",
    schedule=None,          # never runs automatically; trigger manually from UI or CLI
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistics", "spark", "batch"],
) as dag:

    # ── Bronze layer: parse raw JSON from MinIO into typed Delta tables ────────
    bronze_master = spark_task("bronze_master", "script_02_master_data_bronze.py")
    bronze_events = spark_task("bronze_events", "script_06_event_bronze.py")

    # ── Silver layer: deduplicate, cast types, apply business rules ───────────
    silver_master = spark_task("silver_master", "script_03_master_data_silver.py")
    silver_events = spark_task("silver_events", "script_07_event_silver.py")

    # ── Gold layer: star schema (fact_shipment + SCD2 dims) ───────────────────
    gold_processing = spark_task("gold_processing", "script_08_gold_processing.py")

    # ── BI layer: materialize aggregations into PostgreSQL for Grafana ────────
    bi_aggregations = spark_task("bi_aggregations", "script_09_bi_aggregations.py")

    # ── Wire dependencies ─────────────────────────────────────────────────────
    bronze_master >> silver_master
    bronze_events >> silver_events
    [silver_master, silver_events] >> gold_processing >> bi_aggregations
