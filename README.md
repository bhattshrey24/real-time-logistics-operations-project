# Real-Time Logistics Operations Platform
- An end-to-end streaming data engineering platform that ingests live logistics events from a fault-tolerant Kafka cluster, processes them through a Medallion Delta Lake architecture using Spark Structured Streaming and batch jobs, and surfaces operational insights to logistics and business teams via real-time Grafana dashboards.
- The platform covers the outbound logistics flow — from customer order placement through fulfillment center processing, delivery station handoff, and last-mile delivery to the customer.

## Logistics Overview
<img width="1378" height="611" alt="Logistics Overview" src="https://github.com/user-attachments/assets/15817942-c9a2-4bad-842c-97facba88bda" />

## Architecture Overview
<img width="1409" height="525" alt="Architecture" src="https://github.com/user-attachments/assets/308476bc-fb2a-47fa-b820-4d0285fc3dd7" />

### The platform is split into three layers:
#### Data Sources
Two Python producers simulate real-world logistics activity:
- Batch Producer — publishes master data (fulfillment centers, delivery stations, vehicles) to Kafka once at startup and again when master data changes
- Stream Producer — continuously publishes order creation and shipment status update events to Kafka (one event per second, tick-based state machine across 7 shipment lifecycle stages)

#### Transformation Layer (Medallion Architecture)
- Real-Time Path (Kafka → PostgreSQL)
- Batch Path (Kafka → Raw → Bronze → Silver → Gold)
  
Airflow orchestrates the batch pipeline with parallel Bronze/Silver branches converging into Gold.

#### Serving Layer (PostgreSQL)
PostgreSQL acts as the serving layer for both dashboards:
- Shipment Live Table — continuously refreshed by the real-time Spark streaming job
- BI Tables — 6 pre-aggregated materialized tables written by the Gold → BI batch job (Script 9)

### Dimensional Model (Gold Layer)
```text
                         ┌───────────────────┐
                         │   fact_shipment   │
                         └─────────┬─────────┘
        ┌────────────────┬──────────┼──────────┬───────────────┐───────────┐
        │                │          │          │               │           │
dim_fulfillment_center dim_delivery_station dim_vehicle  dim_customer   dim_date
      (SCD2)                 (SCD2)          (SCD2)        (SCD2)       (static)
```

## Tech Stack

- Event Streaming : Apache Kafka 3-broker cluster (KRaft, Replication Factor 3, Minimum In-Sync Replicas 2)
- Stream Processing : Apache Spark  Structured Streaming
- Batch Processing : Apache Spark  (PySpark)
- Data Lake Storage : MinIO (S3-compatible) with Delta Lake 
- Serving Database : PostgreSQL 
- Orchestration : Apache Airflow 
- Dashboards : Grafana 
- Kafka Management UI : Redpanda Console
- Containerization : Docker Compose

## Project Highlights

- End-to-end streaming + batch pipeline from raw Kafka events to Grafana dashboards
- Medallion architecture with clear separation across Raw, Bronze, Silver, and Gold layers stored as Delta Lake tables in MinIO
- Dual dashboard strategy — sub-2-minute latency real-time dashboard for operations and on-demand BI dashboard for analytics
- SCD Type 2 dimensions built using pure read-transform-write with unionByName (no delta-spark Python package required)
- Fault-tolerant streaming with Spark checkpointing, 30-second micro-batches, and idempotent Delta writes for resumable processing
- Threshold-based SMTP alerting for SLA breaches, FC/DS backlog, and stale shipment detection triggered during stream processing
- Fully code-provisioned dashboards — Grafana datasources and dashboard JSONs auto-provisioned on container start, no manual UI setup
- Idempotent batch jobs — overwrite-mode writes ensure safe re-runs without duplicating data
- Airflow DAG with parallel Bronze/Silver branches converging into Gold and BI aggregation

## How to Run

### Prerequisites
- Docker and Docker Compose installed
- .env file with Kafka cluster ID, MinIO credentials, and PostgreSQL credentials

### Start the platform

```
docker compose up -d
```

### Publish data
```
# Send master data once at startup
python producer/batch/batch_producer.py

# Start continuous stream producer (keep running)
python producer/stream/stream_producer.py
```

### Start Spark streaming jobs (separate terminals)
```
# Real-time path
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/script_04_realtime_streaming.py

# Event ingestion to MinIO
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/script_05_event_ingestion.py
```

### Run the batch pipeline
- Trigger from Airflow UI at http://localhost:8085 → DAG: logistics_batch_pipeline → Trigger DAG

## URLs for different services (Configured in docker-compose.yml)

-  Grafana Dashboards:  http://localhost:3000  
- Airflow UI: http://localhost:8085  
- Spark Master UI: http://localhost:8081
- Spark History Server: http://localhost:18080
- MinIO Console:  http://localhost:9001  
- Redpanda Console:  http://localhost:8080
