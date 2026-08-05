# StreamPulse — Real-Time Streaming Analytics Pipeline

A production-shaped streaming pipeline: **Kafka → Spark Structured Streaming → PostgreSQL → Grafana**,
orchestrated by **Airflow**, with a dead letter queue, real-time anomaly detection and idempotent writes.

One command starts everything:

```bash
docker compose up -d --build
```

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Airflow | http://localhost:8081 | `admin` / `admin` |
| Spark UI | http://localhost:4040 | — |

---

## Architecture

```mermaid
flowchart LR
    subgraph Ingest
        P["producer.py<br/>1K–14K events/sec<br/>Faker + fault injection"]
    end

    subgraph Kafka["Apache Kafka (3 partitions, keyed by user_id)"]
        RAW[["raw-events"]]
        PROC[["processed-events"]]
        ALERT[["alerts"]]
        DLQ[["dlq-events"]]
    end

    subgraph Spark["Spark Structured Streaming — 3 concurrent queries"]
        Q1["ingest<br/>parse · validate · enrich · score"]
        Q2["windowed<br/>5-min tumbling rollups"]
        Q3["sessions<br/>session windows per user"]
    end

    subgraph PG["PostgreSQL 15"]
        T1[(raw_events)]
        T2[(aggregated_metrics)]
        T3[(alerts)]
        T4[(user_sessions)]
        T5[(dlq_events)]
        T6[(baseline_stats)]
    end

    MON["dlq_monitor.py<br/>60s rollup + alert"]
    AF["Airflow<br/>daily_aggregation · data_quality_check · cleanup"]
    GRAF["Grafana<br/>12 panels + 3 alert rules"]

    P -->|"JSON, key=user_id"| RAW
    RAW --> Q1 & Q2 & Q3
    Q1 -->|"valid"| PROC
    Q1 -->|"malformed"| DLQ
    Q1 -->|"> 3σ"| ALERT
    Q1 --> T1 & T3 & T5 & T6
    Q2 --> T2
    Q3 --> T4
    DLQ --> MON --> PG
    PG --> GRAF
    AF --> PG
```

**Kappa, not Lambda.** There is exactly one code path for events: the stream. The Airflow DAGs do not
recompute the stream's output from raw data — they roll *the stream's own output* up into daily facts and
enforce retention. That is the distinction that matters in an interview: a Lambda architecture maintains two
implementations of the same business logic (batch and speed layers) and has to keep them in agreement;
Kappa keeps one, and reprocesses by replaying the log.

---

## Quick start

```bash
git clone https://github.com/your-handle/StreamPulse.git
cd StreamPulse
cp .env.example .env          # optional: everything has sane defaults

docker compose up -d --build  # ~3 minutes on a cold cache
make smoke                    # live counters straight from PostgreSQL
```

Then open Grafana at http://localhost:3000 → **StreamPulse / Live Pipeline**. Events appear within ~30 s.

```bash
make logs      # tail producer + consumer + DLQ monitor
make lag       # Kafka offsets and Spark micro-batch progress
make psql      # SQL shell
make topics    # topic configuration
make clean     # stop everything and delete the volumes
```

**Requirements:** Docker Desktop with ~6 GB allocated. Nothing else — Java, Spark, Kafka and Python all live
inside the images.
