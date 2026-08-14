#!/usr/bin/env python3
"""Apply the StreamPulse schema to PostgreSQL.

``schema.sql`` is written to be re-runnable (``CREATE TABLE IF NOT EXISTS`` /
``CREATE OR REPLACE VIEW``), so this script doubles as the migration runner and
as a readiness gate: services can block on it until the database is reachable
and the tables exist.

    python sql/migrate.py            # apply
    python sql/migrate.py --verify   # apply, then print the resulting catalogue
    python sql/migrate.py --reset    # drop everything first (destructive)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2

LOG = logging.getLogger("streampulse.migrate")

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

TABLES = (
    "raw_events",
    "aggregated_metrics",
    "alerts",
    "user_sessions",
    "dlq_events",
    "dlq_stats",
    "baseline_stats",
    "batch_log",
    # daily_summary moved to dbt as analytics.fct_event_metrics_daily.
    "data_quality_results",
)

VIEWS = ("v_events_per_minute", "v_error_rate", "v_pipeline_health")


def dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'streampulse')} "
        f"user={os.getenv('POSTGRES_USER', 'streampulse')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'streampulse')}"
    )


def connect(retries: int = 30, delay: float = 2.0):
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(dsn())
        except psycopg2.OperationalError as exc:
            last = exc
            LOG.warning("postgres not ready (attempt %s/%s): %s", attempt, retries, str(exc).strip())
            time.sleep(delay)
    raise SystemExit(f"could not connect to postgres: {last}")


def reset(conn) -> None:
    LOG.warning("dropping all StreamPulse objects")
    with conn.cursor() as cur:
        for view in VIEWS:
            cur.execute(f"DROP VIEW IF EXISTS {view} CASCADE")
        for table in TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    conn.commit()


def apply_schema(conn) -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    LOG.info("schema applied from %s", SCHEMA_PATH)


def verify(conn) -> int:
    missing = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
        )
        present = {row[0] for row in cur.fetchall()}
        for name in TABLES + VIEWS:
            if name not in present:
                missing.append(name)

        print(f"{'object':<24} {'rows':>12}")
        print("-" * 38)
        for table in TABLES:
            if table in present:
                cur.execute(f"SELECT count(*) FROM {table}")
                print(f"{table:<24} {cur.fetchone()[0]:>12,}")
        for view in VIEWS:
            marker = "ok" if view in present else "MISSING"
            print(f"{view:<24} {marker:>12}")

    if missing:
        LOG.error("missing objects: %s", ", ".join(missing))
        return 1
    LOG.info("all %d tables and %d views present", len(TABLES), len(VIEWS))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    parser = argparse.ArgumentParser(description="StreamPulse schema migration")
    parser.add_argument("--reset", action="store_true", help="drop all objects before applying (destructive)")
    parser.add_argument("--verify", action="store_true", help="print the object catalogue after applying")
    args = parser.parse_args(argv)

    conn = connect()
    try:
        if args.reset:
            reset(conn)
        apply_schema(conn)
        if args.verify:
            return verify(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
