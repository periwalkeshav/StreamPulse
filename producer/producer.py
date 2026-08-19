#!/usr/bin/env python3
"""StreamPulse event producer.

Publishes synthetic e-commerce / IoT events to the ``raw-events`` Kafka topic at
a configurable rate (1,000 events/sec by default) and prints a throughput report
every ``REPORT_EVERY_SECONDS``.

Run locally (broker reachable on the host listener)::

    python producer/producer.py --rate 1000 --duration 60

Run in the stack::

    docker compose up -d producer
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Support both `python producer/producer.py` and `python -m producer.producer`.
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import ProducerConfig  # type: ignore[no-redef]
    from events import EventGenerator  # type: ignore[no-redef]
    from schema import SchemaError, validate_event  # type: ignore[no-redef]
else:  # pragma: no cover - exercised when imported as a package
    from .config import ProducerConfig
    from .events import EventGenerator
    from .schema import SchemaError, validate_event

LOG = logging.getLogger("streampulse.producer")

_RUNNING = True


def _handle_signal(signum, _frame):  # pragma: no cover - signal path
    global _RUNNING
    LOG.info("received signal %s, draining and shutting down", signum)
    _RUNNING = False


def serialize(value: Any) -> bytes:
    """Serialize an event for Kafka.

    Strings are passed through untouched so the generator can emit deliberately
    malformed (non-JSON) payloads that must land in the dead letter queue.
    """
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def build_producer(config: ProducerConfig, retries: int = 30) -> KafkaProducer:
    """Create a KafkaProducer, waiting for the broker to accept connections."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                value_serializer=serialize,
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
                **config.as_kafka_kwargs(),
            )
        except NoBrokersAvailable as exc:  # pragma: no cover - needs a broker
            last_error = exc
            LOG.warning("broker not available yet (attempt %s/%s), retrying in 2s", attempt, retries)
            time.sleep(2)
    raise RuntimeError(f"could not reach Kafka at {config.bootstrap_servers}") from last_error


class ThroughputReporter:
    """Tracks send counters and logs a periodic throughput line."""

    def __init__(self, interval_seconds: int) -> None:
        self.interval = max(1, interval_seconds)
        self.start = time.perf_counter()
        self._last_report = self.start
        self._last_sent = 0
        self.sent = 0
        self.malformed = 0
        self.failed = 0

    def maybe_report(self, force: bool = False) -> None:
        now = time.perf_counter()
        if not force and now - self._last_report < self.interval:
            return
        window = now - self._last_report
        window_rate = (self.sent - self._last_sent) / window if window > 0 else 0.0
        overall = self.sent / (now - self.start) if now > self.start else 0.0
        LOG.info(
            "sent=%d (%.0f ev/s window, %.0f ev/s overall) malformed=%d failed=%d",
            self.sent,
            window_rate,
            overall,
            self.malformed,
            self.failed,
        )
        self._last_report = now
        self._last_sent = self.sent


def run(config: ProducerConfig) -> ThroughputReporter:
    generator = EventGenerator(
        seed=config.seed,
        anomaly_rate=config.anomaly_rate,
        late_event_rate=config.late_event_rate,
    )
    producer = build_producer(config)
    reporter = ThroughputReporter(config.report_every)

    LOG.info(
        "producing to %s on %s | target=%d ev/s duration=%s batch_size=%d linger_ms=%d",
        config.topic,
        config.bootstrap_servers,
        config.rate,
        f"{config.duration}s" if config.duration else "unbounded",
        config.batch_size,
        config.linger_ms,
    )

    # Emit in 20 slices per second: small enough to keep the rate smooth, large
    # enough that we are not sleeping on every single message.
    slices_per_second = 20
    per_slice = max(1, config.rate // slices_per_second)
    slice_duration = 1.0 / slices_per_second
    deadline = time.perf_counter() + config.duration if config.duration else None

    try:
        while _RUNNING:
            slice_start = time.perf_counter()
            if deadline and slice_start >= deadline:
                break

            now = datetime.now(timezone.utc)
            for _ in range(per_slice):
                if generator._random.random() < config.malformed_rate:
                    payload = generator.generate_malformed()
                    key = None
                    reporter.malformed += 1
                else:
                    payload = generator.generate(now)
                    try:
                        validate_event(payload)
                    except SchemaError:  # pragma: no cover - generator bug guard
                        LOG.exception("generator produced an invalid event, skipping")
                        continue
                    # Partition by user_id so all events for a user land on the
                    # same partition: sessionization stays local to one task.
                    key = payload["user_id"]

                try:
                    producer.send(config.topic, key=key, value=payload)
                    reporter.sent += 1
                except KafkaError:  # pragma: no cover - broker failure path
                    reporter.failed += 1
                    LOG.exception("send failed")

            reporter.maybe_report()

            elapsed = time.perf_counter() - slice_start
            if elapsed < slice_duration:
                time.sleep(slice_duration - elapsed)
    finally:
        LOG.info("flushing producer buffer ...")
        producer.flush(timeout=30)
        producer.close(timeout=10)
        reporter.maybe_report(force=True)

    return reporter


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="StreamPulse synthetic event producer")
    parser.add_argument("--rate", type=int, help="target events per second")
    parser.add_argument("--duration", type=int, help="seconds to run (0 = forever)")
    parser.add_argument("--topic", help="destination Kafka topic")
    parser.add_argument("--bootstrap-servers", help="Kafka bootstrap servers")
    parser.add_argument("--malformed-rate", type=float, help="fraction of poison-pill messages")
    parser.add_argument("--anomaly-rate", type=float, help="fraction of anomalous values")
    parser.add_argument("--seed", type=int, help="seed for reproducible streams")
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> ProducerConfig:
    config = ProducerConfig()
    for attr, value in (
        ("rate", args.rate),
        ("duration", args.duration),
        ("topic", args.topic),
        ("bootstrap_servers", args.bootstrap_servers),
        ("malformed_rate", args.malformed_rate),
        ("anomaly_rate", args.anomaly_rate),
        ("seed", args.seed),
    ):
        if value is not None:
            setattr(config, attr, value)
    return config


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    config = config_from_args(parse_args(argv))
    reporter = run(config)
    LOG.info("done: %d events published, %d failed", reporter.sent, reporter.failed)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
