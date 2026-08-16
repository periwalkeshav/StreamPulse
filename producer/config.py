"""Runtime configuration for the StreamPulse event producer.

Everything is driven by environment variables so the same image runs locally,
in docker-compose and in CI without code changes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass
class ProducerConfig:
    """Producer tuning knobs.

    ``batch_size`` and ``linger_ms`` are the two levers that decide the
    throughput/latency trade-off of a Kafka producer: bigger batches and a
    longer linger give better compression and fewer requests per second, at the
    cost of a few extra milliseconds of end-to-end latency.
    """

    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    topic: str = field(default_factory=lambda: _env("RAW_TOPIC", "raw-events"))

    # Load shape
    rate: int = field(default_factory=lambda: _env_int("EVENTS_PER_SECOND", 1000))
    duration: int = field(default_factory=lambda: _env_int("DURATION_SECONDS", 0))  # 0 == run forever
    seed: int | None = field(default_factory=lambda: (int(os.environ["SEED"]) if os.getenv("SEED") else None))

    # Fault injection - exercises the DLQ and the anomaly detector end to end.
    malformed_rate: float = field(default_factory=lambda: _env_float("MALFORMED_RATE", 0.01))
    anomaly_rate: float = field(default_factory=lambda: _env_float("ANOMALY_RATE", 0.005))
    late_event_rate: float = field(default_factory=lambda: _env_float("LATE_EVENT_RATE", 0.01))

    # Kafka producer tuning
    batch_size: int = field(default_factory=lambda: _env_int("KAFKA_BATCH_SIZE", 65536))
    linger_ms: int = field(default_factory=lambda: _env_int("KAFKA_LINGER_MS", 20))
    compression_type: str = field(default_factory=lambda: _env("KAFKA_COMPRESSION", "lz4"))
    acks: str = field(default_factory=lambda: _env("KAFKA_ACKS", "1"))
    max_in_flight: int = field(default_factory=lambda: _env_int("KAFKA_MAX_IN_FLIGHT", 5))

    # Reporting
    report_every: int = field(default_factory=lambda: _env_int("REPORT_EVERY_SECONDS", 10))

    def as_kafka_kwargs(self) -> dict:
        # kafka-python wants acks as an int (0 or 1) or the literal string
        # "all"; an env var always arrives as a string, so coerce it here.
        acks: int | str = self.acks
        if isinstance(acks, str) and acks.strip().lstrip("-").isdigit():
            acks = int(acks)

        return {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "batch_size": self.batch_size,
            "linger_ms": self.linger_ms,
            "compression_type": self.compression_type,
            "acks": acks,
            "max_in_flight_requests_per_connection": self.max_in_flight,
            "retries": 5,
        }
