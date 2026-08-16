"""Synthetic e-commerce / clickstream event generation.

The generator is deliberately pure and side-effect free so the unit tests can
assert on the schema without a Kafka broker in the loop.

Canonical event envelope (see ``schema_registry.md``)::

    {
      "event_id":   "<uuid4>",
      "user_id":    "u_000123",
      "event_type": "order_placed",
      "timestamp":  "2026-07-26T08:14:02.123456+00:00",
      "metadata":   { ... type specific ... }
    }
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from faker import Faker

EVENT_TYPES: tuple[str, ...] = (
    "page_view",
    "add_to_cart",
    "checkout_started",
    "order_placed",
    "payment_failed",
    "sensor_reading",
)

# Relative frequency of each event type - a realistic funnel shape.
EVENT_WEIGHTS: tuple[float, ...] = (0.55, 0.18, 0.10, 0.11, 0.02, 0.04)

DEVICES = ("mobile", "desktop", "tablet", "kiosk")
COUNTRIES = ("DE", "AT", "CH", "NL", "FR", "PL")
CURRENCIES = ("EUR",)
CHANNELS = ("organic", "paid_search", "email", "affiliate", "direct")

# Typical monetary value per event type: (mean, stddev). ``page_view`` style
# events carry no money, so the value is a small engagement score instead.
VALUE_PROFILE: dict[str, tuple[float, float]] = {
    "page_view": (1.0, 0.4),
    "add_to_cart": (48.0, 22.0),
    "checkout_started": (96.0, 40.0),
    "order_placed": (110.0, 45.0),
    "payment_failed": (88.0, 38.0),
    "sensor_reading": (21.5, 3.2),
}

N_USERS = 5_000
N_PRODUCTS = 800


class EventGenerator:
    """Generates StreamPulse events with a configurable share of anomalies.

    Parameters
    ----------
    seed:
        Fixing the seed makes the stream reproducible, which is what the unit
        tests and the Locust load profile rely on.
    anomaly_rate:
        Fraction of events whose ``value`` is inflated far beyond the normal
        distribution. These are the records the Spark job should flag as
        anomalies (> 3 sigma from the rolling mean).
    late_event_rate:
        Fraction of events emitted with an event-time in the past. They test
        the watermark / late-data handling in Structured Streaming.
    """

    def __init__(
        self,
        seed: int | None = None,
        anomaly_rate: float = 0.005,
        late_event_rate: float = 0.01,
    ) -> None:
        self._random = random.Random(seed)
        self._faker = Faker("de_DE")
        if seed is not None:
            Faker.seed(seed)
        self.anomaly_rate = anomaly_rate
        self.late_event_rate = late_event_rate
        self._sessions: dict[str, str] = {}

    # ------------------------------------------------------------------ utils
    def _user_id(self) -> str:
        return f"u_{self._random.randrange(N_USERS):06d}"

    def _session_for(self, user_id: str) -> str:
        # ~8% chance the user starts a brand new session; otherwise the previous
        # session id is reused, which gives the sessionization job something
        # meaningful to group on.
        if user_id not in self._sessions or self._random.random() < 0.08:
            self._sessions[user_id] = uuid.uuid4().hex[:16]
        return self._sessions[user_id]

    def _event_time(self, now: datetime) -> datetime:
        if self._random.random() < self.late_event_rate:
            # Late arrival: up to 10 minutes behind wall clock.
            return now - timedelta(seconds=self._random.uniform(60, 600))
        # Small natural jitter so events are not perfectly ordered.
        return now - timedelta(milliseconds=self._random.uniform(0, 750))

    # ------------------------------------------------------------- generation
    def generate(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        event_type = self._random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        user_id = self._user_id()
        mean, stddev = VALUE_PROFILE[event_type]

        is_anomaly = self._random.random() < self.anomaly_rate
        if is_anomaly:
            # 6-14 sigma out: unambiguously abnormal.
            value = mean + stddev * self._random.uniform(6.0, 14.0)
        else:
            value = max(0.01, self._random.gauss(mean, stddev))

        latency_ms = abs(self._random.gauss(35, 18)) + (140 if is_anomaly else 0)

        metadata: dict[str, Any] = {
            "value": round(value, 2),
            "currency": self._random.choice(CURRENCIES),
            "country": self._random.choice(COUNTRIES),
            "device": self._random.choice(DEVICES),
            "channel": self._random.choice(CHANNELS),
            "session_id": self._session_for(user_id),
            "product_id": f"p_{self._random.randrange(N_PRODUCTS):04d}",
            "latency_ms": round(latency_ms, 2),
            "status": "failed" if event_type == "payment_failed" else "ok",
            "synthetic_anomaly": is_anomaly,
        }

        if event_type == "sensor_reading":
            metadata["unit"] = "celsius"
            metadata["sensor_id"] = f"s_{self._random.randrange(200):03d}"

        return {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_type": event_type,
            "timestamp": self._event_time(now).isoformat(),
            "metadata": metadata,
        }

    def generate_malformed(self) -> dict[str, Any] | str:
        """Produce a record the consumer must reject and route to the DLQ."""
        flavour = self._random.randrange(5)
        if flavour == 0:  # not JSON at all
            return "}{ this is not json at all"
        if flavour == 1:  # missing the mandatory event_id
            payload = self.generate()
            payload.pop("event_id")
            return payload
        if flavour == 2:  # wrong type for timestamp
            payload = self.generate()
            payload["timestamp"] = "not-a-timestamp"
            return payload
        if flavour == 3:  # unknown event type
            payload = self.generate()
            payload["event_type"] = "???"
            return payload
        payload = self.generate()  # metadata is not an object
        payload["metadata"] = "oops"
        return payload

    def batch(self, size: int, now: datetime | None = None) -> Iterable[dict[str, Any]]:
        for _ in range(size):
            yield self.generate(now)
