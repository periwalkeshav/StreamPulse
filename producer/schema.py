"""Lightweight, dependency-free validation of the StreamPulse event envelope.

The producer validates before publishing (fail fast on a bug in the generator)
and the unit tests validate the generator output. The Spark consumer performs
the same checks structurally via ``from_json`` plus explicit null guards - see
``consumer/transformations.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "timestamp", "metadata")

VALID_EVENT_TYPES = {
    "page_view",
    "add_to_cart",
    "checkout_started",
    "order_placed",
    "payment_failed",
    "sensor_reading",
}

REQUIRED_METADATA_FIELDS = ("value", "currency", "country", "device", "session_id", "latency_ms")


class SchemaError(ValueError):
    """Raised when an event does not satisfy the registry contract."""


def validate_event(event: Any) -> dict[str, Any]:
    """Validate ``event`` and return it, or raise :class:`SchemaError`."""
    if not isinstance(event, dict):
        raise SchemaError(f"event must be an object, got {type(event).__name__}")

    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        raise SchemaError(f"missing required field(s): {', '.join(missing)}")

    if not isinstance(event["event_id"], str) or not event["event_id"]:
        raise SchemaError("event_id must be a non-empty string")

    if not isinstance(event["user_id"], str) or not event["user_id"]:
        raise SchemaError("user_id must be a non-empty string")

    if event["event_type"] not in VALID_EVENT_TYPES:
        raise SchemaError(f"unknown event_type: {event['event_type']!r}")

    try:
        datetime.fromisoformat(str(event["timestamp"]))
    except (TypeError, ValueError) as exc:
        raise SchemaError(f"timestamp is not ISO-8601: {event['timestamp']!r}") from exc

    metadata = event["metadata"]
    if not isinstance(metadata, dict):
        raise SchemaError("metadata must be an object")

    missing_meta = [f for f in REQUIRED_METADATA_FIELDS if f not in metadata]
    if missing_meta:
        raise SchemaError(f"metadata missing field(s): {', '.join(missing_meta)}")

    if not isinstance(metadata["value"], (int, float)):
        raise SchemaError("metadata.value must be numeric")
    if metadata["value"] < 0:
        raise SchemaError("metadata.value must be non-negative")

    return event


def is_valid(event: Any) -> bool:
    try:
        validate_event(event)
    except SchemaError:
        return False
    return True
