# StreamPulse Schema Registry

The contract between the producer, the Spark job and every downstream consumer.
There is no Confluent Schema Registry in the stack (a deliberate choice for a
single-team local pipeline), so this document *is* the registry: it is versioned
with the code and enforced in three places.

| Enforcement point | Mechanism | File |
|---|---|---|
| Producer, before publishing | `validate_event()` raises `SchemaError` | [`producer/schema.py`](producer/schema.py) |
| Consumer, on read | `from_json` with an explicit `StructType` + null guards | [`consumer/schemas.py`](consumer/schemas.py), [`consumer/transformations.py`](consumer/transformations.py) |
| Storage | `NOT NULL` / `CHECK` constraints and primary keys | [`sql/schema.sql`](sql/schema.sql) |

---

## Topics

| Topic | Partitions | Replication | Retention | Key | Purpose |
|---|---|---|---|---|---|
| `raw-events` | 3 | 1 | 24 h | `user_id` | Everything the producer emits, valid or not |
| `processed-events` | 3 | 1 | 24 h | `user_id` | Parsed + enriched events for downstream consumers |
| `alerts` | 1 | 1 | 7 d | `event_id` | Anomaly notifications, ordered |
| `dlq-events` | 1 | 1 | 7 d | `event_id` | Records that failed validation, with the reason |

**Why these partition counts.** `raw-events` and `processed-events` get 3
partitions so up to three consumers in a group can work in parallel; that is the
unit of consumer parallelism in Kafka. `alerts` and `dlq-events` get 1 partition
because global ordering matters more than throughput for them, and their volume
is tiny by construction.

**Why key on `user_id`.** Kafka guarantees ordering within a partition, and the
default partitioner hashes the key. Keying on `user_id` therefore puts all of a
user's events on the same partition, so sessionization never has to shuffle a
user's history across tasks and per-user ordering is preserved end to end.

Replication factor is 1 because this is a single-broker local stack. In
production this is 3 with `min.insync.replicas=2`.

---

## `raw-events` value schema (v1)

```jsonc
{
  "event_id":   "9f1c2c9e-1c2b-4e51-9c2a-1f0f9e5a51b3",  // uuid4, unique per event
  "user_id":    "u_004217",                               // u_ + 6 digits
  "event_type": "order_placed",                           // enum, see below
  "timestamp":  "2026-07-26T08:14:02.123456+00:00",       // ISO-8601, always UTC
  "metadata": {
    "value":             129.99,      // number >= 0; EUR amount, or engagement score
    "currency":          "EUR",
    "country":           "DE",        // DE | AT | CH | NL | FR | PL
    "device":            "mobile",    // mobile | desktop | tablet | kiosk
    "channel":           "paid_search",
    "session_id":        "8b1d0f2c4a6e9d7b",
    "product_id":        "p_0431",
    "latency_ms":        41.7,        // upstream service latency
    "status":            "ok",        // ok | failed
    "synthetic_anomaly": false,       // ground truth, used to score the detector
    "unit":              "celsius",   // sensor_reading only
    "sensor_id":         "s_017"      // sensor_reading only
  }
}
```

### `event_type` enum

| Value | Share | Typical `value` (mean ± σ) |
|---|---|---|
| `page_view` | 55 % | 1.0 ± 0.4 |
| `add_to_cart` | 18 % | 48 ± 22 |
| `checkout_started` | 10 % | 96 ± 40 |
| `order_placed` | 11 % | 110 ± 45 |
| `payment_failed` | 2 % | 88 ± 38 |
| `sensor_reading` | 4 % | 21.5 ± 3.2 |

### Field-level rules

| Field | Required | Type | Rule |
|---|---|---|---|
| `event_id` | yes | string | Non-empty; primary key in `raw_events` |
| `user_id` | yes | string | Non-empty |
| `event_type` | yes | string | Must be in the enum above |
| `timestamp` | yes | string | Parseable as ISO-8601 |
| `metadata` | yes | object | Must be an object |
| `metadata.value` | yes | number | `>= 0` |
| `metadata.latency_ms` | yes | number | — |
| `metadata.currency`, `.country`, `.device`, `.session_id` | yes | string | — |
| everything else | no | — | Ignored by the consumer if unknown |

---

## `dlq-events` value schema

Every rejected record is wrapped rather than replaced, so nothing is lost and a
fixed consumer can replay the topic.

```jsonc
{
  "error_type":       "MALFORMED_JSON",
  "original_message": "}{ this is not json at all",  // the exact bytes received
  "source_topic":     "raw-events",
  "source_partition": 2,
  "source_offset":    918273,
  "received_at":      "2026-07-26T08:14:02.140Z",    // Kafka append time
  "failed_at":        "2026-07-26T08:14:12.001Z",    // when the consumer rejected it
  "failed_by":        "consumer_spark"
}
```

### `error_type` enum

| Value | Meaning |
|---|---|
| `MALFORMED_JSON` | Payload is not valid JSON, or does not fit the envelope at all |
| `MISSING_EVENT_ID` | `event_id` absent or null |
| `MISSING_USER_ID` | `user_id` absent or null |
| `UNKNOWN_EVENT_TYPE` | `event_type` outside the enum - usually an un-coordinated producer change |
| `INVALID_TIMESTAMP` | `timestamp` not parseable |
| `MISSING_METADATA` | `metadata` absent or not an object |
| `MISSING_VALUE` | `metadata.value` absent |
| `NEGATIVE_VALUE` | `metadata.value < 0` |

`(source_partition, source_offset)` is unique in the `dlq_events` table, so
replaying the DLQ is safe.

---

## `processed-events` value schema

The `raw-events` fields, flattened, plus what the consumer derived:

| Added field | Type | Meaning |
|---|---|---|
| `processed_at` | timestamp | When the micro-batch handled the record |
| `is_revenue_event` | boolean | `event_type ∈ {order_placed, checkout_started}` |
| `value_bucket` | string | `low` < 25, `medium` < 100, `high` < 250, else `very_high` |
| `ingest_lag_seconds` | number | Kafka append time − event time |

## `alerts` value schema

```jsonc
{
  "event_id":   "9f1c2c9e-...",
  "alert_type": "value_outlier",
  "severity":   "critical",         // warning | critical (|z| > 6)
  "event_type": "order_placed",
  "user_id":    "u_004217",
  "event_time": "2026-07-26T08:14:02Z",
  "value":      4821.55,
  "z_score":    12.7,
  "message":    "value 4821.55 is 12.7 sigma from the order_placed baseline mean of 110.02"
}
```

---

## Schema evolution policy

The consumer parses with an explicit `StructType`, which makes the compatibility
rules concrete:

| Change | Safe? | Why |
|---|---|---|
| Add an optional `metadata` field | **yes** | Unknown JSON keys are dropped by `from_json`; old consumers ignore them |
| Add a required top-level field | no | Old producers would emit records the new consumer rejects - add it as optional, backfill, then tighten |
| Remove a field | no | `from_json` yields null and the null guard sends every record to the DLQ |
| Rename a field | no | Equivalent to remove + add; do it as two compatible releases |
| Widen a type (int → double) | **yes** | Spark upcasts |
| Narrow a type (double → int) | no | Silent truncation |
| Add an `event_type` value | no, until deployed | `UNKNOWN_EVENT_TYPE` until `VALID_EVENT_TYPES` ships. **Deploy the consumer first.** |

**Rollout order for any breaking change: consumers first, producers second.**
Because the consumer tolerates unknown fields but not missing ones, a consumer
that already understands v2 can keep reading v1 while producers migrate.

If this pipeline grew past one team, the next step is Confluent Schema Registry
with Avro or Protobuf and `BACKWARD` compatibility enforced at publish time -
which turns the table above from a convention into a build failure.
