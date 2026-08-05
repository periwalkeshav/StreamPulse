#!/usr/bin/env bash
# Create (or reconcile) the StreamPulse topics. Safe to run repeatedly.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"

echo "waiting for broker at ${BOOTSTRAP} ..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done
echo "broker is up"

create_topic() {
  local name="$1" partitions="$2" retention_ms="$3" extra="${4:-}"
  # shellcheck disable=SC2086
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config retention.ms="${retention_ms}" \
    ${extra}
  echo "  -> ${name} (partitions=${partitions}, retention=${retention_ms}ms)"
}

echo "creating topics ..."
# raw ingest: 3 partitions so up to 3 consumers can work in parallel, 24h retention
create_topic raw-events        3 86400000
# cleaned + enriched events for downstream consumers, 24h retention
create_topic processed-events  3 86400000
# anomaly alerts: ordering matters more than parallelism -> a single partition
create_topic alerts            1 604800000
# dead letter queue: keep 7 days so failures can be replayed after a fix,
# and compact nothing - every failure is worth seeing
create_topic dlq-events        1 604800000 "--config cleanup.policy=delete"

echo
echo "topics now on the cluster:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
echo
kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic raw-events
