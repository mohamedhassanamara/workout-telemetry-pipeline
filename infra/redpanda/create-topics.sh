#!/bin/bash
# Idempotent topic creation. Runs INSIDE the redpanda container.
set -euo pipefail

BROKERS="${BROKERS:-localhost:9092}"
DAY_MS=86400000

create_topic() {
  local name="$1" partitions="$2" retention_ms="$3"
  if rpk -X brokers="$BROKERS" topic describe "$name" >/dev/null 2>&1; then
    echo "[topics] $name already exists"
  else
    rpk -X brokers="$BROKERS" topic create "$name" \
      -p "$partitions" -r 1 \
      -c retention.ms="$retention_ms" \
      -c cleanup.policy=delete
  fi
}

create_topic workout.sets.v1    6 $((7 * DAY_MS))
create_topic recovery.daily.v1  3 $((30 * DAY_MS))
create_topic workout.dlq.v1     3 $((30 * DAY_MS))
