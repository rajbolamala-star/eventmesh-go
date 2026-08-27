#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${TMPDIR:-/tmp}/eventmesh-smoke"
API_LOG="$LOG_DIR/api.log"
WORKER_LOG="$LOG_DIR/worker.log"

mkdir -p "$LOG_DIR"
cd "$ROOT_DIR"

api_pid=""
worker_pid=""

cleanup() {
  if [[ -n "$api_pid" ]] && kill -0 "$api_pid" >/dev/null 2>&1; then
    kill "$api_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$worker_pid" ]] && kill -0 "$worker_pid" >/dev/null 2>&1; then
    kill "$worker_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_for_url() {
  local url="$1"
  local name="$2"
  local attempts="${3:-60}"
  for _ in $(seq 1 "$attempts"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $name at $url" >&2
  return 1
}

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if docker compose exec -T postgres pg_isready -U eventmesh -d eventmesh >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for postgres" >&2
  return 1
}

wait_for_kafka() {
  for _ in $(seq 1 60); do
    if docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for kafka" >&2
  return 1
}

wait_for_event_status() {
  local event_id="$1"
  local expected="$2"
  for _ in $(seq 1 60); do
    local body
    body="$(curl -fsS "http://localhost:8080/v1/events/$event_id")"
    local status
    status="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",""))' <<<"$body")"
    if [[ "$status" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for event $event_id to reach $expected" >&2
  return 1
}

require_command docker
require_command go
require_command curl
require_command python3

echo "starting infrastructure"
make infra-up
wait_for_postgres
wait_for_kafka

echo "creating topics"
make topics-create

echo "applying migrations"
make migrate-up

echo "starting worker"
EVENTMESH_DEV_FAILURES_ENABLED=true \
KAFKA_RETRY_DELAYS=2s,3s,4s \
WORKER_METRICS_ADDR=:9091 \
go run ./cmd/worker >"$WORKER_LOG" 2>&1 &
worker_pid="$!"
wait_for_url http://localhost:9091/healthz "worker metrics"

echo "starting api"
go run ./cmd/api >"$API_LOG" 2>&1 &
api_pid="$!"
wait_for_url http://localhost:8080/readyz "api readiness"

echo "publishing happy-path event"
response="$(
  curl -fsS -X POST http://localhost:8080/v1/events \
    -H "Content-Type: application/json" \
    -d '{
      "type": "order.created",
      "source": "smoke-test",
      "payload": {
        "order_id": "ORD-SMOKE-001",
        "amount": 149.99
      }
    }'
)"
event_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["event_id"])' <<<"$response")"

echo "waiting for $event_id to become PROCESSED"
wait_for_event_status "$event_id" PROCESSED

redis_state="$(docker compose exec -T redis redis-cli GET "eventmesh:idempotency:$event_id" | tr -d '\r')"
if [[ "$redis_state" != "COMPLETED" ]]; then
  echo "expected Redis state COMPLETED, got: ${redis_state:-<empty>}" >&2
  exit 1
fi

worker_metrics="$(curl -fsS http://localhost:9091/metrics)"
if ! grep -q "eventmesh_events_processed_total" <<<"$worker_metrics"; then
  echo "worker metrics did not include eventmesh_events_processed_total" >&2
  exit 1
fi

echo "smoke test passed"
echo "event_id=$event_id"
echo "api log: $API_LOG"
echo "worker log: $WORKER_LOG"
