# EventMesh Local Integration Validation

This guide validates EventMesh across Kafka, PostgreSQL, Redis, the API, the worker, retry topics, DLQ, replay, and metrics.

## Prerequisites

- Docker and Docker Compose
- Go
- `curl`
- `python3` for JSON extraction in shell examples

Optional but useful:

- `jq`
- Two or more terminal windows

## Startup

Start infrastructure:

```sh
make infra-up
```

Create local Kafka topics:

```sh
make topics-create
```

Apply migrations:

```sh
make migrate-up
```

Start one worker with shorter retry delays for local validation:

```sh
EVENTMESH_DEV_FAILURES_ENABLED=true \
OTEL_TRACES_ENABLED=true \
OTEL_SERVICE_NAME=eventmesh-worker \
KAFKA_RETRY_DELAYS=2s,3s,4s \
make run-worker
```

Start the API:

```sh
OTEL_TRACES_ENABLED=true \
OTEL_SERVICE_NAME=eventmesh-api \
make run-api
```

Health checks:

```sh
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/readyz
curl -fsS http://localhost:9091/healthz
```

Jaeger UI:

```sh
open http://localhost:16686
```

If `open` is unavailable, visit `http://localhost:16686` in a browser.

## Kafka Topics

The local workflow creates these topics with 3 partitions and replication factor 1:

- `eventmesh.events`
- `eventmesh.retry.1`
- `eventmesh.retry.2`
- `eventmesh.retry.3`
- `eventmesh.dlq`

Three partitions on `eventmesh.events` are enough to demonstrate consumer-group partition distribution with multiple worker processes.

## Scenario A: Successful Event

Send an event:

```sh
EVENT_ID="$(
  curl -fsS -X POST http://localhost:8080/v1/events \
    -H "Content-Type: application/json" \
    -d '{
      "type": "order.created",
      "source": "integration-test",
      "payload": {
        "order_id": "ORD-SUCCESS-001",
        "amount": 149.99
      }
    }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["event_id"])'
)"
echo "$EVENT_ID"
```

Poll the API until the event is processed:

```sh
curl -fsS "http://localhost:8080/v1/events/$EVENT_ID"
```

Expected evidence:

- API returns `202 Accepted` and an `event_id`
- worker logs `message_received`, `idempotency_acquired`, `event_processing_started`, `idempotency_completed`, `event_processing_succeeded`, and `offset_committed`
- PostgreSQL status becomes `PROCESSED`
- Redis state becomes `COMPLETED`

## Scenario B: Duplicate Delivery

Publish the same logical event directly to Kafka using the same `event_id`:

```sh
CREATED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
docker compose exec -T kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic eventmesh.events \
  --property parse.key=true \
  --property key.separator='|' <<EOF
$EVENT_ID|{"event_id":"$EVENT_ID","type":"order.created","source":"integration-test","payload":{"order_id":"ORD-SUCCESS-001","amount":149.99},"created_at":"$CREATED_AT"}
EOF
```

Expected evidence:

- worker receives the duplicate message
- worker logs `duplicate_event_skipped`
- no second business-processing side effect occurs
- PostgreSQL remains `PROCESSED`
- Kafka offset is committed

## Scenario C: Retry Then Success

The development-only event type `test.retry_twice` fails attempts 1 and 2 with retryable errors, then succeeds on attempt 3.

```sh
RETRY_ID="$(
  curl -fsS -X POST http://localhost:8080/v1/events \
    -H "Content-Type: application/json" \
    -d '{
      "type": "test.retry_twice",
      "source": "integration-test",
      "payload": {
        "case": "retry-then-success"
      }
    }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["event_id"])'
)"
echo "$RETRY_ID"
```

Expected flow:

```text
eventmesh.events -> retry.1 -> retry.2 -> success
```

Expected final state:

- PostgreSQL status is `PROCESSED`
- `attempt_count` is `3`
- Redis state is `COMPLETED`
- no DLQ row exists for this event
- worker logs include `retry_scheduled` for retry stages 1 and 2, then `event_processing_succeeded`

## Scenario D: Permanent Failure to DLQ

The development-only event type `test.permanent_failure` fails as non-retryable unless `EVENTMESH_DEV_PERMANENT_FAILURE_RECOVERED=true` is set on the worker.

```sh
DLQ_ID="$(
  curl -fsS -X POST http://localhost:8080/v1/events \
    -H "Content-Type: application/json" \
    -d '{
      "type": "test.permanent_failure",
      "source": "integration-test",
      "payload": {
        "case": "permanent-failure"
      }
    }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["event_id"])'
)"
echo "$DLQ_ID"
```

Expected final state:

- PostgreSQL status is `DLQ`
- `GET /v1/admin/dlq` includes the event
- `GET /v1/admin/dlq/{event_id}` returns its details
- worker logs include `dlq_published` and `offset_committed`

Inspect:

```sh
curl -fsS "http://localhost:8080/v1/admin/dlq?limit=20&offset=0"
curl -fsS "http://localhost:8080/v1/admin/dlq/$DLQ_ID"
```

## Scenario E: DLQ Replay

Restart the worker so the simulated permanent failure is recovered:

```sh
EVENTMESH_DEV_FAILURES_ENABLED=true \
EVENTMESH_DEV_PERMANENT_FAILURE_RECOVERED=true \
KAFKA_RETRY_DELAYS=2s,3s,4s \
make run-worker
```

Replay the DLQ event:

```sh
curl -fsS -X POST "http://localhost:8080/v1/admin/dlq/$DLQ_ID/replay"
```

Expected evidence:

- replay response is `202 Accepted`
- the same `event_id` is published back to `eventmesh.events`
- worker processes the replayed event successfully
- PostgreSQL final status becomes `PROCESSED`
- `attempt_count` continues increasing from historical attempts
- `replayed_at`, `last_error`, and DLQ timestamps remain useful historical context

The event identity must remain unchanged so tracing, idempotency, and operational history continue to refer to one logical event.

## Scenario F: Replay Safety

Replay a non-DLQ event:

```sh
curl -i -X POST "http://localhost:8080/v1/admin/dlq/$EVENT_ID/replay"
```

Expected: `409 Conflict`.

Reject replay when Redis says `COMPLETED`:

```sh
docker compose exec -T redis redis-cli SET "eventmesh:idempotency:$DLQ_ID" COMPLETED EX 86400
curl -i -X POST "http://localhost:8080/v1/admin/dlq/$DLQ_ID/replay"
docker compose exec -T redis redis-cli DEL "eventmesh:idempotency:$DLQ_ID"
```

Expected: `409 Conflict`.

Reject replay when Redis says `PROCESSING`:

```sh
docker compose exec -T redis redis-cli SET "eventmesh:idempotency:$DLQ_ID" PROCESSING EX 300
curl -i -X POST "http://localhost:8080/v1/admin/dlq/$DLQ_ID/replay"
docker compose exec -T redis redis-cli DEL "eventmesh:idempotency:$DLQ_ID"
```

Expected: `409 Conflict`.

Double replay should publish only once. Run both requests at nearly the same time:

```sh
curl -s -o /tmp/replay-a.out -w "%{http_code}\n" -X POST "http://localhost:8080/v1/admin/dlq/$DLQ_ID/replay" &
curl -s -o /tmp/replay-b.out -w "%{http_code}\n" -X POST "http://localhost:8080/v1/admin/dlq/$DLQ_ID/replay" &
wait
cat /tmp/replay-a.out /tmp/replay-b.out
```

Expected: one request is accepted and the other is rejected with `409 Conflict` if both race on a DLQ event.

## Scenario G: Kafka Failure Safety

The source-message commit rule is unit-tested: the worker commits only after successful processing, retry publication, or DLQ publication.

Locally, forcing retry publication failure usually means stopping Kafka, which destabilizes the whole stack. Prefer validating this through the existing unit tests unless you are deliberately running a destructive failure drill.

## Scenario H: Service Restart

Use a short Redis processing lease and the development slow event:

```sh
IDEMPOTENCY_PROCESSING_TTL=10s \
EVENTMESH_DEV_FAILURES_ENABLED=true \
EVENTMESH_DEV_SLOW_DURATION=15s \
make run-worker
```

Publish:

```sh
SLOW_ID="$(
  curl -fsS -X POST http://localhost:8080/v1/events \
    -H "Content-Type: application/json" \
    -d '{"type":"test.slow_processing","source":"integration-test","payload":{"case":"restart"}}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["event_id"])'
)"
echo "$SLOW_ID"
```

Stop the worker before the slow handler completes, wait for the processing lease to expire, and restart the worker. Expected behavior:

- uncommitted Kafka work is redelivered
- Redis prevents unsafe duplicate work while `PROCESSING` is active
- after the lease expires, the restarted worker can process the message

## PostgreSQL Inspection

Recent lifecycle state:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT
    event_id,
    type,
    source,
    status,
    attempt_count,
    last_error,
    created_at,
    processed_at,
    dlq_at,
    replayed_at
FROM events
ORDER BY created_at DESC
LIMIT 20;"
```

Processed events:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT event_id, type, attempt_count, processed_at
FROM events
WHERE status = 'PROCESSED'
ORDER BY updated_at DESC
LIMIT 20;"
```

DLQ events:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT event_id, type, attempt_count, last_error, dlq_at, original_topic, original_partition, original_offset
FROM events
WHERE status = 'DLQ'
ORDER BY dlq_at DESC
LIMIT 20;"
```

Retrying events:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT event_id, type, attempt_count, last_error, updated_at
FROM events
WHERE status = 'RETRYING'
ORDER BY updated_at DESC
LIMIT 20;"
```

## Redis Inspection

Check one event:

```sh
docker compose exec -T redis redis-cli GET "eventmesh:idempotency:$EVENT_ID"
```

Expected values:

- `PROCESSING` during active processing
- `COMPLETED` after success
- absent after retry scheduling or DLQ release

Local-only scan:

```sh
docker compose exec -T redis redis-cli --scan --pattern 'eventmesh:idempotency:*'
```

Avoid `KEYS *` outside local debugging.

## Kafka Inspection

List topics:

```sh
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe topics:

```sh
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic eventmesh.events
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic eventmesh.retry.1
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic eventmesh.retry.2
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic eventmesh.retry.3
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic eventmesh.dlq
```

Describe the consumer group:

```sh
docker compose exec -T kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group eventmesh-processors
```

Consume DLQ messages for inspection:

```sh
docker compose exec -T kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic eventmesh.dlq \
  --from-beginning \
  --max-messages 1
```

## Consumer-Group Demonstration

Run two worker processes in separate terminals with the same group:

```sh
EVENTMESH_DEV_FAILURES_ENABLED=true KAFKA_RETRY_DELAYS=2s,3s,4s make run-worker
```

```sh
WORKER_METRICS_ADDR=:9094 EVENTMESH_DEV_FAILURES_ENABLED=true KAFKA_RETRY_DELAYS=2s,3s,4s make run-worker
```

If port `:9094` is already in use, choose another free metrics port.

Publish several events:

```sh
for i in 1 2 3 4 5 6; do
  curl -fsS -X POST http://localhost:8080/v1/events \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"order.created\",\"source\":\"integration-test\",\"payload\":{\"order_id\":\"ORD-GROUP-$i\",\"amount\":$i}}"
done
```

Then inspect consumer-group assignment and lag:

```sh
docker compose exec -T kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group eventmesh-processors
```

Kafka partitions provide the concurrency here. Per-process processing concurrency remains `1`.

## Prometheus and Metrics

API metrics:

```sh
curl -fsS http://localhost:8080/metrics | grep -E 'eventmesh_events|eventmesh_dlq_replay'
```

Worker metrics:

```sh
curl -fsS http://localhost:9091/metrics | grep -E 'eventmesh_events_consumed_total|eventmesh_events_processed_total|eventmesh_events_processing_failed_total|eventmesh_retries_published_total|eventmesh_dlq_published_total|eventmesh_idempotency_duplicate_total'
```

Prometheus UI:

```sh
open http://localhost:9090
```

If `open` is unavailable, visit `http://localhost:9090` in a browser.

## OpenTelemetry Tracing

Tracing is disabled by default. Enable it per process:

```sh
OTEL_TRACES_ENABLED=true OTEL_SERVICE_NAME=eventmesh-api make run-api
OTEL_TRACES_ENABLED=true OTEL_SERVICE_NAME=eventmesh-worker make run-worker
```

Local OTLP HTTP endpoint:

```text
http://localhost:4318/v1/traces
```

Local trace UI:

```text
http://localhost:16686
```

Send a normal event, then copy `trace_id` from the API or worker JSON logs and search for it in Jaeger.

Expected successful trace shape:

```text
HTTP POST /v1/events
  -> events.create
  -> kafka.publish eventmesh.events
  -> kafka.consume eventmesh.events
  -> eventmesh.process_event
  -> idempotency.acquire
  -> events.mark_processing
  -> idempotency.mark_completed
  -> events.mark_processed
```

Expected retry trace behavior:

```text
eventmesh.process_event
  -> kafka.publish eventmesh.retry.1

later, after backoff:

kafka.consume eventmesh.retry.1
  -> eventmesh.process_event
```

Retry publication and delayed retry processing are connected through propagated W3C trace context, but EventMesh does not hold a span open while the message waits in the retry topic.

## Shutdown

Stop API and worker processes with `Ctrl+C`.

Stop infrastructure:

```sh
make infra-down
```

## Current Limitations

- No transactional outbox yet, so Kafka/PostgreSQL dual-write windows are documented rather than eliminated.
- No authentication yet; `/v1/admin/dlq` must be protected before public or shared production deployment.
- No replay-all or unrestricted bulk replay.
- No schema registry.
- No higher per-worker processing concurrency; Kafka partition distribution is the concurrency model for now.
