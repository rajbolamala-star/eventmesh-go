# EventMesh

EventMesh is a production-style distributed event processing platform scaffold.

## Services

- `cmd/api`: HTTP producer service. It exposes health, readiness, metrics, and `POST /v1/events`.
- `cmd/worker`: Kafka consumer service. It wires the consumer, storage clients, cache client, metrics recorder, and worker lifecycle.

## Internal Packages

- `internal/config`: Environment-based configuration with safe local defaults.
- `internal/logger`: Structured JSON logging using Go's standard `log/slog`.
- `internal/events`: Event model and publisher interface.
- `internal/httpapi`: HTTP handlers, JSON error responses, and request ID middleware.
- `internal/kafka`: Kafka producer and consumer boundaries.
- `internal/postgres`: PostgreSQL pool and event lifecycle repository.
- `internal/redis`: Redis client and idempotency store.
- `internal/workers`: Worker orchestration and event processing entrypoints.
- `internal/metrics`: Prometheus-compatible metrics endpoint and recorder.

## Local Development

```sh
cp .env.example .env
make infra-up
make topics-create
make migrate-up
make test
make build
```

For full local end-to-end validation, see [docs/integration-test.md](docs/integration-test.md).

Enable local tracing with Jaeger:

```sh
OTEL_TRACES_ENABLED=true OTEL_SERVICE_NAME=eventmesh-api make run-api
OTEL_TRACES_ENABLED=true OTEL_SERVICE_NAME=eventmesh-worker make run-worker
```

Run services directly:

```sh
make run-api
make run-worker
```

Publish an event:

```sh
curl -i localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"order.created","source":"order-service","payload":{"order_id":"ORD-123","customer_id":"CUST-1001","amount":149.99}}'
```

## Observability

EventMesh keeps metrics and traces separate because they answer different operational questions.

Metrics answer how much and how often:

- API exposes Prometheus metrics at `http://localhost:8080/metrics`.
- Worker exposes Prometheus metrics at `http://localhost:9091/metrics` by default.
- Docker Compose starts Prometheus at `http://localhost:9090`.

Tracing answers what happened to a specific request or event:

- OpenTelemetry tracing is controlled by `OTEL_TRACES_ENABLED`.
- OTLP HTTP export uses `OTEL_EXPORTER_OTLP_ENDPOINT`, defaulting locally to `http://localhost:4318/v1/traces`.
- Docker Compose starts Jaeger at `http://localhost:16686`.
- API default service name is `eventmesh-api`.
- Worker default service name is `eventmesh-worker`.

Trace flow:

```text
HTTP POST /v1/events
  -> events.create
  -> kafka.publish eventmesh.events
  -> Kafka headers carry W3C trace context
  -> kafka.consume eventmesh.events
  -> eventmesh.process_event
  -> idempotency.acquire
  -> events.mark_processing
  -> idempotency.mark_completed
  -> events.mark_processed
```

Retry and DLQ traces:

- Retry publishing is traced as a Kafka producer span with retry stage and attempt metadata.
- The delayed retry consumer does not keep a span open while waiting for backoff.
- When the retry message becomes due, worker consumption continues from the propagated W3C trace context.
- DLQ publication is traced without attaching payload bodies or raw error strings as high-cardinality attributes.
- Replay publishes back to `eventmesh.events` and adds `eventmesh.replayed = true`.

Log correlation:

- Structured logs include `trace_id` and `span_id` when the current context contains a valid span.
- Use the trace ID from an API or worker log line to find the request/event path in Jaeger.

Telemetry cardinality rules:

- Safe attributes include route, HTTP method, event type, event source, Kafka topic, retry stage, and attempt.
- Payloads are never attached to spans.
- Event IDs, request IDs, raw URLs, Redis keys, Kafka offsets, and raw error messages are not used as Prometheus labels.

## Performance

Load-testing tooling is included under `loadtest/`.

Available k6 profiles:

```sh
make load-smoke
make load-baseline
make load-stress
make load-soak
make load-spike
```

The benchmark methodology, scaling experiments, Kafka lag checks, database queries, and result templates are documented in [docs/performance.md](docs/performance.md). No throughput numbers are claimed until they are measured with environment details.

## Local Kafka Smoke Test

Start infrastructure:

```sh
make infra-up
```

In one terminal, start the worker:

```sh
make run-worker
```

In another terminal, start the API:

```sh
make run-api
```

Send a test event:

```sh
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "order.created",
    "source": "order-service",
    "payload": {
      "order_id": "ORD-123",
      "amount": 149.99
    }
  }'
```

Expected behavior:

- The API returns `202 Accepted` with an `event_id`.
- The worker logs `consumer_started`, `message_received`, `event_processing_started`, `event_processing_succeeded`, and `offset_committed`.
- Logs include event metadata and Kafka topic, partition, and offset. Payloads are not logged.

Fetch event status:

```sh
curl http://localhost:8080/v1/events/{event_id}
```

## PostgreSQL Persistence

PostgreSQL stores the accepted event envelope and tracks lifecycle state. The API creates the initial `RECEIVED` record before publishing to Kafka. The worker updates the same row as processing progresses.

Lifecycle:

```text
RECEIVED -> PROCESSING -> PROCESSED
                      \-> RETRYING
                      \-> DLQ
```

The schema is defined in `migrations/000001_create_events_table.up.sql`.

Key columns:

- `event_id`: stable public event identifier, unique.
- `event_type`, `source`, `payload`: original event envelope.
- `status`: one of `RECEIVED`, `PROCESSING`, `PROCESSED`, `FAILED`, `RETRYING`, `DLQ`, `REPLAY_PENDING`, or `REPLAYED`.
- `attempt_count`: atomically incremented when processing starts.
- `last_error`: sanitized failure reason.
- lifecycle timestamps: `created_at`, `processing_started_at`, `processed_at`, `updated_at`.

Indexes:

- unique index from `event_id`.
- `status` for lifecycle queries.
- `created_at` for time-ordered inspection.
- `event_type` for event-type filtering.

Run migrations:

```sh
make migrate-up
```

Roll back migrations:

```sh
make migrate-down
```

Current implementation uses database-write-then-Kafka-publish. A transactional outbox may be introduced later to eliminate the dual-write consistency gap. Today, if the database insert succeeds but Kafka publish fails, the event row remains in PostgreSQL and is marked `FAILED`, but no Kafka message is guaranteed to exist.

## Consumer Semantics

The worker uses `kafka-go` consumer groups with `FetchMessage` and `CommitMessages`. `FetchMessage` reads without committing. After the event is decoded and handled successfully, `CommitMessages` advances the committed offset for that topic partition.

Kafka commits are partition-scoped progress markers. Committing a later offset means all earlier offsets in that partition are considered consumed, so this scaffold processes one message at a time per worker process. This avoids losing lower-offset messages through out-of-order concurrent commits. Higher throughput will be added later with per-partition ordered processing or explicit offset tracking.

Malformed JSON or invalid event envelopes are routed to the DLQ. The source offset is committed only after DLQ publication succeeds. Valid retryable processing failures are moved to retry topics before the source offset is committed.

## Redis Idempotency

Kafka provides at-least-once delivery, so duplicates can happen. For example, a worker can finish business processing and crash before committing the Kafka offset. Kafka will redeliver the event after restart or rebalance.

EventMesh uses Redis to reduce duplicate worker execution with this key format:

```text
eventmesh:idempotency:<event_id>
```

State values:

- `PROCESSING`: a worker has acquired a processing lease.
- `COMPLETED`: business processing completed and duplicate deliveries should skip business logic.

Processing flow:

```text
Kafka message
  -> acquire Redis idempotency key
  -> if ACQUIRED: mark PostgreSQL PROCESSING, run business handler, mark Redis COMPLETED, mark PostgreSQL PROCESSED, commit Kafka offset
  -> if COMPLETED: skip business handler, reconcile PostgreSQL PROCESSED, commit Kafka offset
  -> if ALREADY_PROCESSING: do not run business handler, do not commit Kafka offset
```

TTL settings:

- `IDEMPOTENCY_PROCESSING_TTL=5m`: processing lease TTL. This must be longer than normal processing time. If processing exceeds the lease, another delivery may acquire the event and duplicate work can occur.
- `IDEMPOTENCY_COMPLETED_TTL=24h`: duplicate suppression window. Longer TTLs suppress duplicates for longer but use more Redis memory. Shorter TTLs save memory but allow old duplicates to process again.

Failure behavior:

- Redis unavailable before processing: fail closed, do not run business logic, do not commit Kafka.
- Business handler fails: mark PostgreSQL `FAILED`, release the Redis `PROCESSING` key, do not mark `COMPLETED`, do not commit Kafka.
- Redis `COMPLETED` write fails after business success: do not mark PostgreSQL `PROCESSED`, do not commit Kafka. This avoids pretending idempotency protection exists.
- PostgreSQL `PROCESSED` update fails after Redis `COMPLETED`: do not commit Kafka. On redelivery, Redis says `COMPLETED`, so the worker skips business logic, reconciles PostgreSQL to `PROCESSED`, then commits.

EventMesh does not claim exactly-once side effects. Redis reduces duplicate processing, while true end-to-end idempotency requires cooperation from the downstream operation or transactional design. Common production strategies include downstream idempotency keys, database unique constraints, transactional inboxes, transactional outboxes, and side-effect-specific idempotency tokens.

## Failure Handling

Normal flow:

```text
eventmesh.events -> process -> commit
```

Retry flow:

```text
eventmesh.events
  -> failure
  -> eventmesh.retry.1
  -> failure
  -> eventmesh.retry.2
  -> failure
  -> eventmesh.retry.3
  -> failure
  -> eventmesh.dlq
```

Default backoff schedule:

- `eventmesh.retry.1`: `5s`, attempt `2`
- `eventmesh.retry.2`: `30s`, attempt `3`
- `eventmesh.retry.3`: `5m`, attempt `4`

Configure retries with:

```env
KAFKA_RETRY_TOPICS=eventmesh.retry.1,eventmesh.retry.2,eventmesh.retry.3
KAFKA_DLQ_TOPIC=eventmesh.dlq
KAFKA_RETRY_DELAYS=5s,30s,5m
```

Retry metadata is carried in Kafka headers:

- `eventmesh-attempt`
- `eventmesh-retry-stage`
- `eventmesh-original-topic`
- `eventmesh-original-partition`
- `eventmesh-original-offset`
- `eventmesh-first-failed-at`
- `eventmesh-last-failed-at`
- `eventmesh-last-error`
- `eventmesh-not-before-at`

Plain processing errors are treated as retryable. Permanent business failures should use `events.NonRetryable(err)` to publish directly to `eventmesh.dlq`. Retryable examples include timeouts, temporary network failures, rate limits, and service unavailable responses. Non-retryable examples include unsupported event types, invalid schema, and permanently invalid business data.

Commit-after-republish rule:

```text
Do not commit the current Kafka message until one of these is durable:
1. processing completed successfully
2. the next retry message was published
3. the DLQ message was published
```

If retry or DLQ publication fails, the source Kafka message is not committed and Kafka redelivers it. This can produce duplicates, but it avoids silent event loss.

Redis interaction:

- failed attempts release the Redis `PROCESSING` lease and never mark `COMPLETED`.
- retry delivery can acquire `PROCESSING` again.
- successful processing marks Redis `COMPLETED`, then PostgreSQL `PROCESSED`.
- completed duplicate delivery skips business logic, reconciles PostgreSQL to `PROCESSED`, and commits.

Poison messages:

- malformed JSON, invalid event envelopes, and malformed retry headers go to `eventmesh.dlq`.
- DLQ envelopes can contain a valid event, or a truncated raw payload when decoding failed.
- if DLQ publication fails, the source message is not committed.

Current limitations:

- delayed retry delivery is implemented by retry consumers waiting until `eventmesh-not-before-at`; high-scale production systems often use broker-native delayed delivery, scheduled topics, or external schedulers.
- transactional outbox is not implemented yet.
- worker concurrency remains `1` per consumer.

## DLQ Inspection And Replay

DLQ means the event could not be processed after retry exhaustion, or the failure was classified as non-retryable. Kafka remains the durable transport DLQ, while PostgreSQL stores operational metadata so operators can inspect and replay individual events without reading Kafka directly.

Admin endpoints:

```text
GET  /v1/admin/dlq?limit=20&offset=0
GET  /v1/admin/dlq/{event_id}
POST /v1/admin/dlq/{event_id}/replay
```

These are operator/admin endpoints. This scaffold does not implement authentication yet, so production deployments must protect `/v1/admin/*` with authentication, authorization, rate limits, and audit controls.

List responses omit payloads. The single-event endpoint includes payload for operator debugging. Payloads can contain sensitive data, so access to that endpoint should be tightly controlled.

Replay state machine:

```text
DLQ
  -> REPLAY_PENDING
  -> publish same event_id to eventmesh.events
  -> REPLAYED
  -> worker consumes
  -> PROCESSING
  -> PROCESSED / RETRYING / DLQ
```

Replay publishes to `eventmesh.events`, not a retry topic. It preserves the original `event_id` because replay is the same logical event; generating a new identity would break traceability and idempotency history.

Redis replay safety:

- Redis key absent: replay may proceed.
- Redis `PROCESSING`: replay is rejected with `409 Conflict`.
- Redis `COMPLETED`: replay is rejected with `409 Conflict` because it may mean the side effect already succeeded.

Replay idempotency:

- replay uses an atomic PostgreSQL transition: `DLQ -> REPLAY_PENDING`.
- only one concurrent request can win that transition.
- a double-click or concurrent replay request gets `409 Conflict`.
- replay-all and unlimited bulk replay are intentionally not implemented.

Attempt count semantics:

- historical `attempt_count` is preserved.
- replay processing continues from the existing count.
- for example, an event with `attempt_count = 4` in DLQ becomes attempt `5` when the worker processes the replayed message.

Replay metadata:

- previous `last_error`, `dlq_at`, and original Kafka metadata are retained.
- successful replay sets `replayed_at` and `REPLAYED`.
- the replay Kafka message includes headers: `eventmesh-replayed`, `eventmesh-replay-at`, and `eventmesh-original-attempt-count`.

Replay dual-write limitation:

```text
Kafka replay publish succeeds
  -> PostgreSQL replay state update fails
```

In that case the event may be replayed even though PostgreSQL still shows a stale replay state. EventMesh does not claim atomic replay across Kafka and PostgreSQL. A transactional outbox may be introduced later to close this gap.

Example commands:

```sh
curl http://localhost:8080/v1/admin/dlq?limit=20\&offset=0
curl http://localhost:8080/v1/admin/dlq/{event_id}
curl -X POST http://localhost:8080/v1/admin/dlq/{event_id}/replay
```

### Replay Smoke Test

1. Create or configure a handler path that returns `events.NonRetryable(err)`.
2. Send an event and let it publish to `eventmesh.dlq`.
3. Confirm `GET /v1/events/{event_id}` shows `DLQ`.
4. Inspect it with `GET /v1/admin/dlq/{event_id}`.
5. Fix the simulated downstream failure.
6. Replay with `POST /v1/admin/dlq/{event_id}/replay`.
7. Confirm the same `event_id` is published to `eventmesh.events`.
8. Let the worker process it.
9. Confirm PostgreSQL reaches `PROCESSED`.
10. Confirm `attempt_count` continued increasing rather than resetting.

### Retry Integration Test Design

Transient scenario:

```text
handler fails attempt 1 -> eventmesh.retry.1
handler fails attempt 2 -> eventmesh.retry.2
handler succeeds attempt 3
```

Expected final state:

- PostgreSQL `status = PROCESSED`
- PostgreSQL `attempt_count = 3`
- Redis key value `COMPLETED`
- `eventmesh.dlq` remains empty

Permanent failure scenario:

```text
handler returns events.NonRetryable(err)
  -> eventmesh.dlq
  -> PostgreSQL DLQ
  -> source offset committed
```

### Duplicate Delivery Smoke Test

Start infrastructure and run migrations:

```sh
make infra-up
make topics-create
make migrate-up
```

Start the worker and API in separate terminals:

```sh
make run-worker
make run-api
```

Create an event:

```sh
curl -s -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{"type":"order.created","source":"order-service","payload":{"order_id":"ORD-123","amount":149.99}}'
```

Copy the returned `event_id`, then publish a duplicate event directly to Kafka with the same `event_id`:

```sh
EVENT_ID=018fc1e0-b5d1-4f5e-9e39-70b6f1f05262
CREATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
printf '%s|{"event_id":"%s","type":"order.created","source":"order-service","payload":{"order_id":"ORD-123","amount":149.99},"created_at":"%s"}\n' "$EVENT_ID" "$EVENT_ID" "$CREATED_AT" | \
  docker compose exec -T kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic eventmesh.events \
    --property parse.key=true \
    --property key.separator='|'
```

Expected worker logs:

- First delivery logs `idempotency_acquired`, `event_processing_started`, `idempotency_completed`, `event_processing_succeeded`, and `offset_committed`.
- Duplicate delivery logs `duplicate_event_skipped` and `offset_committed`.
- Business processing does not run twice.
- `GET /v1/events/{event_id}` remains `PROCESSED`.

## Docker

Build the API image:

```sh
make docker-build
```

Build the worker image:

```sh
docker build --build-arg SERVICE=worker -t eventmesh-worker:local .
```

## Kubernetes

Starter manifests live under `deploy/k8s`. They intentionally reference local images and a ConfigMap; production deployments should move secrets such as database passwords into Kubernetes Secrets.
