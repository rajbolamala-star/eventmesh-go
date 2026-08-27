# EventMesh Performance Characterization

This document defines how to benchmark EventMesh without confusing API acceptance with full event processing.

No benchmark numbers have been collected in this environment because Docker is unavailable here. Populate the result tables only after running the commands on a real local or dedicated test machine.

## What To Measure

API acceptance path:

```text
client -> POST /v1/events -> PostgreSQL RECEIVED insert -> Kafka publish -> 202 Accepted
```

End-to-end processing path:

```text
created_at -> Kafka queueing -> worker consume -> Redis idempotency -> PostgreSQL lifecycle updates -> processed_at
```

`POST /v1/events` returning `202` means the event was accepted and published. It does not mean the worker has processed it.

## Likely Bottlenecks

- API synchronous PostgreSQL insert before Kafka publish.
- Kafka producer configured for durability with synchronous writes and `RequiredAcks=RequireAll`.
- Worker process handles one message at a time to preserve offset correctness.
- PostgreSQL lifecycle updates on every event attempt.
- Redis idempotency acquire/complete operations.
- Kafka partition count limiting useful consumer-group parallelism.
- Trace export volume if tracing is sampled too aggressively during high load.

## Benchmark Hygiene

A benchmark is misleading if it:

- reports API latency as end-to-end processing latency.
- ignores Kafka consumer lag and recovery time.
- changes Kafka durability settings only to improve numbers.
- mixes large intentional failure traffic into the primary happy-path throughput run.
- exports 100% of traces under high load and attributes tracing overhead to EventMesh alone.
- omits machine specs, OS, Go version, Docker version, Kafka partitions, worker count, and load profile.
- runs against a cold system without a warm-up period.

## Environment Record

Fill this out for every benchmark run.

| Field | Value |
|-------|-------|
| Date | |
| Machine CPU | |
| Machine memory | |
| OS | |
| Go version | |
| Docker version | |
| Kafka image/version | `bitnami/kafka:3.7` |
| PostgreSQL image/version | `postgres:16` |
| Redis image/version | `redis:7` |
| Prometheus image/version | `prom/prometheus:v2.53.0` |
| Jaeger image/version | `jaegertracing/all-in-one:1.57` |
| Kafka topic partitions | |
| API instances | |
| Worker instances | |
| `KAFKA_PRODUCER_TIMEOUT` | |
| `POSTGRES_MAX_CONNS` | |
| `REDIS_OPERATION_TIMEOUT` | |
| `OTEL_TRACES_ENABLED` | |
| `OTEL_TRACE_SAMPLE_RATIO` | |
| Load profile | |
| Warm-up period | |
| Test duration | |

## Startup

```sh
make infra-up
make topics-create
make migrate-up
```

Start the API:

```sh
OTEL_TRACES_ENABLED=false make run-api
```

Start one worker:

```sh
OTEL_TRACES_ENABLED=false make run-worker
```

For tracing-specific tests, enable tracing with a bounded sample ratio:

```sh
OTEL_TRACES_ENABLED=true OTEL_TRACE_SAMPLE_RATIO=0.05 make run-api
OTEL_TRACES_ENABLED=true OTEL_TRACE_SAMPLE_RATIO=0.05 make run-worker
```

Use low sampling during high-throughput benchmarks so the trace backend does not become the benchmark.

## k6 Load Profiles

Load test script:

```text
loadtest/k6/events.js
```

Smoke:

```sh
make load-smoke
```

Baseline:

```sh
make load-baseline
```

Stress:

```sh
make load-stress
```

Soak:

```sh
make load-soak
```

Spike:

```sh
make load-spike
```

Useful overrides:

```sh
BASE_URL=http://localhost:8080 \
K6_BASELINE_RPS=100 \
K6_BASELINE_DURATION=10m \
K6_BASELINE_VUS=100 \
K6_BASELINE_MAX_VUS=400 \
make load-baseline
```

k6 captures:

- requests/sec
- P50/P95/P99 API latency
- HTTP error rate
- 202 acceptance rate

The script intentionally uses normal business event types:

- `order.created`
- `payment.received`
- `shipment.created`

It does not use development failure event types.

## End-To-End Processing Latency

Measure from PostgreSQL after load:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT
  COUNT(*) FILTER (WHERE status = 'PROCESSED') AS processed,
  COUNT(*) FILTER (WHERE status = 'RETRYING') AS retrying,
  COUNT(*) FILTER (WHERE status = 'DLQ') AS dlq,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processed_at - created_at))) FILTER (WHERE processed_at IS NOT NULL) AS p50_seconds,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processed_at - created_at))) FILTER (WHERE processed_at IS NOT NULL) AS p95_seconds,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processed_at - created_at))) FILTER (WHERE processed_at IS NOT NULL) AS p99_seconds
FROM events;"
```

Processing throughput over the last minute:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT COUNT(*) AS processed_last_minute
FROM events
WHERE status = 'PROCESSED'
  AND processed_at >= NOW() - INTERVAL '1 minute';"
```

Lifecycle counts:

```sh
make perf-status
```

## Prometheus Checks

API:

```sh
curl -fsS http://localhost:8080/metrics | grep -E 'eventmesh_dlq_replay|eventmesh_events'
```

Worker:

```sh
curl -fsS http://localhost:9091/metrics | grep -E 'eventmesh_events_consumed_total|eventmesh_events_processed_total|eventmesh_events_processing_failed_total|eventmesh_retries_published_total|eventmesh_dlq_published_total|eventmesh_idempotency_duplicate_total'
```

Record counter values before and after each load profile. Rates should be calculated over a known interval.

## Kafka Lag

Capture lag before load:

```sh
docker compose exec -T kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group eventmesh-processors
```

Capture maximum lag during load by sampling the same command every 5-10 seconds.

Capture lag after load stops until it drains. Record recovery time:

```text
recovery_time = time when lag returns to baseline - time when load stopped
```

A scalable setup may accumulate lag during a burst, but it should drain after traffic falls below processing capacity.

## Worker Scaling Experiment

Keep topic partitions constant first, then compare:

- 1 worker
- 2 workers
- 4 workers

Run the same k6 profile each time.

Record:

- actual API RPS
- API P95/P99
- processing P95/P99
- max consumer lag
- recovery time
- worker CPU/memory
- PostgreSQL connection pressure

Kafka partition rule:

```text
active workers for one topic/group <= partition count
```

With 3 partitions and 4 workers, at most 3 workers can actively consume `eventmesh.events` partitions.

## Partition Scaling Experiment

If practical, repeat baseline/stress runs with:

- 3 partitions
- 6 partitions
- 12 partitions

Keep workload and worker count documented. Do not compare runs if the database was not reset or if old Kafka lag remained.

Topic recreation for local-only testing destroys topic data:

```sh
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic eventmesh.events
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic eventmesh.events --partitions 6 --replication-factor 1
```

Repeat for retry/DLQ topics only if the scenario needs them reset. Do not do this against shared environments.

## PostgreSQL Checks

Pool configuration comes from:

- `POSTGRES_MAX_CONNS`
- `POSTGRES_MIN_CONNS`
- `POSTGRES_CONNECT_TIMEOUT`

Observe lifecycle write pressure:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT status, COUNT(*)
FROM events
GROUP BY status
ORDER BY status;"
```

Recent slow end-to-end events:

```sh
docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "
SELECT
  event_id,
  type,
  status,
  attempt_count,
  EXTRACT(EPOCH FROM (processed_at - created_at)) AS processing_seconds
FROM events
WHERE processed_at IS NOT NULL
ORDER BY processing_seconds DESC
LIMIT 20;"
```

Do not add indexes or tune SQL until benchmark evidence points to a database bottleneck.

## Redis Checks

Redis operations should normally be much faster than the event-processing path. Watch idempotency counters and Redis errors:

```sh
curl -fsS http://localhost:9091/metrics | grep -E 'eventmesh_idempotency_acquired_total|eventmesh_idempotency_duplicate_total|eventmesh_idempotency_errors_total'
```

Local-only key scan:

```sh
docker compose exec -T redis redis-cli --scan --pattern 'eventmesh:idempotency:*' | head
```

Do not use Redis keys as metric labels.

## Kafka Producer Settings

Current kafka-go producer settings:

- `Async=false`: API waits for Kafka write result.
- `RequiredAcks=RequireAll`: broker waits for all in-sync replicas.
- `Balancer=&kafka.Hash{}`: `event_id` key keeps one logical event on a stable partition.
- No explicit batch-size or batch-timeout tuning yet.

This favors correctness and durability over maximum ingestion throughput. Do not change these guarantees just to improve a benchmark.

## CPU And Memory

Docker:

```sh
docker stats
```

Capture API, worker, Kafka, Postgres, Redis, Prometheus, and Jaeger during stress and soak profiles.

Kubernetes:

```sh
kubectl top pods
```

Only use this if metrics-server is installed.

## Failure Workloads

Run controlled retry/DLQ load separately from happy-path throughput.

Example small retry workload:

```sh
EVENTMESH_DEV_FAILURES_ENABLED=true KAFKA_RETRY_DELAYS=2s,3s,4s make run-worker
```

Then publish a small number of `test.retry_twice` events manually. Measure:

- retry publication rate
- retry consumer behavior
- main-topic lag
- DLQ count

Do not mix a large failure percentage into the main throughput benchmark.

## Duplicate Workload

Use a small dedicated duplicate-delivery test. Measure:

- duplicate messages delivered
- `eventmesh_idempotency_duplicate_total`
- handler executions from logs or controlled test instrumentation

This validates correctness under duplicate delivery. It is not the primary throughput benchmark.

## Result Template

Populate only with actual measurements.

| Workers | Partitions | Target RPS | Actual RPS | API P95 | API P99 | Processing P95 | Processing P99 | Max Lag | Recovery Time | Errors |
|---------|------------|------------|------------|---------|---------|----------------|----------------|---------|---------------|--------|
| | | | | | | | | | | |

## Scaling Questions

Use the measurements to answer:

- What throughput can one worker sustain?
- When does Kafka lag start growing?
- Does adding workers improve throughput?
- At what point does PostgreSQL become limiting?
- At what point does API latency degrade?
- How quickly does the system recover after overload?

## Resume Wording

Safe qualitative version before benchmark numbers:

```text
Built EventMesh, a production-style distributed event-processing platform in Go and Kafka with consumer groups, Redis idempotency, staged retries, DLQ replay, PostgreSQL lifecycle tracking, Prometheus metrics, OpenTelemetry tracing, Kubernetes manifests, and reproducible k6 benchmarking methodology.
```

Measured-number template after benchmarks:

```text
Built EventMesh, a distributed event-processing platform in Go and Kafka supporting <MEASURED_RPS> events/sec at <MEASURED_P95> P95 processing latency on <ENVIRONMENT>, with consumer groups, Redis idempotency, staged retries, DLQ replay, PostgreSQL lifecycle tracking, Prometheus metrics, OpenTelemetry tracing, and Kubernetes deployment.
```

