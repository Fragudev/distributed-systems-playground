# Comparative dashboard: Kafka vs RabbitMQ

Both `kafka-order-processing` and `rabbitmq-order-processing` push the exact same metric —
`order_processing_events_total{broker, consumer_group, outcome}` — from their `ProcessingMetrics`
helper, tagged only by `broker` (`kafka` / `rabbitmq`, set via `management.metrics.tags.broker` in
each example's `application.yml`). Same name, same tags, two sources: that's what lets one Grafana
dashboard plot both side by side instead of comparing two differently-shaped views.

## Running it

```bash
docker compose --profile kafka --profile rabbitmq --profile observability up -d --wait
../../scripts/run-example.sh kafka-order-processing      # in one terminal
../../scripts/run-example.sh rabbitmq-order-processing   # in another
```

Create a few orders against each (`POST /api/v1/orders` on `:8083` and `:8084`), then open
`http://localhost:3000/d/kafka-vs-rabbitmq` (or whatever `DSPLAYGROUND_GRAFANA_PORT` is set to).

The dashboard itself is provisioned automatically — `observability/grafana/dashboards/kafka-vs-rabbitmq.json`
and `observability/grafana/provisioning/kafka-vs-rabbitmq.yaml`, mounted into the `observability`
Compose service — no manual import needed.

## What it shows

- **Processed events by consumer group** — both brokers' three consumer groups side by side; with
  identical traffic sent to both examples, the bars should match.
- **Duplicate (idempotent no-op) events** — redelivery being absorbed by the ADR 0005 guard, per
  broker.
- **Failed attempts (retry → DLQ path)** — only `inventory-service` produces these, via
  `PoisonMessageAndReplayTest`'s scenario or a manually triggered one; compares retry volume between
  Kafka's in-process backoff and RabbitMQ's queue-based one.
- **Total events processed, by broker** — the single-number summary: same workload, two transports.

Verified live with real traffic from both examples during development — see
[docs/adr/0007-kafka-vs-rabbitmq.md](../adr/0007-kafka-vs-rabbitmq.md) for what the comparison
actually concluded.
