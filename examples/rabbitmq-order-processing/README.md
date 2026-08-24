# RabbitMQ Order Processing

Scenario C/D, same domain as `kafka-order-processing`, over RabbitMQ. This example exists to make
[docs/adr/0007-kafka-vs-rabbitmq.md](../../docs/adr/0007-kafka-vs-rabbitmq.md) honest: the
comparison is built from having implemented the identical problem twice, not read off a table.

## 1. Problem

Same as `kafka-order-processing`'s §1: three services need to react to every order independently,
survive redelivery without double-acting, and stay isolated from each other's processing trouble —
now asking what that costs and buys specifically on RabbitMQ.

## 2. Naive solution

Same naive shortcut as the Kafka example (one queue, one handler doing three things) has the same
two failures here: no real independence between concerns, and a partial failure that's ambiguous
about what actually broke. See `kafka-order-processing`'s README §2 — repeating it here wouldn't add
anything RabbitMQ-specific.

## 3. Improved solution

Three **queues** (`inventory-service`, `payment-service`, `notification-service`) bound to one
**topic exchange** (`order-events`), each getting its own full copy of `order.created.v1` —
`RabbitConfig`. `inventory-service`'s queue additionally has a dead-letter-exchange pointing at a
TTL-delayed retry queue, itself dead-lettering back to the main queue — entirely declarative queue
configuration, no manual retry-topic code. See
[docs/diagrams/rabbitmq-order-processing.md](../../docs/diagrams/rabbitmq-order-processing.md).

## 4. Architecture

See the diagram linked above. The order-creation side (`order/`, `outbox/`) is the same
transactional-outbox flow as `outbox` and `kafka-order-processing`, duplicated per
[ADR 0002](../../docs/adr/0002-duplicated-domain.md) — `RabbitEventPublisher` is the only piece that
actually differs, publishing via `RabbitTemplate` with publisher confirms instead of blocking on a
Kafka producer future.

## 5. Failure modes

**The project's failure scenario 3**, the RabbitMQ shape of scenario 2: `inventory-service`'s
downstream dependency goes down (`InventoryAvailabilityChecker`, swapped for a controllable stub in
`PoisonMessageAndReplayTest`). Every failed attempt rejects the message without requeue; RabbitMQ's
own DLX/TTL chain (not application code) delays and redelivers it; `InventoryEventListener` counts
attempts via the `x-death` header RabbitMQ stamps automatically, and on the third gives up and
publishes straight to `inventory-service.dlq` itself. `payment-service` and `notification-service`
process their own copies throughout, unaffected. Once "fixed," republishing the DLQ message onto the
exchange succeeds — exactly what an operator would do from the RabbitMQ Management UI's "Move
messages" action, or the test's in-process equivalent.

## 6. Trade-offs

- **Fixed backoff, not exponential.** `RabbitConfig`'s retry queue has one TTL; Kafka's
  `ExponentialBackOff` grows the delay per attempt in application code for free. Matching that here
  needs multiple chained wait queues with increasing TTLs — extra queue topology, not extra code,
  but a real cost either way. See ADR 0006.
- **No partitioning, so no per-key ordering-with-parallelism.** A RabbitMQ queue is FIFO for
  whoever's attached to it; there's no `PartitioningTest` in this example because the guarantee
  Kafka's partitions provide doesn't have a RabbitMQ equivalent to test. See ADR 0007.
- **Retry state lives on the message itself** (`x-death`), not in a consumer process's memory —
  survives a consumer restart in a way Kafka's in-process retry count doesn't.

## 7. Testing

| Test | Proves |
|---|---|
| `FanOutTest` | All three queues process the same event independently; redelivery of an already-processed event is a no-op |
| `PoisonMessageAndReplayTest` | Failure scenario 3 end to end: reject → DLX/TTL retry → final DLQ → replay after the cause is fixed |

Both run against real Postgres and real RabbitMQ via Testcontainers.

## 8. Operational concerns

- **Queue depth** on `inventory-service.retry` and `inventory-service.dlq` are the metrics to watch
  — anything sitting in the DLQ means a class of message `inventory-service` can't process.
- The RabbitMQ Management UI (started with this example's Compose profile, `localhost:15672` by
  default) shows queue depths, message rates, and lets you inspect or manually move DLQ messages
  without any extra tooling — unlike Kafka's DLT, which needs a consumer to read.
- See [docs/diagrams/kafka-vs-rabbitmq-dashboard.md](../../docs/diagrams/kafka-vs-rabbitmq-dashboard.md)
  for the comparative Grafana dashboard both this example and `kafka-order-processing` feed.

## 9. When not to use this pattern

When replay — reprocessing historical events, or feeding a new consumer everything that happened
before it existed — is a real requirement: RabbitMQ queues don't retain consumed messages the way a
Kafka log does. Also not the right choice when per-key ordering combined with real parallelism
matters; RabbitMQ genuinely can't do both at once the way Kafka's partitioning does. See ADR 0007.

## Running this example

```bash
../../scripts/run-example.sh rabbitmq-order-processing
```

Then, e.g.:

```bash
curl -X POST localhost:8084/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","lines":[{"productId":"widget","quantity":2,"unitPrice":9.99}]}'
```

Watch the queues in the RabbitMQ Management UI (`localhost:15672`, default credentials
`dsplayground`/`dsplayground`).
