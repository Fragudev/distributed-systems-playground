# Kafka Order Processing

Scenario C: multiple services react to `order.created.v1` independently. This example is about
what happens after the event leaves the outbox — consumer groups, partitioning, idempotency, and
what a message that can never succeed does to the system around it.

## 1. Problem

Three services need to react to every order: reserve inventory, charge payment, send a
notification. Each needs its own full copy of the event (not a share of one queue's messages), each
must survive redelivery without double-acting, and one service's processing trouble shouldn't be
able to affect the other two — or block new orders from being created in the first place.

## 2. Naive solution

The instinctive shortcut: one consumer group, one `if` branch per concern.

```java
@KafkaListener(topics = "order.created.v1", groupId = "order-processor")
void onOrderCreated(String payload) {
  reserveInventory(payload);
  chargePayment(payload);
  sendNotification(payload);
}
```

This breaks in two ways at once. First, it's not three independent consumers — it's one, so scaling
it to more instances *splits the partitions between them* (competing consumption) instead of giving
every concern its own copy; add a second instance and inventory reservation might run on a different
node than the payment charge for the same order, with no relationship between them. Second, and
worse: if `chargePayment` throws, the whole method throws, and depending on error handling either
the message retries from the top (re-reserving inventory that already succeeded) or the DLT gets a
record that looks like "processing failed" when two-thirds of it actually didn't.

## 3. Improved solution

Three separate `@KafkaListener` methods, each its own consumer group
(`InventoryEventListener`/`PaymentEventListener`/`NotificationEventListener`, all reading
`order.created.v1`) — see [docs/diagrams/kafka-order-processing.md](../../docs/diagrams/kafka-order-processing.md).
Each is independently idempotent via `processed_event` (ADR 0005) and independently subject to
retry-then-DLT (ADR 0004, `KafkaConsumerConfig`). One group's trouble is invisible to the other two.

The order-creation side (`order/`, `outbox/`) is the same transactional-outbox flow as
`examples/outbox`, duplicated per [ADR 0002](../../docs/adr/0002-duplicated-domain.md) — this
example's new teaching content is entirely on the consumer side.

## 4. Architecture

See [docs/diagrams/kafka-order-processing.md](../../docs/diagrams/kafka-order-processing.md).

`order.created.v1` is created with 3 partitions (`KafkaConsumerConfig#orderCreatedTopic`); every
publish is keyed by `orderId`, so same order → same partition → order preserved, while different
orders spread across partitions for parallelism.

## 5. Failure modes

**The project's failure scenario 2:** `inventory-service`'s downstream dependency goes down
(simulated via `InventoryAvailabilityChecker`, swapped for a controllable stub in
`PoisonMessageAndReplayTest` — the same "wrap a real collaborator" seam as `EventPublisher` in the
outbox example). The listener throws on every attempt; `DefaultErrorHandler` retries 3 times total
with exponential backoff, then routes the record to `order.created.v1.DLT`. `payment-service` and
`notification-service` process their own copies of the same event throughout, completely unaffected.
Once the cause is "fixed," `scripts/replay-dlq.sh` (or, in the test, the equivalent in-process
replay) republishes the message from the DLT onto the original topic, and it succeeds.

## 6. Trade-offs

- **Fan-out via consumer groups costs one full copy of every event per group.** Three groups means
  three times the consumer-side throughput requirement compared to one shared group — worth it
  precisely because these are three genuinely independent concerns, not three parts of one job.
- **The DLT is per consumer group, not global.** A message that only `inventory-service` can't
  process still only ends up on the DLT because of `inventory-service` specifically — a genuinely
  malformed message that every listener chokes on would be dead-lettered independently by each of
  the three groups. Worth knowing before assuming "one DLT entry" means "the whole event failed."
- **`processed_event` grows without bound** in this example — see ADR 0005's accepted debt.

## 7. Testing

| Test | Proves |
|---|---|
| `ConsumerGroupsTest` | Fan-out: all three groups process the same event independently; redelivery of an already-processed event is a no-op |
| `PartitioningTest` | Same order → same partition; many distinct orders spread across more than one |
| `PoisonMessageAndReplayTest` | Failure scenario 2 end to end: retry → DLT → replay after the cause is fixed |

All three run against real Postgres and real Kafka via Testcontainers.

## 8. Operational concerns

- **Consumer lag per group** is the metric that tells you a specific service is falling behind —
  not a topic-wide number, since each group's lag is independent.
- **`order.created.v1.DLT` message count** should be near-zero in steady state; anything else means
  a consumer group is hitting a class of message it can't process.
- `kafka-ui` (started with this example's Compose profile) shows both the main topic and the DLT,
  plus per-group consumer lag, in real time.

## 9. When not to use this pattern

When there's genuinely one consumer of an event — a single service reacting to it — the consumer
group/fan-out distinction this example is built around doesn't apply, and `outbox` alone (with one
straightforward consumer) is the right level of complexity. Also not the right layer for
cross-service *ordering* guarantees beyond "per order": if service B's action must strictly follow
service A's for the same order across service boundaries, that needs explicit choreography — see
the (future) `saga-order-fulfillment` example.

## Running this example

```bash
../../scripts/run-example.sh kafka-order-processing
```

Then, e.g.:

```bash
curl -X POST localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","lines":[{"productId":"widget","quantity":2,"unitPrice":9.99}]}'
```

Watch `order.created.v1` and its three consumer groups in kafka-ui (`localhost:8090` by default).
To replay whatever is sitting on a dead-letter topic: `../../scripts/replay-dlq.sh order.created.v1`.
