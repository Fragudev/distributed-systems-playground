# Transactional Outbox

Scenario B: order creation announces itself as an event. This example is about what sits between
the database commit and the broker — the reliability gap dual-write leaves open, and the pattern
that closes it.

## 1. Problem

Order creation needs to reliably notify other systems (`order.created.v1`). "Reliably" means: if the
order was committed, the event *will* eventually be delivered — not "probably was," not "was, unless
something else failed at the same moment."

## 2. Naive solution

Save the order, then publish to Kafka:

```java
Order saved = orderRepository.save(Order.create(customerId, lines, clock)); // commits on its own
eventPublisher.publish(EVENT_TYPE, saved.getId().toString(), toJson(saved));  // separate system, separate failure domain
```

This is [`NaiveOrderService`](src/main/java/dev/fgutierrez/dsplayground/outbox/naive/NaiveOrderService.java) —
real, runnable code, not a description. `NaiveDualWriteFailureTest` rigs the publish call to fail and
shows exactly what breaks: the order is committed (`orderRepository.count()` is `1`), but the event
never went out, and — this is the part that makes it a real bug rather than a recoverable hiccup —
there is no outbox row, no queue, no anywhere that records an event was ever supposed to exist for
that order. It's not delayed. It's gone.

## 3. Improved solution

[`OrderService.createOrder`](src/main/java/dev/fgutierrez/dsplayground/outbox/order/OrderService.java)
writes the order *and* an `outbox_event` row in the same `@Transactional` method — one commit, or
none. [`OutboxRelay`](src/main/java/dev/fgutierrez/dsplayground/outbox/outbox/OutboxRelay.java) polls
for unpublished rows every two seconds, publishes each to Kafka, and only marks a row published after
the broker has acknowledged it. See [docs/adr/0003-transactional-outbox.md](../../docs/adr/0003-transactional-outbox.md)
for the alternatives this was weighed against (CDC, 2PC) and why they didn't fit this example.

## 4. Architecture

See [docs/diagrams/outbox.md](../../docs/diagrams/outbox.md) for both paths side by side.

`EventPublisher` is a small seam between `OutboxRelay` and Kafka specifically so failure scenarios
can be simulated realistically (see [§5](#5-failure-modes)) without mocking the broker away — tests
wrap the real `KafkaEventPublisher` instead of replacing it.

## 5. Failure modes

**The scenario this example is built around:** the relay dies mid-batch. `OutboxFailureTest` creates
three orders (three pending rows), rigs the *second* publish attempt to fail, and asserts: the first
row is published, the second and third are still pending — nothing partially corrupted, nothing lost.
Polling again (simulating a restart) publishes both remaining rows, and the broker ends up with
exactly three messages: no loss, no duplicates.

**The gap this doesn't close:** a crash between the broker's ack and the `published_at` write would
produce a duplicate on the next poll. This example's tests don't exercise that specific window — see
[Trade-offs](#6-trade-offs) and ADR 0003 for why that's an honest limitation of the pattern itself,
not a hole in the test suite.

## 6. Trade-offs

- **At-least-once, not exactly-once.** The unclosed gap above means a consumer of this event must be
  idempotent. This example doesn't have a consumer to demonstrate that in — `kafka-order-processing`
  and `rabbitmq-order-processing` do, deliberately.
- **Publish latency is decoupled from request latency**, bounded instead by the relay's poll
  interval (2s here). The client gets its `201` immediately; the event follows within a couple of
  seconds, not instantly.
- **An extra table and an extra process to run**, compared to the naive version's two lines of code.
  That cost buys the delivery guarantee — it's not free, and for a system that can tolerate losing
  the occasional event, it wouldn't be worth paying.

## 7. Testing

| Test | Proves |
|---|---|
| `OutboxRelayTest` | Happy path: a pending row publishes and is marked published; the event actually lands on the real Kafka topic |
| `OutboxFailureTest` | The mid-batch crash scenario from [§5](#5-failure-modes) — zero loss, zero duplicates for a between-row failure |
| `NaiveDualWriteFailureTest` | The naive solution's bug, concretely: order committed, event permanently lost |

All three run against real Postgres and real Kafka via Testcontainers — `FlakyEventPublisher` (in
`src/test/.../support`) wraps the real Kafka producer rather than replacing it, so the only thing
being simulated is *when* the process dies, not whether Kafka itself works.

## 8. Operational concerns

- `outbox_event` rows with `published_at IS NULL` older than a few poll intervals indicate the relay
  is stuck or the broker is unreachable — that's the metric to alert on in a real deployment (not
  implemented here, since demonstrating the guarantee doesn't require a metrics pipeline).
- The partial index on unpublished rows (`idx_outbox_event_unpublished`) keeps the relay's query fast
  regardless of how large the table grows, as long as published rows are eventually archived —
  something a real system would need and this example doesn't implement (see ADR 0003).
- `kafka-ui` (started with this example's Compose profile) is the fastest way to see events land on
  `order.created.v1` in real time while running the demo.

## 9. When not to use this pattern

When the "event" is purely informational and losing one occasionally is genuinely fine (a metrics
counter, a non-critical notification) — the extra table, relay process, and at-least-once handling
on the consumer side aren't worth it. Also not the right layer to solve *ordering* or *exactly-once*
guarantees — those are separate problems this pattern doesn't claim to address.

## Running this example

```bash
../../scripts/run-example.sh outbox
```

Then, e.g.:

```bash
curl -X POST localhost:8082/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","lines":[{"productId":"widget","quantity":2,"unitPrice":9.99}]}'
```

Watch the event land on `order.created.v1` in kafka-ui (`localhost:8090` by default).
