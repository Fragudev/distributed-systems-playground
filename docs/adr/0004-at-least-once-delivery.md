# 0004 — At-least-once delivery as the default guarantee

## Status
Accepted

## Context
`order.created.v1` needs a concrete delivery guarantee, not an implicit one. The three realistic
options are: at-most-once (deliver, don't confirm — a crash before processing loses the message),
at-least-once (redeliver until confirmed — a crash after processing but before acknowledging can
duplicate), and exactly-once (neither loss nor duplication — which Kafka only offers within its own
transactional boundary, not across an external side effect like a database write).

## Decision
At-least-once, everywhere in this example: `OutboxRelay` (already established in `examples/outbox`,
ADR 0003) guarantees the event is published at least once; `DefaultErrorHandler`'s retry-then-DLT
policy (`KafkaConsumerConfig`) guarantees a listener keeps getting the chance to process a message
until it either succeeds or is explicitly given up on; manual replay from the DLT
(`scripts/replay-dlq.sh`) is itself another at-least-once redelivery. Every one of these mechanisms
can, under the right crash timing, deliver the same message twice.

## Alternatives considered
- **At-most-once**: rejected outright — silently losing an order-created notification is worse than
  processing it twice, for every consumer in this example (inventory, payment, notifications all
  care more about completeness than about occasionally doing something twice).
- **Exactly-once via Kafka transactions**: Kafka's transactional producer/consumer APIs can give
  exactly-once *between Kafka topics*, but `InventoryEventListener` and friends write to Postgres,
  not another Kafka topic — the exactly-once boundary doesn't extend to that side effect. Chasing it
  here would add real complexity for a guarantee the example can't actually deliver end-to-end.

## Consequences
- **Every consumer must be idempotent.** This isn't optional cleanup — it's the direct, load-bearing
  consequence of this decision. See ADR 0005.
- **"Delivered" and "processed exactly once" are different claims**, and this project is explicit
  about only making the first one. Duplicate detection is the consumer's job, not the broker's.
- Tests prove this rather than asserting it: `ConsumerGroupsTest`'s redelivery test and
  `PoisonMessageAndReplayTest`'s replay both send the *same* event a second time and check that
  reprocessing is a no-op — the guarantee this ADR describes, not just the happy path.
