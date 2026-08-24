# 0005 — Idempotent consumers via a processed_event table

## Status
Accepted

## Context
Given at-least-once delivery (ADR 0004), every listener in this example — `InventoryEventListener`,
`PaymentEventListener`, `NotificationEventListener` — will eventually see the same event more than
once: after a consumer-group rebalance replays uncommitted offsets, after a DLT replay
(`scripts/replay-dlq.sh`), or simply because `DefaultErrorHandler` retried a message that had
actually already partially succeeded. Without a guard, that means double-reserving inventory,
double-charging a payment, or double-sending a notification.

## Decision
Each listener checks a `processed_event(consumer_group, event_id)` table *before* doing any work,
and writes to it only after the work succeeds. If the row already exists, the listener returns
immediately — redelivery becomes a no-op. See `InventoryEventListener.onOrderCreated` (and the other
two listeners, which follow the identical shape) and the `V1__create_schema.sql` migration for the
table's composite primary key, which is what makes the check-then-write atomic per (consumer,
event) pair without needing a separate lock.

## Alternatives considered
- **Rely on Kafka's own offset commits to prevent redelivery**: doesn't work — offset commits
  prevent redelivery *within a healthy run*, but a rebalance, a consumer restart between processing
  and committing, or a DLT replay all redeliver a message whose offset was never committed (or was
  committed for a *different* copy of it, in the replay case, since replay publishes a new record
  with a new offset entirely). Offsets track position in the log, not "have I done this work."
- **Make the business logic itself naturally idempotent** (e.g., an upsert keyed by order id instead
  of an insert): considered, and often the *better* answer in a real system with real business
  logic. Not used here because this example's listeners don't have real business logic to make
  idempotent — the `processed_event` table itself needed to exist anyway (it's also the queryable
  proof of fan-out that `ConsumerGroupsTest` reads from), so it doubles as the guard for free.
- **Deduplicate at the broker/producer side** (Kafka's idempotent producer, enabled by default in
  this example's `KafkaTemplate`): prevents *duplicate publishing* from a retried send, which is a
  real and separate problem, but does nothing about a consumer seeing the same, once-published
  message more than once. Complementary to this decision, not a substitute for it.

## Consequences
- **The idempotency key is `(consumer_group, event_id)`, not just `event_id`.** Each consumer group
  needs its own independent record of what it's processed — inventory-service having handled an
  event says nothing about whether payment-service has. `ProcessedEventId`'s composite key encodes
  exactly that.
- **This adds a write to every successful processing path.** For business logic with its own
  natural idempotency (an upsert), that write would be redundant; here, since there is no other
  natural guard, it's load-bearing.
- **Cleanup isn't implemented.** `processed_event` grows forever in this example, same accepted
  scope-limiting debt as `outbox_event` in ADR 0003 — a real system would need a retention policy for
  both.
