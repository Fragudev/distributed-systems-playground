# 0003 — Transactional outbox instead of dual-write

## Status
Accepted

## Context
Order creation needs to reliably announce itself as an event (`order.created.v1`) so other systems
can react. The obvious implementation — save the order, then publish to Kafka — writes to two
independent systems (Postgres, the broker) with no shared transaction between them. Either write can
fail independently of the other: the DB commit can succeed while the broker call fails (or the
process dies between the two), silently losing the event with no trace that it should have existed.
`examples/outbox`'s `NaiveOrderService` implements exactly this, and `NaiveDualWriteFailureTest`
demonstrates the loss concretely rather than describing it.

## Decision
Write an `outbox_event` row in the *same* database transaction as the order and its lines. A
separate poller (`OutboxRelay`) reads unpublished rows and publishes them to Kafka, marking each
row published only after the broker has acknowledged it. See `OrderService.createOrder` for the
transactional write and `OutboxRelay.publishPending` for the relay.

## Alternatives considered
- **Change Data Capture (Debezium reading the WAL)**: removes the need for a relay process
  entirely, and is what many production systems use. Rejected for this example specifically because
  it would require running Debezium + Kafka Connect in the local Compose stack, which teaches
  operating a CDC pipeline rather than the outbox pattern itself. Worth a future example if the
  playground grows; noted as accepted scope-limiting debt.
- **Two-phase commit (XA transactions) across Postgres and Kafka**: rejected — Kafka doesn't support
  XA, and even where the infrastructure exists, distributed 2PC trades an availability problem
  (blocking on the slowest participant) for the consistency problem it solves. Not a trade worth
  making here.
- **Publish synchronously inside the HTTP request, but retry indefinitely on failure**: doesn't
  solve the core problem — if the process crashes mid-retry, the event is still gone, because
  nothing durable records that it was ever supposed to be sent.

## Consequences
- **At-least-once delivery, not exactly-once.** A crash between the broker ack and the
  `published_at` write (inside `OutboxRelay.publishPending`) produces a duplicate on redelivery.
  This example's own tests (`OutboxFailureTest`) don't exercise that specific window — they prove
  zero loss and zero duplicates for a crash *between* rows in a batch, which is the achievable,
  honest claim. The harder gap is real and is exactly why `kafka-order-processing` and
  `rabbitmq-order-processing` build idempotent consumers: this pattern moves the reliability problem
  from the producer side to the consumer side, it doesn't eliminate it.
- **Publish latency is bounded by the relay's poll interval** (`OutboxRelay`'s `@Scheduled(fixedDelay
  = PT2S)`), not by request latency — a deliberate trade against instant delivery, in exchange for
  the durability guarantee.
- **The outbox table needs its own housekeeping** in a real system (published rows should eventually
  be archived or deleted); this example doesn't implement that, since demonstrating the delivery
  guarantee doesn't require it.
