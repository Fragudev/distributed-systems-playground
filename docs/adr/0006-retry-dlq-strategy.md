# 0006 — Retry/DLQ strategy: Kafka retry-topics vs RabbitMQ native DLX+TTL

## Status
Accepted

## Context
Both `kafka-order-processing` and `rabbitmq-order-processing` need the same behavior: a message
that genuinely can't be processed right now should be retried a bounded number of times with
backoff, then set aside for manual attention without blocking anything behind it. The two brokers
solve this with fundamentally different primitives, and building the same scenario on both — not
just reading about it — is what makes ADR 0007's comparison honest.

## Decision
**Kafka** (`KafkaConsumerConfig`): `DefaultErrorHandler` retries in-process with an
`ExponentialBackOff` (3 total attempts), then `DeadLetterPublishingRecoverer` publishes the record
to `order.created.v1.DLT`. The retry delay is application code holding the consumer thread; the
broker sees nothing until the final publish.

**RabbitMQ** (`RabbitConfig`): the retry delay is a queue property, not application code. A failed
message is rejected without requeue; the main queue's `x-dead-letter-exchange` routes it to a wait
queue with a fixed `x-message-ttl`; that queue's own dead-letter config routes it back to the main
queue once the TTL expires. `InventoryEventListener` only gets involved to count attempts (via the
`x-death` header RabbitMQ stamps automatically) and to publish to the final DLQ once attempts are
exhausted, since a queue can't conditionally dead-letter to two different destinations by count.

## Alternatives considered
- **Kafka: a single manual retry-topic per attempt** (`order.created.v1.retry-1`,
  `.retry-2`, ...), each consumed by a delayed listener: closer to RabbitMQ's shape, but needs
  application code to create and wire N topics and N listeners for something RabbitMQ gets from two
  queue arguments. Rejected in favor of the simpler, equally valid in-process backoff — it's the
  more common real-world pattern for Kafka specifically because manual retry topics are this
  cumbersome.
- **RabbitMQ: Spring AMQP's `RetryOperationsInterceptor`** (in-process retry, mirroring Kafka's
  `DefaultErrorHandler` almost exactly): rejected on purpose. It would make the two examples
  *look* more alike, but at the cost of the actual point of this ADR — RabbitMQ's queue-level DLX+TTL
  is infrastructure Kafka simply doesn't have. Using Spring's in-memory retry here would compare two
  application-level retry libraries, not two brokers.

## Consequences
- **RabbitMQ's backoff is fixed per hop** (`RabbitConfig.RETRY_DELAY_MS`), not exponential — a
  genuine, not-papered-over difference. Kafka's `ExponentialBackOff` grows the delay each attempt in
  application code for free; matching that in RabbitMQ needs multiple wait queues with increasing
  TTLs chained together, which is exactly the kind of extra queue-topology cost worth naming rather
  than hiding.
- **Kafka's retry state lives in the consumer process's memory**; if the process crashes mid-backoff,
  the retry count resets on redelivery — Kafka doesn't track attempts durably the way RabbitMQ's
  `x-death` header does. RabbitMQ's retry count survives a consumer restart because it's stamped on
  the message itself, not held in a variable.
- **Operationally**, RabbitMQ's DLQ is just another queue, inspectable and replayable from the
  Management UI with no extra tooling. Kafka's DLT needs a consumer (or `scripts/replay-dlq.sh`) to
  read and republish — there's no "browse and click replay" built into the broker itself.
