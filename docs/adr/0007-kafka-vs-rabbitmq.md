# 0007 — Kafka vs RabbitMQ for this domain

## Status
Accepted

## Context
The plan for this repository was explicit: Kafka and RabbitMQ "should be compared, not included
just because both are popular." That's only a real comparison if it comes from having built the
same problem twice, not from a table copied out of a blog post. `kafka-order-processing` and
`rabbitmq-order-processing` implement the identical scenario — the same order domain, the same
transactional outbox, the same three downstream consumers, the same failure scenario — over the two
brokers, and this ADR is written from what that build actually surfaced.

## Comparison

| Axis | Kafka | RabbitMQ | Evidence in this repo |
|---|---|---|---|
| Retention / replay | Log with configurable retention; a consumer group can always replay from any offset | A queue's messages are gone once consumed (no mirroring here) | `scripts/replay-dlq.sh` only makes sense as "read the DLT, republish" for Kafka; the RabbitMQ equivalent is a broker-native "move messages" action in the Management UI — there's nothing to independently *replay from*, only to move |
| Ordering vs. parallelism | Both, together: partition by key gives per-key ordering *and* cross-key parallelism | One or the other: a single consumer on a queue preserves the whole queue's order but doesn't parallelize; competing consumers parallelize but lose ordering | `kafka-order-processing`'s `PartitioningTest` proves same-order-id-same-partition; `rabbitmq-order-processing` has no equivalent test because the guarantee doesn't exist — a single queue is just FIFO, full stop |
| Retry with backoff | Application-level (`DefaultErrorHandler` + `ExponentialBackOff`), holding the consumer thread | Broker-level (queue TTL + DLX chain), no application thread involved during the wait | ADR 0006, and the fact that `InventoryEventListener` looks almost identical in both examples except this exact mechanism |
| Work distribution | Consumer groups: partitions divided among instances in a group | Competing consumers: prefetch-based, any consumer can grab the next message | Not exercised by a dedicated test in either example (out of scope for this comparison — see A5 in the planning doc) but structurally different: Kafka's division is partition-count-bounded, RabbitMQ's isn't |
| Routing model | Topic + key; one dimension | Exchange types (direct/topic/fanout) + routing key; more expressive | `RabbitConfig`'s topic exchange with routing-key bindings vs. `KafkaConsumerConfig`'s flat `NewTopic` |
| Delivery bookkeeping | Consumer-tracked offsets; retry state lives in the consumer process's memory | Broker-tracked per-message state; `x-death` header survives a consumer restart | ADR 0006 |
| Operations | Requires thinking about partition count upfront; rebalancing has real cost | Queues/exchanges can be added and rewired at runtime with less upfront planning | Both examples' `RabbitConfig`/`KafkaConsumerConfig` — RabbitMQ's queue arguments are simpler to read cold than Kafka's topic+consumer-group+error-handler wiring |

## Recommendation

**Kafka** when replay matters (event sourcing, reprocessing history, feeding multiple independent
readers over time) or when partition-scoped ordering with real parallelism is a hard requirement.
**RabbitMQ** when the routing logic itself is the interesting part (multiple exchange types, dynamic
topology), when native per-message retry/delay without extra application code is worth more than log
retention, or when the operational simplicity of "it's just a queue" outweighs Kafka's replay
guarantees. Neither is a strictly better default — the recommendation is genuinely conditional on
which of the rows above the actual workload cares about.

## Consequences
- This example set doesn't demonstrate Kafka consumer-group rebalancing or RabbitMQ competing
  consumers under load — both are real, valid extensions but weren't necessary to make the
  comparison above honest, and adding them would have doubled the scope for a secondary axis (see
  the planning doc, A5).
- The comparative Grafana dashboard (`observability/grafana/dashboards/kafka-vs-rabbitmq.json`,
  provisioned via the `observability` Compose profile) reads the same metric names and tags —
  `order_processing_events_total{broker, consumer_group, outcome}` — from both examples, which is
  what makes "run both, look at one dashboard" possible instead of comparing two separate,
  differently-shaped views.
