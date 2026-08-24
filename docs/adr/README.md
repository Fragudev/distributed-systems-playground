# Architecture Decision Records

| # | Title | Status |
|---|---|---|
| [0001](0001-repo-structure.md) | Repository structure: multiple independent examples, not one application | Accepted |
| [0002](0002-duplicated-domain.md) | Duplicate the order domain across examples instead of a shared library | Accepted |
| [0003](0003-transactional-outbox.md) | Transactional outbox instead of dual-write | Accepted |
| [0004](0004-at-least-once-delivery.md) | At-least-once delivery as the default guarantee | Accepted |
| [0005](0005-idempotent-consumers.md) | Idempotent consumers via a `processed_event` table | Accepted |
| [0006](0006-retry-dlq-strategy.md) | Retry/DLQ strategy: Kafka retry-topics vs RabbitMQ native DLX+TTL | Accepted |
| [0007](0007-kafka-vs-rabbitmq.md) | Kafka vs RabbitMQ for this domain | Accepted |
| [0008](0008-choreography-vs-orchestration.md) | Choreography vs orchestration for the saga | Accepted |
| [0009](0009-eventual-consistency.md) | What "eventual consistency" actually guarantees here | Accepted |

Each record follows Status → Context → Decision → Alternatives considered → Consequences (0002 adds
a short Rationale; 0007 compares two brokers rather than deciding between alternatives, so it uses
Comparison → Recommendation instead).
