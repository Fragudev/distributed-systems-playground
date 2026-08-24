# Distributed Systems Playground

[![CI](https://github.com/Fragudev/distributed-systems-playground/actions/workflows/ci.yml/badge.svg)](https://github.com/Fragudev/distributed-systems-playground/actions/workflows/ci.yml)

A collection of focused, production-quality examples demonstrating distributed-systems patterns
over a shared, generic order-processing domain. Built as technical portfolio evidence and System
Design interview preparation — each example is meant to answer *why the pattern exists*, not just
*how to implement it*.

## Examples

Each row answers the question that example exists to defend in a System Design interview.

| Example | Answers | Teaches | ADRs |
|---|---|---|---|
| [synchronous-processing](examples/synchronous-processing) | What does a correct baseline even look like, before any distributed-systems pattern is added? | Validation, transaction boundaries, synchronous response semantics | [0001](docs/adr/0001-repo-structure.md) |
| [outbox](examples/outbox) | How do you write to your own database and publish an event as one atomic unit, without a distributed transaction? | Transactional outbox vs. broken dual-write, at-least-once delivery | [0002](docs/adr/0002-duplicated-domain.md), [0003](docs/adr/0003-transactional-outbox.md) |
| [kafka-order-processing](examples/kafka-order-processing) | How do multiple independent services each react to the same event, safely handle redelivery, and recover from a message that can never succeed? | Consumer groups (fan-out), partitioning, idempotency, retry/backoff, DLT, replay | [0004](docs/adr/0004-at-least-once-delivery.md), [0005](docs/adr/0005-idempotent-consumers.md), [0006](docs/adr/0006-retry-dlq-strategy.md) |
| [rabbitmq-order-processing](examples/rabbitmq-order-processing) | Same guarantees as Kafka, over fundamentally different broker primitives — what actually differs, concretely? | Exchanges/queues, native DLX/TTL — direct comparison with Kafka | [0004](docs/adr/0004-at-least-once-delivery.md), [0005](docs/adr/0005-idempotent-consumers.md), [0006](docs/adr/0006-retry-dlq-strategy.md), [0007](docs/adr/0007-kafka-vs-rabbitmq.md) |
| [resilience](examples/resilience) | How does a service stay up when a downstream dependency is slow or down, without a broker in the picture at all? | Circuit breaker, bulkhead, timeout, graceful degradation | — |
| [saga-order-fulfillment](examples/saga-order-fulfillment) | How do independent services stay consistent with each other — and compensate correctly — with no distributed transaction spanning them? | Choreography-based saga, compensation, eventual consistency | [0008](docs/adr/0008-choreography-vs-orchestration.md), [0009](docs/adr/0009-eventual-consistency.md) |

Status: all six done — see [docs/adr](docs/adr) for the full decision record and the planning
document that preceded it.

`kafka-order-processing` and `rabbitmq-order-processing` both feed a comparative Grafana dashboard
with real, side-by-side data — see
[docs/diagrams/kafka-vs-rabbitmq-dashboard.md](docs/diagrams/kafka-vs-rabbitmq-dashboard.md) and
[docs/adr/0007-kafka-vs-rabbitmq.md](docs/adr/0007-kafka-vs-rabbitmq.md).

Each example's own README follows [docs/templates/example-readme.md](docs/templates/example-readme.md):
Problem → Naive solution → Improved solution → Architecture → Failure modes → Trade-offs → Testing →
Operational concerns → When not to use the pattern.

## Running an example

```bash
./scripts/run-example.sh synchronous-processing
```

This starts the infrastructure profile the example needs (see [docker-compose.yml](docker-compose.yml))
and runs the Spring Boot app. `./scripts/bootstrap.sh` checks Docker/Java prerequisites once, up front.

## Repository structure

```text
/
├── examples/           one independent Spring Boot app per pattern (see table above)
├── docs/
│   ├── diagrams/         architecture diagrams referenced from each README
│   ├── templates/        the example-README template every example follows
│   └── adr/              architecture decision records — see docs/adr/README.md for the index
├── observability/       Grafana dashboard + provisioning, mounted into the observability profile
├── scripts/             bootstrap, run-example, inject-failure, replay-dlq
└── docker-compose.yml   one file, profile-gated per example
```

Why one example per directory instead of a single application: see
[docs/adr/0001-repo-structure.md](docs/adr/0001-repo-structure.md). Why the order domain is
duplicated across examples instead of shared via a library: see
[docs/adr/0002-duplicated-domain.md](docs/adr/0002-duplicated-domain.md).

## Requirements

- Java 21+
- Maven (wrapper committed, no local install needed: `./mvnw`)
- Docker + Docker Compose v2

### Troubleshooting: Testcontainers can't find Docker

If `./mvnw verify` fails with `Could not find a valid Docker environment` and your Docker runs via
Rancher Desktop (non-standard socket at `~/.rd/docker.sock` instead of `/var/run/docker.sock`),
export before building:

```bash
export DOCKER_HOST="unix://$HOME/.rd/docker.sock"
export TESTCONTAINERS_RYUK_DISABLED=true
```

(`TESTCONTAINERS_RYUK_DISABLED` works around the cleanup sidecar container failing its own startup
wait strategy on some Rancher Desktop VMs; Testcontainers still cleans up via JVM shutdown hooks
without it.) A separate, already-fixed issue — Testcontainers' bundled `docker-java` probing the
daemon with an outdated API version that newer Docker Engine builds reject — is pinned in the root
`pom.xml`'s `maven-surefire-plugin` configuration, so it doesn't need a manual flag.

## License

Apache-2.0 — see [LICENSE](LICENSE).
