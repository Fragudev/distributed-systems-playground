# Distributed Systems Playground

A collection of focused, production-quality examples demonstrating distributed-systems patterns
over a shared, generic order-processing domain. Built as technical portfolio evidence and System
Design interview preparation — each example is meant to answer *why the pattern exists*, not just
*how to implement it*.

Status: **1 of 6 examples done.** See [docs/adr](docs/adr) for the decisions behind this structure and
the planning document that preceded it.

## Examples

| Example | Teaches | Status |
|---|---|---|
| [synchronous-processing](examples/synchronous-processing) | Validation, transaction boundaries, synchronous response semantics — the baseline every other example is compared against | ✅ done |
| outbox | Transactional outbox vs. broken dual-write | planned |
| kafka-order-processing | Consumer groups, partitioning, ordering, retry/backoff, DLT, replay | planned |
| rabbitmq-order-processing | Same domain over exchanges/queues, native DLX/TTL — direct comparison with Kafka | planned |
| resilience | Circuit breakers, bulkheads, timeouts, backpressure, graceful degradation | planned |
| saga-order-fulfillment | Choreography-based saga, compensation, eventual consistency | planned |

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
│   ├── concepts/        cross-cutting write-ups (idempotency, ordering, backpressure...)
│   ├── events/           versioned JSON Schemas for the domain events
│   ├── diagrams/         architecture diagrams referenced from each README
│   ├── templates/        the example-README template every example follows
│   └── adr/              architecture decision records
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
