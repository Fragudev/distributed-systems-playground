# 0001 — Repository structure: multiple independent examples, not one application

## Status
Accepted

## Context
The spec for this project explicitly warns: *"Avoid one giant application. Each example needs a
clear purpose."* The natural instinct for a Spring Boot portfolio project is a single modular
monolith (as in the sibling `ai-engineering-lab` project) with internal module boundaries. That
model optimizes for a cohesive product; this project optimizes for a reviewer being able to open
one folder, understand one pattern end to end, and compare it against a sibling folder solving the
same problem differently (Kafka vs. RabbitMQ).

## Decision
Each pattern lives under `examples/<name>` as an independently buildable and runnable Spring Boot
application, with its own `pom.xml`, own `README.md`, own database schema, and — where relevant —
its own Docker Compose profile. A single build-only aggregator `pom.xml` at the root provides shared
dependency/plugin version management via `dependencyManagement`, so versions stay consistent across
examples without coupling their code.

## Alternatives considered
- **Single modular monolith** (like `ai-engineering-lab`): rejected — it would hide the comparison
  value between Kafka and RabbitMQ inside one deployable, and a reviewer would have to understand the
  whole system to evaluate one pattern.
- **Separate Git repositories per example**: rejected — makes the "compare Kafka vs. RabbitMQ"
  narrative harder to present as one coherent portfolio piece, and duplicates repo-level tooling
  (CI, licensing, contribution docs) six times for no benefit.

## Consequences
- CI must build/test examples independently (path-filtered jobs), not as one monolithic pipeline,
  or a change to one example would needlessly fail CI for all the others.
- Shared infrastructure (`docker-compose.yml`) is profile-gated per example instead of split into
  per-example compose files, to avoid six near-duplicate compose files drifting apart.
- Domain code is deliberately duplicated across examples rather than extracted into a shared
  library — see [0002-duplicated-domain.md](0002-duplicated-domain.md).
