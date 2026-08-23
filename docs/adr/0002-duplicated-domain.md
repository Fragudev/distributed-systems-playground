# 0002 — Duplicate the order domain across examples instead of a shared library

## Status
Accepted

## Context
`Order`, `OrderLine` and the event envelope are near-identical across `synchronous-processing`,
`outbox`, `kafka-order-processing`, `rabbitmq-order-processing` and `saga-order-fulfillment`. The
default engineering instinct is to extract a shared `domain` or `common` library to avoid repeating
~3-4 small classes six times.

## Decision
Duplicate the minimal domain in each example instead of extracting a shared library.

## Rationale
This project's value is a reviewer opening exactly one example folder and reading it top to bottom
without having to jump into a separate module to understand what `Order` means or how the event
envelope is built. A shared library would solve a problem this project doesn't have (multiple teams
maintaining diverging domain models) at the cost of the problem it does have (readability in
isolation). The domain is intentionally kept tiny — two classes, one event envelope — specifically so
that duplicating it stays cheap. This is also, itself, a real distributed-systems trade-off worth
being able to discuss in an interview: shared kernel vs. duplication, and when each is right.

## Alternatives considered
- **Shared `domain` Maven module**: rejected for the reason above. Also would force every example to
  depend on a common artifact, reintroducing exactly the coupling `0001` avoids at the repo-structure
  level.
- **Code generation from a single schema** (e.g. generate `Order` from `docs/events/order.schema.json`
  into each example): considered for a later phase if the duplication ever causes real drift, but
  adds build complexity that isn't justified while there are only six examples and two classes.

## Consequences
- The JSON Schema for each event in `docs/events/` is the single source of truth for the *contract*;
  the Java classes in each example are independent implementations of that contract, not copies of
  each other's code.
- A schema change must be applied by hand to every example that uses that event. Acceptable at this
  scale; would need revisiting if the domain grew significantly.
