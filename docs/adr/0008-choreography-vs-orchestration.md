# 0008 — Choreography vs orchestration for the saga

## Status
Accepted

## Context
`saga-order-fulfillment` needs `payment-service` and `inventory-service` to react to a new order,
and needs *something* to decide whether the order ends up `CONFIRMED` or `CANCELLED` once both have
answered. There are two well-known shapes for this: **choreography** (each participant reacts to
events on its own, no central authority) and **orchestration** (a central coordinator explicitly
calls each participant and tells it what to do next). Per [A4 in the plan](../../02-distributed-systems-playground-PLAN.md),
this example is deliberately the capstone — it reuses `kafka-order-processing`'s consumers rather
than standing up new infrastructure, which shapes which option fits.

## Decision
**Choreography.** `PaymentEventListener` and `InventoryEventListener` each subscribe to
`order.created.v1` directly and publish their own outcome (`payment.completed.v1`/`payment.failed.v1`,
`inventory.reserved.v1`/`inventory.rejected.v1`) — same structure, same outbox, same idempotency
guard `kafka-order-processing`'s consumers already use. `SagaCoordinator` subscribes to those four
outcome topics, keeps one `SagaState` row per order, and once both legs are known, calls
`OrderService.confirmOrder`/`cancelOrder` — the *only* direct call in the whole flow. Cancellation
itself goes back out as `order.cancelled.v1`, which payment and inventory react to as their own
compensation trigger, not because the coordinator told them to compensate.

The result: participants → coordinator → order is the only line of control. The coordinator never
calls payment or inventory; it only ever calls `OrderService`, and even that call is really "publish
another event," not an RPC into another service's internals.

## Alternatives considered
- **Orchestration**: a central `SagaOrchestrator` explicitly invokes `PaymentService.charge(...)` and
  `InventoryService.reserve(...)` (in-process or via commands), then decides confirm/cancel and tells
  each participant explicitly what to undo. Rejected for this example on two grounds. First, it
  contradicts A4's whole premise — reusing `kafka-order-processing`'s consumers as independent,
  already-reactive components is the point; orchestration would mean rewriting them as
  command-handlers instead. Second, orchestration's actual advantage — one place to read the whole
  flow — matters more as the number of participants and steps grows; with two participants and one
  coordinator, choreography's `SagaState` table already gives an equally legible single place to
  observe the flow, without the added coupling of a coordinator that has to know every participant's
  API.
- **A hybrid** (participants publish events, but the coordinator also directly reads/writes their
  tables to double-check state): rejected — it would blur exactly the boundary this ADR is about.
  The coordinator only ever learns what happened through events, never by reaching into another
  participant's data.

## Consequences
- **Adding a third participant** (e.g. `shipping-service`) means adding one more listener on
  `order.created.v1` and teaching `SagaCoordinator` about one more outcome topic — no participant
  needs to change, since none of them know about each other. An orchestrated version would need the
  orchestrator's command sequence rewritten.
- **The coordinator can decide "cancelled" without waiting for a doomed leg to finish.** Because each
  participant reports independently, `SagaState.decideOutcome` cancels as soon as *either* leg fails,
  even if the other hasn't reported yet — see `SagaCompensationTest` and
  [docs/diagrams/saga-order-fulfillment.md](../diagrams/saga-order-fulfillment.md). An orchestrator
  calling participants in sequence would have to decide up front whether to call the second participant
  at all once the first fails, which is its own design question orchestration has to answer that
  choreography sidesteps entirely.
- **There is no single place that reads top-to-bottom as "the saga's steps."** Understanding the full
  flow means reading four components (`PaymentEventListener`, `InventoryEventListener`,
  `SagaCoordinator`, `OrderService`) and the topics between them, not one orchestrator method — the
  architecture diagram exists specifically to make that legible without it (§7's flagged risk in the
  plan).
- **If a future phase wants to compare orchestration too** (open point #4 in the plan), it's a
  separate phase, not a variant folded into this one — mixing both shapes in one example would make
  neither teach cleanly.
