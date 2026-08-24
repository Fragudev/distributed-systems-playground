# 0009 — What "eventual consistency" actually guarantees here

## Status
Accepted

## Context
`saga-order-fulfillment` has no distributed transaction: `orders`, `payment`, and
`inventory_reservation` are three separate write models, each owned by its own participant, updated
independently and asynchronously in reaction to events. At any given moment after an order is
created, they can legitimately disagree — an order can be `CREATED` while `payment` already shows
`COMPLETED` and `inventory_reservation` doesn't exist yet. This ADR states precisely what guarantee
the system actually gives, so "eventually consistent" doesn't stay a slogan — see [ADR 0004](0004-at-least-once-delivery.md)
and [ADR 0005](0005-idempotent-consumers.md), which this ADR builds on directly.

## Decision
**The guarantee is convergence, not synchronization.** Given no further failures, every order
reaches exactly one of two terminal states across all three models within a bounded time:
- `orders.status = CONFIRMED`, `payment.status = COMPLETED`, `inventory_reservation.status = RESERVED`
- `orders.status = CANCELLED`, `payment.status = REFUNDED` (only if it had reached `COMPLETED` first;
  otherwise it's simply never created for a `FAILED` payment), `inventory_reservation.status = RELEASED`
  (only if it had reached `RESERVED`; otherwise `REJECTED`)

There is no guarantee about the *order* in which the three models reach their terminal state, and no
guarantee that they're ever simultaneously consistent mid-flight — only that they converge.
`SagaCompensationTest` proves this with Awaitility rather than asserting it in prose: it forces the
inventory-rejects-after-payment-completes ordering specifically because it's the case most likely to
look like a bug (a "successful" payment on a cancelled order) if convergence weren't actually
verified.

## Alternatives considered
- **A synchronous two-phase commit or distributed transaction** across the three data stores: would
  give real atomicity, but requires all three participants to speak the same transaction coordinator
  protocol (XA or equivalent) and blocks all three during the transaction — exactly the coupling this
  whole playground's messaging examples exist to avoid. Rejected as contrary to the project's premise.
- **Read-your-writes consistency via a synchronous "status" endpoint that blocks until all legs
  report**: would hide the eventual-consistency window from a caller instead of demonstrating it, and
  reintroduces the synchronous-coupling problem `synchronous-processing`'s own README already covers
  as the naive baseline. Rejected — it would make this example redundant with that one.

## Consequences
- **A client reading the order immediately after `POST /api/v1/orders` will usually see `CREATED`**,
  not yet `CONFIRMED` or `CANCELLED` — this is the guarantee working as designed, not a bug to poll
  around. A real caller needs to either poll or subscribe to the order's own state changes; this
  example doesn't build either, since the point being demonstrated is the convergence itself.
- **`saga_state` is the only place that has ever seen both legs' outcomes together.** `orders`,
  `payment`, and `inventory_reservation` each only ever know their own slice — querying any one of
  them alone cannot tell you whether the saga is still in flight or genuinely stuck.
- **A permanently missing outcome (e.g. `payment-service` down indefinitely) means the order never
  converges** — `SagaState` stays with `paymentStatus = null` forever. This example doesn't build a
  saga-level timeout/reaper for that case; it's a real, named limitation (see the example README's
  "When not to use this pattern"), not an oversight.
