# Saga Order Fulfillment

The playground's capstone: a choreographed saga where two independent services (payment,
inventory) react to an order in parallel, and a coordinator that never calls either of them
directly decides whether the order should be confirmed or compensated.

## 1. Problem

`kafka-order-processing` already has `payment-service` and `inventory-service` reacting
independently to `order.created.v1`. Neither one knows about the other, and nothing decides what
the *order itself* should become once both have answered. If payment succeeds but inventory can't
fulfil the order, something has to notice, undo the payment, and cancel the order — without a
distributed transaction spanning three separately-owned data stores, and without either service
knowing the other exists.

## 2. Naive solution

Have `OrderService` call payment and inventory synchronously, in sequence, and roll back by hand:

```java
void createOrder(...) {
  Order order = save(...);
  boolean paid = paymentClient.charge(order);       // blocks
  if (!paid) { cancel(order); return; }
  boolean reserved = inventoryClient.reserve(order); // blocks
  if (!reserved) { paymentClient.refund(order); cancel(order); return; }
  confirm(order);
}
```

This is `synchronous-processing`'s own naive baseline, extended with manual rollback — it reintroduces
every problem that example exists to demonstrate (the request blocks on two downstream calls, a crash
between `charge` and `reserve` leaves the refund never issued, and `OrderService` now has to know
both payment's and inventory's APIs directly). It also throws away the whole point of
`kafka-order-processing`'s consumer groups: two services that were independent become coupled again
the moment one calls the other synchronously.

## 3. Improved solution

Choreography: `PaymentEventListener` and `InventoryEventListener` each react to `order.created.v1`
on their own — same shape as `kafka-order-processing`'s consumers — and each publishes its own
outcome. `SagaCoordinator` watches those four outcome topics, keeps one `SagaState` row per order,
and once both legs are known, tells `OrderService` to confirm or cancel — the only direct call
anywhere in the flow. Cancelling publishes `order.cancelled.v1`, which payment and inventory each
treat as their own compensation trigger (refund / release), idempotently, the same way they treated
`order.created.v1` as their trigger to act in the first place.

See [ADR 0008](../../docs/adr/0008-choreography-vs-orchestration.md) for why choreography over
orchestration, and [ADR 0009](../../docs/adr/0009-eventual-consistency.md) for exactly what guarantee
this gives.

## 4. Architecture

See [docs/diagrams/saga-order-fulfillment.md](../../docs/diagrams/saga-order-fulfillment.md).

Reuses the same Kafka broker as `kafka-order-processing` (Compose profile `saga` maps to the same
`kafka`/`kafka-ui` services) rather than standing up new infrastructure — this example's teaching
content is the choreography and compensation flow, not a new broker.

## 5. Failure modes

**The project's failure scenario 5:** payment completes, inventory rejects. `InventoryEventListener`
publishes `inventory.rejected.v1`; `SagaCoordinator` sees that alongside the already-recorded
`payment.completed.v1`, decides `CANCELLED` without waiting for anything else, and calls
`OrderService.cancelOrder`. That publishes `order.cancelled.v1`, which `PaymentEventListener` reacts
to by refunding a payment that had already gone through — the actual compensation, not just "never
charged." `SagaCompensationTest` forces the rejection via the same controllable-collaborator seam as
`InventoryAvailabilityChecker` elsewhere in the playground, then uses Awaitility to prove convergence
rather than asserting it.

## 6. Trade-offs

- **No saga-level timeout.** If a participant never publishes its outcome (crashed, stuck), the
  order never converges — `saga_state` sits with a null leg forever. A saga reaper/timeout is a real
  gap, named on purpose rather than built, since it's a second pattern (scheduled compensation) this
  example isn't trying to also teach.
- **Compensation is per-participant, not centrally scripted.** The coordinator doesn't know *how*
  payment or inventory compensate — it only publishes `order.cancelled.v1` and trusts each
  participant to react correctly. This is what keeps the coordinator small, but it also means there's
  no single place to read "here's everything that gets undone."
- **Reusing `kafka-order-processing`'s retry-free container factory here is deliberate, not an
  oversight** — see `KafkaListenerConfig`: retry/DLT is already demonstrated end-to-end in that
  example, and repeating it here would be noise around what this example is actually about.

## 7. Testing

| Test | Proves |
|---|---|
| `SagaHappyPathTest` | Both legs succeeding converges the order to `CONFIRMED` |
| `SagaCompensationTest` | Failure scenario 5: inventory rejection compensates an already-completed payment, and all three models converge to their cancelled/refunded/rejected terminal state |

Both run against real Postgres and real Kafka via Testcontainers, and both assert convergence with
Awaitility rather than a fixed sleep — proving the guarantee ADR 0009 describes, not just that the
code runs.

## 8. Operational concerns

- **`saga_state` rows stuck with a null `payment_status` or `inventory_status`** past a reasonable
  window are the signal that a participant isn't reporting — the gap named in §6.
- **`saga_compensation_total{reason}`** (per the project plan's metrics list) is the metric to watch
  for compensation rate; a sudden spike means whatever `InventoryAvailabilityChecker`/
  `PaymentAuthorizer` stand in for in production started failing.
- Reuses `kafka-ui` from the `saga` Compose profile to inspect all six topics and consumer lag per
  participant, same as `kafka-order-processing`.

## 9. When not to use this pattern

If there's only one downstream participant reacting to an event, there's nothing to coordinate —
`kafka-order-processing`'s plain consumer groups are the right level of complexity. If the business
process genuinely needs a single place that reads top-to-bottom as "the steps," or needs to decide
mid-flow whether to even call the next participant, orchestration (not built here — see ADR 0008's
alternatives) fits that shape better than choreography does. And if losing an in-flight saga to a
crashed participant forever (no timeout, per §6) isn't acceptable, this example needs a saga
reaper before it's production-ready, not just before it's "more thorough."

## Running this example

```bash
../../scripts/run-example.sh saga-order-fulfillment
```

Then, e.g.:

```bash
curl -X POST localhost:8086/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","lines":[{"productId":"widget","quantity":2,"unitPrice":9.99}]}'
```

Watch `order.created.v1` through `order.cancelled.v1` and all four consumer groups in kafka-ui
(`localhost:8090` by default). There's no HTTP endpoint to force a rejection — that's only
reachable via `SagaCompensationTest`'s controllable checker, since this example ships with
`AlwaysApprovePaymentAuthorizer`/`AlwaysAvailableInventoryChecker` as its default (happy-path)
collaborators.
