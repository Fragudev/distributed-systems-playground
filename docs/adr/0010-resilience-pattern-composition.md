# 0010 — Composing three resilience patterns around one synchronous dependency

## Status
Accepted

## Context
`resilience` is the one example with no broker in it: order creation calls a simulated shipping
carrier synchronously, and the question is what protects the rest of the app when that one
dependency is slow or down. `NaiveShippingGateway` shows the unprotected baseline and
`NaiveShippingGatewayFailureTest` proves its bug — the call blocks for the carrier's entire delay,
unbounded, so enough concurrent orders exhaust every request thread the server has, including the
ones serving endpoints that never touch shipping.

Three separate things can go wrong with such a dependency, and each has its own well-known pattern:
it can be *slow* (time limiter), it can be *failing consistently* (circuit breaker), and it can tie
up *more concurrent capacity than it deserves* (bulkhead). This example was built by stacking all
three plus a fallback, which is more machinery than any single pattern demo needs — and until now
that composition was described in the example's own README but never recorded as a decision, making
`resilience` the only example in this repository without an ADR.

## Decision
**All three Resilience4j patterns on one call, plus a fallback** —
`ResilientShippingGateway.requestShipment` carries `@CircuitBreaker`, `@Bulkhead` and `@TimeLimiter`
for the same `shipping` instance. Each covers a failure mode the other two don't: the time limiter
(500ms) bounds one slow call, the bulkhead (2 core / 4 max / 2 queued threads) bounds how much of the
app's capacity slow calls can consume in aggregate, and the circuit breaker (50% failure rate over a
10-call window, 5s open, 3 half-open probes) stops calling a dependency that is clearly down instead
of queueing every new order behind it.

**A thread-pool bulkhead, not a semaphore one** (`Bulkhead.Type.THREADPOOL`). A semaphore bulkhead
caps concurrency but blocks the *calling* thread, which is the request thread this example is
specifically trying to protect — the naive version's whole bug. Only the thread-pool variant moves
the wait off the request thread and onto a bounded pool of its own.

**`fallback` is what makes it graceful rather than merely fast-failing.** Whichever of the three
trips, the order is still created and returned `201`; only its shipping confirmation degrades to
`PENDING_CONFIRMATION`. A circuit breaker with no fallback converts a slow failure into a fast one,
which protects the app but not the user.

**The shipping call sits outside the transaction, by placement rather than by annotation.**
`OrderController` calls `orderService.createOrder(...)` — `@Transactional`, commits on return — and
only then calls the gateway. A slow or failing carrier therefore never holds a database connection
open and can never roll back an order that already exists. This is what makes degrading to
`PENDING_CONFIRMATION` honest: the order genuinely is persisted at that point.

## Alternatives considered
- **A circuit breaker alone.** The most commonly demonstrated pattern, and the one most people reach
  for first. Rejected as insufficient on its own here: a breaker only trips *after* enough calls have
  failed, so during the window before it opens — and during every half-open probe — slow calls are
  still consuming request threads with nothing bounding how many. The bulkhead is what covers that
  window; the breaker is what stops the retrying once the verdict is in.
- **A semaphore bulkhead.** Cheaper (no dedicated pool, less overhead per protected call) and enough
  when the goal is purely "don't let too many concurrent calls pile up". Rejected because it leaves
  the caller's own thread blocked for the duration, which does not protect the resource this example
  is about — see the Decision above.
- **Failing the request when shipping fails**, instead of degrading. Simpler and arguably more
  honest for some domains. Rejected for *this* domain specifically: a delayed shipping confirmation
  is a real, acceptable business outcome, so failing an order the customer successfully placed
  discards work for no reason. It would be the right call for a dependency the order genuinely
  cannot be valid without — a payment charge — which is why the example README names that boundary
  explicitly rather than presenting graceful degradation as universally correct.
- **Retrying the carrier call.** Rejected: a retry against a dependency that is slow or down adds
  load to something already struggling, and the bulkhead's bounded pool means retries would consume
  the very capacity the bulkhead exists to protect. The circuit breaker's half-open probes already
  provide the bounded "is it back yet?" retry this needs.

## Consequences
- **The thresholds are chosen for deterministic tests, not tuned against traffic.** The 500ms time
  limit sits deliberately between a `NORMAL` call and the simulator's 2s `SLOW` delay so failure-mode
  tests trigger without racing the clock; the bulkhead and breaker numbers are small so
  `OrderApiResilienceTest` can exhaust them with ten concurrent requests. A production system would
  derive all of them from real latency and failure data.
- **Every degraded order needs a reconciliation path that this example does not build.**
  `shippingStatus` is not even persisted — it is known only at the moment of creation. Accepted
  scope-limiting debt: demonstrating the patterns does not require closing that loop, but a real
  system degrading this way owes itself a background job that retries `PENDING_CONFIRMATION`.
- **A dedicated thread pool per protected dependency is a real cost.** It buys genuine isolation
  from the rest of the app's threads; it is not free, and a system with many protected dependencies
  pays it per dependency.
- **`fallback` must be package-private, not private.** Resilience4j invokes it reflectively via the
  `fallbackMethod` attribute and does not force accessibility, so a `private` fallback fails with
  `IllegalAccessException` on the first trip — a footgun worth naming, since it only surfaces once
  something actually fails.
