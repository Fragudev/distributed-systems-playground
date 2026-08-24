# Resilience

Scenario D over a synchronous call, no broker: order creation calls a simulated shipping-carrier
dependency directly. This example is about what protects the *whole app* from that one
dependency's trouble — not the messaging patterns the rest of this playground focuses on.

## 1. Problem

Creating an order needs to confirm the shipment with an external carrier system, synchronously,
before responding to the client. External dependencies are sometimes slow, sometimes down. Neither
should be able to take the rest of the app down with it.

## 2. Naive solution

Call the carrier directly, with nothing protecting the call:

```java
public CompletableFuture<ShippingConfirmation> requestShipment(UUID orderId) {
  simulator.callCarrier(orderId);  // no timeout, no circuit breaker, no bulkhead
  return CompletableFuture.completedFuture(ShippingConfirmation.confirmed());
}
```

This is [`NaiveShippingGateway`](src/main/java/dev/fgutierrez/dsplayground/resilience/shipping/NaiveShippingGateway.java) —
real, runnable code, not a description. `NaiveShippingGatewayFailureTest` proves the concrete bug:
when the carrier is slow, the call blocks for the carrier's *entire* delay, completely unbounded. At
the scale of one request that's merely slow. At the scale of a real system: every concurrent order
creation blocks a request thread on the same struggling dependency, and with enough concurrent
orders, that's every request thread the server has — not just the ones talking to shipping. One slow
dependency takes down the whole app.

## 3. Improved solution

[`ResilientShippingGateway`](src/main/java/dev/fgutierrez/dsplayground/resilience/shipping/ResilientShippingGateway.java)
wraps the identical call with three Resilience4j patterns (`resilience4j.*.instances.shipping` in
`application.yml`):

- **Bulkhead** (thread pool, 2 core / 4 max / 2 queued) — bounds how many concurrent calls to the
  carrier can ever be in flight, so a slow carrier can only ever tie up a small, fixed slice of
  capacity, never the whole app.
- **Time limiter** (500ms) — bounds how long any one call can take.
- **Circuit breaker** (opens at 50% failure rate over a 10-call window) — once the carrier is
  clearly failing, stops calling it at all until a wait period passes, instead of every new order
  queueing up behind a dependency that's already down.

`fallback()` is what turns all of this into *graceful* degradation rather than just failing
differently: whichever of the three trips, the order still gets created and returned successfully —
only its shipping confirmation is deferred. See
[docs/diagrams/resilience.md](../../docs/diagrams/resilience.md) for every path through this.

## 4. Architecture

See the diagram linked above. The order-creation side (`order/`, `api/`) is the same domain as
`synchronous-processing`, duplicated per [ADR 0002](../../docs/adr/0002-duplicated-domain.md) — this
example's new content is entirely `shipping/`. The shipping call happens in `OrderController`,
*after* `OrderService.createOrder`'s transaction has already committed — a slow or failing carrier
never holds a database connection open, and can never roll back an order that already exists.

## 5. Failure modes

**The project's failure scenario 4:** the simulated carrier (`ShippingSimulator`, toggled via
`ShippingAdminController` for the live demo or directly in tests) goes slow or down.
`OrderApiResilienceTest` proves, through the real HTTP API:

- A slow carrier degrades to `PENDING_CONFIRMATION` rather than a timeout error.
- Enough failures trip the circuit breaker to `OPEN` — verified via
  `CircuitBreakerRegistry`, not assumed.
- Once open, a request comes back in under 200ms — proof the circuit short-circuits before ever
  touching the carrier again, not just that it eventually degrades.
- 10 concurrent requests against a slow carrier and a 2-4-2 bulkhead: every single one still
  returns `201`, and at least some visibly degrade — proof the bulkhead actually engaged under load.

Verified live, not just in tests: the circuit breaker cycles `CLOSED → OPEN → CLOSED` in a real run
against `/actuator/circuitbreakers`, and the same transition shows up in
`resilience4j_circuitbreaker_state` in Prometheus once the app is pointed at the `observability`
Compose profile.

## 6. Trade-offs

- **Every degraded order needs a real reconciliation path in an actual system** — a background job
  retrying `PENDING_CONFIRMATION` orders. This example doesn't implement one: `shippingStatus` isn't
  even persisted (it's only known at the moment of creation, see `OrderResponse`'s doc comment) —
  accepted scope-limiting debt, since demonstrating the resilience patterns doesn't require closing
  that loop.
- **Fixed thresholds, not adaptive ones.** The bulkhead/circuit-breaker/timeout numbers in
  `application.yml` are picked to make this example's tests deterministic, not tuned against real
  traffic — a production system would derive them from actual latency/failure data.
- **The bulkhead is thread-pool-based**, which costs a dedicated thread pool per protected call —
  more overhead than a semaphore bulkhead, in exchange for genuinely isolating this dependency's
  threads from the rest of the app's.

## 7. Testing

| Test | Proves |
|---|---|
| `NaiveShippingGatewayFailureTest` | The naive solution's bug, concretely: an unprotected call blocks for the carrier's full delay |
| `OrderApiResilienceTest` | Healthy carrier confirms; slow/failing carrier degrades gracefully; circuit breaker opens and fails fast; bulkhead caps concurrency without any request failing outright |

Only Postgres is needed via Testcontainers — no broker, consistent with this example having none.

## 8. Operational concerns

- `resilience4j_circuitbreaker_state{name="shipping", state="open"}` is the metric to alert on —
  see [docs/diagrams/kafka-vs-rabbitmq-dashboard.md](../../docs/diagrams/kafka-vs-rabbitmq-dashboard.md)
  for the pattern this playground uses to feed Grafana (this example isn't on that particular
  dashboard, but pushes the same way).
- `GET /actuator/circuitbreakers` gives the current state, failure rate, and buffered/rejected call
  counts directly — the fastest way to check "is shipping healthy right now" without a dashboard.
- The bulkhead's `notPermittedCalls` count (same endpoint) is what tells you the bulkhead is
  actively shedding load, as opposed to just being idle.

## 9. When not to use this pattern

When a dependency's failure genuinely should fail the whole request — e.g., a payment charge that
must succeed for the order to be valid at all. Graceful degradation is the right call for *this*
domain because a delayed shipping confirmation is a real, acceptable outcome; it would be the wrong
call for something the business can't actually proceed without. Also unnecessary complexity for a
dependency that's reliably fast and rarely fails — these patterns cost real overhead (thread pools,
sliding windows) that only pays for itself against a genuinely unreliable dependency.

## Running this example

```bash
../../scripts/run-example.sh resilience
```

Then, e.g.:

```bash
curl -X POST localhost:8085/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","lines":[{"productId":"widget","quantity":2,"unitPrice":9.99}]}'

# Toggle the simulated carrier's behavior:
curl -X POST localhost:8085/admin/shipping-simulator \
  -H 'Content-Type: application/json' -d '{"mode":"FAILING"}'   # or "SLOW" / "NORMAL"

# Watch the circuit breaker react:
curl localhost:8085/actuator/circuitbreakers
```
