# Synchronous Order Processing

Scenario A: what "just persist it correctly" actually requires — validation, an atomic transaction
boundary, and honest response semantics — before any messaging enters the picture. Every other
example in this playground is implicitly compared against this one: it's the answer to "why not
just do it synchronously?"

## 1. Problem

A client creates an order with one or more lines and needs to know, in the same request, whether it
was accepted. The system must guarantee that an order and its lines are persisted together: a client
should never see an order that exists with only some of its lines, or a line that references an
order that doesn't exist.

## 2. Naive solution

Save the order first, then save each line in a separate call:

```java
Order saved = orderRepository.save(order);      // commits (or autocommits) on its own
lineRepository.saveAll(order.getLines());        // separate call, separate failure domain
```

If the second call fails partway — a bad line, a dropped connection, the process crashing between
the two calls — the order row is already committed with zero or partially-saved lines. The client
either got a `201` for an order that doesn't actually match what it asked for, or the failure surfaces
as a `500` for an order that *is* already sitting in the database, invisible unless something
specifically checks for it. Nothing in this naive version makes that state impossible.

## 3. Improved solution

[`OrderService.createOrder`](../synchronous-processing/src/main/java/dev/fgutierrez/dsplayground/syncprocessing/order/OrderService.java)
wraps the whole aggregate write in one `@Transactional` method; `Order` owns its `OrderLine`s via JPA
cascade, so `orderRepository.save(order)` inserts both in the same transaction. Either the order and
all of its lines commit, or none of them do — see [Failure modes](#5-failure-modes) for the test that
proves it.

Validation is deliberately two layers, not one:

- **Bean Validation** on `CreateOrderRequest` (`@NotBlank`, `@NotEmpty`, `@Positive`) rejects bad
  input before it reaches the service — this is what a real client's mistakes hit, returned as a
  `400` Problem Detail via Spring's built-in `spring.mvc.problemdetails.enabled` support, no custom
  exception-handling code needed.
- **Database `CHECK` constraints** on `order_line` (`quantity > 0`, `unit_price > 0`) are a second,
  independent line of defense — not for the API client, but for the data itself, in case that first
  layer is ever bypassed (a bug, a second write path added later, a direct script). Defense in depth
  costs two places to look, but the two failure modes it defends against are genuinely different.

## 4. Architecture

See [docs/diagrams/synchronous-processing.md](../../docs/diagrams/synchronous-processing.md).

`OrderController` → `OrderService` (the transaction boundary) → `OrderRepository` (Spring Data JPA) →
Postgres, with `Order.lines` mapped `fetch = FetchType.EAGER` — deliberately, since `open-in-view` is
disabled and the DTO mapping happens after the transaction (and its Hibernate session) has already
closed; lazy loading here would throw `LazyInitializationException` on every read. EAGER is the
honest choice for a small, owned aggregate like this one — see [Trade-offs](#6-trade-offs) for when
it stops being the right call.

## 5. Failure modes

`OrderTransactionBoundaryTest` builds an `OrderLine` with `quantity = 0` directly (bypassing the API's
Bean Validation on purpose) and calls `OrderService.createOrder` with it alongside one valid line.
The database rejects the invalid line via its `CHECK` constraint; the test asserts that
`orderRepository.count()` is unchanged afterward — proving the valid line's insert didn't leave an
orphaned order behind. "Recovered correctly" here means exactly that: zero partial writes, not a
best-effort cleanup after the fact.

## 6. Trade-offs

- **EAGER fetch on `lines`** avoids `LazyInitializationException` for this small aggregate, but
  doesn't scale to an order with hundreds of lines — every read pulls all of them. Fine here; would
  need revisiting if the domain grew.
- **Synchronous processing blocks the caller** for as long as the transaction takes. That's exactly
  the point of comparison with `outbox` and the messaging examples: this example is the baseline that
  makes their added complexity (and added latency-hiding) legible.
- **Two validation layers** mean two places to keep in sync if the rules ever change, in exchange for
  not trusting a single layer to be the only thing standing between a bug and corrupt data.

## 7. Testing

| Test | Proves |
|---|---|
| `SynchronousProcessingApplicationTests` | The app boots against a real Postgres (Testcontainers) and reports healthy |
| `OrderApiTest` | Create → 201 + Location + correct computed total; empty-lines → 400 Problem Detail; unknown id → 404 Problem Detail |
| `OrderTransactionBoundaryTest` | A constraint violation on one line rolls back the *entire* order — see [§5](#5-failure-modes) |

All three use a real Postgres via Testcontainers rather than H2 — the `CHECK` constraints and schema
validation in `OrderTransactionBoundaryTest` are genuine Postgres behavior, not an in-memory
approximation of it.

## 8. Operational concerns

There's no async processing here, so there's no consumer lag or DLQ to watch — the entire failure
surface is the HTTP request itself. What matters operationally:

- `/actuator/health` reports `db` status — the connection pool to Postgres is the only dependency.
- Request latency *is* the user-facing latency; there's nothing hidden behind a queue. A slow query
  here is a slow response, full stop — unlike the async examples, where a slow consumer is invisible
  to the caller.
- Connection pool sizing (HikariCP, default Spring Boot settings) is the practical concurrency limit
  for this example, since every request holds a connection for the whole transaction.

## 9. When not to use this pattern

When order creation needs to trigger work in other systems (charge a payment, reserve inventory,
notify a warehouse) that shouldn't block the client's response, or that can legitimately take longer
than an HTTP timeout — that's exactly what `outbox` and the messaging examples in this playground
exist to demonstrate. Forcing that kind of multi-system fan-out to stay synchronous just to keep this
example's simplicity would trade correctness (or availability) for a false sense of simplicity.

## Running this example

```bash
../../scripts/run-example.sh synchronous-processing
```
