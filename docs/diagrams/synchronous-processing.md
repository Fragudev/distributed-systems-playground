# synchronous-processing — architecture

```mermaid
sequenceDiagram
    participant Client
    participant OrderController
    participant OrderService
    participant Postgres as Postgres (orders, order_line)

    Client->>OrderController: POST /api/v1/orders
    OrderController->>OrderController: @Valid CreateOrderRequest (Bean Validation)
    alt invalid request
        OrderController-->>Client: 400 Problem Detail
    else valid request
        OrderController->>OrderService: createOrder(customerId, lines)
        activate OrderService
        Note over OrderService,Postgres: single @Transactional boundary
        OrderService->>Postgres: INSERT order + order_line (cascaded)
        alt a line violates a CHECK constraint
            Postgres-->>OrderService: constraint violation
            OrderService-->>OrderController: DataIntegrityViolationException
            Note over Postgres: whole transaction rolled back — no orphaned order
            OrderController-->>Client: 500 (see docs/adr for the two-layer validation trade-off)
        else all rows valid
            Postgres-->>OrderService: committed
            OrderService-->>OrderController: Order
            deactivate OrderService
            OrderController-->>Client: 201 Created + Location
        end
    end

    Client->>OrderController: GET /api/v1/orders/{id}
    OrderController->>OrderService: getOrder(id)
    OrderService->>Postgres: SELECT order + order_line (EAGER)
    Postgres-->>OrderService: Order
    OrderService-->>OrderController: Order
    OrderController-->>Client: 200 OK
```

Two validation layers, deliberately: Bean Validation on `CreateOrderRequest` (`@Positive`, `@NotBlank`,
`@NotEmpty`) rejects bad input before it ever reaches the database — that's what a real client hits.
The database's own `CHECK (quantity > 0)` / `CHECK (unit_price > 0)` constraints on `order_line` are a
second, independent line of defense, proven by `OrderTransactionBoundaryTest`, which bypasses the API
layer on purpose to show that even then, the whole order is rolled back atomically rather than left
half-written.
