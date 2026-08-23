CREATE TABLE orders (
    id            UUID PRIMARY KEY,
    customer_id   VARCHAR(255) NOT NULL,
    total_amount  NUMERIC(12, 2) NOT NULL,
    status        VARCHAR(32) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);

-- The quantity/unit_price checks are a deliberate second line of defense: Bean Validation on the
-- API's request DTOs rejects bad input before it reaches here, but this table should also refuse
-- to store an inconsistent order line if that layer is ever bypassed (see
-- OrderTransactionBoundaryTest, which proves this and that the whole order rolls back with it).
CREATE TABLE order_line (
    id           UUID PRIMARY KEY,
    order_id     UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id   VARCHAR(255) NOT NULL,
    quantity     INT NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0)
);

CREATE INDEX idx_order_line_order_id ON order_line (order_id);
