CREATE TABLE orders (
    id            UUID PRIMARY KEY,
    customer_id   VARCHAR(255) NOT NULL,
    total_amount  NUMERIC(12, 2) NOT NULL,
    status        VARCHAR(32) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE order_line (
    id           UUID PRIMARY KEY,
    order_id     UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id   VARCHAR(255) NOT NULL,
    quantity     INT NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0)
);

CREATE INDEX idx_order_line_order_id ON order_line (order_id);

CREATE TABLE outbox_event (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(64) NOT NULL,
    aggregate_id    UUID NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ
);

CREATE INDEX idx_outbox_event_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_event (
    consumer_group  VARCHAR(64) NOT NULL,
    event_id        UUID NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_group, event_id)
);
