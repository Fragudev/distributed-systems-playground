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

-- Shared by all four publishing components (order, payment, inventory, and — via order — the
-- saga coordinator's own effects), unlike prior examples where only one component ever published.
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

CREATE TABLE payment (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL UNIQUE,
    status      VARCHAR(32) NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE inventory_reservation (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL UNIQUE,
    status      VARCHAR(32) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

-- One row per order, owned by the saga coordinator alone. Not an aggregate any participant reads
-- to decide its own behavior — payment and inventory never look at this table, only at the events
-- they consume. See ADR 0008.
CREATE TABLE saga_state (
    order_id          UUID PRIMARY KEY,
    payment_status    VARCHAR(32),
    inventory_status  VARCHAR(32),
    outcome           VARCHAR(32),
    updated_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE notification_log (
    id         UUID PRIMARY KEY,
    order_id   UUID NOT NULL UNIQUE,
    message    VARCHAR(1024) NOT NULL,
    sent_at    TIMESTAMPTZ NOT NULL
);
