CREATE TABLE product_stock (
    product_id VARCHAR(100) PRIMARY KEY,
    available_stock INTEGER NOT NULL CHECK (available_stock >= 0)
);
CREATE TABLE inventory_reservations (
    order_id UUID PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL REFERENCES product_stock(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status VARCHAR(16) NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE cancelled_orders (
    order_id UUID PRIMARY KEY,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    topic VARCHAR(200) NOT NULL,
    event_key VARCHAR(200) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON outbox_events (created_at) WHERE published_at IS NULL;
CREATE TABLE processed_events (
    consumer_name VARCHAR(150) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);
