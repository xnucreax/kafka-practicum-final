#!/bin/sh

psql -h postgres -U postgres -d postgres -c "
CREATE TABLE IF NOT EXISTS client_requests (
    query TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id  VARCHAR(255) PRIMARY KEY,
    name        TEXT,
    description TEXT,
    price       TEXT,
    category    TEXT,
    brand       TEXT,
    stock       TEXT,
    sku         TEXT,
    tags        TEXT,
    images      TEXT,
    specifications TEXT,
    created_at  TEXT,
    updated_at  TEXT,
    index       TEXT,
    store_id    TEXT
);"
