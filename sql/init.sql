-- Enterprise CDC & Lakehouse Ingestion Platform
-- Initial schema and logical replication publication setup

-- Enable pgcrypto if needed for UUIDs
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Clean schema setup
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;

-- 1. Orders Table
CREATE TABLE orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    total_price NUMERIC(12, 2) NOT NULL CHECK (total_price >= 0),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Inventory Table
CREATE TABLE inventory (
    sku VARCHAR(64) PRIMARY KEY,
    warehouse_id VARCHAR(32) NOT NULL,
    available_stock INTEGER NOT NULL CHECK (available_stock >= 0),
    reserved_stock INTEGER NOT NULL DEFAULT 0 CHECK (reserved_stock >= 0),
    last_restocked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Crucial for Debezium CDC: REPLICA IDENTITY FULL ensures Postgres records both
-- previous (before) and new (after) row states in WAL for UPDATE and DELETE operations.
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;

-- Create CDC publication for Debezium
CREATE PUBLICATION cdc_publication FOR TABLE orders, inventory;

-- Initial Seed Data
INSERT INTO inventory (sku, warehouse_id, available_stock, reserved_stock, last_restocked_at) VALUES
    ('SKU-TIRE-001', 'WH-FRANKFURT-1', 1250, 45, NOW() - INTERVAL '2 hours'),
    ('SKU-BELT-002', 'WH-HANOVER-2', 3400, 120, NOW() - INTERVAL '4 hours'),
    ('SKU-HOSE-003', 'WH-BERLIN-3', 890, 15, NOW() - INTERVAL '1 hour'),
    ('SKU-BRAKE-004', 'WH-FRANKFURT-1', 450, 60, NOW() - INTERVAL '6 hours');

INSERT INTO orders (order_id, customer_id, product_id, quantity, total_price, status, created_at, updated_at) VALUES
    ('ORD-2026-9001', 'CUST-DAIMLER', 'SKU-TIRE-001', 50, 4750.00, 'CONFIRMED', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes'),
    ('ORD-2026-9002', 'CUST-BMW-GROUP', 'SKU-BELT-002', 100, 1850.00, 'PROCESSING', NOW() - INTERVAL '15 minutes', NOW() - INTERVAL '10 minutes'),
    ('ORD-2026-9003', 'CUST-VOLVO', 'SKU-HOSE-003', 20, 620.00, 'SHIPPED', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '2 minutes');
