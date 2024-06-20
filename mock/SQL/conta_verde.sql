-- conta_verde.sql

DROP SCHEMA IF EXISTS conta_verde CASCADE; 
-- Create schema
CREATE SCHEMA conta_verde;

-- Create users table
CREATE TABLE IF NOT EXISTS conta_verde.users (
    id_user SERIAL PRIMARY KEY,
    id VARCHAR(12) NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    address TEXT NOT NULL,
    registration_date VARCHAR(21) NOT NULL,
    birth_date DATE NOT NULL,
    store_id VARCHAR(42) NOT NULL
);

-- Create products table
CREATE TABLE IF NOT EXISTS conta_verde.products (
    id_product SERIAL PRIMARY KEY,
    id VARCHAR(10),
    name VARCHAR(100) NOT NULL,
    image TEXT,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    store_id VARCHAR(42) NOT NULL
);

-- Create stocks table
CREATE TABLE IF NOT EXISTS conta_verde.stock (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(10),
    quantity INT NOT NULL,
    store_id VARCHAR(42) NOT NULL
);

-- Create purchase_orders table
CREATE TABLE IF NOT EXISTS conta_verde.purchase_orders (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(12),
    product_id VARCHAR(10),
    quantity INT NOT NULL,
    creation_date  VARCHAR(20) NOT NULL,
    payment_date  VARCHAR(20),
    delivery_date  VARCHAR(20),
    store_id VARCHAR(42) NOT NULL
);

-- Create price_history table
CREATE TABLE IF NOT EXISTS conta_verde.price_history (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(10),
    price DECIMAL(10, 2) NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    store_id VARCHAR(42) NOT NULL
);

-- Trigger to update price_history
CREATE OR REPLACE FUNCTION conta_verde.update_price_history()
RETURNS TRIGGER AS $$
BEGIN
    If NEW.price <> OLD.price THEN
        INSERT INTO conta_verde.price_history (product_id, price, recorded_at, store_id)
        VALUES (NEW.id, NEW.price, CURRENT_TIMESTAMP, NEW.store_id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER price_history_trigger
AFTER UPDATE OF price ON conta_verde.products
FOR EACH ROW
EXECUTE FUNCTION conta_verde.update_price_history();