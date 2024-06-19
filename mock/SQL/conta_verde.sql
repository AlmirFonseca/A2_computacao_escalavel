-- conta_verde.sql

DROP SCHEMA IF EXISTS conta_verde CASCADE; 
-- Cria o esquema para ContaVerde
CREATE SCHEMA conta_verde;

-- Cria a tabela de usuários
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

-- Cria a tabela de produtos
CREATE TABLE IF NOT EXISTS conta_verde.products (
    id_product SERIAL PRIMARY KEY,
    id VARCHAR(10),
    name VARCHAR(100) NOT NULL,
    image TEXT,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    store_id VARCHAR(42) NOT NULL
);

-- Cria a tabela de estoque
CREATE TABLE IF NOT EXISTS conta_verde.stock (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(10),
    quantity INT NOT NULL,
    store_id VARCHAR(42) NOT NULL
);

-- Cria a tabela de pedidos
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