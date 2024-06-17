-- conta_verde.sql

-- Cria o esquema para ContaVerde
CREATE SCHEMA conta_verde;

-- Cria a tabela de usuários
CREATE TABLE IF NOT EXISTS conta_verde.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    address TEXT NOT NULL,
    registration_date VARCHAR(21) NOT NULL,
    birth_date DATE NOT NULL,
    store_id INT NOT NULL
);

-- Cria a tabela de produtos
CREATE TABLE IF NOT EXISTS conta_verde.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    image TEXT,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    store_id INT NOT NULL
);

-- Cria a tabela de estoque
CREATE TABLE IF NOT EXISTS conta_verde.stock (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES conta_verde.products(id),
    quantity INT NOT NULL,
    store_id INT NOT NULL
);

-- Cria a tabela de pedidos
CREATE TABLE IF NOT EXISTS conta_verde.purchase_orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES conta_verde.users(id),
    product_id INT NOT NULL REFERENCES conta_verde.products(id),
    quantity INT NOT NULL,
    creation_date TIMESTAMP NOT NULL,
    payment_date TIMESTAMP,
    delivery_date TIMESTAMP,
    store_id INT NOT NULL
);