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

-- -- Insere registros de exemplo
-- INSERT INTO conta_verde.users (name, email, address, registration_date, birth_date, store_id) VALUES
-- ('John Doe', 'john.doe@example.com', '123 Main St', '2023-06-01', '1990-01-01', 1),
-- ('Jane Smith', 'jane@example.com', '456 Elm St', '2024-01-02', '1985-02-02', 2);

-- INSERT INTO conta_verde.products (name, image, description, price, store_id) VALUES
-- ('Product A', 'image_a.jpg', 'Description for Product A', 19.99, 1),
-- ('Product B', 'image2.jpg', 'Description of Product B', 29.99, 2);

-- INSERT INTO conta_verde.stock (product_id, quantity, store_id) VALUES
-- (1, 10, 1),
-- (2, 5, 2);


-- INSERT INTO conta_verde.purchase_orders (user_id, product_id, quantity, creation_date, payment_date, delivery_date, store_id) VALUES
-- (1, 1, 2, '2023-06-01 10:00:00', '2023-06-02 10:00:00', '2023-06-03 10:00:00', 1),
-- (2, 2, 1, '2023-06-04 12:00:00', '2023-06-05 12:00:00', '2023-06-06 12:00:00', 2);
