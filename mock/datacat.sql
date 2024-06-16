-- datacat.sql

-- Cria o esquema para DataCat
CREATE SCHEMA IF NOT EXISTS datacat;

-- Cria a tabela de logs de DataCat
CREATE TABLE IF NOT EXISTS datacat.logs (
    id SERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    type VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    extra_1 TEXT,
    extra_2 TEXT
);

-- Insere registros de log
INSERT INTO datacat.logs (timestamp, type, content, extra_1, extra_2) VALUES
(1622470422000, 'ERROR', 'Failed to load resource', 'User-Agent: Mozilla/5.0', 'Status: 404'),
(1622470423000, 'INFO', 'User logged in', 'User-Agent: Mozilla/5.0', 'Status: 200');
