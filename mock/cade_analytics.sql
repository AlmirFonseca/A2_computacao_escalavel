-- cade_analytics.sql

-- Cria o esquema para CadeAnalytics
CREATE SCHEMA IF NOT EXISTS cade_analytics;

-- Cria a tabela de logs de CadeAnalytics
CREATE TABLE IF NOT EXISTS cade_analytics.logs (
    id SERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    type VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    extra_1 TEXT,
    extra_2 TEXT
);

-- Insere registros de log
INSERT INTO cade_analytics.logs (timestamp, type, content, extra_1, extra_2) VALUES
(1622470422000, 'REQUEST', 'GET /index.html', 'User-Agent: Mozilla/5.0', 'Status: 200'),
(1622470423000, 'REQUEST', 'POST /login', 'User-Agent: Mozilla/5.0', 'Status: 302');
