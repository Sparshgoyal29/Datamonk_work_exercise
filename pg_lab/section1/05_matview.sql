-- 05_matview.sql
CREATE TABLE IF NOT EXISTS customers (id serial PRIMARY KEY, name text);
CREATE TABLE IF NOT EXISTS orders (id serial PRIMARY KEY, customer_id int REFERENCES customers(id), amount numeric);

INSERT INTO customers (name) VALUES ('Cust A') ON CONFLICT DO NOTHING;
INSERT INTO customers (name) VALUES ('Cust B') ON CONFLICT DO NOTHING;
INSERT INTO orders (customer_id, amount) VALUES (1, 100), (1, 50), (2, 200);

-- Original join query
SELECT c.name, SUM(o.amount) AS total
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.name;

-- Materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_totals AS
SELECT c.id AS customer_id, c.name, SUM(o.amount) AS total
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.name;

-- Query & refresh
SELECT * FROM mv_customer_totals;
REFRESH MATERIALIZED VIEW mv_customer_totals;
SELECT * FROM mv_customer_totals;
