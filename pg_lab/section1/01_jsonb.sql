-- 01_jsonb.sql
CREATE TABLE IF NOT EXISTS products (
  id serial PRIMARY KEY,
  sku text,
  data jsonb
);

INSERT INTO products (sku, data) VALUES
  ('TOAST-001', '{"name": "Toaster","specs": {"power":"800W","color":"red"} }'),
  ('VAC-007',   '{"name": "Vacuum","specs": {"power":"1200W","color":"black","warranty": 24} }'),
  ('LAMP-05',   '{"name": "Desk Lamp", "specs": {"color":"white"}, "tags": ["office","lighting"] }');

-- Query for verification (this will print when script runs)
SELECT id, data->>'name' AS name, data->'specs'->>'power' AS power FROM products;
