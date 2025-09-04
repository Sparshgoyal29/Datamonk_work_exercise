-- 02_citext.sql
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS users_email (
  id serial PRIMARY KEY,
  email citext
);

INSERT INTO users_email (email) VALUES ('Test@Example.com'), ('test@example.com'), ('another@domain.com');

-- Verify grouping (case-insensitive)
SELECT email, count(*) FROM users_email GROUP BY email;
