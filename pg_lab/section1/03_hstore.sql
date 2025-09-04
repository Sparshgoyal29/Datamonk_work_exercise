-- 03_hstore.sql
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE IF NOT EXISTS user_prefs (
  id serial PRIMARY KEY,
  username text,
  prefs hstore
);

INSERT INTO user_prefs (username, prefs) VALUES
  ('alice', '"theme"=>"dark", "language"=>"en"'::hstore),
  ('bob',   '"theme"=>"light", "language"=>"fr", "emails"=>"no"'::hstore);

-- Query a specific key
SELECT username, prefs->'theme' AS theme FROM user_prefs;
