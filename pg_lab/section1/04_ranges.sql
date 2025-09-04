-- 04_ranges.sql
CREATE TABLE IF NOT EXISTS bookings (
  id serial PRIMARY KEY,
  guest text,
  room int,
  stay daterange
);

INSERT INTO bookings (guest, room, stay) VALUES
  ('John Doe', 101, daterange('2025-09-01','2025-09-05','[)')),
  ('Jane Roe', 102, daterange('2025-09-03','2025-09-06','[)')),
  ('Sam Hill', 101, daterange('2025-09-10','2025-09-12','[)'));

-- Overlap query
SELECT * FROM bookings WHERE stay && daterange('2025-09-04','2025-09-11','[)');

