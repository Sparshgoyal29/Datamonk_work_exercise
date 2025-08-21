# daily_weather.py
import sqlite3

DB = "data.db"

def compute_daily_for_date(target_date: str, db_path=DB):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # group by location_name for that date
    cur.execute("""
    SELECT location_name,
           MAX(temperature) as max_temp,
           MIN(temperature) as min_temp,
           AVG(humidity) as avg_humidity
    FROM weather
    WHERE date = ?
    GROUP BY location_name
    """, (target_date,))

    rows = cur.fetchall()
    # insert or replace into daily_weather table
    for location_name, max_temp, min_temp, avg_humidity in rows:
        cur.execute("""
        INSERT INTO daily_weather (date, location_name, max_temp, min_temp, avg_humidity)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(location_name, date) DO UPDATE SET
            max_temp=excluded.max_temp,
            min_temp=excluded.min_temp,
            avg_humidity=excluded.avg_humidity
        """, (target_date, location_name, max_temp, min_temp, avg_humidity))
    conn.commit()
    conn.close()
    return len(rows)
