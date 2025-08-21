# global_weather.py
import sqlite3
from datetime import datetime

DB = "data.db"

def update_global_stats(db_path=DB):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    SELECT location_name,
           AVG(max_temp) as avg_max_temp,
           AVG(min_temp) as avg_min_temp,
           AVG(avg_humidity) as avg_humidity
    FROM daily_weather
    GROUP BY location_name
    """)

    rows = cur.fetchall()
    now = datetime.utcnow().isoformat()
    for location_name, avg_max_temp, avg_min_temp, avg_humidity in rows:
        cur.execute("""
        INSERT INTO global_weather (location_name, avg_max_temp, avg_min_temp, avg_humidity, last_updated)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(location_name) DO UPDATE SET
            avg_max_temp=excluded.avg_max_temp,
            avg_min_temp=excluded.avg_min_temp,
            avg_humidity=excluded.avg_humidity,
            last_updated=excluded.last_updated
        """, (location_name, avg_max_temp, avg_min_temp, avg_humidity, now))
    conn.commit()
    conn.close()
    return len(rows)
