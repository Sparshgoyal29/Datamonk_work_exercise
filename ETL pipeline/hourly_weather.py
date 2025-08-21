# hourly_weather.py
import sqlite3
from typing import List, Dict

DB = "data.db"

INSERT_SQL = """
INSERT OR IGNORE INTO weather
(date, time, temperature, condition, humidity, location_name, region, country, latitude, longitude, local_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def insert_weather_data(rows: List[Dict], db_path=DB):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for r in rows:
        cur.execute(INSERT_SQL, (
            r["date"], r["time"], r["temperature"], r["condition"],
            r["humidity"], r["location_name"], r["region"], r["country"],
            r["latitude"], r["longitude"], r["local_time"]
        ))
    conn.commit()
    conn.close()
    return len(rows)
