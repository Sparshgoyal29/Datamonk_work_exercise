# create_tables.py
import sqlite3
from datetime import datetime

DB = "data.db"

def create_tables(db_path=DB):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Raw hourly weather
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        time TEXT NOT NULL,
        temperature REAL,
        condition TEXT,
        humidity REAL,
        location_name TEXT NOT NULL,
        region TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        local_time TEXT,
        UNIQUE(location_name, date, time) ON CONFLICT IGNORE
    );
    """)

    # Aggregated per day
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        location_name TEXT NOT NULL,
        max_temp REAL,
        min_temp REAL,
        avg_humidity REAL,
        UNIQUE(location_name, date) ON CONFLICT REPLACE
    );
    """)

    # Long-term / city-wide averages
    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_name TEXT NOT NULL UNIQUE,
        avg_max_temp REAL,
        avg_min_temp REAL,
        avg_humidity REAL,
        last_updated TEXT
    );
    """)

    conn.commit()
    conn.close()
    print(f"Tables created in {db_path}")

if __name__ == "__main__":
    create_tables()
