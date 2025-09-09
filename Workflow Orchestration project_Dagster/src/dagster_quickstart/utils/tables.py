import sqlite3

def create_tables():
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        temp REAL,
        humidity REAL,
        timestamp TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        avg_temp REAL,
        avg_humidity REAL,
        date TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        avg_temp REAL,
        avg_humidity REAL,
        date TEXT
    )
    """)

    conn.commit()
    conn.close()
