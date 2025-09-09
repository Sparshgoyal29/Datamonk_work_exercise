import sqlite3, pandas as pd

def fetch_day_average(city="Delhi"):
    conn = sqlite3.connect("data.db")
    df = pd.read_sql_query("SELECT * FROM weather WHERE city = ?", conn, params=(city,))
    conn.close()

    avg_temp = df["temp"].mean()
    avg_humidity = df["humidity"].mean()

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO daily_weather (city, avg_temp, avg_humidity, date) VALUES (?, ?, ?, date('now'))",
        (city, avg_temp, avg_humidity)
    )
    conn.commit()
    conn.close()

    return df.tail(5)  # preview for Dagster UI
