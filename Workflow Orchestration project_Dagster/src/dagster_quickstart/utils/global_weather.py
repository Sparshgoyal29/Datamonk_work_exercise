import sqlite3, pandas as pd

def fetch_global_average():
    conn = sqlite3.connect("data.db")
    df = pd.read_sql_query("SELECT * FROM daily_weather", conn)
    conn.close()

    avg_temp = df["avg_temp"].mean()
    avg_humidity = df["avg_humidity"].mean()

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO global_weather (avg_temp, avg_humidity, date) VALUES (?, ?, date('now'))",
        (avg_temp, avg_humidity)
    )
    conn.commit()
    conn.close()

    return df.tail(5)
