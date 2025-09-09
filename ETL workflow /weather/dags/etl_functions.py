import sqlite3
import requests
import datetime
import matplotlib.pyplot as plt

DB_NAME = "weather.db"

# Step 1: Create tables
def create_tables():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        city TEXT,
        date TEXT,
        temp REAL,
        humidity REAL
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_weather (
        city TEXT,
        date TEXT,
        avg_temp REAL,
        avg_humidity REAL
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_weather (
        date TEXT,
        avg_temp REAL,
        avg_humidity REAL
    )""")
    conn.commit()
    conn.close()

# Step 2: Fetch hourly weather
def fetch_weather(city="Dispur", date="2025-05-22"):
    url = f"https://api.open-meteo.com/v1/forecast?latitude=26.1445&longitude=91.7362&hourly=temperature_2m,relative_humidity_2m&start_date={date}&end_date={date}"
    r = requests.get(url)
    data = r.json()
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for t, temp in zip(data["hourly"]["time"], data["hourly"]["temperature_2m"]):
        humidity = data["hourly"]["relative_humidity_2m"][0]  # simplified
        cur.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", (city, date, temp, humidity))
    conn.commit()
    conn.close()

# Step 3: Compute daily average
def fetch_daily_weather():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO daily_weather
    SELECT city, date, AVG(temp), AVG(humidity)
    FROM weather
    GROUP BY city, date
    """)
    conn.commit()
    conn.close()

# Step 4: Compute global average
def global_average():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO global_weather
    SELECT date, AVG(avg_temp), AVG(avg_humidity)
    FROM daily_weather
    GROUP BY date
    """)
    conn.commit()
    conn.close()

# Step 5: Visualization
def plot_weather():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT date, avg_temp FROM daily_weather ORDER BY date")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No data found in daily_weather table.")
        return

    dates = [r[0] for r in rows]
    temps = [r[1] for r in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(dates, temps, marker="o", linestyle="-", color="blue")
    plt.title("Daily Average Temperature")
    plt.xlabel("Date")
    plt.ylabel("Avg Temp (°C)")
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig("daily_weather_plot.png")
    print("Plot saved as daily_weather_plot.png")
