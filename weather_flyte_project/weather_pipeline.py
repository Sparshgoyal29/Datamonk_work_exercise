"""
Weather ETL Pipeline with Flyte
"""

import os
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Tuple

import flytekit as fl
from flytekit import task, workflow, Resources
from flytekit.types.structured import StructuredDataset

# ---------------- Config ----------------
API_KEY = os.getenv("WEATHER_API_KEY", "")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DB_FILE = "/tmp/weather_data.db"

image_spec = fl.ImageSpec(
    name="weather-pipeline",
    packages=["requests", "pandas", "python-dotenv", "pyarrow", "fastparquet"],
    registry=os.getenv("FLYTE_IMAGE_REGISTRY", "localhost:30000"),
    python_version="3.11"
)

# ---------------- Database ----------------
SCHEMA = {
    "weather": """
        CREATE TABLE IF NOT EXISTS weather(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            temp REAL,
            humidity INTEGER,
            weather TEXT,
            utc_timestamp TEXT,
            local_timestamp TEXT
        );
    """,
    "daily_weather": """
        CREATE TABLE IF NOT EXISTS daily_weather(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            date TEXT,
            max_temp REAL,
            min_temp REAL,
            avg_humidity REAL
        );
    """,
}

def create_tables():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    for q in SCHEMA.values():
        cur.execute(q)
    conn.commit()
    conn.close()

def fetch_weather(city: str, lat: float, lon: float) -> Dict[str, Any]:
    params = {"lat": lat, "lon": lon, "appid": API_KEY, "units": "metric"}
    res = requests.get(BASE_URL, params=params)
    res.raise_for_status()
    return res.json()

def insert_weather(city: str, data: Dict[str, Any]):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    dt = data["dt"]
    utc_time = datetime.fromtimestamp(dt, tz=timezone.utc)
    local_time = utc_time + timedelta(seconds=data["timezone"])
    cur.execute(
        """INSERT INTO weather(city, temp, humidity, weather, utc_timestamp, local_timestamp)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (city, data["main"]["temp"], data["main"]["humidity"],
         data["weather"][0]["description"], utc_time.isoformat(), local_time.isoformat())
    )
    conn.commit()
    conn.close()

# ---------------- Flyte Tasks ----------------
@task(container_image=image_spec)
def setup_database() -> str:
    create_tables()
    return "✅ Database ready"

@task(container_image=image_spec, requests=Resources(cpu="1", mem="1Gi"))
def fetch_weather_data(_: str) -> Tuple[str, StructuredDataset]:
    cities = {"Delhi": (28.6, 77.2), "Bhopal": (23.3, 77.4), "Bengaluru": (12.9, 77.6)}
    records = []
    for city, (lat, lon) in cities.items():
        data = fetch_weather(city, lat, lon)
        insert_weather(city, data)
        records.append({"city": city, "temp": data["main"]["temp"], "humidity": data["main"]["humidity"]})
    return "✅ Weather fetched", StructuredDataset(dataframe=pd.DataFrame(records))

@task(container_image=image_spec)
def aggregate_daily(_: str) -> Tuple[str, StructuredDataset]:
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT city, DATE(local_timestamp) as date, MAX(temp) as max_temp, MIN(temp) as min_temp, AVG(humidity) as avg_humidity FROM weather GROUP BY city, date", conn)
    conn.close()
    return f"✅ Daily aggregated {len(df)} rows", StructuredDataset(dataframe=df)

@workflow
def weather_etl_pipeline() -> Tuple[str, StructuredDataset, StructuredDataset]:
    db = setup_database()
    status, current = fetch_weather_data(db)
    daily_status, daily = aggregate_daily(status)
    return status, current, daily
