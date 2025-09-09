import requests, os, sqlite3
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")

def fetch_and_store_weather(city="Delhi"):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    r = requests.get(url).json()

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO weather (city, temp, humidity, timestamp) VALUES (?, ?, ?, datetime('now'))",
        (city, r["main"]["temp"], r["main"]["humidity"])
    )
    conn.commit()
    conn.close()

    return r
