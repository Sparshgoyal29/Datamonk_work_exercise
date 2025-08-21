# weather_utils.py
import os
import requests
from dotenv import load_dotenv
from datetime import datetime, date as datecls

load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")
BASE = "https://api.weatherapi.com/v1/history.json"  # from WeatherAPI docs

def validate_date(datestr):
    # expect YYYY-MM-DD and not in the future
    dt = datetime.strptime(datestr, "%Y-%m-%d").date()
    if dt > datecls.today():
        raise ValueError("Date cannot be in the future")
    return datestr

def fetch_hourly_weather(city: str, dt: str):
    """
    Returns a list of hourly records (dicts) for the given city and date (YYYY-MM-DD).
    Each dict will contain keys: date, time, temperature, condition, humidity,
    location_name, region, country, latitude, longitude, local_time
    """
    if not API_KEY:
        raise RuntimeError("WEATHER_API_KEY not found in environment (.env)")

    dt = validate_date(dt)
    params = {"key": API_KEY, "q": city, "dt": dt}
    r = requests.get(BASE, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    # location info
    loc = data.get("location", {})
    forecastday = data.get("forecast", {}).get("forecastday", [])
    if not forecastday:
        return []

    hours = forecastday[0].get("hour", [])
    out = []
    for h in hours:
        # h contains time, temp_c, condition{ text }, humidity
        record = {
            "date": h["time"].split(" ")[0],
            "time": h["time"].split(" ")[1],
            "temperature": h.get("temp_c"),
            "condition": h.get("condition", {}).get("text"),
            "humidity": h.get("humidity"),
            "location_name": loc.get("name"),
            "region": loc.get("region"),
            "country": loc.get("country"),
            "latitude": loc.get("lat"),
            "longitude": loc.get("lon"),
            "local_time": loc.get("localtime"),
        }
        out.append(record)
    return out

