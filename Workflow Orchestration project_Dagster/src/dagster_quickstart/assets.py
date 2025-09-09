from dagster import asset, MaterializeResult, MetadataValue
from .utils.tables import create_tables
from .utils.weather_utils import fetch_and_store_weather
from .utils.daily_weather import fetch_day_average
from .utils.global_weather import fetch_global_average

@asset
def setup_database():
    create_tables()
    return MaterializeResult(metadata={"status": "tables created"})

@asset(deps=[setup_database])
def fetch_weather():
    data = fetch_and_store_weather("Delhi")
    return MaterializeResult(metadata={"city": data["name"], "temp": data["main"]["temp"]})

@asset(deps=[fetch_weather])
def fetch_daily_weather():
    preview = fetch_day_average("Delhi")
    return MaterializeResult(metadata={"preview": MetadataValue.md(preview.to_markdown())})

@asset(deps=[fetch_daily_weather])
def global_weather():
    preview = fetch_global_average()
    return MaterializeResult(metadata={"preview": MetadataValue.md(preview.to_markdown())})
