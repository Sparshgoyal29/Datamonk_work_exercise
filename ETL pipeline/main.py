# main.py
import argparse
import csv
from create_tables import create_tables
from weather_utils import fetch_hourly_weather
from hourly_weather import insert_weather_data
from daily_weather import compute_daily_for_date
from global_weather import update_global_stats

DEFAULT_CSV = "india_cities.csv"

def load_cities(csv_path):
    cities = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # expecting header: city
        for row in reader:
            name = row.get("city") or row.get("City") or row.get("location")
            if name:
                cities.append(name.strip())
    return cities

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date to fetch in YYYY-MM-DD (not future)")
    parser.add_argument("--cities", default=DEFAULT_CSV, help="CSV file with column 'city'")
    args = parser.parse_args()

    # setup DB if needed
    create_tables()

    cities = load_cities(args.cities)
    print(f"Processing {len(cities)} cities for date {args.date}")

    for city in cities:
        try:
            print(f"Fetching {city} ...")
            hourly = fetch_hourly_weather(city, args.date)
            inserted = insert_weather_data(hourly)
            print(f"Inserted {inserted} hourly rows for {city}")
        except Exception as e:
            print(f"Error for {city}: {e}")

    # compute daily for that date
    rows = compute_daily_for_date(args.date)
    print(f"Daily aggregates written for {rows} locations")

    # update global / long-term aggregates
    g = update_global_stats()
    print(f"Global stats updated for {g} locations")

if __name__ == "__main__":
    main()
