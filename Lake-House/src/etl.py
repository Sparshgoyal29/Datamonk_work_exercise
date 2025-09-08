import polars as pl
from deltalake.writer import write_deltalake

# Load CSV
df = pl.read_csv("../data/hourly_weather.csv")

# Parse timestamp
df = df.with_columns([
    pl.col("time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M").alias("timestamp")
])

# Extract the day for grouping
df = df.with_columns([
    df["timestamp"].dt.date().alias("day")
])

# Aggregate: average temp, humidity, count per location per day
agg = (
    df.group_by(["location_name", "day"])
      .agg([
          pl.col("temperature").mean().alias("avg_temp"),
          pl.col("humidity").mean().alias("avg_humidity"),
          pl.len().alias("hourly_count")   # changed from pl.count()
      ])
)

# Write results to Delta Lake
write_deltalake("../data/weather_delta", agg.to_pandas(), mode="overwrite")

print("ETL completed successfully ✅")

