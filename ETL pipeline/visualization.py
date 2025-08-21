# visualization.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect("data.db")

df = pd.read_sql_query("SELECT location_name, avg_humidity FROM global_weather ORDER BY avg_humidity DESC LIMIT 10", conn)
conn.close()

df.plot(kind="bar", x="location_name", y="avg_humidity")
plt.ylabel("Average Humidity")
plt.title("Top 10 most humid cities (avg)")
plt.tight_layout()
plt.show()

