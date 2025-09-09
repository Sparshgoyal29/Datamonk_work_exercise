from airflow.decorators import dag, task
from datetime import datetime
import etl_functions as etl

@dag(
    schedule_interval="0 0 * * *",  # daily at midnight
    start_date=datetime(2025, 5, 22),
    catchup=False,
    tags=["weather", "etl"],
)
def weather_pipeline():

    @task
    def create_task():
        etl.create_tables()

    @task
    def fetch_weather_task():
        etl.fetch_weather()

    @task
    def fetch_daily_weather_task():
        etl.fetch_daily_weather()

    @task
    def global_average_task():
        etl.global_average()

    @task
    def visualization_task():
        etl.plot_weather()

    # Task order
    create_task() >> fetch_weather_task() >> fetch_daily_weather_task() >> global_average_task() >> visualization_task()

weather_pipeline()
