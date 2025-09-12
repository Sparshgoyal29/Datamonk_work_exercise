from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='config/.env')
BROKER = os.getenv('KAFKA_BROKER')
TOPIC = os.getenv('KAFKA_TOPIC')

spark = SparkSession.builder \
    .appName("WeatherSparkConsumer") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType() \
    .add("timestamp", IntegerType()) \
    .add("city", StringType()) \
    .add("country", StringType()) \
    .add("temperature", FloatType()) \
    .add("feels_like", FloatType()) \
    .add("humidity", FloatType()) \
    .add("pressure", FloatType()) \
    .add("description", StringType()) \
    .add("wind_speed", FloatType()) \
    .add("wind_direction", FloatType()) \
    .add("cloudiness", FloatType()) \
    .add("visibility", FloatType()) \
    .add("latitude", FloatType()) \
    .add("longitude", FloatType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

weather_df = df.selectExpr("CAST(value AS STRING) as json")
weather_df = weather_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

query = weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
