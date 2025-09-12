from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='config/.env')

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

def create_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{KAFKA_TOPIC}' created successfully.")
    except Exception as e:
        print(f"Topic may already exist: {e}")

def spark_session():
    spark = SparkSession.builder \
        .appName("WeatherStreamingApp") \
        .master("local[*]") \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    create_kafka_topic()
    spark = spark_session()
    print("Spark session initialized")

