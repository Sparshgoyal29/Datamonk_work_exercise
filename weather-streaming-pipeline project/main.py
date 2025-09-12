import threading
from setup_environment import create_kafka_topic, spark_session
from producer.weather_producer import start_producer
import subprocess

if __name__ == "__main__":
    create_kafka_topic()
    spark = spark_session()

    # Start producer in a separate thread
    producer_thread = threading.Thread(target=start_producer)
    producer_thread.start()

    # Start Spark consumer as a separate process
    subprocess.Popen(["python3", "consumer/spark_consumer.py"])
