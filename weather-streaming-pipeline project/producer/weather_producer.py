import requests, json, time, os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv(dotenv_path='config/.env')
API_KEY = os.getenv('OPENWEATHER_API_KEY')
BROKER = os.getenv('KAFKA_BROKER')
TOPIC = os.getenv('KAFKA_TOPIC')

CITIES = ["New Delhi", "Mumbai", "Bengaluru"]

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            "timestamp": data["dt"],
            "city": data["name"],
            "country": data["sys"]["country"],
            "temperature": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "description": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "wind_direction": data["wind"]["deg"],
            "cloudiness": data["clouds"]["all"],
            "visibility": data["visibility"],
            "latitude": data["coord"]["lat"],
            "longitude": data["coord"]["lon"]
        }
    return {}

def start_producer():
    while True:
        for city in CITIES:
            weather_data = fetch_weather(city)
            producer.send(TOPIC, weather_data)
            print(f"Sent data for {city}: {weather_data}")
        time.sleep(120)  # 2 minutes

if __name__ == "__main__":
    start_producer()

