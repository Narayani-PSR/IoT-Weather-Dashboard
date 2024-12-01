import requests
import json
import paho.mqtt.client as mqtt
import time

# OpenWeatherMap API details
api_key = "your_api_key_here"
city = "London"
weather_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

# MQTT Broker details
mqtt_broker = "mqtt.eclipseprojects.io"
mqtt_port = 1883
mqtt_topic = "weather/data"

# Initialize MQTT client
client = mqtt.Client()
client.connect(mqtt_broker, mqtt_port)

def fetch_and_publish_weather():
    try:
        response = requests.get(weather_url)
        response.raise_for_status()
        weather_data = response.json()
        formatted_data = {
            "city": weather_data["name"],
            "temperature": weather_data["main"]["temp"],
            "humidity": weather_data["main"]["humidity"],
            "description": weather_data["weather"][0]["description"],
            "wind_speed": weather_data["wind"]["speed"]
        }
        weather_json = json.dumps(formatted_data)
        client.publish(mqtt_topic, weather_json)
        print(f"Published: {weather_json}")
    except Exception as e:
        print(f"An error occurred: {e}")

while True:
    fetch_and_publish_weather()
    time.sleep(600)
