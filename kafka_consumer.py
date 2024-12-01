from kafka import KafkaConsumer
import json

kafka_topic = "weather-data"
bootstrap_servers = ["localhost:9092"]

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='weather_consumer_group'
)

for message in consumer:
    print(f"Received: {message.value}")
