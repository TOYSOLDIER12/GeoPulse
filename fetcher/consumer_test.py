from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'gdelt-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # read from the beginning
    enable_auto_commit=True,
    group_id='gdelt-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[INFO] Listening for messages on topic 'gdelt-events'...")
for message in consumer:
    print(message.value)

