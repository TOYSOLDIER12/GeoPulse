import json
from kafka import KafkaProducer

class GDELTKafkaProducer:
    def __init__(self, bootstrap_servers="localhost:9092", topic="gdelt-events"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_event(self, event):
        """
        event: GDELTEvent dataclass instance
        """
        # Convert dataclass to dict
        event_dict = {
            "event_id": event.event_id,
            "date": event.date,
            "actor1_name": event.actor1_name,
            "actor1_country": event.actor1_country,
            "actor2_name": event.actor2_name,
            "actor2_country": event.actor2_country,
            "event_code": event.event_code,
            "event_root_code": event.event_root_code,
            "tone": event.tone,
            "location_country": event.location_country
        }

        # Send to Kafka
        self.producer.send(self.topic, event_dict)

    def flush(self):
        self.producer.flush()

