# gdelt_to_neo4j_pure.py
import json
import re
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from datetime import datetime

# -------------------------
# CONFIG
# -------------------------
KAFKA_TOPIC = "gdelt-events"
KAFKA_BROKER = "localhost:9092"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# -------------------------
# NORMALIZATION HELPERS
# -------------------------
def normalize_country(name: str):
    if not name:
        return None
    name = name.strip().upper()
    return name.title()

def normalize_actor(actor: str):
    if not actor:
        return None
    actor = actor.strip()
    actor = re.sub(r'\s+', ' ', actor)
    return actor.title()

def classify_tone(tone_value: float):
    if tone_value > 1:
        return "Positive"
    elif tone_value < -1:
        return "Negative"
    else:
        return "Neutral"

# -------------------------
# NEO4J FUNCTIONS
# -------------------------
class Neo4jWriter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def merge_actor(self, actor_name):
        if not actor_name:
            return None
        with self.driver.session() as session:
            return session.execute_write(
                lambda tx: tx.run(
                    "MERGE (a:Actor {name:$name}) RETURN a", name=actor_name
                ).single()
            )

    def merge_country(self, country_name):
        if not country_name:
            return None
        with self.driver.session() as session:
            return session.execute_write(
                lambda tx: tx.run(
                    "MERGE (c:Country {name:$name}) RETURN c", name=country_name
                ).single()
            )

    def create_actor_relationship(self, actor1, actor2, event_type, tone, event_desc, timestamp):
        if not actor1 or not actor2:
            return
        if actor1 == actor2:
            return  # skip self-interaction for actors
        with self.driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MATCH (a1:Actor {name:$a1}), (a2:Actor {name:$a2})
                    MERGE (a1)-[r:INTERACTED {event:$event}]->(a2)
                    SET r.type = $type, r.tone = $tone, r.timestamp = $ts
                    """,
                    a1=actor1, a2=actor2,
                    type=event_type, tone=tone,
                    event=event_desc, ts=timestamp
                )
            )

    def create_country_relationship(self, country1, country2, event_type, tone, event_desc, timestamp):
        if not country1 or not country2:
            return
        with self.driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MERGE (c1:Country {name:$c1})
                    MERGE (c2:Country {name:$c2})
                    MERGE (c1)-[r:RELATES_TO {event:$event}]->(c2)
                    SET r.type = $type, r.tone = $tone, r.timestamp = $ts
                    """,
                    c1=country1, c2=country2,
                    type=event_type, tone=tone,
                    event=event_desc, ts=timestamp
                )
            )

# -------------------------
# KAFKA CONSUMER
# -------------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

neo4j_writer = Neo4jWriter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

print("[INFO] Starting Kafka → Neo4j stream...")

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            continue

        for _partition, messages in records.items():
            for msg in messages:
                event = msg.value
                try:
                    # normalize
                    actor1 = normalize_actor(event.get("actor1_name"))
                    actor2 = normalize_actor(event.get("actor2_name"))
                    country1 = normalize_country(event.get("actor1_country"))
                    country2 = normalize_country(event.get("actor2_country"))
                    tone_value = float(event.get("tone", 0))
                    tone_cat = classify_tone(tone_value)
                    event_type = str(event.get("event_code", "Other"))
                    event_desc = str(event.get("event_root_code", "Unknown"))
                    ts = event.get("date", datetime.utcnow().isoformat())

                    # skip if mandatory data missing
                    if not actor1 or not actor2 or not country1 or not country2:
                        print(f"[WARN] Skipping event {event.get('event_id')} due to missing data")
                        continue

                    # merge nodes
                    neo4j_writer.merge_actor(actor1)
                    neo4j_writer.merge_actor(actor2)
                    neo4j_writer.merge_country(country1)
                    neo4j_writer.merge_country(country2)

                    # create relationships
                    neo4j_writer.create_actor_relationship(actor1, actor2, event_type, tone_cat, event_desc, ts)
                    neo4j_writer.create_country_relationship(country1, country2, event_type, tone_cat, event_desc, ts)

                    print(f"[INFO] Event {event.get('event_id')} written: {actor1} -> {actor2} | {country1} -> {country2}")

                except Exception as e:
                    print(f"[ERROR] Failed to write event {event.get('event_id')}: {e}")

finally:
    neo4j_writer.close()
    consumer.close()

