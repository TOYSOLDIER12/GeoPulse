import argparse
import json
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_common import Neo4jWriter, write_event_to_neo4j


DEFAULT_KAFKA_TOPIC = "gdelt-events"
DEFAULT_KAFKA_BROKER = "localhost:9092"
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"


def build_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_servers],
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def run_consumer(topic: str, bootstrap_servers: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str, max_events: int | None = None):
    consumer = build_consumer(topic, bootstrap_servers)
    neo4j_writer = Neo4jWriter(neo4j_uri, neo4j_user, neo4j_password)

    processed_events = 0
    skipped_events = 0
    started_at = time.perf_counter()

    print("[INFO] Starting Kafka → Neo4j stream...")

    try:
        for message in consumer:
            event = message.value
            try:
                if write_event_to_neo4j(event, neo4j_writer):
                    processed_events += 1
                    print(f"[INFO] Event {event.get('event_id')} written")
                else:
                    skipped_events += 1
                    print(f"[WARN] Skipping event {event.get('event_id')} due to missing data")

                if max_events is not None and processed_events >= max_events:
                    break

            except Exception as error:
                skipped_events += 1
                print(f"[ERROR] Failed to write event {event.get('event_id')}: {error}")

    finally:
        elapsed_seconds = time.perf_counter() - started_at
        print(
            f"[INFO] Consumer summary: processed={processed_events}, skipped={skipped_events}, elapsed_seconds={elapsed_seconds:.2f}"
        )
        neo4j_writer.close()
        consumer.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Consume GDELT events from Kafka and write them to Neo4j.")
    parser.add_argument("--topic", default=DEFAULT_KAFKA_TOPIC)
    parser.add_argument("--bootstrap-server", default=DEFAULT_KAFKA_BROKER)
    parser.add_argument("--neo4j-uri", default=DEFAULT_NEO4J_URI)
    parser.add_argument("--neo4j-user", default=DEFAULT_NEO4J_USER)
    parser.add_argument("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
    parser.add_argument("--max-events", type=int, default=None, help="Stop after processing this many events.")
    return parser.parse_args()


def main():
    args = parse_args()
    run_consumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_server,
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        max_events=args.max_events,
    )


if __name__ == "__main__":
    main()