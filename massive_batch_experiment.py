import argparse
import json
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from fetcher.gdelt_fetcher import fetch_gdelt_file, extract_csv
from fetcher.gdelt_parser import parse_gdelt_csv
from pipeline_common import Neo4jWriter, normalize_gdelt_event, write_event_to_neo4j


DEFAULT_KAFKA_BROKER = "localhost:9092"
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"


def round_down_to_15(dt: datetime) -> datetime:
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def parse_iso_timestamp(value: str | None) -> datetime:
    if value:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return round_down_to_15(parsed)
    return round_down_to_15(datetime.now(timezone.utc) - timedelta(minutes=15))


def build_timestamps(end_timestamp: datetime, slices: int) -> list[datetime]:
    if slices <= 0:
        raise ValueError("--slices must be > 0")
    timestamps = [end_timestamp - timedelta(minutes=15 * offset) for offset in reversed(range(slices))]
    return timestamps


def event_to_dict(event) -> dict:
    if isinstance(event, dict):
        return event
    return asdict(event)


def prepare_dataset(dataset_file: Path, timestamps: list[datetime]) -> dict:
    fetched_slices = 0
    missing_slices = 0
    total_events = 0

    fetch_seconds = 0.0
    parse_seconds = 0.0
    normalize_seconds = 0.0

    with dataset_file.open("w", encoding="utf-8") as out:
        for ts in timestamps:
            t_fetch = time.perf_counter()
            zip_bytes = fetch_gdelt_file(ts)
            fetch_seconds += time.perf_counter() - t_fetch

            if zip_bytes is None:
                missing_slices += 1
                continue

            fetched_slices += 1

            t_parse = time.perf_counter()
            csv_file = extract_csv(zip_bytes)
            parsed_events = list(parse_gdelt_csv(csv_file))
            parse_seconds += time.perf_counter() - t_parse

            for event in parsed_events:
                t_norm = time.perf_counter()
                normalized = normalize_gdelt_event(event)
                normalize_seconds += time.perf_counter() - t_norm

                out.write(json.dumps(event_to_dict(normalized), ensure_ascii=True) + "\n")
                total_events += 1

    return {
        "fetched_slices": fetched_slices,
        "missing_slices": missing_slices,
        "total_events": total_events,
        "fetch_seconds": round(fetch_seconds, 3),
        "parse_seconds": round(parse_seconds, 3),
        "normalize_seconds": round(normalize_seconds, 3),
    }


def run_sync(dataset_file: Path, neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> dict:
    writer = Neo4jWriter(neo4j_uri, neo4j_user, neo4j_password)
    processed = 0
    skipped = 0

    t_write = time.perf_counter()
    try:
        with dataset_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                event = json.loads(line)
                try:
                    if write_event_to_neo4j(event, writer):
                        processed += 1
                    else:
                        skipped += 1
                except Exception:
                    skipped += 1
    finally:
        writer.close()
    write_seconds = time.perf_counter() - t_write

    return {
        "processed": processed,
        "skipped": skipped,
        "write_seconds": round(write_seconds, 3),
        "events_per_second": round(processed / write_seconds, 3) if write_seconds > 0 else 0.0,
    }


def create_topic(admin: KafkaAdminClient, topic: str, partitions: int = 3):
    admin.create_topics(
        new_topics=[NewTopic(name=topic, num_partitions=partitions, replication_factor=1)],
        validate_only=False,
    )


def delete_topic(admin: KafkaAdminClient, topic: str):
    try:
        admin.delete_topics([topic])
    except Exception:
        pass


def run_kafka(dataset_file: Path, total_events: int, kafka_broker: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> dict:
    topic = f"gdelt-events-bench-{uuid.uuid4().hex[:8]}"

    admin = KafkaAdminClient(bootstrap_servers=kafka_broker, client_id="geopulse-bench-admin")
    create_topic(admin, topic)

    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    t_produce = time.perf_counter()
    produced = 0
    with dataset_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            producer.send(topic, event)
            produced += 1
    producer.flush()
    produce_seconds = time.perf_counter() - t_produce

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_broker],
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )

    writer = Neo4jWriter(neo4j_uri, neo4j_user, neo4j_password)
    consumed = 0
    processed = 0
    skipped = 0

    t_consume = time.perf_counter()
    try:
        for message in consumer:
            event = message.value
            consumed += 1
            try:
                if write_event_to_neo4j(event, writer):
                    processed += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1

            if consumed >= total_events:
                break
    finally:
        consume_seconds = time.perf_counter() - t_consume
        writer.close()
        consumer.close()
        producer.close()
        delete_topic(admin, topic)
        admin.close()

    return {
        "topic": topic,
        "produced": produced,
        "consumed": consumed,
        "processed": processed,
        "skipped": skipped,
        "produce_seconds": round(produce_seconds, 3),
        "consume_write_seconds": round(consume_seconds, 3),
        "end_to_end_seconds": round(produce_seconds + consume_seconds, 3),
        "events_per_second": round(processed / (produce_seconds + consume_seconds), 3)
        if (produce_seconds + consume_seconds) > 0
        else 0.0,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Massive batch benchmark: synchronous vs Kafka on the same GDELT dataset.")
    parser.add_argument("--mode", choices=["sync", "kafka", "both"], default="both")
    parser.add_argument("--end-timestamp", default=None, help="UTC ISO timestamp (default: now-15m), e.g. 2026-04-14T12:15")
    parser.add_argument("--slices", type=int, default=32, help="Number of 15-minute slices to include")
    parser.add_argument("--dataset-file", default="massive_batch_dataset.jsonl")
    parser.add_argument("--kafka-broker", default=DEFAULT_KAFKA_BROKER)
    parser.add_argument("--neo4j-uri", default=DEFAULT_NEO4J_URI)
    parser.add_argument("--neo4j-user", default=DEFAULT_NEO4J_USER)
    parser.add_argument("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
    parser.add_argument("--output-json", default="massive_batch_results.json")
    return parser.parse_args()


def main():
    args = parse_args()
    end_timestamp = parse_iso_timestamp(args.end_timestamp)
    timestamps = build_timestamps(end_timestamp, args.slices)
    dataset_file = Path(args.dataset_file)

    print(f"[INFO] Building dataset for {len(timestamps)} slices ending at {end_timestamp.isoformat()}")
    prep_start = time.perf_counter()
    prep = prepare_dataset(dataset_file, timestamps)
    prep_total_seconds = time.perf_counter() - prep_start

    if prep["total_events"] == 0:
        print("[WARN] Dataset is empty; choose a different timestamp window.")
        return

    results = {
        "window": {
            "end_timestamp": end_timestamp.isoformat(),
            "slices": args.slices,
        },
        "dataset": {
            **prep,
            "dataset_file": str(dataset_file),
            "prepare_total_seconds": round(prep_total_seconds, 3),
        },
    }

    if args.mode in ("sync", "both"):
        print("[INFO] Running synchronous benchmark...")
        sync_metrics = run_sync(
            dataset_file=dataset_file,
            neo4j_uri=args.neo4j_uri,
            neo4j_user=args.neo4j_user,
            neo4j_password=args.neo4j_password,
        )
        results["sync"] = sync_metrics
        print(f"[INFO] Sync done: {sync_metrics}")

    if args.mode in ("kafka", "both"):
        print("[INFO] Running Kafka benchmark...")
        kafka_metrics = run_kafka(
            dataset_file=dataset_file,
            total_events=prep["total_events"],
            kafka_broker=args.kafka_broker,
            neo4j_uri=args.neo4j_uri,
            neo4j_user=args.neo4j_user,
            neo4j_password=args.neo4j_password,
        )
        results["kafka"] = kafka_metrics
        print(f"[INFO] Kafka done: {kafka_metrics}")

    if "sync" in results and "kafka" in results:
        sync_time = results["sync"]["write_seconds"]
        kafka_time = results["kafka"]["end_to_end_seconds"]
        ratio = (kafka_time / sync_time) if sync_time > 0 else None
        results["comparison"] = {
            "sync_write_seconds": sync_time,
            "kafka_end_to_end_seconds": kafka_time,
            "kafka_to_sync_time_ratio": round(ratio, 3) if ratio is not None else None,
        }
        print(f"[INFO] Comparison: {results['comparison']}")

    Path(args.output_json).write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"[INFO] Results saved to {args.output_json}")


if __name__ == "__main__":
    main()
