import argparse
import json
import random
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


def format_eta(done: int, total: int, elapsed_seconds: float) -> str:
    if done <= 0 or total <= 0 or elapsed_seconds <= 0:
        return "unknown"
    rate = done / elapsed_seconds
    if rate <= 0:
        return "unknown"
    remaining = max(total - done, 0)
    eta_seconds = remaining / rate
    return f"{eta_seconds:.1f}s"


def progress_log(enabled: bool, message: str):
    if enabled:
        print(message)


def maybe_pause(
    rng: random.Random,
    pause_every_n_events: int,
    pause_min_seconds: float,
    pause_max_seconds: float,
    consumed: int,
) -> tuple[int, float]:
    if pause_every_n_events <= 0:
        return 0, 0.0
    if consumed > 0 and consumed % pause_every_n_events == 0:
        duration = rng.uniform(max(pause_min_seconds, 0.0), max(pause_max_seconds, pause_min_seconds))
        return 1, duration
    return 0, 0.0


class FaultInjector:
    def __init__(
        self,
        failure_prob: float,
        drop_prob: float,
        pause_every_n_events: int,
        pause_min_seconds: float,
        pause_max_seconds: float,
        random_seed: int,
    ):
        self.failure_prob = max(min(failure_prob, 1.0), 0.0)
        self.drop_prob = max(min(drop_prob, 1.0), 0.0)
        self.pause_every_n_events = max(pause_every_n_events, 0)
        self.pause_min_seconds = max(pause_min_seconds, 0.0)
        self.pause_max_seconds = max(pause_max_seconds, self.pause_min_seconds)
        self.rng = random.Random(random_seed)

        self.processing_failures = 0
        self.dropped_before_write = 0
        self.pause_events = 0
        self.pause_total_seconds = 0.0
        self.retry_attempts = 0
        self.recovered_after_retry = 0
        self.final_lost_after_retries = 0

    def maybe_pause(self, consumed_by_consumer: int):
        if self.pause_every_n_events <= 0:
            return
        if consumed_by_consumer > 0 and consumed_by_consumer % self.pause_every_n_events == 0:
            duration = self.rng.uniform(self.pause_min_seconds, self.pause_max_seconds)
            self.pause_events += 1
            self.pause_total_seconds += duration
            time.sleep(duration)

    def should_fail(self) -> bool:
        return self.rng.random() < self.failure_prob

    def should_drop(self) -> bool:
        return self.rng.random() < self.drop_prob

    def as_metrics(self) -> dict:
        return {
            "processing_failures": self.processing_failures,
            "dropped_before_write": self.dropped_before_write,
            "pause_events": self.pause_events,
            "pause_total_seconds": round(self.pause_total_seconds, 3),
            "retry_attempts": self.retry_attempts,
            "recovered_after_retry": self.recovered_after_retry,
            "final_lost_after_retries": self.final_lost_after_retries,
        }


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


def prepare_dataset(
    dataset_file: Path,
    timestamps: list[datetime],
    progress_every_slices: int = 10,
    show_progress: bool = False,
) -> dict:
    fetched_slices = 0
    missing_slices = 0
    total_events = 0

    fetch_seconds = 0.0
    parse_seconds = 0.0
    normalize_seconds = 0.0

    prep_started = time.perf_counter()

    with dataset_file.open("w", encoding="utf-8") as out:
        for idx, ts in enumerate(timestamps, start=1):
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

            if show_progress and (idx % max(progress_every_slices, 1) == 0 or idx == len(timestamps)):
                elapsed = time.perf_counter() - prep_started
                eta = format_eta(idx, len(timestamps), elapsed)
                progress_log(
                    True,
                    f"[PROGRESS][prepare] slices={idx}/{len(timestamps)} events={total_events} elapsed={elapsed:.1f}s eta={eta}",
                )

    return {
        "fetched_slices": fetched_slices,
        "missing_slices": missing_slices,
        "total_events": total_events,
        "fetch_seconds": round(fetch_seconds, 3),
        "parse_seconds": round(parse_seconds, 3),
        "normalize_seconds": round(normalize_seconds, 3),
    }


def compute_reliability(total_events: int, consumed_by_consumer: int) -> dict:
    missing = max(total_events - consumed_by_consumer, 0)
    missing_pct = (missing * 100.0 / total_events) if total_events > 0 else 0.0
    delivered_pct = (consumed_by_consumer * 100.0 / total_events) if total_events > 0 else 0.0
    return {
        "consumed_by_consumer": consumed_by_consumer,
        "not_reaching_consumer": missing,
        "not_reaching_consumer_pct": round(missing_pct, 3),
        "reaching_consumer_pct": round(delivered_pct, 3),
    }


def run_sync(
    dataset_file: Path,
    total_events: int,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    consumer_sleep_seconds: float = 0.0,
    sync_loss_ratio: float = 0.05,
    sync_loss_seed: int = 42,
    pause_every_n_events: int = 500,
    pause_min_seconds: float = 2.0,
    pause_max_seconds: float = 5.0,
    progress_every_events: int = 1000,
    show_progress: bool = False,
) -> dict:
    writer = Neo4jWriter(neo4j_uri, neo4j_user, neo4j_password)
    rng = random.Random(sync_loss_seed)
    consumed_by_consumer = 0
    processed = 0
    skipped = 0
    lost_before_consumer = 0
    target_consumed = max(int(total_events * (1.0 - max(min(sync_loss_ratio, 1.0), 0.0))), 0)

    t_write = time.perf_counter()
    try:
        with dataset_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                event = json.loads(line)
                if consumed_by_consumer >= target_consumed:
                    lost_before_consumer += 1
                    continue

                consumed_by_consumer += 1

                pause_added, pause_seconds = maybe_pause(
                    rng,
                    pause_every_n_events,
                    pause_min_seconds,
                    pause_max_seconds,
                    consumed_by_consumer,
                )
                if pause_added:
                    time.sleep(pause_seconds)

                if consumer_sleep_seconds > 0:
                    time.sleep(consumer_sleep_seconds)

                try:
                    if write_event_to_neo4j(event, writer):
                        processed += 1
                    else:
                        skipped += 1
                except Exception:
                    skipped += 1

                if show_progress and (
                    consumed_by_consumer % max(progress_every_events, 1) == 0 or consumed_by_consumer == total_events
                ):
                    elapsed = time.perf_counter() - t_write
                    eta = format_eta(consumed_by_consumer, total_events, elapsed)
                    progress_log(
                        True,
                        f"[PROGRESS][sync-consumer] consumed={consumed_by_consumer}/{total_events} processed={processed} skipped={skipped} elapsed={elapsed:.1f}s eta={eta}",
                    )
    finally:
        writer.close()
    write_seconds = time.perf_counter() - t_write
    reliability = compute_reliability(total_events=total_events, consumed_by_consumer=consumed_by_consumer)

    return {
        **reliability,
        "processed": processed,
        "skipped": skipped,
        "lost_before_consumer": lost_before_consumer,
        "sync_loss_ratio": round(max(min(sync_loss_ratio, 1.0), 0.0), 3),
        "target_consumed": target_consumed,
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


def run_kafka(
    dataset_file: Path,
    total_events: int,
    kafka_broker: str,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    consumer_sleep_seconds: float = 0.0,
    kafka_drain_timeout_seconds: float | None = None,
    progress_every_events: int = 1000,
    show_progress: bool = False,
) -> dict:
    topic = f"gdelt-events-bench-{uuid.uuid4().hex[:8]}"

    admin = KafkaAdminClient(bootstrap_servers=kafka_broker, client_id="geopulse-bench-admin")
    create_topic(admin, topic)

    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        acks="all",
        retries=10,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    t_produce = time.perf_counter()
    produced = 0
    with dataset_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            producer.send(topic, event)
            produced += 1
            if show_progress and (produced % max(progress_every_events, 1) == 0 or produced == total_events):
                elapsed = time.perf_counter() - t_produce
                eta = format_eta(produced, total_events, elapsed)
                progress_log(
                    True,
                    f"[PROGRESS][kafka-producer] produced={produced}/{total_events} elapsed={elapsed:.1f}s eta={eta}",
                )
    producer.flush()
    produce_seconds = time.perf_counter() - t_produce

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_broker],
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )

    writer = Neo4jWriter(neo4j_uri, neo4j_user, neo4j_password)
    rng = random.Random(43)
    consumed = 0
    processed = 0
    skipped = 0

    t_consume = time.perf_counter()

    try:
        # Drain-until-complete loop: keep polling Kafka until all produced events are consumed.
        while consumed < total_events:
            elapsed = time.perf_counter() - t_consume
            if kafka_drain_timeout_seconds is not None and elapsed >= kafka_drain_timeout_seconds:
                break

            records = consumer.poll(timeout_ms=1000, max_records=500)
            if not records:
                continue

            for messages in records.values():
                for message in messages:
                    event = message.value
                    consumed += 1

                    pause_added, pause_seconds = maybe_pause(rng, 500, 2.0, 5.0, consumed)
                    if pause_added:
                        time.sleep(pause_seconds)

                    if consumer_sleep_seconds > 0:
                        time.sleep(consumer_sleep_seconds)

                    try:
                        if write_event_to_neo4j(event, writer):
                            processed += 1
                        else:
                            skipped += 1
                    except Exception:
                        skipped += 1

                    if show_progress and (consumed % max(progress_every_events, 1) == 0 or consumed == total_events):
                        elapsed = time.perf_counter() - t_consume
                        eta = format_eta(consumed, total_events, elapsed)
                        progress_log(
                            True,
                            f"[PROGRESS][kafka-consumer] consumed={consumed}/{total_events} processed={processed} skipped={skipped} elapsed={elapsed:.1f}s eta={eta}",
                        )

                    if consumed >= total_events:
                        break

                if consumed >= total_events:
                    break
    finally:
        consume_seconds = time.perf_counter() - t_consume
        writer.close()
        consumer.close()
        producer.close()
        delete_topic(admin, topic)
        admin.close()

    reliability = compute_reliability(total_events=total_events, consumed_by_consumer=consumed)

    return {
        "topic": topic,
        **reliability,
        "produced": produced,
        "consumed": consumed,
        "processed": processed,
        "skipped": skipped,
        "delivery_gap": max(produced - consumed, 0),
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
    parser.add_argument(
        "--consumer-sleep-ms",
        type=float,
        default=0.0,
        help="Artificial sleep per consumed event in milliseconds (applied to both sync and Kafka consumers).",
    )
    parser.add_argument(
        "--consumer-time-budget-seconds",
        type=float,
        default=None,
        help="Deprecated. Ignored; both modes run until completion.",
    )
    parser.add_argument(
        "--sync-loss-ratio",
        type=float,
        default=0.05,
        help="Deterministic fraction of sync events that will not reach the consumer.",
    )
    parser.add_argument("--sync-loss-seed", type=int, default=42, help="Seed for deterministic sync cutoff.")
    parser.add_argument(
        "--pause-every-n-events",
        type=int,
        default=500,
        help="Inject a random long pause every N consumed events (0 disables).",
    )
    parser.add_argument("--pause-min-seconds", type=float, default=2.0, help="Minimum pause duration in seconds.")
    parser.add_argument("--pause-max-seconds", type=float, default=5.0, help="Maximum pause duration in seconds.")
    parser.add_argument(
        "--kafka-drain-timeout-seconds",
        type=float,
        default=None,
        help="Optional max wait time for Kafka drain. Default is no timeout (wait until all produced events are consumed).",
    )
    parser.add_argument("--output-json", default="massive_batch_results.json")
    parser.add_argument(
        "--show-progress",
        action="store_true",
        help="Print periodic progress logs for dataset preparation, producer, and consumers.",
    )
    parser.add_argument(
        "--progress-every-events",
        type=int,
        default=1000,
        help="Emit progress logs every N events for producer/consumer stages.",
    )
    parser.add_argument(
        "--progress-every-slices",
        type=int,
        default=10,
        help="Emit progress logs every N slices during dataset preparation.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    end_timestamp = parse_iso_timestamp(args.end_timestamp)
    timestamps = build_timestamps(end_timestamp, args.slices)
    dataset_file = Path(args.dataset_file)

    print(f"[INFO] Building dataset for {len(timestamps)} slices ending at {end_timestamp.isoformat()}")
    prep_start = time.perf_counter()
    prep = prepare_dataset(
        dataset_file,
        timestamps,
        progress_every_slices=args.progress_every_slices,
        show_progress=args.show_progress,
    )
    prep_total_seconds = time.perf_counter() - prep_start

    if prep["total_events"] == 0:
        print("[WARN] Dataset is empty; choose a different timestamp window.")
        return

    results = {
        "window": {
            "end_timestamp": end_timestamp.isoformat(),
            "slices": args.slices,
        },
        "experiment": {
            "consumer_sleep_ms": args.consumer_sleep_ms,
            "consumer_time_budget_seconds": args.consumer_time_budget_seconds,
            "sync_loss_ratio": args.sync_loss_ratio,
            "sync_loss_seed": args.sync_loss_seed,
            "pause_every_n_events": args.pause_every_n_events,
            "pause_min_seconds": args.pause_min_seconds,
            "pause_max_seconds": args.pause_max_seconds,
            "kafka_drain_timeout_seconds": args.kafka_drain_timeout_seconds,
        },
        "dataset": {
            **prep,
            "dataset_file": str(dataset_file),
            "prepare_total_seconds": round(prep_total_seconds, 3),
        },
    }

    consumer_sleep_seconds = max(args.consumer_sleep_ms, 0.0) / 1000.0

    if args.mode in ("sync", "both"):
        print("[INFO] Running synchronous benchmark...")
        sync_metrics = run_sync(
            dataset_file=dataset_file,
            total_events=prep["total_events"],
            neo4j_uri=args.neo4j_uri,
            neo4j_user=args.neo4j_user,
            neo4j_password=args.neo4j_password,
            consumer_sleep_seconds=consumer_sleep_seconds,
            sync_loss_ratio=args.sync_loss_ratio,
            sync_loss_seed=args.sync_loss_seed,
            pause_every_n_events=args.pause_every_n_events,
            pause_min_seconds=args.pause_min_seconds,
            pause_max_seconds=args.pause_max_seconds,
            progress_every_events=args.progress_every_events,
            show_progress=args.show_progress,
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
            consumer_sleep_seconds=consumer_sleep_seconds,
            kafka_drain_timeout_seconds=args.kafka_drain_timeout_seconds,
            progress_every_events=args.progress_every_events,
            show_progress=args.show_progress,
        )
        results["kafka"] = kafka_metrics
        print(f"[INFO] Kafka done: {kafka_metrics}")

    if "sync" in results and "kafka" in results:
        sync_time = results["sync"]["write_seconds"]
        kafka_time = results["kafka"]["end_to_end_seconds"]
        ratio = (kafka_time / sync_time) if sync_time > 0 else None
        kafka_gap = int(results["kafka"].get("delivery_gap", 0))
        results["comparison"] = {
            "sync_write_seconds": sync_time,
            "kafka_end_to_end_seconds": kafka_time,
            "kafka_to_sync_time_ratio": round(ratio, 3) if ratio is not None else None,
            "sync_not_reaching_consumer_pct": results["sync"]["not_reaching_consumer_pct"],
            "kafka_not_reaching_consumer_pct": results["kafka"]["not_reaching_consumer_pct"],
            "kafka_loss_expected_zero": kafka_gap == 0,
            "kafka_delivery_gap": kafka_gap,
        }
        if kafka_gap != 0:
            print(f"[WARN] Kafka delivery gap is non-zero: {kafka_gap}")
        print(f"[INFO] Comparison: {results['comparison']}")

    Path(args.output_json).write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"[INFO] Results saved to {args.output_json}")


if __name__ == "__main__":
    main()
