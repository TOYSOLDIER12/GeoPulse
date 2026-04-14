import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from gdelt_fetcher import fetch_gdelt_file, extract_csv
from gdelt_parser import parse_gdelt_csv
from pipeline_common import Neo4jWriter, write_event_to_neo4j


DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"


def round_down_to_15(dt):
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def parse_args():
    parser = argparse.ArgumentParser(description="Run the GDELT pipeline synchronously without Kafka.")
    parser.add_argument("--timestamp", default=None, help="UTC timestamp in ISO format, e.g. 2026-04-14T12:15")
    parser.add_argument("--neo4j-uri", default=DEFAULT_NEO4J_URI)
    parser.add_argument("--neo4j-user", default=DEFAULT_NEO4J_USER)
    parser.add_argument("--neo4j-password", default=DEFAULT_NEO4J_PASSWORD)
    return parser.parse_args()


def resolve_timestamp(timestamp_text: str | None):
    if timestamp_text:
        parsed = datetime.fromisoformat(timestamp_text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return round_down_to_15(parsed)

    return round_down_to_15(datetime.now(timezone.utc) - timedelta(minutes=15))


def main():
    args = parse_args()
    current_time = resolve_timestamp(args.timestamp)

    started_at = time.perf_counter()
    fetch_started_at = time.perf_counter()
    zip_bytes = fetch_gdelt_file(current_time)
    fetch_seconds = time.perf_counter() - fetch_started_at

    if not zip_bytes:
        print(f"[WARN] No data available for {current_time}")
        print(
            f"[INFO] Summary: timestamp={current_time.isoformat()}, fetched=0, processed=0, skipped=0, "
            f"fetch_seconds={fetch_seconds:.2f}, total_seconds={time.perf_counter() - started_at:.2f}"
        )
        return

    parse_started_at = time.perf_counter()
    csv_file = extract_csv(zip_bytes)
    events = list(parse_gdelt_csv(csv_file))
    parse_seconds = time.perf_counter() - parse_started_at

    writer = Neo4jWriter(args.neo4j_uri, args.neo4j_user, args.neo4j_password)
    processed_events = 0
    skipped_events = 0
    write_started_at = time.perf_counter()

    try:
        for event in events:
            try:
                if write_event_to_neo4j(event, writer):
                    processed_events += 1
                else:
                    skipped_events += 1
            except Exception as error:
                skipped_events += 1
                print(f"[ERROR] Failed to write event {getattr(event, 'event_id', 'unknown')}: {error}")
    finally:
        writer.close()

    write_seconds = time.perf_counter() - write_started_at
    total_seconds = time.perf_counter() - started_at

    print(
        "[INFO] Summary: "
        f"timestamp={current_time.isoformat()}, fetched={len(events)}, processed={processed_events}, skipped={skipped_events}, "
        f"fetch_seconds={fetch_seconds:.2f}, parse_seconds={parse_seconds:.2f}, write_seconds={write_seconds:.2f}, total_seconds={total_seconds:.2f}"
    )


if __name__ == "__main__":
    main()
