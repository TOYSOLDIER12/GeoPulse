import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
FETCHER_DIR = ROOT_DIR / "fetcher"
CONSUMER_DIR = ROOT_DIR / "consumer"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(FETCHER_DIR) not in sys.path:
    sys.path.insert(0, str(FETCHER_DIR))

from fetcher.gdelt_fetcher import fetch_gdelt_file, extract_csv
from fetcher.gdelt_parser import parse_gdelt_csv
from fetcher.kafka_producer import GDELTKafkaProducer
from pipeline_common import normalize_gdelt_event


def round_down_to_15(dt):
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def resolve_timestamp(timestamp_text: str | None):
    if timestamp_text:
        parsed = datetime.fromisoformat(timestamp_text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return round_down_to_15(parsed)

    return round_down_to_15(datetime.now(timezone.utc) - timedelta(minutes=15))


def run_sync(timestamp_text: str | None):
    command = [sys.executable, str(FETCHER_DIR / "sync_main.py")]
    if timestamp_text:
        command.extend(["--timestamp", timestamp_text])

    started_at = time.perf_counter()
    result = subprocess.run(command, cwd=str(ROOT_DIR), check=False, text=True, capture_output=True)
    elapsed_seconds = time.perf_counter() - started_at
    return {
        "mode": "sync",
        "returncode": result.returncode,
        "elapsed_seconds": elapsed_seconds,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def run_kafka(timestamp_text: str | None):
    started_at = time.perf_counter()
    target_timestamp = resolve_timestamp(timestamp_text)
    zip_bytes = fetch_gdelt_file(target_timestamp)

    consumer_stdout = ""
    consumer_stderr = ""
    producer_count = 0

    if zip_bytes is None:
        return {
            "mode": "kafka",
            "producer_returncode": 0,
            "consumer_returncode": 0,
            "producer_count": 0,
            "elapsed_seconds": time.perf_counter() - started_at,
            "producer_stdout": "",
            "producer_stderr": "",
            "consumer_stdout": "",
            "consumer_stderr": "[WARN] No data available for the selected timestamp\n",
        }

    csv_file = extract_csv(zip_bytes)
    events = [normalize_gdelt_event(event) for event in parse_gdelt_csv(csv_file)]
    producer_count = len(events)

    if producer_count == 0:
        return {
            "mode": "kafka",
            "producer_returncode": 0,
            "consumer_returncode": 0,
            "producer_count": 0,
            "elapsed_seconds": time.perf_counter() - started_at,
            "producer_stdout": "",
            "producer_stderr": "",
            "consumer_stdout": "",
            "consumer_stderr": "[WARN] No valid events to publish for the selected timestamp\n",
        }

    consumer_command = [sys.executable, str(CONSUMER_DIR / "gdelt_to_neo4j.py"), "--max-events", str(producer_count)]
    consumer_process = subprocess.Popen(
        consumer_command,
        cwd=str(ROOT_DIR),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    producer = GDELTKafkaProducer()

    try:
        for event in events:
            producer.send_event(event)
        producer.flush()
        try:
            consumer_stdout, consumer_stderr = consumer_process.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            consumer_process.terminate()
            consumer_stdout, consumer_stderr = consumer_process.communicate(timeout=30)
    finally:
        if consumer_process.poll() is None:
            consumer_process.terminate()
            consumer_stdout, consumer_stderr = consumer_process.communicate(timeout=30)

    elapsed_seconds = time.perf_counter() - started_at
    return {
        "mode": "kafka",
        "producer_returncode": 0,
        "consumer_returncode": consumer_process.returncode,
        "producer_count": producer_count,
        "elapsed_seconds": elapsed_seconds,
        "producer_stdout": "",
        "producer_stderr": "",
        "consumer_stdout": consumer_stdout,
        "consumer_stderr": consumer_stderr,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Compare synchronous and Kafka-based GDELT processing.")
    parser.add_argument("--mode", choices=["sync", "kafka"], required=True)
    parser.add_argument("--timestamp", default=None, help="UTC timestamp in ISO format, e.g. 2026-04-14T12:15")
    return parser.parse_args()


def main():
    args = parse_args()
    timestamp = resolve_timestamp(args.timestamp)
    timestamp_text = timestamp.isoformat(timespec="minutes")

    if args.mode == "sync":
        result = run_sync(timestamp_text)
        print(result["stdout"], end="")
        if result["stderr"]:
            print(result["stderr"], file=sys.stderr, end="")
        print(f"[INFO] Benchmark mode=sync elapsed_seconds={result['elapsed_seconds']:.2f} returncode={result['returncode']}")
        return

    result = run_kafka(timestamp_text)
    print(result["consumer_stdout"], end="")
    if result["consumer_stderr"]:
        print(result["consumer_stderr"], file=sys.stderr, end="")
    print(
        "[INFO] Benchmark mode=kafka "
        f"elapsed_seconds={result['elapsed_seconds']:.2f} producer_count={result['producer_count']} producer_returncode={result['producer_returncode']} consumer_returncode={result['consumer_returncode']}"
    )


if __name__ == "__main__":
    main()