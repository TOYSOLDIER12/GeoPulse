import time
from datetime import datetime, timezone, timedelta
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from gdelt_fetcher import fetch_gdelt_file, extract_csv
from gdelt_parser import parse_gdelt_csv
from kafka_producer import GDELTKafkaProducer
from pipeline_common import normalize_gdelt_event


POLL_INTERVAL_SECONDS = 60  # 15 minutes

def round_down_to_15(dt):
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def main():
    producer = GDELTKafkaProducer()
    current_time = round_down_to_15(datetime.now(timezone.utc) - timedelta(minutes=15))

    while True:
        print(f"[INFO] Fetching GDELT data for {current_time}")

        zip_bytes = fetch_gdelt_file(current_time)
        if zip_bytes:
            csv_file = extract_csv(zip_bytes)

            event_count = 0
            for event in parse_gdelt_csv(csv_file):
                normalized_event = normalize_gdelt_event(event)
                producer.send_event(normalized_event)
                event_count += 1

            producer.flush()
            print(f"[INFO] Sent {event_count} events to Kafka topic '{producer.topic}'")
        else:
            print("[WARN] No data available for this timestamp")

        # Move to next 15-minute slot
        current_time += timedelta(minutes=15)
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

