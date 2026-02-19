import time
from datetime import datetime, timezone, timedelta
from gdelt_fetcher import fetch_gdelt_file, extract_csv
from gdelt_parser import parse_gdelt_csv
from kafka_producer import GDELTKafkaProducer
from normalizer import normalize_event


POLL_INTERVAL_SECONDS = 900  # 15 minutes

def round_down_to_15(dt):
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)



def clean_event(event):
    """
    Normalize GDELT event before sending to Kafka.
    Ensures:
    - Actor names are standardized
    - Country names are standardized
    - No null values for required fields
    - Optional: add tone or interaction type
    """
    actor = event.get("Actor1Name")
    country = event.get("Actor1CountryCode")

    # Normalize names
    event["Actor1Name"] = normalize_actor_name(actor)
    event["Actor1CountryCode"] = normalize_country_name(country)

    actor2 = event.get("Actor2Name")
    country2 = event.get("Actor2CountryCode")
    event["Actor2Name"] = normalize_actor_name(actor2)
    event["Actor2CountryCode"] = normalize_country_name(country2)

    # Skip any events with missing mandatory info
    if not event["Actor1Name"] or not event["Actor2Name"]:
        return None

    return event


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
                normalized_event = normalize_event(event)  # normalize in place
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

