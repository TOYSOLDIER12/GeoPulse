import csv
try:
    from .models import GDELTEvent
except ImportError:
    from models import GDELTEvent

def parse_gdelt_csv(csv_file):
    reader = csv.reader(csv_file, delimiter="\t")

    for row in reader:
        try:
            event = GDELTEvent(
                event_id=row[0],
                date=row[1],
                actor1_name=row[6],
                actor1_country=row[7],
                actor2_name=row[16],
                actor2_country=row[17],
                event_code=row[26],
                event_root_code=row[27],
                tone=float(row[34]) if row[34] else None,
                location_country=row[51] if row[51] else None
            )

            # Basic filtering
            if not event.actor1_country or not event.actor2_country:
                continue

            yield event

        except (IndexError, ValueError):
            continue

