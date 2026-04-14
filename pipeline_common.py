import re
from datetime import datetime
from typing import Any

import pycountry
from neo4j import GraphDatabase


def normalize_country(name: str | None) -> str | None:
    if not name:
        return None

    cleaned = name.strip().upper()
    if not cleaned:
        return None

    try:
        country = pycountry.countries.lookup(cleaned)
        return country.name
    except LookupError:
        return cleaned.title()


def normalize_actor(actor: str | None) -> str | None:
    if not actor:
        return None

    cleaned = re.sub(r"\s+", " ", actor.strip())
    if not cleaned:
        return None

    return cleaned.title()


def classify_tone(tone_value: float) -> str:
    if tone_value > 1:
        return "Positive"
    if tone_value < -1:
        return "Negative"
    return "Neutral"


def _event_value(event: Any, attribute_name: str, key_name: str) -> Any:
    if isinstance(event, dict):
        return event.get(key_name)
    return getattr(event, attribute_name, None)


def normalize_gdelt_event(event: Any) -> Any:
    if isinstance(event, dict):
        event["actor1_name"] = normalize_actor(event.get("actor1_name"))
        event["actor2_name"] = normalize_actor(event.get("actor2_name"))
        event["actor1_country"] = normalize_country(event.get("actor1_country"))
        event["actor2_country"] = normalize_country(event.get("actor2_country"))
        return event

    if hasattr(event, "actor1_name"):
        event.actor1_name = normalize_actor(event.actor1_name)
    if hasattr(event, "actor2_name"):
        event.actor2_name = normalize_actor(event.actor2_name)
    if hasattr(event, "actor1_country"):
        event.actor1_country = normalize_country(event.actor1_country)
    if hasattr(event, "actor2_country"):
        event.actor2_country = normalize_country(event.actor2_country)

    return event


class Neo4jWriter:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def merge_actor(self, actor_name: str | None):
        if not actor_name:
            return None

        with self.driver.session() as session:
            return session.execute_write(
                lambda tx: tx.run("MERGE (a:Actor {name:$name}) RETURN a", name=actor_name).single()
            )

    def merge_country(self, country_name: str | None):
        if not country_name:
            return None

        with self.driver.session() as session:
            return session.execute_write(
                lambda tx: tx.run("MERGE (c:Country {name:$name}) RETURN c", name=country_name).single()
            )

    def create_actor_relationship(self, actor1, actor2, event_type, tone, event_desc, timestamp):
        if not actor1 or not actor2 or actor1 == actor2:
            return

        with self.driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MATCH (a1:Actor {name:$a1}), (a2:Actor {name:$a2})
                    MERGE (a1)-[r:INTERACTED {event:$event}]->(a2)
                    SET r.type = $type, r.tone = $tone, r.timestamp = $ts
                    """,
                    a1=actor1,
                    a2=actor2,
                    type=event_type,
                    tone=tone,
                    event=event_desc,
                    ts=timestamp,
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
                    c1=country1,
                    c2=country2,
                    type=event_type,
                    tone=tone,
                    event=event_desc,
                    ts=timestamp,
                )
            )


def write_event_to_neo4j(event: Any, writer: Neo4jWriter):
    actor1 = normalize_actor(_event_value(event, "actor1_name", "actor1_name"))
    actor2 = normalize_actor(_event_value(event, "actor2_name", "actor2_name"))
    country1 = normalize_country(_event_value(event, "actor1_country", "actor1_country"))
    country2 = normalize_country(_event_value(event, "actor2_country", "actor2_country"))

    if not actor1 or not actor2 or not country1 or not country2:
        return False

    raw_tone = _event_value(event, "tone", "tone")
    try:
        tone_value = float(raw_tone) if raw_tone is not None else 0.0
    except (TypeError, ValueError):
        tone_value = 0.0

    tone_cat = classify_tone(tone_value)
    event_type = str(_event_value(event, "event_code", "event_code") or "Other")
    event_desc = str(_event_value(event, "event_root_code", "event_root_code") or "Unknown")
    timestamp = _event_value(event, "date", "date") or datetime.utcnow().isoformat()

    writer.merge_actor(actor1)
    writer.merge_actor(actor2)
    writer.merge_country(country1)
    writer.merge_country(country2)
    writer.create_actor_relationship(actor1, actor2, event_type, tone_cat, event_desc, timestamp)
    writer.create_country_relationship(country1, country2, event_type, tone_cat, event_desc, timestamp)

    return True