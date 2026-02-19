import pycountry
import json

# Load canonical actors (regions, orgs, etc.)
# Example: {"EUROPE": "Europe", "UN": "United Nations", ...}
with open("canonical_actors.json", "r") as f:
    CANONICAL_ACTORS = json.load(f)

def normalize_actor(name: str) -> str:
    """
    Normalize actor names:
    - Converts country codes/names to standard country names using pycountry
    - Maps regions/orgs using canonical_actors.json
    - Falls back to original name if unknown
    """
    if not name or name.strip() == "":
        return "UNKNOWN"

    name = name.strip().upper()

    # First, check canonical actors (regions, orgs, etc.)
    if name in CANONICAL_ACTORS:
        return CANONICAL_ACTORS[name]

    # Try pycountry lookup for countries
    try:
        country = pycountry.countries.lookup(name)
        return country.name  # normalized country name
    except LookupError:
        pass  # not a country, continue

    # fallback to cleaned-up name
    return name.title()  # "XI JINPING" -> "Xi Jinping"

def normalize_event(event: dict) -> dict:
    """
    Normalize relevant fields in a GDELT event
    """
    if hasattr(event, "Actor1Name"):
        event.Actor1Name = normalize_actor(event.Actor1Name)
    if hasattr(event, "Actor2Name"):
        event.Actor2Name = normalize_actor(event.Actor2Name)

    return event

