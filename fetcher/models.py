from dataclasses import dataclass
from typing import Optional

@dataclass
class GDELTEvent:
    event_id: str
    date: str
    actor1_name: str
    actor1_country: str
    actor2_name: str
    actor2_country: str
    event_code: str
    event_root_code: str
    tone: Optional[float]
    location_country: Optional[str]

