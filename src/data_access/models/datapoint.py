from dataclasses import dataclass
from typing import Optional

@dataclass
class Datapoint:
    """Represents a single datapoint linked to a measurement."""
    id: int | str
    measurement_id: int
    bow_stroke: int
    up_down: int
    key:str
    time_point: int
    value: float
    sample_id: Optional[int] = None