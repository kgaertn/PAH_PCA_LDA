from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Datapoint:
    measurement_id: str
    bow_stroke: str
    up_down: str
    time_point: str
    value: float