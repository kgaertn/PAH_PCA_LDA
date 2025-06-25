from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Sample:
    id: int 
    measurement_id: int
    bow_stroke_start: int
    bow_stroke_end: int