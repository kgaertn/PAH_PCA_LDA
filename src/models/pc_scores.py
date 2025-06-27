from dataclasses import dataclass, field
from typing import List, Optional
import json

@dataclass
class PC_Scores:
    id: int
    pc_id: int
    sample_id: int
    pc_score: float