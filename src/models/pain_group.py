from dataclasses import dataclass, field
from typing import List, Optional
import json

@dataclass
class PainGroup:
    id: int
    pain_group: str