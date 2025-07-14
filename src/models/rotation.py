from dataclasses import dataclass, field
from typing import List, Optional
import json

@dataclass
class RotationPCA:
    id: int
    rotation_type: str