from dataclasses import dataclass, field
from typing import List, Optional
import json

@dataclass
class Scaler:
    id: int
    measurement_type_id: int
    scaler_type: str
    mean: List[float]
    scale: List[float]

    def mean_to_json(self) -> str:
        return json.dumps(self.mean)
        
    def scale_to_json(self) -> str:
        return json.dumps(self.scale)  # <-- Hier war der Fehler
    
    @staticmethod
    def list_from_json(json_str: str) -> List[float]:
        return json.loads(json_str)