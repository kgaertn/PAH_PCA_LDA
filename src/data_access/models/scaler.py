from dataclasses import dataclass
from typing import List
import json

@dataclass
class Scaler:
    """Represents a PCA scaling configuration with mean and scale values."""
    id: int
    measurement_type_id: int
    scaler_type: str
    mean: List[float]
    scale: List[float]

    def mean_to_json(self) -> str:
        """Return the mean values as a JSON string."""
        return json.dumps(self.mean)
        
    def scale_to_json(self) -> str:
        """Return the scale values as a JSON string."""
        return json.dumps(self.scale)
    
    @staticmethod
    def list_from_json(json_str: str) -> List[float]:
        """Convert a JSON string to a list of floats."""
        return json.loads(json_str)