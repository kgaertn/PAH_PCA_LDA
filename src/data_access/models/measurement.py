from dataclasses import dataclass

@dataclass
class Measurement:
    """Represents a generic measurement with metadata."""
    id: int | str
    participant_id: str
    timepoint: str
    device: str                         
    target: str    
    axis: str                     
    unit: str                 