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
    

# potentially expandable
@dataclass
class EMGMeasurement(Measurement):
    """Represents an EMG measurement for a specific muscle."""
    muscle: str

@dataclass
class MocapMeasurement(Measurement):
    """Represents a motion capture measurement for a specific joint."""
    joint: str