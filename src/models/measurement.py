from dataclasses import dataclass

@dataclass
class Measurement:
    id: int
    participant_id: str
    timepoint: str
    device: str                         
    target: str    
    axis: str                     
    unit: str                 
    

# potentially expandable
@dataclass
class EMGMeasurement(Measurement):
    muscle: str

@dataclass
class MocapMeasurement(Measurement):
    joint: str