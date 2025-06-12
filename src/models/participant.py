from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Participant:
    id: str 
    participant_id: str
    experiment_id: str
    age: Optional[int] = None
    height_cm: Optional[float] = None
    weight_kg: Optional[float] = None
    instrument: Optional[str] = None
    PRMD_shoulder_neck_right: Optional[bool] = None
    PRMD_shoulder_neck_left: Optional[bool] = None
    PRMD_upper_arm_right: Optional[bool] = None
    PRMD_upper_arm_left: Optional[bool] = None
    PRMD_ever: Optional[bool] = None