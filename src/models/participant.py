from dataclasses import dataclass
from typing import Optional

@dataclass
class Participant:
    """Represents a participant's details and pain-related data (PRMD)."""
    id: int | str
    participant_id: str
    experiment_id: int | str
    age: Optional[int] = None
    height_cm: Optional[float] = None
    weight_kg: Optional[float] = None
    instrument: Optional[str] = None
    PRMD_shoulder_neck_right: Optional[bool] = None
    PRMD_shoulder_neck_left: Optional[bool] = None
    PRMD_upper_arm_right: Optional[bool] = None
    PRMD_upper_arm_left: Optional[bool] = None
    PRMD_ever: Optional[bool] = None