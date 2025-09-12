from dataclasses import dataclass
from typing import List

@dataclass
class AnalysisConfig:
    exp_id: int
    measurement_tp: str
    device: str
    pain_groups: List[str]
    check_requirements: bool
    distributions_plotted: bool
    rotation_method: str
    scaler_type: str
    nr_components: int
    test_assumptions: bool
    assumptions_relevant: bool