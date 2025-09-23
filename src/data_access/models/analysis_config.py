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
    create_general_plots: bool
    check_ttest_distribution: bool
    rotation_method: str
    scaler_type: str
    nr_components: int
    test_t_test_assumptions: bool
    t_test_assumptions_relevant: bool
    t_test_distribution_type: str