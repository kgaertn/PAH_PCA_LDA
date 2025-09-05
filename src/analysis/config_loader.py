from pathlib import Path
import yaml
from models.analysis_config import AnalysisConfig
from analysis.data_processing.data_loading import DataLoader

class ConfigLoader:
    @staticmethod
    def load(config_path: str) -> AnalysisConfig:
        data_loader = DataLoader()
        with open(Path(config_path)) as f:
            raw_cfg = yaml.safe_load(f)
        exp_name = raw_cfg["exp_name"]  
        exp_id = data_loader.get_experiment_by_name(exp_name)
        
        return AnalysisConfig(
            exp_id=exp_id,
            measurement_tp=raw_cfg["measurement_tp"],
            device=raw_cfg["device"],
            pain_groups=raw_cfg["pain_groups"],
            check_requirements=raw_cfg["analysis"]["check_requirements"],
            distributions_plotted=raw_cfg["analysis"]["distributions_plotted"],
            rotation_method=raw_cfg["analysis"]["rotation_method"],
            scaler_type=raw_cfg["analysis"]["scaler_type"],
            nr_components=raw_cfg["analysis"]["nr_components"],
            test_assumptions=raw_cfg["analysis"]["test_assumptions"],
            assumptions_relevant=raw_cfg["analysis"]["assumptions_relevant"]
        )
        