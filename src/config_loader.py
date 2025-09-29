from pathlib import Path
import yaml
from data_access.models.analysis_config import AnalysisConfig
from processing.data_loading import DataLoader

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
            check_requirements=raw_cfg["pca_analysis"]["check_requirements"],
            distributions_plotted=raw_cfg["pca_analysis"]["distributions_plotted"],
            create_general_plots=raw_cfg["pca_analysis"]["create_general_plots"],
            check_ttest_distribution=raw_cfg["pca_analysis"]["check_ttest_distribution"],
            rotation_method=raw_cfg["pca_analysis"]["rotation_method"],
            scaler_type=raw_cfg["pca_analysis"]["scaler_type"],
            nr_components=raw_cfg["pca_analysis"]["nr_components"],
            test_t_test_assumptions=raw_cfg["pca_analysis"]["test_t_test_assumptions"],
            t_test_assumptions_relevant=raw_cfg["pca_analysis"]["t_test_assumptions_relevant"],
            t_test_distribution_type = raw_cfg["pca_analysis"]["t_test_distribution_type"],
            lda_nr_components = raw_cfg["lda_analysis"]["lda_nr_components"],
            lda_validation_type = raw_cfg["lda_analysis"]["lda_validation_type"],
            lda_splits = raw_cfg["lda_analysis"]["lda_splits"],
            lda_scaler_type = raw_cfg["lda_analysis"]["lda_scaler_type"],
            lda_imputation_type = raw_cfg["lda_analysis"]["lda_imputation_type"],
            lda_imputer_parameter = raw_cfg["lda_analysis"]["lda_imputer_parameter"],
            lda_repeats = raw_cfg["lda_analysis"]["lda_repeats"]
        )
        