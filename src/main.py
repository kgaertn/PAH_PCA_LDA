from core.analysis_runner import AnalysisRunner
from core.analyses.pca_analysis import PCAAnalyser
#from core.analyses.lda_analysis import LDAAnalysis
#from core.analyses.ttest_analysis import TTestAnalysis

#from core.run_pca import PCARunner
#from core.analyses.general_analysis import GeneralAnalysisRunner
#from models.analysis_config import AnalysisConfig
from analysis.config_loader import ConfigLoader
from analysis.upload_logger import UploadLogger
from core.analysis_runner import AnalysisRunner
from data_access.db.setup import db_setup
from pathlib import Path
import yaml
import json


def main():
    """
    Runs the complete PCA and general analysis workflow, including:
    - Generating summary plots.
    - Performing PCA, rotations, and t-tests.
    - Uploading analysis results.
    """
    db_setup()
    # Load configuration
    cfg = ConfigLoader.load(Path(__file__).resolve().parent.parent /  "config" / "config.yaml")

    # Initialize logger and runner
    steps = ["pca", "distribution", "rotated_pcs", "rotated_distribution", "t_test"]
    logger = UploadLogger(Path(__file__).resolve().parent.parent /  "output" / "logs" / "analysis_log.json", steps)

    # Initialize your PCA runner (assume already implemented)
    #pca_runner = PCARunner()  # your existing class

    # Run analysis
    runner = AnalysisRunner(cfg, logger)
    runner.register_analysis(PCAAnalyser(cfg, logger))
    runner.register_analysis(PCAAnalyser(cfg, logger, run_rotated=True))
    runner.run()

    print("Analysis complete.")
    #pca_analysis_completed = False
    #pca_uploads_completed = []
    #pca_uploads_completed = [(1,'pre','mocap')]
    #pca_uploads_completed = [(1,'pre','mocap'), (1,'post','mocap')]
    #
    ## config variables
    #with open("config.yaml") as f:
    #    raw_cfg = yaml.safe_load(f)
#
    #exp_id = raw_cfg["experiment_name"]
    #
    #cfg = AnalysisConfig(
    #    exp_id=exp_id,
    #    measurement_tp=raw_cfg["measurement_tp"],
    #    device=raw_cfg["device"],
    #    pain_groups=raw_cfg["pain_groups"],
    #    check_requirements=raw_cfg["analysis"]["check_requirements"],
    #    distributions_plotted=raw_cfg["analysis"]["distributions_plotted"],
    #    rotation_method=raw_cfg["analysis"]["rotation_method"],
    #    scaler_type=raw_cfg["analysis"]["scaler_type"],
    #    nr_components=raw_cfg["analysis"]["nr_components"],
    #    test_assumptions=raw_cfg["analysis"]["test_assumptions"],
    #    assumptions_relevant=raw_cfg["analysis"]["assumptions_relevant"],
    #)
#
    #
    ##rotation_method = 'varimax'
    ##pain_groups = ["healthy", "shoulder_neck"]
    ##pain_groups = ["all"]
    #
    #pca_runner = PCARunner()
    #general_analysis_runner = GeneralAnalysisRunner()
    #
    ##general_analysis_runner.create_plots_key_per_group(device, exp_id, measurement_tp, pain_groups)
    ##general_analysis_runner.create_plots_mean_std_keys(device, exp_id, measurement_tp, pain_groups)
    ##general_analysis_runner.create_plots_mean_std(device, exp_id, measurement_tp, pain_groups)
    ##general_analysis_runner.create_plots_mean_std(device, exp_id, measurement_tp, pain_groups, key_diff_controlled=True)
    ##general_analysis_runner.create_plots_key_per_participant(device, exp_id, measurement_tp, pain_groups)
#
    #if ((exp_id, measurement_tp, device) not in pca_uploads_completed):
    #    pca_results = pca_runner.run_pca_analysis(exp_id, measurement_tp, device, pain_groups, check_requirements=True)
    #    pca_runner.upload_pca_analysis(pca_results) 
    #    pc_distribution_results = pca_runner.check_distribution(exp_id, device, measurement_tp, pain_groups, distributions_plotted= False)
    #    pca_runner.upload_distribution(pc_distribution_results)
    #    rotation_results = pca_runner.run_pca_rotation(exp_id, device, measurement_tp, pain_groups, method = rotation_method)
    #    pca_runner.upload_pca_analysis(rotation_results)  
    #    pc_distribution_results = pca_runner.check_distribution(exp_id, device, measurement_tp,pain_groups, distributions_plotted= False)
    #    pca_runner.upload_distribution(pc_distribution_results)      
    #
    #    t_test_results = pca_runner.conduct_t_test(exp_id, device, measurement_tp, pain_groups=pain_groups)
    #    pca_runner.upload_t_test(t_test_results) 
    #
    #nr_components = None
    #pca_runner.reconstruct_pcas(exp_id, device, measurement_tp, pain_groups, 'standard_scaler', nr_components)
    #pca_runner.reconstruct_pcas(exp_id, device, measurement_tp, pain_groups, 'standard_scaler', nr_components, select_rotated=True)
    #print("")
             
if __name__ == '__main__':
    main()