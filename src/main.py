from core.analysis_runner import AnalysisRunner
from core.analyses.pca_analysis import PCAAnalyser
from core.analyses.lda_analysis import LDAAnalyser
from core.analyses.ttest_analysis import TTestAnalyser
from core.analyses.general_analysis import GeneralAnalyser
from core.analyses.statistical_analysis import StatisticalAnalyser

from config_loader import ConfigLoader
from upload_logger import UploadLogger
from core.analysis_runner import AnalysisRunner
from data_access.db.setup import db_setup
from pathlib import Path


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
    steps = ["pca", "pca_rotated", "t_test", "t_test_rotated", "lda", "lda_rotated"]
    logger = UploadLogger(Path(__file__).resolve().parent.parent /  "output" / "logs" / "analysis_log.json", steps)

    # Run analysis
    runner = AnalysisRunner(cfg, logger)
    runner.register_analysis(GeneralAnalyser(cfg, logger))
    runner.register_analysis(PCAAnalyser(cfg, logger))
    runner.register_analysis(TTestAnalyser(cfg, logger))
    runner.register_analysis(PCAAnalyser(cfg, logger, run_rotated=True))
    runner.register_analysis(TTestAnalyser(cfg, logger, run_rotated=True))
    runner.register_analysis(LDAAnalyser(cfg, logger))
    #runner.register_analysis(StatisticalAnalyser(cfg,logger)) #TODO: adjust to new structure (multiple devices)
    runner.run()

    print("Analysis complete.")

             
if __name__ == '__main__':
    main()