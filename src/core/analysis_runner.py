from core.analyses.abstract_analysis import AbstractAnalyser
#from core.analyses.pca_analysis import PCAAnalyser
#from core.analyses.lda_analysis import LDAAnalyser
#from core.analyses.ttest_analysis import TTestAnalyser
#from core.analyses.general_analysis import GeneralAnalyser

from typing import List

class AnalysisRunner:
    def __init__(self, cfg, upload_logger):
        self.cfg = cfg
        self.logger = upload_logger
        self.analyses: List[AbstractAnalyser] = []
        # TODO: automize the steps
        #self.steps = ["pca", "distribution", "rotated_pcs", "rotated_distribution", "t_test"]

    def register_analysis(self, analysis: AbstractAnalyser):
        """Add an analysis to the workflow."""
        self.analyses.append(analysis)

    def run(self):
        """Execute all registered analyses in sequence."""
        
        key = {
            "exp_id": self.cfg.exp_id,
            "measurement_tp": self.cfg.measurement_tp,
            "device": self.cfg.device,
            "pain_groups": self.cfg.pain_groups,
            "check_requirements" : self.cfg.check_requirements,
            "distributions_plotted" : self.cfg.distributions_plotted,
            "create_general_plots" : self.cfg.create_general_plots,
            "check_ttest_distribution" : self.cfg.check_ttest_distribution,  
            "rotation_method": self.cfg.rotation_method,
            "pca_scaler_type": self.cfg.scaler_type, 
            "nr_components": self.cfg.nr_components,
            "create_general_plots" : self.cfg.create_general_plots,
            "test_t_test_assumptions": self.cfg.test_t_test_assumptions,
            "t_test_assumptions_relevant": self.cfg.t_test_assumptions_relevant,
            "t_test_distribution_type": self.cfg.t_test_distribution_type,
            "lda_nr_components": self.cfg.lda_nr_components,
            "lda_validation_type" : self.cfg.lda_validation_type,
            "lda_splits": self.cfg.lda_splits,
            "lda_scaler_type" : self.cfg.lda_scaler_type,
            "lda_imputation_type" : self.cfg.lda_imputation_type,
            "lda_imputer_parameter" : self.cfg.lda_imputer_parameter,
            "lda_repeats" : self.cfg.lda_repeats
        }
        entry = self.logger.get_entry(key)
        
        #self.logger.info("Starting analysis workflow...")
#
        for analysis in self.analyses:
            #self.logger.info(f"Preparing {analysis.__class__.__name__}...")
#
            #self.logger.info(f"Running {analysis.__class__.__name__}...")
            if not self.logger.is_uploaded(entry, analysis.analysis_name):
                results = analysis.run(key)
#
                if results is not None:
                #self.logger.info(f"Handling results of {analysis.__class__.__name__}...")
                    analysis.handle_results(results, entry)
               
        for analysis in self.analyses:                    
            if analysis.analysis_name == 'pca' or analysis.analysis_name == 'pca_rotated' or analysis.analysis_name == 'lda' or analysis.analysis_name == 'lda_rotated':
                analysis.reconstruct_results(key)
            #elif analysis.analysis_name == 't_test' or analysis.analysis_name == 't_test_rotated':
            #    results = analysis.reconstruct_results(key, entry)
                    #analysis._upload_step(entry, analysis.analysis_name, self.pca_runner.upload_pca_analysis, results)
#
        #self.logger.info("All analyses completed.")
#
    #def run(self):
    #    key = {
    #        "exp_id": self.cfg.exp_id,
    #        "measurement_tp": self.cfg.measurement_tp,
    #        "device": self.cfg.device,
    #        "pain_groups": self.cfg.pain_groups,
    #        "nr_components": self.cfg.nr_components,
    #        "scaler_type": self.cfg.scaler_type, 
    #        "rotation_method": self.cfg.rotation_method,
    #    }
    #    entry = self.logger.get_entry(key)
#
# TO#DO: for the analysis, change all functions so they only get the cfg object as input
    #    # Step 1: PCA
    #    if not self.logger.is_uploaded(entry, "pca"):
    #        pca_results = self.pca_runner.run_pca_analysis(
    #            self.cfg.exp_id, self.cfg.measurement_tp, self.cfg.device,
    #            self.cfg.pain_groups, check_requirements=self.cfg.check_requirements
    #        )
    #        self._upload_step(entry, "pca", self.pca_runner.upload_pca_analysis, pca_results)
#
    #    # Step 2: Distribution
    #    dist_results = self.pca_runner.check_distribution(
    #        self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
    #        self.cfg.pain_groups, distributions_plotted=self.cfg.distributions_plotted
    #    )
    #    self._upload_step(entry, "distribution", self.pca_runner.upload_distribution, dist_results)
#
    #    # Step 3: Rotation
    #    if not self.logger.is_uploaded(entry, "rotated_pcs"):
    #        rot_results = self.pca_runner.run_pca_rotation(
    #            self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
    #            self.cfg.pain_groups, method=self.cfg.rotation_method
    #        )
    #        self._upload_step(entry, "rotated_pcs", self.pca_runner.upload_pca_analysis, rot_results)
#
    #    # Step 4: Rotated distribution
    #    rot_dist_results = self.pca_runner.check_distribution(
    #        self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
    #        self.cfg.pain_groups, distributions_plotted=self.cfg.distributions_plotted
    #    )
    #    self._upload_step(entry, "rotated_distribution", self.pca_runner.upload_distribution, rot_dist_results)
#
    #    # Step 5: t-test
    #    t_test_results = self.pca_runner.conduct_t_test(
    #        self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp, pain_groups=self.cfg.pain_groups
    #    )
    #    self._upload_step(entry, "t_test", self.pca_runner.upload_t_test, t_test_results)
#
    #    # Step 6: Reconstruction (always rerun)
    #    self.pca_runner.reconstruct_pcas(
    #        self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
    #        self.cfg.pain_groups, self.cfg.scaler_type, self.cfg.nr_components
    #    )
    #    self.pca_runner.reconstruct_pcas(
    #        self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
    #        self.cfg.pain_groups, self.cfg.scaler_type, self.cfg.nr_components,
    #        select_rotated=True
    #    )
#
    #def _upload_step(self, entry, step, upload_func, result):
    #    if not self.logger.is_uploaded(entry, step):
    #        upload_func(result)
    #        self.logger.mark_uploaded(entry, step)
