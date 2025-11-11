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
        pca_keys = [
            "exp_id","measurement_tp","device", "pain_groups", "check_requirements", "distributions_plotted", "create_general_plots",
            "check_ttest_distribution", "rotation_method", "pca_scaler_type", "nr_components",
            "test_t_test_assumptions", "t_test_assumptions_relevant", "t_test_distribution_type"
        ]

        lda_keys = [
            "exp_id","measurement_tp","device", "pain_groups", "lda_nr_components", "lda_validation_type", "lda_splits",
            "lda_scaler_type", "lda_imputation_type", "lda_imputer_parameter", "lda_repeats"
        ]
        general_keys = [
            "exp_id","measurement_tp","device", "pain_groups"
        ]
        
        run_key = {
            "exp_id": self.cfg.exp_id,
            "measurement_tp": self.cfg.measurement_tp,
            "device": self.cfg.device,
            "pain_groups": self.cfg.pain_groups
        }

        entry = self.logger.get_entry(run_key)
        
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
        #entry = self.logger.get_entry(key)
        
        #self.logger.info("Starting analysis workflow...")
#
        for analysis in self.analyses:
            #self.logger.info(f"Preparing {analysis.__class__.__name__}...")
#           
            if analysis.analysis_name == "pca" or analysis.analysis_name == "pca_rotated" or analysis.analysis_name == "t_test" or analysis.analysis_name == "t_test_rotated":
                params = {k: key[k] for k in pca_keys}  # only keys that affect analysis1
                dependencies = None
            elif analysis.analysis_name == "lda" or analysis.analysis_name == "lda_rotated":
                params = {k: key[k] for k in lda_keys}  # keys for analysis2
                dependencies = {"pca": {k: key[k] for k in pca_keys}}
            else:
                params = {k: key[k] for k in general_keys}  # only keys that affect analysis1
                dependencies = None                
            if not self.logger.is_uploaded(entry, analysis.analysis_name, params, dependencies):
                results = analysis.run(key)
                if results is not None:
                    analysis.handle_results(params, results, entry)
                    self.logger.mark_uploaded(entry, analysis.analysis_name, params)

        for analysis in self.analyses:                    
            if analysis.analysis_name == 'pca' or analysis.analysis_name == 'pca_rotated' or analysis.analysis_name == 'lda' or analysis.analysis_name == 'lda_rotated':
                analysis.reconstruct_results(key)




            ##self.logger.info(f"Running {analysis.__class__.__name__}...")
            #if not self.logger.is_uploaded(entry, analysis.analysis_name, params, dependencies):
            #    results = analysis.run(key)
##
            #    if results is not None:
            #    #self.logger.info(f"Handling results of {analysis.__class__.__name__}...")
            #        analysis.handle_results(results, entry)
               

            #elif analysis.analysis_name == 't_test' or analysis.analysis_name == 't_test_rotated':
            #    results = analysis.reconstruct_results(key, entry)
                    #analysis._upload_step(entry, analysis.analysis_name, self.pca_runner.upload_pca_analysis, results)
#
        #self.logger.info("All analyses completed.")

