from data_access.repositories.base_repository import BaseRepository
#from data_access.models.datapoint import Datapoint

import sqlite3, json, hashlib
import pandas as pd

class AnalysisTracker(BaseRepository):
    def __init__(self):
        """
        Initializes the DatapointRepository with a database connection by calling the parent constructor.
        """
        super().__init__()
        self.RELEVANT_KEYS = ["rotation_method", 
                              "lda_nr_components", 
                              "pca_scaler_type", 
                              "lda_validation_type", 
                              "lda_splits", 
                              "lda_scaler_type", 
                              "lda_imputation_type", 
                              "lda_repeats"]
    
    @staticmethod
    def make_param_signature(analysis_name, params):
        #analysis_name = "t_test" if "t_test" in step_name else ("lda" if "lda" in step_name else step_name)
        relevant_keys = {
            "pca": ["pca_scaler_type"],
            "pca_rotated": ["rotation_method", "pca_scaler_type"],
            "t_test": ["t_test_distribution_type"],
            "t_test_rotated": ["t_test_distribution_type"],
            "lda": ["pca_scaler_type", "lda_validation_type", "lda_splits",
                    "lda_scaler_type", "lda_imputation_type", "lda_imputer_parameter", "lda_repeats"],
            "lda_rotated": ["rotation_method", "pca_scaler_type", "lda_validation_type", "lda_splits",
                    "lda_scaler_type", "lda_imputation_type", "lda_imputer_parameter", "lda_repeats"],
        }
        keys = relevant_keys.get(analysis_name, list(params.keys()))
        subset = {k: params[k] for k in keys if k in params}
        canonical_json = json.dumps(subset, sort_keys=True)
        return hashlib.sha256(canonical_json.encode()).hexdigest(), canonical_json

    #def filter_relevant_params(self, analysis_params: dict) -> dict:
    #    """
    #    Return a dictionary containing only the relevant parameters
    #    from analysis_params, based on RELEVANT_KEYS.
    #    """
    #    return {k: v for k, v in analysis_params.items() if k in self.RELEVANT_KEYS}

    def has_been_analyzed(self, exp_params, analysis_params):
        #relevant_analysis_params = self.filter_relevant_params(analysis_params)
        exp_id = exp_params['exp_id']
        measurement_tp = exp_params['measurement_tp']
        device = exp_params['device']
        pain_groups = exp_params['pain_groups']
        analysis_name = exp_params['analysis_name']

        #if 'pca' not in analysis_name:
        if type(measurement_tp) == list and len(measurement_tp) > 1:
            measurement_tp = json.dumps(measurement_tp)
            analysis_params['measurement_tp'] = measurement_tp
        elif type(measurement_tp) == list and len(measurement_tp) == 1:
            measurement_tp = measurement_tp[0]
            analysis_params['measurement_tp'] = measurement_tp
            
        if type(device) == list and len(device) > 1:
            device = json.dumps(device)
            analysis_params['device'] = device
        elif type(device) == list and len(device) == 1:
            device = device[0]
            analysis_params['device'] = device
            
        signature, _ = self.make_param_signature(analysis_name, analysis_params)
        pain_groups_json = json.dumps(pain_groups)   
        
        filters = {
                "exp_id": exp_id,
                "device": device,
                "measurement_tp": measurement_tp,
                "pain_groups": pain_groups_json,
                "analysis_name": analysis_name,
                "param_signature":signature
            }
            
        #rows = self.get_advanced(
        #    table_or_view="analysis_log",
        #    distinct = False,
        #    return_df=False, 
        #    **filters
        #)
        rows = self.get(table_or_view="analysis_log", **filters)
        rows = pd.DataFrame(rows)
        if not rows.empty:
            return True
        return False


    def record_analysis(self, exp_params, analysis_params, status="success"):
        exp_id = exp_params['exp_id']
        measurement_tp = exp_params['measurement_tp']
        device = exp_params['device']
        pain_groups = exp_params['pain_groups']
        analysis_name = exp_params['analysis_name']
        #device_groups_json = json.dumps(device)
        #param_json = json.dumps(relevant_params)
        if type(measurement_tp) == list and len(measurement_tp) > 1:
            measurement_tp = json.dumps(measurement_tp)
            analysis_params['measurement_tp'] = measurement_tp
        elif type(measurement_tp) == list and len(measurement_tp) == 1:
            measurement_tp = measurement_tp[0]
            analysis_params['measurement_tp'] = measurement_tp
            
        if type(device) == list and len(device) > 1:
            device = json.dumps(device)
            analysis_params['device'] = device
        elif type(device) == list and len(device) == 1:
            device = device[0]
            analysis_params['device'] = device
        
        signature, relevant_params = self.make_param_signature(analysis_name, analysis_params)
        pain_groups_json = json.dumps(pain_groups)
        
        try:
            data = {
                "exp_id": exp_id,
                "device": device,
                "measurement_tp": measurement_tp,
                "pain_groups": pain_groups_json,
                "analysis_name": analysis_name,
                "param_signature":signature, 
                "param_json":relevant_params,
                "status":status                
            }
            self.insert_one("analysis_log", data)
        except sqlite3.IntegrityError:
            # This will happen if it already exists due to UNIQUE constraint
            pass