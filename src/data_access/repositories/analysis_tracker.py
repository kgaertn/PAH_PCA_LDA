from data_access.repositories.base_repository import BaseRepository

import sqlite3, json, hashlib
import pandas as pd

class AnalysisTracker(BaseRepository):
    def __init__(self):
        """
        Initializes the AnalysisTracker with a database connection by calling the parent constructor and sets relevant keys for unique analyses.
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
        """
        Create a canonical parameter JSON and hash signature for an analysis.

        Args:
            analysis_name (str): Name of the analysis type.
            params (dict): Full parameter dictionary.

        Returns:
            tuple[str, str]: (signature hash, canonical JSON string).
        """
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

    def has_been_analyzed(self, exp_params, analysis_params):
        """
        Check whether the given experiment and analysis parameters have already been logged.

        Args:
            exp_params (dict): Experiment-level parameters.
            analysis_params (dict): Analysis parameters used to build the signature.

        Returns:
            bool: True if an identical analysis exists in the log.
        """
        exp_id = exp_params['exp_id']
        measurement_tp = exp_params['measurement_tp']
        devices = exp_params['device']
        pain_groups = exp_params['pain_groups']
        analysis_name = exp_params['analysis_name']
                
        if type(measurement_tp) == list and len(measurement_tp) > 1:
            measurement_tp = [", ".join(measurement_tp), ", ".join(reversed(measurement_tp))]
        elif type(measurement_tp) == str:
            measurement_tp = [measurement_tp]
            
        if type(devices) == list and len(devices) > 1:
            devices = [", ".join(devices), ", ".join(reversed(devices))]
        elif type(devices) == str:
            devices = [devices]
        

        #TODO: this is unnecessary, the signature only looks for specific parameters of the analyses, not the measurement_tp or device
        all_signatures = []
        for tp in measurement_tp:
            for device in devices:
                analysis_params['measurement_tp'] = tp
                analysis_params['device'] = device
                signature,_ = self.make_param_signature(analysis_name, analysis_params)
                all_signatures.append(signature)
        
        pain_groups_json = json.dumps(pain_groups)   
        
        filters = {
                "exp_id": exp_id,
                "device": devices,
                "measurement_tp": measurement_tp,
                "pain_groups": pain_groups_json,
                "analysis_name": analysis_name,
                "param_signature":all_signatures
            }

        rows = self.get(table_or_view="analysis_log", **filters)
        rows = pd.DataFrame(rows)
        if not rows.empty:
            return True
        return False


    def record_analysis(self, exp_params, analysis_params, status="success"):
        """
        Store the analysis parameters and signature in the analysis log.

        Args:
            exp_params (dict): Experiment-level parameters.
            analysis_params (dict): Parameters used to generate the signature.
            status (str): Analysis execution status.
        """
        exp_id = exp_params['exp_id']
        measurement_tp = exp_params['measurement_tp']
        device = exp_params['device']
        pain_groups = exp_params['pain_groups']
        analysis_name = exp_params['analysis_name']
        
        if type(measurement_tp) == list and len(measurement_tp) > 1:
            measurement_tp = ", ".join(measurement_tp)
            analysis_params['measurement_tp'] = measurement_tp
        elif type(measurement_tp) == list and len(measurement_tp) == 1:
            measurement_tp = measurement_tp[0]
            analysis_params['measurement_tp'] = measurement_tp
            
        if type(device) == list and len(device) > 1:
            device = ", ".join(device)
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