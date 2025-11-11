import json
from pathlib import Path

class UploadLogger:
    UNIQUE_KEYS = ["exp_id", "measurement_tp", "device", "pain_groups", "rotation_method", "lda_nr_components", "pca_scaler_type", "lda_validation_type", "lda_splits", 
                   "lda_scaler_type", "lda_imputation_type", "lda_repeats"]

    
    def __init__(self, log_path: str, steps: list):
        self.log_file = Path(log_path)
        self.log_file.parent.mkdir(exist_ok=True)
        self.steps = steps
        self.run_log = self._load_log()

    def _load_log(self):
        if self.log_file.exists():
            with open(self.log_file, "r") as f:
                return json.load(f)
        return []

    def get_entry(self, analysis_key: dict):
        entry = next((e for e in self.run_log if self._match(e, analysis_key)), None)
        if entry is None:
            # Only copy UNIQUE_KEYS as top-level identifiers, not all parameters
            entry = {k: analysis_key[k] for k in self.UNIQUE_KEYS if k in analysis_key}
            entry["uploaded_steps"] = {step: False for step in self.steps}
            self.run_log.append(entry)
        return entry

    #def get_entry(self, analysis_key: dict):
    #    entry = next((e for e in self.run_log if self._match(e, analysis_key)), None)
    #    if entry is None:
    #        entry = {**analysis_key, "uploaded_steps": {step: False for step in self.steps}}
    #        self.run_log.append(entry)
    #    return entry
    
    def _match(self, entry, key):
        return all(entry[k] == key[k] for k in self.UNIQUE_KEYS if k in key)

    #def _match(self, entry, key):
    #    return all(entry[k] == key[k] for k in key)
    def mark_uploaded(self, entry, step: str, params: dict):
        entry["uploaded_steps"][step] = {"params": params}
        self._save_log()
        
    #def mark_uploaded(self, entry, step: str):
    #    entry["uploaded_steps"][step] = True
    #    self._save_log()

    #def is_uploaded(self, entry, step: str):
    #    return entry["uploaded_steps"].get(step, False)

    def is_uploaded(self, entry, step: str, params: dict, dependencies: dict = None):
        logged = entry["uploaded_steps"].get(step)
        if not logged or logged.get("params") != params:
            return False
        
        if dependencies:
            for dep_step, dep_params in dependencies.items():
                dep_logged = entry["uploaded_steps"].get(dep_step)
                if not dep_logged or dep_logged.get("params") != dep_params:
                    return False
        return True

    def _save_log(self):
        with open(self.log_file, "w") as f:
            json.dump(self.run_log, f, indent=2)
