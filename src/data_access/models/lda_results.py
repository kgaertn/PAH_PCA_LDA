from dataclasses import dataclass
from typing import List
import json

@dataclass
class LDAResults:
    """Represents a principal component score for a sample."""
    id: int
    pc_ids: list[int]
    nr_components: int
    acc_values: List[float] | None
    acc_mean: float
    acc_sd: float | None
    missclass_err_values: List[float] | None
    missclass_err_mean: float
    missclass_err_sd: float   | None
    roc_auc_values: List[float]  | None
    roc_auc_mean: float
    roc_auc_sd: float | None
    stacked_feature_values: list[float]|None
    feature_imp_mean: float | None
    feature_imp_sd: float | None
    feature_description: float | None
    validation_type : str | None
    scaler_type: str
    imputation_type: str
    n_folds: int | None
    n_repeats: int | None
    lda_scores: list[dict] | None = None 
    lda_scalings: list[list[float]] | None = None
    lda_class_means: list[list[float]] | None = None
    
    def values_to_json(self, values) -> str:
        """Return the mean values as a JSON string."""
        if values != None:
            return json.dumps(list(values))
        else:
            return None
            
    @staticmethod
    def list_from_json(json_str: str) -> List[float]:
        """Convert a JSON string to a list of floats."""
        if json_str != None:
            return json.loads(json_str)
        else:
            return None