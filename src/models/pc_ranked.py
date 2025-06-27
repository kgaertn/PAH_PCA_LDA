from dataclasses import dataclass, field
from typing import List, Optional
import json

@dataclass
class PC_Ranked:
    id: int
    measurement_type_id: int
    pc_index: Optional[int] = None
    loading_vector: Optional[List[float]] = None
    data_scaled: Optional[bool] = None
    rank: Optional[int] = None
    explained_variance: Optional[float] = None
    group_mean_pain: Optional[float] = None
    group_mean_no_pain: Optional[float] = None
    t_value: Optional[float] = None
    p_value: Optional[float] = None
    pca_info: Optional[str] = None

    def loading_vector_to_json(self) -> str:
        return json.dumps(self.loading_vector)
        
    @staticmethod
    def list_from_json(json_str: str) -> List[float]:
        return json.loads(json_str)