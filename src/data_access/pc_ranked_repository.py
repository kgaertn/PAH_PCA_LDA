import pandas as pd
from db.connection import get_connection
from data_access.base_repository import BaseRepository
from models.pc_ranked import PC_Ranked

class PCRankedRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        super().__init__()

# region Setter
    def insert_new_pc(self, pc: PC_Ranked):
        data = {
            "measurement_type_id": pc.measurement_type_id,
            "parent_id":pc.parent_id,
            "scaler_id": pc.scaler_id,
            "rotation_id": pc.rotation_id,
            "pc_index": pc.pc_index,
            "loading_vector": pc.loading_vector_to_json(),
            "explained_variance": pc.explained_variance,
            "data_scaled": pc.data_scaled
        }
        return self.insert_one("pcs_ranked", data)

    def update_multiple_t_test_info(self, t_test_results: list[PC_Ranked]):
        values = []
        for pc in t_test_results:
            values.append({
                "id": pc.id,
                #"rank": pc.rank,
                "group_mean_pain": pc.group_mean_pain,
                "group_mean_no_pain": pc.group_mean_no_pain,
                "group_std_pain": pc.group_std_pain,
                "group_std_no_pain": pc.group_std_no_pain,
                "t_value": pc.t_value,
                "p_value": pc.p_value,
            })
        self.update_many("pcs_ranked", values_list=values, where_keys=["id"])
 
    def update_pc_score_distribution_info(self, pc_distributions: list[PC_Ranked]):
        values = []
        for pc in pc_distributions:
            values.append({
                "id": pc.id,
                "distribution_info": pc.distribution_info,
                "shap_wilk_w_pain": pc.shap_wilk_w_pain,
                "shap_wilk_w_no_pain": pc.shap_wilk_w_no_pain,
                "shap_wilk_p_pain": pc.shap_wilk_p_pain,
                "shap_wilk_p_no_pain": pc.shap_wilk_p_no_pain
            })
        self.update_many("pcs_ranked", values_list=values, where_keys=["id"])       


# region Getter
# use these functions to access data from the experiment table, depending on the needs
# TODO


# endregion Getter