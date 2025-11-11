from data_access.repositories.base_repository import BaseRepository
from data_access.models.lda_results import LDAResults

class LDARepository(BaseRepository):
    def __init__(self):
        """
        Initializes the PCRankedRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_new_lda(self, lda: LDAResults):
        """
        Inserts a new PC_Ranked record into the 'pcs_ranked' table.

        Args:
            pc (PC_Ranked): The PC_Ranked model instance to insert.

        Returns:
            int: The ID of the newly inserted record.
        """
        data = {
            "nr_components": lda.nr_components,
            "acc_values": lda.values_to_json(lda.acc_values),
            "acc_mean":lda.acc_mean,
            "acc_sd": lda.acc_sd,
            "missclass_err_values": lda.values_to_json(lda.missclass_err_values),
            "missclass_err_mean": lda.missclass_err_mean,
            "missclass_err_sd": lda.missclass_err_sd,
            "roc_auc_values": lda.values_to_json(lda.roc_auc_values),
            "roc_auc_mean": lda.roc_auc_mean,
            "roc_auc_sd": lda.roc_auc_sd,
            "validation_type":lda.validation_type,
            "scaler_type": lda.scaler_type,
            "imputation_type": lda.imputation_type, 
            "n_folds": lda.n_folds,
            "n_repeats": lda.n_repeats,
            "lda_scores": lda.values_to_json(lda.lda_scores),
            "lda_scalings": lda.values_to_json(lda.lda_scalings),
            "lda_class_means": lda.values_to_json(lda.lda_class_means)
        }
        return self.insert_one("lda_results", data)

# region Setter
    def insert_many_pc_scores(self, ldas: list[LDAResults])-> int:
        """
        Inserts multiple PC_Scores records into the 'pc_scores' table.

        Args:
            pc_scores (list[PC_Scores]): A list of PC_Scores model instances to be inserted.

        Returns:
            int: The number of inserted records or the result from insert_many (depending on implementation).
        """
        data_list = [{
            "nr_components": lda.nr_components,
            "acc_mean":lda.acc_mean,
            "acc_sd": lda.acc_sd,
            "missclass_err_mean": lda.missclass_err_mean,
            "missclass_err_sd": lda.missclass_err_sd,
            "roc_auc_mean": lda.roc_auc_mean,
            "roc_auc_sd": lda.roc_auc_sd,
            "imputation": lda.imputation, 
            "n_folds": lda.n_folds
        } for lda in ldas]
        self.insert_many("lda_results", data_list)
    
    def insert_lda_pcs(self, lda_id: int, pc_ids: list[int]):
        """
        Inserts multiple participant-pain group associations in bulk.

        Args:
            participant_ids (list[int]): List of participant IDs.
            pain_group_id (int): The pain group ID to assign.
        """
        data = [{"lda_id": lda_id, "pc_id": pid} for pid in pc_ids]
        self.insert_many("lda_pc", data)
 

# region Getter
# To be implemented
    def get_lda_results(self, exp_id, device, measurement_tp, pain_group_names, pca_scaled, rotation_type, lda_imputation_type, 
            lda_scaler_type, lda_validation_type):
        
            #pain_group_names = [", ".join(pain_groups), ", ".join(reversed(pain_groups))]
            filters = {
                "exp_id": exp_id,
                "device": device,
                "meas_time_point": measurement_tp,
                "pain_groups": pain_group_names,
                "pca_scaled": pca_scaled,
                "pca_rotation_type":rotation_type,
                "lda_imputation_type":lda_imputation_type,
                "lda_scaler_type":lda_scaler_type,
                "lda_validation_type": lda_validation_type
            }
            
            rows = self.get(table_or_view="[Complete_LDA]", **filters)
            
            if rows is not None:
                for index, row in rows.iterrows():
                    rows.at[index, 'acc_values'] = LDAResults.list_from_json(rows.loc[index, 'acc_values'])
                    rows.at[index, 'missclass_err_values'] = LDAResults.list_from_json(rows.loc[index, 'missclass_err_values'])
                    rows.at[index, 'roc_auc_values'] = LDAResults.list_from_json(rows.loc[index, 'roc_auc_values'])
                    rows.at[index, 'lda_scores'] = LDAResults.list_from_json(rows.loc[index, 'lda_scores'])
                    rows.at[index, 'lda_scalings'] = LDAResults.list_from_json(rows.loc[index, 'lda_scalings'])
                    rows.at[index, 'lda_class_means'] = LDAResults.list_from_json(rows.loc[index, 'lda_class_means'])
            return rows
            #    print("")

        

# endregion Getter