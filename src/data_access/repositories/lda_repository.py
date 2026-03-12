from data_access.repositories.base_repository import BaseRepository
from data_access.models.lda_results import LDAResults

class LDARepository(BaseRepository):
    def __init__(self):
        """
        Initializes the LDARepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_new_lda(self, lda: LDAResults):
        """
        Insert a single LDA results record into the database.

        Args:
            lda (LDAResults): The LDA results object to insert.

        Returns:
            int: ID of the inserted LDA record.
        """
        data = {
            "devices":lda.devices,
            "measurement_tp":lda.measurement_tp,
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
            "stacked_feature_values": lda.values_to_json(lda.stacked_feature_values),
            "feature_imp_mean": lda.values_to_json(lda.feature_imp_mean),
            "feature_imp_sd": lda.values_to_json(lda.feature_imp_sd),
            "feature_description": lda.values_to_json(lda.feature_description),
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
    def insert_many_lda_scores(self, ldas: list[LDAResults])-> int:
        """
        Insert multiple LDA score summary records into the database.

        Args:
            ldas (list[LDAResults]): List of LDA results objects to insert.

        Returns:
            int: Number of inserted records.
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
        Create LDA-to-PC association records for a given LDA result.

        Args:
            lda_id (int): The LDA result ID.
            pc_ids (list[int]): List of PC IDs to link to the LDA record.
        """
        data = [{"lda_id": lda_id, "pc_id": pid} for pid in pc_ids]
        self.insert_many("lda_pc", data)
 

# region Getter
    def get_lda_results(self, exp_id, device, measurement_tp, pain_group_names, pca_scaled, rotation_type, lda_imputation_type, 
            lda_scaler_type, lda_validation_type):

        """
        Retrieve LDA results matching the specified experiment and analysis filters.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            measurement_tp (str): Measurement timepoint.
            pain_group_names (str): Pain group labels.
            pca_scaled (str): PCA scaling type.
            rotation_type (str): PCA rotation method.
            lda_imputation_type (str): LDA imputation type.
            lda_scaler_type (str): LDA scaler type.
            lda_validation_type (str): LDA validation method.

        Returns:
            DataFrame: Retrieved and decoded LDA results.
        """
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
                rows.at[index, 'stacked_feature_values'] = LDAResults.list_from_json(rows.loc[index, 'stacked_feature_values'])
                rows.at[index, 'feature_imp_mean'] = LDAResults.list_from_json(rows.loc[index, 'feature_imp_mean'])
                rows.at[index, 'feature_imp_sd'] = LDAResults.list_from_json(rows.loc[index, 'feature_imp_sd'])
                rows.at[index, 'feature_description'] = LDAResults.list_from_json(rows.loc[index, 'feature_description'])
                rows.at[index, 'lda_scores'] = LDAResults.list_from_json(rows.loc[index, 'lda_scores'])
                rows.at[index, 'lda_scalings'] = LDAResults.list_from_json(rows.loc[index, 'lda_scalings'])
                rows.at[index, 'lda_class_means'] = LDAResults.list_from_json(rows.loc[index, 'lda_class_means'])
        return rows

# endregion Getter
