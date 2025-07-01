from data_processing.data_preprocess import DataProcessor
from data_processing.pca_analysis import PCAAnalyser
from core.setup_and_upload import SetupUploader

from pathlib import Path

class SetupAnalysePCA:
    def __init__(self):
        """
        Initializes the SetupUploader with processors.
        """
        self.data_processor = DataProcessor()
        self.pca_analyser = PCAAnalyser() 
        self.setup_uploader = SetupUploader()
    
    def run_pca_analysis(self, measurement_tp, device, scale_data = True):
        existing_target_axes = self.data_processor.get_existing_target_axis_MPA_Clean(device)
        for target, axis in existing_target_axes:
            df_transformed = self.setup_uploader.load_and_process_data_for_pca(device,measurement_tp, target, axis)
            # TODO: Bartlett’s test of sphericity
            # TODO: Kaiser-Meyer-Olkin test
            df_outliers_removed, count_outliers = self.setup_uploader.check_and_remove_outliers(df_transformed)
            #self.plot_data_linearity(target, axis, df_outliers_removed)
            df_part, df_pca = df_outliers_removed.iloc[:, :-202], df_outliers_removed.iloc[:, -202:]
            measurement_type_id = int(df_transformed['measurement_type_id'].unique()[0])
            if scale_data:
                df_analysis, scaler_id = self.standardize_df_and_upload_scaler(df_pca, measurement_type_id)
                data_scaled = True
            else:
                df_analysis = df_pca
                data_scaled = False
                scaler_id = None
            explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores= self.conduct_and_upload_pca_analysis(df_analysis, df_part, measurement_type_id, data_scaled, scaler_id)
        self.conduct_and_upload_t_test(1, device)
        print(f"{target} {axis} analysed and saved")
    
    def plot_data_linearity(self, target, axis, df):
        fig_title = f"{target}, {axis}"
        fig1, fig2 = self.pca_analyser.plot_linearity(df, fig_title)
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots"
        output_path = current_path / "output" / "plots" / "Linearity_plots"
        self.pca_analyser.save_plot(fig1, output_path, f"Scatter_matrix_{target}_{axis}")
        self.pca_analyser.save_plot(fig2, output_path, f"Lag_plot_{target}_{axis}")        
            
    def standardize_df_and_upload_scaler(self, df, measurement_type_id):
        #df_part, df_pca = df.iloc[:, :-202], df.iloc[:, -202:]
        scaled_df, scaler =self.pca_analyser.standardize_df(df)

        mean = list(scaler.mean_)
        scale = list(scaler.scale_)
        scaler_type = "standard_scaler"
        scaler_id = self.pca_analyser.upload_scaler(measurement_type_id, scaler_type, mean, scale)
        #data_scaled = True     
        return scaled_df, scaler_id
   
    def conduct_and_upload_pca_analysis(self, df, df_part, measurement_type_id, data_scaled = True, scaler_id = None):     
        variance_level = 0.9
        explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores = self.pca_analyser.apply_pca(df, variance_level)
        for component_idx in range(k):
            pc_index = component_idx + 1
            loading_vector = list(pca_final.components_[component_idx])
            explained_variance_pc = float(explained_variance[component_idx])
            pc_id = self.pca_analyser.upload_pca(measurement_type_id, pc_index, loading_vector, explained_variance_pc, data_scaled, scaler_id)
            sample_scores = list(zip(df_part['sample_id'], df_pca_scores.iloc[:,component_idx]))
            self.pca_analyser.upload_pc_scores(pc_id, sample_scores)

        return explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores
     
    def conduct_and_upload_t_test(self, exp_id, device):
        pca_df = self.pca_analyser.load_pc_data(exp_id, device)
        t_test_results = self.pca_analyser.rank_pcs(pca_df)
        self.pca_analyser.upload_t_test_results(t_test_results)