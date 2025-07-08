from data_processing.data_preprocess import DataProcessor
from data_processing.pca_analysis import PCAAnalyser
from core.setup_and_upload import SetupUploader

from pathlib import Path

class SetupAnalyserPCA:
    def __init__(self):
        """
        Initializes the SetupUploader with processors.
        """
        self.data_processor = DataProcessor()
        self.pca_analyser = PCAAnalyser() 
        self.setup_uploader = SetupUploader()
    
    def run_pca_analysis(self,exp_id, measurement_tp, device, scale_data = True, scaler_type = 'standard_scaler', check_requirements = False):
        existing_target_axes = self.data_processor.get_existing_target_axis_MPA_Clean(device)
        total_pca_info = {}
        for target, axis in existing_target_axes:
            if(target, axis)== ('left radioulnar joint angle', 'Y'):
                print("")
            df_sorted, df_sorted_transformed  = self.setup_uploader.load_data_for_pca(device,measurement_tp, target, axis)
            df_transformed = self.setup_uploader.process_data_for_pca(df_sorted)
            df_outliers_removed, count_outliers = self.setup_uploader.check_and_remove_outliers(df_transformed)
            
            df_part, df_pca = df_outliers_removed.iloc[:, :-202], df_outliers_removed.iloc[:, -202:]
            measurement_type_id = int(df_transformed['measurement_type_id'].unique()[0])
            if check_requirements:
                self.plot_data_linearity(target, axis, df_sorted_transformed)
                
                fig_corr_mat = self.pca_analyser.check_pca_requirements(df_sorted_transformed.iloc[:, -202:], target, axis)
                current_path = Path.cwd()
                output_path = current_path / "output" / "plots"
                output_path = current_path / "output" / "plots" / "Correlation_matrix"
                self.pca_analyser.save_plot(fig_corr_mat, output_path, f"Original_Data_Correlation_matrix_{target}_{axis}")
                fig_corr_mat = self.pca_analyser.check_pca_requirements(df_transformed.iloc[:, -202:], target, axis)
                self.pca_analyser.save_plot(fig_corr_mat, output_path, f"Mean_Subt_Data_Correlation_matrix_{target}_{axis}")
            if scale_data:
                df_analysis, scaler_info = self.scale_df(df_pca, scaler_type)
                scaler = self.pca_analyser.load_specific_scaler(measurement_type_id, scaler_type)
                if scaler:
                    scaler_id = scaler.id
                else: 
                    scaler_id = self.upload_scaler(measurement_type_id, scaler_info)
            else:
                df_analysis = df_pca
                scaler_id = None
            pca_info, pc_sample_scores = self.conduct_pca_analysis(df_analysis, df_part, measurement_type_id, scale_data, scaler_id)
            total_pca_info.update({
                f'{target}_{axis}' : {
                    'scaler_id' : scaler_id,
                    'scaler_info' : scaler_info,
                    'data_scaled' : scale_data,
                    'pca_info' : pca_info,
                    'pc_sample_scores' : pc_sample_scores                    
                }
            })
        return total_pca_info
    
    def upload_pca_analysis(self, pca_info):
        for target_axis, target_axis_data in pca_info.items():
            scaler_id = target_axis_data['scaler_id']
            data_scaled = target_axis_data['data_scaled']
            pc_info = target_axis_data['pca_info']
            sample_scores = target_axis_data['pc_sample_scores']
            
            self.upload_pca_analysis_target_axis(pc_info, sample_scores, scaler_id, data_scaled)
    
    def plot_data_linearity(self, target, axis, df):
        fig_title = f"{target}, {axis}"
        fig1, fig2 = self.pca_analyser.plot_linearity(df, fig_title)
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots"
        output_path = current_path / "output" / "plots" / "Linearity_plots"
        self.pca_analyser.save_plot(fig1, output_path, f"Scatter_matrix_{target}_{axis}")
        self.pca_analyser.save_plot(fig2, output_path, f"Lag_plot_{target}_{axis}")        
            
    def scale_df(self, df, scaler_type):
        #df_part, df_pca = df.iloc[:, :-202], df.iloc[:, -202:]
        scaled_df, scaler =self.pca_analyser.standardize_df(df)

        scaler_info = {
            'mean' : list(scaler.mean_),
            'scale' : list(scaler.scale_),
            'scaler_type' : scaler_type
        }
        return scaled_df, scaler_info
    
    def upload_scaler(self, measurement_type_id, scaler_info):
        mean = scaler_info['mean']
        scale = scaler_info['scale']
        scaler_type = scaler_info['scaler_type']
        scaler_id = self.pca_analyser.upload_scaler(measurement_type_id, scaler_type, mean, scale)
        return scaler_id
   
    def conduct_pca_analysis(self, df, df_part, measurement_type_id, data_scaled = True, scaler_id = None):     
        variance_level = 0.9
        explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores = self.pca_analyser.apply_pca(df, variance_level)
        pca_info = {}
        pc_scores = {}
        for component_idx in range(k):
            pc_index = component_idx + 1
            loading_vector = list(pca_final.components_[component_idx])
            explained_variance_pc = float(explained_variance[component_idx])
            pca_info.update({
                f'component_{pc_index}':{
                    'measurement_type_id': measurement_type_id,
                    'pc_index': pc_index,
                    'loading_vector' : loading_vector, 
                    'explained_variance' : explained_variance_pc
                } 
            })
            #pc_id = self.pca_analyser.upload_pca(measurement_type_id, pc_index, loading_vector, explained_variance_pc, data_scaled, scaler_id)
            pc_sample_scores = list(zip(df_part['sample_id'], df_pca_scores.iloc[:,component_idx]))
            pc_scores.update({f'component_{pc_index}_scores' : pc_sample_scores})
            #self.pca_analyser.upload_pc_scores(pc_id, pc_sample_scores)

        return pca_info, pc_scores
    
    def upload_pca_analysis_target_axis(self, pca_info, pc_sample_scores, scaler_id = None, data_scaled = False):
        pc_sample_keys = list(pc_sample_scores.keys())
        for index, (_, component) in enumerate(pca_info.items()):
            component_key = pc_sample_keys[index]
            measurement_type_id = component['measurement_type_id']
            pc_index = component['pc_index']
            loading_vector = component['loading_vector']
            explained_variance = component['explained_variance']
            pc_id = self.pca_analyser.upload_pca(measurement_type_id, pc_index, loading_vector, explained_variance, data_scaled, scaler_id)
            self.pca_analyser.upload_pc_scores(pc_id, pc_sample_scores[component_key])
    
    def check_distribution(self, exp_id, device, measurement_tp):
        pca_df = self.pca_analyser.load_pc_data(exp_id, device, measurement_tp)
        pc_distribution_results= self.pca_analyser.check_t_test_assumptions(pca_df)     
        return pc_distribution_results   
    
    def conduct_t_test(self, exp_id, device, measurement_tp):
        pca_df = self.pca_analyser.load_pc_data(exp_id, device, measurement_tp, 'normal_distribution')
        
        t_test_results = self.pca_analyser.rank_pcs(pca_df)
        return t_test_results
        
    def upload_distribution(self, distribution_info):
        self.pca_analyser.upload_distribution_info(distribution_info)
        
    def upload_t_test(self, t_test_results):
        self.pca_analyser.upload_t_test_results(t_test_results)