
from sklearn.preprocessing import StandardScaler
from pathlib import Path
import matplotlib.pyplot as plt

from data_processing.data_preprocess import DataProcessor    
from data_processing.pca_analysis import PCAAnalyser
    
class ReconstructerPCA:
    def __init__(self):
        """
        Initializes the SetupUploader with processors.
        """
        self.data_processor = DataProcessor()
        self.pca_analyser = PCAAnalyser() 
        #self.setup_uploader = SetupUploader()
    
    # reconstruct & plot top PCs
    # plot upper/lower bands per group
    def reconstruct_pcas(self, exp_id, device, measurement_tp, highest_rank, lowest_rank, scaler_type):

        ranked_pc_scores_df = self.pca_analyser.load_pcs_by_rank(1, device,measurement_tp, highest_rank, lowest_rank)
        for rank in range(highest_rank, lowest_rank+1):
            current_pc_df = ranked_pc_scores_df[ranked_pc_scores_df['rank'] == rank]
            target, axis = current_pc_df['target'].unique()[0], current_pc_df['axis'].unique()[0]
            measurement_type_id = int(current_pc_df['meas_type_id'].unique()[0])
            participants = current_pc_df['participant_id'].unique()
            participants = tuple(int(x) for x in participants)
            df = self.data_processor.load_MPA_clean_data_by_device_tp_target_axis_participants(device, measurement_tp, target, participants, axis)
            df_reduced = df[[
                    'participant_id', 'ext_participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever',
                    'measurement_id','measurement_type_id', 'target', 'axis', 'sample_id', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
                    'value'
                ]]
            df_transformed = self.data_processor.pivot_full_cycles_to_wide(df_reduced, 'value')
            scaler_info = self.pca_analyser.load_specific_scaler(measurement_type_id, scaler_type)
            scaler = StandardScaler()
            scaler.mean_ = scaler_info.mean
            scaler.scale_ = scaler_info.scale
            component_reconstruction_data = self.pca_analyser.reconstruct_single_component(current_pc_df, df_transformed, scaler) 
            #plot PCA reconstruction
            pc_name = "PC"+str(current_pc_df['pc_index'].unique()[0])
            title_reconstruction = f'Single component reconstruction: Rank {rank}, {target} {axis}; {pc_name}'
            title_loading_vector = f'Loading vector: Rank {rank} {target} {axis}; {pc_name}'
            fig, axs = self.pca_analyser.plot_PCA_reconstruction(component_reconstruction_data, title_reconstruction, title_loading_vector)

            current_path = Path.cwd()
            output_path = current_path / "output" / "plots" / "PCA_Reconstruction"
            self.pca_analyser.save_plot(fig, output_path, f"{measurement_tp}_Scaled_Orig_Rank_{rank}_{target}_{axis}_{pc_name}")
            plt.close()
            fig, axs = self.pca_analyser.plot_PCA_reconstruction_per_group(component_reconstruction_data, title_reconstruction, title_loading_vector)
            self.pca_analyser.save_plot(fig, output_path, f"Per_group_{measurement_tp}_Scaled_Orig_Rank_{rank}_{target}_{axis}_{pc_name}")
            plt.close()
            