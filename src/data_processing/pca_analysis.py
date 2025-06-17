    
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scipy.stats import ttest_ind
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from data_processing.data_preprocess import DataProcessor

class PCAAnalyser:
    
    def __init__(self):
        """
        Initializes the PCA_Analyser.
        """    
        self.data_processor = DataProcessor()
        
    
    @staticmethod
    def standardize_df(df:pd.DataFrame) -> pd.DataFrame:
        scaler = StandardScaler()
        scaled_df = scaler.fit_transform(df)
        return scaled_df
    
    @staticmethod
    def apply_pca(df:pd.DataFrame, variance_level:float):
        pca_stand = PCA()
        principal_components = pca_stand.fit(df)
        
        # examine cumulative variance
        cumulative_variance = np.cumsum(pca_stand.explained_variance_ratio_)
        k = np.argmax(cumulative_variance >= variance_level) + 1
        
        # apply PCA with k components
        pca_final = PCA(n_components=k)
        pca_scores= pca_final.fit_transform(df)
        df_pca_scores= pd.DataFrame(pca_scores, columns=[f"PC{int(col)}" for col in range(1,k+1)])  
        return    cumulative_variance, k, pca_final, pca_scores, df_pca_scores
    
    @staticmethod
    def calculate_t_test(df:pd.DataFrame) -> list:
        pc_cols = df.iloc[:, 5:]
        target = df['target'][0]
        axis = df['axis'][0]
        t_test_result = []
        for pc in pc_cols:
            if not df[pc].isna().all():
                # calculate mean PC score per participant for t-test, to avoid participants being over-represented
                df_pc_mean_stand = df.groupby(['participant_id', 'PRMD_ever'])[pc].mean().reset_index()
                df_mean_pain = df_pc_mean_stand[df_pc_mean_stand['PRMD_ever'] == 1][pc]
                df_mean_nopain = df_pc_mean_stand[df_pc_mean_stand['PRMD_ever'] == 0][pc]
                
                t_stat, p_value = ttest_ind(df_mean_pain, df_mean_nopain, equal_var=False)
                t_test_result.append([target, axis, pc, t_stat, p_value])
        return t_test_result
        
            #print(f"PC: {pc} \t| t-value: {t_stat} \t| p-value: {p_value}")
        
    def rank_pcs(self,unique_target_axes:list, df:pd.DataFrame):
        # needs to be on all pcs (all joints, all axes)

        t_test_total = []
        for target, axis in unique_target_axes:
            df_target_axis = df[(df["target"] == target) & (df["axis"] == axis)]
            t_test_target_axis = self.calculate_t_test(df_target_axis)
            t_test_total.extend(t_test_target_axis)
        
        df_t_test = pd.DataFrame(t_test_total, columns = ['target', 'axis', 'PC', 't_value', 'p_value'])
        df_t_test_ranked = df_t_test.sort_values(by="t_value", key=lambda x: x.abs(), ascending=False).reset_index(drop=True)
        return df_t_test_ranked

    @staticmethod
    def calculate_mean_waveform_target_axis(df):
        
        df_matrix_pain = np.array(df[df['PRMD_ever'] == 1].iloc[:, -202:])
        df_matrix_no_pain = np.array(df[df['PRMD_ever'] == 0].iloc[:, -202:])
        df_matrix = np.array(df.iloc[:, -202:])
        
        mean_waveform_pain = np.mean(df_matrix_pain, axis=0)
        mean_waveform_no_pain = np.mean(df_matrix_no_pain, axis=0)
        mean_waveform = np.mean(df_matrix, axis=0)
        
        return mean_waveform_pain, mean_waveform_no_pain, mean_waveform
    
    @staticmethod
    def reconstruct_data(pc_scores, mean_note_waveforms, loading_vector):
        
        recon_centered = np.outer(pc_scores, loading_vector)
        reconstructed = recon_centered
        # TODO: check i can iterate over the samples effectively
        sample_count = 0
        for bs in mean_note_waveforms['bow_stroke'].unique():
            time_point_count = 0
            for time_point in mean_note_waveforms['time_point'].unique():
                mean_value = mean_note_waveforms[mean_note_waveforms['bow_stroke'] == bs & mean_note_waveforms['time_point'] == time_point]['mean_value']
                reconstructed[sample_count][time_point_count] += mean_value
        
        return
    
    @staticmethod
    def extract_PCA_index(pca_name:str):
        number = re.search(r"\d+", pca_name)
        if number:
            return int(number.group())-1
        return None
    
    @staticmethod
    def calculate_lower_upper_band(pc_scores, mean_waveform, loading_vector):
        lower_percentile =    np.percentile(pc_scores, 5)
        upper_percentile = np.percentile(pc_scores, 95)
        
        lower_band = mean_waveform + lower_percentile * loading_vector
        upper_band = mean_waveform + upper_percentile * loading_vector
        
        return lower_band, upper_band

    def reconstruct_single_component(self, ranked_pc, pcs_final, pca_reduced, df, mean_note_waveform):
        target = ranked_pc['target']
        axis = ranked_pc['axis']
        pc_idx = self.extract_PCA_index(ranked_pc['PC'])
        
        pca_target_axis = pcs_final[(pcs_final['target'] == target) & (pcs_final['axis'] == axis)]['PCA']
        loading_vector = pca_target_axis.iloc[0].components_[pc_idx]
        pc_scores = pca_reduced[:, pc_idx]
        
        # reconstruct data 
        self.reconstruct_data(pc_scores, mean_note_waveform, loading_vector)
        
        # update the mean calculation
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(df)       
        
        lower_band, upper_band = self.calculate_lower_upper_band(pc_scores, mean_waveform_pain, loading_vector)
        
        return {
        "mean_waveform_pain": mean_waveform_pain,
        "mean_waveform_no_pain": mean_waveform_no_pain,
        "mean_waveform_total": mean_waveform_total,
        "loading_vector": loading_vector,
        "pc_scores": pc_scores,
        "lower_band": lower_band,
        "upper_band": upper_band
        }

    def reconstruct_single_component_old(self, ranked_pc, pcs_final, pca_reduced, df):
        target = ranked_pc['target']
        axis = ranked_pc['axis']
        pc_idx = self.extract_PCA_index(ranked_pc['PC'])
        
        pca_target_axis = pcs_final[(pcs_final['target'] == target) & (pcs_final['axis'] == axis)]['PCA']
        loading_vector = pca_target_axis.iloc[0].components_[pc_idx]
        pc_scores = pca_reduced[:, pc_idx]
        
        # update the mean calculation
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(df)
        
        lower_band, upper_band = self.calculate_lower_upper_band(pc_scores, mean_waveform_pain, loading_vector)
        
        return {
        "mean_waveform_pain": mean_waveform_pain,
        "mean_waveform_no_pain": mean_waveform_no_pain,
        "mean_waveform_total": mean_waveform_total,
        "loading_vector": loading_vector,
        "pc_scores": pc_scores,
        "lower_band": lower_band,
        "upper_band": upper_band
        }
    
    @staticmethod
    def plot_PCA_reconstruction(component_data, title_waveform="Mean Waveform", title_loading="Loading Vector"):
        mean_waveform_pain = component_data['mean_waveform_pain']
        mean_waveform_no_pain = component_data['mean_waveform_no_pain']
        lower_band = component_data['lower_band']
        upper_band = component_data['upper_band']
        loading_vector = component_data['loading_vector']
        
        fig, axs = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True)
    
        # Plot mean waveforms
        axs[0].plot(mean_waveform_pain, label="Pain", color="red")
        axs[0].plot(mean_waveform_no_pain, label="No Pain", color="blue")
        axs[0].plot(lower_band, label="Lower Band", linestyle='--', color="black")
        axs[0].plot(upper_band, label="Upper Band", linestyle=':', color="black")
        axs[0].set_title(title_waveform)
        axs[0].set_xlabel("Normalized time (%)")
        axs[0].set_ylabel("Amplitude (°)")
        axs[0].legend()
        axs[0].grid(True)
        
        # Plot loading vector
        axs[1].plot(loading_vector, color="green")
        axs[1].set_title(title_loading)
        axs[1].set_xlabel("Component Index")
        axs[1].set_ylabel("Loading Value")
        axs[1].grid(True)
        
        return fig, axs        
    
    @staticmethod
    def save_plot(fig, file_path, filename, dpi=300, file_format='png'):
        """
        Saves a matplotlib figure to file.

        Parameters:
            fig (matplotlib.figure.Figure): The figure object to save.
            filename (str): Path or filename without extension.
            dpi (int): Resolution in dots per inch.
            file_format (str): File format, e.g. 'png', 'pdf', 'svg', etc.

        Returns:
            None
        """
        full_filename = f"{filename}.{file_format}"
        fig.savefig(str(file_path) +'\\' + full_filename, dpi=dpi, format=file_format)
        print(f"Plot saved to {full_filename}")
        
    
    