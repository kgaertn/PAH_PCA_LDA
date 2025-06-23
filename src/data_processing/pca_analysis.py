    
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
        Initializes the PCA_Analyser with a data processor.
        """    
        self.data_processor = DataProcessor()
        
    
    @staticmethod
    def standardize_df(df:pd.DataFrame) -> pd.DataFrame:
        """
        Standardize the input DataFrame using z-score normalization.

        Parameters:
            df (pd.DataFrame): DataFrame containing numeric features to standardize.

        Returns:
            Tuple[np.ndarray, StandardScaler]:
                - The standardized values as a NumPy array.
                - The fitted StandardScaler instance.
        """
        scaler = StandardScaler()
        scaled_df = scaler.fit_transform(df)
        return scaled_df, scaler
    
    @staticmethod
    def apply_pca(df:pd.DataFrame, variance_level:float):
        """
        Apply PCA to the input DataFrame and retain components explaining the desired variance.

        Parameters:
            df (pd.DataFrame): Standardized input data.
            variance_level (float): Desired cumulative variance to retain (e.g., 0.95 for 95%).

        Returns:
            Tuple[np.ndarray, int, PCA, np.ndarray, pd.DataFrame]:
                - cumulative_variance: Cumulative explained variance ratio per component.
                - k: Number of components required to reach the desired variance level.
                - pca_final: Fitted PCA model with k components.
                - pca_scores: Transformed data (NumPy array).
                - df_pca_scores: Transformed data as DataFrame with named components.
        """
        pca_stand = PCA()
        principal_components = pca_stand.fit(df)

        cumulative_variance = np.cumsum(pca_stand.explained_variance_ratio_)
        k = np.argmax(cumulative_variance >= variance_level) + 1
        
        # TODO: calculate the actual variance per component and return the value (instead of cumulative variance?)
        pca_final = PCA(n_components=k)
        pca_scores= pca_final.fit_transform(df)
        df_pca_scores= pd.DataFrame(pca_scores, columns=[f"PC{int(col)}" for col in range(1,k+1)])  
        return    cumulative_variance, k, pca_final, pca_scores, df_pca_scores
    
    @staticmethod
    def calculate_t_test(df:pd.DataFrame) -> list:
        """
        Perform independent t-tests for each principal component between pain and no-pain groups.

        Groups are formed using the 'PRMD_ever' column (1 = pain, 0 = no pain).
        To avoid over-representation, mean scores per participant are used.

        Parameters:
            df (pd.DataFrame): DataFrame containing PC scores, 'participant_id', and 'PRMD_ever'.

        Returns:
            list: A list of lists, each containing:
                [target, axis, PC name, t-statistic, p-value]
        """
        pc_cols = df.iloc[:, 5:]
        target = df['target'][0]
        axis = df['axis'][0]
        t_test_result = []
        for pc in pc_cols:
            if not df[pc].isna().all():
                df_pc_mean_stand = df.groupby(['participant_id', 'PRMD_ever'])[pc].mean().reset_index()
                df_mean_pain = df_pc_mean_stand[df_pc_mean_stand['PRMD_ever'] == 1][pc]
                df_mean_nopain = df_pc_mean_stand[df_pc_mean_stand['PRMD_ever'] == 0][pc]
                
                t_stat, p_value = ttest_ind(df_mean_pain, df_mean_nopain, equal_var=False)
                t_test_result.append([target, axis, pc, t_stat, p_value])
        return t_test_result
        
    def rank_pcs(self,unique_target_axes:list, df:pd.DataFrame):
        """
        Rank principal components (PCs) based on their t-test effect size (absolute t-value)
        across all given (target, axis) combinations.

        Parameters:
            unique_target_axes (list): List of (target, axis) tuples to evaluate.
            df (pd.DataFrame): DataFrame containing PC scores and metadata.

        Returns:
            pd.DataFrame: DataFrame of t-test results sorted by absolute t-value, 
                        with columns: ['target', 'axis', 'PC', 't_value', 'p_value'].
        """
        #TODO: calculate and return the group mean and SD (pain/no pain) for each PC
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
        """
        Calculate mean waveforms (over time) for pain, no-pain, and overall across last 202 columns (e.g., PC scores).

        Parameters:
            df (pd.DataFrame): DataFrame with time series data and 'PRMD_ever' column.

        Returns:
            Tuple[np.ndarray, np.ndarray, np.ndarray]:
                - Mean waveform for pain group.
                - Mean waveform for no-pain group.
                - Overall mean waveform.
        """    
        df_matrix_pain = np.array(df[df['PRMD_ever'] == 1].iloc[:, -202:])
        df_matrix_no_pain = np.array(df[df['PRMD_ever'] == 0].iloc[:, -202:])
        df_matrix = np.array(df.iloc[:, -202:])
        
        mean_waveform_pain = np.mean(df_matrix_pain, axis=0)
        mean_waveform_no_pain = np.mean(df_matrix_no_pain, axis=0)
        mean_waveform = np.mean(df_matrix, axis=0)
        
        return mean_waveform_pain, mean_waveform_no_pain, mean_waveform
    
    @staticmethod
    def combine_half_strokes_to_full_cycles_mean_note(mean_note_waveforms):
        """
        Combine up/down half strokes into full bow stroke cycles, if each half has 101 time points.

        Assumes bow strokes are numbered consecutively: even = up, odd = down.

        Parameters:
            mean_note_waveforms (pd.DataFrame): DataFrame containing averaged waveform data
                with columns: ['target', 'axis', 'bow_stroke', 'up_down', 'time_point', 'mean_value'].

        Returns:
            pd.DataFrame: Combined full-stroke waveforms with columns:
                ['target', 'axis', 'bow_stroke', 'full_stroke', 'time_point', 'mean_value'].
        """        
        result_rows = []

        group = mean_note_waveforms.sort_values(["bow_stroke", "up_down"])

        full_stroke_index = 0

        for bs in range(0, 25, 2):  # step in twos to pair 0/1, 2/3, ...
            up_half = group[(group['bow_stroke'] == bs) & (group['up_down'] == 0)]
            down_half = group[(group['bow_stroke'] == bs + 1) & (group['up_down'] == 1)]

            if len(up_half) == 101 and len(down_half) == 101:
                combined = pd.concat([up_half, down_half], ignore_index=True)
                combined = combined.sort_values(["up_down", "time_point"]).reset_index(drop=True)
                combined["time_point"] = range(202)
                combined["full_stroke"] = full_stroke_index
                result_rows.append(combined[["target", "axis", 'bow_stroke', "full_stroke", "time_point",'mean_value']])
                full_stroke_index += 1

        return pd.concat(result_rows, ignore_index=True)
    

    def reconstruct_data(self, scaler, pca_scores, pc_idx, mean_note_waveforms, loading_vector):
        #scaler = StandardScaler()
        pc_scores = np.array(pca_scores[['PC1', 'PC2', 'PC3']])[:, pc_idx]
        recon_centered = np.outer(pc_scores, loading_vector)
        recon_orig = scaler.inverse_transform(recon_centered)
        reconstructed = recon_orig
        mean_note_waveforms = self.combine_half_strokes_to_full_cycles_mean_note(mean_note_waveforms)
        mean_lookup = {(row['full_stroke'], row['time_point']): row['mean_value']
                   for _, row in mean_note_waveforms.iterrows()}
        # TODO: check i can iterate over the samples effectively
        
        # iterate through the pc
        for sample_idx in range(0,len(recon_orig)):
            stroke_idx = sample_idx % 11
            for time_point_idx in range(0,202):
                #mean_value = mean_note_waveforms[(mean_note_waveforms['full_stroke'] == stroke_idx) & 
                #                                                                 (mean_note_waveforms['time_point'] == time_point_idx)]['mean_value']
                mean_value = mean_lookup.get((stroke_idx, time_point_idx), 0.0)
                reconstructed[sample_idx][time_point_idx] += mean_value
        df_reconstructed = pd.DataFrame(reconstructed)
        df_reconstructed = pd.concat([pca_scores, df_reconstructed], axis = 1)
        df_reconstructed.columns = ['participant_id', 'PRMD_ever', 'full_stroke', 'target', 'axis', 'PC1', 'PC2', 'PC3'] + [f"t{int(col)}" for col in df_reconstructed.columns[8:]]
        return df_reconstructed
    
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

    @staticmethod
    def calculate_lower_upper_band_unscaled(pc_scores, mean_waveform, loading_vector, scaler):
        lower_percentile =    np.percentile(pc_scores, 5)
        upper_percentile = np.percentile(pc_scores, 95)
        
        lower_band_scaled = lower_percentile * loading_vector
        upper_band_scaled = upper_percentile * loading_vector
        
        # Convert to 2D arrays (required by inverse_transform)
        lower_band_scaled_2d = lower_band_scaled.reshape(1, -1)
        upper_band_scaled_2d = upper_band_scaled.reshape(1, -1)
        
        lower_band_original = scaler.inverse_transform(lower_band_scaled_2d)[0]
        upper_band_original = scaler.inverse_transform(upper_band_scaled_2d)[0]
        
        lower_band = mean_waveform + lower_band_original
        upper_band = mean_waveform + upper_band_original
        
        
        return lower_band, upper_band
        


    def reconstruct_single_component(self, ranked_pc, pcs_final, pca_scores, df, scaler, mean_note_waveform):
        target = ranked_pc['target']
        axis = ranked_pc['axis']
        pc_idx = self.extract_PCA_index(ranked_pc['PC'])
        
        pca_target_axis = pcs_final[(pcs_final['target'] == target) & (pcs_final['axis'] == axis)]['PCA']
        loading_vector = pca_target_axis.iloc[0].components_[pc_idx]
        pc_scores = np.array(pca_scores[['PC1', 'PC2', 'PC3']])[:, pc_idx]
        
        # reconstruct data 
        df_reconstructed = self.reconstruct_data(scaler, pca_scores, pc_idx, mean_note_waveform, loading_vector)
        
        # update the mean calculation
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(df_reconstructed)       
        
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

    def reconstruct_single_component_old(self, ranked_pc, pcs_final, pca_scores, df, scaler):
        target = ranked_pc['target']
        axis = ranked_pc['axis']
        pc_idx = self.extract_PCA_index(ranked_pc['PC'])
        
        pca_target_axis = pcs_final[(pcs_final['target'] == target) & (pcs_final['axis'] == axis)]['PCA']
        loading_vector = pca_target_axis.iloc[0].components_[pc_idx]
        pc_scores = np.array(pca_scores[['PC1', 'PC2', 'PC3']])[:, pc_idx]
        
        # update the mean calculation
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(df)
        
        #lower_band, upper_band = self.calculate_lower_upper_band(pc_scores, mean_waveform_pain, loading_vector)
        lower_band, upper_band = self.calculate_lower_upper_band_unscaled(pc_scores, mean_waveform_pain, loading_vector, scaler)
        
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
        
    
    