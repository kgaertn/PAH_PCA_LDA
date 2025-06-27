    
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scipy.stats import ttest_ind
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from data_processing.data_preprocess import DataProcessor
from data_access.scaler_repo import ScalerRepository
from data_access.pc_ranked_repo import PCRankedRepository
from data_access.pc_scores_repository import PCScoresRepository
from models.scaler import Scaler
from models.pc_ranked import PC_Ranked
from models.pc_scores import PC_Scores

# TODO: check PCA requirements: correlations between features should be linear, data set should be free of outliers, variables should be continuous
# TODO: check PCA rotation
# TODO: create plots per group (PCA features per group)
class PCAAnalyser:
    
    def __init__(self):
        """
        Initializes the PCA_Analyser with a data processor.
        """    
        self.data_processor = DataProcessor()
        self.scaler_repo = ScalerRepository()
        self.pc_ranked_repo = PCRankedRepository()
        self.pc_scores_repo = PCScoresRepository()
    
    @staticmethod
    def standardize_df(df:pd.DataFrame) -> pd.DataFrame:
        """
        Standardize the input DataFrame using z-score normalization.

        Args:
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

        Args:
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
        pca_stand.fit(df)

        explained_variance = pca_stand.explained_variance_ratio_
        cumulative_variance = np.cumsum(pca_stand.explained_variance_ratio_)
        k = np.argmax(cumulative_variance >= variance_level) + 1
        
        # TODO: calculate the actual variance per component and return the value (instead of cumulative variance?)
        pca_final = PCA(n_components=k)
        pca_scores= pca_final.fit_transform(df)
        df_pca_scores= pd.DataFrame(pca_scores, columns=[f"PC{int(col)}" for col in range(1,k+1)])  
        return    explained_variance[:k], cumulative_variance[:k], k, pca_final, pca_scores, df_pca_scores

    @staticmethod
    def calculate_t_test_old(df:pd.DataFrame) -> list:
        """
        Perform independent t-tests for each principal component between pain and no-pain groups.

        Args:
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
       
    def rank_pcs_old(self, unique_target_axes:list, df:pd.DataFrame):
        """
        Rank principal components (PCs) based on their t-test effect size (absolute t-value)
        across all given (target, axis) combinations.

        Args:
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
            t_test_target_axis = self.calculate_t_test_old(df_target_axis)
            t_test_total.extend(t_test_target_axis)
        
        df_t_test = pd.DataFrame(t_test_total, columns = ['target', 'axis', 'PC', 't_value', 'p_value'])
        df_t_test_ranked = df_t_test.sort_values(by="t_value", key=lambda x: x.abs(), ascending=False).reset_index(drop=True)
        return df_t_test_ranked

    
    @staticmethod
    def calculate_t_test(df:pd.DataFrame) -> list:
        """
        Perform independent t-tests for each principal component between pain and no-pain groups.

        Args:
            df (pd.DataFrame): DataFrame containing PC scores, 'participant_id', and 'PRMD_ever'.

        Returns:
            list: A list of lists, each containing:
                [target, axis, PC name, t-statistic, p-value]
        """
        pc_ids = df['pc_id'].unique()
        #pc_cols = df.iloc[:, 5:]
        target = df['target'].iloc[0]
        axis = df['axis'].iloc[0]
        t_test_result = []
        # temp
        if (target, axis) == ('right ht joint angle','Y'):
            print("")
        for pc_id in pc_ids:
            #if not df[pc].isna().all():
            pc_idx = df[df['pc_id'] == pc_id]['pc_index'].iloc[0]
            df_pc_mean = df[df['pc_id'] == pc_id].groupby(['participant_id', 'PRMD_ever'])['pc_score'].mean().reset_index()
            df_pain = df_pc_mean[df_pc_mean['PRMD_ever'] == 1]['pc_score']
            df_nopain = df_pc_mean[df_pc_mean['PRMD_ever'] == 0]['pc_score']
            mean_pain = df_pain.mean()
            mean_nopain = df_nopain.mean()
            
            t_stat, p_value = ttest_ind(df_pain, df_nopain, equal_var=False)
            t_test_result.append([target, axis, pc_id, pc_idx, t_stat, p_value, mean_pain, mean_nopain])
        return t_test_result
       
    def rank_pcs(self, df:pd.DataFrame):
        """
        Rank principal components (PCs) based on their t-test effect size (absolute t-value)
        across all given (target, axis) combinations.

        Args:
            unique_target_axes (list): List of (target, axis) tuples to evaluate.
            df (pd.DataFrame): DataFrame containing PC scores and metadata.

        Returns:
            pd.DataFrame: DataFrame of t-test results sorted by absolute t-value, 
                        with columns: ['target', 'axis', 'PC', 't_value', 'p_value'].
        """
        #TODO: calculate and return the group mean and SD (pain/no pain) for each PC
        unique_target_axes = df[['target', 'axis']].drop_duplicates().values.tolist()
        df_pc_reduced = df[['participant_id', 'PRMD_ever', 'target', 'axis', 'pc_id', 'pc_index', 'pc_score']]
    
        t_test_total = []
        for target, axis in unique_target_axes:
            df_target_axis = df_pc_reduced[(df_pc_reduced["target"] == target) & (df_pc_reduced["axis"] == axis)]
            t_test_target_axis = self.calculate_t_test(df_target_axis)
            t_test_total.extend(t_test_target_axis)
        
        df_t_test = pd.DataFrame(t_test_total, columns = ['target', 'axis', 'pc_id', 'pc_index', 't_value', 'p_value', 'mean_pain', 'mean_no_pain'])
        df_t_test_ranked = df_t_test.sort_values(by="t_value", key=lambda x: x.abs(), ascending=False).reset_index(drop=True)
        return df_t_test_ranked

    @staticmethod
    def calculate_mean_waveform_target_axis(df):
        """
        Calculate mean waveforms (over time) for pain, no-pain, and overall across last 202 columns (e.g., PC scores).

        Args:
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
    
    #TODO: see if i can adjust the half_strokes function, to avoid doubling
    @staticmethod
    def combine_half_strokes_to_full_cycles_mean_note(mean_note_waveforms):
        """
        Combine up/down half strokes into full bow stroke cycles, if each half has 101 time points.

        Assumes bow strokes are numbered consecutively: even = up, odd = down.

        Args:
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
        """
        Reconstructs the original time-series data from PCA scores and a loading vector.

        Args:
            scaler (StandardScaler): Scaler used to inverse-transform the PCA reconstruction.
            pca_scores (pd.DataFrame): DataFrame containing PC1, PC2, and PC3 scores.
            pc_idx (int): Index of the principal component to reconstruct.
            mean_note_waveforms (pd.DataFrame): Mean waveform values per full stroke and time point.
            loading_vector (np.ndarray): Loading vector for the selected principal component.

        Returns:
            pd.DataFrame: A DataFrame containing the reconstructed time-series and associated metadata.
        """
        
        pc_scores = np.array(pca_scores[['PC1', 'PC2', 'PC3']])[:, pc_idx]
        recon_centered = np.outer(pc_scores, loading_vector)
        recon_orig = scaler.inverse_transform(recon_centered)
        reconstructed = recon_orig
        mean_note_waveforms = self.combine_half_strokes_to_full_cycles_mean_note(mean_note_waveforms)
        mean_lookup = {(row['full_stroke'], row['time_point']): row['mean_value']
                   for _, row in mean_note_waveforms.iterrows()}
        # TODO: check how to iterate over the samples effectively

        for sample_idx in range(0,len(recon_orig)):
            stroke_idx = sample_idx % 11
            for time_point_idx in range(0,202):
                mean_value = mean_lookup.get((stroke_idx, time_point_idx), 0.0)
                reconstructed[sample_idx][time_point_idx] += mean_value
        df_reconstructed = pd.DataFrame(reconstructed)
        df_reconstructed = pd.concat([pca_scores, df_reconstructed], axis = 1)
        df_reconstructed.columns = ['participant_id', 'PRMD_ever', 'full_stroke', 'target', 'axis', 'PC1', 'PC2', 'PC3'] + [f"t{int(col)}" for col in df_reconstructed.columns[8:]]
        return df_reconstructed
    
    @staticmethod
    def extract_PCA_index(pca_name:str):
        """
        Extracts the zero-based index from a PCA component name string (e.g., 'PC1' → 0).

        Args:
            pca_name (str): The name of the PCA component (e.g., 'PC1', 'PC2').

        Returns:
            int | None: The extracted zero-based index, or None if no numeric component found.
        """
        number = re.search(r"\d+", pca_name)
        if number:
            return int(number.group())-1
        return None
    
    @staticmethod
    def calculate_lower_upper_band(pc_scores, mean_waveform, loading_vector):
        """
        Calculates the lower (5th percentile) and upper (95th percentile) bands of the 
        reconstructed waveform based on PCA scores and loading vector.

        Args:
            pc_scores (np.ndarray): The PCA scores for a single component.
            mean_waveform (np.ndarray): The mean waveform to which variation is added.
            loading_vector (np.ndarray): Loading vector used to scale the variation.

        Returns:
            tuple[np.ndarray, np.ndarray]: Arrays representing the lower and upper waveform bands.
        """
        lower_percentile =    np.percentile(pc_scores, 5)
        upper_percentile = np.percentile(pc_scores, 95)
        
        lower_band = mean_waveform + lower_percentile * loading_vector
        upper_band = mean_waveform + upper_percentile * loading_vector
        
        return lower_band, upper_band
    @staticmethod
    def calculate_lower_upper_band_unscaled(pc_scores, mean_waveform, loading_vector, scaler):
        """
        Calculates the lower and upper reconstruction bands in the original data scale 
        using inverse transformation of scaled loadings.

        Args:
            pc_scores (np.ndarray): The PCA scores for a single component.
            mean_waveform (np.ndarray): The mean waveform to which variation is added.
            loading_vector (np.ndarray): Loading vector used to scale the variation.
            scaler (StandardScaler): Scaler used to inverse-transform the waveform.

        Returns:
            tuple[np.ndarray, np.ndarray]: Arrays representing the lower and upper waveform bands.
        """
        
        lower_percentile =    np.percentile(pc_scores, 5)
        upper_percentile = np.percentile(pc_scores, 95)
                
        loading_vector_orig = loading_vector * scaler.scale_
        

        lower_band = mean_waveform + loading_vector_orig * lower_percentile
        upper_band = mean_waveform + loading_vector_orig * upper_percentile
        
        
        return lower_band, upper_band
    @staticmethod
    def calculate_lower_upper_band_unscaled_old(pc_scores, mean_waveform, loading_vector, scaler):
        """
        Calculates the lower and upper reconstruction bands in the original data scale 
        using inverse transformation of scaled loadings.

        Args:
            pc_scores (np.ndarray): The PCA scores for a single component.
            mean_waveform (np.ndarray): The mean waveform to which variation is added.
            loading_vector (np.ndarray): Loading vector used to scale the variation.
            scaler (StandardScaler): Scaler used to inverse-transform the waveform.

        Returns:
            tuple[np.ndarray, np.ndarray]: Arrays representing the lower and upper waveform bands.
        """
        
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
        
    def reconstruct_single_component(self, pc_df, orig_df, scaler):
        """
        Reconstructs a waveform from a single principal component and calculates corresponding
        waveform bands and summary statistics.

        Args:
            ranked_pc (pd.Series): Metadata describing the selected PCA component (e.g., PC, target, axis).
            pcs_final (pd.DataFrame): DataFrame containing trained PCA models per target and axis.
            pca_scores (pd.DataFrame): PCA score data used for reconstruction.
            df (pd.DataFrame): Original input data used for mean waveform calculation.
            scaler (StandardScaler): Scaler used for inverse-transforming PCA outputs.
            mean_note_waveform (pd.DataFrame): DataFrame with mean waveforms used in reconstruction.

        Returns:
            dict: A dictionary containing the reconstructed waveform, loading vector, PCA scores,
                and the lower and upper reconstruction bands.
        """
        
        target = pc_df['target'].unique()[0]
        axis = pc_df['axis'].unique()[0]
        pc_idx = pc_df['pc_index'].unique()[0]
        
        loading_vector = np.array(PC_Ranked.list_from_json(pc_df['loading_vector'].unique()[0]))
        pc_scores = np.array(pc_df['pc_score'])
        #pc_idx = self.extract_PCA_index(ranked_pc['PC'])
        
        #pca_target_axis = pcs_final[(pcs_final['target'] == target) & (pcs_final['axis'] == axis)]['PCA']
        #loading_vector = pca_target_axis.iloc[0].components_[pc_idx]
        #pc_scores = np.array(pca_scores[['PC1', 'PC2', 'PC3']])[:, pc_idx]
        
        # update the mean calculation
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(orig_df)       
        
        lower_band, upper_band = self.calculate_lower_upper_band_unscaled(pc_scores, mean_waveform_total, loading_vector, scaler)
        
        return {
        "mean_waveform_pain": mean_waveform_pain,
        "mean_waveform_no_pain": mean_waveform_no_pain,
        "mean_waveform_total": mean_waveform_total,
        "loading_vector": loading_vector,
        "pc_scores": pc_scores,
        "lower_band": lower_band,
        "upper_band": upper_band
        }

    #def reconstruct_single_component(self, ranked_pc, pcs_final, pca_scores, df, scaler, mean_note_waveform):
    #    """
    #    Reconstructs a waveform from a single principal component and calculates corresponding
    #    waveform bands and summary statistics.
#
    #    Args:
    #        ranked_pc (pd.Series): Metadata describing the selected PCA component (e.g., PC, target, axis).
    #        pcs_final (pd.DataFrame): DataFrame containing trained PCA models per target and axis.
    #        pca_scores (pd.DataFrame): PCA score data used for reconstruction.
    #        df (pd.DataFrame): Original input data used for mean waveform calculation.
    #        scaler (StandardScaler): Scaler used for inverse-transforming PCA outputs.
    #        mean_note_waveform (pd.DataFrame): DataFrame with mean waveforms used in reconstruction.
#
    #    Returns:
    #        dict: A dictionary containing the reconstructed waveform, loading vector, PCA scores,
    #            and the lower and upper reconstruction bands.
    #    """
    #    
    #    target = ranked_pc['target']
    #    axis = ranked_pc['axis']
    #    pc_idx = self.extract_PCA_index(ranked_pc['PC'])
    #    
    #    pca_target_axis = pcs_final[(pcs_final['target'] == target) & (pcs_final['axis'] == axis)]['PCA']
    #    loading_vector = pca_target_axis.iloc[0].components_[pc_idx]
    #    pc_scores = np.array(pca_scores[['PC1', 'PC2', 'PC3']])[:, pc_idx]
    #    
    #    # reconstruct data 
    #    df_reconstructed = self.reconstruct_data(scaler, pca_scores, pc_idx, mean_note_waveform, loading_vector)
    #    
    #    # update the mean calculation
    #    mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(df_reconstructed)       
    #    
    #    lower_band, upper_band = self.calculate_lower_upper_band(pc_scores, mean_waveform_pain, loading_vector)
    #    
    #    return {
    #    "mean_waveform_pain": mean_waveform_pain,
    #    "mean_waveform_no_pain": mean_waveform_no_pain,
    #    "mean_waveform_total": mean_waveform_total,
    #    "loading_vector": loading_vector,
    #    "pc_scores": pc_scores,
    #    "lower_band": lower_band,
    #    "upper_band": upper_band
    #    }

    def reconstruct_single_component_old(self, ranked_pc, pcs_final, pca_scores, df, scaler):
        """
        Reconstructs a waveform from a single principal component without using 
        pre-combined mean waveforms.

        Args:
            ranked_pc (pd.Series): Metadata describing the selected PCA component (e.g., PC, target, axis).
            pcs_final (pd.DataFrame): DataFrame containing trained PCA models per target and axis.
            pca_scores (pd.DataFrame): PCA score data used for reconstruction.
            df (pd.DataFrame): Original input data used for mean waveform calculation.
            scaler (StandardScaler): Scaler used for inverse-transforming PCA outputs.

        Returns:
            dict: A dictionary containing the reconstructed waveform, loading vector, PCA scores,
                and the lower and upper reconstruction bands (in original scale).
        """
        
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
        """
        Plots the reconstructed mean waveforms with percentile bands and the corresponding 
        loading vector of a PCA component.

        Args:
            component_data (dict): Output dictionary from a reconstruction method containing waveform and PCA info.
            title_waveform (str): Title for the mean waveform plot.
            title_loading (str): Title for the loading vector plot.

        Returns:
            tuple[matplotlib.figure.Figure, list[matplotlib.axes._axes.Axes]]: The figure and axes objects for further customization or saving.
        """
        
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
        plt.close()
        print(f"Plot saved to {full_filename}")
    
    @staticmethod
    def plot_linearity(df, fig_title):
        from pandas.plotting import scatter_matrix
        from pandas.plotting import lag_plot
        # Assume 'data' is a DataFrame of shape [n_movements, 202] (flattened over all participants/movements)
        # For demonstration, select every 20th timepoint
        subset = df.iloc[:, 6::20]
        fig1 = plt.figure(figsize=(12, 12))
        scatter_matrix(subset, ax=fig1.add_subplot(111))
        plt.suptitle(f'Scatter Matrix of Selected Timepoints: {fig_title}')
        plt.tight_layout()
        #plt.show()

        # Lag Plot
        #flattened = pd.Series(np.ravel(df))

        #fig2 = plt.figure(figsize=(6, 6))
        #lag_plot(flattened, lag=1)
        #plt.title('Lag Plot (lag=1) for All Samples Combined')
        #plt.xlabel('Value at time t') 
        #plt.ylabel('Value at time t+1')
        #plt.tight_layout()
        #plt.show()
        
        fig2 = plt.figure(figsize=(6, 6))
        unique_participants = df['participant_id'].unique()
        for i, participant in enumerate(unique_participants):  # plot first 5 samples
            df_part = df[df['participant_id'] == participant].iloc[:, -202:]
            flattened = pd.Series(np.ravel(df_part))
            lag_plot(flattened, lag=1, alpha=0.4, c=plt.cm.tab20(i), label=f'Sample {i+1}')
            #lag_plot(df.iloc[i, :], lag=1, alpha=0.4, c=plt.cm.tab20(i), label=f'Sample {i+1}')
        plt.title('Lag Plots for all participants Samples')
        plt.xlabel('Value at time t') 
        plt.ylabel('Value at time t+1')
        plt.legend()
        #plt.show()
        
        return fig1, fig2
    
    @staticmethod    
    def sliding_window_outlier_detection(df, window=10, threshold=3):
        # Sliding window (window size = 10) for datapoints t1 - t200
        #data
        df_outliers_adj = df.copy()
        count_outliers = 0
        
        unique_participants = df['participant_id'].unique()
        for participant in unique_participants:
            df_part = df[df['participant_id']==participant]
            part_row_idx = 0
            for idx, row in df_part.iloc[:, -202:].iterrows():
                series = row.values
                for i in range(len(series)):
                    # Skip first and last datapoints
                    if i == 0 or i == len(series) - 1:
                        continue
                    start = max(0, i - window // 2)
                    end = min(len(series), i + window // 2 + 1)
                    window_vals = np.delete(series[start:end], np.where(np.arange(start, end) == i - start))
                    mean = np.mean(window_vals)
                    std = np.std(window_vals)
                    if std > 0 and abs(series[i] - mean) > threshold * std:
                        df_outliers_adj.at[idx, df.columns[i+5]] = df[df.columns[i+5]].mean()
                        count_outliers += 1
                # Calculate group mean for t0 and t201, as sliding window is unstable at the edges
                # compare value to group mean to decide if it's an outlier
                first_col_mean = df_part.drop(index=idx).iloc[:, -202].mean()
                first_col_std = df_part.drop(index=idx).iloc[:, -202].std()
                if abs(df_part.iloc[part_row_idx, -202] - first_col_mean) >= threshold * first_col_std:
                    df_outliers_adj.iloc[idx, -202] = df_part.iloc[:, -202].mean()
                    count_outliers += 1
                # For last timepoint (tN)
                last_col_mean = df_part.drop(index=idx).iloc[:, -1].mean()
                last_col_std = df_part.drop(index=idx).iloc[:, -1].std()
                if abs(df_part.iloc[part_row_idx, -202] - last_col_mean) >= threshold * last_col_std:
                    df_outliers_adj.iloc[idx, -1] = df_part.iloc[:, -1].mean()
                    count_outliers += 1
                part_row_idx += 1

                
        return df_outliers_adj, count_outliers
    
    def upload_scaler(self, meas_type_id, scaler_type, mean, scale):
        """"""
        scaler = Scaler(
            id = 1,
            measurement_type_id=meas_type_id,
            scaler_type=scaler_type,
            mean=mean,
            scale=scale
        )
        scaler_id = self.scaler_repo.insert_new_scaler(scaler)
        return scaler_id
    
    def upload_pca(self, meas_type_id, pc_index, loading_vector, explained_variance, data_scaled):
        """"""
        pc = PC_Ranked(
            id = 1,
            measurement_type_id=meas_type_id,
            pc_index=pc_index,
            loading_vector=loading_vector,
            explained_variance=explained_variance,
            data_scaled=data_scaled
        )
        pc_id = self.pc_ranked_repo.insert_new_pc(pc)
        return pc_id
    
    def upload_pc_scores(self, pc_id, sample_scores):
        """"""
        pc_scores = []
        for sample, score in sample_scores:
            pc_scores.append(PC_Scores(
                id = 1,
                pc_id=pc_id,
                sample_id=sample,
                pc_score=score
            ))
        self.pc_scores_repo.insert_multiple_pc_scores(pc_scores)
        
    def load_pc_data(self, exp_id, device):
        df = self.pc_scores_repo.get_pc_scores_by_exp_id_device(exp_id, device)
        return df
    
    def load_pcs_by_rank(self, exp_id, device, min_rank, max_rank):
        df = self.pc_scores_repo.get_pc_scores_by_rank(exp_id, device, min_rank, max_rank)
        return df
    
    def upload_t_test_results(self, df):
        t_test_results = []
        for idx, row in df.iterrows():
            t_test_results.append(PC_Ranked(
                id = row['pc_id'],
                measurement_type_id= 1,
                rank = idx + 1,
                group_mean_pain=row['mean_pain'],
                group_mean_no_pain=row['mean_no_pain'],
                t_value=row['t_value'],
                p_value=row['p_value']                
            ))
        self.pc_ranked_repo.update_multiple_t_test_info(t_test_results)
   
    def load_specific_scaler(self, meas_type_id):
        return self.scaler_repo.get_sacler_by_meas_type_id(meas_type_id)         
        
    #def sliding_window_outlier_detection(df, window=10, threshold=3):
    #    # Create a copy to avoid modifying the original DataFrame
    #    outlier_mask = pd.DataFrame(True, index=df.index, columns=df.columns)
    #    
    #    for idx, row in df.iterrows():
    #        series = row.values
    #        for i in range(len(series)):
    #            # Define window, excluding the current point
    #            start = max(0, i - window // 2)
    #            end = min(len(series), i + window // 2 + 1)
    #            window_vals = np.delete(series[start:end], np.where(np.arange(start, end) == i - start))
    #            mean = np.mean(window_vals)
    #            std = np.std(window_vals)
    #            if std > 0 and abs(series[i] - mean) > threshold * std:
    #                outlier_mask.at[idx, df.columns[i]] = False
    #    return outlier_mask
        
    
    