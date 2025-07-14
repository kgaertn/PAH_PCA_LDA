    
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from factor_analyzer import Rotator
import re
import pandas as pd
import numpy as np

import scipy.stats as st

from models.pc_ranked import PC_Ranked

# TODO: check PCA requirements: double check outlier removal (and add an option to mark the outliers in the db)
# TODO: check PCA rotation

class PCAAnalyser:
    
    def __init__(self):
        """
        Initializes the PCA_Analyser with a data processor.
        """    
    
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
                
                t_stat, p_value = st.ttest_ind(df_mean_pain, df_mean_nopain, equal_var=False)
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
            std_pain = df_pain.std()
            std_nopain = df_nopain.std()
            
            t_stat, p_value = st.ttest_ind(df_pain, df_nopain, equal_var=False)
            t_test_result.append([target, axis, pc_id, pc_idx, t_stat, p_value, mean_pain, mean_nopain, std_pain, std_nopain])
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

        unique_target_axes = df[['target', 'axis']].drop_duplicates().values.tolist()
        df_pc_reduced = df[['participant_id', 'PRMD_ever', 'target', 'axis', 'pc_id', 'pc_index', 'pc_score']]
    
        t_test_total = []
        for target, axis in unique_target_axes:
            df_target_axis = df_pc_reduced[(df_pc_reduced["target"] == target) & (df_pc_reduced["axis"] == axis)]
            t_test_target_axis = self.calculate_t_test(df_target_axis)
            t_test_total.extend(t_test_target_axis)
        
        df_t_test = pd.DataFrame(t_test_total, columns = ['target', 'axis', 'pc_id', 'pc_index', 't_value', 'p_value', 'mean_pain', 'mean_no_pain', 'std_pain', 'std_no_pain'])
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
    

    #def reconstruct_data(self, scaler, pca_scores, pc_idx, mean_note_waveforms, loading_vector):
    #    """
    #    Reconstructs the original time-series data from PCA scores and a loading vector.
#
    #    Args:
    #        scaler (StandardScaler): Scaler used to inverse-transform the PCA reconstruction.
    #        pca_scores (pd.DataFrame): DataFrame containing PC1, PC2, and PC3 scores.
    #        pc_idx (int): Index of the principal component to reconstruct.
    #        mean_note_waveforms (pd.DataFrame): Mean waveform values per full stroke and time point.
    #        loading_vector (np.ndarray): Loading vector for the selected principal component.
#
    #    Returns:
    #        pd.DataFrame: A DataFrame containing the reconstructed time-series and associated metadata.
    #    """
    #    
    #    pc_scores = np.array(pca_scores[['PC1', 'PC2', 'PC3']])[:, pc_idx]
    #    recon_centered = np.outer(pc_scores, loading_vector)
    #    recon_orig = scaler.inverse_transform(recon_centered)
    #    reconstructed = recon_orig
    #    mean_note_waveforms = self.combine_half_strokes_to_full_cycles_mean_note(mean_note_waveforms)
    #    mean_lookup = {(row['full_stroke'], row['time_point']): row['mean_value']
    #               for _, row in mean_note_waveforms.iterrows()}
    #    # TODO: check how to iterate over the samples more effectively
#
    #    for sample_idx in range(0,len(recon_orig)):
    #        stroke_idx = sample_idx % 11
    #        for time_point_idx in range(0,202):
    #            mean_value = mean_lookup.get((stroke_idx, time_point_idx), 0.0)
    #            reconstructed[sample_idx][time_point_idx] += mean_value
    #    df_reconstructed = pd.DataFrame(reconstructed)
    #    df_reconstructed = pd.concat([pca_scores, df_reconstructed], axis = 1)
    #    df_reconstructed.columns = ['participant_id', 'PRMD_ever', 'full_stroke', 'target', 'axis', 'PC1', 'PC2', 'PC3'] + [f"t{int(col)}" for col in df_reconstructed.columns[8:]]
    #    return df_reconstructed
    
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
        
        loading_vector = np.array(PC_Ranked.list_from_json(pc_df['loading_vector'].unique()[0]))
        pc_scores_df_pain = pc_df[pc_df['PRMD_ever'] == 1]
        pc_scores_df_nopain = pc_df[pc_df['PRMD_ever'] == 0]
        pc_scores = np.array(pc_df['pc_score'])
        pc_scores_pain = np.array(pc_scores_df_pain['pc_score'])
        pc_scores_nopain = np.array(pc_scores_df_nopain['pc_score'])
        
        
        # update the mean calculation
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(orig_df)       
        
        lower_band_pain, upper_band_pain = self.calculate_lower_upper_band_unscaled(pc_scores_pain, mean_waveform_total, loading_vector, scaler)
        lower_band_nopain, upper_band_nopain = self.calculate_lower_upper_band_unscaled(pc_scores_nopain, mean_waveform_total, loading_vector, scaler)
        lower_band, upper_band = self.calculate_lower_upper_band_unscaled(pc_scores, mean_waveform_total, loading_vector, scaler)        
        
        return {
        "mean_waveform_pain": mean_waveform_pain,
        "mean_waveform_no_pain": mean_waveform_no_pain,
        "mean_waveform_total": mean_waveform_total,
        "loading_vector": loading_vector,
        "pc_scores": pc_scores,
        "lower_band": lower_band,
        "upper_band": upper_band,
        "lower_band_pain": lower_band_pain,
        "upper_band_pain": upper_band_pain,
        "lower_band_no_pain": lower_band_nopain,
        "upper_band_no_pain": upper_band_nopain
        }
        
# TODO: add pca rotation

    @staticmethod
    def rotate_pc_loadings(loading_vectors, method = 'varimax'):
        # pc scores: (n_samples, n_components)
        # loading_vectors: (n_components, n_features)
        # mean: (n_features,)
        # scale: (n_features,)
        rotator = Rotator(method = method)
        loadings = rotator.fit_transform(loading_vectors) # shape: (n_features, n_components)
        return loadings
    
    @staticmethod
    def rotate_pc_scores(df, scaler, rotated_loadings):
        # X_standardized: (n_samples, n_features)
        df_standardized = scaler.transform(df)
        rotated_scores = np.dot(df_standardized, rotated_loadings.T)  # (n_samples, n_components)
        return rotated_scores

    
    def reconstruct_rotated(self, orig_df, rotated_loadings, rotated_scores, scaler, reconstruct_per_group = False):
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(orig_df)  
        percentiles = []
        for i in range(rotated_scores.shape[1]):
            lower_percentile = np.percentile(rotated_scores[:, i], 5)
            upper_percentile = np.percentile(rotated_scores[:, i], 95)
            percentiles.append((lower_percentile, upper_percentile))
            
        for i, (lower_percentile, upper_percentile) in enumerate(percentiles):  
            if not reconstruct_per_group:
                rotated_loadings_unscaled = rotated_loadings[i] * scaler.scale_
                lower_band = mean_waveform_total + (lower_percentile * rotated_loadings_unscaled)
                upper_band = mean_waveform_total + (upper_percentile * rotated_loadings_unscaled)  
    
        
        
