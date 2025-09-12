from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from factor_analyzer import Rotator
import re
import pandas as pd
import numpy as np
import scipy.stats as st

from data_access.models.pc_ranked import PC_Ranked

# TODO: check PCA requirements: double check outlier removal (and add an option to mark the outliers in the db)
# TODO: check PCA rotation

class PCAAnalysis:
    
    def __init__(self):
        """
        Initializes the PCA_Analyser.
        """    
    
    @staticmethod
    def standardize_df(df:pd.DataFrame) -> tuple[np.ndarray, StandardScaler]:
        """
        Standardize numeric DataFrame columns via z-score normalization.

        Args:
            df (pd.DataFrame): Input numeric data.

        Returns:
            Tuple[np.ndarray, StandardScaler]: Standardized data and scaler.
        """
        scaler = StandardScaler()
        scaled_df = scaler.fit_transform(df)
        return scaled_df, scaler
    
    @staticmethod
    def apply_pca(df:pd.DataFrame, variance_level:float):
        """
        Apply PCA to the input DataFrame and retain components explaining the desired variance.

        Args:
        df (pd.DataFrame): Standardized data.
        variance_level (float): Target cumulative variance (e.g., 0.95).

        Returns:
            Tuple[np.ndarray, np.ndarray, int, PCA, np.ndarray, pd.DataFrame]: 
            Explained variance, cumulative variance, number of components, PCA model, scores array, and scores DataFrame.
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
        df (pd.DataFrame): DataFrame with PC scores and group labels.

        Returns:
            list: t-test results with statistics for each principal component.
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
       
    def rank_pcs_old(self, unique_target_axes:list, df:pd.DataFrame) -> pd.DataFrame:
        """
        Rank principal components by absolute t-test statistics across targets and axes.

        Args:
            unique_target_axes (list): List of (target, axis) tuples.
            df (pd.DataFrame): DataFrame with PC scores and metadata.

        Returns:
            pd.DataFrame: Sorted t-test results by absolute t-value.
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
        df (pd.DataFrame): DataFrame with PC scores, participant IDs, and PRMD_ever.

        Returns:
            list: Lists with [target, axis, PC ID, PC index, t-stat, p-val, means, stds].
        """
        pc_ids = df['pc_id'].unique()
        target = df['target'].iloc[0]
        axis = df['axis'].iloc[0]
        t_test_result = []

        for pc_id in pc_ids:
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
       
    def rank_pcs(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        Rank PCs by absolute t-test statistic across all target-axis pairs.

        Args:
            df (pd.DataFrame): DataFrame with PC scores and metadata.

        Returns:
            pd.DataFrame: t-test results sorted by absolute t-value.
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
    def calculate_mean_waveform_target_axis(df:pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Compute mean waveforms over last 202 columns (values) for pain, no-pain, and all data.

        Args:
            df (pd.DataFrame): DataFrame with 'PRMD_ever' and time series columns.

        Returns:
            tuple: Mean waveforms for pain, no-pain, and overall groups.
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
    def combine_half_strokes_to_full_cycles_mean_note(mean_note_waveforms:pd.DataFrame) -> pd.DataFrame:
        """
        Combine consecutive up/down half strokes into full stroke cycles.

        Args:
            mean_note_waveforms (pd.DataFrame): DataFrame with half stroke waveforms.

        Returns:
            pd.DataFrame: DataFrame with combined full stroke waveforms.
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
    
    @staticmethod
    def extract_PCA_index(pca_name:str) -> int | None:
        """
        Extract zero-based index from a PCA component name.

        Args:
            pca_name (str): PCA component name (e.g., 'PC1').

        Returns:
            int | None: Zero-based index or None if not found.
        """
        number = re.search(r"\d+", pca_name)
        if number:
            return int(number.group())-1
        return None
    
    @staticmethod
    def calculate_lower_upper_band(pc_scores:np.ndarray, mean_waveform:np.ndarray, loading_vector:np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """
        Compute 5th and 95th percentile bands of reconstructed waveform.

        Args:
            pc_scores (np.ndarray): PCA scores of one component.
            mean_waveform (np.ndarray): Mean waveform.
            loading_vector (np.ndarray): Loading vector for variation.

        Returns:
            tuple[np.ndarray, np.ndarray]: Lower and upper waveform bands.
        """
        lower_percentile =    np.percentile(pc_scores, 5)
        upper_percentile = np.percentile(pc_scores, 95)
        
        lower_band = mean_waveform + lower_percentile * loading_vector
        upper_band = mean_waveform + upper_percentile * loading_vector
        
        return lower_band, upper_band
    
    @staticmethod
    def calculate_lower_upper_band_unscaled(pc_scores:np.ndarray, mean_waveform:np.ndarray, loading_vector:np.ndarray, 
                                            scaler:StandardScaler) -> tuple[np.ndarray, np.ndarray]:
        """
        Compute 5th and 95th percentile bands in original scale using inverse scaling.

        Args:
            pc_scores (np.ndarray): PCA scores of one component.
            mean_waveform (np.ndarray): Mean waveform.
            loading_vector (np.ndarray): Loading vector.
            scaler (StandardScaler): Scaler for inverse transform.

        Returns:
            tuple[np.ndarray, np.ndarray]: Lower and upper waveform bands.
        """
        
        lower_percentile =    np.percentile(pc_scores, 5)
        upper_percentile = np.percentile(pc_scores, 95)
                
        loading_vector_orig = loading_vector * scaler.scale_
        

        lower_band = mean_waveform + loading_vector_orig * lower_percentile
        upper_band = mean_waveform + loading_vector_orig * upper_percentile
        
        
        return lower_band, upper_band
        
    def reconstruct_single_component(self, pc_df:pd.DataFrame, orig_df:pd.DataFrame, scaler:StandardScaler) -> dict:
        """
        Reconstruct waveform and bands from one principal component.

        Args:
            pc_df (pd.DataFrame): Data for the selected PC including scores and metadata.
            orig_df (pd.DataFrame): Original data for mean waveform calculation.
            scaler (StandardScaler): Scaler for inverse transformation.

        Returns:
            dict: Contains mean waveforms, loading vector, PC scores, and lower/upper bands.
        """
        
        loading_vector = np.array(PC_Ranked.list_from_json(pc_df['loading_vector'].unique()[0]))
        pc_scores_df_pain = pc_df[pc_df['PRMD_ever'] == 1]
        pc_scores_df_nopain = pc_df[pc_df['PRMD_ever'] == 0]
        pc_scores = np.array(pc_df['pc_score'])
        pc_scores_pain = np.array(pc_scores_df_pain['pc_score'])
        pc_scores_nopain = np.array(pc_scores_df_nopain['pc_score'])
        
        mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(orig_df)       
        
        lower_band_pain, upper_band_pain = self.calculate_lower_upper_band_unscaled(pc_scores_pain, mean_waveform_pain, loading_vector, scaler)
        lower_band_nopain, upper_band_nopain = self.calculate_lower_upper_band_unscaled(pc_scores_nopain, mean_waveform_no_pain, loading_vector, scaler)
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

    @staticmethod
    def rotate_pc_loadings(loading_vectors:np.ndarray, method:str = 'varimax') -> np.ndarray:
        """
        Rotate PCA loading vectors using specified method.

        Args:
            loading_vectors (np.ndarray): PCA loadings (components x features).
            method (str): Rotation method, e.g., 'varimax'.

        Returns:
            np.ndarray: Rotated loading vectors.
        """
        rotator = Rotator(method = method)
        loadings = rotator.fit_transform(loading_vectors)
        return loadings
    
    @staticmethod
    def rotate_pc_scores(df:pd.DataFrame, scaler:StandardScaler, rotated_loadings:np.ndarray)-> np.ndarray:
        """
        Compute rotated PCA scores by applying rotated loadings to standardized data.

        Args:
            df (pd.DataFrame): Original data.
            scaler (StandardScaler): Fitted scaler for standardization.
            rotated_loadings (np.ndarray): Rotated loading vectors.

        Returns:
            np.ndarray: Rotated PCA scores (samples x components).
        """
        df_standardized = scaler.transform(df)
        rotated_scores = np.dot(df_standardized, rotated_loadings.T)
        return rotated_scores 
    
        
        
