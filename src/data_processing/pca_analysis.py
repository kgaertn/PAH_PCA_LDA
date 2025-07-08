    
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
#from scipy.stats import ttest_ind
#from scipy.stats import ttest_ind
from factor_analyzer import calculate_bartlett_sphericity
from factor_analyzer.factor_analyzer import calculate_kmo
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as st
from pathlib import Path

from data_processing.data_preprocess import DataProcessor
from data_access.scaler_repo import ScalerRepository
from data_access.pc_ranked_repo import PCRankedRepository
from data_access.pc_scores_repository import PCScoresRepository
from models.scaler import Scaler
from models.pc_ranked import PC_Ranked
from models.pc_scores import PC_Scores

# TODO: check PCA requirements: double check outlier removal (and add an option to mark the outliers in the db)
# TODO: check PCA rotation

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

    def check_t_test_assumptions(self, df):
        """"""
        #pc_ids = df['pc_id'].drop_duplicates().values.tolist()
        #for pc_

        df_mean = df.groupby(['target', 'axis', 'pc_index', 'pc_id', 'participant_id', 'PRMD_ever'])['pc_score'].mean().reset_index()
        #self.kolmogorov_smirnov_test(df_mean)
        
        distributions_plotted = True
        
        target_axes = df[['target', 'axis', 'pc_index', 'pc_id']].drop_duplicates().values.tolist()
        pc_distribution_results = []
        for target, axis, pc_index, pc_id in target_axes:
            stat_pain, p_pain, stat_nopain, p_nopain = self.shapiro_wilk_test(df_mean, target, axis, pc_index)
            distribution_info = 'normal_distribution' if p_pain > 0.05 and p_nopain > 0.05 else 'non_normal_distribution'
            pc_distribution_results.append(PC_Ranked(
                id = pc_id,
                measurement_type_id= 1,
                distribution_info = distribution_info,
                shap_wilk_w_pain=stat_pain,
                shap_wilk_w_no_pain=stat_nopain,
                shap_wilk_p_pain=p_pain,
                shap_wilk_p_no_pain=p_nopain ))
            if not distributions_plotted:
                df_target_axis = df[(df['target'] == target) & (df['axis'] == axis) & (df['pc_index'] == pc_index)]['pc_score']        
                fig = self.plot_distribution(df_target_axis, 'pc_score', target, axis, pc_index, )
                self.save_distribution_plot(fig, target, axis, pc_index)
        print("")
        return pc_distribution_results
        # 
        
    def upload_distribution_info(self, pc_distributions):
        """"""
        self.pc_ranked_repo.update_pc_score_distribution_info(pc_distributions)
        
    @staticmethod    
    def plot_distribution(df, column,target, axis, pc_index, kind='hist', bins=10, **kwargs):
        """
        Plot the distribution of a DataFrame column.

        Parameters:
        - df: pandas DataFrame
        - column: str, column name to plot
        - kind: 'hist' for histogram, 'kde' for density plot
        - bins: int, number of bins (used for histogram)
        - **kwargs: additional keyword arguments for plot customization
        """
        fig, ax = plt.subplots()
        if kind == 'hist':
            df.plot(kind='hist', bins=bins, edgecolor='black', ax = ax, **kwargs)
            plt.xlabel(column)
            plt.ylabel('Frequency')
            plt.title(f'Histogram of PC Scores: {target}, {axis}, PC {pc_index}')
        elif kind == 'kde':
            df.plot(kind='kde', ax = ax, **kwargs)
            plt.xlabel(column)
            plt.ylabel('Density')
            plt.title(f'KDE of PC Scores: {target}, {axis}, PC {pc_index}')
        else:
            raise ValueError("kind must be 'hist' or 'kde'")
        return fig
    
    def save_distribution_plot(self, fig, target, axis, pc_index):
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots" / "Histograms"
        self.save_plot(fig, output_path, f"PC-Scores_Histogram_{target}_{axis}_PC_{pc_index}")
        plt.close()
    
    @staticmethod
    def kolmogorov_smirnov_test(df):
        target_axes = df[['target', 'axis', 'pc_index']].drop_duplicates().values.tolist()
        diff_target_axes = 0
        for target, axis, pc_index in target_axes:
            df_target_axis = df[(df['target'] == target) & (df['axis'] == axis) & (df['pc_index'] == pc_index)]['pc_score']
            #pain_group = df[(df['PRMD_ever'] == 1) & (df['target'] == target) & (df['axis'] == axis) & (df['pc_index'] == pc_index)]['pc_score']
            #nopain_group = df[(df['PRMD_ever'] == 0) & (df['target'] == target) & (df['axis'] == axis) & (df['pc_index'] == pc_index)]['pc_score']
            statistic, p_value = st.kstest(df_target_axis, 'norm')
            #statistic, p_value = st.ks2test(pain_group, nopain_group)
            if p_value <= 0.05:
                print(f"Kolmogorov-Smirnov: {target}, {axis}, PC {pc_index}: Statistic: {statistic}, p-value: {p_value}")
                diff_target_axes += 1
        print(f'Total count different distributions: {diff_target_axes}' )
            
    
    @staticmethod    
    def shapiro_wilk_test(df, target, axis, pc_index):
        #pain_group = df[df['PRMD_ever'] == 1]
        #nopain_group = df[df['PRMD_ever'] == 0]     
        #target_axes = df[['target', 'axis', 'pc_index']].drop_duplicates().values.tolist()
        #diff_target_axes_pain = 0
        #diff_target_axes_nopain = 0
        #for target, axis, pc_index in target_axes:
        pain_group = df[(df['PRMD_ever'] == 1) & (df['target'] == target) & (df['axis'] == axis) & (df['pc_index'] == pc_index)]['pc_score']
        nopain_group = df[(df['PRMD_ever'] == 0) & (df['target'] == target) & (df['axis'] == axis) & (df['pc_index'] == pc_index)]['pc_score']
        stat_pain, p_pain = st.shapiro(pain_group)
        stat_nopain, p_nopain = st.shapiro(nopain_group)
        return stat_pain, p_pain, stat_nopain, p_nopain
        #if p_pain <= 0.05:
        #    print(f"Shapiro-Wilk Pain: {target}, {axis}, PC {pc_index}: Statistic: {stat_pain}, p-value: {p_pain}")
        #    diff_target_axes_pain += 1
        #if p_nopain <= 0.05:
        #    print(f"Shapiro-Wilk No Pain: {target}, {axis}, PC {pc_index}: Statistic: {stat_nopain}, p-value: {p_nopain}")
        #    diff_target_axes_nopain += 1
        #print(f'Total count different distributions: Pain {diff_target_axes_pain} | no Pain {diff_target_axes_nopain}' )
        
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
        # TODO: check how to iterate over the samples more effectively

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
    #@staticmethod
    #def calculate_lower_upper_band_unscaled_old(pc_scores, mean_waveform, loading_vector, scaler):
    #    """
    #    Calculates the lower and upper reconstruction bands in the original data scale 
    #    using inverse transformation of scaled loadings.
#
    #    Args:
    #        pc_scores (np.ndarray): The PCA scores for a single component.
    #        mean_waveform (np.ndarray): The mean waveform to which variation is added.
    #        loading_vector (np.ndarray): Loading vector used to scale the variation.
    #        scaler (StandardScaler): Scaler used to inverse-transform the waveform.
#
    #    Returns:
    #        tuple[np.ndarray, np.ndarray]: Arrays representing the lower and upper waveform bands.
    #    """
    #    
    #    lower_percentile =    np.percentile(pc_scores, 5)
    #    upper_percentile = np.percentile(pc_scores, 95)
    #    
    #    lower_band_scaled = lower_percentile * loading_vector
    #    upper_band_scaled = upper_percentile * loading_vector
    #    
    #    # Convert to 2D arrays (required by inverse_transform)
    #    lower_band_scaled_2d = lower_band_scaled.reshape(1, -1)
    #    upper_band_scaled_2d = upper_band_scaled.reshape(1, -1)
    #    
    #    lower_band_original = scaler.inverse_transform(lower_band_scaled_2d)[0]
    #    upper_band_original = scaler.inverse_transform(upper_band_scaled_2d)[0]
    #    
    #    lower_band = mean_waveform + lower_band_original
    #    upper_band = mean_waveform + upper_band_original
    #    
    #    
    #    return lower_band, upper_band
    

        
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


    #def reconstruct_single_component_old(self, ranked_pc, pcs_final, pca_scores, df, scaler):
    #    """
    #    Reconstructs a waveform from a single principal component without using 
    #    pre-combined mean waveforms.
#
    #    Args:
    #        ranked_pc (pd.Series): Metadata describing the selected PCA component (e.g., PC, target, axis).
    #        pcs_final (pd.DataFrame): DataFrame containing trained PCA models per target and axis.
    #        pca_scores (pd.DataFrame): PCA score data used for reconstruction.
    #        df (pd.DataFrame): Original input data used for mean waveform calculation.
    #        scaler (StandardScaler): Scaler used for inverse-transforming PCA outputs.
#
    #    Returns:
    #        dict: A dictionary containing the reconstructed waveform, loading vector, PCA scores,
    #            and the lower and upper reconstruction bands (in original scale).
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
    #    # update the mean calculation
    #    mean_waveform_pain, mean_waveform_no_pain, mean_waveform_total = self.calculate_mean_waveform_target_axis(df)
    #    
    #    #lower_band, upper_band = self.calculate_lower_upper_band(pc_scores, mean_waveform_pain, loading_vector)
    #    lower_band, upper_band = self.calculate_lower_upper_band_unscaled(pc_scores, mean_waveform_pain, loading_vector, scaler)
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

    @staticmethod
    def plot_PCA_reconstruction_per_group(component_data, title_waveform="Mean Waveform", title_loading="Loading Vector"):
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
        lower_band_pain = component_data['lower_band_pain']
        upper_band_pain = component_data['upper_band_pain']
        lower_band_nopain = component_data['lower_band_no_pain']
        upper_band_nopain = component_data['upper_band_no_pain']
        loading_vector = component_data['loading_vector']
        
        fig, axs = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True)
    
        # Plot mean waveforms
        axs[0].plot(mean_waveform_pain, label="Pain", color="red")
        axs[0].plot(mean_waveform_no_pain, label="No Pain", color="blue")
        axs[0].plot(lower_band_pain, label="Lower Band Pain", linestyle='--', color="orange")
        axs[0].plot(upper_band_pain, label="Upper Band Pain", linestyle=':', color="orange")
        axs[0].plot(lower_band_nopain, label="Lower Band No Pain", linestyle='--', color="purple")
        axs[0].plot(upper_band_nopain, label="Upper Band No Pain", linestyle=':', color="purple")
        
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
        subset = df.iloc[:, -202::20]
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
        plt.title(f'Lag Plot for all participants Samples: {fig_title}')
        plt.xlabel('Value at time t') 
        plt.ylabel('Value at time t+1')
        plt.legend()
        #plt.show()
        
        return fig1, fig2
    
    @staticmethod
    def sliding_window_outlier_detection(df, window=10, threshold=3):
        df_outliers_adj = df.copy()
        count_outliers = 0

        time_cols = df.columns[-202:]
        static_cols = df.columns.difference(time_cols)

        for participant, df_part in df.groupby("participant_id"):
            data = df_part[time_cols].to_numpy()
            n_rows, n_cols = data.shape

            for row_idx in range(n_rows):
                series = data[row_idx]

                # Sliding window: generate shape (n_cols - window + 1, window)
                # Handle each timepoint individually
                for i in range(1, n_cols - 1):  # skip t0 and tN for now
                    start = max(0, i - window // 2)
                    end = min(n_cols, i + window // 2 + 1)
                    window_vals = np.delete(series[start:end], i - start)
                    mean = window_vals.mean()
                    std = window_vals.std()
                    if std > 0 and abs(series[i] - mean) > threshold * std:
                        df_outliers_adj.loc[df_part.index[row_idx], time_cols[i]] = series.mean()
                        count_outliers += 1

                # Edge points: t0
                t0_vals = np.delete(data[:, 0], row_idx)
                t0_mean = t0_vals.mean()
                t0_std = t0_vals.std()
                if t0_std > 0 and abs(series[0] - t0_mean) > threshold * t0_std:
                    df_outliers_adj.loc[df_part.index[row_idx], time_cols[0]] = t0_mean
                    count_outliers += 1

                # Edge points: tN
                tN_vals = np.delete(data[:, -1], row_idx)
                tN_mean = tN_vals.mean()
                tN_std = tN_vals.std()
                if tN_std > 0 and abs(series[-1] - tN_mean) > threshold * tN_std:
                    df_outliers_adj.loc[df_part.index[row_idx], time_cols[-1]] = tN_mean
                    count_outliers += 1

        return df_outliers_adj, count_outliers
    
    #@staticmethod    
    #def sliding_window_outlier_detection(df, window=10, threshold=3):
    #    # Sliding window (window size = 10) for datapoints t1 - t200
    #    #data
    #    df_outliers_adj = df.copy()
    #    count_outliers = 0
    #    
    #    unique_participants = df['participant_id'].unique()
    #    for participant in unique_participants:
    #        df_part = df[df['participant_id']==participant]
    #        part_row_idx = 0
    #        for idx, row in df_part.iloc[:, -202:].iterrows():
    #            series = row.values
    #            for i in range(len(series)):
    #                # Skip first and last datapoints
    #                if i == 0 or i == len(series) - 1:
    #                    continue
    #                start = max(0, i - window // 2)
    #                end = min(len(series), i + window // 2 + 1)
    #                window_vals = np.delete(series[start:end], np.where(np.arange(start, end) == i - start))
    #                mean = np.mean(window_vals)
    #                std = np.std(window_vals)
    #                if std > 0 and abs(series[i] - mean) > threshold * std:
    #                    df_outliers_adj.at[idx, df.columns[i+5]] = df[df.columns[i+5]].mean()
    #                    count_outliers += 1
    #            # Calculate group mean for t0 and t201, as sliding window is unstable at the edges
    #            # compare value to group mean to decide if it's an outlier
    #            first_col_mean = df_part.drop(index=idx).iloc[:, -202].mean()
    #            first_col_std = df_part.drop(index=idx).iloc[:, -202].std()
    #            if abs(df_part.iloc[part_row_idx, -202] - first_col_mean) >= threshold * first_col_std:
    #                df_outliers_adj.iloc[idx, -202] = df_part.iloc[:, -202].mean()
    #                count_outliers += 1
    #            # For last timepoint (tN)
    #            last_col_mean = df_part.drop(index=idx).iloc[:, -1].mean()
    #            last_col_std = df_part.drop(index=idx).iloc[:, -1].std()
    #            if abs(df_part.iloc[part_row_idx, -202] - last_col_mean) >= threshold * last_col_std:
    #                df_outliers_adj.iloc[idx, -1] = df_part.iloc[:, -1].mean()
    #                count_outliers += 1
    #            part_row_idx += 1
#
    #            
    #    return df_outliers_adj, count_outliers
    
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
    
    def upload_pca(self, meas_type_id, pc_index, loading_vector, explained_variance, data_scaled, scaler_id):
        """"""
        pc = PC_Ranked(
            id = 1,
            measurement_type_id=meas_type_id,
            scaler_id = scaler_id,
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
        
    def load_pc_data(self, exp_id, device, meas_timepoint, distribution_info = None):
        df = self.pc_scores_repo.get_pc_scores_by_exp_id_device(exp_id, device, meas_timepoint, distribution_info)
        return df
    
    def load_pcs_by_rank(self, exp_id, device, meas_tp, min_rank, max_rank):
        df = self.pc_scores_repo.get_pc_scores_by_tp_rank(exp_id, device,meas_tp, min_rank, max_rank)
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
                group_std_pain=row['std_pain'],
                group_std_no_pain=row['std_no_pain'],
                t_value=row['t_value'],
                p_value=row['p_value']                
            ))
        self.pc_ranked_repo.update_multiple_t_test_info(t_test_results)
   
    def load_specific_scaler(self, meas_type_id, scaler_type):
        return self.scaler_repo.get_sacler_by_meas_type_id_scaler_type(meas_type_id, scaler_type)         
    
    @staticmethod    
    def calculate_kaiser_meyer_olkin(df):
        kmo_all, kmo_model = calculate_kmo(df)
        if kmo_model < 0.9:
            print("KMO per variable:", kmo_all)
            print("Overall KMO:", kmo_model)
    
    @staticmethod
    def calculate_bartlett_test_for_spericity(df):
        corr_matrix = df.corr()
        corr_det = np.linalg.det(corr_matrix)
        if corr_det > 0:
            print("Determinante der Korrelationsmatrix:", corr_det)
        
        chi_square_value, p_value = calculate_bartlett_sphericity(df)
        if p_value > 0:
            print("Bartlett's Test")
            print("Chi-Square:", chi_square_value)
            print("p-value:", p_value)

    @staticmethod
    def compute_corr_matrix(df):
        corr_matrix = df.corr(method='pearson')
        print(corr_matrix)
        return corr_matrix
    
    @staticmethod    
    def plot_corr_matrix(corr_matrix, target, axis):
        fig = plt.figure(figsize=(10, 8))
        annotate = corr_matrix.shape[0] <= 20
        sns.heatmap(corr_matrix, annot=annotate, fmt=".2f",vmin=0, vmax=1, cmap='coolwarm', square=True)
        plt.title(f'Correlation Matrix Heatmap: {target}, {axis}')
        #plt.show()
        return fig
        
    def check_pca_requirements(self, df, target, axis):
        corr_matrix = self.compute_corr_matrix(df)
        fig = self.plot_corr_matrix(corr_matrix, target, axis)
        return fig
        #self.save_plot()
        #self.calculate_bartlett_test_for_spericity(df)
        #self.calculate_kaiser_meyer_olkin(df)
        
        
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
        
    
    