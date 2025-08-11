from models.pc_ranked import PC_Ranked
from analysis.data_analysis.data_plotting import DataPlotter

from factor_analyzer import calculate_bartlett_sphericity
from factor_analyzer.factor_analyzer import calculate_kmo
import pandas as pd
import scipy.stats as st
import numpy as np
import matplotlib.figure

class AssumptionsTester:
    
    def __init__(self):
        """
        Initialize the AssumptionsTester with plotter.
        """    
        self.data_plotter = DataPlotter()
        
    def check_t_test_assumptions(self, df:pd.DataFrame, pain_groups:list[str], distributions_plotted:bool = True):  
        """
        Check assumptions for t-tests including normality tests on PC scores.

        Args:
            df (pd.DataFrame): DataFrame containing PC scores and related metadata.
            distributions_plotted (bool): Whether to plot the distributions or not.

        Returns:
            List[PC_Ranked]: List of PC_Ranked instances with distribution test results.
        """
        # TODO: split the plotting from the rest of the assumptions testing
        df_mean = df.groupby(['target', 'axis', 'pc_index', 'pc_id', 'participant_id', 'PRMD_ever'])['pc_score'].mean().reset_index()
        meas_time_point = df['meas_time_point'][0]     
        target_axes = df[['target', 'axis', 'pc_index', 'pc_id']].drop_duplicates().values.tolist()
        pc_distribution_results = []
        for target, axis, pc_index, pc_id in target_axes:
            stat_pain, p_pain, stat_nopain, p_nopain = self.shapiro_wilk_test(df_mean, target, axis, pc_id)
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
                df_target_axis = df[(df['target'] == target) & (df['axis'] == axis) & (df['pc_index'] == pc_index)][['PRMD_ever','pc_score']]        
                fig = self.data_plotter.plot_distribution(df_target_axis, 'pc_score', target, axis, pc_index, meas_time_point, pain_groups)
                self.data_plotter.save_distribution_plot(fig, target, axis, pc_index, meas_time_point, pain_groups)
        return pc_distribution_results
    
    @staticmethod
    def kolmogorov_smirnov_test(df:pd.DataFrame):
        """
        Perform Kolmogorov-Smirnov tests for normality on PC score distributions.

        Args:
            df (pd.DataFrame): DataFrame containing PC scores and metadata.
        """
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
    def shapiro_wilk_test(df:pd.DataFrame, target:str, axis:str, pc_id:int) -> tuple[float]:
        """
        Perform Shapiro-Wilk test for normality separately for pain and no-pain groups.

        Args:
            df (pd.DataFrame): DataFrame with PC scores and metadata.
            target (str): Target joint or feature.
            axis (str): Axis corresponding to the target.
            pc_id (int): Principal component ID.

        Returns:
            Tuple :
                stat_pain (float): Test statistic for pain group.
                p_pain (float): p-value for pain group.
                stat_nopain (float): Test statistic for no-pain group.
                p_nopain (float): p-value for no-pain group.
        """
        pain_group = df[(df['PRMD_ever'] == 1) & (df['target'] == target) & (df['axis'] == axis) & (df['pc_id'] == pc_id)]['pc_score']
        nopain_group = df[(df['PRMD_ever'] == 0) & (df['target'] == target) & (df['axis'] == axis) & (df['pc_id'] == pc_id)]['pc_score']
        stat_pain, p_pain = st.shapiro(pain_group)
        stat_nopain, p_nopain = st.shapiro(nopain_group)
        return stat_pain, p_pain, stat_nopain, p_nopain
        
    @staticmethod    
    def calculate_kaiser_meyer_olkin(df:pd.DataFrame):
        """
        Calculate and print the Kaiser-Meyer-Olkin measure of sampling adequacy.

        Args:
            df (pd.DataFrame): DataFrame of data for KMO calculation.
        """
        kmo_all, kmo_model = calculate_kmo(df)
        if kmo_model < 0.98:
            #print("KMO per variable:", kmo_all)
            print("Overall KMO:", kmo_model)

    @staticmethod
    def drop_redundant_columns(df:pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        """
        Remove perfectly correlated redundant columns from the DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame with possible redundant columns.

        Returns:
            Tuple containing:
                - Cleaned DataFrame with redundant columns dropped.
                - List of dropped column names.
        """
        corr_matrix = df.corr()

        exact_ones = corr_matrix.stack()
        exact_ones = exact_ones[exact_ones == 1.0]
        exact_ones = exact_ones[exact_ones.index.get_level_values(0) != exact_ones.index.get_level_values(1)]

        unique_pairs = set()
        columns_to_drop = set()

        for row, col in exact_ones.index:
            pair = tuple(sorted([row, col]))
            if pair not in unique_pairs:
                unique_pairs.add(pair)
                columns_to_drop.add(pair[1])

        df_cleaned = df.drop(columns=columns_to_drop)

        dropped_columns = list(columns_to_drop)
        
        return df_cleaned, dropped_columns

    def calculate_bartlett_test_for_spericity(self, df:pd.DataFrame):
        """
        Calculate Bartlett’s test for sphericity and print results.

        Args:
            df (pd.DataFrame): DataFrame of variables to test.
        """
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
    def compute_corr_matrix(df:pd.DataFrame) -> pd.DataFrame:
        """
        Compute Pearson correlation matrix of the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame of numeric variables.

        Returns:
            pd.DataFrame: Correlation matrix.
        """
        corr_matrix = df.corr(method='pearson')
        return corr_matrix
    
    def check_pca_requirements(self, df:pd.DataFrame):
        """
        Check requirements for PCA by computing correlations and statistical tests.

        Args:
            df (pd.DataFrame): DataFrame with data.

        Returns:
            pd.DataFrame: Correlation matrix of the data.
        """
        corr_matrix = self.compute_corr_matrix(df)
        self.calculate_bartlett_test_for_spericity(df)
        self.calculate_kaiser_meyer_olkin(df)
        return corr_matrix

    def plot_pca_requirements(self, corr_matrix, target, axis) -> matplotlib.figure.Figure:
        """
        Plot the correlation matrix to visualize PCA requirements.

        Args:
            corr_matrix (pd.DataFrame): Correlation matrix.
            target (str): Target variable or joint.
            axis (str): Axis related to the target.

        Returns:
            matplotlib.figure.Figure: Figure object of the correlation matrix plot.
        """
        fig = self.data_plotter.plot_corr_matrix(corr_matrix, target, axis)
        return fig