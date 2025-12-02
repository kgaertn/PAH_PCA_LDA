from core.analyses.abstract_analysis import AbstractAnalyser
from processing.data_loading import DataLoader
from processing.data_plotting import DataPlotter
from processing.data_preprocess import DataProcessor
from processing.assumptions_testing import AssumptionsTester

import pandas as pd
import scipy.stats as st

class TTestAnalyser(AbstractAnalyser):
    #TODO: see if run_rotated is necessary AND add information about whether or not to test for distribution
    def __init__(self, cfg, logger, run_rotated = False):
        super().__init__(cfg, logger)
        self.analysis_name = 't_test' if not run_rotated else 't_test_rotated'
        self.cfg = cfg
        self.run_rotated = run_rotated
        self.data_loader = DataLoader()
        self.data_processor = DataProcessor()
        self.data_plotter = DataPlotter()
        self.assumptions_tester = AssumptionsTester()
        
    def run(self, key):
        """"""
        pc_distribution_results = self.prepare(key)
        return pc_distribution_results
    
    def prepare(self, key):
        """
        Check distribution assumptions (normality) for principal components.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            measurement_tp (str): Measurement time point.
            distributions_plotted (bool): Whether to plot distribution histograms.

        Returns:
            list: Results of distribution tests.
        """
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        pain_groups = key['pain_groups']
        rotation_method = key['rotation_method']
        distributions_plotted = key['distributions_plotted']
        
        if self.run_rotated:
            pca_df = self.data_loader.load_pc_data(exp_id=exp_id, device=device, meas_timepoint=measurement_tp, 
                                                pain_groups=pain_groups, rotation_type= rotation_method)
        else:
            pca_df = self.data_loader.load_pc_data(exp_id=exp_id, device=device, meas_timepoint=measurement_tp, 
                                    pain_groups=pain_groups)
        pc_distribution_results= self.assumptions_tester.check_t_test_assumptions(pca_df, pain_groups, distributions_plotted)    
        results = {
            "pc_distribution_results": pc_distribution_results,
            "key" : key
        } 
        return results  

    def handle_results(self, exp_params, analysis_params, results):
        """"""
        pc_distribution_results = results['pc_distribution_results']
        key = results['key']

        self._upload_step(
            exp_params=exp_params,
            analysis_params=analysis_params,
            uploads=[
                (self.data_loader.upload_distribution_info, pc_distribution_results)
            ]
        )
        
        t_test_results = self.conduct_t_test(key)
        self._upload_step(
            exp_params=exp_params,
            analysis_params=analysis_params,
            uploads=[
                (self.data_loader.upload_t_test_results, t_test_results),
            ]
        )      
           
    def conduct_t_test(self,key)-> pd.DataFrame:
        """
        Perform t-tests on PCA data to rank principal components.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            measurement_tp (str): Measurement time point.
            involve_rotations (bool): Whether to include rotated components.

        Returns:
            pd.DataFrame: T-test results and rankings.
        """
        # TODO: make sure that rotations are not ALWAYS automatically included in loading the data
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        pain_groups = key['pain_groups']
        rotation_type = key['rotation_method']
        check_ttest_distribution = key['check_ttest_distribution']
        
        if self.run_rotated:
            if check_ttest_distribution:
                pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp, pain_groups, 'normal_distribution', rotation_type=rotation_type)
            else:
                pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp, pain_groups, rotation_type=rotation_type)
        else:
            if check_ttest_distribution:
                pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp, pain_groups, 'normal_distribution', rotation_type='unrotated')
            else:
                pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp, pain_groups, rotation_type='unrotated')
        
        t_test_results = self.rank_pcs(pca_df)
        return t_test_results
    
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
            if axis != None:
                df_target_axis = df_pc_reduced[(df_pc_reduced["target"] == target) & (df_pc_reduced["axis"] == axis)]
            else:
                df_target_axis = df_pc_reduced[(df_pc_reduced["target"] == target)]
            t_test_target_axis = self.calculate_t_test(df_target_axis)
            t_test_total.extend(t_test_target_axis)
        
        df_t_test = pd.DataFrame(t_test_total, columns = ['target', 'axis', 'pc_id', 'pc_index', 't_value', 'p_value', 'mean_pain', 'mean_no_pain', 'std_pain', 'std_no_pain'])
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
 
    