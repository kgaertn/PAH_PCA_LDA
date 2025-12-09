from core.analyses.abstract_analysis import AbstractAnalyser
from processing.data_loading import DataLoader
from processing.data_plotting import DataPlotter

from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.stats import ttest_rel, ttest_ind

class StatisticalAnalyser(AbstractAnalyser):
    def __init__(self, cfg, logger, run_rotated = False):
        super().__init__(cfg, logger)
        self.analysis_name = 'statistical'
        self.cfg = cfg
        self.run_rotated = run_rotated
        self.data_loader = DataLoader()
        self.data_plotter = DataPlotter()
        
#TODO calculate pre/post (mean) difference for each person
#TODO compare pre/post difference PER GROUP with paired t-test?
#TODO calculate Cohens d effect size?
#TODO ANOVA / Multiple regression?

    def run(self, key):
        stat_info = self.prepare(key)
        df_mean_pre = stat_info['df_pc_mean_pre']
        df_mean_post = stat_info['df_pc_mean_post']
        results = self.conduct_t_test(df_mean_pre, df_mean_post)
        return results
    
    def handle_results(self, exp_params, analysis_params, results):
        """"""
        pain_groups = exp_params['pain_groups']
        pain_group_names = self.data_plotter.concat_pain_groups(pain_groups)
        device = analysis_params['device']
        current_path = Path.cwd()
        
        output_path = current_path / "output" / "csvs" / f"{device}" /   "Pre_post_comparison" / f"{pain_group_names}"
        output_path.mkdir(parents=True, exist_ok=True)
        filename = "Pre_post_comparison.csv"
        results.to_csv(output_path / filename, index=False, encoding="utf-8", sep = ";", decimal=",")
        #self._upload_step(exp_params= exp_params, analysis_params = analysis_params, uploads=[(self.upload_pca_analysis, results)])
        
    
    def reconstruct_results(self, key):
        """"""
        #if self.run_rotated:
        #    self.reconstruct_pcas(key, select_rotated=True)
        #else:
        #    self.reconstruct_pcas(key)
        
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
            pca_df_pre = self.data_loader.load_pc_data(exp_id=exp_id, device=device, meas_timepoint='pre', 
                                                pain_groups=pain_groups, rotation_type= rotation_method)
            pca_df_post = self.data_loader.load_pc_data(exp_id=exp_id, device=device, meas_timepoint='post', 
                                                pain_groups=pain_groups, rotation_type= rotation_method)
        else:
            pca_df_pre = self.data_loader.load_pc_data(exp_id=exp_id, device=device, meas_timepoint='pre', 
                                    pain_groups=pain_groups)
            pca_df_post = self.data_loader.load_pc_data(exp_id=exp_id, device=device, meas_timepoint='post', 
                                    pain_groups=pain_groups)
        
        unique_combos = pca_df_pre[['target', 'axis', 'pc_index']].drop_duplicates()
        df_pc_mean_pre_list = []
        df_pc_mean_post_list = []
        for _, row in unique_combos.iterrows():
            target = row['target']
            axis = row['axis']
            pc_index = row['pc_index']
            
            df_target_axis_pc_mean_pre = pca_df_pre[(pca_df_pre['target'] == target) & (pca_df_pre['axis'] == axis) & (pca_df_pre['pc_index'] == pc_index)].groupby(['participant_id', 'PRMD_ever'])['pc_score'].mean().reset_index()
            df_target_axis_pc_mean_post = pca_df_post[(pca_df_post['target'] == target) & (pca_df_post['axis'] == axis) & (pca_df_post['pc_index'] == pc_index)].groupby(['participant_id', 'PRMD_ever'])['pc_score'].mean().reset_index()
            
            df_target_axis_pc_mean_pre = df_target_axis_pc_mean_pre.rename(columns={'pc_score': 'pc_score_mean'})
            
            df_target_axis_pc_mean_pre['target'] = target
            df_target_axis_pc_mean_pre['axis'] = axis
            df_target_axis_pc_mean_pre['pc_index'] = pc_index
            
            df_target_axis_pc_mean_post = df_target_axis_pc_mean_post.rename(columns={'pc_score': 'pc_score_mean'})
            df_target_axis_pc_mean_post['target'] = target
            df_target_axis_pc_mean_post['axis'] = axis
            df_target_axis_pc_mean_post['pc_index'] = pc_index
            
            df_pc_mean_pre_list.append(df_target_axis_pc_mean_pre)
            df_pc_mean_post_list.append(df_target_axis_pc_mean_post)

        # Combine into final DataFrames
        df_pc_mean_pre = pd.concat(df_pc_mean_pre_list, ignore_index=True)
        df_pc_mean_post = pd.concat(df_pc_mean_post_list, ignore_index=True)
        results = {"pca_df_pre":pca_df_pre,
                   "pca_df_post":pca_df_post,
                   "df_pc_mean_pre":df_pc_mean_pre,
                   "df_pc_mean_post":df_pc_mean_post
                   }
        #pc_distribution_results= self.assumptions_tester.check_t_test_assumptions(pca_df, pain_groups, distributions_plotted)    
        #results = {
        #    "pc_distribution_results": pc_distribution_results,
        #    "key" : key
        #} 
        return results  
    
    def conduct_t_test(self, pca_df_pre, pca_df_post):
        unique_combos = pca_df_pre[['target', 'axis', 'pc_index']].drop_duplicates()
        unique_pain_groups = pca_df_pre['PRMD_ever'].unique()
        
        #t_stats, p_values, effect_sizes = [], [], []
        all_results = []
        for pain_group in unique_pain_groups:
            for _, row in unique_combos.iterrows():
                target = row['target']
                axis = row['axis']
                pc_index = row['pc_index']
                df_target_axis_pre = pca_df_pre[
                            (pca_df_pre['target'] == target) &
                            (pca_df_pre['axis'] == axis) &
                            (pca_df_pre['pc_index'] == pc_index) &
                            (pca_df_pre['PRMD_ever'] == int(pain_group))
                            ]
                df_target_axis_post = pca_df_post[
                            (pca_df_post['target'] == target) &
                            (pca_df_post['axis'] == axis) &
                            (pca_df_post['pc_index'] == pc_index) &
                            (pca_df_post['PRMD_ever'] == int(pain_group))
                            ]
                pre_scores = df_target_axis_pre['pc_score_mean']
                post_scores = df_target_axis_post['pc_score_mean']
                
                t_stat, p_val = ttest_rel(post_scores, pre_scores)
                d = self.cohen_d_paired(post_scores, pre_scores)
                results = [target, axis, pc_index, pain_group, t_stat, p_val, d]
                all_results.append(results)
                #t_stats.append(t_stat)
                #p_values.append(p_val)
                #effect_sizes.append(d)
        all_results = pd.DataFrame(all_results, columns=['target', 'axis', 'pc_index', 'PRMD_ever', 't_stat', 'p_val', 'cohens_d'])
        return all_results
    
    def cohen_d_paired(self, x, y):
        diff = x - y
        return np.mean(diff) / np.std(diff, ddof=1)
                
            
            
            
        