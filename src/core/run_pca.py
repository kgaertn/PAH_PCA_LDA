from analysis.data_processing.data_preprocess import DataProcessor
from analysis.data_analysis.pca_analysis import PCAAnalyser
from analysis.data_processing.data_loading import DataLoader
from analysis.data_processing.assumptions_testing import AssumptionsTester
from analysis.data_analysis.data_plotting import DataPlotter
#from core.setup_and_upload import SetupUploader
from models.pc_ranked import PC_Ranked

from sklearn.preprocessing import StandardScaler
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

class PCARunner:
    def __init__(self):
        """
        Initializes the SetupUploader with processors.
        """
        self.data_processor = DataProcessor()
        self.pca_analyser = PCAAnalyser() 
        self.data_loader = DataLoader()
        self.data_plotter = DataPlotter()
        self.assumptions_tester = AssumptionsTester()
    
    def run_pca_analysis(self,exp_id, measurement_tp, device, pain_groups, scale_data = True, scaler_type = 'standard_scaler', check_requirements = False):
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        total_pca_info = {}
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            # TODO: check PCA, as there seems to be only one component saved
            df_sorted, df_sorted_transformed  = self.load_data_for_pca(exp_id, device,measurement_tp, target, axis, participant_ids)
            df_transformed = self.process_data_for_pca(df_sorted)
            df_outliers_removed, count_outliers = self.check_and_remove_outliers(df_transformed)
            
            df_part, df_pca = df_outliers_removed.iloc[:, :-202], df_outliers_removed.iloc[:, -202:]
            measurement_type_id = int(df_transformed['measurement_type_id'].unique()[0])
            if check_requirements:
                #self.plot_data_linearity(target, axis, df_sorted_transformed)
                
                corr_mat = self.assumptions_tester.check_pca_requirements(df_sorted_transformed.iloc[:, -202:], target, axis)
                #fig_corr_mat = self.assumptions_tester.plot_pca_requirements(corr_mat, target, axis)
                #current_path = Path.cwd()
                #output_path = current_path / "output" / "plots"
                #output_path = current_path / "output" / "plots" / "Correlation_matrix"
                #self.data_plotter.save_plot(fig_corr_mat, output_path, f"Original_Data_Correlation_matrix_{target}_{axis}")
            if scale_data:
                df_analysis, scaler_info = self.scale_df(df_pca, scaler_type)
                scaler, existing_scaler_info = self.data_loader.load_specific_scaler(measurement_type_id, scaler_type)
                if scaler:
                    scaler_id = existing_scaler_info.id
                else: 
                    scaler_id = self.upload_scaler(measurement_type_id, scaler_info)
            else:
                df_analysis = df_pca
                scaler_info = {
                    'mean' : None,
                    'scale' : None,
                    'scaler_type' : None
        }
                scaler_id = self.upload_scaler(measurement_type_id, scaler_info)
            pca_info, pc_sample_scores = self.conduct_pca_analysis(df_analysis, df_part, measurement_type_id, scale_data, scaler_id)
            total_pca_info.update({
                f'{target}_{axis}' : {
                    'scaler_id' : scaler_id,
                    'scaler_info' : scaler_info,
                    'data_scaled' : scale_data,
                    'pca_info' : pca_info,
                    'pc_sample_scores' : pc_sample_scores,  
                    'pain_group_ids' : pain_group_ids, 
                    'rotation_type' : 'unrotated'              
                }
            })
        return total_pca_info
    

    # TODO: select existing pain groups
    # TODO: upload pain group, if not exists
    # TODO: select participant_ids belonging to the respective pain groups
    # TODO: add pain group info to db, OR 
    
    def upload_new_pain_group(self, pain_group):
        return self.data_loader.upload_new_pain_group(pain_group)
        """"""
    def upload_new_rotation_type(self, rotation_type):
        return self.data_loader.upload_new_rotation_type(rotation_type)
    
    def upload_pca_analysis(self, pca_info):
        # TODO fix pca upload, currently only the first component is uploaded
        for target_axis, target_axis_data in pca_info.items():
            scaler_id = target_axis_data['scaler_id']
            data_scaled = target_axis_data['data_scaled']
            pc_info = target_axis_data['pca_info']
            sample_scores = target_axis_data['pc_sample_scores']
            rotation_type = target_axis_data['rotation_type']
            rotation_id = self.upload_new_rotation_type(rotation_type)
            pc_ids = self.upload_pca_analysis_target_axis(pc_info, sample_scores, scaler_id, data_scaled, rotation_id)
            pain_group_ids = target_axis_data['pain_group_ids']
            for pain_group_id in pain_group_ids:
                for pc_id in pc_ids:
                    self.upload_pc_pain_group(pc_id, pain_group_id)
                
    
    def upload_pc_pain_group(self, pc_id, pain_group_id):
        """"""
        self.data_loader.upload_pc_pain_group_ids(pc_id, pain_group_id)
    
    def plot_data_linearity(self, target, axis, df):
        fig_title = f"{target}, {axis}"
        fig1, fig2 = self.data_plotter.plot_linearity(df, fig_title)
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots"
        output_path = current_path / "output" / "plots" / "Linearity_plots"
        self.data_plotter.save_plot(fig1, output_path, f"Scatter_matrix_{target}_{axis}")
        self.data_plotter.save_plot(fig2, output_path, f"Lag_plot_{target}_{axis}")        
            
    def scale_df(self, df, scaler_type):
        scaled_df, scaler =self.pca_analyser.standardize_df(df)

        scaler_info = {
            'mean' : list(scaler.mean_),
            'scale' : list(scaler.scale_),
            'scaler_type' : scaler_type
        }
        return scaled_df, scaler_info
    
    def upload_scaler(self, measurement_type_id, scaler_info):
        mean = scaler_info['mean']
        scale = scaler_info['scale']
        scaler_type = scaler_info['scaler_type']
        scaler_id = self.data_loader.upload_scaler(measurement_type_id, scaler_type, mean, scale)
        return scaler_id
   
    def conduct_pca_analysis(self, df, df_part, measurement_type_id, data_scaled = True, scaler_id = None):     
        variance_level = 0.9
        explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores = self.pca_analyser.apply_pca(df, variance_level)
        pca_info = {}
        pc_scores = {}
        for component_idx in range(k):
            pc_index = component_idx + 1
            loading_vector = list(pca_final.components_[component_idx])
            explained_variance_pc = float(explained_variance[component_idx])
            pca_info.update({
                f'component_{pc_index}':{
                    'measurement_type_id': measurement_type_id,
                    'pc_index': pc_index,
                    'loading_vector' : loading_vector, 
                    'explained_variance' : explained_variance_pc
                } 
            })
            #pc_id = self.pca_analyser.upload_pca(measurement_type_id, pc_index, loading_vector, explained_variance_pc, data_scaled, scaler_id)
            pc_sample_scores = list(zip(df_part['sample_id'], df_pca_scores.iloc[:,component_idx]))
            pc_scores.update({f'component_{pc_index}_scores' : pc_sample_scores})
            #self.pca_analyser.upload_pc_scores(pc_id, pc_sample_scores)

        return pca_info, pc_scores
    
    def upload_pca_analysis_target_axis(self, pca_info, pc_sample_scores, scaler_id = None, data_scaled = False, rotation_id = None):
        pc_sample_keys = list(pc_sample_scores.keys())
        pc_ids = []
        for index, (_, component) in enumerate(pca_info.items()):
            component_key = pc_sample_keys[index]
            measurement_type_id = component['measurement_type_id']
            parent_id = component.get('parent_id')
            pc_index = component['pc_index']
            loading_vector = component['loading_vector']
            explained_variance = component['explained_variance']
            pc_id = self.data_loader.upload_pca(measurement_type_id, parent_id, pc_index, loading_vector, explained_variance, data_scaled, scaler_id, rotation_id)
            self.data_loader.upload_pc_scores(pc_id, pc_sample_scores[component_key])
            pc_ids.append(pc_id)
        return pc_ids
    
    def check_distribution(self, exp_id, device, measurement_tp, distributions_plotted = True):
        pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp)
        pc_distribution_results= self.assumptions_tester.check_t_test_assumptions(pca_df, distributions_plotted)     
        return pc_distribution_results   
    
    def conduct_t_test(self, exp_id, device, measurement_tp, involve_rotations = False):
        #if not involve_rotations:
        pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp, 'normal_distribution',)
        #else: 
        #    pca_df
        
        t_test_results = self.pca_analyser.rank_pcs(pca_df)
        return t_test_results
        
    def upload_distribution(self, distribution_info):
        self.data_loader.upload_distribution_info(distribution_info)
        
    def upload_t_test(self, t_test_results):
        self.data_loader.upload_t_test_results(t_test_results)
        
    def process_data_for_pca(self, df_sorted):

        # subtract the key mean-waveform from each sample
        # TODO: save mean key per target/axis? / plot mean key? 
        #df_key_normalized, df_mean_key_waveform_target_axis = self.data_processor.subtract_meanwave_key(df_sorted)
        df_key_normalized, df_mean_key_waveform_target_axis = self.data_processor.subtract_meanwave_key_difference(df_sorted)
        # transform the data (columns for each timepoint)
        df_transformed = self.data_processor.pivot_full_cycles_to_wide(df_key_normalized, 'value_centered','dp_time_point')
        return df_transformed
    
    def load_data_for_pca(self, exp_id, device, measurement_tp, target, axis, participant_ids):
        # TODO: select pain groups from DB, if pain_group not in db: upload new pain group
        df = self.data_loader.load_MPA_clean_data_by_device_tp_target_axis(exp_id, device, measurement_tp, target, axis, participant_ids)
        print(f"{target} {axis} loaded")
    # select the correct participants, based on their pain location
        #pain_columns = ['PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left']
        #control_column = 'PRMD_ever'
        #df_pain = self.data_processor.select_pain_data(df, pain_columns, control_column)
        #df_reduced = df_pain[["participant_id", "measurement_id","measurement_type_id", "PRMD_ever", 'target', 'axis', "sample_id", 'dp_time_point', 'value']]
        df_reduced = df[[
            'participant_id', 'ext_participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever',
            'measurement_id', 'measurement_type_id', 'target', 'axis', 'sample_id', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
            'value'
        ]]
        df_sorted = df_reduced.sort_values(by=['participant_id', 'bow_stroke', 'up_down', 'dp_time_point'])  
        df_sorted_transformed = self.data_processor.pivot_full_cycles_to_wide(df_sorted, 'value','dp_time_point')
          
        return df_sorted, df_sorted_transformed
    
    def load_pain_groups():
        """"""
    
    def check_and_remove_outliers(self, df):
        #TODO: save which dp are outliers
        df_outliers_removed, count_outliers = self.data_processor.sliding_window_outlier_detection(df)
        return df_outliers_removed, count_outliers
    
        # plot upper/lower bands per group
    def reconstruct_pcas(self, exp_id, device, measurement_tp, pain_groups, scaler_type, nr_components = None, select_rotated = False):

        ranked_pc_scores_df = self.data_loader.load_pcs_by_rank(exp_id, device,measurement_tp, nr_components= nr_components,select_rotated= select_rotated)
        for id, pc_id in enumerate(ranked_pc_scores_df['pc_id'].unique()):
            #TODO: adjust current df
            rank = id+1
            current_pc_df = ranked_pc_scores_df[ranked_pc_scores_df['pc_id'] == pc_id]
            target, axis = current_pc_df['target'].unique()[0], current_pc_df['axis'].unique()[0]
            measurement_type_id = int(current_pc_df['meas_type_id'].unique()[0])
            #participants = current_pc_df['participant_id'].unique()
            #participants = tuple(int(x) for x in participants)
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df = self.data_loader.load_MPA_clean_data_by_device_tp_target_axis(exp_id, device, measurement_tp, target, axis, participant_ids)
            #df = self.data_loader.load_MPA_clean_data_by_device_tp_target_axis_participants(exp_id, device, measurement_tp, target, participants, axis)
            df_reduced = df[[
                    'participant_id', 'ext_participant_id', 'PRMD_ever',
                    'measurement_id','measurement_type_id', 'target', 'axis', 'sample_id', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
                    'value'
                ]]
            df_transformed = self.data_processor.pivot_full_cycles_to_wide(df_reduced, 'value', 'dp_time_point')
            scaler, _ = self.data_loader.load_specific_scaler(measurement_type_id, scaler_type)
            #scaler = StandardScaler()
            #scaler.mean_ = scaler_info.mean
            #scaler.scale_ = scaler_info.scale
            component_reconstruction_data = self.pca_analyser.reconstruct_single_component(current_pc_df, df_transformed, scaler)
            #plot PCA reconstruction
            pc_name = "PC" + str(current_pc_df['pc_index'].unique()[0])
            rotated_suffix_title = " Rotated" if select_rotated else ""
            rotated_suffix_fname = "_Rotated" if select_rotated else ""

            title_reconstruction = f'Single component reconstruction: Rank {rank}, {target} {axis}; {pc_name}{rotated_suffix_title}'
            title_loading_vector = f'Loading vector: Rank {rank} {target} {axis}; {pc_name}{rotated_suffix_title}'

            fig = self.data_plotter.plot_PCA_reconstruction(component_reconstruction_data, title_reconstruction, title_loading_vector)

            current_path = Path.cwd()
            output_path = current_path / "output" / "plots" / "PCA_Reconstruction"
            fig_name = f"{measurement_tp}{rotated_suffix_fname}_Rank_{rank}_{target}_{axis}_{pc_name}"
            self.data_plotter.save_plot(fig, output_path, fig_name)
            plt.close()
            fig = self.data_plotter.plot_PCA_reconstruction_per_group(component_reconstruction_data, title_reconstruction, title_loading_vector)
            fig_name = f"{measurement_tp}{rotated_suffix_fname}_per_group_Rank_{rank}_{target}_{axis}_{pc_name}"
            self.data_plotter.save_plot(fig, output_path, fig_name)
            plt.close()
    
    #def get_existing_rotation_types(self):
    #    # TODO:
    #    rotations = self.data_loader.load_existing_rotation_ids()
        
        
        #rot_ids = self.data_loader.
       
    def run_pca_rotation(self, exp_id, device, measurement_tp, pain_groups, scaler_type = 'standard_scaler', method = 'varimax'):
        """"""
        pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp, 'normal_distribution',)
        existing_target_axes = pca_df[['target', 'axis']].drop_duplicates().values.tolist()
        total_rotation_info = {}
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.get_participants_pain_groups(pain_groups)
            df_target_axis = pca_df[(pca_df['target'] == target) & (pca_df['axis'] == axis)]
            loading_vector_json = df_target_axis['loading_vector'].drop_duplicates().values
            loading_vectors = []
            for component in loading_vector_json:
                loading_vector = np.array(PC_Ranked.list_from_json(component))
                loading_vectors.append(loading_vector)
            loading_vectors = np.array(loading_vectors)
            if len(loading_vectors) > 1:
                pivoted = df_target_axis.pivot(index='sample_id', columns='pc_index', values='pc_score')
                #pc_scores_matrix = pivoted.values 
                measurement_type_id = int(df_target_axis['meas_type_id'].drop_duplicates().values)
                scaler, existing_scaler_info = self.data_loader.load_specific_scaler(measurement_type_id, scaler_type)
                scaler_id = existing_scaler_info.id
                scaler_info = {
                    'mean' : scaler.mean_,
                    'scale' : scaler.scale_,
                    'scaler_type' : scaler_type
                }
                rotated_loadings = self.pca_analyser.rotate_pc_loadings(loading_vectors, method = method )
            
                df_sorted, _  = self.load_data_for_pca(exp_id, device,measurement_tp, target, axis, participant_ids)
                df_transformed = self.process_data_for_pca(df_sorted)
                df_outliers_removed, _ = self.check_and_remove_outliers(df_transformed)
                df_part, df_pca = df_outliers_removed.iloc[:, :-202], df_outliers_removed.iloc[:, -202:]
            
                rotated_scores = self.pca_analyser.rotate_pc_scores(df_pca, scaler, rotated_loadings)
                pca_info = {}
                pc_scores = {}
                for component_idx in pd.DataFrame(rotated_scores):
                    pc_sample_rotated_scores = list(zip(df_part['sample_id'], pd.DataFrame(rotated_scores).iloc[:,component_idx]))
                    pc_index = int(pivoted.columns[component_idx])
                    loading_vector = list(rotated_loadings[component_idx])
                    explained_variance_pc = float(df_target_axis[df_target_axis['pc_index'] == pc_index]['explained_variance'].unique())
                    parent_id = int(df_target_axis[df_target_axis['pc_index'] == pc_index]['pc_id'].unique())
                    pca_info.update({
                        f'component_{pc_index}':{
                            'measurement_type_id': measurement_type_id,
                            'parent_id':parent_id,
                            'pc_index': pc_index,
                            'loading_vector' : loading_vector, 
                            'explained_variance' : explained_variance_pc
                        } 
                    })
                    pc_scores.update({f'component_{pc_index}_scores' : pc_sample_rotated_scores})
                nr_components = len(df_target_axis['pc_id'].drop_duplicates())
                rotation_method = method if nr_components > 1 else 'Unrotated'
                total_rotation_info.update({
                    f'{target}_{axis}' : {
                        'scaler_id' : scaler_id,
                        'scaler_info' : scaler_info,
                        'data_scaled' : 1 if scaler_type != None else 0,
                        'pca_info' : pca_info,
                        'pc_sample_scores' : pc_scores,  
                        'pain_group_ids' : pain_group_ids, 
                        'rotation_type' :  rotation_method             
                    }})
                
                #total_rotation_info.update({
                #    f'{target}_{axis}' : {
                #        'measurement_type_id' : measurement_type_id,
                #        'rotation_type' : rotation_method,
                #        'rotated_loadings' : rotated_loadings,
                #        'rotated_scores' : rotated_scores,                   
                #    }})
        return total_rotation_info
    
    # TODO
    #def upload_pca_rotations(self):
    #    """"""
    
    def upload_pca_rotations(self, rotation_info):
        rotations = self.data_loader.load_existing_rotation_ids()
        
        """"""
            

            