from core.analyses.abstract_analysis import AbstractAnalyser
from processing.data_loading import DataLoader
from processing.data_plotting import DataPlotter
from processing.data_preprocess import DataProcessor
from processing.assumptions_testing import AssumptionsTester
from data_access.models.pc_ranked import PC_Ranked # TODO: change, so that PC Ranked is only called from data processing level

from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from factor_analyzer import Rotator

class PCAAnalyser(AbstractAnalyser):
    def __init__(self, cfg, logger, run_rotated = False):
        super().__init__(cfg, logger)
        self.analysis_name = 'pca' if not run_rotated else 'pca_rotated'
        self.cfg = cfg
        self.run_rotated = run_rotated
        self.data_loader = DataLoader()
        self.data_processor = DataProcessor()
        self.data_plotter = DataPlotter()
        self.assumptions_tester = AssumptionsTester()
        
    def run(self, key):
        if self.run_rotated == False:
            pca_info = self.run_pca_unrotated(key)
        else:
            pca_info = self.run_pca_rotated(key)
        
        return pca_info
    
    def prepare(self, key, target, axis):
        """"""
        if self.run_rotated:
            prepared_data = self.prepare_rotated(key, target, axis)
        else:
            prepared_data = self.prepare_unrotated(key, target, axis)
        return prepared_data

    def handle_results(self, params, results, entry):
        """"""
        self._upload_step(params = params, entry = entry, analysis_name=self.analysis_name , uploads=[(self.upload_pca_analysis, results)])
        
    def reconstruct_results(self, key):
        """"""
        if self.run_rotated:
            self.reconstruct_pcas(key, select_rotated=True)
        else:
            self.reconstruct_pcas(key)
            
    def run_pca_unrotated(self, key):
        # TODO: improve this function (make it slimmer, less complex and check dependencies)
        """
        Run PCA analysis for all target-axis pairs in the experiment and check the PCA requirements, if check_requirements is set to True.

        Args:

        Returns:
            
        """
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        pain_groups = key['pain_groups']
        check_requirements = key['check_requirements']
        scaler_type = key['pca_scaler_type']
        
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        
        total_pca_info = {}
        for target, axis in existing_target_axes:
            prepared_data = self.prepare(key, target, axis)
            #if check_requirements:
            #    self.check_pca_requirements(prepared_data=prepared_data, target=target, axis=axis, pain_groups=pain_groups)
            df_pca = prepared_data["df_pca"]
            df_part = prepared_data["df_part"]
            pain_group_ids = prepared_data["pain_group_ids"]
            measurement_type_id = prepared_data["measurement_type_id"]
            if scaler_type:
                # TODO: wrap this in a function
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
            pca_info, pc_sample_scores = self.conduct_pca_analysis(df_analysis, df_part, measurement_type_id)
            total_pca_info.update({
                f'{target}_{axis}' : {
                    'scaler_id' : scaler_id,
                    'scaler_info' : scaler_info,
                    'data_scaled' : True if scaler_type else False,
                    'pca_info' : pca_info,
                    'pc_sample_scores' : pc_sample_scores,  
                    'pain_group_ids' : pain_group_ids, 
                    'rotation_type' : 'unrotated'              
                }
            })
        return total_pca_info
    
    def run_pca_rotated(self, key):
        # TODO: improve this function (make it slimmer, less complex and check dependencies)                
        """
        Perform rotation (e.g., varimax) on PCA loadings and recompute scores.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            measurement_tp (str): Measurement time point.
            pain_groups (list): List of pain groups.
            scaler_type (str): Scaler type used.
            method (str): Rotation method name.

        Returns:
            dict: Rotation information including rotated loadings and scores.
        """
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        pain_groups = key['pain_groups']
        check_requirements = key['check_requirements']
        scaler_type = key['pca_scaler_type']
        rotation_method = key['rotation_method']
        
        pca_df = self.data_loader.load_pc_data(exp_id, device, measurement_tp, pain_groups, 'normal_distribution', rotation_type='unrotated')
        existing_target_axes = pca_df[['target', 'axis']].drop_duplicates().values.tolist()
        total_rotation_info = {}
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df_target_axis = pca_df[(pca_df['target'] == target) & (pca_df['axis'] == axis)]
            loading_vector_json = df_target_axis['loading_vector'].drop_duplicates().values
            loading_vectors = []
            for component in loading_vector_json:
                loading_vector = np.array(PC_Ranked.list_from_json(component))
                loading_vectors.append(loading_vector)
            loading_vectors = np.array(loading_vectors)
            if len(loading_vectors) > 1:
                pivoted = df_target_axis.pivot(index='sample_id', columns='pc_index', values='pc_score')
                measurement_type_id = int(df_target_axis['meas_type_id'].drop_duplicates().values)
                scaler, existing_scaler_info = self.data_loader.load_specific_scaler(measurement_type_id, scaler_type)
                scaler_id = existing_scaler_info.id
                scaler_info = {
                    'mean' : scaler.mean_,
                    'scale' : scaler.scale_,
                    'scaler_type' : scaler_type
                }
                rotated_loadings = self.rotate_pc_loadings(loading_vectors, method = rotation_method )
            
                df_sorted, _  = self.load_data_for_pca(key, target, axis, participant_ids)
                df_transformed = self.process_data_for_pca(df_sorted)
                df_outliers_removed, _ = self.check_and_remove_outliers(df_transformed)
                df_part, df_pca = df_outliers_removed.iloc[:, :-202], df_outliers_removed.iloc[:, -202:]
            
                rotated_scores = self.rotate_pc_scores(df_pca, scaler, rotated_loadings)
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
                rotation_method = rotation_method if nr_components > 1 else 'Unrotated'
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
        return total_rotation_info    
          
    def prepare_unrotated(self, key, target, axis):
        """"""
        pain_groups = key['pain_groups']
        check_requirements = key['check_requirements']
        
        participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
        df_sorted, df_sorted_transformed  = self.load_data_for_pca(key, target, axis, participant_ids)
        df_transformed = self.process_data_for_pca(df_sorted)
        df_outliers_removed, count_outliers = self.check_and_remove_outliers(df_transformed)
        print(f"{target} {axis} Outliers (after key norm): {count_outliers}\t")
        df_part, df_pca = df_outliers_removed.iloc[:, :-202], df_outliers_removed.iloc[:, -202:]
        measurement_type_id = int(df_transformed['measurement_type_id'].unique()[0])
        prepared_data = {
            "target": target,
            "axis": axis,
            "participant_ids" : participant_ids,
            "pain_group_ids" : pain_group_ids,
            "df_sorted" : df_sorted,
            "df_sorted_transformed": df_sorted_transformed, 
            "df_transformed" : df_transformed,
            "df_part" : df_part,
            "df_pca" : df_pca,
            "measurement_type_id": measurement_type_id
        }
        if check_requirements:
            self.check_pca_requirements(prepared_data, target, axis, pain_groups)
        return prepared_data
    
    def prepare_rotated():
        """TODO"""
        
    def load_data_for_pca(self, key, target, axis, participant_ids) -> tuple[pd.DataFrame, pd.DataFrame]:

        """
        Load and preprocess data for PCA analysis for one target-axis pair.

        Args:
            

        Returns:
            tuple: Raw sorted dataframe and transformed dataframe for PCA.
        """
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        
        df = self.data_loader.clean_data_by_exp_device_tp_target_axis(exp_id, device, measurement_tp, target, axis, participant_ids)
        print(f"{target} {axis} loaded")
        df_reduced = df[[
            'participant_id', 'ext_participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever',
            'measurement_id', 'measurement_type_id', 'target', 'axis', 'sample_id', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
            'value'
        ]]
        df_sorted = df_reduced.sort_values(by=['participant_id', 'bow_stroke', 'up_down', 'dp_time_point'])  
        df_sorted_transformed = self.data_processor.pivot_full_cycles_to_wide(df_sorted, 'value','dp_time_point')
          
        return df_sorted, df_sorted_transformed
        
    def check_pca_requirements(self, prepared_data, target, axis, pain_groups):
        # TODO create function for this part
        df_sorted_transformed = prepared_data["df_sorted_transformed"]
        df_transformed = prepared_data["df_transformed"]
        pain_group_names = self.data_plotter.concat_pain_groups(pain_groups)
        
        self.plot_data_linearity(target, axis, df_sorted_transformed, pain_groups)
        corr_mat = self.assumptions_tester.check_pca_requirements(df_sorted_transformed.iloc[:, -202:])
        fig_corr_mat = self.assumptions_tester.plot_pca_requirements(corr_mat, target, axis)
        current_path = Path.cwd()
        
        output_path = current_path / "output" / "plots" / "Correlation_matrix" / f"{pain_group_names}"
        output_path.mkdir(parents=True, exist_ok=True)
        self.data_plotter.save_plot(fig_corr_mat, output_path, f"Original_Data_Correlation_matrix_{target}_{axis}")
        
        key_cont_corr_mat = self.assumptions_tester.check_pca_requirements(df_transformed.iloc[:, -202:])
        key_cont_fig_corr_mat = self.assumptions_tester.plot_pca_requirements(key_cont_corr_mat, target, axis)
        self.data_plotter.save_plot(key_cont_fig_corr_mat, output_path, f"Mean_Subt_Data_Correlation_matrix_{target}_{axis}")

    def scale_df(self, df:pd.DataFrame, scaler_type:str) -> tuple[pd.DataFrame, dict]:
        """
        Scale the dataframe using specified scaler.

        Args:
            df (pd.DataFrame): Data to scale.
            scaler_type (str): Scaler type.

        Returns:
            tuple: Scaled dataframe and scaler info dictionary.
        """
        scaled_df, scaler =self.standardize_df(df)

        scaler_info = {
            'mean' : list(scaler.mean_),
            'scale' : list(scaler.scale_),
            'scaler_type' : scaler_type
        }
        return scaled_df, scaler_info
    
    def upload_scaler(self, measurement_type_id:int, scaler_info:dict[str, any]) -> int:
        """
        Upload scaler parameters to the database.

        Args:
            measurement_type_id (int): Measurement type ID.
            scaler_info (dict): Dictionary with mean, scale, and scaler_type.

        Returns:
            int: ID of the uploaded scaler.
        """
        mean = scaler_info['mean']
        scale = scaler_info['scale']
        scaler_type = scaler_info['scaler_type']
        scaler_id = self.data_loader.upload_scaler(measurement_type_id, scaler_type, mean, scale)
        return scaler_id
    
    def conduct_pca_analysis(self, df:pd.DataFrame, df_part:pd.DataFrame, measurement_type_id:int) -> tuple[dict, dict]:
        """
        Perform PCA on the data and collect component loadings and scores.

        Args:
            df (pd.DataFrame): Data for PCA.
            df_part (pd.DataFrame): Participant metadata.
            measurement_type_id (int): Measurement type ID.
            data_scaled (bool): Whether data is scaled.
            scaler_id (int, optional): Scaler ID used.

        Returns:
            tuple: PCA components info dict and PC sample scores dict.
        """     
        variance_level = 0.9
        explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores = self.apply_pca(df, variance_level)
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
            pc_sample_scores = list(zip(df_part['sample_id'], df_pca_scores.iloc[:,component_idx]))
            pc_scores.update({f'component_{pc_index}_scores' : pc_sample_scores})

        return pca_info, pc_scores

    def process_data_for_pca(self, df_sorted:pd.DataFrame) -> pd.DataFrame:
        """
        Normalize and pivot data for PCA input.

        Args:
            df_sorted (pd.DataFrame): Raw sorted data.

        Returns:
            pd.DataFrame: Transformed dataframe suitable for PCA.
        """
        # TODO: save mean key per target/axis? / plot mean key? 
        df_key_normalized = self.data_processor.subtract_meanwave_key_difference(df_sorted)
        df_transformed = self.data_processor.pivot_full_cycles_to_wide(df_key_normalized, 'value_centered','dp_time_point')
        return df_transformed
    
    def check_and_remove_outliers(self, df:pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """
        Detect and remove outliers from the data using a sliding window approach.

        Args:
            df (pd.DataFrame): Dataframe to process.

        Returns:
            tuple: Dataframe with outliers removed and count of removed outliers.
        """
        #TODO: save which dp are outliers
        df_outliers_removed, count_outliers = self.data_processor.sliding_window_outlier_detection(df)
        return df_outliers_removed, count_outliers
    
    def plot_data_linearity(self, target:str, axis:str, df:pd.DataFrame, pain_groups:list[str]):
        #TODO: add the pre/post name to the plot title, so that it is not overwritten
        # TODO: this should be part of the data plotter class
        """
        Plot and save linearity diagnostics for given target and axis.

        Args:
            target (str): Target variable.
            axis (str): Axis name.
            df (pd.DataFrame): Dataframe with data to plot.
        """
        pain_group_names = self.data_plotter.concat_pain_groups(pain_groups)
        fig_title = f"{target}, {axis}"
        fig1, fig2 = self.data_plotter.plot_linearity(df, fig_title)
        current_path = Path.cwd()
        #output_path = current_path / "output" / "plots"
        output_path = current_path / "output" / "plots" / "Linearity_plots" / f"{pain_group_names}"
        output_path.mkdir(parents=True, exist_ok=True)
        self.data_plotter.save_plot(fig1, output_path, f"Scatter_matrix_{target}_{axis}")
        self.data_plotter.save_plot(fig2, output_path, f"Lag_plot_{target}_{axis}") 
        
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
    
    def upload_pca_analysis(self, pca_info:dict[str, any]):
        """
        Upload PCA analysis results including components and scores to the database.

        Args:
            pca_info (dict): Dictionary containing PCA results per target-axis.
        """
        for target_axis, target_axis_data in pca_info.items():
            scaler_id = target_axis_data['scaler_id']
            data_scaled = target_axis_data['data_scaled']
            pc_info = target_axis_data['pca_info']
            sample_scores = target_axis_data['pc_sample_scores']
            rotation_type = target_axis_data['rotation_type']
            rotation_id = self.data_loader.upload_new_rotation_type(rotation_type)
            pc_ids = self.upload_pca_analysis_target_axis(pc_info, sample_scores, scaler_id, data_scaled, rotation_id)
            pain_group_ids = target_axis_data['pain_group_ids']
            for pain_group_id in pain_group_ids:
                for pc_id in pc_ids:
                    self.data_loader.upload_pc_pain_group_ids(pc_id, pain_group_id)
                    
    def upload_pca_analysis_target_axis(self, pca_info:dict[str, any], pc_sample_scores:dict[str, any], scaler_id:int | None = None, data_scaled:bool = False, 
                                        rotation_id:int |None = None)-> list[int]:
        """
        Upload PCA components and scores for one target-axis pair.

        Args:
            pca_info (dict): PCA components info.
            pc_sample_scores (dict): PC scores per sample.
            scaler_id (int, optional): Scaler ID.
            data_scaled (bool): Whether data was scaled.
            rotation_id (int, optional): Rotation type ID.

        Returns:
            list: List of uploaded PC IDs.
        """
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
    
    def reconstruct_pcas(self, key, select_rotated = False):
        """
        Reconstruct and plot principal components for selected conditions.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            measurement_tp (str): Measurement time point ('pre'/'post').
            pain_groups (list): Pain groups to include.
            scaler_type (str): Scaler used for data.
            nr_components (int, optional): Number of components to reconstruct.
            select_rotated (bool): Whether to reconstruct rotated PCs.
        """
        # TODO: make this function cleaner (check dependencies etc.)
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        pain_groups = key['pain_groups']
        nr_components = key['nr_components']
        scaler_type = key['pca_scaler_type']
        rotation_type = key['rotation_method']
        t_test_assumptions_relevant = key['t_test_assumptions_relevant']
        t_test_distribution_type = key['t_test_distribution_type']
               
        ranked_pc_scores_df = self.data_loader.load_pcs_by_rank(exp_id, device,measurement_tp,pain_groups, 
                                                                nr_components= nr_components,select_rotated= select_rotated, 
                                                                use_distribution= t_test_assumptions_relevant, distribution_type = t_test_distribution_type)
        pain_group_names = self.data_plotter.concat_pain_groups(pain_groups)
        ranked_pc_scores_df["loading_vector"] = ranked_pc_scores_df["loading_vector"].apply(PC_Ranked.list_from_json)
        #np.array(PC_Ranked.list_from_json(ranked_pc_scores_df['loading_vector'].unique()[0]))
        overall_min = ranked_pc_scores_df["loading_vector"].explode().min()
        overall_max = ranked_pc_scores_df["loading_vector"].explode().max()
        
        #fig = self.data_plotter.plot_top_3_PCAs()
        orig_data_top_3_components = pd.DataFrame()
        top_3_components = {}
        for id, pc_id in enumerate(ranked_pc_scores_df['pc_id'].unique()):
            rank = id+1
            current_pc_df = ranked_pc_scores_df[ranked_pc_scores_df['pc_id'] == pc_id]
            target, axis = current_pc_df['target'].unique()[0], current_pc_df['axis'].unique()[0]
            measurement_type_id = int(current_pc_df['meas_type_id'].unique()[0])
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df = self.data_loader.clean_data_by_exp_device_tp_target_axis(exp_id, device, measurement_tp, target, axis, participant_ids)
            df_reduced = df[[
                    'participant_id', 'ext_participant_id', 'PRMD_ever',
                    'measurement_id','measurement_type_id', 'target', 'axis', 'sample_id', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
                    'value'
                ]]
            df_keynorm = self.data_processor.subtract_meanwave_key_difference(df_reduced)
            df_transformed = self.data_processor.pivot_full_cycles_to_wide(df_keynorm, 'value', 'dp_time_point')
            
            scaler, _ = self.data_loader.load_specific_scaler(measurement_type_id, scaler_type)
            component_reconstruction_data = self.reconstruct_single_component(current_pc_df, df_transformed, scaler)
            if id <= 2:
                top_3_components[f"Rank_{rank}"] = {"target": target,
                                                    "axis": axis,
                                                    "pc_index": current_pc_df['pc_index'].unique()[0],
                                                    "rotation_sequence": df['rotation_sequence'].unique()[0],
                                                    "component_data":component_reconstruction_data}
                orig_data_top_3_components = pd.concat([orig_data_top_3_components, df_transformed])
            pc_name = "PC" + str(current_pc_df['pc_index'].unique()[0])
            rotated_suffix_title = " Rotated" if select_rotated else ""
            rotated_suffix_fname = "_Rotated" if select_rotated else ""

            title_reconstruction = f'Single component reconstruction: Rank {rank}, {target} {axis}; {pc_name}{rotated_suffix_title}'
            title_loading_vector = f'Loading vector: Rank {rank} {target} {axis}; {pc_name}{rotated_suffix_title}'

            # TODO: adjust the scaling of loading vector in the plots!
            # TODO: create only 1 plot for PCA reconstruction -> regular, per group, loading vector
            fig = self.data_plotter.plot_PCA_reconstruction(component_reconstruction_data, title_reconstruction, title_loading_vector, lv_ymax= overall_max, 
                                                            lv_ymin= overall_min)
            
            distribution_label = f"{t_test_distribution_type}" if t_test_assumptions_relevant else "no_distribution_tested"
            rotation_label = f"{rotation_type}" if select_rotated else "unrotated"
            
            current_path = Path.cwd()
            output_path = current_path / "output" / "plots" / "PCA_Reconstruction" / f"{pain_group_names}" / f"{distribution_label}" / f"{rotation_label}"
            output_path.mkdir(parents=True, exist_ok=True)
            fig_name = f"{measurement_tp}{rotated_suffix_fname}_Rank_{rank}_{target}_{axis}_{pc_name}"
            self.data_plotter.save_plot(fig, output_path, fig_name)
        
        fig = self.data_plotter.plot_top_3_PCAs(orig_data_top_3_components, top_3_components)
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots" / "PCA_Top_3" / f"{pain_group_names}" / f"{distribution_label}" / f"{rotation_label}"
        output_path.mkdir(parents=True, exist_ok=True)
        fig_name = f"{measurement_tp}{rotated_suffix_fname}_TOP_3_PCs"
        self.data_plotter.save_plot(fig, output_path, fig_name)
        
        fig = self.data_plotter.plot_top_3_PCAs_reduced(orig_data_top_3_components, top_3_components)
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots" / "PCA_Top_3" / f"{pain_group_names}" / f"{distribution_label}" / f"{rotation_label}"
        output_path.mkdir(parents=True, exist_ok=True)
        fig_name = f"{measurement_tp}{rotated_suffix_fname}_TOP_3_PCs_reduced"
        self.data_plotter.save_plot(fig, output_path, fig_name)
            #plt.close()
            #fig = self.data_plotter.plot_PCA_reconstruction_per_group(component_reconstruction_data, title_reconstruction, title_loading_vector)
            #fig_name = f"{measurement_tp}{rotated_suffix_fname}_per_group_Rank_{rank}_{target}_{axis}_{pc_name}"
            #self.data_plotter.save_plot(fig, output_path, fig_name)
            #plt.close()
            
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
        
        loading_vector = np.array(pc_df['loading_vector'].iloc[0])
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