from data_access.repositories.experiment_repository import ExperimentRepository
from data_access.repositories.scaler_repository import ScalerRepository
from data_access.repositories.pc_ranked_repository import PCRankedRepository
from data_access.repositories.pc_scores_repository import PCScoresRepository
from data_access.repositories.participant_repository import ParticipantRepository
from data_access.repositories.measurement_repository import MeasurementRepository
from data_access.repositories.sample_repository import SampleRepository
from data_access.repositories.datapoint_repository import DatapointRepository
from data_access.repositories.rotation_repository import RotationRepository
from data_access.repositories.pain_group_repository import PainGroupRepository
from data_access.repositories.lda_repository import LDARepository
from data_access.models.scaler import Scaler
from data_access.models.pc_ranked import PC_Ranked
from data_access.models.pc_scores import PC_Scores
from data_access.models.rotation import RotationPCA
from data_access.models.pain_group import PainGroup

import pandas as pd
from sklearn.preprocessing import StandardScaler

class DataLoader:
    
    def __init__(self):
        """
        Initializes the DataLoader with repository instances.
        """    
        self.exp_repo = ExperimentRepository()
        self.part_repo = ParticipantRepository()
        self.meas_repo = MeasurementRepository()
        self.samp_repo = SampleRepository()
        self.dp_repo = DatapointRepository()  
        self.scaler_repo = ScalerRepository()
        self.pc_ranked_repo = PCRankedRepository()
        self.pc_scores_repo = PCScoresRepository()
        self.rot_repo = RotationRepository()
        self.pain_group_repo = PainGroupRepository()
        self.lda_repo = LDARepository()
        
    def upload_distribution_info(self, pc_distributions:list[PC_Ranked]):
        """
        Upload PC score distribution metadata.

        Args:
            pc_distributions (list[PC_Ranked]): A list of the ranked pcs and their distribution information.
        """
        self.pc_ranked_repo.update_pc_score_distribution_info(pc_distributions)
        
    def upload_scaler(self, meas_type_id:int, scaler_type:str, mean:list[float], scale:list[float]) -> int:
        """
        Save a scaler configuration to the database.

        Args:
            meas_type_id (int): ID of the measurement type.
            scaler_type (str): Type of scaler used (e.g., 'standard').
            mean (list[float]): Mean values used by the scaler.
            scale (list[float]): Scale values used by the scaler.

        Returns:
            int: ID of the inserted scaler.
    """
        scaler = Scaler(
            id = 1,
            measurement_type_id=meas_type_id,
            scaler_type=scaler_type,
            mean=mean,
            scale=scale
        )
        scaler_id = self.scaler_repo.insert_new_scaler(scaler)
        return scaler_id
    
    def upload_pca(self, meas_type_id:int, parent_id:int, pc_index:int, loading_vector:list[float], explained_variance:float, data_scaled:bool, 
                   scaler_id:int | None, rotation_id:int | None) -> int:
        """
        Save PCA component metadata to the database.

        Args:
            meas_type_id (int): Measurement type ID.
            parent_id (int): Parent PCA entity ID.
            pc_index (int): Principal component index.
            loading_vector (list[float]): Loadings of the component.
            explained_variance (float): Variance explained by the component.
            data_scaled (bool): Information on whether the data is scaled or not
            scaler_id (int |None): ID of the associated scaler.
            rotation_id (int |None): ID of the rotation type.

        Returns:
            int: ID of the inserted PCA record.
        """
        pc = PC_Ranked(
            id = 1,
            measurement_type_id=meas_type_id,
            parent_id = parent_id,
            scaler_id = scaler_id,
            rotation_id=rotation_id,
            pc_index=pc_index,
            loading_vector=loading_vector,
            explained_variance=explained_variance,
            data_scaled=data_scaled
        )
        pc_id = self.pc_ranked_repo.insert_new_pc(pc)
        return pc_id
    
    def upload_pc_scores(self, pc_id:int, sample_scores:dict):
        """
        Save PC scores for samples.

        Args:
            pc_id (int): ID of the principal component.
            sample_scores (dict): List of (sample_id, score) pairs.

        Returns:
            None
        """
        pc_scores = []
        for sample, score in sample_scores:
            pc_scores.append(PC_Scores(
                id = 1,
                pc_id=pc_id,
                sample_id=sample,
                pc_score=score
            ))
        self.pc_scores_repo.insert_many_pc_scores(pc_scores)
        
    def load_pc_data(self, exp_id:int, device:str, meas_timepoint:str, pain_groups:list[str], distribution_info:str | None = None, 
                     rotation_type:str | None = 'unrotated') -> pd.DataFrame:
        """
        Load principal component scores for a given experiment, device, and measurement timepoint.

        Args:
            exp_id (int): Experiment ID
            device (str): Device name (e.g., 'mocap')
            meas_timepoint (str): Measurement timepoint (e.g., 'pre', 'post')
            distribution_info (str | None): Optional filter for distribution information

        Returns:
            pd.DataFrame: DataFrame containing the principal component scores
        """
        df = self.pc_scores_repo.get_pc_scores_by_exp_id_device(exp_id=exp_id, device=device, meas_timepoint=meas_timepoint, pain_groups=pain_groups, 
                                                                distribution_info=distribution_info, rotation_type=rotation_type)
        return df
    
    def load_pcs_by_rank(self, exp_id:int, device:str, meas_tp:str, pain_groups:list[str], nr_components:int | None = None, select_rotated:bool = False, 
                         use_distribution: bool = False, distribution_type: str | None = "normal_distribution"):
        """
        Load principal components ordered by rank, optionally selecting rotated components.

        Args:
            exp_id: Experiment ID
            device: Device name
            meas_tp: Measurement timepoint
            nr_components (int | None): Number of components to load (optional)
            select_rotated (bool): Whether to load rotated PCs (default False)

        Returns:
            pd.DataFrame: DataFrame of selected principal components
        """
        pain_group_names = [", ".join(pain_groups), ", ".join(reversed(pain_groups))]
        if select_rotated:
            df = self.pc_scores_repo. get_rotated_pc_scores(exp_id, device, meas_tp, pain_group_names, nr_components= nr_components, 
                                                            use_distribution = use_distribution, distribution_type = distribution_type)
        else:
            df = self.pc_scores_repo. get_unrotated_pc_scores(exp_id, device, meas_tp,pain_group_names,  nr_components= nr_components,
                                                              use_distribution = use_distribution, distribution_type = distribution_type)
        return df
    
    def upload_t_test_results(self, df:pd.DataFrame):
        """
        Upload t-test results from a DataFrame into the database.

        Args:
            df (pd.DataFrame): DataFrame containing columns: pc_id, mean_pain, mean_no_pain,
                            std_pain, std_no_pain, t_value, p_value
        """
        t_test_results = []
        for idx, row in df.iterrows():
            t_test_results.append(PC_Ranked(
                id = row['pc_id'],
                measurement_type_id= 1,
                group_mean_pain=row['mean_pain'],
                group_mean_no_pain=row['mean_no_pain'],
                group_std_pain=row['std_pain'],
                group_std_no_pain=row['std_no_pain'],
                t_value=row['t_value'],
                p_value=row['p_value']                
            ))
        self.pc_ranked_repo.update_multiple_t_test_info(t_test_results)
   
    def load_specific_scaler(self, meas_type_id:int, scaler_type:str) -> tuple[StandardScaler | None, Scaler | None]:
        """
        Load a specific scaler from the database and return it as a sklearn StandardScaler object.

        Args:
            meas_type_id: Measurement type ID
            scaler_type: Type of scaler (e.g., 'standard')

        Returns:
            tuple: (StandardScaler instance or None if not found, scaler information record)
        """
        scaler_info = self.scaler_repo.get_scaler_by_meas_type_id_scaler_type(meas_type_id, scaler_type)  
        if scaler_info != None:  
            scaler = StandardScaler()
            scaler.mean_ = scaler_info.mean
            scaler.scale_ = scaler_info.scale
        else:
            scaler = None
        return scaler, scaler_info
        
    def get_existing_target_axis_exp(self,experiment:int = 1, device:str = 'mocap', meas_time_point:str = 'pre') -> list[str, str]:
        """
        Get existing target and axis pairs for a given experiment, device, and timepoint.

        Args:
            experiment (int): Experiment ID.
            device (str): Device name.
            meas_time_point (str): Measurement timepoint.

        Returns:
            list: List of target-axis pairs.
        """
        target_axes = self.meas_repo.get_advanced(
            table_or_view = 'measurement_type',
            columns=['target', 'axis'], 
            exclude={'rotation_sequence': ['carrying_angle','redundant']},
            order_by=['target', 'axis'],
            experiment_id = experiment,
            device = device,
            meas_time_point = meas_time_point,
            return_df=False )
        return target_axes  
    
    def clean_data_by_exp_device_tp_target_axis(self, exp_id:int, device:str, timepoint:str, target:str, axis:str | None = None, 
                                                     participant_ids:list[int] | None = None):
        """
        Load clean data filtered by exp_id, device, timepoint, target, axis, and participants.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            timepoint (str): Measurement timepoint.
            target (str): Target joint or data point.
            axis (str | None): Axis specification.
            participant_ids (list[int] | None): List of participant IDs.

        Returns:
            pd.DataFrame: Filtered data.
    """
        df = self.dp_repo.get(table_or_view="[Complete Data]", experiment_id = exp_id, device = device, timepoint = timepoint, 
                              target = target, axis = axis, participant_id = participant_ids)
        return df   
         
    def load_clean_data_by_exp_device_tp_target_axis_participants(self,exp_id:int, device:str, timepoint:str, target:str,  
                                                                  part_ids:list[int] = None, axis:str = None) -> pd.DataFrame:
        """
        Load clean data filtered by exp_id, device, timepoint, target, axis, and participant IDs.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            timepoint (str): Measurement timepoint.
            target (str): Target joint or data point.
            part_ids (list[int] | None): Participant IDs.
            axis (str | None): Axis specification.

        Returns:
            pd.DataFrame: Filtered data.
        """
        df = self.dp_repo.get(table_or_view="[Complete Data]",experiment_id = exp_id, device = device, timepoint = timepoint, 
                              target = target, axis = axis, participant_id = part_ids)
        return df   
    
    def load_data_one_joint(self, exp_id:int, device:str, timepoint:str, target:str) -> pd.DataFrame:
        """
        Load data for one joint filtered by experiment, device, timepoint, and target.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            timepoint (str): Measurement timepoint.
            target (str): Target joint.

        Returns:
            pd.DataFrame: Filtered data with selected columns.
        """
        df = self.dp_repo.get(table_or_view="[Complete Data]",experiment_id = exp_id, device = device, timepoint = timepoint, 
                              target = target)
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    def load_full_device_data(self, exp_id:int, device:str, timepoint:str) -> pd.DataFrame:
        """
        Load full device data filtered by experiment, device, and timepoint.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            timepoint (str): Measurement timepoint.

        Returns:
            pd.DataFrame: Filtered data with selected columns.
        """
        df = self.dp_repo.get(table_or_view="[Complete Data]", experiment_id = exp_id, device = device, timepoint = timepoint)
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    def load_existing_rotation_ids(self) -> list[RotationPCA]:
        """
        Load all existing rotation IDs.

        Returns:
            list | None: List of RotationPCA objects with rotation id and rotation type or None, if no rotation objects are found
        """
        return self.rot_repo.get_existing_rotations()
    
    def load_existing_pain_groups(self) -> list[PainGroup] | None:
        """
        Load all existing pain groups.

        Returns:
            list | None: List of pain group objects, with pain group id and pain group name or None, if no pain groups are found
        """
        return self.pain_group_repo.get_existing_pain_groups()

    def load_participants_by_pain_group(self, pain_group_id:int) -> list[int]:
        """
        Load participant IDs belonging to a pain group.

        Args:
            pain_group_id (int): Pain group ID.

        Returns:
            list[int]: List of participant IDs.
        """
        return self.pain_group_repo.get_participant_ids_by_pain_group(pain_group_id)
    
    def upload_new_pain_group(self, pain_group:str) -> tuple[list[int], int]:
        """
        Upload a new pain group and return associated participant IDs and group ID.

        Args:
            pain_group (str): Name of the pain group.

        Returns:
            tuple[list[int], int]: Participant IDs and pain group ID.
        """
        pain_group_id = self.pain_group_repo.insert_pain_group(pain_group)
        if pain_group == 'healthy':
            participant_ids = self.part_repo.get_pain_participants(['PRMD_ever'], 0)
        elif pain_group == 'all':
            participant_ids_no_pain = self.part_repo.get_pain_participants(['PRMD_ever'], 0)
            participant_ids_pain = self.part_repo.get_pain_participants(['PRMD_ever'], 1)
            participant_ids = participant_ids_no_pain + participant_ids_pain
        else:       
            column_names = self.part_repo.get_participant_column_names()
            pain_columns = [name for name in column_names if pain_group.lower() in name.lower()]
            participant_ids = self.part_repo.get_pain_participants(pain_columns, 1)
        self.pain_group_repo.insert_participant_pain_groups(participant_ids, pain_group_id)
        return participant_ids, pain_group_id     
    
    def upload_pc_pain_group_ids(self,pc_id:int, pain_group_id:int):
        """
        Associate a PC with a pain group.

        Args:
            pc_id (int): Principal component ID.
            pain_group_id (int): Pain group ID.
        """
        self.pain_group_repo.insert_pc_pain_group(pc_id, pain_group_id)
        
    def upload_new_rotation_type(self, rotation_type:str) -> int | None:
        """
        Upload a new rotation type if not existing, otherwise return existing ID.

        Args:
            rotation_type (str): Rotation type name.

        Returns:
            int | None: Rotation type ID.
        """
        existing_rotation_types = self.load_existing_rotation_types()        
        existing_rotation_type_names = [rt.rotation_type for rt in existing_rotation_types] if existing_rotation_types else set()        
        if rotation_type not in existing_rotation_type_names:
            rotation_type_id = self.rot_repo.insert_rotation_type(rotation_type)
        else:
            rotation_type_id = next(
                (rt.id for rt in existing_rotation_types if rt.rotation_type == rotation_type),
                None
                )
        return rotation_type_id
    
    def load_existing_rotation_types(self) -> list[RotationPCA]:
        """
        Load all existing rotation types.

        Returns:
            list: List of RotationPCA objects.
        """
        return self.rot_repo.get_existing_rotations()
    
    def get_participants_pain_groups(self, pain_groups:list[str])-> tuple[list[int], list[int]]:
        """
        Retrieve or create pain groups and their participants.

        Args:
            pain_groups (list[str]): Names of the pain groups.

        Returns:
            tuple[list[int], list[int]]: Participant IDs and pain group IDs.
        """
        existing_pain_groups = self.load_existing_pain_groups()        
        existing_group_names = [pg.pain_group for pg in existing_pain_groups] if existing_pain_groups else set()
        all_participant_ids = []
        all_pain_group_ids = []
        for pain_group in pain_groups:
            if pain_group not in existing_group_names:
                participant_ids, pain_group_id = self.upload_new_pain_group(pain_group) 
            else:
                pain_group_id = next(
                    (pg.id for pg in existing_pain_groups if pg.pain_group == pain_group),
                    None
                    )
                participant_ids = self.load_participants_by_pain_group(pain_group_id)
                print("")
                
            all_participant_ids.extend(participant_ids)
            all_pain_group_ids.extend([pain_group_id])
        return all_participant_ids, all_pain_group_ids
    
    def get_experiment_by_name(self, exp_name):
        return self.exp_repo.get_experiment_id_by_name(exp_name)
    
    def load_lda(self,exp_id:int, device:str, measurement_tp:str, pain_groups:list[str], pca_scaled:bool, select_rotated:bool, 
                    rotation_type:str, imputation_type:str, lda_scaler_type, lda_validation_type) -> pd.DataFrame:
        """"""
        
    #def load_pcs_by_rank(self, exp_id:int, device:str, meas_tp:str, pain_groups:list[str], nr_components:int | None = None, select_rotated:bool = False, 
    #                     use_distribution: bool = False, distribution_type: str | None = "normal_distribution"):
        """
        Load principal components ordered by rank, optionally selecting rotated components.

        Args:
            exp_id: Experiment ID
            device: Device name
            meas_tp: Measurement timepoint
            nr_components (int | None): Number of components to load (optional)
            select_rotated (bool): Whether to load rotated PCs (default False)

        Returns:
            pd.DataFrame: DataFrame of selected principal components
        """
        pain_group_names = [", ".join(pain_groups), ", ".join(reversed(pain_groups))]

        if select_rotated:
            rotation_type = rotation_type
        else: 
            rotation_type = 'unrotated'

            df = self.lda_repo.get_lda_results(exp_id, device, measurement_tp, pain_group_names, pca_scaled, rotation_type, imputation_type, 
                                   lda_scaler_type, lda_validation_type)
        return df
    
    def upload_lda_pcs(self, lda_id:int, pc_ids:list[int]):
        """
        Associate a LDA with pc_ids.

        Args:
            lda_id (int): Principal component ID.
            pc_ids list(int): Pain group ID.
        """
        self.lda_repo.insert_lda_pcs(lda_id, pc_ids)
        
    def upload_lda_analysis(self, lda):
        """
        Associate a LDA with pc_ids.

        Args:
            lda_id (int): Principal component ID.
            pc_ids list(int): Pain group ID.
        """
        return self.lda_repo.insert_new_lda(lda)
    
    def check_full_lda_uploaded(self, key, run_rotated):
        exp_id = key['exp_id']
        device = key['device']
        meas_timepoint = key['measurement_tp']
        pain_groups = key['pain_groups']
        pain_group_names = [", ".join(pain_groups), ", ".join(reversed(pain_groups))]
        pca_scaled = True if key['pca_scaler_type'] != None else False
        rotation_type = key['rotation_method'] if run_rotated else "unrotated"
        scaler_type = key['lda_scaler_type']
        imputer_type = key['lda_imputation_type']
        imputer_parameter = key['lda_imputer_parameter']
        validation_type = 'no_validation'
        n_folds = key['lda_splits']
        n_repeats = key['lda_repeats']
        
        uploaded_ldas = self.lda_repo.get_lda_results(exp_id = exp_id, device = device, measurement_tp = meas_timepoint, pain_group_names = pain_group_names, pca_scaled= pca_scaled, 
                                                      rotation_type = rotation_type, lda_imputation_type = imputer_type, lda_scaler_type = scaler_type, lda_validation_type = validation_type)
        
        if uploaded_ldas is None:
            return False
        else: return True
    