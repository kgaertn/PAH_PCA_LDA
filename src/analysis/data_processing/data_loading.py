from data_access.scaler_repo import ScalerRepository
from data_access.pc_ranked_repository import PCRankedRepository
from data_access.pc_scores_repository import PCScoresRepository
#from data_access.experiment_repository import ExperimentRepository
from data_access.participant_repository import ParticipantRepository
from data_access.measurement_repository import MeasurementRepository
from data_access.sample_repository import SampleRepository
from data_access.datapoint_repository import DatapointRepository
from data_access.rotation_repository import RotationRepository
from data_access.pain_group_repository import PainGroupRepository
from models.scaler import Scaler
from models.pc_ranked import PC_Ranked
from models.pc_scores import PC_Scores

import pandas as pd
from sklearn.preprocessing import StandardScaler

class DataLoader:
    
    def __init__(self):
        """
        Initializes the PCA_Analyser with a data processor.
        """    
        #self.exp_repo = ExperimentRepository()
        self.part_repo = ParticipantRepository()
        self.meas_repo = MeasurementRepository()
        self.samp_repo = SampleRepository()
        self.dp_repo = DatapointRepository()  
        self.scaler_repo = ScalerRepository()
        self.pc_ranked_repo = PCRankedRepository()
        self.pc_scores_repo = PCScoresRepository()
        self.rot_repo = RotationRepository()
        self.pain_group_repo = PainGroupRepository()
        
    def upload_distribution_info(self, pc_distributions):
        """"""
        self.pc_ranked_repo.update_pc_score_distribution_info(pc_distributions)
        
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
        self.pc_scores_repo.insert_many_pc_scores(pc_scores)
        
    def load_pc_data(self, exp_id, device, meas_timepoint, distribution_info = None, rotation_type = 'unrotated'):
        df = self.pc_scores_repo.get_pc_scores_by_exp_id_device(exp_id, device, meas_timepoint, distribution_info, rotation_type)
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
        scaler_info = self.scaler_repo.get_scaler_by_meas_type_id_scaler_type(meas_type_id, scaler_type)    
        scaler = StandardScaler()
        scaler.mean_ = scaler_info.mean
        scaler.scale_ = scaler_info.scale
        return scaler, scaler_info
    
    
    def get_existing_target_axis_exp(self,experiment = 1, device = 'mocap', meas_time_point = 'pre'):
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        target_axes = self.meas_repo.get_advanced(
            table_or_view = 'measurement_type',
            columns=['target', 'axis'], 
            exclude={'rotation_sequence': 'carrying_angle'},
            order_by=['target', 'axis'],
            experiment_id = experiment,
            device = device,
            meas_time_point = meas_time_point,
            return_df=False )
        return target_axes  
    
    def load_MPA_clean_data_by_device_tp_target_axis(self, exp_id, device:str, timepoint:str, target:str, axis:str = None, participant_ids:list[int] = None):
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        df = self.dp_repo.get(table_or_view="[Complete Data]", experiment_id = exp_id, device = device, timepoint = timepoint, 
                              target = target, axis = axis, participant_id = participant_ids)
        return df   
         
    def load_MPA_clean_data_by_device_tp_target_axis_participants(self,exp_id, device:str, timepoint:str, target:str,  part_ids:tuple, axis:str = None):
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        df = self.dp_repo.get(table_or_view="[Complete Data]",experiment_id = exp_id, device = device, timepoint = timepoint, 
                              target = target, axis = axis)
        return df   
    
    def load_data_one_joint(self, exp_id, device, timepoint, target:str) -> pd.DataFrame:
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        df = self.dp_repo.get(table_or_view="[Complete Data]",experiment_id = exp_id, device = device, timepoint = timepoint, 
                              target = target)
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    def load_full_device_data(self, exp_id, device:str, timepoint) -> pd.DataFrame:
        """
        Load and filter all datapoints for a given device at the 'pre' timepoint
        of experiment ID 1.

        Args::
            device (str): The name of the device to retrieve data from.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified device.
        """
        df = self.dp_repo.get(table_or_view="[Complete Data]", experiment_id = exp_id, device = device, timepoint = timepoint)
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    def load_existing_rotation_ids(self):
        return self.rot_repo.get_existing_rotations()
    
    def load_existing_pain_groups(self):
        return self.pain_group_repo.get_existing_pain_groups()
        """"""
    def load_participants_by_pain_group(self, pain_group_id):
        return self.pain_group_repo.get_participant_ids_by_pain_group(pain_group_id)
    
    def upload_new_pain_group(self, pain_group):
        pain_group_id = self.pain_group_repo.insert_pain_group(pain_group)
        if pain_group == 'healthy':
            participant_ids = self.part_repo.get_pain_participants(['PRMD_ever'], 0)
        else:       
            column_names = self.part_repo.get_participant_column_names()
            pain_columns = [name for name in column_names if pain_group.lower() in name.lower()]
            participant_ids = self.part_repo.get_pain_participants(pain_columns, 1)
        self.pain_group_repo.insert_participant_pain_groups(participant_ids, pain_group_id)
        return participant_ids     
        print("")
        