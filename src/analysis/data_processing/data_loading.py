from data_access.scaler_repo import ScalerRepository
from data_access.pc_ranked_repository import PCRankedRepository
from data_access.pc_scores_repository import PCScoresRepository
from data_access.experiment_repository import ExperimentRepository
from data_access.participant_repository import ParticipantRepository
from data_access.measurement_repository import MeasurementRepository
from data_access.sample_repository import SampleRepository
from data_access.datapoint_repository import DatapointRepository
from data_access.rotation_repository import RotationRepository
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
        self.exp_repo = ExperimentRepository()
        self.part_repo = ParticipantRepository()
        self.meas_repo = MeasurementRepository()
        self.samp_repo = SampleRepository()
        self.dp_repo = DatapointRepository()  
        self.scaler_repo = ScalerRepository()
        self.pc_ranked_repo = PCRankedRepository()
        self.pc_scores_repo = PCScoresRepository()
        self.rot_repo = RotationRepository()
        
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
        self.pc_scores_repo.insert_multiple_pc_scores(pc_scores)
        
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
        scaler_info = self.scaler_repo.get_sacler_by_meas_type_id_scaler_type(meas_type_id, scaler_type)    
        scaler = StandardScaler()
        scaler.mean_ = scaler_info.mean
        scaler.scale_ = scaler_info.scale
        return scaler, scaler_info
    
    
    def get_existing_target_axis_exp(self, device:str, experiment = 1, meas_time_point = 'pre'):
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        target_axes = self.meas_repo.get_existing_measurement_target_axis_by_device(experiment, device, meas_time_point)
        return target_axes  
    
    def load_MPA_clean_data_by_device_tp_target_axis(self, device:str, timepoint:str, target:str, axis:str = None):
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        df = self.dp_repo.get_datapoints_by_exp_id_device_timepoint_target_axis(1, device, timepoint, target, axis)
        return df   
         
    def load_MPA_clean_data_by_device_tp_target_axis_participants(self, device:str, timepoint:str, target:str,  part_ids:tuple, axis:str = None):
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        df = self.dp_repo.get_datapoints_by_exp_id_device_timepoint_target_axis_part_ids(1, device, timepoint, target, part_ids, axis)
        return df   
    
    def load_data_one_joint(self, joint:str) -> pd.DataFrame:
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        df = self.dp_repo.get_datapoints_by_exp_id_device_timepoint_target(1, 'mocap', 'pre', joint)
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    def load_full_device_data(self, device:str) -> pd.DataFrame:
        """
        Load and filter all datapoints for a given device at the 'pre' timepoint
        of experiment ID 1.

        Args::
            device (str): The name of the device to retrieve data from.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified device.
        """
        df = self.dp_repo.get_datapoints_by_exp_id_device_and_timepoint(1, device, 'pre')
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    def load_existing_rotation_ids(self):
        return self.rot_repo.get_existing_rotations()
        