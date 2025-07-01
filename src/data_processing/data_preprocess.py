from data_access.experiment_repository import ExperimentRepository
from data_access.participant_repository import ParticipantRepository
from data_access.measurement_repository import MeasurementRepository
from data_access.sample_repository import SampleRepository
from data_access.datapoint_repository import DatapointRepository

from models.sample import Sample

import pandas as pd

class DataProcessor:
    def __init__(self):
        """
        Initializes the DataProcessor with data repositories.
        """
        self.exp_repo = ExperimentRepository()
        self.part_repo = ParticipantRepository()
        self.meas_repo = MeasurementRepository()
        self.samp_repo = SampleRepository()
        self.dp_repo = DatapointRepository()   
    
    def get_existing_target_axis_MPA_Clean(self, device:str):
        """
        Load and filter datapoints for a specific joint from the 'mocap' device
        at the 'pre' timepoint of experiment ID 1.

        Args:
            joint (str): The name of the joint to retrieve data for.

        Returns:
            pd.DataFrame: A filtered DataFrame containing selected columns relevant 
            to the specified joint.
        """
        target_axes = self.meas_repo.get_existing_measurement_target_axis_by_device(1, device)
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
    
    # create samples and upload sample info to DB
    def create_samples(self):
        measurement_ids = self.meas_repo.get_existing_measurement_ids()
        existing_samples = self.samp_repo.get_existing_samples()
        for meas_id in measurement_ids:
            df_meas = self.dp_repo.get_datapoints_by_meas_id(meas_id)
            full_stroke_df = self.combine_half_strokes_to_full_cycles(df_meas)
            full_strokes = full_stroke_df['full_stroke'].unique()
            for stroke in full_strokes:
                bow_stroke_start = min(full_stroke_df[full_stroke_df['full_stroke']==stroke]['bow_stroke'])
                bow_stroke_end = max(full_stroke_df[full_stroke_df['full_stroke']==stroke]['bow_stroke'])
                if (meas_id, bow_stroke_start) not in existing_samples:
                    sample = Sample(
                        id=full_strokes,
                        measurement_id=meas_id,
                        bow_stroke_start=bow_stroke_start,
                        bow_stroke_end = bow_stroke_end
                        )
                    sample_id = self.samp_repo.insert_sample_by_measurement_id(sample)
                    self.dp_repo.update_datapoints_sample(meas_id, bow_stroke_start, bow_stroke_end, sample_id)
    
    
    @staticmethod
    def combine_half_strokes_to_full_cycles_old(df: pd.DataFrame) -> pd.DataFrame:
        """
        Efficiently combines up and down half-strokes into full strokes (202 points) per participant.
        
        Args:
            df (pd.DataFrame): Input DataFrame with columns (among others):
                - participant_id
                - bow_stroke
                - up_down
                - dp_time_point
                - value
        Returns:
            pd.DataFrame: DataFrame with combined full strokes:
                - measurement_id
                - bow_stroke
                - full_stroke (index of the full stroke per participant)
                - up_down
                - dp_time_point (0–201)
                - value   
                - mean_value   
                - value_centered   
        """
        result_rows = []

        grouped = df.groupby(['participant_id', 'bow_stroke', 'up_down'])

        stroke_dict = {key: group for key, group in grouped}

        for participant_id in df['participant_id'].unique():
            full_stroke_index = 0

            participant_strokes = sorted(df[df['participant_id'] == participant_id]['bow_stroke'].unique())

            for bs in participant_strokes:
                up_half = stroke_dict.get((participant_id, bs, 0))
                down_half = stroke_dict.get((participant_id, bs + 1, 1))

                if up_half is not None and down_half is not None and len(up_half) == 101 and len(down_half) == 101:
                    combined = pd.concat([up_half, down_half], ignore_index=True)
                    combined = combined.sort_values(["up_down", "dp_time_point"]).reset_index(drop=True)
                    combined["dp_time_point"] = range(202)
                    combined["full_stroke"] = full_stroke_index
                    result_rows.append(combined[[
                        "participant_id", 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left',
                        "PRMD_ever", "target", "axis", 'bow_stroke', "full_stroke",
                        'up_down', "dp_time_point", 'value', 'mean_value', "value_centered"
                    ]])
                    full_stroke_index += 1

        return pd.concat(result_rows, ignore_index=True)      
    
    @staticmethod
    def combine_half_strokes_to_full_cycles(df: pd.DataFrame) -> pd.DataFrame:
        """
        Efficiently combines up and down half-strokes into full strokes (202 points) per participant.
        
        Args:
            df (pd.DataFrame): Input DataFrame with columns (among others):
                - participant_id
                - bow_stroke
                - up_down
                - dp_time_point
                - value
        Returns:
            pd.DataFrame: DataFrame with combined full strokes:
                - measurement_id
                - bow_stroke
                - full_stroke (index of the full stroke per participant)
                - up_down
                - dp_time_point (0–201)
                - value   
                - mean_value   
                - value_centered   
        """
        result_rows = []

        grouped = df.groupby(['bow_stroke', 'up_down'])

        stroke_dict = {key: group for key, group in grouped}

        full_stroke_index = 0

        bow_strokes = sorted(df['bow_stroke'].unique())

        for bs in bow_strokes:
            up_half = stroke_dict.get((bs, 0))
            down_half = stroke_dict.get((bs + 1, 1))

            if up_half is not None and down_half is not None and len(up_half) == 101 and len(down_half) == 101:
                combined = pd.concat([up_half, down_half], ignore_index=True)
                combined = combined.sort_values(["up_down", "time_point"]).reset_index(drop=True)
                combined["updated_time_point"] = range(202)
                combined["full_stroke"] = full_stroke_index
                result_rows.append(combined)
                full_stroke_index += 1

        return pd.concat(result_rows, ignore_index=True)
    
    @staticmethod
    def select_pain_data(df:pd.DataFrame, pain_conditions:list[str], control_condition:str) -> pd.DataFrame:
        """
        Filter the DataFrame to include rows where at least one of the specified pain conditions is present,
        or where the control condition is fulfilled (i.e., equals 0).

        Args:
            df (pd.DataFrame): The input DataFrame containing pain and control condition columns.
            pain_conditions (list[str]): A list of column names indicating binary pain conditions (1 = condition present).
            control_condition (str): The name of the column representing the control condition (0 = valid control).

        Returns:
            pd.DataFrame: A filtered DataFrame containing only rows matching at least one pain condition
            or the control condition.
        """
        
        pain_mask = pd.Series(False, index=df.index)
        for col in pain_conditions:
            pain_mask |= (df[col] == 1)
        control_mask = df[control_condition] == 0
        final_mask = pain_mask | control_mask

        return df[final_mask]
    @staticmethod
    def subtract_meanwave_key(df: pd.DataFrame):
        """
        Compute and subtract the mean waveform per (bow_stroke, key, dp_time_point) combination,
        and return both the centered data and the aggregated mean waveform.
#
        Args:
            df (pd.DataFrame): The input DataFrame containing at least the columns:
                'participant_id', 'PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left', 'PRMD_ever',
                'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value'.
#
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]:
                - df_result: Original data enriched with 'mean_value' and 'value_centered'.
                - df_mean_key_waveform: Aggregated mean key waveforms with 'key', 'bow_stroke', 'up_down',
                'time_point', and 'mean_value'.
        """
        df_mean = (
            df.groupby(['bow_stroke', 'key', 'dp_time_point'], as_index=False)['value']
            .mean()
            .rename(columns={'value': 'mean_value'})
        )
#
        df_merged = df.merge(df_mean, on=['bow_stroke', 'key', 'dp_time_point'])
        df_merged['value_centered'] = df_merged['value'] - df_merged['mean_value']
                
        up_down_map = (
            df.groupby(['bow_stroke', 'key'])['up_down']
            .first()
            .reset_index()
        )
        
        df_mean_key_waveform = df_mean.merge(up_down_map, on=['bow_stroke', 'key'])
        df_mean_key_waveform = df_mean_key_waveform.rename(columns={'dp_time_point': 'time_point'})
        df_mean_key_waveform = df_mean_key_waveform[['key', 'bow_stroke', 'up_down', 'time_point', 'mean_value']]
        
        return df_merged, df_mean_key_waveform
              
    #@staticmethod
    #def subtract_meanwave_key(df: pd.DataFrame):
    #    """
    #    Compute and subtract the mean waveform per (bow_stroke, key, dp_time_point) combination,
    #    and return both the centered data and the aggregated mean waveform.
#
    #    Args:
    #        df (pd.DataFrame): The input DataFrame containing at least the columns:
    #            'participant_id', 'PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left', 'PRMD_ever',
    #            'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value'.
#
    #    Returns:
    #        Tuple[pd.DataFrame, pd.DataFrame]:
    #            - df_result: Original data enriched with 'mean_value' and 'value_centered'.
    #            - df_mean_key_waveform: Aggregated mean key waveforms with 'key', 'bow_stroke', 'up_down',
    #            'time_point', and 'mean_value'.
    #    """
    #    df_mean = (
    #        df.groupby(['bow_stroke', 'key', 'dp_time_point'], as_index=False)['value']
    #        .mean()
    #        .rename(columns={'value': 'mean_value'})
    #    )
#
    #    df_merged = df.merge(df_mean, on=['bow_stroke', 'key', 'dp_time_point'])
    #    df_merged['value_centered'] = df_merged['value'] - df_merged['mean_value']
    #    
    #    df_result = df_merged[[
    #        'participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever',
    #        'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
    #        'value', 'mean_value', 'value_centered'
    #    ]]
    #    
    #    up_down_map = (
    #        df.groupby(['bow_stroke', 'key'])['up_down']
    #        .first()
    #        .reset_index()
    #    )
    #    
    #    df_mean_key_waveform = df_mean.merge(up_down_map, on=['bow_stroke', 'key'])
    #    df_mean_key_waveform = df_mean_key_waveform.rename(columns={'dp_time_point': 'time_point'})
    #    df_mean_key_waveform = df_mean_key_waveform[['key', 'bow_stroke', 'up_down', 'time_point', 'mean_value']]
    #    
    #    return df_result, df_mean_key_waveform
        
    
    @staticmethod
    def pivot_full_cycles_to_wide(df: pd.DataFrame, value) -> pd.DataFrame:
        """
        Transforms a DataFrame with full gait cycles (202 dp_time_point entries per cycle)
        into wide format where each dp_time_point becomes a separate column.

        Args:
            df (pd.DataFrame): DataFrame with columns:
                - participant_id
                - full_stroke
                - dp_time_point
                - value

        Returns:
            pd.DataFrame: Wide-format DataFrame with one row per full stroke and 202 value columns.
        """
        wide_df = df.pivot_table(
            index=["participant_id", "measurement_id","measurement_type_id", "PRMD_ever", 'target', 'axis', "sample_id"],
            columns="dp_time_point",
            values= value
        ).reset_index()

        wide_df.columns = ['participant_id', "measurement_id","measurement_type_id", 'PRMD_ever', 'target', 'axis', 'sample_id'] + [f"t{int(col)}" for col in wide_df.columns[-202:]]

        return wide_df

    @staticmethod
    def pivot_full_cycles_to_wide_old(df: pd.DataFrame, value) -> pd.DataFrame:
        """
        Transforms a DataFrame with full gait cycles (202 dp_time_point entries per cycle)
        into wide format where each dp_time_point becomes a separate column.

        Args:
            df (pd.DataFrame): DataFrame with columns:
                - participant_id
                - full_stroke
                - dp_time_point
                - value

        Returns:
            pd.DataFrame: Wide-format DataFrame with one row per full stroke and 202 value columns.
        """
        wide_df = df.pivot_table(
            index=["participant_id", "PRMD_ever", 'target', 'axis', "full_stroke"],
            columns="dp_time_point",
            values= value
        ).reset_index()

        wide_df.columns = ['participant_id', 'PRMD_ever', 'target', 'axis', 'full_stroke'] + [f"t{int(col)}" for col in wide_df.columns[-202:]]

        return wide_df
    
    


      

        