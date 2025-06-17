from data_access.experiment_repository import ExperimentRepository
from data_access.participant_repository import ParticipantRepository
from data_access.measurement_repository import MeasurementRepository
from data_access.datapoint_repository import DatapointRepository


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class DataProcessor:
    def __init__(self):
        """
        Initializes the DataProcessor with data repositories.
        """
        self.exp_repo = ExperimentRepository()
        self.part_repo = ParticipantRepository()
        self.meas_repo = MeasurementRepository()
        self.dp_repo = DatapointRepository()   
    
    @staticmethod    
    def combine_half_strokes_to_full_cycles(df: pd.DataFrame) -> pd.DataFrame:
        """
        Combines pairs of up and down half-strokes into full strokes (gait cycles)
        for each participant. Each resulting full stroke will have 202 data points.

        Args:
            df (pd.DataFrame): Input DataFrame with columns:
                - participant_id
                - bow_stroke
                - up_down
                - dp_time_point
                - value

        Returns:
            pd.DataFrame: DataFrame with combined full strokes:
                - participant_id
                - PRMD_ever
                - full_stroke (index of the full stroke per participant)
                - dp_time_point (0–201)
                - value
        """
        result_rows = []

        # Iterate over each participant
        for participant_id, group in df.groupby("participant_id"):
            # Sort by bow_stroke and up_down to ensure consistent order
            group = group.sort_values(["bow_stroke", "up_down"])

            # Find all valid bow_stroke indices with both up and down halves
            #valid_strokes = sorted(set(group['bow_stroke']) & set(group['bow_stroke'] - 1))

            full_stroke_index = 0

            for bs in range(0, 25, 2):  # step in twos to pair 0/1, 2/3, ...
                up_half = group[(group['bow_stroke'] == bs) & (group['up_down'] == 0)]
                down_half = group[(group['bow_stroke'] == bs + 1) & (group['up_down'] == 1)]

                if len(up_half) == 101 and len(down_half) == 101:
                    combined = pd.concat([up_half, down_half], ignore_index=True)
                    combined = combined.sort_values(["up_down", "dp_time_point"]).reset_index(drop=True)
                    combined["dp_time_point"] = range(202)
                    combined["full_stroke"] = full_stroke_index
                    result_rows.append(combined[["participant_id",'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left', "PRMD_ever", "target", "axis",
                                                 'bow_stroke', "full_stroke", 'up_down', "dp_time_point",'value', 'mean_value', "value_centered"]])
                    full_stroke_index += 1

        # Combine all full strokes into a single DataFrame
        return pd.concat(result_rows, ignore_index=True)
    
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
        # Pivot the table: each time point becomes a column
        wide_df = df.pivot_table(
            index=["participant_id", "PRMD_ever", "full_stroke", 'target', 'axis'],
            columns="dp_time_point",
            values= value
        ).reset_index()

        # Optionally: Rename columns to indicate time points (e.g., t0, t1, ..., t201)
        wide_df.columns = ['participant_id', 'PRMD_ever', 'full_stroke', 'target', 'axis'] + [f"t{int(col)}" for col in wide_df.columns[5:]]

        return wide_df
    
    def load_data_one_joint(self, joint:str) -> pd.DataFrame:
        df = self.dp_repo.get_datapoints_by_exp_id_device_timepoint_target(1, 'mocap', 'pre', joint)
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    def load_full_device_data(self, device:str) -> pd.DataFrame:
        df = self.dp_repo.get_datapoints_by_exp_id_device_and_timepoint(1, device, 'pre')
        df_reduced = df[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever', 'target', 'axis', 'bow_stroke', 'up_down', 'key', 'dp_time_point', 'value']]
        return df_reduced
    
    @staticmethod
    def select_pain_data(df:pd.DataFrame, pain_conditions:list[str], control_condition:str) -> pd.DataFrame:
        # Starte mit einer False-Maske
        pain_mask = pd.Series(False, index=df.index)

        # Kombiniere alle Bedingungen aus pain_conditions mit OR
        for col in pain_conditions:
            pain_mask |= (df[col] == 1)

        # Bedingung für control_condition
        control_mask = df[control_condition] == 0

        # Gesamte Bedingung: entweder Schmerzbedingung oder Kontrollbedingung
        final_mask = pain_mask | control_mask

        return df[final_mask]
        
    @staticmethod
    def subtract_meanwave_key(df:pd.DataFrame):
        df_mean = pd.DataFrame(columns=['bow_stroke', 'key', 'dp_time_point', 'mean_value'])
        unique_bow_stroke = df['bow_stroke'].unique()
        mean_key_waveform = []
        for bow_stroke in unique_bow_stroke:
            bs_df = df[df['bow_stroke'] == bow_stroke]
            for time_point in range(0,101):
                key_timepoint_df = bs_df[bs_df['dp_time_point'] == time_point]
                mean_timepoint_value = key_timepoint_df['value'].mean()
                df_mean.loc[len(df_mean)] = [bow_stroke, key_timepoint_df['key'].unique()[0], time_point, mean_timepoint_value]
                mean_key_waveform.append([key_timepoint_df['key'].unique()[0], bow_stroke, time_point, mean_timepoint_value])    
                
        df_merged = df.merge(df_mean, on=['bow_stroke', 'key', 'dp_time_point'])
        df_merged['value_centered'] = df_merged['value'] - df_merged['mean_value']
        df_result = df_merged[['participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever','target', 'axis', 'bow_stroke', 'up_down', 
                               'key', 'dp_time_point', 'value', 'mean_value', 'value_centered']]
        df_mean_key_waveform = pd.DataFrame(mean_key_waveform, columns = ['key', 'bow_stroke', 'time_point', 'mean_value'])
        return df_result, df_mean_key_waveform
    
        