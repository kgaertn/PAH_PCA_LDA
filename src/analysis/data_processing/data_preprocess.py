from data_access.measurement_repository import MeasurementRepository
from data_access.sample_repository import SampleRepository
from data_access.datapoint_repository import DatapointRepository

from models.sample import Sample

import pandas as pd
import numpy as np

class DataProcessor:
    def __init__(self):
        """
        Initializes the DataProcessor with data repositories.
        """
        self.meas_repo = MeasurementRepository()
        self.samp_repo = SampleRepository()
        self.dp_repo = DatapointRepository()   
    
    
    def create_samples(self) -> None:
        # TODO: split this up, so that the loading and uploading happens in data_loading and only the samples are created here
        measurement_ids = self.meas_repo.get_existing_measurement_ids()
        existing_samples = self.samp_repo.get_existing_samples()
        meas_ids_with_samples = {mid for (mid, _) in existing_samples}

        for meas_id in measurement_ids:
            if meas_id in meas_ids_with_samples:
                continue 
            df_meas = self.dp_repo.get(table_or_view='datapoint', measurement_id = meas_id)
            full_stroke_df = self.combine_half_strokes_to_full_cycles(df_meas)

            
            for stroke_id, stroke_df in full_stroke_df.groupby("full_stroke"):
                bow_stroke_start = int(stroke_df["bow_stroke"].min())
                bow_stroke_end = int(stroke_df["bow_stroke"].max())

                sample = Sample(
                    id = stroke_id,
                    measurement_id=meas_id,
                    bow_stroke_start=bow_stroke_start,
                    bow_stroke_end=bow_stroke_end,
                )

                sample_id = self.samp_repo.insert_sample_by_measurement_id(sample)
                self.dp_repo.update_datapoints_sample(
                    meas_id=meas_id,
                    bow_stroke_start=bow_stroke_start,
                    bow_stroke_end=bow_stroke_end,
                    sample_id=sample_id
                )
           
    
    #@staticmethod
    #def combine_half_strokes_to_full_cycles_old(df: pd.DataFrame) -> pd.DataFrame:
    #    """
    #    Combines up and down half-strokes into full strokes (202 points) per participant.
    #    
    #    Args:
    #        df (pd.DataFrame): Input DataFrame with columns (among others):
    #            - participant_id
    #            - bow_stroke
    #            - up_down
    #            - dp_time_point
    #            - value
    #    Returns:
    #        pd.DataFrame: DataFrame with combined full strokes:
    #            - measurement_id
    #            - bow_stroke
    #            - full_stroke (index of the full stroke per participant)
    #            - up_down
    #            - dp_time_point (0–201)
    #            - value   
    #            - mean_value   
    #            - value_centered   
    #    """
    #    result_rows = []
#
    #    grouped = df.groupby(['participant_id', 'bow_stroke', 'up_down'])
#
    #    stroke_dict = {key: group for key, group in grouped}
#
    #    for participant_id in df['participant_id'].unique():
    #        full_stroke_index = 0
#
    #        participant_strokes = sorted(df[df['participant_id'] == participant_id]['bow_stroke'].unique())
#
    #        for bs in participant_strokes:
    #            up_half = stroke_dict.get((participant_id, bs, 0))
    #            down_half = stroke_dict.get((participant_id, bs + 1, 1))
#
    #            if up_half is not None and down_half is not None and len(up_half) == 101 and len(down_half) == 101:
    #                combined = pd.concat([up_half, down_half], ignore_index=True)
    #                combined = combined.sort_values(["up_down", "dp_time_point"]).reset_index(drop=True)
    #                combined["dp_time_point"] = range(202)
    #                combined["full_stroke"] = full_stroke_index
    #                result_rows.append(combined[[
    #                    "participant_id", 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left',
    #                    "PRMD_ever", "target", "axis", 'bow_stroke', "full_stroke",
    #                    'up_down', "dp_time_point", 'value', 'mean_value', "value_centered"
    #                ]])
    #                full_stroke_index += 1
#
    #    return pd.concat(result_rows, ignore_index=True)      
    
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

    @staticmethod
    def subtract_meanwave_key_difference(df: pd.DataFrame):
        """
        Centers 'value' by subtracting the difference between the global mean 
        (per dp_time_point) and the key-specific mean (per bow_stroke, key, dp_time_point),
        computed separately for each PRMD group.

        Returns a DataFrame with added columns:
        - 'key_mean_value', 'mean_value', 'key_difference', 'value_centered'

        Args:
            df (pd.DataFrame): Input with required columns including 'PRMD_ever', 
                               'bow_stroke', 'key', 'dp_time_point', and 'value'.

        Returns:
            pd.DataFrame: Data with centered values and intermediate computations.
        """
        df_res = pd.DataFrame()
        for group in df['PRMD_ever'].unique():
            df_group = df[df['PRMD_ever'] == group]
        
            df_mean_key = (
                df_group.groupby(['bow_stroke', 'key', 'dp_time_point'], as_index=False)['value']
                .mean()
                .rename(columns={'value': 'key_mean_value'})
            )
            
            df_mean_group = (
                df_group.groupby(['dp_time_point'], as_index=False)['value']
                .mean()
                .rename(columns={'value': 'mean_value'})
            )
            
            df_mean_merged = df_mean_key.merge(df_mean_group, on=['dp_time_point'])
            df_mean_merged['key_difference'] = df_mean_merged['key_mean_value'] - df_mean_merged['mean_value']
            
            df_group_merged = df_group.merge(df_mean_merged, on=['bow_stroke', 'key', 'dp_time_point'])
            df_group_merged['value_centered'] = df_group_merged['value'] - df_group_merged['key_difference']
            df_res = pd.concat([df_res, df_group_merged], axis=0, ignore_index=True)

        return df_res                  
    
    @staticmethod
    def pivot_full_cycles_to_wide(df: pd.DataFrame, value, pivot_column, index_cols = ["participant_id", "measurement_id","measurement_type_id", "PRMD_ever", 'target', 'axis', "sample_id"]) -> pd.DataFrame:
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
            index=index_cols,
            columns=pivot_column,
            values= value
        ).reset_index()

        timepoint_cols = [f"t{int(col)}" for col in wide_df.columns if isinstance(col, (int, float))]
        wide_df.columns = index_cols + timepoint_cols
        #wide_df.columns = ['participant_id', "measurement_id","measurement_type_id", 'PRMD_ever', 'target', 'axis', 'sample_id'] + [f"t{int(col)}" for col in wide_df.columns[-202:]]

        return wide_df

    @staticmethod
    def sliding_window_outlier_detection(df, window=10, threshold=3):
        df_outliers_adj = df.copy()
        count_outliers = 0

        time_cols = df.columns[-202:]
        static_cols = df.columns.difference(time_cols)

        for participant, df_part in df.groupby("participant_id"):
            data = df_part[time_cols].to_numpy()
            n_rows, n_cols = data.shape

            for row_idx in range(n_rows):
                series = data[row_idx]

                # Sliding window: generate shape (n_cols - window + 1, window)
                # Handle each timepoint individually
                for i in range(1, n_cols - 1):  # skip t0 and tN for now
                    start = max(0, i - window // 2)
                    end = min(n_cols, i + window // 2 + 1)
                    window_vals = np.delete(series[start:end], i - start)
                    mean = window_vals.mean()
                    std = window_vals.std()
                    if std > 0 and abs(series[i] - mean) > threshold * std:
                        df_outliers_adj.loc[df_part.index[row_idx], time_cols[i]] = series.mean()
                        count_outliers += 1

                # Edge points: t0
                t0_vals = np.delete(data[:, 0], row_idx)
                t0_mean = t0_vals.mean()
                t0_std = t0_vals.std()
                if t0_std > 0 and abs(series[0] - t0_mean) > threshold * t0_std:
                    df_outliers_adj.loc[df_part.index[row_idx], time_cols[0]] = t0_mean
                    count_outliers += 1

                # Edge points: tN
                tN_vals = np.delete(data[:, -1], row_idx)
                tN_mean = tN_vals.mean()
                tN_std = tN_vals.std()
                if tN_std > 0 and abs(series[-1] - tN_mean) > threshold * tN_std:
                    df_outliers_adj.loc[df_part.index[row_idx], time_cols[-1]] = tN_mean
                    count_outliers += 1

        return df_outliers_adj, count_outliers    

    #@staticmethod
    #def pivot_full_cycles_to_wide_old(df: pd.DataFrame, value) -> pd.DataFrame:
    #    """
    #    Transforms a DataFrame with full gait cycles (202 dp_time_point entries per cycle)
    #    into wide format where each dp_time_point becomes a separate column.
#
    #    Args:
    #        df (pd.DataFrame): DataFrame with columns:
    #            - participant_id
    #            - full_stroke
    #            - dp_time_point
    #            - value
#
    #    Returns:
    #        pd.DataFrame: Wide-format DataFrame with one row per full stroke and 202 value columns.
    #    """
    #    wide_df = df.pivot_table(
    #        index=["participant_id", "PRMD_ever", 'target', 'axis', "full_stroke"],
    #        columns="dp_time_point",
    #        values= value
    #    ).reset_index()
#
    #    wide_df.columns = ['participant_id', 'PRMD_ever', 'target', 'axis', 'full_stroke'] + [f"t{int(col)}" for col in wide_df.columns[-202:]]
#
    #    return wide_df
    
    


      

        