from data_access.repositories.measurement_repository import MeasurementRepository
from data_access.repositories.sample_repository import SampleRepository
from data_access.repositories.datapoint_repository import DatapointRepository
from data_access.models.sample import Sample

import pandas as pd
import numpy as np

class DataProcessor:
    def __init__(self):
        """
        Initializes the DataProcessor with data repositories for measurements, samples, and datapoints.
        """
        self.meas_repo = MeasurementRepository()
        self.samp_repo = SampleRepository()
        self.dp_repo = DatapointRepository()   
    
    def create_samples(self):
        """
        Create Sample objects by combining half-strokes into full cycles and
        updating datapoints with corresponding sample IDs.
        """
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

                sample_id = self.samp_repo.insert_sample(sample)
                self.dp_repo.update_datapoints_sample(
                    meas_id=meas_id,
                    bow_stroke_start=bow_stroke_start,
                    bow_stroke_end=bow_stroke_end,
                    sample_id=sample_id
                )
    
    @staticmethod
    def combine_half_strokes_to_full_cycles(df: pd.DataFrame) -> pd.DataFrame:
        """
        Combine up and down half-strokes (101 points each) into full strokes (202 points).

        Args:
            df (pd.DataFrame): Data containing 'bow_stroke', 'up_down', 'time_point', and 'value'.

        Returns:
            pd.DataFrame: Combined strokes with new 'full_stroke' and 'updated_time_point'.
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
    def subtract_meanwave_key(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Subtract the mean waveform per (bow_stroke, key, dp_time_point) and return
        centered data along with the aggregated mean waveform.

        Args:
            df (pd.DataFrame): Input data.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Centered data and mean waveform data.
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
    def subtract_meanwave_key_difference(df: pd.DataFrame) -> pd.DataFrame:
        """
        Center 'value' by subtracting the difference between global and key-specific means,
        computed per PRMD group.

        Args:
            df (pd.DataFrame): Input data.

        Returns:
            pd.DataFrame: Data with centered values and intermediate metrics.
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
    def pivot_full_cycles_to_wide(df: pd.DataFrame, value:str, pivot_column:str, 
                                  index_cols = ["participant_id", "measurement_id","measurement_type_id", "PRMD_ever", 
                                                'target', 'axis', "sample_id"]) -> pd.DataFrame:
        """
        Pivot full stroke data into wide format with each time point as a separate column.

        Args:
            df (pd.DataFrame): Data with full strokes.
            value (str): Column name for values to pivot.
            pivot_column (str): Column representing time points.
            index_cols (list[str]): Columns to use as index in wide format.

        Returns:
            pd.DataFrame: Wide-format DataFrame.
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
    def sliding_window_outlier_detection(df:pd.DataFrame, window:int =10, threshold:int=3) -> tuple[pd.DataFrame, int]:
        """
        Detect and replace outliers in time series using a sliding window approach.

        Args:
            df (pd.DataFrame): Input wide-format data with time point columns.
            window (int): Window size for mean/std calculation.
            threshold (int): Threshold multiplier for std deviation.

        Returns:
            tuple[pd.DataFrame, int]: Adjusted DataFrame and count of outliers replaced.
        """
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
    
    


      

        