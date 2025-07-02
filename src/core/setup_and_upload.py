from db.setup import *
from data_processing.data_preprocess import DataProcessor
from data_processing.pca_analysis import PCAAnalyser

from pathlib import Path

class SetupUploader:
    def __init__(self):
        """
        Initializes the SetupUploader with processors.
        """
        self.data_processor = DataProcessor()
        self.pca_analyser = PCAAnalyser() 
    
    def run_db_setup(self):
        adjusted_db_setup()
        #self.run_create_samples()


    def run_create_samples(self):
        self.data_processor.create_samples()
    
    def load_data_for_pca(self, device, measurement_tp, target, axis):
        df = self.data_processor.load_MPA_clean_data_by_device_tp_target_axis(device, measurement_tp, target, axis)
        print(f"{target} {axis} loaded")
    # select the correct participants, based on their pain location
        pain_columns = ['PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left']
        control_column = 'PRMD_ever'
        df_pain = self.data_processor.select_pain_data(df, pain_columns, control_column)
        df_reduced = df_pain[[
            'participant_id', 'ext_participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever',
            'measurement_id', 'measurement_type_id', 'target', 'axis', 'sample_id', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
            'value'
        ]]
        df_sorted = df_reduced.sort_values(by=['participant_id', 'bow_stroke', 'up_down', 'dp_time_point'])  
        df_sorted_transformed = self.data_processor.pivot_full_cycles_to_wide(df_sorted, 'value')
          
        return df_sorted, df_sorted_transformed

    def process_data_for_pca(self, df_sorted):

        # subtract the key mean-waveform from each sample
        # TODO: save mean key per target/axis? / plot mean key? 
        df_key_normalized, df_mean_key_waveform_target_axis = self.data_processor.subtract_meanwave_key(df_sorted)

        # transform the data (columns for each timepoint)
        df_transformed = self.data_processor.pivot_full_cycles_to_wide(df_key_normalized, 'value_centered' )
        return df_transformed
    
    def check_and_remove_outliers(self, df):
        #TODO: save which dp are outliers
        df_outliers_removed, count_outliers = self.pca_analyser.sliding_window_outlier_detection(df)
        return df_outliers_removed, count_outliers

