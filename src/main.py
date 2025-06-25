from data_processing.data_preprocess import DataProcessor
from db.setup import *

def main():
    data_processor = DataProcessor()
    sample_setup = True
    if not sample_setup:
        create_tables()
        add_columns_if_missing('datapoint', new_columns = {'sample_id': 'INTEGER'})
        create_adjusted_view()
        create_datapoints_MPA_view()
        create_datapoints_MPA_device_view('MoCap', 'mocap')
        create_datapoints_MPA_device_view('EMG', 'emg')
        
        data_processor.create_samples()

             
if __name__ == '__main__':
    main()
