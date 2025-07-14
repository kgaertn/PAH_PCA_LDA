from analysis.data_processing.data_preprocess import DataProcessor
from analysis.data_processing.data_loading import DataLoader
from analysis.data_analysis.data_plotting import DataPlotter
#from core.setup_and_upload import SetupUploader
from models.pc_ranked import PC_Ranked

from sklearn.preprocessing import StandardScaler
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

class GeneralAnalysisRunner:
    def __init__(self):
        """
        Initializes the SetupUploader with processors.
        """
        self.data_processor = DataProcessor()
        self.data_loader = DataLoader()
        self.data_plotter = DataPlotter()
        
    def create_plots_mean_std(self, device, exp_id, measurement_tp):
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(device, exp_id, measurement_tp)
        total_pca_info = {}
        for target, axis in existing_target_axes:
            df = self.data_loader.load_MPA_clean_data_by_device_tp_target_axis(device, measurement_tp, target, axis)
            pain_columns = ['PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left']
            control_column = 'PRMD_ever'
            df_pain = self.data_processor.select_pain_data(df, pain_columns, control_column)
            print("")
            title = f"{target}, {axis}: Mean and Std Dev over Time"
            fig = self.data_plotter.plot_mean_std_by_group(df_pain, time_col='dp_time_point', value_cols=['value'], group_col='PRMD_ever', title=title)
            current_path = Path.cwd()
            #output_path = current_path / "output" / "plots"
            output_path = current_path / "output" / "plots" / "Mean_Std"
            self.data_plotter.save_plot(fig, output_path, f"{measurement_tp}_Original_Mean_Std_{target}_{axis}")
            for key in df_pain['key'].unique():
                df_one_key = df_pain[df_pain['key'] == key]
                title = f"{target}, {axis}: key {key} Mean and Std Dev over Time"
                output_path = current_path / "output" / "plots" / "Mean_Std"/ "per_key"
                fig = self.data_plotter.plot_mean_std_by_group(df_one_key, time_col='dp_time_point', value_cols=['value'], group_col='PRMD_ever', title=title)
                self.data_plotter.save_plot(fig, output_path, f"{measurement_tp}_key_{key}_Original_Mean_Std_{target}_{axis}")