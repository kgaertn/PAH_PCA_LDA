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
        
    def create_plots_mean_std(self, device, exp_id, measurement_tp, pain_groups, key_diff_controlled = False):
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        total_pca_info = {}
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df_pain = self.data_loader.load_MPA_clean_data_by_device_tp_target_axis(exp_id, device, measurement_tp, target, axis, participant_ids)
            pain_group_names = self.concat_pain_groups(pain_groups)
            filename = f"{pain_group_names}_{measurement_tp}_Original_Mean_Std_{target}_{axis}"
            if key_diff_controlled:
                df_pain = self.data_processor.subtract_meanwave_key_difference(df_pain)
                #df_pain['value'] = df_pain['value_centered'] 
                filename = "Key_controled_" + filename
                if df_pain['key_difference'].abs().mean() > 5:
                    print("")
            value_cols = ['value_centered'] if key_diff_controlled else ['value']
            title = f"{pain_group_names}, {target}, {axis}: Mean and Std Dev over Time"
            fig = self.data_plotter.plot_mean_std_by_group(df_pain, time_col='dp_time_point', value_cols=value_cols, group_col='PRMD_ever', title=title)
            current_path = Path.cwd()
            #output_path = current_path / "output" / "plots"
            output_path = current_path / "output" / "plots" / "Mean_Std"
            self.data_plotter.save_plot(fig, output_path, filename)
            #for key in df_pain['key'].unique():
            #    df_one_key = df_pain[df_pain['key'] == key]
            #    title = f"{target}, {axis}: key {key} Mean and Std Dev over Time"
            #    output_path = current_path / "output" / "plots" / "Mean_Std"/ "per_key"
            #    fig = self.data_plotter.plot_mean_std_by_group(df_one_key, time_col='dp_time_point', value_cols=['value'], group_col='PRMD_ever', title=title)
            #    self.data_plotter.save_plot(fig, output_path, f"{pain_group_names}_{measurement_tp}_key_{key}_Original_Mean_Std_{target}_{axis}")
                
    def create_plots_mean_std_keys(self, device, exp_id, measurement_tp, pain_groups):
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df_pain = self.data_loader.load_MPA_clean_data_by_device_tp_target_axis(
                exp_id, device, measurement_tp, target, axis, participant_ids
            )

            # Sicherstellen, dass 'bow_stroke' existiert
            if 'bow_stroke' not in df_pain.columns:
                raise ValueError("'bow_stroke' column not found in the data.")

            fig, axes = plt.subplots(4, 3, figsize=(18, 12), sharex=True, sharey=True)
            axes = axes.flatten()

            for i in range(12):
                bs1 = i * 2
                bs2 = bs1 + 1
                ax = axes[i]
                
                df_subset = df_pain[df_pain['bow_stroke'].isin([bs1, bs2])]

                for group in range(2):
                    df_group = df_subset[df_subset['PRMD_ever'] == group]
                    key_1, key_2 = df_subset['key'].unique()[0], df_subset['key'].unique()[1]
                    if df_group.empty:
                        continue

                    mean_series = df_group.groupby('dp_time_point')['value'].mean()
                    std_series = df_group.groupby('dp_time_point')['value'].std()
                    label = f"{'Pain' if group else 'No Pain'}"
                    color = "#BC4B51" if label == 'Pain' else "#8CB369"
                    ax.plot(mean_series.index, mean_series.values, label=label, color = color)
                    ax.fill_between(
                        mean_series.index,
                        mean_series - std_series,
                        mean_series + std_series, 
                        color = color,
                        alpha=0.2
                    )
                ax.legend(loc='upper right',fontsize='small')
                ax.set_title(f"Key {key_1} & {key_2}")
                #ax.legend()
            pain_group_names = self.concat_pain_groups(pain_groups)
            fig.suptitle(f"{pain_group_names}, {target}, {axis}: Mean ± StdDev by Key pair and PRMD", fontsize=16)
            fig.tight_layout(rect=[0, 0.03, 1, 0.95])

            output_path = Path.cwd() / "output" / "plots" / "Mean_Std" / "bow_stroke_pairs"
            output_path.mkdir(parents=True, exist_ok=True)
            self.data_plotter.save_plot(fig, output_path, f"{pain_group_names}_{measurement_tp}_Key_Pairs_Subplots_{target}_{axis}")
    
    @staticmethod        
    def concat_pain_groups(pain_groups):
        if len(pain_groups) == 0:
            result = ''
        elif len(pain_groups) == 1:
            result = pain_groups[0]
        else:
            result = ' and '.join(pain_groups)
        return result
    

    def create_plots_key_per_group(self, device, exp_id, measurement_tp, pain_groups):
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        pain_group_names = self.concat_pain_groups(pain_groups)
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df_pain = self.data_loader.load_MPA_clean_data_by_device_tp_target_axis(
                exp_id, device, measurement_tp, target, axis, participant_ids
            )


            fig, axes = plt.subplots(1, 2, figsize=(24, 6))
            all_y_values = []
            for i in range(12):
                bs1 = i * 2
                bs2 = bs1 + 1
                df_pair = df_pain[df_pain['bow_stroke'].isin([bs1, bs2])]

                for group in range(2):
                    ax = axes[group]
                    key_1, key_2 = df_pair['key'].unique()[0], df_pair['key'].unique()[1]
                    df_group = df_pair[df_pair['PRMD_ever'] == group]

                    mean_series = df_group.groupby('dp_time_point')['value'].mean()
                    #std_series = df_group.groupby('dp_time_point')['value'].std()
                    all_y_values.extend(mean_series.values.tolist())
                    
                    label = f"{key_1}/{key_2}"
                    ax.plot(mean_series.index, mean_series.values, label=label)
                    #ax.fill_between(
                    #    mean_series.index,
                    #    mean_series - std_series,
                    #    mean_series + std_series,
                    #    alpha=0.2
                    #)
                
                    ax.set_title(f"{'Pain' if group else 'No Pain'}")
                    ax.legend(loc='upper right', fontsize='small')
                    ax.set_xlabel("Time point")
                    ax.set_ylabel("Value")
            
            y_min = min(all_y_values)
            y_max = max(all_y_values)
            offset = abs((y_max-y_min))/5
            for ax in axes:
                ax.set_ylim(y_min-offset, y_max+offset)
            
            fig.suptitle(f"{pain_group_names}, {target}, {axis}: Mean over Time by key pairs per group", fontsize=16)
            #ax.set_title(f"{target}, {axis}: Mean over Time by key pairs per group", fontsize=14)

            #ax.legend(loc='upper right')
            fig.tight_layout()

            # Save plot
            output_path = Path.cwd() / "output" / "plots" / "Mean_Std" / "keys_per_group"
            output_path.mkdir(parents=True, exist_ok=True)
            self.data_plotter.save_plot(fig, output_path, f"{measurement_tp}_Combined_Mean_{target}_{axis}")
