from processing.data_preprocess import DataProcessor
from processing.data_loading import DataLoader
from processing.data_plotting import DataPlotter

from pathlib import Path
import matplotlib.pyplot as plt

class GeneralAnalyser:
    def __init__(self):
        """
        Initializes the GeneralAnalysisRunner with data loader, processor and plotter instances.
        """
        self.data_processor = DataProcessor()
        self.data_loader = DataLoader()
        self.data_plotter = DataPlotter()
        
    def create_plots_mean_std(self, device:str, exp_id:int, measurement_tp:str, pain_groups:list[str], key_diff_controlled:bool = False):
        """
        Create and save plots of mean and standard deviation over time for specified groups.

        Args:
            device (str): Device identifier.
            exp_id (str): Experiment ID.
            measurement_tp (str): Measurement time point.
            pain_groups (List[str]): List of pain group names.
            key_diff_controlled (bool): If True, apply key difference control.
        """
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        pain_group_names = self.concat_pain_groups(pain_groups)
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df_pain = self.data_loader.clean_data_by_exp_device_tp_target_axis(exp_id, device, measurement_tp, target, axis, participant_ids)
            pain_group_names = self.concat_pain_groups(pain_groups)
            filename = f"{pain_group_names}_{measurement_tp}_Original_Mean_Std_{target}_{axis}"
            if key_diff_controlled:
                df_pain = self.data_processor.subtract_meanwave_key_difference(df_pain)
                filename = "Key_controled_" + filename
                if df_pain['key_difference'].abs().mean() > 5:
                    print("")
            value_cols = ['value_centered'] if key_diff_controlled else ['value']
            title = f"{pain_group_names}, {target}, {axis}: Mean and Std Dev over Time"
            fig = self.data_plotter.plot_mean_std_by_group(df_pain, time_col='dp_time_point', value_cols=value_cols, group_col='PRMD_ever', title=title)
            current_path = Path.cwd()
            output_path = current_path / "output" / "plots" / "Mean_Std" / "mean_std_per_group" / f"{pain_group_names}"
            output_path.mkdir(parents=True, exist_ok=True)
            self.data_plotter.save_plot(fig, output_path, filename)
                
    def create_plots_mean_std_keys(self, device:str, exp_id:int, measurement_tp:str, pain_groups:list[str]):
        """
        Create and save subplots of mean ± std dev by key pairs and PRMD groups.

        Args:
            device (str): Device identifier.
            exp_id (int): Experiment ID.
            measurement_tp (str): Measurement time point.
            pain_groups (List[str]): List of pain group names.
        """
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        #pain_group_names = self.concat_pain_groups(pain_groups)
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df_pain = self.data_loader.clean_data_by_exp_device_tp_target_axis(
                exp_id, device, measurement_tp, target, axis, participant_ids
            )
            
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
            pain_group_names = self.concat_pain_groups(pain_groups)
            fig.suptitle(f"{pain_group_names}, {target}, {axis}: Mean ± StdDev by Key pair and PRMD", fontsize=16)
            fig.tight_layout(rect=[0, 0.03, 1, 0.95])

            output_path = Path.cwd() / "output" / "plots" / "Mean_Std" / "bow_stroke_pairs" / f"{pain_group_names}"
            output_path.mkdir(parents=True, exist_ok=True)
            self.data_plotter.save_plot(fig, output_path, f"{pain_group_names}_{measurement_tp}_Key_Pairs_Subplots_{target}_{axis}")
    
    @staticmethod        
    def concat_pain_groups(pain_groups:list[str]) -> str:
        """
        Concatenate pain group names with 'and'.

        Args:
            pain_groups (List[str]): List of pain group names.

        Returns:
            str: Concatenated pain group string.
        """
        if len(pain_groups) == 0:
            result = ''
        elif len(pain_groups) == 1:
            result = pain_groups[0]
        else:
            result = ' and '.join(pain_groups)
        return result
    
    def create_plots_key_per_group(self, device:str, exp_id:int, measurement_tp:str, pain_groups:list[str]):
        """
        Create and save plots of mean over time by key pairs per pain group.

        Args:
            device (str): Device identifier.
            exp_id (int): Experiment ID.
            measurement_tp (str): Measurement time point.
            pain_groups (List[str]): List of pain group names.
        """
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        pain_group_names = self.concat_pain_groups(pain_groups)
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df_pain = self.data_loader.clean_data_by_exp_device_tp_target_axis(
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
                    all_y_values.extend(mean_series.values.tolist())
                    
                    label = f"{key_1}/{key_2}"
                    ax.plot(mean_series.index, mean_series.values, label=label)
                
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
            fig.tight_layout()

            output_path = Path.cwd() / "output" / "plots" / "Mean_Std" / "keys_per_group" / f"{pain_group_names}"
            output_path.mkdir(parents=True, exist_ok=True)
            self.data_plotter.save_plot(fig, output_path, f"{measurement_tp}_Combined_Mean_{target}_{axis}")
    
    def create_plots_key_per_participant(self, device:str, exp_id:int, measurement_tp:str, pain_groups:list[str]):
        """
        Create and save plots of mean over time by key pairs per participant with and without key difference control.

        Args:
            device (str): Device identifier.
            exp_id (int): Experiment ID.
            measurement_tp (str): Measurement time point.
            pain_groups (List[str]): List of pain group names.
        """
        existing_target_axes = self.data_loader.get_existing_target_axis_exp(exp_id, device, measurement_tp)
        pain_group_names = self.concat_pain_groups(pain_groups)
        for target, axis in existing_target_axes:
            participant_ids, pain_group_ids = self.data_loader.get_participants_pain_groups(pain_groups)
            df = self.data_loader.clean_data_by_exp_device_tp_target_axis(
                exp_id, device, measurement_tp, target, axis, participant_ids
            )
            df_key_norm = self.data_processor.subtract_meanwave_key_difference(df)
            for participant_id in participant_ids:
                part_df = df[df['participant_id'] == participant_id]
                if part_df.empty:
                    continue
                part_df_norm = df_key_norm[df_key_norm['participant_id'] == participant_id]
                ext_part_id = part_df['ext_participant_id'].unique()[0]
                pain_group = "Pain" if part_df['PRMD_ever'].unique() == 1 else "No Pain"
                fig, axes = plt.subplots(1, 2, figsize=(24, 6))
                all_y_values = []
                for i in range(12):
                    bs1 = i * 2
                    bs2 = bs1 + 1
                    df_pair = part_df[part_df['bow_stroke'].isin([bs1, bs2])]
                    df_pair_norm = part_df_norm[part_df_norm['bow_stroke'].isin([bs1, bs2])]

                    for group in range(2):
                        ax = axes[group]
                        key_1, key_2 = df_pair['key'].unique()[0], df_pair['key'].unique()[1]
                        
                        df_group = df_pair if group == 0 else df_pair_norm

                        mean_series = df_group.groupby('dp_time_point')['value'].mean() if group == 0 else df_group.groupby('dp_time_point')['value_centered'].mean()
                        all_y_values.extend(mean_series.values.tolist())

                        label = f"{key_1}/{key_2}"
                        ax.plot(mean_series.index, mean_series.values, label=label)


                        ax.set_title(f"{'Key_controlled' if group else 'Raw'}")
                        ax.legend(loc='upper right', fontsize='small')
                        ax.set_xlabel("Time point")
                        ax.set_ylabel("Value")

                y_min = min(all_y_values)
                y_max = max(all_y_values)
                offset = abs((y_max-y_min))/5
                for ax in axes:
                    ax.set_ylim(y_min-offset, y_max+offset)

                fig.suptitle(f"{ext_part_id} ({pain_group}), {target}, {axis}: Mean over Time by key pairs", fontsize=16)
                fig.tight_layout()
                
                target_str = target.replace(" ", "_")

                output_path = Path.cwd() / "output" / "plots" / "Mean_Std" / "keys_per_participant" / f"{pain_group_names}"/ f"{target_str}_{axis}" 
                output_path.mkdir(parents=True, exist_ok=True)
                self.data_plotter.save_plot(fig, output_path, f"Part_{ext_part_id}_{measurement_tp}_Key_Mean_{target}_{axis}")