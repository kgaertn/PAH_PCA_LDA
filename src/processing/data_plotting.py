from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pandas.plotting import scatter_matrix
from pandas.plotting import lag_plot
from matplotlib.lines import Line2D
import matplotlib.figure
import seaborn as sns
import pandas as pd
import numpy as np

class DataPlotter:
    
    def __init__(self):
        """
        Initialize DataPlotter with predefined color map.
        """    
        self.COLOR_MAP = {
        "pain": "#BC4B51",
        "no_pain": "#8CB369",
        "loading": "#5B8E7D",   
} 
        
    def save_distribution_plot(self, fig:matplotlib.figure.Figure, target:str, axis:str, pc_index:int, meas_time_point:str, pain_groups: list[str]):
        """
        Save histogram plot to output folder and close it.

        Args:
            fig (matplotlib.figure.Figure): Figure to save.
            target (str): Target label.
            axis (str): Axis label.
            pc_index (int): Principal component index.
            meas_time_point (str): Measurement time point.
        """
        pain_group_names = self.concat_pain_groups(pain_groups)
        
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots" / "Histograms" / f"{pain_group_names}"
        output_path.mkdir(parents=True, exist_ok=True)
        self.save_plot(fig, output_path, f"{meas_time_point}_PC_Scores_Histogram_{target}_{axis}_PC_{pc_index}")
        plt.close()
            
    def get_color_for_label(self, label: str) -> str:
        """
        Return color code based on label content.

        Args:
            label (str): Label string.

        Returns:
            str: Hex color code.
        """
        label_lower = label.lower()
        if "no pain" in label_lower or "no_pain" in label_lower:
            return self.COLOR_MAP["no_pain"]
        elif "pain" in label_lower:
            return self.COLOR_MAP["pain"]
        elif "load" in label_lower:
            return self.COLOR_MAP["loading"]
        return "#000000"

    def plot_PCA_reconstruction(self, component_data:dict, title_waveform:str="Mean Waveform", 
                                title_loading:str="Loading Vector", lv_ymax: float|None = None, lv_ymin: float|None = None) -> matplotlib.figure.Figure:
        """
        Plots the reconstructed mean waveforms with percentile bands and the corresponding 
        loading vector of a PCA component.

        Args:
        component_data (dict): Reconstruction output containing mean waveforms, bands, and loading vector.
        title_waveform (str): Title for the waveform subplot.
        title_loading (str): Title for the loading vector subplot.

        Returns:
            matplotlib.figure.Figure: The figure object containing the plots.
        """
        mean_waveform_pain = component_data['mean_waveform_pain']
        mean_waveform_no_pain = component_data['mean_waveform_no_pain']
        lower_band = component_data['lower_band']
        upper_band = component_data['upper_band']
        lower_band_pain = component_data['lower_band_pain']
        upper_band_pain = component_data['upper_band_pain']
        lower_band_nopain = component_data['lower_band_no_pain']
        upper_band_nopain = component_data['upper_band_no_pain']
        loading_vector = component_data['loading_vector']
        
        labels_colors = [
            ("Pain", mean_waveform_pain),
            ("No Pain", mean_waveform_no_pain),
            ("Lower Band", lower_band),
            ("Upper Band", upper_band),
        ]
        
        labels_colors_groups = [
            ("Pain", mean_waveform_pain),
            ("No Pain", mean_waveform_no_pain),
            ("Lower Band Pain", lower_band_pain),
            ("Upper Band Pain", upper_band_pain),
            ("Lower Band No Pain", lower_band_nopain),
            ("Upper Band No Pain", upper_band_nopain),
        ]

        # TODO: adjust ymin/ymax for reconstruction plots (equal for overall & group plots)
        fig, axs = plt.subplots(3, 1, figsize=(10, 12), constrained_layout=True)
        all_values = []
        
        for label, data in labels_colors:
            linestyle = '-'
            if "Lower Band" in label:
                linestyle = '--'
            elif "Upper Band" in label:
                linestyle = ':'

            axs[0].plot(data, label=label, linestyle=linestyle,
                        color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)
            all_values.extend(data)

        for label, data in labels_colors_groups:
            linestyle = '-'
            if "Lower Band" in label:
                linestyle = '--'
            elif "Upper Band" in label:
                linestyle = ':'

            axs[1].plot(data, label=label, linestyle=linestyle,
                        color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)
            all_values.extend(data)

        reconst_ymin, reconst_ymax = min(all_values), max(all_values)
        reconst_yrange = reconst_ymax - reconst_ymin

        reconst_padding = 0.1 * reconst_yrange
       
        axs[0].set_title(title_waveform)
        axs[0].set_ylim(reconst_ymin - reconst_padding, reconst_ymax + reconst_padding)
        axs[0].set_xlabel("Normalized time (%)")
        axs[0].set_ylabel("Movement in Degrees (°)")
        axs[0].legend(loc='upper right')
        axs[0].grid(True)
        
        axs[1].set_title(title_waveform)
        axs[1].set_ylim(reconst_ymin - reconst_padding, reconst_ymax + reconst_padding)
        axs[1].set_xlabel("Normalized time (%)")
        axs[1].set_ylabel("Movement in Degrees (°)")
        axs[1].legend(loc='upper right')
        axs[1].grid(True)
        
        # Plot loading vector
        lv_range = lv_ymax - lv_ymin
        lv_padding = 0.1 * lv_range
        max_lv_idx = np.argmax(np.abs(loading_vector))

        axs[2].plot(loading_vector, label="Loading Vector",
            color=self.get_color_for_label("loading"))
        axs[2].axvline(x=max_lv_idx, color='black', linestyle='--', label="Absolute Maximum", alpha=0.7)
        axs[2].set_title(title_loading)
        axs[2].set_xlabel("Component Index")
        axs[2].set_ylabel("Loading Value")
        axs[2].set_ylim(lv_ymin - lv_padding, lv_ymax + lv_padding)
        axs[2].legend(loc='upper right')
        axs[2].grid(True)
        plt.close(fig)
        return fig      

    def plot_top_3_PCAs(self, orig_data:pd.DataFrame, component_data:dict, title_waveform:str="Mean Waveform", 
                            title_loading:str="Loading Vector", lv_ymax: float|None = None, lv_ymin: float|None = None) -> matplotlib.figure.Figure:
        """
        Plot the top 3 principal components with raw waveforms, loading vectors, and reconstructed single-PC waveforms.

        Args:
            orig_data (pd.DataFrame): Original time-series data.
            component_data (dict): Dictionary containing PCA component information.
            title_waveform (str): Title for waveform plots.
            title_loading (str): Title for loading vector plots.
            lv_ymax (float | None): Optional max value for loading vector y-axis.
            lv_ymin (float | None): Optional min value for loading vector y-axis.

        Returns:
            matplotlib.figure.Figure: Figure containing the subplots for top 3 PCs.
        """    
        fig, axes = plt.subplots(3, 3, figsize=(18, 15), constrained_layout=True)
        all_values = []
        all_loading_vectors = []
        for i, rank in enumerate(component_data):
            target = component_data[rank]['target']
            target_clean = target.removesuffix("joint angle").strip()
            axis = component_data[rank]['axis']
            pc_index = component_data[rank]['pc_index']
            if component_data[rank]['rotation_sequence'] != None:
                rotation_sequence = component_data[rank]['rotation_sequence'].replace("_", " ")
            else:
                rotation_sequence = ""
            mean_waveform_pain = component_data[rank]['component_data']['mean_waveform_pain']
            mean_waveform_no_pain = component_data[rank]['component_data']['mean_waveform_no_pain']
            lower_band = component_data[rank]['component_data']['lower_band']
            upper_band = component_data[rank]['component_data']['upper_band']
            loading_vector = component_data[rank]['component_data']['loading_vector']
            all_loading_vectors.extend(loading_vector)
                        
            labels_colors = [
                ("Pain", mean_waveform_pain),
                ("No Pain", mean_waveform_no_pain),
                ("Lower Band", lower_band),
                ("Upper Band", upper_band),
            ]  

            if axis != None:
                df_target_axis = orig_data[(orig_data['target'] == target) & (orig_data['axis'] == axis)]
            else:
                df_target_axis = orig_data[(orig_data['target'] == target)]
            time_cols = sorted(
                [col for col in df_target_axis.columns if col.startswith("t") and col[1:].isdigit()],
                key=lambda c: int(c[1:])
            )
            time_points = [int(c[1:]) for c in time_cols]
            df_mean = (
                df_target_axis.groupby(["participant_id", "PRMD_ever"])[time_cols]
                .mean()
                .reset_index()
            )
            
            ax = axes[0,i]
            ax.set_title(target_clean + "\n" + rotation_sequence + "\nPC " + str(pc_index), fontsize = 20)
            for _, row in df_mean.iterrows():
                label = 'Pain' if row["PRMD_ever"] == 1 else "No Pain"
                ax.plot(time_points, row[time_cols].values, color=self.get_color_for_label(label), alpha=1.0)
            
            ax = axes[1,i]  
            ax.plot(loading_vector, label="Loading Vector", color=self.get_color_for_label("loading"))  
            
            ax = axes[2,i]  

            
            for label, data in labels_colors:
                linestyle = '-'
                if "Lower Band" in label:
                    linestyle = '--'
                elif "Upper Band" in label:
                    linestyle = ':'

                ax.plot(data, label=label, linestyle=linestyle,
                            color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)
                all_values.extend(data)
                
            axes[0,i].tick_params(axis='both', which='major', labelsize=14)
            axes[1,i].tick_params(axis='both', which='major', labelsize=14)
            axes[2,i].tick_params(axis='both', which='major', labelsize=14)
        
        
        lv_ymin = min(all_loading_vectors)
        lv_ymax = max(all_loading_vectors)
        reconst_lvrange = lv_ymax - lv_ymin
        reconst_padding = 0.1 * reconst_lvrange
                
        axes[1,0].set_ylim(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        axes[1,1].set_ylim(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        axes[1,2].set_ylim(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        axes[0,0].set_ylabel("Movement in Degrees (°)", fontsize = 16, labelpad = 10)
        axes[2,0].set_ylabel("Movement in Degrees (°)", fontsize = 16, labelpad = 10)
        axes[2,0].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)
        axes[2,1].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)
        axes[2,2].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)

        
        fig.text(0.04, 0.78, 'Raw \nwaveforms', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        fig.text(0.04, 0.5, 'PC \nLoading vector', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        fig.text(0.04, 0.22, 'Single PC \nreconstruction', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        plt.tight_layout(rect=[0.05, 0, 1, 1])    
            
        return fig
    
    def plot_top_3_PCAs_reduced(self, orig_data:pd.DataFrame, component_data:dict, title_waveform:str="Mean Waveform", 
                            title_loading:str="Loading Vector", lv_ymax: float|None = None, lv_ymin: float|None = None) -> matplotlib.figure.Figure:
        """
        Plot the top 3 principal components with raw waveforms and reconstructed single-PC waveforms in a reduced layout.

        Args:
            orig_data (pd.DataFrame): Original time-series data.
            component_data (dict): Dictionary containing PCA component information.
            title_waveform (str): Title for waveform plots.
            title_loading (str): Title for loading vector plots.
            lv_ymax (float | None): Optional max value for loading vector y-axis.
            lv_ymin (float | None): Optional min value for loading vector y-axis.

        Returns:
            matplotlib.figure.Figure: Figure containing the reduced subplots for top 3 PCs.
        """
        #TODO: adjust scaling for the plots (same PCA -> same scaling!)     
        fig, axes = plt.subplots(2, 3, figsize=(18, 10), constrained_layout=True)
        all_values = []
        all_loading_vectors = []
        for i, rank in enumerate(component_data):
            target = component_data[rank]['target']
            target_clean = target.removesuffix("joint angle").strip()
            axis = component_data[rank]['axis']
            pc_index = component_data[rank]['pc_index']
            if component_data[rank]['rotation_sequence'] != None:
                rotation_sequence = component_data[rank]['rotation_sequence'].replace("_", " ")
            else:
                rotation_sequence = ""
            mean_waveform_pain = component_data[rank]['component_data']['mean_waveform_pain']
            mean_waveform_no_pain = component_data[rank]['component_data']['mean_waveform_no_pain']
            lower_band = component_data[rank]['component_data']['lower_band']
            upper_band = component_data[rank]['component_data']['upper_band']
            loading_vector = component_data[rank]['component_data']['loading_vector']
            all_loading_vectors.extend(loading_vector)
                        
            labels_colors = [
                ("Pain", mean_waveform_pain),
                ("No Pain", mean_waveform_no_pain),
                ("Lower Band", lower_band),
                ("Upper Band", upper_band),
            ]  

            if axis != None:
                df_target_axis = orig_data[(orig_data['target'] == target) & (orig_data['axis'] == axis)]
            else:
                df_target_axis = orig_data[(orig_data['target'] == target)]
            time_cols = sorted(
                [col for col in df_target_axis.columns if col.startswith("t") and col[1:].isdigit()],
                key=lambda c: int(c[1:])
            )
            time_points = [int(c[1:]) for c in time_cols]
            df_mean = (
                df_target_axis.groupby(["participant_id", "PRMD_ever"])[time_cols]
                .mean()
                .reset_index()
            )
            
            ax = axes[0,i]
            ax.set_title(target_clean + "\n" + rotation_sequence + "\nPC " + str(pc_index), fontsize = 20)
            for _, row in df_mean.iterrows():
                label = 'Pain' if row["PRMD_ever"] == 1 else "No Pain"
                ax.plot(time_points, row[time_cols].values, color=self.get_color_for_label(label), alpha=1.0)
            
            ax = axes[1,i]  
            
            for label, data in labels_colors:
                linestyle = '-'
                if "Lower Band" in label:
                    linestyle = '--'
                elif "Upper Band" in label:
                    linestyle = ':'

                ax.plot(data, label=label, linestyle=linestyle,
                            color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)
                all_values.extend(data)
            
            y_min = df_mean[time_cols].to_numpy().min()
            y_max = df_mean[time_cols].to_numpy().max()
            range = y_max - y_min
            padding = 0.1 * range
            
            axes[0,i].set_ylim(y_min - padding, y_max + padding)
            axes[1,i].set_ylim(y_min - padding, y_max + padding)
                
            axes[0,i].tick_params(axis='both', which='major', labelsize=14)
            axes[1,i].tick_params(axis='both', which='major', labelsize=14)

        axes[0,0].set_ylabel("Movement in Degrees (°)", fontsize = 16, labelpad = 10)
        axes[1,0].set_ylabel("Movement in Degrees (°)", fontsize = 16, labelpad = 10)
        axes[1,0].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)
        axes[1,1].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)
        axes[1,2].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)
        
        fig.text(0.04, 0.725, 'Raw \nwaveforms', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        fig.text(0.04, 0.29, 'Single PC \nreconstruction', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        plt.tight_layout(rect=[0.05, 0, 1, 1])    
            
        return fig
    
    def plot_feature_importance(self, lda_results_component):
        """
        Plot the feature importance based on LDA coefficients across cross-validation folds.

        Args:
            lda_results_component: LDA component results containing stacked feature values and feature descriptions.

        Returns:
            matplotlib.figure.Figure: Figure showing feature importance as a boxplot.
        """
        stacked_features = lda_results_component['stacked_feature_values'].to_list()
        transposed = list(map(list, zip(*stacked_features[0])))
        features = lda_results_component['feature_description'].to_list()[0]

        fig = plt.figure(figsize=(10, 6))
        plt.boxplot(transposed, labels=features)
        plt.xticks(rotation=45, ha='right')
        plt.ylabel('Mean Absolute Coefficient (Importance)')
        plt.title('Feature Importance with LDA Coefficients Across CV Folds')
        plt.tight_layout()

        return fig
       
    @staticmethod
    def save_plot(fig:matplotlib.figure.Figure, file_path:Path, filename:str, dpi:int=300, file_format:str='png'):
        """
        Saves a matplotlib figure to a specified file path with given resolution and format.

        Args:
            fig (matplotlib.figure.Figure): Figure object to save.
            file_path (Path): Directory path where the file will be saved.
            filename (str): Name of the file without extension.
            dpi (int, optional): Resolution in dots per inch. Defaults to 300.
            file_format (str, optional): File format (e.g., 'png', 'pdf'). Defaults to 'png'.
        """
        full_filename = f"{filename}.{file_format}"
        fig.savefig(file_path/full_filename, dpi=dpi, format=file_format, bbox_inches='tight')
        plt.close()
        print(f"Plot saved to {full_filename}")
    
    def plot_distribution(self, df:pd.DataFrame, column:str, target:str, axis:str, pc_index:int, meas_time_point:str, pain_groups:list[str],
                          kind:str='hist', bins:int=10, **kwargs) -> matplotlib.figure.Figure:
        """
        Plots the distribution of a DataFrame column separately for pain and no-pain groups.

        Args:
            df (pandas.DataFrame): Input data containing scores and group labels.
            column (str): Column name to plot.
            target (str): Target descriptor for plot title.
            axis (str): Axis descriptor for plot title.
            pc_index (int): Principal component index for title.
            meas_time_point (str): Measurement time point for title.
            kind (str, optional): Type of plot, 'hist' for histogram or 'kde' for density. Defaults to 'hist'.
            bins (int, optional): Number of bins for histogram. Defaults to 10.
            **kwargs: Additional plotting keyword arguments.

        Returns:
            matplotlib.figure.Figure: Figure object with the two distribution subplots.
        """

        df_pain = df[df['PRMD_ever'] == 1]
        df_nopain = df[df['PRMD_ever'] == 0]
        pain_group_names = self.concat_pain_groups(pain_groups)
        fig, axs = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)

        if kind == 'hist':
            axs[0].hist(
                df_pain[column].dropna(),
                bins=bins,
                alpha=0.7,
                edgecolor='black',
                color=self.get_color_for_label('Pain'),
                **kwargs
            )
            axs[0].set_title('Pain')
            axs[0].set_xlabel(column)
            axs[0].set_ylabel('Frequency')

            axs[1].hist(
                df_nopain[column].dropna(),
                bins=bins,
                alpha=0.7,
                edgecolor='black',
                color=self.get_color_for_label('No Pain'),
                **kwargs
            )
            axs[1].set_title('No Pain')
            axs[1].set_xlabel(column)
            axs[1].set_ylabel('Frequency')

        elif kind == 'kde':
            df_pain[column].dropna().plot(
                kind='kde',
                ax=axs[0],
                color=self.get_color_for_label('Pain'),
                **kwargs
            )
            axs[0].set_title('Pain')
            axs[0].set_xlabel(column)
            axs[0].set_ylabel('Density')

            df_nopain[column].dropna().plot(
                kind='kde',
                ax=axs[1],
                color=self.get_color_for_label('No Pain'),
                **kwargs
            )
            axs[1].set_title('No Pain')
            axs[1].set_xlabel(column)
            axs[1].set_ylabel('Density')

        else:
            raise ValueError("kind must be 'hist' or 'kde'")

        fig.suptitle(f'{kind.upper()} of PC Scores ({meas_time_point}/Group:{pain_group_names}): {target}, {axis}, PC {pc_index}')
        return fig
    
    @staticmethod
    def plot_linearity(df:pd.DataFrame, fig_title:str) -> tuple[matplotlib.figure.Figure, matplotlib.figure.Figure]:
        """
        Plots a scatter matrix of selected timepoints and a lag plot for all participants.

        Args:
            df (pd.DataFrame): DataFrame with participant data.
            fig_title (str): Title for the plots.

        Returns:
            tuple[Figure, Figure]: Scatter matrix figure and lag plot figure.
        """
        subset = df.iloc[:, -202::20]
        fig1 = plt.figure(figsize=(12, 12))
        scatter_matrix(subset, ax=fig1.add_subplot(111))
        plt.suptitle(f'Scatter Matrix of Selected Timepoints: {fig_title}')
        plt.tight_layout()
        
        fig2 = plt.figure(figsize=(6, 6))
        unique_participants = df['participant_id'].unique()
        for i, participant in enumerate(unique_participants):  # plot first 5 samples
            df_part = df[df['participant_id'] == participant].iloc[:, -202:]
            flattened = pd.Series(np.ravel(df_part))
            lag_plot(flattened, lag=1, alpha=0.4, c=plt.cm.tab20(i), label=f'Sample {i+1}')
        plt.title(f'Lag Plot for all participants Samples: {fig_title}')
        plt.xlabel('Value at time t') 
        plt.ylabel('Value at time t+1')
        plt.legend()
        
        return fig1, fig2

    @staticmethod    
    def plot_corr_matrix(corr_matrix:pd.DataFrame, target:str, axis:str) -> matplotlib.figure.Figure:
        """
        Plots a correlation matrix heatmap.

        Args:
            corr_matrix (pd.DataFrame): Correlation matrix.
            target (str): Target label for title.
            axis (str): Axis label for title.

        Returns:
            matplotlib.figure.Figure: Heatmap figure.
        """
        fig = plt.figure(figsize=(10, 8))
        annotate = corr_matrix.shape[0] <= 20
        sns.heatmap(corr_matrix, annot=annotate, fmt=".2f",vmin=0, vmax=1, cmap='coolwarm', square=True)
        plt.title(f'Correlation Matrix Heatmap: {target}, {axis}')
        return fig
    
    @staticmethod
    def plot_mean_std(df:pd.DataFrame, time_col:str='dp_timepoint', value_cols:list[str] | None = None, 
                      title:str ='Mean and Std Dev over Time') -> matplotlib.figure.Figure:
        """
        Plots mean and standard deviation of time series data.

        Args:
            df (pd.DataFrame): Input data.
            time_col (str): Column with time values.
            value_cols (list[str] | None): Measurement columns; all except time_col if None.
            title (str): Plot title.

        Returns:
            matplotlib.figure.Figure: Mean ± std plot figure.
        """
        if value_cols is None:
            value_cols = [col for col in df.columns if col != time_col]

        grouped = df.groupby(time_col)[value_cols]

        mean_series = grouped.mean()
        std_series = grouped.std()

        plt.figure(figsize=(12, 6))
        
        for col in value_cols:
            plt.plot(mean_series.index, mean_series[col], label=f'{col} Mean')
            plt.fill_between(mean_series.index,
                            mean_series[col] - std_series[col],
                            mean_series[col] + std_series[col],
                            alpha=0.2, label=f'{col} ±1σ')

        plt.title(title)
        plt.xlabel(time_col)
        plt.ylabel("Value")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
 
    def plot_mean_std_by_group(self, df:pd.DataFrame, time_col:str='dp_timepoint', value_cols:list[str] | None = None, 
                               group_col:list[str] | None =None, title:str='Mean and Std Dev over Time by Group'
                               ) -> matplotlib.figure.Figure:
        """
        Plots mean and standard deviation of time series data for each group.

        Args:
            df (pd.DataFrame): Input data.
            time_col (str): Column with time values.
            value_cols (list[str] | None): Measurement columns; inferred if None.
            group_col (str | None): Column to group by.
            title (str): Plot title.

        Returns:
            matplotlib.figure.Figure: Grouped mean ± std plot figure.
        """
        # Determine which value columns to use
        exclude_cols = [time_col]
        if group_col:
            exclude_cols.append(group_col)
        if value_cols is None:
            value_cols = [col for col in df.columns if col not in exclude_cols]
        #elif len(value_cols) == 1:
        #    value_cols =  value_cols[0]
        
        
        fig = plt.figure(figsize=(12, 6))

        if group_col:
            groups = df[group_col].unique()
            for group in groups:
                df_group = df[df[group_col] == group]
                if len(value_cols) == 1:
                    grouped = df_group.groupby(time_col)[value_cols[0]]
                else:
                    grouped = df_group.groupby(time_col)[value_cols]
                mean_series = grouped.mean()
                std_series = grouped.std()

                if group == 1:
                    group_label = "Pain"
                elif group == 0:
                    group_label = "No pain"
                else:
                    group_label = str(group)
                
                color = self.get_color_for_label(group_label.lower())
                
                for col in value_cols:
                    label = f'{group_label} Mean'
                    if len(value_cols) == 1:
                        plt.plot(mean_series.index, mean_series, label=label, color=color)
                        plt.fill_between(mean_series.index,
                                    mean_series - std_series,
                                    mean_series + std_series,
                                    alpha=0.2, color=color)
                    else:
                        plt.plot(mean_series.index, mean_series[col], label=label, color=color)
                        plt.fill_between(mean_series.index,
                                    mean_series[col] - std_series[col],
                                    mean_series[col] + std_series[col],
                                    alpha=0.2, color=color)
                    plt.legend(loc='upper right')
                    plt.title(title)
                    plt.xlabel(time_col)
                    plt.ylabel("Value")
                    plt.grid(True)
                    plt.tight_layout()
        else:
            grouped = df.groupby(time_col)[value_cols]
            mean_series = grouped.mean()
            std_series = grouped.std()

            for col in value_cols:
                plt.plot(mean_series.index, mean_series[col], label=f'{col} Mean')
                plt.fill_between(mean_series.index,
                                mean_series[col] - std_series[col],
                                mean_series[col] + std_series[col],
                                alpha=0.2, label=f'{col} ±1σ')

                plt.legend(loc='upper right')
                plt.title(title)
                plt.xlabel(time_col)
                plt.ylabel("Value")
                plt.legend()
                plt.grid(True)
                plt.tight_layout()
        return fig
    
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
    
    @staticmethod
    def mean_ci(series, confidence=0.95):
        Z = 1.96 if confidence == 0.95 else 2.58

        series = series.dropna()
        mean = np.mean(series)
        se = np.std(series, ddof=1) / np.sqrt(len(series))
        ci_lower = mean - Z * se
        ci_upper = mean + Z * se

        return pd.DataFrame({
            "mean": [mean],
            "ci_lower": [ci_lower],
            "ci_upper": [ci_upper]
        })
    
    @staticmethod
    def plot_lda_boxlpots(df_cv_results, df_no_cv_results, column_names):
        """
        Plot boxplots of LDA results from cross-validation and overlay results without cross-validation.

        Args:
            df_cv_results (pd.DataFrame): Cross-validated LDA results.
            df_no_cv_results (pd.DataFrame): LDA results without cross-validation.
            column_names (list[str]): List containing two column names: [y-values for CV, y-values for no-CV].

        Returns:
            matplotlib.figure.Figure: Figure containing the boxplots with overlaid horizontal lines.
        """
        fig, ax = plt.subplots(figsize=(10, 10))
        df_cv_results = df_cv_results.drop(columns=['stacked_feature_values', 'feature_imp_mean', 'feature_imp_sd', 'feature_description'])
        df_cv_exploded = df_cv_results.apply(pd.Series.explode).reset_index(drop=True)
        df_plot = df_cv_exploded.dropna(subset=[column_names[0]])

        #TODO: select the full list of values as y
        sns.boxplot(
            data=df_plot,
            x="lda_nr_components",
            y=column_names[0],
            color="#5B8E7D",
            fill = False,
            showfliers=False,
            width=0.5,
            ax = ax
        )
       
        box_width = 0.5
        for i, row in df_no_cv_results.iterrows():
            y = row[column_names[1]]
            x_center = i  
            plt.hlines(y=y, xmin=x_center - box_width/2, xmax=x_center + box_width/2, colors='#BC4B51', linewidth=2)

        # TODO: adjust titles and x/y labels
        column_label = (
            'Missclassification Error' if 'missclass' in column_names[0] 
            else 'ROC AUC' if 'roc_auc' in column_names[0] 
            else 'Accuracy'
        )
        
        ax.set_xlabel("Number of Principal Components per LDA", fontsize = 30, labelpad = 10)
        ax.set_ylabel(f"{column_label}", fontsize = 30, labelpad = 10)
        ax.set_ylim(0.0,1.0)
        ax.tick_params(axis='both', which='major', labelsize=24)
        plt.legend()
        plt.tight_layout()
        return fig

    def plot_lda_boxlpots_CI(self, df_cv_results, df_no_cv_results, column_names):
        """
        Plot boxplots of LDA results from cross-validation and overlay results without cross-validation.

        Args:
            df_cv_results (pd.DataFrame): Cross-validated LDA results.
            df_no_cv_results (pd.DataFrame): LDA results without cross-validation.
            column_names (list[str]): List containing two column names: [y-values for CV, y-values for no-CV].

        Returns:
            matplotlib.figure.Figure: Figure containing the boxplots with overlaid horizontal lines.
        """
        #fig, ax = plt.subplots(figsize=(10, 10))
        df_cv_results = df_cv_results.drop(columns=['stacked_feature_values', 'feature_imp_mean', 'feature_imp_sd', 'feature_description'])
        df_cv_exploded = df_cv_results.apply(pd.Series.explode).reset_index(drop=True)
        df_plot = df_cv_exploded.dropna(subset=[column_names[0]])

        df_ci = (
            df_plot
            .groupby("lda_nr_components")[column_names[0]]
            .apply(self.mean_ci)
            .reset_index()
        )
        
        #TODO: select the full list of values as y
        fig, ax = plt.subplots(figsize=(10, 10))

        ax.errorbar(
            x=df_ci["lda_nr_components"],
            y=df_ci["mean"],
            yerr=[
                df_ci["mean"] - df_ci["ci_lower"],
                df_ci["ci_upper"] - df_ci["mean"]
            ],
            fmt='o',
            capsize=5,
            color="#5B8E7D",
            label = "CV with 95% CI"
        )
       
# --- Reference lines (no-CV results) ---
        box_width = 0.5
        ax.hlines(
            y=df_no_cv_results[column_names[1]],
            xmin=df_ci["lda_nr_components"] - box_width / 2,
            xmax=df_ci["lda_nr_components"] + box_width / 2,
            colors="#BC4B51",
            linewidth=2,
            label="No-CV",
            zorder=2
        )

        # --- Labels ---
        column_label = (
            'Missclassification Error' if 'missclass' in column_names[0]
            else 'ROC AUC' if 'roc_auc' in column_names[0]
            else 'Accuracy'
        )

        ax.set_xlabel("Number of Principal Components per LDA", fontsize=30, labelpad=10)
        ax.set_ylabel(column_label, fontsize=30, labelpad=10)

        # --- Axis formatting ---
        min_y = min(min(df_ci['ci_lower']), min(df_no_cv_results[column_names[1]])) - 0.1
        max_y = max(max(df_ci['ci_lower']), max(df_no_cv_results[column_names[1]])) + 0.1
        
        ax.set_ylim(min_y, max_y)
        ax.set_xticks(df_ci["lda_nr_components"])
        ax.tick_params(axis='both', which='major', labelsize=24)

        # --- Legend & layout ---
        ax.legend(fontsize=22)
        plt.tight_layout()
        return fig
        
    def plot_lda_class_distribution_from_row(self, df_row):
        """
        Visualize class separation from LDA results stored in DB.
        Works directly with saved lda_scores, lda_scalings, lda_class_means.
        Returns a matplotlib Figure.
        """
        
        row = df_row.iloc[0]
        if row["lda_scores"] is None:
            print("⚠️ No LDA scores available.")
            return None
    
        scores = row["lda_scores"]
        df_lda = pd.DataFrame(scores)
        scalings = row["lda_scalings"]
        class_means = row["lda_class_means"]

        projected_means = np.dot(class_means, scalings)

        fig, ax = plt.subplots(figsize=(8, 5))
        unique_classes = df_lda["class"].unique()
        
        class_label_map = {0: "no_pain", 1: "pain"}
        class_colors = {cls: self.COLOR_MAP[class_label_map[cls]] for cls in df_lda["class"].unique()}
        
        kde_handles, kde_labels = ax.get_legend_handles_labels()

        sns.kdeplot(
            data=df_lda,
            x="LD1",
            hue="class",
            fill=True,
            common_norm=False,
            alpha=0.5,
            linewidth=1.5,
            ax=ax,
            palette=[class_colors[cls] for cls in sorted(df_lda["class"].unique())]
        )
        class_centers = df_lda.groupby("class")["LD1"].mean()

        mean_handles = []
        for cls, mean_val in class_centers.items():
            line = ax.axvline(
                mean_val,
                color=class_colors[cls],
                linestyle="--",
                linewidth=2
            )
            mean_handles.append(line)

        legend_handles = [
            Line2D([0], [0], color=class_colors[cls], lw=3, label=class_label_map[cls])
            for cls in class_centers.keys()
        ] + [
            Line2D([0], [0], color=class_colors[cls], lw=2, linestyle="--", label=f"{class_label_map[cls]} mean")
            for cls in class_centers.keys()
        ]

        ax.legend(handles=legend_handles, loc="best")

        ax.set_xlabel("LD1 (Discriminant Function 1)", fontsize = 16, labelpad = 10)
        ax.set_ylabel("Density", fontsize = 16, labelpad = 10)
        ax.tick_params(axis='both', which='major', labelsize=14)
        ax.grid(True, linestyle="--", alpha=0.4)
        fig.tight_layout()

        #TEMP
        import scipy.stats as st
        df_scores = pd.DataFrame(scores) 
        participant_means = df_scores.groupby('participant_id').mean()
        df_pain = participant_means[participant_means['class'] == 1]['LD1']
        df_nopain = participant_means[participant_means['class'] == 0]['LD1']
        result = st.ttest_ind(df_pain, df_nopain, equal_var=True)
        t_stat = result.statistic
        p_value = result.pvalue
        degFree = result.df
        return fig
            
    def plot_lda_class_separation(self, df, lda_means=None):
        """
        Visualize LDA separation (LD1 vs LD2) from stored lda_scores.
        """
        row = df.iloc[0]

        if row["lda_scores"] is None:
            print("⚠️ No LDA scores found in this row.")
            return None

        try:
            lda_scores = row["lda_scores"]
            df_lda = pd.DataFrame(lda_scores)
        except Exception as e:
            print(f"⚠️ Could not parse lda_scores: {e}")
            return None

        if "LD1" not in df_lda.columns:
            print("⚠️ LDA scores do not include LD1/LD2 columns.")
            return None

        lda_means = None
        if "lda_class_means" in row and row["lda_class_means"] is not None:
            try:
                lda_means = (
                    row["lda_class_means"]
                )
            except Exception as e:
                print(f"⚠️ Could not parse lda_class_means: {e}")
                lda_means = None

        fig, ax = plt.subplots(figsize=(8, 6))

        if "LD2" in df_lda.columns:
            sns.scatterplot(
                data=df_lda,
                x="LD1",
                y="LD2",
                hue="class",
                palette="Set1",
                s=70,
                alpha=0.8,
                edgecolor="k",
                ax=ax,
            )
        else:
            df_lda["y_dummy"] = 0  
            sns.stripplot(
                data=df_lda,
                x="LD1",
                y="y_dummy",
                hue="class",
                palette="Set1",
                size=8,
                alpha=0.8,
                ax=ax,
                jitter=True,
            )
            ax.set_yticks([])
            ax.set_ylabel("")

        if lda_means is not None:
            for i, mean_vec in enumerate(lda_means):
                if len(mean_vec) >= 2:
                    ax.scatter(
                        mean_vec[0],
                        mean_vec[1],
                        color="black",
                        marker="X",
                        s=200,
                        label=f"Mean class {i}",
                    )

        title = (
            f"LDA Class Separation ({row['device']}, {row['meas_time_point']})\n"
            f"{row['pain_groups']} | {row['lda_nr_components']} PCs | {row['lda_validation_type']}"
        )
        ax.set_title(title)
        ax.set_xlabel("LD1")
        if "LD2" in df_lda.columns:
            ax.set_ylabel("LD2")
        else:
            ax.set_ylabel("")

        ax.legend()
        ax.grid(True, linestyle="--", alpha=0.4)
        fig.tight_layout()

        return fig