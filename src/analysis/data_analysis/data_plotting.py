from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pandas.plotting import scatter_matrix
from pandas.plotting import lag_plot
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
        
    def plot_PCA_reconstruction_per_group(self,component_data:dict, title_waveform:str = "Mean Waveform", 
                                          title_loading:str ="Loading Vector") -> matplotlib.figure.Figure:
        """
        Plots the reconstructed mean waveforms with percentile bands and the corresponding 
        loading vector of a PCA component per participant group.

        Args:
            component_data (dict): Reconstruction data with waveforms and loadings.
            title_waveform (str): Title for waveform plot.
            title_loading (str): Title for loading plot.

        Returns:
            tuple: Matplotlib figure and axes.
        """
        
        mean_waveform_pain = component_data['mean_waveform_pain']
        mean_waveform_no_pain = component_data['mean_waveform_no_pain']
        lower_band_pain = component_data['lower_band_pain']
        upper_band_pain = component_data['upper_band_pain']
        lower_band_nopain = component_data['lower_band_no_pain']
        upper_band_nopain = component_data['upper_band_no_pain']
        loading_vector = component_data['loading_vector']
        fig, axs = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True)

        # Definierte Labels
        labels_colors = [
            ("Pain", mean_waveform_pain),
            ("No Pain", mean_waveform_no_pain),
            ("Lower Band Pain", lower_band_pain),
            ("Upper Band Pain", upper_band_pain),
            ("Lower Band No Pain", lower_band_nopain),
            ("Upper Band No Pain", upper_band_nopain),
        ]

        for label, data in labels_colors:
            linestyle = '-'
            if "Lower Band" in label:
                linestyle = '--'
            elif "Upper Band" in label:
                linestyle = ':'

            axs[0].plot(data, label=label, linestyle=linestyle,
                        color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)

        axs[0].set_title(title_waveform)
        axs[0].set_xlabel("Normalized time (%)")
        axs[0].set_ylabel("Amplitude (°)")
        axs[0].legend(loc='upper right')
        axs[0].grid(True)

        # Loading vector
        axs[1].plot(loading_vector, label="Loading Vector",
                    color=self.get_color_for_label("loading"))
        axs[1].set_title(title_loading)
        axs[1].set_xlabel("Component Index")
        axs[1].set_ylabel("Loading Value")
        axs[1].grid(True)

        return fig
    
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
                                title_loading:str="Loading Vector") -> matplotlib.figure.Figure:
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
        loading_vector = component_data['loading_vector']
        
                # Definierte Labels
        labels_colors = [
            ("Pain", mean_waveform_pain),
            ("No Pain", mean_waveform_no_pain),
            ("Lower Band", lower_band),
            ("Upper Band", upper_band)
        ]
        fig, axs = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True)
        
        for label, data in labels_colors:
            linestyle = '-'
            if "Lower Band" in label:
                linestyle = '--'
            elif "Upper Band" in label:
                linestyle = ':'

            axs[0].plot(data, label=label, linestyle=linestyle,
                        color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)

    
        # Plot mean waveforms
        #axs[0].plot(mean_waveform_pain, label="Pain", color="blue")
        #axs[0].plot(mean_waveform_no_pain, label="No Pain", color="orange")
        #axs[0].plot(lower_band, label="Lower Band", linestyle='--', color="black")
        #axs[0].plot(upper_band, label="Upper Band", linestyle=':', color="black")
        axs[0].set_title(title_waveform)
        axs[0].set_xlabel("Normalized time (%)")
        axs[0].set_ylabel("Amplitude (°)")
        axs[0].legend(loc='upper right')
        axs[0].grid(True)
        
        # Plot loading vector
        #axs[1].plot(loading_vector, color="green")
        axs[1].plot(loading_vector, label="Loading Vector",
            color=self.get_color_for_label("loading"))
        axs[1].set_title(title_loading)
        axs[1].set_xlabel("Component Index")
        axs[1].set_ylabel("Loading Value")
        axs[1].grid(True)
        plt.close(fig)
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
        fig.savefig(str(file_path) +'\\' + full_filename, dpi=dpi, format=file_format, bbox_inches='tight')
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