from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
#plt.rcParams['axes.prop_cycle'] = plt.cycler(color=['#8CB369', '#BC4B51', '#5B8E7D', '#F4A259'])
import seaborn as sns
import pandas as pd
import numpy as np



class DataPlotter:
    
    def __init__(self):
        """
        Initializes the PCA_Analyser with a data processor.
        """   
        self.COLOR_MAP = {
        "pain": "#BC4B51",      # Blau
        "no_pain": "#8CB369",   # Orange
        "loading": "#5B8E7D",   # Grün
} 
        
    def save_distribution_plot(self, fig, target, axis, pc_index, meas_time_point):
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots" / "Histograms"
        self.save_plot(fig, output_path, f"{meas_time_point}_PC_Scores_Histogram_{target}_{axis}_PC_{pc_index}")
        plt.close()
        
        
    def plot_PCA_reconstruction_per_group(self,component_data, title_waveform="Mean Waveform", title_loading="Loading Vector"):
        """
        Plots the reconstructed mean waveforms with percentile bands and the corresponding 
        loading vector of a PCA component.

        Args:
            component_data (dict): Output dictionary from a reconstruction method containing waveform and PCA info.
            title_waveform (str): Title for the mean waveform plot.
            title_loading (str): Title for the loading vector plot.

        Returns:
            tuple[matplotlib.figure.Figure, list[matplotlib.axes._axes.Axes]]: The figure and axes objects for further customization or saving.
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
        label_lower = label.lower()
        if "no pain" in label_lower or "no_pain" in label_lower:
            return self.COLOR_MAP["no_pain"]
        elif "pain" in label_lower:
            return self.COLOR_MAP["pain"]
        elif "load" in label_lower:
            return self.COLOR_MAP["loading"]
        return "#000000"  # Fallback: Schwarz

    #@staticmethod
    #def plot_PCA_reconstruction_per_group(component_data, title_waveform="Mean Waveform", title_loading="Loading Vector"):
    #    """
    #    Plots the reconstructed mean waveforms with percentile bands and the corresponding 
    #    loading vector of a PCA component.
#
    #    Args:
    #        component_data (dict): Output dictionary from a reconstruction method containing waveform and PCA info.
    #        title_waveform (str): Title for the mean waveform plot.
    #        title_loading (str): Title for the loading vector plot.
#
    #    Returns:
    #        tuple[matplotlib.figure.Figure, list[matplotlib.axes._axes.Axes]]: The figure and axes objects for further customization or saving.
    #    """
    #    
    #    mean_waveform_pain = component_data['mean_waveform_pain']
    #    mean_waveform_no_pain = component_data['mean_waveform_no_pain']
    #    lower_band_pain = component_data['lower_band_pain']
    #    upper_band_pain = component_data['upper_band_pain']
    #    lower_band_nopain = component_data['lower_band_no_pain']
    #    upper_band_nopain = component_data['upper_band_no_pain']
    #    loading_vector = component_data['loading_vector']
    #    
    #    fig, axs = plt.subplots(2, 1, figsize=(10, 8))
    #
    #    # Plot mean waveforms
    #    axs[0].plot(mean_waveform_pain, label="Pain", color="blue")
    #    axs[0].plot(mean_waveform_no_pain, label="No Pain", color="orange")
    #    axs[0].plot(lower_band_pain, label="Lower Band Pain", linestyle='--', color="blue", alpha = 0.5)
    #    axs[0].plot(upper_band_pain, label="Upper Band Pain", linestyle=':', color="blue", alpha = 0.5)
    #    axs[0].plot(lower_band_nopain, label="Lower Band No Pain", linestyle='--', color="orange", alpha = 0.5)
    #    axs[0].plot(upper_band_nopain, label="Upper Band No Pain", linestyle=':', color="orange", alpha = 0.5)
    #    
    #    axs[0].set_title(title_waveform)
    #    axs[0].set_xlabel("Normalized time (%)")
    #    axs[0].set_ylabel("Amplitude (°)")
    #    axs[0].legend(loc='upper right')
    #    axs[0].grid(True)
    #    
    #    # Plot loading vector
    #    axs[1].plot(loading_vector, color="green")
    #    axs[1].set_title(title_loading)
    #    axs[1].set_xlabel("Component Index")
    #    axs[1].set_ylabel("Loading Value")
    #    axs[1].grid(True)
    #    
    #    return fig 

    def plot_PCA_reconstruction(self, component_data, title_waveform="Mean Waveform", title_loading="Loading Vector"):
        """
        Plots the reconstructed mean waveforms with percentile bands and the corresponding 
        loading vector of a PCA component.

        Args:
            component_data (dict): Output dictionary from a reconstruction method containing waveform and PCA info.
            title_waveform (str): Title for the mean waveform plot.
            title_loading (str): Title for the loading vector plot.

        Returns:
            tuple[matplotlib.figure.Figure, list[matplotlib.axes._axes.Axes]]: The figure and axes objects for further customization or saving.
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
    def save_plot(fig, file_path, filename, dpi=300, file_format='png'):
        """
        Saves a matplotlib figure to file.

        Parameters:
            fig (matplotlib.figure.Figure): The figure object to save.
            filename (str): Path or filename without extension.
            dpi (int): Resolution in dots per inch.
            file_format (str): File format, e.g. 'png', 'pdf', 'svg', etc.

        Returns:
            None
        """
        full_filename = f"{filename}.{file_format}"
        fig.savefig(str(file_path) +'\\' + full_filename, dpi=dpi, format=file_format, bbox_inches='tight')
        plt.close()
        print(f"Plot saved to {full_filename}")
        
    @staticmethod    
    def plot_distribution(df, column,target, axis, pc_index, meas_time_point, kind='hist', bins=10, **kwargs):
        """
        Plot the distribution of a DataFrame column.

        Parameters:
        - df: pandas DataFrame
        - column: str, column name to plot
        - kind: 'hist' for histogram, 'kde' for density plot
        - bins: int, number of bins (used for histogram)
        - **kwargs: additional keyword arguments for plot customization
        """
        fig, ax = plt.subplots(constrained_layout=True)
        if kind == 'hist':
            df.plot(kind='hist', bins=bins, edgecolor='black', ax = ax, **kwargs)
            plt.xlabel(column)
            plt.ylabel('Frequency')
            plt.title(f'Histogram of PC Scores ({meas_time_point}): {target}, {axis}, PC {pc_index}')
        elif kind == 'kde':
            df.plot(kind='kde', ax = ax, **kwargs)
            plt.xlabel(column)
            plt.ylabel('Density')
            plt.title(f'KDE of PC Scores: {target}, {axis}, PC {pc_index}')
        else:
            raise ValueError("kind must be 'hist' or 'kde'")
        return fig
    
    @staticmethod
    def plot_linearity(df, fig_title):
        from pandas.plotting import scatter_matrix
        from pandas.plotting import lag_plot
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
    def plot_corr_matrix(corr_matrix, target, axis):
        fig = plt.figure(figsize=(10, 8))
        annotate = corr_matrix.shape[0] <= 20
        sns.heatmap(corr_matrix, annot=annotate, fmt=".2f",vmin=0, vmax=1, cmap='coolwarm', square=True)
        plt.title(f'Correlation Matrix Heatmap: {target}, {axis}')
        return fig
    
    @staticmethod
    def plot_mean_std(df, time_col='dp_timepoint', value_cols=None, title='Mean and Std Dev over Time'):
        """
        Plots the mean and standard deviation of time series data.

        Parameters:
        - df: pandas DataFrame containing the data
        - time_col: name of the column with time values
        - value_cols: list of column names with measurements; if None, all except time_col are used
        - title: title of the plot
        """
        if value_cols is None:
            value_cols = [col for col in df.columns if col != time_col]

        # Group by time in case there are multiple measurements per timestamp
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
 
    def plot_mean_std_by_group(self,df, time_col='dp_time_point', value_cols=None, group_col=None, title='Mean and Std Dev over Time by Group'):
        """
        Plots the mean and standard deviation of time series data for one or more groups.

        Parameters:
        - df: pandas DataFrame
        - time_col: name of the time column
        - value_cols: list of measurement columns; if None, all except time_col and group_col are used
        - group_col: column to group data by (e.g., 'group' or 'condition')
        - title: title of the plot
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