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

#from data_access.models.lda_results import LDAResults

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
        
    #def plot_PCA_reconstruction_per_group(self,component_data:dict, title_waveform:str = "Mean Waveform", 
    #                                      title_loading:str ="Loading Vector") -> matplotlib.figure.Figure:
    #    """
    #    Plots the reconstructed mean waveforms with percentile bands and the corresponding 
    #    loading vector of a PCA component per participant group.
#
    #    Args:
    #        component_data (dict): Reconstruction data with waveforms and loadings.
    #        title_waveform (str): Title for waveform plot.
    #        title_loading (str): Title for loading plot.
#
    #    Returns:
    #        tuple: Matplotlib figure and axes.
    #    """
    #    
    #    mean_waveform_pain = component_data['mean_waveform_pain']
    #    mean_waveform_no_pain = component_data['mean_waveform_no_pain']
    #    lower_band_pain = component_data['lower_band_pain']
    #    upper_band_pain = component_data['upper_band_pain']
    #    lower_band_nopain = component_data['lower_band_no_pain']
    #    upper_band_nopain = component_data['upper_band_no_pain']
    #    loading_vector = component_data['loading_vector']
    #    fig, axs = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True)
    #
    #    # Definierte Labels
    #    labels_colors = [
    #        ("Pain", mean_waveform_pain),
    #        ("No Pain", mean_waveform_no_pain),
    #        ("Lower Band Pain", lower_band_pain),
    #        ("Upper Band Pain", upper_band_pain),
    #        ("Lower Band No Pain", lower_band_nopain),
    #        ("Upper Band No Pain", upper_band_nopain),
    #    ]

    #    for label, data in labels_colors:
    #        linestyle = '-'
    #        if "Lower Band" in label:
    #            linestyle = '--'
    #        elif "Upper Band" in label:
    #            linestyle = ':'
#
    #        axs[0].plot(data, label=label, linestyle=linestyle,
    #                    color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)

    #    axs[0].set_title(title_waveform)
    #    axs[0].set_xlabel("Normalized time (%)")
    #    axs[0].set_ylabel("Amplitude (°)")
    #    axs[0].legend(loc='upper right')
    #    axs[0].grid(True)
#
    #    # Loading vector
    #    axs[1].plot(loading_vector, label="Loading Vector",
    #                color=self.get_color_for_label("loading"))
    #    axs[1].set_title(title_loading)
    #    axs[1].set_xlabel("Component Index")
    #    axs[1].set_ylabel("Loading Value")
    #    axs[1].grid(True)
#
    #    return fig
    
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
        
        
        # Definierte Labels
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
        # Plot mean waveforms
        #axs[0].plot(mean_waveform_pain, label="Pain", color="blue")
        #axs[0].plot(mean_waveform_no_pain, label="No Pain", color="orange")
        #axs[0].plot(lower_band, label="Lower Band", linestyle='--', color="black")
        #axs[0].plot(upper_band, label="Upper Band", linestyle=':', color="black")
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
        #axs[1].plot(loading_vector, color="green")
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
   
        
        fig, axes = plt.subplots(3, 3, figsize=(18, 15), constrained_layout=True)
        all_values = []
        all_loading_vectors = []
        for i, rank in enumerate(component_data):
            target = component_data[rank]['target']
            target_clean = target.removesuffix("joint angle").strip()
            axis = component_data[rank]['axis']
            pc_index = component_data[rank]['pc_index']
            rotation_sequence = component_data[rank]['rotation_sequence'].replace("_", " ")
            mean_waveform_pain = component_data[rank]['component_data']['mean_waveform_pain']
            mean_waveform_no_pain = component_data[rank]['component_data']['mean_waveform_no_pain']
            lower_band = component_data[rank]['component_data']['lower_band']
            upper_band = component_data[rank]['component_data']['upper_band']
            #lower_band_pain = component_data['lower_band_pain']
            #upper_band_pain = component_data['upper_band_pain']
            #lower_band_nopain = component_data['lower_band_no_pain']
            #upper_band_nopain = component_data['upper_band_no_pain']
            loading_vector = component_data[rank]['component_data']['loading_vector']
            all_loading_vectors.extend(loading_vector)
                        
            labels_colors = [
                ("Pain", mean_waveform_pain),
                ("No Pain", mean_waveform_no_pain),
                ("Lower Band", lower_band),
                ("Upper Band", upper_band),
            ]  

            df_target_axis = orig_data[(orig_data['target'] == target) & (orig_data['axis'] == axis)]
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
                #color = "tab:red" if row["PRMD_ever"] == 1 else "tab:blue"
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
        
        #axes[1].set_title(title_waveform)
        
        #axes[0,1].set_title(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        #axes[0,2].set_title(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        
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
            #axes[1].set_title(title_waveform)
            #axes[1].set_ylim(reconst_ymin - reconst_padding, reconst_ymax + reconst_padding)
            #axes[1].set_xlabel("Normalized time (%)")
            #axes[1].set_ylabel("Movement in Degrees (°)")
            #axes[1].legend(loc='upper right')
            #axes[1].grid(True)
            
        return fig
    
    def plot_top_3_PCAs_reduced(self, orig_data:pd.DataFrame, component_data:dict, title_waveform:str="Mean Waveform", 
                            title_loading:str="Loading Vector", lv_ymax: float|None = None, lv_ymin: float|None = None) -> matplotlib.figure.Figure:
   
        #TODO: adjust scaling for the plots (same PCA -> same scaling!)     
        fig, axes = plt.subplots(2, 3, figsize=(18, 10), constrained_layout=True)
        all_values = []
        all_loading_vectors = []
        for i, rank in enumerate(component_data):
            target = component_data[rank]['target']
            target_clean = target.removesuffix("joint angle").strip()
            axis = component_data[rank]['axis']
            pc_index = component_data[rank]['pc_index']
            rotation_sequence = component_data[rank]['rotation_sequence'].replace("_", " ")
            mean_waveform_pain = component_data[rank]['component_data']['mean_waveform_pain']
            mean_waveform_no_pain = component_data[rank]['component_data']['mean_waveform_no_pain']
            lower_band = component_data[rank]['component_data']['lower_band']
            upper_band = component_data[rank]['component_data']['upper_band']
            #lower_band_pain = component_data['lower_band_pain']
            #upper_band_pain = component_data['upper_band_pain']
            #lower_band_nopain = component_data['lower_band_no_pain']
            #upper_band_nopain = component_data['upper_band_no_pain']
            loading_vector = component_data[rank]['component_data']['loading_vector']
            all_loading_vectors.extend(loading_vector)
                        
            labels_colors = [
                ("Pain", mean_waveform_pain),
                ("No Pain", mean_waveform_no_pain),
                ("Lower Band", lower_band),
                ("Upper Band", upper_band),
            ]  

            df_target_axis = orig_data[(orig_data['target'] == target) & (orig_data['axis'] == axis)]
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
                #color = "tab:red" if row["PRMD_ever"] == 1 else "tab:blue"
                label = 'Pain' if row["PRMD_ever"] == 1 else "No Pain"
                ax.plot(time_points, row[time_cols].values, color=self.get_color_for_label(label), alpha=1.0)
            
            ax = axes[1,i]  
            #ax.plot(loading_vector, label="Loading Vector", color=self.get_color_for_label("loading"))  
            
            #ax = axes[2,i]  

            
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
            #axes[2,i].tick_params(axis='both', which='major', labelsize=14)
        
        
        #lv_ymin = min(all_loading_vectors)
        #lv_ymax = max(all_loading_vectors)
        #reconst_lvrange = lv_ymax - lv_ymin
        #reconst_padding = 0.1 * reconst_lvrange
        
        #axes[1].set_title(title_waveform)
        
        #axes[0,1].set_title(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        #axes[0,2].set_title(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        
        #axes[1,0].set_ylim(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        #axes[1,1].set_ylim(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        #axes[1,2].set_ylim(lv_ymin - reconst_padding, lv_ymax + reconst_padding)
        axes[0,0].set_ylabel("Movement in Degrees (°)", fontsize = 16, labelpad = 10)
        axes[1,0].set_ylabel("Movement in Degrees (°)", fontsize = 16, labelpad = 10)
        axes[1,0].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)
        axes[1,1].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)
        axes[1,2].set_xlabel("Normalized time (%)", fontsize = 16, labelpad = 10)

        
        fig.text(0.04, 0.725, 'Raw \nwaveforms', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        #fig.text(0.04, 0.5, 'PC \nLoading vector', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        fig.text(0.04, 0.29, 'Single PC \nreconstruction', va='center', rotation='horizontal', fontsize=14, fontweight='bold')
        plt.tight_layout(rect=[0.05, 0, 1, 1])    
            #axes[1].set_title(title_waveform)
            #axes[1].set_ylim(reconst_ymin - reconst_padding, reconst_ymax + reconst_padding)
            #axes[1].set_xlabel("Normalized time (%)")
            #axes[1].set_ylabel("Movement in Degrees (°)")
            #axes[1].legend(loc='upper right')
            #axes[1].grid(True)
            
        return fig
    
    def plot_feature_importance(self, lda_results_component):
        stacked_features = lda_results_component['stacked_feature_values'].to_list()
        transposed = list(map(list, zip(*stacked_features[0])))
        #mean_importance = lda_results_component['feature_imp_mean'].to_list()
        #std_importance = lda_results_component['feature_imp_sd'].to_list()
        features = lda_results_component['feature_description'].to_list()[0]

        # Numeric positions for each bar
        #x_pos = range(len(features))

        # Create figure
        fig = plt.figure(figsize=(10, 6))
        plt.boxplot(transposed, labels=features)
        #plt.bar(x_pos, mean_importance, yerr=std_importance, capsize=5)
        plt.xticks(rotation=45, ha='right')
        plt.ylabel('Mean Absolute Coefficient (Importance)')
        plt.title('Feature Importance with LDA Coefficients Across CV Folds')
        plt.tight_layout()

        return fig
    
    #def plot_feature_importance(self, lda_results_component):
    #    mean_importance = lda_results_component['feature_imp_mean'].to_list()
    #    std_importance = lda_results_component['feature_imp_sd'].to_list()
    #    #n_components = lda_results_component['lda_nr_components']
    #    features = lda_results_component['feature_description'].to_list()
    #    # Create list of feature indices (adjust as needed)
    #    #features = list(range(1, n_components.iloc[0] + 1))  
#
    #    # Create figure
    #    fig = plt.figure(figsize=(10, 6))
    #    plt.bar(features, mean_importance, yerr=std_importance, capsize=5)
    #    plt.xticks(features, rotation=45, ha='right')
    #    plt.ylabel('Mean Absolute Coefficient (Importance)')
    #    plt.title('Feature Importance with LDA Coefficients Across CV Folds')
    #    plt.tight_layout()
#
    #    return fig
    #    #plt.show()

        ## Boxplot to show coefficients distribution
        #plt.figure(figsize=(12, 6))
        #sns.boxplot(data=np.abs(coefs), orient='h')
        #plt.yticks(ticks=np.arange(len(features)), labels=features)
        #plt.xlabel('Absolute Coefficient Value')
        #plt.title('Distribution of Feature Importance Across CV Folds')
        #plt.tight_layout()
        #plt.show()
        # Definierte Labels

        
        #for label, data in labels_colors:
        #    linestyle = '-'
        #    if "Lower Band" in label:
        #        linestyle = '--'
        #    elif "Upper Band" in label:
        #        linestyle = ':'
        
        
        #axes = axes.flatten()
            #target_axis_unique = orig_data[['target', 'axis']].drop_duplicates()
            #for i, target, axis in enumerate(target_axis_unique):
            #    df_target_axis = orig_data[(orig_data['target'] == target) & (orig_data['axis'] == axis)]
            #    time_cols = [col for col in df_target_axis.columns if col.startswith("t")]
            #    time_points = [int(c[1:]) for c in time_cols]
            #    ax = axes[0,i]
            #    for _, row in df_target_axis.iterrows():
            #        #color = "tab:red" if row["PRMD_ever"] == 1 else "tab:blue"
            #        ax.plot(time_points, row[time_cols].values, color=self.get_color_for_label(label), alpha=0.3)
            

    #def plot_PCA_reconstruction(self, component_data:dict, title_waveform:str="Mean Waveform", 
    #                            title_loading:str="Loading Vector") -> matplotlib.figure.Figure:
    #    """
    #    Plots the reconstructed mean waveforms with percentile bands and the corresponding 
    #    loading vector of a PCA component.
#
    #    Args:
    #    component_data (dict): Reconstruction output containing mean waveforms, bands, and loading vector.
    #    title_waveform (str): Title for the waveform subplot.
    #    title_loading (str): Title for the loading vector subplot.
#
    #    Returns:
    #        matplotlib.figure.Figure: The figure object containing the plots.
    #    """
    #    
    #    mean_waveform_pain = component_data['mean_waveform_pain']
    #    mean_waveform_no_pain = component_data['mean_waveform_no_pain']
    #    lower_band = component_data['lower_band']
    #    upper_band = component_data['upper_band']
    #    loading_vector = component_data['loading_vector']
    #    
    #            # Definierte Labels
    #    labels_colors = [
    #        ("Pain", mean_waveform_pain),
    #        ("No Pain", mean_waveform_no_pain),
    #        ("Lower Band", lower_band),
    #        ("Upper Band", upper_band)
    #    ]
    #    fig, axs = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True)
    #    
    #    for label, data in labels_colors:
    #        linestyle = '-'
    #        if "Lower Band" in label:
    #            linestyle = '--'
    #        elif "Upper Band" in label:
    #            linestyle = ':'
#
    #        axs[0].plot(data, label=label, linestyle=linestyle,
    #                    color=self.get_color_for_label(label), alpha=0.7 if "Band" in label else 1.0)
#
    #
    #    # Plot mean waveforms
    #    #axs[0].plot(mean_waveform_pain, label="Pain", color="blue")
    #    #axs[0].plot(mean_waveform_no_pain, label="No Pain", color="orange")
    #    #axs[0].plot(lower_band, label="Lower Band", linestyle='--', color="black")
    #    #axs[0].plot(upper_band, label="Upper Band", linestyle=':', color="black")
    #    axs[0].set_title(title_waveform)
    #    axs[0].set_xlabel("Normalized time (%)")
    #    axs[0].set_ylabel("Amplitude (°)")
    #    axs[0].legend(loc='upper right')
    #    axs[0].grid(True)
    #    
    #    # Plot loading vector
    #    #axs[1].plot(loading_vector, color="green")
    #    axs[1].plot(loading_vector, label="Loading Vector",
    #        color=self.get_color_for_label("loading"))
    #    axs[1].set_title(title_loading)
    #    axs[1].set_xlabel("Component Index")
    #    axs[1].set_ylabel("Loading Value")
    #    axs[1].grid(True)
    #    plt.close(fig)
    #    return fig       
    
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
    
    @staticmethod
    def plot_lda_boxlpots(df_cv_results, df_no_cv_results, column_names):
        """"""
        fig, ax = plt.subplots(figsize=(10, 10))
        df_cv_results = df_cv_results.drop(columns=['stacked_feature_values', 'feature_imp_mean', 'feature_imp_sd', 'feature_description'])
        df_cv_exploded = df_cv_results.apply(pd.Series.explode).reset_index(drop=True)
        df_plot = df_cv_exploded.dropna(subset=[column_names[0]])
        # Boxplot of CV results (blue)
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
            y = row[column_names[1]]  # list of values
            x_center = i  # boxplot position (0-indexed)
            plt.hlines(y=y, xmin=x_center - box_width/2, xmax=x_center + box_width/2, colors='#BC4B51', linewidth=2)


        # TODO: adjust titles and x/y labels
        column_label = (
            'Missclassification Error' if 'missclass' in column_names[0] 
            else 'ROC AUC' if 'roc_auc' in column_names[0] 
            else 'Accuracy'
        )
        
        ax.set_xlabel("Number of Principal Components per LDA", fontsize = 30, labelpad = 10)
        ax.set_ylabel(f"{column_label}", fontsize = 30, labelpad = 10)
        #ax.set_title(f"LDA {column_label} with Increasing PCs")
        ax.set_ylim(0.0,1.0)
        ax.tick_params(axis='both', which='major', labelsize=24)
        #plt.xlabel("Number of Principal Components per LDA")
        #plt.ylabel(f"{column_label}")
        #plt.title(f"LDA {column_label} with Increasing PCs")
        plt.legend()
        plt.tight_layout()
        return fig
        #plt.show()
        
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

        # ---- 2️⃣ Project class means into discriminant space ----
        # (lda_class_means are in feature space, so project them using lda_scalings)
        projected_means = np.dot(class_means, scalings)

        # ---- 3️⃣ Plot the KDEs ----
        fig, ax = plt.subplots(figsize=(8, 5))
        unique_classes = df_lda["class"].unique()
        
        class_label_map = {0: "no_pain", 1: "pain"}
        class_colors = {cls: self.COLOR_MAP[class_label_map[cls]] for cls in df_lda["class"].unique()}
        
        kde_handles, kde_labels = ax.get_legend_handles_labels()
        # ---- 2️⃣ Plot KDE without palette dictionary, use colors via hue mapping ----
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
        # ---- 4️⃣ Overlay vertical lines for projected means ----
        #for i, mean_val in class_centers.items():
        #    ax.axvline(mean_val, color="k", linestyle="--", label=f"Mean class {i}")

        # ---- 5️⃣ Label and return ----
        legend_handles = [
            Line2D([0], [0], color=class_colors[cls], lw=3, label=class_label_map[cls])
            for cls in class_centers.keys()
        ] + [
            Line2D([0], [0], color=class_colors[cls], lw=2, linestyle="--", label=f"{class_label_map[cls]} mean")
            for cls in class_centers.keys()
        ]

        ax.legend(handles=legend_handles, loc="best")

        # ---- 6️⃣ Labels and grid ----
        #ax.set_title(
        #    f"LDA Class Separation (LD1)\nLDA components: {row['lda_nr_components']}", fontsize = 16
        #)
        ax.set_xlabel("LD1 (Discriminant Function 1)", fontsize = 16, labelpad = 10)
        ax.set_ylabel("Density", fontsize = 16, labelpad = 10)
        ax.tick_params(axis='both', which='major', labelsize=14)
        ax.grid(True, linestyle="--", alpha=0.4)
        fig.tight_layout()

        return fig
            
    def plot_lda_class_separation(self, df, lda_means=None):
        """
        Visualize LDA separation (LD1 vs LD2) from stored lda_scores.
        """
# ---- 1️⃣ Extract JSON fields from the single row ----
        row = df.iloc[0]

        # Load lda_scores (list of dicts)
        if row["lda_scores"] is None:
            print("⚠️ No LDA scores found in this row.")
            return None

        try:
            lda_scores = row["lda_scores"]
            df_lda = pd.DataFrame(lda_scores)
        except Exception as e:
            print(f"⚠️ Could not parse lda_scores: {e}")
            return None

        # ---- 2️⃣ Basic validation ----
        if "LD1" not in df_lda.columns:
            print("⚠️ LDA scores do not include LD1/LD2 columns.")
            return None

        # ---- 3️⃣ Extract class means if available ----
        lda_means = None
        if "lda_class_means" in row and row["lda_class_means"] is not None:
            try:
                lda_means = (
                    row["lda_class_means"]
                )
            except Exception as e:
                print(f"⚠️ Could not parse lda_class_means: {e}")
                lda_means = None

        # ---- 4️⃣ Create figure and axis ----
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
            # Only one discriminant (e.g. binary LDA)
            df_lda["y_dummy"] = 0  # so we can still visualize LD1 on x-axis
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

        # ---- 5️⃣ Add class means (centroids) ----
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

        # ---- 6️⃣ Add metadata to title ----
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