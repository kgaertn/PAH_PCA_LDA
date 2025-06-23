#from data_access.datapoint_repository import DatapointRepository
from data_processing.data_preprocess import DataProcessor
from data_processing.pca_analysis import PCAAnalyser

#from sklearn.decomposition import PCA
#from scipy.stats import ttest_ind
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def main():

    # TODO: laden optimieren -> "DB Views"
    data_processor = DataProcessor()
    pca_analyser = PCAAnalyser()
    #df = data_processor.load_data_one_joint('right elbow joint angle')
    df = data_processor.load_full_device_data('mocap')
    unique_target_axes = df[["target", "axis"]].drop_duplicates().values.tolist()
    pcs_final_stand = []
    df_combined_stand = pd.DataFrame()
    df_combined_strokes = pd.DataFrame()
    df_mean_key_total = pd.DataFrame()
    df_transformed_total = pd.DataFrame()
    df_scaled_total = pd.DataFrame()
    scaler_list = []
    for target, axis in unique_target_axes:
        df_target_axis = df[(df['target'] == target) & (df['axis'] == axis)]
        pain_columns = ['PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left']
        control_column = 'PRMD_ever'
        df_reduced = data_processor.select_pain_data(df_target_axis, pain_columns, control_column)

        df_sorted = df_reduced.sort_values(by=['participant_id', 'bow_stroke', 'up_down', 'dp_time_point'])    
        # substract meanwave of the played note
        df_key_normalized, df_mean_key_waveform_target_axis = data_processor.subtract_meanwave_key(df_sorted)
        df_mean_key_waveform_target_axis.insert(0, 'axis', axis)
        df_mean_key_waveform_target_axis.insert(0, 'target', target)
        df_mean_key_total = pd.concat([df_mean_key_total, df_mean_key_waveform_target_axis])
        
        # combine half-cycles into full-cycle, transform data (each dp_timepoint into one column) 
        df_combined_strokes_target_axis = data_processor.combine_half_strokes_to_full_cycles(df_key_normalized)
        df_transformed = data_processor.pivot_full_cycles_to_wide(df_combined_strokes_target_axis, 'value_centered' )
        df_pca = df_transformed.iloc[:, -202:]

        df_part = df_transformed.iloc[:, :5]
        df_combined_strokes = pd.concat([df_combined_strokes, df_combined_strokes_target_axis], axis = 0)
        # TODO: Standardize data -> compare with unstandardized, to see which fits out analytical goals better!
        scaled_df, scaler =pca_analyser.standardize_df(df_pca)
        scaler_list.append([target, axis, scaler])
        df_scaled_total = pd.concat([df_scaled_total, pd.DataFrame(scaled_df)], axis = 0)
        
        # Apply PCA
        # standaradized data
        variance_level = 0.9
        cumulative_variance_stand, k_stand, pca_final_stand, pca_scores_stand, df_pca_scores_stand = pca_analyser.apply_pca(scaled_df, variance_level)

        # TODO: save the PCs for the most relevant features
        pcs_final_stand.append([target, axis, pca_final_stand])

        # concat the participants and pca dataframes
        df_part_pca_combined_stand = pd.concat([df_part, df_pca_scores_stand], axis=1)
        # concat the transformed data and the PC scores
        df_transformed_pca = pd.concat([df_transformed, df_pca_scores_stand], axis = 1)
        df_transformed_total = pd.concat([df_transformed_total, df_transformed_pca], axis = 0)
        
        
        df_combined_stand = pd.concat([df_combined_stand, df_part_pca_combined_stand], axis=0)
        print(f'{target} {axis} preprocessed and analysed')

    pcs_final_stand = pd.DataFrame(pcs_final_stand, columns = ['target', 'axis', 'PCA'])        
    scaler_df = pd.DataFrame(scaler_list, columns = ['target', 'axis', 'scaler']) 
    #pcs_final_orig = pd.DataFrame(pcs_final_orig, columns = ['target', 'axis', 'PCA'])    

    
    # rank pcs
    
    pcs_ranked_stand = pca_analyser.rank_pcs(unique_target_axes, df_combined_stand)
    #pcs_ranked_orig = pca_analyser.rank_pcs(unique_target_axes, df_combined_orig)
    
    max_pcs = 10
    nr_pcs = len(pcs_ranked_stand) if len(pcs_ranked_stand) < max_pcs else max_pcs
    
    # calculate mean waveform per pc (standardized)
    for i in range(0, nr_pcs):
        current_pc = pcs_ranked_stand.iloc[i]
        target = current_pc['target']
        axis = current_pc['axis']
        pca_scores_stand = np.array(df_combined_stand[(df_combined_stand['target'] == target) & (df_combined_stand['axis'] == axis)][['PC1', 'PC2', 'PC3']])
        df_pca_scores_stand = df_combined_stand[(df_combined_stand['target'] == target) & (df_combined_stand['axis'] == axis)]
        df_mean_key_target_axis = df_mean_key_total[(df_mean_key_total['target'] == target) & (df_mean_key_total['axis'] == axis)]
        scaler = scaler_df[(scaler_df['target'] == target) & (scaler_df['axis'] == axis)]['scaler'].iloc[0]
        # TODO: reconstruct the data from the pc_scores, loading vector and mean note
        
        
        # TODO: instead of using the original data, multiply the pc scores with the loading vector, then add the mean waveform of the note
        #df_target_axis = data_processor.pivot_full_cycles_to_wide(
        #    df_combined_strokes[(df_combined_strokes['target'] == target) & (df_combined_strokes['axis'] == axis)], 
        #    'value')
         
        component_reconstruction_data_stand = pca_analyser.reconstruct_single_component(current_pc, pcs_final_stand, 
                                                                                        df_pca_scores_stand, df_target_axis, scaler, df_mean_key_target_axis)
        pc_name = current_pc['PC']
        title_reconstruction = f'Single component reconstruction: Rank {i+1}, {target} {axis}; {pc_name}'
        title_loading_vector = f'Loading vector: Rank {i+1} {target} {axis}; {pc_name}'
        fig_stand, axs_stand = pca_analyser.plot_PCA_reconstruction(component_reconstruction_data_stand, title_reconstruction, title_loading_vector)
               
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots"
        rank = i+1
        pca_analyser.save_plot(fig_stand, output_path, f"Scores_Loading_Rank_{rank}_{target}_{axis}_{current_pc['PC']}_stand")
        
    # calculate mean waveform per pc (standardized)
    for i in range(0, nr_pcs):
        current_pc = pcs_ranked_stand.iloc[i]
        target = current_pc['target']
        axis = current_pc['axis']
        pca_scores_stand = np.array(df_transformed_total[(df_transformed_total['target'] == target) & (df_transformed_total['axis'] == axis)][['PC1', 'PC2', 'PC3']])
        df_pca_scores_stand = df_transformed_total[(df_transformed_total['target'] == target) & (df_transformed_total['axis'] == axis)]
        df_mean_key_target_axis = df_mean_key_total[(df_mean_key_total['target'] == target) & (df_mean_key_total['axis'] == axis)]
        scaler = scaler_df[(scaler_df['target'] == target) & (scaler_df['axis'] == axis)]['scaler'].iloc[0]
        # TODO: reconstruct the data from the pc_scores, loading vector and mean note
        
        
        # visualise on original data
        df_target_axis = df_transformed_total[(df_transformed_total['target'] == target) & (df_transformed_total['axis'] == axis)]
         
        component_reconstruction_data_stand = pca_analyser.reconstruct_single_component_old(current_pc, pcs_final_stand, df_pca_scores_stand, df_target_axis, scaler) 
        pc_name = current_pc['PC']
        title_reconstruction = f'Single component reconstruction: Rank {i+1}, {target} {axis}; {pc_name}'
        title_loading_vector = f'Loading vector: Rank {i+1} {target} {axis}; {pc_name}'
        fig_stand, axs_stand = pca_analyser.plot_PCA_reconstruction(component_reconstruction_data_stand, title_reconstruction, title_loading_vector)
               
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots"
        rank = i+1
        pca_analyser.save_plot(fig_stand, output_path, f"Scaled_Key_Norm_Rank_{rank}_{target}_{axis}_{current_pc['PC']}_stand")
     
    # do the same for the original data
    for i in range(0, nr_pcs):
        current_pc = pcs_ranked_stand.iloc[i]
        target = current_pc['target']
        axis = current_pc['axis']
        pca_scores_stand = np.array(df_transformed_total[(df_transformed_total['target'] == target) & (df_transformed_total['axis'] == axis)][['PC1', 'PC2', 'PC3']])
        df_pca_scores_stand = df_transformed_total[(df_transformed_total['target'] == target) & (df_transformed_total['axis'] == axis)]
        df_mean_key_target_axis = df_mean_key_total[(df_mean_key_total['target'] == target) & (df_mean_key_total['axis'] == axis)]
        scaler = scaler_df[(scaler_df['target'] == target) & (scaler_df['axis'] == axis)]['scaler'].iloc[0]
        # TODO: reconstruct the data from the pc_scores, loading vector and mean note
        
        
        # visualise on original data
        df_target_axis = data_processor.pivot_full_cycles_to_wide(
            df_combined_strokes[(df_combined_strokes['target'] == target) & (df_combined_strokes['axis'] == axis)], 
            'value')
        component_reconstruction_data_stand = pca_analyser.reconstruct_single_component_old(current_pc, pcs_final_stand, df_pca_scores_stand, df_target_axis, scaler) 
        pc_name = current_pc['PC']
        title_reconstruction = f'Single component reconstruction: Rank {i+1}, {target} {axis}; {pc_name}'
        title_loading_vector = f'Loading vector: Rank {i+1} {target} {axis}; {pc_name}'
        fig_stand, axs_stand = pca_analyser.plot_PCA_reconstruction(component_reconstruction_data_stand, title_reconstruction, title_loading_vector)
               
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots"
        rank = i+1
        pca_analyser.save_plot(fig_stand, output_path, f"Scaled_Orig_Rank_{rank}_{target}_{axis}_{current_pc['PC']}_stand")
         
    # do the same for the original data
    # calculate mean waveform per pc (original)
    #pcs_ranked_orig = pca_analyser.rank_pcs(unique_target_axes, df_combined_orig)
    #
    #max_pcs = 10
    #nr_pcs = len(pcs_ranked_orig) if len(pcs_ranked_orig) < max_pcs else max_pcs
    
    #for i in range(0, nr_pcs):
    #    current_pc = pcs_ranked_orig.iloc[i]
    #    target = current_pc['target']
    #    axis = current_pc['axis']
    #    pca_reduced_orig= np.array(df_combined_stand[(df_combined_stand['target'] == target) & (df_combined_stand['axis'] == axis)][['PC1', 'PC2', 'PC3']])
    #    df_target_axis = data_processor.pivot_full_cycles_to_wide(
    #        df_combined_strokes[(df_combined_strokes['target'] == target) & (df_combined_strokes['axis'] == axis)], 
    #        'value')
#
    #    component_reconstruction_data_orig = pca_analyser.reconstruct_single_component(current_pc, pcs_final_orig, pca_reduced_orig, df_target_axis)
    #    pc_name = current_pc['PC']
    #    rank = i+1
    #    title_reconstruction = f'Single component reconstruction: Rank {rank}, {target}; {pc_name}'
    #    title_loading_vector = f'Loading vector: Rank {i+1} {target}; {pc_name}'
    #    fig_orig, axs_orig = pca_analyser.plot_PCA_reconstruction(component_reconstruction_data_orig, rank)
#
    #    current_path = Path.cwd()
    #    output_path = current_path / "output" / "plots"
    #    pca_analyser.save_plot(fig_orig, output_path, f"{target}_{axis}_{current_pc['PC']}_orig")
#
    #pcs_ranked_stand = pca_analyser.rank_pcs(unique_target_axes, df_combined_stand)
    ##pcs_ranked_orig = pca_analyser.rank_pcs(unique_target_axes, df_combined_orig)
    #
    #max_pcs = 10
    #nr_pcs = len(pcs_ranked_stand) if len(pcs_ranked_stand) < max_pcs else max_pcs
    #
    ## calculate mean waveform per pc (standardized)
    #for i in range(0, nr_pcs):
    #    current_pc = pcs_ranked_stand.iloc[i]
    #    target = current_pc['target']
    #    axis = current_pc['axis']
    #    df_target_axis = df_transformed[(df_transformed['target'] == target) & (df_transformed['axis'] == axis)]
#
    #    component_reconstruction_data_stand = pca_analyser.reconstruct_single_component(current_pc, pcs_final_stand, pca_scores_stand, df_target_axis)
#
    #    fig_stand, axs_stand = pca_analyser.plot_PCA_reconstruction(component_reconstruction_data_stand)
    #           
    #    current_path = Path.cwd()
    #    output_path = current_path / "output" / "plots"
    #    pca_analyser.save_plot(fig_stand, output_path, f"{target}_{axis}_{current_pc['PC']}_norm_stand")  

    
if __name__ == '__main__':
    main()
