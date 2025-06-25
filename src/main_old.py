from data_processing.data_preprocess import DataProcessor
from data_processing.pca_analysis import PCAAnalyser

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def main():

    # TODO: reduce to only the original component reconstruction (original data)
    # TODO: check which dataframes and information really need to be saved (get rid of redundancy)
    # TODO: save results to DB
    data_processor = DataProcessor()
    pca_analyser = PCAAnalyser()
    #df = data_processor.load_data_one_joint('right elbow joint angle')
    df = data_processor.load_full_device_data('mocap')
    unique_target_axes = df[["target", "axis"]].drop_duplicates().values.tolist()

    df_combined_strokes, df_mean_key_total = pd.DataFrame(), pd.DataFrame()
    df_combined, df_transformed_total, df_scaled_total,  = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    pcs_final, scaler_list = [], []
    
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
        df_strokes_target_axis = data_processor.combine_half_strokes_to_full_cycles(df_key_normalized)
        df_combined_strokes = pd.concat([df_combined_strokes, df_strokes_target_axis])
        
        df_transformed = data_processor.pivot_full_cycles_to_wide(df_strokes_target_axis, 'value_centered' )
        #df_part, df_pca = df_transformed.iloc[:, :5], df_transformed.iloc[:, -202:]

        # check df for outliers:
        df_outliers_removed, count_outliers = pca_analyser.sliding_window_outlier_detection(df_transformed)
        
        # plot linearity
        #if count_outliers > 0:
            #pca_analyser.plot_linearity(df_outliers_removed)
        df_part, df_pca = df_outliers_removed.iloc[:, :5], df_outliers_removed.iloc[:, -202:]

        # TODO: Standardize data -> compare with unstandardized, to see which fits out analytical goals better!
        scaled_df, scaler =pca_analyser.standardize_df(df_pca)
        scaler_list.append([target, axis, scaler])
        df_scaled_total = pd.concat([df_scaled_total, pd.DataFrame(scaled_df)], axis = 0)
        
        # TODO: use the variances and k to further inform the analysis
        variance_level = 0.9
        explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores = pca_analyser.apply_pca(scaled_df, variance_level)

        # TODO: save the PCs for the most relevant features
        pcs_final.append([target, axis, pca_final])

        # concat the participants and pca dataframes
        df_combined = pd.concat([df_combined, pd.concat([df_part, df_pca_scores], axis=1)], axis = 0)
        df_transformed_total = pd.concat([df_transformed_total, pd.concat([df_transformed, df_pca_scores], axis=1)], axis = 0)

        print(f'{target} {axis} preprocessed and analysed')
        print("Outliers removed: " + str(count_outliers))

    pcs_final = pd.DataFrame(pcs_final, columns = ['target', 'axis', 'PCA'])        
    scaler_df = pd.DataFrame(scaler_list, columns = ['target', 'axis', 'scaler']) 
    
    pcs_ranked = pca_analyser.rank_pcs(unique_target_axes, df_combined)
    #pcs_ranked_orig = pca_analyser.rank_pcs(unique_target_axes, df_combined_orig)
    
    max_pcs = 10
    nr_pcs = len(pcs_ranked) if len(pcs_ranked) < max_pcs else max_pcs
         
    # do the same for the original data
    for i in range(0, nr_pcs):
        current_pc = pcs_ranked.iloc[i]
        target = current_pc['target']
        axis = current_pc['axis']
        pca_scores = np.array(df_transformed_total[(df_transformed_total['target'] == target) & (df_transformed_total['axis'] == axis)][['PC1', 'PC2', 'PC3']])
        df_pca_scores = df_transformed_total[(df_transformed_total['target'] == target) & (df_transformed_total['axis'] == axis)]
        df_mean_key_target_axis = df_mean_key_total[(df_mean_key_total['target'] == target) & (df_mean_key_total['axis'] == axis)]
        scaler = scaler_df[(scaler_df['target'] == target) & (scaler_df['axis'] == axis)]['scaler'].iloc[0]
        # TODO: reconstruct the data from the pc_scores, loading vector and mean note
        
        
        # visualise on original data
        df_target_axis = data_processor.pivot_full_cycles_to_wide(
            df_combined_strokes[(df_combined_strokes['target'] == target) & (df_combined_strokes['axis'] == axis)], 
            'value')
        component_reconstruction_data = pca_analyser.reconstruct_single_component_old(current_pc, pcs_final, df_pca_scores, df_target_axis, scaler) 
        pc_name = current_pc['PC']
        title_reconstruction = f'Single component reconstruction: Rank {i+1}, {target} {axis}; {pc_name}'
        title_loading_vector = f'Loading vector: Rank {i+1} {target} {axis}; {pc_name}'
        fig, axs = pca_analyser.plot_PCA_reconstruction(component_reconstruction_data, title_reconstruction, title_loading_vector)
               
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots"
        rank = i+1
        pca_analyser.save_plot(fig, output_path, f"Scaled_Orig_Rank_{rank}_{target}_{axis}_{current_pc['PC']}")
             
if __name__ == '__main__':
    main()
