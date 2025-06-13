#from data_access.datapoint_repository import DatapointRepository
from data_processing.data_preprocess import DataProcessor
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scipy.stats import ttest_ind
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def main():

    data_processor = DataProcessor()
    df = data_processor.load_data_one_joint('right elbow joint angle')
    pain_columns = ['PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left']
    control_column = 'PRMD_ever'
    df_reduced = data_processor.select_pain_data(df, pain_columns, control_column)
    


    df_sorted = df_reduced.sort_values(by=['participant_id', 'bow_stroke', 'up_down', 'dp_time_point'])    
    # substract meanwave of the played note
    df_key_normalized = data_processor.substract_meanwave_key(df_sorted)
    print("")    

    # combine half-cycles into full-cycle, transform data (each dp_timepoint into one column) 
    df_combined_strokes = data_processor.combine_half_strokes_to_full_cycles(df_key_normalized)
    df_transformed = data_processor.pivot_full_cycles_to_wide(df_combined_strokes, 'value_centered' )
    df_pca = df_transformed.iloc[:, -202:]
    df_part = df_transformed.iloc[:, :3]

    # TODO: Standardize data -> compare with unstandardized, to see which fits out analytical goals better!
    scaler = StandardScaler()
    scaled_df = scaler.fit_transform(df_pca)
    
    # Apply PCA
    # PCA standardized data
    pca_stand = PCA()
    principal_components_stand = pca_stand.fit(scaled_df)
    
    # PCA original data
    pca_orig = PCA()
    principal_components_orig = pca_orig.fit(df_pca)
    
    # examine explained variance 
    # for standardized data
    cumulative_variance_stand = np.cumsum(pca_stand.explained_variance_ratio_)
    k_stand = np.argmax(cumulative_variance_stand >= 0.9) + 1
    # for original data
    cumulative_variance_orig = np.cumsum(pca_orig.explained_variance_ratio_)
    k_orig = np.argmax(cumulative_variance_orig >= 0.9) + 1    
    
    # apply PCA with k components
    # standardized data
    pca_final_stand = PCA(n_components=k_stand)
    pca_reduced_stand = pca_final_stand.fit_transform(scaled_df)
    df_pca_reduced_stand = pd.DataFrame(pca_reduced_stand, columns=[f"PC{int(col)}" for col in range(1,k_stand+1)])    
    # original data
    pca_final_orig = PCA(n_components=k_orig)
    pca_reduced_orig = pca_final_orig.fit_transform(df_pca)
    df_pca_reduced_orig = pd.DataFrame(pca_reduced_orig, columns=[f"PC{int(col)}" for col in range(1,k_orig+1)])    
    
    # concat the two dataframes
    df_combined_stand = pd.concat([df_part, df_pca_reduced_stand], axis=1)
    df_combined_orig = pd.concat([df_part, df_pca_reduced_orig], axis=1)
    print("")
    
    # TODO: t-test
    # standardized
    #t_stat, p_value = ttest_ind(gruppe1, gruppe2, equal_var=False) 
    # calculate mean PC score per participant for t-test, to avoid participants being over-represented
    pc_cols = df_combined_stand.iloc[:, 3:]
    for pc in pc_cols:
        
        df_pc_mean_stand = df_combined_stand.groupby(['participant_id', 'PRMD_ever'])[pc].mean().reset_index()
        df_mean_pain = df_pc_mean_stand[df_pc_mean_stand['PRMD_ever'] == 1][pc]
        df_mean_nopain = df_pc_mean_stand[df_pc_mean_stand['PRMD_ever'] == 0][pc]
        
        t_stat, p_value = ttest_ind(df_mean_pain, df_mean_nopain, equal_var=False)
        print(f"PC: {pc} \t| t-value: {t_stat} \t| p-value: {p_value}")

    
    print("")
    
    
    # original 
    
    
    # TODO: single component reconstruction 
    # standardized
    # calculate the mean waveform for the two groups (pain/no pain)
    df_transformed_orig = data_processor.pivot_full_cycles_to_wide(df_combined_strokes, 'value' )
    df_matrix_pain = np.array(df_transformed_orig[df_transformed_orig['PRMD_ever'] == 1].iloc[:, -202:])
    df_matrix_no_pain = np.array(df_transformed_orig[df_transformed_orig['PRMD_ever'] == 0].iloc[:, -202:])
    df_matrix = np.array(df_transformed_orig.iloc[:, -202:])
    
    
    
    mean_waveform_pain = np.mean(df_matrix_pain, axis=0)
    mean_waveform_no_pain = np.mean(df_matrix_no_pain, axis=0)
    mean_waveform = np.mean(df_matrix, axis=0)
    # select the PC and its loading vector
    pc_idx = 2  # 0 for PC1
    loading_vector = pca_final_stand.components_[pc_idx]  # shape: (n_timepoints,)
    # get the 5th and 95th percentile of PC scores
    pc_scores = pca_reduced_orig[:, pc_idx]
    lower_percentile = np.percentile(pc_scores, 5)
    upper_percentile = np.percentile(pc_scores, 95)
    # scale the loading vector and create reconstructions
    lower_band = mean_waveform + lower_percentile * loading_vector
    upper_band = mean_waveform + upper_percentile * loading_vector
    # plot everything
    plt.figure(figsize=(10, 6))
    plt.plot(mean_waveform_pain, label='Mean Waveform', color='black')
    plt.plot(mean_waveform_no_pain, label='Mean Waveform', color='black')
    plt.plot(mean_waveform, label='Mean Waveform', color='black')
    plt.plot(lower_band, label='Mean + 5th percentile PC1', linestyle='--', color='blue')
    plt.plot(upper_band, label='Mean + 95th percentile PC1', linestyle='--', color='red')
    plt.figure(figsize=(10, 6))
    plt.plot(loading_vector, label='PC1 Loading Vector (scaled)', linestyle=':', color='green')
    plt.xlabel('Timepoint')
    plt.ylabel('Amplitude')
    plt.title('Single Component Reconstruction and PC1 Loading Vector')
    plt.legend()
    plt.show()
    # original 
    
if __name__ == '__main__':
    main()
