from data_processing.data_preprocess import DataProcessor
from data_processing.pca_analysis import PCAAnalyser
from db.setup import *

from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

def main():
    data_processor = DataProcessor()
    pca_analyser = PCAAnalyser()
    # setup the db adjustments
    samples_setup = True
    if not samples_setup:
        adjusted_db_setup()
        #data_processor.create_samples()
        
    # load the data (by target/axis)
    measurement_tp = 'pre'
    device = 'mocap'
    existing_target_axes = data_processor.get_existing_target_axis_MPA_Clean(device)
    
    existing_targets_analysed = True
    if not existing_targets_analysed:
        for target, axis in existing_target_axes:
            df = data_processor.load_MPA_clean_data_by_device_tp_target_axis(device, measurement_tp, target, axis)
            print(f"{target} {axis} loaded")
        # select the correct participants, based on their pain location
            pain_columns = ['PRMD_shoulder_neck_right', 'PRMD_shoulder_neck_left']
            control_column = 'PRMD_ever'
            df_pain = data_processor.select_pain_data(df, pain_columns, control_column)
            df_reduced = df_pain[[
                'participant_id', 'ext_participant_id', 'PRMD_shoulder_neck_right','PRMD_shoulder_neck_left','PRMD_ever',
                'measurement_id', 'target', 'axis', 'sample_id', 'bow_stroke', 'up_down', 'key', 'dp_time_point',
                'value'
            ]]
            df_sorted = df_reduced.sort_values(by=['participant_id', 'bow_stroke', 'up_down', 'dp_time_point'])    

        # subtract the key mean-waveform from each sample
        # TODO: save mean key per target/axis? / plot mean key? 
            df_key_normalized, df_mean_key_waveform_target_axis = data_processor.subtract_meanwave_key(df_sorted)

        # transform the data (columns for each timepoint)
            df_transformed = data_processor.pivot_full_cycles_to_wide(df_key_normalized, 'value_centered' )

        # check data for outliers
            
            #TODO: save which dp are outliers
            df_outliers_removed, count_outliers = pca_analyser.sliding_window_outlier_detection(df_transformed)
            print(f"{count_outliers} outliers removed")
        # check data linearity and save plots
            fig_title = f"{target}, {axis}"
            fig1, fig2 = pca_analyser.plot_linearity(df_outliers_removed, fig_title)
            current_path = Path.cwd()
            output_path = current_path / "output" / "plots"
            output_path = current_path / "output" / "plots" / "Linearity_plots"
            pca_analyser.save_plot(fig1, output_path, f"Scatter_matrix_{target}_{axis}")
            pca_analyser.save_plot(fig2, output_path, f"Lag_plot_{target}_{axis}")
        # standardize data
            # TODO: save scalers to db
            df_part, df_pca = df_outliers_removed.iloc[:, :-202], df_outliers_removed.iloc[:, -202:]
            scaled_df, scaler =pca_analyser.standardize_df(df_pca)

            mean = list(scaler.mean_)
            scale = list(scaler.scale_)
            measurement_type_id = int(df['measurement_type_id'].unique()[0])
            scaler_type = "standard_scaler"
            pca_analyser.upload_scaler(measurement_type_id, scaler_type, mean, scale)
            data_scaled = True
        # PCA analysis
            variance_level = 0.9
            explained_variance, cumulative_variance, k, pca_final, pca_scores, df_pca_scores = pca_analyser.apply_pca(scaled_df, variance_level)
            for component_idx in range(k):
                pc_index = component_idx + 1
                loading_vector = list(pca_final.components_[component_idx])
                explained_variance_pc = float(explained_variance[component_idx])
                pc_id = pca_analyser.upload_pca(measurement_type_id, pc_index, loading_vector, explained_variance_pc, data_scaled)
                sample_scores = list(zip(df_part['sample_id'], df_pca_scores.iloc[:,component_idx]))
                pca_analyser.upload_pc_scores(pc_id, sample_scores)
            #print("")
            print(f"{target} {axis} analysed and saved")
    # TODO: rank PCs and save results to DB
    # select the PCs and PCscores, as well as participant information (pain/no pain)
    pca_df = pca_analyser.load_pc_data(1, device)
    t_test_results = pca_analyser.rank_pcs(pca_df)
    #df for t_test braucht: target, axis, participant id, pc_scores, PRMD_ever
    print("")
    # TODO: reconstruct & plot top PCs
    # TODO: LDA
    

             
if __name__ == '__main__':
    main()
