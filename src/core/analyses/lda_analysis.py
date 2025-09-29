from core.analyses.abstract_analysis import AbstractAnalyser
from processing.data_loading import DataLoader
from processing.data_plotting import DataPlotter
from processing.data_preprocess import DataProcessor
from processing.assumptions_testing import AssumptionsTester
from data_access.models.lda_results import LDAResults

import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report, roc_auc_score
from sklearn.model_selection import KFold, StratifiedKFold, StratifiedGroupKFold, GroupKFold, LeaveOneOut, LeaveOneGroupOut, cross_validate

from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import SimpleImputer, KNNImputer, IterativeImputer
from sklearn.pipeline import make_pipeline
from itertools import combinations

class LDAAnalyser(AbstractAnalyser):
    def __init__(self, cfg, logger, run_rotated = False):
        super().__init__(cfg, logger)
        #self.n_components = n_components
        self.analysis_name = 'lda' if not run_rotated else 'lda_rotated'
        self.cfg = cfg
        self.run_rotated = run_rotated
        self.data_loader = DataLoader()
        self.data_processor = DataProcessor()
        self.data_plotter = DataPlotter()
        self.assumptions_tester = AssumptionsTester()
        
        self.scaler = StandardScaler()
    
    def run(self, key):
        """"""
        # This should be part of the config
        scaler_type = key['lda_scaler_type']
        imputer_type = key['lda_imputation_type']
        imputer_parameter = key['lda_imputer_parameter']
        scaler = self.get_scaler(scaler_type)
        imputer = self.get_imputer(imputer_type, imputer_parameter)
        validation_type = key['lda_validation_type']
        n_folds = key['lda_splits']
        n_repeats = key['lda_repeats']
        
        pipe = self.create_pipeline(scaler, imputer)
        
        # check if PCA on full dataset is upload for these settings
        full_lda_uploaded = self.data_loader.check_full_lda_uploaded(key, self.run_rotated)
        #gkf = StratifiedGroupKFold(n_splits=n_folds)
        
        prepared_data = self.prepare(key)
        nr_components = key["lda_nr_components"]
        
        all_results = []
        # Loop through subsets of variables (2 up to 10)
        for k in range(2, nr_components + 1):
            #for subset in combinations(range(prepared_data.shape[1]), k):
            
            pc_id_columns = prepared_data.iloc[:, 3:3+k].columns
            pc_ids = [int(column_name.split("pc")[-1]) for column_name in pc_id_columns]
            subset = prepared_data.iloc[:, :3+k]
            
            if imputer == None:
                clean_data = self.remove_missing_values(subset)
                pc_scores_clean = np.array(clean_data.iloc[:, 3:3+k])
                PRMD_clean = np.array(clean_data['PRMD_ever'])
                part_ids_clean = np.array(clean_data['participant_id'])
            else:
                pc_scores_clean = np.array(subset.iloc[:, 3:3+k])
                PRMD_clean = np.array(subset['PRMD_ever'])
                part_ids_clean = np.array(subset['participant_id'])
            
            
            # Store *all* scores across repetitions
            acc_values_all = []
            roc_auc_values_all = []

            for rep in range(n_repeats):
                #gkf = StratifiedGroupKFold(n_splits=n_folds, shuffle=True, random_state=rep)
                cvf = self.get_cross_validater(validation_type, n_folds, rep)
                scores = cross_validate(pipe, pc_scores_clean, PRMD_clean, cv=cvf, groups=part_ids_clean, 
                                        scoring=["accuracy", "roc_auc"], error_score="raise")

                acc_values_all.extend(scores['test_accuracy'])
                roc_auc_values_all.extend(scores['test_roc_auc'])

            acc_values_all = np.array(acc_values_all)
            roc_auc_values_all = np.array(roc_auc_values_all)
            error_values_all = 1 - acc_values_all


            result = LDAResults(
                id=0,
                pc_ids=pc_ids,
                nr_components=k,
                acc_values=acc_values_all.tolist(),  # keep raw values
                acc_mean=np.nanmean(acc_values_all),
                acc_sd=np.nanstd(acc_values_all),
                missclass_err_values=error_values_all.tolist(),
                missclass_err_mean=np.nanmean(error_values_all),
                missclass_err_sd=np.nanstd(error_values_all),
                roc_auc_values=roc_auc_values_all.tolist(),
                roc_auc_mean=np.nanmean(roc_auc_values_all),
                roc_auc_sd=np.nanstd(roc_auc_values_all),
                validation_type= validation_type,
                scaler_type=scaler_type,
                imputation_type=imputer_type,
                n_folds=n_folds,
                n_repeats=n_repeats
            )

            all_results.append(result)
            
            if not full_lda_uploaded:
                # ---- Full dataset performance (fit & predict on all data) ----
                pipe.fit(pc_scores_clean, PRMD_clean)
                y_pred_full = pipe.predict(pc_scores_clean)

                acc_full = accuracy_score(PRMD_clean, y_pred_full)
                err_full = 1 - acc_full
                try:
                    roc_auc_full = roc_auc_score(PRMD_clean, pipe.predict_proba(pc_scores_clean)[:, 1])
                except ValueError:
                    roc_auc_full = float("nan")
                    
                result = LDAResults(
                    id=0,
                    pc_ids=pc_ids,
                    nr_components=k,
                    acc_values=None,  # keep raw values
                    acc_mean=np.nanmean(acc_full),
                    acc_sd=None,
                    missclass_err_values=None,
                    missclass_err_mean=np.nanmean(err_full),
                    missclass_err_sd=None,
                    roc_auc_values=None,
                    roc_auc_mean=np.nanmean(roc_auc_full),
                    roc_auc_sd=None,
                    validation_type= 'no_validation',
                    scaler_type=scaler_type,
                    imputation_type=imputer_type,
                    n_folds=None,
                    n_repeats=None
                )

                all_results.append(result)            
            
            #pc_scores_clean, PRMD_clean, part_ids_clean = self.remove_missing_values(pc_scores_sub, df_PRMD, df_part_ids)
            #scores = cross_val_score(lda, pc_scores_clean, PRMD_clean, cv=gkf, groups=part_ids_clean, scoring="accuracy", error_score="raise")
            #scores = cross_validate(pipe, pc_scores_clean, PRMD_clean, cv=gkf, groups=part_ids_clean, scoring=["accuracy", "roc_auc"], error_score="raise")
            #print("")
            #accuracy_folds = scores['test_accuracy']
            #error_folds = 1 - accuracy_folds 
            #result = LDAResults(
            #    id = 0,
            #    pc_ids = pc_ids,
            #    nr_components = k,
            #    acc_values = accuracy_folds,
            #    acc_mean = np.nanmean(accuracy_folds),
            #    acc_sd = np.nanstd(accuracy_folds),
            #    missclass_err_values = error_folds,
            #    missclass_err_mean = np.nanmean(error_folds),
            #    missclass_err_sd =  np.nanstd(error_folds),   
            #    roc_auc_values =  scores['test_roc_auc'],
            #    roc_auc_mean = np.nanmean(scores['test_roc_auc']),
            #    roc_auc_sd = np.nanstd(scores['test_roc_auc']),
            #    scaler_type = scaler_type,
            #    imputation_type = imputer_type,
            #    n_folds = n_folds
            #)
            #accuracy_folds = scores['test_accuracy']
            #result['nr_variables (k)'] = k
            #result["Accuracy_Mean"] = np.nanmean(accuracy_folds)
            #result["Accuracy_SD"] = np.nanstd(accuracy_folds)
            #error_folds = 1 - accuracy_folds 
            #result["Missclass_Err_Mean"] = np.nanmean(error_folds)
            #result["Missclass_Err_SD"] = np.nanstd(error_folds) 
            #result['ROC_AUC_Mean'] = np.nanmean(scores['test_roc_auc'])
            #result['ROC_AUC_SD'] = np.nanstd(scores['test_roc_auc'])
            #all_results.append(result)

        # Sort results by mean accuracy
        #df_results = pd.DataFrame(all_results)
        return all_results
        #sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)

    @staticmethod
    def create_pipeline(scaler, imputer):
        steps = []
        
        if imputer is not None:
            steps.append(('imputer', imputer))
        
        if scaler is not None:
            steps.append(('scaler', scaler))
        
        steps.append(('lda', LinearDiscriminantAnalysis()))
        
        pipe = make_pipeline(*[step[1] for step in steps])
        return pipe

    @staticmethod
    def get_cross_validater(cv_type, n_fold, repitions):
        scalers = {'k_fold': KFold(n_splits=n_fold, shuffle=True, random_state=repitions),
                'strat_k_fold' : StratifiedKFold(n_splits=n_fold, shuffle=True, random_state=repitions), 
                'group_k_fold' : GroupKFold(n_splits=n_fold),
                'strat_group_k_fold': StratifiedGroupKFold(n_splits=n_fold, shuffle=True, random_state=repitions),
                'leave_one_out' : LeaveOneOut(),
                'leave_one_group_out' : LeaveOneGroupOut(),
                'Default': StratifiedGroupKFold(n_splits=n_fold, shuffle=True, random_state=repitions)
                }
        return scalers[cv_type]

    @staticmethod
    def get_scaler(scaler_type):
        scalers = {'standard_scaler': StandardScaler(),
                   'minmax_scaler' : MinMaxScaler(), 
                   'None': None
                   }
        
        return scalers[scaler_type]
    @staticmethod
    def get_imputer(imputer_type, imputer_parameter):
        imputer = {'simple_imputer': SimpleImputer(strategy = imputer_parameter),
                   'knn_imputer' : KNNImputer(n_neighbors = imputer_parameter),
                   'iterative_imputer' : IterativeImputer(random_state=imputer_parameter), 
                   'None': None
                   }
        
        return imputer[imputer_type]
    
    def prepare(self, key):
        """"""
        if self.run_rotated:
            prepared_data = self.prepare_rotated(key)
        else:
            prepared_data = self.prepare_unrotated(key)
        return prepared_data
    
    def prepare_unrotated(self, key):
        """"""
        # TODO: load data (top X pcs)
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        pain_groups = key['pain_groups']
        nr_components = key["lda_nr_components"]
        #scaler_type = key['scaler_type']
        #rotation_type = key['rotation_method']
        t_test_assumptions_relevant = key['t_test_assumptions_relevant']
        t_test_distribution_type = key['t_test_distribution_type']
        
        ranked_pc_scores_df = self.data_loader.load_pcs_by_rank(exp_id, device,measurement_tp,pain_groups, 
                                                                nr_components= nr_components,select_rotated= self.run_rotated, 
                                                                use_distribution= t_test_assumptions_relevant, distribution_type = t_test_distribution_type)
        df_reduced = ranked_pc_scores_df[['participant_id', 'target', 'axis', 'pc_id', 'PRMD_ever', 'full_stroke', 'pc_score']]
        
        pc_ids_unique = df_reduced['pc_id'].unique()
        rank_map = {int(rank+1): pc_id for rank, pc_id in enumerate(pc_ids_unique)}
        
        for id, pc_id in enumerate(df_reduced['pc_id'].unique()):
            rank = int(id+1)
            df_reduced.loc[df_reduced['pc_id'] == pc_id, 'rank'] = rank
            
           
        df_wide = df_reduced.pivot(index=['participant_id', 'full_stroke', 'PRMD_ever'], 
                columns='rank', 
                values='pc_score')
        df_wide = df_wide.rename(columns=lambda x: f'pc_score_rank{x}_pc{rank_map[x]}').reset_index()
        #df_wide = df_wide.rename(columns=lambda x: f'pc_score_rank_{x}').reset_index()
        
        #df_part_ids = df_wide['participant_id']
        #df_PRMD = df_wide['PRMD_ever']
        #df_pc_scores = df_wide.iloc[:, -nr_components:]
        #result = {
        #    "df_reduced": df_reduced,
        #    "df_part_ids": df_part_ids, 
        #    "df_PRMD": df_PRMD,
        #    "df_pc_scores":df_pc_scores
        #}
        #print("")
        return df_wide
    
    @staticmethod
    def remove_missing_values(data):
        mask = ~np.isnan(data).any(axis=1)
        data_clean = data[mask]
        #y_clean = y[mask]
        #groups_clean = groups[mask]
        
        return data_clean
    
    def handle_results(self, results, entry):
        """"""
        self._upload_step(entry = entry, analysis_name=self.analysis_name , uploads=[(self.upload_lda_analysis, results)])
    
    def upload_lda_analysis(self, lda_results):
        """
        Upload PCA analysis results including components and scores to the database.

        Args:
            pca_info (dict): Dictionary containing PCA results per target-axis.
        """
        for lda in lda_results:
            """"""
            
            lda_id = self.data_loader.upload_lda_analysis(lda)
            pc_ids = lda.pc_ids
            self.data_loader.upload_lda_pcs(lda_id, pc_ids)

    def reconstruct_results(self, key):
        """"""
        exp_id = key['exp_id']
        device = key['device']
        measurement_tp = key['measurement_tp']
        pain_groups = key['pain_groups']
        #nr_components = key['lda_nr_components']
        pca_scaled = True if key['pca_scaler_type'] else False
        rotation_type = key['rotation_method']
        imputation_type = key['lda_imputation_type']
        lda_scaler_type = key['lda_scaler_type']
        lda_validation_type = key['lda_validation_type']
        
        lda_results_cv = self.data_loader.load_lda(exp_id, device, measurement_tp, pain_groups, pca_scaled, self.run_rotated, 
                                                rotation_type, imputation_type, lda_scaler_type, lda_validation_type)  
        lda_results_full = self.data_loader.load_lda(exp_id, device, measurement_tp, pain_groups, pca_scaled, self.run_rotated, 
                                        rotation_type, imputation_type, lda_scaler_type, 'no_validation') 
        
        self.plot_lda_results(lda_results_cv, lda_results_full)      
        
    def plot_lda_results(self, lda_results_cv, lda_results_full):
        column_names = ['missclass_err_values', 'missclass_err_mean']
        fig = self.data_plotter.plot_lda_boxlpots(lda_results_cv, lda_results_full, column_names)   
    
        #distribution_label = f"{t_test_distribution_type}" if t_test_assumptions_relevant else "no_distribution_tested"
        rotation_label = lda_results_cv['pca_rotation_type'].unique()[0]
        pain_group_names = lda_results_cv['pain_groups'].unique()[0]
        lda_validation_label = lda_results_cv['lda_validation_type'].unique()[0]
        lda_imputation_label = lda_results_cv['lda_imputation_type'].unique()[0]
        measurement_tp = lda_results_cv['meas_time_point'].unique()[0]
        
        current_path = Path.cwd()
        output_path = current_path / "output" / "plots" / "LDA_results" / f"{pain_group_names}" / f"{rotation_label}" / f"{lda_validation_label}" / f"{lda_imputation_label}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        fig_name = f"{measurement_tp}_LDA_Results_Missclassification_Error"
        self.data_plotter.save_plot(fig, output_path, fig_name)
        
        column_names = ['roc_auc_values', 'roc_auc_mean']
        fig = self.data_plotter.plot_lda_boxlpots(lda_results_cv, lda_results_full, column_names) 
        
        fig_name = f"{measurement_tp}_LDA_Results_ROC_AUC"
        self.data_plotter.save_plot(fig, output_path, fig_name)        
    
    
        column_names = ['acc_values', 'acc_mean']
        fig = self.data_plotter.plot_lda_boxlpots(lda_results_cv, lda_results_full, column_names) 
        
        fig_name = f"{measurement_tp}_LDA_Results_Accuracy"
        self.data_plotter.save_plot(fig, output_path, fig_name)     
    #def preprocess(self, X_train, X_test):
    #    """Fit scaler on training and transform both train and test data."""
    #    self.scaler.fit(X_train)
    #    X_train_scaled = self.scaler.transform(X_train)
    #    X_test_scaled = self.scaler.transform(X_test)
    #    return X_train_scaled, X_test_scaled
    #
    #def fit(self, X_train, y_train):
    #    """Fit LDA model on training data."""
    #    self.lda.fit(X_train, y_train)
    #
    #def transform(self, X):
    #    """Project data onto LDA components."""
    #    return self.lda.transform(X)
    #
    #def predict(self, X):
    #    """Predict class labels for given data."""
    #    return self.lda.predict(X)
    #
    #def evaluate(self, y_true, y_pred):
    #    """Provide accuracy, confusion matrix, and classification report."""
    #    acc = accuracy_score(y_true, y_pred)
    #    cm = confusion_matrix(y_true, y_pred)
    #    cr = classification_report(y_true, y_pred)
    #    return {'accuracy': acc, 'confusion_matrix': cm, 'classification_report': cr}
    #
    #def full_fit_predict_evaluate(self, X_train, X_test, y_train, y_test):
    #    """Complete pipeline: preprocess, fit, predict, evaluate."""
    #    X_train_scaled, X_test_scaled = self.preprocess(X_train, X_test)
    #    self.fit(X_train_scaled, y_train)
    #    y_pred = self.predict(X_test_scaled)
    #    return self.evaluate(y_test, y_pred)
## Example data
#X, y = load_iris(return_X_y=True)
#
## The maximum number of LDA components is min(n_classes - 1, n_features)
#max_k = min(len(np.unique(y)) - 1, X.shape[1])
#
## Loop over desired k values (1 to max_k)
#for k in range(1, max_k + 1):
#    lda = LinearDiscriminantAnalysis(n_components=k)
#    X_lda = lda.fit_transform(X, y)
#    print(f"LDA with {k} component(s): Transformed shape = {X_lda.shape}")
#    # You can now use X_lda for further analysis or classification