from core.run_pca import PCARunner
from core.run_general_analysis import GeneralAnalysisRunner
def main():
    pca_analysis_completed = False
    pca_uploads_completed = []
    #pca_uploads_completed = [(1,'pre','mocap')]
    #pca_uploads_completed = [(1,'pre','mocap'), (1,'post','mocap')]
    
    # config variables
    exp_id = 1
    measurement_tp = 'pre'
    device = 'mocap'
    rotation_method = 'varimax'
    pain_groups = ["healthy", "shoulder_neck"]
    
    pca_runner = PCARunner()
    general_analysis_runner = GeneralAnalysisRunner()
    
    #general_analysis_runner.create_plots_key_per_group(device, exp_id, measurement_tp, pain_groups)
    #general_analysis_runner.create_plots_mean_std_keys(device, exp_id, measurement_tp, pain_groups)
    general_analysis_runner.create_plots_mean_std(device, exp_id, measurement_tp, pain_groups, True)
    
    if ((exp_id, measurement_tp, device) not in pca_uploads_completed):
        pca_results = pca_runner.run_pca_analysis(exp_id, measurement_tp, device, pain_groups, check_requirements=True)
        pca_runner.upload_pca_analysis(pca_results) 
        pc_distribution_results = pca_runner.check_distribution(exp_id, device, measurement_tp, distributions_plotted= True)
        pca_runner.upload_distribution(pc_distribution_results)
        rotation_results = pca_runner.run_pca_rotation(exp_id, device, measurement_tp, pain_groups, method = rotation_method)
        pca_runner.upload_pca_analysis(rotation_results)  
        pc_distribution_results = pca_runner.check_distribution(exp_id, device, measurement_tp, distributions_plotted= True)
        pca_runner.upload_distribution(pc_distribution_results)      
    
        t_test_results = pca_runner.conduct_t_test(exp_id, device, measurement_tp)
        pca_runner.upload_t_test(t_test_results) 
    
    nr_components = None
    pca_runner.reconstruct_pcas(exp_id, device, measurement_tp, pain_groups, 'standard_scaler', nr_components)
    pca_runner.reconstruct_pcas(exp_id, device, measurement_tp, pain_groups, 'standard_scaler', nr_components, select_rotated=True)
    print("")
             
if __name__ == '__main__':
    main()