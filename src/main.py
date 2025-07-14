#from core.setup_analyse_pca import SetupAnalyserPCA
#from core.setup_and_upload import SetupUploader
#from core.reconstruct_pca import ReconstructerPCA
from core.run_pca import PCARunner
from core.run_general_analysis import GeneralAnalysisRunner
def main():
    pca_analysis_completed = False
    pca_uploads_completed = []
    pca_uploads_completed = [(1,'pre','mocap')]
    pca_uploads_completed = [(1,'pre','mocap'), (1,'post','mocap')]
    
    exp_id = 1
    measurement_tp = 'post'
    device = 'mocap'
    
    pca_runner = PCARunner()
    general_analysis_runner = GeneralAnalysisRunner()
    #setup_loader_pca = SetupAnalyserPCA()
    #pc_reconstructer = ReconstructerPCA()
    pain_group = "shoulder_neck_healthy"

    #general_analysis_runner.create_plots_mean_std(device, exp_id, measurement_tp)
    
    if ((exp_id, measurement_tp, device) not in pca_uploads_completed):
        pca_results = pca_runner.run_pca_analysis(exp_id, measurement_tp, device, pain_group, check_requirements=False)
        pca_runner.upload_pca_analysis(pca_results)        
        pc_distribution_results = pca_runner.check_distribution(exp_id, device, measurement_tp, distributions_plotted= True)
        pca_runner.upload_distribution(pc_distribution_results)
        t_test_results = pca_runner.conduct_t_test(exp_id, device, measurement_tp)

        pca_runner.upload_t_test(t_test_results)        
        highest_rank = 1
        lowest_rank = 10
        pca_runner.reconstruct_pcas(exp_id, device, measurement_tp, highest_rank, lowest_rank, 'standard_scaler')
    
    
    rotation_method = 'varimax'
    # TODO: do pca with rotation
    rotation_results = pca_runner.run_pca_rotation(exp_id, device, measurement_tp,  method = rotation_method)
    print("")
             
if __name__ == '__main__':
    main()