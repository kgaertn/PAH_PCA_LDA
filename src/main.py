from core.setup_analyse_pca import SetupAnalyserPCA
from core.setup_and_upload import SetupUploader
from core.reconstruct_pca import ReconstructerPCA

def main():
    db_setup = True
    #pca_uploads_completed = []
    pca_uploads_completed = [(1,'pre','mocap'), (1,'post','mocap')]
    exp_id = 1
    measurement_tp = 'pre'
    device = 'mocap'
    
    setup_loader = SetupUploader()
    setup_loader_pca = SetupAnalyserPCA()
    pc_reconstructer = ReconstructerPCA()
    if not db_setup:
        setup_loader.run_db_setup()  
    #if ((exp_id, measurement_tp, device) not in pca_uploads_completed):
    pca_results = setup_loader_pca.run_pca_analysis(exp_id, measurement_tp, device)
    t_test_results = setup_loader_pca.conduct_t_test(exp_id, device, measurement_tp)
    if ((exp_id, measurement_tp, device) not in pca_uploads_completed):
        setup_loader_pca.upload_pca_analysis(pca_results)
        setup_loader_pca.upload_t_test(t_test_results)        
    highest_rank = 1
    lowest_rank = 10
    pc_reconstructer.reconstruct_pcas(exp_id, device, measurement_tp, highest_rank, lowest_rank, 'standard_scaler')
    
    # TODO: do pca with rotation
    # TODO: do requirement tests
    
    
             
if __name__ == '__main__':
    main()