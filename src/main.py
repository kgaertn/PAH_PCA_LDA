from core.setup_analyse_pca import SetupAnalysePCA
from core.setup_and_upload import SetupUploader

def main():
    measurement_tp = 'pre'
    device = 'mocap'
    
    setup_loader = SetupUploader()
    setup_loader_pca = SetupAnalysePCA()
    setup_loader.run_db_setup()    
    setup_loader_pca.run_pca_analysis(measurement_tp, device)
    
    
    
             
if __name__ == '__main__':
    main()