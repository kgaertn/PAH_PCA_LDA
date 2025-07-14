
from sklearn.preprocessing import StandardScaler
from pathlib import Path
import matplotlib.pyplot as plt

from analysis.data_processing.data_preprocess import DataProcessor    
from analysis.data_processing.data_loading import DataLoader
from analysis.data_analysis.pca_analysis import PCAAnalyser
from analysis.data_analysis.data_plotting import DataPlotter
    
class ReconstructerPCA:
    def __init__(self):
        """
        Initializes the SetupUploader with processors.
        """
        self.data_processor = DataProcessor()
        self.pca_analyser = PCAAnalyser() 
        self.data_loader = DataLoader()
        self.data_plotter = DataPlotter()
        #self.setup_uploader = SetupUploader()
    
    # reconstruct & plot top PCs
