import sys
import os
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from core.uploader_service import UploaderService
from analysis.data_processing.data_preprocess import DataProcessor
from data_access.experiment_repository import ExperimentRepository
from db.setup import adjusted_db_setup
from db.setup import add_measurement_type_info



def main():
    #create_tables()
    adjusted_db_setup()
    print("Database tables created.")
    
    project_root = Path(__file__).parent.parent # Adjust as needed depending on your script's location
    mpa_data_folder = project_root / 'data/Sample_Data_PAH/MusikPhysioAnalysis'
    #raw_mpa_folder_mocap = mpa_data_folder / '00_VIOLIN/00_JOINT_ANGLE'
    clean_mpa_folder_mocap = mpa_data_folder / '00_VIOLIN/00_JOINT_ANGLE/output'
    #raw_mpa_folder_emg = mpa_data_folder / '00_VIOLIN/00_JOINT_ANGLE'
    clean_mpa_folder_emg = mpa_data_folder / '00_VIOLIN/01_EMG/output'
    
    
    mpa_pain_data =  mpa_data_folder / 'PRMD_High_Strings_Subgroups_Pain.xlsx'

    # TODO: Layers wieder sauber trennen: wie erkennen, dass alle dateien schon in der DB sind? 
    exp_repo = ExperimentRepository()

    
    uploader = UploaderService()
    
    #relative_path = uploader.anonymize_path(clean_mpa_folder_mocap)
    #relative_path
    relative_path = uploader.get_relative_data_path_without_filename(clean_mpa_folder_mocap)
    uploaded_data = exp_repo.get_complete_data_folders()
    if uploaded_data == None or relative_path not in uploaded_data:
        # all filepaths for clean mpa data
        data_folders = list(clean_mpa_folder_mocap.glob('*')) + list(clean_mpa_folder_emg.glob('*'))        
        uploader.upload(data_folders)
        #uploader.upload(list(clean_mpa_folder_emg.glob('*')))
        
    uploader.upload_mpa_pain(str(mpa_pain_data))
    add_measurement_type_info()
    
    data_processor = DataProcessor()
    data_processor.create_samples()

if __name__ == "__main__":
    main()