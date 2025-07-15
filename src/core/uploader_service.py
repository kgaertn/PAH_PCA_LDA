
from core.data_parser import *
from data_access.experiment_repository import ExperimentRepository
from data_access.participant_repository import ParticipantRepository
from data_access.measurement_repository import MeasurementRepository
from data_access.datapoint_repository import DatapointRepository

import os

class UploaderService:
    def __init__(self):
        self.experiment_repo = ExperimentRepository()
        self.participant_repo = ParticipantRepository()
        self.measurement_repo = MeasurementRepository()
        self.datapoint_repo = DatapointRepository()

    def _choose_parser(self, file_path: str) -> DataParser:
        # Basierend auf Dateiendung oder Dateiinhalt passenden Parser auswählen
        #file_path = str(file_path)
        _, ext = os.path.splitext(file_path)
        if ext.lower() in [".xlsx", ".xls", ".tsv"]:
            # Beispiel: Unterscheide anhand Dateiname oder anderen Kriterien
            if "00_violin" in file_path.lower() and not "output" in file_path.lower():
                return FormatRawMPAParser()
            elif "00_violin" in file_path.lower() and "output" in file_path.lower():
                return FormatCleanMPAParser()
            elif "pain" in file_path.lower():
                return FormatPainMPAParser()
            else:
                raise ValueError(f"Unbekanntes Excel-Format für Datei {file_path}")
        else:
            raise ValueError(f"Unbekannter Dateityp: {ext}")

    def anonymize_path(self, path: Path) -> Path:
        target_folders = {"00_JOINT_ANGLE", "01_EMG", "02_FORCE_PRESSURE"}
        parts = [("..." if part in target_folders else part) for part in path.parts]
        return Path(*parts)
    
    def get_relative_data_path_without_filename(self, file_path):
        # Get all parts of the path
        file_path = Path(file_path)
        file_path= self.anonymize_path(file_path)
        parts = file_path.parts

        # Find the index of "data"
        try:
            data_index = parts.index("data")
        except ValueError:
            raise ValueError("'data' not found in the path")

        # Extract parts from 'data' up to (but excluding) the filename
        relative_parts = parts[data_index:]

        # Rebuild path from those parts
        relative_path = Path(*relative_parts)
        return str(relative_path)
    
    def get_relative_data_path(self, file_path):
        # Get all parts of the path
        file_path = Path(file_path)
        file_path= self.anonymize_path(file_path)
        parts = file_path.parts

        # Find the index of "data"
        try:
            data_index = parts.index("data")
        except ValueError:
            raise ValueError("'data' not found in the path")

        # Extract parts from 'data' up to (but excluding) the filename
        relative_parts = parts[data_index:-1]

        # Rebuild path from those parts
        relative_path = Path(*relative_parts)
        return str(relative_path)

    def upload(self, files_location: list) -> None:
        #relative_path = self.get_relative_data_path(file_path)
        #uploaded_data = self.experiment_repo.get_complete_data_folders()
        #if uploaded_data == None or relative_path not in uploaded_data:
        data_folder = str(files_location[0])
        parser = self._choose_parser(data_folder)
        exp_data = parser.parse_experiment(data_folder)
        relative_path = ""
        for experiment in exp_data.get("experiments", []):
            ext_exp_id = experiment.id
            exp_id = self.experiment_repo.insert_experiment(experiment) 
            for file_path in files_location:
                file_path = str(file_path)
                print(file_path)
                relative_path = self.get_relative_data_path(file_path)
                #shortened = relative_path.replace("data/folder1", "...")
                #parser = self._choose_parser(file_path)
                data = parser.parse(file_path)
                
                for participant in data.get("participants", []):
                    db_participant_ids = self.participant_repo.get_participant_ids(exp_id)
                    ext_part_id = participant.id
                    if db_participant_ids == None or not participant.participant_id in db_participant_ids:
                        if participant.experiment_id == ext_exp_id:
                            participant.experiment_id = exp_id
                            part_id = self.participant_repo.insert_participant(participant)
                    else:
                        part_id = self.participant_repo.get_participant_db_id(participant.participant_id, exp_id)

                    for measurement in data.get("measurements", []):
                        #self.measurement_repo.insert_experiment(measurement)
                        ext_meas_id = measurement.id
                        if measurement.participant_id == ext_part_id:
                            measurement.participant_id = part_id
                            meas_id = self.measurement_repo.insert_measurement(measurement)

                            datapoints = []
                            for datapoint in data.get("datapoints", []):
                                if datapoint.measurement_id == ext_meas_id:
                                    datapoint.measurement_id = meas_id
                                    datapoints.append(datapoint)
                            if datapoints != []:
                                self.datapoint_repo.insert_many_datapoints(datapoints)
                    print("Participant " + str(ext_part_id) + " uploaded to the DB")
            self.experiment_repo.update_upload_complete(relative_path, exp_id) 
            
    def upload_mpa_pain(self, file_path: str) -> None:
        parser = self._choose_parser(file_path)
        pain_data = parser.parse(file_path)
        exp_id = self.experiment_repo.get_experiment_id_by_name('mpa')
        for participant in pain_data.get("participants", []):
            self.participant_repo.update_pain_data(participant, exp_id)
            
    
        
        
        
        