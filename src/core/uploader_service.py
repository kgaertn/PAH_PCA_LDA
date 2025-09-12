from core.data_parser import *
from data_access.repositories.experiment_repository import ExperimentRepository
from data_access.repositories.participant_repository import ParticipantRepository
from data_access.repositories.measurement_repository import MeasurementRepository
from data_access.repositories.datapoint_repository import DatapointRepository

import os

class UploaderService:
    def __init__(self):
        """
        Initialize the UploaderService with repository instances for experiments, participants,
        measurements, and datapoints.
        """
        self.experiment_repo = ExperimentRepository()
        self.participant_repo = ParticipantRepository()
        self.measurement_repo = MeasurementRepository()
        self.datapoint_repo = DatapointRepository()

    def _choose_parser(self, file_path: str) -> DataParser:
        """
        Select an appropriate data parser based on the file extension and filename patterns.

        Args:
            file_path (str): Path to the file to parse.

        Returns:
            DataParser: An instance of the parser suited for the file format.

        Raises:
            ValueError: If the file format or name pattern is unrecognized.
        """
        _, ext = os.path.splitext(file_path)
        if ext.lower() in [".xlsx", ".xls", ".tsv"]:
            if "00_violin" in file_path.lower() and not "output" in file_path.lower():
                return FormatRawMPAParser()
            elif "00_violin" in file_path.lower() and "output" in file_path.lower():
                return FormatCleanMPAParser()
            elif "pain" in file_path.lower():
                return FormatPainMPAParser()
            else:
                raise ValueError(f"Unknown Excel-Format for file {file_path}")
        else:
            raise ValueError(f"Unknown file format: {ext}")

    def anonymize_path(self, path: Path) -> Path:
        """
        "Anonymize" parts of a path by replacing specific folder names with '...'.

        Args:
            path (Path): The original file or folder path.

        Returns:
            Path: A new Path object with sensitive folder names anonymized.
        """
        target_folders = {"00_JOINT_ANGLE", "01_EMG", "02_FORCE_PRESSURE"}
        parts = [("..." if part in target_folders else part) for part in path.parts]
        return Path(*parts)
    
    def get_relative_data_path_without_filename(self, file_path:str)-> str:
        """
        Extract the relative path starting from the 'data' folder including the filename.

        Args:
            file_path (str or Path): Full file path.

        Returns:
            str: Relative path from 'data' folder including filename.

        Raises:
            ValueError: If 'data' is not found in the path.
        """
        file_path = Path(file_path)
        file_path= self.anonymize_path(file_path)
        parts = file_path.parts
        
        try:
            data_index = parts.index("data")
        except ValueError:
            raise ValueError("'data' not found in the path")

        relative_parts = parts[data_index:]

        relative_path = Path(*relative_parts)
        return str(relative_path)
    
    def get_relative_data_path(self, file_path:str)-> str:
        """
        Extract the relative path starting from the 'data' folder excluding the filename.

        Args:
            file_path (str): Full file path.

        Returns:
            str: Relative path from 'data' folder excluding filename.

        Raises:
            ValueError: If 'data' is not found in the path.
        """
        file_path = Path(file_path)
        file_path= self.anonymize_path(file_path)
        parts = file_path.parts

        try:
            data_index = parts.index("data")
        except ValueError:
            raise ValueError("'data' not found in the path")

        relative_parts = parts[data_index:-1]

        relative_path = Path(*relative_parts)
        return str(relative_path)

    def upload(self, files_location: list):
        """
        Upload experiment data from given file paths by parsing and inserting into the database.

        For each experiment parsed, participants, measurements, and datapoints are inserted
        into their respective repositories, updating IDs as needed.

        Args:
            files_location (list): List of file paths to upload.
        """
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
            
    def upload_mpa_pain(self, file_path: str):
        """
        Upload pain-related data from a specified file and update participant pain data in the database.

        Args:
            file_path (str): Path to the pain data file.
        """
        parser = self._choose_parser(file_path)
        pain_data = parser.parse(file_path)
        exp_id = self.experiment_repo.get_experiment_id_by_name('mpa')
        for participant in pain_data.get("participants", []):
            self.participant_repo.update_pain_data(participant, exp_id)
            
    
        
        
        
        