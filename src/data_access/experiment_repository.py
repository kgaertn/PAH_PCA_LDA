from data_access.base_repository import BaseRepository
from models.experiment import Experiment

class ExperimentRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_experiment(self, experiment: Experiment) -> int:
        """
        Inserts a new experiment record into the 'experiment' table.

        Args:
            experiment (Experiment): The Experiment object containing the experiment details.

        Returns:
            int: The ID of the newly inserted experiment.
        """
        data = {
            "name": experiment.name,
            "data_state": experiment.data_state
        }
        return self.insert_one("experiment", data)
        
    def update_upload_complete(self, relative_path: str, exp_id:int):
        """
        Marks an experiment's data upload as complete and stores the corresponding folder path.

        Args:
            relative_path (str): The relative path to the uploaded data folder.
            exp_id (int): The ID of the experiment to update.
        """
        self.update(
            table="experiment",
            values={"data_folder": relative_path, "upload_complete": 1},
            where={"id": exp_id}
        )
# endregion Setter

# region Getter    
    def get_experiment_id_by_name(self, exp_name):
        """
        Retrieves a list of participant IDs associated with the specified pain group.

        Args:
            pain_group_id (int): The pain group ID to filter participants by.

        Returns:
            list[int] | None: List of participant IDs belonging to the pain group, or None if none found.
        """
        
        rows = self.get_advanced(
            table_or_view="experiment",
            columns=["id"],
            name = exp_name,
            return_df=False
        )
    
        return rows[0][0] if rows else None

    def get_complete_data_folders(self) -> list[str] | None:
        """
        Retrieves a list of data folder paths for experiments that have completed uploading.

        Returns:
            list[str] | None: A list of relative data folder paths if any exist, otherwise None.
        """
        
        df = self.get(table_or_view='experiment', columns=["data_folder"], upload_complete=1)
        if df is None or df.empty:
            return None
        return df["data_folder"].tolist()
# endregion Getter