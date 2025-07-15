import pandas as pd
#from db.connection import get_connection
from data_access.base_repository import BaseRepository
from models.experiment import Experiment

class ExperimentRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        super().__init__()
        #self.conn = get_connection()

# region Setter
    def insert_experiment(self, experiment: Experiment):
        data = {
            "name": experiment.name,
            "data_state": experiment.data_state
        }
        return self.insert_one("experiment", data)
        
    def update_upload_complete(self, relative_path: str, exp_id):
        """
        Beispiel mit der generischen Update-Funktion.
        Setzt sample_id, wo measurement_id = meas_id und bow_stroke IN (bow_stroke_start, bow_stroke_end).

        Da IN mit mehreren Werten nicht unterstützt ist, lösen wir das mit zwei OR Bedingungen oder zwei Updates.
        Hier als einfache Variante zwei Updates:

        """
        self.update(
            table="experiment",
            values={"data_folder": relative_path, "upload_complete": 1},
            where={"id": exp_id}
        )
# endregion Setter

# region Getter
    
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