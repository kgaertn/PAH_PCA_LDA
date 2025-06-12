import pandas as pd
from db.connection import get_connection
from models.experiment import Experiment

class ExperimentRepository:
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        self.conn = get_connection()

# region Setter
    def insert_experiment(self, experiment: Experiment):
        """
        Inserts a new experiment into the database.

        Args:
            experiment (Experiment): The experiment object containing name and data_state.

        Returns:
            int: The database ID of the newly inserted experiment.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO experiment (name, data_state)
            VALUES (?, ?)
        """, (experiment.name, experiment.data_state))
        self.conn.commit()
        return cursor.lastrowid

    def experiment_upload_complete(self, relative_path, exp_id):
        """
        Marks an experiment as upload complete and saves the data folder path of that experiment (to make sure data from the same experiment is not uploaded multiple times).

        Args:
            relative_path (str): The relative path to the experiment's data folder.
            exp_id (int): The database ID of the experiment to update.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE experiment
            SET data_folder = ?, upload_complete = ?
            WHERE id = ?
        """, (relative_path, 1, exp_id))
        self.conn.commit()      
# endregion Setter

# region Getter
# use these functions to access data from the experiment table, depending on the needs

    def get_all_experiments(self) -> pd.DataFrame | None:
        """
        Retrieves all experiments from the database and returns them as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all rows from the 'experiment' table.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM experiment")
        rows = cursor.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    
    def get_experiment_by_id(self, experiment_id: int) -> Experiment | None:
        """
        Retrieves an experiment by its database ID.

        Args:
            experiment_id (int): The ID of the experiment to retrieve.

        Returns:
            Experiment | None: The experiment object if found, otherwise None.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM experiment WHERE id = ?", (experiment_id,))
        row = cursor.fetchone()
        if row:
            return Experiment(id=row["id"], name=row["name"], data_state=row["data_state"], 
                              data_folder = row["data_folder"], upload_complete = row["upload_complete"])
        return None
    
    
    
    def get_experiment_id_by_name(self, experiment_name: str) -> int | None:
        """
        Retrieves the ID of an experiment by its name.

        Args:
            experiment_name (str): The name of the experiment.

        Returns:
            int | None: The experiment ID if found, otherwise None.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM experiment WHERE name = ?", (experiment_name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        return None
    
    def get_experiment_id_by_name_and_data_state(self, experiment_name: str, data_state:str) -> int | None:
        """
        Retrieves the ID of an experiment by its name and data_state.

        Args:
            experiment_name (str): The name of the experiment.
            data_state (str): The data_state of the experiment.

        Returns:
            int | None: The experiment ID if found, otherwise None.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM experiment WHERE name = ? AND data_state = ?", (experiment_name,data_state))
        row = cursor.fetchone()
        if row:
            return row[0]
        return None
    
    def get_complete_data_folders(self) -> list[str] | None:
        """
        Retrieves a list of data folder paths for experiments that have completed uploading.

        Returns:
            list[str] | None: A list of relative data folder paths if any exist, otherwise None.
        """
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT data_folder FROM experiment WHERE upload_complete = 1")
        rows = cursor.fetchall()
        if rows:
            return [row[0] for row in rows]
        return None
# endregion Getter