import pandas as pd
from db.connection import get_connection
from models.datapoint import Datapoint

class DatapointRepository:
    def __init__(self):
        """
        Initializes the DatapointRepository with a database connection.
        """
        self.conn = get_connection()

# region Setter
    def insert_datapoint(self, datapoint: Datapoint):
        """
        Inserts a single datapoint into the database.

        Args:
            datapoint (Datapoint): The Datapoint object containing measurement ID, bow stroke, 
                                   time point, and value.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO datapoint (measurement_id, bow_stroke, up_down, time_point, value)
            VALUES (?, ?, ?, ?, ?)
        """, (datapoint.measurement_id, datapoint.bow_stroke, datapoint.up_down, datapoint.time_point, 
              datapoint.value))
        self.conn.commit()
        
    def insert_many_datapoints(self, datapoints: list[Datapoint]):
        """
        Inserts multiple datapoints into the database in a single batch operation.

        Args:
            datapoints (list[Datapoint]): A list of Datapoint objects, each containing:
                - measurement_id
                - bow_stroke
                - up_down
                - time_point
                - value
        """
        
        cursor = self.conn.cursor()
        cursor.executemany("""
        INSERT INTO datapoint (measurement_id, bow_stroke, up_down, time_point, value)
        VALUES (?, ?, ?, ?, ?)
        """, [(dp.measurement_id, dp.bow_stroke, dp.up_down, dp.time_point, dp.value) for dp in datapoints])
        self.conn.commit()
# endregion Setter

#region Getter
# use these functions to access data from the datapoint table, depending on the needs

    def get_datapoints_by_exp_id(self, exp_id:int) -> pd.DataFrame | None:
        """
        Retrieves all datapoints associated with a specific experiment ID by joining
        datapoint, measurement, participant, and experiment tables.

        Args:
            exp_id (int): The ID of the experiment.

        Returns:
            pd.DataFrame | None: A DataFrame containing datapoint information along with 
            measurement metadata and participant pain-related fields. Returns None if no data found.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT 
                experiment.id AS experiment_id,
                experiment.name AS experiment_name,
                participant.participant_id,
                participant.instrument,
                participant.PRMD_shoulder_neck_right,
                participant.PRMD_shoulder_neck_left,
                participant.PRMD_upper_arm_right,
                participant.PRMD_upper_arm_left,
                participant.PRMD_ever,
                measurement.id AS measurement_id,
                measurement.timepoint,
                measurement.device,
                measurement.target,
                measurement.axis,
                measurement.unit,
                datapoint.*
            FROM datapoint
            JOIN measurement ON datapoint.measurement_id = measurement.id
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE experiment.id = ?
        """
        cursor.execute(query, (exp_id,))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def get_datapoints_by_exp_id_and_device(self, exp_id:int, device:str) -> pd.DataFrame | None:
        """
        Retrieves all datapoints associated with a specific experiment ID and measurement device by joining
        datapoint, measurement, participant, and experiment tables.

        Args:
            exp_id (int): The ID of the experiment.
            device (str): The name of the measurement id (e.g., 'emg').

        Returns:
            pd.DataFrame | None: A DataFrame containing datapoint information along with 
            measurement metadata and participant pain-related fields. Returns None if no data found.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT 
                experiment.id AS experiment_id,
                experiment.name AS experiment_name,
                participant.participant_id,
                participant.instrument,
                participant.PRMD_shoulder_neck_right,
                participant.PRMD_shoulder_neck_left,
                participant.PRMD_upper_arm_right,
                participant.PRMD_upper_arm_left,
                participant.PRMD_ever,
                measurement.id AS measurement_id,
                measurement.timepoint AS measurement_time_point,
                measurement.device,
                measurement.target,
                measurement.axis,
                measurement.unit,
                datapoint.id AS datapoint_id,
                datapoint.bow_stroke,
                datapoint.up_down,
                datapoint.time_point AS dp_time_point,
                datapoint.value
            FROM datapoint
            JOIN measurement ON datapoint.measurement_id = measurement.id
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE experiment.id = ? AND measurement.device = ?
        """
        cursor.execute(query, (exp_id, device))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def get_datapoints_by_exp_id_device_and_timepoint(self, exp_id:int, device:str, timepoint:str) -> pd.DataFrame | None:
        """
        Retrieves all datapoints associated with a specific experiment ID, measurement device and timepoint by joining
        datapoint, measurement, participant, and experiment tables.

        Args:
            exp_id (int): The ID of the experiment.
            device (str): The name of the measurement device (e.g., 'emg').
            timepoint (str): The name of the measurement timepoint (e.g., 'pre').

        Returns:
            pd.DataFrame | None: A DataFrame containing datapoint information along with 
            measurement metadata and participant pain-related fields. Returns None if no data found.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT                
                experiment.id AS experiment_id,
                experiment.name AS experiment_name,
                participant.participant_id,
                participant.instrument,
                participant.PRMD_shoulder_neck_right,
                participant.PRMD_shoulder_neck_left,
                participant.PRMD_upper_arm_right,
                participant.PRMD_upper_arm_left,
                participant.PRMD_ever,
                measurement.id AS measurement_id,
                measurement.timepoint AS measurement_time_point,
                measurement.device,
                measurement.target,
                measurement.axis,
                measurement.unit,
                datapoint.id AS datapoint_id,
                datapoint.bow_stroke,
                datapoint.up_down,
                datapoint.time_point AS dp_time_point,
                datapoint.value
            FROM datapoint
            JOIN measurement ON datapoint.measurement_id = measurement.id
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE experiment.id = ? AND measurement.device = ? AND measurement.timepoint = ?
        """
        cursor.execute(query, (exp_id, device, timepoint))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def get_datapoints_nopain_by_exp_id_device_and_timepoint(self, exp_id:int, device:str, timepoint:str) -> pd.DataFrame | None:
        """
        Retrieves all datapoints (without the pain and instrument information) associated with a specific experiment ID, measurement device and timepoint by joining
        datapoint, measurement, participant, and experiment tables.

        Args:
            exp_id (int): The ID of the experiment.
            device (str): The name of the measurement device (e.g., 'emg').
            timepoint (str): The name of the measurement timepoint (e.g., 'pre').

        Returns:
            pd.DataFrame | None: A DataFrame containing datapoint information along with 
            measurement metadata and participant pain-related fields. Returns None if no data found.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT 
                experiment.id AS experiment_id,
                experiment.name AS experiment_name,
                participant.participant_id,
                measurement.id AS measurement_id,
                measurement.timepoint AS measurement_time_point,
                measurement.device,
                measurement.target,
                measurement.axis,
                measurement.unit,
                datapoint.id AS datapoint_id,
                datapoint.bow_stroke,
                datapoint.up_down,
                datapoint.time_point AS dp_time_point,
                datapoint.value
            FROM datapoint
            JOIN measurement ON datapoint.measurement_id = measurement.id
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE experiment.id = ? AND measurement.device = ? AND measurement.timepoint = ?
        """
        cursor.execute(query, (exp_id, device, timepoint))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)   

    def get_datapoints_by_exp_id_device_timepoint_target(self, exp_id:int, device:str, timepoint:str, target:str) -> pd.DataFrame | None:
        """
        Retrieves all datapoints associated with a specific experiment ID, measurement device and timepoint by joining
        datapoint, measurement, participant, and experiment tables.

        Args:
            exp_id (int): The ID of the experiment.
            device (str): The name of the measurement device (e.g., 'emg').
            timepoint (str): The name of the measurement timepoint (e.g., 'pre').

        Returns:
            pd.DataFrame | None: A DataFrame containing datapoint information along with 
            measurement metadata and participant pain-related fields. Returns None if no data found.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT                
                experiment.id AS experiment_id,
                experiment.name AS experiment_name,
                participant.participant_id,
                participant.instrument,
                participant.PRMD_shoulder_neck_right,
                participant.PRMD_shoulder_neck_left,
                participant.PRMD_upper_arm_right,
                participant.PRMD_upper_arm_left,
                participant.PRMD_ever,
                measurement.id AS measurement_id,
                measurement.timepoint AS measurement_time_point,
                measurement.device,
                measurement.target,
                measurement.axis,
                measurement.unit,
                datapoint.id AS datapoint_id,
                datapoint.bow_stroke,
                datapoint.up_down,
                datapoint.time_point AS dp_time_point,
                datapoint.value
            FROM datapoint
            JOIN measurement ON datapoint.measurement_id = measurement.id
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE experiment.id = ? AND measurement.device = ? AND measurement.timepoint = ? AND measurement.target = ?
        """
        cursor.execute(query, (exp_id, device, timepoint, target))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def get_datapoint_by_id(self, datapoint_id: int) -> Datapoint | None:
        """
        Retrieves a datapoint from the database using its unique ID.

        Args:
            datapoint_id (int): The primary key ID of the datapoint in the database.

        Returns:
            Datapoint | None: A Datapoint object containing the fields:
                - id
                - measurement_id
                - bow_stroke
                - up_down
                - time_point
                - value
            If no matching datapoint is found, returns None.
        """
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM datapoint WHERE id = ?", (datapoint_id,))
        row = cursor.fetchone()
        if row:
            return Datapoint(id=row["id"], measurement_id=row["measurement_id"], bow_stroke=row["bow_stroke"],
                             up_down=row["up_down"], time_point=row["time_point"], value=row["value"])
        return None
    
# endregion Getter