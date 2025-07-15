import pandas as pd
from data_access.base_repository import BaseRepository
from db.connection import get_connection
from models.datapoint import Datapoint

class DatapointRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the DatapointRepository.
        """
        super().__init__()
        #super().__init__(table_or_view="[Complete Data]")
        #self.conn = get_connection()

# region Setter
    def insert_datapoint(self, datapoint: Datapoint):
        data = {
            "measurement_id": datapoint.measurement_id,
            "bow_stroke": datapoint.bow_stroke,
            "up_down": datapoint.up_down,
            "key": datapoint.key,
            "time_point": datapoint.time_point,
            "value": datapoint.value
        }
        return self.insert_one("datapoint", data)
        
    def insert_many_datapoints(self, datapoints: list[Datapoint]):
        data_list = [{
            "measurement_id": dp.measurement_id,
            "bow_stroke": dp.bow_stroke,
            "up_down": dp.up_down,
            "key": dp.key,
            "time_point": dp.time_point,
            "value": dp.value
        } for dp in datapoints]
        self.insert_many("datapoint", data_list)

    def update_datapoints_sample(self, meas_id: int, bow_stroke_start, bow_stroke_end, sample_id):
        """
        Beispiel mit der generischen Update-Funktion.
        Setzt sample_id, wo measurement_id = meas_id und bow_stroke IN (bow_stroke_start, bow_stroke_end).

        Da IN mit mehreren Werten nicht unterstützt ist, lösen wir das mit zwei OR Bedingungen oder zwei Updates.
        Hier als einfache Variante zwei Updates:

        """
        self.update(
            table="datapoint",
            values={"sample_id": sample_id},
            where={"measurement_id": meas_id, "bow_stroke": bow_stroke_start}
        )
        self.update(
            table="datapoint",
            values={"sample_id": sample_id},
            where={"measurement_id": meas_id, "bow_stroke": bow_stroke_end}
        )
        
# endregion Setter

#region Getter
# use these functions to access data from the datapoint table, depending on the needs

    #def get_datapoints_by_exp_id(self, exp_id:int) -> pd.DataFrame | None:
    #    """
    #    Retrieves all datapoints associated with a specific experiment ID by selecting the corresponding 
    #    View from the database.
#
    #    Args:
    #        exp_id (int): The ID of the experiment.
#
    #    Returns:
    #        pd.DataFrame | None: A DataFrame containing datapoint information along with 
    #        measurement metadata and participant pain-related fields. Returns None if no data found.
    #    """
    #    cursor = self.conn.cursor()
    #    query = ""
    #    if exp_id == 1:
    #        query = """
    #            SELECT * FROM [Complete Data]
    #        """
    #    cursor.execute(query)
    #    rows = cursor.fetchall()
    #    if not rows:
    #        return None
    #    columns = [desc[0] for desc in cursor.description]
    #    return pd.DataFrame(rows, columns=columns)
    #
    #def get_datapoints_by_exp_id_and_device(self, exp_id:int, device:str) -> pd.DataFrame | None:
    #    """
    #    Retrieves all datapoints associated with a specific experiment ID and measurement device by selecting the corresponding 
    #    View from the database.
#
    #    Args:
    #        exp_id (int): The ID of the experiment.
    #        device (str): The name of the measurement id (e.g., 'emg').
#
    #    Returns:
    #        pd.DataFrame | None: A DataFrame containing datapoint information along with 
    #        measurement metadata and participant pain-related fields. Returns None if no data found.
    #    """
    #    cursor = self.conn.cursor()
    #    query ="""
    #        SELECT * FROM [Complete Data] WHERE experiment_id = ? AND device = ?;
    #    """
    #    cursor.execute(query, (exp_id, device))
    #    rows = cursor.fetchall()
    #    if not rows:
    #        return None
    #    columns = [desc[0] for desc in cursor.description]
    #    return pd.DataFrame(rows, columns=columns)
    #
    #def get_datapoints_by_exp_id_device_and_timepoint(self, exp_id:int, device:str, timepoint:str) -> pd.DataFrame | None:
    #    """
    #    Retrieves all datapoints associated with a specific experiment ID, measurement device and timepoint by selecting the corresponding 
    #    View from the database.
#
    #    Args:
    #        exp_id (int): The ID of the experiment.
    #        device (str): The name of the measurement device (e.g., 'emg').
    #        timepoint (str): The name of the measurement timepoint (e.g., 'pre').
#
    #    Returns:
    #        pd.DataFrame | None: A DataFrame containing datapoint information along with 
    #        measurement metadata and participant pain-related fields. Returns None if no data found.
    #    """
    #    cursor = self.conn.cursor()
    #    query ="""
    #        SELECT * FROM [Complete Data] WHERE experiment_id = ? AND device = ? AND timepoint = ?;
    #    """
    #    cursor.execute(query, (exp_id, device, timepoint,))
    #    rows = cursor.fetchall()
    #    if not rows:
    #        return None
    #    columns = [desc[0] for desc in cursor.description]
    #    return pd.DataFrame(rows, columns=columns)
    #
    #def get_datapoints_by_exp_id_device_timepoint_target(self, exp_id:int, device:str, timepoint:str, target:str) -> pd.DataFrame | None:
    #    """
    #    Retrieves all datapoints associated with a specific experiment ID, measurement device and timepoint by selecting the corresponding 
    #    View from the database.
#
    #    Args:
    #        exp_id (int): The ID of the experiment.
    #        device (str): The name of the measurement device (e.g., 'emg').
    #        timepoint (str): The name of the measurement timepoint (e.g., 'pre').
#
    #    Returns:
    #        pd.DataFrame | None: A DataFrame containing datapoint information along with 
    #        measurement metadata and participant pain-related fields. Returns None if no data found.
    #    """
    #    cursor = self.conn.cursor()
    #    query = """
    #        SELECT * FROM [Complete Data] WHERE experiment_id = ? AND device = ? AND timepoint = ? AND target = ?;
    #    """
    #    cursor.execute(query, (exp_id, device, timepoint, target))
    #    rows = cursor.fetchall()
    #    if not rows:
    #        return None
    #    columns = [desc[0] for desc in cursor.description]
    #    return pd.DataFrame(rows, columns=columns)
#
    #def get_datapoints_by_exp_id_device_timepoint_target_axis(self, exp_id:int, device:str, timepoint:str, target:str, axis:str = None) -> pd.DataFrame | None:
    #    """
    #    Retrieves all datapoints associated with a specific experiment ID, measurement device and timepoint by selecting the corresponding 
    #    View from the database.
#
    #    Args:
    #        exp_id (int): The ID of the experiment.
    #        device (str): The name of the measurement device (e.g., 'emg').
    #        timepoint (str): The name of the measurement timepoint (e.g., 'pre').
#
    #    Returns:
    #        pd.DataFrame | None: A DataFrame containing datapoint information along with 
    #        measurement metadata and participant pain-related fields. Returns None if no data found.
    #    """
    #    cursor = self.conn.cursor()
    #    query = ""
    #    if device == 'emg':
    #        query ="""
    #            SELECT * FROM [Complete Data]
    #            WHERE experiment_id = ? AND device = ? AND timepoint = ? AND target = ?;
    #        """
    #    elif device == 'mocap':
    #        query ="""
    #            SELECT * FROM [Complete Data]
    #            WHERE experiment_id = ? AND device = ? AND timepoint = ? AND target = ? AND axis = ?;
    #            
    #        """
    #    if device == 'emg':
    #        cursor.execute(query, (exp_id, device, timepoint, target))
    #    else:
    #        cursor.execute(query, (exp_id, device, timepoint, target, axis))
    #    rows = cursor.fetchall()
    #    if not rows:
    #        return None
    #    columns = [desc[0] for desc in cursor.description]
    #    return pd.DataFrame(rows, columns=columns)
#
    #def get_datapoints_by_exp_id_device_timepoint_target_axis_part_ids(self, exp_id:int, device:str, timepoint:str, target:str, participant_ids:tuple, axis:str = None) -> pd.DataFrame | None:
    #    """
    #    Retrieves all datapoints associated with a specific experiment ID, measurement device and timepoint by selecting the corresponding 
    #    View from the database.
#
    #    Args:
    #        exp_id (int): The ID of the experiment.
    #        device (str): The name of the measurement device (e.g., 'emg').
    #        timepoint (str): The name of the measurement timepoint (e.g., 'pre').
#
    #    Returns:
    #        pd.DataFrame | None: A DataFrame containing datapoint information along with 
    #        measurement metadata and participant pain-related fields. Returns None if no data found.
    #    """
    #    placeholders = ','.join(['?'] * len(participant_ids)) 
    #    cursor = self.conn.cursor()
    #    query = ""
    #    if device == 'emg':
    #        query =f"""
    #            SELECT * FROM [Complete Data]
    #            WHERE experiment_id = ? AND device = ? AND timepoint = ? AND target = ? AND participant_id IN ({placeholders})
    #        """
    #        params = (exp_id, device, timepoint, target) + participant_ids
    #    elif device == 'mocap':
    #        query =f"""
    #            SELECT * FROM [Complete Data]
    #            WHERE experiment_id = ? AND device = ? AND timepoint = ? AND target = ? AND axis = ? AND participant_id IN ({placeholders})
    #            
    #        """
    #        params = (exp_id, device, timepoint, target, axis) + participant_ids
    #    #if device == 'emg':
    #    cursor.execute(query, params)
    #    rows = cursor.fetchall()
    #    if not rows:
    #        return None
    #    columns = [desc[0] for desc in cursor.description]
    #    return pd.DataFrame(rows, columns=columns)
#
#
    #def get_datapoints_by_meas_id(self, meas_id: int) -> Datapoint | None:
    #    """
    #    Retrieves a datapoint from the database using its unique ID.
#
    #    Args:
    #        datapoint_id (int): The primary key ID of the datapoint in the database.
#
    #    Returns:
    #        pd.DataFrame | None: A DataFrame containing datapoint information along with 
    #        measurement metadata. Returns None if no data found.
    #    """
    #    
    #    cursor = self.conn.cursor()
    #    cursor.execute("SELECT * FROM datapoint WHERE measurement_id = ?", (meas_id,))
    #    rows = cursor.fetchall()
    #    if not rows:
    #        return None
    #    columns = [desc[0] for desc in cursor.description]
    #    return pd.DataFrame(rows, columns=columns)
    #
    #def get_datapoint_by_id(self, datapoint_id: int) -> Datapoint | None:
    #    """
    #    Retrieves a datapoint from the database using its unique ID.
#
    #    Args:
    #        datapoint_id (int): The primary key ID of the datapoint in the database.
#
    #    Returns:
    #        Datapoint | None: A Datapoint object containing the fields:
    #            - id
    #            - measurement_id
    #            - bow_stroke
    #            - up_down
    #            - key
    #            - time_point
    #            - value
    #        If no matching datapoint is found, returns None.
    #    """
    #    
    #    cursor = self.conn.cursor()
    #    cursor.execute("SELECT * FROM datapoint WHERE id = ?", (datapoint_id,))
    #    row = cursor.fetchone()
    #    if row:
    #        return Datapoint(id=row["id"], measurement_id=row["measurement_id"], bow_stroke=row["bow_stroke"],
    #                         up_down=row["up_down"], key = row["key"], time_point=row["time_point"], value=row["value"])
    #    return None
    
# endregion Getter
