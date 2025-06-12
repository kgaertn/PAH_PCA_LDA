import pandas as pd
from db.connection import get_connection
from models.measurement import Measurement

class MeasurementRepository:
    def __init__(self):
        """
        Initializes the MeasurementRepository with a database connection.
        """
        self.conn = get_connection()

# region Setter
    def insert_measurement(self, measurement: Measurement):
        """
        Inserts a new measurement record into the database.

        Args:
            measurement (Measurement): The measurement object containing participant ID,
                timepoint, device, target, axis, and unit information.

        Returns:
            int: The ID of the newly inserted measurement record.
        """
        
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO measurement (participant_id, timepoint, device, target, axis, unit)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (measurement.participant_id , measurement.timepoint, measurement.device, measurement.target,
              measurement.axis, measurement.unit))
        self.conn.commit()
        return cursor.lastrowid
# endregion Setter

# region Getter
# use these functions to access data from the measurement table, depending on the needs
    def get_measurements_by_participant_id(self, participant_id: str) -> pd.DataFrame:
        """
        Retrieves all measurement data for a participant using their participant ID
        (e.g., 'P001'), including associated participant metadata such as pain-related fields.

        Args:
            participant_id (str): The external participant identifier.

        Returns:
            pd.DataFrame: A DataFrame containing measurements joined with participant information.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT 
                measurement.*,
                participant.id,
                participant.participant_id
                participant.instrument,
                participant.PRMD_shoulder_neck_right,
                participant.PRMD_shoulder_neck_left,
                participant.PRMD_upper_arm_right,
                participant.PRMD_upper_arm_left,
                participant.PRMD_ever
            FROM measurement
            JOIN participant ON measurement.participant_id = participant.participant_id
            WHERE measurement.participant_id = ?
        """
        cursor.execute(query, (participant_id,))
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns) 

    def get_measurements_by_device(self, device: str, exp_id: int) -> pd.DataFrame:
        """
        Retrieves all measurement data for a specific device and experiment name, including associated
        participant information (instrument and pain-related fields) and experiment metadata.

        Args:
            device (str): The name of the measurement device (e.g., 'emg').
            exp_id (int): The id of the experiment to filter by.

        Returns:
            pd.DataFrame: A DataFrame containing joined data from measurement, participant, and experiment tables.
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
                measurement.*
            FROM measurement
            JOIN participant ON measurement.participant_id = participant.participant_id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE measurement.device = ? AND experiment.id = ?
        """
        cursor.execute(query, (device, exp_id))
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)      

    def get_measurements_by_timepoint(self, timepoint: str, exp_id: int) -> pd.DataFrame:
        """
        Retrieves all measurement data for a specific device and experiment id, including associated
        participant information (instrument and pain-related fields) and experiment metadata.

        Args:
            timepoint (str): The name of the measurement timepoint (e.g., 'pre').
            exp_id (int): The id of the experiment to filter by.

        Returns:
            pd.DataFrame: A DataFrame containing joined data from measurement, participant, and experiment tables.
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
                measurement.*
            FROM measurement
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE measurement.timepoint = ? AND experiment.id = ?
        """
        cursor.execute(query, (timepoint, exp_id))
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)   
    
    def get_measurements_by_target(self, target: str, exp_id: int) -> pd.DataFrame:
        """
        Retrieves all measurement data for a specific target and experiment id, including associated
        participant information (instrument and pain-related fields) and experiment metadata.

        Args:
            target (str): The name of the measurement target (e.g., 'left elbow joint angle').
            exp_id (int): The id of the experiment to filter by.

        Returns:
            pd.DataFrame: A DataFrame containing joined data from measurement, participant, and experiment tables.
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
                measurement.*
            FROM measurement
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE measurement.target = ? AND experiment.id = ?
        """
        cursor.execute(query, (target, exp_id))
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns) 

    def get_measurements_by_target_and_axis(self, target: str, axis: str, exp_id: int) -> pd.DataFrame:
        """
        Retrieves all measurement data for a specific target and experiment id, including associated
        participant information (instrument and pain-related fields) and experiment metadata.

        Args:
            target (str): The name of the measurement target (e.g., 'left elbow joint angle').
            axis (str): The name of the measurement axis (e.g., 'X').
            exp_id (int): The id of the experiment to filter by.

        Returns:
            pd.DataFrame: A DataFrame containing joined data from measurement, participant, and experiment tables.
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
                measurement.*
            FROM measurement
            JOIN participant ON measurement.participant_id = participant.id
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE measurement.target = ? AND experiment.id = ?
        """
        cursor.execute(query, (target, axis, exp_id))
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns) 
    
    def get_measurement_by_id(self, measurement_id: int) -> Measurement | None:
        """
        Retrieves a measurement record by its ID.

        Args:
            measurement_id (int): The ID of the measurement to retrieve.

        Returns:
            Measurement | None: The Measurement object if found, otherwise None.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM measurement WHERE id = ?", (measurement_id,))
        row = cursor.fetchone()
        if row:
            return Measurement(
                id=row["id"],
                articipant_id=row["participant_id"],
                timepoint=row["timepoint"],
                device=row["device"],
                target=row["target"],
                axis=row["axis"],
                unit=row["unit"]
            )
        return None

    def get_measurement_target_by_id(self, measurement_id: int) -> str | None:
        """
        Retrieves a measurement target by its ID.

        Args:
            measurement_id (int): The ID of the measurement to retrieve.

        Returns:
            int | None: The name of the target, if found, otherwise None.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT target FROM measurement WHERE id = ?", (measurement_id,))
        row = cursor.fetchone()
        if row:
            return row[0]
        return None

# endregion Getter