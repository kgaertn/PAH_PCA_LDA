import pandas as pd
from db.connection import get_connection
from models.scaler import Scaler

class ScalerRepository:
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        self.conn = get_connection()

# region Setter
    def insert_new_scaler(self, scaler: Scaler) -> int:
        """
        Inserts a new scaler into the database.

        Args:
            scaler (Scaler): The scaler object containing X.

        Returns:
            int: The database ID of the newly inserted sample.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO scaler (measurement_type_id, scaler_type, mean, scale)
            VALUES (?, ?, ?, ?)
        """, (scaler.measurement_type_id, scaler.scaler_type, scaler.mean_to_json(), scaler.scale_to_json()))
        self.conn.commit()
        return cursor.lastrowid

# region Getter
# use these functions to access data from the experiment table, depending on the needs
# TODO

    def get_sacler_by_meas_type_id(self, meas_type_id):
        """
        Returns a set of (measurement_id, bow_stroke_start) tuples from the sample table.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT * FROM scaler WHERE measurement_type_id = ?;""",
                       (meas_type_id,))
        row = cursor.fetchone()
        if row:
            return Scaler(id=row[0], measurement_type_id=row[1], scaler_type=row[2], 
                              mean = Scaler.list_from_json(row[3]), scale = Scaler.list_from_json(row[4]))
        return None
    
    def get_sacler_by_meas_type_id_scaler_type(self, meas_type_id, scaler_type):
        """
        Returns a set of (measurement_id, bow_stroke_start) tuples from the sample table.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT * FROM scaler WHERE measurement_type_id = ? AND scaler_type = ?;""",
                       (meas_type_id,scaler_type))
        row = cursor.fetchone()
        if row:
            return Scaler(id=row[0], measurement_type_id=row[1], scaler_type=row[2], 
                              mean = Scaler.list_from_json(row[3]), scale = Scaler.list_from_json(row[4]))
        return None
# endregion Getter