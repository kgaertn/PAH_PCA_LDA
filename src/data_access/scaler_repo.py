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

    def get_existing_samples(self):
        """
        Returns a set of (measurement_id, bow_stroke_start) tuples from the sample table.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT measurement_id, bow_stroke_start FROM sample
        """)
        results = cursor.fetchall()
        return set(results) 
# endregion Getter