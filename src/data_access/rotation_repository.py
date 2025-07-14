import pandas as pd
from db.connection import get_connection
from models.rotation import RotationPCA

class RotationRepository:
    def __init__(self):
        """
        Initializes the MeasurementRepository with a database connection.
        """
        self.conn = get_connection()

    def get_existing_rotations(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM pca_rotation
        """)
        rows = cursor.fetchone()
        if rows:
            return [RotationPCA(id=row["id"], rotation_type=row["rotation_type"]) for row in rows]
        return None