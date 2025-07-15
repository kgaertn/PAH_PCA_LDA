import pandas as pd
from db.connection import get_connection
from models.pain_group import PainGroup

class RotationRepository:
    def __init__(self):
        """
        Initializes the MeasurementRepository with a database connection.
        """
        self.conn = get_connection()

    def get_existing_pain_groups(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM pain_group
        """)
        rows = cursor.fetchone()
        if rows:
            return [PainGroup(id=row["id"], rotation_type=row["pain_group"]) for row in rows]
        return None