import pandas as pd
from db.connection import get_connection
from data_access.base_repository import BaseRepository
from models.rotation import RotationPCA

class RotationRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the MeasurementRepository with a database connection.
        """
        super().__init__()
        #self.conn = get_connection()

    def insert_rotation_type(self, rotation_type: str) -> int:
        data = {
            "rotation_type": rotation_type
        }
        return self.insert_one("pca_rotation", data)

    def get_existing_rotations(self) -> list[RotationPCA] | None:
        """
        Retrieves all existing pain groups from the database.

        Returns:
            list[PainGroup] | None: A list of PainGroup objects, or None if no entries exist.
        """
        rows = self.get_advanced(
            table_or_view="pca_rotation",
            return_df=False
        )
        if rows:
            return [RotationPCA(id=row[0], rotation_type=row[1]) for row in rows]
        return None

    #def get_existing_rotations(self):
    #    cursor = self.conn.cursor()
    #    cursor.execute("""
    #        SELECT * FROM pca_rotation
    #    """)
    #    rows = cursor.fetchone()
    #    if rows:
    #        return [RotationPCA(id=row["id"], rotation_type=row["rotation_type"]) for row in rows]
    #    return None