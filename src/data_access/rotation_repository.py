from data_access.base_repository import BaseRepository
from models.rotation import RotationPCA

class RotationRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the RotationRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

    def insert_rotation_type(self, rotation_type: str) -> int:
        """
        Inserts a new rotation type into the 'pca_rotation' table.

        Args:
            rotation_type (str): The name/type of the rotation to insert.

        Returns:
            int: The ID of the newly inserted rotation type.
        """
        data = {
            "rotation_type": rotation_type
        }
        return self.insert_one("pca_rotation", data)

    def get_existing_rotations(self) -> list[RotationPCA] | None:
        """
        Retrieves all existing rotation entries from the 'pca_rotation' table.

        Returns:
            list[RotationPCA] | None: A list of RotationPCA objects representing existing rotations,
                                     or None if no entries are found.
        """
        rows = self.get_advanced(
            table_or_view="pca_rotation",
            return_df=False
        )
        if rows:
            return [RotationPCA(id=row[0], rotation_type=row[1]) for row in rows]
        return None
