from data_access.base_repository import BaseRepository
from models.measurement import Measurement

class MeasurementRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the MeasurementRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter

    def insert_measurement(self, measurement: Measurement) -> int:
        """
        Inserts a new measurement record into the 'measurement' table.

        Args:
            measurement (Measurement): The Measurement object containing the data to insert.

        Returns:
            int: The ID of the newly inserted measurement.
        """
        data = {
            "participant_id": measurement.participant_id,
            "timepoint": measurement.timepoint,
            "device": measurement.device,
            "target": measurement.target,
            "axis": measurement.axis,
            "unit": measurement.unit
        }
        return self.insert_one("measurement", data)
        
# endregion Setter

# region Getter
    def get_existing_measurement_ids(self) -> list[int]:
        """
        Retrieves all distinct measurement IDs from the 'measurement' table.

        Returns:
            list[int]: A list of measurement IDs. Empty list if none found.
        """
        rows = self.get_advanced(
            table_or_view="measurement",
            columns=["id"],
            distinct=True,
            return_df=False
        )
        return [row[0] for row in rows] if rows else []

# endregion Getter