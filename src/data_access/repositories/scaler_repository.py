from data_access.repositories.base_repository import BaseRepository
from data_access.models.scaler import Scaler

class ScalerRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the ScalerRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_new_scaler(self, scaler: Scaler) -> int:
        """
        Inserts a new scaler record into the 'scaler' table.

        Args:
            scaler (Scaler): The Scaler object containing measurement_type_id, scaler_type, mean, and scale.

        Returns:
            int: The ID of the newly inserted scaler record.
        """
        data = {
            "measurement_type_id": scaler.measurement_type_id,
            "scaler_type": scaler.scaler_type,
            "mean": scaler.mean_to_json(),
            "scale": scaler.scale_to_json()
        }
        return self.insert_one("scaler", data)
    
# region Getter
    def get_scaler_by_meas_type_id_scaler_type(self, meas_type_id:int, scaler_type:str) -> Scaler | None:
        """
        Retrieves a Scaler object by measurement_type_id and scaler_type.

        Args:
            meas_type_id (int): The ID of the measurement type.
            scaler_type (str): The type of scaler.

        Returns:
            Scaler | None: Returns the Scaler object if found; otherwise, None.
        """
        result = self.get_advanced(
            table_or_view="scaler",
            measurement_type_id= meas_type_id, 
            scaler_type= scaler_type,
            return_df=False
        )
        if result:
            row = result[0]
            return Scaler(
                id=row[0],
                measurement_type_id=row[1],
                scaler_type=row[2],
                mean=Scaler.list_from_json(row[3]),
                scale=Scaler.list_from_json(row[4])
            )
        return None

# endregion Getter