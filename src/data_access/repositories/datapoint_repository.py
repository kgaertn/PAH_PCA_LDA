from data_access.repositories.base_repository import BaseRepository
from data_access.models.datapoint import Datapoint

class DatapointRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the DatapointRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_datapoint(self, datapoint: Datapoint) -> int:
        """
        Inserts a single datapoint record into the 'datapoint' table.

        Args:
            datapoint (Datapoint): The Datapoint object to be inserted.

        Returns:
            int: The ID of the newly inserted datapoint.
        """
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
        """
        Inserts multiple datapoint records into the 'datapoint' table.

        Args:
            datapoints (list[Datapoint]): A list of Datapoint objects to be inserted.
        """
        data_list = [{
            "measurement_id": dp.measurement_id,
            "bow_stroke": dp.bow_stroke,
            "up_down": dp.up_down,
            "key": dp.key,
            "time_point": dp.time_point,
            "value": dp.value,
            "sample_id":dp.sample_id
        } for dp in datapoints]
        self.insert_many("datapoint", data_list)

    def update_datapoints_sample(self, meas_id: int, bow_stroke_start:int, bow_stroke_end:int, sample_id:int):
        """
        Updates the sample_id for datapoints of a given measurement where bow_stroke matches start or end.

        Args:
            meas_id (int): The measurement ID to filter datapoints.
            bow_stroke_start(int): The bow stroke value representing the start.
            bow_stroke_end(int): The bow stroke value representing the end.
            sample_id(int): The sample ID to set.
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

