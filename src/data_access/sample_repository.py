from data_access.base_repository import BaseRepository
from models.sample import Sample

class SampleRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the SampleRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_sample(self, sample: Sample) -> int:
        """
        Inserts a new sample record into the 'sample' table.

        Args:
            sample (Sample): The Sample object containing measurement_id, bow_stroke_start, and bow_stroke_end.

        Returns:
            int: The ID of the newly inserted sample record.
        """
        data = {
            "measurement_id": sample.measurement_id,
            "bow_stroke_start": int(sample.bow_stroke_start),
            "bow_stroke_end": int(sample.bow_stroke_end)
        }
        return self.insert_one("sample", data)

# region Getter
    def get_existing_samples(self) -> set[tuple]:
        """
        Retrieves distinct (measurement_id, bow_stroke_start) pairs from the 'sample' table.

        Returns:
            set[tuple]: A set of tuples where each tuple contains (measurement_id, bow_stroke_start).
                        Returns an empty set if no results are found.
        """
        results = self.get_advanced(
            table_or_view="sample",
            columns=["measurement_id", "bow_stroke_start"],
            distinct=True,
            return_df=False
        )
        if results is None:
            return set()
        return set(results)   
# endregion Getter