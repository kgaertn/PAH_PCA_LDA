import pandas as pd
from db.connection import get_connection
from data_access.base_repository import BaseRepository
from models.sample import Sample

class SampleRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        super().__init__()
        #self.conn = get_connection()

# region Setter
    def insert_sample(self, sample: Sample) -> int:
        data = {
            "measurement_id": sample.measurement_id,
            "bow_stroke_start": int(sample.bow_stroke_start),
            "bow_stroke_end": int(sample.bow_stroke_end)
        }
        return self.insert_one("sample", data)

    #def insert_sample_by_measurement_id(self, sample: Sample) -> int:
    #    """
    #    Inserts a new sample into the database.
#
    #    Args:
    #        sample (Sample): The sample object containing id and measurement_id.
#
    #    Returns:
    #        int: The database ID of the newly inserted sample.
    #    """
    #    cursor = self.conn.cursor()
    #    cursor.execute("""
    #        INSERT OR IGNORE INTO sample (measurement_id, bow_stroke_start, bow_stroke_end)
    #        VALUES (?, ?, ?)
    #    """, (sample.measurement_id, int(sample.bow_stroke_start), int(sample.bow_stroke_end)))
    #    self.conn.commit()
    #    return cursor.lastrowid

# region Getter
# use these functions to access data from the experiment table, depending on the needs
# TODO
    def get_existing_samples(self) -> set[tuple]:
        """
        Returns a set of (measurement_id, bow_stroke_start) tuples from the sample table.
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