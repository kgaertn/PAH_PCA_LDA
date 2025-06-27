import pandas as pd
from db.connection import get_connection
from models.pc_ranked import PC_Ranked

class PCRankedRepository:
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        self.conn = get_connection()

# region Setter
    def insert_new_pc(self, pc: PC_Ranked) -> int:
        """
        Inserts a new pc into the database.

        Args:
            scaler (Scaler): The scaler object containing X.

        Returns:
            int: The database ID of the newly inserted sample.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO pcs_ranked (measurement_type_id, pc_index, loading_vector, explained_variance, data_scaled)
            VALUES (?, ?, ?, ?, ?)
        """, (pc.measurement_type_id, pc.pc_index, pc.loading_vector_to_json(), pc.explained_variance, pc.data_scaled))
        self.conn.commit()
        return cursor.lastrowid

# region Getter
# use these functions to access data from the experiment table, depending on the needs
# TODO


# endregion Getter