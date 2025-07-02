import pandas as pd
from db.connection import get_connection
from models.pc_scores import PC_Scores

class PCScoresRepository:
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        self.conn = get_connection()

# region Setter
    def insert_multiple_pc_scores(self, pc_scores: list[PC_Scores]) -> int:
        """
        Inserts a new pc into the database.

        Args:
            scaler (Scaler): The scaler object containing X.

        Returns:
            int: The database ID of the newly inserted sample.
        """
        cursor = self.conn.cursor()
        cursor.executemany("""
        INSERT INTO pc_scores (pc_id, sample_id, pc_score)
        VALUES (?, ?, ?)
        """, [(score.pc_id, score.sample_id, score.pc_score) for score in pc_scores])
        self.conn.commit()

# region Getter
# use these functions to access data from the experiment table, depending on the needs
# TODO

    def get_pc_scores_by_exp_id_device(self, exp_id, device, meas_timepoint):
        """
        Returns a Dataframe from the Participants PCs table.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT * FROM [Participants PCs]
            WHERE exp_id = ? AND device = ? AND meas_time_point = ?
        """
        cursor.execute(query, (exp_id, device, meas_timepoint))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def get_pc_scores_by_tp_rank(self, exp_id, device, meas_timepoint, min_rank, max_rank):
        """
        Returns a Dataframe from the Participants PCs table.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT * FROM [Participants PCs]
            WHERE exp_id = ? AND device = ? AND meas_time_point = ? AND rank BETWEEN ? AND ?
            ORDER BY rank
        """
        cursor.execute(query, (exp_id, device, meas_timepoint, min_rank, max_rank))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
# endregion Getter