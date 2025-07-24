import pandas as pd
from db.connection import get_connection
from data_access.base_repository import BaseRepository
from models.pc_scores import PC_Scores

class PCScoresRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the ExperimentRepository with a database connection.
        """
        super().__init__()
        #self.conn = get_connection()

# region Setter
    def insert_many_pc_scores(self, pc_scores: list[PC_Scores])-> int:
        data_list = [{
            "pc_id": score.pc_id,
            "sample_id": score.sample_id,
            "pc_score": score.pc_score
        } for score in pc_scores]
        self.insert_many("pc_scores", data_list)

    #def insert_multiple_pc_scores(self, pc_scores: list[PC_Scores]) -> int:
    #    """
    #    Inserts a new pc into the database.
#
    #    Args:
    #        scaler (Scaler): The scaler object containing X.
#
    #    Returns:
    #        int: The database ID of the newly inserted sample.
    #    """
    #    cursor = self.conn.cursor()
    #    cursor.executemany("""
    #    INSERT INTO pc_scores (pc_id, sample_id, pc_score)
    #    VALUES (?, ?, ?)
    #    """, [(score.pc_id, score.sample_id, score.pc_score) for score in pc_scores])
    #    self.conn.commit()

# region Getter
# use these functions to access data from the experiment table, depending on the needs
# TODO

    def get_pc_scores_by_exp_id_device(self, exp_id, device, meas_timepoint, distribution_info=None, rotation_type=None):
        """
        Returns a DataFrame from the Participants PCs table filtered by given criteria.

        Args:
            exp_id (int): Experiment ID
            device (str): Device name
            meas_timepoint (str): Measurement time point
            distribution_info (optional): Distribution info filter
            rotation_type (optional): Rotation type filter

        Returns:
            pd.DataFrame | None: Result dataframe or None if no rows found.
        """
        filters = {
            "exp_id": exp_id,
            "device": device,
            "meas_time_point": meas_timepoint
        }
        if distribution_info is not None:
            filters["distribution_info"] = distribution_info
        if rotation_type is not None:
            filters["rotation_type"] = rotation_type

        return self.get(table_or_view="[Participants PCs]", **filters)
   
    def get_rotated_pc_scores(self, exp_id, device, meas_timepoint, nr_components=None):
        if nr_components is not None:
            query = """
                WITH top_pcs AS (
                    SELECT pc_id
                    FROM [Participants PCs]
                    WHERE exp_id = ? 
                    AND device = ? 
                    AND meas_time_point = ?
                    AND (
                        rotation_type != 'unrotated'
                        OR (
                            rotation_type = 'unrotated'
                            AND pc_id NOT IN (
                                SELECT parent_id
                                FROM [Participants PCs]
                                WHERE parent_id IS NOT NULL
                            )
                        )
                    )
                    GROUP BY pc_id
                    ORDER BY MAX(ABS(t_value)) DESC
                    LIMIT ?
                )
                SELECT *
                FROM [Participants PCs]
                WHERE pc_id IN (SELECT pc_id FROM top_pcs)
                ORDER BY ABS(t_value) DESC
            """
            params = [exp_id, device, meas_timepoint, nr_components]
        else:
            query = """
                SELECT *
                FROM [Participants PCs]
                WHERE exp_id = ? 
                AND device = ? 
                AND meas_time_point = ? 
                AND (
                    rotation_type != 'unrotated'
                    OR (
                        rotation_type = 'unrotated'
                        AND pc_id NOT IN (
                            SELECT parent_id
                            FROM [Participants PCs]
                            WHERE parent_id IS NOT NULL
                        )
                    )
                )
                ORDER BY ABS(t_value) DESC
            """
            params = [exp_id, device, meas_timepoint]

        return self.get_raw_query(query, params)
    
    def get_unrotated_pc_scores(self, exp_id, device, meas_timepoint, nr_components=None):
        if nr_components is not None:
            query = """
                WITH top_pcs AS (
                    SELECT pc_id
                    FROM [Participants PCs]
                    WHERE exp_id = ? 
                    AND device = ? 
                    AND meas_time_point = ?
                    AND rotation_type = 'unrotated'
                    GROUP BY pc_id
                    ORDER BY MAX(ABS(t_value)) DESC
                    LIMIT ?
                )
                SELECT *
                FROM [Participants PCs]
                WHERE pc_id IN (SELECT pc_id FROM top_pcs)
                ORDER BY ABS(t_value) DESC
            """
            params = [exp_id, device, meas_timepoint, nr_components]
        else:
            query = """
                SELECT *
                FROM [Participants PCs]
                WHERE exp_id = ?
                AND device = ?
                AND meas_time_point = ?
                AND rotation_type = 'unrotated'
                ORDER BY ABS(t_value) DESC
            """
            params = [exp_id, device, meas_timepoint]

        return self.get_raw_query(query, params)
   
    #def get_pc_scores_by_tp_rank(self, exp_id, device, meas_timepoint, min_rank, max_rank, select_rotated=False):
    #    if select_rotated:
    #        query = """
    #            SELECT *
    #            FROM [Participants PCs]
    #            WHERE exp_id = ? 
    #            AND device = ? 
    #            AND meas_time_point = ? 
    #            AND (
    #                rotation_type != 'unrotated'
    #                OR (
    #                    rotation_type = 'unrotated'
    #                    AND pc_id NOT IN (
    #                        SELECT parent_id
    #                        FROM [Participants PCs]
    #                        WHERE parent_id IS NOT NULL
    #                    )
    #                )
    #            )
    #            AND rank BETWEEN ? AND ?
    #            ORDER BY ABS(t_value) DESC;
    #        """
    #    else:
    #        query = """
    #            SELECT *
    #            FROM [Participants PCs]
    #            WHERE exp_id = ? 
    #            AND device = ? 
    #            AND meas_time_point = ?
    #            AND rotation_type = 'unrotated'
    #            AND rank BETWEEN ? AND ?
    #            ORDER BY ABS(t_value) DESC;
    #        """
#
    #    return self.conn.execute(query, (exp_id, device, meas_timepoint, min_rank, max_rank)).fetchall()
   #
   #
   #
    #def get_pc_scores_by_tp_rank(self, exp_id, device, meas_timepoint, min_rank, max_rank, select_rotated = False):
    #    query = """
    #        SELECT *
    #        FROM [Participants PCs]
    #        WHERE exp_id = ? AND device = ? AND meas_time_point = ?
    #        ORDER BY ABS(t_value) DESC
    #        LIMIT 10;
    #    
    #        SELECT * FROM [Participants PCs]
    #        WHERE exp_id = ? AND device = ? AND meas_time_point = ? AND rank BETWEEN ? AND ?
    #        ORDER BY rank
    #    """
    #    params = (exp_id, device, meas_timepoint, min_rank, max_rank)
    #    return self.get_raw_query(query, params)
# endregion Getter