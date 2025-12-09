from data_access.repositories.base_repository import BaseRepository
from data_access.models.pc_scores import PC_Scores
import pandas as pd

class PCScoresRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the PCScoresRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_many_pc_scores(self, pc_scores: list[PC_Scores])-> int:
        """
        Inserts multiple PC_Scores records into the 'pc_scores' table.

        Args:
            pc_scores (list[PC_Scores]): A list of PC_Scores model instances to be inserted.

        Returns:
            int: The number of inserted records or the result from insert_many (depending on implementation).
        """
        data_list = [{
            "pc_id": score.pc_id,
            "sample_id": score.sample_id,
            "pc_score": score.pc_score
        } for score in pc_scores]
        self.insert_many("pc_scores", data_list)

# endregion Setter

# region Getter
    def get_pc_scores_by_exp_id_device(self, exp_id:int, device:str, meas_timepoint:str, pain_groups:list[str], distribution_info:str | None = None, 
                                       rotation_type:str | None = None)-> pd.DataFrame | None:
        """
        Returns a DataFrame from the 'Participants PCs' table filtered by given criteria.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            meas_timepoint (str): Measurement time point.
            distribution_info (optional): Filter for distribution information.
            rotation_type (optional): Filter for rotation type.

        Returns:
            pd.DataFrame | None: Result dataframe or None if no matching rows found.
        """
        pain_group_names = [", ".join(pain_groups), ", ".join(reversed(pain_groups))]
        filters = {
            "exp_id": exp_id,
            "device": device,
            "meas_time_point": meas_timepoint,
            "pain_groups": pain_group_names
        }
        if distribution_info is not None:
            filters["distribution_info"] = distribution_info
        if rotation_type is not None:
            filters["rotation_type"] = rotation_type

        return self.get(table_or_view="[Participants PCs]", **filters)
   
    def get_rotated_pc_scores(self, exp_id:int, device:str, meas_timepoint:str, pain_group_names:list[str], nr_components:int | None =None, 
                                  use_distribution: bool = False, distribution_type: str | None = "normal_distribution") -> pd.DataFrame:
        """
        Retrieves rotated PC scores from the 'Participants PCs' table, 
        optionally limited by number of components and distribution type.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            meas_timepoint (str): Measurement time point.
            pain_group_names (list[str]): Names of pain groups.
            nr_components (int, optional): Limit to top N components.
            use_distribution (bool, optional): Whether to filter by distribution_info.
            distribution_type (str, optional): Distribution type to filter on if use_distribution=True.

        Returns:
            pd.DataFrame | None: DataFrame of rotated PC scores matching the criteria.
        """
        device_list = device if isinstance(device, list) else [device]
        tp_list = meas_timepoint if isinstance(meas_timepoint, list) else [meas_timepoint]
        
        device_ph = ",".join(["?"] * len(device_list))
        tp_ph = ",".join(["?"] * len(tp_list))
        pain_ph = ",".join(["?"] * len(pain_group_names))
        #placeholders = ','.join(['?'] * len(pain_group_names))

        distribution_clause = ""
        distribution_param = []
        if use_distribution:
            distribution_clause = "AND distribution_info = ?"
            distribution_param = [distribution_type]

        if nr_components is not None:
            query = f"""
                WITH top_pcs AS (
                    SELECT pc_id
                    FROM [Participants PCs]
                    WHERE exp_id = ? 
                    AND device IN ({device_ph})
                    AND meas_time_point IN ({tp_ph})
                    AND pain_groups IN ({pain_ph})
                    {distribution_clause}
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
            #params = [exp_id, device, meas_timepoint] + pain_group_names + distribution_param + [nr_components]
            params = (
                [exp_id]
                + device_list
                + tp_list
                + pain_group_names
                + distribution_param
                + [nr_components]
            )
        else:
            query = f"""
                SELECT *
                FROM [Participants PCs]
                WHERE exp_id = ? 
                AND device IN ({device_ph})
                AND meas_time_point IN ({tp_ph})
                AND pain_groups IN ({pain_ph})
                {distribution_clause}
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
            #params = [exp_id, device, meas_timepoint] + pain_group_names + distribution_param
            params = (
                [exp_id]
                + device_list
                + tp_list
                + pain_group_names
                + distribution_param
            )

        return self.get_raw_query(query, params)
        

    def get_unrotated_pc_scores(self, exp_id:int, device:str|list[str], meas_timepoint:str|list[str], pain_group_names:list[str], nr_components:int | None =None,
                                use_distribution: bool = False, distribution_type: str | None = "normal_distribution") -> pd.DataFrame:
        """
        Retrieves unrotated PC scores from the 'Participants PCs' table, optionally limited by number of components.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            meas_timepoint (str): Measurement time point.
            nr_components (int, optional): Limit to top N components based on absolute t_value.

        Returns:
            pd.DataFrame | None: DataFrame of unrotated PC scores matching the criteria.
        """
        # TODO: adjust the selection for distribution here
        device_list = device if isinstance(device, list) else [device]
        tp_list = meas_timepoint if isinstance(meas_timepoint, list) else [meas_timepoint]
        
        device_ph = ",".join(["?"] * len(device_list))
        tp_ph = ",".join(["?"] * len(tp_list))
        pain_ph = ','.join(['?'] * len(pain_group_names))
        
        distribution_clause = ""
        distribution_param = []
        if use_distribution:
            distribution_clause = "AND distribution_info = ?"
            distribution_param = [distribution_type]
        
        if nr_components is not None:
            query = f"""
                WITH top_pcs AS (
                    SELECT pc_id
                    FROM [Participants PCs]
                    WHERE 
                    exp_id = ? 
                    AND device IN ({device_ph})
                    AND meas_time_point IN ({tp_ph})
                    AND pain_groups IN ({pain_ph})
                    {distribution_clause}
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
            params = (
                [exp_id]
                + device_list
                + tp_list
                + pain_group_names
                + distribution_param
                + [nr_components]
            )
        else:
            query = f"""
                SELECT *
                FROM [Participants PCs]
                WHERE exp_id = ?
                    AND device IN ({device_ph})
                    AND meas_time_point IN ({tp_ph})
                    AND pain_groups IN ({pain_ph})
                    {distribution_clause}
                    AND rotation_type = 'unrotated'
                ORDER BY ABS(t_value) DESC
            """
            params = (
                [exp_id]
                + device_list
                + tp_list
                + pain_group_names
                + distribution_param
            )

        return self.get_raw_query(query, params)
    
# endregion Getter