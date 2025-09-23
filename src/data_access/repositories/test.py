        """
        Retrieves rotated PC scores from the 'Participants PCs' table, optionally limited by number of components.

        Args:
            exp_id (int): Experiment ID.
            device (str): Device name.
            meas_timepoint (str): Measurement time point.
            nr_components (int, optional): Limit to top N components based on absolute t_value.

        Returns:
            pd.DataFrame | None: DataFrame of rotated PC scores matching the criteria.
        """
        # TODO: adjust the selection for distribution here
        placeholders = ','.join(['?'] * len(pain_group_names)) 
        
        if nr_components is not None:
            query = f"""
                WITH top_pcs AS (
                    SELECT pc_id
                    FROM [Participants PCs]
                    WHERE
                    distribution_info = 'normal_distribution' 
                    AND exp_id = ? 
                    AND device = ? 
                    AND meas_time_point = ?
                    AND pain_groups IN ({placeholders})
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
            params = [exp_id, device, meas_timepoint] + pain_group_names + [nr_components]
        else:
            query = f"""
                SELECT *
                FROM [Participants PCs]
                WHERE exp_id = ? 
                AND device = ? 
                AND meas_time_point = ? 
                AND pain_groups IN ({placeholders})
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
            params = [exp_id, device, meas_timepoint] + pain_group_names

        return self.get_raw_query(query, params)
    