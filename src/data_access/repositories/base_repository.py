import pandas as pd
from typing import Any

from data_access.db.connection import get_connection

class BaseRepository:
    def __init__(self):
        """
        Initializes the Repository with a database connection.
        """
        self.conn = get_connection()
# region Setter
    def insert_one(self, table: str, data: dict) -> int:
        """
        Generic insert one method: Insert a single record into the specified table.

        Args:
            table (str): Name of the table to insert into.
            data (dict): Dictionary of column names and corresponding values to insert.

        Returns:
            int: The row ID of the inserted record.
        """
        cursor = self.conn.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = tuple(data.values())
        query = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        self.conn.commit()
        return cursor.lastrowid

    def insert_many(self, table: str, data_list: list[dict]) -> None:
        """
        Generic insert many method: Insert multiple records into the specified table in a batch operation.

        Args:
            table (str): Name of the table to insert into.
            data_list (list[dict]): List of dictionaries, each representing a row to insert.
        Returns:
            None : When data_list is empty
        """
        if not data_list:
            return

        cursor = self.conn.cursor()
        columns = ', '.join(data_list[0].keys())
        placeholders = ', '.join(['?'] * len(data_list[0]))
        values_list = [tuple(data.values()) for data in data_list]
        query = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.executemany(query, values_list)
        self.conn.commit()
    
    def update(self, table: str, values: dict[str, any], where: dict[str, any]):
        """
        Generic update method: Update rows in the specified table matching the conditions.

        Args:
            table (str): Name of the table to update.
            values (dict): Dictionary of columns and their new values.
            where (dict): Dictionary specifying the WHERE conditions (column-value pairs).
        """
        cursor = self.conn.cursor()

        set_clause = ', '.join([f"{col} = ?" for col in values.keys()])
        where_clause = ' AND '.join([f"{col} = ?" for col in where.keys()])

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        params = tuple(values.values()) + tuple(where.values())

        cursor.execute(query, params)
        self.conn.commit()
        
    def update_many(self, table: str, values_list: list[dict], where_keys: list[str]) -> None:
        """
        Generic update many method: Perform multiple update operations efficiently using executemany.

        Args:
            table (str): Table name to update.
            values_list (list[dict]): List of dictionaries containing columns to set and keys for WHERE conditions.
            where_keys (list[str]): List of keys in the dictionaries used for WHERE clause.

        Returns:
            None : When values_list is empty
        """
        cursor = self.conn.cursor()

        if not values_list:
            return

        set_keys = [k for k in values_list[0].keys() if k not in where_keys]
        set_clause = ', '.join([f"{col} = ?" for col in set_keys])
        where_clause = ' AND '.join([f"{col} = ?" for col in where_keys])

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        param_tuples = []
        for vals in values_list:
            set_vals = [vals[k] for k in set_keys]
            where_vals = [vals[k] for k in where_keys]
            param_tuples.append(tuple(set_vals + where_vals))

        cursor.executemany(query, param_tuples)
        self.conn.commit()
# endregion Setter

# region Getter
    def select_with_filters(self, table_or_view: str, filters: dict, columns: list[str] | None = None) -> pd.DataFrame | None:
        """
        Generic select method: Perform a SELECT query with WHERE conditions combined by AND, supporting '=' and 'IN'.

        Args:
            table_or_view (str): Table or view to query.
            filters (dict): Mapping of column names to filter values or lists of values.
            columns (list[str], optional): Columns to select. Defaults to all columns.

        Returns:
            pd.DataFrame | None: Resulting data as DataFrame or None if no results found.
        """
        cursor = self.conn.cursor()
        if columns is None:
            columns = ['*']

        select_clause = ", ".join(columns)

        where_clauses = []
        params = []

        for col, val in filters.items():
            if isinstance(val, (list, tuple)):
                placeholders = ','.join(['?'] * len(val))
                where_clauses.append(f"{col} IN ({placeholders})")
                params.extend(val)
            else:
                where_clauses.append(f"{col} = ?")
                params.append(val)

        where_statement = ""
        if where_clauses:
            where_statement = " WHERE " + " AND ".join(where_clauses)

        query = f"SELECT {select_clause} FROM {table_or_view}{where_statement}"

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def get_by_filter(self, table: str, filters: dict[str, Any]) -> pd.DataFrame:
        """
        Generic select method: Retrieve rows from the specified table applying filter conditions.

        Args:
            table (str): Name of the table or view to query.
            filters (dict[str, Any]): Dictionary of column-value filters.
                Values can be single values or iterables for IN-clauses.

        Returns:
            pd.DataFrame: Resulting rows as a DataFrame. Empty if no matches.
        """
        cursor = self.conn.cursor()

        where_clauses = []
        values = []

        for col, val in filters.items():
            if isinstance(val, (list, tuple, set)):
                placeholders = ', '.join(['?'] * len(val))
                where_clauses.append(f"{col} IN ({placeholders})")
                values.extend(val)
            else:
                where_clauses.append(f"{col} = ?")
                values.append(val)

        where_sql = " AND ".join(where_clauses)
        query = f"SELECT * FROM [{table}]"
        if where_clauses:
            query += f" WHERE {where_sql}"

        cursor.execute(query, values)
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()

        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def select_advanced(self, table_or_view: str, filters: dict[str, Any] | None = None, exclude: dict[str, Any] | None = None, columns: list[str] | None = None,
                        distinct: bool = False, order_by: list[str] = None, return_df: bool = True) -> pd.DataFrame | list[tuple] | None:
        """
        Generic select method: Perform an advanced SELECT query supporting filters, exclusions, distinct, ordering, and column selection.

        Args:
            table_or_view (str): Table or view name to query.
            filters (dict[str, Any], optional): Column-value pairs for filtering using '=' or 'IN'.
            exclude (dict[str, Any], optional): Column-value pairs for exclusion using '!=' or 'NOT IN'.
            columns (list[str], optional): List of columns to select. Defaults to all columns.
            distinct (bool, optional): Whether to apply DISTINCT to results.
            order_by (list[str], optional): List of columns to order results by.
            return_df (bool, optional): Whether to return a DataFrame (True) or list of tuples (False).

        Returns:
            pd.DataFrame | list[tuple] | None: Query results or None if no results.
        """
        filters = filters or {}
        exclude = exclude or {}

        cursor = self.conn.cursor()
        cols = ", ".join(columns) if columns else "*"
        select_clause = f"SELECT {'DISTINCT ' if distinct else ''}{cols} FROM {table_or_view}"

        where_clauses = []
        params = []

        for col, val in filters.items():
            if isinstance(val, (list, tuple, set)):
                placeholders = ', '.join(['?'] * len(val))
                where_clauses.append(f"{col} IN ({placeholders})")
                params.extend(val)
            else:
                where_clauses.append(f"{col} = ?")
                params.append(val)

        for col, val in exclude.items():
            if isinstance(val, (list, tuple, set)):
                placeholders = ', '.join(['?'] * len(val))
                where_clauses.append(f"{col} NOT IN ({placeholders})")
                params.extend(val)
            else:
                where_clauses.append(f"{col} != ?")
                params.append(val)

        query = select_clause
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        if order_by:
            query += " ORDER BY " + ", ".join(order_by)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return None

        if return_df:
            column_names = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=column_names)
        return rows
    
    def select_with_or(self, table_or_view: str, or_filters: dict[str, Any], columns: list[str] | None = None, return_df: bool = True
                       ) -> pd.DataFrame | list[tuple] | None:
        """
        Generic select method: Perform a SELECT query with OR-combined filter conditions.

        Args:
            table_or_view (str): Table or view name to query.
            or_filters (dict[str, Any]): Dictionary of column-value pairs combined with OR.
            columns (list[str], optional): Columns to select. Defaults to all columns.
            return_df (bool, optional): Whether to return results as a DataFrame or raw tuples.

        Returns:
            pd.DataFrame | list[tuple] | None: Query results or None if no rows match.
        """
        cursor = self.conn.cursor()
        cols = ", ".join(columns) if columns else "*"

        where_clauses = []
        params = []

        for col, val in or_filters.items():
            if isinstance(val, (list, tuple, set)):
                placeholders = ', '.join(['?'] * len(val))
                where_clauses.append(f"{col} IN ({placeholders})")
                params.extend(val)
            else:
                where_clauses.append(f"{col} = ?")
                params.append(val)

        where_clause = " OR ".join(where_clauses)
        query = f"SELECT {cols} FROM {table_or_view} WHERE {where_clause}"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return None

        if return_df:
            column_names = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=column_names)

        return rows
    
    def get(self, table_or_view, columns: list[str] | None = None, **filters) -> pd.DataFrame | None:
        """
        Generic select method: Shortcut to select rows with AND-filters from the specified table or view.

        Args:
            table_or_view (str): Table or view to query.
            columns (list[str], optional): Columns to select. Defaults to all.
            **filters: Filter conditions as column=value pairs.

        Returns:
            pd.DataFrame | None: Resulting rows or None if empty.
        """
        return self.select_with_filters(
            table_or_view=table_or_view,
            filters=filters,
            columns=columns
        )
        
    def get_advanced(self, table_or_view: str, columns: list[str] | None = None, exclude: dict[str, Any] | None = None, distinct: bool = True,
                     order_by: list[str] | None = None, return_df: bool = True, **filters) -> pd.DataFrame | list[tuple] | None:
        """
        Wrapper for advanced SELECT with keyword filter arguments.

        Args:
            table_or_view (str): Table or view to query.
            columns (list[str], optional): Columns to select.
            exclude (dict[str, Any], optional): Filters for exclusion.
            distinct (bool, optional): Apply DISTINCT clause.
            order_by (list[str], optional): Columns to sort by.
            return_df (bool, optional): Return a DataFrame if True, else list of tuples.
            **filters: Column filters as keyword arguments.

        Returns:
            pd.DataFrame | list[tuple] | None: Query results or None if empty.
        """
        return self.select_advanced(
            table_or_view=table_or_view,
            filters=filters,
            exclude=exclude,
            columns=columns,
            distinct=distinct,
            order_by=order_by,
            return_df=return_df
        )
    
    def get_raw_query(self, query: str, params: tuple = (), return_df: bool = True):
        """
        Execute a raw SQL query with optional parameters.

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Parameters to safely substitute in the query.
            return_df (bool, optional): Whether to return a DataFrame or raw tuples.

        Returns:
            pd.DataFrame | list[tuple] | None: Query results or None if no rows.
        """
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        if not rows:
            return None
        columns = [desc[0] for desc in cursor.description]
        if return_df:
            return pd.DataFrame(rows, columns=columns)
        else:
            return rows
        
    def get_column_names(self, table_name: str) -> list[str]:
        """
        Retrieve the column names for a specified table.

        Args:
            table_name (str): Name of the table.

        Returns:
            list[str]: List of column names in the table.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()
        return [row[1] for row in rows] if rows else []
# endregion Getter