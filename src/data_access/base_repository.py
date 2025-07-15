import pandas as pd
from typing import Any

from db.connection import get_connection

class BaseRepository:
    def __init__(self):
        #self.table_or_view = table_or_view
        self.conn = get_connection()

    def insert_one(self, table: str, data: dict):
        """
        Insert a single record into the specified table.

        Args:
            table (str): The table name.
            data (dict): A dictionary of column names and values.
        """
        cursor = self.conn.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = tuple(data.values())
        query = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        self.conn.commit()
        return cursor.lastrowid

    def insert_many(self, table: str, data_list: list[dict]):
        """
        Insert multiple records into the specified table in one batch.

        Args:
            table (str): The table name.
            data_list (list[dict]): A list of dicts representing rows to insert.
        """
        if not data_list:
            return  # nothing to insert

        cursor = self.conn.cursor()
        columns = ', '.join(data_list[0].keys())
        placeholders = ', '.join(['?'] * len(data_list[0]))
        values_list = [tuple(data.values()) for data in data_list]
        query = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.executemany(query, values_list)
        self.conn.commit()
    
    def update(self, table: str, values: dict, where: dict):
        """
        Generic update function.

        Args:
            table (str): Table name to update.
            values (dict): Columns and their new values to set.
            where (dict): Conditions for the WHERE clause (column=value).
        """
        cursor = self.conn.cursor()

        set_clause = ', '.join([f"{col} = ?" for col in values.keys()])
        where_clause = ' AND '.join([f"{col} = ?" for col in where.keys()])

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        params = tuple(values.values()) + tuple(where.values())

        cursor.execute(query, params)
        self.conn.commit()
        
    def update_many(self, table: str, values_list: list[dict], where_keys: list[str]):
        """
        Perform multiple UPDATEs using executemany.

        Args:
            table (str): Table name.
            values_list (list[dict]): Each dict contains both values to update and keys to filter (WHERE).
            where_keys (list[str]): Keys to use for WHERE clause.
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
    
    def select_with_filters(self, table_or_view: str, filters: dict, columns: list[str] = None) -> pd.DataFrame | None:
        """
        Generic SELECT with dynamic WHERE clause supporting '=' and 'IN' conditions.

        Args:
            table_or_view (str): Table or view name to query.
            filters (dict): Dictionary of column -> value or list of values.
                            - If value is list/tuple -> generate IN (...)
                            - else generate column = ?
            columns (list[str], optional): List of columns to select. Defaults to ['*'].

        Returns:
            pd.DataFrame | None: Result dataframe or None if no results.
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
        Führt eine SELECT-Abfrage gegen die angegebene Tabelle/View mit optionalen Filtern durch.

        Args:
            table (str): Tabellen- oder View-Name.
            filters (dict): Spaltennamen und ihre Filterwerte.

        Returns:
            pd.DataFrame: Resultierende Daten.
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
    
    def select_advanced(
        self,
        table_or_view: str,
        filters: dict[str, Any] = None,
        exclude: dict[str, Any] = None,
        columns: list[str] = None,
        distinct: bool = False,
        order_by: list[str] = None,
        return_df: bool = True,
    ) -> pd.DataFrame | list[tuple] | None:
        """
        Erweiterte generische SELECT-Methode mit Support für:
        - '=' und 'IN' Filter (via `filters`)
        - '!=' und 'NOT IN' Filter (via `exclude`)
        - DISTINCT
        - ORDER BY
        - Auswahl einzelner Spalten
        - Rückgabe als DataFrame oder raw tuples

        Args:
            table_or_view (str): Tabelle oder View-Name.
            filters (dict): Spalten und Werte für '=' bzw. 'IN'.
            exclude (dict): Spalten und Werte für '!=' bzw. 'NOT IN'.
            columns (list): Spaltenauswahl, Default: ['*'].
            distinct (bool): Ob DISTINCT verwendet wird.
            order_by (list): Spaltennamen zum Sortieren.
            return_df (bool): Ob ein DataFrame zurückgegeben wird.

        Returns:
            DataFrame, List of Tuples oder None
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
    
    def select_with_or(
        self,
        table_or_view: str,
        or_filters: dict[str, Any],
        columns: list[str] = None,
        return_df: bool = True
    ) -> pd.DataFrame | list[tuple] | None:
        """
        Führt eine SELECT-Abfrage mit OR-verknüpften Bedingungen durch.

        Args:
            table_or_view (str): Name der Tabelle oder View.
            or_filters (dict): Spaltennamen als Schlüssel, Vergleichswerte als Werte.
            columns (list[str], optional): Liste der zurückzugebenden Spalten. Default: ['*'].
            return_df (bool): Ob ein DataFrame zurückgegeben wird.

        Returns:
            DataFrame oder Liste von Tupeln oder None
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
    
    def get(self, table_or_view, columns: list[str] = None, **filters) -> pd.DataFrame | None:
        return self.select_with_filters(
            table_or_view=table_or_view,
            filters=filters,
            columns=columns
        )
        
    def get_advanced(self, table_or_view: str, columns: list[str] = None, exclude: dict[str, Any] = None, distinct: bool = True,
        order_by: list[str] = None, return_df: bool = True, **filters) -> pd.DataFrame | list[tuple] | None:
        """
        Wrapper für `select_advanced` mit vereinfachtem Aufruf über Keyword-Filters.

        Args:
            table_or_view (str): Tabelle oder View.
            columns (list[str], optional): Spaltenauswahl.
            exclude (dict, optional): NOT-Filter.
            distinct (bool): Ob DISTINCT verwendet wird.
            order_by (list[str], optional): Sortierung.
            return_df (bool): Ob DataFrame zurückgegeben wird.
            **filters: Beliebige weitere Filter als Keyword-Argumente.

        Returns:
            pd.DataFrame | list[tuple] | None
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
        Returns a list of column names for the given table.

        Args:
            table_name (str): Name of the table.

        Returns:
            list[str]: A list of column names.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()
        return [row[1] for row in rows] if rows else []