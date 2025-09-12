import sqlite3
from sqlite3 import Connection
import threading

# Thread-safe Singleton for DB-connection
_connection = None
_connection_lock = threading.Lock()

def get_connection(db_path: str = 'data/PAH_database.db') -> Connection:
    """
    Returns Singleton-Database-Connection
    If no connection exists, a new one is initiated.
    Args:
        db_path: Path to SQLite database file.
    Returns:
        sqlite3.connection-object
    """
    global _connection
    if _connection is None:
        with _connection_lock:
            if _connection is None:
                _connection = sqlite3.connect(db_path, check_same_thread=False)
    return _connection

def close_connection():
    """
    Closes the database connection, if it is open.
    """
    global _connection
    if _connection:
        _connection.close()
        _connection = None