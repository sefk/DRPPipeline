"""
SQLite implementation of the Storage protocol for DRP Pipeline.

Provides SQLite-based storage with concurrent access support using WAL mode.

This class should be instantiated via Storage.initialize() factory method.
"""

import sqlite3
from pathlib import Path
from typing import Literal, Optional, Dict, Any, Tuple, TYPE_CHECKING

from utils.Errors import record_crash
from utils.Logger import Logger

if TYPE_CHECKING:
    from storage.StorageProtocol import StorageProtocol


class StorageSQLLite:
    """
    SQLite implementation of the Storage protocol.
    
    This class implements StorageProtocol. Type checkers will verify
    that all required protocol methods are present through structural typing.
    """
    
    _connection: Optional[sqlite3.Connection] = None
    _db_path: Optional[Path] = None
    _initialized: bool = False
    
    # Table schema definition
    _schema_sql = """
    CREATE TABLE IF NOT EXISTS projects (
        DRPID INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT,
        status_notes TEXT,
        warnings TEXT,
        errors TEXT,
        datalumos_id TEXT UNIQUE,
        source_url TEXT NOT NULL UNIQUE,
        folder_path TEXT,
        title TEXT,
        agency TEXT,
        office TEXT,
        summary TEXT,
        keywords TEXT,
        time_start TEXT,
        time_end TEXT,
        data_types TEXT,
        extensions TEXT,
        download_date TEXT,
        collection_notes TEXT,
        file_size TEXT,
        geographic_coverage TEXT,
        published_url TEXT
    );
    
    CREATE INDEX IF NOT EXISTS idx_source_url ON projects(source_url);
    CREATE INDEX IF NOT EXISTS idx_datalumos_id ON projects(datalumos_id);
    CREATE INDEX IF NOT EXISTS idx_status ON projects(status);
    """
    
    def _ensure_initialized(self) -> None:
        """
        Ensure storage is initialized and connection is available.
        
        Raises:
            RuntimeError: If storage is not initialized or connection is not available
        """
        if not self._initialized:
            record_crash("Storage has not been initialized. Call initialize() first.")
        
        if self._connection is None:
            record_crash("Database connection is not available.")
    
    def _execute_query(
        self,
        query: str,
        parameters: Optional[Tuple[Any, ...]] = None,
        operation_name: str = "operation",
        commit: bool = True
    ) -> sqlite3.Cursor:
        """
        Execute a SQL query with error handling.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
            operation_name: Name of the operation for error messages
            commit: Whether to commit after execution (for non-SELECT queries)
            
        Returns:
            sqlite3.Cursor object
            
        Raises:
            RuntimeError: If storage is not initialized
            sqlite3.Error: If query execution fails
        """
        self._ensure_initialized()
        
        try:
            if parameters:
                cursor = self._connection.execute(query, parameters)
            else:
                cursor = self._connection.execute(query)
            
            if commit:
                self._connection.commit()
            
            return cursor
            
        except sqlite3.Error as e:
            Logger.error(f"Failed to {operation_name}: {e}")
            raise
    
    def initialize(self, db_path: Optional[Path] = None) -> None:
        """
        Initialize the database connection and create schema if needed.
        
        Sets up SQLite with WAL mode for concurrent access. Creates the database
        file and tables if they don't exist.
        
        Args:
            db_path: Path to SQLite database file. If None, uses 'drp_pipeline.db'
                    in the current working directory.
        
        Raises:
            RuntimeError: If initialization fails
        """
        if self._initialized:
            return
        
        if db_path is None:
            db_path = Path.cwd() / "drp_pipeline.db"
        
        self._db_path = db_path
        
        try:
            # Create parent directory if it doesn't exist
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Connect to database
            self._connection = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,  # Allow use from multiple threads
                timeout=30.0  # Wait up to 30 seconds for locks
            )
            
            # Enable WAL mode for concurrent reads/writes
            self._connection.execute("PRAGMA journal_mode=WAL")
            
            # Set other pragmas for better concurrency
            self._connection.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
            self._connection.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed
            
            # Create schema
            self._connection.executescript(self._schema_sql)
            self._connection.commit()

            # Migration: add extensions column if missing (existing DBs)
            try:
                self._connection.execute(
                    "ALTER TABLE projects ADD COLUMN extensions TEXT"
                )
                self._connection.commit()
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise

            self._initialized = True
            Logger.info(f"Storage initialized: {self._db_path}")
            
        except sqlite3.Error as e:
            self._connection = None
            self._initialized = False
            error_msg = f"Failed to initialize database at {self._db_path}: {e}"
            record_crash(error_msg)
    
    def create_record(self, source_url: str) -> int:
        """
        Create a new record with the given source_url.
        
        Args:
            source_url: The source URL for the project
            
        Returns:
            The DRPID of the created record
            
        Raises:
            RuntimeError: If Storage is not initialized
            sqlite3.Error: If insert fails (e.g., duplicate source_url)
        """
        cursor = self._execute_query(
            "INSERT INTO projects (source_url) VALUES (?)",
            (source_url,),
            operation_name=f"create record with source_url '{source_url}'"
        )
        return cursor.lastrowid
    
    def update_record(self, drpid: int, values: Dict[str, Any]) -> None:
        """
        Update an existing record with the provided values.
        
        Only the columns specified in values are updated. DRPID and source_url
        cannot be updated.
        
        Args:
            drpid: The DRPID of the record to update
            values: Dictionary of column names and values to update
            
        Raises:
            RuntimeError: If Storage is not initialized
            ValueError: If trying to update DRPID/source_url or if record doesn't exist
            sqlite3.Error: If update fails (e.g., invalid column name)
        """
        # Check for forbidden columns
        if "DRPID" in values or "source_url" in values:
            raise ValueError("Cannot update DRPID or source_url")
        
        if not values:
            return  # Nothing to update
        
        # Build UPDATE query - database will raise error for invalid columns
        set_clauses = [f"{column} = ?" for column in values.keys()]
        update_query = f"UPDATE projects SET {', '.join(set_clauses)} WHERE DRPID = ?"
        params = tuple(values.values()) + (drpid,)
        
        cursor = self._execute_query(
            update_query,
            params,
            operation_name=f"update record {drpid}"
        )
        
        # Check if any rows were affected (record exists)
        if cursor.rowcount == 0:
            raise ValueError(f"Record with DRPID {drpid} does not exist")
    
    def get(self, drpid: int) -> Optional[Dict[str, Any]]:
        """
        Get a record by DRPID.
        
        Args:
            drpid: The DRPID of the record to retrieve
            
        Returns:
            Dictionary of non-null column values, or None if record not found
            
        Raises:
            RuntimeError: If Storage is not initialized
        """
        cursor = self._execute_query(
            "SELECT * FROM projects WHERE DRPID = ?",
            (drpid,),
            operation_name=f"get record {drpid}",
            commit=False
        )
        
        row = cursor.fetchone()
        if row is None:
            return None
        
        # Get column names
        column_names = [description[0] for description in cursor.description]
        
        # Build dictionary with non-null values only
        result = {}
        for col_name, value in zip(column_names, row):
            if value is not None:
                result[col_name] = value
        
        return result
    
    def exists_by_source_url(self, source_url: str) -> bool:
        """
        Check whether a record with the given source_url already exists.
        
        Args:
            source_url: The source URL to look up
            
        Returns:
            True if a record exists, False otherwise
            
        Raises:
            RuntimeError: If Storage is not initialized
        """
        cursor = self._execute_query(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE source_url = ?)",
            (source_url,),
            operation_name="check exists by source_url",
            commit=False
        )
        row = cursor.fetchone()
        return bool(row[0]) if row else False
    
    def delete(self, drpid: int) -> None:
        """
        Delete a record by DRPID.
        
        Args:
            drpid: The DRPID of the record to delete
            
        Raises:
            RuntimeError: If Storage is not initialized
            ValueError: If record doesn't exist
            sqlite3.Error: If delete fails
        """
        cursor = self._execute_query(
            "DELETE FROM projects WHERE DRPID = ?",
            (drpid,),
            operation_name=f"delete record {drpid}"
        )
        
        # Check if any rows were affected (record exists)
        if cursor.rowcount == 0:
            raise ValueError(f"Record with DRPID {drpid} does not exist")
    
    def clear_all_records(self) -> None:
        """
        Delete all records from the database and reset auto-increment counter.
        
        This will remove all projects from the projects table, reset the
        auto-increment counter so new IDs start at 1, but keep the table
        structure intact.
        
        Raises:
            RuntimeError: If Storage is not initialized
            sqlite3.Error: If delete fails
        """
        self._execute_query(
            "DELETE FROM projects",
            (),
            operation_name="clear all records"
        )
        # Reset auto-increment counter so new IDs start at 1
        try:
            self._execute_query(
                "DELETE FROM sqlite_sequence WHERE name='projects'",
                (),
                operation_name="reset auto-increment"
            )
        except sqlite3.OperationalError:
            # sqlite_sequence table may not exist if no AUTOINCREMENT was used
            # or if the table was never populated. This is fine.
            pass
        Logger.info("All records cleared from database and auto-increment reset")
    
    def close(self) -> None:
        """
        Close the database connection.
        
        Note: The connection will be automatically closed when the process exits,
        but this can be useful for explicit cleanup.
        """
        if self._connection:
            try:
                self._connection.close()
                Logger.info("Database connection closed")
            except sqlite3.Error as e:
                Logger.warning(f"Error closing database connection: {e}")
            finally:
                self._connection = None
                self._initialized = False
    
    def get_db_path(self) -> Path:
        """
        Get the path to the database file.
        
        Returns:
            Path to the database file
        
        Raises:
            RuntimeError: If Storage is not initialized
        """
        self._ensure_initialized()
        
        if self._db_path is None:
            raise RuntimeError("Database path is not set.")

        return self._db_path

    def list_eligible_projects(
        self,
        prereq_status: Optional[str],
        limit: Optional[int],
        start_row: Optional[int] = None,
        min_drpid: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        """
        List projects eligible for the next module: status == prereq_status.

        Order by DRPID ASC. Optionally limit the number of rows. Optionally skip
        first (start_row - 1) rows when start_row is set (1-origin), or filter by
        min_drpid (DRPID >= min_drpid). min_drpid takes precedence when both are set.

        Args:
            prereq_status: Required status (e.g. "sourced" for collectors). None -> [].
            limit: Max rows to return. None = no limit.
            start_row: If set, skip first (start_row - 1) rows of the full table (1-origin).
            min_drpid: If set, only return projects with DRPID >= this value.

        Returns:
            List of full row dicts (all columns, including None for nulls).
        """
        if prereq_status is None:
            return []
        params: list[Any] = [prereq_status]
        min_drpid_clause = ""
        if min_drpid is not None:
            min_drpid_clause = " AND DRPID >= ?"
            params.append(min_drpid)
        elif start_row is not None and start_row > 1:
            # Get DRPID at position start_row (1-origin) in full table
            subq = "SELECT DRPID FROM projects ORDER BY DRPID LIMIT 1 OFFSET ?"
            min_drpid_clause = " AND DRPID >= (" + subq + ")"
            params.append(start_row - 1)
        query = (
            "SELECT * FROM projects "
            "WHERE status = ? AND (errors IS NULL OR errors = '')"
            + min_drpid_clause
            + " ORDER BY DRPID ASC"
        )
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        cursor = self._execute_query(
            query, tuple(params), operation_name="list_eligible_projects", commit=False
        )
        rows = cursor.fetchall()
        column_names = [d[0] for d in cursor.description]
        result: list[Dict[str, Any]] = []
        for row in rows:
            result.append(dict(zip(column_names, row)))
        return result

    def list_records_with_status_notes(self) -> list[Dict[str, Any]]:
        """
        List all records that have non-null, non-empty status_notes.

        Returns:
            List of full row dicts ordered by DRPID ASC.
        """
        query = (
            "SELECT * FROM projects "
            "WHERE status_notes IS NOT NULL AND TRIM(status_notes) != '' "
            "ORDER BY DRPID ASC"
        )
        cursor = self._execute_query(
            query, None, operation_name="list_records_with_status_notes", commit=False
        )
        rows = cursor.fetchall()
        column_names = [d[0] for d in cursor.description]
        return [dict(zip(column_names, row)) for row in rows]

    def append_to_field(
        self, drpid: int, field: Literal["warnings", "errors"], text: str
    ) -> None:
        """
        Append text to the warnings or errors field. Format: one entry per line (newline).

        Args:
            drpid: The DRPID of the record to update.
            field: Either "warnings" or "errors".
            text: Text to append.

        Raises:
            ValueError: If field is not "warnings" or "errors", or record does not exist.
        """
        if field not in ("warnings", "errors"):
            raise ValueError(f"field must be 'warnings' or 'errors', got: {field!r}")
        cursor = self._execute_query(
            f"SELECT {field} FROM projects WHERE DRPID = ?",
            (drpid,),
            operation_name=f"read {field} for append",
            commit=False,
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Record with DRPID {drpid} does not exist")
        current = row[0] or ""
        # Preserve whitespace consistently: append with newline, only strip trailing whitespace
        # from the entire field to avoid trailing newlines, but preserve whitespace within entries
        new_value = (current + "\n" + text).rstrip() if current else text
        self.update_record(drpid, {field: new_value})
