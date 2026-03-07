"""
Storage protocol interface for DRP Pipeline.

Defines the Storage protocol that all storage implementations must follow.
"""

from pathlib import Path
from typing import Literal, Optional, Protocol, Dict, Any


class StorageProtocol(Protocol):
    """
    Protocol defining the storage API interface.
    
    Any class implementing this protocol must provide these methods
    for managing project records.
    """
    
    def initialize(self, db_path: Optional[Path] = None) -> None:
        """
        Initialize the storage backend.
        
        Args:
            db_path: Optional path to storage file/database.
        """
        ...
    
    def create_record(self, source_url: str) -> int:
        """
        Create a new record with the given source_url.
        
        Args:
            source_url: The source URL for the project
            
        Returns:
            The DRPID of the created record
        """
        ...
    
    def update_record(self, drpid: int, values: Dict[str, Any]) -> None:
        """
        Update an existing record with the provided values.
        
        Only the columns specified in values are updated. DRPID and source_url
        cannot be updated.
        
        Args:
            drpid: The DRPID of the record to update
            values: Dictionary of column names and values to update
            
        Raises:
            ValueError: If record doesn't exist or if trying to update DRPID/source_url
        """
        ...
    
    def exists_by_source_url(self, source_url: str) -> bool:
        """
        Check whether a record with the given source_url already exists.
        
        Args:
            source_url: The source URL to look up
            
        Returns:
            True if a record exists, False otherwise
        """
        ...

    def get(self, drpid: int) -> Optional[Dict[str, Any]]:
        """
        Get a record by DRPID.
        
        Args:
            drpid: The DRPID of the record to retrieve
            
        Returns:
            Dictionary of non-null column values, or None if record not found
        """
        ...
    
    def delete(self, drpid: int) -> None:
        """
        Delete a record by DRPID.
        
        Args:
            drpid: The DRPID of the record to delete
            
        Raises:
            ValueError: If record doesn't exist
        """
        ...
    
    def clear_all_records(self) -> None:
        """
        Delete all records from the database.
        
        This will remove all projects from the projects table, but keep
        the table structure intact.
        
        Raises:
            RuntimeError: If Storage is not initialized
        """
        ...
    
    def list_eligible_projects(
        self,
        prereq_status: Optional[str],
        limit: Optional[int],
        start_row: Optional[int] = None,
        min_drpid: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        """
        List projects eligible for the next module: status == prereq_status and no errors.

        Order by DRPID ASC. Optionally limit the number of rows. Optionally skip
        first (start_row - 1) rows when start_row is set (1-origin), or filter by
        min_drpid (DRPID >= min_drpid) when min_drpid is set.

        Args:
            prereq_status: Required status (e.g. "sourced" for collectors). None -> [].
            limit: Max rows to return. None = no limit.
            start_row: If set, skip first (start_row - 1) rows of the full table (1-origin).
            min_drpid: If set, only return projects with DRPID >= this value (takes precedence over start_row when both set).

        Returns:
            List of full row dicts (all columns, including None for nulls).
        """
        ...

    def list_records_with_status_notes(self) -> list[Dict[str, Any]]:
        """
        List all records that have non-null, non-empty status_notes.

        Returns:
            List of full row dicts (DRPID, source_url, status_notes, etc.) ordered by DRPID ASC.
        """
        ...

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
        ...
