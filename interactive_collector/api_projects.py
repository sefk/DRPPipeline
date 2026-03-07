"""
API module for project operations.

Serves: GET /api/projects/first, /api/projects/next, /api/projects/<drpid>.
Provides project listing, next-eligible lookup, and output folder creation
for the Interactive Collector SPA.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from interactive_collector.collector_state import get_base_output_dir, get_db_path, get_result_by_drpid


def _ensure_storage() -> None:
    """
    Initialize Storage if not already, using Args.db_path or default.
    Ensures Logger is initialized first. Idempotent.
    """
    from storage import Storage
    try:
        Storage.list_eligible_projects(None, 0)
    except RuntimeError:
        try:
            from utils.Logger import Logger
            if not getattr(Logger, "_initialized", False):
                Logger.initialize(log_level="WARNING")
        except Exception:
            pass
        path = get_db_path()
        Storage.initialize("StorageSQLLite", db_path=path)


def get_first_eligible() -> Optional[Dict[str, Any]]:
    """
    Return the first eligible project (prereq=sourcing, no errors) or None.

    Returns:
        Project dict with DPRID, source_url, etc., or None.
    """
    _ensure_storage()
    from storage import Storage
    projects = Storage.list_eligible_projects("sourced", 1)
    return projects[0] if projects else None


def get_next_eligible_after(current_drpid: int) -> Optional[Dict[str, Any]]:
    """
    Return the next eligible project after current_drpid, or None.

    Args:
        current_drpid: Current project's DRPID.

    Returns:
        Next project dict or None.
    """
    _ensure_storage()
    from storage import Storage
    projects = Storage.list_eligible_projects("sourced", 200)
    for proj in projects:
        if proj["DRPID"] > current_drpid:
            return proj
    return None


def get_project_by_drpid(drpid: int) -> Optional[Dict[str, Any]]:
    """
    Return the project record for the given DRPID, or None.

    Args:
        drpid: Project identifier.

    Returns:
        Project dict or None.
    """
    _ensure_storage()
    from storage import Storage
    return Storage.get(drpid)


def ensure_output_folder(drpid: int) -> Optional[str]:
    """
    Create or empty the output folder for this DRPID; store in result state.

    Args:
        drpid: Project identifier.

    Returns:
        folder_path string or None if creation failed.
    """
    from utils.file_utils import create_output_folder

    result = get_result_by_drpid()
    if drpid in result and result[drpid].get("folder_path"):
        return result[drpid]["folder_path"]
    base_path = get_base_output_dir()
    folder_path = create_output_folder(base_path, drpid)
    if not folder_path:
        return None
    path_str = str(folder_path)
    result[drpid] = {"folder_path": path_str}
    return path_str


def folder_path_for_drpid(display_drpid: Optional[str]) -> Optional[str]:
    """
    Return folder_path from result state for the given display_drpid.

    Args:
        display_drpid: DRPID as string (may be None).

    Returns:
        folder_path or None.
    """
    if not display_drpid:
        return None
    try:
        drpid = int(display_drpid)
        return get_result_by_drpid().get(drpid, {}).get("folder_path")
    except (ValueError, TypeError):
        return None
