"""
Utilities for project/dict field access.

Shared helpers for extracting trimmed field values from project dicts.
"""

from typing import Any, Dict


def get_field(project: Dict[str, Any], key: str) -> str:
    """
    Get and trim a project field. Returns empty string if missing or None.

    Args:
        project: Project dict (e.g. from Storage.get)
        key: Field name

    Returns:
        Trimmed string value, or "" if missing/None
    """
    return (project.get(key) or "").strip()
