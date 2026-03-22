"""
Utilities for parsing Google Sheets URLs and resolving sheet name to gid.

Used by Sourcing to get spreadsheet ID and sheet gid (from URL or by resolving sheet name via API).
"""

import re
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse


def get_gid_for_sheet_name(
    spreadsheet_id: str,
    sheet_name: str,
    credentials_path: Optional[Path] = None,
) -> Optional[str]:
    """
    Resolve a worksheet name to its gid (sheetId) using the Google Sheets API.

    The public CSV export URL only accepts gid; this allows using the same
    google_sheet_name as the publisher by resolving it when credentials are available.

    Args:
        spreadsheet_id: The spreadsheet ID from the URL.
        sheet_name: The worksheet/tab name (e.g. "CDC", "Data_Inventories").
        credentials_path: Path to service account JSON. If None or API unavailable, returns None.

    Returns:
        The gid as string, or None if not found or credentials/API unavailable.
    """
    if not spreadsheet_id or not (sheet_name or "").strip() or not credentials_path:
        return None
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        return None
    try:
        creds = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        meta = (
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
        )
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if (props.get("title") or "").strip() == (sheet_name or "").strip():
                return str(props.get("sheetId", 0))
        return None
    except Exception:
        return None


def parse_spreadsheet_url(url: str) -> tuple[str, str]:
    """
    Extract spreadsheet ID and sheet gid from a Google Sheets URL.

    Supports edit URLs (e.g. .../edit?gid=123#gid=123) and export URLs.
    If gid is absent, returns "0" (first sheet).

    Args:
        url: Google Sheets edit or export URL.

    Returns:
        (spreadsheet_id, gid) as strings.

    Raises:
        ValueError: If URL format is not recognized or ID cannot be extracted.

    Example:
        >>> parse_spreadsheet_url(
        ...     "https://docs.google.com/spreadsheets/d/ABC123/edit?gid=101637367"
        ... )
        ('ABC123', '101637367')
    """
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    if not match:
        raise ValueError(
            f"Could not extract spreadsheet ID from URL. "
            f"Expected pattern .../spreadsheets/d/{{id}}/... : {url[:80]}..."
        )
    sheet_id = match.group(1)

    parsed = urlparse(url)
    gid: str | None = None

    if parsed.query:
        qs = parse_qs(parsed.query)
        gids = qs.get("gid", [])
        if gids:
            gid = str(gids[0]).strip()
    if gid is None and parsed.fragment:
        frag = parsed.fragment
        if "gid=" in frag:
            parts = parse_qs(frag)
            gids = parts.get("gid", [])
            if gids:
                gid = str(gids[0]).strip()
    if gid is None:
        gid = "0"

    return (sheet_id, gid)
