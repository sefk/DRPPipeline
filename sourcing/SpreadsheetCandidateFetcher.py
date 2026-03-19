"""
Fetches candidate source URLs from a Google Sheets tab.

Reads google_sheet_id, google_sheet_name, google_credentials, and sourcing_url_column from Args.
Requires google_credentials to resolve the sheet name to a tab (gid). Raises ValueError
if any required config is missing, the sheet is not found, or required columns are missing.
"""

import csv
import io
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from utils.Args import Args
from utils.Logger import Logger
from utils.sheet_url_utils import get_gid_for_sheet_name


class SpreadsheetCandidateFetcher:
    """
    Fetches candidate URLs from a Google Sheets tab (e.g. DRP Data_Inventories).

    All configuration comes from Args: google_sheet_id, google_sheet_name, google_credentials,
    sourcing_url_column. google_credentials is required to resolve the sheet name to a tab.
    """

    def get_candidate_urls(self, limit: int | None = None) -> tuple[list[dict[str, str]], int]:
        """
        Obtain candidate source URLs and Office/Agency from the configured spreadsheet.

        Fetches the tab as CSV, filters rows with _row_passes_filter, returns
        dicts with url, office, agency. Continues until limit filtered rows are found (if set).

        Args:
            limit: Max URLs to return. None = unlimited. Provided by orchestrator.

        Returns:
            Tuple of (list of dicts with keys url, office, agency; count of skipped rows).
        """
        sheet_id = (getattr(Args, "google_sheet_id", None) or "").strip()
        if not sheet_id:
            raise ValueError(
                "google_sheet_id is required for sourcing. Set it in config (the sheet ID from the Google Sheet URL)."
            )
        sheet_name = (getattr(Args, "google_sheet_name", None) or "").strip()
        if not sheet_name:
            raise ValueError(
                "google_sheet_name is required for sourcing. Set it in config (the worksheet/tab name)."
            )
        creds_path = getattr(Args, "google_credentials", None)
        creds_path = Path(creds_path) if creds_path else None
        if not creds_path or not creds_path.exists():
            raise ValueError(
                "google_credentials is required for sourcing (to resolve sheet name to tab). "
                "Set it in config to the path of your service account JSON file."
            )
        gid = get_gid_for_sheet_name(sheet_id, sheet_name, creds_path)
        if gid is None:
            raise ValueError(
                f"Sheet '{sheet_name}' not found in spreadsheet (or Sheets API error). "
                f"Verify google_sheet_name matches the worksheet/tab name and that credentials have read access."
            )
        csv_text = self._fetch_sheet_csv(sheet_id, gid)
        url_column = Args.sourcing_url_column
        return self._extract_urls_from_csv(csv_text, url_column, limit)

    def _fetch_sheet_csv(self, sheet_id: str, gid: str) -> str:
        """
        Fetch a Google Sheets tab as CSV via the public export URL.

        Args:
            sheet_id: Spreadsheet ID from the sheet URL.
            gid: Sheet/tab gid.

        Returns:
            CSV body as string (UTF-8-sig).

        Raises:
            URLError: On network or HTTP errors.
        """
        export_url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        )
        req = Request(export_url, headers={"User-Agent": "DRPPipeline/1.0"})
        try:
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8-sig")
        except (HTTPError, URLError) as e:
            Logger.error(f"Failed to fetch spreadsheet CSV: {e}")
            raise

    def _row_passes_filter(self, row: dict[str, str]) -> bool:
        """
        Return True if the row meets the desired criteria based on sourcing_mode.

        sourcing_mode controls which rows are selected:
          unclaimed  (default): Claimed="" and Download Location="" (available, unworked rows)
          completed:            Download Location is non-empty (manually archived rows)
          all:                  any row with a non-empty URL

        URL must also start with sourcing_url_prefix (if set).

        Note: This method assumes required columns are present (validated in
        _extract_urls_from_csv). Missing columns would indicate a bug.
        """
        url_prefix = (getattr(Args, "sourcing_url_prefix", None) or "").strip()
        claimed = (row.get("Claimed (add your name)") or "").strip()
        download_location = (row.get("Download Location") or "").strip()
        url = (row.get("URL") or "").strip()
        mode = (getattr(Args, "sourcing_mode", None) or "unclaimed").strip().lower()

        if mode == "unclaimed":
            passes = claimed == "" and download_location == ""
        elif mode == "completed":
            passes = download_location != ""
        elif mode == "all":
            passes = True
        else:
            Logger.warning(f"Unknown sourcing_mode '{mode}', defaulting to 'unclaimed'")
            passes = claimed == "" and download_location == ""

        return passes and (not url_prefix or url.startswith(url_prefix))

    def _extract_urls_from_csv(
        self, csv_text: str, url_column: str, num_rows: int | None = None
    ) -> tuple[list[dict[str, str]], int]:
        """
        Parse CSV, filter rows with _row_passes_filter, collect url plus Office and Agency.

        Continues processing until num_rows filtered rows have been collected (if num_rows is set).

        Args:
            csv_text: CSV content to parse.
            url_column: Column name containing URLs.
            num_rows: Maximum number of filtered rows to return. None = unlimited.

        Returns:
            Tuple of (list of dicts with keys url, office, agency; count of skipped rows).

        Raises:
            ValueError: If required columns (URL column or filter columns) are missing.
        """
        reader = csv.DictReader(io.StringIO(csv_text))
        fieldnames = reader.fieldnames or []

        # Required columns for _row_passes_filter
        required_filter_columns = ["Claimed (add your name)", "Download Location"]

        # Check URL column
        if url_column not in fieldnames:
            raise ValueError(
                f"CSV missing required URL column '{url_column}'. "
                f"Available columns: {fieldnames}"
            )

        # Check filter columns
        missing_filter_columns = [
            col for col in required_filter_columns if col not in fieldnames
        ]
        if missing_filter_columns:
            raise ValueError(
                f"CSV missing required filter columns: {missing_filter_columns}. "
                f"Available columns: {fieldnames}"
            )

        rows_out: list[dict[str, str]] = []
        skipped_count = 0

        for row in reader:
            if num_rows is not None and len(rows_out) >= num_rows:
                break

            if not self._row_passes_filter(row):
                skipped_count += 1
                continue

            raw = row.get(url_column, "")
            url = (raw or "").strip()
            if url:
                office = (row.get("Office") or "").strip()
                agency = (row.get("Agency") or "").strip()
                rows_out.append({"url": url, "office": office, "agency": agency})

        return (rows_out, skipped_count)
