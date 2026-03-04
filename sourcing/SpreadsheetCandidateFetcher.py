"""
Fetches candidate source URLs from a Google Sheets tab.

Reads spreadsheet URL and URL column from Args, filters rows via _row_passes_filter,
returns non-empty URL values.
"""

import csv
import io
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from utils.Args import Args
from utils.Logger import Logger
from utils.sheet_url_utils import parse_spreadsheet_url


class SpreadsheetCandidateFetcher:
    """
    Fetches candidate URLs from a Google Sheets tab (e.g. DRP Data_Inventories).

    All configuration comes from Args: sourcing_spreadsheet_url, sourcing_url_column.
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
        spreadsheet_url = Args.sourcing_spreadsheet_url
        sheet_id, gid = parse_spreadsheet_url(spreadsheet_url)
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
        Return True if the row meets the desired criteria.

        Keeps rows where Claimed (add your name) and Download Location are empty.
        
        Note: This method assumes required columns are present (validated in
        _extract_urls_from_csv). Missing columns would indicate a bug.
        """
        claimed = (row.get("Claimed (add your name)") or "").strip()
        download_location = (row.get("Download Location") or "").strip()
        return claimed == "" and download_location == "" # and (row.get("URL") or "").strip().startswith("https://catalog.data.gov/")

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
