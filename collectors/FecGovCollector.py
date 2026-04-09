"""FecGovCollector — Collector for Federal Election Commission (fec.gov) bulk data downloads

Collector for DRP Pipeline. Fetches bulk CSV data files and metadata from
fec.gov. Handles two URL patterns:
  1. Direct file URLs (/files/bulk-downloads/...) — download the single file
  2. HTML pages containing download links — parse and download all bulk files

Pages are standard HTML — no browser rendering required.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from storage import Storage
from utils.Args import Args
from utils.Errors import record_error, record_warning
from utils.Logger import Logger
from utils.download_with_progress import download_via_url
from utils.file_utils import (
    create_output_folder,
    folder_extensions_and_size,
    format_file_size,
    sanitize_filename,
)
from utils.url_utils import BROWSER_HEADERS, is_valid_url

_BASE_URL = "https://www.fec.gov"
_AGENCY = "Federal Election Commission"
_GEOGRAPHIC_COVERAGE = "United States"
_DATA_TYPES = "event/transaction data"

# File extensions considered downloadable data files
_DATA_EXTENSIONS = {
    ".csv", ".zip", ".xlsx", ".xls", ".txt", ".json", ".xml", ".pdf", ".gz",
}


class FecGovCollector:
    """Collector for Federal Election Commission (fec.gov) bulk data downloads"""

    def __init__(self, headless: bool = True) -> None:
        self._headless = headless

    def run(self, drpid: int) -> None:
        """ModuleProtocol entry point — called by Orchestrator for each eligible project."""
        record = Storage.get(drpid)
        if record is None:
            record_error(drpid, "Record not found in Storage", update_storage=False)
            return

        source_url = record.get("source_url")
        if not source_url or not is_valid_url(source_url):
            record_error(drpid, f"Invalid or missing source_url: {source_url!r}")
            return

        try:
            result = self._collect(source_url, drpid)
            self._update_storage_from_result(drpid, result)
        except Exception as exc:
            Logger.exception(f"Unexpected error in FecGovCollector: {exc}")
            record_error(drpid, f"Unexpected error: {exc}")

    def _collect(self, url: str, drpid: int) -> Dict[str, Any]:
        """
        Fetch the source URL and extract data and metadata.

        Returns a dict with Storage field names as keys. folder_path being set
        signals successful collection (triggers status="collected").
        """
        result: Dict[str, Any] = {}
        result["agency"] = _AGENCY
        result["geographic_coverage"] = _GEOGRAPHIC_COVERAGE
        result["data_types"] = _DATA_TYPES
        result["download_date"] = date.today().isoformat()

        # --- Create output folder ---
        base_dir = Path(Args.base_output_dir)
        folder_path = create_output_folder(base_dir, drpid)
        if folder_path is None:
            record_error(drpid, "Failed to create output folder")
            return result

        # --- Determine if URL is a direct file download or an HTML page ---
        if self._is_direct_download(url):
            download_links = self._make_direct_link(url)
            result["title"] = self._title_from_filename(url)
            result["summary"] = f"FEC bulk data file: {Path(url).name}"
            cycle = self._extract_cycle(url)
            if cycle:
                result["time_start"], result["time_end"] = cycle
        else:
            # Fetch page and parse for downloads and metadata
            soup = self._fetch_page(url, drpid)
            if soup is None:
                return result
            result["title"] = self._extract_title(soup)
            result["summary"] = self._extract_summary(soup)
            download_links = self._extract_download_links(soup, url)
            if not download_links:
                record_warning(drpid, "No download links found on page")

        # --- Download files ---
        downloaded = 0
        for file_url, filename in download_links:
            dest = folder_path / filename
            Logger.info(f"[{drpid}] Downloading {filename} from {file_url}")
            _bytes, ok = download_via_url(file_url, dest, headers=BROWSER_HEADERS)
            if ok:
                downloaded += 1
            else:
                record_warning(drpid, f"Failed to download: {file_url}")

        if downloaded == 0 and download_links:
            record_error(drpid, "All file downloads failed")
            return result

        if downloaded == 0 and not download_links:
            record_error(drpid, "No files to download")
            return result

        # --- Record folder stats ---
        extensions, total_bytes = folder_extensions_and_size(folder_path)
        result["folder_path"] = str(folder_path)
        result["file_size"] = format_file_size(total_bytes)
        result["extensions"] = ",".join(extensions)
        return result

    def _is_direct_download(self, url: str) -> bool:
        """Check if URL points directly to a downloadable file."""
        path = url.split("?")[0].lower()
        return any(path.endswith(ext) for ext in _DATA_EXTENSIONS)

    def _make_direct_link(self, url: str) -> List[Tuple[str, str]]:
        """Wrap a direct download URL into the standard (url, filename) list."""
        clean = url.split("?")[0]
        filename = sanitize_filename(clean.split("/")[-1]) or "download"
        return [(url, filename)]

    def _title_from_filename(self, url: str) -> str:
        """Derive a human-readable title from a bulk download filename."""
        clean = url.split("?")[0]
        name = Path(clean).stem
        # e.g. "independent_expenditure_2026" → "Independent Expenditure 2026"
        return name.replace("_", " ").title()

    def _extract_cycle(self, url: str) -> Optional[Tuple[str, str]]:
        """Extract election cycle years from a URL like /2026/foo_2026.csv.

        FEC bulk data uses 2-year election cycles. A file labeled 2026
        covers 2025-2026.
        """
        match = re.search(r"/(\d{4})/", url)
        if match:
            end_year = int(match.group(1))
            start_year = end_year - 1
            return (str(start_year), str(end_year))
        return None

    def _fetch_page(self, url: str, drpid: int) -> Optional[BeautifulSoup]:
        """Fetch HTML page and return parsed soup, or None on failure."""
        try:
            resp = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            record_error(drpid, f"Failed to fetch page: {exc}")
            return None
        return BeautifulSoup(resp.text, "html.parser")

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page title from h1, falling back to <title> tag."""
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True) or None
        title_tag = soup.find("title")
        if title_tag:
            text = title_tag.get_text(strip=True)
            return re.sub(r"\s*\|\s*FEC\s*$", "", text).strip() or None
        return None

    def _extract_summary(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract description from <meta name='description'>."""
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return meta["content"].strip() or None
        return None

    def _extract_download_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> List[Tuple[str, str]]:
        """
        Find all download links: <a> tags whose href contains
        "/files/bulk-downloads/" and ends with a known data extension.

        Returns list of (absolute_url, filename) tuples, deduplicated by URL.
        """
        seen: set[str] = set()
        links: List[Tuple[str, str]] = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/files/bulk-downloads/" not in href:
                continue

            clean_path = href.split("?")[0]
            suffix = Path(clean_path).suffix.lower()
            if suffix not in _DATA_EXTENSIONS:
                continue

            abs_url = urljoin(_BASE_URL, clean_path)
            if abs_url in seen:
                continue
            seen.add(abs_url)

            filename = sanitize_filename(abs_url.split("/")[-1]) or f"file_{len(links)}"
            links.append((abs_url, filename))

        Logger.info(f"Found {len(links)} download link(s)")
        return links

    def _update_storage_from_result(self, drpid: int, result: Dict[str, Any]) -> None:
        """Persist result to Storage and set status to collected or error."""
        update_fields = {k: v for k, v in result.items() if v is not None}
        if update_fields:
            Storage.update_record(drpid, update_fields)
        if result.get("folder_path"):
            Storage.update_record(drpid, {"status": "collected"})
        else:
            record_error(drpid, "Collection incomplete: no folder_path in result")
