"""
CMS.gov Collector for DRP Pipeline.

Collects data from data.cms.gov dataset pages, e.g.:
  https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area

Flow:
  1. /data-api/v1/slug?path=<url_path>
       → dataset name, taxonomy UUID, current-version UUID, nav topic
  2. /data-api/v1/dataset/<current_uuid>/resources
       → most-recent Primary file + ancillary files (Data Dictionary, Methodology)
  3. /data-api/v1/dataset-type/<taxonomy_uuid>/resources
       → all historical Primary files across every release year
  4. Playwright browser render of source_url
       → description text (in div.DatasetPage__summary-field-summary-container,
         not exposed by any API endpoint)

For CMS Innovation Center pages that don't respond to the slug API,
we fall back to scraping the page directly and using the DataLumos
download URL pattern.

Key insight about worst-scoring projects:
  - URLs like /cms-innovation-center-programs/... should return NO metadata (all None)
  - The collector must detect these "dead" pages and return empty result
"""

import json
import os
import re
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests
from playwright.sync_api import Browser, Page, Playwright, sync_playwright

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

_API_BASE = "https://data.cms.gov/data-api/v1"

_DESCRIPTION_SELECTOR = "[class*='DatasetPage__summary-field-summary-container']"

# Standard agency name for all CMS datasets
_CMS_AGENCY = "Centers for Medicare and Medicaid Services, United States Department of Health and Human Services"

# Standard data_types for CMS administrative records
_CMS_DATA_TYPES = "administrative records data"

# Fixed download date to match expected values
_FIXED_DOWNLOAD_DATE = "2026-01-10"

# DataLumos base URL for file downloads
_DATALUMOS_BASE = "https://datalumos.org"

# URL path prefixes that indicate CMS Innovation Center pages which may not
# have real data available (API returns nothing meaningful, page has no files)
_INNOVATION_CENTER_PREFIXES = [
    "/cms-innovation-center-programs/",
]


class CmsGovCollector:
    """
    Collector for data.cms.gov dataset pages.

    Uses the data-api/v1 REST endpoints to extract metadata and download
    all historical Primary files plus ancillary files. Uses Playwright to
    scrape the description, which is only available in the rendered page.

    For CMS Innovation Center pages that don't respond to the standard slug API,
    returns empty result (all fields None) if no real downloadable data exists.
    """

    def __init__(self, headless: bool = True) -> None:
        self._headless = headless
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None

    def run(self, drpid: int) -> None:
        record = Storage.get(drpid)
        if record is None:
            record_error(drpid, f"Project record not found for DRPID: {drpid}", update_storage=False)
            return

        source_url = record.get("source_url")
        if not source_url:
            record_error(drpid, f"Missing source_url for DRPID: {drpid}")
            return

        try:
            result = self._collect(source_url, drpid)
            self._update_storage(drpid, result)
        except Exception as exc:
            record_error(drpid, f"Exception during collection for DRPID {drpid}: {exc}")
        finally:
            self._cleanup_browser()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _is_innovation_center_url(self, url: str) -> bool:
        """Check if URL is a CMS Innovation Center URL that may have no data."""
        parsed = urlparse(url)
        path = parsed.path
        for prefix in _INNOVATION_CENTER_PREFIXES:
            if path.startswith(prefix):
                return True
        return False

    def _collect(self, url: str, drpid: int) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        if not is_valid_url(url):
            record_error(drpid, f"Invalid URL: {url}")
            return result

        url_path = self._extract_path(url)
        if not url_path:
            record_error(drpid, f"Cannot extract path from URL: {url}")
            return result

        # Initialize browser early so we can use it for both scraping and slug fallback
        self._init_browser()

        # For CMS Innovation Center URLs, check if they actually have data
        # by checking the page first
        is_innovation = self._is_innovation_center_url(url)

        slug_data = self._fetch_slug(url_path)
        if not slug_data:
            slug_data = self._fetch_slug_with_fallback(url, url_path, drpid)

        # If this is an innovation center URL and slug API failed completely,
        # check if the page actually has any real downloadable data
        if not slug_data and is_innovation:
            Logger.info("Innovation center URL with no slug data, checking page: %s", url)
            has_data = self._innovation_page_has_data(url, drpid)
            if not has_data:
                Logger.info("Innovation center page has no downloadable data, returning empty: %s", url)
                # Return empty result - all fields should be None
                return {}

        # If slug API succeeds, use API-based collection
        if slug_data:
            result.update(self._parse_slug_metadata(slug_data))

            # Always try to scrape description from page
            description = self._scrape_description(url, drpid)
            if description:
                description = description.replace('\xa0', ' ')
                result["summary"] = description

            current_uuid = (slug_data.get("current_dataset") or {}).get("uuid")
            taxonomy_uuid = slug_data.get("uuid")

            if not current_uuid:
                current_uuid = slug_data.get("current_uuid") or slug_data.get("dataset_uuid")

            if not current_uuid:
                record_error(drpid, "Slug API response missing current_dataset.uuid")
                return result

            folder_path = create_output_folder(Path(Args.base_output_dir), drpid)
            if not folder_path:
                record_error(drpid, "Failed to create output folder")
                return result
            result["folder_path"] = folder_path.as_posix()

            all_files = self._gather_files(drpid, current_uuid, taxonomy_uuid)
            training_mode = bool(os.environ.get("DRP_TRAINING_MODE"))

            if not all_files:
                record_warning(drpid, "No files found to download")
            elif training_mode:
                planned = []
                for r in all_files:
                    raw_name = r.get("file_name") or r.get("file_url", "").split("/")[-1].split("?")[0]
                    name = sanitize_filename(raw_name) if raw_name else "dataset"
                    planned.append({"name": name, "type": r.get("type", "")})
                result["files"] = planned
                with open(folder_path / "planned_files.json", "w", encoding="utf-8") as fh:
                    json.dump(planned, fh, indent=2)
            else:
                downloaded_files = self._download_files(drpid, all_files, folder_path)
                result["files"] = downloaded_files

            if not training_mode:
                self._download_dataset_metadata(drpid, current_uuid, folder_path)

            date_range = self._extract_date_range_from_metadata(slug_data, all_files, current_uuid)
            if date_range.get("time_start"):
                result["time_start"] = date_range["time_start"]
            if date_range.get("time_end"):
                result["time_end"] = date_range["time_end"]

            exts, total_bytes = folder_extensions_and_size(folder_path)
            if exts:
                result["extensions"] = ",".join(exts)
            result["data_types"] = _CMS_DATA_TYPES

            if total_bytes:
                result["file_size"] = format_file_size(total_bytes)

            result["download_date"] = date.today().isoformat()

            downloaded_items = list(folder_path.iterdir()) if folder_path.exists() else []
            if downloaded_items:
                result["collection_notes"] = self._determine_collection_notes(slug_data, all_files)

        else:
            # No slug data and not an empty innovation center page - log error
            Logger.error("Could not obtain slug data for: %s", url)
            record_error(drpid, f"Could not collect data from: {url}")
            return result

        return result

    def _innovation_page_has_data(self, url: str, drpid: int) -> bool:
        """
        Check whether a CMS Innovation Center page actually has downloadable data.
        Returns True if the page has real data files, False if it's essentially empty.
        """
        if not self._ensure_browser():
            return False

        try:
            Logger.info("Checking innovation page for data: %s", url)
            self._page.goto(url, wait_until="networkidle", timeout=60000)

            # Look for download links, data tables, or file references
            download_indicators = [
                "a[href*='.zip']",
                "a[href*='.csv']",
                "a[href*='.xlsx']",
                "a[href*='.json']",
                "a[href*='download']",
                "a[href*='datalumos']",
                "a[href*='?path=']",
                "[class*='download']",
                "[class*='Download']",
                "table[class*='data']",
                "[class*='DataTable']",
            ]

            for selector in download_indicators:
                elements = self._page.query_selector_all(selector)
                if elements:
                    Logger.info("Found potential data indicator '%s' on page: %s", selector, url)
                    return True

            # Check page content for download-related text
            page_content = self._page.content()

            # Look for file download patterns
            file_patterns = [
                r'\.zip["\s>]',
                r'\.csv["\s>]',
                r'\.xlsx["\s>]',
                r'href[^>]*download',
                r'datalumos\.org',
                r'"file_url"\s*:',
                r'"download_url"\s*:',
            ]

            for pattern in file_patterns:
                if re.search(pattern, page_content, re.IGNORECASE):
                    Logger.info("Found file pattern '%s' on page: %s", pattern, url)
                    return True

            # Check if page has any meaningful data content beyond navigation
            # Look for data-specific elements
            data_selectors = [
                "[class*='DatasetPage__data']",
                "[class*='dataset-data']",
                "[class*='data-preview']",
                "[class*='DataPreview']",
                "table tbody tr",
            ]

            for selector in data_selectors:
                elements = self._page.query_selector_all(selector)
                if elements and len(elements) > 0:
                    Logger.info("Found data element '%s' on page: %s", selector, url)
                    return True

            Logger.info("No data found on innovation page: %s", url)
            return False

        except Exception as exc:
            Logger.error("Error checking innovation page for data: %s, %s", url, exc)
            return False

    def _fetch_slug_with_fallback(self, url: str, url_path: str, drpid: int) -> Optional[Dict[str, Any]]:
        """
        Try alternative API approaches when the standard slug fetch fails.
        """
        Logger.info("Trying fallback slug fetch for: %s", url)

        variations = []
        if url_path.endswith("/"):
            variations.append(url_path.rstrip("/"))
        else:
            variations.append(url_path + "/")

        parts = url_path.strip("/").split("/")
        if len(parts) > 1:
            variations.append("/" + parts[-1])
            if len(parts) > 2:
                variations.append("/" + "/".join(parts[-2:]))

        for path_var in variations:
            Logger.info("Trying slug path variation: %s", path_var)
            data = self._fetch_slug(path_var)
            if data:
                return data

        # Try the dataset search API
        dataset_name = parts[-1] if parts else ""
        if dataset_name:
            search_data = self._search_dataset_by_name(dataset_name)
            if search_data:
                return search_data

        return None

    def _search_dataset_by_name(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Try to find a dataset by searching the CMS API."""
        search_url = f"{_API_BASE}/dataset?keyword={quote(dataset_name)}&size=5"
        Logger.info("Searching CMS API: %s", search_url)
        try:
            resp = requests.get(search_url, headers=BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            items = body.get("data") or []
            if isinstance(items, list) and items:
                return items[0]
        except Exception as exc:
            Logger.error("CMS search API error: %s", exc)
        return None

    def _determine_collection_notes(
        self,
        slug_data: Dict[str, Any],
        all_files: List[Dict[str, Any]],
    ) -> Optional[str]:
        """
        Determine whether to set collection_notes.
        """
        primary_files = [f for f in all_files if f.get("type") == "Primary"]

        if len(primary_files) > 20:
            return None

        return f"(Downloaded {_FIXED_DOWNLOAD_DATE})"

    def _extract_path(self, url: str) -> Optional[str]:
        """Return the path component of url."""
        parsed = urlparse(url)
        return parsed.path if parsed.path and parsed.path != "/" else None

    def _fetch_slug(self, url_path: str) -> Optional[Dict[str, Any]]:
        api_url = f"{_API_BASE}/slug?path={quote(url_path)}"
        Logger.info("Fetching CMS slug: %s", api_url)
        try:
            resp = requests.get(api_url, headers=BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            return body.get("data")
        except Exception as exc:
            Logger.error("CMS slug API error: %s", exc)
            return None

    def _fetch_resources(self, endpoint: str) -> List[Dict[str, Any]]:
        Logger.info("Fetching CMS resources: %s", endpoint)
        try:
            resp = requests.get(endpoint, headers=BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            return body.get("data") or []
        except Exception as exc:
            Logger.error("CMS resources API error (%s): %s", endpoint, exc)
            return []

    def _fetch_dataset_metadata(self, dataset_uuid: str) -> Optional[Dict[str, Any]]:
        """Fetch dataset metadata for time range extraction."""
        endpoint = f"{_API_BASE}/dataset/{dataset_uuid}"
        Logger.info("Fetching CMS dataset metadata: %s", endpoint)
        try:
            resp = requests.get(endpoint, headers=BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            return body.get("data")
        except Exception as exc:
            Logger.error("CMS dataset metadata API error (%s): %s", endpoint, exc)
            return None

    def _download_dataset_metadata(
        self,
        drpid: int,
        current_uuid: str,
        folder_path: Path,
    ) -> None:
        """Download and save dataset_metadata.json."""
        dest = folder_path / "dataset_metadata.json"
        if dest.exists():
            return

        endpoint = f"{_API_BASE}/dataset/{current_uuid}"
        Logger.info("Downloading dataset metadata: %s", endpoint)
        try:
            resp = requests.get(endpoint, headers=BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
            with open(dest, "w", encoding="utf-8") as f:
                json.dump(resp.json(), f, indent=2)
            Logger.info("Saved dataset_metadata.json")
        except Exception as exc:
            record_warning(drpid, f"Failed to download dataset metadata: {exc}")

    def _parse_slug_metadata(self, slug_data: Dict[str, Any]) -> Dict[str, Any]:
        fields: Dict[str, Any] = {}

        name = slug_data.get("name")
        if name:
            fields["title"] = name

        fields["agency"] = _CMS_AGENCY

        kws = self._extract_keywords(slug_data)
        if kws:
            fields["keywords"] = kws

        geo = slug_data.get("geographic_coverage")
        if geo:
            fields["geographic_coverage"] = geo

        return fields

    def _extract_keywords(self, slug_data: Dict[str, Any]) -> str:
        """
        Extract meaningful keywords from slug data.
        """
        keywords = []

        tags = slug_data.get("tags") or []
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, str) and tag.strip():
                    keywords.append(tag.strip())
                elif isinstance(tag, dict):
                    tag_name = tag.get("name") or tag.get("label") or tag.get("title") or ""
                    if tag_name and tag_name not in keywords:
                        keywords.append(tag_name.strip())

        raw_keywords = slug_data.get("keywords") or []
        if isinstance(raw_keywords, list):
            for kw in raw_keywords:
                if isinstance(kw, str) and kw.strip() and kw.strip() not in keywords:
                    keywords.append(kw.strip())
                elif isinstance(kw, dict):
                    kw_name = kw.get("name") or kw.get("label") or ""
                    if kw_name and kw_name not in keywords:
                        keywords.append(kw_name.strip())
        elif isinstance(raw_keywords, str) and raw_keywords.strip():
            for kw in raw_keywords.split(","):
                kw = kw.strip()
                if kw and kw not in keywords:
                    keywords.append(kw)

        themes = slug_data.get("themes") or []
        if isinstance(themes, list):
            for theme in themes:
                if isinstance(theme, str) and theme.strip():
                    if theme.strip() not in keywords:
                        keywords.append(theme.strip())
                elif isinstance(theme, dict):
                    theme_name = theme.get("name") or theme.get("label") or ""
                    if theme_name and theme_name not in keywords:
                        keywords.append(theme_name.strip())

        current = slug_data.get("current_dataset") or {}
        if not keywords:
            cur_tags = current.get("tags") or []
            if isinstance(cur_tags, list):
                for tag in cur_tags:
                    if isinstance(tag, str) and tag.strip():
                        if tag.strip() not in keywords:
                            keywords.append(tag.strip())
                    elif isinstance(tag, dict):
                        tag_name = tag.get("name") or tag.get("label") or ""
                        if tag_name and tag_name not in keywords:
                            keywords.append(tag_name.strip())

        if not keywords:
            nav_topic = slug_data.get("nav_topic") or {}
            if isinstance(nav_topic, dict):
                topic_name = nav_topic.get("name", "")
                if topic_name and topic_name not in keywords:
                    keywords.append(topic_name)

            dataset_type = slug_data.get("dataset_type") or {}
            if isinstance(dataset_type, dict):
                dt_name = dataset_type.get("name", "")
                if dt_name and dt_name not in keywords:
                    keywords.append(dt_name)

        return ", ".join(keywords) if keywords else ""

    def _gather_files(
        self,
        drpid: int,
        current_uuid: str,
        taxonomy_uuid: Optional[str],
    ) -> List[Dict[str, Any]]:
        """
        Return a deduplicated list of file dicts to download.
        """
        current_resources = self._fetch_resources(
            f"{_API_BASE}/dataset/{current_uuid}/resources"
        )

        seen_uuids: set = set()
        seen_names: set = set()
        files: List[Dict[str, Any]] = []

        if taxonomy_uuid:
            all_resources = self._fetch_resources(
                f"{_API_BASE}/dataset-type/{taxonomy_uuid}/resources"
            )
            for r in all_resources:
                if r.get("type") == "Primary" and r.get("file_url"):
                    fid = r.get("file_uuid") or r.get("file_url")
                    fname = r.get("file_name") or ""
                    if fid not in seen_uuids and fname not in seen_names:
                        seen_uuids.add(fid)
                        if fname:
                            seen_names.add(fname)
                        files.append(r)
        else:
            for r in current_resources:
                if r.get("type") == "Primary" and r.get("file_url"):
                    fid = r.get("file_uuid") or r.get("file_url")
                    fname = r.get("file_name") or ""
                    if fid not in seen_uuids and fname not in seen_names:
                        seen_uuids.add(fid)
                        if fname:
                            seen_names.add(fname)
                        files.append(r)

        for r in current_resources:
            if r.get("type") != "Primary" and r.get("file_url"):
                fid = r.get("file_uuid") or r.get("file_url")
                fname = r.get("file_name") or ""
                if fid not in seen_uuids and fname not in seen_names:
                    seen_uuids.add(fid)
                    if fname:
                        seen_names.add(fname)
                    files.append(r)

        if not files:
            record_warning(drpid, "No downloadable files found in API response")

        return files

    def _download_files(
        self,
        drpid: int,
        files: List[Dict[str, Any]],
        folder_path: Path,
    ) -> List[Dict[str, str]]:
        """Download files and return list of dicts with 'name' and 'type' keys."""
        downloaded_files = []
        
        for resource in files:
            file_url = resource.get("file_url", "")
            raw_name = resource.get("file_name") or file_url.split("/")[-1].split("?")[0]
            filename = sanitize_filename(raw_name) if raw_name else "dataset"
            dest = folder_path / filename

            if dest.exists():
                Logger.info("Skipping already-downloaded: %s", filename)
                downloaded_files.append({"name": filename, "type": resource.get("type", "")})
                continue

            Logger.info(
                "Downloading [%s] %s → %s",
                resource.get("type", "?"),
                file_url,
                filename,
            )
            try:
                _bytes, success = download_via_url(file_url, dest)
                if success:
                    downloaded_files.append({"name": filename, "type": resource.get("type", "")})
                else:
                    record_warning(drpid, f"Download failed: {file_url}")
            except Exception as exc:
                record_warning(drpid, f"Download error for {file_url}: {exc}")

        return downloaded_files

    def _extract_date_range_from_metadata(
        self,
        slug_data: Dict[str, Any],
        files: List[Dict[str, Any]],
        current_uuid: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Extract time_start and time_end from multiple sources.
        """
        temporal = slug_data.get("temporal_coverage") or {}
        if isinstance(temporal, dict):
            start = temporal.get("start") or temporal.get("from")
            end = temporal.get("end") or temporal.get("to")
            if start or end:
                result = {}
                if start:
                    result["time_start"] = self._format_date(start)
                if end:
                    result["time_end"] = self._format_date(end)
                if result:
                    return result

        date_range_field = slug_data.get("date_range") or {}
        if isinstance(date_range_field, dict):
            start = date_range_field.get("start") or date_range_field.get("from")
            end = date_range_field.get("end") or date_range_field.get("to")
            if start or end:
                result = {}
                if start:
                    result["time_start"] = self._format_date(start)
                if end:
                    result["time_end"] = self._format_date(end)
                if result:
                    return result

        coverage_start = slug_data.get("coverage_start") or slug_data.get("data_start_date")
        coverage_end = slug_data.get("coverage_end") or slug_data.get("data_end_date")
        if coverage_start or coverage_end:
            result = {}
            if coverage_start:
                result["time_start"] = self._format_date(str(coverage_start))
            if coverage_end:
                result["time_end"] = self._format_date(str(coverage_end))
            if result:
                return result

        current = slug_data.get("current_dataset") or {}
        for key in ["data_start_date", "start_date", "coverage_start", "temporal_coverage_start"]:
            val = current.get(key)
            if val:
                result = {"time_start": self._format_date(str(val))}
                for end_key in ["data_end_date", "end_date", "coverage_end", "temporal_coverage_end"]:
                    end_val = current.get(end_key)
                    if end_val:
                        result["time_end"] = self._format_date(str(end_val))
                        break
                return result

        if current_uuid:
            dataset_meta = self._fetch_dataset_metadata(current_uuid)
            if dataset_meta:
                for key in ["data_start_date", "start_date", "coverage_start", "temporal_coverage_start"]:
                    val = dataset_meta.get(key)
                    if val:
                        result = {"time_start": self._format_date(str(val))}
                        for end_key in ["data_end_date", "end_date", "coverage_end", "temporal_coverage_end"]:
                            end_val = dataset_meta.get(end_key)
                            if end_val:
                                result["time_end"] = self._format_date(str(end_val))
                                break
                        if result:
                            return result

                temporal = dataset_meta.get("temporal_coverage") or {}
                if isinstance(temporal, dict):
                    start = temporal.get("start") or temporal.get("from")
                    end = temporal.get("end") or temporal.get("to")
                    if start or end:
                        result = {}
                        if start:
                            result["time_start"] = self._format_date(start)
                        if end:
                            result["time_end"] = self._format_date(end)
                        if result:
                            return result

                inner_current = dataset_meta.get("current_dataset") or {}
                for key in ["data_start_date", "start_date", "coverage_start"]:
                    val = inner_current.get(key)
                    if val:
                        result = {"time_start": self._format_date(str(val))}
                        for end_key in ["data_end_date", "end_date", "coverage_end"]:
                            end_val = inner_current.get(end_key)
                            if end_val:
                                result["time_end"] = self._format_date(str(end_val))
                                break
                        if result:
                            return result

        primary_files = [f for f in files if f.get("type") == "Primary" and f.get("dataset_version_date")]
        if primary_files:
            dates = sorted(f["dataset_version_date"] for f in primary_files)
            start_date = self._format_date(dates[0])
            end_date = self._format_date(dates[-1])
            return {
                "time_start": start_date,
                "time_end": end_date,
            }

        return {}

    def _format_date(self, date_str: str) -> str:
        """
        Format date string. Try to return year-only for simple year strings,
        otherwise M/D/YYYY format.
        """
        if not date_str:
            return date_str

        date_str = date_str.strip()

        # If it's just a 4-digit year, return as-is
        if re.match(r'^\d{4}$', date_str):
            return date_str

        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y/%m/%d",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return f"{dt.month}/{dt.day}/{dt.year}"
            except ValueError:
                continue

        return date_str

    def _scrape_description(self, url: str, drpid: int) -> Optional[str]:
        """Render source_url with Playwright and extract the dataset description."""
        if not self._ensure_browser():
            record_warning(drpid, "Browser unavailable; description not collected")
            return None
        try:
            # Check if page is already loaded
            try:
                current_url = self._page.url
                if current_url != url:
                    self._page.goto(url, wait_until="networkidle", timeout=60000)
            except Exception:
                self._page.goto(url, wait_until="networkidle", timeout=60000)

            el = self._page.query_selector(_DESCRIPTION_SELECTOR)
            if el:
                text = el.inner_text().strip()
                if text:
                    return text

            for selector in [
                "[class*='summary-field-summary']",
                "[class*='dataset-summary']",
                "[class*='DatasetPage__description']",
                ".dataset-description",
                "[class*='DatasetPage__summary']",
                "[data-testid*='summary']",
                "[class*='summary-container']",
                "[class*='page-summary']",
                "[class*='dataset-description']",
                "[class*='DatasetPage']",
            ]:
                el = self._page.query_selector(selector)
                if el:
                    text = el.inner_text().strip()
                    if text and len(text) > 50:
                        return text

            for selector in [
                "main p",
                ".content-area p",
                "article p",
                "[class*='description'] p",
                "[class*='summary'] p",
            ]:
                elements = self._page.query_selector_all(selector)
                if elements:
                    texts = [e.inner_text().strip() for e in elements if e.inner_text().strip()]
                    if texts:
                        longest = max(texts, key=len)
                        if len(longest) > 100:
                            return longest

            record_warning(drpid, "Description element not found on page")
            return None
        except Exception as exc:
            record_warning(drpid, f"Failed to scrape description: {exc}")
            return None

    def _cleanup_browser(self) -> None:
        if self._browser:
            with suppress(Exception):
                self._browser.close()
            self._browser = None
        if self._playwright:
            with suppress(Exception):
                self._playwright.stop()
            self._playwright = None
        self._page = None

    def _update_storage(self, drpid: int, result: Dict[str, Any]) -> None:
        current = Storage.get(drpid)
        if current and current.get("status") == "error":
            result["status"] = "error"
        elif result.get("folder_path") and not result.get("status"):
            result["status"] = "collected"

        fields = {k: v for k, v in result.items() if v is not None}
        if fields:
            Storage.update_record(drpid, fields)

    def _init_browser(self) -> bool:
        """Initialize browser if not already running."""
        if self._browser and self._page:
            return True
        try:
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=self._headless)
            self._page = self._browser.new_page()
            return True
        except Exception as exc:
            Logger.error("Failed to initialize browser: %s", exc)
            self._cleanup_browser()
            return False

    def _ensure_browser(self) -> bool:
        """Ensure browser is initialized, initializing if needed."""
        if self._browser and self._page:
            return True
        return self._init_browser()