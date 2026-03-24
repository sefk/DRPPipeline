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
"""

from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
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


class CmsGovCollector:
    """
    Collector for data.cms.gov dataset pages.

    Uses the data-api/v1 REST endpoints to extract metadata and download
    all historical Primary files plus ancillary files. Uses Playwright to
    scrape the description, which is only available in the rendered page.
    """

    def __init__(self, headless: bool = True) -> None:
        self._headless = headless
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None

    def run(self, drpid: int) -> None:
        """
        Run the collector for a single project (ModuleProtocol interface).

        Args:
            drpid: The DRPID of the project to process.
        """
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

    def _collect(self, url: str, drpid: int) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        if not is_valid_url(url):
            record_error(drpid, f"Invalid URL: {url}")
            return result

        url_path = self._extract_path(url)
        if not url_path:
            record_error(drpid, f"Cannot extract path from URL: {url}")
            return result

        slug_data = self._fetch_slug(url_path)
        if not slug_data:
            record_error(drpid, f"Slug API returned nothing for path: {url_path}")
            return result

        result.update(self._parse_slug_metadata(slug_data))

        description = self._scrape_description(url, drpid)
        if description:
            # Clean up non-breaking spaces and normalize whitespace
            description = description.replace('\xa0', ' ')
            result["summary"] = description

        current_uuid = (slug_data.get("current_dataset") or {}).get("uuid")
        taxonomy_uuid = slug_data.get("uuid")

        if not current_uuid:
            record_error(drpid, "Slug API response missing current_dataset.uuid")
            return result

        folder_path = create_output_folder(Path(Args.base_output_dir), drpid)
        if not folder_path:
            record_error(drpid, "Failed to create output folder")
            return result
        result["folder_path"] = folder_path.as_posix()

        # Collect files: all historical Primary files + ancillaries (once each)
        all_files = self._gather_files(drpid, current_uuid, taxonomy_uuid)
        if not all_files:
            record_warning(drpid, "No files found to download")
        else:
            self._download_files(drpid, all_files, folder_path)

        # Also download dataset_metadata.json
        self._download_dataset_metadata(drpid, current_uuid, folder_path)

        # Extract time_start / time_end from dataset metadata
        date_range = self._extract_date_range_from_metadata(slug_data, all_files, current_uuid)
        if date_range.get("time_start"):
            result["time_start"] = date_range["time_start"]
        if date_range.get("time_end"):
            result["time_end"] = date_range["time_end"]

        exts, total_bytes = folder_extensions_and_size(folder_path)
        if exts:
            result["extensions"] = ",".join(exts)
        # Always set data_types to administrative records data for CMS
        result["data_types"] = _CMS_DATA_TYPES

        if total_bytes:
            result["file_size"] = format_file_size(total_bytes)

        result["download_date"] = date.today().isoformat()

        # Set collection notes with fixed download date
        # Only set if the dataset has a fixed/historical end date (not continuously updated)
        downloaded_files = list(folder_path.iterdir()) if folder_path.exists() else []
        if downloaded_files:
            result["collection_notes"] = self._determine_collection_notes(slug_data, all_files)

        return result

    def _determine_collection_notes(
        self,
        slug_data: Dict[str, Any],
        all_files: List[Dict[str, Any]],
    ) -> Optional[str]:
        """
        Determine whether to set collection_notes.
        For datasets that are continuously updated (like pending enrollment files),
        collection_notes should be None.
        For historical/static datasets, return the fixed download date note.
        """
        # Check if dataset appears to be continuously updated
        # by looking at file naming patterns or update frequency
        # Datasets with many files dated across a long span are likely continuously updated
        # and should NOT have collection_notes set

        # Check for a large number of files with date patterns in names
        # (like PendingInitialLandTsNonPhysicians_20230601.csv)
        primary_files = [f for f in all_files if f.get("type") == "Primary"]

        # If there are more than 20 primary files, it's likely a continuously updated dataset
        # that shouldn't have collection_notes
        if len(primary_files) > 20:
            return None

        # Otherwise, use the fixed download date
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
            import json
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

        # Use the full agency name
        fields["agency"] = _CMS_AGENCY

        # Extract keywords from tags/topics available in the API
        kws = self._extract_keywords(slug_data)
        if kws:
            fields["keywords"] = kws

        return fields

    def _extract_keywords(self, slug_data: Dict[str, Any]) -> str:
        """
        Extract meaningful keywords from slug data.
        The expected format is comma-separated tags like:
        'Medicare, CMS, Original Medicare, Value-Based Care, Payment Models'

        We look at tags, themes, nav_topic, and dataset_type but prefer
        structured tag lists that match the expected CMS keyword format.
        """
        keywords = []

        # Try 'tags' field first - most likely to have structured keywords
        tags = slug_data.get("tags") or []
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, str) and tag.strip():
                    keywords.append(tag.strip())
                elif isinstance(tag, dict):
                    tag_name = tag.get("name") or tag.get("label") or tag.get("title") or ""
                    if tag_name and tag_name not in keywords:
                        keywords.append(tag_name.strip())

        # Try 'keywords' field directly
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

        # Try 'themes' field
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

        # Try current_dataset tags/keywords
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

        # Only add nav_topic and dataset_type if we have no other keywords
        # These tend to be broader categories, not the specific tags expected
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

        # Historical Primary files via dataset-type (all years)
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
            # Fall back to current resources Primary only
            for r in current_resources:
                if r.get("type") == "Primary" and r.get("file_url"):
                    fid = r.get("file_uuid") or r.get("file_url")
                    fname = r.get("file_name") or ""
                    if fid not in seen_uuids and fname not in seen_names:
                        seen_uuids.add(fid)
                        if fname:
                            seen_names.add(fname)
                        files.append(r)

        # Ancillary files from current resources (Data Dictionary, Methodology, etc.)
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
    ) -> None:
        for resource in files:
            file_url = resource.get("file_url", "")
            raw_name = resource.get("file_name") or file_url.split("/")[-1].split("?")[0]
            filename = sanitize_filename(raw_name) if raw_name else "dataset"
            dest = folder_path / filename

            if dest.exists():
                Logger.info("Skipping already-downloaded: %s", filename)
                continue

            Logger.info(
                "Downloading [%s] %s → %s",
                resource.get("type", "?"),
                file_url,
                filename,
            )
            try:
                _bytes, success = download_via_url(file_url, dest)
                if not success:
                    record_warning(drpid, f"Download failed: {file_url}")
            except Exception as exc:
                record_warning(drpid, f"Download error for {file_url}: {exc}")

    def _extract_date_range_from_metadata(
        self,
        slug_data: Dict[str, Any],
        files: List[Dict[str, Any]],
        current_uuid: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Extract time_start and time_end from multiple sources.

        The expected format is M/D/YYYY (e.g. '1/1/2017', '12/31/2017').

        Priority:
        1. temporal_coverage or date_range fields from slug_data
        2. dataset version temporal metadata from current dataset API
        3. dataset_version_date on Primary resources (earliest → time_start, latest → time_end)
        """
        # Try temporal coverage from slug data
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

        # Try date_range field
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

        # Try coverage_start / coverage_end
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

        # Try current_dataset metadata for temporal info
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

        # Try fetching full dataset metadata for temporal coverage
        if current_uuid:
            dataset_meta = self._fetch_dataset_metadata(current_uuid)
            if dataset_meta:
                # Look through all fields for temporal data
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

                # Check nested fields in dataset metadata
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

                # Try to find temporal info in current_dataset nested inside dataset_meta
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

        # Fall back: infer from Primary resource version dates
        # For datasets with many primary files (continuously updated),
        # use the most recent file date as time_end and earliest as time_start
        primary_files = [f for f in files if f.get("type") == "Primary" and f.get("dataset_version_date")]
        if primary_files:
            dates = sorted(f["dataset_version_date"] for f in primary_files)
            # For continuously updated datasets, time_start should be the date of the first file
            # and time_end should be the date of the most recent file
            start_date = self._format_date(dates[0])
            end_date = self._format_date(dates[-1])
            return {
                "time_start": start_date,
                "time_end": end_date,
            }

        return {}

    def _format_date(self, date_str: str) -> str:
        """
        Format date string to M/D/YYYY format if possible.
        The expected format based on examples is like '1/1/2017', '12/31/2017'.
        """
        if not date_str:
            return date_str

        date_str = date_str.strip()

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
                # Return in M/D/YYYY format (no leading zeros)
                return f"{dt.month}/{dt.day}/{dt.year}"
            except ValueError:
                continue

        # Return as-is if we can't parse it
        return date_str

    def _scrape_description(self, url: str, drpid: int) -> Optional[str]:
        """Render source_url with Playwright and extract the dataset description."""
        if not self._init_browser():
            record_warning(drpid, "Browser unavailable; description not collected")
            return None
        try:
            self._page.goto(url, wait_until="networkidle", timeout=60000)

            # Try the primary selector
            el = self._page.query_selector(_DESCRIPTION_SELECTOR)
            if el:
                text = el.inner_text().strip()
                if text:
                    return text

            # Try alternative selectors if primary not found
            for selector in [
                "[class*='summary-field-summary']",
                "[class*='dataset-summary']",
                "[class*='DatasetPage__description']",
                ".dataset-description",
                "[class*='DatasetPage__summary']",
                "[data-testid*='summary']",
            ]:
                el = self._page.query_selector(selector)
                if el:
                    text = el.inner_text().strip()
                    if text:
                        return text

            # Try to find any large text block that might be the description
            # by looking for common content containers
            for selector in [
                "main p",
                ".content-area p",
                "article p",
            ]:
                elements = self._page.query_selector_all(selector)
                if elements:
                    texts = [e.inner_text().strip() for e in elements if e.inner_text().strip()]
                    if texts:
                        # Return the longest paragraph as it's likely the description
                        longest = max(texts, key=len)
                        if len(longest) > 100:
                            return longest

            record_warning(drpid, "Description element not found on page")
            return None
        except Exception as exc:
            record_warning(drpid, f"Failed to scrape description: {exc}")
            return None

    def _init_browser(self) -> bool:
        try:
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=self._headless)
            self._page = self._browser.new_page()
            return True
        except Exception as exc:
            Logger.error("Failed to initialize browser: %s", exc)
            self._cleanup_browser()
            return False

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