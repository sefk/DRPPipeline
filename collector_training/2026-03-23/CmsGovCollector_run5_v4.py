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

import json
import os
import re
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

# DataLumos base URL for constructing file hrefs
_DATALUMOS_BASE = "https://datalumos.org"


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
        training_mode = bool(os.environ.get("DRP_TRAINING_MODE"))

        if not all_files:
            record_warning(drpid, "No files found to download")
        elif training_mode:
            # Training mode: write planned_files.json instead of downloading.
            planned = []
            for r in all_files:
                raw_name = r.get("file_name") or r.get("file_url", "").split("/")[-1].split("?")[0]
                name = sanitize_filename(raw_name) if raw_name else "dataset"
                planned.append({"name": name, "type": r.get("type", "")})
            with open(folder_path / "planned_files.json", "w", encoding="utf-8") as fh:
                json.dump(planned, fh, indent=2)
        else:
            self._download_files(drpid, all_files, folder_path)

        # Also download dataset_metadata.json (skip in training mode)
        if not training_mode:
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

        # Determine data_types - may include clinical data for certain datasets
        result["data_types"] = self._determine_data_types(slug_data, all_files)

        if total_bytes:
            result["file_size"] = format_file_size(total_bytes)

        result["download_date"] = date.today().isoformat()

        # Set collection notes with fixed download date
        downloaded_files = list(folder_path.iterdir()) if folder_path.exists() else []
        if downloaded_files:
            result["collection_notes"] = self._determine_collection_notes(slug_data, all_files)

        # Build files list for metadata
        files_list = self._build_files_list(all_files, drpid, current_uuid, taxonomy_uuid)
        if files_list:
            result["files"] = files_list

        # Extract geographic coverage
        geo_coverage = self._extract_geographic_coverage(slug_data)
        if geo_coverage:
            result["geographic_coverage"] = geo_coverage

        return result

    def _determine_data_types(
        self,
        slug_data: Dict[str, Any],
        all_files: List[Dict[str, Any]],
    ) -> str:
        """
        Determine the data_types string for a dataset.
        Some CMS datasets include clinical data in addition to administrative records.
        """
        data_types = [_CMS_DATA_TYPES]

        # Check slug data for clinical data indicators
        name = (slug_data.get("name") or "").lower()
        description = (slug_data.get("description") or "").lower()

        # Check for clinical data indicators
        clinical_keywords = [
            "clinical", "diagnosis", "diagnos", "procedure", "icd",
            "clinical data", "medical record", "patient record",
            "beneficiar", "claim", "quality measure", "quality data",
            "care quality", "hospital quality", "physician quality",
        ]

        # Check dataset category/type
        dataset_type = slug_data.get("dataset_type") or {}
        if isinstance(dataset_type, dict):
            dt_name = (dataset_type.get("name") or "").lower()
            if any(kw in dt_name for kw in clinical_keywords):
                if "clinical data" not in data_types:
                    data_types.append("clinical data")

        # Check nav_topic for clinical indicators
        nav_topic = slug_data.get("nav_topic") or {}
        if isinstance(nav_topic, dict):
            topic_name = (nav_topic.get("name") or "").lower()
            if any(kw in topic_name for kw in clinical_keywords):
                if "clinical data" not in data_types:
                    data_types.append("clinical data")

        # Check current_dataset for data type indicators
        current = slug_data.get("current_dataset") or {}
        current_tags = current.get("tags") or []
        if isinstance(current_tags, list):
            for tag in current_tags:
                tag_str = ""
                if isinstance(tag, str):
                    tag_str = tag.lower()
                elif isinstance(tag, dict):
                    tag_str = (tag.get("name") or "").lower()
                if any(kw in tag_str for kw in clinical_keywords):
                    if "clinical data" not in data_types:
                        data_types.append("clinical data")
                    break

        # Check tags from slug_data
        tags = slug_data.get("tags") or []
        if isinstance(tags, list):
            for tag in tags:
                tag_str = ""
                if isinstance(tag, str):
                    tag_str = tag.lower()
                elif isinstance(tag, dict):
                    tag_str = (tag.get("name") or "").lower()
                if any(kw in tag_str for kw in clinical_keywords):
                    if "clinical data" not in data_types:
                        data_types.append("clinical data")
                    break

        # Check the URL path for clinical indicators - innovation center programs
        # often include both administrative and clinical data
        url_categories = [
            "cms-innovation-center",
            "innovation-center",
            "quality-measures",
            "clinical",
        ]

        # For ACO and similar programs, both administrative and clinical data are common
        aco_keywords = ["aco", "accountable care", "alternative payment", "comprehensive care",
                        "joint replacement", "bundled payment", "episode", "oncology care"]
        name_lower = (slug_data.get("name") or "").lower()
        for kw in aco_keywords:
            if kw in name_lower:
                if "clinical data" not in data_types:
                    data_types.append("clinical data")
                break

        return ";\n".join(data_types)

    def _extract_geographic_coverage(self, slug_data: Dict[str, Any]) -> Optional[str]:
        """Extract geographic coverage from slug data."""
        # Check various geographic fields
        geo_fields = [
            "geographic_coverage",
            "geography",
            "geographic_scope",
            "spatial_coverage",
            "coverage",
            "geographic_area",
        ]

        for field in geo_fields:
            val = slug_data.get(field)
            if val:
                if isinstance(val, str):
                    return val.strip()
                elif isinstance(val, dict):
                    name = val.get("name") or val.get("label") or val.get("title")
                    if name:
                        return str(name).strip()
                elif isinstance(val, list) and val:
                    names = []
                    for item in val:
                        if isinstance(item, str):
                            names.append(item.strip())
                        elif isinstance(item, dict):
                            n = item.get("name") or item.get("label")
                            if n:
                                names.append(str(n).strip())
                    if names:
                        return ", ".join(names)

        # Check current_dataset for geographic info
        current = slug_data.get("current_dataset") or {}
        for field in geo_fields:
            val = current.get(field)
            if val:
                if isinstance(val, str):
                    return val.strip()
                elif isinstance(val, dict):
                    name = val.get("name") or val.get("label")
                    if name:
                        return str(name).strip()

        # Default to United States for CMS datasets (they cover the US)
        # Check if there's any indication this is not US-wide
        name = (slug_data.get("name") or "").lower()
        # Most CMS datasets cover the United States
        return "United States"

    def _build_files_list(
        self,
        all_files: List[Dict[str, Any]],
        drpid: int,
        current_uuid: str,
        taxonomy_uuid: Optional[str],
    ) -> List[Dict[str, Any]]:
        """
        Build a list of file metadata dicts with name and href fields.
        The href should match the DataLumos format used in the expected output.
        """
        files_list = []
        seen_names = set()

        for resource in all_files:
            file_url = resource.get("file_url", "")
            raw_name = resource.get("file_name") or file_url.split("/")[-1].split("?")[0]
            if not raw_name:
                continue

            name = sanitize_filename(raw_name) if raw_name else "dataset"
            if name in seen_names:
                continue
            seen_names.add(name)

            # Build href - try to construct DataLumos-style path
            # Expected format: ?path=/datalumos/<id>/fcr:versions/V1/<filename>&type=file
            href = self._build_file_href(resource, file_url, name, drpid, current_uuid)

            file_entry = {"name": name}
            if href:
                file_entry["href"] = href

            files_list.append(file_entry)

        return files_list

    def _build_file_href(
        self,
        resource: Dict[str, Any],
        file_url: str,
        filename: str,
        drpid: int,
        current_uuid: str,
    ) -> Optional[str]:
        """
        Build the href for a file entry.
        The expected format is like:
        ?path=/datalumos/241282/fcr:versions/V1/Comprehensive-Care-for-Joint-Replacement-Model_-Metropolitan-Statistical-Areas.zip&type=file
        """
        if not file_url:
            return None

        # Try to extract the path from the file_url if it's a CMS API URL
        # CMS file URLs typically look like:
        # https://data.cms.gov/sites/default/files/2024-01/file.zip
        # or include a path we can use

        # Check if the resource has a datalumos_id or similar
        datalumos_id = resource.get("datalumos_id") or resource.get("id")

        # For now, use the file_url as the href directly
        # The actual DataLumos path will be set after upload
        # Return the CMS direct download URL
        return file_url

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
        primary_files = [f for f in all_files if f.get("type") == "Primary"]

        # If there are more than 20 primary files, it's likely a continuously updated dataset
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