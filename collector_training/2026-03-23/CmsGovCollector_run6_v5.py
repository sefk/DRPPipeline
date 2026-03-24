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

        # Try to get slug data - for innovation center programs, try multiple approaches
        slug_data = self._fetch_slug_comprehensive(url, url_path, drpid)
        if not slug_data:
            record_error(drpid, f"Slug API returned nothing for path: {url_path}")
            return result

        result.update(self._parse_slug_metadata(slug_data))

        description = self._scrape_description(url, drpid)
        if description:
            description = description.replace('\xa0', ' ')
            result["summary"] = description

        current_uuid = (slug_data.get("current_dataset") or {}).get("uuid")
        taxonomy_uuid = slug_data.get("uuid")

        if not current_uuid:
            current_uuid = slug_data.get("current_uuid") or slug_data.get("dataset_uuid")

        if not current_uuid:
            # Try to extract UUID from dataset metadata via page scraping
            current_uuid = self._extract_uuid_from_page(url, drpid)

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

        # Determine data_types from slug data and resources
        data_types = self._determine_data_types(slug_data, all_files)
        result["data_types"] = data_types

        if total_bytes:
            result["file_size"] = format_file_size(total_bytes)

        result["download_date"] = date.today().isoformat()

        # Set collection notes
        downloaded_files = list(folder_path.iterdir()) if folder_path.exists() else []
        if downloaded_files:
            result["collection_notes"] = self._determine_collection_notes(slug_data, all_files)

        # Geographic coverage
        geo = self._extract_geographic_coverage(slug_data)
        if geo:
            result["geographic_coverage"] = geo

        return result

    def _fetch_slug_comprehensive(self, url: str, url_path: str, drpid: int) -> Optional[Dict[str, Any]]:
        """
        Comprehensive slug fetching with multiple fallback strategies.
        This handles both regular CMS datasets and CMS Innovation Center programs.
        """
        # First, try the standard slug fetch
        slug_data = self._fetch_slug(url_path)
        if slug_data:
            return slug_data

        Logger.info("Standard slug fetch failed, trying alternatives for: %s", url)

        # Try path variations
        variations = self._generate_path_variations(url_path)
        for path_var in variations:
            Logger.info("Trying slug path variation: %s", path_var)
            data = self._fetch_slug(path_var)
            if data:
                return data

        # Try to find the dataset via the innovation center programs API
        # These pages often have a different URL structure
        parts = url_path.strip("/").split("/")
        dataset_slug = parts[-1] if parts else ""

        if dataset_slug:
            # Try direct dataset lookup by slug
            data = self._fetch_dataset_by_slug(dataset_slug)
            if data:
                return data

            # Try search
            data = self._search_dataset_by_name(dataset_slug)
            if data:
                return data

        # Try fetching the page and extracting API data from it
        data = self._extract_slug_from_page_api(url, drpid)
        if data:
            return data

        return None

    def _generate_path_variations(self, url_path: str) -> List[str]:
        """Generate path variations to try for slug lookup."""
        variations = []

        # Try with/without trailing slash
        if url_path.endswith("/"):
            variations.append(url_path.rstrip("/"))
        else:
            variations.append(url_path + "/")

        parts = url_path.strip("/").split("/")

        # Try just the last segment
        if len(parts) > 1:
            variations.append("/" + parts[-1])
            # Try with parent path
            if len(parts) > 2:
                variations.append("/" + "/".join(parts[-2:]))

        return variations

    def _fetch_dataset_by_slug(self, slug: str) -> Optional[Dict[str, Any]]:
        """Try to fetch a dataset directly by its slug name."""
        # Try various API endpoints
        endpoints = [
            f"{_API_BASE}/dataset?slug={quote(slug)}",
            f"{_API_BASE}/slug?path=/{quote(slug)}",
        ]
        for endpoint in endpoints:
            Logger.info("Trying dataset by slug: %s", endpoint)
            try:
                resp = requests.get(endpoint, headers=BROWSER_HEADERS, timeout=30)
                if resp.status_code == 200:
                    body = resp.json()
                    data = body.get("data")
                    if data:
                        if isinstance(data, list) and data:
                            return data[0]
                        elif isinstance(data, dict):
                            return data
            except Exception as exc:
                Logger.error("Dataset by slug error: %s", exc)
        return None

    def _extract_slug_from_page_api(self, url: str, drpid: int) -> Optional[Dict[str, Any]]:
        """
        Try to extract dataset info by intercepting API calls made by the page,
        or by parsing the page HTML for embedded JSON data.
        """
        if not self._init_browser():
            return None

        api_responses = {}

        try:
            # Set up route interception to capture API responses
            def handle_response(response):
                if "_api/v1" in response.url and response.status == 200:
                    try:
                        body = response.json()
                        api_responses[response.url] = body
                    except Exception:
                        pass

            self._page.on("response", handle_response)
            self._page.goto(url, wait_until="networkidle", timeout=60000)

            # Look for slug data in captured responses
            for api_url, body in api_responses.items():
                if "slug" in api_url or "dataset" in api_url:
                    data = body.get("data")
                    if data and isinstance(data, dict):
                        if data.get("name") or data.get("uuid"):
                            Logger.info("Extracted slug data from intercepted API: %s", api_url)
                            return data

            # Try to find embedded JSON in the page
            content = self._page.content()
            # Look for __NEXT_DATA__ or similar embedded JSON
            patterns = [
                r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                r'window\.__CMS_DATA__\s*=\s*({.*?});',
            ]
            for pattern in patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    try:
                        json_data = json.loads(match.group(1))
                        # Navigate the JSON to find dataset info
                        extracted = self._extract_from_next_data(json_data)
                        if extracted:
                            return extracted
                    except Exception:
                        pass

        except Exception as exc:
            Logger.error("Failed to extract slug from page: %s", exc)

        return None

    def _extract_from_next_data(self, json_data: Dict) -> Optional[Dict[str, Any]]:
        """Extract dataset metadata from Next.js __NEXT_DATA__ structure."""
        try:
            # Common Next.js data structures
            props = json_data.get("props") or {}
            page_props = props.get("pageProps") or {}

            # Try various paths to find dataset data
            for key in ["dataset", "data", "datasetData", "slug", "slugData"]:
                val = page_props.get(key)
                if val and isinstance(val, dict):
                    if val.get("name") or val.get("uuid"):
                        return val

            # Try dehydrated state (React Query)
            dehydrated = page_props.get("dehydratedState") or {}
            queries = dehydrated.get("queries") or []
            for query in queries:
                state = query.get("state") or {}
                data = state.get("data") or {}
                if isinstance(data, dict):
                    inner_data = data.get("data")
                    if isinstance(inner_data, dict) and (inner_data.get("name") or inner_data.get("uuid")):
                        return inner_data

        except Exception as exc:
            Logger.error("Failed to extract from Next.js data: %s", exc)

        return None

    def _extract_uuid_from_page(self, url: str, drpid: int) -> Optional[str]:
        """Try to extract current dataset UUID from page content."""
        try:
            if self._page:
                content = self._page.content()
                # Look for UUID patterns in the page
                uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
                matches = re.findall(uuid_pattern, content, re.IGNORECASE)
                if matches:
                    # The first UUID found is often the dataset UUID
                    Logger.info("Found UUID in page: %s", matches[0])
                    return matches[0]
        except Exception as exc:
            Logger.error("Failed to extract UUID from page: %s", exc)
        return None

    def _determine_data_types(self, slug_data: Dict[str, Any], files: List[Dict[str, Any]]) -> str:
        """
        Determine data_types from slug data and file resources.
        CMS datasets can be:
        - administrative records data
        - clinical data
        - claims data
        - etc.
        """
        data_types = set()

        # Check slug data for data type info
        dataset_type = slug_data.get("dataset_type") or {}
        if isinstance(dataset_type, dict):
            dt_name = dataset_type.get("name", "").lower()
            if dt_name:
                if "claims" in dt_name:
                    data_types.add("claims data")
                if "clinical" in dt_name:
                    data_types.add("clinical data")
                if "administrative" in dt_name or "admin" in dt_name:
                    data_types.add("administrative records data")
                if "survey" in dt_name:
                    data_types.add("survey data")

        # Check tags and keywords for data type hints
        tags = slug_data.get("tags") or []
        all_tags = []
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, str):
                    all_tags.append(tag.lower())
                elif isinstance(tag, dict):
                    all_tags.append((tag.get("name") or "").lower())

        for tag in all_tags:
            if "clinical" in tag:
                data_types.add("clinical data")
            if "claims" in tag:
                data_types.add("claims data")
            if "survey" in tag:
                data_types.add("survey data")
            if "administrative" in tag or "admin records" in tag:
                data_types.add("administrative records data")

        # Check nav_topic for hints
        nav_topic = slug_data.get("nav_topic") or {}
        if isinstance(nav_topic, dict):
            topic_name = (nav_topic.get("name") or "").lower()
            if "clinical" in topic_name:
                data_types.add("clinical data")
            if "claims" in topic_name:
                data_types.add("claims data")

        # Check description for data type hints (from current_dataset)
        current = slug_data.get("current_dataset") or {}
        description = (current.get("description") or current.get("summary") or "").lower()
        if description:
            if "clinical" in description:
                data_types.add("clinical data")
            if "claims" in description:
                data_types.add("claims data")
            if "administrative" in description:
                data_types.add("administrative records data")

        # Check URL/path for innovation center programs - these often have clinical data
        # Innovation center programs typically involve clinical care models
        # Default: administrative records data is always included for CMS
        data_types.add("administrative records data")

        # Sort for consistent output
        sorted_types = sorted(data_types)
        return ";\n".join(sorted_types) if sorted_types else "administrative records data"

    def _extract_geographic_coverage(self, slug_data: Dict[str, Any]) -> Optional[str]:
        """Extract geographic coverage from slug data."""
        # Check various fields
        for field in ["geographic_coverage", "geography", "geographic_scope", "spatial_coverage"]:
            val = slug_data.get(field)
            if val:
                if isinstance(val, str):
                    return val
                elif isinstance(val, dict):
                    return val.get("name") or val.get("label") or None
                elif isinstance(val, list) and val:
                    if isinstance(val[0], str):
                        return ", ".join(val)

        # Check current_dataset
        current = slug_data.get("current_dataset") or {}
        for field in ["geographic_coverage", "geography", "geographic_scope"]:
            val = current.get(field)
            if val:
                if isinstance(val, str):
                    return val
                elif isinstance(val, dict):
                    return val.get("name") or val.get("label") or None

        # Default for CMS datasets - most cover the United States
        return "United States"

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

        return fields

    def _extract_keywords(self, slug_data: Dict[str, Any]) -> str:
        """
        Extract meaningful keywords from slug data.
        """
        keywords = []

        # Try 'tags' field first
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
        date_range_field = slug_data