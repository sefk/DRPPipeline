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

For innovation center / special program URLs that don't match the slug API,
we fall back to scraping the page directly and using the DataLumos file URL
pattern derived from the URL path.
"""

import json
import os
import re
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse

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

# DataLumos base URL pattern for file downloads
_DATALUMOS_BASE = "https://datalumos.org/datalumos"


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

        # First try standard slug API
        slug_data = self._fetch_slug(url_path)

        # If slug fails, try alternative approaches
        if not slug_data:
            slug_data = self._fetch_slug_with_fallback(url, url_path, drpid)

        # Even if slug_data is None, we can still try scraping
        if slug_data:
            result.update(self._parse_slug_metadata(slug_data))
        else:
            Logger.warning("No slug data found for %s, will rely on page scraping", url)
            # Set basic agency info even without slug data
            result["agency"] = _CMS_AGENCY

        # Always scrape the page for description and potentially title
        description, page_title, page_files, page_metadata = self._scrape_page_full(url, drpid)

        if description:
            description = description.replace('\xa0', ' ')
            result["summary"] = description

        # Use page title if no title from slug or slug title is empty
        if not result.get("title") and page_title:
            result["title"] = page_title

        # Extract metadata from page if available
        if page_metadata:
            if not result.get("keywords") and page_metadata.get("keywords"):
                result["keywords"] = page_metadata["keywords"]
            if not result.get("time_start") and page_metadata.get("time_start"):
                result["time_start"] = page_metadata["time_start"]
            if not result.get("time_end") and page_metadata.get("time_end"):
                result["time_end"] = page_metadata["time_end"]
            if page_metadata.get("data_types"):
                result["data_types"] = page_metadata["data_types"]
            if not result.get("geographic_coverage") and page_metadata.get("geographic_coverage"):
                result["geographic_coverage"] = page_metadata["geographic_coverage"]

        current_uuid = None
        taxonomy_uuid = None

        if slug_data:
            current_uuid = (slug_data.get("current_dataset") or {}).get("uuid")
            taxonomy_uuid = slug_data.get("uuid")

            if not current_uuid:
                current_uuid = slug_data.get("current_uuid") or slug_data.get("dataset_uuid")

        folder_path = create_output_folder(Path(Args.base_output_dir), drpid)
        if not folder_path:
            record_error(drpid, "Failed to create output folder")
            return result
        result["folder_path"] = folder_path.as_posix()

        # Collect files
        all_files = []
        if current_uuid:
            all_files = self._gather_files(drpid, current_uuid, taxonomy_uuid)

        # If no files from API and we have page files, use those
        if not all_files and page_files:
            all_files = page_files
            Logger.info("Using %d file(s) scraped from page", len(all_files))

        # If still no files, try to infer file from URL pattern (DataLumos)
        if not all_files:
            inferred = self._infer_files_from_url(url, url_path)
            if inferred:
                all_files = inferred
                Logger.info("Using %d inferred file(s) from URL pattern", len(all_files))

        # If still no files, try fetching from the CMS data API directly
        if not all_files:
            api_files = self._fetch_files_from_data_api(url_path)
            if api_files:
                all_files = api_files
                Logger.info("Using %d file(s) from data API", len(all_files))

        training_mode = bool(os.environ.get("DRP_TRAINING_MODE"))

        if not all_files:
            record_warning(drpid, "No files found to download")
        elif training_mode:
            planned = []
            for r in all_files:
                raw_name = r.get("file_name") or r.get("name") or r.get("file_url", "").split("/")[-1].split("?")[0]
                name = sanitize_filename(raw_name) if raw_name else "dataset"
                planned.append({"name": name, "type": r.get("type", "Primary")})
            with open(folder_path / "planned_files.json", "w", encoding="utf-8") as fh:
                json.dump(planned, fh, indent=2)
        else:
            self._download_files(drpid, all_files, folder_path)

        # Also download dataset_metadata.json (skip in training mode)
        if not training_mode and current_uuid:
            self._download_dataset_metadata(drpid, current_uuid, folder_path)

        # Extract time_start / time_end from dataset metadata
        if slug_data:
            date_range = self._extract_date_range_from_metadata(slug_data, all_files, current_uuid)
            if date_range.get("time_start") and not result.get("time_start"):
                result["time_start"] = date_range["time_start"]
            if date_range.get("time_end") and not result.get("time_end"):
                result["time_end"] = date_range["time_end"]

        # Try to extract dates from page metadata if still not set
        if not result.get("time_start") and page_metadata.get("time_start"):
            result["time_start"] = page_metadata["time_start"]
        if not result.get("time_end") and page_metadata.get("time_end"):
            result["time_end"] = page_metadata["time_end"]

        exts, total_bytes = folder_extensions_and_size(folder_path)
        if exts:
            result["extensions"] = ",".join(exts)

        # Set data_types - check if we got clinical data from page metadata
        if not result.get("data_types"):
            result["data_types"] = _CMS_DATA_TYPES

        if total_bytes:
            result["file_size"] = format_file_size(total_bytes)

        result["download_date"] = date.today().isoformat()

        downloaded_files = list(folder_path.iterdir()) if folder_path.exists() else []
        if downloaded_files:
            result["collection_notes"] = self._determine_collection_notes(slug_data or {}, all_files)

        return result

    def _fetch_files_from_data_api(self, url_path: str) -> List[Dict[str, Any]]:
        """
        Try to fetch files directly from the CMS data API using the URL path.
        This handles innovation center and other special program pages.
        """
        files = []
        parts = url_path.strip("/").split("/")
        if not parts:
            return files

        dataset_slug = parts[-1]

        # Try fetching the dataset resources via the dataset slug directly
        # CMS stores datasets at /data-api/v1/dataset/<slug>/resources pattern
        endpoints_to_try = [
            f"{_API_BASE}/dataset/{dataset_slug}/resources",
        ]

        # Also try by searching for the dataset
        search_url = f"{_API_BASE}/slug?path={quote(url_path)}"
        try:
            resp = requests.get(search_url, headers=BROWSER_HEADERS, timeout=30)
            if resp.ok:
                body = resp.json()
                data = body.get("data") or {}
                if isinstance(data, dict):
                    uuid = (data.get("current_dataset") or {}).get("uuid")
                    if uuid:
                        endpoints_to_try.insert(0, f"{_API_BASE}/dataset/{uuid}/resources")
        except Exception:
            pass

        for endpoint in endpoints_to_try:
            try:
                resp = requests.get(endpoint, headers=BROWSER_HEADERS, timeout=30)
                if resp.ok:
                    body = resp.json()
                    resources = body.get("data") or []
                    if isinstance(resources, list):
                        for r in resources:
                            file_url = r.get("file_url") or r.get("url") or ""
                            if file_url:
                                files.append({
                                    "file_url": file_url,
                                    "file_name": r.get("file_name") or r.get("name") or dataset_slug,
                                    "type": r.get("type") or r.get("resource_type") or "Primary",
                                })
                        if files:
                            break
            except Exception as exc:
                Logger.error("Data API fetch error (%s): %s", endpoint, exc)

        return files

    def _infer_files_from_url(self, url: str, url_path: str) -> List[Dict[str, Any]]:
        """
        Try to infer downloadable file URLs from the page URL pattern.
        For CMS innovation center pages, files are often stored in DataLumos
        with a predictable naming pattern based on the URL path.
        """
        files = []

        # Extract the dataset slug from the URL path
        parts = url_path.strip("/").split("/")
        if not parts:
            return files

        dataset_slug = parts[-1]

        # Try to find the DataLumos ID from the page via API
        search_results = self._search_datasets_api(dataset_slug)
        if search_results:
            for item in search_results:
                file_url = item.get("file_url") or item.get("download_url")
                file_name = item.get("file_name") or item.get("name")
                if file_url:
                    files.append({
                        "file_url": file_url,
                        "file_name": file_name or dataset_slug,
                        "type": "Primary",
                    })

        return files

    def _search_datasets_api(self, keyword: str) -> List[Dict[str, Any]]:
        """Search CMS datasets API for files matching a keyword."""
        results = []
        search_url = f"{_API_BASE}/dataset?keyword={quote(keyword)}&size=10"
        Logger.info("Searching CMS API: %s", search_url)
        try:
            resp = requests.get(search_url, headers=BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            data = body.get("data") or []
            if isinstance(data, list):
                results.extend(data)
        except Exception as exc:
            Logger.error("CMS search API error: %s", exc)
        return results

    def _scrape_page_full(
        self, url: str, drpid: int
    ) -> Tuple[Optional[str], Optional[str], List[Dict[str, Any]], Dict[str, Any]]:
        """
        Render source_url with Playwright and extract:
        - description text
        - page title
        - file download links
        - additional metadata (keywords, dates, data types, geographic coverage)

        Returns (description, title, files, metadata_dict)
        """
        if not self._init_browser():
            record_warning(drpid, "Browser unavailable; description not collected")
            return None, None, [], {}

        description = None
        title = None
        files = []
        metadata = {}

        try:
            self._page.goto(url, wait_until="networkidle", timeout=60000)

            # Wait a bit for dynamic content to load
            try:
                self._page.wait_for_timeout(3000)
            except Exception:
                pass

            # Extract description - try multiple selectors
            description = self._extract_description()

            # Extract page title
            title = self._extract_page_title()

            # Extract file download links from the page
            files = self._extract_page_files()

            # Extract additional metadata from page
            metadata = self._extract_page_metadata()

            # Try to extract dates from page content if not in metadata
            if not metadata.get("time_start") or not metadata.get("time_end"):
                date_info = self._extract_dates_from_page()
                if date_info.get("time_start") and not metadata.get("time_start"):
                    metadata["time_start"] = date_info["time_start"]
                if date_info.get("time_end") and not metadata.get("time_end"):
                    metadata["time_end"] = date_info["time_end"]

            # Try to extract keywords from page if not in metadata
            if not metadata.get("keywords"):
                keywords = self._extract_keywords_from_page()
                if keywords:
                    metadata["keywords"] = keywords

            # Try to extract geographic coverage from page
            if not metadata.get("geographic_coverage"):
                geo = self._extract_geographic_coverage_from_page()
                if geo:
                    metadata["geographic_coverage"] = geo

        except Exception as exc:
            record_warning(drpid, f"Failed to scrape page: {exc}")

        return description, title, files, metadata

    def _extract_description(self) -> Optional[str]:
        """Extract description/summary from the page."""
        if not self._page:
            return None

        description = None

        # Try primary selector
        el = self._page.query_selector(_DESCRIPTION_SELECTOR)
        if el:
            text = el.inner_text().strip()
            if text:
                description = text

        if not description:
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
                "[class*='description-content']",
                "[class*='about-dataset']",
                "[class*='dataset-about']",
                ".field--name-body",
                ".field-body",
                "[class*='field-body']",
            ]:
                try:
                    el = self._page.query_selector(selector)
                    if el:
                        text = el.inner_text().strip()
                        if text and len(text) > 50:
                            description = text
                            break
                except Exception:
                    continue

        if not description:
            for selector in ["main p", ".content-area p", "article p",
                              "[class*='description'] p", "[class*='summary'] p",
                              "[class*='content'] p", "section p"]:
                try:
                    elements = self._page.query_selector_all(selector)
                    if elements:
                        texts = [e.inner_text().strip() for e in elements if e.inner_text().strip()]
                        if texts:
                            longest = max(texts, key=len)
                            if len(longest) > 100:
                                description = longest
                                break
                except Exception:
                    continue

        # Try to get description from JSON-LD data
        if not description:
            try:
                scripts = self._page.query_selector_all("script[type='application/ld+json']")
                for script in scripts:
                    try:
                        content = script.inner_text()
                        data = json.loads(content)
                        desc = data.get("description") or ""
                        if desc and len(desc) > 50:
                            description = desc
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        # Try Open Graph description
        if not description:
            try:
                meta_desc = self._page.query_selector("meta[property='og:description']")
                if not meta_desc:
                    meta_desc = self._page.query_selector("meta[name='description']")
                if meta_desc:
                    content = meta_desc.get_attribute("content")
                    if content and len(content) > 50:
                        description = content
            except Exception:
                pass

        return description

    def _extract_dates_from_page(self) -> Dict[str, str]:
        """Extract temporal coverage dates from the page content."""
        if not self._page:
            return {}

        dates = {}

        try:
            # Look for date-related elements on the page
            page_text = self._page.inner_text("body")

            # Look for patterns like "January 2020 - December 2023" or "2020-2023"
            # or "Data through December 2023"
            year_range_pattern = re.search(r'(\d{4})\s*[-–]\s*(\d{4})', page_text)
            if year_range_pattern:
                dates["time_start"] = year_range_pattern.group(1)
                dates["time_end"] = year_range_pattern.group(2)

            # Look for structured metadata fields
            date_selectors = [
                "[class*='date-range']",
                "[class*='temporal']",
                "[class*='coverage-date']",
                "[class*='time-period']",
                "dt:has-text('Date Range') + dd",
                "dt:has-text('Temporal') + dd",
                "dt:has-text('Coverage') + dd",
            ]

            for selector in date_selectors:
                try:
                    el = self._page.query_selector(selector)
                    if el:
                        text = el.inner_text().strip()
                        if text:
                            # Try to parse dates from text
                            yr_match = re.findall(r'\d{4}', text)
                            if len(yr_match) >= 2:
                                dates["time_start"] = yr_match[0]
                                dates["time_end"] = yr_match[-1]
                            elif len(yr_match) == 1:
                                dates["time_start"] = yr_match[0]
                            break
                except Exception:
                    continue

        except Exception as exc:
            Logger.error("Error extracting dates from page: %s", exc)

        return dates

    def _extract_keywords_from_page(self) -> Optional[str]:
        """Extract keywords/tags from the page."""
        if not self._page:
            return None

        try:
            # Look for tag/keyword elements
            tag_selectors = [
                "[class*='tag']",
                "[class*='keyword']",
                "[class*='topic']",
                "[class*='category']",
                "a[href*='tag']",
                "a[href*='keyword']",
                "a[href*='topic']",
                ".tags a",
                ".keywords a",
            ]

            keywords = []
            for selector in tag_selectors:
                try:
                    elements = self._page.query_selector_all(selector)
                    for el in elements:
                        text = el.inner_text().strip()
                        if text and len(text) > 1 and len(text) < 100 and text not in keywords:
                            keywords.append(text)
                    if keywords:
                        break
                except Exception:
                    continue

            if keywords:
                return ", ".join(keywords[:20])  # Limit to 20 keywords

        except Exception as exc:
            Logger.error("Error extracting keywords from page: %s", exc)

        return None

    def _extract_geographic_coverage_from_page(self) -> Optional[str]:
        """Extract geographic coverage from the page."""
        if not self._page:
            return None

        try:
            geo_selectors = [
                "[class*='geographic']",
                "[class*='spatial']",
                "[class*='location']",
                "dt:has-text('Geographic') + dd",
                "dt:has-text('Location') + dd",
                "dt:has-text('Spatial') + dd",
            ]

            for selector in geo_selectors:
                try:
                    el = self._page.query_selector(selector)
                    if el:
                        text = el.inner_text().strip()
                        if text and len(text) > 1:
                            return text
                except Exception:
                    continue

            # Check page text for "United States" or "National" mentions
            try:
                page_text = self._page.inner_text("body")
                if "United States" in page_text or "nationwide" in page_text.lower() or "national" in page_text.lower():
                    return "United States"
            except Exception:
                pass

        except Exception as exc:
            Logger.error("Error extracting geographic coverage: %s", exc)

        return None

    def _extract_page_title(self) -> Optional[str]:
        """Extract the dataset title from the rendered page."""
        if not self._page:
            return None

        # Try various title selectors
        for selector in [
            "h1[class*='DatasetPage']",
            "h1[class*='dataset']",
            "h1[class*='page-title']",
            ".dataset-title h1",
            "[class*='DatasetPage__title']",
            "[class*='page-header'] h1",
            "[class*='dataset-header'] h1",
            "[class*='page-title']",
            "[class*='dataset-title']",
            "h1",
        ]:
            try:
                el = self._page.query_selector(selector)
                if el:
                    text = el.inner_text().strip()
                    if text and len(text) > 3:
                        return text
            except Exception:
                continue

        # Fall back to page title
        try:
            title = self._page.title()
            if title:
                # Clean up common suffixes like " | CMS" or " - data.cms.gov"
                for suffix in [" | CMS", " | data.cms.gov", " - CMS", " - data.cms.gov", " | Centers for Medicare"]:
                    if suffix in title:
                        title = title[:title.index(suffix)]
                title = title.strip()
                if title:
                    return title
        except Exception:
            pass

        return None

    def _extract_page_files(self) -> List[Dict[str, Any]]:
        """Extract downloadable file links from the rendered page."""
        if not self._page:
            return []

        files = []
        seen_hrefs = set()

        try:
            # Look for download buttons/links
            download_selectors = [
                "a[href*='.zip']",
                "a[href*='.csv']",
                "a[href*='.xlsx']",
                "a[href*='.json']",
                "a[href*='download']",
                "a[href*='datalumos']",
                "[class*='download'] a",
                "[class*='Download'] a",
                "a[class*='download']",
                "a[class*='Download']",
                "button[class*='download']",
                "a[href*='/data-api/']",
                "a[href*='data.cms.gov/api/']",
            ]

            for selector in download_selectors:
                try:
                    elements = self._page.query_selector_all(selector)
                    for el in elements:
                        href = el.get_attribute("href") or ""
                        text = el.inner_text().strip()

                        if href and href not in seen_hrefs:
                            seen_hrefs.add(href)
                            file_name = self._extract_filename_from_href(href) or text or "dataset"
                            files.append({
                                "file_url": href,
                                "file_name": file_name,
                                "name": file_name,
                                "href": href,
                                "type": "Primary",
                            })
                except Exception:
                    continue

            # Also look for data in JSON-LD or script tags
            try:
                scripts = self._page.query_selector_all("script[type='application/ld+json']")
                for script in scripts:
                    try:
                        content = script.inner_text()
                        data = json.loads(content)
                        dist = data.get("distribution") or []
                        if isinstance(dist, list):
                            for d in dist:
                                dl_url = d.get("contentUrl") or d.get("url") or ""
                                if dl_url and dl_url not in seen_hrefs:
                                    seen_hrefs.add(dl_url)
                                    name = d.get("name") or self._extract_filename_from_href(dl_url) or "dataset"
                                    files.append({
                                        "file_url": dl_url,
                                        "file_name": name,
                                        "name": name,
                                        "href": dl_url,
                                        "type": "Primary",
                                    })
                    except Exception:
                        continue
            except Exception:
                pass

            # Look for data in Next.js __NEXT_DATA__ or similar state
            try:
                next_data_el = self._page.query_selector("#__NEXT_DATA__")
                if next_data_el:
                    content = next_data_el.inner_text()
                    next_data = json.loads(content)
                    # Navigate into props.pageProps to find dataset resources
                    page_props = (next_data.get("props") or {}).get("pageProps") or {}
                    self._extract_files_from_next_data(page_props, files, seen_hrefs)
            except Exception:
                pass

        except Exception as exc:
            Logger.error("Error extracting page files: %s", exc)

        return files

    def _extract_files_from_next_data(
        self,
        data: Any,
        files: List[Dict[str, Any]],
        seen_hrefs: set,
    ) -> None:
        """Recursively extract file URLs from Next.js __NEXT_DATA__."""
        if isinstance(data, dict):
            # Check for file_url or similar keys
            file_url = data.get("file_url") or data.get("fileUrl") or data.get("download_url") or ""
            if file_url and file_url not in seen_hrefs:
                seen_hrefs.add(file_url)
                name = (data.get("file_name") or data.get("fileName") or
                        data.get("name") or self._extract_filename_from_href(file_url) or "dataset")
                files.append({
                    "file_url": file_url,
                    "file_name": name,
                    "name": name,
                    "href": file_url,
                    "type": data.get("type") or data.get("resource_type") or "Primary",
                })
            # Recurse into values
            for v in data.values():
                self._extract_files_from_next_data(v, files, seen_hrefs)
        elif isinstance(data, list):
            for item in data:
                self._extract_files_from_next_data(item, files, seen_hrefs)

    def _extract_filename_from_href(self, href: str) -> Optional[str]:
        """Extract a filename from a URL href."""
        if not href:
            return None

        # Check for 'path=' parameter which often contains filename
        if "path=" in href:
            try:
                path_match = re.search(r'path=([^&]+)', href)
                if path_match:
                    path_val = unquote(path_match.group(1))
                    filename = path_val.split("/")[-1]
                    if "." in filename:
                        return filename
            except Exception:
                pass

        # Try to get filename from URL path
        try:
            parsed = urlparse(href)
            path = parsed.path
            filename = path.split("/")[-1]
            if filename and "." in filename:
                return unquote(filename)
        except Exception:
            pass

        return None

    def _extract_page_metadata(self) -> Dict[str, Any]:
        """Extract additional metadata fields from the rendered page."""
        if not self._page:
            return {}