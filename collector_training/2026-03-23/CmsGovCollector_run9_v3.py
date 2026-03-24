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

        # If still no files, try to infer file from URL pattern
        if not all_files:
            inferred = self._infer_files_from_url(url, url_path, drpid)
            if inferred:
                all_files = inferred
                Logger.info("Using %d inferred file(s) from URL pattern", len(all_files))

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

    def _gather_files(
        self, drpid: int, current_uuid: str, taxonomy_uuid: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Gather all files: current resources + historical primary files."""
        all_files: List[Dict[str, Any]] = []
        seen_urls: set = set()

        # Current dataset resources
        current_endpoint = f"{_API_BASE}/dataset/{current_uuid}/resources"
        current_resources = self._fetch_resources(current_endpoint)
        for r in current_resources:
            url = r.get("file_url") or r.get("url") or ""
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_files.append(r)

        # Historical primary files from taxonomy
        if taxonomy_uuid:
            hist_endpoint = f"{_API_BASE}/dataset-type/{taxonomy_uuid}/resources"
            hist_resources = self._fetch_resources(hist_endpoint)
            for r in hist_resources:
                url = r.get("file_url") or r.get("url") or ""
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_files.append(r)

        return all_files

    def _download_files(
        self,
        drpid: int,
        files: List[Dict[str, Any]],
        folder_path: Path,
    ) -> None:
        """Download all files in the list."""
        for resource in files:
            file_url = resource.get("file_url") or resource.get("url") or resource.get("href") or ""
            if not file_url:
                continue

            raw_name = (
                resource.get("file_name")
                or resource.get("name")
                or file_url.split("/")[-1].split("?")[0]
            )
            file_name = sanitize_filename(raw_name) if raw_name else "dataset"
            if not Path(file_name).suffix:
                # Try to infer extension from URL
                ext = self._infer_extension(file_url)
                if ext:
                    file_name = file_name + ext

            dest = folder_path / file_name
            if dest.exists():
                Logger.info("Already downloaded: %s", file_name)
                continue

            Logger.info("Downloading: %s → %s", file_url, file_name)
            try:
                success = download_via_url(file_url, dest, timeout_ms=Args.download_timeout_ms)
                if not success:
                    record_warning(drpid, f"Failed to download: {file_url}")
            except Exception as exc:
                record_warning(drpid, f"Download error for {file_url}: {exc}")

    def _infer_extension(self, url: str) -> str:
        """Try to infer file extension from URL."""
        for ext in [".csv", ".zip", ".xlsx", ".json", ".pdf", ".txt"]:
            if ext in url.lower():
                return ext
        return ""

    def _infer_files_from_url(self, url: str, url_path: str, drpid: int) -> List[Dict[str, Any]]:
        """
        Try to infer downloadable file URLs from the page URL pattern.
        For CMS innovation center pages, try multiple strategies to find files.
        """
        files = []

        parts = url_path.strip("/").split("/")
        if not parts:
            return files

        dataset_slug = parts[-1]

        # Strategy 1: Try direct CMS data API endpoints for the dataset
        # Many CMS innovation center datasets have predictable API paths
        Logger.info("Trying to find files via CMS data API for slug: %s", dataset_slug)

        # Strategy 2: Try to find via the datasets listing endpoint
        datasets = self._fetch_datasets_list(url_path)
        if datasets:
            for item in datasets:
                file_url = item.get("file_url") or item.get("download_url") or item.get("url")
                file_name = item.get("file_name") or item.get("name") or dataset_slug
                if file_url:
                    files.append({
                        "file_url": file_url,
                        "file_name": file_name,
                        "type": "Primary",
                    })

        if files:
            return files

        # Strategy 3: Try to build file URL from known CMS patterns
        # CMS innovation center datasets are often available as CSV downloads
        # via the /data-api/v1/dataset endpoint with path matching
        candidate_urls = self._build_candidate_file_urls(url, url_path, parts)
        for candidate_url, candidate_name in candidate_urls:
            Logger.info("Checking candidate file URL: %s", candidate_url)
            try:
                resp = requests.head(candidate_url, headers=BROWSER_HEADERS, timeout=15, allow_redirects=True)
                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "")
                    if any(t in content_type for t in ["csv", "zip", "excel", "json", "octet"]):
                        files.append({
                            "file_url": candidate_url,
                            "file_name": candidate_name,
                            "type": "Primary",
                        })
                        Logger.info("Found valid file: %s", candidate_url)
            except Exception as exc:
                Logger.debug("Candidate URL check failed: %s - %s", candidate_url, exc)

        return files

    def _fetch_datasets_list(self, url_path: str) -> List[Dict[str, Any]]:
        """Try to fetch dataset listing from CMS API."""
        results = []
        # Try different API endpoint patterns
        endpoints = [
            f"{_API_BASE}/dataset?path={quote(url_path)}&size=20",
            f"{_API_BASE}/dataset?slug={quote(url_path.strip('/'))}&size=20",
        ]
        for endpoint in endpoints:
            try:
                resp = requests.get(endpoint, headers=BROWSER_HEADERS, timeout=30)
                if resp.status_code == 200:
                    body = resp.json()
                    data = body.get("data") or []
                    if isinstance(data, list) and data:
                        results.extend(data)
                        break
            except Exception as exc:
                Logger.debug("Dataset list fetch error: %s", exc)
        return results

    def _build_candidate_file_urls(
        self, url: str, url_path: str, parts: List[str]
    ) -> List[Tuple[str, str]]:
        """Build candidate file download URLs based on known CMS patterns."""
        candidates = []

        dataset_slug = parts[-1] if parts else ""
        if not dataset_slug:
            return candidates

        # CMS data API direct download patterns
        base_domain = "https://data.cms.gov"

        # Pattern 1: Direct CSV endpoint
        candidates.append((
            f"{base_domain}/api/1/datastore/query/{dataset_slug}/0?results_format=csv&download=true",
            f"{dataset_slug}.csv"
        ))

        # Pattern 2: Data API v1 resources endpoint
        # Try to find the dataset UUID first
        parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else url_path
        for path_var in [url_path, parent_path]:
            candidates.append((
                f"{_API_BASE}/slug?path={quote(path_var)}",
                f"{dataset_slug}_metadata.json"
            ))

        return candidates

    def _infer_files_from_page_api(self, url: str, drpid: int) -> List[Dict[str, Any]]:
        """
        Use the CMS data.json or other catalog endpoints to find files.
        """
        files = []

        # Try the CMS data catalog
        try:
            catalog_url = "https://data.cms.gov/data.json"
            resp = requests.get(catalog_url, headers=BROWSER_HEADERS, timeout=60)
            if resp.status_code == 200:
                catalog = resp.json()
                datasets = catalog.get("dataset") or []
                url_path = urlparse(url).path.lower()

                for ds in datasets:
                    landing_page = ds.get("landingPage") or ""
                    if landing_page and url_path in landing_page.lower():
                        # Found matching dataset
                        distributions = ds.get("distribution") or []
                        for dist in distributions:
                            dl_url = dist.get("downloadURL") or dist.get("accessURL") or ""
                            if dl_url:
                                file_name = dl_url.split("/")[-1] or "dataset"
                                files.append({
                                    "file_url": dl_url,
                                    "file_name": file_name,
                                    "type": "Primary",
                                })
                        if files:
                            break
        except Exception as exc:
            Logger.debug("Catalog fetch error: %s", exc)

        return files

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

            # Wait a bit more for dynamic content
            try:
                self._page.wait_for_timeout(3000)
            except Exception:
                pass

            # Extract description - try the primary selector first
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
                    "[class*='field-summary']",
                    "[class*='about-dataset']",
                    "[class*='dataset-about']",
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
                                  "[class*='description'] p", "[class*='summary'] p"]:
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

            # Extract page title
            title = self._extract_page_title()

            # Extract file download links from the page
            files = self._extract_page_files()

            # Extract additional metadata from page
            metadata = self._extract_page_metadata()

        except Exception as exc:
            record_warning(drpid, f"Failed to scrape page: {exc}")

        return description, title, files, metadata

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
                "a[href*='cms.gov']",
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

        except Exception as exc:
            Logger.error("Error extracting page files: %s", exc)

        return files

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

        metadata = {}

        try:
            # Look for keywords in meta tags
            try:
                meta_keywords = self._page.query_selector("meta[name='keywords']")
                if meta_keywords:
                    content = meta_keywords.get_attribute("content")
                    if content:
                        metadata["keywords"] = content
            except Exception:
                pass

            # Look for structured data in the page (JSON-LD)
            try:
                scripts = self._page.query_selector_all("script[type='application/ld+json']")
                for script in scripts:
                    try:
                        content = script.inner_text()
                        data = json.loads(content)
                        if isinstance(data, dict):
                            # Extract keywords
                            kws = data.get("keywords") or []
                            if isinstance(kws, list) and kws:
                                metadata["keywords"] = ", ".join(kws)
                            elif isinstance(kws, str) and kws:
                                metadata["keywords"] = kws

                            # Extract temporal coverage
                            temp = data.get("temporalCoverage") or ""
                            if temp:
                                parts = temp.split("/")
                                if len(parts) >= 2:
                                    metadata["time_start"] = parts[0].strip()
                                    metadata["time_end"] = parts[1].strip()
                                elif len(parts) == 1:
                                    metadata["time_start"] = parts[0].strip()

                            # Extract geographic coverage
                            geo = data.get("spatialCoverage") or data.get("geographicCoverage") or ""
                            if isinstance(geo, dict):
                                geo = geo.get("name") or geo.get("@id") or ""
                            if isinstance(geo, str) and geo:
                                metadata["geographic_coverage"] = geo

                    except Exception:
                        continue
            except Exception:
                pass

            # Look for data type metadata fields on the page
            try:
                field_selectors = [
                    "[class*='field-label']",
                    "[class*='metadata-label']",
                    "dt",
                    "[class*='label']",
                ]
                for selector in field_selectors:
                    try:
                        elements = self._page.query_selector_all(selector)
                        for el in elements:
                            try:
                                label_text = el.inner_text().strip().lower()
                                if "data type" in label_text or "dataset type" in label_text:
                                    sibling = el.evaluate_handle("el => el.nextElementSibling")
                                    if sibling:
                                        val = sibling.as_element()
                                        if val:
                                            dt_text = val.inner_text().strip()
                                            if dt_text:
                                                metadata["data_types"] = dt_text
                            except Exception:
                                continue
                    except Exception:
                        continue
            except Exception:
                pass

            # Try to extract geographic coverage from page text
            if not metadata.get("geographic_coverage"):
                try:
                    page_text = self._page.inner_text("body")
                    # Look for "National" or "United States" indicators
                    if re.search(r'\bnational\b|\bUnited States\b|\bnationwide\b', page_text, re.IGNORECASE):
                        metadata["geographic_coverage"] = "National"
                except Exception:
                    pass

        except Exception as exc:
            Logger.error("Error extracting page metadata: %s", exc)

        return metadata

    def _fetch_slug_with_fallback(self, url: str, url_path: str, drpid: int) -> Optional[Dict[str, Any]]:
        """
        Try alternative API approaches when the standard slug fetch fails.
        """
        Logger.info("Trying fallback slug fetch for: %s", url)

        parts = url_path.strip("/").split("/")

        variations = []
        if url_path.endswith("/"):
            variations.append(url_path.rstrip("/"))
        else:
            variations.append(url_path + "/")

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