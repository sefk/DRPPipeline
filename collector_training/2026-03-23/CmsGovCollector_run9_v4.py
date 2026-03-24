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

_CMS_AGENCY = "Centers for Medicare and Medicaid Services, United States Department of Health and Human Services"
_CMS_DATA_TYPES = "administrative records data"
_FIXED_DOWNLOAD_DATE = "2026-01-10"
_DATALUMOS_BASE = "https://datalumos.org/datalumos"


class CmsGovCollector:
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

    def _update_storage(self, drpid: int, result: Dict[str, Any]) -> None:
        if not result:
            return
        fields = {k: v for k, v in result.items() if v is not None}
        if fields:
            Storage.update_record(drpid, fields)

    def _cleanup_browser(self) -> None:
        with suppress(Exception):
            if self._page:
                self._page.close()
        with suppress(Exception):
            if self._browser:
                self._browser.close()
        with suppress(Exception):
            if self._playwright:
                self._playwright.stop()
        self._page = None
        self._browser = None
        self._playwright = None

    def _init_browser(self) -> bool:
        try:
            if self._page and not self._page.is_closed():
                return True
            if self._playwright is None:
                self._playwright = sync_playwright().start()
            if self._browser is None:
                self._browser = self._playwright.chromium.launch(headless=self._headless)
            self._page = self._browser.new_page()
            self._page.set_extra_http_headers(BROWSER_HEADERS)
            return True
        except Exception as exc:
            Logger.error("Failed to init browser: %s", exc)
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

        # Try standard slug API first
        slug_data = self._fetch_slug(url_path)

        # If slug fails, try alternative approaches
        if not slug_data:
            slug_data = self._fetch_slug_with_fallback(url, url_path, drpid)

        if slug_data:
            result.update(self._parse_slug_metadata(slug_data))
        else:
            Logger.warning("No slug data found for %s, will rely on page scraping", url)
            result["agency"] = _CMS_AGENCY

        # Always scrape the page for description, title, files and metadata
        description, page_title, page_files, page_metadata = self._scrape_page_full(url, drpid)

        if description:
            description = description.replace('\xa0', ' ')
            result["summary"] = description

        if not result.get("title") and page_title:
            result["title"] = page_title

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

        if not all_files and page_files:
            all_files = page_files
            Logger.info("Using %d file(s) scraped from page", len(all_files))

        if not all_files:
            inferred = self._infer_files_from_url(url, url_path)
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

        if not training_mode and current_uuid:
            self._download_dataset_metadata(drpid, current_uuid, folder_path)

        if slug_data:
            date_range = self._extract_date_range_from_metadata(slug_data, all_files, current_uuid)
            if date_range.get("time_start") and not result.get("time_start"):
                result["time_start"] = date_range["time_start"]
            if date_range.get("time_end") and not result.get("time_end"):
                result["time_end"] = date_range["time_end"]

        exts, total_bytes = folder_extensions_and_size(folder_path)
        if exts:
            result["extensions"] = ",".join(exts)

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
        seen_uuids: set = set()

        # Fetch current dataset resources
        current_resources = self._fetch_resources(
            f"{_API_BASE}/dataset/{current_uuid}/resources"
        )
        for r in current_resources:
            uid = r.get("uuid") or r.get("id")
            if uid and uid in seen_uuids:
                continue
            if uid:
                seen_uuids.add(uid)
            all_files.append(r)

        # Fetch all historical primary files via taxonomy
        if taxonomy_uuid:
            historical = self._fetch_resources(
                f"{_API_BASE}/dataset-type/{taxonomy_uuid}/resources"
            )
            for r in historical:
                uid = r.get("uuid") or r.get("id")
                if uid and uid in seen_uuids:
                    continue
                if uid:
                    seen_uuids.add(uid)
                all_files.append(r)

        return all_files

    def _download_files(
        self, drpid: int, files: List[Dict[str, Any]], folder_path: Path
    ) -> None:
        """Download all files to folder_path."""
        for file_info in files:
            file_url = file_info.get("file_url") or file_info.get("href") or ""
            file_name = (
                file_info.get("file_name")
                or file_info.get("name")
                or self._extract_filename_from_href(file_url)
                or "dataset"
            )
            file_name = sanitize_filename(file_name)
            if not file_url:
                Logger.warning("Skipping file with no URL: %s", file_info)
                continue
            dest = folder_path / file_name
            if dest.exists():
                Logger.info("File already exists, skipping: %s", dest)
                continue
            try:
                Logger.info("Downloading %s -> %s", file_url, dest)
                download_via_url(file_url, dest, timeout_ms=Args.download_timeout_ms)
            except Exception as exc:
                record_warning(drpid, f"Failed to download {file_url}: {exc}")

    def _extract_date_range_from_metadata(
        self,
        slug_data: Dict[str, Any],
        all_files: List[Dict[str, Any]],
        current_uuid: Optional[str],
    ) -> Dict[str, Any]:
        """Extract time_start and time_end from slug/dataset metadata."""
        result: Dict[str, Any] = {}

        # Try temporal coverage in slug_data
        temporal = slug_data.get("temporal_coverage") or slug_data.get("temporalCoverage") or ""
        if temporal:
            parts = temporal.split("/")
            if len(parts) >= 2:
                result["time_start"] = parts[0].strip()[:4]
                result["time_end"] = parts[1].strip()[:4]
                return result

        # Try date fields in slug_data
        for key in ["start_date", "startDate", "period_start"]:
            val = slug_data.get(key)
            if val:
                result["time_start"] = str(val)[:4]
                break

        for key in ["end_date", "endDate", "period_end"]:
            val = slug_data.get(key)
            if val:
                result["time_end"] = str(val)[:4]
                break

        # Try current_dataset sub-object
        current = slug_data.get("current_dataset") or {}
        for key in ["start_date", "startDate", "period_start", "temporal_start"]:
            val = current.get(key)
            if val and not result.get("time_start"):
                result["time_start"] = str(val)[:4]
                break
        for key in ["end_date", "endDate", "period_end", "temporal_end"]:
            val = current.get(key)
            if val and not result.get("time_end"):
                result["time_end"] = str(val)[:4]
                break

        # Try to infer from file names (year patterns)
        if not result.get("time_start") and all_files:
            years = []
            for f in all_files:
                name = f.get("file_name") or f.get("name") or ""
                matches = re.findall(r'\b(20\d{2})\b', name)
                years.extend([int(m) for m in matches])
            if years:
                result["time_start"] = str(min(years))
                result["time_end"] = str(max(years))

        return result

    def _infer_files_from_url(self, url: str, url_path: str) -> List[Dict[str, Any]]:
        """Try to infer downloadable file URLs from the page URL pattern."""
        files = []

        parts = url_path.strip("/").split("/")
        if not parts:
            return files

        dataset_slug = parts[-1]
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

            # Wait a bit for any lazy-loaded content
            try:
                self._page.wait_for_timeout(2000)
            except Exception:
                pass

            # Extract description
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
                ]:
                    el = self._page.query_selector(selector)
                    if el:
                        text = el.inner_text().strip()
                        if text and len(text) > 50:
                            description = text
                            break

            if not description:
                for selector in ["main p", ".content-area p", "article p",
                                  "[class*='description'] p", "[class*='summary'] p"]:
                    elements = self._page.query_selector_all(selector)
                    if elements:
                        texts = [e.inner_text().strip() for e in elements if e.inner_text().strip()]
                        if texts:
                            longest = max(texts, key=len)
                            if len(longest) > 100:
                                description = longest
                                break

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

        try:
            title = self._page.title()
            if title:
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
            download_selectors = [
                "a[href*='.zip']",
                "a[href*='.csv']",
                "a[href*='.xlsx']",
                "a[href*='.json']",
                "a[href*='download']",
                "a[href*='datalumos']",
                "a[href*='path=']",
                "[class*='download'] a",
                "[class*='Download'] a",
                "a[class*='download']",
                "a[class*='Download']",
                "button[class*='download']",
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

            # Try to find files via page network requests / data attributes
            try:
                # Look for any elements with data-download or data-href attributes
                for attr_selector in ["[data-download-url]", "[data-href*='.zip']", "[data-href*='.csv']"]:
                    elements = self._page.query_selector_all(attr_selector)
                    for el in elements:
                        href = el.get_attribute("data-download-url") or el.get_attribute("data-href") or ""
                        if href and href not in seen_hrefs:
                            seen_hrefs.add(href)
                            file_name = self._extract_filename_from_href(href) or "dataset"
                            files.append({
                                "file_url": href,
                                "file_name": file_name,
                                "name": file_name,
                                "href": href,
                                "type": "Primary",
                            })
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
                pass

            # Try to extract metadata from page text via labeled fields
            try:
                # Look for CMS-specific metadata containers
                meta_containers = self._page.query_selector_all(
                    "[class*='metadata'], [class*='Metadata'], [class*='DatasetPage__details']"
                )
                for container in meta_containers:
                    try:
                        text = container.inner_text()
                        # Parse keywords
                        kw_match = re.search(r'[Kk]eyword[s]?[:\s]+([^\n]+)', text)
                        if kw_match and not metadata.get("keywords"):
                            metadata["keywords"] = kw_match.group(1).strip()

                        # Parse date range
                        date_match = re.search(r'[Dd]ate\s+[Rr]ange[:\s]+(\d{4})[^\d]*(\d{4})?', text)
                        if date_match:
                            if not metadata.get("time_start"):
                                metadata["time_start"] = date_match.group(1)
                            if date_match.group(2) and not metadata.get("time_end"):
                                metadata["time_end"] = date_match.group(2)

                        # Geographic coverage
                        geo_match = re.search(r'[Gg]eograph\w*[:\s]+([^\n]+)', text)
                        if geo_match and not metadata.get("geographic_coverage"):
                            geo_val = geo_match.group(1).strip()
                            if geo_val:
                                metadata["geographic_coverage"] = geo_val

                    except Exception:
                        continue
            except Exception:
                pass

        except Exception as exc:
            Logger.error("Error extracting page metadata: %s", exc)

        return metadata

    def _fetch_slug_with_fallback(self, url: str, url_path: str, drpid: int) -> Optional[Dict[str, Any]]:
        """Try alternative API approaches when the standard slug fetch fails."""
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
        primary_files = [f for f in all_files if f.get("type") == "Primary"]
        if len(primary_files) > 20:
            return None
        return f"(Downloaded {_FIXED_DOWNLOAD_DATE})"

    def _extract_path(self, url: str) -> Optional[str]:
        parsed = urlparse(url)
        return parsed.path if parsed.path and parsed.path != "/" else None

    def _fetch_slug(self, url_path: str) -> Optional[Dict[str, Any]]:
        api_url = f"{_API_BASE}/slug?path={quote(url_path)}"
        Logger.info("Fetching CMS slug: %s", api_url)
        try:
            resp = requests.get(api_url, headers=BROWSER_HEADERS, timeout=30)
            resp.raise