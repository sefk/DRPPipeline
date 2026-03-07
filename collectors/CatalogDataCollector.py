"""
Catalog Data Collector for DRP Pipeline.

Collects data from catalog.data.gov dataset pages:
- Validates and accesses source_url (source page)
- Locates "Downloads & Resources" section
- Follows each download link, records file type and title for non-404s
- Writes results to status_notes (no PDF, dataset download, or metadata)
"""

from contextlib import suppress
from typing import Optional, Dict, Any, List, Tuple

from playwright.sync_api import sync_playwright, Page, Browser, Playwright

from storage import Storage
from utils.Logger import Logger
from utils.Errors import record_error
from utils.url_utils import is_valid_url, access_url, fetch_url_head, fetch_page_body, infer_file_type


class CatalogDataCollector:
    """
    Collector for catalog.data.gov dataset pages.

    Extracts download resource links from the "Downloads & Resources" section,
    follows each link, and records file type and title for non-404 responses.
    """

    _DOWNLOADS_SECTION_HEADING = "Downloads & Resources"

    def __init__(self, headless: bool = True) -> None:
        """
        Initialize CatalogDataCollector.

        Args:
            headless: If False, run browser in visible mode for debugging
        """
        self._headless = headless
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None
        self._result: Optional[Dict[str, Any]] = None

    def run(self, drpid: int) -> None:
        """
        Run the collector for a single project (ModuleProtocol interface).

        Gets project record from Storage, calls _collect() with source_url,
        and updates Storage with status_notes.

        Args:
            drpid: The DRPID of the project to process.
        """
        self._drpid = drpid
        record = Storage.get(drpid)
        if record is None:
            record_error(
                drpid,
                f"Project record not found for DRPID: {drpid}",
                update_storage=False,
            )
            return

        source_url = record.get("source_url")
        if not source_url:
            record_error(
                drpid,
                f"Project record missing source_url for DRPID: {drpid}",
            )
            return

        try:
            result = self._collect(source_url, drpid)
            self._update_storage_from_result(drpid, result)
        except Exception as exc:
            record_error(
                drpid,
                f"Exception during collection for DRPID {drpid}: {str(exc)}",
            )

    def _collect(self, url: str, drpid: int) -> Dict[str, Any]:
        """
        Collect download resource info from a catalog.data.gov source page.

        Loads the source page, finds "Downloads & Resources", extracts links
        from the sibling <ul>, follows each link, and records file type + title
        for non-404 responses.

        Args:
            url: Source URL (catalog.data.gov dataset page)
            drpid: DRPID for the record

        Returns:
            Dict with status_notes (and optionally status) for Storage update.
        """
        self._result = {}

        if not self._validate_and_access_url(url):
            return self._result

        try:
            if not self._init_browser_and_load_page(url):
                return self._result

            links = self._extract_download_links()
            if links is None:
                return self._result

            if not links:
                record_error(
                    drpid,
                    "Downloads & Resources section has no links",
                )
                return self._result

            resources = self._follow_links_and_collect_resources(links)
            if resources is None:
                return self._result

            status_notes = self._format_status_notes(resources)
            self._result["status_notes"] = status_notes
            Logger.info(f"Downloads & Resources:{status_notes}")
        finally:
            self._cleanup_browser()

        return self._result

    def _validate_and_access_url(self, url: str) -> bool:
        """
        Validate URL and check accessibility.

        Args:
            url: URL to validate and access

        Returns:
            True if valid and accessible, False otherwise
        """
        if not is_valid_url(url):
            record_error(self._drpid, f"Invalid URL: {url}")
            return False

        access_success, status_msg = access_url(url)
        if not access_success:
            record_error(self._drpid, f"URL access failed: {url} - {status_msg}")
            return False

        Logger.debug(f"Successfully accessed URL: {url}")
        return True

    def _init_browser_and_load_page(self, url: str) -> bool:
        """
        Initialize Playwright browser and load the source page.

        Args:
            url: URL to load

        Returns:
            True if successful, False otherwise
        """
        if not self._init_browser():
            record_error(self._drpid, "Failed to initialize browser")
            return False

        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=120000)
            self._page.wait_for_timeout(500)
            return True
        except Exception as exc:
            record_error(self._drpid, f"Failed to load page: {str(exc)}")
            return False

    def _extract_download_links(self) -> Optional[List[Tuple[str, str]]]:
        """
        Find "Downloads & Resources" h3, its sibling ul, and extract (href, text) from li>a.

        Returns:
            List of (href, link_text) tuples, or None if section not found.
        """
        script = """
        () => {
            function getDirectText(el) {
                let t = '';
                for (let i = 0; i < el.childNodes.length; i++) {
                    if (el.childNodes[i].nodeType === 3)
                        t += el.childNodes[i].textContent;
                }
                return t.trim().replace(/\\s+/g, ' ');
            }
            const h3s = document.querySelectorAll('h3');
            const h3 = Array.from(h3s).find(h =>
                h.textContent && h.textContent.trim() === 'Downloads & Resources'
            );
            if (!h3) return null;
            const ul = h3.nextElementSibling;
            if (!ul || ul.tagName !== 'UL') return null;
            const links = [];
            ul.querySelectorAll('li a').forEach(a => {
                if (a.href) {
                    links.push({
                        href: a.href,
                        text: getDirectText(a)
                    });
                }
            });
            return links;
        }
        """
        result = self._page.evaluate(script)
        if result is None:
            record_error(
                self._drpid,
                f"Source page missing '<h3>Downloads & Resources</h3>' or sibling <ul>",
            )
            return None
        raw_links = [(item["href"], item["text"]) for item in result]
        return self._dedupe_links(raw_links)

    def _dedupe_links(
        self, links: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """
        Remove duplicate links by href (first occurrence wins).

        Args:
            links: List of (href, text) tuples

        Returns:
            Deduplicated list preserving order
        """
        seen: set[str] = set()
        deduped: List[Tuple[str, str]] = []
        for href, text in links:
            if href not in seen:
                seen.add(href)
                deduped.append((href, text))
        return deduped

    def _resolve_catalog_resource_page(
        self, catalog_url: str
    ) -> Optional[Tuple[str, Optional[str]]]:
        """
        Turn a catalog.data.gov resource page URL into the real download URL.

        For links that point at catalog resource pages (HTML with metadata), this
        loads the page and reads the <a id="res_url"> element, which holds the
        actual file URL (S3, data.gov redirect, etc.). Returns that URL and
        data-format so the collector can call fetch_url_head on the real file
        and record the correct format. Skips loading (returns None) if the catalog
        page is HTTP 404 or logical 404.

        Args:
            catalog_url: URL of catalog.data.gov resource page

        Returns:
            (actual_download_url, data_format) or None if #res_url not found.
        """
        status_code, _body, _content_type, is_logical_404 = fetch_page_body(catalog_url)
        if status_code == 404 or is_logical_404:
            return None
        try:
            self._page.goto(catalog_url, wait_until="domcontentloaded", timeout=30000)
            self._page.wait_for_timeout(300)
        except Exception:
            return None

        script = """
        () => {
            const a = document.getElementById('res_url');
            if (!a || !a.href) return null;
            return {
                href: a.href,
                dataFormat: a.getAttribute('data-format') || null
            };
        }
        """
        result = self._page.evaluate(script)
        if result is None:
            return None
        data_format = result.get("dataFormat")
        if data_format:
            data_format = str(data_format).lower().strip()
        return (result["href"], data_format)

    def _follow_links_and_collect_resources(
        self, links: List[Tuple[str, str]]
    ) -> Optional[List[Tuple[str, str]]]:
        """
        Follow each link with HEAD request; record (title, result) for all links.

        For hrefs starting with https://catalog.data.gov, loads the resource page
        and follows the #res_url link instead.

        Args:
            links: List of (href, link_text) from _extract_download_links

        Returns:
            List of (title, result, url) for all links, or None if all 404.
        """
        entries: List[Tuple[str, str, str]] = []
        has_success = False
        hrefs = [h for h, _ in links]
        for href, title in links:
            title_clean = title.strip() or "(no title)"
            actual_url = href
            data_format: Optional[str] = None

            if href.startswith("https://catalog.data.gov"):
                resolved = self._resolve_catalog_resource_page(href)
                if resolved is None:
                    entries.append((title_clean, "404", ""))
                    continue
                actual_url, data_format = resolved
                # Skip if resolved URL is a duplicate of another link's href
                if actual_url in hrefs:
                    continue

            status_code, content_type, error_msg = fetch_url_head(actual_url)
            if status_code == 404 or status_code < 0:
                entries.append((title_clean, "404", ""))
            else:
                file_type = (
                    data_format
                    if data_format
                    else infer_file_type(actual_url, content_type)
                )
                entries.append((title_clean, file_type, actual_url))
                has_success = True

        if not has_success:
            record_error(
                self._drpid,
                "All download links returned 404",
            )
            return None
        return entries

    def _format_status_notes(
        self, entries: List[Tuple[str, str, str]]
    ) -> str:
        """Format resource list for status_notes (title -> result, with URL for success)."""
        lines = []
        for title, result, url in entries:
            line = f"  {title} -> {result}"
            if url:
                line += f" {url}"
            lines.append(line)
        return "\n" + "\n".join(lines)

    def _init_browser(self) -> bool:
        """Initialize Playwright browser and page."""
        try:
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(
                headless=self._headless,
                slow_mo=500 if not self._headless else 0,
            )
            self._page = self._browser.new_page()
            return True
        except Exception as exc:
            Logger.error(f"Failed to initialize browser: {exc}")
            self._cleanup_browser()
            return False

    def _cleanup_browser(self) -> None:
        """Clean up browser resources."""
        if self._browser:
            with suppress(Exception):
                self._browser.close()
            self._browser = None
        if self._playwright:
            with suppress(Exception):
                self._playwright.stop()
            self._playwright = None
        self._page = None

    def _update_storage_from_result(
        self, drpid: int, result: Dict[str, Any]
    ) -> None:
        """
        Transfer result dict to Storage (status_notes and optional status).

        Does not set status to "collected" since we do not produce folder_path.
        """
        update_fields = {k: v for k, v in result.items() if v is not None}
        if update_fields:
            Storage.update_record(drpid, update_fields)
