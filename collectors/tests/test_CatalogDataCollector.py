"""
Unit tests for CatalogDataCollector.
"""

import sys
import unittest
from unittest.mock import Mock, patch

from storage import Storage
from utils.Args import Args
from utils.Logger import Logger

from collectors.CatalogDataCollector import CatalogDataCollector
from collectors.tests.test_utils import setup_mock_playwright


class TestCatalogDataCollector(unittest.TestCase):
    """Test cases for CatalogDataCollector class."""

    def setUp(self) -> None:
        """Set up test environment before each test."""
        self._original_argv = sys.argv.copy()
        sys.argv = ["test", "noop"]

        Args.initialize()
        Logger.initialize(log_level="WARNING")

        self.collector = CatalogDataCollector(headless=True)
        self.collector._drpid = 1

    def tearDown(self) -> None:
        """Clean up after each test."""
        sys.argv = self._original_argv
        self.collector._cleanup_browser()

    def test_init(self) -> None:
        """Test CatalogDataCollector initialization."""
        collector = CatalogDataCollector(headless=True)
        self.assertTrue(collector._headless)
        self.assertIsNone(collector._playwright)
        self.assertIsNone(collector._browser)
        self.assertIsNone(collector._page)

    @patch("collectors.CatalogDataCollector.record_error")
    def test_collect_invalid_url(self, mock_record_error: Mock) -> None:
        """Test _collect with invalid URL calls record_error."""
        result = self.collector._collect("not-a-url", 1)

        mock_record_error.assert_called_once_with(1, "Invalid URL: not-a-url")
        self.assertNotIn("status_notes", result)

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("utils.url_utils.requests.get")
    def test_collect_url_access_fails(
        self, mock_get: Mock, mock_record_error: Mock
    ) -> None:
        """Test _collect when URL access fails calls record_error."""
        import requests

        mock_get.side_effect = requests.exceptions.ConnectionError()

        result = self.collector._collect("https://catalog.data.gov/dataset/x", 1)

        mock_record_error.assert_called_once()
        self.assertIn("URL access failed", mock_record_error.call_args[0][1])
        self.assertNotIn("status_notes", result)

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("utils.url_utils.requests.get")
    def test_collect_page_load_fails(
        self, mock_get: Mock, mock_playwright: Mock, mock_record_error: Mock
    ) -> None:
        """Test _collect when page load fails calls record_error."""
        mock_get.return_value = Mock(status_code=200)

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.side_effect = Exception("Load failed")
        mock_page.wait_for_timeout.return_value = None

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/electric-vehicle-population-data", 1
        )

        mock_record_error.assert_called_once()
        self.assertIn("Failed to load page", mock_record_error.call_args[0][1])

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("utils.url_utils.requests.get")
    def test_collect_downloads_section_missing(
        self, mock_get: Mock, mock_playwright: Mock, mock_record_error: Mock
    ) -> None:
        """Test _collect when Downloads & Resources section not found."""
        mock_get.return_value = Mock(status_code=200)

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None
        mock_page.evaluate.return_value = None

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/x", 1
        )

        mock_record_error.assert_called_once()
        self.assertIn("Downloads & Resources", mock_record_error.call_args[0][1])
        self.assertNotIn("status_notes", result)

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("utils.url_utils.requests.get")
    def test_collect_no_links_in_section(
        self, mock_get: Mock, mock_playwright: Mock, mock_record_error: Mock
    ) -> None:
        """Test _collect when section has no links."""
        mock_get.return_value = Mock(status_code=200)

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None
        mock_page.evaluate.return_value = []

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/x", 1
        )

        mock_record_error.assert_called_once()
        self.assertIn("no links", mock_record_error.call_args[0][1])

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("collectors.CatalogDataCollector.fetch_url_head")
    @patch("utils.url_utils.requests.get")
    def test_collect_all_links_404(
        self,
        mock_get: Mock,
        mock_fetch_head: Mock,
        mock_playwright: Mock,
        mock_record_error: Mock,
    ) -> None:
        """Test _collect when all links return 404 calls record_error."""
        mock_get.return_value = Mock(status_code=200)
        mock_fetch_head.return_value = (404, None, None)

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None
        mock_page.evaluate.return_value = [
            {"href": "https://example.com/file.csv", "text": "CSV"},
        ]

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/x", 1
        )

        mock_record_error.assert_called_once()
        self.assertIn("All download links returned 404", mock_record_error.call_args[0][1])
        self.assertNotIn("status_notes", result)

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("collectors.CatalogDataCollector.fetch_url_head")
    @patch("utils.url_utils.requests.get")
    def test_collect_treats_exception_as_failed_link(
        self,
        mock_get: Mock,
        mock_fetch_head: Mock,
        mock_playwright: Mock,
        mock_record_error: Mock,
    ) -> None:
        """Test _collect treats status -1 as 404; no status_notes when all fail."""
        mock_get.return_value = Mock(status_code=200)
        mock_fetch_head.return_value = (-1, None, "Connection timeout")

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None
        mock_page.evaluate.return_value = [
            {"href": "https://example.com/a.csv", "text": "CSV"},
            {"href": "https://example.com/b.json", "text": "JSON"},
        ]

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/x", 1
        )

        self.assertNotIn("status_notes", result)
        mock_record_error.assert_called_once_with(
            1, "All download links returned 404"
        )

    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("collectors.CatalogDataCollector.fetch_url_head")
    @patch("utils.url_utils.requests.get")
    def test_collect_success(
        self, mock_get: Mock, mock_fetch_head: Mock, mock_playwright: Mock
    ) -> None:
        """Test _collect success: extracts links, follows them, writes status_notes."""
        mock_get.return_value = Mock(status_code=200)

        def fetch_head_side_effect(url: str):
            if "404" in url:
                return 404, None, None
            return 200, "text/csv", None

        mock_fetch_head.side_effect = fetch_head_side_effect

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None
        mock_page.evaluate.return_value = [
            {"href": "https://example.com/data.csv", "text": "Comma Separated Values CSV"},
            {"href": "https://example.com/404.csv", "text": "Missing file"},
            {"href": "https://example.com/data.json", "text": "JSON File"},
        ]

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/x", 1
        )

        self.assertIn("status_notes", result)
        notes = result["status_notes"]
        self.assertIn("Comma Separated Values CSV -> csv", notes)
        self.assertIn("example.com/data.csv", notes)
        self.assertIn("Missing file -> 404", notes)
        self.assertIn("JSON File -> json", notes)

    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("collectors.CatalogDataCollector.fetch_url_head")
    @patch("utils.url_utils.requests.get")
    def test_collect_resolves_catalog_resource_page(
        self,
        mock_get: Mock,
        mock_fetch_head: Mock,
        mock_playwright: Mock,
    ) -> None:
        """Test _collect resolves catalog.data.gov resource pages via #res_url."""
        mock_get.return_value = Mock(status_code=200)
        mock_fetch_head.return_value = (200, "text/csv", None)

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None

        links_response = [
            {
                "href": "https://catalog.data.gov/dataset/x/resource/abc",
                "text": "CSV",
            },
        ]
        res_url_response = {
            "href": "http://aspe.hhs.gov/health/reports/2015/data.csv",
            "dataFormat": "csv",
        }
        mock_page.evaluate.side_effect = [links_response, res_url_response]

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/x", 1
        )

        self.assertIn("status_notes", result)
        notes = result["status_notes"]
        self.assertIn("CSV -> csv", notes)
        self.assertIn("aspe.hhs.gov", notes)
        mock_page.goto.assert_any_call(
            "https://catalog.data.gov/dataset/x/resource/abc",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        mock_fetch_head.assert_called_with(
            "http://aspe.hhs.gov/health/reports/2015/data.csv"
        )

    @patch("collectors.CatalogDataCollector.sync_playwright")
    @patch("collectors.CatalogDataCollector.fetch_url_head")
    @patch("utils.url_utils.requests.get")
    def test_collect_catalog_resource_page_missing_res_url(
        self,
        mock_get: Mock,
        mock_fetch_head: Mock,
        mock_playwright: Mock,
    ) -> None:
        """Test catalog.data.gov resource page without #res_url is treated as 404."""
        mock_get.return_value = Mock(status_code=200)
        mock_fetch_head.return_value = (200, "text/html", None)

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None

        links_response = [
            {"href": "https://catalog.data.gov/dataset/x/resource/abc", "text": "CSV"},
            {"href": "https://example.com/ok.html", "text": "HTML"},
        ]
        mock_page.evaluate.side_effect = [links_response, None]

        result = self.collector._collect(
            "https://catalog.data.gov/dataset/x", 1
        )

        self.assertIn("status_notes", result)
        notes = result["status_notes"]
        self.assertIn("CSV -> 404", notes)
        self.assertIn("HTML -> html", notes)
        mock_fetch_head.assert_called_once_with("https://example.com/ok.html")

    def test_dedupe_links(self) -> None:
        """Test _dedupe_links removes duplicate hrefs, keeps first occurrence."""
        links = [
            ("https://example.com/a.csv", "CSV"),
            ("https://example.com/b.json", "JSON"),
            ("https://example.com/a.csv", "Duplicate CSV"),
        ]
        deduped = self.collector._dedupe_links(links)
        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0], ("https://example.com/a.csv", "CSV"))
        self.assertEqual(deduped[1], ("https://example.com/b.json", "JSON"))

    def test_format_status_notes(self) -> None:
        """Test _format_status_notes includes URL for successful entries."""
        entries = [
            ("Download", "404", ""),
            ("Text File", "html", "https://example.com/page.html"),
        ]
        notes = self.collector._format_status_notes(entries)
        self.assertEqual(
            notes,
            "\n  Download -> 404\n  Text File -> html https://example.com/page.html",
        )

    def test_cleanup_browser_no_browser(self) -> None:
        """Test _cleanup_browser when no browser initialized."""
        self.collector._cleanup_browser()
        self.assertIsNone(self.collector._browser)
        self.assertIsNone(self.collector._playwright)

    @patch("collectors.CatalogDataCollector.Storage")
    @patch.object(CatalogDataCollector, "_collect")
    def test_run_success(
        self, mock_collect: Mock, mock_storage: Mock
    ) -> None:
        """Test run() with successful collection updates Storage with status_notes."""
        mock_storage.get.return_value = {
            "DRPID": 123,
            "source_url": "https://catalog.data.gov/dataset/test",
            "status": "sourced",
        }
        mock_collect.return_value = {
            "status_notes": "\n  CSV File -> csv https://example.com/a.csv\n  JSON File -> json https://example.com/b.json",
        }

        self.collector.run(123)

        mock_storage.get.assert_any_call(123)
        mock_collect.assert_called_once_with(
            "https://catalog.data.gov/dataset/test", 123
        )
        mock_storage.update_record.assert_called_once()
        update_fields = mock_storage.update_record.call_args[0][1]
        self.assertEqual(
            update_fields["status_notes"],
            "\n  CSV File -> csv https://example.com/a.csv\n  JSON File -> json https://example.com/b.json",
        )

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("collectors.CatalogDataCollector.Storage")
    def test_run_record_not_found(
        self, mock_storage: Mock, mock_record_error: Mock
    ) -> None:
        """Test run() when project record doesn't exist."""
        mock_storage.get.return_value = None

        self.collector.run(123)

        mock_record_error.assert_called_once_with(
            123,
            "Project record not found for DRPID: 123",
            update_storage=False,
        )

    @patch("collectors.CatalogDataCollector.record_error")
    @patch("collectors.CatalogDataCollector.Storage")
    def test_run_missing_source_url(
        self, mock_storage: Mock, mock_record_error: Mock
    ) -> None:
        """Test run() when source_url is missing."""
        mock_storage.get.return_value = {"DRPID": 123, "status": "sourced"}

        self.collector.run(123)

        mock_record_error.assert_called_once_with(
            123,
            "Project record missing source_url for DRPID: 123",
        )
