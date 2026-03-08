"""
Unit tests for SocrataDatasetDownloader.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from utils.Args import Args
from utils.Logger import Logger

from collectors.SocrataCollector import SocrataCollector
from collectors.SocrataDatasetDownloader import (
    SocrataDatasetDownloader,
    _build_socrata_export_url,
    _extension_from_export_url,
    _get_socrata_view_id_from_url,
    _is_socrata_export_url,
)
from collectors.tests.test_utils import setup_mock_playwright


class TestSocrataDatasetDownloader(unittest.TestCase):
    """Test cases for SocrataDatasetDownloader class."""
    
    def setUp(self) -> None:
        """Set up test environment before each test."""
        self._original_argv = sys.argv.copy()
        sys.argv = ["test", "noop"]
        
        Args.initialize()
        Logger.initialize(log_level="WARNING")
        
        self.temp_dir = Path(tempfile.mkdtemp())
        with patch.object(Args, "base_output_dir", self.temp_dir):
            self.collector = SocrataCollector(headless=True)
            self.collector._result = {}
            self.collector._drpid = 1
            self.downloader = SocrataDatasetDownloader(self.collector)
    
    def tearDown(self) -> None:
        """Clean up after each test."""
        sys.argv = self._original_argv
        self.collector._cleanup_browser()
        if self.temp_dir.exists():
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_init(self) -> None:
        """Test SocrataDatasetDownloader initialization."""
        with patch.object(Args, 'base_output_dir', self.temp_dir):
            collector = SocrataCollector(headless=True)
            downloader = SocrataDatasetDownloader(collector)
            self.assertEqual(downloader._collector, collector)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_click_export_button_success(self, mock_playwright: Mock) -> None:
        """Test _click_export_button successfully clicks Export button."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        mock_button = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        mock_page.locator.return_value = mock_button
        mock_button.count.return_value = 1
        mock_button.first.scroll_into_view_if_needed.return_value = None
        mock_button.first.click.return_value = None
        
        result = downloader._click_export_button()
        
        self.assertTrue(result)
        mock_page.locator.assert_called_with('forge-button[data-testid="export-data-button"]')
        mock_button.first.click.assert_called_once()
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_click_export_button_not_found(self, mock_playwright: Mock) -> None:
        """Test _click_export_button returns False when button not found."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        mock_button = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        mock_page.locator.return_value = mock_button
        mock_button.count.return_value = 0
        
        result = downloader._click_export_button()
        
        self.assertFalse(result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_click_export_button_exception(self, mock_playwright: Mock) -> None:
        """Test _click_export_button handles exceptions."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        mock_page.locator.side_effect = Exception("Error")
        
        result = downloader._click_export_button()
        
        self.assertFalse(result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_find_download_button_success(self, mock_playwright: Mock) -> None:
        """Test _find_download_button finds Download button."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        mock_button = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        mock_page.locator.return_value = mock_button
        mock_button.count.return_value = 1
        
        result = downloader._find_download_button()
        
        self.assertIsNotNone(result)
        self.assertEqual(result, mock_button.first)
        mock_page.locator.assert_called_with('forge-button[data-testid="export-download-button"]')
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_find_download_button_not_found(self, mock_playwright: Mock) -> None:
        """Test _find_download_button returns None when button not found."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        mock_button = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        mock_page.locator.return_value = mock_button
        mock_button.count.return_value = 0
        
        result = downloader._find_download_button()
        
        self.assertIsNone(result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_get_file_extension_success(self, mock_playwright: Mock) -> None:
        """Test _get_file_extension extracts extension from file."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        # Create a test file
        test_file = self.temp_dir / "test.csv"
        test_file.write_text("test data")
        
        result = downloader._get_file_extension(test_file)
        
        self.assertEqual(result, "csv")
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_get_file_extension_no_extension(self, mock_playwright: Mock) -> None:
        """Test _get_file_extension returns None when no extension."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        # Create a test file without extension
        test_file = self.temp_dir / "test"
        test_file.write_text("test data")
        
        result = downloader._get_file_extension(test_file)
        
        self.assertIsNone(result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_get_file_extension_file_not_exists(self, mock_playwright: Mock) -> None:
        """Test _get_file_extension returns None when file doesn't exist."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        test_file = self.temp_dir / "nonexistent.csv"
        
        result = downloader._get_file_extension(test_file)
        
        self.assertIsNone(result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_download_file_success(self, mock_playwright: Mock) -> None:
        """Test _download_file successfully downloads and saves file."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        mock_button = Mock()
        mock_download = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        # Setup mocks
        mock_page.locator.return_value = mock_button
        mock_button.count.return_value = 1
        mock_button.first.scroll_into_view_if_needed.return_value = None
        mock_button.first.click.return_value = None
        
        # Mock download
        mock_download.suggested_filename = "dataset.csv"
        mock_download.save_as = Mock()
        
        # Mock expect_download context manager
        mock_context = MagicMock()
        mock_context.__enter__ = Mock(return_value=Mock(value=mock_download))
        mock_context.__exit__ = Mock(return_value=None)
        mock_page.expect_download.return_value = mock_context
        
        # Create test file after download
        test_file = self.temp_dir / "dataset.csv"
        
        with patch.object(Args, 'use_url_download', False), \
             patch.object(downloader, '_find_download_button', return_value=mock_button.first), \
             patch.object(downloader, '_get_file_extension', return_value="csv"):
            # Manually create file to simulate download (save_as is mocked). This test
            # asserts that _download_file updates _result (file_size, extensions,
            # download_date) correctly after a successful save.
            test_file.write_text("test,data\n1,2")
            
            result = downloader._download_file(self.temp_dir, timeout=60000)
        
        self.assertTrue(result)
        self.assertIn("file_size", self.collector._result)
        self.assertEqual(self.collector._result["file_size"], str(test_file.stat().st_size))
        self.assertIn("extensions", self.collector._result)
        self.assertEqual(self.collector._result["extensions"], "pdf, csv")
        self.assertIn("download_date", self.collector._result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_download_file_no_button(self, mock_playwright: Mock) -> None:
        """Test _download_file returns False when Download button not found."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        # Mock expect_download context manager
        mock_context = MagicMock()
        mock_context.__enter__ = Mock()
        mock_context.__exit__ = Mock(return_value=None)
        mock_page.expect_download.return_value = mock_context
        
        with patch("collectors.SocrataDatasetDownloader.record_error") as mock_record_error, \
             patch.object(downloader, '_find_download_button', return_value=None):
            result = downloader._download_file(self.temp_dir, timeout=60000)
        
        self.assertFalse(result)
        mock_record_error.assert_called_once_with(1, "Download button not found in dialog")
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_download_success(self, mock_playwright: Mock) -> None:
        """Test download() returns True when _download_file succeeds."""
        setup_mock_playwright(mock_playwright)

        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)

        with patch.object(Args, 'use_url_download', False), \
             patch.object(downloader, '_click_export_button', return_value=True), \
             patch.object(downloader, '_download_file', return_value=True):
            result = downloader.download(self.temp_dir)

        self.assertTrue(result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_download_export_button_not_found(self, mock_playwright: Mock) -> None:
        """Test download() returns False when Export button not found."""
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        mock_page.url = "https://data.cityofnewyork.us/view/abc123/about_data"
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        with patch("collectors.SocrataDatasetDownloader.record_error") as mock_record_error, \
             patch.object(Args, 'use_url_download', False), \
             patch.object(downloader, '_click_export_button', return_value=False):
            result = downloader.download(self.temp_dir)
        
        self.assertFalse(result)
        mock_record_error.assert_called_once_with(1, "Export button not found")
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_download_timeout(self, mock_playwright: Mock) -> None:
        """Test download() handles timeout exception."""
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        
        mock_playwright_instance = Mock()
        mock_browser = Mock()
        mock_page = Mock()
        
        mock_playwright.return_value.start.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        self.collector._init_browser()
        downloader = SocrataDatasetDownloader(self.collector)
        
        with patch.object(Args, 'use_url_download', False), \
             patch("collectors.SocrataDatasetDownloader.record_error") as mock_record_error, \
             patch.object(downloader, '_click_export_button', return_value=True), \
             patch.object(downloader, '_download_file', side_effect=PlaywrightTimeoutError("Timeout")):
            result = downloader.download(self.temp_dir)
        
        self.assertFalse(result)
        mock_record_error.assert_called_once_with(1, "Timeout waiting for download")

    def test_get_socrata_view_id_from_url_about_data(self) -> None:
        """_get_socrata_view_id_from_url returns segment before about_data."""
        self.assertEqual(
            _get_socrata_view_id_from_url("https://data.cdc.gov/view/yctb-fv7w/about_data"),
            "yctb-fv7w",
        )
        self.assertIsNone(_get_socrata_view_id_from_url("https://example.com/about_data"))

    def test_get_socrata_view_id_from_url_last_segment(self) -> None:
        """_get_socrata_view_id_from_url returns last segment when no about_data."""
        self.assertEqual(
            _get_socrata_view_id_from_url("https://data.cdc.gov/view/abc-123"),
            "abc-123",
        )

    def test_get_socrata_view_id_from_url_empty_path(self) -> None:
        """_get_socrata_view_id_from_url returns None for empty path."""
        self.assertIsNone(_get_socrata_view_id_from_url("https://example.com/"))

    def test_build_socrata_export_url(self) -> None:
        """_build_socrata_export_url builds API export URL."""
        url = _build_socrata_export_url("https://data.cdc.gov/view/yctb-fv7w", "yctb-fv7w")
        self.assertIn("/api/v3/views/yctb-fv7w/export.csv", url)
        self.assertIn("accessType=DOWNLOAD", url)

    def test_is_socrata_export_url_true(self) -> None:
        """_is_socrata_export_url returns True for Socrata export URLs."""
        self.assertTrue(_is_socrata_export_url("https://data.cdc.gov/api/v3/views/abc/export.csv?x=1"))

    def test_is_socrata_export_url_false(self) -> None:
        """_is_socrata_export_url returns False for non-export URLs."""
        self.assertFalse(_is_socrata_export_url("https://data.cdc.gov/view/abc"))

    def test_extension_from_export_url_path(self) -> None:
        """_extension_from_export_url returns extension from path."""
        self.assertEqual(_extension_from_export_url("https://x/api/views/id/export.csv"), "csv")

    def test_extension_from_export_url_query_format(self) -> None:
        """_extension_from_export_url uses format query param when path has no extension."""
        self.assertEqual(_extension_from_export_url("https://x/api/views/id?format=json"), "json")

    def test_extension_from_export_url_default_csv(self) -> None:
        """_extension_from_export_url defaults to csv."""
        self.assertEqual(_extension_from_export_url("https://x/api/views/id"), "csv")

    @patch("collectors.SocrataDatasetDownloader.download_via_url")
    @patch("collectors.SocrataCollector.sync_playwright")
    def test_download_via_constructed_url_success(
        self, mock_playwright: Mock, mock_download: Mock
    ) -> None:
        """download() with use_url_download and view_id uses _download_via_constructed_url and succeeds."""
        setup_mock_playwright(mock_playwright)
        self.collector._init_browser()
        self.collector._page.url = "https://data.cdc.gov/view/yctb-fv7w/about_data"
        self.collector._page.title = Mock(return_value="My Dataset")
        mock_download.return_value = (1000, True)
        downloader = SocrataDatasetDownloader(self.collector)
        (self.temp_dir / "existing.pdf").write_text("x")
        (self.temp_dir / "My_Dataset.csv").write_text("a,b\n1,2")
        with patch.object(Args, "use_url_download", True):
            result = downloader.download(self.temp_dir, timeout=60000)
        self.assertTrue(result)
        mock_download.assert_called_once()
        self.assertEqual(self.collector._result.get("extensions"), "pdf, csv")

    @patch("collectors.SocrataDatasetDownloader.download_via_url")
    @patch("collectors.SocrataCollector.sync_playwright")
    def test_download_via_constructed_url_401_falls_back_to_dialog(
        self, mock_playwright: Mock, mock_download: Mock
    ) -> None:
        """download() when direct URL returns 401 falls back to Export dialog."""
        import requests
        setup_mock_playwright(mock_playwright)
        self.collector._init_browser()
        self.collector._page.url = "https://data.cdc.gov/view/yctb-fv7w/about_data"
        self.collector._page.title = Mock(return_value="My Dataset")
        resp = Mock(status_code=401)
        mock_download.side_effect = requests.HTTPError(response=resp)
        downloader = SocrataDatasetDownloader(self.collector)
        mock_download_file = Mock(return_value=True)
        with patch.object(Args, "use_url_download", True), \
             patch.object(downloader, "_click_export_button", return_value=True), \
             patch.object(downloader, "_download_file", mock_download_file):
            result = downloader.download(self.temp_dir, timeout=60000)
        self.assertTrue(result)
        mock_download_file.assert_called_once()

    @patch("collectors.SocrataCollector.sync_playwright")
    def test_download_generic_exception_records_error(self, mock_playwright: Mock) -> None:
        """download() records error and returns False on generic exception."""
        setup_mock_playwright(mock_playwright)
        self.collector._init_browser()
        self.collector._page.url = "https://data.cdc.gov/view/abc/about_data"
        downloader = SocrataDatasetDownloader(self.collector)
        with patch.object(Args, "use_url_download", True), \
             patch("collectors.SocrataDatasetDownloader.record_error") as mock_record_error, \
             patch.object(downloader, "_download_via_constructed_url", side_effect=RuntimeError("network error")):
            result = downloader.download(self.temp_dir)
        self.assertFalse(result)
        mock_record_error.assert_called_once()
        self.assertIn("network error", mock_record_error.call_args[0][1])
