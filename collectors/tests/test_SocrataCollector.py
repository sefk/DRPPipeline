"""
Unit tests for SocrataCollector.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from storage import Storage
from utils.Args import Args
from utils.Logger import Logger

from collectors.SocrataCollector import SocrataCollector
from collectors.tests.test_utils import setup_mock_playwright


class TestSocrataCollector(unittest.TestCase):
    """Test cases for SocrataCollector class."""
    
    def setUp(self) -> None:
        """Set up test environment before each test."""
        self._original_argv = sys.argv.copy()
        sys.argv = ["test", "noop"]
        
        Args.initialize()
        Logger.initialize(log_level="WARNING")
        
        self.temp_dir = Path(tempfile.mkdtemp())
        # Mock Args.base_output_dir to use temp directory
        with patch.object(Args, 'base_output_dir', self.temp_dir):
            self.collector = SocrataCollector(headless=True)
            self.collector._drpid = 1
    
    def tearDown(self) -> None:
        """Clean up after each test."""
        sys.argv = self._original_argv
        self.collector._cleanup_browser()
        if self.temp_dir.exists():
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_init(self) -> None:
        """Test SocrataCollector initialization."""
        with patch.object(Args, 'base_output_dir', self.temp_dir):
            collector = SocrataCollector(headless=True)
            self.assertTrue(collector._headless)
            self.assertIsNone(collector._playwright)
            self.assertIsNone(collector._browser)
            self.assertIsNone(collector._page)
    
    
    @patch("collectors.SocrataCollector.record_error")
    def test_collect_invalid_url(self, mock_record_error: Mock) -> None:
        """Test collect() with invalid URL calls record_error and returns without folder_path."""
        result = self.collector._collect("not-a-url", 1)

        mock_record_error.assert_called_once_with(1, "Invalid URL: not-a-url")
        self.assertNotIn("folder_path", result)
    
    @patch("collectors.SocrataCollector.record_error")
    @patch('utils.url_utils.requests.get')
    def test_collect_url_access_fails(self, mock_get: Mock, mock_record_error: Mock) -> None:
        """Test collect() when URL access fails calls record_error."""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError()

        result = self.collector._collect("https://example.com", 1)

        mock_record_error.assert_called_once()
        self.assertIn("URL access failed", mock_record_error.call_args[0][1])
        self.assertNotIn("folder_path", result)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    @patch('utils.url_utils.requests.get')
    @patch('collectors.SocrataCollector.SocrataPageProcessor')
    @patch('collectors.SocrataCollector.SocrataMetadataExtractor')
    @patch('collectors.SocrataCollector.SocrataDatasetDownloader')
    def test_collect_success_mock(self, mock_downloader_cls: Mock, mock_extractor_cls: Mock, 
                                   mock_processor_cls: Mock, mock_get: Mock, mock_playwright: Mock) -> None:
        """Test collect() with mocked browser (basic flow)."""
        # Mock successful URL access
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        mock_page, _, _ = setup_mock_playwright(mock_playwright)

        # Mock page methods
        mock_page.goto.return_value = None
        mock_page.wait_for_timeout.return_value = None
        
        # Mock processor, downloader, and extractor
        mock_processor = mock_processor_cls.return_value
        mock_processor.generate_pdf.return_value = True
        
        mock_downloader = mock_downloader_cls.return_value
        mock_downloader.download.return_value = False  # Returns bool now
        
        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_all_metadata.return_value = {
            'title': None,
            'rows': None,
            'columns': None,
            'description': None,
            'keywords': None
        }
        
        result = self.collector._collect("https://data.cdc.gov/view/test", 1)

        mock_page.goto.assert_called_once()
        self.assertIn("folder_path", result)
    
    def test_cleanup_browser_no_browser(self) -> None:
        """Test _cleanup_browser when no browser is initialized."""
        # Should not raise error
        self.collector._cleanup_browser()
        self.assertIsNone(self.collector._browser)
        self.assertIsNone(self.collector._playwright)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_init_browser_success(self, mock_playwright: Mock) -> None:
        """Test _init_browser successfully initializes browser."""
        setup_mock_playwright(mock_playwright)

        result = self.collector._init_browser()
        
        self.assertTrue(result)
        self.assertIsNotNone(self.collector._playwright)
        self.assertIsNotNone(self.collector._browser)
        self.assertIsNotNone(self.collector._page)
    
    @patch('collectors.SocrataCollector.sync_playwright')
    def test_init_browser_failure(self, mock_playwright: Mock) -> None:
        """Test _init_browser handles initialization failure."""
        mock_playwright.side_effect = Exception("Browser init failed")
        
        result = self.collector._init_browser()
        
        # Should return False and clean up on failure
        self.assertFalse(result)
        self.assertIsNone(self.collector._browser)
        self.assertIsNone(self.collector._playwright)

    @patch("collectors.SocrataCollector.record_error")
    @patch('collectors.SocrataCollector.create_output_folder', return_value=None)
    @patch('utils.url_utils.requests.get')
    def test_collect_output_folder_fails(self, mock_get: Mock, mock_create: Mock, mock_record_error: Mock) -> None:
        """Test collect() when output folder creation fails calls record_error."""
        mock_get.return_value = Mock(status_code=200)

        result = self.collector._collect("https://data.cdc.gov/view/x", 1)

        mock_record_error.assert_called_once_with(1, "Failed to create output folder")
        self.assertNotIn("folder_path", result)

    @patch("collectors.SocrataCollector.record_error")
    @patch('collectors.SocrataCollector.sync_playwright')
    @patch('utils.url_utils.requests.get')
    def test_collect_page_load_fails(self, mock_get: Mock, mock_playwright: Mock, mock_record_error: Mock) -> None:
        """Test collect() when browser loads URL but page.goto fails calls record_error."""
        mock_get.return_value = Mock(status_code=200)

        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.side_effect = Exception("Load failed")
        mock_page.wait_for_timeout.return_value = None

        with patch.object(Args, "base_output_dir", self.temp_dir):
            result = self.collector._collect("https://data.cdc.gov/view/x", 1)

        mock_record_error.assert_called_once()
        self.assertIn("Failed to load page", mock_record_error.call_args[0][1])
    
    @patch("collectors.SocrataCollector.record_error")
    @patch("collectors.SocrataCollector.Storage")
    def test_run_record_not_found(self, mock_storage: Mock, mock_record_error: Mock) -> None:
        """Test run() when project record doesn't exist: record_error(update_storage=False)."""
        mock_storage.get.return_value = None

        self.collector.run(123)

        mock_storage.get.assert_called_once_with(123)
        mock_record_error.assert_called_once_with(
            123,
            "Project record not found for DRPID: 123",
            update_storage=False,
        )
        self.assertIsNone(self.collector._result)
    
    @patch("collectors.SocrataCollector.record_error")
    @patch("collectors.SocrataCollector.Storage")
    def test_run_missing_source_url(self, mock_storage: Mock, mock_record_error: Mock) -> None:
        """Test run() when project record has no source_url: record_error(update_storage=True)."""
        mock_storage.get.return_value = {"DRPID": 123, "status": "sourced"}

        self.collector.run(123)

        mock_storage.get.assert_called_once_with(123)
        mock_record_error.assert_called_once_with(
            123,
            "Project record missing source_url for DRPID: 123",
        )
        self.assertIsNone(self.collector._result)
    
    @patch("collectors.SocrataCollector.Storage")
    @patch.object(SocrataCollector, "_collect")
    def test_run_successful_collection(self, mock_collect: Mock, mock_storage: Mock) -> None:
        """Test run() with successful collection (flat result dict)."""
        mock_storage.get.return_value = {
            "DRPID": 123,
            "source_url": "https://data.cdc.gov/view/test",
            "status": "sourced",
        }

        folder_path = self.temp_dir / "DRP000123"
        folder_path.mkdir(parents=True, exist_ok=True)
        mock_collect.return_value = {
            "folder_path": str(folder_path),
            "title": "Test Dataset",
            "summary": "<p>Test description</p>",
            "keywords": "health, data, test",
            "collection_notes": "Successfully accessed URL; PDF generated; Dataset downloaded: x.csv",
            "file_size": "5000",
            "download_date": "2025-01-27",
        }

        self.collector.run(123)

        mock_storage.get.assert_any_call(123)
        mock_collect.assert_called_once_with("https://data.cdc.gov/view/test", 123)

        mock_storage.update_record.assert_called_once()
        call_args = mock_storage.update_record.call_args
        self.assertEqual(call_args[0][0], 123)
        update_fields = call_args[0][1]

        self.assertEqual(update_fields["status"], "collected")
        self.assertEqual(update_fields["title"], "Test Dataset")
        self.assertEqual(update_fields["summary"], "<p>Test description</p>")
        self.assertEqual(update_fields["keywords"], "health, data, test")
        self.assertEqual(update_fields["file_size"], "5000")
        self.assertIn("folder_path", update_fields)
        self.assertIn("download_date", update_fields)
        self.assertIn("collection_notes", update_fields)
    
    @patch("collectors.SocrataCollector.Storage")
    @patch.object(SocrataCollector, "_collect")
    def test_run_collection_with_errors(
        self, mock_collect: Mock, mock_storage: Mock
    ) -> None:
        """Test run() when collect() returns result with no folder_path: result is transferred."""
        mock_storage.get.return_value = {
            "DRPID": 123,
            "source_url": "https://data.cdc.gov/view/test",
            "status": "sourced",
        }

        mock_collect.return_value = {"collection_notes": "Invalid URL"}

        self.collector.run(123)

        mock_storage.update_record.assert_called_once()
        update_fields = mock_storage.update_record.call_args[0][1]
        self.assertEqual(update_fields.get("collection_notes"), "Invalid URL")
        self.assertNotIn("status", update_fields)
    
    @patch("collectors.SocrataCollector.Storage")
    @patch.object(SocrataCollector, "_collect")
    def test_run_collection_with_warnings(
        self, mock_collect: Mock, mock_storage: Mock
    ) -> None:
        """Test run() when collect() returns folder_path: status set to collector."""
        mock_storage.get.return_value = {
            "DRPID": 123,
            "source_url": "https://data.cdc.gov/view/test",
            "status": "sourced",
        }

        folder_path = self.temp_dir / "DRP000123"
        folder_path.mkdir(parents=True, exist_ok=True)
        notes = "PDF generated; Large dataset warning - download skipped"
        mock_collect.return_value = {
            "folder_path": str(folder_path),
            "title": "Test Dataset",
            "collection_notes": notes,
        }

        self.collector.run(123)

        update_call = mock_storage.update_record.call_args
        self.assertIsNotNone(update_call)
        self.assertEqual(update_call[0][1].get("status"), "collected")
    
    @patch("collectors.SocrataCollector.record_error")
    @patch("collectors.SocrataCollector.Storage")
    @patch.object(SocrataCollector, "_collect")
    def test_run_collection_exception(self, mock_collect: Mock, mock_storage: Mock, mock_record_error: Mock) -> None:
        """Test run() when collect() raises: record_error is invoked."""
        mock_storage.get.return_value = {
            "DRPID": 123,
            "source_url": "https://data.cdc.gov/view/test",
            "status": "sourced",
        }

        mock_collect.side_effect = Exception("Collection failed")

        self.collector.run(123)

        mock_record_error.assert_called_once()
        args, kwargs = mock_record_error.call_args
        self.assertEqual(args[0], 123)
        self.assertIn("Exception during collection for DRPID 123", args[1])
    
    @patch("collectors.SocrataCollector.Storage")
    @patch.object(SocrataCollector, "_collect")
    def test_run_partial_success_pdf_only(self, mock_collect: Mock, mock_storage: Mock) -> None:
        """Test run() with partial success (PDF but no dataset) using flat result."""
        mock_storage.get.return_value = {
            "DRPID": 123,
            "source_url": "https://data.cdc.gov/view/test",
            "status": "sourced",
        }

        folder_path = self.temp_dir / "DRP000123"
        folder_path.mkdir(parents=True, exist_ok=True)
        mock_collect.return_value = {
            "folder_path": str(folder_path),
            "title": "Test Dataset",
            "collection_notes": "Successfully accessed URL; PDF generated",
        }

        self.collector.run(123)

        update_call = mock_storage.update_record.call_args
        self.assertIsNotNone(update_call)
        update_fields = update_call[0][1]
        self.assertEqual(update_fields["status"], "collected")
        self.assertNotIn("download_date", update_fields)

    # record_error() is in utils.Errors and tested in utils.test_Errors.

