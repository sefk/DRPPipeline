"""
Unit tests for Sourcing module.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage import Storage
from utils.Args import Args
from utils.Logger import Logger

from sourcing import Sourcing


class TestSourcing(unittest.TestCase):
    """Test cases for Sourcing."""

    def setUp(self) -> None:
        """Set up test environment before each test."""
        self._original_argv = sys.argv.copy()
        sys.argv = ["test", "sourcing"]

        Args.initialize()
        Logger.initialize(log_level="WARNING")

        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_db_path = self.temp_dir / "test_drp_pipeline.db"
        self.storage = Storage.initialize("StorageSQLLite", db_path=self.test_db_path)
        self.sourcing = Sourcing()

    def tearDown(self) -> None:
        """Clean up after each test."""
        sys.argv = self._original_argv
        self.storage.close()
        Storage.reset()  # Reset singleton for next test
        if self.temp_dir.exists():
            import shutil
            shutil.rmtree(self.temp_dir)

    @patch.object(Sourcing, "get_candidate_urls", return_value=([], 0))  # list of row dicts, skipped_count
    def test_run_returns_none(self, _mock_get: object) -> None:
        """Test run(-1) returns None after processing (no URLs)."""
        result = self.sourcing.run(-1)
        self.assertIsNone(result)

    @patch("sourcing.Sourcing.SpreadsheetCandidateFetcher")
    def test_get_candidate_urls_delegates_to_fetcher(self, mock_fetcher_cls: object) -> None:
        """Test get_candidate_urls(limit=...) delegates to SpreadsheetCandidateFetcher."""
        mock_fetcher = mock_fetcher_cls.return_value
        mock_fetcher.get_candidate_urls.return_value = (
            [{"url": "https://example.com/1", "office": "OHA", "agency": "CDC"}],
            0,
        )
        rows, skipped = self.sourcing.get_candidate_urls(limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["url"], "https://example.com/1")
        self.assertEqual(rows[0]["office"], "OHA")
        self.assertEqual(rows[0]["agency"], "CDC")
        self.assertEqual(skipped, 0)
        mock_fetcher_cls.assert_called_once()
        mock_fetcher.get_candidate_urls.assert_called_once_with(limit=10)

    @patch("utils.url_utils.fetch_page_body", return_value=(200, "", "text/html", False))
    @patch.object(Sourcing, "get_candidate_urls")
    def test_run_creates_row_status_sourced(self, mock_get: object, _mock_fetch: object) -> None:
        """Test run creates record and sets status 'sourced' when URL is good."""
        mock_get.return_value = (
            [{"url": "https://example.com/good", "office": "O", "agency": "A"}],
            0,
        )
        self.sourcing.run(-1)
        projects = self.storage.list_eligible_projects("sourced", None)
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]["status"], "sourced")
        self.assertEqual(projects[0]["source_url"], "https://example.com/good")

    @patch("utils.url_utils.fetch_page_body", return_value=(404, "", None, False))
    @patch.object(Sourcing, "get_candidate_urls")
    def test_run_creates_row_status_not_found(self, mock_get: object, _mock_fetch: object) -> None:
        """Test run creates record and sets status 'not_found' when URL returns 404."""
        mock_get.return_value = (
            [{"url": "https://example.com/missing", "office": "O", "agency": "A"}],
            0,
        )
        self.sourcing.run(-1)
        record = self.storage.get(1)
        self.assertIsNotNone(record)
        self.assertEqual(record["status"], "not_found")
        self.assertEqual(record["source_url"], "https://example.com/missing")

    @patch("utils.Logger.Logger.error")
    @patch.object(Sourcing, "get_candidate_urls")
    def test_run_dupe_in_storage_logs_error_no_row(self, mock_get: object, mock_log_error: object) -> None:
        """Test run does not create a row for duplicate URL; logs Error."""
        self.storage.create_record("https://example.com/dup")
        mock_get.return_value = (
            [{"url": "https://example.com/dup", "office": "O", "agency": "A"}],
            0,
        )
        self.sourcing.run(-1)
        # Only one row (the pre-existing one); no second row created
        record1 = self.storage.get(1)
        self.assertEqual(record1["source_url"], "https://example.com/dup")
        self.assertIsNone(self.storage.get(2))
        mock_log_error.assert_called_once()
        call_msg = mock_log_error.call_args[0][0]
        self.assertIn("Duplicate source URL already in storage", call_msg)
        self.assertIn("https://example.com/dup", call_msg)

    @patch("utils.url_utils.fetch_page_body", side_effect=Exception("network error"))
    @patch.object(Sourcing, "get_candidate_urls")
    def test_run_creates_row_status_error_on_exception(
        self, mock_get: object, _mock_fetch: object
    ) -> None:
        """Test run creates record and sets status 'Error' when fetch raises."""
        mock_get.return_value = (
            [{"url": "https://example.com/bad", "office": "O", "agency": "A"}],
            0,
        )
        self.sourcing.run(-1)
        record = self.storage.get(1)
        self.assertIsNotNone(record)
        self.assertEqual(record["status"], "error")
        self.assertIn("network error", record.get("errors", ""))


