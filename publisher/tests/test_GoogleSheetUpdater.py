"""
Unit tests for GoogleSheetUpdater (publisher module).
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from utils.Args import Args
from utils.Logger import Logger

from publisher.GoogleSheetUpdater import (
    DOWNLOAD_LOCATION_TEMPLATE,
    GoogleSheetUpdater,
)

# Skip tests that mock Google API when the API is not installed
import publisher.GoogleSheetUpdater as _gsu_module
_GOOGLE_AVAILABLE = getattr(_gsu_module, "_GOOGLE_SHEETS_AVAILABLE", False)
skip_if_no_google = unittest.skipIf(not _GOOGLE_AVAILABLE, "Google Sheets API not installed")


class TestGoogleSheetUpdater(unittest.TestCase):
    """Test cases for GoogleSheetUpdater."""

    def setUp(self) -> None:
        """Initialize Args and Logger so updater can read Args."""
        sys.argv = ["test", "publisher"]
        Args._initialized = False
        Args._config = {}
        Args._parsed_args = {}
        Args.initialize()
        Logger.initialize(log_level="WARNING")

    def tearDown(self) -> None:
        """Reset Args."""
        Args._initialized = False
        Args._config = {}
        Args._parsed_args = {}

    def test_column_index_to_letter(self) -> None:
        """Test _column_index_to_letter for A, B, Z, AA."""
        updater = GoogleSheetUpdater()
        self.assertEqual(updater._column_index_to_letter(1), "A")
        self.assertEqual(updater._column_index_to_letter(2), "B")
        self.assertEqual(updater._column_index_to_letter(26), "Z")
        self.assertEqual(updater._column_index_to_letter(27), "AA")

    def test_download_location_template(self) -> None:
        """Test DOWNLOAD_LOCATION_TEMPLATE format."""
        url = DOWNLOAD_LOCATION_TEMPLATE.format(workspace_id="239181")
        self.assertEqual(
            url,
            "https://www.datalumos.org/datalumos/project/239181/version/V1/view",
        )

    def test_build_update_requests_formats_file_size(self) -> None:
        """Test _build_update_requests formats raw byte count as user-friendly size."""
        updater = GoogleSheetUpdater()
        column_map = {
            "URL": "A",
            "Claimed": "B",
            "Data Added": "C",
            "Download Location": "D",
            "Date Downloaded": "E",
            "Dataset Size": "F",
            "File extensions of data uploads": "G",
            "Metadata availability info": "H",
            "Dataset Download Possible?": "I",
            "Nominated to EOT / USGWDA": "J",
        }
        project = {"file_size": "10485760", "download_date": "2025-01-15", "extensions": "csv"}
        requests = updater._build_update_requests(
            "CDC", 2, column_map, "239181", project, "testuser"
        )
        dataset_size_requests = [r for r in requests if "F2" in r.get("range", "")]
        self.assertEqual(len(dataset_size_requests), 1)
        self.assertEqual(dataset_size_requests[0]["values"], [["10.0 MB"]])

    @patch("publisher.GoogleSheetUpdater._GOOGLE_SHEETS_AVAILABLE", True)
    def test_update_missing_sheet_id(self) -> None:
        """Test update returns error when Args.google_sheet_id is empty."""
        updater = GoogleSheetUpdater()
        with patch.object(Args, "google_sheet_id", ""), patch.object(Args, "google_credentials", None):
            success, msg = updater.update("https://example.com", "123", {})
        self.assertFalse(success)
        self.assertIn("required", (msg or "").lower())

    @patch("publisher.GoogleSheetUpdater._GOOGLE_SHEETS_AVAILABLE", True)
    def test_update_missing_source_url(self) -> None:
        """Test update returns error when source_url is empty."""
        updater = GoogleSheetUpdater()
        cred = Path(tempfile.gettempdir()) / "nonexistent.json"
        with patch.object(Args, "google_sheet_id", "abc123"), patch.object(Args, "google_credentials", cred):
            success, msg = updater.update("", "123", {})
        self.assertFalse(success)
        self.assertIn("source url", (msg or "").lower())

    @patch("publisher.GoogleSheetUpdater._GOOGLE_SHEETS_AVAILABLE", False)
    def test_update_google_sheets_not_available(self) -> None:
        """Test update returns error when Google Sheets API is not installed."""
        updater = GoogleSheetUpdater()
        success, msg = updater.update("https://example.com", "123", {})
        self.assertFalse(success)
        self.assertIn("not installed", msg.lower())

    @patch("publisher.GoogleSheetUpdater._GOOGLE_SHEETS_AVAILABLE", True)
    def test_update_for_not_found_or_no_links_missing_sheet_id(self) -> None:
        """Test update_for_not_found_or_no_links returns error when sheet ID missing."""
        updater = GoogleSheetUpdater()
        with patch.object(Args, "google_sheet_id", ""), patch.object(Args, "google_credentials", None):
            success, msg = updater.update_for_not_found_or_no_links(
                "https://example.com", "Not found"
            )
        self.assertFalse(success)
        self.assertIn("required", (msg or "").lower())

    @patch("publisher.GoogleSheetUpdater._GOOGLE_SHEETS_AVAILABLE", False)
    def test_update_for_not_found_or_no_links_api_not_available(self) -> None:
        """Test update_for_not_found_or_no_links returns error when API not installed."""
        updater = GoogleSheetUpdater()
        success, msg = updater.update_for_not_found_or_no_links(
            "https://example.com", "No live links"
        )
        self.assertFalse(success)
        self.assertIn("not installed", (msg or "").lower())

    @skip_if_no_google
    @patch("google.oauth2.service_account.Credentials.from_service_account_file")
    def test_update_credentials_not_found(self, mock_from_sa: MagicMock) -> None:
        """Test update returns error when credentials file does not exist."""
        mock_from_sa.side_effect = FileNotFoundError()
        updater = GoogleSheetUpdater()
        cred = Path(tempfile.gettempdir()) / "nonexistent_creds.json"
        with patch.object(Args, "google_sheet_id", "abc"), patch.object(
            Args, "google_credentials", cred
        ), patch.object(Args, "google_sheet_name", "CDC"):
            success, msg = updater.update("https://example.com", "123", {})
        self.assertFalse(success)
        self.assertIn("not found", (msg or "").lower())

    @skip_if_no_google
    @patch("publisher.GoogleSheetUpdater.build")
    @patch("google.oauth2.service_account.Credentials.from_service_account_file")
    def test_update_success_mocked(
        self, mock_from_sa: MagicMock, mock_build: MagicMock
    ) -> None:
        """Test update success path with mocked Sheets API."""
        mock_creds = MagicMock()
        mock_creds.universe_domain = "googleapis.com"
        mock_from_sa.return_value = mock_creds
        mock_service = MagicMock()

        # First get: header row (CDC!1:1). Second get: URL column A2:A.
        header_response = {
            "values": [
                [
                    "URL",
                    "Claimed",
                    "Data Added",
                    "Download Location",
                    "Date Downloaded",
                    "Dataset Size",
                    "File extensions of data uploads",
                    "Metadata availability info",
                    "Dataset Download Possible?",
                    "Nominated to EOT / USGWDA",
                ]
            ]
        }
        url_column_response = {"values": [["https://example.com/data"], ["https://other.com"]]}
        mock_get = mock_service.spreadsheets.return_value.values.return_value.get.return_value
        mock_get.execute.side_effect = [header_response, url_column_response]
        mock_service.spreadsheets.return_value.values.return_value.batchUpdate.return_value.execute.return_value = {}

        mock_build.return_value = mock_service

        cred_path = Path(tempfile.gettempdir()) / "creds_pub_test.json"
        cred_path.write_text("{}")

        updater = GoogleSheetUpdater()
        with patch.object(Args, "google_sheet_id", "sheet123"), patch.object(
            Args, "google_credentials", cred_path
        ), patch.object(Args, "google_sheet_name", "CDC"), patch.object(Args, "google_username", "testuser"):
            success, msg = updater.update(
                "https://example.com/data",
                "239181",
                {
                    "download_date": "2025-01-15",
                    "file_size": "10 MB",
                    "extensions": "csv, zip",
                },
            )

        cred_path.unlink(missing_ok=True)

        self.assertTrue(success)
        self.assertIsNone(msg)
        mock_service.spreadsheets.return_value.values.return_value.batchUpdate.assert_called_once()
