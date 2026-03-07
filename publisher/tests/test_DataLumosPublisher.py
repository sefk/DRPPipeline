"""
Unit tests for DataLumosPublisher (publisher module).
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from storage import Storage
from utils.Args import Args
from utils.Logger import Logger

from publisher.DataLumosPublisher import DataLumosPublisher, PUBLISHED_URL_TEMPLATE


class TestDataLumosPublisher(unittest.TestCase):
    """Test cases for DataLumosPublisher module."""

    def setUp(self) -> None:
        """Set up test environment before each test."""
        self._original_argv = sys.argv.copy()
        sys.argv = ["test", "publisher"]

        Args._initialized = False
        Args._config = {}
        Args._parsed_args = {}
        Args.initialize()
        Logger.initialize(log_level="WARNING")

        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_db_path = self.temp_dir / "test_drp_pipeline.db"
        self.storage = Storage.initialize("StorageSQLLite", db_path=self.test_db_path)
        self.publisher = DataLumosPublisher()

    def tearDown(self) -> None:
        """Clean up after each test."""
        sys.argv = self._original_argv
        self.storage.close()
        Storage.reset()
        Args._initialized = False
        Args._config = {}
        Args._parsed_args = {}
        if self.temp_dir.exists():
            import shutil
            try:
                shutil.rmtree(self.temp_dir)
            except OSError:
                pass

    def test_get_field(self) -> None:
        """Test _get_field returns trimmed value or empty string."""
        project = {"datalumos_id": "  12345  ", "missing": None}
        self.assertEqual(self.publisher._get_field(project, "datalumos_id"), "12345")
        self.assertEqual(self.publisher._get_field(project, "missing"), "")

    def test_project_url(self) -> None:
        """Test _project_url builds correct workspace URL."""
        url = self.publisher._project_url("239181")
        self.assertIn("datalumos/239181", url)
        self.assertIn("goToLevel=project", url)

    def test_published_url_template(self) -> None:
        """Test PUBLISHED_URL_TEMPLATE format."""
        url = PUBLISHED_URL_TEMPLATE.format(workspace_id="239181")
        self.assertEqual(
            url,
            "https://www.datalumos.org/datalumos/project/239181/version/V1/view",
        )

    def test_run_project_not_found(self) -> None:
        """Test run records error when project not found."""
        with patch("publisher.DataLumosPublisher.record_error") as mock_record_error:
            self.publisher.run(9999)
            mock_record_error.assert_called_once()
            args = mock_record_error.call_args[0]
            self.assertEqual(args[0], 9999)
            self.assertIn("not found", args[1])

    def test_run_missing_datalumos_id(self) -> None:
        """Test run records error when datalumos_id is missing."""
        drpid = Storage.create_record("https://example.com/test")
        # Project has no datalumos_id (only source_url from create_record)

        with patch("publisher.DataLumosPublisher.record_error") as mock_record_error:
            self.publisher.run(drpid)
            mock_record_error.assert_called_once()
            args = mock_record_error.call_args[0]
            self.assertEqual(args[0], drpid)
            self.assertIn("datalumos_id", args[1])

    def test_run_not_found_updates_sheet_only(self) -> None:
        """Test run with status not_found updates sheet only and sets status updated_inventory."""
        drpid = Storage.create_record("https://example.com/notfound")
        Storage.update_record(drpid, {"status": "not_found"})

        mock_updater = MagicMock()
        mock_updater.update_for_not_found_or_no_links.return_value = (True, None)

        with patch("publisher.GoogleSheetUpdater.GoogleSheetUpdater", return_value=mock_updater), patch.object(
            Args, "google_sheet_id", "sheet1"
        ), patch.object(Args, "google_credentials", __file__):
            self.publisher.run(drpid)

        mock_updater.update_for_not_found_or_no_links.assert_called_once_with(
            source_url="https://example.com/notfound",
            notes_value="Not found",
        )
        record = Storage.get(drpid)
        self.assertEqual(record.get("status"), "updated_not_found")

    def test_run_no_links_updates_sheet_only(self) -> None:
        """Test run with status no_links updates sheet only with Notes 'No live links'."""
        drpid = Storage.create_record("https://example.com/nolinks")
        Storage.update_record(drpid, {"status": "no_links"})

        mock_updater = MagicMock()
        mock_updater.update_for_not_found_or_no_links.return_value = (True, None)

        with patch("publisher.GoogleSheetUpdater.GoogleSheetUpdater", return_value=mock_updater), patch.object(
            Args, "google_sheet_id", "sheet1"
        ), patch.object(Args, "google_credentials", __file__):
            self.publisher.run(drpid)

        mock_updater.update_for_not_found_or_no_links.assert_called_once_with(
            source_url="https://example.com/nolinks",
            notes_value="No live links",
        )
        record = Storage.get(drpid)
        self.assertEqual(record.get("status"), "updated_no_links")

    @patch("upload.DataLumosAuthenticator.wait_for_human_verification")
    @patch("publisher.DataLumosPublisher.DataLumosPublisher._publish_workspace")
    def test_run_success_updates_storage(
        self,
        mock_publish_workspace: MagicMock,
        mock_wait_for_human: MagicMock,
    ) -> None:
        """Test run updates Storage with published_url and status on success."""
        drpid = Storage.create_record("https://example.com/test")
        Storage.update_record(drpid, {"datalumos_id": "239181", "status": "uploaded"})

        mock_page = MagicMock()
        self.publisher._session.ensure_browser = MagicMock(return_value=mock_page)
        self.publisher._session.ensure_authenticated = MagicMock(return_value=None)
        self.publisher._session.close = MagicMock(return_value=None)
        mock_publish_workspace.return_value = (True, None)

        with patch("publisher.DataLumosPublisher.record_error"), patch.object(
            Args, "google_sheet_id", None
        ), patch.object(Args, "google_credentials", None):
            self.publisher.run(drpid)

        mock_publish_workspace.assert_called_once_with(mock_page, drpid)
        expected_url = "https://www.datalumos.org/datalumos/project/239181/version/V1/view"
        record = Storage.get(drpid)
        self.assertIsNotNone(record)
        self.assertEqual(record.get("status"), "published")
        self.assertEqual(record.get("published_url"), expected_url)

    @patch("publisher.DataLumosPublisher.DataLumosPublisher._update_google_sheet_if_configured")
    @patch("upload.DataLumosAuthenticator.wait_for_human_verification")
    @patch("publisher.DataLumosPublisher.DataLumosPublisher._publish_workspace")
    def test_run_calls_google_sheet_update_when_configured(
        self,
        mock_publish_workspace: MagicMock,
        mock_wait_for_human: MagicMock,
        mock_update_sheet: MagicMock,
    ) -> None:
        """Test run calls _update_google_sheet_if_configured after successful publish."""
        drpid = Storage.create_record("https://example.com/test")
        Storage.update_record(drpid, {"datalumos_id": "239181", "status": "uploaded"})
        mock_page = MagicMock()
        self.publisher._session.ensure_browser = MagicMock(return_value=mock_page)
        self.publisher._session.ensure_authenticated = MagicMock(return_value=None)
        self.publisher._session.close = MagicMock(return_value=None)
        mock_publish_workspace.return_value = (True, None)

        with patch("publisher.DataLumosPublisher.record_error"):
            self.publisher.run(drpid)

        call_args = mock_update_sheet.call_args[0]
        self.assertEqual(call_args[0], drpid)
        self.assertIsInstance(call_args[1], dict)
        self.assertEqual(call_args[1].get("datalumos_id"), "239181")
        self.assertEqual(call_args[2], "239181")

    @patch("publisher.GoogleSheetUpdater.GoogleSheetUpdater")
    @patch("upload.DataLumosAuthenticator.wait_for_human_verification")
    @patch("publisher.DataLumosPublisher.DataLumosPublisher._publish_workspace")
    def test_run_sets_status_updated_inventory_when_sheet_update_succeeds(
        self,
        mock_publish_workspace: MagicMock,
        mock_wait_for_human: MagicMock,
        mock_gsu_class: MagicMock,
    ) -> None:
        """Test run sets status to updated_inventory after successful Google Sheet update."""
        drpid = Storage.create_record("https://example.com/test")
        Storage.update_record(drpid, {"datalumos_id": "239181", "status": "uploaded"})
        mock_page = MagicMock()
        self.publisher._session.ensure_browser = MagicMock(return_value=mock_page)
        self.publisher._session.ensure_authenticated = MagicMock(return_value=None)
        self.publisher._session.close = MagicMock(return_value=None)
        mock_publish_workspace.return_value = (True, None)
        mock_gsu_class.return_value.update.return_value = (True, None)

        cred_file = self.temp_dir / "creds.json"
        cred_file.write_text("{}")

        with patch.object(Args, "google_sheet_id", "sheet123"), patch.object(
            Args, "google_credentials", str(cred_file)
        ), patch("publisher.DataLumosPublisher.record_error"):
            self.publisher.run(drpid)

        record = Storage.get(drpid)
        self.assertIsNotNone(record)
        self.assertEqual(record.get("status"), "updated_inventory")

    @patch("publisher.DataLumosPublisher.record_crash")
    @patch("upload.DataLumosAuthenticator.wait_for_human_verification")
    @patch("publisher.DataLumosPublisher.DataLumosPublisher._publish_workspace")
    def test_run_crashes_when_google_sheet_configured_but_credentials_missing(
        self,
        mock_publish_workspace: MagicMock,
        mock_wait_for_human: MagicMock,
        mock_record_crash: MagicMock,
    ) -> None:
        """Test run calls record_crash when Google Sheet is configured but credentials file missing."""
        drpid = Storage.create_record("https://example.com/test")
        Storage.update_record(drpid, {"datalumos_id": "239181", "status": "uploaded"})
        mock_page = MagicMock()
        self.publisher._session.ensure_browser = MagicMock(return_value=mock_page)
        self.publisher._session.ensure_authenticated = MagicMock(return_value=None)
        self.publisher._session.close = MagicMock(return_value=None)
        mock_publish_workspace.return_value = (True, None)

        with patch.object(Args, "google_sheet_id", "sheet123"), patch.object(
            Args, "google_credentials", str(Path(tempfile.gettempdir()) / "nonexistent_creds.json")
        ), patch("publisher.DataLumosPublisher.record_error"):
            mock_record_crash.side_effect = RuntimeError("crash")
            with self.assertRaises(RuntimeError):
                self.publisher.run(drpid)
            mock_record_crash.assert_called_once()
            self.assertIn("credentials", mock_record_crash.call_args[0][0].lower())

    def test_session_close_no_browser(self) -> None:
        """Test _session.close() is safe when browser was never started."""
        self.publisher._session.close()
        self.assertIsNone(self.publisher._session._page)
        self.assertIsNone(self.publisher._session._playwright)
        self.assertFalse(self.publisher._session._authenticated)
