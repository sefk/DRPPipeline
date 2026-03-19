"""
Unit tests for Orchestrator.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from utils.Args import Args
from utils.Logger import Logger

from orchestration.Orchestrator import Orchestrator, _stop_requested


class TestOrchestrator(unittest.TestCase):
    """Test cases for Orchestrator."""

    def setUp(self) -> None:
        """Set up test environment before each test."""
        self._original_argv = sys.argv.copy()
        sys.argv = ["test", "noop"]
        Args.initialize()
        Logger.initialize(log_level="WARNING")

    def tearDown(self) -> None:
        """Restore argv after each test."""
        sys.argv = self._original_argv

    def test_stop_requested_false_when_no_stop_file(self) -> None:
        """_stop_requested() returns False when Args has no stop_file or file does not exist."""
        self.assertFalse(_stop_requested())
        with patch.object(Args, "stop_file", str(Path("/nonexistent/drp_stop"))):
            self.assertFalse(_stop_requested())

    def test_stop_requested_true_when_file_exists(self) -> None:
        """_stop_requested() returns True when Args.stop_file is set and file exists."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".stop") as f:
            stop_path = f.name
        try:
            with patch.object(Args, "stop_file", stop_path):
                self.assertTrue(_stop_requested())
        finally:
            Path(stop_path).unlink(missing_ok=True)

    def test_run_unknown_module_raises(self) -> None:
        """Test run() with unknown module raises ValueError with valid modules listed."""
        with self.assertRaises(ValueError) as cm:
            Orchestrator.run("unknown")
        self.assertIn("unknown", str(cm.exception))
        self.assertIn("noop", str(cm.exception))
        self.assertIn("sourcing", str(cm.exception))
        self.assertIn("collector", str(cm.exception))
        self.assertIn("cleanup_inprogress", str(cm.exception))
        self.assertIn("interactive_collector", str(cm.exception))

    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_sourcing_calls_sourcing_run(
        self, mock_storage_cls: MagicMock, mock_find_class: MagicMock
    ) -> None:
        """Test run("sourcing") instantiates Sourcing and calls run(-1)."""
        mock_storage = MagicMock()
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.get_instance.return_value = mock_storage
        mock_sourcing_instance = MagicMock()
        mock_sourcing_cls = MagicMock(return_value=mock_sourcing_instance)
        mock_find_class.return_value = mock_sourcing_cls

        with patch("orchestration.Orchestrator.Storage", mock_storage_cls):
            Orchestrator.run("sourcing")

        mock_storage_cls.initialize.assert_called_once()
        mock_find_class.assert_called_once_with("Sourcing")
        mock_sourcing_cls.assert_called_once()
        mock_sourcing_instance.run.assert_called_once_with(-1)

    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_cleanup_inprogress_calls_run_minus_one(
        self, mock_storage_cls: MagicMock, mock_find_class: MagicMock
    ) -> None:
        """Test run('cleanup_inprogress') instantiates CleanupInProgress and calls run(-1)."""
        mock_storage = MagicMock()
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.get_instance.return_value = mock_storage
        mock_cleanup_instance = MagicMock()
        mock_cleanup_cls = MagicMock(return_value=mock_cleanup_instance)
        mock_find_class.return_value = mock_cleanup_cls

        with patch("orchestration.Orchestrator.Storage", mock_storage_cls):
            Orchestrator.run("cleanup_inprogress")

        mock_find_class.assert_called_once_with("CleanupInProgress")
        mock_cleanup_cls.assert_called_once()
        mock_cleanup_instance.run.assert_called_once_with(-1)

    @patch("orchestration.Orchestrator.record_error")
    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_collectors_appends_error_when_run_raises(
        self,
        mock_storage_cls: MagicMock,
        mock_find_class: MagicMock,
        mock_record_error: MagicMock,
    ) -> None:
        """Test run("catalog_collector") calls record_error when run() raises, and continues."""
        sys.argv = ["test", "noop"]
        Args._initialized = False
        Args.initialize(config_file=Path("/tmp/nonexistent_drp_test_config.json"))

        mock_storage = MagicMock()
        mock_storage.list_eligible_projects.return_value = [
            {"DRPID": 1, "source_url": "https://example.com"}
        ]
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.list_eligible_projects = mock_storage.list_eligible_projects

        mock_collector_instance = MagicMock()
        mock_collector_instance.run.side_effect = NotImplementedError(
            "collector run not yet implemented"
        )
        mock_collector_cls = MagicMock(return_value=mock_collector_instance)
        mock_find_class.return_value = mock_collector_cls

        with patch("orchestration.Orchestrator.Storage", mock_storage_cls):
            Orchestrator.run("catalog_collector")

        mock_storage_cls.initialize.assert_called_once()
        mock_storage.list_eligible_projects.assert_called_once_with("sourced", None, None, None)
        mock_find_class.assert_called_once_with("CatalogDataCollector")
        mock_collector_cls.assert_called_once()
        mock_collector_instance.run.assert_called_once_with(1)
        mock_record_error.assert_called_once()
        args = mock_record_error.call_args[0]
        self.assertEqual(args[0], 1)
        self.assertIn("not yet implemented", args[1])

    @patch("interactive_collector.app.app.run")
    def test_run_interactive_collector_starts_app(self, mock_app_run: MagicMock) -> None:
        """Test run('interactive_collector') starts Flask app (uses Args like rest of pipeline)."""
        Orchestrator.run("interactive_collector")
        mock_app_run.assert_called_once()
        call_kw = mock_app_run.call_args[1]
        self.assertEqual(call_kw.get("host"), "127.0.0.1")
        self.assertEqual(call_kw.get("port"), 5000)
        self.assertFalse(call_kw.get("debug"))

    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_noop_returns_early(self, mock_storage_cls: MagicMock, mock_find_class: MagicMock) -> None:
        """Test run('noop') never loads a module class and returns after logging."""
        with patch("orchestration.Orchestrator.Storage", mock_storage_cls):
            Orchestrator.run("noop")
        mock_storage_cls.initialize.assert_called_once()
        mock_find_class.assert_not_called()

    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_sourcing_with_delete_all_db_entries_clears_storage(
        self, mock_storage_cls: MagicMock, mock_find_class: MagicMock
    ) -> None:
        """Test run with delete_all_db_entries calls Storage.clear_all_records."""
        mock_storage = MagicMock()
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.get_instance.return_value = mock_storage
        mock_sourcing_instance = MagicMock()
        mock_find_class.return_value = MagicMock(return_value=mock_sourcing_instance)
        with patch("orchestration.Orchestrator.Storage", mock_storage_cls), \
             patch.object(Args, "delete_all_db_entries", True), \
             patch("orchestration.Orchestrator.Logger") as mock_logger:
            Orchestrator.run("sourcing")
        mock_storage_cls.clear_all_records.assert_called_once()
        mock_sourcing_instance.run.assert_called_once_with(-1)

    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_publisher_lists_uploaded_not_found_no_links(
        self, mock_storage_cls: MagicMock, mock_find_class: MagicMock
    ) -> None:
        """Test run('publisher') calls list_eligible_projects for uploaded, not_found, no_links and dedupes."""
        mock_storage = MagicMock()
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.get_instance.return_value = mock_storage
        # Return overlapping DRPIDs to test dedupe
        mock_storage_cls.list_eligible_projects.side_effect = [
            [{"DRPID": 2, "source_url": "https://a.com"}],
            [{"DRPID": 2, "source_url": "https://a.com"}],
            [{"DRPID": 3, "source_url": "https://b.com"}],
        ]
        mock_pub_instance = MagicMock()
        mock_find_class.return_value = MagicMock(return_value=mock_pub_instance)
        with patch("orchestration.Orchestrator.Storage", mock_storage_cls):
            Orchestrator.run("publisher")
        self.assertEqual(mock_storage_cls.list_eligible_projects.call_count, 3)
        mock_storage_cls.list_eligible_projects.assert_any_call("uploaded", None, None, None)
        mock_storage_cls.list_eligible_projects.assert_any_call("not_found", None, None, None)
        mock_storage_cls.list_eligible_projects.assert_any_call("no_links", None, None, None)
        self.assertEqual(mock_pub_instance.run.call_count, 2)  # DRPID 2 and 3

    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_publisher_respects_num_rows(
        self, mock_storage_cls: MagicMock, mock_find_class: MagicMock
    ) -> None:
        """Test run('publisher') limits projects to num_rows."""
        mock_storage = MagicMock()
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.get_instance.return_value = mock_storage
        mock_storage_cls.list_eligible_projects.side_effect = [
            [{"DRPID": i, "source_url": f"https://{i}.com"} for i in range(1, 6)],
            [],
            [],
        ]
        mock_pub_instance = MagicMock()
        mock_find_class.return_value = MagicMock(return_value=mock_pub_instance)
        with patch("orchestration.Orchestrator.Storage", mock_storage_cls), \
             patch.object(Args, "num_rows", 2):
            Orchestrator.run("publisher")
        self.assertEqual(mock_pub_instance.run.call_count, 2)

    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_prereq_module_passes_start_row_start_drpid(
        self, mock_storage_cls: MagicMock, mock_find_class: MagicMock
    ) -> None:
        """Test run with prereq passes start_row and start_drpid to list_eligible_projects."""
        mock_storage = MagicMock()
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.get_instance.return_value = mock_storage
        mock_storage_cls.list_eligible_projects.return_value = []
        mock_find_class.return_value = MagicMock(return_value=MagicMock())
        with patch("orchestration.Orchestrator.Storage", mock_storage_cls), \
             patch.object(Args, "start_row", 5), \
             patch.object(Args, "start_drpid", 100):
            Orchestrator.run("catalog_collector")
        mock_storage_cls.list_eligible_projects.assert_called_once_with("sourced", None, 5, 100)

    @patch("orchestration.Orchestrator._stop_requested")
    @patch("orchestration.Orchestrator._find_module_class")
    @patch("storage.Storage")
    def test_run_stop_requested_during_loop_exits(
        self, mock_storage_cls: MagicMock, mock_find_class: MagicMock, mock_stop: MagicMock
    ) -> None:
        """Test run stops when _stop_requested returns True during single-threaded loop."""
        mock_storage = MagicMock()
        mock_storage_cls.initialize.return_value = mock_storage
        mock_storage_cls.get_instance.return_value = mock_storage
        mock_storage_cls.list_eligible_projects.return_value = [
            {"DRPID": 1, "source_url": "https://one.com"},
            {"DRPID": 2, "source_url": "https://two.com"},
        ]
        run_count = 0
        def stop_after_first(*args, **kwargs):
            nonlocal run_count
            run_count += 1
            return run_count > 1
        mock_stop.side_effect = stop_after_first
        mock_instance = MagicMock()
        mock_find_class.return_value = MagicMock(return_value=mock_instance)
        with patch("orchestration.Orchestrator.Storage", mock_storage_cls):
            Orchestrator.run("catalog_collector")
        mock_instance.run.assert_called_once_with(1)

    def test_find_module_class_returns_sourcing(self) -> None:
        """Test _find_module_class finds Sourcing in the project."""
        from orchestration.Orchestrator import _find_module_class
        cls = _find_module_class("Sourcing")
        self.assertEqual(cls.__name__, "Sourcing")
