"""
Unit tests for interactive_collector.downloads_watcher.
"""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from interactive_collector.downloads_watcher import (
    _is_complete_file,
    _unique_dest_name,
    get_downloads_folder,
    is_watching,
    start_watching,
    stop_watching,
)


class TestGetDownloadsFolder(unittest.TestCase):
    """Tests for get_downloads_folder."""

    @patch.dict(os.environ, {"USERPROFILE": "C:\\Users\\Test"}, clear=False)
    def test_nt_uses_userprofile_downloads(self) -> None:
        """On Windows, uses USERPROFILE/Downloads."""
        with patch("interactive_collector.downloads_watcher.os.name", "nt"):
            p = get_downloads_folder()
            self.assertEqual(str(p), "C:\\Users\\Test\\Downloads")

    @unittest.skipIf(os.name != "posix", "Path.home()/Downloads behavior is platform-specific; run on posix to test")
    def test_posix_uses_home_downloads(self) -> None:
        """On non-Windows, uses Path.home()/Downloads."""
        home = Path.home()
        with patch("interactive_collector.downloads_watcher.os.name", "posix"):
            p = get_downloads_folder()
            self.assertEqual(p, home / "Downloads")


class TestIsCompleteFile(unittest.TestCase):
    """Tests for _is_complete_file."""

    def test_complete_file_returns_true(self) -> None:
        """Normal file returns True."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            p = Path(f.name)
        try:
            self.assertTrue(_is_complete_file(p))
        finally:
            p.unlink(missing_ok=True)

    def test_crdownload_returns_false(self) -> None:
        """In-progress Chrome download returns False."""
        with tempfile.NamedTemporaryFile(suffix=".crdownload", delete=False) as f:
            p = Path(f.name)
        try:
            self.assertFalse(_is_complete_file(p))
        finally:
            p.unlink(missing_ok=True)

    def test_directory_returns_false(self) -> None:
        """Directory returns False."""
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(_is_complete_file(Path(tmp)))


class TestUniqueDestName(unittest.TestCase):
    """Tests for _unique_dest_name."""

    def test_new_name_unchanged(self) -> None:
        """Non-existing name returned as-is."""
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            p = _unique_dest_name(d, "file.csv")
            self.assertEqual(p, d / "file.csv")

    def test_existing_gets_suffix(self) -> None:
        """Existing file gets _1, _2 suffix."""
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "file.csv").write_text("a")
            p = _unique_dest_name(d, "file.csv")
            self.assertEqual(p, d / "file_1.csv")


class TestStartStopWatching(unittest.TestCase):
    """Tests for start_watching, stop_watching, is_watching."""

    def tearDown(self) -> None:
        """Ensure watcher is stopped after each test."""
        stop_watching()

    def test_is_watching_false_initially(self) -> None:
        """Initially not watching."""
        self.assertFalse(is_watching())

    def test_start_watching_requires_watchdog(self) -> None:
        """When watchdog not installed, start_watching returns (False, message)."""
        builtins_import = __import__

        def mock_import(name, *args, **kwargs):
            if name == "watchdog.observers":
                raise ImportError("No module named 'watchdog.observers'")
            return builtins_import(name, *args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            with patch("builtins.__import__", side_effect=mock_import):
                ok, msg = start_watching(1, out, lambda *a: None)
            self.assertFalse(ok)
            self.assertIn("watchdog", msg.lower())

    def test_start_then_stop_then_is_watching(self) -> None:
        """Start watching (when possible), then stop; is_watching reflects state."""
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            downloads = get_downloads_folder()
            if not downloads.is_dir():
                self.skipTest("No Downloads folder in environment")
            ok, msg = start_watching(1, out, lambda *a: None)
            if not ok:
                self.skipTest(f"Could not start watcher: {msg}")
            self.assertTrue(is_watching())
            stop_ok, stop_msg = stop_watching()
            self.assertTrue(stop_ok)
            self.assertFalse(is_watching())

    def test_start_when_already_active_returns_false(self) -> None:
        """Starting when already watching returns (False, message)."""
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            downloads = get_downloads_folder()
            if not downloads.is_dir():
                self.skipTest("No Downloads folder")
            ok1, _ = start_watching(1, out, lambda *a: None)
            if not ok1:
                self.skipTest("Could not start watcher")
            ok2, msg2 = start_watching(2, out, lambda *a: None)
            self.assertFalse(ok2)
            self.assertIn("already", msg2.lower())
            stop_watching()
