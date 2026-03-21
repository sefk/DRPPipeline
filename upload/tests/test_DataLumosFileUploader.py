"""
Unit tests for DataLumosFileUploader.
"""

import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

from utils.Logger import Logger

from upload.DataLumosFileUploader import DataLumosFileUploader


class TestDataLumosFileUploader(unittest.TestCase):
    """Test cases for DataLumosFileUploader."""

    @classmethod
    def setUpClass(cls) -> None:
        """Initialize Logger once for all tests."""
        Logger.initialize(log_level="WARNING")

    def test_init(self) -> None:
        """Test file uploader initialization."""
        mock_page = MagicMock()
        uploader = DataLumosFileUploader(mock_page, timeout=5000)
        self.assertEqual(uploader._page, mock_page)
        self.assertEqual(uploader._timeout, 5000)

    def test_get_file_paths_returns_files(self) -> None:
        """Test get_file_paths returns only files, not subdirs."""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "a.txt").write_text("a")
            (Path(tmp) / "b.csv").write_text("b")
            Path(tmp).joinpath("sub").mkdir()
            (Path(tmp) / "sub" / "c.txt").write_text("c")
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            paths = uploader.get_file_paths(tmp)
            names = {p.name for p in paths}
            self.assertEqual(names, {"a.txt", "b.csv"})

    def test_get_file_paths_empty_folder(self) -> None:
        """Test get_file_paths returns empty list for empty folder."""
        with tempfile.TemporaryDirectory() as tmp:
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            paths = uploader.get_file_paths(tmp)
            self.assertEqual(paths, [])

    def test_upload_files_empty_folder_returns_without_error(self) -> None:
        """Test upload_files with empty folder returns without opening modal or raising."""
        with tempfile.TemporaryDirectory() as tmp:
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            uploader.upload_files(tmp)
            mock_page.locator.assert_not_called()

    def test_get_file_paths_missing_folder_raises(self) -> None:
        """Test get_file_paths raises FileNotFoundError for missing path."""
        mock_page = MagicMock()
        uploader = DataLumosFileUploader(mock_page)
        with self.assertRaises(FileNotFoundError):
            uploader.get_file_paths("/nonexistent/folder")

    def test_get_file_paths_not_directory_raises(self) -> None:
        """Test get_file_paths raises NotADirectoryError for file path."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name
        try:
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            with self.assertRaises(NotADirectoryError):
                uploader.get_file_paths(path)
        finally:
            Path(path).unlink(missing_ok=True)

    def test_folder_has_subfolders_true_when_subdir_exists(self) -> None:
        """Test _folder_has_subfolders returns True when folder has subdirs."""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "sub").mkdir()
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            self.assertTrue(uploader._folder_has_subfolders(tmp))

    def test_folder_has_subfolders_false_when_only_files(self) -> None:
        """Test _folder_has_subfolders returns False when folder has only files."""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "a.txt").write_text("a")
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            self.assertFalse(uploader._folder_has_subfolders(tmp))

    def test_folder_has_subfolders_false_when_empty(self) -> None:
        """Test _folder_has_subfolders returns False for empty folder."""
        with tempfile.TemporaryDirectory() as tmp:
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            self.assertFalse(uploader._folder_has_subfolders(tmp))

    def test_zip_folder_contents_creates_valid_zip_with_files_and_subdirs(self) -> None:
        """Test _zip_folder_contents creates a zip containing all contents."""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "root.txt").write_text("root")
            sub = Path(tmp) / "sub"
            sub.mkdir()
            (sub / "nested.txt").write_text("nested")
            mock_page = MagicMock()
            uploader = DataLumosFileUploader(mock_page)
            zip_path = uploader._zip_folder_contents(tmp)
            try:
                self.assertTrue(zip_path.exists())
                with zipfile.ZipFile(zip_path, "r") as zf:
                    names = set(zf.namelist())
                self.assertIn("root.txt", names)
                self.assertIn("sub/nested.txt", names)
            finally:
                zip_path.unlink(missing_ok=True)

    def test_upload_files_with_subfolders_uses_import_from_zip(self) -> None:
        """Test upload_files with subfolders clicks Import From Zip, not Upload Files."""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "sub").mkdir()
            (Path(tmp) / "sub" / "f.txt").write_text("x")

            def locator_side_effect(selector: str) -> MagicMock:
                loc = MagicMock()
                if selector == "#busy":
                    loc.count.return_value = 0
                else:
                    loc.wait_for = MagicMock()
                    loc.click = MagicMock()
                    loc.nth = MagicMock(return_value=MagicMock())
                    gbt = MagicMock()
                    gbt.count.return_value = 1
                    loc.get_by_text = MagicMock(return_value=gbt)
                    loc.inner_text = MagicMock(return_value="")
                return loc

            mock_page = MagicMock()
            mock_page.locator.side_effect = locator_side_effect
            mock_page.evaluate.return_value = "pw-datalumos-file-input"

            uploader = DataLumosFileUploader(mock_page)
            uploader.upload_files(tmp)

            calls = [c[0][0] for c in mock_page.locator.call_args_list]
            import_zip_calls = [c for c in calls if "Import From Zip" in c]
            upload_calls = [c for c in calls if "btn-primary" in str(c)]
            self.assertGreater(len(import_zip_calls), 0, "Should use Import From Zip")
            self.assertEqual(len(upload_calls), 0, "Should not use Upload Files")
