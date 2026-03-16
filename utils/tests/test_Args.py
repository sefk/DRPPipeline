"""
Unit tests for Args module.
"""

import json
import tempfile
import unittest
from pathlib import Path

from utils.Args import Args


class TestArgs(unittest.TestCase):
    """Test cases for Args class."""
    
    def setUp(self) -> None:
        """Reset Args state before each test."""
        import sys
        # Save and restore original argv to prevent test interference
        self._original_argv = sys.argv.copy()
        Args._parsed_args = {}
        Args._config = {}
        Args._initialized = False
    
    def tearDown(self) -> None:
        """Restore original argv after each test."""
        import sys
        sys.argv = self._original_argv
    
    def test_initialize_default(self) -> None:
        """Test Args initialization with default values."""
        import sys
        sys.argv = ["test", "noop"]
        Args.initialize()
        self.assertTrue(Args._initialized)
        self.assertEqual(Args.log_level, "INFO")
        self.assertEqual(Args.module, "noop")

    def test_sourcing_defaults(self) -> None:
        """Test sourcing-related defaults are present."""
        import sys
        sys.argv = ["test", "noop"]
        Args.initialize(config_file=Path("/tmp/nonexistent_drp_test_config.json"))
        self.assertEqual(Args.sourcing_url_column, "URL")
        self.assertIsNone(Args.num_rows)  # Default is None (unlimited)

    
    def test_initialize_with_config_file(self) -> None:
        """Test Args initialization with config file."""
        import sys
        sys.argv = ["test", "noop"]

        config_data = {"log_level": "DEBUG"}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            Args.initialize(config_file=config_path)
            self.assertEqual(Args.log_level, "DEBUG")
        finally:
            config_path.unlink()

    def test_command_line_override(self) -> None:
        """Test that command line args override config file values."""
        import sys

        config_data = {"log_level": "DEBUG"}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            original_argv = sys.argv.copy()
            sys.argv = ["test", "noop", "--config", str(config_path), "--log-level", "ERROR"]

            Args._initialized = False
            Args.initialize()
            self.assertEqual(Args.log_level, "ERROR")

            sys.argv = original_argv
        finally:
            config_path.unlink()

    def test_default_values(self) -> None:
        """Test that default values from argparse are correctly set."""
        import sys

        original_argv = sys.argv.copy()
        sys.argv = ["test", "noop"]

        Args._initialized = False
        Args.initialize()

        self.assertEqual(Args.log_level, "INFO")

        sys.argv = original_argv

    def test_config_file_not_found(self) -> None:
        """Test handling of missing config file (explicitly specified)."""
        import sys
        from io import StringIO

        original_argv = sys.argv.copy()
        original_stderr = sys.stderr

        stderr_capture = StringIO()
        sys.stderr = stderr_capture

        sys.argv = ["test", "noop", "--config", "nonexistent.json"]
        Args._initialized = False
        Args.initialize()

        self.assertTrue(Args._initialized)
        self.assertIn("not found", stderr_capture.getvalue())

        sys.argv = original_argv
        sys.stderr = original_stderr

    def test_config_file_default_not_found(self) -> None:
        """Test that default config file warning appears when file doesn't exist."""
        import sys
        from io import StringIO

        original_argv = sys.argv.copy()
        original_stderr = sys.stderr
        original_cwd = Path.cwd()

        stderr_capture = StringIO()
        sys.stderr = stderr_capture

        # Run from a temp dir where default config.json does not exist
        with tempfile.TemporaryDirectory() as tmpdir:
            import os
            os.chdir(tmpdir)
            sys.argv = ["test", "noop"]  # No --config specified, should use default
            Args._initialized = False
            Args.initialize()
            os.chdir(original_cwd)

        self.assertTrue(Args._initialized)
        # Should warn about default config file not found
        self.assertIn("not found", stderr_capture.getvalue())
        self.assertIn("config.json", stderr_capture.getvalue())

        sys.argv = original_argv
        sys.stderr = original_stderr

    def test_config_file_invalid_json(self) -> None:
        """Test handling of invalid JSON in config file."""
        import sys
        sys.argv = ["test", "noop"]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content {")
            config_path = Path(f.name)
        
        try:
            Args._initialized = False
            with self.assertRaises(ValueError) as cm:
                Args.initialize(config_file=config_path)
            self.assertIn("Invalid JSON", str(cm.exception))
        finally:
            config_path.unlink()
    
    def test_path_conversion(self) -> None:
        """Test that Path objects are converted to strings in config."""
        import sys

        config_data = {}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            sys.argv = ["test", "sourcing", "--config", str(config_path)]

            Args._initialized = False
            Args.initialize()

            if hasattr(Args, 'config_file'):
                config_file = Args.config_file
                self.assertIsInstance(config_file, str)
        finally:
            config_path.unlink()

    def test_attribute_not_found(self) -> None:
        """Test AttributeError when accessing non-existent attribute."""
        import sys
        sys.argv = ["test", "noop"]
        Args.initialize()

        with self.assertRaises(AttributeError) as cm:
            _ = Args.nonexistent_attribute
        self.assertIn("not found", str(cm.exception))

    def test_not_initialized_error(self) -> None:
        """Test that accessing Args before initialization raises error."""
        with self.assertRaises(RuntimeError) as cm:
            _ = Args.log_level
        self.assertIn("not been initialized", str(cm.exception))

    def test_get_args(self) -> None:
        """Test get_args method."""
        import sys
        sys.argv = ["test", "noop"]
        Args.initialize()
        args = Args.get_args()
        self.assertIsNotNone(args)
        self.assertIsInstance(args, dict)
        self.assertIn("module", args)

    def test_get_config(self) -> None:
        """Test get_config method."""
        import sys
        sys.argv = ["test", "noop"]
        Args.initialize()
        config = Args.get_config()
        self.assertIsInstance(config, dict)
        self.assertIn("log_level", config)
        self.assertIn("module", config)

    def test_idempotent_initialize(self) -> None:
        """Test that initialize can be called multiple times safely."""
        import sys
        sys.argv = ["test", "noop"]
        Args.initialize()
        first_config = Args.get_config().copy()

        Args.initialize()
        second_config = Args.get_config()

        self.assertEqual(first_config, second_config)

    def test_module_num_rows_db_path_storage(self) -> None:
        """Test module (required), num_rows, db_path, storage_implementation from CLI."""
        import sys
        sys.argv = ["test", "collectors", "--num-rows", "5", "--db-path", "C:\\data\\drp.db", "--storage", "StorageSQLLite"]
        Args._initialized = False
        Args.initialize()
        self.assertEqual(Args.module, "collectors")
        self.assertEqual(Args.num_rows, 5)
        self.assertEqual(str(Args.db_path), "C:\\data\\drp.db")
        self.assertEqual(Args.storage_implementation, "StorageSQLLite")

    def test_start_row_from_cli(self) -> None:
        """Test --start-row from CLI."""
        import sys
        sys.argv = ["test", "collector", "--start-row", "10"]
        Args._initialized = False
        Args.initialize()
        self.assertEqual(Args.start_row, 10)


if __name__ == "__main__":
    unittest.main()

