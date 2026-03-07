"""
Unit tests for utils.Errors helpers.
"""

import unittest
from unittest.mock import Mock, patch

from utils.Errors import record_crash, record_error, record_warning

# Module-level mocks for patch decorators (must be defined before use in decorators)
_mock_storage = Mock()
_mock_logger = Mock()


class TestRecordCrash(unittest.TestCase):
    """Test record_crash."""

    @patch("utils.Errors.Logger", Mock())
    def test_record_crash_logs_and_raises(self) -> None:
        """record_crash logs at exception level and raises RuntimeError."""
        with self.assertRaises(RuntimeError) as ctx:
            record_crash("fatal")
        self.assertEqual(str(ctx.exception), "fatal")


class TestRecordError(unittest.TestCase):
    """Test record_error."""

    @patch("utils.Errors.Storage", _mock_storage)
    @patch("utils.Errors.Logger", _mock_logger)
    def test_record_error_updates_storage(self) -> None:
        """record_error(update_storage=True) sets status and appends error."""
        record_error(123, "boom", update_storage=True)

        _mock_logger.error.assert_called_once_with("boom")
        _mock_storage.update_record.assert_called_once_with(123, {"status": "error"})
        _mock_storage.append_to_field.assert_called_once_with(123, "errors", "boom")

    @patch("utils.Errors.Logger")
    @patch("utils.Errors.Storage")
    def test_record_error_no_storage(self, mock_storage: Mock, mock_logger: Mock) -> None:
        """record_error(update_storage=False) only logs."""
        record_error(123, "nope", update_storage=False)

        mock_logger.error.assert_called_once_with("nope")
        mock_storage.update_record.assert_not_called()
        mock_storage.append_to_field.assert_not_called()

    @patch("utils.Errors.Logger", new_callable=Mock)
    @patch("utils.Errors.Storage", new_callable=Mock)
    def test_record_error_custom_status(self, mock_storage: Mock, mock_logger: Mock) -> None:
        """record_error uses status_value when provided."""
        record_error(99, "bad", update_storage=True, status_value="failed")

        mock_storage.update_record.assert_called_once_with(99, {"status": "failed"})


class TestRecordWarning(unittest.TestCase):
    """Test record_warning."""

    @patch("utils.Errors.Storage", _mock_storage)
    @patch("utils.Errors.Logger", _mock_logger)
    def test_record_warning_updates_storage(self) -> None:
        """record_warning(update_storage=True) logs and appends to warnings."""
        _mock_logger.reset_mock()
        _mock_storage.reset_mock()
        record_warning(123, "careful", update_storage=True)

        _mock_logger.warning.assert_called_once_with("careful")
        _mock_storage.append_to_field.assert_called_once_with(123, "warnings", "careful")

    @patch("utils.Errors.Storage", _mock_storage)
    @patch("utils.Errors.Logger", _mock_logger)
    def test_record_warning_no_storage(self) -> None:
        """record_warning(update_storage=False) only logs."""
        _mock_logger.reset_mock()
        _mock_storage.reset_mock()
        record_warning(123, "nope", update_storage=False)

        _mock_logger.warning.assert_called_once_with("nope")
        _mock_storage.append_to_field.assert_not_called()
