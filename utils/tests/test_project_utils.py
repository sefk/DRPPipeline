"""
Unit tests for project_utils module.
"""

import unittest

from utils.project_utils import get_field


class TestProjectUtils(unittest.TestCase):
    """Test cases for project_utils module."""

    def test_get_field_trimmed(self) -> None:
        """Test get_field returns trimmed value."""
        project = {"datalumos_id": "  12345  ", "title": "  My Title  "}
        self.assertEqual(get_field(project, "datalumos_id"), "12345")
        self.assertEqual(get_field(project, "title"), "My Title")

    def test_get_field_missing(self) -> None:
        """Test get_field returns empty string for missing/None."""
        project = {"present": "x", "missing": None}
        self.assertEqual(get_field(project, "missing"), "")
        self.assertEqual(get_field(project, "nonexistent"), "")
