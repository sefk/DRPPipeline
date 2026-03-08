"""
Unit tests for interactive_collector.pdf_utils.
"""

import unittest
from pathlib import Path
from unittest.mock import MagicMock

from interactive_collector.pdf_utils import page_title_or_h1, unique_pdf_basename


class TestPageTitleOrH1(unittest.TestCase):
    """Tests for page_title_or_h1."""

    def test_returns_page_title(self) -> None:
        """When page.title() returns non-empty, use it."""
        page = MagicMock()
        page.title.return_value = "  My Dataset  "
        self.assertEqual(page_title_or_h1(page), "My Dataset")

    def test_returns_h1_when_title_empty(self) -> None:
        """When title is empty, use first h1 text."""
        page = MagicMock()
        page.title.return_value = ""
        page.evaluate.return_value = ""
        locator = MagicMock()
        locator.first.text_content.return_value = "  Dataset Page  "
        page.locator.return_value = locator
        self.assertEqual(page_title_or_h1(page), "Dataset Page")

    def test_returns_url_path_segment_when_title_and_h1_empty(self) -> None:
        """When title and h1 empty, use URL path segment."""
        page = MagicMock()
        page.title.return_value = ""
        page.evaluate.return_value = ""
        page.locator.return_value.first.text_content.side_effect = Exception("no h1")
        self.assertEqual(
            page_title_or_h1(page, "https://example.com/datasets/my-dataset"),
            "my-dataset",
        )

    def test_returns_netloc_when_no_path(self) -> None:
        """When URL has no path segment, use hostname."""
        page = MagicMock()
        page.title.return_value = ""
        page.evaluate.return_value = ""
        page.locator.return_value.first.text_content.side_effect = Exception("no h1")
        self.assertEqual(
            page_title_or_h1(page, "https://data.cdc.gov"),
            "data",
        )

    def test_returns_empty_on_exception(self) -> None:
        """When all fail, return empty string."""
        page = MagicMock()
        page.title.side_effect = Exception("error")
        self.assertEqual(page_title_or_h1(page, ""), "")


class TestUniquePdfBasename(unittest.TestCase):
    """Tests for unique_pdf_basename."""

    def test_first_use_no_suffix(self) -> None:
        """First use of a base returns base.pdf."""
        used: dict = {}
        result = unique_pdf_basename("My Page", used)
        self.assertTrue(result.endswith(".pdf"))
        self.assertIn(result, ("My_Page.pdf", "My Page.pdf"))
        key = result.rsplit(".", 1)[0].lower().replace(" ", "_")
        self.assertEqual(used.get(key, used.get("my_page")), 1)

    def test_duplicate_gets_suffix(self) -> None:
        """Repeated base gets _1, _2, etc."""
        used: dict = {}
        self.assertEqual(unique_pdf_basename("Page", used), "Page.pdf")
        self.assertEqual(unique_pdf_basename("Page", used), "Page_1.pdf")
        self.assertEqual(unique_pdf_basename("Page", used), "Page_2.pdf")

    def test_respects_folder_path_existing_file(self) -> None:
        """When folder_path has existing file, skip to next index."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "Doc.pdf").write_text("x")
            used: dict = {}
            name = unique_pdf_basename("Doc", used, folder_path=folder)
            self.assertEqual(name, "Doc_1.pdf")

    def test_empty_base_becomes_page_or_untitled(self) -> None:
        """Empty base is sanitized to a default (e.g. Untitled or page)."""
        used: dict = {}
        result = unique_pdf_basename("", used)
        self.assertTrue(result.endswith(".pdf"))
        self.assertIn(result, ("page.pdf", "Untitled.pdf"))
