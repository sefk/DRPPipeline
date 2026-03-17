"""
Unit tests for SpreadsheetCandidateFetcher.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from utils.Args import Args
from utils.Logger import Logger

from sourcing.SpreadsheetCandidateFetcher import SpreadsheetCandidateFetcher


def _csv_candidates() -> str:
    """Sample CSV matching Data_Inventories layout: URL, Claimed, Download Location. Uses catalog.data.gov so _row_passes_filter passes."""
    return (
        "Admin Notes,Claimed (add your name),URL,Download Location\r\n"
        ",,https://catalog.data.gov/dataset/a,\r\n"
        ",alice,https://catalog.data.gov/dataset/b,\r\n"
        ",,https://catalog.data.gov/dataset/c,/path\r\n"
        ",,https://catalog.data.gov/dataset/d,\r\n"
        ",,,\r\n"
    )


class TestSpreadsheetCandidateFetcher(unittest.TestCase):
    """Test cases for SpreadsheetCandidateFetcher."""

    def setUp(self) -> None:
        """Set up test environment before each test."""
        self._original_argv = sys.argv.copy()
        self._creds_file = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self._creds_file.write(b"{}")
        self._creds_file.close()
        sys.argv = ["test", "sourcing"]
        Args.initialize()
        Args._config["google_sheet_id"] = "test_sheet_id"
        Args._config["google_sheet_name"] = "CDC"
        Args._config["google_credentials"] = self._creds_file.name
        Args._config["sourcing_mode"] = "unclaimed"
        Logger.initialize(log_level="WARNING")
        self.fetcher = SpreadsheetCandidateFetcher()

    def tearDown(self) -> None:
        """Clean up after each test."""
        sys.argv = self._original_argv
        try:
            Path(self._creds_file.name).unlink(missing_ok=True)
        except Exception:
            pass

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_returns_filtered_urls(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test get_candidate_urls fetches CSV, filters rows, returns list of url/office/agency dicts."""
        mock_fetch.return_value = _csv_candidates()
        rows, skipped = self.fetcher.get_candidate_urls()
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["url"], "https://catalog.data.gov/dataset/a")
        self.assertEqual(rows[1]["url"], "https://catalog.data.gov/dataset/d")
        self.assertEqual(rows[0]["office"], "")
        self.assertEqual(rows[0]["agency"], "")
        mock_fetch.assert_called_once()

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_missing_url_column_raises(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test get_candidate_urls raises ValueError when URL column missing."""
        mock_fetch.return_value = "ColA,ColB\r\n1,2\r\n"
        with self.assertRaises(ValueError) as cm:
            self.fetcher.get_candidate_urls()
        self.assertIn("missing required URL column", str(cm.exception))
        self.assertIn("URL", str(cm.exception))

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_missing_filter_column_raises(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test get_candidate_urls raises ValueError when filter column missing."""
        csv_no_dl = (
            "Admin Notes,Claimed (add your name),URL\r\n"
            ",,https://example.com/x\r\n"
            ",alice,https://example.com/y\r\n"
        )
        mock_fetch.return_value = csv_no_dl
        with self.assertRaises(ValueError) as cm:
            self.fetcher.get_candidate_urls()
        self.assertIn("missing required filter columns", str(cm.exception))
        self.assertIn("Download Location", str(cm.exception))

    def test_row_passes_filter_both_empty(self) -> None:
        """Test _row_passes_filter returns True when Claimed and Download Location empty and URL matches sourcing_url_prefix."""
        row = {"Claimed (add your name)": "", "Download Location": "", "URL": "https://catalog.data.gov/dataset/x"}
        self.assertTrue(self.fetcher._row_passes_filter(row))

    def test_row_passes_filter_claimed_filled(self) -> None:
        """Test _row_passes_filter returns False when Claimed non-empty."""
        row = {"Claimed (add your name)": "alice", "Download Location": "", "URL": "https://catalog.data.gov/dataset/x"}
        self.assertFalse(self.fetcher._row_passes_filter(row))

    def test_row_passes_filter_download_location_filled(self) -> None:
        """Test _row_passes_filter returns False when Download Location non-empty."""
        row = {"Claimed (add your name)": "", "Download Location": "/path", "URL": "https://catalog.data.gov/dataset/x"}
        self.assertFalse(self.fetcher._row_passes_filter(row))

    def test_row_passes_filter_missing_url_treated_empty_fails(self) -> None:
        """Test _row_passes_filter returns False when URL is missing (empty); requires matching sourcing_url_prefix."""
        # Missing URL yields "".startswith(prefix) -> False when prefix is set.
        self.assertFalse(self.fetcher._row_passes_filter({}))

    def test_row_passes_filter_custom_prefix(self) -> None:
        """Test _row_passes_filter respects sourcing_url_prefix config."""
        Args._config["sourcing_url_prefix"] = "https://data.cms.gov/"
        row = {"Claimed (add your name)": "", "Download Location": "", "URL": "https://data.cms.gov/dataset/x"}
        self.assertTrue(self.fetcher._row_passes_filter(row))
        row_wrong = {"Claimed (add your name)": "", "Download Location": "", "URL": "https://catalog.data.gov/dataset/x"}
        self.assertFalse(self.fetcher._row_passes_filter(row_wrong))
        Args._config["sourcing_url_prefix"] = "https://catalog.data.gov/"

    def test_row_passes_filter_empty_prefix_allows_any_url(self) -> None:
        """Test _row_passes_filter with empty sourcing_url_prefix accepts any URL."""
        Args._config["sourcing_url_prefix"] = ""
        row = {"Claimed (add your name)": "", "Download Location": "", "URL": "https://example.com/dataset"}
        self.assertTrue(self.fetcher._row_passes_filter(row))
        Args._config["sourcing_url_prefix"] = "https://catalog.data.gov/"

    def test_get_candidate_urls_requires_google_sheet_id(self) -> None:
        """Test get_candidate_urls raises ValueError when google_sheet_id is missing."""
        Args._config["google_sheet_id"] = ""
        with self.assertRaises(ValueError) as cm:
            self.fetcher.get_candidate_urls()
        self.assertIn("google_sheet_id", str(cm.exception))

    def test_get_candidate_urls_requires_google_sheet_name(self) -> None:
        """Test get_candidate_urls raises ValueError when google_sheet_name is missing."""
        Args._config["google_sheet_name"] = ""
        with self.assertRaises(ValueError) as cm:
            self.fetcher.get_candidate_urls()
        self.assertIn("google_sheet_name", str(cm.exception))

    def test_get_candidate_urls_requires_google_credentials(self) -> None:
        """Test get_candidate_urls raises ValueError when google_credentials is missing."""
        Args._config["google_credentials"] = None
        with self.assertRaises(ValueError) as cm:
            self.fetcher.get_candidate_urls()
        self.assertIn("google_credentials", str(cm.exception))

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value=None)
    def test_get_candidate_urls_raises_when_sheet_not_found(self, _mock_gid: object) -> None:
        """Test get_candidate_urls raises ValueError when sheet name can't be resolved."""
        with self.assertRaises(ValueError) as cm:
            self.fetcher.get_candidate_urls()
        self.assertIn("not found", str(cm.exception))

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_respects_limit(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test get_candidate_urls stops at limit (from caller)."""
        csv_with_many = (
            "Admin Notes,Claimed (add your name),URL,Download Location\r\n"
            ",,https://catalog.data.gov/dataset/a,\r\n"
            ",alice,https://catalog.data.gov/dataset/b,\r\n"
            ",,https://catalog.data.gov/dataset/c,/path\r\n"
            ",,https://catalog.data.gov/dataset/d,\r\n"
            ",,https://catalog.data.gov/dataset/e,\r\n"
            ",,https://catalog.data.gov/dataset/f,\r\n"
        )
        mock_fetch.return_value = csv_with_many

        rows, _ = self.fetcher.get_candidate_urls(limit=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["url"], "https://catalog.data.gov/dataset/a")
        self.assertEqual(rows[1]["url"], "https://catalog.data.gov/dataset/d")

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_unlimited_when_limit_none(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test get_candidate_urls returns all URLs when limit is None."""
        csv_with_many = (
            "Admin Notes,Claimed (add your name),URL,Download Location\r\n"
            ",,https://catalog.data.gov/dataset/a,\r\n"
            ",,https://catalog.data.gov/dataset/d,\r\n"
            ",,https://catalog.data.gov/dataset/e,\r\n"
        )
        mock_fetch.return_value = csv_with_many

        rows, _ = self.fetcher.get_candidate_urls(limit=None)
        self.assertEqual(len(rows), 3)
        self.assertEqual([r["url"] for r in rows], ["https://catalog.data.gov/dataset/a", "https://catalog.data.gov/dataset/d", "https://catalog.data.gov/dataset/e"])

    def test_row_passes_filter_mode_completed_download_location_filled(self) -> None:
        """Test _row_passes_filter returns True in 'completed' mode when Download Location non-empty."""
        Args._config["sourcing_mode"] = "completed"
        Args._config["sourcing_url_prefix"] = ""
        row = {"Claimed (add your name)": "alice", "Download Location": "/some/path", "URL": "https://example.com/x"}
        self.assertTrue(self.fetcher._row_passes_filter(row))

    def test_row_passes_filter_mode_completed_empty_download_location_excluded(self) -> None:
        """Test _row_passes_filter returns False in 'completed' mode when Download Location is empty."""
        Args._config["sourcing_mode"] = "completed"
        Args._config["sourcing_url_prefix"] = ""
        row = {"Claimed (add your name)": "", "Download Location": "", "URL": "https://example.com/x"}
        self.assertFalse(self.fetcher._row_passes_filter(row))

    def test_row_passes_filter_mode_all_includes_claimed_rows(self) -> None:
        """Test _row_passes_filter returns True in 'all' mode regardless of Claimed/Download Location."""
        Args._config["sourcing_mode"] = "all"
        Args._config["sourcing_url_prefix"] = ""
        for claimed, dl in [("alice", "/path"), ("", "/path"), ("alice", ""), ("", "")]:
            row = {"Claimed (add your name)": claimed, "Download Location": dl, "URL": "https://example.com/x"}
            self.assertTrue(self.fetcher._row_passes_filter(row), f"Expected True for claimed={claimed!r}, dl={dl!r}")

    def test_row_passes_filter_mode_all_still_applies_url_prefix(self) -> None:
        """Test _row_passes_filter in 'all' mode still filters by sourcing_url_prefix."""
        Args._config["sourcing_mode"] = "all"
        Args._config["sourcing_url_prefix"] = "https://catalog.data.gov/"
        row_match = {"Claimed (add your name)": "alice", "Download Location": "/path", "URL": "https://catalog.data.gov/dataset/x"}
        row_no_match = {"Claimed (add your name)": "alice", "Download Location": "/path", "URL": "https://other.gov/dataset/x"}
        self.assertTrue(self.fetcher._row_passes_filter(row_match))
        self.assertFalse(self.fetcher._row_passes_filter(row_no_match))

    def test_row_passes_filter_unknown_mode_defaults_to_unclaimed(self) -> None:
        """Test _row_passes_filter with unknown mode falls back to 'unclaimed' behavior."""
        Args._config["sourcing_mode"] = "bogus"
        Args._config["sourcing_url_prefix"] = ""
        row_unclaimed = {"Claimed (add your name)": "", "Download Location": "", "URL": "https://example.com/x"}
        row_claimed = {"Claimed (add your name)": "alice", "Download Location": "", "URL": "https://example.com/x"}
        self.assertTrue(self.fetcher._row_passes_filter(row_unclaimed))
        self.assertFalse(self.fetcher._row_passes_filter(row_claimed))

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_mode_completed_returns_completed_rows(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test get_candidate_urls in 'completed' mode returns only rows with Download Location filled."""
        Args._config["sourcing_mode"] = "completed"
        Args._config["sourcing_url_prefix"] = ""
        mock_fetch.return_value = _csv_candidates()
        rows, _ = self.fetcher.get_candidate_urls()
        # Only dataset/c has Download Location filled
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["url"], "https://catalog.data.gov/dataset/c")

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_mode_all_returns_all_rows_with_url(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test get_candidate_urls in 'all' mode returns all rows that have a non-empty URL."""
        Args._config["sourcing_mode"] = "all"
        Args._config["sourcing_url_prefix"] = ""
        mock_fetch.return_value = _csv_candidates()
        rows, _ = self.fetcher.get_candidate_urls()
        # a, b, c, d all have URLs; last row has empty URL
        urls = [r["url"] for r in rows]
        self.assertIn("https://catalog.data.gov/dataset/a", urls)
        self.assertIn("https://catalog.data.gov/dataset/b", urls)
        self.assertIn("https://catalog.data.gov/dataset/c", urls)
        self.assertIn("https://catalog.data.gov/dataset/d", urls)
        self.assertEqual(len(rows), 4)

    @patch("sourcing.SpreadsheetCandidateFetcher.get_gid_for_sheet_name", return_value="0")
    @patch.object(SpreadsheetCandidateFetcher, "_fetch_sheet_csv")
    def test_get_candidate_urls_stops_early_when_limit_reached(self, mock_fetch: object, _mock_gid: object) -> None:
        """Test that processing stops once limit is reached (doesn't process all rows)."""
        csv_many_rows = (
            "Admin Notes,Claimed (add your name),URL,Download Location\r\n"
            ",,https://catalog.data.gov/dataset/a,\r\n"
            ",,https://catalog.data.gov/dataset/b,\r\n"
            ",alice,https://catalog.data.gov/dataset/c,\r\n"
            ",,https://catalog.data.gov/dataset/d,/path\r\n"
            ",,https://catalog.data.gov/dataset/e,\r\n"
            ",,https://catalog.data.gov/dataset/f,\r\n"
        )
        mock_fetch.return_value = csv_many_rows

        rows, _ = self.fetcher.get_candidate_urls(limit=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["url"], "https://catalog.data.gov/dataset/a")
        self.assertEqual(rows[1]["url"], "https://catalog.data.gov/dataset/b")
