"""
Unit tests for CmsGovCollector.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from utils.Args import Args
from utils.Logger import Logger

from collectors.CmsGovCollector import CmsGovCollector
from collectors.tests.test_utils import setup_mock_playwright

_SAMPLE_SLUG_DATA = {
    "uuid": "8708ca8b-8636-44ed-8303-724cbfaf78ad",
    "name": "Hospital Service Area",
    "nav_topic": {
        "name": "Medicare Inpatient Hospitals",
    },
    "current_dataset": {
        "uuid": "22e11819-48e8-44a8-a036-fd6351f4173a",
        "name": "Hospital Service Area : 2024-01-01",
        "last_modified_date": "2025-09-10",
        "version": "2024-01-01",
    },
}

_SAMPLE_CURRENT_RESOURCES = [
    {
        "type": "Primary",
        "file_uuid": "dc75987c-1",
        "file_name": "Hospital_Service_Area_2024.csv",
        "file_mime": "text/csv",
        "file_size": 26951355,
        "file_url": "https://data.cms.gov/sites/default/files/2025-07/Hospital_Service_Area_2024.csv",
    },
    {
        "type": "Data Dictionary",
        "file_uuid": "30328efd-1",
        "file_name": "Hospital_Service_Area_Data_Dictionary.pdf",
        "file_mime": "application/pdf",
        "file_size": 70220,
        "file_url": "https://data.cms.gov/sites/default/files/2020-09/Hospital_Service_Area_Data_Dictionary.pdf",
    },
]

_SAMPLE_HISTORICAL_RESOURCES = [
    {
        "type": "Primary",
        "file_uuid": "aaa-2023",
        "file_name": "Hospital_Service_Area_2023.csv",
        "file_mime": "text/csv",
        "file_size": 44451075,
        "file_url": "https://data.cms.gov/sites/default/files/2024-07/Hospital_Service_Area_2023.csv",
        "dataset_version_label": "2023",
    },
    {
        "type": "Primary",
        "file_uuid": "dc75987c-1",  # same uuid as current — should be deduped
        "file_name": "Hospital_Service_Area_2024.csv",
        "file_mime": "text/csv",
        "file_size": 26951355,
        "file_url": "https://data.cms.gov/sites/default/files/2025-07/Hospital_Service_Area_2024.csv",
        "dataset_version_label": "2024",
    },
    {
        "type": "Data Dictionary",
        "file_uuid": "30328efd-1",  # same uuid as in current resources — deduped
        "file_name": "Hospital_Service_Area_Data_Dictionary.pdf",
        "file_mime": "application/pdf",
        "file_size": 70220,
        "file_url": "https://data.cms.gov/sites/default/files/2020-09/Hospital_Service_Area_Data_Dictionary.pdf",
    },
]


class TestCmsGovCollector(unittest.TestCase):

    def setUp(self) -> None:
        self._original_argv = sys.argv.copy()
        sys.argv = ["test", "noop"]
        Args.initialize()
        Logger.initialize(log_level="WARNING")
        self.collector = CmsGovCollector(headless=True)

    def tearDown(self) -> None:
        sys.argv = self._original_argv

    # ── _extract_path ────────────────────────────────────────────────────────

    def test_extract_path_standard(self) -> None:
        url = "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area"
        self.assertEqual(
            self.collector._extract_path(url),
            "/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area",
        )

    def test_extract_path_root_returns_none(self) -> None:
        self.assertIsNone(self.collector._extract_path("https://data.cms.gov/"))

    def test_extract_path_with_query(self) -> None:
        url = "https://data.cms.gov/some/path?foo=bar"
        self.assertEqual(self.collector._extract_path(url), "/some/path")

    # ── _parse_slug_metadata ─────────────────────────────────────────────────

    def test_parse_slug_metadata_full(self) -> None:
        fields = self.collector._parse_slug_metadata(_SAMPLE_SLUG_DATA)
        self.assertEqual(fields["title"], "Hospital Service Area")
        self.assertEqual(fields["agency"], "Centers for Medicare & Medicaid Services")
        self.assertIn("Medicare Inpatient Hospitals", fields["keywords"])
        self.assertIn("Hospital Service Area", fields["keywords"])
        self.assertEqual(fields["time_end"], "2025-09-10")  # last_modified wins

    def test_parse_slug_metadata_no_nav_topic(self) -> None:
        data = {**_SAMPLE_SLUG_DATA, "nav_topic": None}
        fields = self.collector._parse_slug_metadata(data)
        self.assertIn("Hospital Service Area", fields["keywords"])

    def test_parse_slug_metadata_missing_fields(self) -> None:
        fields = self.collector._parse_slug_metadata({})
        self.assertNotIn("title", fields)
        self.assertNotIn("time_end", fields)
        self.assertEqual(fields["agency"], "Centers for Medicare & Medicaid Services")

    # ── _gather_files ────────────────────────────────────────────────────────

    @patch.object(CmsGovCollector, "_fetch_resources")
    def test_gather_files_deduplicates(self, mock_fetch: Mock) -> None:
        mock_fetch.side_effect = [
            _SAMPLE_CURRENT_RESOURCES,     # first call: dataset/{current_uuid}/resources
            _SAMPLE_HISTORICAL_RESOURCES,  # second call: dataset-type/{taxonomy_uuid}/resources
        ]
        files = self.collector._gather_files(1, "current-uuid", "taxonomy-uuid")
        urls = [f["file_url"] for f in files]
        # 2023 Primary, 2024 Primary (deduped), Data Dictionary (deduped) = 3 unique
        self.assertEqual(len(urls), 3)
        self.assertEqual(len(set(urls)), 3)

    @patch.object(CmsGovCollector, "_fetch_resources")
    def test_gather_files_no_taxonomy_uuid_falls_back_to_current(self, mock_fetch: Mock) -> None:
        mock_fetch.return_value = _SAMPLE_CURRENT_RESOURCES
        files = self.collector._gather_files(1, "current-uuid", None)
        # Only Primary from current resources (no taxonomy call), plus Data Dictionary
        primary = [f for f in files if f["type"] == "Primary"]
        self.assertEqual(len(primary), 1)

    @patch.object(CmsGovCollector, "_fetch_resources")
    def test_gather_files_ancillaries_not_duplicated(self, mock_fetch: Mock) -> None:
        mock_fetch.side_effect = [
            _SAMPLE_CURRENT_RESOURCES,
            _SAMPLE_HISTORICAL_RESOURCES,
        ]
        files = self.collector._gather_files(1, "current-uuid", "taxonomy-uuid")
        dict_files = [f for f in files if f["type"] == "Data Dictionary"]
        self.assertEqual(len(dict_files), 1)

    # ── _scrape_description ──────────────────────────────────────────────────

    @patch("collectors.CmsGovCollector.sync_playwright")
    def test_scrape_description_success(self, mock_playwright: Mock) -> None:
        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_el = Mock()
        mock_el.inner_text.return_value = "  Hospital Service Area data description.  "
        mock_page.query_selector.return_value = mock_el

        result = self.collector._scrape_description("https://data.cms.gov/some/path", 1)

        self.assertEqual(result, "Hospital Service Area data description.")
        mock_page.query_selector.assert_called_once_with(
            "[class*='DatasetPage__summary-field-summary-container']"
        )

    @patch("collectors.CmsGovCollector.record_warning")
    @patch("collectors.CmsGovCollector.sync_playwright")
    def test_scrape_description_element_missing(
        self, mock_playwright: Mock, mock_warn: Mock
    ) -> None:
        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.return_value = None
        mock_page.query_selector.return_value = None

        result = self.collector._scrape_description("https://data.cms.gov/some/path", 1)

        self.assertIsNone(result)
        mock_warn.assert_called_once()
        self.assertIn("not found", mock_warn.call_args[0][1])

    @patch("collectors.CmsGovCollector.record_warning")
    @patch("collectors.CmsGovCollector.sync_playwright")
    def test_scrape_description_browser_init_fails(
        self, mock_playwright: Mock, mock_warn: Mock
    ) -> None:
        mock_playwright.return_value.start.side_effect = Exception("no browser")

        result = self.collector._scrape_description("https://data.cms.gov/some/path", 1)

        self.assertIsNone(result)
        mock_warn.assert_called_once()
        self.assertIn("Browser unavailable", mock_warn.call_args[0][1])

    @patch("collectors.CmsGovCollector.record_warning")
    @patch("collectors.CmsGovCollector.sync_playwright")
    def test_scrape_description_page_error(
        self, mock_playwright: Mock, mock_warn: Mock
    ) -> None:
        mock_page, _, _ = setup_mock_playwright(mock_playwright)
        mock_page.goto.side_effect = Exception("timeout")

        result = self.collector._scrape_description("https://data.cms.gov/some/path", 1)

        self.assertIsNone(result)
        mock_warn.assert_called_once()
        self.assertIn("Failed to scrape description", mock_warn.call_args[0][1])

    # ── _collect ─────────────────────────────────────────────────────────────

    @patch("collectors.CmsGovCollector.record_error")
    def test_collect_invalid_url(self, mock_record_error: Mock) -> None:
        result = self.collector._collect("not-a-url", 1)
        mock_record_error.assert_called_once_with(1, "Invalid URL: not-a-url")
        self.assertNotIn("folder_path", result)

    @patch("collectors.CmsGovCollector.record_error")
    def test_collect_root_url_fails(self, mock_record_error: Mock) -> None:
        result = self.collector._collect("https://data.cms.gov/", 1)
        mock_record_error.assert_called_once()
        self.assertIn("Cannot extract path", mock_record_error.call_args[0][1])

    @patch("collectors.CmsGovCollector.record_error")
    @patch("collectors.CmsGovCollector.requests.get")
    def test_collect_slug_api_failure(self, mock_get: Mock, mock_record_error: Mock) -> None:
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError()
        result = self.collector._collect(
            "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area",
            1,
        )
        mock_record_error.assert_called_once()
        self.assertIn("Slug API returned nothing", mock_record_error.call_args[0][1])

    @patch("collectors.CmsGovCollector.record_error")
    @patch("collectors.CmsGovCollector.requests.get")
    def test_collect_slug_missing_current_dataset_uuid(
        self, mock_get: Mock, mock_record_error: Mock
    ) -> None:
        mock_resp = Mock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"data": {"name": "Test", "uuid": "abc"}}
        mock_get.return_value = mock_resp
        result = self.collector._collect(
            "https://data.cms.gov/some/path", 1
        )
        mock_record_error.assert_called()
        self.assertIn("current_dataset.uuid", mock_record_error.call_args[0][1])

    @patch("collectors.CmsGovCollector.folder_extensions_and_size")
    @patch("collectors.CmsGovCollector.download_via_url")
    @patch("collectors.CmsGovCollector.create_output_folder")
    @patch.object(CmsGovCollector, "_gather_files")
    @patch.object(CmsGovCollector, "_scrape_description")
    @patch.object(CmsGovCollector, "_fetch_slug")
    def test_collect_success(
        self,
        mock_slug: Mock,
        mock_scrape: Mock,
        mock_gather: Mock,
        mock_create_folder: Mock,
        mock_download: Mock,
        mock_ext_size: Mock,
    ) -> None:
        mock_slug.return_value = _SAMPLE_SLUG_DATA
        mock_scrape.return_value = "Hospital Service Area data description."
        mock_gather.return_value = [_SAMPLE_CURRENT_RESOURCES[0]]
        mock_create_folder.return_value = Path("/tmp/DRP000001")
        mock_download.return_value = (26951355, True)
        mock_ext_size.return_value = ([".csv"], 26951355)

        result = self.collector._collect(
            "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area",
            1,
        )

        self.assertEqual(result["title"], "Hospital Service Area")
        self.assertEqual(result["agency"], "Centers for Medicare & Medicaid Services")
        self.assertEqual(result["summary"], "Hospital Service Area data description.")
        self.assertEqual(result["folder_path"], "/tmp/DRP000001")
        self.assertEqual(result["extensions"], ".csv")
        self.assertEqual(result["data_types"], "tabular")
        mock_download.assert_called_once()

    @patch("collectors.CmsGovCollector.record_warning")
    @patch("collectors.CmsGovCollector.folder_extensions_and_size")
    @patch("collectors.CmsGovCollector.download_via_url")
    @patch("collectors.CmsGovCollector.create_output_folder")
    @patch.object(CmsGovCollector, "_gather_files")
    @patch.object(CmsGovCollector, "_scrape_description")
    @patch.object(CmsGovCollector, "_fetch_slug")
    def test_collect_download_failure_warns(
        self,
        mock_slug: Mock,
        mock_scrape: Mock,
        mock_gather: Mock,
        mock_create_folder: Mock,
        mock_download: Mock,
        mock_ext_size: Mock,
        mock_warn: Mock,
    ) -> None:
        mock_slug.return_value = _SAMPLE_SLUG_DATA
        mock_scrape.return_value = None
        mock_gather.return_value = [_SAMPLE_CURRENT_RESOURCES[0]]
        mock_create_folder.return_value = Path("/tmp/DRP000001")
        mock_download.return_value = (0, False)
        mock_ext_size.return_value = ([], 0)

        self.collector._collect("https://data.cms.gov/some/path", 1)

        mock_warn.assert_called()
        self.assertIn("Download failed", mock_warn.call_args[0][1])

    # ── run() ────────────────────────────────────────────────────────────────

    @patch("collectors.CmsGovCollector.record_error")
    @patch("collectors.CmsGovCollector.Storage")
    def test_run_record_not_found(self, mock_storage: Mock, mock_record_error: Mock) -> None:
        mock_storage.get.return_value = None
        self.collector.run(123)
        mock_record_error.assert_called_once_with(
            123, "Project record not found for DRPID: 123", update_storage=False
        )

    @patch("collectors.CmsGovCollector.record_error")
    @patch("collectors.CmsGovCollector.Storage")
    def test_run_missing_source_url(self, mock_storage: Mock, mock_record_error: Mock) -> None:
        mock_storage.get.return_value = {"DRPID": 123, "status": "sourced"}
        self.collector.run(123)
        mock_record_error.assert_called_once_with(123, "Missing source_url for DRPID: 123")

    @patch("collectors.CmsGovCollector.Storage")
    @patch.object(CmsGovCollector, "_collect")
    def test_run_success_sets_collected(self, mock_collect: Mock, mock_storage: Mock) -> None:
        mock_storage.get.return_value = {
            "DRPID": 1,
            "source_url": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area",
            "status": "sourced",
        }
        mock_collect.return_value = {
            "folder_path": "/tmp/DRP000001",
            "title": "Hospital Service Area",
        }

        self.collector.run(1)

        fields = mock_storage.update_record.call_args[0][1]
        self.assertEqual(fields["status"], "collected")
        self.assertEqual(fields["title"], "Hospital Service Area")

    @patch("collectors.CmsGovCollector.Storage")
    @patch.object(CmsGovCollector, "_collect")
    def test_run_preserves_error_status(self, mock_collect: Mock, mock_storage: Mock) -> None:
        mock_storage.get.side_effect = [
            {
                "DRPID": 1,
                "source_url": "https://data.cms.gov/some/path",
                "status": "sourced",
            },
            {"DRPID": 1, "status": "error"},
        ]
        mock_collect.return_value = {"folder_path": "/tmp/DRP000001"}

        self.collector.run(1)

        fields = mock_storage.update_record.call_args[0][1]
        self.assertEqual(fields["status"], "error")


if __name__ == "__main__":
    unittest.main()
