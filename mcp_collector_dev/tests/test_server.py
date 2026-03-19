"""
Unit tests for mcp_collector_dev/server.py

Tests cover all 7 tools:
  - fetch_url_content
  - analyze_page_structure
  - get_collector_interface
  - list_collector_examples
  - scaffold_collector
  - register_collector
  - test_collector_on_project
"""

import shutil
import sqlite3
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import mcp_collector_dev.server as server


class TestFetchUrlContent(unittest.TestCase):

    @patch("mcp_collector_dev.server.requests.get")
    def test_returns_status_and_body(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text="<html>hello</html>",
            headers={"content-type": "text/html"},
        )
        result = server.fetch_url_content("https://example.com")
        self.assertIn("Status: 200", result)
        self.assertIn("<html>hello</html>", result)
        self.assertIn("text/html", result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_truncates_at_max_chars(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text="X" * 30000,
            headers={"content-type": "text/html"},
        )
        result = server.fetch_url_content("https://example.com", max_chars=100)
        self.assertIn("TRUNCATED", result)
        # Body section should be exactly 100 chars of X
        self.assertIn("X" * 100, result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_no_truncation_banner_when_short(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text="short",
            headers={"content-type": "text/plain"},
        )
        result = server.fetch_url_content("https://example.com")
        self.assertNotIn("TRUNCATED", result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_returns_error_on_exception(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = Exception("connection refused")
        result = server.fetch_url_content("https://example.com")
        self.assertIn("Error", result)
        self.assertIn("connection refused", result)


class TestAnalyzePageStructure(unittest.TestCase):

    SAMPLE_HTML = textwrap.dedent("""
        <html>
        <head>
            <title>Test Page</title>
            <meta name="description" content="A test page">
            <script type="application/ld+json">{"@type": "Dataset", "name": "MyData"}</script>
        </head>
        <body>
            <h1>Main Heading</h1>
            <h2>Sub Heading</h2>
            <a href="https://example.com/data.csv">Download CSV</a>
            <script>var api = "https://api.example.com/api/v1/data";</script>
        </body>
        </html>
    """)

    @patch("mcp_collector_dev.server.requests.get")
    def test_extracts_title_and_headings(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text=self.SAMPLE_HTML,
        )
        result = server.analyze_page_structure("https://example.com")
        self.assertIn("Test Page", result)
        self.assertIn("[H1] Main Heading", result)
        self.assertIn("[H2] Sub Heading", result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_extracts_links(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text=self.SAMPLE_HTML,
        )
        result = server.analyze_page_structure("https://example.com")
        self.assertIn("data.csv", result)
        self.assertIn("Download CSV", result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_extracts_meta_tags(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text=self.SAMPLE_HTML,
        )
        result = server.analyze_page_structure("https://example.com")
        self.assertIn("description", result)
        self.assertIn("A test page", result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_extracts_json_ld(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text=self.SAMPLE_HTML,
        )
        result = server.analyze_page_structure("https://example.com")
        self.assertIn("JSON-LD", result)
        self.assertIn("Dataset", result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_extracts_api_endpoints(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            text=self.SAMPLE_HTML,
        )
        result = server.analyze_page_structure("https://example.com")
        self.assertIn("api.example.com/api/v1/data", result)

    @patch("mcp_collector_dev.server.requests.get")
    def test_returns_error_on_exception(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = Exception("timeout")
        result = server.analyze_page_structure("https://example.com")
        self.assertIn("Error", result)
        self.assertIn("timeout", result)


class TestGetCollectorInterface(unittest.TestCase):

    def test_returns_string(self) -> None:
        result = server.get_collector_interface()
        self.assertIsInstance(result, str)

    def test_includes_storage_schema(self) -> None:
        result = server.get_collector_interface()
        for field in ["DRPID", "source_url", "folder_path", "status", "title", "agency"]:
            self.assertIn(field, result)

    def test_includes_required_interface(self) -> None:
        result = server.get_collector_interface()
        self.assertIn("def run", result)
        self.assertIn("Storage.get", result)
        self.assertIn("Storage.update_record", result)

    def test_includes_utility_functions(self) -> None:
        result = server.get_collector_interface()
        self.assertIn("record_error", result)
        self.assertIn("create_output_folder", result)
        self.assertIn("fetch_page_body", result)

    def test_includes_status_values(self) -> None:
        result = server.get_collector_interface()
        self.assertIn("sourced", result)
        self.assertIn("collected", result)


class TestListCollectorExamples(unittest.TestCase):

    def test_returns_existing_collectors(self) -> None:
        result = server.list_collector_examples()
        self.assertIn("SocrataCollector.py", result)
        self.assertIn("CatalogDataCollector.py", result)

    def test_excludes_init_files(self) -> None:
        result = server.list_collector_examples()
        # __init__.py should not appear as a FILE: header
        self.assertNotIn("FILE: __init__.py", result)

    def test_includes_source_code(self) -> None:
        result = server.list_collector_examples()
        self.assertIn("def run", result)
        self.assertIn("class", result)

    def test_empty_dir_message(self) -> None:
        with patch.object(server, "COLLECTORS_DIR", Path("/nonexistent/dir")):
            # Path.glob will raise or return empty; patch glob to return []
            with patch("pathlib.Path.glob", return_value=iter([])):
                result = server.list_collector_examples()
        self.assertIn("No collector files found", result)


class TestScaffoldCollector(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())
        self._orig_collectors_dir = server.COLLECTORS_DIR
        server.COLLECTORS_DIR = self.temp_dir

    def tearDown(self) -> None:
        server.COLLECTORS_DIR = self._orig_collectors_dir
        shutil.rmtree(self.temp_dir)

    def test_dry_run_shows_content_without_creating_file(self) -> None:
        result = server.scaffold_collector(
            class_name="FooCollector",
            module_name="foo_collector",
            description="Fetches Foo data",
            dry_run=True,
        )
        self.assertIn("DRY RUN", result)
        self.assertIn("FooCollector", result)
        self.assertIn("Fetches Foo data", result)
        self.assertFalse((self.temp_dir / "FooCollector.py").exists())

    def test_creates_file_when_dry_run_false(self) -> None:
        result = server.scaffold_collector(
            class_name="FooCollector",
            module_name="foo_collector",
            dry_run=False,
        )
        expected_file = self.temp_dir / "FooCollector.py"
        self.assertTrue(expected_file.exists())
        self.assertNotIn("DRY RUN", result)
        content = expected_file.read_text()
        self.assertIn("class FooCollector", content)
        self.assertIn("def run", content)
        self.assertIn("def _collect", content)
        self.assertIn("def _update_storage_from_result", content)

    def test_generated_file_is_valid_python(self) -> None:
        server.scaffold_collector(
            class_name="FooCollector",
            module_name="foo_collector",
            dry_run=False,
        )
        source = (self.temp_dir / "FooCollector.py").read_text()
        compile(source, "FooCollector.py", "exec")  # raises SyntaxError if invalid

    def test_no_overwrite_by_default(self) -> None:
        server.scaffold_collector("FooCollector", "foo_collector", dry_run=False)
        result = server.scaffold_collector("FooCollector", "foo_collector", dry_run=False)
        self.assertIn("Error", result)
        self.assertIn("already exists", result)

    def test_overwrite_flag_replaces_file(self) -> None:
        server.scaffold_collector("FooCollector", "foo_collector", dry_run=False)
        result = server.scaffold_collector(
            "FooCollector", "foo_collector",
            description="New description",
            dry_run=False,
            overwrite=True,
        )
        self.assertNotIn("Error", result)
        self.assertIn("New description", (self.temp_dir / "FooCollector.py").read_text())

    def test_invalid_class_name(self) -> None:
        result = server.scaffold_collector("lowercase", "foo_collector", dry_run=False)
        self.assertIn("Error", result)

    def test_invalid_module_name(self) -> None:
        result = server.scaffold_collector("FooCollector", "FooCollector", dry_run=False)
        self.assertIn("Error", result)


class TestRegisterCollector(unittest.TestCase):

    # Minimal Orchestrator.py text with the insertion marker
    ORCHESTRATOR_STUB = textwrap.dedent("""\
        MODULES = {
            "catalog_collector": {
                "prereq": "sourced",
                "class_name": "CatalogDataCollector",
            }
            ,"upload": {
                "prereq": "collected",
                "class_name": "DataLumosUploader",
            },
        }
    """)

    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())
        self.fake_orchestrator = self.temp_dir / "Orchestrator.py"
        self.fake_orchestrator.write_text(self.ORCHESTRATOR_STUB)
        self._orig_file = server.ORCHESTRATOR_FILE
        server.ORCHESTRATOR_FILE = self.fake_orchestrator

    def tearDown(self) -> None:
        server.ORCHESTRATOR_FILE = self._orig_file
        shutil.rmtree(self.temp_dir)

    def test_dry_run_shows_diff_without_editing(self) -> None:
        result = server.register_collector(
            module_name="bar_collector",
            class_name="BarCollector",
            dry_run=True,
        )
        self.assertIn("DRY RUN", result)
        self.assertIn("bar_collector", result)
        self.assertIn("BarCollector", result)
        # File unchanged
        self.assertEqual(self.fake_orchestrator.read_text(), self.ORCHESTRATOR_STUB)

    def test_applies_registration_when_dry_run_false(self) -> None:
        result = server.register_collector(
            module_name="bar_collector",
            class_name="BarCollector",
            dry_run=False,
        )
        updated = self.fake_orchestrator.read_text()
        self.assertIn('"bar_collector"', updated)
        self.assertIn('"BarCollector"', updated)
        self.assertNotIn("DRY RUN", result)

    def test_insertion_is_before_upload(self) -> None:
        server.register_collector("bar_collector", "BarCollector", dry_run=False)
        updated = self.fake_orchestrator.read_text()
        bar_pos = updated.index("bar_collector")
        upload_pos = updated.index('"upload"')
        self.assertLess(bar_pos, upload_pos)

    def test_duplicate_module_name_returns_error(self) -> None:
        server.register_collector("bar_collector", "BarCollector", dry_run=False)
        result = server.register_collector("bar_collector", "BarCollector", dry_run=False)
        self.assertIn("Error", result)
        self.assertIn("already exists", result)

    def test_invalid_module_name_returns_error(self) -> None:
        result = server.register_collector("BadName", "BadCollector", dry_run=False)
        self.assertIn("Error", result)

    def test_custom_prereq(self) -> None:
        server.register_collector("bar_collector", "BarCollector", prereq="collected", dry_run=False)
        updated = self.fake_orchestrator.read_text()
        self.assertIn('"prereq": "collected"', updated)


class TestTestCollectorOnProject(unittest.TestCase):

    ORCHESTRATOR_WITH_MODULE = textwrap.dedent("""\
        MODULES = {
            "test_col": {
                "prereq": "sourced",
                "class_name": "TestCol",
            }
        }
    """)

    ORCHESTRATOR_WITHOUT_MODULE = 'MODULES = {}'

    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())

        # Fake Orchestrator.py that includes test_col
        self.fake_orchestrator = self.temp_dir / "Orchestrator.py"
        self.fake_orchestrator.write_text(self.ORCHESTRATOR_WITH_MODULE)
        self._orig_orch = server.ORCHESTRATOR_FILE
        server.ORCHESTRATOR_FILE = self.fake_orchestrator

        # Fake SQLite DB with one project
        self.db_path = self.temp_dir / "test.db"
        con = sqlite3.connect(self.db_path)
        con.execute(
            "CREATE TABLE projects "
            "(DRPID INTEGER PRIMARY KEY, source_url TEXT, status TEXT, "
            "errors TEXT, warnings TEXT, folder_path TEXT, title TEXT)"
        )
        con.execute(
            "INSERT INTO projects (DRPID, source_url, status) VALUES (?, ?, ?)",
            (42, "https://example.com/data", "sourced"),
        )
        con.commit()
        con.close()

        self._orig_config = server._read_config
        server._read_config = lambda: {"db_path": str(self.db_path)}

    def tearDown(self) -> None:
        server.ORCHESTRATOR_FILE = self._orig_orch
        server._read_config = self._orig_config
        shutil.rmtree(self.temp_dir)

    def test_unregistered_module_returns_error(self) -> None:
        self.fake_orchestrator.write_text(self.ORCHESTRATOR_WITHOUT_MODULE)
        result = server.test_collector_on_project("ghost_collector", 42)
        self.assertIn("Error", result)
        self.assertIn("not registered", result)

    def test_missing_drpid_returns_error(self) -> None:
        result = server.test_collector_on_project("test_col", 999)
        self.assertIn("Error", result)
        self.assertIn("999", result)

    @patch("mcp_collector_dev.server.subprocess.run")
    def test_successful_run_shows_record_diff(self, mock_run: MagicMock) -> None:
        db_path = self.db_path

        def fake_run(*args, **kwargs):
            # Simulate the collector updating the DB during the subprocess call
            con = sqlite3.connect(db_path)
            con.execute(
                "UPDATE projects SET status='collected', folder_path='/tmp/DRP000042' WHERE DRPID=42"
            )
            con.commit()
            con.close()
            return MagicMock(returncode=0, stdout="INFO: collected DRPID 42\n", stderr="")

        mock_run.side_effect = fake_run

        result = server.test_collector_on_project("test_col", 42)
        self.assertIn("Exit code: 0", result)
        self.assertIn("status", result)
        self.assertIn("'sourced'", result)
        self.assertIn("'collected'", result)

    @patch("mcp_collector_dev.server.subprocess.run")
    def test_shows_subprocess_stdout(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="collector output line\n",
            stderr="",
        )
        result = server.test_collector_on_project("test_col", 42)
        self.assertIn("collector output line", result)

    @patch("mcp_collector_dev.server.subprocess.run")
    def test_shows_errors_from_storage(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
        con = sqlite3.connect(self.db_path)
        con.execute("UPDATE projects SET errors='URL not found' WHERE DRPID=42")
        con.commit()
        con.close()

        result = server.test_collector_on_project("test_col", 42)
        self.assertIn("URL not found", result)

    @patch("mcp_collector_dev.server.subprocess.run")
    def test_timeout_returns_error(self, mock_run: MagicMock) -> None:
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="python", timeout=300)
        result = server.test_collector_on_project("test_col", 42)
        self.assertIn("timed out", result)


if __name__ == "__main__":
    unittest.main()
