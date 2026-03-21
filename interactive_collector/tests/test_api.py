"""
Unit tests for the Interactive Collector JSON API.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from interactive_collector.app import app
from interactive_collector.api_scoreboard import add_to_scoreboard, clear_scoreboard
from pipeline_chat.schemas import ChatQueryResponse


class TestApiProjects(unittest.TestCase):
    """Tests for /api/projects/* endpoints."""

    def setUp(self) -> None:
        """Use test client for each test."""
        self.client = app.test_client()

    def test_projects_first_returns_json_or_404(self) -> None:
        """GET /api/projects/first returns project or 404."""
        with patch("interactive_collector.api.get_first_eligible", return_value=None):
            resp = self.client.get("/api/projects/first")
            self.assertEqual(resp.status_code, 404)
            data = json.loads(resp.data)
            self.assertIn("error", data)

    def test_projects_next_requires_current_drpid(self) -> None:
        """GET /api/projects/next without current_drpid returns 400."""
        resp = self.client.get("/api/projects/next")
        self.assertEqual(resp.status_code, 400)

    def test_projects_next_invalid_current_drpid_returns_400(self) -> None:
        """GET /api/projects/next with non-integer current_drpid returns 400."""
        resp = self.client.get("/api/projects/next?current_drpid=abc")
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertIn("error", data)

    def test_projects_first_returns_project_when_found(self) -> None:
        """GET /api/projects/first returns project when get_first_eligible returns one."""
        proj = {"DRPID": 1, "source_url": "https://example.com"}
        with patch("interactive_collector.api.get_first_eligible", return_value=proj):
            resp = self.client.get("/api/projects/first")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.data), proj)

    def test_projects_get_returns_404_for_missing(self) -> None:
        """GET /api/projects/999 returns 404 when not found."""
        with patch("interactive_collector.api.get_project_by_drpid", return_value=None):
            resp = self.client.get("/api/projects/999")
            self.assertEqual(resp.status_code, 404)


class TestApiProjectsLoad(unittest.TestCase):
    """Tests for /api/projects/load."""

    def setUp(self) -> None:
        self.client = app.test_client()
        clear_scoreboard()

    def test_projects_load_returns_404_when_no_project(self) -> None:
        """POST /api/projects/load with no eligible project returns 404."""
        with patch("interactive_collector.api.get_first_eligible", return_value=None):
            with patch("interactive_collector.api.get_project_by_drpid", return_value=None):
                resp = self.client.post(
                    "/api/projects/load",
                    json={},
                    content_type="application/json",
                )
                self.assertEqual(resp.status_code, 404)

    def test_projects_load_returns_project_when_found(self) -> None:
        """POST /api/projects/load with drpid returns project and clears scoreboard."""
        proj = {
            "DRPID": 1,
            "source_url": "https://example.com/dataset",
            "title": "Test",
        }
        with patch("interactive_collector.api.get_project_by_drpid", return_value=proj):
            with patch("interactive_collector.api.ensure_output_folder", return_value="C:\\out\\1"):
                add_to_scoreboard("https://old.com", None, "OK")
                resp = self.client.post(
                    "/api/projects/load",
                    json={"drpid": 1},
                    content_type="application/json",
                )
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data["DRPID"], 1)
        self.assertEqual(data["source_url"], "https://example.com/dataset")
        self.assertIn("scoreboard", data)
        self.assertEqual(data["scoreboard"], [])

    def test_projects_load_invalid_drpid_returns_400(self) -> None:
        """POST /api/projects/load with invalid drpid returns 400."""
        resp = self.client.post(
            "/api/projects/load",
            json={"drpid": "not-a-number"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertIn("error", data)


class TestApiMetadataFromPage(unittest.TestCase):
    """Tests for /api/metadata-from-page (Copy & Open page preload)."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_metadata_from_page_post_then_get_returns_and_consumes(self) -> None:
        """POST stores metadata for drpid; GET returns and clears it."""
        resp = self.client.post(
            "/api/metadata-from-page",
            json={"drpid": 1, "title": "Dataset Title", "summary": "<p>Desc</p>", "download_date": "2025-02-22"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        resp2 = self.client.get("/api/metadata-from-page?drpid=1")
        self.assertEqual(resp2.status_code, 200)
        data = json.loads(resp2.data)
        self.assertEqual(data["metadata"].get("title"), "Dataset Title")
        self.assertEqual(data["metadata"].get("summary"), "<p>Desc</p>")
        self.assertEqual(data["metadata"].get("download_date"), "2025-02-22")
        resp3 = self.client.get("/api/metadata-from-page?drpid=1")
        data3 = json.loads(resp3.data)
        self.assertEqual(data3["metadata"], {})

    def test_metadata_from_page_post_requires_drpid(self) -> None:
        """POST without drpid returns 400."""
        resp = self.client.post("/api/metadata-from-page", json={}, content_type="application/json")
        self.assertEqual(resp.status_code, 400)


class TestApiProxy(unittest.TestCase):
    """Tests for /api/proxy (resource proxy for iframe CSS/JS/images)."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_proxy_requires_valid_url(self) -> None:
        """GET /api/proxy without url or with invalid url returns 400."""
        resp = self.client.get("/api/proxy")
        self.assertEqual(resp.status_code, 400)
        resp = self.client.get("/api/proxy?url=not-a-url")
        self.assertEqual(resp.status_code, 400)

    def test_proxy_returns_502_on_request_error(self) -> None:
        """GET /api/proxy returns 502 when requests.get raises."""
        import requests as req
        with patch("interactive_collector.api.requests.get", side_effect=req.RequestException("timeout")):
            resp = self.client.get("/api/proxy?url=https://example.com/style.css")
        self.assertEqual(resp.status_code, 502)
        data = json.loads(resp.data)
        self.assertIn("error", data)


class TestApiScoreboard(unittest.TestCase):
    """Tests for /api/scoreboard."""

    def setUp(self) -> None:
        self.client = app.test_client()
        clear_scoreboard()

    def test_scoreboard_get_returns_empty(self) -> None:
        """GET /api/scoreboard returns scoreboard and urls."""
        resp = self.client.get("/api/scoreboard")
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertIn("scoreboard", data)
        self.assertIn("urls", data)
        self.assertEqual(data["scoreboard"], [])
        self.assertEqual(data["urls"], [])

    def test_scoreboard_clear_clears_and_returns_empty(self) -> None:
        """POST /api/scoreboard/clear clears scoreboard and returns empty."""
        add_to_scoreboard("https://example.com", None, "OK")
        resp = self.client.post("/api/scoreboard/clear")
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data["scoreboard"], [])
        self.assertEqual(data["urls"], [])
        resp2 = self.client.get("/api/scoreboard")
        self.assertEqual(json.loads(resp2.data)["scoreboard"], [])

    def test_scoreboard_add_requires_url(self) -> None:
        """POST /api/scoreboard/add without url returns 400."""
        resp = self.client.post("/api/scoreboard/add", json={}, content_type="application/json")
        self.assertEqual(resp.status_code, 400)

    def test_scoreboard_add_returns_tree_and_urls(self) -> None:
        """POST /api/scoreboard/add with url adds and returns scoreboard."""
        resp = self.client.post(
            "/api/scoreboard/add",
            json={"url": "https://example.com/page", "referrer": None, "status_label": "OK"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertIn("scoreboard", data)
        self.assertIn("urls", data)
        self.assertEqual(data["urls"], ["https://example.com/page"])


class TestApiNoLinks(unittest.TestCase):
    """Tests for /api/no-links."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_no_links_requires_drpid(self) -> None:
        """POST /api/no-links without drpid returns 400."""
        resp = self.client.post("/api/no-links", json={}, content_type="application/json")
        self.assertEqual(resp.status_code, 400)

    def test_no_links_success(self) -> None:
        """POST /api/no-links with valid drpid updates storage and returns 200."""
        with patch("storage.Storage") as mock_storage:
            resp = self.client.post(
                "/api/no-links",
                json={"drpid": 1},
                content_type="application/json",
            )
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), True)


class TestApiExtensionSavePdf(unittest.TestCase):
    """Tests for /api/extension/save-pdf."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_extension_save_pdf_options_returns_204(self) -> None:
        """OPTIONS /api/extension/save-pdf returns 204 with CORS headers."""
        resp = self.client.open("/api/extension/save-pdf", method="OPTIONS")
        self.assertEqual(resp.status_code, 204)
        self.assertIn("Access-Control-Allow-Origin", resp.headers)

    def test_extension_save_pdf_requires_drpid(self) -> None:
        """POST without drpid returns 400."""
        resp = self.client.post(
            "/api/extension/save-pdf",
            data={"url": "https://example.com/page"},
            content_type="multipart/form-data",
        )
        self.assertIn(resp.status_code, (400, 500))
        if resp.status_code == 400:
            data = json.loads(resp.data)
            self.assertIn("error", data)

    def test_extension_save_pdf_requires_valid_url(self) -> None:
        """POST with invalid url returns 400."""
        resp = self.client.post(
            "/api/extension/save-pdf",
            data={"drpid": "1", "url": "not-a-url"},
            content_type="multipart/form-data",
        )
        self.assertIn(resp.status_code, (400, 500))

    def test_extension_save_pdf_success(self) -> None:
        """POST with drpid, url, and pdf file writes file and returns 200."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("interactive_collector.api.get_result_by_drpid", return_value={1: {"folder_path": None}}):
                with patch("interactive_collector.api.ensure_output_folder", return_value=tmpdir):
                    with open(Path(__file__).parent / "test_api.py", "rb") as fake_pdf:
                        resp = self.client.post(
                            "/api/extension/save-pdf",
                            data={
                                "drpid": "1",
                                "url": "https://example.com/page",
                                "pdf": (fake_pdf, "page.pdf"),
                            },
                            content_type="multipart/form-data",
                        )
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            self.assertEqual(data.get("ok"), True)
            self.assertIn("filename", data)


class TestDownloadsWatcher(unittest.TestCase):
    """Tests for /api/downloads-watcher/* endpoints."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_start_requires_drpid(self) -> None:
        """POST /api/downloads-watcher/start without drpid returns 400."""
        resp = self.client.post(
            "/api/downloads-watcher/start",
            json={},
            content_type="application/json",
        )
        self.assertIn(resp.status_code, (400, 500))
        data = json.loads(resp.data)
        self.assertIn("error", data)

    def test_status_returns_watching(self) -> None:
        """GET /api/downloads-watcher/status returns watching flag."""
        resp = self.client.get("/api/downloads-watcher/status")
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertIn("watching", data)
        self.assertIsInstance(data["watching"], bool)

    def test_stop_returns_ok(self) -> None:
        """POST /api/downloads-watcher/stop returns ok."""
        resp = self.client.post("/api/downloads-watcher/stop")
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertIn("ok", data)


class TestApiPipeline(unittest.TestCase):
    """Tests for /api/pipeline/* endpoints."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_pipeline_modules_returns_list(self) -> None:
        """GET /api/pipeline/modules returns module names in Orchestrator order, no noop."""
        resp = self.client.get("/api/pipeline/modules")
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertIn("modules", data)
        mods = data["modules"]
        self.assertIsInstance(mods, list)
        self.assertIn("sourcing", mods)
        self.assertIn("interactive_collector", mods)
        self.assertNotIn("noop", mods)

    def test_pipeline_run_requires_module(self) -> None:
        """POST /api/pipeline/run without module returns 400."""
        resp = self.client.post(
            "/api/pipeline/run",
            json={},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertIn("error", data)

    def test_pipeline_run_rejects_unknown_module(self) -> None:
        """POST /api/pipeline/run with unknown module returns 400."""
        resp = self.client.post(
            "/api/pipeline/run",
            json={"module": "unknown_module"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertIn("error", data)

    def test_pipeline_run_interactive_collector_returns_400(self) -> None:
        """POST /api/pipeline/run with interactive_collector returns 400 (use button instead)."""
        resp = self.client.post(
            "/api/pipeline/run",
            json={"module": "interactive_collector"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertIn("error", data)

    def test_pipeline_stop_returns_ok(self) -> None:
        """POST /api/pipeline/stop returns 200 and ok: true."""
        resp = self.client.post("/api/pipeline/stop")
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), True)

    def test_pipeline_run_noop_streams_output(self) -> None:
        """POST /api/pipeline/run with noop streams NDJSON log frames."""
        proc = unittest.mock.MagicMock()
        _reads = [
            b"2025-01-01 12:00:00 - INFO - DRP Pipeline starting...\n",
            b"Done\n",
            b"",
        ]

        def _read(_n: int = 8192) -> bytes:
            return _reads.pop(0) if _reads else b""

        proc.stdout.read = _read
        proc.poll.return_value = 0
        proc.wait.return_value = 0
        with patch(
            "interactive_collector.api_pipeline.subprocess.Popen",
            return_value=proc,
        ) as mock_popen:
            resp = self.client.post(
                "/api/pipeline/run",
                json={"module": "noop"},
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.data.decode("utf-8")
            self.assertIn("DRP Pipeline", body)
            self.assertIn('"line"', body)
            mock_popen.assert_called_once()
            call_args = mock_popen.call_args[0][0]
            self.assertIn("noop", call_args)


class TestApiChat(unittest.TestCase):
    """Tests for /api/chat/* endpoints."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_chat_query_requires_message(self) -> None:
        """POST /api/chat/query without message returns 400."""
        resp = self.client.post(
            "/api/chat/query",
            json={},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), False)
        self.assertIn("error", data)

    def test_chat_query_rejects_overlong_message(self) -> None:
        """POST /api/chat/query rejects message above guardrail length."""
        resp = self.client.post(
            "/api/chat/query",
            json={"message": "x" * 5001},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), False)
        self.assertIn("too long", str(data.get("error", "")))

    def test_chat_query_success(self) -> None:
        """POST /api/chat/query returns tool output on successful query."""
        with patch(
            "interactive_collector.api_chat.run_chat_query",
            return_value=ChatQueryResponse(
                ok=True,
                requires_confirmation=False,
                tool_name="get_pipeline_stats",
                arguments={},
                result="Database: data_cms_gov.db",
            ),
        ):
            resp = self.client.post(
                "/api/chat/query",
                json={"message": "database status"},
                content_type="application/json",
            )
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), True)
        self.assertEqual(data.get("tool_name"), "get_pipeline_stats")
        self.assertIn("Database:", data.get("result", ""))

    def test_chat_query_error_response(self) -> None:
        """POST /api/chat/query returns 400 when query cannot map to a tool."""
        with patch(
            "interactive_collector.api_chat.run_chat_query",
            return_value=ChatQueryResponse(
                ok=False,
                requires_confirmation=False,
                error="I could not map that request to a tool yet.",
            ),
        ):
            resp = self.client.post(
                "/api/chat/query",
                json={"message": "some unsupported request"},
                content_type="application/json",
            )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), False)
        self.assertIn("error", data)

    def test_chat_confirm_requires_token(self) -> None:
        """POST /api/chat/confirm without confirmation_token returns 400."""
        resp = self.client.post(
            "/api/chat/confirm",
            json={},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), False)
        self.assertIn("error", data)

    def test_chat_confirm_success(self) -> None:
        """POST /api/chat/confirm executes proposed action when token is valid."""
        with patch(
            "interactive_collector.api_chat.confirm_chat_action",
            return_value=ChatQueryResponse(
                ok=True,
                requires_confirmation=False,
                tool_name="run_module",
                arguments={"module": "sourcing", "dry_run": True},
                result="ok",
            ),
        ):
            resp = self.client.post(
                "/api/chat/confirm",
                json={"confirmation_token": "abc123", "session_id": "s1"},
                content_type="application/json",
            )
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data.get("ok"), True)
        self.assertEqual(data.get("tool_name"), "run_module")
