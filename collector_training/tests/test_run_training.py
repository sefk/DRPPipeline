"""
Tests for collector_training/run_training.py.

run_training.py has two modes:

  --execute   Subprocess mode (used by run_training_loop MCP tool):
              loads config from DB and calls TrainingCoordinator.run() directly.

  (default)   CLI mode: delegates to run_training_loop from the MCP server,
              which launches training as a background process.
"""
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import collector_training.run_training as run_training


_SAMPLE_CONFIG = json.dumps({
    "collector_name": "CmsGovCollector",
    "collector_module_name": "cms_collector",
    "source_site": "data.cms.gov",
})


def _make_db(has_run: bool = True) -> Path:
    """Return path to a temp SQLite DB with an optional training run record."""
    tmp = tempfile.mkdtemp()
    db_path = Path(tmp) / "training.db"
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE training_runs (
            run_id INTEGER PRIMARY KEY,
            config_json TEXT,
            status TEXT,
            collector_name TEXT,
            source_site TEXT,
            started_at TEXT,
            notes TEXT
        )
    """)
    if has_run:
        con.execute(
            "INSERT INTO training_runs VALUES (1,?,'running','CmsGovCollector','data.cms.gov',datetime('now'),'')",
            (_SAMPLE_CONFIG,),
        )
    con.commit()
    con.close()
    return db_path


class TestExecuteMode(unittest.TestCase):
    """Tests for _execute() — the --execute / subprocess mode."""

    def test_missing_run_returns_error_code(self) -> None:
        db_path = _make_db(has_run=False)
        real_con = sqlite3.connect(str(db_path))
        real_con.row_factory = sqlite3.Row
        with patch("collector_training.run_training.get_connection", return_value=real_con), \
             patch("collector_training.run_training.init_db"):
            result = run_training._execute(999)
        self.assertEqual(result, 1)

    def test_valid_run_calls_coordinator(self) -> None:
        db_path = _make_db()
        mock_result = MagicMock(
            best_score=0.75,
            best_iteration=2,
            total_cost_usd=0.05,
            stop_reason="score_plateau",
            best_collector_path=None,
        )
        real_con = sqlite3.connect(str(db_path))
        real_con.row_factory = sqlite3.Row
        mock_coord = MagicMock()
        mock_coord.run.return_value = mock_result
        with patch("collector_training.run_training.get_connection", return_value=real_con), \
             patch("collector_training.run_training.init_db"), \
             patch("collector_training.run_training.TrainingCoordinator", return_value=mock_coord):
            exit_code = run_training._execute(1)

        self.assertEqual(exit_code, 0)
        mock_coord.run.assert_called_once_with(1)

    def test_valid_run_builds_config_from_db(self) -> None:
        db_path = _make_db()
        mock_result = MagicMock(
            best_score=0.5, best_iteration=1, total_cost_usd=0.01,
            stop_reason="max_iterations", best_collector_path=None,
        )
        mock_coord = MagicMock()
        mock_coord.run.return_value = mock_result
        real_con = sqlite3.connect(str(db_path))
        real_con.row_factory = sqlite3.Row
        with patch("collector_training.run_training.get_connection", return_value=real_con), \
             patch("collector_training.run_training.init_db"), \
             patch("collector_training.run_training.TrainingCoordinator", return_value=mock_coord) as mock_coord_cls, \
             patch("collector_training.run_training.TrainingConfig") as mock_cfg_cls:
            mock_cfg_cls.__dataclass_fields__ = {
                "collector_name", "collector_module_name", "source_site"
            }
            mock_cfg_cls.return_value = MagicMock()
            run_training._execute(1)

        mock_cfg_cls.assert_called_once()
        kwargs = mock_cfg_cls.call_args[1]
        self.assertEqual(kwargs["collector_name"], "CmsGovCollector")
        self.assertEqual(kwargs["source_site"], "data.cms.gov")


class TestCliMode(unittest.TestCase):
    """Tests for main() without --execute — the CLI / background-launch mode."""

    def test_no_args_exits(self) -> None:
        with patch.object(sys, "argv", ["run_training.py"]):
            with self.assertRaises(SystemExit):
                run_training.main()

    def test_delegates_to_run_training_loop(self) -> None:
        with patch.object(sys, "argv", ["run_training.py", "7"]), \
             patch("mcp_collector_dev.server.run_training_loop") as mock_loop, \
             patch("builtins.print"):
            mock_loop.return_value = "Training loop started for run 7 (PID 9999)."
            run_training.main()

        mock_loop.assert_called_once_with(7)

    def test_execute_flag_skips_run_training_loop(self) -> None:
        with patch.object(sys, "argv", ["run_training.py", "3", "--execute"]), \
             patch("collector_training.run_training._execute", return_value=0) as mock_exec, \
             patch("mcp_collector_dev.server.run_training_loop") as mock_loop, \
             self.assertRaises(SystemExit) as cm:
            run_training.main()

        mock_exec.assert_called_once_with(3)
        mock_loop.assert_not_called()
        self.assertEqual(cm.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
