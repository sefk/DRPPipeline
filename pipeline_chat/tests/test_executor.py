"""
Unit tests for pipeline_chat.executor.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from pipeline_chat.executor import ToolExecutionError, execute_read_only_tool, list_read_only_tools
from pipeline_chat.executor import execute_mutating_tool


def test_list_read_only_tools_contains_expected() -> None:
    tools = list_read_only_tools()
    assert "get_pipeline_stats" in tools
    assert "list_projects" in tools


def test_execute_read_only_tool_rejects_unknown() -> None:
    with pytest.raises(ToolExecutionError) as exc:
        execute_read_only_tool("not_a_real_tool", {})
    assert "not allowed" in str(exc.value)


def test_execute_read_only_tool_validates_arguments() -> None:
    # get_project requires drpid, so empty args should fail bind()
    with pytest.raises(ToolExecutionError) as exc:
        execute_read_only_tool("get_project", {})
    assert "Invalid arguments" in str(exc.value)


def test_execute_read_only_tool_calls_allowlisted_function() -> None:
    with patch("pipeline_chat.executor.READ_ONLY_TOOLS", {"get_pipeline_stats": Mock(return_value="ok")}):
        result = execute_read_only_tool("get_pipeline_stats", {})
    assert result == "ok"


def test_execute_mutating_tool_defaults_dry_run_false() -> None:
    # Ensure we actually execute (dry_run=False) after confirmation unless explicitly overridden.
    def _fake_set_project_status(drpid: int, status: str, dry_run: bool = True) -> str:
        return f"{drpid}:{status}:{dry_run}"

    with patch("pipeline_chat.executor.MUTATING_TOOLS", {"set_project_status": _fake_set_project_status}):
        result = execute_mutating_tool("set_project_status", {"drpid": 1, "status": "sourced"})
    assert result.endswith(":False")

