"""
Unit tests for pipeline_chat.executor.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from pipeline_chat.executor import ToolExecutionError, execute_read_only_tool, list_read_only_tools


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

