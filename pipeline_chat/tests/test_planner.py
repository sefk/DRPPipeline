"""
Unit tests for pipeline_chat.planner.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipeline_chat.planner import PlannerError, _validate_planner_json, plan_tool_call


def test_validate_planner_json_accepts_read_only() -> None:
    decision = _validate_planner_json({"tool_name": "get_pipeline_stats", "arguments": {}})
    assert decision.call.tool_name == "get_pipeline_stats"
    assert decision.call.arguments == {}
    assert decision.is_mutating is False


def test_validate_planner_json_accepts_mutating() -> None:
    decision = _validate_planner_json({"tool_name": "run_module", "arguments": {"module": "sourcing"}})
    assert decision.call.tool_name == "run_module"
    assert decision.is_mutating is True


def test_validate_planner_json_rejects_bad_shape() -> None:
    with pytest.raises(PlannerError):
        _validate_planner_json(["bad"])  # type: ignore[arg-type]
    with pytest.raises(PlannerError):
        _validate_planner_json({"arguments": {}})
    with pytest.raises(PlannerError):
        _validate_planner_json({"tool_name": "get_pipeline_stats", "arguments": "bad"})


def test_plan_tool_call_uses_mocked_openai_response() -> None:
    with patch(
        "pipeline_chat.planner._call_openai_json",
        return_value={"tool_name": "list_projects", "arguments": {"status": "sourced", "limit": 5}},
    ):
        decision = plan_tool_call("what can i collect next")
    assert decision.call.tool_name == "list_projects"
    assert decision.call.arguments["limit"] == 5


def test_plan_tool_call_rejects_unknown_tool() -> None:
    with patch(
        "pipeline_chat.planner._call_openai_json",
        return_value={"tool_name": "totally_fake_tool", "arguments": {}},
    ):
        with pytest.raises(PlannerError):
            plan_tool_call("do something")

