"""
Tests for confirmation token lifecycle and execution guard behavior.
"""

from __future__ import annotations

from unittest.mock import patch

from pipeline_chat.confirmations import cancel_pending_action, consume_pending_action, create_pending_action
from pipeline_chat.schemas import ToolCall
from pipeline_chat.service import confirm_chat_action, run_chat_query


def test_pending_action_create_consume_success() -> None:
    action = create_pending_action("s1", ToolCall(tool_name="run_module", arguments={"module": "sourcing"}))
    consumed = consume_pending_action(action.token, "s1")
    assert consumed is not None
    assert consumed.call.tool_name == "run_module"
    # one-time token
    assert consume_pending_action(action.token, "s1") is None


def test_pending_action_session_guard() -> None:
    action = create_pending_action("s1", ToolCall(tool_name="run_module", arguments={"module": "sourcing"}))
    assert consume_pending_action(action.token, "s2") is None
    # still available for owner
    assert consume_pending_action(action.token, "s1") is not None


def test_pending_action_cancel() -> None:
    action = create_pending_action("s1", ToolCall(tool_name="run_module", arguments={"module": "sourcing"}))
    assert cancel_pending_action(action.token, "s1") is True
    assert consume_pending_action(action.token, "s1") is None


def test_run_chat_query_mutating_returns_confirmation_token() -> None:
    with patch("pipeline_chat.service.plan_tool_call") as mock_plan:
        mock_plan.return_value.call = ToolCall("run_module", {"module": "sourcing"})
        mock_plan.return_value.is_mutating = True
        resp = run_chat_query("run sourcing", session_id="abc")
    assert resp.ok is True
    assert resp.requires_confirmation is True
    assert resp.confirmation_token is not None


def test_confirm_chat_action_executes_mutating_tool() -> None:
    with patch("pipeline_chat.service.plan_tool_call") as mock_plan:
        mock_plan.return_value.call = ToolCall("run_module", {"module": "sourcing", "dry_run": True})
        mock_plan.return_value.is_mutating = True
        proposal = run_chat_query("run sourcing", session_id="abc")
    with patch("pipeline_chat.service.execute_mutating_tool", return_value="ok"):
        resp = confirm_chat_action(proposal.confirmation_token or "", session_id="abc")
    assert resp.ok is True
    assert resp.result == "ok"


def test_confirm_chat_action_rejects_invalid_token() -> None:
    resp = confirm_chat_action("bad-token", session_id="abc")
    assert resp.ok is False
    assert "invalid" in (resp.error or "").lower()

