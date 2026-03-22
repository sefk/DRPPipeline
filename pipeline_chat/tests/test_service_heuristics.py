"""
Unit tests for pipeline_chat.service deterministic heuristics.
"""

from __future__ import annotations

from pipeline_chat.service import run_chat_query


def test_reset_drpid_eligible_proposes_set_project_status() -> None:
    # OpenAI planner may be disabled; this should fall back to deterministic heuristics.
    resp = run_chat_query("reset drpid21 so it is eligible for collection", session_id="s1")
    assert resp.ok is True
    assert resp.requires_confirmation is True
    assert resp.tool_name == "set_project_status"
    assert resp.arguments is not None
    assert resp.arguments.get("drpid") == 21
    assert resp.arguments.get("status") == "sourced"
    assert resp.confirmation_token is not None


def test_explicit_call_mutating_requires_confirmation() -> None:
    resp = run_chat_query("call clear_errors({\"drpid\": 21})", session_id="s1")
    assert resp.ok is True
    assert resp.requires_confirmation is True
    assert resp.tool_name == "clear_errors"
    assert resp.arguments is not None
    assert resp.arguments.get("drpid") == 21
    assert resp.confirmation_token is not None

