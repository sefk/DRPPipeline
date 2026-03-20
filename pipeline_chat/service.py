"""
Phase-1 chat service.

Implements deterministic mapping from plain-English requests to read-only MCP 1
tool calls. LLM-based planning is added in a later phase.
"""

from __future__ import annotations

import json
import re
from typing import Any

from pipeline_chat.confirmations import consume_pending_action, create_pending_action
from pipeline_chat.executor import (
    ToolExecutionError,
    execute_mutating_tool,
    execute_read_only_tool,
    list_mutating_tools,
    list_read_only_tools,
)
from pipeline_chat.planner import PlannerError, plan_tool_call
from pipeline_chat.schemas import ChatQueryResponse, ToolCall
from utils.Logger import Logger

_CALL_RE = re.compile(r"^\s*call\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\((.*)\))?\s*$", re.IGNORECASE)


def _parse_call_syntax(message: str) -> ToolCall | None:
    """
    Parse `call tool_name(...)` message syntax.

    Args in parens support either:
      - empty
      - JSON object: `{"limit": 10}`
    """
    m = _CALL_RE.match(message or "")
    if not m:
        return None
    tool_name = m.group(1)
    args_text = (m.group(2) or "").strip()
    if not args_text:
        return ToolCall(tool_name=tool_name, arguments={})
    try:
        # Accept JSON dict in parentheses for explicit argument passing.
        parsed = json.loads(args_text)
        if not isinstance(parsed, dict):
            return None
        return ToolCall(tool_name=tool_name, arguments=parsed)
    except json.JSONDecodeError:
        return None


def _heuristic_tool_selection(message: str) -> ToolCall | None:
    """
    Minimal deterministic mapping for phase 1.
    """
    s = (message or "").strip().lower()
    if not s:
        return None
    if "database status" in s or "pipeline stats" in s:
        return ToolCall(tool_name="get_pipeline_stats", arguments={})
    if "next eligible" in s and "collection" in s:
        return ToolCall(tool_name="list_projects", arguments={"status": "sourced", "limit": 1, "offset": 0})
    if s == "list tools" or "available tools" in s:
        return ToolCall(tool_name="list_projects", arguments={"limit": 5, "offset": 0})
    return None


def run_chat_query(message: str, session_id: str = "anon") -> ChatQueryResponse:
    """
    Resolve a chat message to a read-only tool call and execute it.
    """
    explicit_call = _parse_call_syntax(message)
    if explicit_call:
        call = explicit_call
        is_mutating = False
    else:
        try:
            decision = plan_tool_call(message)
            call = decision.call
            is_mutating = decision.is_mutating
        except PlannerError:
            heuristic_call = _heuristic_tool_selection(message)
            if heuristic_call:
                call = heuristic_call
                is_mutating = False
            else:
                call = None
                is_mutating = False
    if not call:
        return ChatQueryResponse(
            ok=False,
            requires_confirmation=False,
            error=(
                "I could not map that request to a tool yet. "
                "Try: call get_pipeline_stats() or call list_projects({\"status\":\"sourced\",\"limit\":10})."
            ),
        )
    if is_mutating:
        pending = create_pending_action(session_id=session_id, call=call)
        _audit("proposal_created", session_id, call.tool_name, call.arguments, True)
        return ChatQueryResponse(
            ok=True,
            requires_confirmation=True,
            tool_name=call.tool_name,
            arguments=call.arguments,
            confirmation_token=pending.token,
            result=(
                f"Proposed mutating action: {call.tool_name}({json.dumps(call.arguments)}). "
                f"Confirm to execute."
            ),
        )
    try:
        result = execute_read_only_tool(call.tool_name, call.arguments)
        _audit("read_executed", session_id, call.tool_name, call.arguments, True)
        return ChatQueryResponse(
            ok=True,
            requires_confirmation=False,
            tool_name=call.tool_name,
            arguments=call.arguments,
            result=result,
        )
    except ToolExecutionError as exc:
        _audit("read_failed", session_id, call.tool_name, call.arguments, False, str(exc))
        return ChatQueryResponse(
            ok=False,
            requires_confirmation=False,
            tool_name=call.tool_name,
            arguments=call.arguments,
            error=f"{exc}. Read-only tools: {list_read_only_tools()}",
        )


def confirm_chat_action(confirmation_token: str, session_id: str = "anon") -> ChatQueryResponse:
    """
    Execute previously proposed mutating action after explicit confirmation.
    """
    action = consume_pending_action(confirmation_token, session_id=session_id)
    if not action:
        _audit("confirm_rejected", session_id, None, None, False, "invalid_or_expired_token")
        return ChatQueryResponse(
            ok=False,
            requires_confirmation=False,
            error="Confirmation token is invalid, expired, or not owned by this session.",
        )
    try:
        result = execute_mutating_tool(action.call.tool_name, action.call.arguments)
        _audit("mutating_executed", session_id, action.call.tool_name, action.call.arguments, True)
        return ChatQueryResponse(
            ok=True,
            requires_confirmation=False,
            tool_name=action.call.tool_name,
            arguments=action.call.arguments,
            result=result,
        )
    except ToolExecutionError as exc:
        _audit("mutating_failed", session_id, action.call.tool_name, action.call.arguments, False, str(exc))
        return ChatQueryResponse(
            ok=False,
            requires_confirmation=False,
            tool_name=action.call.tool_name,
            arguments=action.call.arguments,
            error=f"{exc}. Mutating tools: {list_mutating_tools()}",
        )


def _audit(
    event: str,
    session_id: str,
    tool_name: str | None,
    arguments: dict[str, Any] | None,
    ok: bool,
    error: str | None = None,
) -> None:
    """
    Structured audit logging for pipeline chat tool usage.
    """
    payload = {
        "event": event,
        "session_id": session_id,
        "tool_name": tool_name,
        "arguments": arguments or {},
        "ok": ok,
        "error": error or "",
    }
    try:
        Logger.info("pipeline_chat_audit %s", json.dumps(payload, sort_keys=True))
    except Exception:
        # Do not fail chat flows due to logging issues.
        pass

