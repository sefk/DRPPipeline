"""
LLM planner for converting English requests into tool calls.

Phase 2 uses an OpenAI backend with strict JSON output validation.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import requests

from pipeline_chat.executor import is_mutating_tool, list_all_tools
from pipeline_chat.schemas import ToolCall


class PlannerError(RuntimeError):
    """Raised when planning fails or returns invalid structure."""


@dataclass
class PlannerDecision:
    """Validated planner decision."""

    call: ToolCall
    is_mutating: bool


def _system_prompt() -> str:
    tools = ", ".join(list_all_tools())
    return (
        "You are a tool planner for the DRP pipeline. "
        "Select exactly one tool and arguments based on the user request. "
        "Return strict JSON object with keys: tool_name (string), arguments (object). "
        f"Allowed tools: {tools}. "
        "Do not include markdown or extra text."
    )


def _validate_planner_json(payload: Any) -> PlannerDecision:
    if not isinstance(payload, dict):
        raise PlannerError("Planner output must be a JSON object.")
    tool_name = payload.get("tool_name")
    arguments = payload.get("arguments", {})
    if not isinstance(tool_name, str) or not tool_name.strip():
        raise PlannerError("Planner output missing valid tool_name.")
    tool_name = tool_name.strip()
    allowed = set(list_all_tools())
    if tool_name not in allowed:
        raise PlannerError(f"Tool {tool_name!r} is not allowed. Allowed: {sorted(allowed)}")
    if not isinstance(arguments, dict):
        raise PlannerError("Planner output 'arguments' must be an object.")
    return PlannerDecision(call=ToolCall(tool_name=tool_name, arguments=arguments), is_mutating=is_mutating_tool(tool_name))


def _call_openai_json(user_message: str) -> dict[str, Any]:
    api_key = os.environ.get("PIPELINE_CHAT_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise PlannerError("OpenAI API key not configured. Set PIPELINE_CHAT_OPENAI_API_KEY or OPENAI_API_KEY.")
    model = os.environ.get("PIPELINE_CHAT_OPENAI_MODEL", "gpt-4o-mini")
    body = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _system_prompt()},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0,
    }
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=30,
    )
    if resp.status_code >= 400:
        raise PlannerError(f"OpenAI request failed ({resp.status_code}): {resp.text[:300]}")
    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except Exception as exc:
        raise PlannerError(f"Failed to parse OpenAI planner response: {exc}") from exc
    if not isinstance(parsed, dict):
        raise PlannerError("Planner response JSON must be an object.")
    return parsed


def plan_tool_call(user_message: str) -> PlannerDecision:
    """
    Convert natural-language input to validated tool call decision.
    """
    parsed = _call_openai_json(user_message)
    return _validate_planner_json(parsed)

