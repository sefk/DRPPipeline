"""
Pipeline chat API routes.

This module is a thin Flask blueprint that delegates orchestration logic to the
top-level `pipeline_chat` package so chat behavior remains pipeline-wide.
"""

from __future__ import annotations

from typing import Any

from flask import Blueprint, request

from pipeline_chat.service import confirm_chat_action, run_chat_query

chat_bp = Blueprint("chat", __name__, url_prefix="/api/chat")
_MAX_MESSAGE_CHARS = 4000


@chat_bp.route("/query", methods=["POST"])
def chat_query() -> Any:
    """
    Resolve a natural-language query to a pipeline tool call and return output.
    """
    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    session_id = (data.get("session_id") or "").strip() or "anon"
    if not message:
        return {"ok": False, "error": "message is required"}, 400
    if len(message) > _MAX_MESSAGE_CHARS:
        return {"ok": False, "error": f"message too long (max {_MAX_MESSAGE_CHARS} chars)"}, 400
    response = run_chat_query(message, session_id=session_id)
    status = 200 if response.ok else 400
    return {
        "ok": response.ok,
        "requires_confirmation": response.requires_confirmation,
        "tool_name": response.tool_name,
        "arguments": response.arguments,
        "confirmation_token": response.confirmation_token,
        "result": response.result,
        "error": response.error,
    }, status


@chat_bp.route("/confirm", methods=["POST"])
def chat_confirm() -> Any:
    """
    Confirm and execute a previously proposed mutating action.
    """
    data = request.get_json(silent=True) or {}
    token = (data.get("confirmation_token") or "").strip()
    session_id = (data.get("session_id") or "").strip() or "anon"
    if not token:
        return {"ok": False, "error": "confirmation_token is required"}, 400
    response = confirm_chat_action(token, session_id=session_id)
    status = 200 if response.ok else 400
    return {
        "ok": response.ok,
        "requires_confirmation": response.requires_confirmation,
        "tool_name": response.tool_name,
        "arguments": response.arguments,
        "confirmation_token": response.confirmation_token,
        "result": response.result,
        "error": response.error,
    }, status

