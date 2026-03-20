"""
Typed request/response shapes for pipeline chat orchestration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ChatQueryRequest:
    """Incoming chat query payload."""

    message: str


@dataclass
class ToolCall:
    """Normalized tool call proposal."""

    tool_name: str
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChatQueryResponse:
    """API response payload for chat query."""

    ok: bool
    requires_confirmation: bool
    tool_name: str | None = None
    arguments: dict[str, Any] | None = None
    confirmation_token: str | None = None
    result: str | None = None
    error: str | None = None

