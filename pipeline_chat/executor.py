"""
Tool executor for pipeline chat.

Phase 1 supports read-only MCP-1 tool calls via direct function calls into
`mcp_server.server` with explicit allowlisting and argument validation.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import inspect
from typing import Any, Callable

from mcp_server import server as mcp_server


class ToolExecutionError(RuntimeError):
    """Raised when a requested tool cannot be safely executed."""


_TOOL_TIMEOUT_SECONDS = int(os.environ.get("PIPELINE_CHAT_TOOL_TIMEOUT_SECONDS", "120"))


READ_ONLY_TOOLS: dict[str, Callable[..., str]] = {
    "get_pipeline_stats": mcp_server.get_pipeline_stats,
    "list_projects": mcp_server.list_projects,
    "get_project": mcp_server.get_project,
    "preview_sourcing": mcp_server.preview_sourcing,
    "verify_module_run": mcp_server.verify_module_run,
    "check_project_files": mcp_server.check_project_files,
}

MUTATING_TOOLS: dict[str, Callable[..., str]] = {
    "run_module": mcp_server.run_module,
    "update_project": mcp_server.update_project,
    "clear_errors": mcp_server.clear_errors,
    "set_project_status": mcp_server.set_project_status,
    "delete_project": mcp_server.delete_project,
}


def list_read_only_tools() -> list[str]:
    """Return sorted read-only tool names."""
    return sorted(READ_ONLY_TOOLS.keys())


def list_mutating_tools() -> list[str]:
    """Return sorted mutating tool names."""
    return sorted(MUTATING_TOOLS.keys())


def list_all_tools() -> list[str]:
    """Return sorted union of read-only and mutating tools."""
    return sorted(set(READ_ONLY_TOOLS.keys()) | set(MUTATING_TOOLS.keys()))


def is_mutating_tool(tool_name: str) -> bool:
    """Return True when tool_name is mutating."""
    return tool_name in MUTATING_TOOLS


def execute_read_only_tool(tool_name: str, arguments: dict[str, Any] | None = None) -> str:
    """
    Execute an allowlisted read-only tool.

    Args:
        tool_name: MCP 1 tool function name.
        arguments: Tool argument dict.

    Returns:
        Tool response string.

    Raises:
        ToolExecutionError: for unknown tools, invalid args, or runtime failure.
    """
    if tool_name not in READ_ONLY_TOOLS:
        raise ToolExecutionError(
            f"Tool {tool_name!r} is not allowed in read-only mode. "
            f"Allowed: {list_read_only_tools()}"
        )
    fn = READ_ONLY_TOOLS[tool_name]
    kwargs = arguments or {}
    try:
        inspect.signature(fn).bind(**kwargs)
    except TypeError as exc:
        raise ToolExecutionError(f"Invalid arguments for {tool_name!r}: {exc}") from exc

    return _invoke_with_timeout(tool_name, fn, kwargs)


def execute_mutating_tool(tool_name: str, arguments: dict[str, Any] | None = None) -> str:
    """
    Execute an allowlisted mutating tool.
    """
    if tool_name not in MUTATING_TOOLS:
        raise ToolExecutionError(
            f"Tool {tool_name!r} is not allowed in mutating mode. "
            f"Allowed: {list_mutating_tools()}"
        )
    fn = MUTATING_TOOLS[tool_name]
    kwargs = dict(arguments or {})

    # All MCP mutating tools default to `dry_run=True`. After explicit user
    # confirmation, we want the mutation to actually execute unless the
    # caller explicitly opted into another mode.
    try:
        sig = inspect.signature(fn)
        if "dry_run" in sig.parameters and "dry_run" not in kwargs:
            kwargs["dry_run"] = False
    except Exception:
        pass
    try:
        inspect.signature(fn).bind(**kwargs)
    except TypeError as exc:
        raise ToolExecutionError(f"Invalid arguments for {tool_name!r}: {exc}") from exc

    return _invoke_with_timeout(tool_name, fn, kwargs)


def _invoke_with_timeout(tool_name: str, fn: Callable[..., Any], kwargs: dict[str, Any]) -> str:
    """
    Run tool function with timeout guard.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(fn, **kwargs)
        try:
            value = fut.result(timeout=_TOOL_TIMEOUT_SECONDS)
            return str(value)
        except FuturesTimeoutError as exc:
            raise ToolExecutionError(
                f"Tool {tool_name!r} timed out after {_TOOL_TIMEOUT_SECONDS}s"
            ) from exc
        except Exception as exc:  # pragma: no cover - defensive
            raise ToolExecutionError(f"Tool {tool_name!r} failed: {exc}") from exc

