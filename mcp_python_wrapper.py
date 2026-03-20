"""
Select the Python interpreter for MCP server processes.

Goal:
- If the project has a local virtualenv at `.venv/`, use its python.
  - Windows: `.venv\\Scripts\\python.exe`
  - Unix:    `.venv/bin/python`
- Otherwise, fall back to the system Python used to start this wrapper.

This is used by `.mcp.json` so the MCP client works on both Windows and Unix
without requiring conditional logic in the config.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _pick_venv_python(project_root: Path) -> str | None:
    candidates = [
        project_root / ".venv" / "Scripts" / "python.exe",
        project_root / ".venv" / "Scripts" / "python",
        project_root / ".venv" / "bin" / "python",
        project_root / "venv" / "Scripts" / "python.exe",
        project_root / "venv" / "Scripts" / "python",
        project_root / "venv" / "bin" / "python",
    ]
    for p in candidates:
        try:
            if p.exists():
                return str(p)
        except OSError:
            pass
    return None


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python mcp_python_wrapper.py <server_entry.py> [server args...]")

    project_root = Path(__file__).resolve().parent
    server_entry = Path(sys.argv[1])
    server_args = sys.argv[2:]

    if not server_entry.is_absolute():
        server_entry = project_root / server_entry

    venv_python = _pick_venv_python(project_root)
    python_exe = venv_python or sys.executable

    # Ensure the server runs with the project root as CWD so relative paths
    # (e.g. config.json) resolve correctly.
    env = os.environ.copy()
    subprocess.run(
        [python_exe, str(server_entry), *server_args],
        cwd=str(project_root),
        env=env,
        check=False,
    )


if __name__ == "__main__":
    main()

