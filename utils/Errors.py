"""
Shared error/warning reporting for DRP Pipeline.

Modules report problems here:
- A "crash" is a fatal problem that stops the entire run.
- An "error" aborts the current project but the pipeline continues with the next.
- A "warning" is non-fatal; processing of the current project continues.
"""

from __future__ import annotations

from typing import NoReturn

from storage import Storage
from utils.Logger import Logger


def record_crash(msg: str) -> NoReturn:
    """
    Record a crash: log at exception level and raise so the run stops.

    Use for unrecoverable failures (e.g. DB connection lost, config missing).

    Args:
        msg: Crash message to log and raise.

    Raises:
        RuntimeError: Always, with the given message.
    """
    Logger.exception(msg)
    raise RuntimeError(msg)


def record_error(
    drpid: int,
    error_msg: str,
    *,
    update_storage: bool = True,
    status_value: str = "error",
) -> None:
    """
    Record an error for the current project: abort this project, continue with the next.

    Logs the message, then optionally sets project status and appends to the
    ``errors`` field so the project is skipped in later steps.

    Use ``update_storage=False`` when the record may not exist (e.g. DRPID not found).

    Args:
        drpid: Project DRPID.
        error_msg: Error message to log and persist.
        update_storage: If True, update Storage status and append to errors field.
        status_value: Value to set for the ``status`` column when updating Storage.
    """
    Logger.error(error_msg)

    if not update_storage:
        return

    try:
        Storage.update_record(drpid, {"status": status_value})
        Storage.append_to_field(drpid, "errors", error_msg)
    except Exception as exc:  # pragma: no cover (defensive; Storage impl may vary)
        Logger.exception(f"Failed recording error for DRPID={drpid}: {exc}")


def record_warning(
    drpid: int,
    warning_msg: str,
    *,
    update_storage: bool = True,
) -> None:
    """
    Record a warning for the current project: non-fatal, processing continues.

    Logs the message, then optionally appends to the project's ``warnings`` field.

    Use ``update_storage=False`` when the record may not exist.

    Args:
        drpid: Project DRPID.
        warning_msg: Warning message to log and persist.
        update_storage: If True, append to Storage warnings field.
    """
    Logger.warning(warning_msg)

    if not update_storage:
        return

    try:
        Storage.append_to_field(drpid, "warnings", warning_msg)
    except Exception as exc:  # pragma: no cover (defensive; Storage impl may vary)
        Logger.exception(f"Failed recording warning for DRPID={drpid}: {exc}")

