"""
In-memory pending confirmation store for mutating chat actions.
"""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass

from pipeline_chat.schemas import ToolCall

_DEFAULT_TTL_SECONDS = 600


@dataclass
class PendingAction:
    token: str
    session_id: str
    call: ToolCall
    created_at: float
    expires_at: float


_LOCK = threading.Lock()
_PENDING: dict[str, PendingAction] = {}


def create_pending_action(session_id: str, call: ToolCall, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> PendingAction:
    now = time.time()
    action = PendingAction(
        token=secrets.token_urlsafe(24),
        session_id=session_id,
        call=call,
        created_at=now,
        expires_at=now + ttl_seconds,
    )
    with _LOCK:
        _PENDING[action.token] = action
    return action


def consume_pending_action(token: str, session_id: str) -> PendingAction | None:
    now = time.time()
    with _LOCK:
        action = _PENDING.get(token)
        if not action:
            return None
        if action.session_id != session_id:
            return None
        if action.expires_at < now:
            _PENDING.pop(token, None)
            return None
        _PENDING.pop(token, None)
        return action


def cancel_pending_action(token: str, session_id: str) -> bool:
    with _LOCK:
        action = _PENDING.get(token)
        if not action or action.session_id != session_id:
            return False
        _PENDING.pop(token, None)
        return True

