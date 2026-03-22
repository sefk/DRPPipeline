import { useMemo, useState } from "react";

type ChatItem = {
  id: string;
  role: "user" | "assistant";
  text: string;
  toolName?: string | null;
  result?: string | null;
  error?: string | null;
  confirmationToken?: string | null;
  argumentsJson?: string | null;
  requiresConfirmation?: boolean;
};

type ChatQueryResponse = {
  ok: boolean;
  requires_confirmation: boolean;
  tool_name?: string | null;
  arguments?: Record<string, unknown> | null;
  confirmation_token?: string | null;
  result?: string | null;
  error?: string | null;
};

const CHAT_API = "/api/chat";
const SESSION_KEY = "pipeline_chat_session_id";

function getSessionId(): string {
  try {
    const existing = localStorage.getItem(SESSION_KEY);
    if (existing) return existing;
    const created = `chat-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    localStorage.setItem(SESSION_KEY, created);
    return created;
  } catch {
    return "anon";
  }
}

async function postJson<T>(url: string, body: Record<string, unknown>): Promise<T> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = (data && typeof data.error === "string" && data.error) || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data as T;
}

export function PipelineChatPanel() {
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [items, setItems] = useState<ChatItem[]>([]);

  const sessionId = useMemo(() => getSessionId(), []);

  async function handleSend() {
    const text = message.trim();
    if (!text || busy) return;
    setMessage("");
    const userItem: ChatItem = {
      id: `u-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      role: "user",
      text,
    };
    setItems((prev) => [...prev, userItem]);
    setBusy(true);
    try {
      const data = await postJson<ChatQueryResponse>(`${CHAT_API}/query`, {
        message: text,
        session_id: sessionId,
      });
      const argsJson = data.arguments ? JSON.stringify(data.arguments) : null;
      const assistantItem: ChatItem = {
        id: `a-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        role: "assistant",
        text: data.ok
          ? (data.result || "Done.")
          : (data.error || "Request failed."),
        toolName: data.tool_name || null,
        result: data.result || null,
        error: data.error || null,
        confirmationToken: data.confirmation_token || null,
        argumentsJson: argsJson,
        requiresConfirmation: !!data.requires_confirmation,
      };
      setItems((prev) => [...prev, assistantItem]);
    } catch (err) {
      const assistantItem: ChatItem = {
        id: `a-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        role: "assistant",
        text: err instanceof Error ? `Error: ${err.message}` : "Error: request failed",
        error: err instanceof Error ? err.message : "request failed",
      };
      setItems((prev) => [...prev, assistantItem]);
    } finally {
      setBusy(false);
    }
  }

  async function handleConfirm(token: string) {
    if (!token || busy) return;
    setBusy(true);
    try {
      const data = await postJson<ChatQueryResponse>(`${CHAT_API}/confirm`, {
        confirmation_token: token,
        session_id: sessionId,
      });
      const assistantItem: ChatItem = {
        id: `c-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        role: "assistant",
        text: data.ok ? (data.result || "Confirmed.") : (data.error || "Confirmation failed."),
        toolName: data.tool_name || null,
        result: data.result || null,
        error: data.error || null,
      };
      setItems((prev) =>
        prev.map((x) => (x.confirmationToken === token ? { ...x, requiresConfirmation: false } : x)).concat(assistantItem)
      );
    } catch (err) {
      const assistantItem: ChatItem = {
        id: `c-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        role: "assistant",
        text: err instanceof Error ? `Error: ${err.message}` : "Error: confirmation failed",
        error: err instanceof Error ? err.message : "confirmation failed",
      };
      setItems((prev) => [...prev, assistantItem]);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="pipeline-chat">
      <div className="pipeline-chat-header">Pipeline Chat</div>
      <div className="pipeline-chat-items">
        {items.length === 0 ? (
          <div className="pipeline-chat-empty">
            Ask things like: <code>database status</code> or{" "}
            <code>call list_projects({`{"status":"sourced","limit":5}`})</code>
          </div>
        ) : (
          items.map((item) => (
            <div key={item.id} className={`pipeline-chat-item pipeline-chat-item-${item.role}`}>
              <div className="pipeline-chat-item-role">{item.role === "user" ? "You" : "Assistant"}</div>
              <div className="pipeline-chat-item-text">{item.text}</div>
              {item.toolName ? <div className="pipeline-chat-item-tool">Tool: {item.toolName}</div> : null}
              {item.argumentsJson ? <div className="pipeline-chat-item-args">Args: {item.argumentsJson}</div> : null}
              {item.requiresConfirmation && item.confirmationToken ? (
                <div className="pipeline-chat-actions">
                  <button type="button" onClick={() => handleConfirm(item.confirmationToken!)} disabled={busy}>
                    Confirm action
                  </button>
                </div>
              ) : null}
            </div>
          ))
        )}
      </div>
      <div className="pipeline-chat-input-row">
        <input
          type="text"
          value={message}
          placeholder="Ask pipeline chat..."
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              handleSend();
            }
          }}
          disabled={busy}
        />
        <button type="button" onClick={handleSend} disabled={busy || !message.trim()}>
          Send
        </button>
      </div>
    </div>
  );
}

