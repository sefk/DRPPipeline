/**
 * Main page: run pipeline modules (CLI replacement).
 *
 * Left: module controls (Start DRPID, Max rows, Log level, Max workers) and module buttons.
 * Right: either Log output pane (when running other modules) or Collector pane (Scoreboard,
 * Metadata, Copy & Open in top rail) when "Interactive collector" is active.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { CollectorRightPane } from "./CollectorRightPane";
import { PipelineChatPanel } from "./PipelineChatPanel";
import { useCollectorStore } from "../store";

const API = "/api/pipeline";
const LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR"] as const;

export function MainPage() {
  const [modules, setModules] = useState<string[]>([]);
  const [startDrpid, setStartDrpid] = useState<string>("");
  const [maxRows, setMaxRows] = useState<string>("");
  const [logLevel, setLogLevel] = useState<string>("INFO");
  const [maxWorkers, setMaxWorkers] = useState<string>("1");
  const [logOutput, setLogOutput] = useState<string>("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rightPaneMode, setRightPaneMode] = useState<"log" | "collector">("log");
  const logEndRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  /** Canonical log text; stream appends here then copies to state so chunks are never dropped by batched functional updates. */
  const logTextRef = useRef<string>("");

  const { loadProject, loadFirstProject } = useCollectorStore();

  useEffect(() => {
    fetch(`${API}/modules`)
      .then((r) => r.json())
      .then((data: { modules: string[] }) => setModules(data.modules || []))
      .catch(() => setModules([]));
  }, []);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logOutput]);

  const runModule = useCallback(
    async (module: string) => {
      if (module === "interactive_collector") {
        setRightPaneMode("collector");
        const start = startDrpid.trim();
        const id = start ? parseInt(start, 10) : NaN;
        if (!isNaN(id)) {
          loadProject(id);
        } else {
          loadFirstProject();
        }
        return;
      }
      setRightPaneMode("log");
      setRunning(true);
      setError(null);
      abortRef.current = new AbortController();
      logTextRef.current = logTextRef.current
        ? `${logTextRef.current}\n--- Running ${module} ---\n`
        : `--- Running ${module} ---\n`;
      setLogOutput(logTextRef.current);
      const body: Record<string, unknown> = { module };
      const num = maxRows.trim();
      if (num) {
        const n = parseInt(num, 10);
        if (!isNaN(n)) body.num_rows = n;
      }
      const start = startDrpid.trim();
      if (start) {
        const s = parseInt(start, 10);
        if (!isNaN(s)) body.start_drpid = s;
      }
      if (logLevel) body.log_level = logLevel;
      const mw = maxWorkers.trim();
      if (mw) {
        const w = parseInt(mw, 10);
        if (!isNaN(w)) body.max_workers = w;
      }
      try {
        const res = await fetch(`${API}/run`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
          signal: abortRef.current.signal,
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          setError((err as { error?: string }).error || `HTTP ${res.status}`);
          setRunning(false);
          return;
        }
        const reader = res.body?.getReader();
        if (!reader) {
          logTextRef.current += "\n(No response body)\n";
          setLogOutput(logTextRef.current);
          setRunning(false);
          return;
        }
        const dec = new TextDecoder();
        /** Buffer incomplete NDJSON lines (TCP chunks can split mid-line). */
        let ndBuf = "";
        const appendLogText = (s: string) => {
          logTextRef.current += s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
        };
        const appendNdjsonChunk = (chunk: string) => {
          ndBuf += chunk;
          let cut: number;
          while ((cut = ndBuf.indexOf("\n")) >= 0) {
            const row = ndBuf.slice(0, cut).trimEnd();
            ndBuf = ndBuf.slice(cut + 1);
            if (!row) continue;
            try {
              const o = JSON.parse(row) as { line?: string; ping?: boolean };
              if (o.ping) continue;
              if (typeof o.line === "string") appendLogText(o.line);
            } catch {
              /* Plain-text fallback if a line is not valid NDJSON. */
              appendLogText(row + "\n");
            }
          }
          setLogOutput(logTextRef.current);
        };
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          appendNdjsonChunk(dec.decode(value, { stream: true }));
        }
        appendNdjsonChunk(dec.decode());
        if (ndBuf.trim()) {
          const rest = ndBuf.trim();
          try {
            const o = JSON.parse(rest) as { line?: string; ping?: boolean };
            if (!o.ping && typeof o.line === "string") appendLogText(o.line);
          } catch {
            appendLogText(rest + "\n");
          }
        }
        logTextRef.current += "\n--- Done ---\n";
        setLogOutput(logTextRef.current);
      } catch (e) {
        if ((e as Error).name === "AbortError") {
          logTextRef.current += "\n--- Stopped ---\n";
          setLogOutput(logTextRef.current);
        } else {
          const msg = e instanceof Error ? e.message : "Run failed";
          const hint =
            /failed to fetch|networkerror|load failed|connection reset/i.test(msg)
              ? " Long pipeline runs (upload/publisher) often hit this if Flask restarts: use `flask run --debug --no-reload`."
              : "";
          setError(msg + hint);
        }
      } finally {
        setRunning(false);
        abortRef.current = null;
      }
    },
    [startDrpid, maxRows, logLevel, maxWorkers, loadProject, loadFirstProject]
  );

  const stopRun = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    fetch(`${API}/stop`, { method: "POST" }).catch(() => {});
  }, []);

  const clearLog = useCallback(() => {
    logTextRef.current = "";
    setLogOutput("");
  }, []);

  const rightPane =
    rightPaneMode === "log" ? (
      <div className="main-page-log-pane">
        <div className="main-page-log-toolbar">
          <span>Log output</span>
          <span>
            <button type="button" onClick={clearLog} disabled={running}>
              Clear
            </button>
            {running && (
              <button type="button" className="main-page-stop-btn" onClick={stopRun}>
                Stop
              </button>
            )}
          </span>
        </div>
        {error && <div className="main-page-log-error">{error}</div>}
        <pre className="main-page-log-pre">
          {logOutput || "(Run a module to see output.)"}
          <div ref={logEndRef} />
        </pre>
      </div>
    ) : (
      <CollectorRightPane onShowLog={() => setRightPaneMode("log")} />
    );

  return (
    <div className="main-page">
      <header className="main-page-header">
        <h1 className="main-page-title">DRP Pipeline</h1>
      </header>
      <div className="main-page-body">
        <div className="main-page-controls">
          <div className="main-page-field">
            <label htmlFor="start_drpid">Start DRPID (blank = from first)</label>
            <input
              id="start_drpid"
              type="number"
              min={1}
              placeholder=""
              value={startDrpid}
              onChange={(e) => setStartDrpid(e.target.value)}
              disabled={running}
            />
          </div>
          <div className="main-page-field">
            <label htmlFor="max_rows">Max rows (blank = unlimited)</label>
            <input
              id="max_rows"
              type="number"
              min={0}
              placeholder="unlimited"
              value={maxRows}
              onChange={(e) => setMaxRows(e.target.value)}
              disabled={running}
            />
          </div>
          <div className="main-page-field">
            <label htmlFor="log_level">Log level</label>
            <select
              id="log_level"
              value={logLevel}
              onChange={(e) => setLogLevel(e.target.value)}
              disabled={running}
            >
              {LOG_LEVELS.map((l) => (
                <option key={l} value={l}>
                  {l}
                </option>
              ))}
            </select>
          </div>
          <div className="main-page-field">
            <label htmlFor="max_workers">Max workers</label>
            <input
              id="max_workers"
              type="number"
              min={1}
              value={maxWorkers}
              onChange={(e) => setMaxWorkers(e.target.value)}
              disabled={running}
            />
          </div>
          <div className="main-page-buttons">
            {modules.map((mod) => (
              <button
                key={mod}
                type="button"
                onClick={() => runModule(mod)}
                disabled={running && mod !== "interactive_collector"}
                title={mod === "interactive_collector" ? "Open Interactive Collector in right pane" : `Run module: ${mod}`}
              >
                {mod === "interactive_collector" ? "Interactive collector" : mod}
              </button>
            ))}
          </div>
          <PipelineChatPanel />
        </div>
        {rightPane}
      </div>
    </div>
  );
}
