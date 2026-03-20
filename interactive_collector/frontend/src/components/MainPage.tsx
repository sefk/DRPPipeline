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
      setLogOutput((prev) => prev ? `${prev}\n--- Running ${module} ---\n` : `--- Running ${module} ---\n`);
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
          setLogOutput((prev) => prev + "\n(No response body)\n");
          setRunning(false);
          return;
        }
        const dec = new TextDecoder();
        let acc = "";
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          acc += dec.decode(value, { stream: true });
          setLogOutput((prev) => prev + acc);
          acc = "";
        }
        if (acc) setLogOutput((prev) => prev + acc);
        setLogOutput((prev) => prev + "\n--- Done ---\n");
      } catch (e) {
        if ((e as Error).name === "AbortError") {
          setLogOutput((prev) => prev + "\n--- Stopped ---\n");
        } else {
          setError(e instanceof Error ? e.message : "Run failed");
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

  const clearLog = useCallback(() => setLogOutput(""), []);

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
