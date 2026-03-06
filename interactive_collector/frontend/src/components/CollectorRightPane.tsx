/**
 * CollectorRightPane - Renders in the main page right pane when "Interactive collector" is active.
 *
 * Top rail: Show log, Copy & Open, DRPID, Next, Load DRPID, No Links, Save.
 * Below: Scoreboard, then Metadata.
 */
import { useCallback, useEffect, useState } from "react";
import { Scoreboard } from "./Scoreboard";
import { MetadataForm } from "./MetadataForm";
import { useCollectorStore } from "../store";

interface CollectorRightPaneProps {
  onShowLog: () => void;
}

export function CollectorRightPane({ onShowLog }: CollectorRightPaneProps) {
  const {
    drpid,
    sourceUrl,
    folderPath,
    loadProject,
    loadNext,
    save,
    setMetadata,
    setNoLinks,
    openSkipModal,
    loading,
    refreshScoreboard,
    startDownloadsWatcher,
    downloadsWatcherActive,
    stopDownloadsWatcher,
  } = useCollectorStore();
  const [toast, setToast] = useState<string | null>(null);

  useEffect(() => {
    const updateStatus = () => {
      fetch("/api/downloads-watcher/status")
        .then((r) => r.json())
        .then((data) => {
          useCollectorStore.setState({ downloadsWatcherActive: !!data.watching });
        })
        .catch(() => {});
    };
    updateStatus();
    const interval = setInterval(updateStatus, 3000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (drpid == null) return;
    refreshScoreboard();
    const interval = setInterval(refreshScoreboard, 2000);
    return () => clearInterval(interval);
  }, [drpid, refreshScoreboard]);

  useEffect(() => {
    if (drpid == null) return;
    const poll = () => {
      fetch(`/api/metadata-from-page?drpid=${drpid}`)
        .then((r) => r.json())
        .then((data: { metadata?: Record<string, string> }) => {
          const fromPage = data.metadata;
          if (!fromPage || Object.keys(fromPage).length === 0) return;
          const state = useCollectorStore.getState();
          const current = state.metadata;
          const updates: Partial<typeof current> = {};
          const keys = ["title", "summary", "keywords", "agency", "time_start", "time_end", "download_date"] as const;
          for (const k of keys) {
            const v = fromPage[k];
            if (v && !(current[k] || "").trim()) updates[k] = v;
          }
          if (Object.keys(updates).length) setMetadata(updates);
        })
        .catch(() => {});
    };
    poll();
    const interval = setInterval(poll, 4000);
    return () => clearInterval(interval);
  }, [drpid, setMetadata]);

  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 3000);
    return () => clearTimeout(t);
  }, [toast]);

  const copyAndOpen = useCallback(async () => {
    if (!sourceUrl || !drpid || !/^https?:\/\//.test(sourceUrl)) {
      setToast("Load a project with a source URL first.");
      return;
    }
    const launcher = `${window.location.origin}/extension/launcher?drpid=${drpid}&url=${encodeURIComponent(sourceUrl)}`;
    // Open window first (synchronously) so it’s in the user gesture chain and not blocked as a popup.
    const a = document.createElement("a");
    a.href = launcher;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    document.body.appendChild(a);
    a.click();
    a.remove();
    try {
      await startDownloadsWatcher();
      await navigator.clipboard.writeText(launcher);
      setToast("Copied and opened in new window. If it didn’t open, check popup blocker or paste from clipboard.");
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Watcher could not start";
      try {
        await navigator.clipboard.writeText(launcher);
        setToast(`URL copied. Watcher failed: ${msg}`);
      } catch {
        setToast(`Watcher failed: ${msg}. Paste this URL in browser: ${launcher.slice(0, 50)}…`);
      }
    }
    try {
      const halfW = Math.floor((typeof screen !== "undefined" ? screen.availWidth : 1920) / 2);
      const availH = typeof screen !== "undefined" ? screen.availHeight : 1000;
      const availX = typeof screen !== "undefined" ? (screen as { availLeft?: number }).availLeft ?? 0 : 0;
      const availY = typeof screen !== "undefined" ? (screen as { availTop?: number }).availTop ?? 0 : 0;
      window.resizeTo(halfW, availH);
      window.moveTo(availX, availY);
    } catch {
      /* ignore */
    }
  }, [sourceUrl, drpid, startDownloadsWatcher]);

  const onLoadDrpidSubmit = useCallback(
    (e: React.FormEvent<HTMLFormElement>) => {
      e.preventDefault();
      const form = e.currentTarget;
      const val = (form.querySelector('input[name="load_drpid"]') as HTMLInputElement)?.value?.trim();
      if (val) {
        const id = parseInt(val, 10);
        if (!isNaN(id)) loadProject(id);
      }
    },
    [loadProject]
  );

  return (
    <div className="collector-right-pane">
      <div className="collector-rail">
        <button type="button" className="btn-top" onClick={onShowLog}>
          Show log
        </button>
        {drpid != null && (
          <>
            <span className="drpid">DRPID: {drpid}</span>
            <button type="button" className="btn-top" onClick={loadNext}>
              Next
            </button>
          </>
        )}
        <form className="top-form" onSubmit={onLoadDrpidSubmit}>
          <label htmlFor="collector_load_drpid">Load DRPID</label>
          <input
            type="number"
            id="collector_load_drpid"
            name="load_drpid"
            placeholder="e.g. 1"
            min={1}
            max={99999}
            className="top-input-drpid"
          />
          <button type="submit" className="btn-top">
            Load
          </button>
        </form>
        {sourceUrl && drpid != null && (
          <button
            type="button"
            className="btn-top btn-copy-open"
            onClick={copyAndOpen}
            title="Copy launcher URL to paste in extended browser (with extension)"
          >
            Copy &amp; Open
          </button>
        )}
        {folderPath && (
          <>
            <button
              type="button"
              className="btn-top"
              title="No live links"
              onClick={setNoLinks}
            >
              No Links
            </button>
            <button
              type="button"
              className="btn-top"
              title="Skip project with reason (sets status to collector hold)"
              onClick={openSkipModal}
              disabled={loading}
            >
              Skip
            </button>
            <button
              type="button"
              className="btn-top"
              title="Save metadata to database"
              onClick={save}
              disabled={loading}
            >
              Save
            </button>
          </>
        )}
        {drpid != null && (
          downloadsWatcherActive ? (
            <button
              type="button"
              className="collector-status-collecting-btn"
              onClick={() => stopDownloadsWatcher()}
              title="Save as PDF and downloads capturing are on. Click to turn off (e.g. for debugging)."
            >
              Collecting
            </button>
          ) : (
            <span className="collector-status-not-collecting" title="Click Copy & Open to start capturing">
              Not collecting
            </span>
          )
        )}
        {toast && <span className="collector-rail-toast">{toast}</span>}
      </div>
      {drpid == null && !loading && (
        <div className="collector-empty-state">
          No project loaded. Use <strong>Load DRPID</strong> above to open a project, or run the{" "}
          <strong>sourcing</strong> module from the left to add candidates.
        </div>
      )}
      {drpid != null && (
        <div className="collector-right-content">
          <div className="collector-scoreboard-wrap">
            <Scoreboard />
          </div>
          <div className="collector-metadata-wrap">
            <MetadataForm />
          </div>
        </div>
      )}
    </div>
  );
}
