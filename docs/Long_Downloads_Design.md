# Long downloads: design alternatives

Some datasets are 10GB+ and can take 20+ minutes to download. Blocking the pipeline on a single download is undesirable. Below are alternatives so the main flow can continue with the next project while long downloads complete in the background.

## Current behavior

- **Orchestrator** runs projects **sequentially**: for each eligible project it calls `module_instance.run(drpid)` and waits for it to finish before starting the next.
- **SocrataCollector** for each project: validates URL, creates folder, opens browser, generates PDF, then calls **SocrataDatasetDownloader.download()**, which clicks Export → Download and uses Playwright’s `expect_download()` + `save_as()`. That save is synchronous and blocks until the full file is written (default timeout 60s; large files need a much higher timeout).
- **Storage** is already multi-thread friendly (SQLite with `check_same_thread=False`, WAL, busy_timeout).
- **Logger** uses a single class-level `_current_drpid`; for concurrent execution it needs thread-local drpid so each worker logs with its own project id.

---

## Option 1: Thread pool in the Orchestrator (recommended first step)

**Idea:** Run **N projects concurrently** (e.g. 2–4). Each worker runs the full collector (browser, PDF, download) for one project. One long download no longer blocks the rest of the pipeline because other workers are handling other projects.

**Changes:**

- **Orchestrator:** For modules with a prereq, submit each `run(drpid)` to a `concurrent.futures.ThreadPoolExecutor` (e.g. `max_workers=2` or from `Args.max_workers`) instead of a simple `for` loop. Wait for all futures to complete (or process as completed and log errors).
- **Logger:** Use **thread-local** storage for `_current_drpid` so each worker’s logs are tagged with the correct DRPID. Each worker should call `Logger.set_current_drpid(drpid)` at the start of its task and `Logger.clear_current_drpid()` in a `finally` (or the Orchestrator wrapper does that).
- **SocrataDatasetDownloader:** Increase the download **timeout** for large files (e.g. 30 minutes or configurable). Default 60s is too low for 10GB.

**Pros:** Small change set; no new “download-only” path; pipeline naturally overlaps work.  
**Cons:** N browser instances and N concurrent downloads (memory/bandwidth); still one long download per worker.

---

## Option 2: Deferred download queue (background workers)

**Idea:** For each project, the **main thread** does only the quick work (URL check, folder, browser, PDF, metadata). When it’s time to download, it **enqueues a job** (drpid, source_url, folder_path, and any needed context) and continues to the next project. One or more **background threads** (each with its own browser) pull jobs from the queue, open the page, click Export → Download, `save_as()`, then update Storage. Main thread never blocks on a long download.

**Changes:**

- **Collector:** Split into “quick phase” (PDF + metadata) and “download phase”. After the quick phase, push a download job (e.g. `(drpid, source_url, folder_path)`) onto a shared queue and write status like `"collected - file pending"`. Return so the Orchestrator can continue.
- **Download workers:** Dedicated thread(s) that loop: pop a job, open browser, load `source_url`, click Export → Download, save to `folder_path`, update Storage (file_size, download_date, status → `"collector"`), then close browser and repeat.
- **Orchestrator:** After dispatching all projects, **join** the download workers (or wait until the queue is empty and workers idle) so the process doesn’t exit with pending downloads.
- **Logger:** Thread-local drpid in workers (same as Option 1).

**Pros:** Main loop stays fast; long downloads are isolated; you can tune the number of download workers and timeouts independently.  
**Cons:** More code (queue, worker loop, download-only flow); each worker needs a full browser session per download.

---

## Option 3: Two-phase run (batch “collect” then “download”)

**Idea:** **Phase 1:** Run the collector with an option to **skip** the actual download for large (or all) datasets: do PDF + metadata, set status to `"collected - file pending"`. **Phase 2:** A separate run (or script) that only processes records with status `"collected - file pending"`, using a thread/process pool and long timeouts to perform only the download step (open page, Export, Download, save, update Storage).

**Changes:**

- **Collector:** Add a flag or heuristic (e.g. “large dataset” warning, or “always defer download”) to skip `SocrataDatasetDownloader.download()` and set status to `"collected - file pending"`.
- **New “download” module or script:** List projects with status `"collected - file pending"`, then for each (or in parallel via pool) run the download-only flow (browser → Export → Download → save → update). Use a long timeout (e.g. 30 min) and optionally retries.

**Pros:** Fits cron/batch workflows; main pipeline stays fast; large downloads can be run in a separate, tuned environment.  
**Cons:** Two passes; need a robust “download-only” path and possibly a way to get the exact download URL/session if the site requires it.

---

## Option 4: Increase timeout only (minimal change)

**Idea:** Keep the current sequential flow but **increase** the download timeout (e.g. to 30 minutes) so 10GB+ downloads don’t fail with a timeout. Pipeline still blocks on each project until its download finishes.

**Changes:**

- **SocrataDatasetDownloader:** Use a much larger `timeout` (e.g. 30 * 60 * 1000 ms) or make it configurable via Args.

**Pros:** Trivial change.  
**Cons:** Pipeline still waits 20+ minutes per large project; no overlap with the next project.

---

## Recommendation

- **Short term:** Implement **Option 1** (Orchestrator thread pool + thread-local Logger + higher download timeout). That gives immediate overlap: multiple projects in flight so one long download doesn’t stall everything.
- **If needed later:** Add **Option 2** (deferred download queue) so the main thread never waits on any download; or use **Option 3** for a dedicated “download phase” in batch/cron.

Next step: implement Option 1 (thread pool in Orchestrator, thread-local Logger, configurable download timeout).
