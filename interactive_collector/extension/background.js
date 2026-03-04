/**
 * Background script: POST PDF to collector; browser print-to-PDF via debugger API.
 */
function postPdfToCollector(collectorBase, drpid, url, referrer, pdfBase64, pageTitle) {
  const binary = atob(pdfBase64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  const blob = new Blob([bytes], { type: "application/pdf" });
  const fd = new FormData();
  fd.append("drpid", String(drpid));
  fd.append("url", url);
  fd.append("referrer", referrer || "");
  if (pageTitle && String(pageTitle).trim()) fd.append("title", String(pageTitle).trim());
  fd.append("pdf", blob, "page.pdf");
  return fetch(`${collectorBase}/api/extension/save-pdf`, {
    method: "POST",
    body: fd,
  }).then(r => r.json().catch(() => ({}))).then(data => ({
    ok: data.ok,
    error: data.error,
    filename: data.filename,
  }));
}

function getWatcherStatus(collectorBase) {
  return fetch(`${collectorBase}/api/downloads-watcher/status`)
    .then(r => r.json().catch(() => ({})))
    .then(data => ({ watching: !!data.watching }));
}

function stopWatcher(collectorBase) {
  return fetch(`${collectorBase}/api/downloads-watcher/stop`, { method: "POST" })
    .then(r => r.json().catch(() => ({})))
    .then(data => ({ ok: !!data.ok }));
}

function postMetadataFromPage(collectorBase, payload) {
  return fetch(`${collectorBase}/api/metadata-from-page`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  }).then(r => r.json().catch(() => ({}))).then(data => ({ ok: !!data.ok }));
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === "drp-watcher-status") {
    const { collectorBase } = msg;
    if (!collectorBase) {
      sendResponse({ watching: false });
      return true;
    }
    getWatcherStatus(collectorBase).then(sendResponse).catch(() => sendResponse({ watching: false }));
    return true;
  }
  if (msg.type === "drp-watcher-stop") {
    const { collectorBase } = msg;
    if (!collectorBase) {
      sendResponse({ ok: false });
      return true;
    }
    stopWatcher(collectorBase).then(sendResponse).catch(() => sendResponse({ ok: false }));
    return true;
  }
  if (msg.type === "drp-save-pdf") {
    const { collectorBase, drpid, url, referrer, pdfBase64, title } = msg;
    if (!collectorBase || !drpid || !url || !pdfBase64) {
      sendResponse({ ok: false, error: "Missing data" });
      return true;
    }
    (async () => {
      try {
        const data = await postPdfToCollector(collectorBase, drpid, url, referrer || "", pdfBase64, title);
        sendResponse(data);
      } catch (e) {
        sendResponse({ ok: false, error: String(e && e.message || e) });
      }
    })();
    return true;
  }

  if (msg.type === "drp-metadata-from-page") {
    const { collectorBase, payload } = msg;
    if (!collectorBase || !payload) {
      sendResponse({ ok: false });
      return true;
    }
    postMetadataFromPage(collectorBase, payload).then(sendResponse).catch(() => sendResponse({ ok: false }));
    return true;
  }
  if (msg.type === "drp-print-to-pdf") {
    const { collectorBase, drpid, url, referrer, title } = msg;
    const tabId = sender.tab && sender.tab.id;
    if (!collectorBase || !drpid || !url || tabId == null) {
      sendResponse({ ok: false, error: "Missing data", fallback: true });
      return true;
    }
    (async () => {
      try {
        chrome.debugger.attach({ tabId }, "1.3");
        try {
          await chrome.debugger.sendCommand({ tabId }, "Page.enable");
          const res = await chrome.debugger.sendCommand({ tabId }, "Page.printToPDF", {
            printBackground: true,
            preferCSSPageSize: true,
          });
          const pdfBase64 = res && res.data;
          if (!pdfBase64) {
            sendResponse({ ok: false, error: "No PDF data", fallback: true });
            return;
          }
          const data = await postPdfToCollector(collectorBase, drpid, url, referrer || "", pdfBase64, title);
          sendResponse(data);
        } finally {
          try {
            chrome.debugger.detach({ tabId });
          } catch (_) {}
        }
      } catch (e) {
        sendResponse({
          ok: false,
          error: String(e && e.message || e),
          fallback: true,
        });
      }
    })();
    return true;
  }
});
