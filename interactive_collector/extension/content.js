/**
 * DRP Collector extension - content script.
 *
 * On launcher page: store drpid and collectorBase from URL, let redirect proceed.
 * On other pages: show "Save as PDF" when the collector's downloads watcher is on
 * (same as Copy & Open). When Save is pressed in the collector, watcher turns off
 * and we hide the button (via polling).
 */
(function () {
  "use strict";

  const LAUNCHER_MATCH = /\/extension\/launcher/;
  const DRP_ID = "drp-collector-save-btn";
  const WATCHER_POLL_MS = 25000;
  var pageScriptInjected = false;
  var watcherPollTimer = null;

  function isLauncherPage() {
    return LAUNCHER_MATCH.test(window.location.pathname);
  }

  function handleLauncherPage() {
    try {
      if (!chrome.runtime || !chrome.runtime.id) return;
    } catch (e) {
      return;
    }
    const params = new URLSearchParams(window.location.search);
    const drpid = params.get("drpid");
    const url = params.get("url");
    const collectorBase = window.location.origin;
    if (drpid && url) {
      chrome.storage.local.set({ drpid, collectorBase, sourcePageUrl: url }).then(
        function () {
          window.location.href = url;
        },
        function () {
          window.location.href = url;
        }
      );
    }
  }

  function urlOriginAndPath(u) {
    if (!u) return "";
    try {
      var a = document.createElement("a");
      a.href = u;
      var path = (a.pathname || "/").replace(/\/+$/, "") || "/";
      return (a.host || a.hostname) + path;
    } catch (e) {
      return u;
    }
  }

  function isCmsDomain() {
    try {
      return (window.location.hostname || "").indexOf("data.cms.gov") !== -1;
    } catch (e) {
      return false;
    }
  }

  function extractMetadataFromPage() {
    var meta = {};
    var el;
    var isCms = isCmsDomain();

    if (isCms) {
      // data.cms.gov selectors (SPA; often need delay before extract)
      el = document.querySelector("h1");
      if (el && el.textContent) meta.title = el.textContent.trim();
      el = document.querySelector("div.DatasetPage__summary-field-summary-container") ||
           document.querySelector("div[class*='DatasetPage__summary']");
      if (el && el.innerHTML) meta.summary = el.innerHTML.trim();
      el = document.querySelector("ul.DatasetDetails__tags") ||
           document.querySelector("ul[class*='DatasetDetails__tags']") ||
           document.querySelector("div.DatasetDetails__tags") ||
           document.querySelector("div[class*='DatasetDetails__tags']");
      if (el) {
        var tagEls = el.querySelectorAll("a");
        if (tagEls.length) {
          meta.keywords = Array.prototype.map.call(tagEls, function (n) { return n.textContent.trim(); }).filter(Boolean).join("; ");
        }
        if (!meta.keywords && el.textContent) meta.keywords = el.textContent.trim();
      }
      // Agency: div.DatasetHero__meta with <span>Data source</span> and sibling <span>agency name</span>
      var metaDivs = document.querySelectorAll("div.DatasetHero__meta, div[class*='DatasetHero__meta']");
      for (var d = 0; d < metaDivs.length; d++) {
        var spans = metaDivs[d].querySelectorAll("span");
        for (var s = 0; s < spans.length; s++) {
          if (spans[s].textContent.trim().toLowerCase().indexOf("data source") !== -1 && spans[s].nextElementSibling) {
            meta.agency = spans[s].nextElementSibling.textContent.trim();
            break;
          }
        }
        if (meta.agency) break;
      }
    }

    if (!meta.title) {
      el = document.querySelector('h1[itemprop="name"]');
      if (el && el.textContent) meta.title = el.textContent.trim();
    }
    if (!meta.title) {
      el = document.querySelector("h2.asset-name");
      if (el && el.textContent) meta.title = el.textContent.trim();
    }
    if (!meta.title && document.title) meta.title = document.title.trim();

    if (!meta.summary) {
      el = document.querySelector("div.description-section");
      if (el && el.innerHTML) meta.summary = el.innerHTML.trim();
    }
    if (!meta.summary) {
      el = document.querySelector('div[itemprop="description"]');
      if (el && el.innerHTML) meta.summary = el.innerHTML.trim();
    }

    if (!meta.keywords) {
      var tagsSection = document.querySelector("section.tags");
      if (tagsSection) {
        var tagEls = tagsSection.querySelectorAll("a");
        if (tagEls.length) {
          meta.keywords = Array.prototype.map.call(tagEls, function (n) { return n.textContent.trim(); }).filter(Boolean).join("; ");
        }
        if (!meta.keywords && tagsSection.textContent) meta.keywords = tagsSection.textContent.trim();
      }
    }
    if (!meta.keywords) {
      var kwNodes = document.querySelectorAll('[itemprop="keywords"]');
      if (kwNodes.length) {
        meta.keywords = Array.prototype.map.call(kwNodes, function (n) { return n.textContent.trim(); }).filter(Boolean).join("; ");
      }
    }

    if (!meta.agency) {
      el = document.querySelector('[itemprop="publisher"]');
      if (el) {
        var name = el.getAttribute("content") || (el.querySelector("[itemprop='name']") && el.querySelector("[itemprop='name']").textContent) || el.textContent;
        if (name && name.trim()) meta.agency = name.trim();
      }
    }
    if (!meta.office) {
      el = document.querySelector(".dataset-office, [data-field='organization'] .value, .publisher-name");
      if (el && el.textContent) meta.office = el.textContent.trim();
    }
    if (!meta.office && meta.agency) meta.office = meta.agency;

    var today = new Date();
    meta.download_date = today.getFullYear() + "-" + String(today.getMonth() + 1).padStart(2, "0") + "-" + String(today.getDate()).padStart(2, "0");
    return meta;
  }

  function doSendMetadataFromPage(collectorBase, drpid, sourcePageUrl) {
    if (!sourcePageUrl) return;
    var currentKey = urlOriginAndPath(window.location.href);
    var sourceKey = urlOriginAndPath(sourcePageUrl);
    if (currentKey !== sourceKey) return;
    var meta = extractMetadataFromPage();
    meta.drpid = parseInt(drpid, 10);
    delete meta.office;
    if (Object.keys(meta).length <= 1) return;
    // POST via background script so the request is from the extension (localhost allowed), not the page (blocked by Private Network Access)
    chrome.runtime.sendMessage(
      { type: "drp-metadata-from-page", collectorBase: collectorBase, payload: meta },
      function (response) {
        if (response && response.ok) {
          chrome.storage.local.remove(["sourcePageUrl"]).catch(function () {});
        }
      }
    );
  }

  function sendMetadataFromPageIfSource(collectorBase, drpid, sourcePageUrl) {
    if (!collectorBase || !drpid || !sourcePageUrl) return;
    // data.cms.gov is likely a SPA; wait for JS to render before extracting
    if (isCmsDomain()) {
      setTimeout(function () { doSendMetadataFromPage(collectorBase, drpid, sourcePageUrl); }, 3000);
      return;
    }
    doSendMetadataFromPage(collectorBase, drpid, sourcePageUrl);
  }

  function clearWatcherPoll() {
    if (watcherPollTimer) {
      clearInterval(watcherPollTimer);
      watcherPollTimer = null;
    }
  }

  function isContextInvalidated(err) {
    var msg = err && (err.message || String(err));
    return typeof msg === "string" && msg.indexOf("Extension context invalidated") !== -1;
  }

  function checkWatcherAndShowOrHide() {
    try {
      if (!chrome.runtime || !chrome.runtime.id) return;
    } catch (e) {
      return;
    }
    chrome.storage.local.get(["drpid", "collectorBase", "sourcePageUrl"]).then(
      function (stored) {
        var drpid = stored.drpid, collectorBase = stored.collectorBase, sourcePageUrl = stored.sourcePageUrl;
        if (!drpid || !collectorBase) {
          removeCollectorButtons();
          clearWatcherPoll();
          return;
        }
        sendMetadataFromPageIfSource(collectorBase, drpid, sourcePageUrl);
        addSaveButton();
        startWatcherPoll(collectorBase);
      },
      function (e) {
        if (isContextInvalidated(e)) {
          clearWatcherPoll();
          removeCollectorButtons();
        }
      }
    );
  }

  function startWatcherPoll(collectorBase) {
    clearWatcherPoll();
    watcherPollTimer = setInterval(function () {
      try {
        if (!chrome.runtime || !chrome.runtime.id) {
          clearWatcherPoll();
          removeCollectorButtons();
          return;
        }
      } catch (e) {
        clearWatcherPoll();
        removeCollectorButtons();
        return;
      }
      chrome.runtime.sendMessage({ type: "drp-watcher-status", collectorBase })
        .then(function (res) {
          if (res && res.watching) return;
          try {
            chrome.storage.local.remove(["drpid", "collectorBase"]).catch(function () {});
          } catch (_) {}
          removeCollectorButtons();
          clearWatcherPoll();
        })
        .catch(function (e) {
          if (isContextInvalidated(e)) {
            clearWatcherPoll();
            removeCollectorButtons();
          }
        });
    }, WATCHER_POLL_MS);
  }

  function removeCollectorButtons() {
    var btn = document.getElementById(DRP_ID);
    if (btn) btn.remove();
  }

  function injectPageScript() {
    if (pageScriptInjected) return;
    pageScriptInjected = true;
    const s = document.createElement("script");
    s.src = chrome.runtime.getURL("page.js");
    (document.head || document.documentElement).appendChild(s);
  }

  function createPdfBlob(collectorBase) {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        document.removeEventListener("drp-pdf-ready", onReady);
        document.removeEventListener("drp-pdf-error", onErr);
        reject(new Error("PDF generation timed out (try a shorter page)"));
      }, 300000);
      const onReady = (e) => {
        clearTimeout(timeout);
        document.removeEventListener("drp-pdf-ready", onReady);
        document.removeEventListener("drp-pdf-error", onErr);
        resolve(e.detail);
      };
      const onErr = (e) => {
        clearTimeout(timeout);
        document.removeEventListener("drp-pdf-ready", onReady);
        document.removeEventListener("drp-pdf-error", onErr);
        reject(new Error(e.detail || "PDF generation failed"));
      };
      document.addEventListener("drp-pdf-ready", onReady);
      document.addEventListener("drp-pdf-error", onErr);
      document.dispatchEvent(new CustomEvent("drp-generate-pdf", { detail: { collectorBase } }));
    });
  }

  function showToast(msg, isError) {
    const el = document.getElementById(DRP_ID + "-toast");
    if (el) el.remove();
    const toast = document.createElement("div");
    toast.id = DRP_ID + "-toast";
    toast.textContent = msg;
    toast.style.cssText =
      "position:fixed;bottom:80px;right:20px;padding:12px 18px;background:" +
      (isError ? "#c44" : "#282") +
      ";color:#fff;border-radius:6px;z-index:2147483647;font-size:14px;max-width:90vw;";
    document.body.appendChild(toast);
    setTimeout(() => toast.remove(), isError ? 6000 : 5000);
  }

  function addSaveButton() {
    if (document.getElementById(DRP_ID)) return;

    injectPageScript();

    const btn = document.createElement("button");
    btn.id = DRP_ID;
    btn.textContent = "Save as PDF";
    btn.className = "drp-collector-btn";
    document.body.appendChild(btn);

    btn.addEventListener("click", async () => {
      var stored;
      try {
        stored = await chrome.storage.local.get(["drpid", "collectorBase"]);
      } catch (e) {
        if (e && e.message && String(e.message).indexOf("invalidated") !== -1) {
          showToast("Extension was reloaded. Refresh this page and use Copy & Open again.", true);
          return;
        }
        throw e;
      }
      var drpid = stored.drpid, collectorBase = stored.collectorBase;
      if (!drpid || !collectorBase) {
        showToast("Use Copy & Open from collector first.", true);
        return;
      }

      btn.disabled = true;
      btn.textContent = "Saving...";
      showToast("Generating PDF...", false);
      try {
        var res = await chrome.runtime.sendMessage({
          type: "drp-print-to-pdf",
          collectorBase,
          drpid,
          url: window.location.href,
          referrer: (document.referrer || "").trim() || "",
          title: (document.title || "").trim() || "",
        });
        if (res && res.ok) {
          showToast("Saved: " + (res.filename || "OK"), false);
          return;
        }
        if (res && res.fallback) {
          showToast("Using alternative PDF method...", false);
          const pdfBase64 = await createPdfBlob(collectorBase);
          if (!pdfBase64 || typeof pdfBase64 !== "string") {
            showToast("No PDF data received", true);
            return;
          }
          var r2 = await chrome.runtime.sendMessage({
            type: "drp-save-pdf",
            collectorBase,
            drpid,
            url: window.location.href,
            referrer: (document.referrer || "").trim() || "",
            pdfBase64,
            title: (document.title || "").trim() || "",
          });
          if (r2 && r2.ok) {
            showToast("Saved: " + (r2.filename || "OK"), false);
          } else {
            showToast((r2 && r2.error) || "Save failed", true);
          }
          return;
        }
        showToast((res && res.error) || "Print to PDF failed", true);
        return;
      } catch (e) {
        var msg = e && e.message ? String(e.message) : "Failed to save";
        if (msg.indexOf("invalidated") !== -1) {
          showToast("Extension was reloaded. Refresh this page and use Copy & Open again.", true);
        } else {
          try {
            showToast("Using alternative PDF method...", false);
            const pdfBase64 = await createPdfBlob(collectorBase);
            if (pdfBase64 && typeof pdfBase64 === "string") {
              var r2 = await chrome.runtime.sendMessage({
                type: "drp-save-pdf",
                collectorBase,
                drpid,
                url: window.location.href,
                referrer: (document.referrer || "").trim() || "",
                pdfBase64,
                title: (document.title || "").trim() || "",
              });
              if (r2 && r2.ok) {
                showToast("Saved: " + (r2.filename || "OK"), false);
                return;
              }
            }
          } catch (_) {}
          showToast(msg, true);
        }
      } finally {
        btn.disabled = false;
        btn.textContent = "Save as PDF";
      }
    });
  }

  function ensureButtons() {
    if (document.getElementById(DRP_ID)) return;
    try {
      checkWatcherAndShowOrHide();
    } catch (e) {
      if (isContextInvalidated(e)) {
        clearWatcherPoll();
        removeCollectorButtons();
      }
    }
  }

  function onUrlChange() {
    if (isLauncherPage()) return;
    setTimeout(ensureButtons, 150);
  }

  if (isLauncherPage()) {
    handleLauncherPage();
  } else {
    try {
      checkWatcherAndShowOrHide();
    } catch (e) {
      if (isContextInvalidated(e)) {
        clearWatcherPoll();
        removeCollectorButtons();
      }
    }
    window.addEventListener("popstate", onUrlChange);
    var _push = history.pushState, _replace = history.replaceState;
    history.pushState = function () {
      _push.apply(this, arguments);
      onUrlChange();
    };
    history.replaceState = function () {
      _replace.apply(this, arguments);
      onUrlChange();
    };
  }
})();
