/**
 * useHistorySync must not rewrite the URL on /extension/* (Copy & Open launcher keeps ?url=).
 */
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useCollectorStore } from "./store";
import { useHistorySync } from "./useHistorySync";

describe("useHistorySync", () => {
  beforeEach(() => {
    useCollectorStore.setState({ drpid: null });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("does not call replaceState on /extension/launcher (preserves url= for extension)", async () => {
    const replaceSpy = vi.spyOn(window.history, "replaceState").mockImplementation(() => {});

    vi.stubGlobal("location", {
      pathname: "/extension/launcher",
      search: "?drpid=1&url=https%3A%2F%2Fexample.com%2F",
    });

    renderHook(() => useHistorySync());

    await waitFor(() => {
      expect(replaceSpy).not.toHaveBeenCalled();
    });
  });

  it("calls replaceState on / when query does not match drpid-only sync", async () => {
    useCollectorStore.setState({ drpid: 99 });
    const replaceSpy = vi.spyOn(window.history, "replaceState").mockImplementation(() => {});

    vi.stubGlobal("location", {
      pathname: "/",
      search: "?stale=1",
    });

    renderHook(() => useHistorySync());

    await waitFor(() => {
      expect(replaceSpy).toHaveBeenCalledWith({ drpid: 99 }, "", "/?drpid=99");
    });
  });
});
