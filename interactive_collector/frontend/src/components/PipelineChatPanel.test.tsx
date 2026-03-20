import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { PipelineChatPanel } from "./PipelineChatPanel";

describe("PipelineChatPanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(window.localStorage.__proto__, "getItem").mockReturnValue("test-session");
    vi.spyOn(window.localStorage.__proto__, "setItem").mockImplementation(() => {});
  });

  it("sends a query and renders tool response", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        ok: true,
        requires_confirmation: false,
        tool_name: "get_pipeline_stats",
        arguments: {},
        result: "Database: data_cms_gov.db",
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<PipelineChatPanel />);

    fireEvent.change(screen.getByPlaceholderText("Ask pipeline chat..."), {
      target: { value: "database status" },
    });
    fireEvent.click(screen.getByText("Send"));

    await waitFor(() => {
      expect(screen.getByText(/Database: data_cms_gov\.db/)).toBeInTheDocument();
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/chat/query",
      expect.objectContaining({ method: "POST" })
    );
  });

  it("shows confirm button and executes confirm endpoint", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ok: true,
          requires_confirmation: true,
          tool_name: "run_module",
          arguments: { module: "sourcing", dry_run: false },
          confirmation_token: "tok-1",
          result: "Proposed mutating action",
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ok: true,
          requires_confirmation: false,
          tool_name: "run_module",
          arguments: { module: "sourcing", dry_run: false },
          result: "Executed",
        }),
      });
    vi.stubGlobal("fetch", fetchMock);

    render(<PipelineChatPanel />);

    fireEvent.change(screen.getByPlaceholderText("Ask pipeline chat..."), {
      target: { value: "run module sourcing" },
    });
    fireEvent.click(screen.getByText("Send"));

    await waitFor(() => {
      expect(screen.getByText("Confirm action")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText("Confirm action"));

    await waitFor(() => {
      expect(screen.getByText("Executed")).toBeInTheDocument();
    });
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/chat/confirm",
      expect.objectContaining({ method: "POST" })
    );
  });
});

