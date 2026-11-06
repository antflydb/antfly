import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TablePrivacyPanel } from "./TablePrivacyPanel";

const api = vi.hoisted(() => ({
  privacy: { get: vi.fn(), getScan: vi.fn(), put: vi.fn() },
}));
vi.mock("@/hooks/use-api-config", () => ({ useApi: () => api }));

const initialPolicy = { mode: "monitor", fields: ["body"], category_groups: [] };
function resource(etag: string, fields = ["body"]) {
  return {
    etag,
    data: {
      configured: true,
      policy: { ...initialPolicy, fields },
      etag,
      policy_version: etag,
      scan_id: "scan",
    },
  };
}

describe("privacy policy editor concurrency", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    api.privacy.get.mockResolvedValue(resource('"one"'));
    api.privacy.getScan.mockResolvedValue({ data: { phase: "running" } });
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it("keeps refreshing coverage after the scan completes and shows its observation time", async () => {
    const saved = resource('"one"');
    api.privacy.get.mockResolvedValue({
      ...saved,
      data: {
        ...saved.data,
        coverage: {
          available: true,
          current: 100,
          total: 100,
          with_findings: 2,
          pending: 0,
          failed: 0,
          completed_at_millis: 1700000000000,
          refreshing: true,
        },
      },
    });
    api.privacy.getScan.mockResolvedValue({ data: { phase: "succeeded" } });
    await act(async () => {
      render(<TablePrivacyPanel tableName="customers" />);
    });
    expect(screen.getByText(/Observed/)).toBeTruthy();
    expect(screen.getByText(/Updating coverage/)).toBeTruthy();
    const initialCalls = api.privacy.get.mock.calls.length;
    await act(async () => vi.advanceTimersByTimeAsync(10000));
    expect(api.privacy.get.mock.calls.length).toBeGreaterThan(initialCalls);
  });

  it("keeps the edited policy's ETag when polling observes a concurrent save", async () => {
    await act(async () => {
      render(<TablePrivacyPanel tableName="customers" />);
    });
    fireEvent.change(screen.getByLabelText("Fields (comma separated)"), {
      target: { value: "subject" },
    });
    api.privacy.get.mockResolvedValue(resource('"two"', ["description"]));
    await act(async () => vi.advanceTimersByTimeAsync(2500));
    expect(screen.getByLabelText("Fields (comma separated)").getAttribute("value")).toBe("subject");
    expect(screen.getByText(/saved policy changed while you were editing/)).toBeTruthy();
    api.privacy.put.mockRejectedValue(new Error("The policy changed; reload it before saving."));
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Save policy" }));
    });
    expect(api.privacy.put).toHaveBeenCalledWith(
      "customers",
      expect.objectContaining({ fields: ["subject"] }),
      { ifMatch: '"one"' }
    );
    expect(screen.getByText("The policy changed; reload it before saving.")).toBeTruthy();
  });

  it("adopts a concurrent policy and its ETag only after explicit reload", async () => {
    await act(async () => {
      render(<TablePrivacyPanel tableName="customers" />);
    });
    api.privacy.get.mockResolvedValue(resource('"two"', ["description"]));
    await act(async () => vi.advanceTimersByTimeAsync(2500));
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Reload policy" }));
    });
    expect(screen.getByLabelText("Fields (comma separated)").getAttribute("value")).toBe(
      "description"
    );
    api.privacy.put.mockRejectedValue(new Error("sent"));
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Save policy" }));
    });
    expect(api.privacy.put).toHaveBeenCalledWith(
      "customers",
      expect.objectContaining({ fields: ["description"] }),
      { ifMatch: '"two"' }
    );
  });

  it("adopts policy and ETag even when the initial scan request fails", async () => {
    api.privacy.get.mockResolvedValue(resource('"loaded"', ["subject"]));
    api.privacy.getScan
      .mockRejectedValueOnce(new Error("Scan temporarily unavailable"))
      .mockResolvedValue({ data: { scan_id: "scan", phase: "running" } });
    await act(async () => render(<TablePrivacyPanel tableName="customers" />));
    expect(screen.getByLabelText("Fields (comma separated)").getAttribute("value")).toBe("subject");
    expect(screen.getByText(/Scan status is unavailable/)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Fields (comma separated)"), {
      target: { value: "edited" },
    });
    await act(async () => vi.advanceTimersByTimeAsync(2500));
    expect(screen.queryByText(/Scan status is unavailable/)).toBeNull();
    expect(screen.getByLabelText("Fields (comma separated)").getAttribute("value")).toBe("edited");
    api.privacy.put.mockRejectedValue(new Error("record request"));
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Save policy" })));
    expect(api.privacy.put).toHaveBeenCalledWith(
      "customers",
      expect.objectContaining({ fields: ["edited"] }),
      { ifMatch: '"loaded"' }
    );
  });

  it("reloads the policy and ETag independently of unavailable progress", async () => {
    api.privacy.getScan.mockRejectedValue(new Error("Scan unavailable"));
    await act(async () => render(<TablePrivacyPanel tableName="customers" />));
    api.privacy.get.mockResolvedValue(resource('"two"', ["description"]));
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Reload policy" })));
    expect(screen.getByLabelText("Fields (comma separated)").getAttribute("value")).toBe(
      "description"
    );
    api.privacy.put.mockRejectedValue(new Error("record request"));
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Save policy" })));
    expect(api.privacy.put).toHaveBeenCalledWith(
      "customers",
      expect.objectContaining({ fields: ["description"] }),
      { ifMatch: '"two"' }
    );
  });

  it("keeps accepted saves visible without another policy read or available progress", async () => {
    api.privacy.getScan.mockRejectedValue(new Error("Scan unavailable"));
    await act(async () => render(<TablePrivacyPanel tableName="customers" />));
    api.privacy.put.mockResolvedValue(resource('"saved"'));
    api.privacy.get.mockRejectedValue(new Error("Policy read unavailable"));
    const reads = api.privacy.get.mock.calls.length;
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Save policy" })));
    expect(screen.getByText(/Policy accepted/)).toBeTruthy();
    expect(screen.queryByText("Policy read unavailable")).toBeNull();
    expect(api.privacy.get).toHaveBeenCalledTimes(reads);
    api.privacy.put.mockRejectedValue(new Error("record request"));
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Save policy" })));
    expect(api.privacy.put).toHaveBeenLastCalledWith("customers", expect.anything(), {
      ifMatch: '"saved"',
    });
  });

  it("does not overlap slow progress requests and cancels on unmount", async () => {
    let complete: (result: { data: { scan_id: string; phase: string } }) => void = () => {};
    api.privacy.getScan.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        })
    );
    let view: ReturnType<typeof render> | undefined;
    await act(async () => {
      view = render(<TablePrivacyPanel tableName="customers" />);
    });
    await act(async () => vi.advanceTimersByTimeAsync(10000));
    expect(api.privacy.getScan).toHaveBeenCalledTimes(1);
    expect(api.privacy.get.mock.calls.length).toBeGreaterThan(1);
    await act(async () => complete({ data: { scan_id: "scan", phase: "running" } }));
    await act(async () => vi.advanceTimersByTimeAsync(2500));
    expect(api.privacy.getScan).toHaveBeenCalledTimes(2);
    const signal = api.privacy.getScan.mock.calls[1][2] as AbortSignal;
    view?.unmount();
    expect(signal.aborted).toBe(true);
    await act(async () => vi.advanceTimersByTimeAsync(10000));
    expect(api.privacy.getScan).toHaveBeenCalledTimes(2);
  });

  it("retries progress immediately when Refresh is clicked", async () => {
    api.privacy.getScan
      .mockRejectedValueOnce(new Error("Scan unavailable"))
      .mockResolvedValue({ data: { scan_id: "scan", phase: "running" } });
    await act(async () => render(<TablePrivacyPanel tableName="customers" />));
    expect(screen.getByText(/Scan status is unavailable/)).toBeTruthy();
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Refresh" })));
    expect(api.privacy.getScan).toHaveBeenCalledTimes(2);
    expect(screen.queryByText(/Scan status is unavailable/)).toBeNull();
  });
});
