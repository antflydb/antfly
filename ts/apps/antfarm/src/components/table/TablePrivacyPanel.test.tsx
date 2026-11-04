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
});
