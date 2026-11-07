import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TimeSeriesPanel } from "./TimeSeriesPanel";

const api = vi.hoisted(() => ({ series: vi.fn() }));
vi.mock("../../hooks/use-api-config", () => ({ useApi: () => api }));
vi.mock("../JsonViewer", () => ({
  default: ({ json }: { json: unknown }) => <pre>{JSON.stringify(json)}</pre>,
}));

describe("time series explorer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    api.series.mockResolvedValue({
      series: [
        {
          key: "sensor",
          points: [
            {
              at: "2026-11-06T12:00:00Z",
              count: 2,
              measures: { average: 15, minimum: 10, maximum: 20 },
            },
            {
              at: "2026-11-06T12:05:00Z",
              count: 0,
              measures: { average: null, minimum: null, maximum: null },
            },
          ],
        },
      ],
      result: {},
    });
  });
  afterEach(cleanup);
  it("only queries on demand, then renders a chart and exact bucket data", async () => {
    render(<TimeSeriesPanel tableName="readings" />);
    expect(api.series).not.toHaveBeenCalled();
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Run" }));
    });
    expect(api.series).toHaveBeenCalledWith(
      expect.objectContaining({
        table: "readings",
        bucket: { every: "5m" },
        groupBy: ["device_id"],
      }),
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    );
    expect(screen.getByRole("img").getAttribute("aria-label")).toContain("sensor");
    expect(screen.getByText("2026-11-06T12:05:00Z")).toBeTruthy();
  });
  it("aborts an obsolete request and allows running changed controls", async () => {
    api.series.mockImplementation(() => new Promise(() => {}));
    render(<TimeSeriesPanel tableName="readings" />);
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    const signal = api.series.mock.calls[0][1].signal;
    await act(async () => {
      fireEvent.change(screen.getByLabelText("Measurement"), { target: { value: "humidity" } });
    });
    expect(signal.aborted).toBe(true);
    expect(screen.getByRole("button", { name: "Run" }).hasAttribute("disabled")).toBe(false);
  });
  it("rejects unsupported calendar intervals before sending a query", () => {
    render(<TimeSeriesPanel tableName="readings" />);
    fireEvent.click(screen.getByLabelText("Calendar interval"));
    expect(screen.getByText(/Calendar interval must be/)).toBeTruthy();
    expect(api.series).not.toHaveBeenCalled();
  });
});
