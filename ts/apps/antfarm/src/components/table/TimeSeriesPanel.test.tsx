import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SeriesChart, TimeSeriesPanel } from "./TimeSeriesPanel";

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
    const data = screen.getByText(/buckets · view data/).closest("details")!;
    await act(async () => {
      data.open = true;
      fireEvent(data, new Event("toggle"));
    });
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
  it("sends explicit alignment and zero-fill choices", async () => {
    render(<TimeSeriesPanel tableName="readings" />);
    fireEvent.change(screen.getByLabelText("Fixed alignment origin (optional RFC3339)"), {
      target: { value: "1970-01-01T00:02:00Z" },
    });
    fireEvent.click(screen.getByLabelText("Fill missing measurements with zero"));
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Run" }));
    });
    expect(api.series).toHaveBeenCalledWith(
      expect.objectContaining({ bucket: { every: "5m", origin: "1970-01-01T00:02:00Z" }, fill: 0 }),
      expect.anything()
    );
  });
});

describe("bounded series rendering", () => {
  afterEach(cleanup);
  it("renders isolated samples in a gap-filled range", () => {
    const { container } = render(
      <SeriesChart
        series={{
          key: "sparse",
          points: [null, 23, null, 27, null].map((value, i) => ({
            at: `2026-01-01T00:00:0${i}Z`,
            count: value == null ? 0 : 1,
            measures: { average: value },
          })),
        }}
      />
    );
    expect(container.querySelectorAll("circle")).toHaveLength(2);
    expect(container.querySelector("path")?.getAttribute("d")?.match(/M/g)).toHaveLength(2);
  });
  it("bounds chart samples and mounts only an opened page of exact data", async () => {
    const points = Array.from({ length: 10000 }, (_, i) => ({
      at: new Date(Date.UTC(2026, 0, 1) + i * 1000).toISOString(),
      count: 1,
      measures: { average: Math.sin(i) },
    }));
    const { container } = render(<SeriesChart series={{ key: "large", points }} />);
    const commands = container.querySelector("path")?.getAttribute("d")?.match(/[ML]/g) ?? [];
    expect(commands.length).toBeLessThanOrEqual(3044);
    expect(container.querySelectorAll("tbody tr")).toHaveLength(0);
    const data = screen.getByText(/buckets · view data/).closest("details")!;
    await act(async () => {
      data.open = true;
      fireEvent(data, new Event("toggle"));
    });
    expect(container.querySelectorAll("tbody tr")).toHaveLength(100);
    fireEvent.click(screen.getByRole("button", { name: "Next buckets" }));
    expect(screen.getByText(points[100].at)).toBeTruthy();
    await act(async () => {
      data.open = false;
      fireEvent(data, new Event("toggle"));
    });
    expect(container.querySelectorAll("tbody tr")).toHaveLength(0);
  });
});
