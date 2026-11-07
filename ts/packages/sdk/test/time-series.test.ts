import { describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import {
  fixedDurationNanoseconds,
  seriesBatch,
  seriesQuery,
  seriesRead,
  seriesResults,
  seriesSQL,
  timeSeriesTable,
} from "../src/time-series.js";

const options = {
  table: "readings",
  timestamp: "observed_at",
  start: "2026-11-06T00:00:00Z",
  end: "2026-11-06T01:00:00Z",
  bucket: { every: "5m" },
  groupBy: ["device_id"],
  measures: { temperature: { op: "avg" as const, field: "temperature" } },
};

describe("time-series query contracts", () => {
  it("builds half-open ranges, composite series and bounded empty buckets", () => {
    const query = seriesQuery({ ...options, dimensions: { device_id: "sensor" } });
    expect(query.filter_query).toEqual({
      conjuncts: [
        {
          field: "observed_at",
          start: options.start,
          end: options.end,
          inclusive_start: true,
          inclusive_end: false,
        },
        { field: "device_id", equals: "sensor" },
      ],
    });
    expect(query.aggregations?.series.sub_aggregations?.points).toMatchObject({
      fixed_interval: "5m",
      date_bounds: { start: options.start, end: options.end },
      sub_aggregations: { temperature: { type: "avg", field: "temperature" } },
    });
    expect(query.limit).toBe(0);
  });
  it("rejects excessive bucket ranges and malformed or overflowing durations", () => {
    expect(fixedDurationNanoseconds("1h30m1ns")).toBe(5400000000001n);
    for (const value of ["0s", "-1s", "1M", "1.5s", "18446744073709551615w"])
      expect(() => fixedDurationNanoseconds(value)).toThrow();
    expect(() => seriesQuery({ ...options, bucket: { every: "1ns" } })).toThrow(/10000/);
    expect(() => seriesQuery({ ...options, end: options.start })).toThrow();
  });
  it("compares nanosecond range endpoints without millisecond rounding", () => {
    expect(() =>
      seriesQuery({
        ...options,
        start: "2026-11-06T00:00:00.000000001Z",
        end: "2026-11-06T00:00:00.000000002Z",
      })
    ).not.toThrow();
  });
  it("quotes SQL identifiers and binds values", () => {
    const sql = seriesSQL({ ...options, table: 'a"b', dimensions: { device_id: "x' OR true" } });
    expect(sql.statement).toContain('FROM "a""b"');
    expect(sql.statement).not.toContain("x' OR true");
    expect(sql.parameters).toContain("x' OR true");
    expect(sql.statement).toContain("date_bin($3");
  });
  it("retains snapshot cursors for latest-reading continuations", () => {
    const continuation = {
      remote_snapshot: "snapshot",
      search_after: ["2026-11-06T00:30:00Z", "event-2"],
    };
    expect(seriesRead({ ...options, continuation })).toMatchObject({
      ...continuation,
      order_by: [{ field: "observed_at", desc: true }],
    });
  });
  it("requires explicit unique event IDs and preserves timestamp precision", () => {
    const document = { observed_at: "2026-11-06T00:00:00.000000001Z", temperature: 1 };
    expect(seriesBatch([{ id: "event", document }], "observed_at").inserts?.event).toEqual(
      document
    );
    expect(() =>
      seriesBatch(
        [
          { id: "event", document },
          { id: "event", document },
        ],
        "observed_at"
      )
    ).toThrow();
  });
  it("creates typed, sortable fields, retention and temporal aggregate recipes", () => {
    const table = timeSeriesTable({
      timestamp: "observed_at",
      dimensions: ["device_id"],
      measurements: ["temperature"],
      retention: "7d",
    });
    expect(table.schema?.ttl).toEqual({ field: "observed_at", duration: "7d" });
    expect(table.schema?.index_sort).toEqual([{ field: "observed_at", order: "desc" }]);
    expect(table.indexes?.series_aggregates).toMatchObject({
      type: "algebraic",
      aggregates: expect.arrayContaining([
        expect.objectContaining({ op: "avg", time: "observed_at", bucket: "5m" }),
      ]),
    });
  });
  it("preserves empty measures as null", () => {
    expect(
      seriesResults({
        aggregations: {
          points: {
            buckets: [{ key: options.start, doc_count: 0, sub_aggregations: { temperature: {} } }],
          },
        },
      })[0].points[0].measures.temperature
    ).toBeNull();
  });
  it("normalizes average state and explicitly fills missing values", () => {
    const result = {
      aggregations: {
        points: {
          buckets: [
            {
              key: options.start,
              doc_count: 2,
              sub_aggregations: { temperature: { count: 2, sum: 30, avg: 15 } },
            },
            {
              key: options.end,
              doc_count: 0,
              sub_aggregations: { temperature: { count: 0, sum: 0, avg: 0 } },
            },
          ],
        },
      },
    };
    expect(seriesResults(result)[0].points.map((point) => point.measures.temperature)).toEqual([
      15,
      null,
    ]);
    expect(seriesResults(result, 0)[0].points.map((point) => point.measures.temperature)).toEqual([
      15, 0,
    ]);
  });
  it("imports bounded batches and preserves committed visibility outcomes", async () => {
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const batch = vi
      .spyOn(client.tables, "batchWithOptions")
      .mockResolvedValue({ status: "committed_pending", inserted: 2 });
    const document = { observed_at: options.start, temperature: 1 };
    const result = await client.writeSeries(
      "readings",
      "observed_at",
      [0, 1, 2].map((id) => ({ id: String(id), document })),
      { batchSize: 2 }
    );
    expect(batch).toHaveBeenCalledTimes(2);
    expect(Object.keys(batch.mock.calls[0][1].inserts ?? {})).toHaveLength(2);
    expect(result.submitted).toBe(3);
    expect(result.committedPendingBatches).toBe(2);
  });
  it("stops on an ambiguous write and exposes acknowledged progress without retry", async () => {
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const batch = vi
      .spyOn(client.tables, "batchWithOptions")
      .mockResolvedValueOnce({ status: "committed", inserted: 1 })
      .mockRejectedValueOnce(new Error("connection lost"));
    const document = { observed_at: options.start };
    await expect(
      client.writeSeries(
        "readings",
        "observed_at",
        [0, 1, 2].map((id) => ({ id: String(id), document })),
        { batchSize: 1 }
      )
    ).rejects.toMatchObject({ name: "SeriesImportError", committedReadings: 1, pendingIds: ["1"] });
    expect(batch).toHaveBeenCalledTimes(2);
  });
  it("rejects invalid runtime SQL operators rather than interpolating them", () => {
    expect(() =>
      seriesSQL({
        ...options,
        measures: { value: { op: "sum); DROP TABLE readings;--" as "sum", field: "temperature" } },
      })
    ).toThrow("Invalid named measurement");
  });
});
