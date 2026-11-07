import { readFileSync } from "node:fs";
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

describe("time-series review regressions", () => {
  it("counts aligned and origin-shifted buckets exactly", () => {
    const range = {
      ...options,
      start: "2026-01-01T00:00:00Z",
      end: "2026-01-01T02:46:40Z",
      bucket: { every: "1s" },
    };
    expect(() => seriesQuery(range)).not.toThrow();
    expect(() =>
      seriesQuery({ ...range, bucket: { every: "1s", origin: "1970-01-01T00:00:00.5Z" } })
    ).toThrow(/10000/);
    expect(() =>
      seriesQuery({ ...range, emptyBuckets: false, bucket: { every: "1ns" } })
    ).not.toThrow();
  });
  it("uses measurement presence to distinguish missing sums from measured zero", () => {
    const query = seriesQuery({
      ...options,
      measures: { total: { op: "sum", field: "temperature" } },
    });
    expect(
      query.aggregations?.series.sub_aggregations?.points.sub_aggregations
        ?.__antfly_series_count_total
    ).toEqual({ type: "avg", field: "temperature" });
    const result = {
      aggregations: {
        points: {
          buckets: [
            {
              key: options.start,
              doc_count: 1,
              sub_aggregations: { total: { value: 0 }, __antfly_series_count_total: { count: 0 } },
            },
            {
              key: options.end,
              doc_count: 1,
              sub_aggregations: { total: { value: 0 }, __antfly_series_count_total: { count: 1 } },
            },
          ],
        },
      },
    };
    expect(seriesResults(result)[0].points.map((p) => p.measures)).toEqual([
      { total: null },
      { total: 0 },
    ]);
    expect(seriesResults(result, 0)[0].points[0].measures).toEqual({ total: 0 });
  });
  it("surfaces query errors before normalizing series", async () => {
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    vi.spyOn(client, "query").mockResolvedValue({ status: 422, error: "invalid_aggregation" });
    await expect(client.series(options)).rejects.toThrow(/422.*invalid_aggregation/);
  });
  it("reports committed and buffered readings when the input stream fails", async () => {
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    vi.spyOn(client.tables, "batchWithOptions").mockResolvedValue({ status: "committed_pending" });
    async function* readings() {
      yield { id: "a", document: { observed_at: options.start } };
      yield { id: "b", document: { observed_at: options.start } };
      yield { id: "buffered", document: { observed_at: options.start } };
      throw new Error("source failed");
    }
    await expect(
      client.writeSeries("r", "observed_at", readings(), { batchSize: 2 })
    ).rejects.toMatchObject({
      committedReadings: 2,
      pendingIds: [],
      details: {
        phase: "source",
        bufferedIds: ["buffered"],
        mayHaveCommitted: false,
        committedPendingBatches: 1,
      },
    });
  });
  it("keeps acknowledged callback failures separate from ambiguous submissions", async () => {
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    vi.spyOn(client.tables, "batchWithOptions").mockResolvedValue({
      status: "committed_repair_required",
    });
    await expect(
      client.writeSeries(
        "r",
        "observed_at",
        [{ id: "a", document: { observed_at: options.start } }],
        {
          onBatch: async () => {
            await Promise.resolve();
            throw new Error("callback failed");
          },
        }
      )
    ).rejects.toMatchObject({
      committedReadings: 1,
      pendingIds: [],
      details: {
        phase: "callback",
        bufferedIds: [],
        mayHaveCommitted: false,
        repairRequiredBatches: 1,
      },
    });
  });
  it("retains progress when cancelled between batches", async () => {
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const controller = new AbortController();
    vi.spyOn(client.tables, "batchWithOptions").mockResolvedValue({ status: "committed" });
    await expect(
      client.writeSeries(
        "r",
        "observed_at",
        ["a", "b"].map((id) => ({ id, document: { observed_at: options.start } })),
        {
          batchSize: 1,
          signal: controller.signal,
          onBatch: () => controller.abort(),
        }
      )
    ).rejects.toMatchObject({
      committedReadings: 1,
      pendingIds: [],
      details: { phase: "aborted", bufferedIds: ["b"], mayHaveCommitted: false },
    });
  });
});

describe("shared SDK/server fixture", () => {
  it("keeps the end-to-end fixture equal to the actual preset and query", () => {
    const fixture = JSON.parse(
      readFileSync(
        new URL("../../../../zig/e2e/antfly/fixtures/time-series-sdk.json", import.meta.url),
        "utf8"
      )
    );
    expect(fixture.table).toEqual(
      timeSeriesTable({
        timestamp: "observed_at",
        dimensions: ["device_id", "region"],
        measurements: ["temperature"],
        bucket: "5m",
      })
    );
    expect(fixture.query).toEqual(
      JSON.parse(
        JSON.stringify(
          seriesQuery({
            table: "fixture",
            timestamp: "observed_at",
            start: "2026-11-06T12:00:00Z",
            end: "2026-11-06T12:20:00Z",
            bucket: { every: "300s" },
            groupBy: ["device_id"],
            dimensions: { region: "west" },
            measures: {
              average: { op: "avg", field: "temperature" },
              total: { op: "sum", field: "temperature" },
              minimum: { op: "min", field: "temperature" },
              maximum: { op: "max", field: "temperature" },
            },
            profile: true,
          })
        )
      )
    );
  });
});

it("matches JSON document-count semantics in generated SQL", () => {
  expect(
    seriesSQL({ ...options, measures: { readings: { op: "count", field: "temperature" } } })
      .statement
  ).toContain('COUNT(*) AS "readings"');
});

it("preserves progress when a later record is malformed at runtime", async () => {
  const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
  vi.spyOn(client.tables, "batchWithOptions").mockResolvedValue({ status: "committed" });
  const readings = [{ id: "a", document: { observed_at: options.start } }, null] as unknown as {
    id: string;
    document: Record<string, unknown>;
  }[];
  await expect(
    client.writeSeries("r", "observed_at", readings, { batchSize: 1 })
  ).rejects.toMatchObject({
    committedReadings: 1,
    pendingIds: [],
    details: { phase: "validation", bufferedIds: [], mayHaveCommitted: false },
  });
});
