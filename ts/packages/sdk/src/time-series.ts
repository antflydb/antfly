import type {
  AggregationRequest,
  AggregationResult,
  BatchRequest,
  CreateTableRequest,
  GlobalQueryRequest,
  QueryResult,
  SQLRequest,
} from "./types.js";

export interface SeriesRange {
  timestamp: string;
  start: string;
  end: string;
  /** Exact keyword dimension filters. */
  dimensions?: Record<string, string>;
}

export type SeriesBucket =
  | { every: string; origin?: string; calendar?: never }
  | {
      calendar: "minute" | "hour" | "day" | "week" | "month" | "quarter" | "year";
      every?: never;
      origin?: never;
    };

export interface SeriesQueryOptions extends SeriesRange {
  table: string;
  bucket: SeriesBucket;
  groupBy?: string[];
  measures: Record<string, { op: "count" | "sum" | "avg" | "min" | "max"; field: string }>;
  /** Maximum returned series; the ordinary terms aggregation limit applies. */
  maxSeries?: number;
  emptyBuckets?: boolean;
  /** Explicit presentation-only replacement for missing measurement values. */
  fill?: null | 0;
  profile?: boolean;
}

function instant(text: string): bigint {
  // Date is used only to validate the civil date and timezone, never to round
  // the returned timestamp or compare its submillisecond component.
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(text))
    throw new TypeError("Expected an RFC3339 timestamp with a timezone");
  const civil = text.slice(0, 10);
  const civilTime = Date.parse(`${civil}T00:00:00Z`);
  if (
    !Number.isFinite(civilTime) ||
    new Date(civilTime).toISOString().slice(0, 10) !== civil ||
    Number(text.slice(11, 13)) > 23 ||
    Number(text.slice(14, 16)) > 59 ||
    Number(text.slice(17, 19)) > 59
  )
    throw new TypeError("Invalid timestamp");
  const ms = Date.parse(text);
  if (!Number.isFinite(ms)) throw new TypeError("Invalid timestamp");
  const fraction = /\.(\d+)/.exec(text)?.[1] ?? "";
  return BigInt(ms) * 1_000_000n + BigInt(fraction.padEnd(9, "0").slice(3));
}

export function fixedDurationNanoseconds(text: string): bigint {
  const units: Record<string, bigint> = {
    ns: 1n,
    us: 1000n,
    ms: 1_000_000n,
    s: 1_000_000_000n,
    m: 60_000_000_000n,
    h: 3_600_000_000_000n,
    d: 86_400_000_000_000n,
    w: 604_800_000_000_000n,
  };
  let total = 0n;
  let end = 0;
  for (const match of text.matchAll(/(\d+)(ns|us|ms|s|m|h|d|w)/g)) {
    if (match.index !== end) throw new TypeError("Invalid fixed duration");
    const amount = match[1],
      suffix = match[2];
    if (amount === undefined || suffix === undefined || units[suffix] === undefined)
      throw new TypeError("Invalid fixed duration");
    total += BigInt(amount) * units[suffix];
    end += match[0].length;
  }
  if (end !== text.length || total <= 0n || total > 18_446_744_073_709_551_615n)
    throw new TypeError("Fixed duration must be a positive uint64 nanosecond duration");
  return total;
}

function rangeFilter(range: SeriesRange) {
  if (!range.timestamp || instant(range.start) >= instant(range.end))
    throw new TypeError("A timestamp field and increasing [start, end) range are required");
  return {
    conjuncts: [
      {
        field: range.timestamp,
        start: range.start,
        end: range.end,
        inclusive_start: true,
        inclusive_end: false,
      },
      ...Object.entries(range.dimensions ?? {}).map(([field, term]) => ({ field, term })),
    ],
  };
}

/** Builds an ordinary JSON query; no separate time-series endpoint is needed. */
export function seriesQuery(options: SeriesQueryOptions): GlobalQueryRequest {
  const filter_query = rangeFilter(options);
  if (options.bucket.every !== undefined && options.bucket.calendar !== undefined)
    throw new TypeError("Choose a fixed or calendar interval, not both");
  if (
    !options.bucket.every &&
    !["minute", "hour", "day", "week", "month", "quarter", "year"].includes(
      options.bucket.calendar ?? ""
    )
  )
    throw new TypeError("Invalid calendar interval");
  for (const [name, metric] of Object.entries(options.measures)) {
    if (!name || !metric.field || !["count", "sum", "avg", "min", "max"].includes(metric.op))
      throw new TypeError("Invalid named measurement");
  }

  const maxSeries = options.maxSeries ?? 100;
  if (!Number.isInteger(maxSeries) || maxSeries < 1 || maxSeries > 1000)
    throw new TypeError("maxSeries must be between 1 and 1000");
  if (options.bucket.every) {
    const every = fixedDurationNanoseconds(options.bucket.every);
    if ((instant(options.end) - instant(options.start)) / every + 2n > 10000n)
      throw new RangeError("Select a coarser interval: at most 10000 buckets are supported");
    if (options.bucket.origin) instant(options.bucket.origin);
  }
  if (Object.keys(options.measures).length === 0)
    throw new TypeError("At least one measure is required");
  const measures: Record<string, AggregationRequest> = Object.fromEntries(
    Object.entries(options.measures).map(([name, value]) => [
      name,
      { type: value.op, field: value.field },
    ])
  );
  const histogram: AggregationRequest = {
    type: "date_histogram",
    field: options.timestamp,
    ...(options.bucket.every
      ? { fixed_interval: options.bucket.every, bucket_origin: options.bucket.origin }
      : { calendar_interval: options.bucket.calendar }),
    min_doc_count: options.emptyBuckets === false ? 1 : 0,
    ...(options.emptyBuckets === false
      ? {}
      : { date_bounds: { start: options.start, end: options.end } }),
    sub_aggregations: measures,
  };
  const groupBy = options.groupBy ?? [];
  return {
    table: options.table,
    profile: options.profile,
    filter_query,
    limit: 0,
    aggregations: groupBy.length
      ? {
          series: {
            type: "terms",
            fields: groupBy,
            size: maxSeries,
            sub_aggregations: { points: histogram },
          },
        }
      : { points: histogram },
  };
}

/** Latest/range reads retain the server's exact snapshot and _id tie breaker. */
export function seriesRead(
  options: SeriesRange & {
    table: string;
    limit?: number;
    descending?: boolean;
    continuation?: Pick<GlobalQueryRequest, "search_after" | "remote_snapshot">;
  }
): GlobalQueryRequest {
  const limit = options.limit ?? 100;
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000)
    throw new RangeError("limit must be between 1 and 1000");
  return {
    table: options.table,
    filter_query: rangeFilter(options),
    limit,
    order_by: [{ field: options.timestamp, desc: options.descending ?? true }],
    search_after: options.continuation?.search_after,
    remote_snapshot: options.continuation?.remote_snapshot,
  };
}

export interface SeriesPoint {
  at: string;
  count: number;
  measures: Record<string, number | null>;
}
export interface TimeSeries {
  key: string;
  points: SeriesPoint[];
}

/** Preserve missing values as null. Callers can explicitly zero-fill for display. */
export function seriesResults(result: QueryResult, fill: null | 0 = null): TimeSeries[] {
  const points = (aggregation: AggregationResult | undefined): SeriesPoint[] =>
    (aggregation?.buckets ?? []).map((bucket) => ({
      at: bucket.key,
      count: bucket.doc_count,
      measures: Object.fromEntries(
        Object.entries(bucket.sub_aggregations ?? {}).map(([name, metric]) => [
          name,
          typeof metric.value === "number"
            ? metric.value
            : metric.count === 0
              ? fill
              : (metric.avg ?? fill),
        ])
      ),
    }));
  const groups = result.aggregations?.series?.buckets;
  return groups
    ? groups.map((group) => ({ key: group.key, points: points(group.sub_aggregations?.points) }))
    : [{ key: "all", points: points(result.aggregations?.points) }];
}

/** Generates an inspectable document schema plus opt-in exact aggregate recipes. */
export function timeSeriesTable(options: {
  timestamp: string;
  dimensions: string[];
  measurements: string[];
  retention?: string;
  bucket?: string;
  numShards?: number;
}): CreateTableRequest {
  const fields = [options.timestamp, ...options.dimensions, ...options.measurements];
  if (
    fields.some((name) => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) ||
    new Set(fields).size !== fields.length
  )
    throw new TypeError("Preset fields must be distinct simple column names");
  const properties = Object.fromEntries(
    fields.map((name) => [
      name,
      {
        type: options.measurements.includes(name) ? "number" : "string",
        ...(name === options.timestamp ? { format: "date-time" } : {}),
        "x-antfly-field": {
          type:
            name === options.timestamp
              ? "date"
              : options.dimensions.includes(name)
                ? "keyword"
                : "number",
          sortable: true,
        },
      },
    ])
  );
  return {
    num_shards: options.numShards ?? 1,
    schema: {
      index_sort: [{ field: options.timestamp, order: "desc" }],
      default_type: "reading",
      document_schemas: {
        reading: {
          schema: {
            type: "object",
            required: [options.timestamp, ...options.dimensions],
            properties,
          },
        },
      },
      ...(options.retention
        ? { ttl: { field: options.timestamp, duration: options.retention } }
        : {}),
    },
    indexes: {
      series_aggregates: {
        type: "algebraic",
        derive_from_schema: true,
        aggregates: [
          {
            name: "reading_count",
            op: "count",
            group_by: options.dimensions,
            time: options.timestamp,
            bucket: options.bucket ?? "5m",
          },
          ...options.measurements.flatMap((measure, i) =>
            (["count", "sum", "avg", "min", "max"] as const).map((op) => ({
              name: `m${i}_${op}`,
              op,
              measure,
              group_by: options.dimensions,
              time: options.timestamp,
              bucket: options.bucket ?? "5m",
            }))
          ),
        ],
      },
    },
  };
}

/** Explicit IDs are mandatory: equal timestamps need not identify equal events.
 * This builds ordinary upserts. Use version-conditional writes for corrections
 * that must be protected from an older producer retry.
 */
export function seriesBatch(
  readings: { id: string; document: Record<string, unknown> }[],
  timestamp: string
): BatchRequest {
  if (readings.length === 0 || readings.length > 1000)
    throw new RangeError("Batch must contain 1–1000 readings");
  const inserts: Record<string, Record<string, unknown>> = Object.create(null);
  for (const { id, document } of readings) {
    if (!id || id in inserts)
      throw new TypeError("Reading IDs must be nonempty and unique within the batch");
    if (typeof document[timestamp] !== "string")
      throw new TypeError("Readings require an RFC3339 timestamp");
    instant(document[timestamp]);
    inserts[id] = document;
  }
  return { inserts };
}

/** Parameterized SQL equivalent for populated buckets (empty filling is a JSON
 * presentation option). Identifier quoting preserves names without SQL injection.
 */
export function seriesSQL(options: SeriesQueryOptions): SQLRequest {
  seriesQuery(options);
  const quote = (name: string) => `"${name.replace(/"/g, '""')}"`;
  const parameters: unknown[] = [options.start, options.end];
  const parameter = (value: unknown) => {
    parameters.push(value);
    return `$${parameters.length}`;
  };
  const timestamp = quote(options.timestamp);
  const bucket = options.bucket.every
    ? `date_bin(${parameter(options.bucket.every)}, ${timestamp}, ${parameter(options.bucket.origin ?? "1970-01-01T00:00:00Z")}::datetime)`
    : `date_trunc(${parameter(options.bucket.calendar)}, ${timestamp})`;
  const dimensions = (options.groupBy ?? []).map(quote);
  const columns = [
    ...dimensions,
    `${bucket} AS "bucket"`,
    ...Object.entries(options.measures).map(
      ([name, metric]) => `${metric.op.toUpperCase()}(${quote(metric.field)}) AS ${quote(name)}`
    ),
  ];
  const predicates = [
    `${timestamp} >= $1::datetime`,
    `${timestamp} < $2::datetime`,
    ...Object.entries(options.dimensions ?? {}).map(
      ([name, value]) => `${quote(name)} = ${parameter(value)}`
    ),
  ];
  return {
    statement: `SELECT ${columns.join(", ")} FROM ${quote(options.table)} WHERE ${predicates.join(" AND ")} GROUP BY ${[...dimensions, bucket].join(", ")} ORDER BY ${[...dimensions, '"bucket"'].join(", ")}`,
    parameters,
  };
}

/** Import stopped without replaying an ambiguous write. committedReadings
 * counts earlier acknowledged batches; the pending batch may have committed.
 */
export class SeriesImportError extends Error {
  readonly cause: unknown;
  constructor(
    readonly committedReadings: number,
    readonly pendingIds: string[],
    cause: unknown
  ) {
    super(
      `Series import stopped after ${committedReadings} acknowledged readings; reconcile the pending batch before retrying`
    );
    this.name = "SeriesImportError";
    this.cause = cause;
  }
}
