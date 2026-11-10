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
  /** Keyword dimensions; SQL follows the preset's scalar dimension model. */
  groupBy?: string[];
  measures: Record<string, { op: "count" | "sum" | "avg" | "min" | "max"; field: string }>;
  /** Maximum returned series; the ordinary terms aggregation limit applies. */
  maxSeries?: number;
  emptyBuckets?: boolean;
  /** Explicit presentation-only replacement for missing measurement values. */
  fill?: null | 0;
  profile?: boolean;
}

export function seriesTimestampNanoseconds(text: string): bigint {
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

const instant = seriesTimestampNanoseconds;

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
      ...Object.entries(range.dimensions ?? {}).map(([field, equals]) => ({ field, equals })),
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
    if (
      !name ||
      name.startsWith("__antfly_series_") ||
      !metric.field ||
      !["count", "sum", "avg", "min", "max"].includes(metric.op)
    )
      throw new TypeError("Invalid named measurement");
  }

  const maxSeries = options.maxSeries ?? 100;
  if (!Number.isInteger(maxSeries) || maxSeries < 1 || maxSeries > 1000)
    throw new TypeError("maxSeries must be between 1 and 1000");
  if (options.bucket.every) {
    const every = fixedDurationNanoseconds(options.bucket.every);
    const origin = options.bucket.origin ? instant(options.bucket.origin) : 0n;
    const floor = (value: bigint) => value / every - (value % every < 0n ? 1n : 0n);
    const buckets =
      floor(instant(options.end) - 1n - origin) - floor(instant(options.start) - origin) + 1n;
    if (options.emptyBuckets !== false && buckets > 10000n)
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
  for (const [name, metric] of Object.entries(options.measures)) {
    if (metric.op === "sum")
      measures[`__antfly_series_count_${name}`] = { type: "avg", field: metric.field };
  }
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
    timeout_ms: 30000,
    profile: options.profile,
    filter_query,
    limit: 0,
    aggregations: groupBy.length
      ? {
          series: {
            type: "terms",
            fields: groupBy,
            exclude_null: true,
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
        Object.entries(bucket.sub_aggregations ?? {})
          .filter(([name]) => !name.startsWith("__antfly_series_"))
          .map(([name, metric]) => [
            name,
            bucket.sub_aggregations?.[`__antfly_series_count_${name}`]?.count === 0
              ? fill
              : typeof metric.value === "number"
                ? metric.value
                : metric.count === 0
                  ? fill
                  : (metric.avg ?? fill),
          ])
      ),
    }));
  const grouped = result.aggregations?.series;
  return grouped
    ? (grouped.buckets ?? []).map((group) => ({
        key: group.key,
        points: points(group.sub_aggregations?.points),
      }))
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
            (["sum", "avg", "min", "max"] as const).map((op) => ({
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

function ownFieldPath(document: unknown, path: string): unknown {
  let value = document;
  for (const segment of path.split(".")) {
    if (
      !segment ||
      !value ||
      typeof value !== "object" ||
      Array.isArray(value) ||
      !Object.prototype.hasOwnProperty.call(value, segment)
    )
      return undefined;
    value = (value as Record<string, unknown>)[segment];
  }
  return value;
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
  for (const reading of readings) {
    if (!reading || typeof reading !== "object") throw new TypeError("Readings must be objects");
    const { id, document } = reading;
    if (typeof id !== "string" || !id || id in inserts)
      throw new TypeError("Reading IDs must be nonempty and unique within the batch");
    const observedAt = ownFieldPath(document, timestamp);
    if (
      !document ||
      typeof document !== "object" ||
      Array.isArray(document) ||
      typeof observedAt !== "string"
    )
      throw new TypeError("Readings require an RFC3339 timestamp");
    instant(observedAt);
    inserts[id] = document;
  }
  return { inserts };
}

export interface SeriesSQLOptions {
  fieldPaths?: Record<string, readonly string[]>;
  /** Explicit output name for the time bucket. Must not overlap a group or measure.
   * By default, uses "bucket", appending underscores to avoid collisions. */
  bucketAlias?: string;
}

/** Parameterized SQL equivalent for populated buckets (empty filling is a JSON
 * presentation option). Identifier quoting preserves names without SQL injection.
 */
export function seriesSQL(
  options: SeriesQueryOptions,
  sqlOptions: SeriesSQLOptions = {}
): SQLRequest {
  // SQL emits populated buckets, so JSON presentation limits do not apply.
  seriesQuery({ ...options, emptyBuckets: false });
  const quote = (name: string) => `"${name.replace(/"/g, '""')}"`;
  const parameters: unknown[] = [options.start, options.end];
  const parameter = (value: unknown) => {
    parameters.push(value);
    return `$${parameters.length}`;
  };
  const groupBy = options.groupBy ?? [];
  const outputNames = new Set(groupBy);
  if (outputNames.size !== groupBy.length)
    throw new TypeError("SQL group fields must have distinct output names");
  for (const name of Object.keys(options.measures)) {
    if (outputNames.has(name))
      throw new TypeError(
        `SQL measurement output name ${JSON.stringify(name)} overlaps a group field`
      );
    outputNames.add(name);
  }
  let bucketAlias = sqlOptions.bucketAlias ?? "bucket";
  if (sqlOptions.bucketAlias !== undefined) {
    if (typeof sqlOptions.bucketAlias !== "string" || !bucketAlias || outputNames.has(bucketAlias))
      throw new TypeError(
        "SQL bucketAlias must be nonempty and distinct from group and measurement output names"
      );
  } else {
    while (outputNames.has(bucketAlias)) bucketAlias += "_";
  }

  const used = new Set([
    options.table,
    options.timestamp,
    ...groupBy,
    ...Object.keys(options.measures),
    bucketAlias,
  ]);
  const internal = (base: string) => {
    let name = base;
    while (used.has(name)) name += "_";
    used.add(name);
    return quote(name);
  };
  const source = internal("__antfly_series_source");
  const selected = internal("__antfly_series_selected");
  const selectedCount = internal("__antfly_series_documents");
  // JSON field names denote dotted paths. Explicit segments disambiguate a
  // literal dotted column from a nested path without interpolating SQL syntax.
  const validPath = (path: unknown): path is readonly [string, ...string[]] =>
    Array.isArray(path) &&
    path.length > 0 &&
    path.every((part) => typeof part === "string" && part.length > 0);
  const segments = (field: string) => {
    const explicit =
      sqlOptions.fieldPaths && Object.getOwnPropertyDescriptor(sqlOptions.fieldPaths, field)
        ? sqlOptions.fieldPaths[field]
        : undefined;
    const path = explicit ?? field.split(".");
    if (!validPath(path)) throw new TypeError("SQL field paths require nonempty segments");
    return path;
  };
  const column = (field: string, prefix = "") => {
    const [root, ...path] = segments(field);
    const base = `${prefix}${quote(root)}`;
    return path.length
      ? `jsonb_extract_path_text(${base}, ${path.map((part) => `'${part.replace(/'/g, "''")}'`).join(", ")})`
      : base;
  };
  // The preset's logical JSON column is a string. Cast source instants as well
  // as parameters; textual comparison would order timezone offsets incorrectly.
  const timestamp = `${column(options.timestamp, `${source}.`)}::datetime`;
  const bucket = options.bucket.every
    ? `date_bin(${parameter(options.bucket.every)}, ${timestamp}, ${parameter(options.bucket.origin ?? "1970-01-01T00:00:00Z")}::datetime)`
    : `date_trunc(${parameter(options.bucket.calendar)}, ${timestamp})`;
  const dimensionFilters = Object.entries(options.dimensions ?? {}).map(
    ([field, value]) => [field, parameter(value)] as const
  );
  const predicates = (prefix = "") =>
    [
      `${column(options.timestamp, prefix)}::datetime >= $1::datetime`,
      `${column(options.timestamp, prefix)}::datetime < $2::datetime`,
      ...dimensionFilters.map(([field, value]) => `${column(field, prefix)} = ${value}`),
      ...groupBy.flatMap((field) => [
        `NOT is_missing(${column(field, prefix)})`,
        `${column(field, prefix)} IS NOT NULL`,
      ]),
    ].join(" AND ");
  const dimensions = groupBy.map((field) => column(field, `${source}.`));
  const columns = [
    ...groupBy.map((field) => {
      const expression = column(field, `${source}.`);
      return `${expression} AS ${quote(field)}`;
    }),
    `${bucket} AS ${quote(bucketAlias)}`,
    ...Object.entries(options.measures).map(([name, metric]) => {
      const argument = metric.op === "count" ? "*" : column(metric.field, `${source}.`);
      const aggregate = `${metric.op.toUpperCase()}(${metric.op !== "count" && segments(metric.field).length > 1 ? `(${argument})::double precision` : argument})`;
      const value =
        options.fill === 0 && metric.op !== "count" ? `COALESCE(${aggregate}, 0)` : aggregate;
      return `${value} AS ${quote(name)}`;
    }),
  ];
  // Composite terms tie-break on their canonical JSON tuple, including escaping.
  const key = (fields: string[]) =>
    fields.length === 1
      ? fields[0]
      : `concat('[', ${fields.map((field) => `COALESCE(to_jsonb(${field})::text, 'null')`).join(", ',', ")}, ']')`;
  // Qualified CTE keys cannot bind an output alias when a remapped physical
  // column has the same name as the internal count (especially in ORDER BY).
  const topDimensions = dimensions;
  const selectedDimensions = groupBy.map((field, i) => `${topDimensions[i]} AS ${quote(field)}`);
  const cte = groupBy.length
    ? `WITH ${selected} AS (SELECT ${selectedDimensions.join(", ")}, COUNT(*) AS ${selectedCount} FROM ${quote(options.table)} AS ${source} WHERE ${predicates(`${source}.`)} GROUP BY ${topDimensions.join(", ")} ORDER BY COUNT(*) DESC, ${key(topDimensions)} ASC LIMIT ${parameter(options.maxSeries ?? 100)}) `
    : "";
  const join = groupBy.length
    ? ` JOIN ${selected} ON ${groupBy.map((field) => `${column(field, `${source}.`)} IS NOT DISTINCT FROM ${selected}.${quote(field)}`).join(" AND ")}`
    : "";
  const count = `${selected}.${selectedCount}`;
  return {
    statement: `${cte}SELECT ${columns.join(", ")} FROM ${quote(options.table)} AS ${source}${join} WHERE ${predicates(`${source}.`)} GROUP BY ${[...dimensions, bucket, ...(groupBy.length ? [count] : [])].join(", ")} ORDER BY ${[...(groupBy.length ? [`${count} DESC`, `${key(dimensions)} ASC`] : []), bucket].join(", ")}`,
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
    cause: unknown,
    readonly details: {
      phase: "source" | "validation" | "submission" | "callback" | "aborted";
      bufferedIds: string[];
      mayHaveCommitted: boolean;
      committedPendingBatches: number;
      repairRequiredBatches: number;
      lastOutcome?: unknown;
    } = {
      phase: "submission",
      bufferedIds: [],
      mayHaveCommitted: true,
      committedPendingBatches: 0,
      repairRequiredBatches: 0,
    }
  ) {
    super(
      `Series import stopped during ${details.phase} after ${committedReadings} acknowledged readings${details.mayHaveCommitted ? "; reconcile the pending batch before retrying" : ""}`
    );
    this.name = "SeriesImportError";
    this.cause = cause;
  }
}
