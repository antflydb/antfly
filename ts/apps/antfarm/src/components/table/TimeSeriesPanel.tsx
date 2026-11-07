import {
  Alert,
  AlertDescription,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Input,
  Label,
  Textarea,
} from "@antfly/design-system";
import { type SeriesQueryOptions, seriesQuery, seriesSQL, type TimeSeries } from "@antfly/sdk";
import { useEffect, useMemo, useRef, useState } from "react";
import { useApi } from "../../hooks/use-api-config";
import JsonViewer from "../JsonViewer";

export function TimeSeriesPanel({ tableName }: { tableName: string }) {
  const client = useApi();
  const [timestamp, setTimestamp] = useState("observed_at");
  const [measurement, setMeasurement] = useState("temperature");
  const [group, setGroup] = useState("device_id");
  const [every, setEvery] = useState("5m");
  const [calendar, setCalendar] = useState(false);
  const [origin, setOrigin] = useState("");
  const [zeroFill, setZeroFill] = useState(false);
  const [start, setStart] = useState(() => new Date(Date.now() - 3600000).toISOString());
  const [end, setEnd] = useState(() => new Date().toISOString());
  const [filters, setFilters] = useState("{}");
  const [poll, setPoll] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [series, setSeries] = useState<TimeSeries[]>([]);
  const [metadata, setMetadata] = useState<unknown>();
  const controller = useRef<AbortController | null>(null);
  const built = useMemo(() => {
    try {
      const dimensions: unknown = JSON.parse(filters);
      if (
        !dimensions ||
        typeof dimensions !== "object" ||
        Array.isArray(dimensions) ||
        Object.values(dimensions).some((value) => typeof value !== "string")
      )
        throw new Error("Dimension filters must be a JSON object of string values");
      const calendarUnits = ["minute", "hour", "day", "week", "month", "quarter", "year"] as const;
      if (calendar && !calendarUnits.some((unit) => unit === every))
        throw new Error(
          "Calendar interval must be minute, hour, day, week, month, quarter, or year (UTC)"
        );
      const options: SeriesQueryOptions = {
        table: tableName,
        timestamp,
        start,
        end,
        dimensions: dimensions as Record<string, string>,
        groupBy: group
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
        bucket: calendar
          ? { calendar: every as (typeof calendarUnits)[number] }
          : { every, ...(origin ? { origin } : {}) },
        fill: zeroFill ? 0 : null,
        measures: {
          average: { op: "avg", field: measurement },
          minimum: { op: "min", field: measurement },
          maximum: { op: "max", field: measurement },
        },
        maxSeries: 20,
        profile: true,
      };
      return { options, query: seriesQuery(options), sql: seriesSQL(options) };
    } catch (cause) {
      return { error: cause instanceof Error ? cause.message : String(cause) };
    }
  }, [
    tableName,
    timestamp,
    measurement,
    group,
    every,
    calendar,
    origin,
    zeroFill,
    start,
    end,
    filters,
  ]);

  useEffect(() => {
    controller.current?.abort();
    setBusy(false);
    if (!built.options) return;
    const queryOptions = built.options;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const run = async () => {
      controller.current?.abort();
      const request = new AbortController();
      controller.current = request;
      setBusy(true);
      setError(undefined);
      try {
        const response = await client.series(queryOptions, { signal: request.signal });
        if (!cancelled) {
          setSeries(response.series);
          setMetadata(response.result.profile);
        }
      } catch (cause) {
        if (!cancelled && !request.signal.aborted)
          setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        if (!cancelled) {
          setBusy(false);
          if (poll) timer = setTimeout(run, 10000);
        }
      }
    };
    // Poll only after explicit enablement. Changing controls cancels the old request.
    if (poll) void run();
    return () => {
      cancelled = true;
      controller.current?.abort();
      if (timer) clearTimeout(timer);
    };
  }, [client, built, poll]);

  const run = async () => {
    if (!built.options) return;
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    setBusy(true);
    setError(undefined);
    try {
      const response = await client.series(built.options, { signal: request.signal });
      if (!request.signal.aborted) {
        setSeries(response.series);
        setMetadata(response.result.profile);
      }
    } catch (cause) {
      if (!request.signal.aborted) setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      if (!request.signal.aborted) setBusy(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Time series</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          Explore up to 20 series in a fixed [start, end) range. Calendar boundaries are UTC. Raw
          retention and algebraic index status are available in Schema and Indexes.
        </p>
        <div className="grid gap-3 md:grid-cols-2">
          {[
            ["Timestamp field", timestamp, setTimestamp],
            ["Measurement", measurement, setMeasurement],
            ["Group by (comma separated)", group, setGroup],
            ["Interval", every, setEvery],
            ["Fixed alignment origin (optional RFC3339)", origin, setOrigin],
            ["Start (RFC3339)", start, setStart],
            ["End (exclusive RFC3339)", end, setEnd],
          ].map(([label, value, setter]) => (
            <div key={label as string}>
              <Label>
                {label as string}
                <Input
                  value={value as string}
                  onChange={(event) => (setter as (value: string) => void)(event.target.value)}
                />
              </Label>
            </div>
          ))}
        </div>
        <Label>
          Dimension filters
          <Textarea value={filters} onChange={(event) => setFilters(event.target.value)} />
        </Label>
        <div className="flex items-center gap-4">
          <Label>
            <input
              type="checkbox"
              checked={calendar}
              onChange={(event) => setCalendar(event.target.checked)}
            />{" "}
            Calendar interval
          </Label>
          <Label>
            <input
              type="checkbox"
              checked={poll}
              onChange={(event) => setPoll(event.target.checked)}
            />{" "}
            Refresh every 10 seconds
          </Label>
          <Label>
            <input
              type="checkbox"
              checked={zeroFill}
              onChange={(event) => setZeroFill(event.target.checked)}
            />{" "}
            Fill missing measurements with zero
          </Label>
          <Button disabled={busy || Boolean(built.error)} onClick={() => void run()}>
            {busy ? "Loading…" : "Run"}
          </Button>
        </div>
        {(built.error || error) && (
          <Alert variant="destructive">
            <AlertDescription>{built.error || error}</AlertDescription>
          </Alert>
        )}
        {series.map((item) => (
          <SeriesChart key={item.key} series={item} />
        ))}
        <details>
          <summary>SQL and JSON requests</summary>
          <p className="text-sm text-muted-foreground">
            SQL returns populated buckets. JSON additionally fills empty buckets across the selected
            range.
          </p>
          <JsonViewer json={built.query ?? {}} />
          <JsonViewer json={built.sql ?? {}} />
        </details>
        {metadata ? (
          <details>
            <summary>Query profile</summary>
            <JsonViewer json={metadata} />
          </details>
        ) : null}
      </CardContent>
    </Card>
  );
}

function SeriesChart({ series }: { series: TimeSeries }) {
  const values = series.points.flatMap((point) =>
    point.measures.average == null ? [] : [point.measures.average]
  );
  const min = Math.min(...values),
    max = Math.max(...values);
  const x = (at: string) => {
    const first = Date.parse(series.points[0]?.at ?? at),
      last = Date.parse(series.points.at(-1)?.at ?? at);
    return 20 + (760 * (Date.parse(at) - first)) / Math.max(1, last - first);
  };
  const y = (value: number) => 170 - (150 * (value - min)) / Math.max(1e-12, max - min);
  let drawing = false;
  const path = series.points
    .map((point) => {
      const value = point.measures.average;
      if (value == null) {
        drawing = false;
        return "";
      }
      const command = drawing ? "L" : "M";
      drawing = true;
      return `${command}${x(point.at)},${y(value)}`;
    })
    .join(" ");
  return (
    <section className="space-y-2">
      <h3 className="font-medium">{series.key}</h3>
      {values.length ? (
        <svg
          role="img"
          aria-label={`Average measurement over time for ${series.key}`}
          viewBox="0 0 800 200"
          className="w-full text-primary"
        >
          <path d={path} stroke="currentColor" fill="none" strokeWidth="2" />
          {series.points.length === 1 && values.length === 1 && (
            <circle cx={20} cy={y(values[0])} r={3} fill="currentColor" />
          )}
        </svg>
      ) : (
        <p>No measurements in this range.</p>
      )}
      {values.length > 0 && (
        <p className="text-xs text-muted-foreground">
          Average range: {min} to {max} · {series.points[0]?.at} to{" "}
          {series.points[series.points.length - 1]?.at}
        </p>
      )}
      <details>
        <summary>{series.points.length} buckets · view data</summary>
        <div className="max-h-80 overflow-auto">
          <table className="w-full text-sm">
            <thead>
              <tr>
                <th>UTC bucket</th>
                <th>Count</th>
                <th>Average</th>
                <th>Min</th>
                <th>Max</th>
              </tr>
            </thead>
            <tbody>
              {series.points.map((point) => (
                <tr key={point.at}>
                  <td>{point.at}</td>
                  <td>{point.count}</td>
                  <td>{point.measures.average ?? "—"}</td>
                  <td>{point.measures.minimum ?? "—"}</td>
                  <td>{point.measures.maximum ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </section>
  );
}
