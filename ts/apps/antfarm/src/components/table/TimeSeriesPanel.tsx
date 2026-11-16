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
import {
  type SeriesQueryOptions,
  seriesQuery,
  seriesSQL,
  seriesTimestampNanoseconds,
  type TimeSeries,
} from "@antfly/sdk";
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
  const [start, setStart] = useState(() =>
    new Date(Math.floor(Date.now() / 300000) * 300000 - 3600000).toISOString()
  );
  const [end, setEnd] = useState(() =>
    new Date(Math.floor(Date.now() / 300000) * 300000).toISOString()
  );
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

export function SeriesChart({ series }: { series: TimeSeries }) {
  const [dataOpen, setDataOpen] = useState(false);
  const [page, setPage] = useState(0);
  const pageSize = 100;
  const [previousSeries, setPreviousSeries] = useState(series);
  // Adjust before rendering the replacement series, rather than showing one
  // frame of the old page and scheduling another render from an effect.
  if (previousSeries !== series) {
    setPreviousSeries(series);
    setPage(0);
  }
  const chart = useMemo(() => {
    const first = series.points[0];
    const last = series.points.at(-1);
    if (!first || !last)
      return {
        path: "",
        dots: [] as { at: TimeSeries["points"][number]["at"]; x: number; y: number }[],
        min: 0,
        max: 0,
        count: 0,
      };
    const start = seriesTimestampNanoseconds(first.at);
    const span = seriesTimestampNanoseconds(last.at) - start;
    const x = (index: number) =>
      20 +
      (span === 0n
        ? 0
        : (760 * Number(seriesTimestampNanoseconds(series.points[index].at) - start)) /
          Number(span));
    const gaps = new Uint32Array(series.points.length + 1);
    const bins = new Map<number, { first: number; last: number; min: number; max: number }>();
    let min = Infinity,
      max = -Infinity,
      count = 0;
    series.points.forEach((point, index) => {
      const value = point.measures.average;
      gaps[index + 1] = gaps[index] + (value == null ? 1 : 0);
      if (value == null) return;
      min = Math.min(min, value);
      max = Math.max(max, value);
      count++;
      const bin = Math.floor(x(index));
      const prior = bins.get(bin);
      if (!prior) bins.set(bin, { first: index, last: index, min: index, max: index });
      else {
        prior.last = index;
        if (value < (series.points[prior.min].measures.average ?? Infinity)) prior.min = index;
        if (value > (series.points[prior.max].measures.average ?? -Infinity)) prior.max = index;
      }
    });
    // At most four samples per horizontal pixel. Preserve extrema and never
    // connect selected samples across a missing measurement, even within a bin.
    const selected = [
      ...new Set([...bins.values()].flatMap((bin) => [bin.first, bin.min, bin.max, bin.last])),
    ].sort((a, b) => a - b);
    const y = (value: number) => 170 - (150 * (value - min)) / Math.max(1e-12, max - min);
    const samples = selected.map((index, i) => ({
      at: series.points[index].at,
      x: x(index),
      y: y(series.points[index].measures.average ?? 0),
      begins: i === 0 || gaps[index] > gaps[selected[i - 1] + 1],
    }));
    return {
      min,
      max,
      count,
      path: samples.map((point) => `${point.begins ? "M" : "L"}${point.x},${point.y}`).join(" "),
      dots: samples.filter(
        (point, i) => point.begins && (i === samples.length - 1 || samples[i + 1].begins)
      ),
    };
  }, [series]);
  const pageCount = Math.max(1, Math.ceil(series.points.length / pageSize));
  const currentPage = Math.min(page, pageCount - 1);
  return (
    <section className="space-y-2">
      <h3 className="font-medium">{series.key}</h3>
      {chart.count ? (
        <svg
          role="img"
          aria-label={`Average measurement over time for ${series.key}`}
          viewBox="0 0 800 200"
          className="w-full text-primary"
        >
          <path d={chart.path} stroke="currentColor" fill="none" strokeWidth="2" />
          {chart.dots.map((point) => (
            <circle key={point.at} cx={point.x} cy={point.y} r={3} fill="currentColor" />
          ))}
        </svg>
      ) : (
        <p>No measurements in this range.</p>
      )}
      {chart.count > 0 && (
        <p className="text-xs text-muted-foreground">
          Average range: {chart.min} to {chart.max} · {series.points[0]?.at} to{" "}
          {series.points.at(-1)?.at}
        </p>
      )}
      <details onToggle={(event) => setDataOpen(event.currentTarget.open)}>
        <summary>{series.points.length} buckets · view data</summary>
        {dataOpen && (
          <div className="space-y-2">
            <div className="flex gap-2 items-center">
              <Button disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>
                Previous buckets
              </Button>
              <span>
                Page {currentPage + 1} of {pageCount}
              </span>
              <Button
                disabled={currentPage + 1 === pageCount}
                onClick={() => setPage(currentPage + 1)}
              >
                Next buckets
              </Button>
            </div>
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
                  {series.points
                    .slice(currentPage * pageSize, (currentPage + 1) * pageSize)
                    .map((point) => (
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
          </div>
        )}
      </details>
    </section>
  );
}
