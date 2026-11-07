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
} from "@antfly/design-system";
import { timeSeriesTable } from "@antfly/sdk";
import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useApi } from "../../hooks/use-api-config";
import { useTable } from "../../hooks/use-table";
import JsonViewer from "../JsonViewer";

export function TimeSeriesPreset() {
  const api = useApi();
  const navigate = useNavigate();
  const { refreshTables, setSelectedTable } = useTable();
  const [name, setName] = useState("readings");
  const [timestamp, setTimestamp] = useState("observed_at");
  const [dimension, setDimension] = useState("device_id");
  const [measurement, setMeasurement] = useState("temperature");
  const [retention, setRetention] = useState("7d");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const preset = useMemo(() => {
    try {
      return {
        config: timeSeriesTable({
          timestamp,
          dimensions: [dimension],
          measurements: [measurement],
          retention: retention || undefined,
        }),
      };
    } catch (cause) {
      return { error: cause instanceof Error ? cause.message : String(cause) };
    }
  }, [timestamp, dimension, measurement, retention]);
  const create = async () => {
    if (!preset.config) return;
    setBusy(true);
    setError(undefined);
    try {
      await api.tables.create(name, preset.config);
      setSelectedTable(name);
      await refreshTables();
      navigate(`/tables/${encodeURIComponent(name)}?section=time-series`);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>Time-series preset</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-sm text-muted-foreground">
          Create an ordinary table with sortable typed fields and five-minute algebraic aggregate
          recipes. Retention expires raw readings and their aggregate contributions.
        </p>
        <div className="grid gap-3 md:grid-cols-3">
          {[
            ["Table name", name, setName],
            ["Timestamp", timestamp, setTimestamp],
            ["Dimension", dimension, setDimension],
            ["Measurement", measurement, setMeasurement],
            ["Raw retention (blank to keep)", retention, setRetention],
          ].map(([label, value, setter]) => (
            <Label key={label as string}>
              {label as string}
              <Input
                value={value as string}
                onChange={(event) => (setter as (value: string) => void)(event.target.value)}
              />
            </Label>
          ))}
        </div>
        {(preset.error || error) && (
          <Alert variant="destructive">
            <AlertDescription>{preset.error || error}</AlertDescription>
          </Alert>
        )}
        <details>
          <summary>Inspect generated schema and indexes</summary>
          <JsonViewer json={preset.config ?? {}} />
        </details>
        <Button disabled={busy || !name || !preset.config} onClick={() => void create()}>
          {busy ? "Creating…" : "Create time-series table"}
        </Button>
      </CardContent>
    </Card>
  );
}
