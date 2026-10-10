import {
  Button,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  Input,
} from "@antfly/design-system";
import {
  ArrowUpRight,
  Cpu,
  Laptop,
  MemoryStick,
  Pencil,
  Plus,
  SlidersHorizontal,
} from "lucide-react";
import { useId, useState } from "react";
import type { InferenceModel } from "@/data/inference-models";
import {
  type DeviceProfile,
  deviceProfileSchema,
  emptyModelProfile,
  type ModelProfile,
  modelProfileSchema,
  type ProfileAxis,
  profileAxes,
  type RuntimeProfile,
  readRuntimeProfile,
} from "@/lib/runtime-profile";
import { cn } from "@/lib/utils";

function point(index: number, radius: number): string {
  const angle = -Math.PI / 2 + (index * 2 * Math.PI) / 5;
  return `${180 + Math.cos(angle) * radius},${170 + Math.sin(angle) * radius}`;
}

export function ProfileRadar({ axes }: { axes: ProfileAxis[] }) {
  const titleId = useId();
  const complete = axes.every((axis) => axis.score !== undefined);
  const hasData = axes.some((axis) => axis.score !== undefined);
  return (
    <svg
      viewBox="0 0 360 340"
      role="img"
      aria-labelledby={titleId}
      className="mx-auto w-full max-w-sm"
    >
      <title id={titleId}>
        Model profile. {axes.map((axis) => `${axis.label}: ${axis.value}`).join(". ")}
      </title>
      {[0.25, 0.5, 0.75, 1].map((scale) => (
        <polygon
          key={scale}
          points={axes.map((_, i) => point(i, 106 * scale)).join(" ")}
          fill="none"
          className="stroke-border"
          strokeWidth="1"
        />
      ))}
      {axes.map((axis, i) => (
        <line
          key={axis.label}
          x1="180"
          y1="170"
          x2={point(i, 106).split(",")[0]}
          y2={point(i, 106).split(",")[1]}
          className="stroke-border"
        />
      ))}
      {complete && (
        <polygon
          points={axes.map((axis, i) => point(i, 106 * (axis.score ?? 0))).join(" ")}
          className="fill-primary/15 stroke-primary"
          strokeWidth="2"
        />
      )}
      {axes.map((axis, i) => {
        const [x, y] = point(i, 139).split(",").map(Number);
        const [dotX, dotY] = point(i, 106 * (axis.score ?? 0)).split(",");
        return (
          <g key={axis.label}>
            {axis.score !== undefined && (
              <circle cx={dotX} cy={dotY} r="3.5" className="fill-primary" />
            )}
            <text x={x} y={y} textAnchor="middle" className="fill-muted-foreground text-[11px]">
              {axis.label}
            </text>
            <text x={x} y={y + 16} textAnchor="middle" className="fill-foreground text-[10px]">
              {axis.value}
            </text>
          </g>
        );
      })}
      {!hasData && (
        <text x="180" y="174" textAnchor="middle" className="fill-muted-foreground text-[11px]">
          Add measurements
        </text>
      )}
    </svg>
  );
}

type Field = { key: string; label: string; placeholder?: string; numeric?: boolean; max?: number };
const deviceFields: Field[] = [
  { key: "name", label: "Device name", placeholder: "13-inch MacBook Air" },
  { key: "chip", label: "Chip / GPU", placeholder: "Apple M4" },
  { key: "cpuCores", label: "CPU cores", numeric: true },
  { key: "memoryGB", label: "Memory (GB)", numeric: true },
  { key: "bandwidthGBs", label: "Memory bandwidth (GB/s)", numeric: true },
];
const modelFields: Field[] = [
  { key: "quantization", label: "Quantization", placeholder: "Q4 QAT" },
  { key: "qualityPercent", label: "Evaluation score (%)", numeric: true, max: 100 },
  { key: "decodeTokensPerSecond", label: "Decode (tok/s)", numeric: true },
  { key: "prefillTokensPerSecond", label: "Prefill (tok/s)", numeric: true },
  { key: "peakMemoryGB", label: "Peak runtime memory (GB)", numeric: true },
  { key: "contextTokens", label: "Tested context (tokens)", numeric: true },
  {
    key: "notes",
    label: "Measurement source / workload",
    placeholder: "Artifact, prompt length, output length, evaluation, date…",
  },
];

function ProfileEditor({
  kind,
  initial,
  onSave,
  onClose,
}: {
  kind: "device" | "model";
  initial: DeviceProfile | ModelProfile;
  onSave: (value: DeviceProfile | ModelProfile) => void;
  onClose: () => void;
}) {
  const id = useId();
  const [error, setError] = useState("");
  const fields = kind === "device" ? deviceFields : modelFields;
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogContent className="max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            {kind === "device" ? "Configure runtime device" : "Record model measurements"}
          </DialogTitle>
          <DialogDescription>
            {kind === "device"
              ? "Enter the hardware running this connection. Browsers cannot reliably detect its chip or model. Changing hardware clears this connection’s recorded model measurements."
              : "Use measurements from this model on the configured device. Leave unknown values blank. These entries are saved in this browser."}
          </DialogDescription>
        </DialogHeader>
        <form
          className="space-y-4"
          onSubmit={(event) => {
            event.preventDefault();
            const data = new FormData(event.currentTarget);
            const values = Object.fromEntries(
              fields.map((field) => {
                const value = String(data.get(field.key) ?? "").trim();
                return [
                  field.key,
                  field.numeric ? (value === "" ? undefined : Number(value)) : value,
                ];
              })
            );
            const parsed = (kind === "device" ? deviceProfileSchema : modelProfileSchema).safeParse(
              values
            );
            if (!parsed.success) {
              setError(
                "Check the values. Measurements must be positive; evaluation scores must be between 0 and 100."
              );
              return;
            }
            onSave(parsed.data);
          }}
        >
          <div className="grid gap-4 sm:grid-cols-2">
            {fields.map((field) => (
              <label
                key={field.key}
                htmlFor={`${id}-${field.key}`}
                className={cn(
                  "space-y-1.5 text-xs font-medium",
                  field.key === "notes" && "sm:col-span-2"
                )}
              >
                <span>{field.label}</span>
                <Input
                  id={`${id}-${field.key}`}
                  name={field.key}
                  type={field.numeric ? "number" : "text"}
                  min={
                    field.key === "qualityPercent"
                      ? 0
                      : field.key === "cpuCores" || field.key === "contextTokens"
                        ? 1
                        : field.numeric
                          ? 0.001
                          : undefined
                  }
                  max={field.max}
                  step={field.key === "cpuCores" || field.key === "contextTokens" ? 1 : "any"}
                  maxLength={field.key === "notes" ? 1000 : 120}
                  defaultValue={
                    (initial as Record<string, string | number | undefined>)[field.key] ?? ""
                  }
                  placeholder={field.placeholder ?? "Not recorded"}
                />
              </label>
            ))}
          </div>
          {error && (
            <p role="alert" className="text-xs text-destructive">
              {error}
            </p>
          )}
          <div className="flex justify-end gap-2">
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit">Save {kind === "device" ? "device" : "measurements"}</Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function DeviceCard({
  device,
  connectionName,
  onEdit,
}: {
  device: DeviceProfile;
  connectionName: string;
  onEdit: () => void;
}) {
  return (
    <section
      aria-label="Runtime device"
      className="grid gap-5 border border-border bg-card p-5 sm:grid-cols-[120px_1fr] lg:p-6"
    >
      <div
        className="relative flex min-h-28 items-center justify-center border border-border bg-muted/30 text-primary"
        aria-hidden="true"
      >
        <Laptop className="h-20 w-20" strokeWidth={1} />
        <span className="absolute bottom-3 font-mono text-[9px] tracking-[0.2em]">RUNTIME</span>
      </div>
      <div className="min-w-0">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">
              {connectionName} / configured hardware
            </p>
            <h2 className="mt-1 text-xl font-semibold">{device.name || "Your runtime device"}</h2>
          </div>
          <Button variant="outline" size="sm" onClick={onEdit}>
            <Pencil className="mr-2 h-3.5 w-3.5" />
            {device.name ? "Edit device" : "Set up device"}
          </Button>
        </div>
        <div className="mt-5 grid gap-5 sm:grid-cols-2">
          <div>
            <p className="flex items-center gap-2 text-xs text-muted-foreground">
              <Cpu className="h-3.5 w-3.5" />
              Chip / GPU
            </p>
            <p className="mt-1 font-medium">{device.chip || "Not configured"}</p>
            {device.cpuCores !== undefined && (
              <p className="mt-1 text-xs text-muted-foreground">{device.cpuCores} CPU cores</p>
            )}
          </div>
          <div>
            <p className="flex items-center gap-2 text-xs text-muted-foreground">
              <MemoryStick className="h-3.5 w-3.5" />
              Memory
            </p>
            <p className="mt-1 font-medium">
              {device.memoryGB === undefined ? "Not configured" : `${device.memoryGB} GB`}
            </p>
            {device.bandwidthGBs !== undefined && (
              <p className="mt-1 text-xs text-muted-foreground">
                {device.bandwidthGBs} GB/s memory bandwidth
              </p>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}

// Mount per endpoint and connection so profiles never leak across runtime switches.
function ConnectionProfiles({
  models,
  storageKey,
  onDetails,
}: {
  models: InferenceModel[];
  storageKey: string;
  onDetails: (model: InferenceModel) => void;
}) {
  const [stored, setStored] = useState<RuntimeProfile>(() => {
    try {
      return readRuntimeProfile(localStorage.getItem(storageKey));
    } catch {
      return readRuntimeProfile(null);
    }
  });
  const [selectedId, setSelectedId] = useState(models[0]?.id ?? "");
  const [editor, setEditor] = useState<"device" | "model" | null>(null);
  const [saveError, setSaveError] = useState("");
  const selected = models.find((model) => model.id === selectedId) ?? models[0];
  const modelKey = selected ? JSON.stringify([selected.type, selected.source]) : "";
  const profile = stored.models[modelKey] ?? emptyModelProfile;
  const axes = profileAxes(stored.device, profile);
  const save = (value: RuntimeProfile) => {
    setStored(value);
    try {
      localStorage.setItem(storageKey, JSON.stringify(value));
      setSaveError("");
    } catch {
      setSaveError("Browser storage is unavailable. Changes will last until you leave this page.");
    }
    setEditor(null);
  };
  return (
    <div className="space-y-4">
      <DeviceCard
        device={stored.device}
        connectionName={models[0]?.connectionName ?? "Inference connection"}
        onEdit={() => setEditor("device")}
      />
      <section aria-label="Model profiles" className="overflow-hidden border border-border bg-card">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border p-5">
          <div>
            <h2 className="font-semibold">Find your balance</h2>
            <p className="mt-1 text-xs text-muted-foreground">
              Explore quality, speed, context and memory on your device.
            </p>
          </div>
          <span className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            {models.length} models / browser profiles
          </span>
        </div>
        <div className="grid md:grid-cols-[minmax(180px,0.8fr)_minmax(0,1.2fr)]">
          <div
            className="max-h-[460px] overflow-y-auto border-b border-border p-3 md:border-b-0 md:border-r"
            role="group"
            aria-label="Select a model profile"
          >
            {models.map((model) => (
              <button
                key={model.id}
                type="button"
                aria-pressed={selected?.id === model.id}
                onClick={() => setSelectedId(model.id)}
                className={cn(
                  "mb-1 flex w-full items-center gap-3 border p-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                  selected?.id === model.id
                    ? "border-primary/50 bg-primary/5"
                    : "border-transparent hover:bg-muted"
                )}
              >
                <Cpu
                  className={cn(
                    "h-4 w-4 shrink-0",
                    selected?.id === model.id ? "text-primary" : "text-muted-foreground"
                  )}
                />
                <span className="min-w-0">
                  <span className="block truncate text-sm font-medium">{model.name}</span>
                  <span className="block text-[11px] text-muted-foreground">
                    {model.type} · {model.provider}
                  </span>
                </span>
              </button>
            ))}
            {models.length === 0 && (
              <p className="p-3 text-sm text-muted-foreground">No models match your filters.</p>
            )}
          </div>
          <div className="min-w-0 p-5">
            {selected ? (
              <>
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <p className="font-medium">{selected.name}</p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      {profile.quantization || "Quantization not recorded"}
                    </p>
                  </div>
                  <Button size="sm" variant="ghost" onClick={() => onDetails(selected)}>
                    Details
                    <ArrowUpRight className="ml-1 h-3.5 w-3.5" />
                  </Button>
                </div>
                <ProfileRadar axes={axes} />
                <details className="text-xs text-muted-foreground">
                  <summary className="cursor-pointer">Values & chart scales</summary>
                  <dl className="mt-3 space-y-2">
                    {axes.map((axis) => (
                      <div key={axis.label}>
                        <dt className="font-medium text-foreground">
                          {axis.label}: {axis.value}
                        </dt>
                        <dd>Outer ring: {axis.scale}</dd>
                      </div>
                    ))}
                  </dl>
                  <p className="mt-2">
                    Values above the chart scale are capped at the outer ring. Missing values are
                    omitted; a filled profile requires all five axes.
                  </p>
                </details>
                {profile.notes && (
                  <p className="mt-3 break-words text-xs text-muted-foreground">{profile.notes}</p>
                )}
                <div className="mt-4 flex flex-wrap items-center gap-2">
                  <Button size="sm" variant="outline" onClick={() => setEditor("model")}>
                    <Plus className="mr-1.5 h-3.5 w-3.5" />
                    Record measurements
                  </Button>
                  {stored.models[modelKey] && (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        const nextModels = { ...stored.models };
                        delete nextModels[modelKey];
                        save({ ...stored, models: nextModels });
                      }}
                    >
                      Clear measurements
                    </Button>
                  )}
                </div>
                <p className="mt-3 text-[11px] text-muted-foreground">
                  User-recorded values, scoped to this connection. No speed or quality estimates are
                  supplied.
                </p>
              </>
            ) : (
              <p className="text-sm text-muted-foreground">
                Select a connection with available models to build a profile.
              </p>
            )}
          </div>
        </div>
      </section>
      {saveError && (
        <p role="alert" className="text-xs text-destructive">
          {saveError}
        </p>
      )}
      {editor && (
        <ProfileEditor
          key={`${editor}-${modelKey}`}
          kind={editor}
          initial={editor === "device" ? stored.device : profile}
          onClose={() => setEditor(null)}
          onSave={(value) => {
            if (editor === "device") {
              const device = value as DeviceProfile;
              save({
                ...stored,
                device,
                models:
                  JSON.stringify(device) === JSON.stringify(stored.device) ? stored.models : {},
              });
            } else
              save({ ...stored, models: { ...stored.models, [modelKey]: value as ModelProfile } });
          }}
        />
      )}
    </div>
  );
}

export function RuntimeProfiles({
  models,
  endpointKey,
  onDetails,
}: {
  models: InferenceModel[];
  endpointKey: string;
  onDetails: (model: InferenceModel) => void;
}) {
  const [connectionId, setConnectionId] = useState("");
  const connections = [
    ...new Map(models.map((model) => [model.connectionId, model.connectionName])).entries(),
  ];
  const selectedConnection =
    connections.find(([id]) => id === connectionId)?.[0] ?? connections[0]?.[0] ?? "";
  const storageKey = `antfarm-runtime-profile:${JSON.stringify([endpointKey, selectedConnection])}`;
  return (
    <div className="mb-6 space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <SlidersHorizontal className="h-4 w-4 text-muted-foreground" />
        <label className="flex items-center gap-2 text-xs text-muted-foreground">
          Runtime connection
          <select
            aria-label="Runtime profile connection"
            value={selectedConnection}
            onChange={(event) => setConnectionId(event.target.value)}
            className="max-w-64 border border-border bg-background px-2 py-1.5 text-foreground"
          >
            {connections.map(([id, name]) => (
              <option key={id} value={id}>
                {name}
              </option>
            ))}
            {connections.length === 0 && <option value="">No matching connections</option>}
          </select>
        </label>
      </div>
      <ConnectionProfiles
        key={storageKey}
        storageKey={storageKey}
        models={models.filter((model) => model.connectionId === selectedConnection)}
        onDetails={onDetails}
      />
    </div>
  );
}
