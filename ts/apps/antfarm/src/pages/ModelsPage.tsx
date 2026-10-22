import {
  Button,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
  DashboardToolbar,
  Input,
  MonoLabel,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@antfly/design-system";
import {
  Apple,
  ArrowRight,
  ArrowUpDown,
  AudioLines,
  BookOpen,
  Check,
  ChevronDown,
  CircleDot,
  Cloud,
  Copy,
  Cpu,
  Download,
  ExternalLink,
  Fingerprint,
  Gauge,
  Monitor,
  Package,
  Power,
  PowerOff,
  RefreshCw,
  Scissors,
  Search,
  Sparkles,
  Star,
  Tag,
  X,
  Zap,
} from "lucide-react";
import type React from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { AIWorkflowHeader, AIWorkflowPage } from "@/components/ai-workflow";
import { ErrorState, NoResultsState } from "@/components/branded-empty-state";
import { ConnectedProvidersSummary } from "@/components/ConnectedProvidersSummary";
import { RuntimeProfiles } from "@/components/models/RuntimeProfiles";
import {
  type Backend,
  type ExtractorCapability,
  getDownloadCommand,
  getHardwareCapabilities,
  HARDWARE_INFO,
  type HardwareCapability,
  type InferenceModel,
  MODEL_TYPE_DETAILS,
  MODEL_TYPE_PLAYGROUND,
  type ModelType,
  type QuantizationOption,
  type QuantizationType,
  VARIANT_PRESETS,
  type VariantPreset,
} from "@/data/inference-models";
import { useApiConfig } from "@/hooks/use-api-config";
import { useInferenceRegistry } from "@/hooks/use-inference-registry";
import {
  type ModelInstance,
  type ModelInstancesForModel,
  modelInstanceTask,
  useModelInstancesForModel,
} from "@/hooks/use-model-instances";
import { huggingFaceRepoId } from "@/lib/huggingface-model-ref";
import { cn } from "@/lib/utils";

// Icon mapping for model types
const MODEL_TYPE_ICONS: Record<ModelType, React.FC<{ className?: string }>> = {
  decider: Sparkles,
  synthesizer: AudioLines,
  predictor: Fingerprint,
  embedder: Fingerprint,
  reranker: ArrowUpDown,
  chunker: Scissors,
  extractor: Tag,
  rewriter: RefreshCw,
  generator: Sparkles,
  reader: BookOpen,
  transcriber: AudioLines,
};

// Model-type chrome is neutral mono — the type is already carried by its
// icon and name, so it takes no per-type hue (single amber accent language).
const NEUTRAL_ACCENT = {
  text: "text-muted-foreground",
  border: "border-border",
} as const;

// Capability chips are neutral mono — the capability is carried by its label.
const CAPABILITY_CHIP = "bg-muted text-muted-foreground border-border";

const CAPABILITY_LABELS: Record<ExtractorCapability, string> = {
  labels: "NER",
  zeroshot: "Zero-Shot",
  relations: "Relations",
  answers: "QA",
};

function capabilityLabel(capability: string): string {
  return CAPABILITY_LABELS[capability as ExtractorCapability] ?? capability.replaceAll("_", " ");
}

function modelProvider(model: InferenceModel): string {
  // Local inference's connection provider is Antfly; the namespace identifies
  // the model publisher (for example, google or fastino).
  const namespace = huggingFaceRepoId(model.source)?.split("/")[0];
  return (namespace || model.provider).toLowerCase();
}

function runtimeStatus(runtime: ModelInstancesForModel | undefined) {
  if (!runtime) return { label: "Available", tone: "text-muted-foreground" };
  if (
    runtime.load_state === "loading" ||
    runtime.runtimes.some((item) => item.state === "loading")
  ) {
    return { label: "Loading", tone: "af-status-text-warning" };
  }
  if (runtime.runtimes.some((item) => item.active_handles > 0)) {
    return { label: "In use", tone: "af-status-text-info" };
  }
  if (runtime.runtimes.length > 0) return { label: "Loaded", tone: "af-status-text-success" };
  return { label: "Available", tone: "text-muted-foreground" };
}

function runtimeInstanceLabel(instance: ModelInstance): string {
  if (instance.state === "loading") return "Loading";
  if (instance.active_handles > 0) return "In use";
  return "Loaded";
}

// Hardware capability icons
const HARDWARE_ICONS: Record<HardwareCapability, React.FC<{ className?: string }>> = {
  "nvidia-gpu": Monitor,
  "apple-silicon": Apple,
  "google-tpu": Cloud,
  cpu: Cpu,
};

// Hardware badges are neutral mono — each kind is differentiated by its icon
// (single amber accent language; no per-vendor hue).
const HARDWARE_CHIP = "bg-muted text-muted-foreground border-border";

// Hardware badges component
const HardwareBadges: React.FC<{
  backends?: string[];
  showLabels?: boolean;
}> = ({ backends, showLabels = true }) => {
  if (!backends || backends.length === 0) {
    return <span className="text-xs text-muted-foreground">Reported after runtime load</span>;
  }
  const capabilities = getHardwareCapabilities(backends as Backend[]);

  return (
    <TooltipProvider>
      <div className="flex flex-wrap gap-1.5">
        {capabilities.map((cap) => {
          const Icon = HARDWARE_ICONS[cap];
          const info = HARDWARE_INFO[cap];

          return (
            <Tooltip key={cap}>
              <TooltipTrigger asChild>
                <span
                  className={cn(
                    "inline-flex items-center gap-1.5 px-2 py-1 rounded-none text-xs font-mono border cursor-help",
                    HARDWARE_CHIP
                  )}
                >
                  <Icon className="w-3 h-3" />
                  {showLabels && <span>{info.shortLabel}</span>}
                </span>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="max-w-xs">
                <p className="font-medium">{info.label}</p>
                <p className="text-xs text-muted-foreground">{info.description}</p>
              </TooltipContent>
            </Tooltip>
          );
        })}
      </div>
    </TooltipProvider>
  );
};

// HuggingFace stats types and cache
interface HFStats {
  downloads: number;
  likes: number;
  loading: boolean;
  error: boolean;
}

// Global cache for HF stats to persist across re-renders
const hfStatsCache = new Map<string, { downloads: number; likes: number }>();

// Format large numbers (1234 -> 1.2k, 1234567 -> 1.2M)
function formatNumber(num: number): string {
  if (num >= 1_000_000) {
    return `${(num / 1_000_000).toFixed(1)}M`;
  }
  if (num >= 1_000) {
    return `${(num / 1_000).toFixed(1)}k`;
  }
  return num.toString();
}

// Hook to fetch HuggingFace stats for a model
function useHFStats(source: string): HFStats {
  const repoId = huggingFaceRepoId(source);
  const [stats, setStats] = useState<HFStats>(() => {
    if (!repoId) {
      return { downloads: 0, likes: 0, loading: false, error: true };
    }
    const cached = hfStatsCache.get(repoId);
    if (cached) {
      return { ...cached, loading: false, error: false };
    }
    return { downloads: 0, likes: 0, loading: true, error: false };
  });

  useEffect(() => {
    if (!repoId) {
      setStats({ downloads: 0, likes: 0, loading: false, error: true });
      return;
    }
    const normalizedRepoId = repoId;

    // Skip if already cached
    const cached = hfStatsCache.get(normalizedRepoId);
    if (cached) {
      setStats({ ...cached, loading: false, error: false });
      return;
    }

    const controller = new AbortController();

    async function fetchStats() {
      try {
        const response = await fetch(`https://huggingface.co/api/models/${normalizedRepoId}`, {
          signal: controller.signal,
        });

        // Check if aborted before processing response
        if (controller.signal.aborted) return;

        if (!response.ok) {
          // Handle rate limiting specifically
          if (response.status === 429) {
            console.warn(`HuggingFace API rate limited for ${normalizedRepoId}`);
          }
          throw new Error(`Failed to fetch: ${response.status}`);
        }

        const data = await response.json();

        // Check if aborted before updating state
        if (controller.signal.aborted) return;

        const result = {
          downloads: data.downloads || 0,
          likes: data.likes || 0,
        };

        // Cache the result
        hfStatsCache.set(normalizedRepoId, result);
        setStats({ ...result, loading: false, error: false });
      } catch (err) {
        if ((err as Error).name !== "AbortError" && !controller.signal.aborted) {
          setStats((prev) => ({ ...prev, loading: false, error: true }));
        }
      }
    }

    fetchStats();

    return () => controller.abort();
  }, [repoId]);

  return stats;
}

// HuggingFace logo as inline SVG
const HFLogo: React.FC<{ className?: string }> = ({ className }) => (
  <svg
    className={className}
    viewBox="0 0 95 88"
    fill="currentColor"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path d="M47.2 0C26.6 0 9.8 16.8 9.8 37.4c0 7.8 2.4 15 6.5 21L.8 87.5l29.6-15c5.2 2.5 11 3.9 17.2 3.9 20.6 0 37.4-16.8 37.4-37.4C85 17.4 67.8 0 47.2 0zm0 67.8c-4.6 0-9-1-13-2.8l-1-.4-9.8 5.1 5.2-9.5-.8-1.2c-3.4-5.2-5.2-11.2-5.2-17.6 0-18 14.6-32.6 32.6-32.6S79.8 23.4 79.8 41.4 65.2 67.8 47.2 67.8z" />
    <circle cx="35" cy="38" r="5" />
    <circle cx="59" cy="38" r="5" />
    <path
      d="M58.5 50c-2 3.5-6 6-11 6s-9-2.5-11-6"
      strokeWidth="3"
      stroke="currentColor"
      fill="none"
      strokeLinecap="round"
    />
  </svg>
);

// Stats display component for cards
const ModelStats: React.FC<{ source: string; compact?: boolean }> = ({ source, compact }) => {
  const { downloads, likes, loading, error } = useHFStats(source);

  if (!huggingFaceRepoId(source)) {
    return null;
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-xs text-muted-foreground/50">
        <span className="w-8 h-3 bg-muted animate-pulse rounded-none" />
        <span className="w-8 h-3 bg-muted animate-pulse rounded-none" />
      </div>
    );
  }

  if (error) {
    return null;
  }

  if (compact) {
    return (
      <div
        className="flex items-center gap-2 text-xs text-muted-foreground"
        title="HuggingFace stats"
      >
        <HFLogo className="w-3 h-3 opacity-50" />
        <span className="flex items-center gap-1">
          <Download className="w-3 h-3" />
          {formatNumber(downloads)}
        </span>
        <span className="flex items-center gap-1">
          <Star className="w-3 h-3" />
          {formatNumber(likes)}
        </span>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-2 px-3 py-2 rounded-none bg-muted/50 border border-border/50">
      <HFLogo className="w-4 h-4 text-muted-foreground" />
      <div className="flex items-center gap-3 text-xs">
        <span className="flex items-center gap-1.5 text-muted-foreground">
          <Download className="w-3.5 h-3.5" />
          <span className="font-medium text-foreground">{formatNumber(downloads)}</span>
        </span>
        <span className="flex items-center gap-1.5 text-muted-foreground">
          <Star className="w-3.5 h-3.5" />
          <span className="font-medium text-foreground">{formatNumber(likes)}</span>
        </span>
      </div>
      <span className="text-[10px] text-muted-foreground/60 ml-auto">on HuggingFace</span>
    </div>
  );
};

// Animated number counter for hero stats
const AnimatedCounter: React.FC<{ value: number; duration?: number }> = ({
  value,
  duration = 1000,
}) => {
  const [count, setCount] = useState(0);
  const isMountedRef = useRef(true);

  useEffect(() => {
    isMountedRef.current = true;
    let startTime: number;
    let animationFrame: number;

    const animate = (timestamp: number) => {
      if (!isMountedRef.current) return;
      if (!startTime) startTime = timestamp;
      const progress = Math.min((timestamp - startTime) / duration, 1);
      const easeOut = 1 - (1 - progress) ** 3;
      setCount(Math.floor(easeOut * value));

      if (progress < 1 && isMountedRef.current) {
        animationFrame = requestAnimationFrame(animate);
      }
    };

    animationFrame = requestAnimationFrame(animate);
    return () => {
      isMountedRef.current = false;
      cancelAnimationFrame(animationFrame);
    };
  }, [value, duration]);

  return <span>{count}</span>;
};

const TypePill: React.FC<{
  type: ModelType;
  typeName: string;
  count: number;
  selected: boolean;
  onClick: () => void;
}> = ({ typeName, count, selected, onClick }) => {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={selected}
      className={cn(
        "px-3 py-1.5 rounded-none text-sm transition-colors",
        selected
          ? "bg-foreground text-background"
          : "text-muted-foreground hover:text-foreground hover:bg-muted"
      )}
    >
      {typeName}
      <span
        className={cn(
          "ml-1.5 tabular-nums text-xs",
          selected ? "text-background/60" : "text-muted-foreground/50"
        )}
      >
        {count}
      </span>
    </button>
  );
};

const ModelCard: React.FC<{
  model: InferenceModel;
  runtime?: ModelInstancesForModel;
  providerManaged: boolean;
  onClick: () => void;
}> = ({ model, runtime, providerManaged, onClick }) => {
  const Icon = MODEL_TYPE_ICONS[model.type];
  const status = runtimeStatus(runtime);
  const builtIn = model.type === "chunker" && model.source === "fixed";

  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={`View details for ${model.name}`}
      className={cn(
        "group text-left w-full",
        "bg-card border border-border rounded-none p-5",
        "transition-colors duration-150",
        "hover:bg-accent/50",
        "focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 focus:ring-offset-background"
      )}
    >
      <div className="flex items-start gap-3 mb-3">
        <div className="flex items-center justify-center w-9 h-9 rounded-none bg-muted shrink-0">
          <Icon className="w-4 h-4 text-muted-foreground" />
        </div>
        <div className="min-w-0">
          <h3 className="font-semibold text-sm text-foreground leading-tight truncate">
            {model.name}
          </h3>
          <p className="text-xs text-muted-foreground truncate mt-0.5 font-mono">
            {model.connectionName} · {model.provider}
          </p>
        </div>
      </div>

      <p className="text-sm text-muted-foreground line-clamp-2 mb-4 leading-relaxed">
        {model.description}
      </p>

      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          {builtIn ? (
            <span className="inline-flex items-center gap-1 af-status-text-success">
              <CircleDot className="w-3 h-3" />
              Built-in
            </span>
          ) : providerManaged ? (
            <span className="inline-flex items-center gap-1">
              <Zap className="w-3 h-3" />
              {model.freshness === "configured" ? "Configured" : "Ready"}
            </span>
          ) : (
            <span className={cn("inline-flex items-center gap-1", status.tone)}>
              <CircleDot className="w-3 h-3" />
              {status.label}
            </span>
          )}
          {model.inRegistry && providerManaged ? (
            <span className="inline-flex items-center gap-1">
              <Zap className="w-3 h-3" />
              Available
            </span>
          ) : !model.inRegistry ? (
            <span className="inline-flex items-center gap-1">
              <Package className="w-3 h-3" />
              Export
            </span>
          ) : null}
          {model.capabilities?.slice(0, 2).map((cap) => (
            <span key={cap}>{capabilityLabel(cap)}</span>
          ))}
          {model.capabilities && model.capabilities.length > 2 && (
            <span>+{model.capabilities.length - 2}</span>
          )}
        </div>

        <ModelStats source={model.source} compact />
      </div>
    </button>
  );
};

// Model detail sheet - refined documentation style
const ModelDetailSheet: React.FC<{
  model: InferenceModel | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  types: { type: ModelType; name: string }[];
  quantizationOptions: QuantizationOption[];
  runtime?: ModelInstancesForModel;
  runtimeSupported: boolean;
  providerManaged: boolean;
  actionPending: boolean;
  runtimeError: string | null;
  onLoad: (task: string, model: string) => Promise<unknown>;
  onUnload: (runtimeId: string) => Promise<unknown>;
}> = ({
  model,
  open,
  onOpenChange,
  types,
  quantizationOptions,
  runtime,
  runtimeSupported,
  providerManaged,
  actionPending,
  runtimeError,
  onLoad,
  onUnload,
}) => {
  const navigate = useNavigate();
  const { setInferenceConnectionId } = useApiConfig();
  const [selectedVariant, setSelectedVariant] = useState<QuantizationType | undefined>(undefined);
  const [selectedPreset, setSelectedPreset] = useState<VariantPreset | null>("recommended");
  const [showAllVariants, setShowAllVariants] = useState(false);
  const [copiedCommand, setCopiedCommand] = useState(false);

  // Reset state when model changes
  useEffect(() => {
    if (model) {
      // Auto-select recommended variant
      const recommendedPreset = VARIANT_PRESETS.find((p) => p.type === "recommended");
      const recommendedVariant = recommendedPreset?.selectVariant(model.variants);
      setSelectedVariant(recommendedVariant);
      setSelectedPreset("recommended");
      setShowAllVariants(false);
    }
  }, [model]);

  if (!model) return null;

  const Icon = MODEL_TYPE_ICONS[model.type];
  const accent = NEUTRAL_ACCENT;
  const typeInfo = types.find((t) => t.type === model.type);
  const command = getDownloadCommand(model, selectedVariant);

  const handleCopyCommand = async () => {
    await navigator.clipboard.writeText(command);
    setCopiedCommand(true);
    setTimeout(() => setCopiedCommand(false), 2000);
  };

  const availableVariants = quantizationOptions.filter((opt) => {
    if (opt.generatorOnly && model.type !== "generator") return false;
    return model.variants.includes(opt.type);
  });
  const canShowDownloadCommand = model.allowDownloads === true && model.sourceUrl !== "";
  const status = runtimeStatus(runtime);
  const builtIn = model.type === "chunker" && model.source === "fixed";

  const selectedVariantInfo = selectedVariant
    ? availableVariants.find((q) => q.type === selectedVariant)
    : null;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full sm:max-w-md overflow-y-auto p-0">
        {/* Header */}
        <div className={cn("relative px-6 pt-6 pb-5", "border-b border-border/50")}>
          <SheetHeader className="relative">
            <div className="flex items-start gap-3.5">
              <div
                className={cn(
                  "flex items-center justify-center w-11 h-11 rounded-none",
                  "bg-background border",
                  accent.border
                )}
              >
                <Icon className={cn("w-5 h-5", accent.text)} />
              </div>
              <div className="flex-1 min-w-0 pt-0.5">
                <SheetTitle className="text-lg font-semibold leading-tight">
                  {model.name}
                </SheetTitle>
                <SheetDescription className="font-mono text-[11px] mt-1 truncate">
                  {model.source}
                </SheetDescription>
              </div>
            </div>

            {/* Status badges inline with header */}
            <div className="flex items-center gap-2 mt-4">
              <span
                className={cn(
                  "inline-flex items-center gap-1.5 px-2 py-1 rounded-none text-xs font-mono border",
                  accent.text,
                  accent.border,
                  "bg-muted"
                )}
              >
                <Icon className="w-3 h-3" />
                {typeInfo?.name}
              </span>

              {model.inRegistry ? (
                <span className="inline-flex items-center gap-1 px-2 py-1 rounded-none text-xs font-medium bg-success/10 text-success dark:text-success border border-success/20">
                  <Zap className="w-3 h-3" />
                  {model.freshness === "configured" ? "Configured" : "Ready"}
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 px-2 py-1 rounded-none text-xs bg-muted text-muted-foreground border border-border">
                  <Package className="w-3 h-3" />
                  Export
                </span>
              )}

              {model.size && (
                <span className="px-2 py-1 rounded-none text-xs font-mono text-muted-foreground bg-muted/50 border border-border/50">
                  {model.size}
                </span>
              )}
            </div>
          </SheetHeader>
        </div>

        {/* Content */}
        <div className="px-6 py-5 space-y-5">
          {/* Description */}
          <p className="text-sm text-muted-foreground leading-relaxed">{model.description}</p>

          {/* Hardware is authoritative only after runtime selection. */}
          <div>
            <h4 className="text-xs font-medium text-muted-foreground mb-2">Catalog targets</h4>
            <HardwareBadges backends={model.backends} />
          </div>

          {/* Capabilities - only if present */}
          {model.capabilities && model.capabilities.length > 0 && (
            <div>
              <h4 className="text-xs font-medium text-muted-foreground mb-2">Capabilities</h4>
              <div className="flex flex-wrap gap-1.5">
                {model.capabilities.map((cap) => (
                  <span
                    key={cap}
                    className={cn(
                      "inline-flex items-center gap-1 px-2 py-0.5 rounded-none text-xs font-mono border",
                      CAPABILITY_CHIP
                    )}
                  >
                    <Check className="w-3 h-3" />
                    {capabilityLabel(cap)}
                  </span>
                ))}
              </div>
            </div>
          )}

          <div className="border border-border bg-background-secondary/25 p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h4 className="font-mono text-xs font-medium uppercase tracking-[0.08em]">
                  Model status
                </h4>
                <p className="mt-1 text-xs text-muted-foreground">
                  {builtIn
                    ? "This built-in chunking mode does not load a model."
                    : providerManaged
                      ? "Loading and residency are managed by this provider."
                      : !runtimeSupported
                        ? "This server does not support model instance controls. Running a request still loads the model on demand."
                        : "Load and unload affect the shared server cache, including other clients. Idle eviction still applies."}
                </p>
              </div>
              <span className={cn("shrink-0 font-mono text-xs", status.tone)}>
                {builtIn
                  ? "Built-in · Available"
                  : providerManaged
                    ? "Provider-managed"
                    : status.label}
              </span>
            </div>

            {!builtIn && !providerManaged && runtimeSupported ? (
              <div className="mt-3 space-y-2">
                {(runtime?.runtimes ?? []).map((instance: ModelInstance) => (
                  <div
                    key={instance.id}
                    className="flex items-center justify-between gap-3 border-t border-border pt-2"
                  >
                    <div className="min-w-0 text-xs">
                      <div className="font-mono text-foreground">
                        {runtimeInstanceLabel(instance)}
                        {instance.backend ? ` · ${instance.backend}` : ""}
                      </div>
                      <div className="truncate text-muted-foreground">
                        {instance.residency_mode ? ` · ${instance.residency_mode}` : ""}
                        {instance.startup_managed ? " · Startup managed" : ""}
                      </div>
                    </div>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={actionPending || !instance.unloadable}
                      title={
                        instance.startup_managed
                          ? "Startup-managed models cannot be unloaded here"
                          : instance.unload_blocked_reason === "in_use"
                            ? "Model is busy; try again when idle"
                            : undefined
                      }
                      onClick={() => void onUnload(instance.id).catch(() => undefined)}
                    >
                      <PowerOff className="size-3.5" /> Unload
                    </Button>
                  </div>
                ))}
                {(runtime?.runtimes.length ?? 0) === 0 ? (
                  <Button
                    type="button"
                    size="sm"
                    className="w-full"
                    disabled={actionPending || runtime?.load_state === "loading"}
                    onClick={() => void onLoad(model.type, model.source).catch(() => undefined)}
                  >
                    <Power className="size-3.5" />
                    {runtime?.load_state === "loading" ? "Loading…" : "Load model"}
                  </Button>
                ) : null}
                {runtimeError ? (
                  <p role="alert" className="text-xs text-destructive">
                    {runtimeError}
                  </p>
                ) : null}
              </div>
            ) : null}
          </div>

          {/* Download Configuration */}
          {!canShowDownloadCommand ? (
            <div className="rounded-none border border-border bg-muted/30 px-4 py-3">
              <p className="text-xs text-muted-foreground">
                {model.provider === "antfly"
                  ? "This model is provided by the running Antfly inference runtime and does not need a download command."
                  : `Manage this model through ${model.connectionName}. Antfly CLI downloads are only available for Antfly inference models.`}
              </p>
            </div>
          ) : (
            <div className="rounded-none border border-border bg-muted/30 overflow-hidden">
              {/* Preset selector */}
              <div className="px-4 py-3 border-b border-border/50">
                <h4 className="text-xs font-medium text-foreground mb-3">Download variant</h4>

                {/* Preset buttons */}
                <div className="flex flex-wrap gap-2 mb-3">
                  {VARIANT_PRESETS.map((preset) => {
                    const presetVariant = preset.selectVariant(model.variants);
                    const isAvailable = presetVariant !== undefined;
                    const isSelected = selectedPreset === preset.type;

                    return (
                      <button
                        key={preset.type}
                        type="button"
                        disabled={!isAvailable}
                        onClick={() => {
                          if (isAvailable) {
                            setSelectedPreset(preset.type);
                            setSelectedVariant(presetVariant);
                          }
                        }}
                        className={cn(
                          "flex-1 min-w-[100px] px-3 py-2 rounded-none text-left transition-all",
                          isSelected
                            ? "bg-foreground text-background"
                            : isAvailable
                              ? "bg-background text-foreground border border-border hover:border-foreground/30"
                              : "bg-muted/50 text-muted-foreground/50 border border-border/50 cursor-not-allowed"
                        )}
                      >
                        <div className="flex items-center gap-2">
                          {preset.type === "recommended" && <Zap className="w-3.5 h-3.5" />}
                          {preset.type === "smallest" && <Gauge className="w-3.5 h-3.5" />}
                          {preset.type === "highest-quality" && (
                            <Sparkles className="w-3.5 h-3.5" />
                          )}
                          <span className="text-xs font-medium">{preset.label}</span>
                        </div>
                        <p
                          className={cn(
                            "text-[10px] mt-0.5 leading-tight",
                            isSelected ? "text-background/70" : "text-muted-foreground"
                          )}
                        >
                          {preset.description}
                        </p>
                      </button>
                    );
                  })}
                </div>

                {/* Selected variant info */}
                {selectedVariantInfo && (
                  <div className="flex items-center justify-between text-xs text-muted-foreground px-1">
                    <span>
                      Selected:{" "}
                      <span className="font-medium text-foreground">
                        {selectedVariantInfo.name}
                      </span>
                    </span>
                  </div>
                )}

                {/* Collapsible all variants */}
                <Collapsible open={showAllVariants} onOpenChange={setShowAllVariants}>
                  <CollapsibleTrigger className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground mt-3 transition-colors">
                    <ChevronDown
                      className={cn(
                        "w-3.5 h-3.5 transition-transform",
                        showAllVariants && "rotate-180"
                      )}
                    />
                    <span>All variants ({availableVariants.length})</span>
                  </CollapsibleTrigger>
                  <CollapsibleContent className="mt-3">
                    <div className="grid grid-cols-2 gap-1.5">
                      {availableVariants.map((variant) => (
                        <button
                          key={variant.type}
                          type="button"
                          onClick={() => {
                            setSelectedVariant(variant.type);
                            setSelectedPreset(null);
                          }}
                          className={cn(
                            "px-2.5 py-1.5 rounded-none text-xs text-left transition-all",
                            selectedVariant === variant.type && selectedPreset === null
                              ? "bg-foreground text-background"
                              : selectedVariant === variant.type
                                ? "bg-foreground/10 text-foreground border border-foreground/20"
                                : "bg-background text-muted-foreground hover:text-foreground border border-border hover:border-foreground/20"
                          )}
                        >
                          <span className="font-medium">{variant.name}</span>
                          {variant.recommended && (
                            <span className="ml-1 text-[10px] opacity-70">(rec)</span>
                          )}
                        </button>
                      ))}
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </div>

              {/* Command block */}
              <div className="relative">
                <div className="px-4 py-3 bg-muted border border-border">
                  <div className="flex items-start justify-between gap-2">
                    <code className="text-xs font-mono text-foreground break-all leading-relaxed">
                      {command}
                    </code>
                    <button
                      type="button"
                      onClick={handleCopyCommand}
                      aria-label={copiedCommand ? "Copied!" : "Copy command"}
                      className={cn(
                        "shrink-0 p-1.5 rounded-none transition-colors",
                        "text-muted-foreground hover:text-foreground",
                        "hover:bg-muted"
                      )}
                    >
                      {copiedCommand ? (
                        <Check className="w-3.5 h-3.5 text-success" />
                      ) : (
                        <Copy className="w-3.5 h-3.5" />
                      )}
                    </button>
                  </div>
                </div>

                {/* Copy feedback toast */}
                <div
                  className={cn(
                    "absolute inset-x-4 bottom-full mb-2 py-1.5 px-2 rounded-none bg-foreground text-background text-xs text-center",
                    "transition-all duration-200",
                    copiedCommand
                      ? "opacity-100 translate-y-0"
                      : "opacity-0 translate-y-1 pointer-events-none"
                  )}
                >
                  Copied to clipboard
                </div>
              </div>

              {/* Help text for export models */}
              {!model.inRegistry && (
                <div className="px-4 py-2.5 bg-muted/50 border-t border-border/50">
                  <p className="text-[11px] text-muted-foreground leading-relaxed">
                    Downloads from HuggingFace and converts to ONNX format.
                  </p>
                </div>
              )}
            </div>
          )}

          {/* Metadata row */}
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <div className="flex items-center gap-3">
              {model.backends && model.backends.length > 0 && (
                <span className="flex items-center gap-1.5">
                  <span className="text-muted-foreground/60">Backend:</span>
                  <span className="font-mono">{model.backends.join(", ").toUpperCase()}</span>
                </span>
              )}
              {model.architecture && (
                <span className="flex items-center gap-1.5">
                  <span className="text-muted-foreground/60">Arch:</span>
                  <span>{model.architecture}</span>
                </span>
              )}
            </div>
          </div>

          {/* Open in Playground button */}
          {model.connectionId === "named-inference" ? (
            <Button
              onClick={() =>
                navigate(`/inference/gateway?model=${encodeURIComponent(model.source)}`)
              }
            >
              Try in gateway
            </Button>
          ) : model.inRegistry &&
            model.provider === "antfly" &&
            MODEL_TYPE_PLAYGROUND[model.type] ? (
            <Button
              className="w-full"
              onClick={() => {
                const route = MODEL_TYPE_PLAYGROUND[model.type];
                setInferenceConnectionId(model.connectionId);
                navigate(
                  `${route}?model=${encodeURIComponent(model.source)}&connection=${encodeURIComponent(model.connectionId)}`
                );
                onOpenChange(false);
              }}
            >
              <Sparkles className="w-4 h-4 mr-2" />
              Open in Playground
            </Button>
          ) : model.inRegistry && MODEL_TYPE_PLAYGROUND[model.type] ? (
            <p className="text-xs text-muted-foreground text-center">
              This playground currently supports Antfly-compatible connections. Use this model in a
              configured database workflow.
            </p>
          ) : null}

          {/* HuggingFace link */}
          {model.sourceUrl && (
            <a
              href={model.sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className={cn(
                "flex items-center justify-center gap-2 w-full py-2.5 rounded-none",
                "text-sm text-muted-foreground hover:text-foreground",
                "border border-border hover:border-foreground/20",
                "transition-colors"
              )}
            >
              <ExternalLink className="w-3.5 h-3.5" />
              View on HuggingFace
            </a>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
};

const TypeContextBanner: React.FC<{
  selectedType: ModelType;
  navigate: ReturnType<typeof useNavigate>;
}> = ({ selectedType, navigate }) => {
  const detail = MODEL_TYPE_DETAILS[selectedType];
  const Icon = MODEL_TYPE_ICONS[selectedType];

  return (
    <div className="relative isolate mb-8 rounded-none border border-border overflow-hidden">
      <div className="p-5">
        <div className="flex items-start gap-4">
          <div className="flex items-center justify-center w-9 h-9 rounded-none bg-muted shrink-0">
            <Icon className="w-4 h-4 text-muted-foreground" />
          </div>
          <div className="flex-1 min-w-0">
            <h3 className="font-aeonik text-base font-semibold mb-1">{detail.tagline}</h3>
            <p className="text-sm text-muted-foreground leading-relaxed mb-3">
              {detail.description}
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <MonoLabel className="block mb-2">Use cases</MonoLabel>
                <ul className="space-y-1">
                  {detail.useCases.map((useCase) => (
                    <li
                      key={useCase}
                      className="text-xs text-muted-foreground flex items-start gap-2"
                    >
                      <span className="mt-1.5 w-1 h-1 rounded-full shrink-0 bg-muted-foreground/40" />
                      {useCase}
                    </li>
                  ))}
                </ul>
              </div>
              <div>
                <MonoLabel className="block mb-2">Pipeline context</MonoLabel>
                <p className="text-xs text-muted-foreground leading-relaxed">
                  {detail.pipelineNote}
                </p>
                {MODEL_TYPE_PLAYGROUND[detail.type] && (
                  <button
                    type="button"
                    onClick={() => {
                      const path = MODEL_TYPE_PLAYGROUND[detail.type];
                      if (path) navigate(path);
                    }}
                    className="mt-3 inline-flex items-center gap-1.5 text-xs font-medium text-foreground hover:underline"
                  >
                    <Sparkles className="w-3 h-3" />
                    Try in Playground
                    <ArrowRight className="w-3 h-3" />
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

// Main page component
const ModelsPage: React.FC = () => {
  const { models, types, quantizationOptions, loading, error, retry } = useInferenceRegistry();
  const { apiUrl, inferenceApiUrl } = useApiConfig();
  const runtime = useModelInstancesForModel();
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedType, setSelectedType] = useState<ModelType | "all">("all");
  const [selectedConnection, setSelectedConnection] = useState("all");
  const [selectedProvider, setSelectedProvider] = useState("all");
  const [selectedModelId, setSelectedModelId] = useState<string | null>(null);
  const [sheetOpen, setSheetOpen] = useState(false);
  const selectedModel = selectedModelId
    ? (models.find((model) => model.id === selectedModelId) ?? null)
    : null;

  useEffect(() => {
    if (!selectedModelId || models.some((model) => model.id === selectedModelId)) return;
    setSheetOpen(false);
    setSelectedModelId(null);
  }, [models, selectedModelId]);

  const connections = useMemo(
    () =>
      Array.from(
        new Map(models.map((model) => [model.connectionId, model.connectionName])).entries()
      ),
    [models]
  );

  const providers = useMemo(
    () => [...new Set(models.map(modelProvider))].sort((a, b) => a.localeCompare(b)),
    [models]
  );

  const filteredModels = useMemo(() => {
    return models.filter((model) => {
      if (selectedType !== "all" && model.type !== selectedType) return false;
      if (selectedConnection !== "all" && model.connectionId !== selectedConnection) return false;
      if (selectedProvider !== "all" && modelProvider(model) !== selectedProvider) return false;

      if (searchQuery) {
        const query = searchQuery.toLowerCase();
        return (
          model.name.toLowerCase().includes(query) ||
          model.source.toLowerCase().includes(query) ||
          model.description.toLowerCase().includes(query)
        );
      }

      return true;
    });
  }, [models, searchQuery, selectedConnection, selectedProvider, selectedType]);

  const modelsByType = useMemo(() => {
    const grouped: Record<ModelType, InferenceModel[]> = {
      decider: [],
      synthesizer: [],
      predictor: [],
      embedder: [],
      reranker: [],
      chunker: [],
      extractor: [],
      rewriter: [],
      generator: [],
      reader: [],
      transcriber: [],
    };

    for (const model of filteredModels) {
      grouped[model.type]?.push(model);
    }

    return grouped;
  }, [filteredModels]);

  const modelCounts = useMemo(() => {
    const counts: Record<ModelType, number> = {
      decider: 0,
      synthesizer: 0,
      predictor: 0,
      embedder: 0,
      reranker: 0,
      chunker: 0,
      extractor: 0,
      rewriter: 0,
      generator: 0,
      reader: 0,
      transcriber: 0,
    };

    for (const model of models) {
      if (model.type in counts) {
        counts[model.type]++;
      }
    }

    return counts;
  }, [models]);

  const handleModelClick = (model: InferenceModel) => {
    setSelectedModelId(model.id);
    setSheetOpen(true);
  };

  const availableCount = models.filter((m) => m.inRegistry).length;
  const huggingFaceCount = models.filter((m) => m.sourceUrl).length;

  // Loading state
  if (loading) {
    return (
      <AIWorkflowPage>
        <div className="af-ai-workflow-header flex flex-col gap-4 pb-5 md:flex-row md:items-end md:justify-between">
          <div>
            <div className="mb-3 h-7 w-64 animate-pulse rounded-none bg-muted" />
            <div className="h-4 w-full max-w-md animate-pulse rounded-none bg-muted" />
          </div>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: skeleton placeholders are positional
            <div key={i} className="bg-card border border-border rounded-none p-5">
              <div className="flex items-start gap-3 mb-3">
                <div className="w-10 h-10 bg-muted animate-pulse rounded-none" />
                <div className="flex-1">
                  <div className="h-5 w-32 bg-muted animate-pulse rounded-none mb-2" />
                  <div className="h-3 w-24 bg-muted animate-pulse rounded-none" />
                </div>
              </div>
              <div className="h-10 bg-muted animate-pulse rounded-none mb-4" />
              <div className="flex gap-2">
                <div className="h-5 w-16 bg-muted animate-pulse rounded-none" />
                <div className="h-5 w-12 bg-muted animate-pulse rounded-none" />
              </div>
            </div>
          ))}
        </div>
      </AIWorkflowPage>
    );
  }

  // Error state
  if (error) {
    return (
      <AIWorkflowPage className="min-h-full items-center justify-center">
        <ErrorState
          message="Could not load models from configured inference connections. Check Connections and try again."
          onRetry={retry}
        />
      </AIWorkflowPage>
    );
  }

  return (
    <AIWorkflowPage>
      <AIWorkflowHeader
        title="Models & Instances"
        description={`Browse ${models.length} models available through local, shared, and external inference connections.`}
        actions={
          <div className="flex items-center gap-4 text-sm text-muted-foreground">
            <span>
              <span className="font-medium text-foreground tabular-nums">
                <AnimatedCounter value={availableCount} />
              </span>{" "}
              available
            </span>
            <span>
              <span className="font-medium text-foreground tabular-nums">
                <AnimatedCounter value={huggingFaceCount} />
              </span>{" "}
              Hugging Face
            </span>
          </div>
        }
      />

      <ConnectedProvidersSummary />

      {/* Search and filters */}
      <DashboardToolbar className="md:flex-col md:items-stretch">
        <div className="flex flex-col gap-3 sm:flex-row">
          <div className="relative flex-1">
            <Search className="absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <Input
              placeholder="Search models..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10! h-11 bg-background w-full"
            />
            {searchQuery && (
              <Button
                variant="ghost"
                size="icon"
                aria-label="Clear search"
                className="absolute right-1 top-1/2 -translate-y-1/2 h-8 w-8 hover:bg-muted"
                onClick={() => setSearchQuery("")}
              >
                <X className="w-4 h-4" />
              </Button>
            )}
          </div>
          <Select value={selectedProvider} onValueChange={setSelectedProvider}>
            <SelectTrigger
              aria-label="Filter models by provider"
              className="w-full shrink-0 data-[size=default]:h-11 sm:w-52!"
            >
              <SelectValue placeholder="All providers" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All providers</SelectItem>
              {providers.map((provider) => (
                <SelectItem key={provider} value={provider}>
                  {provider}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div
          role="group"
          aria-label="Filter models by connection"
          className="flex flex-wrap items-center gap-1.5"
        >
          <button
            type="button"
            onClick={() => setSelectedConnection("all")}
            aria-pressed={selectedConnection === "all"}
            className={cn(
              "px-3 py-1.5 rounded-none text-sm transition-colors",
              selectedConnection === "all"
                ? "bg-foreground text-background"
                : "text-muted-foreground hover:text-foreground hover:bg-muted"
            )}
          >
            All connections
          </button>
          {connections.map(([id, name]) => (
            <button
              key={id}
              type="button"
              onClick={() => setSelectedConnection(id)}
              aria-pressed={selectedConnection === id}
              className={cn(
                "px-3 py-1.5 rounded-none text-sm transition-colors",
                selectedConnection === id
                  ? "bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted"
              )}
            >
              {name}
            </button>
          ))}
        </div>

        <div
          role="group"
          aria-label="Filter models by type"
          className="flex flex-wrap items-center gap-1.5"
        >
          <button
            type="button"
            onClick={() => setSelectedType("all")}
            aria-pressed={selectedType === "all"}
            className={cn(
              "px-3 py-1.5 rounded-none text-sm transition-colors",
              selectedType === "all"
                ? "bg-foreground text-background"
                : "text-muted-foreground hover:text-foreground hover:bg-muted"
            )}
          >
            All
            <span
              className={cn(
                "ml-1.5 tabular-nums text-xs",
                selectedType === "all" ? "text-background/60" : "text-muted-foreground/50"
              )}
            >
              {models.length}
            </span>
          </button>

          {types.map((typeInfo) => (
            <TypePill
              key={typeInfo.type}
              type={typeInfo.type}
              typeName={typeInfo.name}
              count={modelCounts[typeInfo.type]}
              selected={selectedType === typeInfo.type}
              onClick={() => setSelectedType(typeInfo.type)}
            />
          ))}
        </div>
      </DashboardToolbar>

      {filteredModels.length > 0 && (
        <Collapsible className="border border-border bg-background-secondary/20">
          <CollapsibleTrigger className="flex w-full items-center justify-between gap-4 px-4 py-3 text-left outline-none hover:bg-accent/40 focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset">
            <span>
              <span className="block font-mono text-xs font-medium uppercase tracking-[0.08em]">
                Runtime profiles
              </span>
              <span className="mt-0.5 block text-xs text-muted-foreground">
                Optional browser-entered estimates for deployment planning
              </span>
            </span>
            <ChevronDown className="size-4 shrink-0 transition-transform [[data-state=open]_&]:rotate-180" />
          </CollapsibleTrigger>
          <CollapsibleContent className="border-t border-border p-4">
            <RuntimeProfiles
              key={JSON.stringify([apiUrl, inferenceApiUrl])}
              models={filteredModels}
              endpointKey={JSON.stringify([apiUrl, inferenceApiUrl])}
              onDetails={handleModelClick}
            />
          </CollapsibleContent>
        </Collapsible>
      )}

      {/* Type Context Banner - shows when a specific type is selected */}
      {selectedType !== "all" && (
        <TypeContextBanner selectedType={selectedType} navigate={navigate} />
      )}

      {/* Results */}
      {filteredModels.length === 0 ? (
        <NoResultsState query={searchQuery} />
      ) : selectedType === "all" ? (
        // Grouped view
        <div className="space-y-12">
          {types.map((typeInfo) => {
            const typeModels = modelsByType[typeInfo.type];
            if (typeModels.length === 0) return null;

            return (
              <section key={typeInfo.type}>
                <div className="flex items-baseline gap-3 mb-4">
                  <h2 className="text-base font-semibold">{typeInfo.name}s</h2>
                  <span className="text-xs text-muted-foreground tabular-nums">
                    {typeModels.length}
                  </span>
                </div>

                {/* Cards grid */}
                <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                  {typeModels.map((model) => (
                    <ModelCard
                      key={model.id}
                      model={model}
                      runtime={runtime.runtimeFor(model.type, model.source)}
                      providerManaged={model.connectionId !== "local-inference"}
                      onClick={() => handleModelClick(model)}
                    />
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      ) : (
        // Flat grid for filtered view
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {filteredModels.map((model) => (
            <ModelCard
              key={model.id}
              model={model}
              runtime={runtime.runtimeFor(model.type, model.source)}
              providerManaged={model.connectionId !== "local-inference"}
              onClick={() => handleModelClick(model)}
            />
          ))}
        </div>
      )}

      {/* Detail Sheet */}
      <ModelDetailSheet
        model={selectedModel}
        open={sheetOpen}
        onOpenChange={setSheetOpen}
        types={types}
        quantizationOptions={quantizationOptions}
        runtime={
          selectedModel ? runtime.runtimeFor(selectedModel.type, selectedModel.source) : undefined
        }
        runtimeSupported={
          runtime.supported && !!selectedModel && modelInstanceTask(selectedModel.type) !== null
        }
        providerManaged={selectedModel?.connectionId !== "local-inference"}
        actionPending={runtime.actionPending}
        runtimeError={runtime.error}
        onLoad={runtime.load}
        onUnload={runtime.unload}
      />
    </AIWorkflowPage>
  );
};

export default ModelsPage;
