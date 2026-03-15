import {
  AlertCircle,
  Apple,
  ArrowRight,
  ArrowUpDown,
  AudioLines,
  BookOpen,
  Check,
  ChevronDown,
  Cloud,
  Copy,
  Cpu,
  Download,
  ExternalLink,
  Fingerprint,
  Gauge,
  Monitor,
  Package,
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
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { isProductEnabled } from "@/config/products";
import {
  type Backend,
  getDownloadCommand,
  getHardwareCapabilities,
  HARDWARE_INFO,
  type HardwareCapability,
  MODEL_TYPE_DETAILS,
  MODEL_TYPE_PLAYGROUND,
  type ModelType,
  type QuantizationOption,
  type QuantizationType,
  type RecognizerCapability,
  type TermiteModel,
  VARIANT_PRESETS,
  type VariantPreset,
} from "@/data/termite-models";
import { useApiConfig } from "@/hooks/use-api-config";
import { useTermiteRegistry } from "@/hooks/use-termite-registry";
import { cn } from "@/lib/utils";

// Icon mapping for model types
const MODEL_TYPE_ICONS: Record<ModelType, React.FC<{ className?: string }>> = {
  embedder: Fingerprint,
  reranker: ArrowUpDown,
  chunker: Scissors,
  recognizer: Tag,
  rewriter: RefreshCw,
  generator: Sparkles,
  reader: BookOpen,
  transcriber: AudioLines,
};

// Refined color system - bold accent colors with monochrome base
const MODEL_TYPE_ACCENT: Record<
  ModelType,
  { bg: string; text: string; border: string; glow: string }
> = {
  embedder: {
    bg: "bg-blue-500",
    text: "text-blue-500 dark:text-blue-400",
    border: "border-blue-500/20 dark:border-blue-400/20",
    glow: "shadow-blue-500/20",
  },
  reranker: {
    bg: "bg-violet-500",
    text: "text-violet-500 dark:text-violet-400",
    border: "border-violet-500/20 dark:border-violet-400/20",
    glow: "shadow-violet-500/20",
  },
  chunker: {
    bg: "bg-emerald-500",
    text: "text-emerald-500 dark:text-emerald-400",
    border: "border-emerald-500/20 dark:border-emerald-400/20",
    glow: "shadow-emerald-500/20",
  },
  recognizer: {
    bg: "bg-amber-500",
    text: "text-amber-500 dark:text-amber-400",
    border: "border-amber-500/20 dark:border-amber-400/20",
    glow: "shadow-amber-500/20",
  },
  rewriter: {
    bg: "bg-rose-500",
    text: "text-rose-500 dark:text-rose-400",
    border: "border-rose-500/20 dark:border-rose-400/20",
    glow: "shadow-rose-500/20",
  },
  generator: {
    bg: "bg-cyan-500",
    text: "text-cyan-500 dark:text-cyan-400",
    border: "border-cyan-500/20 dark:border-cyan-400/20",
    glow: "shadow-cyan-500/20",
  },
  reader: {
    bg: "bg-orange-500",
    text: "text-orange-500 dark:text-orange-400",
    border: "border-orange-500/20 dark:border-orange-400/20",
    glow: "shadow-orange-500/20",
  },
  transcriber: {
    bg: "bg-teal-500",
    text: "text-teal-500 dark:text-teal-400",
    border: "border-teal-500/20 dark:border-teal-400/20",
    glow: "shadow-teal-500/20",
  },
};

// Capability styling
const CAPABILITY_STYLES: Record<RecognizerCapability, string> = {
  labels: "bg-foreground/5 text-foreground/70 border-foreground/10",
  zeroshot: "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20",
  relations: "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20",
  answers: "bg-rose-500/10 text-rose-600 dark:text-rose-400 border-rose-500/20",
};

const CAPABILITY_LABELS: Record<RecognizerCapability, string> = {
  labels: "NER",
  zeroshot: "Zero-Shot",
  relations: "Relations",
  answers: "QA",
};

// Hardware capability icons
const HARDWARE_ICONS: Record<HardwareCapability, React.FC<{ className?: string }>> = {
  "nvidia-gpu": Monitor,
  "apple-silicon": Apple,
  "google-tpu": Cloud,
  cpu: Cpu,
};

// Hardware badge colors
const HARDWARE_COLORS: Record<HardwareCapability, string> = {
  "nvidia-gpu": "bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20",
  "apple-silicon": "bg-slate-500/10 text-slate-600 dark:text-slate-400 border-slate-500/20",
  "google-tpu": "bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20",
  cpu: "bg-foreground/5 text-foreground/70 border-foreground/10",
};

// Hardware badges component
const HardwareBadges: React.FC<{
  backends?: string[];
  showLabels?: boolean;
}> = ({ backends, showLabels = true }) => {
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
                    "inline-flex items-center gap-1.5 px-2 py-1 rounded-md text-xs border cursor-help",
                    HARDWARE_COLORS[cap]
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
  const [stats, setStats] = useState<HFStats>(() => {
    const cached = hfStatsCache.get(source);
    if (cached) {
      return { ...cached, loading: false, error: false };
    }
    return { downloads: 0, likes: 0, loading: true, error: false };
  });

  useEffect(() => {
    // Skip if already cached
    const cached = hfStatsCache.get(source);
    if (cached) {
      setStats({ ...cached, loading: false, error: false });
      return;
    }

    const controller = new AbortController();

    async function fetchStats() {
      try {
        const response = await fetch(`https://huggingface.co/api/models/${source}`, {
          signal: controller.signal,
        });

        // Check if aborted before processing response
        if (controller.signal.aborted) return;

        if (!response.ok) {
          // Handle rate limiting specifically
          if (response.status === 429) {
            console.warn(`HuggingFace API rate limited for ${source}`);
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
        hfStatsCache.set(source, result);
        setStats({ ...result, loading: false, error: false });
      } catch (err) {
        if ((err as Error).name !== "AbortError" && !controller.signal.aborted) {
          setStats((prev) => ({ ...prev, loading: false, error: true }));
        }
      }
    }

    fetchStats();

    return () => controller.abort();
  }, [source]);

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

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-xs text-muted-foreground/50">
        <span className="w-8 h-3 bg-muted animate-pulse rounded" />
        <span className="w-8 h-3 bg-muted animate-pulse rounded" />
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
    <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-muted/50 border border-border/50">
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

// Type filter pill with visual indicator
const TypePill: React.FC<{
  type: ModelType;
  typeName: string;
  count: number;
  selected: boolean;
  onClick: () => void;
}> = ({ type, typeName, count, selected, onClick }) => {
  const Icon = MODEL_TYPE_ICONS[type];
  const accent = MODEL_TYPE_ACCENT[type];

  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={selected}
      className={cn(
        "group relative flex items-center gap-2.5 px-4 py-2.5 rounded-full transition-all duration-300",
        "border text-sm font-medium",
        selected
          ? cn("bg-foreground text-background border-foreground", "shadow-lg", accent.glow)
          : cn(
              "bg-transparent border-border hover:border-foreground/30",
              "text-muted-foreground hover:text-foreground"
            )
      )}
    >
      <span
        className={cn(
          "flex items-center justify-center w-5 h-5 rounded-full transition-colors",
          selected ? "bg-background/20" : `${accent.bg}/10`
        )}
      >
        <Icon className={cn("w-3 h-3", selected ? "text-background" : accent.text)} />
      </span>
      <span>{typeName}</span>
      <span
        className={cn(
          "text-xs tabular-nums",
          selected ? "text-background/60" : "text-muted-foreground/60"
        )}
      >
        {count}
      </span>
    </button>
  );
};

// Model card with refined design
const ModelCard: React.FC<{
  model: TermiteModel;
  onClick: () => void;
  index: number;
}> = ({ model, onClick, index }) => {
  const Icon = MODEL_TYPE_ICONS[model.type];
  const accent = MODEL_TYPE_ACCENT[model.type];

  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={`View details for ${model.name}`}
      className={cn(
        "group relative text-left w-full",
        "bg-card border border-border rounded-xl p-5",
        "transition-all duration-300 ease-out",
        "hover:border-foreground/20 hover:shadow-lg hover:-translate-y-0.5",
        "focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 focus:ring-offset-background"
      )}
      style={{
        animationDelay: `${index * 50}ms`,
      }}
    >
      {/* Accent line */}
      <div
        className={cn(
          "absolute top-0 left-5 right-5 h-px",
          accent.bg,
          "opacity-0 group-hover:opacity-100 transition-opacity duration-300"
        )}
      />

      {/* Header */}
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="flex items-center gap-3">
          <div
            className={cn(
              "flex items-center justify-center w-10 h-10 rounded-lg",
              "bg-muted/50 border",
              accent.border,
              "transition-colors duration-300 group-hover:bg-muted"
            )}
          >
            <Icon className={cn("w-5 h-5", accent.text)} />
          </div>
          <div className="min-w-0">
            <h3 className="font-semibold text-foreground leading-tight truncate group-hover:text-foreground transition-colors">
              {model.name}
            </h3>
            <p className="text-xs text-muted-foreground truncate mt-0.5 font-mono">
              {model.source}
            </p>
          </div>
        </div>

        <ArrowRight
          className={cn(
            "w-4 h-4 text-muted-foreground/0 group-hover:text-muted-foreground",
            "transform translate-x-0 group-hover:translate-x-1",
            "transition-all duration-300"
          )}
        />
      </div>

      {/* Description */}
      <p className="text-sm text-muted-foreground line-clamp-2 mb-4 leading-relaxed">
        {model.description}
      </p>

      {/* Footer */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 flex-wrap">
          {model.inRegistry ? (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md bg-success-500/10 text-success-600 dark:text-success-400 text-xs font-medium">
              <Zap className="w-3 h-3" />
              Ready
            </span>
          ) : (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md bg-muted text-muted-foreground text-xs">
              <Package className="w-3 h-3" />
              Export
            </span>
          )}

          {model.capabilities?.slice(0, 2).map((cap) => (
            <span
              key={cap}
              className={cn("px-2 py-0.5 rounded-md text-xs border", CAPABILITY_STYLES[cap])}
            >
              {CAPABILITY_LABELS[cap]}
            </span>
          ))}
          {model.capabilities && model.capabilities.length > 2 && (
            <span className="text-xs text-muted-foreground">+{model.capabilities.length - 2}</span>
          )}
        </div>

        <ModelStats source={model.source} compact />
      </div>
    </button>
  );
};

// Model detail sheet - refined documentation style
const ModelDetailSheet: React.FC<{
  model: TermiteModel | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  types: { type: ModelType; name: string }[];
  quantizationOptions: QuantizationOption[];
  allowDownloads: boolean;
}> = ({ model, open, onOpenChange, types, quantizationOptions, allowDownloads }) => {
  const navigate = useNavigate();
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
  const accent = MODEL_TYPE_ACCENT[model.type];
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

  const selectedVariantInfo = selectedVariant
    ? availableVariants.find((q) => q.type === selectedVariant)
    : null;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full sm:max-w-md overflow-y-auto p-0">
        {/* Header with gradient accent */}
        <div className={cn("relative px-6 pt-6 pb-5", "border-b border-border/50")}>
          {/* Subtle gradient background */}
          <div className={cn("absolute inset-0 opacity-[0.03] dark:opacity-[0.06]", accent.bg)} />

          <SheetHeader className="relative">
            <div className="flex items-start gap-3.5">
              <div
                className={cn(
                  "flex items-center justify-center w-11 h-11 rounded-lg",
                  "bg-background border shadow-sm",
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
                  "inline-flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium border",
                  accent.text,
                  accent.border,
                  "bg-background"
                )}
              >
                <Icon className="w-3 h-3" />
                {typeInfo?.name}
              </span>

              {model.inRegistry ? (
                <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-xs font-medium bg-success-500/10 text-success-600 dark:text-success-400 border border-success-500/20">
                  <Zap className="w-3 h-3" />
                  Ready
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-xs bg-muted text-muted-foreground border border-border">
                  <Package className="w-3 h-3" />
                  Export
                </span>
              )}

              {model.size && (
                <span className="px-2 py-1 rounded-md text-xs font-mono text-muted-foreground bg-muted/50 border border-border/50">
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

          {/* Hardware Compatibility */}
          <div>
            <h4 className="text-xs font-medium text-muted-foreground mb-2">Runs on</h4>
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
                      "inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs border",
                      CAPABILITY_STYLES[cap]
                    )}
                  >
                    <Check className="w-3 h-3" />
                    {CAPABILITY_LABELS[cap]}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Download Configuration */}
          {!allowDownloads ? (
            <div className="rounded-lg border border-border bg-muted/30 px-4 py-3">
              <p className="text-xs text-muted-foreground">
                Use <code className="font-mono bg-muted px-1 py-0.5 rounded">termite pull</code> CLI
                or the Termite operator to manage models in production deployments.
              </p>
            </div>
          ) : (
            <div className="rounded-lg border border-border bg-muted/30 overflow-hidden">
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
                          "flex-1 min-w-[100px] px-3 py-2 rounded-lg text-left transition-all",
                          isSelected
                            ? "bg-foreground text-background shadow-sm"
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
                            "px-2.5 py-1.5 rounded text-xs text-left transition-all",
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
                <div className="px-4 py-3 bg-searchaf-11 dark:bg-searchaf-2">
                  <div className="flex items-start justify-between gap-2">
                    <code className="text-xs font-mono text-searchaf-1 dark:text-searchaf-11 break-all leading-relaxed">
                      {command}
                    </code>
                    <button
                      type="button"
                      onClick={handleCopyCommand}
                      aria-label={copiedCommand ? "Copied!" : "Copy command"}
                      className={cn(
                        "shrink-0 p-1.5 rounded transition-colors",
                        "text-searchaf-6 hover:text-searchaf-1 dark:text-searchaf-6 dark:hover:text-searchaf-11",
                        "hover:bg-searchaf-9/20 dark:hover:bg-searchaf-4/30"
                      )}
                    >
                      {copiedCommand ? (
                        <Check className="w-3.5 h-3.5 text-success-400" />
                      ) : (
                        <Copy className="w-3.5 h-3.5" />
                      )}
                    </button>
                  </div>
                </div>

                {/* Copy feedback toast */}
                <div
                  className={cn(
                    "absolute inset-x-4 bottom-full mb-2 py-1.5 px-2 rounded bg-foreground text-background text-xs text-center",
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
          {model.inRegistry && MODEL_TYPE_PLAYGROUND[model.type] && (
            <Button
              className="w-full"
              onClick={() => {
                const route = MODEL_TYPE_PLAYGROUND[model.type];
                navigate(`${route}?model=${encodeURIComponent(model.name)}`);
                onOpenChange(false);
              }}
            >
              <Sparkles className="w-4 h-4 mr-2" />
              Open in Playground
            </Button>
          )}

          {/* HuggingFace link */}
          <a
            href={model.sourceUrl}
            target="_blank"
            rel="noopener noreferrer"
            className={cn(
              "flex items-center justify-center gap-2 w-full py-2.5 rounded-lg",
              "text-sm text-muted-foreground hover:text-foreground",
              "border border-border hover:border-foreground/20",
              "transition-colors"
            )}
          >
            <ExternalLink className="w-3.5 h-3.5" />
            View on HuggingFace
          </a>
        </div>
      </SheetContent>
    </Sheet>
  );
};

// Type context banner - shows when a specific type filter is selected
const TypeContextBanner: React.FC<{
  selectedType: ModelType;
  navigate: ReturnType<typeof useNavigate>;
}> = ({ selectedType, navigate }) => {
  const detail = MODEL_TYPE_DETAILS[selectedType];
  const Icon = MODEL_TYPE_ICONS[selectedType];
  const accent = MODEL_TYPE_ACCENT[selectedType];

  return (
    <div
      className={cn(
        "mb-8 rounded-xl border-l-4 p-5 bg-card border border-border",
        accent.border.replace("/20", "/40")
      )}
    >
      <div className="flex items-start gap-4">
        <div
          className={cn(
            "flex items-center justify-center w-12 h-12 rounded-xl shrink-0",
            "bg-muted border",
            accent.border
          )}
        >
          <Icon className={cn("w-6 h-6", accent.text)} />
        </div>
        <div className="flex-1 min-w-0">
          <h3 className={cn("text-lg font-semibold mb-1", accent.text)}>{detail.tagline}</h3>
          <p className="text-sm text-muted-foreground leading-relaxed mb-3">{detail.description}</p>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <h4 className="text-xs font-medium text-foreground mb-2">Use cases</h4>
              <ul className="space-y-1">
                {detail.useCases.map((useCase) => (
                  <li
                    key={useCase}
                    className="text-xs text-muted-foreground flex items-start gap-2"
                  >
                    <span className={cn("mt-1.5 w-1 h-1 rounded-full shrink-0", accent.bg)} />
                    {useCase}
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h4 className="text-xs font-medium text-foreground mb-2">Pipeline context</h4>
              <p className="text-xs text-muted-foreground leading-relaxed">{detail.pipelineNote}</p>
              {MODEL_TYPE_PLAYGROUND[detail.type] && (
                <button
                  type="button"
                  onClick={() => {
                    const path = MODEL_TYPE_PLAYGROUND[detail.type];
                    if (path) navigate(path);
                  }}
                  className={cn(
                    "mt-3 inline-flex items-center gap-1.5 text-xs font-medium",
                    accent.text,
                    "hover:underline"
                  )}
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
  );
};

// Main page component
const ModelsPage: React.FC = () => {
  const { models, types, quantizationOptions, loading, error, retry } = useTermiteRegistry();
  const { apiUrl, termiteApiUrl } = useApiConfig();
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedType, setSelectedType] = useState<ModelType | "all">("all");
  const [selectedModel, setSelectedModel] = useState<TermiteModel | null>(null);
  const [sheetOpen, setSheetOpen] = useState(false);
  const [allowDownloads, setAllowDownloads] = useState(false);

  // Determine download availability
  useEffect(() => {
    const checkDownloads = async () => {
      try {
        if (isProductEnabled("antfly")) {
          // Full antfarm build: check swarm_mode from antfly status
          const response = await fetch(`${apiUrl}/status`);
          if (response.ok) {
            const data = await response.json();
            setAllowDownloads(data.swarm_mode === true);
          }
        } else {
          // Termite-only build: check allow_downloads from Termite version
          const response = await fetch(`${termiteApiUrl}/api/version`);
          if (response.ok) {
            const data = await response.json();
            setAllowDownloads(data.allow_downloads === true);
          }
        }
      } catch {
        // Default to not showing downloads on error
      }
    };
    checkDownloads();
  }, [apiUrl, termiteApiUrl]);

  const filteredModels = useMemo(() => {
    return models.filter((model) => {
      if (selectedType !== "all" && model.type !== selectedType) return false;

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
  }, [models, searchQuery, selectedType]);

  const modelsByType = useMemo(() => {
    const grouped: Record<ModelType, TermiteModel[]> = {
      embedder: [],
      reranker: [],
      chunker: [],
      recognizer: [],
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
      embedder: 0,
      reranker: 0,
      chunker: 0,
      recognizer: 0,
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

  const handleModelClick = (model: TermiteModel) => {
    setSelectedModel(model);
    setSheetOpen(true);
  };

  const registryCount = models.filter((m) => m.inRegistry).length;

  // Loading state
  if (loading) {
    return (
      <div className="min-h-full">
        <header className="relative mb-12">
          <div className="pt-4 pb-8">
            <div className="h-10 w-64 bg-muted animate-pulse rounded-lg mb-3" />
            <div className="h-6 w-96 bg-muted animate-pulse rounded-lg" />
          </div>
        </header>
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: skeleton placeholders are positional
            <div key={i} className="bg-card border border-border rounded-xl p-5">
              <div className="flex items-start gap-3 mb-3">
                <div className="w-10 h-10 bg-muted animate-pulse rounded-lg" />
                <div className="flex-1">
                  <div className="h-5 w-32 bg-muted animate-pulse rounded mb-2" />
                  <div className="h-3 w-24 bg-muted animate-pulse rounded" />
                </div>
              </div>
              <div className="h-10 bg-muted animate-pulse rounded mb-4" />
              <div className="flex gap-2">
                <div className="h-5 w-16 bg-muted animate-pulse rounded" />
                <div className="h-5 w-12 bg-muted animate-pulse rounded" />
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="min-h-full flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="w-16 h-16 rounded-2xl bg-destructive/10 flex items-center justify-center mx-auto mb-4">
            <AlertCircle className="w-8 h-8 text-destructive" />
          </div>
          <h2 className="text-xl font-semibold mb-2">Unable to load models</h2>
          <p className="text-muted-foreground mb-6">
            Could not load the model registry. Check your network connection and try again.
          </p>
          <Button onClick={retry} variant="outline">
            <RefreshCw className="w-4 h-4 mr-2" />
            Try again
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-full">
      {/* Hero Section */}
      <header className="relative mb-12">
        {/* Background gradient */}
        <div className="absolute inset-0 -z-10 overflow-hidden">
          <div className="absolute top-0 left-1/4 w-96 h-96 bg-gradient-to-br from-blue-500/5 to-violet-500/5 dark:from-blue-500/10 dark:to-violet-500/10 rounded-full blur-3xl" />
          <div className="absolute top-20 right-1/4 w-64 h-64 bg-gradient-to-br from-emerald-500/5 to-cyan-500/5 dark:from-emerald-500/10 dark:to-cyan-500/10 rounded-full blur-3xl" />
        </div>

        <div className="pt-4 pb-8">
          {/* Title */}
          <h1 className="text-4xl font-bold tracking-tight mb-3">Model Directory</h1>
          <p className="text-muted-foreground text-lg max-w-2xl leading-relaxed">
            Browse {models.length} optimized models for embeddings, NER, chunking, and more. All
            models run locally via ONNX.
          </p>

          {/* Stats */}
          <div className="flex items-center gap-6 mt-6">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-success-500" />
              <span className="text-sm">
                <span className="font-semibold text-foreground">
                  <AnimatedCounter value={registryCount} />
                </span>
                <span className="text-muted-foreground"> ready to pull</span>
              </span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-muted-foreground/50" />
              <span className="text-sm">
                <span className="font-semibold text-foreground">
                  <AnimatedCounter value={models.length - registryCount} />
                </span>
                <span className="text-muted-foreground"> exportable</span>
              </span>
            </div>
          </div>
        </div>
      </header>

      {/* Filters */}
      <div className="mb-8 space-y-6">
        {/* Search */}
        <div className="relative max-w-md">
          <Search className="absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input
            placeholder="Search models..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 h-11 bg-background"
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

        {/* Type filters */}
        <div
          role="group"
          aria-label="Filter models by type"
          className="flex flex-wrap items-center gap-2"
        >
          <button
            type="button"
            onClick={() => setSelectedType("all")}
            aria-pressed={selectedType === "all"}
            className={cn(
              "px-4 py-2.5 rounded-full text-sm font-medium transition-all duration-300 border",
              selectedType === "all"
                ? "bg-foreground text-background border-foreground shadow-lg"
                : "bg-transparent border-border text-muted-foreground hover:text-foreground hover:border-foreground/30"
            )}
          >
            All models
            <span
              className={cn(
                "ml-2 tabular-nums",
                selectedType === "all" ? "text-background/60" : "text-muted-foreground/60"
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
      </div>

      {/* Type Context Banner - shows when a specific type is selected */}
      {selectedType !== "all" && (
        <TypeContextBanner selectedType={selectedType} navigate={navigate} />
      )}

      {/* Results */}
      {filteredModels.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-center">
          <div className="w-16 h-16 rounded-2xl bg-muted flex items-center justify-center mb-4">
            <Search className="w-7 h-7 text-muted-foreground/50" />
          </div>
          <h3 className="text-lg font-semibold mb-1">No models found</h3>
          <p className="text-muted-foreground text-sm mb-4">Try adjusting your search or filters</p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              setSearchQuery("");
              setSelectedType("all");
            }}
          >
            Clear filters
          </Button>
        </div>
      ) : selectedType === "all" ? (
        // Grouped view
        <div className="space-y-12">
          {types.map((typeInfo) => {
            const typeModels = modelsByType[typeInfo.type];
            if (typeModels.length === 0) return null;

            const Icon = MODEL_TYPE_ICONS[typeInfo.type];
            const accent = MODEL_TYPE_ACCENT[typeInfo.type];

            return (
              <section key={typeInfo.type}>
                {/* Section header */}
                <div className="flex items-center gap-4 mb-6">
                  <div
                    className={cn(
                      "flex items-center justify-center w-10 h-10 rounded-xl",
                      "bg-muted border",
                      accent.border
                    )}
                  >
                    <Icon className={cn("w-5 h-5", accent.text)} />
                  </div>
                  <div className="flex-1">
                    <h2 className="text-xl font-semibold">{typeInfo.name}s</h2>
                    <p className="text-sm text-muted-foreground">{typeInfo.description}</p>
                  </div>
                  <Badge variant="secondary" className="text-xs font-medium">
                    {typeModels.length}
                  </Badge>
                </div>

                {/* Cards grid */}
                <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                  {typeModels.map((model, idx) => (
                    <ModelCard
                      key={model.id}
                      model={model}
                      onClick={() => handleModelClick(model)}
                      index={idx}
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
          {filteredModels.map((model, idx) => (
            <ModelCard
              key={model.id}
              model={model}
              onClick={() => handleModelClick(model)}
              index={idx}
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
        allowDownloads={allowDownloads}
      />
    </div>
  );
};

export default ModelsPage;
