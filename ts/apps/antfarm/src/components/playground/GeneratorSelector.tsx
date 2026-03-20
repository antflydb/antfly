import type { GeneratorConfig, GeneratorProvider } from "@antfly/sdk";
import { generatorProviders } from "@antfly/sdk";
import { useId } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";

export const GENERATOR_PROVIDER_DEFAULTS: Record<GeneratorProvider, string> = {
  ollama: "llama3.3:70b",
  gemini: "gemini-2.5-flash",
  openai: "gpt-4.1",
  anthropic: "claude-sonnet-4-5-20250929",
  vertex: "gemini-2.5-flash",
  cohere: "command-r-plus",
  termite: "gemma-3-1b-it",
  openrouter: "openai/gpt-4.1",
  bedrock: "anthropic.claude-sonnet-4-5-20250929-v1:0",
  mock: "mock",
};

export const GENERATOR_PROVIDER_LABELS: Record<GeneratorProvider, string> = {
  ollama: "Ollama (Local)",
  gemini: "Google AI (Gemini)",
  openai: "OpenAI",
  anthropic: "Anthropic (Claude)",
  vertex: "Google Cloud Vertex AI",
  cohere: "Cohere",
  termite: "Termite (Local)",
  openrouter: "OpenRouter",
  bedrock: "AWS Bedrock",
  mock: "Mock (Testing)",
};

/** Default generator config used as the initial "custom" value across playgrounds. */
export const GENERATOR_DEFAULT_CONFIG: GeneratorConfig = {
  provider: "openai",
  model: "gpt-4.1",
  temperature: 0.7,
};

/** Providers shown in the query-builder generator selectors. */
export const QUERY_BUILDER_PROVIDERS: GeneratorProvider[] = [
  "gemini",
  "vertex",
  "openai",
  "anthropic",
  "bedrock",
  "ollama",
  "cohere",
  "openrouter",
  "termite",
  "mock",
];

export function formatGeneratorSummary(
  config: Pick<GeneratorConfig, "provider" | "model"> | null | undefined,
  defaultLabel = "Server default"
): string {
  if (!config?.provider || !config?.model) {
    return defaultLabel;
  }
  return `${config.provider}/${config.model}`;
}

/**
 * Derive the label and description shown in the "default" radio of a
 * GeneratorSelector, depending on whether a dashboard-level generator has
 * been configured.
 */
export function getInheritedGeneratorLabels(dashboardGenerator: GeneratorConfig | null): {
  label: string;
  description: string;
} {
  if (dashboardGenerator) {
    return {
      label: "Dashboard default",
      description: `Use the dashboard default generator (${formatGeneratorSummary(dashboardGenerator)}).`,
    };
  }
  return {
    label: "Server default",
    description:
      "Use the generator configured in the Antfly server config and omit any local override.",
  };
}

interface GeneratorSelectorProps {
  value: GeneratorConfig | null;
  onChange: (value: GeneratorConfig | null) => void;
  defaultConfig: GeneratorConfig;
  defaultLabel?: string;
  defaultDescription?: string;
  customLabel?: string;
  showTemperature?: boolean;
  temperatureDisabled?: boolean;
  temperatureHelpText?: string;
  providers?: GeneratorProvider[];
  className?: string;
}

export function GeneratorSelector({
  value,
  onChange,
  defaultConfig,
  defaultLabel = "Server default",
  defaultDescription = "Omit the generator override and let the backend choose the configured default.",
  customLabel = "Custom override",
  showTemperature = true,
  temperatureDisabled = false,
  temperatureHelpText,
  providers = generatorProviders,
  className,
}: GeneratorSelectorProps) {
  const mode = value ? "custom" : "default";
  const id = useId();
  const defaultId = `${id}-generator-mode-default`;
  const customId = `${id}-generator-mode-custom`;

  const handleModeChange = (nextMode: string) => {
    if (nextMode === "default") {
      onChange(null);
      return;
    }
    onChange(value ? { ...value } : { ...defaultConfig });
  };

  const handleProviderChange = (provider: string) => {
    if (!value) {
      return;
    }
    const nextProvider = provider as GeneratorProvider;
    onChange({
      ...value,
      provider: nextProvider,
      model: GENERATOR_PROVIDER_DEFAULTS[nextProvider] || value.model,
    });
  };

  return (
    <div className={cn("space-y-4", className)}>
      <RadioGroup value={mode} onValueChange={handleModeChange} className="gap-2">
        <label
          htmlFor={defaultId}
          className="flex items-start gap-3 rounded-lg border p-3 cursor-pointer hover:bg-muted/30"
        >
          <RadioGroupItem value="default" id={defaultId} className="mt-0.5" />
          <div className="space-y-1">
            <Label htmlFor={defaultId} className="cursor-pointer">
              {defaultLabel}
            </Label>
            <p className="text-xs text-muted-foreground">{defaultDescription}</p>
          </div>
        </label>
        <label
          htmlFor={customId}
          className="flex items-start gap-3 rounded-lg border p-3 cursor-pointer hover:bg-muted/30"
        >
          <RadioGroupItem value="custom" id={customId} className="mt-0.5" />
          <div className="space-y-1">
            <Label htmlFor={customId} className="cursor-pointer">
              {customLabel}
            </Label>
            <p className="text-xs text-muted-foreground">
              Pick a provider and model for this playground only.
            </p>
          </div>
        </label>
      </RadioGroup>

      {value && (
        <div
          className={cn(
            "grid gap-4",
            showTemperature ? "grid-cols-1 lg:grid-cols-3" : "grid-cols-1 lg:grid-cols-2"
          )}
        >
          <div className="space-y-2">
            <Label className="text-xs text-muted-foreground">Provider</Label>
            <Select value={value.provider} onValueChange={handleProviderChange}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {providers.map((provider) => (
                  <SelectItem key={provider} value={provider}>
                    {GENERATOR_PROVIDER_LABELS[provider] || provider}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label className="text-xs text-muted-foreground">Model</Label>
            <Input
              value={value.model}
              onChange={(e) => onChange({ ...value, model: e.target.value })}
              placeholder={GENERATOR_PROVIDER_DEFAULTS[value.provider]}
            />
          </div>
          {showTemperature && (
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">Temperature</Label>
              <Input
                type="number"
                value={value.temperature ?? defaultConfig.temperature ?? 0}
                onChange={(e) => {
                  const parsed = Number.parseFloat(e.target.value);
                  onChange({
                    ...value,
                    temperature: Number.isFinite(parsed)
                      ? parsed
                      : (defaultConfig.temperature ?? 0),
                  });
                }}
                min={0}
                max={2}
                step={0.1}
                disabled={temperatureDisabled}
                className={cn(temperatureDisabled && "bg-muted")}
              />
              {temperatureHelpText && (
                <p className="text-xs text-muted-foreground">{temperatureHelpText}</p>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
