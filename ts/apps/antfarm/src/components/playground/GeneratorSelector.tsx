import {
  Input,
  Label,
  RadioGroup,
  RadioGroupItem,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@antfly/design-system";
import type { GeneratorConfig, GeneratorProvider } from "@antfly/sdk";
import { generatorProviders } from "@antfly/sdk";
import { useEffect, useId, useMemo } from "react";
import { Combobox } from "@/components/Combobox";
import { useChatGPT } from "@/components/chatgpt-provider";
import { liveModelSuggestions, useConnectedModels } from "@/hooks/use-connections";
import { cn } from "@/lib/utils";

export const GENERATOR_PROVIDER_DEFAULTS: Partial<Record<GeneratorProvider, string>> = {
  antfly: "gemma-3-1b-it",
  ollama: "llama3.3:70b",
  gemini: "gemini-2.5-flash",
  openai: "gpt-4.1",
  openrouter: "openai/gpt-4o-mini",
  vertex: "gemini-2.5-flash",
};

export const GENERATOR_PROVIDER_LABELS: Partial<Record<GeneratorProvider, string>> = {
  antfly: "Antfly (Local)",
  ollama: "Ollama (Local)",
  gemini: "Google AI (Gemini)",
  openai: "OpenAI",
  chatgpt: "ChatGPT plan",
  openrouter: "OpenRouter",
  vertex: "Google Cloud Vertex AI",
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
  "openrouter",
  "ollama",
  "antfly",
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
  allowPersonalConnections?: boolean;
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
  allowPersonalConnections = false,
  showTemperature = true,
  temperatureDisabled = false,
  temperatureHelpText,
  providers = generatorProviders,
  className,
}: GeneratorSelectorProps) {
  const chatgpt = useChatGPT();
  const accounts = chatgpt.accounts.filter((account) => account.connected && account.plan_enabled);
  const connectionId = value?.provider === "chatgpt" ? value.connection_id : undefined;
  useEffect(() => {
    // Account discovery marks the current API/auth scope ready. Waiting for it
    // also retries a persisted selection when the provider replaces that scope.
    if (chatgpt.supported && connectionId) void chatgpt.loadModels(connectionId);
  }, [connectionId, chatgpt.loadModels, chatgpt.supported]);
  const availableProviders = providers.filter(
    (provider) =>
      provider !== "chatgpt" ||
      (allowPersonalConnections && (accounts.length > 0 || value?.provider === "chatgpt"))
  );
  const mode = value ? "custom" : "default";
  const id = useId();
  const defaultId = `${id}-generator-mode-default`;
  const customId = `${id}-generator-mode-custom`;

  // Live model lists per configured provider from /db/v1/connections.
  // Falls back to the static per-provider default when the provider is not
  // configured/connected or the server predates the endpoint.
  const { providers: connectedProviders } = useConnectedModels();
  const liveGenerators = useMemo(
    () => liveModelSuggestions(connectedProviders, "generator"),
    [connectedProviders]
  );
  const modelOptions = useMemo(() => {
    if (!value) return [];
    if (value.provider === "chatgpt")
      return (chatgpt.models[value.connection_id ?? ""] ?? []).map((model) => ({
        value: model.slug,
        label: model.display_name,
      }));
    const live = liveGenerators[value.provider] ?? [];
    const names = live.length > 0 ? live : [GENERATOR_PROVIDER_DEFAULTS[value.provider]];
    return names
      .filter((name): name is string => Boolean(name))
      .map((name) => ({ value: name, label: name }));
  }, [liveGenerators, value, chatgpt.models]);

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
    if (nextProvider === "chatgpt") {
      const id = accounts[0]?.connection_id;
      if (!id) return;
      onChange({
        provider: "chatgpt",
        connection_id: id,
        model: chatgpt.models[id]?.[0]?.slug ?? "",
      });
      return;
    }
    if (value.provider === "chatgpt") {
      onChange({
        provider: nextProvider,
        model: liveGenerators[nextProvider]?.[0] ?? GENERATOR_PROVIDER_DEFAULTS[nextProvider] ?? "",
        temperature: defaultConfig.temperature,
      } as GeneratorConfig);
      return;
    }
    const liveDefault = liveGenerators[nextProvider]?.[0];
    onChange({
      ...value,
      provider: nextProvider,
      model: liveDefault || GENERATOR_PROVIDER_DEFAULTS[nextProvider] || value.model,
    } as GeneratorConfig);
  };

  return (
    <div className={cn("space-y-4", className)}>
      <RadioGroup value={mode} onValueChange={handleModeChange} className="gap-2">
        <label
          htmlFor={defaultId}
          className="flex items-start gap-3 rounded-none border p-3 cursor-pointer hover:bg-muted/30"
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
          className="flex items-start gap-3 rounded-none border p-3 cursor-pointer hover:bg-muted/30"
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

      {value?.provider === "chatgpt" && (
        <div className="text-xs space-y-1">
          <p>
            Using ChatGPT plan ·{" "}
            <a
              href="https://chatgpt.com/settings/usage"
              target="_blank"
              rel="noopener noreferrer"
              className="underline"
            >
              Manage usage
            </a>
          </p>
          {chatgpt.error && (
            <p role="alert" className="text-destructive">
              {chatgpt.error}
            </p>
          )}
          {!value.model && <p>Select an available model to continue.</p>}
          {!accounts.some((account) => account.connection_id === value.connection_id) && (
            <p>Reconnect this account on the Connections page.</p>
          )}
        </div>
      )}
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
                {availableProviders.map((provider) => (
                  <SelectItem key={provider} value={provider}>
                    {GENERATOR_PROVIDER_LABELS[provider] || provider}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          {value.provider === "chatgpt" && (
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">ChatGPT account</Label>
              <Select
                value={value.connection_id}
                onValueChange={(id) =>
                  onChange({
                    provider: "chatgpt",
                    connection_id: id,
                    model: chatgpt.models[id]?.[0]?.slug ?? "",
                  })
                }
              >
                <SelectTrigger>
                  <SelectValue placeholder="Choose an account" />
                </SelectTrigger>
                <SelectContent>
                  {accounts.map((account) => (
                    <SelectItem key={account.connection_id} value={account.connection_id}>
                      {account.email || "ChatGPT account"} · {account.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}
          <div className="space-y-2">
            <Label className="text-xs text-muted-foreground">Model</Label>
            <Combobox
              options={modelOptions}
              value={value.model}
              onChange={(model) => onChange({ ...value, model })}
              placeholder={GENERATOR_PROVIDER_DEFAULTS[value.provider]}
              searchPlaceholder="Search or type a model..."
              emptyText="Type a model name."
              allowCustomValue={value.provider !== "chatgpt"}
            />
          </div>
          {showTemperature && value.provider !== "chatgpt" && (
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
