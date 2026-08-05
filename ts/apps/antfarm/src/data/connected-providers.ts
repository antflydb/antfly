// Connected provider metadata for the Connections page and providers summary.
// Covers the full inference model-kind taxonomy reported by /db/v1/connections.

import type { ConnectedModelType } from "@antfly/sdk";
import {
  ArrowUpDown,
  AudioLines,
  BookOpen,
  Cloud,
  Cpu,
  FileSearch,
  Fingerprint,
  type LucideIcon,
  Package,
  RefreshCw,
  Scissors,
  Server,
  Sparkles,
  Tags,
} from "lucide-react";

export const PROVIDER_TYPE_LABELS: Record<string, string> = {
  antfly: "Antfly (Local)",
  ollama: "Ollama (Local)",
  gemini: "Google AI (Gemini)",
  openai: "OpenAI",
  anthropic: "Anthropic (Claude)",
  vertex: "Google Cloud Vertex AI",
  cohere: "Cohere",
  openrouter: "OpenRouter",
  bedrock: "AWS Bedrock",
  mock: "Mock (Testing)",
};

export function providerTypeLabel(provider: string): string {
  return PROVIDER_TYPE_LABELS[provider] ?? provider;
}

const LOCAL_PROVIDERS = new Set(["antfly", "ollama", "mock"]);

export function providerTypeIcon(provider: string): LucideIcon {
  if (LOCAL_PROVIDERS.has(provider)) return Cpu;
  if (provider in PROVIDER_TYPE_LABELS) return Cloud;
  return Server;
}

export interface ConnectedModelKindInfo {
  kind: Exclude<ConnectedModelType, "other">;
  label: string;
  icon: LucideIcon;
}

// Ordered list of model kinds for grouped display. "other" renders last with
// a generic label for models the provider's API does not classify by task.
export const CONNECTED_MODEL_KINDS: ConnectedModelKindInfo[] = [
  { kind: "embedder", label: "Embedders", icon: Fingerprint },
  { kind: "generator", label: "Generators", icon: Sparkles },
  { kind: "reranker", label: "Rerankers", icon: ArrowUpDown },
  { kind: "chunker", label: "Chunkers", icon: Scissors },
  { kind: "classifier", label: "Classifiers", icon: Tags },
  { kind: "rewriter", label: "Rewriters", icon: RefreshCw },
  { kind: "reader", label: "Readers", icon: BookOpen },
  { kind: "transcriber", label: "Transcribers", icon: AudioLines },
  { kind: "extractor", label: "Extractors", icon: FileSearch },
];

export const OTHER_MODELS_GROUP = {
  key: "other",
  label: "Other models",
  icon: Package,
} as const;
