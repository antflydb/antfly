import type { ConnectedModelType } from "@antfly/sdk";

// Keep every provider-neutral task represented, plus the gateway's chain alias.
// The exhaustive key type makes SDK task additions a compile-time check.
type InferenceModelGroup = `${Exclude<ConnectedModelType, "other">}s` | "other" | "chains";
export const INFERENCE_MODEL_GROUP_TYPES = {
  embedders: "embedder",
  generators: "generator",
  rerankers: "reranker",
  chunkers: "chunker",
  classifiers: "classifier",
  rewriters: "rewriter",
  readers: "reader",
  transcribers: "transcriber",
  extractors: "extractor",
  deciders: "decider",
  synthesizers: "synthesizer",
  predictors: "predictor",
  chains: "generator",
  other: "other",
} as const satisfies Record<InferenceModelGroup, ConnectedModelType>;

export const INFERENCE_MODEL_GROUPS = Object.keys(
  INFERENCE_MODEL_GROUP_TYPES
) as InferenceModelGroup[];

const groupsByKind = new Map<ConnectedModelType, InferenceModelGroup[]>();
for (const group of INFERENCE_MODEL_GROUPS) {
  const kind = INFERENCE_MODEL_GROUP_TYPES[group];
  const groups = groupsByKind.get(kind) ?? [];
  groups.push(group);
  groupsByKind.set(kind, groups);
}

export function inferenceModelGroups(kind: ConnectedModelType): readonly InferenceModelGroup[] {
  return groupsByKind.get(kind) ?? [];
}
