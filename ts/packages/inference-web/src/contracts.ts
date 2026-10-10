// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
export type WeightPrecision = "q8_0" | "q4_k" | "q4_0" | "fp32" | "fp16_encoder" | "fp16" | "bf16";
export type Backend = "auto" | "wasm" | "webgpu";
export type RuntimeArchitecture = "span" | "boundary" | "modernbert";
export type ModelFamily = "gliner2" | "gliner25" | "decide" | "laya" | "opendecider";
export type InferenceProgress = {
  stage:
    | "inspect"
    | "compatibility"
    | "gpu-init"
    | "runtime-init"
    | "verify"
    | "weights"
    | "download"
    | "reload"
    | "inference"
    | "validation"
    | "complete";
  file?: string;
  loaded: number;
  total: number;
};
export type BundleFiles = Map<string, Blob> | File[];
export type ModelInfo = Readonly<{
  family: ModelFamily;
  architecture: RuntimeArchitecture;
  precision: WeightPrecision;
  bytes: number;
  backend: "wasm" | "webgpu";
  /** Browser execution has no production qualification profile yet. */
  qualified: false;
  fallbackReason?: string;
}>;
export type RunOptions = {
  signal?: AbortSignal;
  onProgress?: (progress: InferenceProgress) => void;
};
export type OffsetUnit = "utf8_bytes" | "unicode_codepoints" | "utf16_codeunits";
export type InferenceRequestV1 = {
  schema_version: 1;
  /** A response label; the loaded bundle selects the model. */
  model: string;
  text: string;
  task?: "entities" | "classification" | "structures" | "relations";
  labels?: string[];
  relation_labels?: string[];
  schema?: Record<string, string[]>;
  threshold?: number;
  flat_ner?: boolean;
  multi_label?: boolean;
};
export type RegexValidator = {
  type?: "regex";
  pattern: string;
  mode?: "full" | "partial";
  exclude?: boolean;
  flags?: number;
};
export type ExtractionField = {
  type?: "str" | "list";
  dtype?: "str" | "list";
  description?: string;
  threshold?: number;
  choices?: string[];
  cardinality?: "optional_one" | "required_one" | "zero_or_more" | "one_or_more";
  exclusive?: boolean;
  validators?: RegexValidator[];
};
export type ClassificationTask = {
  name: string;
  labels: string[];
  mode?: "single" | "multi" | "ordinal";
  prompt?: string;
  instruction?: string;
  label_definitions?: Record<string, { description?: string }>;
  multi_label?: boolean;
  top_k?: number;
  hypothesis_template?: string;
  min_labels?: number;
  max_labels?: number | null;
  ordered?: boolean;
  threshold?: number;
  candidate_threshold?: number;
  activation?: "auto" | "sigmoid" | "softmax";
  temperature?: number;
  default?: string;
  examples?: [string, string][];
};
export type LayaQuestion = Omit<ClassificationTask, "mode" | "prompt" | "instruction"> &
  ({ prompt: string; instruction?: never } | { instruction: string; prompt?: never }) & {
    mode?: "single" | "ordinal" | "boolean";
  };
export type ExtractionSchema = {
  entities?: string[];
  entity_definitions?: Record<string, ExtractionField>;
  entity_attributes?: Record<
    string,
    {
      labels: string[];
      multi_label?: boolean;
      threshold?: number;
      applies_to?: string[] | null;
      qualify_labels?: boolean;
    }
  >;
  classifications?: ClassificationTask[];
  structures?: Record<
    string,
    {
      fields: Record<string, ExtractionField | string>;
      mode?: "natural" | "latent" | "anchorless";
      anchor?: string;
      occurrence_policy?: "all" | "first" | "error_on_ambiguous" | "latent_all";
    }
  >;
  relations?: (
    | string
    | { type: string; source?: string; target?: string; description?: string; threshold?: number }
  )[];
};
export type InferenceOptionsV2 = {
  threshold?: number;
  word_splitter?: "whitespace" | "char";
  flat_ner?: boolean;
  overlap?: "flat" | "allow" | "disallow";
  offset_unit?: OffsetUnit;
  include_confidence?: boolean;
  include_spans?: boolean;
  long_document?: {
    mode: "reject" | "window";
    window_words?: number;
    overlap_words?: number;
    max_windows?: number;
    record_identity?: "occurrence" | "semantic";
  };
  decoder?: {
    algorithm?: "auto" | "exact" | "beam";
    beam_width?: number;
    max_search_nodes?: number;
    max_local_assignments?: number;
    best_effort?: boolean;
  };
};
export type InferenceInput<S = ExtractionSchema, O = InferenceOptionsV2> = {
  id?: string;
  content: string;
  metadata?: Record<string, unknown>;
  schema?: S;
  options?: O;
};
export type ExtractionRequestV2 = {
  schema_version: 2;
  model: string;
  inputs: InferenceInput[];
  schema: ExtractionSchema;
  options?: InferenceOptionsV2;
};
export type DecideRequestV2 = Omit<ExtractionRequestV2, "schema" | "inputs"> & {
  schema: { classifications: ClassificationTask[] };
  inputs: InferenceInput<{ classifications: ClassificationTask[] }>[];
};
export type LayaSchema = { classifications: LayaQuestion[] };
export type LayaOptions = { include_confidence?: boolean; long_document?: { mode: "reject" } };
export type LayaRequestV2 = {
  schema_version: 2;
  model: string;
  inputs: InferenceInput<LayaSchema, LayaOptions>[];
  schema: LayaSchema;
  options?: LayaOptions;
};
export type InferenceRequestV2 = ExtractionRequestV2 | DecideRequestV2 | LayaRequestV2;
export type InferenceRequest = InferenceRequestV1 | InferenceRequestV2;
/** Explicit escape hatch for features not yet represented by these types.
 * The runtime still validates the entire request and rejects unknown features. */
export type ExtensionInferenceRequest = Record<string, unknown> & {
  schema_version: number;
  model: string;
};
export type EntityV1 = { label: string; text: string; start: number; end: number; score: number };
export type InferenceResponseV1 = {
  schema_version: 1;
  offset_unit?: "utf8_bytes";
  entities?: EntityV1[];
  classifications?: { label: string; score: number }[];
  relations?: {
    head: EntityV1;
    tail: EntityV1;
    label: string;
    score: number;
    owned_head_label?: string | null;
  }[];
  structures?: {
    name: string;
    instances: {
      fields: {
        name: string;
        value:
          | {
              single: {
                value: string;
                score?: number | null;
                start?: number | null;
                end?: number | null;
              };
            }
          | {
              list: {
                value: string;
                score?: number | null;
                start?: number | null;
                end?: number | null;
              }[];
            };
      }[];
    }[];
  }[];
};
export type ExtractionValue = {
  value: string;
  source: "document" | "schema";
  score?: number;
  start?: number;
  end?: number;
};
export type EntityV2 = {
  label: string;
  text: string;
  score?: number;
  start?: number;
  end?: number;
  attributes?: Record<
    string,
    { label: string; confidence: number } | { label: string; confidence: number }[]
  >;
};
export type ClassificationV2 = { name: string; label: string; score?: number };
export type RelationEndpoint = {
  text: string;
  entity_index?: number;
  label?: string;
  score?: number;
  start?: number;
  end?: number;
};
export type ExtractionOutputV2 = {
  id?: string;
  offset_unit: OffsetUnit;
  entities?: EntityV2[];
  classifications?: ClassificationV2[];
  relations?: {
    type: string;
    source: RelationEndpoint;
    target: RelationEndpoint;
    score?: number;
    derived?: true;
  }[];
  structures?: Record<string, Record<string, ExtractionValue | ExtractionValue[]>[]>;
  structure_metadata?: Record<
    string,
    { score?: number; anchor?: { start: number; end: number } }[]
  >;
  long_document?: {
    version: 1;
    window_count: number;
    window_policy: "source_words_midpoint_ownership";
    classification_aggregation: "owned_word_weighted_mean_raw_logits";
    duplicate_score: "maximum_calibrated_score";
    natural_record_identity: "exact_source_anchor";
    other_record_identity: "occurrence" | "semantic";
    solver_optimality_scope: "retained_candidate_graph";
  };
  solvers?: Partial<
    Record<
      "classification" | "joint_ie" | "records",
      { status: "optimal" | "feasible"; utility: number; visited_nodes: number; exhausted: boolean }
    >
  >;
};
export type Decision = {
  name: string;
  label: string;
  probabilities: { label: string; probability: number }[];
  confidence: number;
  confidence_method: "max_probability" | "normalized_inverse_entropy";
  act_probability?: number;
} & (
  | { type: "choice" }
  | { type: "score"; expected_value: number }
  | { type: "boolean"; true_probability: number }
);
export type InferenceUsage = {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
};
type EnvelopeV2<T> = {
  object: "extraction";
  schema_version: 2;
  model: string;
  data: T[];
  usage: InferenceUsage;
};
export type ExtractionResponseV2 = EnvelopeV2<ExtractionOutputV2>;
export type LayaResponseV2 = EnvelopeV2<{
  id?: string;
  classifications: ClassificationV2[];
  decisions: Decision[];
}>;
export type InferenceResponseV2 = ExtractionResponseV2 | LayaResponseV2;
export type InferenceResponse = InferenceResponseV1 | InferenceResponseV2;
export type RunResult<T = InferenceResponse> = {
  value: T;
  elapsedMs: number;
  wasmBytes: number;
  backend: "wasm" | "webgpu";
};
export type ValidationResult = RunResult<{ valid: true; encoded_tokens: number }>;
export type CatalogFile = { path: string; url: string; sha256: string; size_bytes: number };
export type CatalogModel = {
  id: string;
  name: string;
  family: ModelFamily;
  architecture: RuntimeArchitecture;
  precision: WeightPrecision;
  files: CatalogFile[];
  /** Catalog checkpoint verification only; does not qualify browser execution. */
  qualification: "pending" | "passed";
  license: string;
};
/** @deprecated Use WeightPrecision. */
export type Precision = WeightPrecision;
/** @deprecated Use InferenceProgress. */
export type Progress = InferenceProgress;
/** @deprecated Use InferenceRequest. */
export type Request = InferenceRequest;
