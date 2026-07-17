import type { Connection } from "@antfly/sdk";
import { useMemo } from "react";
import type {
  InferenceModel,
  ModelType,
  ModelTypeInfo,
  QuantizationOption,
} from "@/data/inference-models";
import { useConnectedModels } from "@/hooks/use-connections";

type InferenceTaskKey =
  | "embedders"
  | "rerankers"
  | "chunkers"
  | "generators"
  | "recognizers"
  | "rewriters"
  | "readers"
  | "transcribers"
  | "other";

const TASK_TO_TYPE: Record<InferenceTaskKey, ModelType> = {
  embedders: "embedder",
  rerankers: "reranker",
  chunkers: "chunker",
  generators: "generator",
  recognizers: "recognizer",
  rewriters: "rewriter",
  readers: "reader",
  transcribers: "transcriber",
  // OpenAI-compatible listing APIs generally do not report task metadata.
  other: "generator",
};

const MODEL_TYPE_NAMES: Record<ModelType, string> = {
  embedder: "embedding",
  reranker: "reranking",
  chunker: "chunking",
  recognizer: "recognition",
  rewriter: "rewriting",
  generator: "generation",
  reader: "reader",
  transcriber: "transcription",
};

const TASK_KEYS = Object.keys(TASK_TO_TYPE) as InferenceTaskKey[];

function modelId(type: ModelType, name: string): string {
  return `${type}-${name}`
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function isHuggingFaceModelRef(name: string): boolean {
  return name.includes("/") && !name.startsWith("/") && !name.includes("://");
}

// Static model type metadata (not provided by Antfly inference's model list)
const MODEL_TYPES: ModelTypeInfo[] = [
  {
    type: "embedder",
    name: "Embedder",
    description:
      "Generate vector embeddings from text or images for semantic search and similarity",
    icon: "Fingerprint",
  },
  {
    type: "reranker",
    name: "Reranker",
    description: "Re-rank documents by relevance to a query for improved search results",
    icon: "ArrowUpDown",
  },
  {
    type: "chunker",
    name: "Chunker",
    description: "Semantic text chunking and segmentation for document processing",
    icon: "Scissors",
  },
  {
    type: "recognizer",
    name: "Recognizer",
    description: "Entity recognition, relation extraction, and question answering",
    icon: "Tag",
  },
  {
    type: "rewriter",
    name: "Rewriter",
    description:
      "Sequence-to-sequence text transformation like paraphrasing and question generation",
    icon: "RefreshCw",
  },
  {
    type: "generator",
    name: "Generator",
    description: "Generative language models for text generation and function calling",
    icon: "Sparkles",
  },
  {
    type: "reader",
    name: "Reader",
    description: "Document and image reading models for OCR and text extraction",
    icon: "BookOpen",
  },
  {
    type: "transcriber",
    name: "Transcriber",
    description: "Speech-to-text models for audio transcription and dictation",
    icon: "AudioLines",
  },
];

// Static quantization options (not provided by Antfly inference's model list)
const QUANTIZATION_OPTIONS: QuantizationOption[] = [
  {
    type: "f32",
    name: "Float32",
    description: "Full precision - largest size, highest accuracy",
  },
  {
    type: "f16",
    name: "Float16",
    description: "Half precision - recommended for ARM64/M-series Macs",
    recommended: true,
  },
  {
    type: "i8",
    name: "INT8",
    description: "8-bit integer quantization - smallest size, fastest inference",
  },
  {
    type: "i8-st",
    name: "INT8 Static",
    description: "Static INT8 quantization - calibrated for specific data distributions",
  },
  {
    type: "i4",
    name: "INT4",
    description: "4-bit integer quantization - very small, for generators only",
    generatorOnly: true,
  },
  {
    type: "i4-cuda",
    name: "INT4 CUDA",
    description: "CUDA-optimized 4-bit quantization - for NVIDIA GPUs, generators only",
    generatorOnly: true,
  },
];

export interface InferenceRegistryState {
  models: InferenceModel[];
  types: ModelTypeInfo[];
  quantizationOptions: QuantizationOption[];
  loading: boolean;
  error: string | null;
  retry: () => void;
}

export function transformConnectionModels(connections: Connection[]): InferenceModel[] {
  const models: InferenceModel[] = [];
  const seen = new Set<string>();
  for (const connection of connections) {
    const inference = connection.inference;
    if (!inference?.models) continue;
    const connectionName = connection.display_name ?? connection.name;

    for (const task of TASK_KEYS) {
      const type = TASK_TO_TYPE[task];
      for (const listed of inference.models[task] ?? []) {
        const id = modelId(type, `${connection.id}-${listed.name}`);
        if (seen.has(id)) continue;
        seen.add(id);
        const sourceUrl = isHuggingFaceModelRef(listed.name)
          ? `https://huggingface.co/${listed.name}`
          : "";
        models.push({
          id,
          name: listed.display_name ?? listed.name,
          connectionId: connection.id,
          connectionName,
          provider: inference.provider,
          source: listed.name,
          sourceUrl,
          type,
          description: `${MODEL_TYPE_NAMES[type][0]?.toUpperCase()}${MODEL_TYPE_NAMES[type].slice(1)} model available through ${connectionName}.`,
          variants: [],
          inRegistry: connection.status === "connected",
        });
      }
    }
  }
  return models;
}

export function useInferenceRegistry(): InferenceRegistryState {
  const { providers, loading, error, retry } = useConnectedModels();
  const models = useMemo(() => transformConnectionModels(providers), [providers]);

  return {
    models,
    types: MODEL_TYPES,
    quantizationOptions: QUANTIZATION_OPTIONS,
    loading,
    error,
    retry,
  };
}
