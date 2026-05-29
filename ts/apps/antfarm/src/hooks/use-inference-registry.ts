import { useCallback, useEffect, useRef, useState } from "react";
import type {
  Backend,
  ModelType,
  ModelTypeInfo,
  QuantizationOption,
  RecognizerCapability,
  InferenceModel,
} from "@/data/inference-models";
import { useApiConfig } from "@/hooks/use-api-config";

const FETCH_TIMEOUT = 10000; // 10 seconds

type InferenceTaskKey =
  | "embedders"
  | "rerankers"
  | "chunkers"
  | "generators"
  | "recognizers"
  | "rewriters"
  | "readers"
  | "transcribers";

interface InferenceModelInfoResponse {
  capabilities?: unknown;
  inputs?: unknown;
}

type BackendRuntimesResponse = Partial<Record<Backend, unknown>>;

type InferenceModelsResponse = Partial<
  Record<InferenceTaskKey, Record<string, InferenceModelInfoResponse>>
> & {
  object?: unknown;
  data?: unknown;
  backends?: BackendRuntimesResponse;
};

const TASK_TO_TYPE: Record<InferenceTaskKey, ModelType> = {
  embedders: "embedder",
  rerankers: "reranker",
  chunkers: "chunker",
  generators: "generator",
  recognizers: "recognizer",
  rewriters: "rewriter",
  readers: "reader",
  transcribers: "transcriber",
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

function isInferenceModelsResponse(value: unknown): value is InferenceModelsResponse {
  if (typeof value !== "object" || value === null) return false;
  return TASK_KEYS.some((task) => {
    const models = (value as Partial<InferenceModelsResponse>)[task];
    return typeof models === "object" && models !== null && !Array.isArray(models);
  });
}

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string");
}

function isRecognizerCapability(value: string): value is RecognizerCapability {
  return value === "labels" || value === "zeroshot" || value === "relations" || value === "answers";
}

const BACKEND_ORDER: Backend[] = ["native", "onnx", "metal", "mlx", "cuda", "xla", "wasm"];

function enabledBackends(backends: BackendRuntimesResponse | undefined): Backend[] {
  if (!backends) return [];
  return BACKEND_ORDER.filter((backend) => backends[backend] === true);
}

function modelId(type: ModelType, name: string): string {
  return `${type}-${name}`
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function isHuggingFaceModelRef(name: string): boolean {
  return name.includes("/") && !name.startsWith("/") && !name.includes("://");
}

function modelDescription(type: ModelType, name: string, inputs: string[]): string {
  if (type === "chunker" && name.startsWith("fixed_")) {
    return "Built-in fixed-size chunker available without downloading a model.";
  }

  const inputText = inputs.length > 0 ? ` for ${inputs.join(", ")} input` : "";
  return `Installed ${MODEL_TYPE_NAMES[type]} model reported by Antfly inference${inputText}.`;
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

// Transform Antfly inference's live /ai/v1/models response into the UI model card format.
function transformModel(
  task: InferenceTaskKey,
  name: string,
  info: InferenceModelInfoResponse,
  backends: Backend[]
): InferenceModel {
  const type = TASK_TO_TYPE[task];
  const capabilities = stringArray(info.capabilities).filter(isRecognizerCapability);
  const inputs = stringArray(info.inputs);
  const sourceUrl = isHuggingFaceModelRef(name) ? `https://huggingface.co/${name}` : "";

  return {
    id: modelId(type, name),
    name,
    source: name,
    sourceUrl,
    type,
    description: modelDescription(type, name, inputs),
    capabilities,
    variants: sourceUrl ? ["f32"] : [],
    backends,
    inRegistry: true,
  };
}

function transformModels(data: InferenceModelsResponse): InferenceModel[] {
  const models: InferenceModel[] = [];
  const seen = new Set<string>();
  const runtimeBackends = enabledBackends(data.backends);

  for (const task of TASK_KEYS) {
    const taskModels = data[task];
    if (!taskModels) continue;

    for (const [name, info] of Object.entries(taskModels)) {
      const model = transformModel(task, name, info ?? {}, runtimeBackends);
      if (seen.has(model.id)) continue;
      seen.add(model.id);
      models.push(model);
    }
  }

  return models;
}

// Cache model data while keeping each configured Antfly inference endpoint isolated.
let modelCache: {
  key: string;
  models: InferenceModel[];
  types: ModelTypeInfo[];
  quantizationOptions: QuantizationOption[];
} | null = null;

export function useInferenceRegistry(): InferenceRegistryState {
  const { inferenceApiUrl } = useApiConfig();
  const cacheKey = `${inferenceApiUrl}/ai/v1/models`;
  const cached = modelCache?.key === cacheKey ? modelCache : null;
  const [models, setModels] = useState<InferenceModel[]>(cached?.models ?? []);
  const [types, setTypes] = useState<ModelTypeInfo[]>(cached?.types ?? []);
  const [quantizationOptions, setQuantizationOptions] = useState<QuantizationOption[]>(
    cached?.quantizationOptions ?? []
  );
  const [loading, setLoading] = useState(!cached);
  const [error, setError] = useState<string | null>(null);
  const isMountedRef = useRef(true);

  const fetchRegistry = useCallback(
    async (signal?: AbortSignal) => {
      setLoading(true);
      setError(null);
      const controller = new AbortController();
      const timeoutId = window.setTimeout(() => {
        controller.abort(new DOMException("Request timed out", "TimeoutError"));
      }, FETCH_TIMEOUT);
      const abortFromParent = () => controller.abort(signal?.reason);
      signal?.addEventListener("abort", abortFromParent, { once: true });

      try {
        const response = await fetch(cacheKey, {
          method: "GET",
          signal: controller.signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to fetch Antfly inference models: ${response.status}`);
        }

        const data: unknown = await response.json();

        if (!isMountedRef.current) return;

        if (!isInferenceModelsResponse(data)) {
          throw new Error("Antfly inference model response is missing model groups");
        }

        const transformedModels = transformModels(data);

        modelCache = {
          key: cacheKey,
          models: transformedModels,
          types: MODEL_TYPES,
          quantizationOptions: QUANTIZATION_OPTIONS,
        };

        setModels(transformedModels);
        setTypes(MODEL_TYPES);
        setQuantizationOptions(QUANTIZATION_OPTIONS);
        setLoading(false);
      } catch (err) {
        if (signal?.aborted) return;
        if (!isMountedRef.current) return;

        const message = err instanceof Error ? err.message : "Failed to fetch Antfly inference models";
        setError(message);
        setLoading(false);
      } finally {
        window.clearTimeout(timeoutId);
        signal?.removeEventListener("abort", abortFromParent);
      }
    },
    [cacheKey]
  );

  const retry = useCallback(() => {
    fetchRegistry();
  }, [fetchRegistry]);

  // Initial fetch on mount
  useEffect(() => {
    isMountedRef.current = true;

    // Skip fetch if we have cached data for this Antfly inference endpoint.
    if (modelCache?.key === cacheKey) {
      setModels(modelCache.models);
      setTypes(modelCache.types);
      setQuantizationOptions(modelCache.quantizationOptions);
      setLoading(false);
      return () => {
        isMountedRef.current = false;
      };
    }

    const controller = new AbortController();
    fetchRegistry(controller.signal);

    return () => {
      isMountedRef.current = false;
      controller.abort();
    };
  }, [cacheKey, fetchRegistry]);

  return {
    models,
    types,
    quantizationOptions,
    loading,
    error,
    retry,
  };
}
