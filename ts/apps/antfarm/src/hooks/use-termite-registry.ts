import { useCallback, useEffect, useRef, useState } from "react";
import type {
  Backend,
  ModelType,
  ModelTypeInfo,
  QuantizationOption,
  QuantizationType,
  RecognizerCapability,
  TermiteModel,
} from "@/data/termite-models";

// Always use the relative path — Vite proxy (dev) or Go reverse proxy (production)
const REGISTRY_URL = "/registry/index.json";
const FETCH_TIMEOUT = 10000; // 10 seconds

// Remote registry response types
interface RegistryModelResponse {
  name: string;
  owner: string;
  source: string;
  type: ModelType;
  description: string;
  capabilities?: RecognizerCapability[];
  variants: QuantizationType[];
  backends?: Backend[];
  size?: number; // Size in bytes from registry
}

interface RegistryResponse {
  schemaVersion: number;
  models: RegistryModelResponse[];
}

// Static model type metadata (not provided by registry)
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

// Static quantization options (not provided by registry)
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

export interface TermiteRegistryState {
  models: TermiteModel[];
  types: ModelTypeInfo[];
  quantizationOptions: QuantizationOption[];
  loading: boolean;
  error: string | null;
  retry: () => void;
}

// Transform registry model to TermiteModel format
function transformModel(apiModel: RegistryModelResponse): TermiteModel {
  // Generate ID from name (lowercase, replace underscores with dashes)
  const id = apiModel.name.toLowerCase().replace(/_/g, "-");

  // Generate source URL from source (owner/model format)
  const sourceUrl = apiModel.source ? `https://huggingface.co/${apiModel.source}` : "";

  // Format size as human-readable string if available
  const size = apiModel.size ? formatBytes(apiModel.size) : undefined;

  return {
    id,
    name: apiModel.name,
    source: apiModel.source,
    sourceUrl,
    type: apiModel.type,
    description: apiModel.description,
    capabilities: apiModel.capabilities,
    variants: apiModel.variants || [],
    backends: apiModel.backends,
    size,
    inRegistry: true, // All models from registry are in registry
  };
}

// Format bytes to human-readable string
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

// Cache for registry data to avoid refetching
let registryCache: {
  models: TermiteModel[];
  types: ModelTypeInfo[];
  quantizationOptions: QuantizationOption[];
} | null = null;

export function useTermiteRegistry(): TermiteRegistryState {
  const [models, setModels] = useState<TermiteModel[]>(registryCache?.models ?? []);
  const [types, setTypes] = useState<ModelTypeInfo[]>(registryCache?.types ?? []);
  const [quantizationOptions, setQuantizationOptions] = useState<QuantizationOption[]>(
    registryCache?.quantizationOptions ?? []
  );
  const [loading, setLoading] = useState(!registryCache);
  const [error, setError] = useState<string | null>(null);
  const isMountedRef = useRef(true);

  const fetchRegistry = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(REGISTRY_URL, {
        method: "GET",
        signal: signal ?? AbortSignal.timeout(FETCH_TIMEOUT),
      });

      if (!response.ok) {
        throw new Error(`Failed to fetch registry: ${response.status}`);
      }

      const data: RegistryResponse = await response.json();

      if (!isMountedRef.current) return;

      const transformedModels = data.models.map(transformModel);

      // Update cache (types and quantization are static)
      registryCache = {
        models: transformedModels,
        types: MODEL_TYPES,
        quantizationOptions: QUANTIZATION_OPTIONS,
      };

      setModels(transformedModels);
      setTypes(MODEL_TYPES);
      setQuantizationOptions(QUANTIZATION_OPTIONS);
      setLoading(false);
    } catch (err) {
      if ((err as Error).name === "AbortError") return;
      if (!isMountedRef.current) return;

      const message = err instanceof Error ? err.message : "Failed to fetch model registry";
      setError(message);
      setLoading(false);
    }
  }, []);

  const retry = useCallback(() => {
    fetchRegistry();
  }, [fetchRegistry]);

  // Initial fetch on mount
  useEffect(() => {
    isMountedRef.current = true;

    // Skip fetch if we have cached data
    if (registryCache) {
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
  }, [fetchRegistry]);

  return {
    models,
    types,
    quantizationOptions,
    loading,
    error,
    retry,
  };
}
