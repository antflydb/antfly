// Termite Model Type Definitions
// Types and helper functions for working with Termite registry models

export type ModelType =
  | "embedder"
  | "reranker"
  | "chunker"
  | "recognizer"
  | "rewriter"
  | "generator"
  | "reader"
  | "transcriber";

export type RecognizerCapability = "labels" | "zeroshot" | "relations" | "answers";

export type QuantizationType = "f32" | "f16" | "i8" | "i8-st" | "i4" | "i4-cuda";

export type Backend = "onnx" | "xla" | "go";

// Hardware capabilities that models can run on
export type HardwareCapability = "nvidia-gpu" | "apple-silicon" | "google-tpu" | "cpu";

export interface HardwareInfo {
  type: HardwareCapability;
  label: string;
  shortLabel: string;
  description: string;
}

// Hardware capability metadata
export const HARDWARE_INFO: Record<HardwareCapability, HardwareInfo> = {
  "nvidia-gpu": {
    type: "nvidia-gpu",
    label: "NVIDIA GPU",
    shortLabel: "GPU",
    description: "NVIDIA CUDA-enabled GPUs for accelerated inference",
  },
  "apple-silicon": {
    type: "apple-silicon",
    label: "Apple Silicon",
    shortLabel: "Apple",
    description: "Apple M-series chips with Metal/CoreML acceleration",
  },
  "google-tpu": {
    type: "google-tpu",
    label: "Google TPU",
    shortLabel: "TPU",
    description: "Google Cloud TPU for high-throughput inference",
  },
  cpu: {
    type: "cpu",
    label: "CPU",
    shortLabel: "CPU",
    description: "Standard CPU inference (always available)",
  },
};

// Map backends to supported hardware capabilities
export const BACKEND_TO_HARDWARE: Record<Backend, HardwareCapability[]> = {
  onnx: ["nvidia-gpu", "apple-silicon", "cpu"],
  xla: ["google-tpu", "nvidia-gpu", "cpu"],
  go: ["cpu"],
};

// Get unique hardware capabilities from a list of backends
export function getHardwareCapabilities(backends?: Backend[]): HardwareCapability[] {
  if (!backends || backends.length === 0) {
    return ["cpu"]; // Default to CPU-only if no backends specified
  }

  const capabilities = new Set<HardwareCapability>();
  for (const backend of backends) {
    const hardware = BACKEND_TO_HARDWARE[backend];
    if (hardware) {
      for (const cap of hardware) {
        capabilities.add(cap);
      }
    }
  }

  // Return in a logical order: GPU first, then Apple, then TPU, then CPU
  const order: HardwareCapability[] = ["nvidia-gpu", "apple-silicon", "google-tpu", "cpu"];
  return order.filter((cap) => capabilities.has(cap));
}

// Variant presets for quick selection
export type VariantPreset = "recommended" | "smallest" | "highest-quality";

export interface VariantPresetInfo {
  type: VariantPreset;
  label: string;
  description: string;
  // Function to find the best matching variant from available options
  selectVariant: (available: QuantizationType[]) => QuantizationType | undefined;
}

export const VARIANT_PRESETS: VariantPresetInfo[] = [
  {
    type: "recommended",
    label: "Recommended",
    description: "Best balance of speed and accuracy (FP16)",
    selectVariant: (available) => {
      // Prefer f16, then f32, then i8
      if (available.includes("f16")) return "f16";
      if (available.includes("f32")) return "f32";
      if (available.includes("i8")) return "i8";
      return available[0];
    },
  },
  {
    type: "smallest",
    label: "Smallest",
    description: "Fastest inference, smallest download (INT8)",
    selectVariant: (available) => {
      // Prefer i8, then i4, then f16
      if (available.includes("i8")) return "i8";
      if (available.includes("i4")) return "i4";
      if (available.includes("f16")) return "f16";
      return available[0];
    },
  },
  {
    type: "highest-quality",
    label: "Best Quality",
    description: "Maximum accuracy, larger download (FP32)",
    selectVariant: (available) => {
      // Prefer f32, then f16
      if (available.includes("f32")) return "f32";
      if (available.includes("f16")) return "f16";
      return available[0];
    },
  },
];

export interface QuantizationOption {
  type: QuantizationType;
  name: string;
  description: string;
  recommended?: boolean;
  generatorOnly?: boolean;
}

export interface TermiteModel {
  id: string;
  name: string;
  source: string;
  sourceUrl: string;
  type: ModelType;
  description: string;
  capabilities?: RecognizerCapability[];
  variants: QuantizationType[];
  backends?: Backend[];
  architecture?: string;
  size?: string;
  inRegistry: boolean;
}

export interface ModelTypeInfo {
  type: ModelType;
  name: string;
  description: string;
  icon: string;
}

// Map quantization types to CLI variant names
const VARIANT_CLI_NAMES: Record<QuantizationType, string> = {
  f32: "fp32",
  f16: "fp16",
  i8: "int8",
  "i8-st": "int8-static",
  i4: "int4",
  "i4-cuda": "int4-cuda",
};

// Helper functions that work with model data

export function getModelsByType(models: TermiteModel[], type: ModelType): TermiteModel[] {
  return models.filter((model) => model.type === type);
}

export function getModelById(models: TermiteModel[], id: string): TermiteModel | undefined {
  return models.find((model) => model.id === id);
}

export function getRegistryModels(models: TermiteModel[]): TermiteModel[] {
  return models.filter((model) => model.inRegistry);
}

export function getExportableModels(models: TermiteModel[]): TermiteModel[] {
  return models.filter((model) => !model.inRegistry);
}

export function getModelsWithCapability(
  models: TermiteModel[],
  capability: RecognizerCapability
): TermiteModel[] {
  return models.filter((model) => model.capabilities?.includes(capability));
}

export function getQuantizationInfo(
  options: QuantizationOption[],
  type: QuantizationType
): QuantizationOption | undefined {
  return options.find((opt) => opt.type === type);
}

// Shell-safe: only allow alphanumeric, hyphens, underscores, dots, slashes, and colons
function shellSafe(s: string): string {
  return s.replace(/[^a-zA-Z0-9\-_./:]/g, "");
}

// Generate download command for a model
export function getDownloadCommand(model: TermiteModel, quantization?: QuantizationType): string {
  const source = shellSafe(model.source);

  if (model.inRegistry) {
    // Registry pull: termite pull SOURCE (variant appended with colon)
    let cmd = `termite pull ${source}`;
    if (quantization) {
      cmd += `:${quantization}`;
    }
    return cmd;
  }

  // HuggingFace pull: termite pull SOURCE --type TYPE
  let cmd = `termite pull ${source} --type ${model.type}`;
  if (quantization) {
    const variantName = VARIANT_CLI_NAMES[quantization];
    if (!variantName) {
      console.error(`Unknown quantization type: ${quantization}`);
      return cmd;
    }
    cmd += ` --variant ${variantName}`;
  }

  return cmd;
}

// Map model types to their playground routes
export const MODEL_TYPE_PLAYGROUND: Partial<Record<ModelType, string>> = {
  chunker: "/playground/chunking",
  recognizer: "/playground/recognize",
  rewriter: "/playground/rewrite",
  reranker: "/playground/rerank",
  transcriber: "/playground/transcribe",
};

// Detailed model type information for educational banners
export interface ModelTypeDetail {
  type: ModelType;
  tagline: string;
  description: string;
  useCases: string[];
  pipelineNote: string;
}

export const MODEL_TYPE_DETAILS: Record<ModelType, ModelTypeDetail> = {
  embedder: {
    type: "embedder",
    tagline: "Convert text into dense vector representations",
    description:
      "Embedding models transform text into high-dimensional vectors that capture semantic meaning. Similar texts produce similar vectors, enabling semantic search, clustering, and recommendation systems.",
    useCases: [
      "Semantic search over documents and knowledge bases",
      "Clustering similar content for deduplication",
      "Building recommendation engines",
      "Zero-shot classification via cosine similarity",
    ],
    pipelineNote:
      "Embedders are the foundation of any vector search pipeline. They run at index time (to embed documents) and at query time (to embed the search query).",
  },
  reranker: {
    type: "reranker",
    tagline: "Score document relevance with cross-encoder precision",
    description:
      "Rerankers use cross-encoder architectures to jointly evaluate a query-document pair, producing a fine-grained relevance score. They are slower than embeddings but significantly more accurate for ranking.",
    useCases: [
      "Improving search result ordering after initial retrieval",
      "RAG pipelines to select the best context passages",
      "Filtering out irrelevant results before LLM generation",
      "A/B testing different retrieval strategies",
    ],
    pipelineNote:
      "Rerankers sit between retrieval and generation. They re-score the top-K results from an embedding search to produce a more accurate ranking.",
  },
  chunker: {
    type: "chunker",
    tagline: "Split documents into semantically coherent pieces",
    description:
      "Chunking models intelligently split long documents into smaller segments that preserve meaning. Unlike fixed-size splitting, semantic chunkers detect natural topic boundaries using neural networks.",
    useCases: [
      "Preprocessing documents before embedding for search",
      "Splitting long articles while preserving context",
      "Preparing content for RAG pipelines",
      "Breaking down legal or technical documents by section",
    ],
    pipelineNote:
      "Chunkers are the first step in an indexing pipeline. Good chunking directly impacts retrieval quality — chunks that are too large dilute relevance, too small lose context.",
  },
  recognizer: {
    type: "recognizer",
    tagline: "Extract named entities and structured data from text",
    description:
      "Recognizer models identify and classify entities (people, organizations, dates, custom types) in text. GLiNER-based models support zero-shot recognition — add any label without retraining.",
    useCases: [
      "Extracting people, places, and organizations from documents",
      "Building knowledge graphs from unstructured text",
      "Enriching search indexes with entity metadata",
      "Structured data extraction with custom schemas",
    ],
    pipelineNote:
      "Recognizers enhance indexed documents with structured metadata. Entity annotations improve faceted search, filtering, and knowledge graph construction.",
  },
  rewriter: {
    type: "rewriter",
    tagline: "Transform text using sequence-to-sequence models",
    description:
      "Rewriter models use T5/FLAN-T5 architectures for text transformation tasks like question generation, paraphrasing, and summarization. They take a text input and produce a transformed output.",
    useCases: [
      "Generating evaluation questions from context passages",
      "Creating training data for search quality testing",
      "Query expansion and paraphrasing for better retrieval",
      "Building evaluation datasets automatically",
    ],
    pipelineNote:
      "Rewriters are used in evaluation pipelines and query augmentation. They can generate synthetic Q&A pairs for testing search quality.",
  },
  generator: {
    type: "generator",
    tagline: "Generate text responses using language models",
    description:
      "Generator models produce natural language output for tasks like answering questions, summarization, and content generation. In Termite, generators power the answer synthesis step in RAG pipelines.",
    useCases: [
      "Answering questions based on retrieved context (RAG)",
      "Summarizing search results into concise answers",
      "Generating descriptions or metadata for content",
      "Powering conversational search interfaces",
    ],
    pipelineNote:
      "Generators are the final step in a RAG pipeline. They synthesize retrieved context into a coherent answer for the user.",
  },
  reader: {
    type: "reader",
    tagline: "Extract precise answers from context passages",
    description:
      "Reader models perform extractive question answering — given a question and a passage, they identify the exact span of text that answers the question, along with a confidence score.",
    useCases: [
      "Extractive QA over retrieved documents",
      "Finding specific facts in long documents",
      "Powering FAQ and support answer systems",
      "Validating generated answers against source text",
    ],
    pipelineNote:
      "Readers complement generators in QA pipelines. Where generators synthesize answers, readers extract exact spans — useful for verifiable, citation-backed responses.",
  },
  transcriber: {
    type: "transcriber",
    tagline: "Convert speech to text with automatic speech recognition",
    description:
      "Transcriber models perform speech-to-text (ASR), converting audio into accurate text transcriptions. Whisper-based models support multilingual transcription and can handle noisy audio, accents, and technical vocabulary.",
    useCases: [
      "Transcribing meeting recordings and voice memos",
      "Building voice-driven search and dictation interfaces",
      "Processing podcast and video audio for indexing",
      "Real-time speech-to-text with VAD-based chunking",
    ],
    pipelineNote:
      "Transcribers convert audio into text that can then be chunked, embedded, and indexed. Combine with VAD for streaming and LLM cleanup for polished output.",
  },
};

// Get the model card URL
export function getModelCardUrl(model: TermiteModel): string {
  return model.sourceUrl;
}
