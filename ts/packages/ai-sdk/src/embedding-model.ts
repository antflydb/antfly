import {
  type EmbeddingModelV4,
  type EmbeddingModelV4CallOptions,
  type EmbeddingModelV4Result,
  UnsupportedFunctionalityError,
} from "@ai-sdk/provider";
import { EXPERIMENTAL_EMBEDDING_MODEL_MAX_INPUT_BYTES_PER_CALL } from "@ai-sdk/provider-utils";
import type { InferenceClient } from "@antfly/sdk";

import { providerInferenceError } from "./inference-error.js";

export interface AntflyEmbeddingLimits {
  /** Effective deployment/model batch limit; defaults to 128. */
  maxEmbeddingsPerCall?: number;
  /** Maximum UTF-8 input bytes per call; defaults to 1 MiB. */
  maxInputBytesPerCall?: number;
}

export interface AntflyEmbeddingModelOptions extends AntflyEmbeddingLimits {
  inference: InferenceClient;
  modelId: string;
  providerId?: string;
}

/** `EmbeddingModelV4` over `/ai/v1/embed`, built on `InferenceClient.embed()`. */
export class AntflyEmbeddingModel implements EmbeddingModelV4 {
  readonly specificationVersion = "v4" as const;
  readonly provider: string;
  readonly modelId: string;
  readonly maxEmbeddingsPerCall: number;
  readonly [EXPERIMENTAL_EMBEDDING_MODEL_MAX_INPUT_BYTES_PER_CALL]: number;
  readonly supportsParallelCalls = true;

  private readonly inference: InferenceClient;

  constructor(options: AntflyEmbeddingModelOptions) {
    this.maxEmbeddingsPerCall = options.maxEmbeddingsPerCall ?? 128;
    if (!Number.isSafeInteger(this.maxEmbeddingsPerCall) || this.maxEmbeddingsPerCall < 1) {
      throw new RangeError("maxEmbeddingsPerCall must be a positive safe integer");
    }
    const byteLimit = options.maxInputBytesPerCall ?? 1024 * 1024;
    if (!Number.isSafeInteger(byteLimit) || byteLimit < 1) {
      throw new RangeError("maxInputBytesPerCall must be a positive safe integer");
    }
    this[EXPERIMENTAL_EMBEDDING_MODEL_MAX_INPUT_BYTES_PER_CALL] = byteLimit;
    this.inference = options.inference;
    this.modelId = options.modelId;
    this.provider = options.providerId ?? "antfly";
  }

  async doEmbed(options: EmbeddingModelV4CallOptions): Promise<EmbeddingModelV4Result> {
    if (options.values.length > this.maxEmbeddingsPerCall) {
      throw new RangeError(
        `Embedding batch exceeds maxEmbeddingsPerCall (${this.maxEmbeddingsPerCall})`
      );
    }
    const byteLimit = this[EXPERIMENTAL_EMBEDDING_MODEL_MAX_INPUT_BYTES_PER_CALL];
    if (
      options.values.reduce((bytes, value) => bytes + Buffer.byteLength(value, "utf8"), 0) >
      byteLimit
    ) {
      throw new RangeError(`Embedding batch exceeds maxInputBytesPerCall (${byteLimit})`);
    }
    const response = await this.inference
      .embed(this.modelId, options.values, {
        dimensions: options.dimensions,
        signal: options.abortSignal,
        headers: options.headers,
      })
      .catch((error) => {
        throw providerInferenceError(error, "embed");
      });
    if (response.errors?.length) {
      throw new Error(
        `Antfly embedding request failed for ${response.errors.length} of ${options.values.length} inputs: ${JSON.stringify(response.errors[0])}`
      );
    }
    const byIndex = new Map(response.data.map((item) => [item.index, item]));
    const embeddings = options.values.map((_, index) => {
      const item = byIndex.get(index);
      if (!item) throw new Error(`Antfly embedding response is missing input index ${index}`);
      if (!Array.isArray(item.embedding)) {
        throw new UnsupportedFunctionalityError({
          functionality: "sparse embeddings",
          message: `Model '${this.modelId}' returned a sparse embedding; the AI SDK embedding model interface expects dense vectors.`,
        });
      }
      return item.embedding;
    });
    return {
      embeddings,
      usage: response.usage ? { tokens: response.usage.prompt_tokens } : undefined,
      warnings: [],
    };
  }
}
