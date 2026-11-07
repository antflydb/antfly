import {
  type EmbeddingModelV4,
  type EmbeddingModelV4CallOptions,
  type EmbeddingModelV4Result,
  UnsupportedFunctionalityError,
} from "@ai-sdk/provider";
import type { InferenceClient } from "@antfly/sdk";

export interface AntflyEmbeddingModelOptions {
  inference: InferenceClient;
  modelId: string;
  providerId?: string;
}

/** `EmbeddingModelV4` over `/ai/v1/embed`, built on `InferenceClient.embed()`. */
export class AntflyEmbeddingModel implements EmbeddingModelV4 {
  readonly specificationVersion = "v4" as const;
  readonly provider: string;
  readonly modelId: string;
  // The server does not document a maximum batch size for /ai/v1/embed.
  readonly maxEmbeddingsPerCall: number | undefined = undefined;
  readonly supportsParallelCalls = true;

  private readonly inference: InferenceClient;

  constructor(options: AntflyEmbeddingModelOptions) {
    this.inference = options.inference;
    this.modelId = options.modelId;
    this.provider = options.providerId ?? "antfly";
  }

  async doEmbed(options: EmbeddingModelV4CallOptions): Promise<EmbeddingModelV4Result> {
    const response = await this.inference.embed(this.modelId, options.values, {
      dimensions: options.dimensions,
      signal: options.abortSignal,
      headers: options.headers,
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
