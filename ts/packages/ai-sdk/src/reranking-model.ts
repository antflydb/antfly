import type {
  RerankingModelV4,
  RerankingModelV4CallOptions,
  RerankingModelV4Result,
} from "@ai-sdk/provider";
import { UnsupportedFunctionalityError } from "@ai-sdk/provider";
import type { InferenceClient } from "@antfly/sdk";

export interface AntflyRerankingModelOptions {
  inference: InferenceClient;
  modelId: string;
  providerId?: string;
}

/** `RerankingModelV4` over `/ai/v1/rerank`, built on `InferenceClient.rerank()`. */
export class AntflyRerankingModel implements RerankingModelV4 {
  readonly specificationVersion = "v4" as const;
  readonly provider: string;
  readonly modelId: string;

  private readonly inference: InferenceClient;

  constructor(options: AntflyRerankingModelOptions) {
    this.inference = options.inference;
    this.modelId = options.modelId;
    this.provider = options.providerId ?? "antfly";
  }

  async doRerank(options: RerankingModelV4CallOptions): Promise<RerankingModelV4Result> {
    if (options.documents.type !== "text") {
      throw new UnsupportedFunctionalityError({
        functionality: "object documents",
        message:
          "Antfly's /ai/v1/rerank endpoint reranks text or image content parts, not arbitrary JSON objects. Render documents to text first.",
      });
    }
    const response = await this.inference.rerank(
      this.modelId,
      options.query,
      options.documents.values,
      {
        signal: options.abortSignal,
        headers: options.headers,
      }
    );
    const ranking = response.data
      .map((item) => ({ index: item.index, relevanceScore: item.score }))
      .sort((a, b) => b.relevanceScore - a.relevanceScore)
      .slice(0, options.topN ?? response.data.length);
    return { ranking, warnings: [] };
  }
}
