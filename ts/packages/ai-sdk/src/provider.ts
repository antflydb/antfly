import type { ImageModelV4, LanguageModelV4, ProviderV4, RerankingModelV4 } from "@ai-sdk/provider";
import { NoSuchModelError } from "@ai-sdk/provider";
import {
  type AntflyClient,
  Client as AntflySDK,
  type InferenceClient,
  type SDKConfig,
} from "@antfly/sdk";
import type { AuthorizeAsset } from "./asset/reference.js";
import { AntflyEmbeddingModel } from "./embedding-model.js";
import { AntflyChatLanguageModel } from "./language-model.js";
import { AntflyRerankingModel } from "./reranking-model.js";

export type AntflyProviderOptions = SDKConfig & {
  /**
   * The app's decision on referenced assets. Without it, file parts carrying
   * `providerReference.antfly` are refused.
   */
  authorizeAsset?: AuthorizeAsset;
};

/**
 * An Antfly `ProviderV4`, plus the underlying `AntflyClient` so callers can
 * build the memory/search/graph/SQL tools (`createMemoryTools`,
 * `createSearchTool`, `createGraphTool`, `createSqlTool`) and the chat
 * history persistence helper without constructing a second client.
 */
export interface AntflyLanguageModelOptions {
  /** Overrides the provider's `authorizeAsset`, for a decision bound to one user. */
  authorizeAsset?: AuthorizeAsset;
}

export interface AntflyProvider extends ProviderV4 {
  languageModel(modelId: string, options?: AntflyLanguageModelOptions): LanguageModelV4;
  readonly client: AntflyClient;
  readonly inference: InferenceClient;
  // Narrowed from ProviderV4's optional `rerankingModel?`: this provider
  // always implements it, so callers don't need an `?.` or non-null assertion.
  rerankingModel(modelId: string): RerankingModelV4;
}

/**
 * Creates an Antfly provider for the Vercel AI SDK (`ai` v7).
 *
 * Builds on `@antfly/sdk`'s own `Client` (the same pair of hand-written
 * `AntflyClient`/`InferenceClient` wrappers the rest of the SDK uses), so
 * authentication headers and base-URL handling stay in one place rather
 * than being re-derived here.
 *
 * `imageModel()` always throws `NoSuchModelError`: Antfly has no image
 * generation model today, and `ProviderV4` requires the method to exist.
 * `transcriptionModel`, `speechModel`, `files`, and `skills` are
 * intentionally omitted (optional on `ProviderV4`) rather than stubbed.
 */
export function createAntfly(options: AntflyProviderOptions): AntflyProvider {
  const sdk = new AntflySDK(options);
  const client = sdk.antfly;
  const inference = sdk.inference;
  return {
    client,
    inference,
    specificationVersion: "v4",
    languageModel: (modelId: string, modelOptions?: AntflyLanguageModelOptions) =>
      new AntflyChatLanguageModel({
        inference,
        modelId,
        providerId: "antfly",
        client,
        authorizeAsset: modelOptions?.authorizeAsset ?? options.authorizeAsset,
      }),
    embeddingModel: (modelId: string) =>
      new AntflyEmbeddingModel({ inference, modelId, providerId: "antfly" }),
    rerankingModel: (modelId: string) =>
      new AntflyRerankingModel({ inference, modelId, providerId: "antfly" }),
    imageModel: (modelId: string): ImageModelV4 => {
      throw new NoSuchModelError({
        modelId,
        modelType: "imageModel",
        message: "Antfly has no image generation model.",
      });
    },
  };
}
