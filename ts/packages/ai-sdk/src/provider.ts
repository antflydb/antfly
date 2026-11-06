import type { ImageModelV4, ProviderV4 } from "@ai-sdk/provider";
import { NoSuchModelError } from "@ai-sdk/provider";
import {
  type AntflyClient,
  Client as AntflySDK,
  type InferenceClient,
  type SDKConfig,
} from "@antfly/sdk";
import { AntflyEmbeddingModel } from "./embedding-model.js";
import { AntflyChatLanguageModel } from "./language-model.js";
import { AntflyRerankingModel } from "./reranking-model.js";

export type AntflyProviderOptions = SDKConfig;

/**
 * An Antfly `ProviderV4`, plus the underlying `AntflyClient` so callers can
 * build the memory/search/graph/SQL tools (`createMemoryTools`,
 * `createSearchTool`, `createGraphTool`, `createSqlTool`) and the chat
 * history persistence helper without constructing a second client.
 */
export interface AntflyProvider extends ProviderV4 {
  readonly client: AntflyClient;
  readonly inference: InferenceClient;
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
    languageModel: (modelId: string) =>
      new AntflyChatLanguageModel({ inference, modelId, providerId: "antfly" }),
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
