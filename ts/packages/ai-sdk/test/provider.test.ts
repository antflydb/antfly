import { NoSuchModelError } from "@ai-sdk/provider";
import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import { AntflyEmbeddingModel } from "../src/embedding-model.js";
import { AntflyChatLanguageModel } from "../src/language-model.js";
import { createAntfly } from "../src/provider.js";
import { AntflyRerankingModel } from "../src/reranking-model.js";

describe("createAntfly", () => {
  it("builds each model kind and exposes the underlying clients", () => {
    const provider = createAntfly({ baseUrl: "http://provider.test" });
    expect(provider.specificationVersion).toBe("v4");
    expect(provider.client).toBeInstanceOf(AntflyClient);
    expect(provider.inference).toBeInstanceOf(InferenceClient);
    expect(provider.languageModel("m")).toBeInstanceOf(AntflyChatLanguageModel);
    expect(provider.embeddingModel("m")).toBeInstanceOf(AntflyEmbeddingModel);
    expect(provider.rerankingModel?.("m")).toBeInstanceOf(AntflyRerankingModel);
  });

  it("throws NoSuchModelError for imageModel, since Antfly has none", () => {
    const provider = createAntfly({ baseUrl: "http://provider.test" });
    expect(() => provider.imageModel("anything")).toThrow(NoSuchModelError);
  });
});
