// Type-level compile checks only: this file asserts, at `tsc` time, that
// `createAntfly()` really satisfies the installed `@ai-sdk/provider@4.0.35`
// `ProviderV4` interface (and that each model kind satisfies its own V4
// model interface) -- not from memory, but against the real package in
// node_modules. A regression here fails `pnpm typecheck` / `pnpm test`,
// not just a runtime assertion.

import type {
  EmbeddingModelV4,
  ImageModelV4,
  LanguageModelV4,
  ProviderV4,
  RerankingModelV4,
} from "@ai-sdk/provider";
import { it } from "vitest";
import { AntflyEmbeddingModel } from "../src/embedding-model.js";
import { AntflyChatLanguageModel } from "../src/language-model.js";
import type { AntflyProvider } from "../src/provider.js";
import { createAntfly } from "../src/provider.js";
import { AntflyRerankingModel } from "../src/reranking-model.js";

// `AntflyProvider` must be a `ProviderV4`.
const _providerIsProviderV4: ProviderV4 = createAntfly({ baseUrl: "http://contract.test" });
// `createAntfly`'s return type itself, independent of the `ProviderV4` widening above.
const _providerType: AntflyProvider = createAntfly({ baseUrl: "http://contract.test" });

// Each model kind must satisfy its own V4 model interface.
const _languageModel: LanguageModelV4 = new AntflyChatLanguageModel({
  inference: _providerType.inference,
  modelId: "m",
});
const _embeddingModel: EmbeddingModelV4 = new AntflyEmbeddingModel({
  inference: _providerType.inference,
  modelId: "m",
});
const _rerankingModel: RerankingModelV4 = new AntflyRerankingModel({
  inference: _providerType.inference,
  modelId: "m",
});

// imageModel() must at least type-check as returning an ImageModelV4, even
// though every call throws at runtime (asserted in provider.test.ts).
function _imageModelTypeOnly(provider: ProviderV4): ImageModelV4 {
  return provider.imageModel("m");
}

void _providerIsProviderV4;
void _providerType;
void _languageModel;
void _embeddingModel;
void _rerankingModel;
void _imageModelTypeOnly;

it("has no runtime assertions; see the module-level type checks above", () => {});
