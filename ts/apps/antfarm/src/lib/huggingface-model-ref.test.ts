import { describe, expect, it } from "vitest";
import { huggingFaceRepoId } from "./huggingface-model-ref";

describe("huggingFaceRepoId", () => {
  it.each([
    ["antflydb/mxbai-rerank-base-v1", "antflydb/mxbai-rerank-base-v1"],
    ["Qwen/Qwen3-Embedding-0.6B-GGUF:q8-0-bundle-v1", "Qwen/Qwen3-Embedding-0.6B-GGUF"],
    [
      "google/gemma-4-E2B-it-qat-q4_0-gguf:gguf@675cff42a74c774d6cb76f76d8eacb49b48c9b93",
      "google/gemma-4-E2B-it-qat-q4_0-gguf",
    ],
    ["antflydb/clipclap:gguf:Q4_K", "antflydb/clipclap"],
  ])("normalizes %s", (model, expected) => {
    expect(huggingFaceRepoId(model)).toBe(expected);
  });

  it.each([
    "fixed",
    "/local/model",
    "https://example.com/model",
    "owner/repo:a:b:c",
  ])("rejects non-Hugging Face model reference %s", (model) => {
    expect(huggingFaceRepoId(model)).toBeNull();
  });
});
