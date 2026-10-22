import { describe, expect, it } from "vitest";
import {
  findIndexedResult,
  getMediaChunks,
  modelsAcceptingInput,
  reconcileModelSelection,
} from "./inference-playground-contracts";

describe("inference playground contracts", () => {
  it("maps reader output by the server-provided input index", () => {
    const results = [
      { index: 2, text: "third" },
      { index: 0, text: "first" },
    ];

    expect(findIndexedResult(results, 0)?.text).toBe("first");
    expect(findIndexedResult(results, 1)).toBeNull();
  });

  it("reads VAD audio chunks from the list response data property", () => {
    const chunks = [{ data: "wav-base64", start_time_ms: 100, end_time_ms: 900 }];

    expect(getMediaChunks({ data: chunks })).toEqual(chunks);
  });

  it("reconciles a removed connection model to the first available model", () => {
    expect(reconcileModelSelection("removed", ["current", "other"])).toBe("current");
    expect(reconcileModelSelection("current", ["current", "other"])).toBe("current");
    expect(reconcileModelSelection("removed", [], "fixed")).toBe("fixed");
  });

  it("filters declared incompatible inputs while preserving older model listings", () => {
    const models = [
      { name: "legacy" },
      { name: "text", inputs: ["text"] },
      { name: "multimodal", inputs: ["text", "image"] },
      { name: "audio", inputs: ["audio"] },
      { name: "declared-empty", inputs: [] },
    ];

    expect(modelsAcceptingInput(models, "text").map((model) => model.name)).toEqual([
      "legacy",
      "text",
      "multimodal",
    ]);
    expect(modelsAcceptingInput(models, "image").map((model) => model.name)).toEqual([
      "legacy",
      "multimodal",
    ]);
    expect(modelsAcceptingInput(models, "audio").map((model) => model.name)).toEqual([
      "legacy",
      "audio",
    ]);
  });
});
