import { describe, expect, it } from "vitest";
import type {
  Chunk,
  ChunkConfig,
  ChunkResponse,
  ContentPart,
  EmbedInput,
  EmbedResponse,
  ModelsResponse,
  RerankResponse,
  TermiteConfig,
  VersionResponse,
} from "../src/index.js";
import { logLevels, logStyles } from "../src/index.js";

describe("Type exports", () => {
  it("should export TermiteConfig type", () => {
    const config: TermiteConfig = {
      baseUrl: "http://localhost:8080/api",
      headers: { "X-Custom-Header": "value" },
    };
    expect(config.baseUrl).toBe("http://localhost:8080/api");
  });

  it("should export EmbedInput type supporting all formats", () => {
    // Single string
    const input1: EmbedInput = "hello world";
    expect(typeof input1).toBe("string");

    // Array of strings
    const input2: EmbedInput = ["hello", "world"];
    expect(Array.isArray(input2)).toBe(true);

    // Array of content parts
    const input3: EmbedInput = [
      { type: "text", text: "hello" },
      { type: "image_url", image_url: { url: "data:image/png;base64,..." } },
    ];
    expect(Array.isArray(input3)).toBe(true);
  });

  it("should export EmbedResponse type", () => {
    const response: EmbedResponse = {
      model: "bge-small-en-v1.5",
      embeddings: [
        [0.1, 0.2, 0.3],
        [0.4, 0.5, 0.6],
      ],
    };
    expect(response.model).toBe("bge-small-en-v1.5");
    expect(response.embeddings.length).toBe(2);
  });

  it("should export ChunkConfig type", () => {
    const config: ChunkConfig = {
      model: "fixed",
      target_tokens: 500,
      overlap_tokens: 50,
      separator: "\n\n",
      max_chunks: 100,
      threshold: 0.5,
    };
    expect(config.model).toBe("fixed");
    expect(config.target_tokens).toBe(500);
  });

  it("should export Chunk type", () => {
    const chunk: Chunk = {
      id: 0,
      text: "This is a chunk",
      start_char: 0,
      end_char: 15,
    };
    expect(chunk.id).toBe(0);
    expect(chunk.text).toBe("This is a chunk");
  });

  it("should export ChunkResponse type", () => {
    const response: ChunkResponse = {
      chunks: [{ id: 0, text: "chunk 1", start_char: 0, end_char: 7 }],
      model: "fixed",
      cache_hit: false,
    };
    expect(response.model).toBe("fixed");
    expect(response.cache_hit).toBe(false);
  });

  it("should export RerankResponse type", () => {
    const response: RerankResponse = {
      model: "bge-reranker-v2-m3",
      scores: [0.9, 0.7, 0.3],
    };
    expect(response.scores.length).toBe(3);
  });

  it("should export ModelsResponse type", () => {
    const response: ModelsResponse = {
      embedders: ["bge-small-en-v1.5"],
      chunkers: ["fixed"],
      rerankers: ["bge-reranker-v2-m3"],
    };
    expect(response.embedders).toContain("bge-small-en-v1.5");
    expect(response.chunkers).toContain("fixed");
  });

  it("should export VersionResponse type", () => {
    const response: VersionResponse = {
      version: "v1.0.0",
      git_commit: "abc1234",
      build_time: "2024-01-15T10:30:00Z",
      go_version: "go1.21.0",
    };
    expect(response.version).toBe("v1.0.0");
  });

  it("should export ContentPart type", () => {
    const textPart: ContentPart = { type: "text", text: "hello" };
    const imagePart: ContentPart = {
      type: "image_url",
      image_url: { url: "data:image/png;base64,..." },
    };
    expect(textPart.type).toBe("text");
    expect(imagePart.type).toBe("image_url");
  });
});

describe("Constant exports", () => {
  it("should export logLevels array", () => {
    expect(logLevels).toEqual(["debug", "info", "warn", "error"]);
  });

  it("should export logStyles array", () => {
    expect(logStyles).toEqual(["terminal", "json", "logfmt", "noop"]);
  });
});
