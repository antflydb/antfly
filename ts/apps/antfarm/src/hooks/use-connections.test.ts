/**
 * Unit tests for liveModelSuggestions
 */
import type { Connection } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import { liveModelSuggestions } from "./use-connections";

function providerConnection(overrides: Partial<Connection> = {}): Connection {
  return {
    name: "openai",
    kind: "inference_provider",
    status: "connected",
    inference_provider: {
      provider: "openai",
      models: {
        embedders: [{ name: "text-embedding-3-small" }],
        other: [{ name: "gpt-4o" }],
      },
    },
    ...overrides,
  };
}

describe("liveModelSuggestions", () => {
  it("merges the kind bucket with the other bucket", () => {
    const suggestions = liveModelSuggestions([providerConnection()], "embedder");
    expect(suggestions.openai).toEqual(["text-embedding-3-small", "gpt-4o"]);
  });

  it("ignores providers that are not connected", () => {
    const suggestions = liveModelSuggestions([providerConnection({ status: "error" })], "embedder");
    expect(suggestions.openai).toBeUndefined();
  });

  it("ignores connections without model expansions", () => {
    const suggestions = liveModelSuggestions(
      [
        providerConnection({
          inference_provider: { provider: "openai" },
        }),
      ],
      "embedder"
    );
    expect(suggestions).toEqual({});
  });

  it("dedupes models across instances of the same provider type", () => {
    const first = providerConnection();
    const second = providerConnection({
      name: "openai-2",
      inference_provider: {
        provider: "openai",
        models: {
          embedders: [{ name: "text-embedding-3-small" }, { name: "text-embedding-3-large" }],
        },
      },
    });
    const suggestions = liveModelSuggestions([first, second], "embedder");
    expect(suggestions.openai).toEqual([
      "text-embedding-3-small",
      "gpt-4o",
      "text-embedding-3-large",
    ]);
  });

  it("returns generator models for the generator kind", () => {
    const connection = providerConnection({
      name: "claude",
      inference_provider: {
        provider: "anthropic",
        models: {
          generators: [{ name: "claude-sonnet-4-5" }],
        },
      },
    });
    const suggestions = liveModelSuggestions([connection], "generator");
    expect(suggestions.anthropic).toEqual(["claude-sonnet-4-5"]);
  });
});
