/**
 * Unit tests for liveModelSuggestions
 */
import type { Connection } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import { liveModelSuggestions } from "./use-connections";

function providerConnection(overrides: Partial<Connection> = {}): Connection {
  return {
    id: "openai",
    name: "openai",
    kind: "inference",
    status: "connected",
    capabilities: ["models.embed", "models.generate"],
    inference: {
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
  it("does not merge unclassified models into embedder suggestions", () => {
    const suggestions = liveModelSuggestions([providerConnection()], "embedder");
    expect(suggestions.openai).toEqual(["text-embedding-3-small"]);
  });

  it("ignores providers that are not connected", () => {
    const suggestions = liveModelSuggestions([providerConnection({ status: "error" })], "embedder");
    expect(suggestions.openai).toBeUndefined();
  });

  it("ignores connections without model expansions", () => {
    const suggestions = liveModelSuggestions(
      [
        providerConnection({
          inference: { provider: "openai" },
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
      inference: {
        provider: "openai",
        models: {
          embedders: [{ name: "text-embedding-3-small" }, { name: "text-embedding-3-large" }],
        },
      },
    });
    const suggestions = liveModelSuggestions([first, second], "embedder");
    expect(suggestions.openai).toEqual(["text-embedding-3-small", "text-embedding-3-large"]);
  });

  it("returns generator models for the generator kind", () => {
    const connection = providerConnection({
      name: "claude",
      inference: {
        provider: "anthropic",
        models: {
          generators: [{ name: "claude-sonnet-4-5" }],
        },
      },
    });
    const suggestions = liveModelSuggestions([connection], "generator");
    expect(suggestions.anthropic).toEqual(["claude-sonnet-4-5"]);
  });

  it("merges unclassified models into generator suggestions", () => {
    const suggestions = liveModelSuggestions([providerConnection()], "generator");
    expect(suggestions.openai).toEqual(["gpt-4o"]);
  });
});
