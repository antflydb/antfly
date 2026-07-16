import type { Connection } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import { transformConnectionModels } from "./use-inference-registry";

describe("transformConnectionModels", () => {
  it("keeps provider identity when two connections expose the same model", () => {
    const connection = (id: string, displayName: string): Connection => ({
      id,
      name: id,
      display_name: displayName,
      kind: "inference",
      status: "connected",
      capabilities: [],
      sources: [],
      inference: {
        provider: "antfly",
        models: { other: [{ name: "acme/chat" }] },
      },
    });

    const models = transformConnectionModels([
      connection("local-inference", "Local inference"),
      connection("shared-inference", "Antfly shared inference"),
    ]);

    expect(models).toHaveLength(2);
    expect(models.map((model) => model.connectionId)).toEqual([
      "local-inference",
      "shared-inference",
    ]);
    expect(new Set(models.map((model) => model.id)).size).toBe(2);
  });

  it("keeps the provider model ID separate from its display name and removes duplicates", () => {
    const models = transformConnectionModels([
      {
        id: "shared-inference",
        name: "shared-inference",
        kind: "inference",
        status: "connected",
        capabilities: [],
        sources: [],
        inference: {
          provider: "antfly",
          models: {
            generators: [{ name: "acme/chat", display_name: "Acme Chat" }],
            other: [{ name: "acme/chat", display_name: "Acme Chat" }],
          },
        },
      },
    ]);

    expect(models).toHaveLength(1);
    expect(models[0]).toMatchObject({ name: "Acme Chat", source: "acme/chat" });
  });
});
