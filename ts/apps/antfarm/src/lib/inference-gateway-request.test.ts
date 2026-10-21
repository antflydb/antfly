import { describe, expect, it } from "vitest";
import { inferenceGatewayRequest } from "./inference-gateway-request";

describe("gateway request model selection", () => {
  const first = { custom_id: "first", body: { model: "generators/first", messages: [] } };
  const second = { custom_id: "second", body: { model: "chains/second", messages: [] } };
  it("preserves mixed models without requiring a selected model", () => {
    const batch = { mode: "sync", requests: [first, second] };
    expect(inferenceGatewayRequest("generate/batch", JSON.stringify(batch), "")).toEqual(batch);
    expect(inferenceGatewayRequest("generate/batch", JSON.stringify(batch), "unused")).toEqual(
      batch
    );
  });
  it("uses selection only for missing per-item models and keeps envelope metadata", () => {
    expect(
      inferenceGatewayRequest(
        "generate/batch",
        JSON.stringify({ requests: [first, { custom_id: "default", body: { messages: [] } }] }),
        " generators/default "
      )
    ).toEqual({
      requests: [
        first,
        { custom_id: "default", body: { messages: [], model: "generators/default" } },
      ],
    });
  });
  it("rejects unusable batch envelopes before submission", () => {
    for (const request of [
      [],
      {},
      { requests: [] },
      { requests: [{}] },
      { requests: [{ body: {} }] },
      { model: "invalid", requests: [first] },
    ]) {
      expect(() =>
        inferenceGatewayRequest("generate/batch", JSON.stringify(request), "")
      ).toThrow();
    }
  });
  it.each([
    ["decisions", "decision_method"],
    ["extract", "method"],
  ] as const)("selects generator.model for generated %s", (operation, field) => {
    const request = {
      [field]: "generated",
      generator: { provider: "antfly", model: "old", max_tokens: 40 },
      input: "hello",
    };
    expect(inferenceGatewayRequest(operation, JSON.stringify(request), " generators/new ")).toEqual(
      {
        ...request,
        generator: { ...request.generator, model: "generators/new" },
      }
    );
    for (const invalid of [
      { ...request, model: "ambiguous" },
      { [field]: "generated" },
      { ...request, generator: [] },
    ]) {
      expect(() =>
        inferenceGatewayRequest(operation, JSON.stringify(invalid), "generators/new")
      ).toThrow();
    }
    expect(
      inferenceGatewayRequest(
        operation,
        JSON.stringify({ [field]: "model", input: "hello" }),
        "named/task"
      )
    ).toEqual({ [field]: "model", input: "hello", model: "named/task" });
  });
  it("applies selection to ordinary operations", () => {
    expect(inferenceGatewayRequest("generate", '{"messages":[]}', "generators/default")).toEqual({
      messages: [],
      model: "generators/default",
    });
    expect(() => inferenceGatewayRequest("generate", '{"messages":[]}', " ")).toThrow();
  });
});
