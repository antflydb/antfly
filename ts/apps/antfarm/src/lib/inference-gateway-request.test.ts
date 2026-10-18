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
  it("applies selection to ordinary operations", () => {
    expect(inferenceGatewayRequest("generate", '{"messages":[]}', "generators/default")).toEqual({
      messages: [],
      model: "generators/default",
    });
    expect(() => inferenceGatewayRequest("generate", '{"messages":[]}', " ")).toThrow();
  });
});
