import type { InferenceGatewayOperation } from "@antfly/sdk";

function object(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

/** Apply model selection at the operation's model-bearing envelope. */
export function inferenceGatewayRequest(
  operation: InferenceGatewayOperation,
  json: string,
  selectedModel: string
): Record<string, unknown> {
  const request: unknown = JSON.parse(json);
  if (!object(request)) throw new Error("Request must be a JSON object");
  const model = selectedModel.trim();
  if (operation !== "generate/batch") {
    if (!model) throw new Error("Select a model or enter its canonical ID");
    const generated =
      (operation === "decisions" && request.decision_method === "generated") ||
      (operation === "extract" && request.method === "generated");
    if (generated) {
      if ("model" in request) throw new Error("Generated tasks select generator.model");
      if (!object(request.generator)) throw new Error("Generated tasks require a generator object");
      return { ...request, generator: { ...request.generator, model } };
    }
    return { ...request, model };
  }
  if ("model" in request) throw new Error("Batch models belong in each request body");
  if (!Array.isArray(request.requests) || request.requests.length === 0)
    throw new Error("Batch requests must be a nonempty array");
  return {
    ...request,
    requests: request.requests.map((item, index) => {
      if (!object(item) || !object(item.body))
        throw new Error(`Batch request ${index + 1} must contain a body object`);
      const supplied = item.body.model;
      const itemModel = supplied == null ? model : supplied;
      if (typeof itemModel !== "string" || !itemModel.trim())
        throw new Error(`Batch request ${index + 1} needs a model or a selected default`);
      return { ...item, body: { ...item.body, model: itemModel } };
    }),
  };
}
