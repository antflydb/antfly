import {
  InferenceClient,
  type InferenceRequest,
  type LayaRequestV2,
  type InferenceState,
  type ClassificationTask,
} from "../src/index.js";
async function contracts(client: InferenceClient) {
  const v1 = await client.run({
    schema_version: 1,
    model: "local",
    text: "John works at Apple",
    labels: ["person"],
  });
  const v1Version: 1 = v1.value.schema_version;
  const question: LayaRequestV2 = {
    schema_version: 2,
    model: "laya",
    inputs: [{ content: "Need search?" }],
    schema: {
      classifications: [
        {
          name: "search",
          labels: ["false", "true"],
          mode: "boolean",
          instruction: "Need a search?",
        },
      ],
    },
  };
  const laya = await client.run(question);
  const row = laya.value.data[0];
  if ("decisions" in row)
    for (const decision of row.decisions) {
      if (decision.type === "boolean") {
        const probability: number = decision.true_probability;
        void probability;
      }
    }
  const validation = await client.validateRequest(question);
  const valid: true = validation.value.valid;
  const tokens: number = validation.value.encoded_tokens;
  // @ts-expect-error Validation results do not contain inference outputs.
  validation.value.data;
  // @ts-expect-error A v1 request requires text.
  const missing: InferenceRequest = { schema_version: 1, model: "local" };
  // @ts-expect-error Classification mode must be a supported wire value.
  const invalidMode: ClassificationTask["mode"] = "unknown";
  // @ts-expect-error Future schemas must use the explicit extension method.
  client.run({ schema_version: 3, model: "future", future_feature: true });
  // @ts-expect-error Model is a readonly accessor.
  client.model = null;
  const state: InferenceState = client.state;
  // @ts-expect-error State snapshots are readonly.
  state.status = "running";
  // The explicit extension method retains forward extensibility.
  await client.runExtension({
    schema_version: 3,
    model: "future",
    future_feature: { enabled: true },
  });
  void [v1Version, valid, tokens, missing, invalidMode];
}
void contracts;
