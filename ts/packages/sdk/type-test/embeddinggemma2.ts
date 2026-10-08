import type { components } from "../src/public-api.js";

const group: components["schemas"]["InferenceEmbedRequest"] = {
  model: "embeddinggemma2",
  model_identity: "a".repeat(64),
  dimensions: 128,
  input: [{ title: "Access", content: [{ type: "text", text: "Reset password" }] }],
};
const abstained: components["schemas"]["InferenceDecideAnswer"] = {
  type: "choice", choice: null, decision_method: "embedding_similarity",
  similarities: { account: 0.2, billing: 0.2 }, margin: 0,
  status: "abstained", abstention_reason: "tie",
};
const raw: components["schemas"]["ExtractionDecision"] = {
  name: "tags", mode: "multi", decision_method: "embedding_similarity",
  labels: ["account"], similarities: { account: 0.7, billing: 0.1 }, status: "selected",
};
void group;
void abstained;
void raw;
