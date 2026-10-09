import type { components } from "../src/public-api.js";

const group: components["schemas"]["InferenceEmbedRequest"] = {
  model: "embeddinggemma2",
  model_identity: "a".repeat(64),
  dimensions: 128,
  input: [{ title: "Access", content: [{ type: "text", text: "Reset password" }] }],
};
const abstained: components["schemas"]["InferenceDecideAnswer"] = {
  name: "route",
  type: "choice",
  choice: null,
  similarity_metric: "cosine",
  prototype_set_hash: "a".repeat(64),
  decision_method: "embedding_similarity",
  similarities: [
    { value: "account", similarity: 0.2 },
    { value: "billing", similarity: 0.2 },
  ],
  margin: 0,
  status: "abstained",
  abstention_reason: "tie",
};
const raw: components["schemas"]["InferenceDecideAnswer"] = {
  name: "tags",
  type: "multi_choice",
  decision_method: "embedding_similarity",
  similarity_metric: "cosine",
  choices: [],
  similarities: [
    { value: "account", similarity: 0.2 },
    { value: "billing", similarity: 0.1 },
  ],
  similarity_thresholds: { account: 0.5, billing: 0.5 },
  margin: 0.3,
  status: "empty",
  prototype_set_hash: "a".repeat(64),
};
void group;
void abstained;
void raw;
