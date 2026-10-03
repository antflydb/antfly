import type {
  EmbedderConfig,
  GeneratorConfig,
  QueryRequest,
} from "@antfly/sdk";

export const inferenceUrl = (
  process.env.GOVERNANCE_INFERENCE_URL || "http://127.0.0.1:8091"
).replace(/\/$/, "");
export const extractionModel =
  process.env.GOVERNANCE_EXTRACTION_MODEL || "antflydb/gliner2-base-v1";
export const extractionSchemaVersion = Number(
  process.env.GOVERNANCE_EXTRACTION_SCHEMA_VERSION || "1",
);
if (![1, 2].includes(extractionSchemaVersion))
  throw new Error("GOVERNANCE_EXTRACTION_SCHEMA_VERSION must be 1 or 2");
export const readerModel = "antflydb/Florence-2-base";
// This checkout's SDK predates the runtime's bounded reranker candidate window.
export const governanceReranker: NonNullable<QueryRequest["reranker"]> & {
  candidate_count: number;
} = {
  provider: "antfly",
  model: "antflydb/mxbai-rerank-base-v1",
  url: `${inferenceUrl}/ai/v1`,
  field: "text",
  candidate_count: 32,
};
export const governanceEmbedder: EmbedderConfig = {
  provider: "antfly",
  model: "Qwen/Qwen3-Embedding-0.6B-GGUF",
  api_url: inferenceUrl,
};
export const governanceGenerator: GeneratorConfig = process.env
  .GOVERNANCE_GENERATOR
  ? JSON.parse(process.env.GOVERNANCE_GENERATOR)
  : {
      provider: "antfly",
      model: "ggml-org/gemma-4-E4B-it-GGUF",
      api_url: `${inferenceUrl}/ai/v1`,
      max_tokens: 1024,
      temperature: 0,
    };
export const extractionSchema = {
  entities: [
    "person",
    "agency",
    "committee",
    "legislative measure",
    "funding program",
    "project",
    "place",
  ],
  relations: [
    "administers",
    "reports_to",
    "funds",
    "amends",
    "requires",
    "oversees",
  ].map((type) => ({ type })),
};

export async function inference<T>(route: string, body: unknown): Promise<T> {
  const response = await fetch(`${inferenceUrl}/ai/v1/${route}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(180_000),
  });
  const result = await response.json();
  if (!response.ok)
    throw new Error(
      `Antfly ${route}: ${result.error || response.status}: ${result.message || "inference failed"}`,
    );
  return result as T;
}
