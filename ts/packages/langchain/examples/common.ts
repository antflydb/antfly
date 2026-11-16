import { AntflyEmbeddings } from "@antfly/langchain";
import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { Document } from "@langchain/core/documents";

export const fixture = [
  new Document({
    id: "apples",
    pageContent: "Apples are stored in the cool orchard warehouse.",
    metadata: { source: "orchard-handbook", section: "storage" },
  }),
  new Document({
    id: "owner",
    pageContent: "Mira Chen owns operations and handles shipment escalation.",
    metadata: { source: "orchard-handbook", section: "operations" },
  }),
  new Document({
    id: "shipment",
    pageContent: "The Q3 shipment target is 120 crates.",
    metadata: { source: "orchard-handbook", section: "shipments" },
  }),
];
export function environment() {
  const baseUrl = process.env.ANTFLY_URL ?? "http://127.0.0.1:8080";
  const model = process.env.ANTFLY_EMBEDDING_MODEL;
  if (!model)
    throw new Error(
      "Set ANTFLY_EMBEDDING_MODEL to an installed text embedding model; no model is downloaded implicitly"
    );
  const headers: Record<string, string> = process.env.ANTFLY_TOKEN
    ? { Authorization: `Bearer ${process.env.ANTFLY_TOKEN}` }
    : {};
  const client = new AntflyClient({ baseUrl, headers });
  const inference = new InferenceClient({ baseUrl, headers });
  const embeddings = new AntflyEmbeddings({ client: inference, model });
  return { client, inference, embeddings };
}
