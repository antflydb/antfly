import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import type { GovernanceSnapshot } from "../src/governance/types.ts";

export const EVIDENCE = "oregon_governance_evidence";
export const GOVERNANCE_META = "oregon_governance_meta";
export const GRAPH = "decision_graph";
export const EMBEDDINGS = "qwen3_evidence_v1";
export const relations = [
  "evidence",
  "contrary",
  "involves",
  "supersedes",
] as const;
export async function readSnapshot() {
  const raw = await readFile("data/governance/snapshot.json", "utf8");
  const data = JSON.parse(raw) as GovernanceSnapshot;
  if (data.version !== 1 || !data.passages.length || !data.claims.length)
    throw new Error(
      "Run governance:snapshot and governance:curate before ingestion.",
    );
  validateSnapshot(data);
  return {
    data,
    id: createHash("sha256").update(raw).digest("hex").slice(0, 16),
  };
}
export function validateSnapshot(data: GovernanceSnapshot) {
  const docs = new Map(data.documents.map((d) => [d.id, d]));
  const pages = new Map(
    data.pages.map((p) => [`${p.document_id}:${p.page}`, p]),
  );
  const passages = new Set(data.passages.map((p) => p.id));
  if (passages.size !== data.passages.length)
    throw new Error("Duplicate passage IDs");
  for (const p of data.passages) {
    const doc = docs.get(p.document_id);
    const page = pages.get(`${p.document_id}:${p.page}`);
    if (
      !doc ||
      !page ||
      page.text.slice(p.start, p.end) !== p.text ||
      p.id !== `${doc.sha256.slice(0, 16)}-p${p.page}-${p.start}`
    )
      throw new Error(`Invalid source locator: ${p.id}`);
  }
  for (const claim of data.claims)
    for (const id of [...claim.evidence, ...claim.contrary])
      if (!passages.has(id)) throw new Error(`Missing claim evidence: ${id}`);
  const nodes = new Set(data.nodes.map((n) => n.id));
  if (nodes.size !== data.nodes.length)
    throw new Error("Duplicate graph node IDs");
  for (const node of data.nodes)
    for (const relation of relations)
      for (const target of node[relation] || [])
        if (!nodes.has(target))
          throw new Error(
            `Dangling ${relation} edge from ${node.id}: ${target}`,
          );
}
