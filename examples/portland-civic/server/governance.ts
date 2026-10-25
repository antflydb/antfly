import { Router } from "express";
import { resolve } from "node:path";
import { diffLines } from "diff";
import type { QueryRequest } from "@antfly/sdk";
import type {
  GovernanceManifest,
  EvidenceSearch,
  PassageHit,
} from "../src/governance/types.ts";
import { client, exists, queryAntfly } from "./antfly.ts";
import {
  governanceEmbedder,
  governanceGenerator,
  governanceReranker,
  readerModel,
  extractionModel,
} from "./governance-models.ts";
import { generateBrief } from "./governance-agent.ts";
const briefs = new Map<
  string,
  { expires: number; result: ReturnType<typeof generateBrief> }
>();
import { InputError } from "./search.ts";
import {
  EVIDENCE,
  GOVERNANCE_META,
  EMBEDDINGS,
  GRAPH,
  readSnapshot,
} from "./governance-data.ts";

export const governance = Router();
let snapshot: ReturnType<typeof readSnapshot> | undefined;
const local = () =>
  (snapshot ||= readSnapshot().catch((error) => {
    snapshot = undefined;
    throw error;
  }));
async function state() {
  const [{ data, id }, current, indexes] = await Promise.all([
    local(),
    client.tables.lookup(GOVERNANCE_META, "current"),
    client.indexes.list(EVIDENCE),
  ]);
  if (current?.snapshot_id !== id)
    throw new Error(
      "Legislative snapshot changed or is not ingested. Run pnpm governance:ingest and restart the app.",
    );
  const semantic = indexes.find((i) => i.config.name === EMBEDDINGS);
  const graph = indexes.find((i) => i.config.name === GRAPH);
  return {
    data,
    id,
    semantic: Boolean(semantic),
    semantic_ready: isIndexReady(semantic),
    graph_ready: isIndexReady(graph),
  };
}
function isIndexReady(index: { status: unknown } | undefined) {
  return Boolean(
    (index?.status as { readiness?: { complete?: boolean } } | undefined)
      ?.readiness?.complete,
  );
}
const string = (value: unknown, max = 500) => {
  if (value === undefined) return "";
  if (typeof value !== "string" || value.length > max)
    throw new InputError("Invalid query parameter");
  return value.trim();
};
async function search(
  params: Record<string, unknown>,
): Promise<EvidenceSearch> {
  const { data, id, semantic_ready } = await state();
  const q = string(params.q),
    doc = string(params.document),
    mode = string(params.mode) || "keyword";
  if (!["keyword", "hybrid"].includes(mode))
    throw new InputError("Unknown search mode");
  if (mode === "hybrid" && !semantic_ready)
    throw new InputError("Semantic index is not ready. Use keyword search.");
  if (doc && !data.documents.some((d) => d.id === doc))
    throw new InputError("Unknown document");
  const conjuncts = [
    { term: id, field: "snapshot_id" },
    {
      disjuncts: [
        { term: "passage", field: "kind" },
        { term: "caption", field: "kind" },
      ],
    },
  ];
  if (doc) conjuncts.push({ term: doc, field: "document_id" });
  const query: QueryRequest = {
    table: EVIDENCE,
    filter_query: { conjuncts },
    full_text_search: q
      ? { match: q, field: "search_text" }
      : { match_all: {} },
    ...(mode === "hybrid" && q
      ? { semantic_search: q, indexes: [EMBEDDINGS] }
      : {}),
    limit: 12,
  };
  const start = performance.now();
  const result = await queryAntfly(query, {
    signal: AbortSignal.timeout(30_000),
  });
  if (!result || result.error || result.status >= 400)
    throw new Error(result?.error || "Evidence search failed");
  return {
    hits: (result.hits?.hits || [])
      .filter((h) => h._source?.kind !== "caption")
      .map((h) => ({ ...h._source, score: h._score }) as unknown as PassageHit),
    captions: (result.hits?.hits || [])
      .filter((h) => h._source?.kind === "caption")
      .map((h) => ({
        document_id: String(h._source!.document_id),
        page: Number(h._source!.page),
        caption: String(h._source!.caption),
        score: h._score || 0,
      })),
    mode: mode as EvidenceSearch["mode"],
    took: Math.round(performance.now() - start),
  };
}
governance.get("/manifest", async (_req, res) => {
  const { data, id, ...status } = await state();
  res.json({
    snapshot_id: id,
    documents: data.documents,
    events: data.events,
    claims: data.claims,
    gaps: data.gaps,
    nodes: data.nodes.filter((n) => n.kind !== "passage"),
    passage_count: data.passages.length,
    fetched_at: data.fetched_at,
    atlas_url: data.atlas_url,
    ...status,
  } satisfies GovernanceManifest);
});
governance.get("/search", async (req, res) =>
  res.json(await search(req.query)),
);
governance.get("/pipeline", async (_req, res) => {
  const current = await state();
  const extraction = await client.tables
    .lookup(GOVERNANCE_META, "extraction")
    .catch(() => null);
  const graphIndexes = (await exists("oregon_governance_mentions_v1"))
    ? await client.indexes.list("oregon_governance_mentions_v1")
    : [];
  res.json({
    snapshot_id: current.id,
    reader: {
      model: readerModel,
      native_pages: current.data.pages.filter(
        (p) =>
          current.data.documents.find((d) => d.id === p.document_id)
            ?.extraction === "pdf-text" &&
          (!p.extraction || p.extraction === "native-text"),
      ).length,
      ocr_pages: current.data.pages.filter(
        (p) => p.extraction === "florence-ocr",
      ).length,
      unreadable_pages: current.data.pages.filter(
        (p) => p.extraction === "unreadable",
      ).length,
    },
    embeddings: {
      model: governanceEmbedder.model,
      ready: current.semantic_ready,
    },
    generator: {
      model: governanceGenerator.model,
      provider: governanceGenerator.provider,
    },
    reranker: {
      model: governanceReranker.model,
      candidate_count: governanceReranker.candidate_count,
    },
    extraction:
      extraction?.snapshot_id === current.id
        ? extraction
        : { state: "not-started", model: extractionModel },
    autograph_ready:
      extraction?.snapshot_id === current.id &&
      extraction.state === "extracted" &&
      isIndexReady(graphIndexes.find((i) => i.config.name === "autograph_v1")),
  });
});
governance.get("/autograph", async (req, res) => {
  const { data, id } = await state();
  const passage = string(req.query.passage);
  if (!data.passages.some((p) => p.id === passage))
    throw new InputError("Unknown passage");
  const extraction = await client.tables
    .lookup(GOVERNANCE_META, "extraction")
    .catch(() => null);
  if (extraction?.snapshot_id !== id || extraction.state !== "extracted") {
    res.status(503).json({
      error:
        extraction?.error ||
        `Native ${extractionModel} extraction is not complete for this snapshot.`,
    });
    return;
  }
  const result = await queryAntfly({
    table: "oregon_governance_mentions_v1",
    graph_queries: {
      mentions: {
        index: "autograph_v1",
        traverse: {
          start: { keys: [`${id}:${passage}`] },
          direction: "out",
          max_depth: 1,
          limit: 30,
          include_paths: true,
          include_documents: true,
        },
      },
    },
  });
  if (!result || result.error)
    throw new Error(
      result?.error || "Antfly returned no automatic graph result",
    );
  const record = await client.tables.lookup(
    "oregon_governance_mentions_v1",
    `${id}:${passage}`,
  );
  const extracted = record?.extraction as
    | { entities?: { id: string; text: string }[] }
    | undefined;
  const relations = (record?.candidate_relations || []) as {
    type: string;
    score: number;
    source: { id: string };
    target: { id: string };
  }[];
  res.json({
    graph:
      result.graph_results?.mentions?.kind === "nodes"
        ? result.graph_results.mentions
        : undefined,
    relations: relations.map((r) => ({
      type: r.type,
      subject: extracted?.entities?.find((e) => e.id === r.source.id)?.text,
      object: extracted?.entities?.find((e) => e.id === r.target.id)?.text,
      score: r.score,
    })),
    review: "Machine-extracted candidate relationships; unreviewed",
    snapshot_id: id,
  });
});
governance.post("/brief", async (req, res) => {
  const question = string(req.body?.question);
  if (!question) throw new InputError("Enter a question");
  const { data, id, semantic_ready } = await state();
  const candidates = await search({
    q: question,
    mode: semantic_ready ? "hybrid" : "keyword",
  });
  const key = JSON.stringify([
    id,
    question,
    governanceGenerator,
    semantic_ready,
  ]);
  let entry = briefs.get(key);
  if (!entry || entry.expires < Date.now()) {
    if (briefs.size >= 32) briefs.delete(briefs.keys().next().value!);
    const result = generateBrief(
      question,
      id,
      data.passages,
      candidates.hits,
      semantic_ready,
    );
    entry = { expires: Date.now() + 15 * 60_000, result };
    briefs.set(key, entry);
    result.then(
      (answer) => {
        if (answer.citation_validation === "rejected") briefs.delete(key);
      },
      () => briefs.delete(key),
    );
  }
  res.json(await entry.result);
});
governance.get("/passage/:id", async (req, res) => {
  const { data } = await local();
  const passage = data.passages.find((p) => p.id === req.params.id);
  if (!passage) {
    res.status(404).json({ error: "Unknown passage" });
    return;
  }
  res.json({
    passage,
    document: data.documents.find((d) => d.id === passage.document_id),
    page: data.pages.find(
      (p) => p.document_id === passage.document_id && p.page === passage.page,
    ),
  });
});
governance.get("/sources/:hash.pdf", async (req, res) => {
  const { data } = await local();
  const doc = data.documents.find(
    (d) => d.sha256 === req.params.hash && d.extraction === "pdf-text",
  );
  if (!doc) {
    res.status(404).json({ error: "Unknown preserved source" });
    return;
  }
  res.set("Cache-Control", "public, max-age=31536000, immutable");
  res.sendFile(resolve(`data/governance/sources/${doc.sha256}.pdf`));
});
governance.get("/compare", async (req, res) => {
  const { data } = await local();
  const left = string(req.query.left) || "introduced",
    right = string(req.query.right) || "engrossed";
  const allowed = ["introduced", "engrossed", "enrolled", "chapter"];
  if (!allowed.includes(left) || !allowed.includes(right))
    throw new InputError("Select two bill versions");
  const lp = Number(req.query.left_page || 1),
    rp = Number(req.query.right_page || 1);
  const page = (id: string, n: number) =>
    data.pages.find((p) => p.document_id === id && p.page === n);
  const a = page(left, lp),
    b = page(right, rp);
  if (!a || !b) throw new InputError("Page not found");
  res.json({
    left: a,
    right: b,
    changes: diffLines(a.text, b.text),
    citations: [a, b].map(
      (p) =>
        data.passages.find(
          (s) => s.document_id === p.document_id && s.page === p.page,
        )?.id,
    ),
  });
});
governance.get("/graph", async (req, res) => {
  const { data, id, graph_ready } = await state();
  const node = string(req.query.node) || "claim-effective";
  if (!data.nodes.some((n) => n.id === node))
    throw new InputError("Unknown evidence node");
  if (!graph_ready) throw new Error("Relationship index is still building");
  const result = await queryAntfly(
    {
      table: EVIDENCE,
      limit: 10,
      graph_queries: {
        connections: {
          index: GRAPH,
          traverse: {
            start: { keys: [`${id}:${node}`] },
            direction: "out",
            max_depth: 2,
            limit: 30,
            include_paths: true,
            include_documents: true,
          },
        },
      },
    },
    { signal: AbortSignal.timeout(30_000) },
  );
  if (!result || result.error || result.status >= 400)
    throw new Error(result?.error || "Graph query failed");
  const graphResult = result.graph_results?.connections;
  const graph = graphResult?.kind === "nodes" ? graphResult : undefined;
  if (!graph) throw new Error("Antfly returned no graph result");
  res.json({
    root: data.nodes.find((n) => n.id === node),
    graph,
    snapshot_id: id,
  });
});
