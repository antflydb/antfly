import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { readSnapshot } from "../server/governance-data.ts";
import { extractionModel } from "../server/governance-models.ts";
import type { EvidenceBrief } from "../src/governance/types.ts";
const base = process.env.DEMO_URL || "http://127.0.0.1:3007";
async function api(path: string, init?: RequestInit) {
  const response = await fetch(`${base}/api/governance/${path}`, {
    ...init,
    signal: AbortSignal.timeout(300_000),
  });
  assert.ok(
    response.ok,
    `${path}: HTTP ${response.status}: ${response.ok ? "" : await response.text()}`,
  );
  return response.json();
}
const { data } = await readSnapshot();
const report: unknown[] = [];
const manifest = await api("manifest");
assert.ok(
  manifest.semantic_ready,
  "Qwen enrichment must complete before evaluating semantic answers",
);
for (const [question, expected] of [
  ["When did HB 2017 take effect?", "conflicting-sources"],
  [
    "What effective date is given for Oregon's 2017 transportation package?",
    "conflicting-sources",
  ],
  ["How much HB 2017 money was actually spent?", "not-established"],
]) {
  const answer = (await api("brief", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
  })) as EvidenceBrief;
  report.push({ question, answer });
  await writeFile(
    "data/governance/cache/generation-eval.json",
    JSON.stringify(report, null, 2),
  );
  assert.equal(answer.curated, false);
  assert.ok(answer.generator);
  assert.equal(answer.citation_validation, "valid", question);
  assert.equal(answer.status, expected, question);
  if (expected === "conflicting-sources") {
    const quotes = [...answer.evidence, ...answer.contrary];
    assert.ok(quotes.some((p) => p.document_id === "chapter" && p.page === 98));
    assert.ok(
      quotes.some((p) => p.document_id === "summary" && p.page === 435),
    );
    assert.match(answer.statement, /October 6, 2017/);
    assert.match(answer.statement, /August 6, 2017/);
  } else assert.deepEqual(answer.evidence, []);
  for (const passage of [...answer.evidence, ...answer.contrary]) {
    const source = await api(`passage/${passage.id}`);
    assert.equal(
      source.page.text.slice(passage.start, passage.end),
      passage.text,
    );
  }
  console.log(`PASS generated answer: ${question}`);
}
for (const mode of ["keyword", "hybrid"]) {
  const result = await api(
    `search?${new URLSearchParams({ q: "internal auditor", document: "chapter", mode })}`,
  );
  assert.ok(result.hits.some((p: { page: number }) => [5, 6].includes(p.page)));
  assert.ok(
    result.hits.every(
      (p: { document_id: string }) => p.document_id === "chapter",
    ),
  );
}
const graph = await api("graph?node=claim-effective");
assert.ok(
  graph.graph.nodes.some(
    (n: { document: { document_id?: string } }) =>
      n.document.document_id === "summary",
  ),
);
const pipeline = await api("pipeline");
assert.equal(pipeline.extraction.model, extractionModel);
console.log(
  `Generated date conflict, paraphrase, abstention, exact citations, keyword/hybrid filters and maintained graph traversal passed against ${data.passages.length} passages. Native automatic extraction: ${pipeline.extraction.state}. This is a bounded smoke evaluation, not an independent research-quality benchmark.`,
);
