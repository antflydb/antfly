import { readFile, writeFile, stat } from "node:fs/promises";
import type { GovernanceSnapshot, Passage } from "../src/governance/types.ts";
import { validateSnapshot } from "../server/governance-data.ts";

const file = "data/governance/snapshot.json";
const data: GovernanceSnapshot = JSON.parse(await readFile(file, "utf8"));
let atlasFetched =
  data.documents.find((d) => d.id === "atlas-events")?.fetched_at ||
  data.fetched_at;
try {
  atlasFetched = (
    await stat("data/governance/cache/atlas.html")
  ).mtime.toISOString();
} catch (error) {
  if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
}
// Re-running curation does not duplicate the attributed procedural extraction.
data.documents = data.documents.filter((d) => d.id !== "atlas-events");
data.pages = data.pages.filter((p) => p.document_id !== "atlas-events");
data.passages = data.passages.filter((p) => p.document_id !== "atlas-events");
data.documents.push({
  id: "atlas-events",
  title: "Oregon Governance Atlas · HB 2017 procedural events",
  publisher: "Portland Civic Lab · Oregon Governance Atlas",
  source_url: data.atlas_url,
  sha256: data.atlas_sha256,
  fetched_at: atlasFetched,
  published: "2026-09-05",
  version: "Atlas procedural data",
  page_count: data.events.length,
  indexed_pages: data.events.map((_, i) => i + 1),
  extraction: "atlas-structured-data",
  review: "unreviewed",
});
for (const [i, event] of data.events.entries()) {
  const text = `${event.date} · ${event.chamber}\n${event.text}${event.vote ? `\n${event.vote}` : ""}\nSource: ${event.source_url}`;
  const p: Passage = {
    id: `${data.atlas_sha256.slice(0, 16)}-p${i + 1}-0`,
    document_id: "atlas-events",
    page: i + 1,
    start: 0,
    end: text.length,
    text,
  };
  data.pages.push({ document_id: p.document_id, page: p.page, text });
  data.passages.push(p);
  event.evidence = [p.id];
  event.involves = [
    /Governor/.test(event.text)
      ? "entity-governor"
      : /Transportation Preservation/.test(event.text)
        ? "entity-jtpm"
        : event.chamber === "House"
          ? "entity-house"
          : "entity-senate",
  ];
}
function passage(doc: string, needle: string, page?: number) {
  const found = data.passages.find(
    (p) =>
      p.document_id === doc &&
      (!page || p.page === page) &&
      p.text.replace(/\s+/g, " ").includes(needle),
  );
  if (!found)
    throw new Error(`Curated citation no longer resolves: ${doc}: ${needle}`);
  return found.id;
}
const study = passage("introduced", "Department of Transportation shall study");
const deadline = passage("introduced", "September 15, 2019");
const effective = passage("chapter", "Effective date October 6, 2017");
const contrary = passage("summary", "Effective Date: August 6, 2017", 435);
const oversight = passage("chapter", "Joint Legislative Audit", 6);
const house = data.events.find((e) => e.vote && e.chamber === "House")!;
const senate = data.events.find((e) => e.vote && e.chamber === "Senate")!;
data.claims = [
  {
    id: "claim-original",
    question: "What did HB 2017 originally propose?",
    statement:
      "The introduced bill directed ODOT to study improvements to Oregon’s transportation system, report by September 15, 2019, and repeal that study provision on January 2, 2020.",
    evidence: [study, deadline],
    contrary: [],
    involves: ["entity-odot"],
    limitation:
      "This finding describes the introduced text only. Later versions replaced it with a much larger package; it is not a description of the enacted law.",
  },
  {
    id: "claim-effective",
    question: "When did HB 2017 take effect?",
    statement:
      "The enacted chapter records October 6, 2017 as its general effective date. The annual session summary prints August 6, 2017 for the same bill.",
    evidence: [effective],
    contrary: [contrary],
    involves: ["entity-governor"],
    limitation:
      "For this historical date, the pilot relies on the enacted chapter and retains the conflicting summary entry. Individual provisions have separate operative dates. This does not establish the law currently in force.",
  },
  {
    id: "claim-votes",
    question: "What were the floor vote totals?",
    statement:
      "The Atlas’s procedural record reports House passage on July 5 by 39–20, with one excused, and Senate passage on July 6 by 22–7, with one excused.",
    evidence: [...house.evidence, ...senate.evidence],
    contrary: [],
    involves: ["entity-house", "entity-senate"],
    limitation:
      "These totals are attributed to the Atlas’s preserved OLIS extraction and have not been independently checked against the original roll calls in this pilot. They do not establish voting motives or a negotiated coalition.",
  },
  {
    id: "claim-oversight",
    question: "Who was required to report on ODOT audits?",
    statement:
      "Section 18 of chapter 750 requires the Oregon Transportation Commission to report on ODOT audits to the Joint Legislative Audit Committee and the Joint Committee on Transportation at least once each biennium.",
    evidence: [oversight],
    contrary: [],
    involves: ["entity-otc", "entity-odot", "entity-jct", "entity-audit"],
    limitation:
      "A reporting requirement is evidence of legal design. This pilot has not acquired the later reports and cannot establish whether the requirement was fulfilled.",
  },
];
data.gaps = [
  {
    id: "gap-spending",
    question: "How much HB 2017 money was actually spent?",
    reason:
      "The fiscal and revenue statements describe anticipated impacts. They are not expenditure ledgers or reconciled accounts.",
    needed:
      "Agency expenditure records by fiscal year and program, appropriations and allotment changes, and reconciliation to the act’s funding streams.",
    query: "expenditure limitation revenue fiscal impact",
  },
  {
    id: "gap-delivery",
    question: "Did the transportation projects deliver the promised outcomes?",
    reason:
      "Enactment, project authority and predicted revenues do not establish completion, costs or transport outcomes.",
    needed:
      "Dated project delivery reports, audited costs, baseline measures, and outcome evaluations, including contrary findings.",
    query: "Rose Quarter project cost report",
  },
  {
    id: "gap-motives",
    question: "Why did legislators vote for HB 2017?",
    reason:
      "A roll call establishes a recorded vote. It cannot establish motives, negotiations or influence.",
    needed:
      "Contemporaneous testimony, hearing recordings, public statements and documented negotiations, assessed against competing explanations.",
    query: "committee transportation hearing",
  },
  {
    id: "gap-oversight",
    question: "Were the required ODOT audit reports delivered?",
    reason:
      "The chapter establishes a reporting duty, but the later reports and receipt records are outside this pilot’s acquired corpus.",
    needed:
      "Biennial OTC audit reports, committee agendas and minutes, publication dates and any documented gaps in reporting.",
    query: "audits Joint Legislative Audit Committee biennium",
  },
];
const entities = [
  {
    id: "entity-odot",
    title: "Oregon Department of Transportation",
    evidence: [study, oversight],
  },
  {
    id: "entity-otc",
    title: "Oregon Transportation Commission",
    evidence: [oversight],
  },
  {
    id: "entity-jct",
    title: "Joint Committee on Transportation",
    evidence: [oversight],
  },
  {
    id: "entity-audit",
    title: "Joint Legislative Audit Committee",
    evidence: [oversight],
  },
  {
    id: "entity-house",
    title: "Oregon House of Representatives",
    evidence: house.evidence,
  },
  { id: "entity-senate", title: "Oregon Senate", evidence: senate.evidence },
  { id: "entity-governor", title: "Governor of Oregon", evidence: [effective] },
  {
    id: "entity-jtpm",
    title: "Joint Committee on Transportation Preservation and Modernization",
    evidence: data.events.find((e) =>
      /Referred to Transportation/.test(e.text),
    )!.evidence,
  },
];
data.nodes = [
  ...data.documents.map((d) => ({
    id: `doc-${d.id}`,
    kind: "document" as const,
    title: d.title,
    document_id: d.id,
    search_text: `${d.title} ${d.version}`,
    supersedes: d.supersedes ? [`doc-${d.supersedes}`] : [],
  })),
  ...data.passages.map((p) => ({
    id: p.id,
    kind: "passage" as const,
    title: `${data.documents.find((d) => d.id === p.document_id)!.title} · ${p.document_id === "atlas-events" ? "event" : "p."} ${p.page}`,
    search_text: p.text,
    passage_id: p.id,
    document_id: p.document_id,
    evidence: [`doc-${p.document_id}`],
  })),
  ...data.events.map((e) => ({
    id: e.id,
    kind: "event" as const,
    title: `${e.date} · ${e.text}`,
    search_text: `${e.text} ${e.vote || ""}`,
    evidence: e.evidence,
    involves: e.involves,
  })),
  ...data.claims.map((c) => ({
    id: c.id,
    kind: "claim" as const,
    title: c.question,
    search_text: c.statement,
    evidence: c.evidence,
    contrary: c.contrary,
    involves: c.involves,
  })),
  ...entities.map((e) => ({
    ...e,
    kind: "entity" as const,
    search_text: e.title,
  })),
  ...data.gaps.map((g) => ({
    id: g.id,
    kind: "gap" as const,
    title: g.question,
    search_text: `${g.question} ${g.needed}`,
  })),
];
validateSnapshot(data);
await writeFile(file, JSON.stringify(data));
console.log(
  `Curated ${data.claims.length} findings, ${data.gaps.length} open questions and ${data.nodes.length} evidence nodes. All remain awaiting human review.`,
);
