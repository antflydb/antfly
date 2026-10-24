import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile, stat, rename } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { load } from "cheerio";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";
import { createRequire } from "node:module";
import { readPage, textQuality } from "../server/governance-extraction.ts";
import type {
  GovernanceSnapshot,
  SourceDocument,
} from "../src/governance/types.ts";

const root = "data/governance";
await mkdir(`${root}/sources`, { recursive: true });
await mkdir(`${root}/cache`, { recursive: true });
const atlasUrl =
  "https://oregon.portlandciviclab.org/decisions/dc-2017-hb2017-transportation";
const sha = (data: string | Buffer) =>
  createHash("sha256").update(data).digest("hex");
// Cache downloads for repeatable extraction. Delete the cache explicitly to refresh originals.
async function download(id: string, url: string) {
  const path = `${root}/cache/${id}`;
  try {
    return await readFile(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  execFileSync("curl", [
    "--fail",
    "--location",
    "--retry",
    "2",
    "--max-time",
    "360",
    "--silent",
    "--show-error",
    url,
    "-o",
    `${path}.partial`,
  ]);
  await rename(`${path}.partial`, path);
  return readFile(path);
}
const atlas = await download("atlas.html", atlasUrl);
const $ = load(atlas.toString());
const snapshot: GovernanceSnapshot = {
  version: 1,
  fetched_at: new Date().toISOString(),
  atlas_url: atlasUrl,
  atlas_sha256: sha(atlas),
  documents: [],
  pages: [],
  passages: [],
  events: [],
  claims: [],
  gaps: [],
  nodes: [],
};
const bill =
  "https://olis.oregonlegislature.gov/liz/2017R1/Downloads/MeasureDocument/HB2017/";
const analysis =
  "https://olis.oregonlegislature.gov/liz/2017R1/Downloads/MeasureAnalysisDocument/";
const specs = [
  {
    id: "introduced",
    title: "HB 2017 · Introduced",
    url: bill + "Introduced",
    date: "2017-03-01",
    version: "Introduced",
  },
  {
    id: "engrossed",
    title: "HB 2017 · A-Engrossed",
    url: bill + "A-Engrossed",
    date: "2017-07-04",
    version: "A-Engrossed",
    supersedes: "introduced",
  },
  {
    id: "enrolled",
    title: "HB 2017 · Enrolled",
    url: bill + "Enrolled",
    date: "2017-07-10",
    version: "Enrolled",
    supersedes: "engrossed",
  },
  {
    id: "chapter",
    title: "Oregon Laws 2017 · Chapter 750",
    url: "https://www.oregonlegislature.gov/bills_laws/lawsstatutes/2017orlaw0750.pdf",
    date: "2017-08-22",
    version: "Enacted chapter",
    supersedes: "enrolled",
  },
  {
    id: "fiscal",
    title: "HB 2017-A · Fiscal impact",
    url: analysis + "39868",
    date: "2017-07-03",
    version: "Fiscal estimate",
  },
  {
    id: "staff",
    title: "HB 2017-A · Staff measure summary",
    url: analysis + "39989",
    date: "2017-07-06",
    version: "Staff summary",
  },
  {
    id: "revenue",
    title: "HB 2017-A · Revenue impact",
    url: analysis + "40077",
    date: "2017-07-01",
    version: "Revenue estimate",
  },
  {
    id: "summary",
    title: "2017 Summary of Legislation · HB 2017 entry",
    url: "https://www.oregonlegislature.gov/lpro/summleg/2017%20-%20Summary%20of%20Legislation.pdf",
    date: "2017",
    version: "Session summary",
  },
];
for (const spec of specs) {
  const bytes = await download(`${spec.id}.pdf`, spec.url);
  if (!bytes.subarray(0, 1024).toString().includes("%PDF-"))
    throw new Error(`${spec.id} did not return a PDF`);
  const checksum = sha(bytes);
  await writeFile(`${root}/sources/${checksum}.pdf`, bytes);
  const task = getDocument({
    data: new Uint8Array(bytes),
    useSystemFonts: true,
  });
  const pdf = await task.promise;
  const doc: SourceDocument = {
    id: spec.id,
    title: spec.title,
    publisher: "Oregon Legislative Assembly",
    source_url: spec.url,
    sha256: checksum,
    fetched_at: (
      await stat(`${root}/cache/${spec.id}.pdf`)
    ).mtime.toISOString(),
    published: spec.date,
    version: spec.version,
    supersedes: spec.supersedes,
    page_count: pdf.numPages,
    indexed_pages: [],
    extraction: "pdf-text",
    review: "unreviewed",
  };
  const atlasLink = $("#evidence a")
    .filter((_, el) => $(el).attr("href") === spec.url)
    .first();
  const hash = atlasLink
    .parent()
    .text()
    .match(/SHA-256\s*([a-f0-9]{64})/)?.[1];
  if (hash) {
    doc.atlas_sha256 = hash;
    doc.atlas_hash_matches = hash === checksum;
  }
  const pages = [];
  for (let n = 1; n <= pdf.numPages; n++) {
    const page = await pdf.getPage(n);
    const content = await page.getTextContent();
    let text = "",
      lastY: number | undefined;
    for (const item of content.items) {
      if (!("str" in item) || !item.str.trim()) continue;
      const y = Math.round(item.transform[5]);
      text += (text ? (lastY !== y ? "\n" : " ") : "") + item.str;
      lastY = item.hasEOL ? undefined : y;
    }
    text = text.trim();
    // This bounded pilot selects summary pages by their native measure reference.
    // Preserve the whole PDF, but do not OCR hundreds of unrelated summary pages.
    if (spec.id === "summary" && !/(?:HB|House Bill)\s*2017\b/i.test(text)) {
      page.cleanup();
      continue;
    }
    let extracted: Awaited<ReturnType<typeof readPage>> = {
      text,
      extraction: "native-text",
      quality_flags: [],
    };
    if (textQuality(text).length) {
      const require = createRequire(import.meta.url);
      const canvasModule = require(
        require.resolve("@napi-rs/canvas", {
          paths: [require.resolve("pdfjs-dist/package.json")],
        }),
      );
      const viewport = page.getViewport({ scale: 150 / 72 });
      if (viewport.width * viewport.height > 20_000_000)
        throw new Error(`Page render exceeds pixel budget: ${spec.id} ${n}`);
      const canvas = canvasModule.createCanvas(
        Math.ceil(viewport.width),
        Math.ceil(viewport.height),
      );
      await page.render({ canvas, viewport }).promise;
      extracted = await readPage(canvas.toBuffer("image/png"), text);
    }
    pages.push({ document_id: spec.id, page: n, ...extracted });
    page.cleanup();
  }
  // Preserve the entire annual publication, but index only pages mentioning this measure.
  const selected =
    spec.id === "summary"
      ? pages.filter((p) => /(?:HB|House Bill)\s*2017\b/i.test(p.text))
      : pages;
  for (const page of selected) {
    doc.indexed_pages.push(page.page);
    snapshot.pages.push(page);
    // Sentence/line boundaries within 900 characters, with overlap for retrieval context.
    for (let start = 0; start < page.text.length; ) {
      let end = Math.min(start + 900, page.text.length);
      if (end < page.text.length) {
        const boundary = page.text.lastIndexOf("\n", end);
        if (boundary > start + 450) end = boundary;
      }
      const text = page.text.slice(start, end);
      snapshot.passages.push({
        id: `${checksum.slice(0, 16)}-p${page.page}-${start}`,
        document_id: spec.id,
        page: page.page,
        start,
        end,
        text,
      });
      if (end === page.text.length) break;
      // Start on a whole line within the previous chunk, never before start.
      const overlap = page.text.indexOf("\n", Math.max(start + 1, end - 160));
      start = overlap >= 0 && overlap < end ? overlap + 1 : end;
    }
  }
  snapshot.documents.push(doc);
  await task.destroy();
  console.log(
    `${spec.id}: ${doc.page_count} preserved pages, ${selected.length} indexed, SHA-256 ${checksum.slice(0, 16)}`,
  );
}
// Only structured procedural data is reused from the Atlas; its authored narrative is not copied.
$("#journey details > ol > li[id]").each((_, el) => {
  const spans = $(el).children("span");
  const label = spans.eq(0).text();
  const month = { Mar: "03", May: "05", Jun: "06", Jul: "07", Aug: "08" }[
    label.slice(0, 3)
  ];
  if (!month) throw new Error(`Unexpected event date ${label}`);
  const action = spans.eq(2).clone();
  const vote = action.find("span").text().trim();
  action.children().remove();
  snapshot.events.push({
    id: $(el).attr("id")!,
    date: `2017-${month}-${label.split(" ")[1].padStart(2, "0")}`,
    chamber: spans.eq(1).text(),
    text: action.text().trim(),
    vote: vote || undefined,
    source_url: `${atlasUrl}#${$(el).attr("id")}`,
    evidence: [],
    involves: [],
  });
});
if (snapshot.events.length !== 20)
  throw new Error(
    "Atlas timeline changed; review the importer before publishing",
  );
await writeFile(`${root}/snapshot.json.partial`, JSON.stringify(snapshot));
await rename(`${root}/snapshot.json.partial`, `${root}/snapshot.json`);
console.log(
  `Saved ${snapshot.passages.length} passages and ${snapshot.events.length} Atlas events. Run governance:curate next.`,
);
