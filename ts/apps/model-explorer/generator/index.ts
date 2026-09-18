/**
 * Model-explorer data generator.
 *
 * Extracts ground truth from the Zig runtime (op vocabulary, kernel
 * inventory, production schedules, env flags), merges hand-curated model
 * graphs from data/curated/, verifies + self-heals file:line anchors, and
 * emits validated JSON into data/generated/.
 *
 *   pnpm generate    — regenerate in place
 *   pnpm gen:check   — regenerate in memory and diff (exit 1 on drift)
 */
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  type FrameScenario,
  KernelsFile,
  Manifest,
  SCHEMA_VERSION,
} from "../lib/schema/index.ts";
import {
  extractEnvFlags,
  extractKernelInventory,
  extractOpKinds,
  extractSchedules,
} from "./extract.ts";
import { validateFrame } from "./frames.ts";
import { appRoot, gitCommit, toJson, verifySourceRevision } from "./lib.ts";
import { buildMergeContext, collectLinks, mergeCuratedModels, mergeNamedLinks } from "./merge.ts";
import { buildSnippets } from "./snippets.ts";

const OUT_DIR = join(appRoot, "data", "generated");
/**
 * Per-file size budgets for the generated snapshot. These guard repository
 * weight, not client payload: pages ship only the snippets they cite, via
 * `snippetsFor`, so the whole map never reaches a browser.
 *
 * snippets.json scales with the number of curated anchors, which is the point
 * of the app, so it carries its own allowance rather than making every build
 * warn. Lower it again if the anchor set is ever trimmed.
 */
const SIZE_WARN_BYTES = 800 * 1024;
const SIZE_WARN_OVERRIDES: Record<string, number> = {
  "snippets.json": 1400 * 1024,
};
const budgetFor = (name: string) => SIZE_WARN_OVERRIDES[name] ?? SIZE_WARN_BYTES;
const checkMode = process.argv.includes("--check");
const refIndex = process.argv.indexOf("--source-ref");
const sourceRef = refIndex >= 0 ? process.argv[refIndex + 1] : "HEAD";
if (!sourceRef || sourceRef.startsWith("--"))
  throw new Error("--source-ref requires a Git revision");
if (checkMode && refIndex >= 0)
  throw new Error(
    "--source-ref applies to generation; --check verifies the recorded source revision"
  );

async function generate(): Promise<Map<string, string>> {
  // Retain the recorded revision in check mode, and verify its source bytes.
  // Unrelated app commits need no refresh; changed runtime source does.
  const saved =
    checkMode && existsSync(join(OUT_DIR, "manifest.json"))
      ? Manifest.parse(JSON.parse(readFileSync(join(OUT_DIR, "manifest.json"), "utf8")))
      : undefined;
  const commit = saved?.gitCommit ?? gitCommit(sourceRef);
  const generatedAt = saved?.generatedAt ?? new Date().toISOString();

  const opKinds = extractOpKinds();
  const routes = extractSchedules();
  const inventory = extractKernelInventory();
  const flags = extractEnvFlags();

  const ctx = buildMergeContext(opKinds, inventory, routes, flags, commit, generatedAt);
  const models = mergeCuratedModels(ctx);
  const namedLinks = mergeNamedLinks(ctx);
  for (const warning of ctx.warnings) console.warn(`  warn: ${warning}`);

  const files = new Map<string, string>();
  files.set("op-kinds.json", toJson({ schemaVersion: SCHEMA_VERSION, opKinds }));
  files.set(
    "kernels.json",
    toJson(KernelsFile.parse({ schemaVersion: SCHEMA_VERSION, routes, inventory }))
  );
  files.set("links.json", toJson({ schemaVersion: SCHEMA_VERSION, links: namedLinks }));
  for (const spec of models) {
    files.set(`models/${spec.id}.json`, toJson(spec));
  }

  // Invalid curated frames must fail before static export, just like graphs.
  const framesDir = join(appRoot, "data", "curated", "frames");
  const frames: FrameScenario[] = [];
  if (existsSync(framesDir)) {
    for (const file of readdirSync(framesDir).sort()) {
      if (file.endsWith(".json")) {
        const frame = validateFrame(
          JSON.parse(readFileSync(join(framesDir, file), "utf8")),
          inventory
        );
        if (!models.some((model) => model.id === frame.modelId))
          throw new Error(`${file}: model is not generated`);
        if (frames.some((other) => other.id === frame.id))
          throw new Error(`${file}: duplicate frame ID ${frame.id}`);
        frames.push(frame);
        files.set(`frames/${file}`, toJson(frame));
      }
    }
  }

  // Snippets only for links the UI shows hover peeks for (model graphs +
  // schedule rows) — the full kernel/flag inventories link out without peeks.
  const links = collectLinks([routes, models, frames, Object.values(namedLinks)]);
  const snippets = await buildSnippets(links);
  files.set("snippets.json", toJson({ schemaVersion: SCHEMA_VERSION, snippets }));
  verifySourceRevision(commit);

  files.set(
    "manifest.json",
    toJson({
      schemaVersion: SCHEMA_VERSION,
      gitCommit: commit,
      generatedAt,
      permalinkBase: "https://github.com/antflydb/antfly/blob",
      models: models.map((m) => m.id),
      counts: {
        opKinds: opKinds.length,
        kernels: inventory.length,
        routes: routes.length,
        snippets: Object.keys(snippets).length,
      },
    })
  );

  for (const [name, content] of files) {
    const budget = budgetFor(name);
    if (Buffer.byteLength(content) > budget) {
      console.warn(
        `  warn: ${name} is ${(Buffer.byteLength(content) / 1024).toFixed(0)} KB (budget ${(budget / 1024).toFixed(0)} KB)`
      );
    }
  }
  return files;
}

function check(files: Map<string, string>): number {
  let drift = 0;
  for (const [name, content] of files) {
    const target = join(OUT_DIR, name);
    if (!existsSync(target)) {
      console.error(`MISSING  ${name}`);
      drift++;
      continue;
    }
    const existing = readFileSync(target, "utf8");
    let same: boolean;
    try {
      same = JSON.stringify(JSON.parse(existing)) === JSON.stringify(JSON.parse(content));
    } catch {
      // A corrupted on-disk file is drift, not a crash.
      same = false;
    }
    if (!same) {
      console.error(`DRIFT    ${name}`);
      drift++;
    }
  }
  // Extra files on disk that would no longer be generated are drift too.
  const walk = (dir: string, prefix: string) => {
    if (!existsSync(dir)) return;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.name.startsWith(".")) continue; // .DS_Store and editor droppings
      const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) walk(join(dir, entry.name), rel);
      else if (!files.has(rel)) {
        console.error(`STALE    ${rel}`);
        drift++;
      }
    }
  };
  walk(OUT_DIR, "");
  return drift;
}

const files = await generate();

if (checkMode) {
  const drift = check(files);
  if (drift > 0) {
    console.error(`\ngen:check: ${drift} file(s) drifted — run \`pnpm generate\` and review.`);
    process.exit(1);
  }
  console.log(`gen:check: ${files.size} files match.`);
} else {
  rmSync(OUT_DIR, { recursive: true, force: true });
  for (const [name, content] of files) {
    const target = join(OUT_DIR, name);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, content);
  }
  console.log(`generated ${files.size} files into data/generated/`);
}
