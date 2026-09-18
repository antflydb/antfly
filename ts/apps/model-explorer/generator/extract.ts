import { readdirSync } from "node:fs";
import { join } from "node:path";
import type {
  EnvFlagGate,
  KernelInventoryEntry,
  KernelRoute,
  SourceLink,
} from "../lib/schema/index.ts";
import { lineOf, readRepoFile, repoRoot } from "./lib.ts";
import { requireUnique } from "./merge.ts";

const NODE_ZIG = "zig/lib/ml/src/graph/node.zig";
const COMPILER_ZIG = "zig/pkg/inference/src/graph/quant_kernel_compiler.zig";
const KERNELS_M = "zig/pkg/inference/src/backends/metal_kernels.m";
const GENERATED_DIR = "zig/pkg/inference/src/ops/metal/generated";
const INFERENCE_SRC = "zig/pkg/inference/src";

export interface OpKindEntry {
  name: string;
  group: "primitive" | "fused";
  source: SourceLink;
}

/** Parse PrimitiveOp + FusedOp enum members from node.zig. */
export function extractOpKinds(): OpKindEntry[] {
  const content = readRepoFile(NODE_ZIG);
  const out: OpKindEntry[] = [];
  for (const [enumName, group] of [
    ["PrimitiveOp", "primitive"],
    ["FusedOp", "fused"],
  ] as const) {
    const start = content.indexOf(`pub const ${enumName} = enum`);
    if (start < 0) throw new Error(`enum ${enumName} not found in ${NODE_ZIG}`);
    const bodyStart = content.indexOf("{", start);
    const bodyEnd = content.indexOf("\n};", bodyStart);
    if (bodyEnd < 0) throw new Error(`unterminated enum ${enumName}`);
    const body = content.slice(bodyStart, bodyEnd);
    const memberRe = /^[ \t]+([a-z][a-z0-9_]*),[ \t]*(?:\/\/[^\n]*)?$/gm;
    for (const m of body.matchAll(memberRe)) {
      out.push({
        name: m[1],
        group,
        source: { path: NODE_ZIG, line: lineOf(content, bodyStart + m.index), anchor: `${m[1]},` },
      });
    }
  }
  requireUnique(
    out.map((op) => `${op.group}/${op.name}`),
    "op vocabulary"
  );
  if (out.length < 50) throw new Error(`suspiciously few op kinds parsed: ${out.length}`);
  return out;
}

/** Parse metal_production_schedules struct-literal rows. */
export function extractSchedules(): KernelRoute[] {
  return parseSchedules(readRepoFile(COMPILER_ZIG));
}

export function parseSchedules(content: string): KernelRoute[] {
  const tableStart = content.indexOf("pub const metal_production_schedules");
  if (tableStart < 0) throw new Error(`metal_production_schedules not found in ${COMPILER_ZIG}`);
  const tableEnd = content.indexOf("\n};", tableStart);
  if (tableEnd < 0) throw new Error(`unterminated metal_production_schedules`);
  const table = content.slice(tableStart, tableEnd);
  const rowRe =
    /\.\{\s*\.format\s*=\s*\.(\w+),\s*\.row_bucket\s*=\s*\.(\w+),\s*\.epilogue\s*=\s*\.(\w+),\s*\.schedule\s*=\s*\.\{\s*([^}]*)\}\s*\}/g;
  const routes: KernelRoute[] = [];
  for (const m of table.matchAll(rowRe)) {
    const [, format, rowBucket, epilogue, schedBody] = m;
    const sched: Record<string, number | string | boolean> = {};
    for (const fieldMatch of schedBody.matchAll(/\.(\w+)\s*=\s*\.?([\w]+)\s*(?:,|$)/g)) {
      const [, key, raw] = fieldMatch;
      sched[key] = /^\d+$/.test(raw)
        ? Number(raw)
        : raw === "true"
          ? true
          : raw === "false"
            ? false
            : raw;
    }
    if (Object.keys(sched).length !== [...schedBody.matchAll(/\.\w+\s*=/g)].length) {
      throw new Error(`unsupported schedule expression in ${format}/${rowBucket}/${epilogue}`);
    }
    const known = new Set([
      "threads_per_threadgroup",
      "cols_per_threadgroup",
      "rows_per_threadgroup",
      "reduction",
      "key_chunk",
      "skip_rescale",
    ]);
    const extra = Object.fromEntries(Object.entries(sched).filter(([key]) => !known.has(key)));
    routes.push({
      id: `${format}/${rowBucket}/${epilogue}`,
      format,
      rowBucket,
      epilogue,
      schedule: {
        threadsPerThreadgroup: sched.threads_per_threadgroup as number | undefined,
        colsPerThreadgroup: sched.cols_per_threadgroup as number | undefined,
        rowsPerThreadgroup: sched.rows_per_threadgroup as number | undefined,
        reduction: sched.reduction as string | undefined,
        keyChunk: sched.key_chunk as number | undefined,
        skipRescale: sched.skip_rescale as boolean | undefined,
        ...(Object.keys(extra).length ? { extra } : {}),
      },
      generated: true,
      generatedFile: guessGeneratedFile(format, epilogue),
      source: {
        path: COMPILER_ZIG,
        line: lineOf(content, tableStart + m.index),
        anchor: `.format = .${format}, .row_bucket = .${rowBucket}, .epilogue = .${epilogue}`,
      },
    });
  }
  const expectedRows = [...table.matchAll(/\.format\s*=/g)].length;
  if (routes.length === 0 || routes.length !== expectedRows) {
    throw new Error(
      `schedule extraction incomplete: parsed ${routes.length} of ${expectedRows} rows`
    );
  }
  requireUnique(
    routes.map((route) => route.id),
    "kernel routes"
  );
  return routes;
}

function guessGeneratedFile(format: string, epilogue: string): string | undefined {
  const suffix = epilogue === "none" ? "" : `_${epilogue}`;
  const candidate = `quant_kernel_${format}_small_batch${suffix}.metal`;
  try {
    const files = readdirSync(join(repoRoot, GENERATED_DIR));
    return files.includes(candidate) ? `${GENERATED_DIR}/${candidate}` : undefined;
  } catch {
    return undefined;
  }
}

const FAMILY_RULES: Array<[RegExp, KernelInventoryEntry["family"]]> = [
  [/(lora|adamw|training|_bwd|backward|bce|grad)/, "training"],
  [/moe/, "moe"],
  [/(gliner|deberta|disentangled)/, "gliner"],
  [/(florence|vision|conv2d|conv1d|window_pack|window_unpack|channel_scores)/, "vision"],
  [/(attention|sdpa|paged)/, "attention"],
  [/_mm(_|$)|_mm_sg/, "mm_sg"],
  [/(pair_activation|pair_linear|head_rms_rope|rms_norm_add|post_residual|pair_)/, "fusion"],
  [/(sample|argmax|top8|topk|lm_head)/, "sampling"],
  [/(kv|polar4|turbo3|compressed_attention|_key$|_key_)/, "kv"],
  [/(rms|norm|rope)/, "norm_rope"],
  [/(cpy|get_rows|set_rows|transpose|concat|slice|broadcast|convert|embed|dtype)/, "data_movement"],
  [/linear/, "matvec"],
];

function classifyKernel(name: string): KernelInventoryEntry["family"] {
  for (const [re, family] of FAMILY_RULES) if (re.test(name)) return family;
  return "other";
}

/**
 * Blank out `//` and `/* *\/` comments with spaces so declaration scans cannot
 * match commented-out code. Offsets and newlines are preserved for lineOf().
 */
function blankComments(src: string): string {
  return src
    .replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, " "))
    .replace(/\/\/[^\n]*/g, (m) => " ".repeat(m.length));
}

/** Scan metal_kernels.m + generated/*.metal for `kernel void <name>`. */
export function extractKernelInventory(): KernelInventoryEntry[] {
  const seen = new Map<string, KernelInventoryEntry>();

  const mContent = readRepoFile(KERNELS_M);
  // Markers live inside comments, so locate them on the raw text; match
  // kernel declarations on the comment-blanked text (same offsets).
  const beginMarker = mContent.indexOf("quant-kernel-codegen:begin generated quant kernels");
  const endMarker = mContent.indexOf("quant-kernel-codegen:end generated quant kernels");
  if ((beginMarker >= 0) !== (endMarker >= 0) || (beginMarker >= 0 && endMarker < beginMarker)) {
    throw new Error(`unpaired quant-kernel-codegen markers in ${KERNELS_M}`);
  }
  const kernelRe = /kernel void ([a-zA-Z0-9_]+)/g;
  for (const m of blankComments(mContent).matchAll(kernelRe)) {
    const name = m[1];
    if (seen.has(name)) continue;
    const inGenerated = beginMarker >= 0 && m.index > beginMarker && m.index < endMarker;
    seen.set(name, {
      name,
      family: classifyKernel(name),
      generated: inGenerated,
      source: { path: KERNELS_M, line: lineOf(mContent, m.index), anchor: `kernel void ${name}` },
    });
  }

  for (const file of readdirSync(join(repoRoot, GENERATED_DIR)).sort()) {
    if (!file.endsWith(".metal")) continue;
    const rel = `${GENERATED_DIR}/${file}`;
    const content = readRepoFile(rel);
    const re = /kernel void ([a-zA-Z0-9_]+)/g;
    for (const g of blankComments(content).matchAll(re)) {
      const name = g[1];
      const entry: KernelInventoryEntry = {
        name,
        family: classifyKernel(name),
        generated: true,
        source: { path: rel, line: lineOf(content, g.index), anchor: `kernel void ${name}` },
      };
      // Prefer the generated-file location for generated kernels.
      seen.set(name, entry);
    }
  }

  const inventory = [...seen.values()].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  if (inventory.length < 300)
    throw new Error(`suspiciously few kernels found: ${inventory.length}`);
  return inventory;
}

function inferFlagKind(name: string): EnvFlagGate["kind"] {
  if (/_MB$/.test(name)) return "mb";
  if (/_BYTES$/.test(name)) return "bytes";
  if (/_CAPACITY$/.test(name)) return "int";
  if (/(_COUNT|_THREADS|_CHUNK|_MIN_KV|_SIZE|_LIMIT|_MAX|_ROWS|_COLS)$/.test(name)) return "int";
  if (/(DISABLE|ENABLE|FORCE|STRICT|TRACE|DEBUG)/.test(name)) return "bool";
  return "unknown";
}

/** Textual TERMITE_/ANTFLY_ references, including comments/tests; kinds are inferred from names. */
export function extractEnvFlags(): EnvFlagGate[] {
  const byName = new Map<string, { links: SourceLink[]; count: number }>();
  const walk = (relDir: string) => {
    for (const entry of readdirSync(join(repoRoot, relDir), { withFileTypes: true }).sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0
    )) {
      const rel = `${relDir}/${entry.name}`;
      if (entry.isDirectory()) {
        if (entry.name === "generated") continue;
        walk(rel);
      } else if (entry.name.endsWith(".zig") || entry.name.endsWith(".m")) {
        const content = readRepoFile(rel);
        // Both-side boundaries: prefix string literals like
        // "TERMITE_METAL_DISABLE_" must not register (truncated or otherwise).
        const re = /(?<![A-Z0-9_])(?:TERMITE|ANTFLY)_[A-Z0-9][A-Z0-9_]{2,}[A-Z0-9](?![A-Z0-9_])/g;
        for (const m of content.matchAll(re)) {
          const name = m[0];
          const rec = byName.get(name) ?? { links: [], count: 0 };
          rec.count++;
          if (rec.links.length < 5) {
            rec.links.push({ path: rel, line: lineOf(content, m.index), anchor: name });
          }
          byName.set(name, rec);
        }
      }
    }
  };
  walk(INFERENCE_SRC);
  const flags = [...byName.entries()]
    .map(([name, rec]) => ({
      name,
      kind: inferFlagKind(name),
      occurrences: rec.count,
      sources: rec.links,
    }))
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  if (flags.length < 200) throw new Error(`suspiciously few env flags found: ${flags.length}`);
  return flags;
}
