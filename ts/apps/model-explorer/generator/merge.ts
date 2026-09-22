import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { z } from "zod";
import {
  type EnvFlagGate,
  type KernelInventoryEntry,
  type KernelRoute,
  ModelSpec,
  SCHEMA_VERSION,
  SourceLink,
  type Stage,
} from "../lib/schema/index.ts";
import { appRoot, verifySourceLink } from "./lib.ts";

const CURATED_DIR = join(appRoot, "data", "curated");

interface MergeContext {
  opKinds: Set<string>;
  kernels: Set<string>;
  routes: Set<string>;
  flags: Set<string>;
  gitCommit: string;
  generatedAt: string;
  warnings: string[];
}

export function buildMergeContext(
  opKinds: { name: string }[],
  inventory: KernelInventoryEntry[],
  routes: KernelRoute[],
  flags: EnvFlagGate[],
  gitCommit: string,
  generatedAt: string
): MergeContext {
  return {
    opKinds: new Set(opKinds.map((o) => o.name)),
    kernels: new Set(inventory.map((k) => k.name)),
    routes: new Set(routes.map((r) => r.id)),
    flags: new Set(flags.map((f) => f.name)),
    gitCommit,
    generatedAt,
    warnings: [],
  };
}

function healLink(link: SourceLink | undefined, ctx: MergeContext, where: string): void {
  if (!link) return;
  try {
    const result = verifySourceLink(link);
    if (result?.healed) {
      if (link.line !== undefined)
        ctx.warnings.push(
          `healed anchor for ${where}: ${link.path}:${link.line} -> :${result.line}`
        );
      if (link.endLine !== undefined && link.line !== undefined)
        link.endLine += result.line - link.line;
      link.line = result.line;
    }
  } catch (err) {
    throw new Error(`${where}: ${(err as Error).message}`);
  }
}

/**
 * A curated model file is a ModelSpec minus `schemaVersion`/`sources`, or a
 * family file with `variants` (one topology emitted per variant with
 * stat/repeat overrides).
 */
export function mergeCuratedModels(ctx: MergeContext): ModelSpec[] {
  if (!existsSync(CURATED_DIR)) return [];
  const specs: ModelSpec[] = [];
  for (const file of readdirSync(CURATED_DIR).sort()) {
    if (!file.endsWith(".model.json")) continue;
    let raw: Record<string, unknown> & { variants?: Array<Record<string, unknown>> };
    try {
      raw = JSON.parse(readFileSync(join(CURATED_DIR, file), "utf8"));
    } catch (err) {
      throw new Error(`${file}: ${(err as Error).message}`);
    }
    const variants: Array<Record<string, unknown>> = raw.variants ?? [
      { id: raw.id, displayName: raw.displayName, tagline: raw.tagline, stats: raw.stats },
    ];
    for (const variant of variants) {
      const candidate = structuredClone({
        schemaVersion: SCHEMA_VERSION,
        id: variant.id,
        family: raw.family,
        displayName: variant.displayName ?? raw.displayName,
        tagline: variant.tagline ?? raw.tagline,
        stats: { ...(raw.stats ?? {}), ...((variant.stats as object) ?? {}) },
        stages: raw.stages,
        graphs: raw.graphs,
        sankey: raw.sankey,
        sources: { gitCommit: ctx.gitCommit, generatedAt: ctx.generatedAt },
      });
      const repeatOverrides = (variant.repeatOverrides ?? {}) as Record<string, number>;
      const stageOverrides = (variant.stageOverrides ?? {}) as Record<string, Partial<Stage>>;
      const stageIds = new Set((candidate.stages as Stage[]).map((stage) => stage.id));
      for (const id of [...Object.keys(repeatOverrides), ...Object.keys(stageOverrides)]) {
        if (!stageIds.has(id))
          throw new Error(`${file}/${String(variant.id)}: unknown stage override "${id}"`);
      }
      for (const stage of candidate.stages as Stage[]) {
        const override = stageOverrides[stage.id];
        if (override) {
          const repeat = override.repeat ? { ...stage.repeat, ...override.repeat } : stage.repeat;
          Object.assign(stage, override, { id: stage.id, repeat });
        }
        const count = repeatOverrides[stage.id];
        if (count !== undefined && stage.repeat) stage.repeat.count = count;
      }
      let spec: ModelSpec;
      try {
        spec = ModelSpec.parse(candidate);
      } catch (err) {
        throw new Error(`${file}/${String(variant.id)}: ${(err as Error).message}`);
      }
      validateSpec(spec, ctx, file);
      specs.push(spec);
    }
  }
  requireUnique(
    specs.map((spec) => spec.id),
    "model IDs"
  );
  return specs;
}

export function requireUnique(ids: string[], where: string): void {
  const seen = new Set<string>();
  for (const id of ids) {
    if (seen.has(id)) throw new Error(`${where}: duplicate ID "${id}"`);
    seen.add(id);
  }
}

export function validateSpec(spec: ModelSpec, ctx: MergeContext, file: string): void {
  requireUnique(
    spec.stages.map((stage) => stage.id),
    `${file}/stages`
  );
  const stageIds = new Set(spec.stages.map((s) => s.id));
  for (const stage of spec.stages) {
    healLink(stage.source, ctx, `${file}/${stage.id}`);
    if (stage.repeat?.variants) {
      const count = stage.repeat.count;
      requireUnique(
        stage.repeat.variants.map((variant) => variant.tag),
        `${file}/${stage.id}/variants`
      );
      for (const variant of stage.repeat.variants) {
        requireUnique(variant.layerIdxs.map(String), `${file}/${stage.id}/${variant.tag}/layers`);
        if (variant.layerIdxs.some((layer) => layer >= count)) {
          throw new Error(`${file}/${stage.id}/${variant.tag}: layer index outside repeat count`);
        }
      }
    }
  }

  for (const [phase, graph] of Object.entries(spec.graphs)) {
    if (!graph) continue;
    requireUnique(
      graph.nodes.map((node) => node.id),
      `${file}/${phase}/nodes`
    );
    requireUnique(
      graph.edges.map((edge) => edge.id),
      `${file}/${phase}/edges`
    );
    const nodeIds = new Set(graph.nodes.map((n) => n.id));
    for (const node of graph.nodes) {
      const where = `${file}/${phase}/${node.id}`;
      if (!ctx.opKinds.has(node.opKind)) {
        throw new Error(`${where}: unknown opKind "${node.opKind}"`);
      }
      if (!stageIds.has(node.stageId)) {
        throw new Error(`${where}: unknown stageId "${node.stageId}"`);
      }
      for (const k of node.kernels) {
        if (!ctx.kernels.has(k)) throw new Error(`${where}: unknown kernel "${k}"`);
      }
      for (const r of node.kernelRouteIds) {
        if (!ctx.routes.has(r)) throw new Error(`${where}: unknown kernel route "${r}"`);
      }
      for (const f of node.envFlagNames) {
        if (!ctx.flags.has(f)) throw new Error(`${where}: env flag not found in scan: "${f}"`);
      }
      healLink(node.source, ctx, where);
      healLink(node.lowererSource, ctx, `${where} (lowerer)`);
    }
    for (const edge of graph.edges) {
      if (!nodeIds.has(edge.from) || !nodeIds.has(edge.to)) {
        throw new Error(
          `${file}/${phase}/${edge.id}: edge endpoint missing (${edge.from} -> ${edge.to})`
        );
      }
    }
  }
  if (spec.sankey) {
    const sankey = spec.sankey;
    requireUnique(
      spec.sankey.nodes.map((node) => node.id),
      `${file}/sankey/nodes`
    );
    const ids = new Set(spec.sankey.nodes.map((node) => node.id));
    for (const node of spec.sankey.nodes) {
      if (node.stageId && !stageIds.has(node.stageId))
        throw new Error(`${file}/sankey: unknown stage "${node.stageId}"`);
    }
    for (const link of spec.sankey.links) {
      if (!ids.has(link.source) || !ids.has(link.target))
        throw new Error(`${file}/sankey: edge endpoint missing (${link.source} -> ${link.target})`);
    }
    const visiting = new Set<string>();
    const visited = new Set<string>();
    const visit = (id: string) => {
      if (visiting.has(id)) throw new Error(`${file}/sankey: circular link involving "${id}"`);
      if (visited.has(id)) return;
      visiting.add(id);
      for (const link of sankey.links) if (link.source === id) visit(link.target);
      visiting.delete(id);
      visited.add(id);
    };
    for (const id of ids) visit(id);
  }
}

/**
 * data/curated/links.json: a registry of named SourceLinks used by TSX
 * content ({ "id": { path, anchor, line? } }). Anchors are verified and
 * line numbers healed, so prose links never rot.
 */
export function mergeNamedLinks(ctx: MergeContext): Record<string, SourceLink> {
  const file = join(CURATED_DIR, "links.json");
  if (!existsSync(file)) return {};
  const raw = z.record(z.string(), SourceLink).parse(JSON.parse(readFileSync(file, "utf8")));
  for (const [id, link] of Object.entries(raw)) {
    healLink(link, ctx, `links.json/${id}`);
  }
  return raw;
}

/** Collect every SourceLink in the emitted data, for the snippet cache. */
export function collectLinks(values: unknown[]): SourceLink[] {
  const out: SourceLink[] = [];
  const visit = (v: unknown): void => {
    if (Array.isArray(v)) {
      for (const item of v) visit(item);
    } else if (v && typeof v === "object") {
      const obj = v as Record<string, unknown>;
      if (typeof obj.path === "string" && (obj.path as string).startsWith("zig/")) {
        out.push(obj as unknown as SourceLink);
      } else {
        for (const value of Object.values(obj)) visit(value);
      }
    }
  };
  visit(values);
  return out;
}
