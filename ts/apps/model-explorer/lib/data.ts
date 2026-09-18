// Guard: this module carries the full generated data layer (snippets,
// env flags, kernels) — importing it from a client component would put
// ~2 MB of JSON plus zod parsing into the browser bundle.
import "server-only";
import envFlagsJson from "@/data/generated/env-flags.json";
import kernelsJson from "@/data/generated/kernels.json";
import linksJson from "@/data/generated/links.json";
import manifestJson from "@/data/generated/manifest.json";
import opKindsJson from "@/data/generated/op-kinds.json";
import snippetsJson from "@/data/generated/snippets.json";
import { L } from "@/lib/links";
import { EnvFlagsFile, KernelsFile, Manifest, type SourceLink } from "@/lib/schema";

// Single implementation lives in lib/links.ts (client-safe); re-exported here
// so server code keeps one import site and the two can never drift.
export { L };

export const manifest = Manifest.parse(manifestJson);
export const kernels = KernelsFile.parse(kernelsJson);
export const envFlags = EnvFlagsFile.parse(envFlagsJson);

export const opKinds = (opKindsJson as { opKinds: Array<{ name: string; group: string; source: SourceLink }> })
  .opKinds;

export interface Snippet {
  path: string;
  line: number;
  startLine: number;
  html: string;
  lang: string;
}

const snippets = (snippetsJson as { snippets: Record<string, Snippet> }).snippets;

export const namedLinks = (linksJson as { links: Record<string, SourceLink> }).links;

export function getSnippet(link: SourceLink): Snippet | undefined {
  if (link.line === undefined) return undefined;
  return snippets[`${link.path}:${link.line}`];
}

/** Collect every SourceLink nested anywhere in `values` (for SnippetProvider). */
export function collectSourceLinks(values: unknown): SourceLink[] {
  const out: SourceLink[] = [];
  const visit = (v: unknown): void => {
    if (Array.isArray(v)) {
      for (const item of v) visit(item);
    } else if (v && typeof v === "object") {
      const obj = v as Record<string, unknown>;
      // Shape-based: any {path, line|anchor} object is a source link — don't
      // key on a "zig/" prefix, or future docs/go links silently lose snippets.
      if (
        typeof obj.path === "string" &&
        (typeof obj.line === "number" || typeof obj.anchor === "string")
      ) {
        out.push(obj as unknown as SourceLink);
      } else {
        for (const value of Object.values(obj)) visit(value);
      }
    }
  };
  visit(values);
  return out;
}

/** Subset of the snippet cache covering `links` — pass to SnippetProvider. */
export function snippetsFor(links: SourceLink[]): Record<string, Snippet> {
  const out: Record<string, Snippet> = {};
  for (const link of links) {
    if (link.line === undefined) continue;
    const key = `${link.path}:${link.line}`;
    const snippet = snippets[key];
    if (snippet) out[key] = snippet;
  }
  return out;
}

export function permalinkFor(link: SourceLink): string | undefined {
  if (!manifest.permalinkBase) return undefined;
  const anchor = link.line !== undefined ? `#L${link.line}` : "";
  return `${manifest.permalinkBase}/${manifest.gitCommit}/${link.path}${anchor}`;
}
