import gemma4E2bJson from "@/data/generated/models/gemma4-e2b.json";
import gemma4E4bJson from "@/data/generated/models/gemma4-e4b.json";
import gliner2Json from "@/data/generated/models/gliner2.json";
import gliner25Json from "@/data/generated/models/gliner25.json";
import qwen3EmbeddingJson from "@/data/generated/models/qwen3-embedding.json";
import qwen3VlJson from "@/data/generated/models/qwen3-vl.json";
import { ModelSpec } from "@/lib/schema";

/**
 * Registry of generated model specs. Static imports keep everything in the
 * build graph; zod-parse fails the build (not the browser) on schema drift.
 */
const rawSpecs: Record<string, unknown> = {
  "gemma4-e2b": gemma4E2bJson,
  "gemma4-e4b": gemma4E4bJson,
  gliner2: gliner2Json,
  gliner25: gliner25Json,
  "qwen3-embedding": qwen3EmbeddingJson,
  "qwen3-vl": qwen3VlJson,
};

const parsed = new Map<string, ModelSpec>();

export function getModelSpec(slug: string): ModelSpec | undefined {
  if (parsed.has(slug)) return parsed.get(slug);
  const raw = rawSpecs[slug];
  if (!raw) return undefined;
  const spec = ModelSpec.parse(raw);
  parsed.set(slug, spec);
  return spec;
}

export function modelSlugs(): string[] {
  return Object.keys(rawSpecs);
}
