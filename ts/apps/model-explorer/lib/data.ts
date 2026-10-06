// Guard: this module carries the full generated data layer (env flags,
// kernels) — importing it from a client component would put the JSON plus
// zod parsing into the browser bundle.
import "server-only";
import kernelsJson from "@/data/generated/kernels.json";
import linksJson from "@/data/generated/links.json";
import manifestJson from "@/data/generated/manifest.json";
import opKindsJson from "@/data/generated/op-kinds.json";
import { L } from "@/lib/links";
import { KernelsFile, Manifest, type SourceLink } from "@/lib/schema";

// Single implementation lives in lib/links.ts (client-safe); re-exported here
// so server code keeps one import site and the two can never drift.
export { L };

export const manifest = Manifest.parse(manifestJson);
export const kernels = KernelsFile.parse(kernelsJson);

export const opKinds = (
  opKindsJson as { opKinds: Array<{ name: string; group: string; source: SourceLink }> }
).opKinds;

export const namedLinks = (linksJson as { links: Record<string, SourceLink> }).links;

export function permalinkFor(link: SourceLink): string {
  return `${manifest.permalinkBase}/${link.path}`;
}
