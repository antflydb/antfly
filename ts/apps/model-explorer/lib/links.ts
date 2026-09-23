/** Client-safe named-link lookup (imports only the small links.json). */
import linksJson from "@/data/generated/links.json";
import type { SourceLink } from "@/lib/schema";

const namedLinks = (linksJson as { links: Record<string, SourceLink> }).links;

export function L(id: string): SourceLink {
  const link = namedLinks[id];
  if (!link) throw new Error(`unknown named link "${id}" — add it to data/curated/links.json`);
  return link;
}
