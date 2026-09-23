import type { SourceLink } from "../lib/schema/index.ts";
import { readRepoFile } from "./lib.ts";

const CONTEXT_LINES = 5;
const MAX_SNIPPETS = 800;

export interface Snippet {
  path: string;
  line: number;
  startLine: number;
  html: string;
  lang: string;
}

function langFor(path: string): string {
  if (path.endsWith(".zig")) return "zig";
  if (path.endsWith(".metal")) return "cpp";
  if (path.endsWith(".m")) return "objective-c";
  if (path.endsWith(".md")) return "markdown";
  return "text";
}

/** Pre-highlight ±5-line excerpts for every SourceLink, keyed "path:line". */
export async function buildSnippets(links: SourceLink[]): Promise<Record<string, Snippet>> {
  const { createHighlighter } = await import("shiki");
  const highlighter = await createHighlighter({
    themes: ["github-light", "github-dark"],
    langs: ["zig", "cpp", "objective-c", "markdown"],
  });

  const out: Record<string, Snippet> = {};
  const fileCache = new Map<string, string[]>();
  let count = 0;
  for (const link of links) {
    if (link.line === undefined) continue;
    const key = `${link.path}:${link.line}`;
    if (out[key]) continue;
    if (++count > MAX_SNIPPETS)
      throw new Error(
        `snippet budget exceeded (${MAX_SNIPPETS}); raise the budget or scope the requested links`
      );
    let lines = fileCache.get(link.path);
    if (!lines) {
      lines = readRepoFile(link.path).split("\n");
      fileCache.set(link.path, lines);
    }
    // Each schedule row is a complete long declaration. Avoid embedding ten
    // neighboring, almost-identical rows in every table hover.
    const context = link.anchor?.startsWith(".format = ") ? 1 : CONTEXT_LINES;
    const start = Math.max(1, link.line - context);
    const end = Math.min(lines.length, link.line + context);
    const code = lines.slice(start - 1, end).join("\n");
    const lang = langFor(link.path);
    const html = highlighter.codeToHtml(code, {
      lang: lang === "text" ? "markdown" : lang,
      themes: { light: "github-light", dark: "github-dark" },
      defaultColor: "light",
      cssVariablePrefix: "--shiki-",
    });
    out[key] = { path: link.path, line: link.line, startLine: start, html, lang };
  }
  highlighter.dispose();
  return out;
}
