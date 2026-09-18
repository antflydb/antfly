import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** Walk up from this file until we find the antfly repo root (.git). */
export function findRepoRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  while (dir !== "/") {
    if (existsSync(join(dir, ".git"))) return dir;
    dir = resolve(dir, "..");
  }
  throw new Error("could not locate repo root (.git) above generator/");
}

export const repoRoot = findRepoRoot();
export const appRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const sourcePaths = new Set<string>();

export function readRepoFile(relPath: string): string {
  if (relPath.startsWith("/") || relPath.split(/[\\/]/).includes("..")) {
    throw new Error(`source path must be repo-relative: ${relPath}`);
  }
  sourcePaths.add(relPath);
  return readFileSync(join(repoRoot, relPath), "utf8");
}

export function gitCommit(ref = "HEAD"): string {
  return execFileSync("git", ["rev-parse", "--verify", "--end-of-options", `${ref}^{commit}`], {
    cwd: repoRoot,
  })
    .toString()
    .trim();
}

/** A permalink must describe exactly the source bytes used for extraction. */
export function verifySourceRevision(commit: string): void {
  if (!/^[a-f0-9]{40}$/.test(commit)) throw new Error(`invalid source revision: ${commit}`);
  const paths = [...sourcePaths].sort();
  const changed = execFileSync("git", ["diff", "--name-only", commit, "--", ...paths], {
    cwd: repoRoot,
  })
    .toString()
    .trim();
  const untracked = execFileSync("git", ["ls-files", "--others", "--", ...paths], {
    cwd: repoRoot,
  })
    .toString()
    .trim();
  if (changed || untracked) {
    const differences = [changed, untracked].filter(Boolean).join("\n").split("\n");
    throw new Error(
      `${differences.length} source files differ from permalink revision ${commit.slice(0, 8)}:\n${differences.slice(0, 10).join("\n")}\n` +
        (differences.length > 10 ? `... and ${differences.length - 10} more\n` : "") +
        "Regenerate from the intended source revision. Source edits must exist in that revision before they can have accurate GitHub permalinks."
    );
  }
}

/** 1-indexed line lookup. */
export function lineOf(content: string, index: number): number {
  let line = 1;
  for (let i = 0; i < index; i++) if (content.charCodeAt(i) === 10) line++;
  return line;
}

export interface AnchorResult {
  line: number;
  healed: boolean;
}

const isWordChar = (ch: string | undefined) => ch !== undefined && /[A-Za-z0-9_]/.test(ch);

/**
 * True when `text` contains `anchor` at an identifier boundary: an anchor
 * whose first/last character is a word character must not continue an
 * identifier on that side (so anchor "add," never matches "lazy_add,").
 */
export function containsAnchor(text: string, anchor: string): boolean {
  const anchorStartsWord = isWordChar(anchor[0]);
  const anchorEndsWord = isWordChar(anchor[anchor.length - 1]);
  let from = 0;
  for (;;) {
    const at = text.indexOf(anchor, from);
    if (at < 0) return false;
    const okLeft = !anchorStartsWord || !isWordChar(text[at - 1]);
    const okRight = !anchorEndsWord || !isWordChar(text[at + anchor.length]);
    if (okLeft && okRight) return true;
    from = at + 1;
  }
}

/**
 * Verify that `anchor` appears at `line` in the file; if not, search for a
 * unique occurrence and heal the line number. Matching is identifier-boundary
 * aware, so a superstring line (e.g. "lazy_add," for anchor "add,") neither
 * satisfies nor silently heals the link. Throws if the anchor is gone or
 * ambiguous.
 */
export function verifyAnchor(
  relPath: string,
  line: number | undefined,
  anchor: string
): AnchorResult {
  const content = readRepoFile(relPath);
  const lines = content.split("\n");
  if (line !== undefined && lines[line - 1] !== undefined && containsAnchor(lines[line - 1], anchor)) {
    return { line, healed: false };
  }
  const hits: number[] = [];
  for (let i = 0; i < lines.length; i++) {
    if (containsAnchor(lines[i], anchor)) hits.push(i + 1);
  }
  if (hits.length === 1) return { line: hits[0], healed: true };
  if (hits.length === 0) {
    throw new Error(`anchor not found in ${relPath}: "${anchor}"`);
  }
  throw new Error(`anchor ambiguous (${hits.length} hits) in ${relPath}: "${anchor}"`);
}

export function verifySourceLink(link: {
  path: string;
  line?: number;
  endLine?: number;
  anchor?: string;
}): AnchorResult | undefined {
  const lines = readRepoFile(link.path).split("\n");
  if (lines.at(-1) === "") lines.pop();
  const result = link.anchor ? verifyAnchor(link.path, link.line, link.anchor) : undefined;
  const line = result?.line ?? link.line;
  const endLine =
    link.endLine === undefined
      ? undefined
      : link.endLine + ((result?.line ?? link.line ?? 0) - (link.line ?? 0));
  if (
    (line !== undefined && line > lines.length) ||
    (endLine !== undefined && endLine > lines.length)
  ) {
    throw new Error(`source line outside ${link.path} (${lines.length} lines)`);
  }
  return result;
}

/** Stable stringify with 1-space indent to keep generated JSON diffs readable. */
export function toJson(value: unknown): string {
  return `${JSON.stringify(value, null, 1)}\n`;
}
