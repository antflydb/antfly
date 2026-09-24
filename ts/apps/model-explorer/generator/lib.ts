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

function assertRepoRelative(relPath: string): void {
  if (relPath.startsWith("/") || relPath.split(/[\\/]/).includes("..")) {
    throw new Error(`source path must be repo-relative: ${relPath}`);
  }
}

export function readRepoFile(relPath: string): string {
  assertRepoRelative(relPath);
  return readFileSync(join(repoRoot, relPath), "utf8");
}

/**
 * A source link must name a file that exists in this checkout. Links carry
 * no line numbers or anchors, so this is the only way they can rot: a file
 * was moved or deleted.
 */
export function verifySourcePath(link: { path: string }): void {
  assertRepoRelative(link.path);
  if (!existsSync(join(repoRoot, link.path))) {
    throw new Error(`source path not found: ${link.path}`);
  }
}

/** Stable stringify with 1-space indent to keep generated JSON diffs readable. */
export function toJson(value: unknown): string {
  return `${JSON.stringify(value, null, 1)}\n`;
}
