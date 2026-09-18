import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { MODEL_LINK_IDS } from "../content/link-ids.ts";

const contentRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "content");

function idsUsedIn(dir: string): string[] {
  const out = new Set<string>();
  for (const file of readdirSync(join(contentRoot, dir))) {
    if (!file.endsWith(".tsx")) continue;
    const src = readFileSync(join(contentRoot, dir, file), "utf8");
    for (const m of src.matchAll(/L\("([a-z0-9-]+)"\)/g)) out.add(m[1]);
  }
  return [...out].sort();
}

test("MODEL_LINK_IDS matches the L(\"…\") ids each content directory references", () => {
  for (const [dir, ids] of Object.entries(MODEL_LINK_IDS)) {
    assert.deepEqual(
      [...ids].sort(),
      idsUsedIn(dir),
      `content/link-ids.ts is stale for "${dir}" — update MODEL_LINK_IDS to match the chapters' L("…") calls`
    );
  }
});
