import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { exportFile } from "./preview.ts";

test("static export resolves deep links and assets without leaking files or hiding 404s", () => {
  const temp = realpathSync(mkdtempSync(join(tmpdir(), "model-explorer-preview-")));
  const root = join(temp, "out");
  mkdirSync(join(root, "models"), { recursive: true });
  writeFileSync(join(root, "index.html"), "Home");
  writeFileSync(join(root, "models", "gliner2.html"), "Model");
  writeFileSync(join(root, "asset.js"), "export default 1");
  writeFileSync(join(temp, "outside.txt"), "Not served");
  symlinkSync(join(temp, "outside.txt"), join(root, "escape.txt"));
  try {
    assert.equal(exportFile(root, "/"), join(root, "index.html"));
    assert.equal(exportFile(root, "/models/gliner2"), join(root, "models", "gliner2.html"));
    assert.equal(exportFile(root, "/models/gliner2/"), join(root, "models", "gliner2.html"));
    assert.equal(exportFile(root, "/asset.js"), join(root, "asset.js"));
    for (const path of ["/missing", "/../outside.txt", "/escape.txt", "/\0", "/models\\gliner2"]) {
      assert.equal(exportFile(root, path), undefined, path);
    }
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
});
