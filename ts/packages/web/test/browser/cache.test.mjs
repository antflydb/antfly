import assert from "node:assert/strict";
import { test } from "node:test";
import { chromium } from "@playwright/test";
import { startRuntimeServer } from "./server.mjs";

test("catalog cache verifies hashes, repairs corruption and supports no-cache mode", {
  timeout: 90000,
}, async () => {
  const server = await startRuntimeServer();
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    let downloads = 0;
    await page.route("**/cache-fixture", async (route) => {
      downloads++;
      await route.fulfill({ status: 200, body: "abc", contentType: "application/octet-stream" });
    });
    await page.goto(server.url);
    const output = await page.evaluate(async (moduleUrl) => {
      const { downloadCatalogModel, clearModelCache } = await import(moduleUrl);
      const digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
      const model = {
        id: "test",
        files: [
          {
            path: "config.json",
            url: `${location.origin}/cache-fixture`,
            sha256: digest,
            size_bytes: 3,
          },
        ],
      };
      await clearModelCache();
      const first = await downloadCatalogModel(model);
      const second = await downloadCatalogModel(model);
      const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle(
        "antfly-inference-models-v1"
      );
      const handle = await dir.getFileHandle(digest),
        writer = await handle.createWritable();
      await writer.write("bad");
      await writer.close();
      const repaired = await downloadCatalogModel(model);
      const uncached = await downloadCatalogModel(model, { cache: false });
      const controller = new AbortController();
      controller.abort();
      let aborted = false;
      try {
        await downloadCatalogModel(model, { signal: controller.signal });
      } catch (error) {
        aborted = error.name === "AbortError";
      }
      await clearModelCache();
      return {
        first: await first
          .get("config.json")
          .text()
          .catch(() => "replaced"),
        second: second.size,
        repaired: await repaired
          .get("config.json")
          .text()
          .catch(() => "removed"),
        uncached: await uncached.get("config.json").text(),
        aborted,
      };
    }, "/client/index.js");
    assert.equal(downloads, 3, "cache hit must avoid network, corruption must redownload");
    assert.equal(output.second, 1);
    assert.equal(output.uncached, "abc");
    assert(output.aborted);
  } finally {
    await browser.close();
    await server.close();
  }
});
