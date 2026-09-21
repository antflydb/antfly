import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { chromium, firefox, webkit } from "@playwright/test";
const base = process.env.PLAYGROUND_URL || "http://127.0.0.1:3101/";
const model = process.env.EXTRACTION_MODEL;
const precision = process.env.EXTRACTION_PRECISION || "q8_0";
const engine = process.env.BROWSER || "chromium";
const gpu = process.env.EXTRACTION_GPU === "1";
const browserType = { chromium, firefox, webkit }[engine];
const packageUrl = `/@fs${new URL("../../../packages/inference-web/src/index.ts", import.meta.url).pathname}`;

test("catalog cache verifies hashes, repairs corruption and supports no-cache mode", { timeout: 90000 }, async () => {
  const browser = await browserType.launch();
  try {
    const page = await browser.newPage();
    let downloads = 0;
    await page.route("**/cache-fixture", async route => {
      downloads++;
      await route.fulfill({ status: 200, body: "abc", contentType: "application/octet-stream" });
    });
    await page.goto(base);
    const output = await page.evaluate(async moduleUrl => {
      const { downloadCatalogModel, clearModelCache } = await import(moduleUrl);
      const digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
      const model = { id: "test", files: [{ path: "config.json", url: `${location.origin}/cache-fixture`, sha256: digest, size_bytes: 3 }] };
      await clearModelCache();
      const first = await downloadCatalogModel(model);
      const second = await downloadCatalogModel(model);
      const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle("antfly-inference-models-v1");
      const handle = await dir.getFileHandle(digest), writer = await handle.createWritable();
      await writer.write("bad"); await writer.close();
      const repaired = await downloadCatalogModel(model);
      const uncached = await downloadCatalogModel(model, { cache: false });
      const controller = new AbortController(); controller.abort();
      let aborted = false;
      try { await downloadCatalogModel(model, { signal: controller.signal }); } catch (error) { aborted = error.name === "AbortError"; }
      await clearModelCache();
      return { first: await first.get("config.json").text().catch(() => "replaced"), second: second.size, repaired: await repaired.get("config.json").text().catch(() => "removed"), uncached: await uncached.get("config.json").text(), aborted };
    }, packageUrl);
    assert.equal(downloads, 3, "cache hit must avoid network, corruption must redownload");
    assert.equal(output.second, 1); assert.equal(output.uncached, "abc"); assert(output.aborted);
  } finally { await browser.close(); }
});

async function ready(page, text) {
  await page.waitForFunction(
    (expected) =>
      document.querySelector(".status")?.textContent.includes(expected) ||
      document.querySelector("[role=alert]"),
    text,
    { timeout: 90000 }
  );
  assert.deepEqual(await page.locator("[role=alert]").allTextContents(), []);
}
test("desktop/mobile shell, local inference, cancellation and CPU without isolation", {
  timeout: 240000,
}, async () => {
  const browser = await browserType.launch({
    ...(gpu && engine === "chromium"
      ? { args: ["--enable-unsafe-webgpu", "--use-angle=metal"] }
      : {}),
  });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
    const page = await context.newPage(),
      errors = [],
      requests = [];
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error" || message.type() === "warning") console.log(`Browser ${message.type()}: ${message.text()}`);
    });
    page.on("request", (request) =>
      requests.push({ url: request.url(), method: request.method(), body: request.postData() })
    );
    if (!gpu)
      await page.route(base, async (route) => {
        const response = await route.fetch(),
          headers = { ...response.headers() };
        delete headers["cross-origin-opener-policy"];
        delete headers["cross-origin-embedder-policy"];
        await route.fulfill({ response, headers });
      });
    await page.goto(base);
    await page.getByRole("heading", { name: /Find the meaning/ }).waitFor();
    assert.equal(
      await page.evaluate(() => document.documentElement.scrollWidth > innerWidth),
      false
    );
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(
      await page.evaluate(() => document.documentElement.scrollWidth > innerWidth),
      false,
      "Mobile layout must not overflow"
    );
    await page.setViewportSize({ width: 1440, height: 1000 });
    if (!model) {
      console.log("Set EXTRACTION_MODEL for real-model browser checks");
      return;
    }
    if (!gpu) assert.equal(await page.evaluate(() => crossOriginIsolated), false);
    await page
      .locator("select")
      .nth(1)
      .selectOption(gpu ? "webgpu" : "wasm");
    await page.locator("input[webkitdirectory]").setInputFiles(model);
    await page.locator("select").nth(2).selectOption(precision);
    await page.getByRole("button", { name: /Load local bundle/ }).click();
    await ready(page, "Ready");
    await page.getByLabel("Input text").fill("John works at Apple.");
    await page.getByRole("button", { name: /Run extraction/ }).click();
    await ready(page, "Complete");
    const result = JSON.parse(await page.locator(".results details > pre").last().innerText());
    const entities = result.entities ?? result.data?.[0]?.entities;
    assert(
      entities.some((e) => e.text === "John" && e.label === "person"),
      "Expected person from real model"
    );
    assert(
      entities.some((e) => e.text === "Apple" && e.label === "organization"),
      "Expected organization from real model"
    );
    assert((await page.locator(".highlighted mark").count()) > 0);
    if (gpu)
      assert.match(
        await page.locator(".metrics").innerText(),
        /WEBGPU/,
        "Do not silently count CPU fallback as GPU validation"
      );
    await page.getByRole("button", { name: /Run extraction/ }).click();
    await page.getByRole("button", { name: "Cancel", exact: true }).click();
    await page.waitForFunction(
      () =>
        !document.querySelector("button:enabled")?.textContent?.includes("Cancel") &&
        !document.querySelector(".spinner")
    );
    await page.getByRole("button", { name: /Run extraction/ }).click();
    await ready(page, "Complete");
    for (const task of ["classification", "structures", "relations"]) {
      await page.getByRole("button", { name: task, exact: true }).click();
      await page.getByRole("button", { name: "Use example", exact: true }).click();
      await page.getByRole("button", { name: /Run extraction/ }).click();
      await ready(page, "Complete");
      assert(JSON.parse(await page.locator(".results details > pre").last().innerText()));
    }
    if (process.env.EXTRACTION_ORACLE) {
      const fixture = JSON.parse(await readFile(process.env.EXTRACTION_ORACLE, "utf8"));
      await page.getByLabel("Advanced JSON").check();
      for (const item of fixture.cases) {
        await page.getByLabel("Request JSON").fill(JSON.stringify({ schema_version: 2, model: "oracle", inputs: [{ content: item.text }], schema: item.schema, options: { include_confidence: true, include_spans: true, offset_unit: "unicode_codepoints" } }));
        await page.getByRole("button", { name: /Run extraction/ }).click();
        await ready(page, "Complete");
        const output = JSON.parse(await page.locator(".results details > pre").last().innerText()).data[0];
        assert.deepEqual((output.entities ?? []).map(e => `${e.label}:${e.text}:${e.start}:${e.end}`).sort(), item.expected.entities.flatMap(group => group.values.map(e => `${group.name}:${e.text}:${e.source.start}:${e.source.end}`)).sort(), `${item.id}: entity decisions`);
        assert.deepEqual((output.classifications ?? []).map(c => `${c.name}:${c.label}`).sort(), item.expected.classifications.flatMap(group => group.labels.map(c => `${group.name}:${c.label}`)).sort(), `${item.id}: classification decisions`);
        for (const group of item.expected.structures) assert.equal(output.structures?.[group.name]?.length ?? 0, group.instances.length, `${item.id}: record count`);
        assert.equal(output.relations?.length ?? 0, item.expected.relations.length, `${item.id}: relation count`);
        console.log(`Browser oracle decisions matched: ${item.id}`);
      }
    }
    await page.getByRole("button", { name: "Unload", exact: true }).click();
    assert.deepEqual(errors, []);
    assert(
      !requests.some((request) => request.body || !["GET", "HEAD"].includes(request.method)),
      "No inference text or local file upload"
    );
    assert(
      !requests.some((request) => new URL(request.url).origin !== new URL(base).origin),
      "Local workflow must be fully same-origin"
    );
  } finally {
    await browser.close();
  }
});
