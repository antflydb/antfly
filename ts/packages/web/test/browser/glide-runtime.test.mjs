// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { createReadStream } from "node:fs";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { chromium } from "@playwright/test";
import { startRuntimeServer } from "./server.mjs";

const { GLIDE_MODEL, EXTRACTION_GPU } = process.env;
const nativeCases =
  process.env.GLIDE_NATIVE_CASES ??
  new URL("../../../../../zig/pkg/inference/web/glide-native-cases.json", import.meta.url);
test("GLiDE WebGPU executes GPU operations and matches native decisions", {
  skip: EXTRACTION_GPU !== "1" || !GLIDE_MODEL,
  timeout: 300_000,
}, async () => {
  const cases = JSON.parse(await readFile(nativeCases, "utf8"));
  assert(cases.length > 0);
  const server = await startRuntimeServer();
  let browser;
  try {
    browser = await chromium.launch({ args: ["--enable-unsafe-webgpu", "--use-angle=metal"] });
    const page = await browser.newPage();
    await page.goto(server.url);
    await page.setContent('<input type="file" webkitdirectory multiple>');
    await page.locator("input").setInputFiles(GLIDE_MODEL);
    const runs = await page.evaluate(
      async (requests) => {
        const { Inference } = await import("/client/inference.js");
        const { WebGPUOps } = await import("/inference/webgpu-ops.js");
        const files = new Map(
          [...document.querySelector("input").files].map((file) => [
            file.webkitRelativePath.split("/").slice(1).join("/"),
            file,
          ])
        );
        const client = new Inference("/inference/");
        const commands = new Set();
        const original = WebGPUOps.prototype.handleWorkerCommand;
        try {
          const model = await client.loadModel(files, { backend: "webgpu", precision: "bf16" });
          if (model.backend !== "webgpu") throw Error(model.fallbackReason ?? "Expected WebGPU");
          WebGPUOps.prototype.handleWorkerCommand = function (message, buffer) {
            commands.add(message.cmd);
            return original.call(this, message, buffer);
          };
          const outputs = [];
          for (const request of requests) outputs.push((await client.decide(request)).value);
          return { outputs, commands: [...commands] };
        } finally {
          WebGPUOps.prototype.handleWorkerCommand = original;
          client.dispose();
        }
      },
      cases.map(({ request }) => request)
    );
    assert(
      runs.commands.some((command) => /matmul|attention|gemm/.test(command)),
      runs.commands.join(", ")
    );
    for (const [index, actual] of runs.outputs.entries()) {
      const expected = cases[index].response;
      assert.equal(actual.usage.input_tokens, expected.usage.input_tokens);
      assert.deepEqual(
        actual.answers.map((a) => a.name),
        expected.answers.map((a) => a.name)
      );
      for (const [i, answer] of actual.answers.entries()) {
        assert.equal(answer.choice, expected.answers[i].choice);
        if (expected.answers[i].score !== undefined)
          assert(Math.abs(answer.score - expected.answers[i].score) <= 0.02);
        if (expected.answers[i].probability !== undefined)
          assert(Math.abs(answer.probability - expected.answers[i].probability) <= 0.01);
        assert(Math.abs(answer.confidence - expected.answers[i].confidence) <= 0.01, answer.name);
        if (expected.answers[i].probabilities) {
          assert.deepEqual(
            answer.probabilities.map((entry) => entry.value),
            expected.answers[i].probabilities.map((entry) => entry.value)
          );
          for (const [j, entry] of answer.probabilities.entries()) {
            assert(
              Math.abs(entry.probability - expected.answers[i].probabilities[j].probability) <=
                0.01,
              entry.value
            );
          }
        }
      }
    }
  } finally {
    await browser?.close();
    await server.close();
  }
});

test("mailbox classifier is opt-in and downloads the pinned checkpoint", {
  skip: EXTRACTION_GPU !== "1" || !GLIDE_MODEL,
  timeout: 300_000,
}, async () => {
  const mail = fileURLToPath(
    new URL("../../../../../examples/cloudflare-serverless/mail-public/", import.meta.url)
  );
  const catalog = JSON.parse(await readFile(join(GLIDE_MODEL, "catalog.json"), "utf8"));
  const assets = new Set([
    "index.html",
    "mail.css",
    "mail.js",
    "local-index.js",
    "local-schema.js",
    "sw.js",
    "manifest.webmanifest",
    "icon.svg",
  ]);
  const pins = new Map(
    catalog.files.map((file) => [new URL(file.url, "http://localhost").pathname, file])
  );
  const id = "00000000-0000-4000-8000-000000000009";
  const message = {
    id,
    from: "airline@example.net",
    to: "me@example.test",
    subject: "Your flight moved",
    text: "Your flight leaves at 6 AM instead of 9 AM tomorrow. Please confirm the change.",
    received_at: Date.now(),
  };
  let downloads = 0;
  const server = await startRuntimeServer(async (path, _req, res, headers) => {
    const json = (value) =>
      res
        .writeHead(200, { ...headers, "Content-Type": "application/json" })
        .end(JSON.stringify(value));
    if (path === "/api/me")
      json({ user: { id: "u", email: message.to }, mailbox: { id, address: message.to } });
    else if (path === "/mail/messages") json({ messages: [message], cursor: null });
    else if (path === "/mail/messages/" + id) json(message);
    else if (path === "/local/config.json")
      json({
        runtime: { web: "/client/inference.js", inference: "/inference/" },
        classifier: { catalog: "/local/models/glide/catalog.json" },
      });
    else if (path === "/local/models/glide/catalog.json") json(catalog);
    else if (pins.has(path)) {
      const file = pins.get(path);
      downloads++;
      res.writeHead(200, {
        ...headers,
        "Content-Type": "application/octet-stream",
        "Content-Length": file.size_bytes,
      });
      createReadStream(join(GLIDE_MODEL, file.path)).pipe(res);
    } else {
      const name = path === "/" ? "index.html" : path.slice(1);
      if (!assets.has(name)) return false;
      const type = name.endsWith(".js")
        ? "text/javascript"
        : name.endsWith(".css")
          ? "text/css"
          : name.endsWith(".html")
            ? "text/html"
            : "application/octet-stream";
      res
        .writeHead(200, { ...headers, "Content-Type": type })
        .end(await readFile(join(mail, name)));
    }
    return true;
  });
  let browser;
  try {
    browser = await chromium.launch({ args: ["--enable-unsafe-webgpu", "--use-angle=metal"] });
    const page = await browser.newPage({ serviceWorkers: "block" });
    await page.goto(server.url);
    await page.getByRole("button", { name: /Your flight moved/ }).click();
    const classify = page.getByRole("button", { name: "Classify on this device (model download)" });
    await classify.waitFor();
    assert.equal(downloads, 0);
    await classify.click();
    await page
      .getByText(/(?:Primary|Updates|Promotions|Social|Forums).*Built with Fastino GLiDE/)
      .waitFor({ timeout: 180_000 });
    assert.equal(downloads, pins.size);
  } finally {
    await browser?.close();
    await server.close();
  }
});
