// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
const runtime = () =>
  import(`../dist/runtime-assets/runtime/model-configuration.js?test=${Math.random()}`);
const tick = () => new Promise((resolve) => setImmediate(resolve));
test("cancelling the last configuration waiter aborts fetch and permits a fresh probe", async (t) => {
  const previous = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = previous;
  });
  const signals = [];
  globalThis.fetch = async (_url, { signal }) => {
    signals.push(signal);
    return new Promise((_resolve, reject) =>
      signal.addEventListener("abort", () => reject(signal.reason), { once: true })
    );
  };
  const { configurationReason } = await runtime();
  for (let i = 0; i < 2; i++) {
    const controller = new AbortController();
    const pending = configurationReason(new Map(), {}, {}, "fp32", controller.signal);
    const rejected = assert.rejects(pending, { name: "AbortError" });
    await tick();
    controller.abort();
    await rejected;
    assert.equal(signals.length, i + 1, "reload must start a new fetch");
    assert(signals[i].aborted);
  }
});
test("a cancelled waiter cannot cancel another inspection sharing compilation", async (t) => {
  const previous = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = previous;
  });
  let signal,
    calls = 0;
  globalThis.fetch = async (_url, options) => {
    calls++;
    signal = options.signal;
    return new Promise((_resolve, reject) =>
      signal.addEventListener("abort", () => reject(signal.reason), { once: true })
    );
  };
  const { configurationReason } = await runtime();
  const first = new AbortController(),
    second = new AbortController();
  const a = configurationReason(new Map(), {}, {}, "fp32", first.signal);
  const b = configurationReason(new Map(), {}, {}, "fp32", second.signal);
  const rejectedA = assert.rejects(a, { name: "AbortError" });
  const rejectedB = assert.rejects(b, { name: "AbortError" });
  await tick();
  first.abort();
  await rejectedA;
  assert.equal(calls, 1);
  assert.equal(signal.aborted, false);
  second.abort();
  await rejectedB;
  assert(signal.aborted);
});
test("cancellation also abandons an in-flight WebAssembly compilation", async (t) => {
  const previousFetch = globalThis.fetch,
    previousCompile = WebAssembly.compile;
  t.after(() => {
    globalThis.fetch = previousFetch;
    WebAssembly.compile = previousCompile;
  });
  let finish,
    fetches = 0,
    compiling;
  const entered = new Promise((resolve) => {
    compiling = resolve;
  });
  const compilation = new Promise((resolve) => {
    finish = resolve;
  });
  globalThis.fetch = async (_url, { signal }) => {
    if (++fetches === 1) return { ok: true, arrayBuffer: async () => new ArrayBuffer(0) };
    return new Promise((_resolve, reject) =>
      signal.addEventListener("abort", () => reject(signal.reason), { once: true })
    );
  };
  WebAssembly.compile = () => {
    compiling();
    return compilation;
  };
  const { configurationReason } = await runtime();
  const first = new AbortController();
  const a = configurationReason(new Map(), {}, {}, "fp32", first.signal);
  const rejectedA = assert.rejects(a, { name: "AbortError" });
  await entered;
  first.abort();
  await rejectedA;
  const second = new AbortController();
  const b = configurationReason(new Map(), {}, {}, "fp32", second.signal);
  const rejectedB = assert.rejects(b, { name: "AbortError" });
  await tick();
  assert.equal(fetches, 2, "a cancelled compilation must not hold the cache slot");
  second.abort();
  await rejectedB;
  finish({});
  await tick();
});
