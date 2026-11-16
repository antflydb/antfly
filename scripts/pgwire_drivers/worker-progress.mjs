// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/** Bounded worker diagnostics; never include SQL text, parameters or URLs. */
export function createPhaseTimings({
  send = process.send?.bind(process),
  connected = () => process.connected,
  now = () => performance.now(),
  intervalMs = 1000,
} = {}) {
  const started = now();
  const phases = new Map();
  const active = new Set();
  const slowest = [];
  let activeCount = 0;
  let pending = false;
  let closed = false;
  let lastPublished = -Infinity;
  const snapshot = () => ({
    total_ms: Math.round(now() - started),
    queries: Object.fromEntries(phases),
    slowest: [...slowest],
    active_count: activeCount,
    active: [...active].map((token) => ({
      phase: token.phase,
      start_ms: Math.round(token.before - started),
      duration_ms: Math.round(now() - token.before),
    })),
  });
  const publish = () => {
    if (closed || pending || !send || !connected() || now() - lastPublished < intervalMs) return;
    pending = true;
    lastPublished = now();
    try {
      send({ type: "progress", timings: snapshot() }, () => { pending = false; });
    } catch {
      // Losing the diagnostic channel must not change the query's outcome.
      pending = false;
    }
  };
  const interval = setInterval(() => {
    if (activeCount) publish();
  }, intervalMs);
  interval.unref();
  return {
    snapshot,
    async track(phase, work) {
      const token = { phase, before: now() };
      activeCount++;
      // Retain a fixed-size sample even if a future fixture adds concurrency.
      if (active.size < 8) active.add(token);
      if (activeCount === 1) publish();
      try {
        return await work();
      } finally {
        activeCount--;
        active.delete(token);
        const elapsed = now() - token.before;
        // Phase labels are fixed fixture operations; cap unexpected additions.
        const key = phases.has(phase) || phases.size < 32 ? phase : "OTHER";
        const entry = phases.get(key) ?? { calls: 0, milliseconds: 0 };
        phases.set(key, { calls: entry.calls + 1, milliseconds: entry.milliseconds + elapsed });
        slowest.push({ phase, start_ms: Math.round(token.before - started), duration_ms: Math.round(elapsed) });
        slowest.sort((a, b) => b.duration_ms - a.duration_ms);
        slowest.length = Math.min(slowest.length, 8);
      }
    },
    close() {
      closed = true;
      clearInterval(interval);
    },
  };
}

/** Track buffered SDK operations through body consumption and decoding.
 * Fetch itself stops at response headers. Keep stream ownership with the SDK;
 * do not clone, buffer again, or change the response returned to its caller.
 */
export function profileClientOperations(client, profile) {
  const originals = new Map();
  for (const method of ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE"]) {
    const original = client[method];
    if (typeof original !== "function") continue;
    const wrapped = function (...args) {
      return profile.track("HTTP", () => Reflect.apply(original, this, args));
    };
    originals.set(method, { original, wrapped });
    client[method] = wrapped;
  }
  return () => {
    for (const [method, { original, wrapped }] of originals) {
      if (client[method] === wrapped) client[method] = original;
    }
  };
}

/** Wait for a terminal result while retaining the worker's latest bounded progress. */
export function waitForWorkerResult(child, mode, { timeoutMs = 60_000 } = {}) {
  return new Promise((resolve, reject) => {
    let progress;
    const cleanup = () => {
      clearTimeout(timer);
      child.off("message", reported);
      child.off("exit", stopped);
      child.off("error", failed);
    };
    const failed = (error) => {
      cleanup();
      reject(new Error(`${error.message}; last progress=${JSON.stringify(progress ?? null)}`, { cause: error }));
    };
    const reported = (value) => {
      if (value?.type === "progress") {
        progress = value.timings;
        return;
      }
      cleanup();
      resolve(value);
    };
    const stopped = (code, signal) => failed(new Error(
      `Kysely ${mode} worker exited before reporting: code=${code} signal=${signal}`,
    ));
    const timer = setTimeout(() => failed(new Error(
      `Kysely ${mode} worker did not report within ${timeoutMs / 1000} seconds (pid=${child.pid})`,
    )), timeoutMs);
    child.on("message", reported);
    child.once("exit", stopped);
    child.once("error", failed);
  });
}
