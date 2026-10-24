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

// A single suspended stack per instance. I/O completes exactly once; rewind
// restores the original computation instead of re-executing writes or queries.
import { ObjectStoreError } from "./r2.mjs";
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
export async function instantiateHost(module, transport, { stackBytes = 1024 * 1024, onTransportError = () => {}, onLog = () => {}, onMetrics = () => {}, clock = Date.now } = {}) {
  if (!(module instanceof WebAssembly.Module)) throw new TypeError("a precompiled WebAssembly.Module is required");
  if (!Number.isSafeInteger(stackBytes) || stackBytes < 4096 || stackBytes > 8 * 1024 * 1024) throw new TypeError("invalid suspension stack size");
  let wasm, pending, completed, activeTransport, activeOptions, operations, inferenceCalls, transferred, busy = false, poisoned = false;
  const bytes = (ptr, len) => new Uint8Array(wasm.memory.buffer, ptr, len);
  function allocate(value) {
    if (value.byteLength === 0) return 0;
    const ptr = wasm.antfly_host_alloc(value.byteLength);
    if (!ptr) throw new Error("WASM allocation failed");
    bytes(ptr, value.byteLength).set(value);
    return ptr;
  }
  let lastWall = 0, invalidClock = false;
  function exchange(ptr, len, bodyPtr, bodyLen, replyPtr, handler) {
      if (wasm.asyncify_get_state() === 2) {
        wasm.asyncify_stop_rewind();
        const view = new DataView(wasm.memory.buffer);
        const value = completed;
        if (!value) throw new Error("missing I/O completion");
        view.setUint32(replyPtr, value.jsonPtr, true);
        view.setUint32(replyPtr + 4, value.jsonLen, true);
        view.setUint32(replyPtr + 8, value.bodyPtr, true);
        view.setUint32(replyPtr + 12, value.bodyLen, true);
        completed = null;
        return value.status;
      }
      if (typeof handler !== "function") return 3;
      if (pending) throw new Error("overlapping WASM I/O");
      if (activeOptions.signal?.aborted) return 5;
      if (clock() >= activeOptions.deadlineMs) return 9;
      if (++operations > activeOptions.maxOperations || bodyLen + len > activeOptions.maxTransferBytes - transferred) return 8;
      transferred += bodyLen + len;
      const request = JSON.parse(decoder.decode(bytes(ptr, len)));
      // The WASM memory may grow while preparing a completion. Do not retain
      // views across the async boundary, including upload bodies.
      const body = bytes(bodyPtr, bodyLen).slice();
      pending = Promise.resolve().then(() => handler(request, body)).then(
        value => ({ status: 0, value }),
        error => { if (!(error instanceof ObjectStoreError) || error.status >= 6) onTransportError(error, request.op); return { status: error instanceof ObjectStoreError ? error.status : /10058/.test(String(error)) ? 6 : 7, value: {} }; },
      );
      wasm.asyncify_start_unwind(stackDescriptor);
      return 0;
  }
  const instance = await WebAssembly.instantiate(module, { antfly_host: {
    check_cancel() { return activeOptions?.signal?.aborted || clock() >= (activeOptions?.deadlineMs ?? Infinity) ? 1 : 0; },
    log(ptr, len) { onLog(decoder.decode(bytes(ptr, len))); },
    now_ns(kind) {
      const wall = clock();
      if (wall < lastWall) invalidClock = true;
      lastWall = wall;
      if (invalidClock) return 0n;
      return kind === 0 ? BigInt(Math.floor(performance.now() * 1e6)) : BigInt(wall) * 1000000n;
    },
    object_call: (ptr, len, bodyPtr, bodyLen, replyPtr) => exchange(ptr, len, bodyPtr, bodyLen, replyPtr, activeTransport),
    infer_call: (ptr, len, bodyPtr, bodyLen, replyPtr) => {
      if (wasm.asyncify_get_state() !== 2 && ++inferenceCalls > 8) return 8;
      return exchange(ptr, len, bodyPtr, bodyLen, replyPtr, activeOptions?.inference);
    },
  }, env: {
    antfly_platform_random_secure(ptr, len) {
      try {
        for (let offset = 0; offset < len; offset += 65536) crypto.getRandomValues(bytes(ptr + offset, Math.min(65536, len - offset)));
        return 0;
      } catch { return 1; }
    },
  } });
  wasm = instance.exports;
  for (const name of ["asyncify_start_unwind", "asyncify_stop_unwind", "asyncify_start_rewind", "asyncify_stop_rewind", "asyncify_get_state"]) {
    if (typeof wasm[name] !== "function") throw new Error("module must be built with Asyncify");
  }
  const stackDescriptor = wasm.antfly_host_alloc(stackBytes + 8);
  if (!stackDescriptor) throw new Error("WASM stack allocation failed");
  new DataView(wasm.memory.buffer).setUint32(stackDescriptor, stackDescriptor + 8, true);
  new DataView(wasm.memory.buffer).setUint32(stackDescriptor + 4, stackDescriptor + 8 + stackBytes, true);
  async function request(value, { transport: requestTransport = transport, signal,
    inference, onMetrics: requestMetrics = onMetrics, deadlineMs = clock() + 30000, maxOperations = 1024, maxTransferBytes = 32 * 1024 * 1024 } = {}) {
    const unusable = poisoned || invalidClock;
    if (busy || unusable) throw new Error(unusable ? "WASM instance is unusable" : "WASM instance is busy");
    if (typeof requestTransport !== "function") throw new TypeError("request transport required");
    if (!Number.isSafeInteger(deadlineMs) || deadlineMs < 0 || !Number.isSafeInteger(maxOperations) || maxOperations < 1 ||
      !Number.isSafeInteger(maxTransferBytes) || maxTransferBytes < 1) throw new TypeError("invalid request budget");
    // Serialize before taking the execution lane: caller errors such as a
    // circular value must not leave an otherwise idle instance busy.
    const input = encoder.encode(JSON.stringify(value));
    busy = true;
    activeTransport = requestTransport;
    activeOptions = { signal, deadlineMs, maxOperations, maxTransferBytes, inference };
    operations = 0; inferenceCalls = 0; transferred = 0;
    let requestPtr = 0, protocolError = false, outcome = "error", failureCode = null, waitMs = 0;
    const started = performance.now();
    try {
      requestPtr = allocate(input);
      while (true) {
        const result = wasm.antfly_host_request(requestPtr, input.byteLength);
        if (wasm.asyncify_get_state() === 0) {
          if (completed) throw new Error("unconsumed I/O completion");
          const ptr = Number(result & 0xffffffffn), len = Number(result >> 32n);
          if (!ptr) throw new Error("WASM response allocation failed");
          try {
            const response = JSON.parse(decoder.decode(bytes(ptr, len)));
            if (response.err) { protocolError = true; throw new Error(response.err); }
            outcome = "ok";
            return response;
          } finally { wasm.antfly_host_free(ptr, len); }
        }
        if (wasm.asyncify_get_state() !== 1 || !pending) throw new Error("invalid WASM suspension");
        wasm.asyncify_stop_unwind();
        const waitStarted = performance.now();
        const completion = await pending;
        waitMs += performance.now() - waitStarted;
        pending = null;
        const header = encoder.encode(JSON.stringify(completion.value.json ?? {}));
        const body = completion.value.body ?? new Uint8Array();
        // Account response bytes before transferring ownership to WASM.
        // A write's successful receipt is never replaced with a deadline error.
        if (body.byteLength + header.byteLength > activeOptions.maxTransferBytes - transferred) {
          completion.status = 8; completion.value = {};
          wasm.asyncify_start_rewind(stackDescriptor);
          completed = { status: 8, jsonPtr: 0, jsonLen: 0, bodyPtr: 0, bodyLen: 0 };
          continue;
        }
        transferred += body.byteLength + header.byteLength;
        const jsonPtr = allocate(header);
        let bodyPtr;
        try { bodyPtr = allocate(body); } catch (error) { wasm.antfly_host_free(jsonPtr, header.byteLength); throw error; }
        completed = { status: completion.status, jsonPtr, jsonLen: header.byteLength, bodyPtr, bodyLen: body.byteLength };
        wasm.asyncify_start_rewind(stackDescriptor);
      }
    } catch (error) {
      failureCode = protocolError && /^[A-Za-z][A-Za-z0-9_]{0,95}$/.test(error.message) ? error.message : "HostFailure";
      // Ordinary database errors are returned with an idle stack. A trap or
      // incomplete unwind poisons the instance; never reuse its mutable state.
      // A foreign JS exception may bypass Zig defers even with an idle
      // Asyncify state. Only a normal owned error response permits reuse.
      if (!protocolError || wasm.asyncify_get_state() !== 0 || completed || pending) poisoned = true;
      throw error;
    } finally {
      if (requestPtr && !poisoned) wasm.antfly_host_free(requestPtr, input.byteLength);
      activeTransport = null;
      activeOptions = null;
      busy = false;
      const metrics = Object.freeze({outcome, failure_code: failureCode, clock_valid: !invalidClock, operations, inference_calls: inferenceCalls,
        transferred_bytes: transferred, elapsed_ms: performance.now() - started, io_wait_ms: waitMs,
        wasm_linear_bytes: wasm.memory.buffer.byteLength,
        working_set_peak_bytes: wasm.antfly_host_working_set_peak_bytes?.() ?? null});
      // Observation must never alter an accepted mutation or poison its lane.
      try { requestMetrics(metrics); } catch {}
    }
  }
  return { request, usable: () => !poisoned && !invalidClock, memoryBytes: () => wasm.memory.buffer.byteLength };
}
