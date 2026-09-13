#!/usr/bin/env node
// Copyright 2026 Antfly, Inc.
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

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import { WebGPUOps } from './webgpu-ops.js';

const DATA_OFFSET = 64;
const pattern = Uint8Array.from({ length: 96 }, (_, i) => (i * 17 + 3) % 256);
const workerSource = readFileSync(new URL('./inference-worker.js', import.meta.url), 'utf8');

function workerBridge(payloadBytes = 22) {
  const shared = new SharedArrayBuffer(DATA_OFFSET + payloadBytes);
  const control = new Int32Array(shared, 0, 16);
  const memory = { buffer: new ArrayBuffer(160) };
  new Uint8Array(memory.buffer).fill(0xee);
  const state = { messages: [], waits: 0, failOffset: null, uploaded: new Uint8Array(128) };
  const context = {
    TextEncoder, TextDecoder,
    shared, memory,
    self: {
      postMessage(msg) {
        assert.equal(msg.type, 'gpu-sync');
        assert.equal(Atomics.load(control, 0), 0);
        state.messages.push({ ...msg });
        let result = 0;
        if (msg.cmd === 'download') {
          if (msg.offsetBytes === state.failOffset) result = -1;
          else new Uint8Array(shared, DATA_OFFSET, msg.size).set(pattern.subarray(msg.offsetBytes, msg.offsetBytes + msg.size));
        } else {
          assert.equal(msg.cmd, 'write_buffer_at_offset');
          state.uploaded.set(new Uint8Array(shared, DATA_OFFSET, msg.sizeBytes), msg.offsetBytes);
        }
        Atomics.store(control, 1, result);
        Atomics.store(control, 0, 1);
      },
    },
    Atomics: {
      store: Atomics.store,
      wait(view, index) {
        assert.equal(Atomics.load(view, index), 1, 'a chunk must be acknowledged before its bytes are consumed');
        state.waits += 1;
        return 'not-equal';
      },
    },
  };
  // Evaluate the actual worker import implementation with only its unrelated
  // model-loading imports removed. No production test hook or WASM is needed.
  vm.runInNewContext(workerSource.replace(/^import .*;\r?$/gm, '') + `
    sab = globalThis.shared;
    ctrl = new Int32Array(sab, 0, 16);
    globalThis.imports = getGpuImports(() => globalThis.memory);
  `, context, { filename: 'inference-worker.js' });
  return { ...state, state, imports: context.imports, shared, control, bytes: new Uint8Array(memory.buffer) };
}

test('worker GPU transfers chunk by aligned actual SAB capacity and retain exact offsets', () => {
  const fixture = workerBridge(); // 22 usable bytes => 20-byte aligned chunks.
  fixture.imports.gpu_download(7, 12n, 68);
  assert.deepEqual(fixture.state.messages.map(m => [m.offsetBytes, m.size]), [[0, 20], [20, 20], [40, 20], [60, 8]]);
  assert.equal(fixture.state.waits, 4);
  assert.deepEqual(fixture.bytes.subarray(12, 80), pattern.subarray(0, 68));
  assert.ok(fixture.bytes.subarray(0, 12).every(value => value === 0xee));
  assert.ok(fixture.bytes.subarray(80).every(value => value === 0xee));

  fixture.state.messages.length = 0;
  fixture.imports.gpu_write_buffer_at_offset(7, 4, 12n, 68);
  assert.deepEqual(fixture.state.messages.map(m => [m.offsetBytes, m.sizeBytes]), [[4, 20], [24, 20], [44, 20], [64, 8]]);
  assert.deepEqual(fixture.state.uploaded.subarray(4, 72), pattern.subarray(0, 68));
});

test('worker GPU readback rejects a failed chunk without copying stale data and can retry', () => {
  const fixture = workerBridge();
  fixture.state.failOffset = 20;
  assert.throws(() => fixture.imports.gpu_download(7, 8, 68), /GPU download failed at byte offset 20/);
  assert.equal(fixture.state.messages.length, 2);
  assert.deepEqual(fixture.bytes.subarray(8, 28), pattern.subarray(0, 20));
  assert.ok(fixture.bytes.subarray(28).every(value => value === 0xee));
  fixture.state.failOffset = null;
  fixture.imports.gpu_download(7, 8, 68);
  assert.deepEqual(fixture.bytes.subarray(8, 76), pattern.subarray(0, 68));
});

test('worker GPU transfers reject invalid geometry before blocking and skip empty transfers', () => {
  for (const payload of [0, 3]) {
    const fixture = workerBridge(payload);
    fixture.imports.gpu_download(7, 0, 0);
    fixture.imports.gpu_write_buffer_at_offset(7, 0, 0, 0);
    assert.throws(() => fixture.imports.gpu_download(7, 0, 4), /no aligned data capacity/);
    assert.throws(() => fixture.imports.gpu_write_buffer_at_offset(7, 0, 0, 4), /no aligned data capacity/);
    assert.equal(fixture.state.messages.length, 0);
  }
  const fixture = workerBridge();
  assert.throws(() => fixture.imports.gpu_download(7, 0, 6), /multiple of four/);
  assert.throws(() => fixture.imports.gpu_write_buffer_at_offset(7, 2, 0, 8), /multiple of four/);
  assert.throws(() => fixture.imports.gpu_write_buffer_at_offset(7, 0, 0, 6), /multiple of four/);
  assert.equal(fixture.state.messages.length, 0);
});

// Small host-side GPU double: mapped staging bytes are distinct from the SAB,
// and every failure phase can be observed without a browser or GPU device.
function hostBridge() {
  const shared = new SharedArrayBuffer(DATA_OFFSET + 22);
  const control = new Int32Array(shared, 0, 16);
  const state = { failure: null, staging: [], copies: [], wait: null };
  const ops = new WebGPUOps();
  const source = { size: pattern.length, bytes: pattern.slice(), destroy() {} };
  ops.buffers.set(7, source);
  ops.device = {
    createBuffer({ size }) {
      if (state.failure === 'allocation') throw new Error('allocation failure');
      const staging = {
        size, bytes: new Uint8Array(size), unmapped: 0, destroyed: 0,
        async mapAsync() { if (state.failure === 'map') throw new Error('map failure'); },
        getMappedRange() {
          if (state.failure === 'read') throw new Error('read failure');
          return this.bytes.buffer;
        },
        unmap() { this.unmapped += 1; },
        destroy() { this.destroyed += 1; },
      };
      state.staging.push(staging);
      return staging;
    },
    createCommandEncoder() {
      let command;
      return {
        copyBufferToBuffer(src, offset, dst, dstOffset, size) {
          if (state.failure === 'copy') throw new Error('copy failure');
          assert.equal(src, source);
          assert.equal(offset % 4, 0);
          assert.equal(size % 4, 0);
          state.copies.push([offset, size]);
          command = () => dst.bytes.set(src.bytes.subarray(offset, offset + size), dstOffset);
        },
        finish() { return command; },
      };
    },
    queue: {
      submit(commands) { for (const command of commands) command(); },
      async onSubmittedWorkDone() {
        if (state.failure === 'queue') throw new Error('queue failure');
        if (state.wait) await state.wait;
      },
    },
  };
  return { ops, shared, control, state };
}

globalThis.GPUBufferUsage = { MAP_READ: 1, COPY_DST: 2 };
globalThis.GPUMapMode = { READ: 1 };

test('host GPU readback uses offsets, waits for completion, and defaults legacy offset to zero', async () => {
  const fixture = hostBridge();
  let release;
  fixture.state.wait = new Promise(resolve => { release = resolve; });
  const pending = fixture.ops.handleWorkerCommand({ type: 'gpu-sync', cmd: 'download', id: 7, offsetBytes: 24, size: 12 }, fixture.shared);
  assert.equal(Atomics.load(fixture.control, 0), 0);
  assert.equal(fixture.state.staging[0].destroyed, 0);
  release();
  await pending;
  assert.equal(Atomics.load(fixture.control, 0), 1);
  assert.equal(Atomics.load(fixture.control, 1), 0);
  assert.deepEqual(new Uint8Array(fixture.shared, DATA_OFFSET, 12), pattern.subarray(24, 36));
  assert.equal(fixture.state.staging[0].unmapped, 1);
  assert.equal(fixture.state.staging[0].destroyed, 1);

  await fixture.ops.handleWorkerCommand({ cmd: 'download', id: 7, size: 8 }, fixture.shared);
  assert.deepEqual(new Uint8Array(fixture.shared, DATA_OFFSET, 8), pattern.subarray(0, 8));
  assert.deepEqual(fixture.state.copies, [[24, 12], [0, 8]]);
});

test('host GPU readback failures release staging, wake the worker with failure, and retry', async () => {
  for (const phase of ['allocation', 'copy', 'queue', 'map', 'read']) {
    const fixture = hostBridge();
    fixture.state.failure = phase;
    new Uint8Array(fixture.shared, DATA_OFFSET).fill(0xee);
    await fixture.ops.handleWorkerCommand({ type: 'gpu-sync', cmd: 'download', id: 7, size: 12 }, fixture.shared);
    assert.equal(Atomics.load(fixture.control, 0), 1, phase);
    assert.equal(Atomics.load(fixture.control, 1), -1, phase);
    assert.ok(new Uint8Array(fixture.shared, DATA_OFFSET).every(value => value === 0xee));
    for (const staging of fixture.state.staging) {
      assert.equal(staging.destroyed, 1, phase);
      assert.equal(staging.unmapped, phase === 'read' ? 1 : 0, phase);
    }
    fixture.state.failure = null;
    Atomics.store(fixture.control, 0, 0);
    await fixture.ops.handleWorkerCommand({ type: 'gpu-sync', cmd: 'download', id: 7, size: 12 }, fixture.shared);
    assert.equal(Atomics.load(fixture.control, 0), 1);
    assert.equal(Atomics.load(fixture.control, 1), 0);
    assert.deepEqual(new Uint8Array(fixture.shared, DATA_OFFSET, 12), pattern.subarray(0, 12));
    for (const staging of fixture.state.staging) assert.equal(staging.destroyed, 1);
  }
});

test('host GPU readback validates ranges before allocation and async frees never acknowledge a wait', async () => {
  const fixture = hostBridge();
  for (const range of [{ size: 24 }, { size: 6 }, { size: 4, offsetBytes: -4 }, { size: 4, offsetBytes: 2 }, { size: 8, offsetBytes: 92 }, { size: 4, id: 99 }]) {
    Atomics.store(fixture.control, 0, 0);
    await fixture.ops.handleWorkerCommand({ type: 'gpu-sync', cmd: 'download', id: 7, ...range }, fixture.shared);
    assert.equal(Atomics.load(fixture.control, 0), 1);
    assert.equal(Atomics.load(fixture.control, 1), -1);
  }
  await fixture.ops.handleWorkerCommand({ type: 'gpu-sync', cmd: 'download', id: 7, size: 0 }, fixture.shared);
  assert.equal(Atomics.load(fixture.control, 1), 0);
  assert.equal(fixture.state.staging.length, 0);
  Atomics.store(fixture.control, 0, 0);
  Atomics.store(fixture.control, 1, 77);
  await fixture.ops.handleWorkerCommand({ type: 'gpu', cmd: 'free_buffer', id: 7 }, fixture.shared);
  assert.equal(Atomics.load(fixture.control, 0), 0);
  assert.equal(Atomics.load(fixture.control, 1), 77);
});
