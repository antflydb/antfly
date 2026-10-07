// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { createWasmAbi } from './wasm-abi.js';
import { ExtractionSession } from './extraction-session.js';
let session;
self.onmessage = async ({ data }) => {
  try {
    if (!session) {
      const response = await fetch(new URL('../antfly-extraction-cpu.wasm', import.meta.url));
      if (!response.ok) throw new Error('Missing CPU WASM asset for bundle verification');
      const { instance } = await WebAssembly.instantiate(await response.arrayBuffer(), { env: {} });
      session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
    }
    self.postMessage({ id: data.id, hash: await session.hash(data.file) });
  } catch (error) { self.postMessage({ id: data.id, error: error.message }); }
};
