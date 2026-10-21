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
