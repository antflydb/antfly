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

import fs from 'fs';
import { createWasmAbi } from './runtime/wasm-abi.js';
import { resolveWasmPath, wasmBuildHint } from './test-wasm-path.mjs';

function unavailableWebgpuImports(module) {
  const imports = Object.create(null);
  for (const { module: namespace, name, kind } of WebAssembly.Module.imports(module)) {
    if (namespace !== 'webgpu' || kind !== 'function') continue;
    imports[name] = name === 'gpu_is_available' ? () => 0 : () => {
      throw new Error(`CPU-only WASM test unexpectedly called ${name}`);
    };
  }
  return imports;
}

export async function instantiateAntflyInferenceWasm(root, options = {}) {
  const { wasmPath, memoryModel } = resolveWasmPath(root, options);
  if (!wasmPath) {
    throw new Error(`WASM module not found for ${memoryModel}. ${wasmBuildHint(memoryModel)}`);
  }

  const module = await WebAssembly.compile(fs.readFileSync(wasmPath));
  const importObject = {
    env: {},
    webgpu: options.webgpuStubs ?? unavailableWebgpuImports(module),
  };
  const instance = await WebAssembly.instantiate(module, importObject);
  const wasm = instance.exports;
  const abi = createWasmAbi(wasm);
  wasm.init();

  return {
    wasm,
    abi,
    wasmPath,
    memoryModel,
    alloc(byteLen) {
      const ptr = abi.alloc(byteLen);
      if (!ptr) throw new Error(`WASM alloc failed for ${byteLen} bytes`);
      return ptr;
    },
    free(ptr, byteLen) {
      abi.free(ptr, byteLen);
    },
    bytesIn(ptr, bytes) {
      abi.copyBytesIn(ptr, bytes);
    },
    write(Type, ptr, values) {
      abi.writeArray(Type, ptr, values);
    },
    read(Type, ptr, length) {
      return abi.readCopy(Type, ptr, length);
    },
    view(Type, ptr, length) {
      return abi.view(Type, ptr, length);
    },
    offset(ptr) {
      return abi.offset(ptr);
    },
    size(byteLen) {
      return abi.size(byteLen);
    },
  };
}
