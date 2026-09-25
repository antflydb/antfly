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

function detectMemoryModel(wasm) {
  try {
    const ptr = wasm.wasm_alloc(0);
    if (typeof ptr === 'number') {
      if (ptr) wasm.wasm_dealloc(ptr, 0);
      return 'wasm32';
    }
  } catch (_) {
    // Fall through to wasm64 probe.
  }

  try {
    const ptr = wasm.wasm_alloc(0n);
    if (typeof ptr === 'bigint') {
      if (ptr) wasm.wasm_dealloc(ptr, 0n);
      return 'wasm64';
    }
  } catch (_) {
    // Give the caller a clearer error below.
  }

  throw new Error('Unable to determine WASM host ABI for Antfly inference module');
}

export function createWasmAbi(wasm) {
  const memoryModel = detectMemoryModel(wasm);
  const isWasm64 = memoryModel === 'wasm64';

  function size(value) {
    if (!Number.isInteger(value) || value < 0) {
      throw new Error(`Invalid WASM size: ${value}`);
    }
    if (isWasm64) return BigInt(value);
    if (value > 0xffffffff) {
      throw new Error(`WASM32 size exceeds 4GB ceiling: ${value}`);
    }
    return value;
  }

  function offset(ptr) {
    if (typeof ptr === 'bigint') {
      if (ptr > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error(`WASM pointer exceeds JS safe integer range: ${ptr}`);
      }
      return Number(ptr);
    }
    return ptr;
  }

  function alloc(byteLen) {
    return wasm.wasm_alloc(size(byteLen));
  }

  function free(ptr, byteLen) {
    if (!ptr) return;
    wasm.wasm_dealloc(ptr, size(byteLen));
  }

  function view(Type, ptr, length) {
    return new Type(wasm.memory.buffer, offset(ptr), length);
  }

  function copyBytesIn(ptr, bytes) {
    view(Uint8Array, ptr, bytes.length).set(bytes);
  }

  function writeArray(Type, ptr, values) {
    view(Type, ptr, values.length).set(values);
  }

  function readCopy(Type, ptr, length) {
    const out = new Type(length);
    out.set(view(Type, ptr, length));
    return out;
  }

  return {
    memoryModel,
    isWasm64,
    size,
    offset,
    alloc,
    free,
    view,
    copyBytesIn,
    writeArray,
    readCopy,
  };
}
