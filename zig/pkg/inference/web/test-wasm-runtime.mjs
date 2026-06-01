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

import fs from 'fs';
import { createWasmAbi } from './runtime/wasm-abi.js';
import { resolveWasmPath, wasmBuildHint } from './test-wasm-path.mjs';

export const webgpuStubs = {
  gpu_is_available: () => 0,
  gpu_create_buffer: () => 0,
  gpu_free_buffer: () => {},
  gpu_upload: () => {},
  gpu_download: () => {},
  gpu_copy_buffer_to_buffer: () => {},
  gpu_matmul: () => {},
  gpu_matmul_transb: () => {},
  gpu_add: () => {},
  gpu_add_broadcast: () => {},
  gpu_mul: () => {},
  gpu_sub: () => {},
  gpu_div: () => {},
  gpu_less_than: () => {},
  gpu_where_select: () => {},
  gpu_neg: () => {},
  gpu_sqrt: () => {},
  gpu_rsqrt: () => {},
  gpu_exp: () => {},
  gpu_log: () => {},
  gpu_sin: () => {},
  gpu_cos: () => {},
  gpu_tanh: () => {},
  gpu_abs: () => {},
  gpu_erf: () => {},
  gpu_gelu: () => {},
  gpu_softmax: () => {},
  gpu_log_softmax: () => {},
  gpu_reduce_sum_last_dim: () => {},
  gpu_reduce_max_last_dim: () => {},
  gpu_reduce_mean_last_dim: () => {},
  gpu_reduce_sum: () => {},
  gpu_reduce_max: () => {},
  gpu_reduce_mean: () => {},
  gpu_broadcast_in_dim: () => {},
  gpu_matmul_transb_q4_0: () => {},
  gpu_matmul_transb_q4_1: () => {},
  gpu_matmul_transb_q5_0: () => {},
  gpu_matmul_transb_q5_1: () => {},
  gpu_matmul_transb_q8_0: () => {},
  gpu_matmul_transb_q8_1: () => {},
  gpu_matmul_transb_iq4_nl: () => {},
  gpu_matmul_transb_iq4_xs: () => {},
  gpu_matmul_transb_q2_k: () => {},
  gpu_matmul_transb_q3_k: () => {},
  gpu_matmul_transb_q4_k: () => {},
  gpu_matmul_transb_q5_k: () => {},
  gpu_matmul_transb_q6_k: () => {},
  gpu_matmul_transb_q8_k: () => {},
  gpu_matmul_transb_i2_s: () => {},
  gpu_attention: () => {},
  gpu_causal_attention: () => {},
  gpu_gqa_causal_attention: () => {},
  gpu_gqa_cached_attention: () => {},
  gpu_gqa_cached_attention_ex: () => {},
  gpu_write_buffer_at_offset: () => {},
  gpu_cross_attention: () => {},
  gpu_rms_norm: () => {},
  gpu_layer_norm: () => {},
};

export async function instantiateAntflyInferenceWasm(root, options = {}) {
  const { wasmPath, memoryModel } = resolveWasmPath(root, options);
  if (!wasmPath) {
    throw new Error(`WASM module not found for ${memoryModel}. ${wasmBuildHint(memoryModel)}`);
  }

  const wasmBytes = fs.readFileSync(wasmPath);
  const importObject = {
    env: {},
    webgpu: options.webgpuStubs ?? webgpuStubs,
  };
  const { instance } = await WebAssembly.instantiate(wasmBytes, importObject);
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
