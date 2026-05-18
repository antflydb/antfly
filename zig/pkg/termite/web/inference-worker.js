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

import { createWasmAbi } from './runtime/wasm-abi.js';
import { streamRegisterSafetensors } from './runtime/safetensors-stream.js';
import { streamRegisterGguf } from './runtime/gguf-stream.js';

// Inference Worker: runs Termite WASM module in a dedicated Web Worker.
//
// GPU operations are proxied to the main thread (which owns the WebGPU device)
// via SharedArrayBuffer + Atomics. This enables synchronous GPU downloads from
// WASM's perspective — the worker blocks with Atomics.wait() while the main
// thread performs the async staging buffer map.
//
// Protocol:
//   Fire-and-forget GPU ops (upload, matmul, attention, free):
//     Worker sends postMessage({type:'gpu', ...})
//     Worker continues immediately (no blocking)
//
//   Sync GPU ops (is_available, create_buffer, download):
//     Worker resets ctrl[0] = 0
//     Worker sends postMessage({type:'gpu-sync', ...})
//     Worker calls Atomics.wait(ctrl, 0, 0) → blocks
//     Main thread processes, writes result to ctrl[1], sets ctrl[0] = 1, notifies
//     Worker wakes, reads result
//
// SharedArrayBuffer layout:
//   Int32[0]: response flag (0 = pending, 1 = ready)
//   Int32[1]: result u32 value
//   Bytes 64+: data payload (for upload/download)

let wasm = null;
let abi = null;
let sab = null;   // SharedArrayBuffer
let ctrl = null;  // Int32Array view of control region (first 16 ints)

// ONNX Runtime Web (lazy, optional)
let ort = null;
let onnxSessions = new Map();
let onnxNextId = 1;

const DATA_OFFSET = 64;
const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

// --- GPU command helpers ---

function gpuSync(msg) {
  Atomics.store(ctrl, 0, 0);   // response not ready
  self.postMessage({ type: 'gpu-sync', ...msg });
  Atomics.wait(ctrl, 0, 0);    // block until main thread signals
  return ctrl[1];               // result value
}

function gpuAsync(msg, transfer) {
  self.postMessage({ type: 'gpu', ...msg }, transfer || []);
}

function gpuWriteBufferAtOffsetSync(id, offsetBytes, srcBytes) {
  const maxChunk = sab.byteLength - DATA_OFFSET;
  let offset = 0;
  while (offset < srcBytes.length) {
    const take = Math.min(maxChunk, srcBytes.length - offset);
    const dst = new Uint8Array(sab, DATA_OFFSET, take);
    dst.set(srcBytes.subarray(offset, offset + take));
    gpuSync({ cmd: 'write_buffer_at_offset', id, offsetBytes: offsetBytes + offset, sizeBytes: take });
    offset += take;
  }
}

function toJsIndex(value, label) {
  if (typeof value === 'bigint') {
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error(`${label} exceeds JS safe integer range: ${value}`);
    }
    return Number(value);
  }
  return value;
}

function readU32Params(memoryFn, ptr) {
  return Array.from(new Uint32Array(memoryFn().buffer, toJsIndex(ptr, 'WASM pointer'), 8));
}

function reduceMessage(memoryFn, cmd, input, out, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) {
  return {
    cmd,
    input,
    out,
    outLen,
    reduceCount,
    inRank,
    outRank,
    inputShape: readU32Params(memoryFn, inputShapePtr),
    outShape: readU32Params(memoryFn, outShapePtr),
    reduced: readU32Params(memoryFn, reducedPtr),
    inStrides: readU32Params(memoryFn, inStridesPtr),
    outStrides: readU32Params(memoryFn, outStridesPtr),
    keptAxes: readU32Params(memoryFn, keptAxesPtr),
    reducedAxes: readU32Params(memoryFn, reducedAxesPtr),
  };
}

function getGpuImports(memoryFn) {
  return {
    gpu_is_available: () => gpuSync({ cmd: 'is_available' }),

    gpu_create_buffer: (size) => gpuSync({ cmd: 'create_buffer', size }),

    gpu_free_buffer: (id) => {
      gpuAsync({ cmd: 'free_buffer', id });
    },

    gpu_upload: (id, ptr, size) => {
      // Copy WASM memory to SAB data region, then signal main thread
      const ptrIndex = toJsIndex(ptr, 'WASM pointer');
      const byteSize = toJsIndex(size, 'GPU upload size');
      const src = new Uint8Array(memoryFn().buffer, ptrIndex, byteSize);
      const dst = new Uint8Array(sab, DATA_OFFSET, byteSize);
      dst.set(src);
      gpuSync({ cmd: 'upload', id, size });
    },

    gpu_download: (id, ptr, size) => {
      // Block until main thread completes async download into SAB data region
      gpuSync({ cmd: 'download', id, size });
      // Copy from SAB data region to WASM memory
      const ptrIndex = toJsIndex(ptr, 'WASM pointer');
      const byteSize = toJsIndex(size, 'GPU download size');
      const src = new Uint8Array(sab, DATA_OFFSET, byteSize);
      const dst = new Uint8Array(memoryFn().buffer, ptrIndex, byteSize);
      dst.set(src);
    },

    gpu_copy_buffer_to_buffer: (src, srcOffsetBytes, dst, dstOffsetBytes, sizeBytes) => {
      gpuAsync({ cmd: 'copy_buffer_to_buffer', src, srcOffsetBytes, dst, dstOffsetBytes, sizeBytes });
    },

    gpu_matmul: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul', a, b, out, m, n, k });
    },

    gpu_matmul_transb: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb', a, b, out, m, n, k });
    },

    gpu_add: (a, b, out, len) => {
      gpuAsync({ cmd: 'add', a, b, out, len });
    },

    gpu_add_broadcast: (a, b, out, len, aLen, bLen) => {
      gpuAsync({ cmd: 'add_broadcast', a, b, out, len, aLen, bLen });
    },

    gpu_mul: (a, b, out, len, aLen, bLen) => {
      gpuAsync({ cmd: 'mul', a, b, out, len, aLen, bLen });
    },

    gpu_sub: (a, b, out, len, aLen, bLen) => {
      gpuAsync({ cmd: 'sub', a, b, out, len, aLen, bLen });
    },

    gpu_div: (a, b, out, len, aLen, bLen) => {
      gpuAsync({ cmd: 'div', a, b, out, len, aLen, bLen });
    },

    gpu_less_than: (a, b, out, len, aLen, bLen) => {
      gpuAsync({ cmd: 'less_than', a, b, out, len, aLen, bLen });
    },

    gpu_where_select: (cond, onTrue, onFalse, out, len, trueLen, falseLen) => {
      gpuAsync({ cmd: 'where_select', cond, onTrue, onFalse, out, len, trueLen, falseLen });
    },

    gpu_neg: (input, out, len) => {
      gpuAsync({ cmd: 'neg', input, out, len });
    },

    gpu_sqrt: (input, out, len) => {
      gpuAsync({ cmd: 'sqrt', input, out, len });
    },

    gpu_rsqrt: (input, out, len) => {
      gpuAsync({ cmd: 'rsqrt', input, out, len });
    },

    gpu_exp: (input, out, len) => {
      gpuAsync({ cmd: 'exp', input, out, len });
    },

    gpu_log: (input, out, len) => {
      gpuAsync({ cmd: 'log', input, out, len });
    },

    gpu_sin: (input, out, len) => {
      gpuAsync({ cmd: 'sin', input, out, len });
    },

    gpu_cos: (input, out, len) => {
      gpuAsync({ cmd: 'cos', input, out, len });
    },

    gpu_tanh: (input, out, len) => {
      gpuAsync({ cmd: 'tanh', input, out, len });
    },

    gpu_abs: (input, out, len) => {
      gpuAsync({ cmd: 'abs', input, out, len });
    },

    gpu_erf: (input, out, len) => {
      gpuAsync({ cmd: 'erf', input, out, len });
    },

    gpu_gelu: (input, out, len) => {
      gpuAsync({ cmd: 'gelu', input, out, len });
    },

    gpu_softmax: (input, out, rows, dim) => {
      gpuAsync({ cmd: 'softmax', input, out, rows, dim });
    },

    gpu_log_softmax: (input, out, rows, dim) => {
      gpuAsync({ cmd: 'log_softmax', input, out, rows, dim });
    },

    gpu_reduce_sum_last_dim: (input, out, rows, dim) => {
      gpuAsync({ cmd: 'reduce_sum_last_dim', input, out, rows, dim });
    },

    gpu_reduce_max_last_dim: (input, out, rows, dim) => {
      gpuAsync({ cmd: 'reduce_max_last_dim', input, out, rows, dim });
    },

    gpu_reduce_mean_last_dim: (input, out, rows, dim) => {
      gpuAsync({ cmd: 'reduce_mean_last_dim', input, out, rows, dim });
    },

    gpu_reduce_sum: (input, out, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) => {
      gpuAsync(reduceMessage(memoryFn, 'reduce_sum', input, out, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr));
    },

    gpu_reduce_max: (input, out, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) => {
      gpuAsync(reduceMessage(memoryFn, 'reduce_max', input, out, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr));
    },

    gpu_reduce_mean: (input, out, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) => {
      gpuAsync(reduceMessage(memoryFn, 'reduce_mean', input, out, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr));
    },

    gpu_broadcast_in_dim: (input, out, outLen, outRank, inRank, targetShapePtr, inputShapePtr, axesPtr, outStridesPtr, inStridesPtr) => {
      gpuAsync({
        cmd: 'broadcast_in_dim',
        input,
        out,
        outLen,
        outRank,
        inRank,
        targetShape: readU32Params(memoryFn, targetShapePtr),
        inputShape: readU32Params(memoryFn, inputShapePtr),
        axes: readU32Params(memoryFn, axesPtr),
        outStrides: readU32Params(memoryFn, outStridesPtr),
        inStrides: readU32Params(memoryFn, inStridesPtr),
      });
    },

    gpu_matmul_transb_q4_0: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q4_0', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q4_1: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q4_1', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q5_0: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q5_0', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q5_1: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q5_1', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q8_0: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q8_0', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q8_1: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q8_1', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq4_nl: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq4_nl', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq4_xs: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq4_xs', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q2_k: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q2_k', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q3_k: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q3_k', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q4_k: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q4_k', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q5_k: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q5_k', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q6_k: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q6_k', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q8_k: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q8_k', a, b, out, m, n, k });
    },

    gpu_matmul_transb_i2_s: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_i2_s', a, b, out, m, n, k });
    },

    gpu_matmul_transb_i8_s: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_i8_s', a, b, out, m, n, k });
    },

    gpu_matmul_transb_q1_0: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_q1_0', a, b, out, m, n, k });
    },

    gpu_matmul_transb_tq1_0: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_tq1_0', a, b, out, m, n, k });
    },

    gpu_matmul_transb_tq2_0: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_tq2_0', a, b, out, m, n, k });
    },

    gpu_matmul_transb_mxfp4: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_mxfp4', a, b, out, m, n, k });
    },

    gpu_matmul_transb_nvfp4: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_nvfp4', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq1_s: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq1_s', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq1_m: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq1_m', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq2_xxs: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq2_xxs', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq2_xs: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq2_xs', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq2_s: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq2_s', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq3_xxs: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq3_xxs', a, b, out, m, n, k });
    },

    gpu_matmul_transb_iq3_s: (a, b, out, m, n, k) => {
      gpuAsync({ cmd: 'matmul_transb_iq3_s', a, b, out, m, n, k });
    },

    // MMV (qLen=1) variants — see web/shaders/matmul_transb_<fmt>_mmv.wgsl.
    gpu_matmul_transb_q4_0_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q4_0_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q4_1_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q4_1_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q5_0_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q5_0_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q5_1_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q5_1_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q8_0_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q8_0_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q8_1_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q8_1_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_iq4_nl_mmv:  (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq4_nl_mmv',  a, b, out, m, n, k }),
    gpu_matmul_transb_iq4_xs_mmv:  (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq4_xs_mmv',  a, b, out, m, n, k }),
    gpu_matmul_transb_q2_k_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q2_k_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q3_k_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q3_k_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q4_k_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q4_k_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q5_k_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q5_k_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q6_k_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q6_k_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q8_k_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q8_k_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_i8_s_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_i8_s_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_q1_0_mmv:    (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_q1_0_mmv',    a, b, out, m, n, k }),
    gpu_matmul_transb_tq1_0_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_tq1_0_mmv',   a, b, out, m, n, k }),
    gpu_matmul_transb_tq2_0_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_tq2_0_mmv',   a, b, out, m, n, k }),
    gpu_matmul_transb_mxfp4_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_mxfp4_mmv',   a, b, out, m, n, k }),
    gpu_matmul_transb_nvfp4_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_nvfp4_mmv',   a, b, out, m, n, k }),
    gpu_matmul_transb_iq1_s_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq1_s_mmv',   a, b, out, m, n, k }),
    gpu_matmul_transb_iq1_m_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq1_m_mmv',   a, b, out, m, n, k }),
    gpu_matmul_transb_iq2_xxs_mmv: (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq2_xxs_mmv', a, b, out, m, n, k }),
    gpu_matmul_transb_iq2_xs_mmv:  (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq2_xs_mmv',  a, b, out, m, n, k }),
    gpu_matmul_transb_iq2_s_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq2_s_mmv',   a, b, out, m, n, k }),
    gpu_matmul_transb_iq3_xxs_mmv: (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq3_xxs_mmv', a, b, out, m, n, k }),
    gpu_matmul_transb_iq3_s_mmv:   (a, b, out, m, n, k) => gpuAsync({ cmd: 'matmul_transb_iq3_s_mmv',   a, b, out, m, n, k }),

    gpu_attention: (q, k, v, mask, out, batch, seqLen, numHeads, headDim) => {
      gpuAsync({ cmd: 'attention', q, k, v, mask, out, batch, seqLen, numHeads, headDim });
    },

    gpu_deberta_disentangled_attention: (q, k, v, qRel, kRel, mask, out, batch, seqLen, numHeads, headDim) => {
      gpuAsync({ cmd: 'deberta_disentangled_attention', q, k, v, qRel, kRel, mask, out, batch, seqLen, numHeads, headDim });
    },

    gpu_causal_attention: (q, k, v, out, batch, seqLen, numHeads, headDim) => {
      gpuAsync({ cmd: 'causal_attention', q, k, v, out, batch, seqLen, numHeads, headDim });
    },

    gpu_gqa_causal_attention: (q, k, v, out, batch, seqLen, numHeads, numKvHeads, headDim) => {
      gpuAsync({ cmd: 'gqa_causal_attention', q, k, v, out, batch, seqLen, numHeads, numKvHeads, headDim });
    },

    gpu_gqa_cached_attention: (q, k, v, out, batch, qLen, kvLen, numHeads, numKvHeads, headDim) => {
      gpuAsync({ cmd: 'gqa_cached_attention', q, k, v, out, batch, qLen, kvLen, numHeads, numKvHeads, headDim });
    },

    gpu_gqa_cached_attention_ex: (q, kMain, kAux, v, out, batch, qLen, kvLen, numHeads, numKvHeads, headDim, kFormat, vFormat, kRowBytes, vRowBytes, flags) => {
      gpuAsync({ cmd: 'gqa_cached_attention_ex', q, kMain, kAux, v, out, batch, qLen, kvLen, numHeads, numKvHeads, headDim, kFormat, vFormat, kRowBytes, vRowBytes, flags });
    },

    gpu_write_buffer_at_offset: (id, offsetBytes, ptr, sizeBytes) => {
      const ptrIndex = toJsIndex(ptr, 'WASM pointer');
      const byteSize = toJsIndex(sizeBytes, 'GPU upload size');
      const src = new Uint8Array(memoryFn().buffer, ptrIndex, byteSize);
      gpuWriteBufferAtOffsetSync(id, toJsIndex(offsetBytes, 'GPU buffer offset'), src);
    },

    gpu_cross_attention: (q, k, v, mask, out, batch, decSeq, encSeq, numHeads, headDim) => {
      gpuAsync({ cmd: 'cross_attention', q, k, v, mask, out, batch, decSeq, encSeq, numHeads, headDim });
    },

    gpu_rms_norm: (input, weight, out, totalRows, dim, epsBits) => {
      gpuAsync({ cmd: 'rms_norm', input, weight, out, totalRows, dim, epsBits });
    },

    gpu_layer_norm: (input, gamma, beta, out, totalRows, dim, epsBits) => {
      gpuAsync({ cmd: 'layer_norm', input, gamma, beta, out, totalRows, dim, epsBits });
    },
  };
}

// --- Message handler ---

let nextReqId = 1;

async function instantiateFromWasmUrls(wasmUrls, importObject) {
  let lastError = null;
  for (const wasmUrl of wasmUrls) {
    try {
      const resp = await fetch(wasmUrl);
      if (!resp.ok) {
        throw new Error(`Failed to fetch ${wasmUrl}: ${resp.status} ${resp.statusText}`);
      }
      return WebAssembly.instantiateStreaming(resp, importObject);
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError ?? new Error('Unable to instantiate termite WASM module in worker');
}

self.onmessage = async (e) => {
  const { type, id } = e.data;

  try {
    switch (type) {
      case 'init': {
        sab = e.data.sharedBuffer;
        ctrl = new Int32Array(sab, 0, 16);

        const wasmUrls = e.data.wasmUrls ?? (e.data.wasmUrl ? [e.data.wasmUrl] : []);
        const hasGpu = e.data.hasGpu;

        let wasmMemory = null;
        const memoryFn = () => wasmMemory;

        const importObject = { env: {} };

        if (hasGpu) {
          importObject.webgpu = getGpuImports(memoryFn);
        } else {
          importObject.webgpu = {
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
            gpu_matmul_transb_i8_s: () => {},
            gpu_matmul_transb_q1_0: () => {},
            gpu_matmul_transb_tq1_0: () => {},
            gpu_matmul_transb_tq2_0: () => {},
            gpu_matmul_transb_mxfp4: () => {},
            gpu_matmul_transb_nvfp4: () => {},
            gpu_matmul_transb_iq1_s: () => {},
            gpu_matmul_transb_iq1_m: () => {},
            gpu_matmul_transb_iq2_xxs: () => {},
            gpu_matmul_transb_iq2_xs: () => {},
            gpu_matmul_transb_iq2_s: () => {},
            gpu_matmul_transb_iq3_xxs: () => {},
            gpu_matmul_transb_iq3_s: () => {},
            gpu_matmul_transb_q4_0_mmv: () => {},
            gpu_matmul_transb_q4_1_mmv: () => {},
            gpu_matmul_transb_q5_0_mmv: () => {},
            gpu_matmul_transb_q5_1_mmv: () => {},
            gpu_matmul_transb_q8_0_mmv: () => {},
            gpu_matmul_transb_q8_1_mmv: () => {},
            gpu_matmul_transb_iq4_nl_mmv: () => {},
            gpu_matmul_transb_iq4_xs_mmv: () => {},
            gpu_matmul_transb_q2_k_mmv: () => {},
            gpu_matmul_transb_q3_k_mmv: () => {},
            gpu_matmul_transb_q4_k_mmv: () => {},
            gpu_matmul_transb_q5_k_mmv: () => {},
            gpu_matmul_transb_q6_k_mmv: () => {},
            gpu_matmul_transb_q8_k_mmv: () => {},
            gpu_matmul_transb_i8_s_mmv: () => {},
            gpu_matmul_transb_q1_0_mmv: () => {},
            gpu_matmul_transb_tq1_0_mmv: () => {},
            gpu_matmul_transb_tq2_0_mmv: () => {},
            gpu_matmul_transb_mxfp4_mmv: () => {},
            gpu_matmul_transb_nvfp4_mmv: () => {},
            gpu_matmul_transb_iq1_s_mmv: () => {},
            gpu_matmul_transb_iq1_m_mmv: () => {},
            gpu_matmul_transb_iq2_xxs_mmv: () => {},
            gpu_matmul_transb_iq2_xs_mmv: () => {},
            gpu_matmul_transb_iq2_s_mmv: () => {},
            gpu_matmul_transb_iq3_xxs_mmv: () => {},
            gpu_matmul_transb_iq3_s_mmv: () => {},
            gpu_attention: () => {},
            gpu_deberta_disentangled_attention: () => {},
            gpu_causal_attention: () => {},
            gpu_gqa_causal_attention: () => {},
            gpu_gqa_cached_attention: () => {},
            gpu_gqa_cached_attention_ex: () => {},
            gpu_write_buffer_at_offset: () => {},
            gpu_cross_attention: () => {},
            gpu_rms_norm: () => {},
            gpu_layer_norm: () => {},
          };
        }

        const source = await instantiateFromWasmUrls(wasmUrls, importObject);
        wasm = source.instance.exports;
        abi = createWasmAbi(wasm);
        wasmMemory = wasm.memory;
        wasm.init();

        self.postMessage({ type: 'init-done', id });
        break;
      }

      case 'load-model': {
        const { modelBytes, configJson, format } = e.data;

        const modelPtr = abi.alloc(modelBytes.length);
        if (!modelPtr) throw new Error('WASM alloc failed for model');
        abi.copyBytesIn(modelPtr, modelBytes);

        const configBytes = new TextEncoder().encode(configJson);
        const configPtr = abi.alloc(configBytes.length);
        if (!configPtr) {
          abi.free(modelPtr, modelBytes.length);
          throw new Error('WASM alloc failed for config');
        }
        abi.copyBytesIn(configPtr, configBytes);

        const loadFn = format === 'gguf' ? wasm.load_model_gguf : wasm.load_model_safetensors;
        const handle = loadFn(
          modelPtr,
          abi.size(modelBytes.length),
          configPtr,
          abi.size(configBytes.length),
        );

        abi.free(modelPtr, modelBytes.length);
        abi.free(configPtr, configBytes.length);

        if (handle === 0) throw new Error('Failed to load model');
        self.postMessage({ type: 'load-model-done', id, handle });
        break;
      }

      case 'unload-model': {
        wasm.unload_model(e.data.handle);
        self.postMessage({ type: 'unload-model-done', id });
        break;
      }

      case 'load-projector': {
        const { projectorBytes } = e.data;
        const ptr = abi.alloc(projectorBytes.length);
        if (!ptr) throw new Error('WASM alloc failed for projector');
        abi.copyBytesIn(ptr, projectorBytes);

        const handle = wasm.load_projector_gguf(ptr, abi.size(projectorBytes.length));
        abi.free(ptr, projectorBytes.length);

        if (handle === 0) throw new Error('Failed to load projector GGUF');
        const rawKind = wasm.projector_kind(handle);
        self.postMessage({
          type: 'load-projector-done',
          id,
          handle,
          kind: rawKind === 1 ? 'termite_gemma3'
            : rawKind === 2 ? 'clip_gemma4_image'
            : rawKind === 3 ? 'clip_gemma4_audio'
            : rawKind === 4 ? 'clip_gemma4_image_audio'
            : 'unknown',
        });
        break;
      }

      case 'unload-projector': {
        wasm.unload_projector(e.data.handle);
        self.postMessage({ type: 'unload-projector-done', id });
        break;
      }

      case 'load-tokenizer': {
        const { tokenizerBytes, format = 'json' } = e.data;
        const ptr = abi.alloc(tokenizerBytes.length);
        if (!ptr) throw new Error('WASM alloc failed for tokenizer');
        abi.copyBytesIn(ptr, tokenizerBytes);

        const handle = format === 'gguf'
          ? wasm.load_tokenizer_gguf(ptr, abi.size(tokenizerBytes.length))
          : wasm.load_tokenizer(ptr, abi.size(tokenizerBytes.length));
        abi.free(ptr, tokenizerBytes.length);

        if (handle === 0) throw new Error('Failed to load tokenizer');
        self.postMessage({ type: 'load-tokenizer-done', id, handle });
        break;
      }

      case 'unload-tokenizer': {
        wasm.unload_tokenizer(e.data.handle);
        self.postMessage({ type: 'unload-tokenizer-done', id });
        break;
      }

      case 'decode-tokens': {
        const { tokenizerHandle, tokenIds } = e.data;
        const ids = Int32Array.from(tokenIds);
        const idsByteLen = ids.length * 4;
        const idsPtr = abi.alloc(idsByteLen);
        if (!idsPtr) throw new Error('WASM alloc failed for decode token IDs');
        abi.writeArray(Int32Array, idsPtr, ids);

        const outLenPtr = abi.alloc(4);
        if (!outLenPtr) {
          abi.free(idsPtr, idsByteLen);
          throw new Error('WASM alloc failed for decode metadata');
        }

        const textPtr = wasm.decode_tokens(
          tokenizerHandle,
          idsPtr,
          abi.size(ids.length),
          outLenPtr,
        );
        const textLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];
        abi.free(outLenPtr, 4);
        abi.free(idsPtr, idsByteLen);

        if (!textPtr) throw new Error('Token decode failed');
        const textBytes = abi.readCopy(Uint8Array, textPtr, textLen);
        abi.free(textPtr, textLen);

        self.postMessage({
          type: 'decode-tokens-done',
          id,
          text: TEXT_DECODER.decode(textBytes),
        });
        break;
      }

      case 'render-chat-prompt': {
        const { tokenizerHandle, templateSource, userPrompt, systemPrompt = '', addGenerationPrompt = true } = e.data;
        const templateBytes = TEXT_ENCODER.encode(templateSource);
        const userBytes = TEXT_ENCODER.encode(userPrompt);
        const systemBytes = TEXT_ENCODER.encode(systemPrompt);

        const templateAllocLen = Math.max(1, templateBytes.length);
        const userAllocLen = Math.max(1, userBytes.length);
        const systemAllocLen = Math.max(1, systemBytes.length);

        const templatePtr = abi.alloc(templateAllocLen);
        const userPtr = abi.alloc(userAllocLen);
        const systemPtr = abi.alloc(systemAllocLen);
        if (!templatePtr || !userPtr || !systemPtr) {
          throw new Error('WASM alloc failed for chat template render input');
        }
        if (templateBytes.length > 0) abi.copyBytesIn(templatePtr, templateBytes);
        if (userBytes.length > 0) abi.copyBytesIn(userPtr, userBytes);
        if (systemBytes.length > 0) abi.copyBytesIn(systemPtr, systemBytes);

        const outLenPtr = abi.alloc(4);
        if (!outLenPtr) {
          abi.free(templatePtr, templateAllocLen);
          abi.free(userPtr, userAllocLen);
          abi.free(systemPtr, systemAllocLen);
          throw new Error('WASM alloc failed for chat template render metadata');
        }

        const textPtr = wasm.render_chat_prompt(
          tokenizerHandle,
          templatePtr,
          abi.size(templateBytes.length),
          systemPtr,
          abi.size(systemBytes.length),
          userPtr,
          abi.size(userBytes.length),
          addGenerationPrompt ? 1 : 0,
          outLenPtr,
        );
        const textLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];

        abi.free(outLenPtr, 4);
        abi.free(templatePtr, templateAllocLen);
        abi.free(userPtr, userAllocLen);
        abi.free(systemPtr, systemAllocLen);

        if (!textPtr || textLen === 0) throw new Error('Chat prompt render failed');
        const textBytes = abi.readCopy(Uint8Array, textPtr, textLen);
        abi.free(textPtr, textLen);

        self.postMessage({
          type: 'render-chat-prompt-done',
          id,
          text: TEXT_DECODER.decode(textBytes),
        });
        break;
      }

      case 'embed': {
        const { modelHandle, tokenIds, attentionMask, batchSize, seqLen } = e.data;

        const ids = new BigInt64Array(tokenIds.map(BigInt));
        const mask = new BigInt64Array(attentionMask.map(BigInt));

        const idsByteLen = ids.length * 8;
        const maskByteLen = mask.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        const maskPtr = abi.alloc(maskByteLen);
        if (!idsPtr || !maskPtr) throw new Error('WASM alloc failed for embed inputs');

        abi.writeArray(BigInt64Array, idsPtr, ids);
        abi.writeArray(BigInt64Array, maskPtr, mask);

        const maxOutputFloats = batchSize * 4096;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) throw new Error('WASM alloc failed for embed output');

        const resultLen = wasm.embed(
          modelHandle, idsPtr, abi.size(ids.length), maskPtr, abi.size(mask.length),
          batchSize, seqLen, outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(idsPtr, idsByteLen);
        abi.free(maskPtr, maskByteLen);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('Embedding inference failed');
        self.postMessage({ type: 'embed-done', id, output }, [output.buffer]);
        break;
      }

      case 'audio-whisper-mel': {
        const pcm = e.data.pcm;
        const sampleRate = e.data.sampleRate;
        const channels = e.data.channels;

        const pcmByteLen = pcm.length * 4;
        const pcmPtr = abi.alloc(pcmByteLen);
        if (!pcmPtr) throw new Error('WASM alloc failed for audio PCM');
        abi.writeArray(Float32Array, pcmPtr, pcm);

        const outLen = wasm.audio_whisper_mel_size();
        const outByteLen = outLen * 4;
        const outPtr = abi.alloc(outByteLen);
        const metaPtr = abi.alloc(8);
        if (!outPtr || !metaPtr) throw new Error('WASM alloc failed for Whisper audio output');

        const written = wasm.audio_whisper_mel_interleaved(
          pcmPtr,
          abi.size(pcm.length),
          sampleRate,
          channels,
          outPtr,
          metaPtr,
        );

        let data = null;
        let nMels = 0;
        let nFrames = 0;
        if (written > 0) {
          data = abi.readCopy(Float32Array, outPtr, written);
          const meta = abi.readCopy(Uint32Array, metaPtr, 2);
          nMels = meta[0];
          nFrames = meta[1];
        }

        abi.free(pcmPtr, pcmByteLen);
        abi.free(outPtr, outByteLen);
        abi.free(metaPtr, 8);

        if (!data) throw new Error('Whisper audio preprocessing failed');
        self.postMessage({ type: 'audio-whisper-mel-done', id, data, nMels, nFrames }, [data.buffer]);
        break;
      }

      case 'audio-clap-features': {
        const pcm = e.data.pcm;
        const sampleRate = e.data.sampleRate;
        const inputChannels = e.data.inputChannels;
        const outputChannels = e.data.outputChannels;

        const pcmByteLen = pcm.length * 4;
        const pcmPtr = abi.alloc(pcmByteLen);
        if (!pcmPtr) throw new Error('WASM alloc failed for audio PCM');
        abi.writeArray(Float32Array, pcmPtr, pcm);

        const outLen = wasm.audio_clap_feature_size(outputChannels);
        const outByteLen = outLen * 4;
        const outPtr = abi.alloc(outByteLen);
        const metaPtr = abi.alloc(16);
        if (!outPtr || !metaPtr) throw new Error('WASM alloc failed for CLAP audio output');

        const written = wasm.audio_clap_features_interleaved(
          pcmPtr,
          abi.size(pcm.length),
          sampleRate,
          inputChannels,
          outputChannels,
          outPtr,
          metaPtr,
        );

        let data = null;
        let channelsUsed = 0;
        let timeFrames = 0;
        let melBins = 0;
        let isLonger = false;
        if (written > 0) {
          data = abi.readCopy(Float32Array, outPtr, written);
          const meta = abi.readCopy(Uint32Array, metaPtr, 4);
          channelsUsed = meta[0];
          timeFrames = meta[1];
          melBins = meta[2];
          isLonger = meta[3] !== 0;
        }

        abi.free(pcmPtr, pcmByteLen);
        abi.free(outPtr, outByteLen);
        abi.free(metaPtr, 16);

        if (!data) throw new Error('CLAP audio preprocessing failed');
        self.postMessage({
          type: 'audio-clap-features-done',
          id,
          data,
          channelsUsed,
          timeFrames,
          melBins,
          isLonger,
        }, [data.buffer]);
        break;
      }

      case 'tokenize': {
        const { tokenizerHandle, text, maxLen } = e.data;
        const textBytes = new TextEncoder().encode(text);
        const textPtr = abi.alloc(textBytes.length);
        if (!textPtr) throw new Error('WASM alloc failed for text');
        abi.copyBytesIn(textPtr, textBytes);

        const idsByteLen = maxLen * 4;
        const idsPtr = abi.alloc(idsByteLen);
        const maskPtr = abi.alloc(idsByteLen);
        if (!idsPtr || !maskPtr) throw new Error('WASM alloc failed for tokenize output');

        const result = wasm.tokenize(
          tokenizerHandle,
          textPtr,
          abi.size(textBytes.length),
          maxLen,
          idsPtr,
          maskPtr,
        );

        let ids, mask;
        if (result > 0) {
          ids = abi.readCopy(Int32Array, idsPtr, maxLen);
          mask = abi.readCopy(Int32Array, maskPtr, maxLen);
        }

        abi.free(textPtr, textBytes.length);
        abi.free(idsPtr, idsByteLen);
        abi.free(maskPtr, idsByteLen);

        if (!ids) throw new Error('Tokenization failed');
        self.postMessage({ type: 'tokenize-done', id, ids, mask }, [ids.buffer, mask.buffer]);
        break;
      }

      case 'tokenize-pair': {
        const { tokenizerHandle, textA, textB, maxLen } = e.data;
        const aBytes = new TextEncoder().encode(textA);
        const bBytes = new TextEncoder().encode(textB);

        const aPtr = abi.alloc(aBytes.length);
        const bPtr = abi.alloc(bBytes.length);
        if (!aPtr || !bPtr) throw new Error('WASM alloc failed for text');
        abi.copyBytesIn(aPtr, aBytes);
        abi.copyBytesIn(bPtr, bBytes);

        const idsByteLen = maxLen * 4;
        const idsPtr = abi.alloc(idsByteLen);
        const maskPtr = abi.alloc(idsByteLen);
        if (!idsPtr || !maskPtr) throw new Error('WASM alloc failed for tokenize output');

        const result = wasm.tokenize_pair(
          tokenizerHandle, aPtr, abi.size(aBytes.length), bPtr, abi.size(bBytes.length),
          maxLen, idsPtr, maskPtr,
        );

        let ids, mask;
        if (result > 0) {
          ids = abi.readCopy(Int32Array, idsPtr, maxLen);
          mask = abi.readCopy(Int32Array, maskPtr, maxLen);
        }

        abi.free(aPtr, aBytes.length);
        abi.free(bPtr, bBytes.length);
        abi.free(idsPtr, idsByteLen);
        abi.free(maskPtr, idsByteLen);

        if (!ids) throw new Error('Pair tokenization failed');
        self.postMessage({ type: 'tokenize-pair-done', id, ids, mask }, [ids.buffer, mask.buffer]);
        break;
      }

      case 'load-model-gliner': {
        const { modelBytes, configJson } = e.data;

        const modelPtr = abi.alloc(modelBytes.length);
        if (!modelPtr) throw new Error('WASM alloc failed for model');
        abi.copyBytesIn(modelPtr, modelBytes);

        const configBytes = new TextEncoder().encode(configJson);
        const configPtr = abi.alloc(configBytes.length);
        if (!configPtr) {
          abi.free(modelPtr, modelBytes.length);
          throw new Error('WASM alloc failed for config');
        }
        abi.copyBytesIn(configPtr, configBytes);

        const handle = wasm.load_model_gliner(
          modelPtr,
          abi.size(modelBytes.length),
          configPtr,
          abi.size(configBytes.length),
        );

        abi.free(modelPtr, modelBytes.length);
        abi.free(configPtr, configBytes.length);

        if (handle === 0) throw new Error('Failed to load GLiNER model');
        self.postMessage({ type: 'load-model-gliner-done', id, handle });
        break;
      }

      case 'tokenize-raw': {
        const { tokenizerHandle, text, maxIds } = e.data;
        const textBytes = TEXT_ENCODER.encode(text);
        const textPtr = abi.alloc(textBytes.length);
        if (!textPtr) throw new Error('WASM alloc failed for text');
        abi.copyBytesIn(textPtr, textBytes);

        const outByteLen = maxIds * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) throw new Error('WASM alloc failed for output');

        const numIds = wasm.tokenize_raw(
          tokenizerHandle,
          textPtr,
          abi.size(textBytes.length),
          outPtr,
          maxIds,
        );

        let ids;
        if (numIds > 0) {
          ids = abi.readCopy(Int32Array, outPtr, numIds);
        }

        abi.free(textPtr, textBytes.length);
        abi.free(outPtr, outByteLen);

        if (!ids) throw new Error('Raw tokenization failed');
        self.postMessage({ type: 'tokenize-raw-done', id, ids }, [ids.buffer]);
        break;
      }

      case 'gliner': {
        const { modelHandle, inputIds, attentionMask, wordsMask, spanIdx, batchSize, seqLen } = e.data;

        const idsI64 = new BigInt64Array(inputIds.map(BigInt));
        const maskI64 = new BigInt64Array(attentionMask.map(BigInt));
        const wmaskI64 = new BigInt64Array(wordsMask.map(BigInt));
        const spanI64 = new BigInt64Array(spanIdx.map(BigInt));

        const idsByteLen = idsI64.length * 8;
        const maskByteLen = maskI64.length * 8;
        const wmaskByteLen = wmaskI64.length * 8;
        const spanByteLen = spanI64.length * 8;

        const idsPtr = abi.alloc(idsByteLen);
        const maskPtr = abi.alloc(maskByteLen);
        const wmaskPtr = abi.alloc(wmaskByteLen);
        const spanPtr = abi.alloc(spanByteLen);
        if (!idsPtr || !maskPtr || !wmaskPtr || !spanPtr) throw new Error('WASM alloc failed');

        abi.writeArray(BigInt64Array, idsPtr, idsI64);
        abi.writeArray(BigInt64Array, maskPtr, maskI64);
        abi.writeArray(BigInt64Array, wmaskPtr, wmaskI64);
        abi.writeArray(BigInt64Array, spanPtr, spanI64);

        // Output buffer
        const maxLogits = 1024 * 1024; // generous upper bound
        const logitsByteLen = maxLogits * 4;
        const logitsPtr = abi.alloc(logitsByteLen);
        const metaByteLen = 3 * 4;
        const metaPtr = abi.alloc(metaByteLen);
        if (!logitsPtr || !metaPtr) throw new Error('WASM alloc failed for output');

        const numLogits = wasm.gliner(
          modelHandle,
          idsPtr, abi.size(idsI64.length),
          maskPtr, abi.size(maskI64.length),
          wmaskPtr, abi.size(wmaskI64.length),
          spanPtr, abi.size(spanI64.length),
          batchSize, seqLen,
          logitsPtr, metaPtr,
        );

        let logits = null;
        let meta = null;
        if (numLogits > 0) {
          logits = abi.readCopy(Float32Array, logitsPtr, numLogits);
          meta = abi.readCopy(Uint32Array, metaPtr, 3);
        }

        abi.free(idsPtr, idsByteLen);
        abi.free(maskPtr, maskByteLen);
        abi.free(wmaskPtr, wmaskByteLen);
        abi.free(spanPtr, spanByteLen);
        abi.free(logitsPtr, logitsByteLen);
        abi.free(metaPtr, metaByteLen);

        if (!logits) throw new Error('GLiNER inference failed');
        self.postMessage({ type: 'gliner-done', id, logits, meta }, [logits.buffer, meta.buffer]);
        break;
      }

      case 'load-model-clip': {
        const { modelBytes, configJson } = e.data;

        const modelPtr = abi.alloc(modelBytes.length);
        if (!modelPtr) throw new Error('WASM alloc failed for model');
        abi.copyBytesIn(modelPtr, modelBytes);

        const configBytes = new TextEncoder().encode(configJson);
        const configPtr = abi.alloc(configBytes.length);
        if (!configPtr) {
          abi.free(modelPtr, modelBytes.length);
          throw new Error('WASM alloc failed for config');
        }
        abi.copyBytesIn(configPtr, configBytes);

        const handle = wasm.load_model_clip(
          modelPtr,
          abi.size(modelBytes.length),
          configPtr,
          abi.size(configBytes.length),
        );

        abi.free(modelPtr, modelBytes.length);
        abi.free(configPtr, configBytes.length);

        if (handle === 0) throw new Error('Failed to load CLIP model');
        self.postMessage({ type: 'load-model-clip-done', id, handle });
        break;
      }

      case 'clip-embed-text': {
        const { modelHandle, tokenIds, batchSize, seqLen } = e.data;

        const ids = new BigInt64Array(tokenIds.map(BigInt));
        const idsByteLen = ids.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        if (!idsPtr) throw new Error('WASM alloc failed for token IDs');
        abi.writeArray(BigInt64Array, idsPtr, ids);

        const maxOutputFloats = batchSize * 2048;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) throw new Error('WASM alloc failed for output');

        const resultLen = wasm.clip_embed_text(
          modelHandle, idsPtr, abi.size(ids.length), batchSize, seqLen, outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(idsPtr, idsByteLen);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('CLIP text embedding failed');
        self.postMessage({ type: 'clip-embed-text-done', id, output }, [output.buffer]);
        break;
      }

      case 'clip-embed-image': {
        const { modelHandle, pixelValues, batchSize } = e.data;

        const pixelByteLen = pixelValues.length * 4;
        const pixelPtr = abi.alloc(pixelByteLen);
        if (!pixelPtr) throw new Error('WASM alloc failed for pixel data');
        abi.writeArray(Float32Array, pixelPtr, pixelValues);

        const maxOutputFloats = batchSize * 2048;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) throw new Error('WASM alloc failed for output');

        const resultLen = wasm.clip_embed_image(
          modelHandle, pixelPtr, abi.size(pixelValues.length), batchSize, outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(pixelPtr, pixelByteLen);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('CLIP image embedding failed');
        self.postMessage({ type: 'clip-embed-image-done', id, output }, [output.buffer]);
        break;
      }

      case 'preprocess-image': {
        const { rgbaData, width, height, targetSize, mean, std } = e.data;

        const rgbaPtr = abi.alloc(rgbaData.length);
        if (!rgbaPtr) throw new Error('WASM alloc failed for image data');
        abi.copyBytesIn(rgbaPtr, rgbaData);

        const meanArr = new Float32Array(mean);
        const stdArr = new Float32Array(std);
        const meanPtr = abi.alloc(12);
        const stdPtr = abi.alloc(12);
        if (!meanPtr || !stdPtr) throw new Error('WASM alloc failed for normalization params');
        abi.writeArray(Float32Array, meanPtr, meanArr);
        abi.writeArray(Float32Array, stdPtr, stdArr);

        const outLen = 3 * targetSize * targetSize;
        const outByteLen = outLen * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) throw new Error('WASM alloc failed for output');

        const resultLen = wasm.preprocess_image(
          rgbaPtr, abi.size(rgbaData.length), width, height, targetSize,
          meanPtr, stdPtr, outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(rgbaPtr, rgbaData.length);
        abi.free(meanPtr, 12);
        abi.free(stdPtr, 12);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('Image preprocessing failed');
        self.postMessage({ type: 'preprocess-image-done', id, output }, [output.buffer]);
        break;
      }

      // --- Whisper worker handlers ---

      case 'load-model-whisper': {
        const { modelBytes, configJson } = e.data;
        const configBytes = new TextEncoder().encode(configJson);
        const modelPtr = abi.alloc(modelBytes.length);
        abi.copyBytesIn(modelPtr, modelBytes);
        const configPtr = abi.alloc(configBytes.length);
        abi.copyBytesIn(configPtr, configBytes);
        const handle = wasm.load_model_whisper(modelPtr, abi.size(modelBytes.length), configPtr, abi.size(configBytes.length));
        abi.free(modelPtr, modelBytes.length);
        abi.free(configPtr, configBytes.length);
        if (handle === 0) throw new Error('Failed to load Whisper model');
        self.postMessage({ type: 'load-model-whisper-done', id, handle });
        break;
      }

      case 'whisper-encode': {
        const { modelHandle, melFeatures, batchSize, timeSteps } = e.data;
        const melByteLen = melFeatures.length * 4;
        const melPtr = abi.alloc(melByteLen);
        abi.writeArray(Float32Array, melPtr, melFeatures);
        const maxOut = batchSize * 1500 * 1024;
        const outByteLen = maxOut * 4;
        const outPtr = abi.alloc(outByteLen);
        const resultLen = wasm.whisper_encode(modelHandle, melPtr, abi.size(melFeatures.length), batchSize, timeSteps, outPtr);
        let output;
        if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
        abi.free(melPtr, melByteLen);
        abi.free(outPtr, outByteLen);
        if (!output) throw new Error('Whisper encode failed');
        self.postMessage({ type: 'whisper-encode-done', id, output }, [output.buffer]);
        break;
      }

      case 'whisper-decode': {
        const { modelHandle, decoderIds, encoderHidden, encoderMask, batchSize, decSeq, encSeq } = e.data;
        const ids = new BigInt64Array(decoderIds.map(BigInt));
        const mask = new BigInt64Array(encoderMask.map(BigInt));
        const idsByteLen = ids.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        abi.writeArray(BigInt64Array, idsPtr, ids);
        const encByteLen = encoderHidden.length * 4;
        const encPtr = abi.alloc(encByteLen);
        abi.writeArray(Float32Array, encPtr, encoderHidden);
        const maskByteLen = mask.length * 8;
        const maskPtr = abi.alloc(maskByteLen);
        abi.writeArray(BigInt64Array, maskPtr, mask);
        const maxOut = batchSize * 52000;
        const outByteLen = maxOut * 4;
        const outPtr = abi.alloc(outByteLen);
        const resultLen = wasm.whisper_decode(modelHandle, idsPtr, abi.size(ids.length), encPtr, abi.size(encoderHidden.length), maskPtr, abi.size(mask.length), batchSize, decSeq, encSeq, outPtr);
        let output;
        if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
        abi.free(idsPtr, idsByteLen);
        abi.free(encPtr, encByteLen);
        abi.free(maskPtr, maskByteLen);
        abi.free(outPtr, outByteLen);
        if (!output) throw new Error('Whisper decode failed');
        self.postMessage({ type: 'whisper-decode-done', id, output }, [output.buffer]);
        break;
      }

      // --- CLAP worker handlers ---

      case 'load-model-clap': {
        const { modelBytes, configJson } = e.data;
        const configBytes = new TextEncoder().encode(configJson);
        const modelPtr = abi.alloc(modelBytes.length);
        abi.copyBytesIn(modelPtr, modelBytes);
        const configPtr = abi.alloc(configBytes.length);
        abi.copyBytesIn(configPtr, configBytes);
        const handle = wasm.load_model_clap(modelPtr, abi.size(modelBytes.length), configPtr, abi.size(configBytes.length));
        abi.free(modelPtr, modelBytes.length);
        abi.free(configPtr, configBytes.length);
        if (handle === 0) throw new Error('Failed to load CLAP model');
        self.postMessage({ type: 'load-model-clap-done', id, handle });
        break;
      }

      case 'clap-embed-text': {
        const { modelHandle, tokenIds, attentionMask, batchSize, seqLen } = e.data;
        const ids = new BigInt64Array(tokenIds.map(BigInt));
        const mask = new BigInt64Array(attentionMask.map(BigInt));
        const idsByteLen = ids.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        abi.writeArray(BigInt64Array, idsPtr, ids);
        const maskByteLen = mask.length * 8;
        const maskPtr = abi.alloc(maskByteLen);
        abi.writeArray(BigInt64Array, maskPtr, mask);
        const maxOut = batchSize * 2048;
        const outByteLen = maxOut * 4;
        const outPtr = abi.alloc(outByteLen);
        const resultLen = wasm.clap_embed_text(modelHandle, idsPtr, abi.size(ids.length), maskPtr, abi.size(mask.length), batchSize, seqLen, outPtr);
        let output;
        if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
        abi.free(idsPtr, idsByteLen);
        abi.free(maskPtr, maskByteLen);
        abi.free(outPtr, outByteLen);
        if (!output) throw new Error('CLAP text embedding failed');
        self.postMessage({ type: 'clap-embed-text-done', id, output }, [output.buffer]);
        break;
      }

      case 'clap-embed-audio': {
        const { modelHandle, inputFeatures, batchSize, channels, timeFrames, melBins, isLonger } = e.data;
        const featByteLen = inputFeatures.length * 4;
        const featPtr = abi.alloc(featByteLen);
        abi.writeArray(Float32Array, featPtr, inputFeatures);
        const longerPtr = abi.alloc(isLonger.length);
        abi.copyBytesIn(longerPtr, isLonger);
        const maxOut = batchSize * 2048;
        const outByteLen = maxOut * 4;
        const outPtr = abi.alloc(outByteLen);
        const resultLen = wasm.clap_embed_audio(modelHandle, featPtr, abi.size(inputFeatures.length), batchSize, channels, timeFrames, melBins, longerPtr, abi.size(isLonger.length), outPtr);
        let output;
        if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
        abi.free(featPtr, featByteLen);
        abi.free(longerPtr, isLonger.length);
        abi.free(outPtr, outByteLen);
        if (!output) throw new Error('CLAP audio embedding failed');
        self.postMessage({ type: 'clap-embed-audio-done', id, output }, [output.buffer]);
        break;
      }

      // --- Florence-2 worker handlers ---

      case 'load-model-florence': {
        const { modelBytes, configJson } = e.data;
        const configBytes = new TextEncoder().encode(configJson);
        const modelPtr = abi.alloc(modelBytes.length);
        abi.copyBytesIn(modelPtr, modelBytes);
        const configPtr = abi.alloc(configBytes.length);
        abi.copyBytesIn(configPtr, configBytes);
        const handle = wasm.load_model_florence(modelPtr, abi.size(modelBytes.length), configPtr, abi.size(configBytes.length));
        abi.free(modelPtr, modelBytes.length);
        abi.free(configPtr, configBytes.length);
        if (handle === 0) throw new Error('Failed to load Florence model');
        self.postMessage({ type: 'load-model-florence-done', id, handle });
        break;
      }

      case 'florence-encode': {
        const { modelHandle, pixelValues, promptIds, batchSize } = e.data;
        const ids = new BigInt64Array(promptIds.map(BigInt));
        const pixelByteLen = pixelValues.length * 4;
        const pixelPtr = abi.alloc(pixelByteLen);
        abi.writeArray(Float32Array, pixelPtr, pixelValues);
        const idsByteLen = ids.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        abi.writeArray(BigInt64Array, idsPtr, ids);
        const maxOut = batchSize * 2000 * 1024;
        const outByteLen = maxOut * 4;
        const outPtr = abi.alloc(outByteLen);
        const encSeqPtr = abi.alloc(4);
        const resultLen = wasm.florence_encode(modelHandle, pixelPtr, abi.size(pixelValues.length), idsPtr, abi.size(ids.length), batchSize, outPtr, encSeqPtr);
        let hidden, encSeq;
        if (resultLen > 0) {
          hidden = abi.readCopy(Float32Array, outPtr, resultLen);
          encSeq = abi.readCopy(Uint32Array, encSeqPtr, 1)[0];
        }
        abi.free(pixelPtr, pixelByteLen);
        abi.free(idsPtr, idsByteLen);
        abi.free(outPtr, outByteLen);
        abi.free(encSeqPtr, 4);
        if (!hidden) throw new Error('Florence encode failed');
        self.postMessage({ type: 'florence-encode-done', id, hidden, encSeq }, [hidden.buffer]);
        break;
      }

      case 'florence-encode-text': {
        const { modelHandle, inputIds, batchSize, seqLen } = e.data;
        const ids = new BigInt64Array(inputIds.map(BigInt));
        const idsByteLen = ids.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        abi.writeArray(BigInt64Array, idsPtr, ids);
        const maxOut = batchSize * Math.max(seqLen, 1) * 2048;
        const outByteLen = maxOut * 4;
        const outPtr = abi.alloc(outByteLen);
        const resultLen = wasm.florence_encode_text(modelHandle, idsPtr, abi.size(ids.length), batchSize, seqLen, outPtr);
        let output;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }
        abi.free(idsPtr, idsByteLen);
        abi.free(outPtr, outByteLen);
        if (!output) throw new Error('Florence text encode failed');
        self.postMessage({ type: 'florence-encode-text-done', id, output }, [output.buffer]);
        break;
      }

      case 'florence-decode': {
        const { modelHandle, decoderIds, encoderHidden, encoderMask, batchSize, decSeq, encSeq } = e.data;
        const ids = new BigInt64Array(decoderIds.map(BigInt));
        const mask = new BigInt64Array(encoderMask.map(BigInt));
        const idsByteLen = ids.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        abi.writeArray(BigInt64Array, idsPtr, ids);
        const encByteLen = encoderHidden.length * 4;
        const encPtr = abi.alloc(encByteLen);
        abi.writeArray(Float32Array, encPtr, encoderHidden);
        const maskByteLen = mask.length * 8;
        const maskPtr = abi.alloc(maskByteLen);
        abi.writeArray(BigInt64Array, maskPtr, mask);
        const maxOut = batchSize * 52000;
        const outByteLen = maxOut * 4;
        const outPtr = abi.alloc(outByteLen);
        const resultLen = wasm.florence_decode(modelHandle, idsPtr, abi.size(ids.length), encPtr, abi.size(encoderHidden.length), maskPtr, abi.size(mask.length), batchSize, decSeq, encSeq, outPtr);
        let output;
        if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
        abi.free(idsPtr, idsByteLen);
        abi.free(encPtr, encByteLen);
        abi.free(maskPtr, maskByteLen);
        abi.free(outPtr, outByteLen);
        if (!output) throw new Error('Florence decode failed');
        self.postMessage({ type: 'florence-decode-done', id, output }, [output.buffer]);
        break;
      }

      case 'rerank': {
        const { modelHandle, tokenizerHandle, query, documents, maxLen, numLabels } = e.data;
        const batch = documents.length;
        if (batch === 0) {
          self.postMessage({ type: 'rerank-done', id, scores: new Float32Array(0) });
          break;
        }

        // Tokenize all query-doc pairs
        const allIds = new BigInt64Array(batch * maxLen);
        const allMask = new BigInt64Array(batch * maxLen);

        for (let i = 0; i < batch; i++) {
          // Inline tokenize_pair
          const aBytes = new TextEncoder().encode(query);
          const bBytes = new TextEncoder().encode(documents[i]);
          const aPtr = abi.alloc(aBytes.length);
          const bPtr = abi.alloc(bBytes.length);
          abi.copyBytesIn(aPtr, aBytes);
          abi.copyBytesIn(bPtr, bBytes);

          const idsByteLen = maxLen * 4;
          const idsPtr = abi.alloc(idsByteLen);
          const maskPtr = abi.alloc(idsByteLen);

          wasm.tokenize_pair(
            tokenizerHandle, aPtr, abi.size(aBytes.length), bPtr, abi.size(bBytes.length),
            maxLen, idsPtr, maskPtr,
          );

          const ids = abi.readCopy(Int32Array, idsPtr, maxLen);
          const mask = abi.readCopy(Int32Array, maskPtr, maxLen);
          for (let j = 0; j < maxLen; j++) {
            allIds[i * maxLen + j] = BigInt(ids[j]);
            allMask[i * maxLen + j] = BigInt(mask[j]);
          }

          abi.free(aPtr, aBytes.length);
          abi.free(bPtr, bBytes.length);
          abi.free(idsPtr, idsByteLen);
          abi.free(maskPtr, idsByteLen);
        }

        const idsByteLen = allIds.length * 8;
        const maskByteLen = allMask.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        const maskPtr = abi.alloc(maskByteLen);
        abi.writeArray(BigInt64Array, idsPtr, allIds);
        abi.writeArray(BigInt64Array, maskPtr, allMask);

        const outByteLen = batch * 4;
        const outPtr = abi.alloc(outByteLen);

        const resultLen = wasm.rerank(
          modelHandle, idsPtr, abi.size(allIds.length), maskPtr, abi.size(allMask.length),
          batch, maxLen, numLabels, outPtr,
        );

        let scores = null;
        if (resultLen > 0) {
          scores = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(idsPtr, idsByteLen);
        abi.free(maskPtr, maskByteLen);
        abi.free(outPtr, outByteLen);

        if (!scores) throw new Error('Reranking failed');
        self.postMessage({ type: 'rerank-done', id, scores }, [scores.buffer]);
        break;
      }

      // --- GPT worker handlers ---

      case 'load-model-gpt': {
        const { modelBytes, configJson, format } = e.data;

        const modelPtr = abi.alloc(modelBytes.length);
        if (!modelPtr) throw new Error('WASM alloc failed for model');
        abi.copyBytesIn(modelPtr, modelBytes);

        const configBytes = new TextEncoder().encode(configJson);
        const configPtr = abi.alloc(configBytes.length);
        if (!configPtr) {
          abi.free(modelPtr, modelBytes.length);
          throw new Error('WASM alloc failed for config');
        }
        abi.copyBytesIn(configPtr, configBytes);

        const loadFn = format === 'gguf' ? wasm.load_model_gpt_gguf : wasm.load_model_gpt;
        const handle = loadFn(
          modelPtr,
          abi.size(modelBytes.length),
          configPtr,
          abi.size(configBytes.length),
        );

        abi.free(modelPtr, modelBytes.length);
        abi.free(configPtr, configBytes.length);

        if (handle === 0) throw new Error('Failed to load GPT model');
        self.postMessage({ type: 'load-model-gpt-done', id, handle });
        break;
      }

      case 'stream-load-model-gpt': {
        const { modelUrl, configJson, family } = e.data;

        const configBytes = new TextEncoder().encode(configJson);
        const configPtr = abi.alloc(configBytes.length);
        if (!configPtr) throw new Error('WASM alloc failed for config');
        abi.copyBytesIn(configPtr, configBytes);

        const handle = wasm.create_model_gpt(configPtr, abi.size(configBytes.length));
        abi.free(configPtr, configBytes.length);
        if (!handle) throw new Error('Failed to create empty GPT model');

        try {
          const response = await fetch(modelUrl);
          await streamRegisterSafetensors({
            response,
            abi,
            wasm,
            handle,
            family,
            onProgress: (progress) => {
              self.postMessage({ type: 'progress', id, ...progress });
            },
          });
        } catch (err) {
          wasm.unload_model(handle);
          throw err;
        }

        self.postMessage({ type: 'stream-load-model-gpt-done', id, handle });
        break;
      }

      case 'stream-load-model-gpt-gguf': {
        const { modelUrl, configJson } = e.data;

        const configBytes = new TextEncoder().encode(configJson);
        const configPtr = abi.alloc(configBytes.length);
        if (!configPtr) throw new Error('WASM alloc failed for config');
        abi.copyBytesIn(configPtr, configBytes);

        const handle = wasm.create_model_gpt(configPtr, abi.size(configBytes.length));
        abi.free(configPtr, configBytes.length);
        if (!handle) throw new Error('Failed to create empty GPT model');

        try {
          const response = await fetch(modelUrl);
          const gpuResidency = typeof wasm.register_weight_gguf_gpu === 'function'
            ? {
                createBuffer: (sizeBytes) => gpuSync({ cmd: 'create_buffer', size: sizeBytes }),
                writeBufferAtOffset: (id, offsetBytes, srcBytes) => gpuWriteBufferAtOffsetSync(id, offsetBytes, srcBytes),
                freeBuffer: (id) => gpuAsync({ cmd: 'free_buffer', id }),
              }
            : null;
          await streamRegisterGguf({
            response,
            abi,
            wasm,
            handle,
            gpuResidency,
            onProgress: (progress) => {
              self.postMessage({ type: 'progress', id, ...progress });
            },
            onMetadata: (metadata) => {
              self.postMessage({ type: 'metadata', id, ...metadata });
            },
          });
        } catch (err) {
          wasm.unload_model(handle);
          throw err;
        }

        self.postMessage({ type: 'stream-load-model-gpt-gguf-done', id, handle });
        break;
      }

      case 'infer-gpt-config-gguf': {
        const { modelBytes } = e.data;
        const modelPtr = abi.alloc(modelBytes.length);
        if (!modelPtr) throw new Error('WASM alloc failed for GGUF data');
        abi.copyBytesIn(modelPtr, modelBytes);

        const outLenPtr = abi.alloc(4);
        if (!outLenPtr) {
          abi.free(modelPtr, modelBytes.length);
          throw new Error('WASM alloc failed for GGUF config metadata');
        }

        const jsonPtr = wasm.gpt_config_json_from_gguf(modelPtr, abi.size(modelBytes.length), outLenPtr);
        const jsonLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];
        abi.free(outLenPtr, 4);
        abi.free(modelPtr, modelBytes.length);

        if (!jsonPtr || jsonLen === 0) {
          throw new Error('Failed to derive GPT config from GGUF metadata');
        }

        const jsonBytes = abi.readCopy(Uint8Array, jsonPtr, jsonLen);
        abi.free(jsonPtr, jsonLen);

        self.postMessage({
          type: 'infer-gpt-config-gguf-done',
          id,
          configJson: TEXT_DECODER.decode(jsonBytes),
        });
        break;
      }

      case 'infer-chat-template-gguf': {
        const { modelBytes } = e.data;
        const modelPtr = abi.alloc(modelBytes.length);
        if (!modelPtr) throw new Error('WASM alloc failed for GGUF data');
        abi.copyBytesIn(modelPtr, modelBytes);

        const outLenPtr = abi.alloc(4);
        if (!outLenPtr) {
          abi.free(modelPtr, modelBytes.length);
          throw new Error('WASM alloc failed for GGUF chat template metadata');
        }

        const templatePtr = wasm.gguf_chat_template(modelPtr, abi.size(modelBytes.length), outLenPtr);
        const templateLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];
        abi.free(outLenPtr, 4);
        abi.free(modelPtr, modelBytes.length);

        let chatTemplate = null;
        if (templatePtr && templateLen > 0) {
          const templateBytes = abi.readCopy(Uint8Array, templatePtr, templateLen);
          abi.free(templatePtr, templateLen);
          chatTemplate = TEXT_DECODER.decode(templateBytes);
        }

        self.postMessage({
          type: 'infer-chat-template-gguf-done',
          id,
          chatTemplate,
        });
        break;
      }

      case 'gpt-forward': {
        const { modelHandle, tokenIds, batchSize, seqLen } = e.data;

        const ids = new BigInt64Array(tokenIds.map(BigInt));
        const idsByteLen = ids.length * 8;
        const idsPtr = abi.alloc(idsByteLen);
        if (!idsPtr) throw new Error('WASM alloc failed for token IDs');
        abi.writeArray(BigInt64Array, idsPtr, ids);

        // Output: batch * seq_len * vocab_size (conservative max)
        const maxOutputFloats = batchSize * seqLen * 128256;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) {
          abi.free(idsPtr, idsByteLen);
          throw new Error('WASM alloc failed for output');
        }

        const resultLen = wasm.gpt_forward(
          modelHandle,
          idsPtr, abi.size(ids.length),
          batchSize, seqLen,
          outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(idsPtr, idsByteLen);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('GPT forward pass failed');
        self.postMessage({ type: 'gpt-forward-done', id, output }, [output.buffer]);
        break;
      }

      case 'gpt-projector-vision-encode': {
        const { modelHandle, projectorHandle, pixelValues, batch } = e.data;
        const pixelByteLen = pixelValues.length * 4;
        const pixelPtr = abi.alloc(pixelByteLen);
        if (!pixelPtr) throw new Error('WASM alloc failed for projector pixel values');
        abi.writeArray(Float32Array, pixelPtr, pixelValues);

        const maxOutputFloats = batch * 256 * 3072;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) {
          abi.free(pixelPtr, pixelByteLen);
          throw new Error('WASM alloc failed for projector output');
        }

        const resultLen = wasm.gpt_projector_vision_encode(
          modelHandle,
          projectorHandle,
          pixelPtr, abi.size(pixelValues.length),
          batch,
          outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(pixelPtr, pixelByteLen);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('Projector vision encode failed');
        self.postMessage({ type: 'gpt-projector-vision-encode-done', id, output }, [output.buffer]);
        break;
      }

      case 'gpt-projector-image-encode': {
        const { projectorHandle, imageBytes } = e.data;
        const inPtr = abi.alloc(imageBytes.length);
        if (!inPtr) throw new Error('WASM alloc failed for image bytes');
        abi.copyBytesIn(inPtr, imageBytes);

        const outTokensPtr = abi.alloc(4);
        if (!outTokensPtr) {
          abi.free(inPtr, imageBytes.length);
          throw new Error('WASM alloc failed for image token count');
        }

        const maxOutputFloats = 4096 * 4096;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) {
          abi.free(inPtr, imageBytes.length);
          abi.free(outTokensPtr, 4);
          throw new Error('WASM alloc failed for projector embeddings');
        }

        const resultLen = wasm.gpt_projector_image_encode(
          projectorHandle,
          inPtr, abi.size(imageBytes.length),
          outTokensPtr,
          outPtr,
        );

        let embeddings = null;
        let tokensPerImage = null;
        if (resultLen > 0) {
          embeddings = abi.readCopy(Float32Array, outPtr, resultLen);
          tokensPerImage = [abi.readCopy(Uint32Array, outTokensPtr, 1)[0]];
        }

        abi.free(inPtr, imageBytes.length);
        abi.free(outTokensPtr, 4);
        abi.free(outPtr, outByteLen);

        if (!embeddings || !tokensPerImage) throw new Error('Projector image encode failed');
        self.postMessage({ type: 'gpt-projector-image-encode-done', id, embeddings, tokensPerImage }, [embeddings.buffer]);
        break;
      }

      case 'gpt-projector-audio-encode': {
        const { projectorHandle, audioBytes } = e.data;
        const inPtr = abi.alloc(audioBytes.length);
        if (!inPtr) throw new Error('WASM alloc failed for audio bytes');
        abi.copyBytesIn(inPtr, audioBytes);

        const outTokensPtr = abi.alloc(4);
        if (!outTokensPtr) {
          abi.free(inPtr, audioBytes.length);
          throw new Error('WASM alloc failed for audio token count');
        }

        const maxOutputFloats = 4096 * 4096;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) {
          abi.free(inPtr, audioBytes.length);
          abi.free(outTokensPtr, 4);
          throw new Error('WASM alloc failed for audio projector embeddings');
        }

        const resultLen = wasm.gpt_projector_audio_encode(
          projectorHandle,
          inPtr, abi.size(audioBytes.length),
          outTokensPtr,
          outPtr,
        );

        let embeddings = null;
        let tokensPerAudio = null;
        if (resultLen > 0) {
          embeddings = abi.readCopy(Float32Array, outPtr, resultLen);
          tokensPerAudio = [abi.readCopy(Uint32Array, outTokensPtr, 1)[0]];
        }

        abi.free(inPtr, audioBytes.length);
        abi.free(outTokensPtr, 4);
        abi.free(outPtr, outByteLen);

        if (!embeddings || !tokensPerAudio) throw new Error('Projector audio encode failed');
        self.postMessage({ type: 'gpt-projector-audio-encode-done', id, embeddings, tokensPerAudio }, [embeddings.buffer]);
        break;
      }

      case 'gpt-forward-multimodal-gemma4': {
        const { modelHandle, tokenizerHandle, expandedIds, imageEmbeddings, tokensPerImage, audioEmbeddings, tokensPerAudio, batchSize, seqLen } = e.data;
        const ids = new BigInt64Array(expandedIds.map(BigInt));
        const imageCounts = new Uint32Array(tokensPerImage);
        const audioCounts = new Uint32Array(tokensPerAudio);

        const idsByteLen = ids.length * 8;
        const imgEmbByteLen = imageEmbeddings.length * 4;
        const imgCountsByteLen = imageCounts.length * 4;
        const audioEmbByteLen = audioEmbeddings.length * 4;
        const audioCountsByteLen = audioCounts.length * 4;

        const idsPtr = abi.alloc(idsByteLen);
        if (!idsPtr) throw new Error('WASM alloc failed for multimodal IDs');
        abi.writeArray(BigInt64Array, idsPtr, ids);
        const imgEmbPtr = abi.alloc(imgEmbByteLen);
        if (!imgEmbPtr) throw new Error('WASM alloc failed for image embeddings');
        abi.writeArray(Float32Array, imgEmbPtr, imageEmbeddings);
        const imgCountsPtr = abi.alloc(imgCountsByteLen);
        if (!imgCountsPtr) throw new Error('WASM alloc failed for image token counts');
        abi.writeArray(Uint32Array, imgCountsPtr, imageCounts);
        const audioEmbPtr = abi.alloc(audioEmbByteLen);
        if (!audioEmbPtr) throw new Error('WASM alloc failed for audio embeddings');
        abi.writeArray(Float32Array, audioEmbPtr, audioEmbeddings);
        const audioCountsPtr = abi.alloc(audioCountsByteLen);
        if (!audioCountsPtr) throw new Error('WASM alloc failed for audio token counts');
        abi.writeArray(Uint32Array, audioCountsPtr, audioCounts);

        const maxOutputFloats = batchSize * seqLen * 262144;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) throw new Error('WASM alloc failed for multimodal output');

        const resultLen = wasm.gpt_forward_multimodal_gemma4(
          modelHandle,
          tokenizerHandle,
          idsPtr, abi.size(ids.length),
          imgEmbPtr, abi.size(imageEmbeddings.length),
          imgCountsPtr, abi.size(imageCounts.length),
          audioEmbPtr, abi.size(audioEmbeddings.length),
          audioCountsPtr, abi.size(audioCounts.length),
          batchSize,
          seqLen,
          outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(idsPtr, idsByteLen);
        abi.free(imgEmbPtr, imgEmbByteLen);
        abi.free(imgCountsPtr, imgCountsByteLen);
        abi.free(audioEmbPtr, audioEmbByteLen);
        abi.free(audioCountsPtr, audioCountsByteLen);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('Gemma4 multimodal forward failed');
        self.postMessage({ type: 'gpt-forward-multimodal-gemma4-done', id, output }, [output.buffer]);
        break;
      }

      case 'gpt-forward-cached-multimodal-gemma4': {
        const { modelHandle, tokenizerHandle, cacheHandle, expandedIds, imageEmbeddings, tokensPerImage, audioEmbeddings, tokensPerAudio, batchSize, seqLen } = e.data;
        const ids = new BigInt64Array(expandedIds.map(BigInt));
        const imageCounts = new Uint32Array(tokensPerImage);
        const audioCounts = new Uint32Array(tokensPerAudio);

        const idsByteLen = ids.length * 8;
        const imgEmbByteLen = imageEmbeddings.length * 4;
        const imgCountsByteLen = imageCounts.length * 4;
        const audioEmbByteLen = audioEmbeddings.length * 4;
        const audioCountsByteLen = audioCounts.length * 4;

        const idsPtr = abi.alloc(idsByteLen);
        if (!idsPtr) throw new Error('WASM alloc failed for cached multimodal IDs');
        abi.writeArray(BigInt64Array, idsPtr, ids);
        const imgEmbPtr = abi.alloc(imgEmbByteLen);
        if (!imgEmbPtr) throw new Error('WASM alloc failed for cached image embeddings');
        abi.writeArray(Float32Array, imgEmbPtr, imageEmbeddings);
        const imgCountsPtr = abi.alloc(imgCountsByteLen);
        if (!imgCountsPtr) throw new Error('WASM alloc failed for cached image token counts');
        abi.writeArray(Uint32Array, imgCountsPtr, imageCounts);
        const audioEmbPtr = abi.alloc(audioEmbByteLen);
        if (!audioEmbPtr) throw new Error('WASM alloc failed for cached audio embeddings');
        abi.writeArray(Float32Array, audioEmbPtr, audioEmbeddings);
        const audioCountsPtr = abi.alloc(audioCountsByteLen);
        if (!audioCountsPtr) throw new Error('WASM alloc failed for cached audio token counts');
        abi.writeArray(Uint32Array, audioCountsPtr, audioCounts);

        const maxOutputFloats = batchSize * seqLen * 262144;
        const outByteLen = maxOutputFloats * 4;
        const outPtr = abi.alloc(outByteLen);
        if (!outPtr) throw new Error('WASM alloc failed for cached multimodal output');

        const resultLen = wasm.gpt_forward_cached_multimodal_gemma4(
          modelHandle,
          tokenizerHandle,
          cacheHandle,
          idsPtr, abi.size(ids.length),
          imgEmbPtr, abi.size(imageEmbeddings.length),
          imgCountsPtr, abi.size(imageCounts.length),
          audioEmbPtr, abi.size(audioEmbeddings.length),
          audioCountsPtr, abi.size(audioCounts.length),
          batchSize,
          seqLen,
          outPtr,
        );

        let output = null;
        if (resultLen > 0) {
          output = abi.readCopy(Float32Array, outPtr, resultLen);
        }

        abi.free(idsPtr, idsByteLen);
        abi.free(imgEmbPtr, imgEmbByteLen);
        abi.free(imgCountsPtr, imgCountsByteLen);
        abi.free(audioEmbPtr, audioEmbByteLen);
        abi.free(audioCountsPtr, audioCountsByteLen);
        abi.free(outPtr, outByteLen);

        if (!output) throw new Error('Gemma4 cached multimodal forward failed');
        self.postMessage({ type: 'gpt-forward-cached-multimodal-gemma4-done', id, output }, [output.buffer]);
        break;
      }

      // --- ONNX Runtime Web worker handlers ---

      case 'onnx-init': {
        const { wasmPaths, executionProviders } = e.data;
        try {
          ort = await import('onnxruntime-web');
        } catch (_) {
          throw new Error('onnxruntime-web not available in worker. Add it to your importmap or bundle.');
        }
        if (wasmPaths) {
          ort.env.wasm.wasmPaths = wasmPaths;
        }
        self.postMessage({ type: 'onnx-init-done', id });
        break;
      }

      case 'load-onnx-model': {
        if (!ort) throw new Error('Call onnx-init first');
        const { modelBytes, executionProviders } = e.data;
        const session = await ort.InferenceSession.create(modelBytes.buffer, {
          executionProviders: executionProviders || ['wasm'],
        });
        const handle = onnxNextId++;
        onnxSessions.set(handle, session);
        self.postMessage({ type: 'load-onnx-model-done', id, handle });
        break;
      }

      case 'onnx-infer': {
        if (!ort) throw new Error('Call onnx-init first');
        const { sessionHandle, feeds } = e.data;
        const session = onnxSessions.get(sessionHandle);
        if (!session) throw new Error(`ONNX session ${sessionHandle} not found`);

        // Convert serialized feeds to ORT Tensors
        const ortFeeds = {};
        for (const [name, tensor] of Object.entries(feeds)) {
          ortFeeds[name] = new ort.Tensor(tensor.type, tensor.data, tensor.dims);
        }

        const results = await session.run(ortFeeds);

        // Serialize outputs
        const outputs = {};
        for (const [name, tensor] of Object.entries(results)) {
          outputs[name] = { data: tensor.data, dims: Array.from(tensor.dims), type: tensor.type };
        }
        self.postMessage({ type: 'onnx-infer-done', id, outputs });
        break;
      }

      case 'unload-onnx-model': {
        const session = onnxSessions.get(e.data.handle);
        if (session) {
          await session.release();
          onnxSessions.delete(e.data.handle);
        }
        self.postMessage({ type: 'unload-onnx-model-done', id });
        break;
      }
    }
  } catch (err) {
    self.postMessage({ type: 'error', id, message: err.message });
  }
};
