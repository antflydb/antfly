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

// End-to-end test for the GGUF matmul-transB shaders in web/shaders/.
//
// For every newly-added quant format we:
//   1. Generate a random quantized B of shape [N, K] using a CPU codec.
//      For most formats we don't have a forward quantizer; we instead emit
//      random *block* bytes with structurally-valid scale/index fields and let
//      the reference CPU dequantizer define the f32 ground truth.
//   2. Run the WGSL `matmul_transb_<fmt>` shader on a real WebGPU device
//      (Dawn-Node + lavapipe), feeding it the same quantized bytes plus a
//      random f32 A of shape [M, K].
//   3. Compare GPU C[M,N] against the CPU reference A @ dequant(B)^T element
//      by element.
//
// The CPU reference is produced out-of-process by ../scripts/dequant_cli, a
// tiny Zig wrapper around gguf.quant_codec.dequantizeToFloat32. That keeps the
// test agnostic to changes in the CPU dequant logic.
//
// Run with:
//   VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.json \
//     node web/test-quant-kernels-webgpu.cjs

const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const pkg = require('@kmamal/gpu');
const { create: createGpu } = pkg;
// @kmamal/gpu exposes class names as top-level keys; bind the WebGPU-style
// globals onto globalThis ourselves.
for (const key of Object.keys(pkg)) {
  if (key.startsWith('GPU')) globalThis[key] = pkg[key];
}

const INFERENCE_ROOT = path.resolve(__dirname, '..');
const SHADERS_DIR = path.join(__dirname, 'shaders');

// Locate the helper binary in either of the two places `zig build dequant-cli`
// might leave it.
const DEQUANT_CANDIDATES = [
  path.join(INFERENCE_ROOT, 'zig-out/bin/dequant_cli'),
  path.join(INFERENCE_ROOT, 'scripts/dequant_cli'),
];
const DEQUANT_CLI = DEQUANT_CANDIDATES.find((p) => fs.existsSync(p));
if (!DEQUANT_CLI) {
  console.error(`missing helper binary; build it with:\n  zig build dequant-cli`);
  process.exit(2);
}

// Per-format block geometry (mirrors src/gguf/tensor_types.zig).
const FORMATS = {
  // Pre-existing formats, included for regression coverage.
  Q4_0:    { blockBytes: 18,  blockValues: 32,  shader: 'matmul_transb_q4_0',    entry: 'matmul_transb_q4_0' },
  Q4_1:    { blockBytes: 20,  blockValues: 32,  shader: 'matmul_transb_q4_1',    entry: 'matmul_transb_q4_1' },
  Q5_0:    { blockBytes: 22,  blockValues: 32,  shader: 'matmul_transb_q5_0',    entry: 'matmul_transb_q5_0' },
  Q5_1:    { blockBytes: 24,  blockValues: 32,  shader: 'matmul_transb_q5_1',    entry: 'matmul_transb_q5_1' },
  Q8_0:    { blockBytes: 34,  blockValues: 32,  shader: 'matmul_transb_q8_0',    entry: 'matmul_transb_q8_0' },
  Q8_1:    { blockBytes: 36,  blockValues: 32,  shader: 'matmul_transb_q8_1',    entry: 'matmul_transb_q8_1' },
  IQ4_NL:  { blockBytes: 18,  blockValues: 32,  shader: 'matmul_transb_iq4_nl',  entry: 'matmul_transb_iq4_nl' },
  IQ4_XS:  { blockBytes: 136, blockValues: 256, shader: 'matmul_transb_iq4_xs',  entry: 'matmul_transb_iq4_xs' },
  Q2_K:    { blockBytes: 84,  blockValues: 256, shader: 'matmul_transb_q2_k',    entry: 'matmul_transb_q2_k' },
  Q3_K:    { blockBytes: 110, blockValues: 256, shader: 'matmul_transb_q3_k',    entry: 'matmul_transb_q3_k' },
  Q4_K:    { blockBytes: 144, blockValues: 256, shader: 'matmul_transb_q4_k',    entry: 'matmul_transb_q4_k' },
  Q5_K:    { blockBytes: 176, blockValues: 256, shader: 'matmul_transb_q5_k',    entry: 'matmul_transb_q5_k' },
  Q6_K:    { blockBytes: 210, blockValues: 256, shader: 'matmul_transb_q6_k',    entry: 'matmul_transb_q6_k' },
  Q8_K:    { blockBytes: 292, blockValues: 256, shader: 'matmul_transb_q8_k',    entry: 'matmul_transb_q8_k' },
  // I2_S is intentionally omitted: its kernel does BitNet-style per-row int8
  // activation quantization before the dot product, so its semantics differ
  // from the straight A @ dequant(B)^T this harness validates against.
  // New formats added in this change.
  Q1_0:    { blockBytes: 18,  blockValues: 128, shader: 'matmul_transb_q1_0',    entry: 'matmul_transb_q1_0' },
  I8_S:    { blockBytes: 1,   blockValues: 1,   shader: 'matmul_transb_i8_s',    entry: 'matmul_transb_i8_s' },
  TQ1_0:   { blockBytes: 54,  blockValues: 256, shader: 'matmul_transb_tq1_0',   entry: 'matmul_transb_tq1_0' },
  TQ2_0:   { blockBytes: 66,  blockValues: 256, shader: 'matmul_transb_tq2_0',   entry: 'matmul_transb_tq2_0' },
  MXFP4:   { blockBytes: 17,  blockValues: 32,  shader: 'matmul_transb_mxfp4',   entry: 'matmul_transb_mxfp4' },
  NVFP4:   { blockBytes: 36,  blockValues: 64,  shader: 'matmul_transb_nvfp4',   entry: 'matmul_transb_nvfp4' },
  IQ1_S:   { blockBytes: 50,  blockValues: 256, shader: 'matmul_transb_iq1_s',   entry: 'matmul_transb_iq1_s' },
  IQ1_M:   { blockBytes: 56,  blockValues: 256, shader: 'matmul_transb_iq1_m',   entry: 'matmul_transb_iq1_m' },
  IQ2_XXS: { blockBytes: 66,  blockValues: 256, shader: 'matmul_transb_iq2_xxs', entry: 'matmul_transb_iq2_xxs' },
  IQ2_XS:  { blockBytes: 74,  blockValues: 256, shader: 'matmul_transb_iq2_xs',  entry: 'matmul_transb_iq2_xs' },
  IQ2_S:   { blockBytes: 82,  blockValues: 256, shader: 'matmul_transb_iq2_s',   entry: 'matmul_transb_iq2_s' },
  IQ3_XXS: { blockBytes: 98,  blockValues: 256, shader: 'matmul_transb_iq3_xxs', entry: 'matmul_transb_iq3_xxs' },
  IQ3_S:   { blockBytes: 110, blockValues: 256, shader: 'matmul_transb_iq3_s',   entry: 'matmul_transb_iq3_s' },
};

// Test geometry. K must be a multiple of every format's blockValues; 256 covers
// all current formats.
const N = 8;
const K = 256;
// We sweep two M values: M=4 exercises the tiled GEMM path (the WGSL kernel
// processes 16x16 tiles, so M=4 still leaves 12/16 of each row idle but the
// dispatch is identical to wider-M cases). M=1 exercises the new MMV path.
const M_CASES = [4, 1];

// Deterministic LCG so failures are easy to reproduce.
function lcg(seed) {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s;
  };
}

function randomBytes(rng, len) {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = rng() & 0xFF;
  return out;
}

function randomF32Matrix(rng, rows, cols) {
  const out = new Float32Array(rows * cols);
  for (let i = 0; i < out.length; i++) {
    // Generate small floats in [-1, 1] from raw 32-bit values.
    const u = rng();
    out[i] = ((u & 0xFFFF) / 0xFFFF) * 2.0 - 1.0;
  }
  return out;
}

function cpuDequant(fmt, bytes) {
  const rawF32 = execFileSync(DEQUANT_CLI, [fmt], {
    input: Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength),
    maxBuffer: 64 * 1024 * 1024,
  });
  return new Float32Array(rawF32.buffer, rawF32.byteOffset, rawF32.byteLength / 4);
}

// CPU reference: C[M,N] = A[M,K] @ dequant(B_quant)[N,K]^T.
function cpuMatmulTransB(A, Bf32, m, n, k) {
  const C = new Float32Array(m * n);
  for (let i = 0; i < m; i++) {
    for (let j = 0; j < n; j++) {
      let acc = 0;
      for (let kk = 0; kk < k; kk++) {
        acc += A[i * k + kk] * Bf32[j * k + kk];
      }
      C[i * n + j] = acc;
    }
  }
  return C;
}

// kind: 'gemm' | 'mmv'. mmv loads `<shader>_mmv.wgsl` and uses 1 workgroup per
// output column with workgroup_size(128, 1, 1) — matches webgpu-ops.js dispatch.
async function runOne(device, fmtName, fmt, M, kind) {
  const rng = lcg(0xc0ffee01 ^ fmtName.split('').reduce((a, c) => a + c.charCodeAt(0), 0) ^ (M * 31) ^ kind.charCodeAt(0));

  if (K % fmt.blockValues !== 0) {
    throw new Error(`K=${K} not a multiple of ${fmtName} blockValues=${fmt.blockValues}`);
  }
  const blocksPerRow = K / fmt.blockValues;
  const rowQuantBytes = blocksPerRow * fmt.blockBytes;
  const totalBytes = N * rowQuantBytes;

  const Bquant = randomBytes(rng, totalBytes);
  const A = randomF32Matrix(rng, M, K);

  const Bf32 = cpuDequant(fmtName, Bquant);
  if (Bf32.length !== N * K) {
    throw new Error(`unexpected dequant length: got ${Bf32.length}, want ${N * K}`);
  }
  const Cref = cpuMatmulTransB(A, Bf32, M, N, K);

  // Pick shader + entry point for the requested dispatch kind.
  const shaderName = kind === 'mmv' ? `${fmt.shader}_mmv` : fmt.shader;
  const entryPoint = kind === 'mmv' ? `${fmt.entry}_mmv` : fmt.entry;
  const shaderSrc = fs.readFileSync(path.join(SHADERS_DIR, shaderName + '.wgsl'), 'utf8');
  const module = device.createShaderModule({ code: shaderSrc });

  const bindGroupLayout = device.createBindGroupLayout({
    entries: [
      { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
      { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
      { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'storage' } },
      { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'uniform' } },
    ],
  });
  const pipelineLayout = device.createPipelineLayout({ bindGroupLayouts: [bindGroupLayout] });
  const pipeline = device.createComputePipeline({
    layout: pipelineLayout,
    compute: { module, entryPoint },
  });

  const aBuf = device.createBuffer({
    size: A.byteLength,
    usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_DST,
  });
  device.queue.writeBuffer(aBuf, 0, A);

  // Round B up to 4-byte alignment for storage<u32> view.
  const bAligned = Math.ceil(Bquant.byteLength / 4) * 4;
  const bBuf = device.createBuffer({
    size: bAligned,
    usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_DST,
  });
  const bPadded = new Uint8Array(bAligned);
  bPadded.set(Bquant);
  device.queue.writeBuffer(bBuf, 0, bPadded);

  const cBuf = device.createBuffer({
    size: M * N * 4,
    usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC,
  });

  const paramsBuf = device.createBuffer({
    size: 16,
    usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
  });
  const params = new Uint32Array([M, N, K, 0]);
  device.queue.writeBuffer(paramsBuf, 0, params);

  const bindGroup = device.createBindGroup({
    layout: bindGroupLayout,
    entries: [
      { binding: 0, resource: { buffer: aBuf } },
      { binding: 1, resource: { buffer: bBuf } },
      { binding: 2, resource: { buffer: cBuf } },
      { binding: 3, resource: { buffer: paramsBuf } },
    ],
  });

  let workgroupsX, workgroupsY;
  if (kind === 'mmv') {
    // One workgroup per output column; the workgroup itself does the K reduction.
    workgroupsX = N;
    workgroupsY = 1;
  } else {
    workgroupsX = Math.ceil(N / 16);
    workgroupsY = Math.ceil(M / 16);
  }

  const encoder = device.createCommandEncoder();
  const pass = encoder.beginComputePass();
  pass.setPipeline(pipeline);
  pass.setBindGroup(0, bindGroup);
  pass.dispatchWorkgroups(workgroupsX, workgroupsY, 1);
  pass.end();

  const readBuf = device.createBuffer({
    size: M * N * 4,
    usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST,
  });
  encoder.copyBufferToBuffer(cBuf, 0, readBuf, 0, M * N * 4);
  device.queue.submit([encoder.finish()]);

  await readBuf.mapAsync(GPUMapMode.READ);
  const Cgpu = new Float32Array(readBuf.getMappedRange().slice(0));
  readBuf.unmap();

  // Compare element-wise. The tolerance scales with sqrt(K) to absorb the
  // accumulation-order differences between the straight-line CPU reference and
  // the tiled GPU kernel.
  const tolScale = 1e-4 * Math.sqrt(K);
  let maxDiff = 0;
  let badIdx = -1;
  for (let i = 0; i < Cgpu.length; i++) {
    const want = Cref[i];
    const got = Cgpu[i];
    const diff = Math.abs(got - want);
    const tol = tolScale * (Math.abs(want) + 1.0);
    if (diff > tol) {
      maxDiff = Math.max(maxDiff, diff);
      if (badIdx < 0) badIdx = i;
    }
  }

  // Cleanup
  for (const b of [aBuf, bBuf, cBuf, paramsBuf, readBuf]) b.destroy();

  if (badIdx >= 0) {
    return {
      ok: false,
      message: `mismatch at index ${badIdx}: gpu=${Cgpu[badIdx]} ref=${Cref[badIdx]} maxDiff=${maxDiff}`,
    };
  }
  return { ok: true };
}

async function main() {
  const filterArg = process.argv[2];
  const formats = filterArg
    ? Object.fromEntries(Object.entries(FORMATS).filter(([k]) => k === filterArg))
    : FORMATS;
  if (Object.keys(formats).length === 0) {
    console.error(`unknown format filter: ${filterArg}`);
    process.exit(2);
  }

  const gpu = createGpu([
    '--enable-dawn-features=allow_unsafe_apis',
  ]);
  const adapter = await gpu.requestAdapter();
  if (!adapter) {
    console.error('no WebGPU adapter; ensure VK_ICD_FILENAMES points at a working Vulkan ICD');
    process.exit(2);
  }
  const info = adapter.info ?? {};
  console.log(`adapter: ${info.description ?? info.vendor ?? 'unknown'}`);
  // I2_S asks for 16448 bytes of workgroup storage — bump the limit when the
  // adapter advertises a higher cap (lavapipe defaults to 16384 but supports
  // 32768; Chrome/Dawn defaults to 16384 too).
  const requiredLimits = {};
  if (adapter.limits?.maxComputeWorkgroupStorageSize >= 32768) {
    requiredLimits.maxComputeWorkgroupStorageSize = 32768;
  }
  const device = await adapter.requestDevice({ requiredLimits });

  // I2_S's GEMM kernel does BitNet-style activation pre-quantization, which
  // the MMV variant doesn't replicate; we don't generate or wire its MMV
  // shader, so the test driver skips MMV for it too.
  const MMV_SKIP = new Set(['I2_S']);

  let failures = 0;
  for (const M of M_CASES) {
    const kind = M === 1 ? 'mmv' : 'gemm';
    console.log(`\n# ${kind.toUpperCase()} (M=${M}, N=${N}, K=${K})`);
    for (const [name, fmt] of Object.entries(formats)) {
      if (kind === 'mmv' && MMV_SKIP.has(name)) {
        console.log(`  ${name.padEnd(8)} ... skipped (no MMV variant)`);
        continue;
      }
      process.stdout.write(`  ${name.padEnd(8)} ... `);
      try {
        const res = await runOne(device, name, fmt, M, kind);
        if (res.ok) {
          console.log('ok');
        } else {
          console.log(`FAIL — ${res.message}`);
          failures += 1;
        }
      } catch (err) {
        console.log(`ERROR — ${err.message}`);
        failures += 1;
      }
    }
  }

  process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
