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

// WebGPU compute shader dispatch for Termite WASM.
//
// Provides implementations for the extern "webgpu" imports declared in
// src/ops/wasm_extern.zig. Heavy ops (matmul, attention) are dispatched to GPU
// compute shaders; WASM SIMD handles everything else.
//
// Two modes:
// 1. Direct mode (default): GPU ops called on the same thread. Downloads are
//    async and require flush(). Suitable for non-worker usage.
// 2. Worker mode: GPU ops proxied from an inference worker via SharedArrayBuffer
//    and Atomics. Downloads block synchronously. Use initWorkerBridge().

const SHADER_PATHS = {
  matmul: './shaders/matmul.wgsl',
  matmulTransB: './shaders/matmul_transb.wgsl',
  matmulTransBQ4_0: './shaders/matmul_transb_q4_0.wgsl',
  matmulTransBQ4_1: './shaders/matmul_transb_q4_1.wgsl',
  matmulTransBQ5_0: './shaders/matmul_transb_q5_0.wgsl',
  matmulTransBQ5_1: './shaders/matmul_transb_q5_1.wgsl',
  matmulTransBQ8_0: './shaders/matmul_transb_q8_0.wgsl',
  matmulTransBQ8_1: './shaders/matmul_transb_q8_1.wgsl',
  matmulTransBIQ4_NL: './shaders/matmul_transb_iq4_nl.wgsl',
  matmulTransBIQ4_XS: './shaders/matmul_transb_iq4_xs.wgsl',
  matmulTransBQ2_K: './shaders/matmul_transb_q2_k.wgsl',
  matmulTransBQ3_K: './shaders/matmul_transb_q3_k.wgsl',
  matmulTransBQ4_K: './shaders/matmul_transb_q4_k.wgsl',
  matmulTransBQ5_K: './shaders/matmul_transb_q5_k.wgsl',
  matmulTransBQ6_K: './shaders/matmul_transb_q6_k.wgsl',
  matmulTransBQ8_K: './shaders/matmul_transb_q8_k.wgsl',
  matmulTransBI2_S: './shaders/matmul_transb_i2_s.wgsl',
  matmulTransBI8_S: './shaders/matmul_transb_i8_s.wgsl',
  matmulTransBQ1_0: './shaders/matmul_transb_q1_0.wgsl',
  matmulTransBTQ1_0: './shaders/matmul_transb_tq1_0.wgsl',
  matmulTransBTQ2_0: './shaders/matmul_transb_tq2_0.wgsl',
  matmulTransBMXFP4: './shaders/matmul_transb_mxfp4.wgsl',
  matmulTransBNVFP4: './shaders/matmul_transb_nvfp4.wgsl',
  matmulTransBIQ1_S: './shaders/matmul_transb_iq1_s.wgsl',
  matmulTransBIQ1_M: './shaders/matmul_transb_iq1_m.wgsl',
  matmulTransBIQ2_XXS: './shaders/matmul_transb_iq2_xxs.wgsl',
  matmulTransBIQ2_XS: './shaders/matmul_transb_iq2_xs.wgsl',
  matmulTransBIQ2_S: './shaders/matmul_transb_iq2_s.wgsl',
  matmulTransBIQ3_XXS: './shaders/matmul_transb_iq3_xxs.wgsl',
  matmulTransBIQ3_S: './shaders/matmul_transb_iq3_s.wgsl',
  // MMV (qLen=1 / decode) variants — selected by gpuSgemmTransBQuant when
  // rows == 1. One workgroup per output column instead of a 16x16 GEMM tile.
  matmulTransBQ4_0Mmv: './shaders/matmul_transb_q4_0_mmv.wgsl',
  matmulTransBQ4_1Mmv: './shaders/matmul_transb_q4_1_mmv.wgsl',
  matmulTransBQ5_0Mmv: './shaders/matmul_transb_q5_0_mmv.wgsl',
  matmulTransBQ5_1Mmv: './shaders/matmul_transb_q5_1_mmv.wgsl',
  matmulTransBQ8_0Mmv: './shaders/matmul_transb_q8_0_mmv.wgsl',
  matmulTransBQ8_1Mmv: './shaders/matmul_transb_q8_1_mmv.wgsl',
  matmulTransBIQ4_NLMmv: './shaders/matmul_transb_iq4_nl_mmv.wgsl',
  matmulTransBIQ4_XSMmv: './shaders/matmul_transb_iq4_xs_mmv.wgsl',
  matmulTransBQ2_KMmv: './shaders/matmul_transb_q2_k_mmv.wgsl',
  matmulTransBQ3_KMmv: './shaders/matmul_transb_q3_k_mmv.wgsl',
  matmulTransBQ4_KMmv: './shaders/matmul_transb_q4_k_mmv.wgsl',
  matmulTransBQ5_KMmv: './shaders/matmul_transb_q5_k_mmv.wgsl',
  matmulTransBQ6_KMmv: './shaders/matmul_transb_q6_k_mmv.wgsl',
  matmulTransBQ8_KMmv: './shaders/matmul_transb_q8_k_mmv.wgsl',
  matmulTransBI8_SMmv: './shaders/matmul_transb_i8_s_mmv.wgsl',
  matmulTransBQ1_0Mmv: './shaders/matmul_transb_q1_0_mmv.wgsl',
  matmulTransBTQ1_0Mmv: './shaders/matmul_transb_tq1_0_mmv.wgsl',
  matmulTransBTQ2_0Mmv: './shaders/matmul_transb_tq2_0_mmv.wgsl',
  matmulTransBMXFP4Mmv: './shaders/matmul_transb_mxfp4_mmv.wgsl',
  matmulTransBNVFP4Mmv: './shaders/matmul_transb_nvfp4_mmv.wgsl',
  matmulTransBIQ1_SMmv: './shaders/matmul_transb_iq1_s_mmv.wgsl',
  matmulTransBIQ1_MMmv: './shaders/matmul_transb_iq1_m_mmv.wgsl',
  matmulTransBIQ2_XXSMmv: './shaders/matmul_transb_iq2_xxs_mmv.wgsl',
  matmulTransBIQ2_XSMmv: './shaders/matmul_transb_iq2_xs_mmv.wgsl',
  matmulTransBIQ2_SMmv: './shaders/matmul_transb_iq2_s_mmv.wgsl',
  matmulTransBIQ3_XXSMmv: './shaders/matmul_transb_iq3_xxs_mmv.wgsl',
  matmulTransBIQ3_SMmv: './shaders/matmul_transb_iq3_s_mmv.wgsl',
  add: './shaders/add.wgsl',
  elementwiseBinary: './shaders/elementwise_binary.wgsl',
  elementwiseUnary: './shaders/elementwise_unary.wgsl',
  whereSelect: './shaders/where_select.wgsl',
  softmax: './shaders/softmax.wgsl',
  reduction: './shaders/reduce_last_dim.wgsl',
  broadcast: './shaders/broadcast_in_dim.wgsl',
  gelu: './shaders/gelu.wgsl',
  attention: './shaders/attention.wgsl',
  debertaDisentangledAttention: './shaders/deberta_disentangled_attention.wgsl',
  causalAttention: './shaders/causal_attention.wgsl',
  crossAttention: './shaders/cross_attention.wgsl',
  gqaCausalAttention: './shaders/gqa_causal_attention.wgsl',
  gqaCachedAttention: './shaders/gqa_cached_attention.wgsl',
  gqaCachedAttentionPolar4: './shaders/gqa_cached_attention_polar4.wgsl',
  gqaCachedAttentionTurbo3: './shaders/gqa_cached_attention_turbo3.wgsl',
  rmsNorm: './shaders/rms_norm.wgsl',
  layerNorm: './shaders/layer_norm.wgsl',
};

const TILE = 16;
const GQA_K_FORMAT_F32 = 0;
const GQA_K_FORMAT_POLAR4 = 1;
const GQA_K_FORMAT_TURBO3 = 2;
const GQA_V_FORMAT_F32 = 0;
const GQA_CACHED_ATTN_MAX_KV = 2048;

function toJsIndex(value, label) {
  if (typeof value === 'bigint') {
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error(`${label} exceeds JS safe integer range: ${value}`);
    }
    return Number(value);
  }
  return value;
}

async function loadShaderText(path) {
  const url = new URL(path, import.meta.url);
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`Failed to fetch shader ${url}: ${resp.status} ${resp.statusText}`);
  }
  return resp.text();
}

async function loadShaderSources() {
  const entries = await Promise.all(
    Object.entries(SHADER_PATHS).map(async ([key, path]) => [key, await loadShaderText(path)]),
  );
  return Object.fromEntries(entries);
}

export class WebGPUOps {
  constructor() {
    this.device = null;
    this.buffers = new Map();   // id -> GPUBuffer
    this.nextId = 1;
    this.pipelines = {};
    this.matmulBindGroupLayout = null;
    this.attnBindGroupLayout = null;
    this.debertaAttnBindGroupLayout = null;
    this.causalAttnBindGroupLayout = null;
    this.crossAttnBindGroupLayout = null;
    this.whereSelectBindGroupLayout = null;
    this.lastInitError = null;
  }

  /**
   * Initialize WebGPU device and compile shaders.
   * @returns {boolean} true if WebGPU is available
   */
  async init() {
    this.lastInitError = null;
    if (!navigator.gpu) {
      this.lastInitError = 'navigator.gpu is unavailable';
      return false;
    }

    const adapter = await navigator.gpu.requestAdapter();
    if (!adapter) {
      this.lastInitError = 'navigator.gpu.requestAdapter() returned null';
      return false;
    }

    try {
      this.device = await adapter.requestDevice();
    } catch (err) {
      this.lastInitError = `requestDevice failed: ${err?.message ?? err}`;
      return false;
    }
    if (!this.device) {
      this.lastInitError = 'adapter.requestDevice() returned null';
      return false;
    }

    let shaderSources;
    try {
      shaderSources = await loadShaderSources();
    } catch (err) {
      this.lastInitError = err?.message ?? String(err);
      return false;
    }

    // --- Matmul bind group layout (4 bindings: A, B, C, params) ---
    this.matmulBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'storage' } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'uniform' } },
      ],
    });

    const matmulPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.matmulBindGroupLayout],
    });

    this.pipelines.matmul = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmul }),
        entryPoint: 'matmul',
      },
    });

    this.pipelines.matmulTransB = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransB }),
        entryPoint: 'matmul_transb',
      },
    });

    this.pipelines.matmulTransBQ4_0 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ4_0 }),
        entryPoint: 'matmul_transb_q4_0',
      },
    });

    this.pipelines.matmulTransBQ4_1 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ4_1 }),
        entryPoint: 'matmul_transb_q4_1',
      },
    });

    this.pipelines.matmulTransBQ5_0 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ5_0 }),
        entryPoint: 'matmul_transb_q5_0',
      },
    });

    this.pipelines.matmulTransBQ5_1 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ5_1 }),
        entryPoint: 'matmul_transb_q5_1',
      },
    });

    this.pipelines.matmulTransBQ8_0 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ8_0 }),
        entryPoint: 'matmul_transb_q8_0',
      },
    });

    this.pipelines.matmulTransBQ8_1 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ8_1 }),
        entryPoint: 'matmul_transb_q8_1',
      },
    });

    this.pipelines.matmulTransBIQ4_NL = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBIQ4_NL }),
        entryPoint: 'matmul_transb_iq4_nl',
      },
    });

    this.pipelines.matmulTransBIQ4_XS = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBIQ4_XS }),
        entryPoint: 'matmul_transb_iq4_xs',
      },
    });

    this.pipelines.matmulTransBQ2_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ2_K }),
        entryPoint: 'matmul_transb_q2_k',
      },
    });

    this.pipelines.matmulTransBQ3_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ3_K }),
        entryPoint: 'matmul_transb_q3_k',
      },
    });

    this.pipelines.matmulTransBQ4_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ4_K }),
        entryPoint: 'matmul_transb_q4_k',
      },
    });

    this.pipelines.matmulTransBQ5_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ5_K }),
        entryPoint: 'matmul_transb_q5_k',
      },
    });

    this.pipelines.matmulTransBQ6_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ6_K }),
        entryPoint: 'matmul_transb_q6_k',
      },
    });

    this.pipelines.matmulTransBQ8_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBQ8_K }),
        entryPoint: 'matmul_transb_q8_k',
      },
    });

    this.pipelines.matmulTransBI2_S = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.matmulTransBI2_S }),
        entryPoint: 'matmul_transb_i2_s',
      },
    });

    const extraQuantPipelines = [
      ['matmulTransBI8_S', 'matmul_transb_i8_s'],
      ['matmulTransBQ1_0', 'matmul_transb_q1_0'],
      ['matmulTransBTQ1_0', 'matmul_transb_tq1_0'],
      ['matmulTransBTQ2_0', 'matmul_transb_tq2_0'],
      ['matmulTransBMXFP4', 'matmul_transb_mxfp4'],
      ['matmulTransBNVFP4', 'matmul_transb_nvfp4'],
      ['matmulTransBIQ1_S', 'matmul_transb_iq1_s'],
      ['matmulTransBIQ1_M', 'matmul_transb_iq1_m'],
      ['matmulTransBIQ2_XXS', 'matmul_transb_iq2_xxs'],
      ['matmulTransBIQ2_XS', 'matmul_transb_iq2_xs'],
      ['matmulTransBIQ2_S', 'matmul_transb_iq2_s'],
      ['matmulTransBIQ3_XXS', 'matmul_transb_iq3_xxs'],
      ['matmulTransBIQ3_S', 'matmul_transb_iq3_s'],
      // MMV (qLen=1) variants of every quant kernel except I2_S, which has
      // BitNet-specific activation pre-quantization that the MMV path does
      // not yet replicate.
      ['matmulTransBQ4_0Mmv', 'matmul_transb_q4_0_mmv'],
      ['matmulTransBQ4_1Mmv', 'matmul_transb_q4_1_mmv'],
      ['matmulTransBQ5_0Mmv', 'matmul_transb_q5_0_mmv'],
      ['matmulTransBQ5_1Mmv', 'matmul_transb_q5_1_mmv'],
      ['matmulTransBQ8_0Mmv', 'matmul_transb_q8_0_mmv'],
      ['matmulTransBQ8_1Mmv', 'matmul_transb_q8_1_mmv'],
      ['matmulTransBIQ4_NLMmv', 'matmul_transb_iq4_nl_mmv'],
      ['matmulTransBIQ4_XSMmv', 'matmul_transb_iq4_xs_mmv'],
      ['matmulTransBQ2_KMmv', 'matmul_transb_q2_k_mmv'],
      ['matmulTransBQ3_KMmv', 'matmul_transb_q3_k_mmv'],
      ['matmulTransBQ4_KMmv', 'matmul_transb_q4_k_mmv'],
      ['matmulTransBQ5_KMmv', 'matmul_transb_q5_k_mmv'],
      ['matmulTransBQ6_KMmv', 'matmul_transb_q6_k_mmv'],
      ['matmulTransBQ8_KMmv', 'matmul_transb_q8_k_mmv'],
      ['matmulTransBI8_SMmv', 'matmul_transb_i8_s_mmv'],
      ['matmulTransBQ1_0Mmv', 'matmul_transb_q1_0_mmv'],
      ['matmulTransBTQ1_0Mmv', 'matmul_transb_tq1_0_mmv'],
      ['matmulTransBTQ2_0Mmv', 'matmul_transb_tq2_0_mmv'],
      ['matmulTransBMXFP4Mmv', 'matmul_transb_mxfp4_mmv'],
      ['matmulTransBNVFP4Mmv', 'matmul_transb_nvfp4_mmv'],
      ['matmulTransBIQ1_SMmv', 'matmul_transb_iq1_s_mmv'],
      ['matmulTransBIQ1_MMmv', 'matmul_transb_iq1_m_mmv'],
      ['matmulTransBIQ2_XXSMmv', 'matmul_transb_iq2_xxs_mmv'],
      ['matmulTransBIQ2_XSMmv', 'matmul_transb_iq2_xs_mmv'],
      ['matmulTransBIQ2_SMmv', 'matmul_transb_iq2_s_mmv'],
      ['matmulTransBIQ3_XXSMmv', 'matmul_transb_iq3_xxs_mmv'],
      ['matmulTransBIQ3_SMmv', 'matmul_transb_iq3_s_mmv'],
    ];
    for (const [pipelineKey, entryPoint] of extraQuantPipelines) {
      this.pipelines[pipelineKey] = this.device.createComputePipeline({
        layout: matmulPipelineLayout,
        compute: {
          module: this.device.createShaderModule({ code: shaderSources[pipelineKey] }),
          entryPoint,
        },
      });
    }

    this.pipelines.add = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.add }),
        entryPoint: 'add',
      },
    });

    this.pipelines.gelu = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.gelu }),
        entryPoint: 'gelu',
      },
    });

    const vectorBinaryModule = this.device.createShaderModule({ code: shaderSources.elementwiseBinary });
    this.pipelines.addBroadcast = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: vectorBinaryModule, entryPoint: 'add_broadcast' },
    });
    this.pipelines.mul = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: vectorBinaryModule, entryPoint: 'mul' },
    });
    this.pipelines.sub = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: vectorBinaryModule, entryPoint: 'sub' },
    });
    this.pipelines.div = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: vectorBinaryModule, entryPoint: 'div' },
    });
    this.pipelines.lessThan = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: vectorBinaryModule, entryPoint: 'less_than' },
    });

    const vectorUnaryModule = this.device.createShaderModule({ code: shaderSources.elementwiseUnary });
    const unaryEntries = [
      ['neg', 'neg'],
      ['sqrt', 'sqrt_op'],
      ['rsqrt', 'rsqrt'],
      ['exp', 'exp_op'],
      ['log', 'log_op'],
      ['sin', 'sin_op'],
      ['cos', 'cos_op'],
      ['tanh', 'tanh_op'],
      ['abs', 'abs_op'],
      ['erf', 'erf_op'],
    ];
    for (const [pipelineName, entryPoint] of unaryEntries) {
      this.pipelines[pipelineName] = this.device.createComputePipeline({
        layout: matmulPipelineLayout,
        compute: { module: vectorUnaryModule, entryPoint },
      });
    }

    this.whereSelectBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'storage' } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'uniform' } },
      ],
    });
    const whereSelectPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.whereSelectBindGroupLayout],
    });
    this.pipelines.whereSelect = this.device.createComputePipeline({
      layout: whereSelectPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.whereSelect }),
        entryPoint: 'where_select',
      },
    });

    const softmaxModule = this.device.createShaderModule({ code: shaderSources.softmax });
    this.pipelines.softmax = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: softmaxModule, entryPoint: 'softmax' },
    });
    this.pipelines.logSoftmax = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: softmaxModule, entryPoint: 'log_softmax' },
    });

    const reductionModule = this.device.createShaderModule({ code: shaderSources.reduction });
    this.pipelines.reduceSum = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: reductionModule, entryPoint: 'reduce_sum' },
    });
    this.pipelines.reduceMax = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: reductionModule, entryPoint: 'reduce_max' },
    });
    this.pipelines.reduceMean = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: reductionModule, entryPoint: 'reduce_mean' },
    });

    this.pipelines.broadcastInDim = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.broadcast }),
        entryPoint: 'broadcast_in_dim',
      },
    });

    // --- Attention bind group layout (6 bindings: Q, K, V, mask, out, params) ---
    this.attnBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'storage' } },
        { binding: 5, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'uniform' } },
      ],
    });

    const attnPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.attnBindGroupLayout],
    });

    this.pipelines.attention = this.device.createComputePipeline({
      layout: attnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.attention }),
        entryPoint: 'attention',
      },
    });

    this.debertaAttnBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 5, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 6, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'storage' } },
        { binding: 7, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'uniform' } },
      ],
    });

    const debertaAttnPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.debertaAttnBindGroupLayout],
    });

    this.pipelines.debertaDisentangledAttention = this.device.createComputePipeline({
      layout: debertaAttnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.debertaDisentangledAttention }),
        entryPoint: 'deberta_disentangled_attention',
      },
    });

    // --- Causal attention bind group layout (5 bindings: Q, K, V, out, params — no mask) ---
    this.causalAttnBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'storage' } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'uniform' } },
      ],
    });

    const causalAttnPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.causalAttnBindGroupLayout],
    });

    this.pipelines.causalAttention = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.causalAttention }),
        entryPoint: 'causal_attention',
      },
    });

    // GQA causal attention reuses same 5-binding layout (Q, K, V, out, params)
    this.pipelines.gqaCausalAttention = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.gqaCausalAttention }),
        entryPoint: 'gqa_causal_attention',
      },
    });

    // GQA cached attention (asymmetric Q/KV lengths) — same 5-binding layout
    this.pipelines.gqaCachedAttention = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.gqaCachedAttention }),
        entryPoint: 'gqa_cached_attention',
      },
    });
    this.pipelines.gqaCachedAttentionPolar4 = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.gqaCachedAttentionPolar4 }),
        entryPoint: 'gqa_cached_attention_polar4',
      },
    });
    this.pipelines.gqaCachedAttentionTurbo3 = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.gqaCachedAttentionTurbo3 }),
        entryPoint: 'gqa_cached_attention_turbo3',
      },
    });

    // --- Cross attention bind group layout (6 bindings: Q, K, V, enc_mask, out, params) ---
    // Same layout as regular attention (reuse attnBindGroupLayout)
    this.crossAttnBindGroupLayout = this.attnBindGroupLayout;

    const crossAttnPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.crossAttnBindGroupLayout],
    });

    this.pipelines.crossAttention = this.device.createComputePipeline({
      layout: crossAttnPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.crossAttention }),
        entryPoint: 'cross_attention',
      },
    });

    // --- Norm pipelines ---
    // RMSNorm: reuses matmul layout (input, weight, output, params)
    this.pipelines.rmsNorm = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.rmsNorm }),
        entryPoint: 'rms_norm',
      },
    });

    // LayerNorm: needs 5 bindings (input, gamma, beta, output, params)
    this.normBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'read-only-storage' } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'storage' } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: 'uniform' } },
      ],
    });

    const normPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.normBindGroupLayout],
    });

    this.pipelines.layerNorm = this.device.createComputePipeline({
      layout: normPipelineLayout,
      compute: {
        module: this.device.createShaderModule({ code: shaderSources.layerNorm }),
        entryPoint: 'layer_norm',
      },
    });

    return true;
  }

  createBuffer(sizeBytes) {
    const size = toJsIndex(sizeBytes, 'GPU buffer size');
    const buf = this.device.createBuffer({
      size,
      usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
    });
    const id = this.nextId++;
    this.buffers.set(id, buf);
    return id;
  }

  freeBuffer(id) {
    const buf = this.buffers.get(id);
    if (buf) {
      buf.destroy();
      this.buffers.delete(id);
    }
  }

  writeBufferAtOffset(id, offsetBytes, srcBytes) {
    const buf = this.buffers.get(id);
    if (!buf) return;
    const offset = toJsIndex(offsetBytes, 'GPU buffer offset');
    this.device.queue.writeBuffer(buf, offset, srcBytes);
  }

  copyBufferToBuffer(srcId, srcOffsetBytes, dstId, dstOffsetBytes, sizeBytes) {
    const src = this.buffers.get(srcId);
    const dst = this.buffers.get(dstId);
    if (!src || !dst) return;
    const srcOffset = toJsIndex(srcOffsetBytes, 'GPU src buffer offset');
    const dstOffset = toJsIndex(dstOffsetBytes, 'GPU dst buffer offset');
    const size = toJsIndex(sizeBytes, 'GPU copy size');
    const encoder = this.device.createCommandEncoder();
    encoder.copyBufferToBuffer(src, srcOffset, dst, dstOffset, size);
    this.device.queue.submit([encoder.finish()]);
  }

  /**
   * Build WASM import object for the "webgpu" module (direct mode).
   * @param {WebAssembly.Memory} memory - WASM linear memory (or proxy)
   * @returns {object} import functions
   */
  getImports(memory) {
    return {
      gpu_is_available: () => (this.device ? 1 : 0),

      gpu_create_buffer: (sizeBytes) => {
        return this.createBuffer(sizeBytes);
      },

      gpu_free_buffer: (id) => {
        this.freeBuffer(id);
      },

      gpu_upload: (id, ptr, sizeBytes) => {
        const buf = this.buffers.get(id);
        if (!buf) return;
        const ptrIndex = toJsIndex(ptr, 'WASM pointer');
        const size = toJsIndex(sizeBytes, 'GPU upload size');
        const src = new Uint8Array(memory.buffer, ptrIndex, size);
        this.device.queue.writeBuffer(buf, 0, src);
      },

      gpu_download: (id, ptr, sizeBytes) => {
        // In direct mode, downloads are async and require flush().
        const buf = this.buffers.get(id);
        if (!buf) return;
        const ptrIndex = toJsIndex(ptr, 'WASM pointer');
        const size = toJsIndex(sizeBytes, 'GPU download size');

        const staging = this.device.createBuffer({
          size,
          usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST,
        });

        const encoder = this.device.createCommandEncoder();
        encoder.copyBufferToBuffer(buf, 0, staging, 0, size);
        this.device.queue.submit([encoder.finish()]);

        this._pendingDownload = { staging, ptr: ptrIndex, sizeBytes: size, memory };
      },

      gpu_copy_buffer_to_buffer: (srcId, srcOffsetBytes, dstId, dstOffsetBytes, sizeBytes) => {
        this.copyBufferToBuffer(srcId, srcOffsetBytes, dstId, dstOffsetBytes, sizeBytes);
      },

      gpu_matmul: (aId, bId, outId, m, n, k) => {
        this._dispatchMatmul('matmul', aId, bId, outId, m, n, k);
      },

      gpu_matmul_transb: (aId, bId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransB', aId, bId, outId, m, n, k);
      },

      gpu_add: (aId, bId, outId, len) => {
        this._dispatchVectorBinary('add', aId, bId, outId, len);
      },

      gpu_add_broadcast: (aId, bId, outId, len, aLen, bLen) => {
        this._dispatchVectorBinaryBroadcast('addBroadcast', aId, bId, outId, len, aLen, bLen);
      },

      gpu_mul: (aId, bId, outId, len, aLen, bLen) => {
        this._dispatchVectorBinaryBroadcast('mul', aId, bId, outId, len, aLen, bLen);
      },

      gpu_sub: (aId, bId, outId, len, aLen, bLen) => {
        this._dispatchVectorBinaryBroadcast('sub', aId, bId, outId, len, aLen, bLen);
      },

      gpu_div: (aId, bId, outId, len, aLen, bLen) => {
        this._dispatchVectorBinaryBroadcast('div', aId, bId, outId, len, aLen, bLen);
      },

      gpu_less_than: (aId, bId, outId, len, aLen, bLen) => {
        this._dispatchVectorBinaryBroadcast('lessThan', aId, bId, outId, len, aLen, bLen);
      },

      gpu_where_select: (condId, trueId, falseId, outId, len, trueLen, falseLen) => {
        this._dispatchWhereSelect(condId, trueId, falseId, outId, len, trueLen, falseLen);
      },

      gpu_neg: (inputId, outId, len) => {
        this._dispatchVectorUnary('neg', inputId, outId, len);
      },

      gpu_sqrt: (inputId, outId, len) => {
        this._dispatchVectorUnary('sqrt', inputId, outId, len);
      },

      gpu_rsqrt: (inputId, outId, len) => {
        this._dispatchVectorUnary('rsqrt', inputId, outId, len);
      },

      gpu_exp: (inputId, outId, len) => {
        this._dispatchVectorUnary('exp', inputId, outId, len);
      },

      gpu_log: (inputId, outId, len) => {
        this._dispatchVectorUnary('log', inputId, outId, len);
      },

      gpu_sin: (inputId, outId, len) => {
        this._dispatchVectorUnary('sin', inputId, outId, len);
      },

      gpu_cos: (inputId, outId, len) => {
        this._dispatchVectorUnary('cos', inputId, outId, len);
      },

      gpu_tanh: (inputId, outId, len) => {
        this._dispatchVectorUnary('tanh', inputId, outId, len);
      },

      gpu_abs: (inputId, outId, len) => {
        this._dispatchVectorUnary('abs', inputId, outId, len);
      },

      gpu_erf: (inputId, outId, len) => {
        this._dispatchVectorUnary('erf', inputId, outId, len);
      },

      gpu_gelu: (inputId, outId, len) => {
        this._dispatchVectorUnary('gelu', inputId, outId, len);
      },

      gpu_softmax: (inputId, outId, rows, dim) => {
        this._dispatchSoftmax('softmax', inputId, outId, rows, dim);
      },

      gpu_log_softmax: (inputId, outId, rows, dim) => {
        this._dispatchSoftmax('logSoftmax', inputId, outId, rows, dim);
      },

      gpu_reduce_sum_last_dim: (inputId, outId, rows, dim) => {
        this._dispatchReduceLastDim('reduceSum', inputId, outId, rows, dim);
      },

      gpu_reduce_max_last_dim: (inputId, outId, rows, dim) => {
        this._dispatchReduceLastDim('reduceMax', inputId, outId, rows, dim);
      },

      gpu_reduce_mean_last_dim: (inputId, outId, rows, dim) => {
        this._dispatchReduceLastDim('reduceMean', inputId, outId, rows, dim);
      },

      gpu_reduce_sum: (inputId, outId, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) => {
        this._dispatchReduce('reduceSum', inputId, outId, outLen, reduceCount, inRank, outRank, memory, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr);
      },

      gpu_reduce_max: (inputId, outId, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) => {
        this._dispatchReduce('reduceMax', inputId, outId, outLen, reduceCount, inRank, outRank, memory, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr);
      },

      gpu_reduce_mean: (inputId, outId, outLen, reduceCount, inRank, outRank, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) => {
        this._dispatchReduce('reduceMean', inputId, outId, outLen, reduceCount, inRank, outRank, memory, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr);
      },

      gpu_broadcast_in_dim: (inputId, outId, outLen, outRank, inRank, targetShapePtr, inputShapePtr, axesPtr, outStridesPtr, inStridesPtr) => {
        this._dispatchBroadcastInDim(inputId, outId, outLen, outRank, inRank, memory, targetShapePtr, inputShapePtr, axesPtr, outStridesPtr, inStridesPtr);
      },

      gpu_matmul_transb_q4_0: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ4_0', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q4_1: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ4_1', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q5_0: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ5_0', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q5_1: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ5_1', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q8_0: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ8_0', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q8_1: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ8_1', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq4_nl: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ4_NL', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq4_xs: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ4_XS', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q2_k: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ2_K', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q3_k: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ3_K', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q4_k: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ4_K', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q5_k: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ5_K', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q6_k: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ6_K', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q8_k: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ8_K', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_i2_s: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBI2_S', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_i8_s: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBI8_S', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_q1_0: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBQ1_0', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_tq1_0: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBTQ1_0', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_tq2_0: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBTQ2_0', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_mxfp4: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBMXFP4', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_nvfp4: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBNVFP4', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq1_s: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ1_S', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq1_m: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ1_M', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq2_xxs: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ2_XXS', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq2_xs: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ2_XS', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq2_s: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ2_S', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq3_xxs: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ3_XXS', aId, bQuantId, outId, m, n, k);
      },

      gpu_matmul_transb_iq3_s: (aId, bQuantId, outId, m, n, k) => {
        this._dispatchMatmul('matmulTransBIQ3_S', aId, bQuantId, outId, m, n, k);
      },

      // MMV (qLen=1) bindings.
      gpu_matmul_transb_q4_0_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ4_0Mmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q4_1_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ4_1Mmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q5_0_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ5_0Mmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q5_1_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ5_1Mmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q8_0_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ8_0Mmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q8_1_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ8_1Mmv',    a, b, o, m, n, k),
      gpu_matmul_transb_iq4_nl_mmv:  (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ4_NLMmv',  a, b, o, m, n, k),
      gpu_matmul_transb_iq4_xs_mmv:  (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ4_XSMmv',  a, b, o, m, n, k),
      gpu_matmul_transb_q2_k_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ2_KMmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q3_k_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ3_KMmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q4_k_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ4_KMmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q5_k_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ5_KMmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q6_k_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ6_KMmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q8_k_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ8_KMmv',    a, b, o, m, n, k),
      gpu_matmul_transb_i8_s_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBI8_SMmv',    a, b, o, m, n, k),
      gpu_matmul_transb_q1_0_mmv:    (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBQ1_0Mmv',    a, b, o, m, n, k),
      gpu_matmul_transb_tq1_0_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBTQ1_0Mmv',   a, b, o, m, n, k),
      gpu_matmul_transb_tq2_0_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBTQ2_0Mmv',   a, b, o, m, n, k),
      gpu_matmul_transb_mxfp4_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBMXFP4Mmv',   a, b, o, m, n, k),
      gpu_matmul_transb_nvfp4_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBNVFP4Mmv',   a, b, o, m, n, k),
      gpu_matmul_transb_iq1_s_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ1_SMmv',   a, b, o, m, n, k),
      gpu_matmul_transb_iq1_m_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ1_MMmv',   a, b, o, m, n, k),
      gpu_matmul_transb_iq2_xxs_mmv: (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ2_XXSMmv', a, b, o, m, n, k),
      gpu_matmul_transb_iq2_xs_mmv:  (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ2_XSMmv',  a, b, o, m, n, k),
      gpu_matmul_transb_iq2_s_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ2_SMmv',   a, b, o, m, n, k),
      gpu_matmul_transb_iq3_xxs_mmv: (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ3_XXSMmv', a, b, o, m, n, k),
      gpu_matmul_transb_iq3_s_mmv:   (a, b, o, m, n, k) => this._dispatchMatmul('matmulTransBIQ3_SMmv',   a, b, o, m, n, k),

      gpu_attention: (qId, kId, vId, maskId, outId, batch, seqLen, numHeads, headDim) => {
        this._dispatchAttention(qId, kId, vId, maskId, outId, batch, seqLen, numHeads, headDim);
      },

      gpu_deberta_disentangled_attention: (qId, kId, vId, qRelId, kRelId, maskId, outId, batch, seqLen, numHeads, headDim) => {
        this._dispatchDebertaDisentangledAttention(qId, kId, vId, qRelId, kRelId, maskId, outId, batch, seqLen, numHeads, headDim);
      },

      gpu_causal_attention: (qId, kId, vId, outId, batch, seqLen, numHeads, headDim) => {
        this._dispatchCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, headDim);
      },

      gpu_gqa_causal_attention: (qId, kId, vId, outId, batch, seqLen, numHeads, numKvHeads, headDim) => {
        this._dispatchGqaCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, numKvHeads, headDim);
      },

      gpu_gqa_cached_attention: (qId, kId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim) => {
        this._dispatchGqaCachedAttention(qId, kId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim);
      },

      gpu_gqa_cached_attention_ex: (qId, kMainId, kAuxId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim, kFormat, vFormat, kRowBytes, vRowBytes, flags) => {
        this._dispatchGqaCachedAttentionEx(qId, kMainId, kAuxId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim, kFormat, vFormat, kRowBytes, vRowBytes, flags);
      },

      gpu_write_buffer_at_offset: (id, offsetBytes, ptr, sizeBytes) => {
        const buf = this.buffers.get(id);
        if (!buf) return;
        const ptrIndex = toJsIndex(ptr, 'WASM pointer');
        const offset = toJsIndex(offsetBytes, 'GPU buffer offset');
        const size = toJsIndex(sizeBytes, 'GPU upload size');
        const src = new Uint8Array(memory.buffer, ptrIndex, size);
        this.device.queue.writeBuffer(buf, offset, src);
      },

      gpu_cross_attention: (qId, kId, vId, maskId, outId, batch, decSeq, encSeq, numHeads, headDim) => {
        this._dispatchCrossAttention(qId, kId, vId, maskId, outId, batch, decSeq, encSeq, numHeads, headDim);
      },

      gpu_rms_norm: (inputId, weightId, outId, totalRows, dim, epsBits) => {
        this._dispatchRmsNorm(inputId, weightId, outId, totalRows, dim, epsBits);
      },

      gpu_layer_norm: (inputId, gammaId, betaId, outId, totalRows, dim, epsBits) => {
        this._dispatchLayerNorm(inputId, gammaId, betaId, outId, totalRows, dim, epsBits);
      },
    };
  }

  _dispatchMatmul(pipelineName, aId, bId, outId, m, n, k) {
    const aBuf = this.buffers.get(aId);
    const bBuf = this.buffers.get(bId);
    const outBuf = this.buffers.get(outId);
    if (!aBuf || !bBuf || !outBuf) return;

    const paramsData = new Uint32Array([m, n, k, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: aBuf } },
        { binding: 1, resource: { buffer: bBuf } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines[pipelineName]);
    pass.setBindGroup(0, bindGroup);
    // MMV variants ship one workgroup per output column with the K-reduction
    // happening inside the workgroup. GEMM variants tile over M*N with a 16x16
    // workgroup. Detect by name suffix; the bind-group layout is identical.
    if (pipelineName.endsWith('Mmv')) {
      pass.dispatchWorkgroups(n, 1, 1);
    } else {
      pass.dispatchWorkgroups(Math.ceil(n / TILE), Math.ceil(m / TILE));
    }
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchVectorBinary(pipelineName, aId, bId, outId, len) {
    const aBuf = this.buffers.get(aId);
    const bBuf = this.buffers.get(bId);
    const outBuf = this.buffers.get(outId);
    if (!aBuf || !bBuf || !outBuf) return;

    const paramsData = new Uint32Array([len, 0, 0, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: aBuf } },
        { binding: 1, resource: { buffer: bBuf } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines[pipelineName]);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(Math.ceil(len / 256));
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchVectorBinaryBroadcast(pipelineName, aId, bId, outId, len, aLen, bLen) {
    const aBuf = this.buffers.get(aId);
    const bBuf = this.buffers.get(bId);
    const outBuf = this.buffers.get(outId);
    if (!aBuf || !bBuf || !outBuf) return;

    const paramsData = new Uint32Array([len, aLen, bLen, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: aBuf } },
        { binding: 1, resource: { buffer: bBuf } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines[pipelineName]);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(Math.ceil(len / 256));
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchVectorUnary(pipelineName, inputId, outId, len) {
    const inputBuf = this.buffers.get(inputId);
    const outBuf = this.buffers.get(outId);
    if (!inputBuf || !outBuf) return;

    const scratch = inputBuf;
    const paramsData = new Uint32Array([len, 0, 0, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } },
        { binding: 1, resource: { buffer: scratch } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines[pipelineName]);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(Math.ceil(len / 256));
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchWhereSelect(condId, trueId, falseId, outId, len, trueLen, falseLen) {
    const condBuf = this.buffers.get(condId);
    const trueBuf = this.buffers.get(trueId);
    const falseBuf = this.buffers.get(falseId);
    const outBuf = this.buffers.get(outId);
    if (!condBuf || !trueBuf || !falseBuf || !outBuf) return;

    const paramsData = new Uint32Array([len, trueLen, falseLen, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.whereSelectBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: condBuf } },
        { binding: 1, resource: { buffer: trueBuf } },
        { binding: 2, resource: { buffer: falseBuf } },
        { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.whereSelect);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(Math.ceil(len / 256));
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchSoftmax(pipelineName, inputId, outId, rows, dim) {
    const inputBuf = this.buffers.get(inputId);
    const outBuf = this.buffers.get(outId);
    if (!inputBuf || !outBuf) return;

    const scratch = inputBuf;
    const paramsData = new Uint32Array([rows, dim, 0, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } },
        { binding: 1, resource: { buffer: scratch } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines[pipelineName]);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(rows);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchReduceLastDim(pipelineName, inputId, outId, rows, dim) {
    this._dispatchReduceWithParams(
      pipelineName,
      inputId,
      outId,
      rows,
      dim,
      2,
      1,
      [rows, dim, 1, 1, 1, 1, 1, 1],
      [rows, 1, 1, 1, 1, 1, 1, 1],
      [0, 1, 0, 0, 0, 0, 0, 0],
      [dim, 1, 1, 1, 1, 1, 1, 1],
      [1, 1, 1, 1, 1, 1, 1, 1],
      [0, 0, 0, 0, 0, 0, 0, 0],
      [1, 0, 0, 0, 0, 0, 0, 0],
    );
  }

  _dispatchReduce(pipelineName, inputId, outId, outLen, reduceCount, inRank, outRank, memory, inputShapePtr, outShapePtr, reducedPtr, inStridesPtr, outStridesPtr, keptAxesPtr, reducedAxesPtr) {
    const readU32 = (ptr) => {
      const index = toJsIndex(ptr, 'WASM pointer');
      return Array.from(new Uint32Array(memory.buffer, index, 8));
    };
    this._dispatchReduceWithParams(
      pipelineName,
      inputId,
      outId,
      outLen,
      reduceCount,
      inRank,
      outRank,
      readU32(inputShapePtr),
      readU32(outShapePtr),
      readU32(reducedPtr),
      readU32(inStridesPtr),
      readU32(outStridesPtr),
      readU32(keptAxesPtr),
      readU32(reducedAxesPtr),
    );
  }

  _dispatchReduceWithParams(pipelineName, inputId, outId, outLen, reduceCount, inRank, outRank, inputShape, outShape, reduced, inStrides, outStrides, keptAxes, reducedAxes) {
    const inputBuf = this.buffers.get(inputId);
    const outBuf = this.buffers.get(outId);
    if (!inputBuf || !outBuf) return;

    const scratch = inputBuf;
    const paramsData = new Uint32Array([
      outLen, reduceCount, inRank, outRank,
      ...inputShape,
      ...outShape,
      ...reduced,
      ...inStrides,
      ...outStrides,
      ...keptAxes,
      ...reducedAxes,
    ]);
    const paramsBuffer = this.device.createBuffer({
      size: paramsData.byteLength,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } },
        { binding: 1, resource: { buffer: scratch } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines[pipelineName]);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(outLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchBroadcastInDim(inputId, outId, outLen, outRank, inRank, memory, targetShapePtr, inputShapePtr, axesPtr, outStridesPtr, inStridesPtr) {
    const readU32 = (ptr) => {
      const index = toJsIndex(ptr, 'WASM pointer');
      return Array.from(new Uint32Array(memory.buffer, index, 8));
    };
    this._dispatchBroadcastInDimWithParams(
      inputId,
      outId,
      outLen,
      outRank,
      inRank,
      readU32(targetShapePtr),
      readU32(inputShapePtr),
      readU32(axesPtr),
      readU32(outStridesPtr),
      readU32(inStridesPtr),
    );
  }

  _dispatchBroadcastInDimWithParams(inputId, outId, outLen, outRank, inRank, targetShape, inputShape, axes, outStrides, inStrides) {
    const inputBuf = this.buffers.get(inputId);
    const outBuf = this.buffers.get(outId);
    if (!inputBuf || !outBuf) return;

    const scratch = inputBuf;
    const paramsData = new Uint32Array([
      outLen, outRank, inRank, 0,
      ...targetShape,
      ...inputShape,
      ...axes,
      ...outStrides,
      ...inStrides,
    ]);
    const paramsBuffer = this.device.createBuffer({
      size: paramsData.byteLength,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } },
        { binding: 1, resource: { buffer: scratch } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.broadcastInDim);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(Math.ceil(outLen / 256));
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchAttention(qId, kId, vId, maskId, outId, batch, seqLen, numHeads, headDim) {
    const qBuf = this.buffers.get(qId);
    const kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId);
    const maskBuf = this.buffers.get(maskId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !maskBuf || !outBuf) return;

    // Params: seq_len, num_heads, head_dim, scale (16 bytes, matches Params struct)
    const scale = 1.0 / Math.sqrt(headDim);
    const paramsAB = new ArrayBuffer(16);
    const paramsU32 = new Uint32Array(paramsAB);
    const paramsF32 = new Float32Array(paramsAB);
    paramsU32[0] = seqLen;
    paramsU32[1] = numHeads;
    paramsU32[2] = headDim;
    paramsF32[3] = scale;

    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsAB);

    const bindGroup = this.device.createBindGroup({
      layout: this.attnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } },
        { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } },
        { binding: 3, resource: { buffer: maskBuf } },
        { binding: 4, resource: { buffer: outBuf } },
        { binding: 5, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.attention);
    pass.setBindGroup(0, bindGroup);
    // Dispatch (num_heads, batch, seq_len) workgroups
    pass.dispatchWorkgroups(numHeads, batch, seqLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchDebertaDisentangledAttention(qId, kId, vId, qRelId, kRelId, maskId, outId, batch, seqLen, numHeads, headDim) {
    const qBuf = this.buffers.get(qId);
    const kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId);
    const qRelBuf = this.buffers.get(qRelId);
    const kRelBuf = this.buffers.get(kRelId);
    const maskBuf = this.buffers.get(maskId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !qRelBuf || !kRelBuf || !maskBuf || !outBuf) return;

    const scale = 1.0 / Math.sqrt(headDim * 3.0);
    const paramsAB = new ArrayBuffer(16);
    const paramsU32 = new Uint32Array(paramsAB);
    const paramsF32 = new Float32Array(paramsAB);
    paramsU32[0] = seqLen;
    paramsU32[1] = numHeads;
    paramsU32[2] = headDim;
    paramsF32[3] = scale;

    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsAB);

    const bindGroup = this.device.createBindGroup({
      layout: this.debertaAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } },
        { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } },
        { binding: 3, resource: { buffer: qRelBuf } },
        { binding: 4, resource: { buffer: kRelBuf } },
        { binding: 5, resource: { buffer: maskBuf } },
        { binding: 6, resource: { buffer: outBuf } },
        { binding: 7, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.debertaDisentangledAttention);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(numHeads, batch, seqLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, headDim) {
    const qBuf = this.buffers.get(qId);
    const kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !outBuf) return;

    // Params: seq_len, num_heads, head_dim, scale (16 bytes)
    const scale = 1.0 / Math.sqrt(headDim);
    const paramsAB = new ArrayBuffer(16);
    const paramsU32 = new Uint32Array(paramsAB);
    const paramsF32 = new Float32Array(paramsAB);
    paramsU32[0] = seqLen;
    paramsU32[1] = numHeads;
    paramsU32[2] = headDim;
    paramsF32[3] = scale;

    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsAB);

    const bindGroup = this.device.createBindGroup({
      layout: this.causalAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } },
        { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } },
        { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.causalAttention);
    pass.setBindGroup(0, bindGroup);
    // Dispatch (num_heads, batch, seq_len) workgroups
    pass.dispatchWorkgroups(numHeads, batch, seqLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchGqaCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, numKvHeads, headDim) {
    const qBuf = this.buffers.get(qId);
    const kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !outBuf) return;

    // Params: seq_len, num_heads, num_kv_heads, head_dim, scale, _pad (24 bytes)
    const scale = 1.0 / Math.sqrt(headDim);
    const paramsAB = new ArrayBuffer(24);
    const paramsU32 = new Uint32Array(paramsAB);
    const paramsF32 = new Float32Array(paramsAB);
    paramsU32[0] = seqLen;
    paramsU32[1] = numHeads;
    paramsU32[2] = numKvHeads;
    paramsU32[3] = headDim;
    paramsF32[4] = scale;
    paramsU32[5] = 0; // padding

    const paramsBuffer = this.device.createBuffer({
      size: 24,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsAB);

    const bindGroup = this.device.createBindGroup({
      layout: this.causalAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } },
        { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } },
        { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.gqaCausalAttention);
    pass.setBindGroup(0, bindGroup);
    // Dispatch (num_heads, batch, seq_len) workgroups
    pass.dispatchWorkgroups(numHeads, batch, seqLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchGqaCachedAttention(qId, kId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim) {
    if (!this._validateGqaCachedAttentionLength(kvLen)) return;

    const qBuf = this.buffers.get(qId);
    const kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !outBuf) return;

    // Params: q_len, kv_len, num_heads, num_kv_heads, head_dim, scale (24 bytes)
    const scale = 1.0 / Math.sqrt(headDim);
    const paramsAB = new ArrayBuffer(24);
    const paramsU32 = new Uint32Array(paramsAB);
    const paramsF32 = new Float32Array(paramsAB);
    paramsU32[0] = qLen;
    paramsU32[1] = kvLen;
    paramsU32[2] = numHeads;
    paramsU32[3] = numKvHeads;
    paramsU32[4] = headDim;
    paramsF32[5] = scale;

    const paramsBuffer = this.device.createBuffer({
      size: 24,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsAB);

    const bindGroup = this.device.createBindGroup({
      layout: this.causalAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } },
        { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } },
        { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.gqaCachedAttention);
    pass.setBindGroup(0, bindGroup);
    // Dispatch (num_heads, batch, q_len) workgroups
    pass.dispatchWorkgroups(numHeads, batch, qLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchGqaCachedAttentionEx(qId, kMainId, kAuxId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim, kFormat, vFormat, kRowBytes, vRowBytes, flags) {
    void kAuxId;
    void flags;

    if (kFormat === GQA_K_FORMAT_F32 && vFormat === GQA_V_FORMAT_F32) {
      this._dispatchGqaCachedAttention(qId, kMainId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim);
      return;
    }

    if (kFormat === GQA_K_FORMAT_POLAR4 && vFormat === GQA_V_FORMAT_F32) {
      const expectedKRowBytes = (numKvHeads * headDim) >> 1;
      const expectedVRowBytes = numKvHeads * headDim * 4;
      if ((headDim !== 64 && headDim !== 128) || kRowBytes !== expectedKRowBytes || vRowBytes !== expectedVRowBytes) {
        console.warn('unsupported polar4 cached attention shape', { headDim, kRowBytes, expectedKRowBytes, vRowBytes, expectedVRowBytes });
        return;
      }
      this._dispatchGqaCachedAttentionPolar4(qId, kMainId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim);
      return;
    }

    if (kFormat === GQA_K_FORMAT_TURBO3 && vFormat === GQA_V_FORMAT_F32) {
      const baseKRowBytes = Math.ceil((numKvHeads * headDim * 3) / 8);
      const residualKRowBytes = Math.ceil((numKvHeads * 32) / 8);
      const expectedKRowBytes = baseKRowBytes + residualKRowBytes;
      const expectedVRowBytes = numKvHeads * headDim * 4;
      if ((headDim !== 64 && headDim !== 128) || kRowBytes !== expectedKRowBytes || vRowBytes !== expectedVRowBytes) {
        console.warn('unsupported turbo3 cached attention shape', { headDim, kRowBytes, expectedKRowBytes, vRowBytes, expectedVRowBytes });
        return;
      }
      this._dispatchGqaCachedAttentionTurbo3(qId, kMainId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim);
      return;
    }

    console.warn('unsupported cached attention KV format', { kFormat, vFormat });
  }

  _dispatchGqaCachedAttentionPolar4(qId, kPolar4Id, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim) {
    this._dispatchGqaCachedAttentionPacked('gqaCachedAttentionPolar4', qId, kPolar4Id, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim);
  }

  _dispatchGqaCachedAttentionTurbo3(qId, kTurbo3Id, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim) {
    this._dispatchGqaCachedAttentionPacked('gqaCachedAttentionTurbo3', qId, kTurbo3Id, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim);
  }

  _dispatchGqaCachedAttentionPacked(pipelineName, qId, kPackedId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim) {
    if (!this._validateGqaCachedAttentionLength(kvLen)) return;

    const qBuf = this.buffers.get(qId);
    const kBuf = this.buffers.get(kPackedId);
    const vBuf = this.buffers.get(vId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !outBuf) return;

    // Params: q_len, kv_len, num_heads, num_kv_heads, head_dim, scale (24 bytes)
    const scale = 1.0 / Math.sqrt(headDim);
    const paramsAB = new ArrayBuffer(24);
    const paramsU32 = new Uint32Array(paramsAB);
    const paramsF32 = new Float32Array(paramsAB);
    paramsU32[0] = qLen;
    paramsU32[1] = kvLen;
    paramsU32[2] = numHeads;
    paramsU32[3] = numKvHeads;
    paramsU32[4] = headDim;
    paramsF32[5] = scale;

    const paramsBuffer = this.device.createBuffer({
      size: 24,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsAB);

    const bindGroup = this.device.createBindGroup({
      layout: this.causalAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } },
        { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } },
        { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines[pipelineName]);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(numHeads, batch, qLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _validateGqaCachedAttentionLength(kvLen) {
    if (kvLen <= GQA_CACHED_ATTN_MAX_KV) return true;
    console.warn('unsupported cached attention length', { kvLen, maxKvLen: GQA_CACHED_ATTN_MAX_KV });
    return false;
  }

  _dispatchCrossAttention(qId, kId, vId, maskId, outId, batch, decSeq, encSeq, numHeads, headDim) {
    const qBuf = this.buffers.get(qId);
    const kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId);
    const maskBuf = this.buffers.get(maskId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !maskBuf || !outBuf) return;

    // Params: dec_seq, enc_seq, num_heads, head_dim, scale, _pad (24 bytes)
    const scale = 1.0 / Math.sqrt(headDim);
    const paramsAB = new ArrayBuffer(24);
    const paramsU32 = new Uint32Array(paramsAB);
    const paramsF32 = new Float32Array(paramsAB);
    paramsU32[0] = decSeq;
    paramsU32[1] = encSeq;
    paramsU32[2] = numHeads;
    paramsU32[3] = headDim;
    paramsF32[4] = scale;
    paramsU32[5] = 0; // padding

    const paramsBuffer = this.device.createBuffer({
      size: 24,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsAB);

    const bindGroup = this.device.createBindGroup({
      layout: this.crossAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } },
        { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } },
        { binding: 3, resource: { buffer: maskBuf } },
        { binding: 4, resource: { buffer: outBuf } },
        { binding: 5, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.crossAttention);
    pass.setBindGroup(0, bindGroup);
    // Dispatch (num_heads, batch, dec_seq) workgroups
    pass.dispatchWorkgroups(numHeads, batch, decSeq);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchRmsNorm(inputId, weightId, outId, totalRows, dim, epsBits) {
    const inputBuf = this.buffers.get(inputId);
    const weightBuf = this.buffers.get(weightId);
    const outBuf = this.buffers.get(outId);
    if (!inputBuf || !weightBuf || !outBuf) return;

    const paramsData = new Uint32Array([totalRows, dim, epsBits, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } },
        { binding: 1, resource: { buffer: weightBuf } },
        { binding: 2, resource: { buffer: outBuf } },
        { binding: 3, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.rmsNorm);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(totalRows);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  _dispatchLayerNorm(inputId, gammaId, betaId, outId, totalRows, dim, epsBits) {
    const inputBuf = this.buffers.get(inputId);
    const gammaBuf = this.buffers.get(gammaId);
    const betaBuf = this.buffers.get(betaId);
    const outBuf = this.buffers.get(outId);
    if (!inputBuf || !gammaBuf || !betaBuf || !outBuf) return;

    const paramsData = new Uint32Array([totalRows, dim, epsBits, 0]);
    const paramsBuffer = this.device.createBuffer({
      size: 16,
      usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.normBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } },
        { binding: 1, resource: { buffer: gammaBuf } },
        { binding: 2, resource: { buffer: betaBuf } },
        { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.layerNorm);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(totalRows);
    pass.end();
    this.device.queue.submit([encoder.finish()]);

    paramsBuffer.destroy();
  }

  /**
   * Flush pending GPU work and resolve any pending downloads.
   * Call this after a sequence of GPU ops to ensure results are available.
   * Only needed in direct mode — worker mode downloads are synchronous.
   * @returns {Promise<void>}
   */
  async flush() {
    await this.device.queue.onSubmittedWorkDone();

    if (this._pendingDownload) {
      const { staging, ptr, sizeBytes, memory } = this._pendingDownload;
      this._pendingDownload = null;

      await staging.mapAsync(GPUMapMode.READ);
      const mapped = new Uint8Array(staging.getMappedRange());
      const dst = new Uint8Array(memory.buffer, ptr, sizeBytes);
      dst.set(mapped);
      staging.unmap();
      staging.destroy();
    }
  }

  /**
   * Handle a GPU command from the inference worker (worker mode).
   * Called by the main thread when the worker sends a gpu-sync message.
   *
   * @param {object} msg - Command message from worker
   * @param {SharedArrayBuffer} sab - Shared buffer for sync + data
   * @returns {Promise<void>}
   */
  async handleWorkerCommand(msg, sab) {
    const ctrl = new Int32Array(sab, 0, 16);
    const dataOffset = 64;
    let result = 0;

    switch (msg.cmd) {
      case 'is_available':
        result = this.device ? 1 : 0;
        break;

      case 'create_buffer': {
        const size = toJsIndex(msg.size, 'GPU buffer size');
        const buf = this.device.createBuffer({
          size,
          usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
        });
        const id = this.nextId++;
        this.buffers.set(id, buf);
        result = id;
        break;
      }

      case 'free_buffer': {
        const buf = this.buffers.get(msg.id);
        if (buf) {
          buf.destroy();
          this.buffers.delete(msg.id);
        }
        break;
      }

      case 'upload': {
        const buf = this.buffers.get(msg.id);
        if (buf) {
          const size = toJsIndex(msg.size, 'GPU upload size');
          const src = new Uint8Array(sab, dataOffset, size);
          this.device.queue.writeBuffer(buf, 0, src);
        }
        break;
      }

      case 'write_buffer_at_offset': {
        const buf = this.buffers.get(msg.id);
        if (buf) {
          const size = toJsIndex(msg.sizeBytes, 'GPU upload size');
          const offset = toJsIndex(msg.offsetBytes, 'GPU buffer offset');
          const src = new Uint8Array(sab, dataOffset, size);
          this.device.queue.writeBuffer(buf, offset, src);
        }
        break;
      }

      case 'download': {
        const buf = this.buffers.get(msg.id);
        if (buf) {
          const size = toJsIndex(msg.size, 'GPU download size');
          const staging = this.device.createBuffer({
            size,
            usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST,
          });

          const encoder = this.device.createCommandEncoder();
          encoder.copyBufferToBuffer(buf, 0, staging, 0, size);
          this.device.queue.submit([encoder.finish()]);

          await this.device.queue.onSubmittedWorkDone();
          await staging.mapAsync(GPUMapMode.READ);

          const mapped = new Uint8Array(staging.getMappedRange());
          const dst = new Uint8Array(sab, dataOffset, msg.size);
          dst.set(mapped);

          staging.unmap();
          staging.destroy();
        }
        break;
      }

      case 'copy_buffer_to_buffer': {
        this.copyBufferToBuffer(msg.src, msg.srcOffsetBytes, msg.dst, msg.dstOffsetBytes, msg.sizeBytes);
        break;
      }

      case 'matmul':
        this._dispatchMatmul('matmul', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb':
        this._dispatchMatmul('matmulTransB', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'add':
        this._dispatchVectorBinary('add', msg.a, msg.b, msg.out, msg.len);
        break;

      case 'add_broadcast':
        this._dispatchVectorBinaryBroadcast('addBroadcast', msg.a, msg.b, msg.out, msg.len, msg.aLen, msg.bLen);
        break;

      case 'mul':
        this._dispatchVectorBinaryBroadcast('mul', msg.a, msg.b, msg.out, msg.len, msg.aLen, msg.bLen);
        break;

      case 'sub':
        this._dispatchVectorBinaryBroadcast('sub', msg.a, msg.b, msg.out, msg.len, msg.aLen, msg.bLen);
        break;

      case 'div':
        this._dispatchVectorBinaryBroadcast('div', msg.a, msg.b, msg.out, msg.len, msg.aLen, msg.bLen);
        break;

      case 'less_than':
        this._dispatchVectorBinaryBroadcast('lessThan', msg.a, msg.b, msg.out, msg.len, msg.aLen, msg.bLen);
        break;

      case 'where_select':
        this._dispatchWhereSelect(msg.cond, msg.onTrue, msg.onFalse, msg.out, msg.len, msg.trueLen, msg.falseLen);
        break;

      case 'neg':
      case 'sqrt':
      case 'rsqrt':
      case 'exp':
      case 'log':
      case 'sin':
      case 'cos':
      case 'tanh':
      case 'abs':
      case 'erf':
        this._dispatchVectorUnary(msg.cmd, msg.input, msg.out, msg.len);
        break;

      case 'gelu':
        this._dispatchVectorUnary('gelu', msg.input, msg.out, msg.len);
        break;

      case 'softmax':
        this._dispatchSoftmax('softmax', msg.input, msg.out, msg.rows, msg.dim);
        break;

      case 'log_softmax':
        this._dispatchSoftmax('logSoftmax', msg.input, msg.out, msg.rows, msg.dim);
        break;

      case 'reduce_sum_last_dim':
        this._dispatchReduceLastDim('reduceSum', msg.input, msg.out, msg.rows, msg.dim);
        break;

      case 'reduce_max_last_dim':
        this._dispatchReduceLastDim('reduceMax', msg.input, msg.out, msg.rows, msg.dim);
        break;

      case 'reduce_mean_last_dim':
        this._dispatchReduceLastDim('reduceMean', msg.input, msg.out, msg.rows, msg.dim);
        break;

      case 'reduce_sum':
        this._dispatchReduceWithParams('reduceSum', msg.input, msg.out, msg.outLen, msg.reduceCount, msg.inRank, msg.outRank, msg.inputShape, msg.outShape, msg.reduced, msg.inStrides, msg.outStrides, msg.keptAxes, msg.reducedAxes);
        break;

      case 'reduce_max':
        this._dispatchReduceWithParams('reduceMax', msg.input, msg.out, msg.outLen, msg.reduceCount, msg.inRank, msg.outRank, msg.inputShape, msg.outShape, msg.reduced, msg.inStrides, msg.outStrides, msg.keptAxes, msg.reducedAxes);
        break;

      case 'reduce_mean':
        this._dispatchReduceWithParams('reduceMean', msg.input, msg.out, msg.outLen, msg.reduceCount, msg.inRank, msg.outRank, msg.inputShape, msg.outShape, msg.reduced, msg.inStrides, msg.outStrides, msg.keptAxes, msg.reducedAxes);
        break;

      case 'broadcast_in_dim':
        this._dispatchBroadcastInDimWithParams(msg.input, msg.out, msg.outLen, msg.outRank, msg.inRank, msg.targetShape, msg.inputShape, msg.axes, msg.outStrides, msg.inStrides);
        break;

      case 'matmul_transb_q4_0':
        this._dispatchMatmul('matmulTransBQ4_0', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q4_1':
        this._dispatchMatmul('matmulTransBQ4_1', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q5_0':
        this._dispatchMatmul('matmulTransBQ5_0', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q5_1':
        this._dispatchMatmul('matmulTransBQ5_1', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q8_0':
        this._dispatchMatmul('matmulTransBQ8_0', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q8_1':
        this._dispatchMatmul('matmulTransBQ8_1', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq4_nl':
        this._dispatchMatmul('matmulTransBIQ4_NL', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq4_xs':
        this._dispatchMatmul('matmulTransBIQ4_XS', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q2_k':
        this._dispatchMatmul('matmulTransBQ2_K', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q3_k':
        this._dispatchMatmul('matmulTransBQ3_K', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q4_k':
        this._dispatchMatmul('matmulTransBQ4_K', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q5_k':
        this._dispatchMatmul('matmulTransBQ5_K', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q6_k':
        this._dispatchMatmul('matmulTransBQ6_K', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q8_k':
        this._dispatchMatmul('matmulTransBQ8_K', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_i2_s':
        this._dispatchMatmul('matmulTransBI2_S', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_i8_s':
        this._dispatchMatmul('matmulTransBI8_S', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_q1_0':
        this._dispatchMatmul('matmulTransBQ1_0', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_tq1_0':
        this._dispatchMatmul('matmulTransBTQ1_0', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_tq2_0':
        this._dispatchMatmul('matmulTransBTQ2_0', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_mxfp4':
        this._dispatchMatmul('matmulTransBMXFP4', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_nvfp4':
        this._dispatchMatmul('matmulTransBNVFP4', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq1_s':
        this._dispatchMatmul('matmulTransBIQ1_S', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq1_m':
        this._dispatchMatmul('matmulTransBIQ1_M', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq2_xxs':
        this._dispatchMatmul('matmulTransBIQ2_XXS', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq2_xs':
        this._dispatchMatmul('matmulTransBIQ2_XS', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq2_s':
        this._dispatchMatmul('matmulTransBIQ2_S', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq3_xxs':
        this._dispatchMatmul('matmulTransBIQ3_XXS', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      case 'matmul_transb_iq3_s':
        this._dispatchMatmul('matmulTransBIQ3_S', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k);
        break;

      // MMV (qLen=1) variants.
      case 'matmul_transb_q4_0_mmv':    this._dispatchMatmul('matmulTransBQ4_0Mmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q4_1_mmv':    this._dispatchMatmul('matmulTransBQ4_1Mmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q5_0_mmv':    this._dispatchMatmul('matmulTransBQ5_0Mmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q5_1_mmv':    this._dispatchMatmul('matmulTransBQ5_1Mmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q8_0_mmv':    this._dispatchMatmul('matmulTransBQ8_0Mmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q8_1_mmv':    this._dispatchMatmul('matmulTransBQ8_1Mmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq4_nl_mmv':  this._dispatchMatmul('matmulTransBIQ4_NLMmv',  msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq4_xs_mmv':  this._dispatchMatmul('matmulTransBIQ4_XSMmv',  msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q2_k_mmv':    this._dispatchMatmul('matmulTransBQ2_KMmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q3_k_mmv':    this._dispatchMatmul('matmulTransBQ3_KMmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q4_k_mmv':    this._dispatchMatmul('matmulTransBQ4_KMmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q5_k_mmv':    this._dispatchMatmul('matmulTransBQ5_KMmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q6_k_mmv':    this._dispatchMatmul('matmulTransBQ6_KMmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q8_k_mmv':    this._dispatchMatmul('matmulTransBQ8_KMmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_i8_s_mmv':    this._dispatchMatmul('matmulTransBI8_SMmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_q1_0_mmv':    this._dispatchMatmul('matmulTransBQ1_0Mmv',    msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_tq1_0_mmv':   this._dispatchMatmul('matmulTransBTQ1_0Mmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_tq2_0_mmv':   this._dispatchMatmul('matmulTransBTQ2_0Mmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_mxfp4_mmv':   this._dispatchMatmul('matmulTransBMXFP4Mmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_nvfp4_mmv':   this._dispatchMatmul('matmulTransBNVFP4Mmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq1_s_mmv':   this._dispatchMatmul('matmulTransBIQ1_SMmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq1_m_mmv':   this._dispatchMatmul('matmulTransBIQ1_MMmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq2_xxs_mmv': this._dispatchMatmul('matmulTransBIQ2_XXSMmv', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq2_xs_mmv':  this._dispatchMatmul('matmulTransBIQ2_XSMmv',  msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq2_s_mmv':   this._dispatchMatmul('matmulTransBIQ2_SMmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq3_xxs_mmv': this._dispatchMatmul('matmulTransBIQ3_XXSMmv', msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;
      case 'matmul_transb_iq3_s_mmv':   this._dispatchMatmul('matmulTransBIQ3_SMmv',   msg.a, msg.b, msg.out, msg.m, msg.n, msg.k); break;

      case 'attention':
        this._dispatchAttention(
          msg.q, msg.k, msg.v, msg.mask, msg.out,
          msg.batch, msg.seqLen, msg.numHeads, msg.headDim,
        );
        break;

      case 'deberta_disentangled_attention':
        this._dispatchDebertaDisentangledAttention(
          msg.q, msg.k, msg.v, msg.qRel, msg.kRel, msg.mask, msg.out,
          msg.batch, msg.seqLen, msg.numHeads, msg.headDim,
        );
        break;

      case 'causal_attention':
        this._dispatchCausalAttention(
          msg.q, msg.k, msg.v, msg.out,
          msg.batch, msg.seqLen, msg.numHeads, msg.headDim,
        );
        break;

      case 'gqa_causal_attention':
        this._dispatchGqaCausalAttention(
          msg.q, msg.k, msg.v, msg.out,
          msg.batch, msg.seqLen, msg.numHeads, msg.numKvHeads, msg.headDim,
        );
        break;

      case 'gqa_cached_attention':
        this._dispatchGqaCachedAttention(
          msg.q, msg.k, msg.v, msg.out,
          msg.batch, msg.qLen, msg.kvLen, msg.numHeads, msg.numKvHeads, msg.headDim,
        );
        break;

      case 'gqa_cached_attention_ex':
        this._dispatchGqaCachedAttentionEx(
          msg.q, msg.kMain, msg.kAux, msg.v, msg.out,
          msg.batch, msg.qLen, msg.kvLen, msg.numHeads, msg.numKvHeads, msg.headDim,
          msg.kFormat, msg.vFormat, msg.kRowBytes, msg.vRowBytes, msg.flags,
        );
        break;

      case 'cross_attention':
        this._dispatchCrossAttention(
          msg.q, msg.k, msg.v, msg.mask, msg.out,
          msg.batch, msg.decSeq, msg.encSeq, msg.numHeads, msg.headDim,
        );
        break;

      case 'rms_norm':
        this._dispatchRmsNorm(msg.input, msg.weight, msg.out, msg.totalRows, msg.dim, msg.epsBits);
        break;

      case 'layer_norm':
        this._dispatchLayerNorm(msg.input, msg.gamma, msg.beta, msg.out, msg.totalRows, msg.dim, msg.epsBits);
        break;
    }

    // Write result and signal worker
    Atomics.store(ctrl, 1, result);     // ctrl[1] = result value
    Atomics.store(ctrl, 0, 1);          // ctrl[0] = response ready
    Atomics.notify(ctrl, 0);            // wake worker
  }

  destroy() {
    for (const buf of this.buffers.values()) {
      buf.destroy();
    }
    this.buffers.clear();
    this.device?.destroy();
    this.device = null;
  }
}
