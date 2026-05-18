// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// WebGPU compute shader dispatch for unified antfly.wasm.
//
// Adapted from termite-zig/web/webgpu-ops.js for non-bundler usage.
// Shaders are loaded via fetch() relative to this module's URL.
//
// Provides implementations for the extern "webgpu" imports declared in
// termite's src/ops/wasm_extern.zig. Heavy ops (matmul, attention) are
// dispatched to GPU compute shaders; WASM SIMD handles everything else.

const TILE = 16;

async function loadShader(baseUrl, name) {
  const url = new URL(`./shaders/${name}.wgsl`, baseUrl);
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`failed to load shader ${name}: ${resp.status}`);
  return resp.text();
}

export class WebGPUOps {
  constructor() {
    this.device = null;
    this.buffers = new Map();
    this.nextId = 1;
    this.pipelines = {};
    this.matmulBindGroupLayout = null;
    this.attnBindGroupLayout = null;
    this.causalAttnBindGroupLayout = null;
    this.crossAttnBindGroupLayout = null;
  }

  /**
   * Initialize WebGPU device and compile shaders.
   * @param {string|URL} [baseUrl] - Base URL for shader fetch. Defaults to this module's URL.
   * @returns {boolean} true if WebGPU is available
   */
  async init(baseUrl) {
    if (typeof navigator === "undefined" || !navigator.gpu) return false;

    const adapter = await navigator.gpu.requestAdapter();
    if (!adapter) return false;

    this.device = await adapter.requestDevice();
    if (!this.device) return false;

    const base = baseUrl ?? import.meta.url;

    const [
      matmulSource,
      matmulTransBSource,
      matmulTransBQ4_0Source,
      matmulTransBQ4_1Source,
      matmulTransBQ5_0Source,
      matmulTransBQ5_1Source,
      matmulTransBQ8_0Source,
      matmulTransBQ8_1Source,
      matmulTransBIQ4_NLSource,
      matmulTransBIQ4_XSSource,
      matmulTransBQ2_KSource,
      matmulTransBQ3_KSource,
      matmulTransBQ4_KSource,
      matmulTransBQ5_KSource,
      matmulTransBQ6_KSource,
      matmulTransBQ8_KSource,
      attentionSource,
      causalAttentionSource,
      crossAttentionSource,
      gqaCausalAttentionSource,
      gqaCachedAttentionSource,
      rmsNormSource,
      layerNormSource,
    ] = await Promise.all([
      loadShader(base, "matmul"),
      loadShader(base, "matmul_transb"),
      loadShader(base, "matmul_transb_q4_0"),
      loadShader(base, "matmul_transb_q4_1"),
      loadShader(base, "matmul_transb_q5_0"),
      loadShader(base, "matmul_transb_q5_1"),
      loadShader(base, "matmul_transb_q8_0"),
      loadShader(base, "matmul_transb_q8_1"),
      loadShader(base, "matmul_transb_iq4_nl"),
      loadShader(base, "matmul_transb_iq4_xs"),
      loadShader(base, "matmul_transb_q2_k"),
      loadShader(base, "matmul_transb_q3_k"),
      loadShader(base, "matmul_transb_q4_k"),
      loadShader(base, "matmul_transb_q5_k"),
      loadShader(base, "matmul_transb_q6_k"),
      loadShader(base, "matmul_transb_q8_k"),
      loadShader(base, "attention"),
      loadShader(base, "causal_attention"),
      loadShader(base, "cross_attention"),
      loadShader(base, "gqa_causal_attention"),
      loadShader(base, "gqa_cached_attention"),
      loadShader(base, "rms_norm"),
      loadShader(base, "layer_norm"),
    ]);

    // --- Matmul bind group layout (4 bindings: A, B, C, params) ---
    this.matmulBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: "storage" } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: "uniform" } },
      ],
    });

    const matmulPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.matmulBindGroupLayout],
    });

    this.pipelines.matmul = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulSource }), entryPoint: "matmul" },
    });
    this.pipelines.matmulTransB = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBSource }), entryPoint: "matmul_transb" },
    });
    this.pipelines.matmulTransBQ4_0 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ4_0Source }), entryPoint: "matmul_transb_q4_0" },
    });
    this.pipelines.matmulTransBQ4_1 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ4_1Source }), entryPoint: "matmul_transb_q4_1" },
    });
    this.pipelines.matmulTransBQ5_0 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ5_0Source }), entryPoint: "matmul_transb_q5_0" },
    });
    this.pipelines.matmulTransBQ5_1 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ5_1Source }), entryPoint: "matmul_transb_q5_1" },
    });
    this.pipelines.matmulTransBQ8_0 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ8_0Source }), entryPoint: "matmul_transb_q8_0" },
    });
    this.pipelines.matmulTransBQ8_1 = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ8_1Source }), entryPoint: "matmul_transb_q8_1" },
    });
    this.pipelines.matmulTransBIQ4_NL = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBIQ4_NLSource }), entryPoint: "matmul_transb_iq4_nl" },
    });
    this.pipelines.matmulTransBIQ4_XS = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBIQ4_XSSource }), entryPoint: "matmul_transb_iq4_xs" },
    });
    this.pipelines.matmulTransBQ2_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ2_KSource }), entryPoint: "matmul_transb_q2_k" },
    });
    this.pipelines.matmulTransBQ3_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ3_KSource }), entryPoint: "matmul_transb_q3_k" },
    });
    this.pipelines.matmulTransBQ4_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ4_KSource }), entryPoint: "matmul_transb_q4_k" },
    });
    this.pipelines.matmulTransBQ5_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ5_KSource }), entryPoint: "matmul_transb_q5_k" },
    });
    this.pipelines.matmulTransBQ6_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ6_KSource }), entryPoint: "matmul_transb_q6_k" },
    });
    this.pipelines.matmulTransBQ8_K = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: matmulTransBQ8_KSource }), entryPoint: "matmul_transb_q8_k" },
    });

    // --- Attention bind group layout (6 bindings: Q, K, V, mask, out, params) ---
    this.attnBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: "storage" } },
        { binding: 5, visibility: GPUShaderStage.COMPUTE, buffer: { type: "uniform" } },
      ],
    });

    const attnPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.attnBindGroupLayout],
    });

    this.pipelines.attention = this.device.createComputePipeline({
      layout: attnPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: attentionSource }), entryPoint: "attention" },
    });

    // --- Causal attention bind group layout (5 bindings: Q, K, V, out, params) ---
    this.causalAttnBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: "storage" } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: "uniform" } },
      ],
    });

    const causalAttnPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.causalAttnBindGroupLayout],
    });

    this.pipelines.causalAttention = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: causalAttentionSource }), entryPoint: "causal_attention" },
    });
    this.pipelines.gqaCausalAttention = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: gqaCausalAttentionSource }), entryPoint: "gqa_causal_attention" },
    });
    this.pipelines.gqaCachedAttention = this.device.createComputePipeline({
      layout: causalAttnPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: gqaCachedAttentionSource }), entryPoint: "gqa_cached_attention" },
    });

    // --- Cross attention (reuses 6-binding attn layout) ---
    this.crossAttnBindGroupLayout = this.attnBindGroupLayout;
    const crossAttnPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.crossAttnBindGroupLayout],
    });
    this.pipelines.crossAttention = this.device.createComputePipeline({
      layout: crossAttnPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: crossAttentionSource }), entryPoint: "cross_attention" },
    });

    // --- Norm pipelines ---
    this.pipelines.rmsNorm = this.device.createComputePipeline({
      layout: matmulPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: rmsNormSource }), entryPoint: "rms_norm" },
    });

    this.normBindGroupLayout = this.device.createBindGroupLayout({
      entries: [
        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 2, visibility: GPUShaderStage.COMPUTE, buffer: { type: "read-only-storage" } },
        { binding: 3, visibility: GPUShaderStage.COMPUTE, buffer: { type: "storage" } },
        { binding: 4, visibility: GPUShaderStage.COMPUTE, buffer: { type: "uniform" } },
      ],
    });
    const normPipelineLayout = this.device.createPipelineLayout({
      bindGroupLayouts: [this.normBindGroupLayout],
    });
    this.pipelines.layerNorm = this.device.createComputePipeline({
      layout: normPipelineLayout,
      compute: { module: this.device.createShaderModule({ code: layerNormSource }), entryPoint: "layer_norm" },
    });

    return true;
  }

  /**
   * Build WASM import object for the "webgpu" module.
   * @param {WebAssembly.Memory} memory - WASM linear memory
   * @returns {object} import functions for WebAssembly.instantiate
   */
  getImports(memory) {
    return {
      gpu_is_available: () => (this.device ? 1 : 0),

      gpu_create_buffer: (sizeBytes) => {
        const buf = this.device.createBuffer({
          size: sizeBytes,
          usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
        });
        const id = this.nextId++;
        this.buffers.set(id, buf);
        return id;
      },

      gpu_free_buffer: (id) => {
        const buf = this.buffers.get(id);
        if (buf) { buf.destroy(); this.buffers.delete(id); }
      },

      gpu_upload: (id, ptr, sizeBytes) => {
        const buf = this.buffers.get(id);
        if (!buf) return;
        this.device.queue.writeBuffer(buf, 0, new Uint8Array(memory.buffer, ptr, sizeBytes));
      },

      gpu_download: (id, ptr, sizeBytes) => {
        const buf = this.buffers.get(id);
        if (!buf) return;
        const staging = this.device.createBuffer({
          size: sizeBytes,
          usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST,
        });
        const encoder = this.device.createCommandEncoder();
        encoder.copyBufferToBuffer(buf, 0, staging, 0, sizeBytes);
        this.device.queue.submit([encoder.finish()]);
        this._pendingDownload = { staging, ptr, sizeBytes, memory };
      },

      gpu_write_buffer_at_offset: (id, offsetBytes, ptr, sizeBytes) => {
        const buf = this.buffers.get(id);
        if (!buf) return;
        this.device.queue.writeBuffer(buf, offsetBytes, new Uint8Array(memory.buffer, ptr, sizeBytes));
      },

      gpu_matmul_transb: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransB", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q4_0: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ4_0", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q4_1: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ4_1", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q5_0: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ5_0", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q5_1: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ5_1", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q8_0: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ8_0", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q8_1: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ8_1", aId, bId, outId, m, n, k),
      gpu_matmul_transb_iq4_nl: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBIQ4_NL", aId, bId, outId, m, n, k),
      gpu_matmul_transb_iq4_xs: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBIQ4_XS", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q2_k: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ2_K", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q3_k: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ3_K", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q4_k: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ4_K", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q5_k: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ5_K", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q6_k: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ6_K", aId, bId, outId, m, n, k),
      gpu_matmul_transb_q8_k: (aId, bId, outId, m, n, k) => this._dispatchMatmul("matmulTransBQ8_K", aId, bId, outId, m, n, k),

      gpu_attention: (qId, kId, vId, maskId, outId, batch, seqLen, numHeads, headDim) =>
        this._dispatchAttention(qId, kId, vId, maskId, outId, batch, seqLen, numHeads, headDim),
      gpu_causal_attention: (qId, kId, vId, outId, batch, seqLen, numHeads, headDim) =>
        this._dispatchCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, headDim),
      gpu_cross_attention: (qId, kId, vId, maskId, outId, batch, decSeq, encSeq, numHeads, headDim) =>
        this._dispatchCrossAttention(qId, kId, vId, maskId, outId, batch, decSeq, encSeq, numHeads, headDim),
      gpu_gqa_causal_attention: (qId, kId, vId, outId, batch, seqLen, numHeads, numKvHeads, headDim) =>
        this._dispatchGqaCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, numKvHeads, headDim),
      gpu_gqa_cached_attention: (qId, kId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim) =>
        this._dispatchGqaCachedAttention(qId, kId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim),

      gpu_rms_norm: (inputId, weightId, outId, totalRows, dim, epsBits) =>
        this._dispatchRmsNorm(inputId, weightId, outId, totalRows, dim, epsBits),
      gpu_layer_norm: (inputId, gammaId, betaId, outId, totalRows, dim, epsBits) =>
        this._dispatchLayerNorm(inputId, gammaId, betaId, outId, totalRows, dim, epsBits),
    };
  }

  _dispatchMatmul(pipelineName, aId, bId, outId, m, n, k) {
    const aBuf = this.buffers.get(aId);
    const bBuf = this.buffers.get(bId);
    const outBuf = this.buffers.get(outId);
    if (!aBuf || !bBuf || !outBuf) return;

    const paramsData = new Uint32Array([m, n, k, 0]);
    const paramsBuffer = this.device.createBuffer({ size: 16, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
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
    pass.dispatchWorkgroups(Math.ceil(n / TILE), Math.ceil(m / TILE));
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchAttention(qId, kId, vId, maskId, outId, batch, seqLen, numHeads, headDim) {
    const qBuf = this.buffers.get(qId), kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId), maskBuf = this.buffers.get(maskId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !maskBuf || !outBuf) return;

    const ab = new ArrayBuffer(16);
    new Uint32Array(ab).set([seqLen, numHeads, headDim, 0]);
    new Float32Array(ab)[3] = 1.0 / Math.sqrt(headDim);
    const paramsBuffer = this.device.createBuffer({ size: 16, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    this.device.queue.writeBuffer(paramsBuffer, 0, ab);

    const bindGroup = this.device.createBindGroup({
      layout: this.attnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } }, { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } }, { binding: 3, resource: { buffer: maskBuf } },
        { binding: 4, resource: { buffer: outBuf } }, { binding: 5, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.attention);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(numHeads, batch, seqLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, headDim) {
    const qBuf = this.buffers.get(qId), kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId), outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !outBuf) return;

    const ab = new ArrayBuffer(16);
    new Uint32Array(ab).set([seqLen, numHeads, headDim, 0]);
    new Float32Array(ab)[3] = 1.0 / Math.sqrt(headDim);
    const paramsBuffer = this.device.createBuffer({ size: 16, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    this.device.queue.writeBuffer(paramsBuffer, 0, ab);

    const bindGroup = this.device.createBindGroup({
      layout: this.causalAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } }, { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } }, { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.causalAttention);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(numHeads, batch, seqLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchGqaCausalAttention(qId, kId, vId, outId, batch, seqLen, numHeads, numKvHeads, headDim) {
    const qBuf = this.buffers.get(qId), kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId), outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !outBuf) return;

    const ab = new ArrayBuffer(24);
    const u32 = new Uint32Array(ab);
    const f32 = new Float32Array(ab);
    u32[0] = seqLen; u32[1] = numHeads; u32[2] = numKvHeads; u32[3] = headDim;
    f32[4] = 1.0 / Math.sqrt(headDim); u32[5] = 0;
    const paramsBuffer = this.device.createBuffer({ size: 24, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    this.device.queue.writeBuffer(paramsBuffer, 0, ab);

    const bindGroup = this.device.createBindGroup({
      layout: this.causalAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } }, { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } }, { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.gqaCausalAttention);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(numHeads, batch, seqLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchGqaCachedAttention(qId, kId, vId, outId, batch, qLen, kvLen, numHeads, numKvHeads, headDim) {
    const qBuf = this.buffers.get(qId), kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId), outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !outBuf) return;

    const ab = new ArrayBuffer(24);
    const u32 = new Uint32Array(ab);
    const f32 = new Float32Array(ab);
    u32[0] = qLen; u32[1] = kvLen; u32[2] = numHeads; u32[3] = numKvHeads;
    u32[4] = headDim; f32[5] = 1.0 / Math.sqrt(headDim);
    const paramsBuffer = this.device.createBuffer({ size: 24, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    this.device.queue.writeBuffer(paramsBuffer, 0, ab);

    const bindGroup = this.device.createBindGroup({
      layout: this.causalAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } }, { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } }, { binding: 3, resource: { buffer: outBuf } },
        { binding: 4, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.gqaCachedAttention);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(numHeads, batch, qLen);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchCrossAttention(qId, kId, vId, maskId, outId, batch, decSeq, encSeq, numHeads, headDim) {
    const qBuf = this.buffers.get(qId), kBuf = this.buffers.get(kId);
    const vBuf = this.buffers.get(vId), maskBuf = this.buffers.get(maskId);
    const outBuf = this.buffers.get(outId);
    if (!qBuf || !kBuf || !vBuf || !maskBuf || !outBuf) return;

    const ab = new ArrayBuffer(24);
    const u32 = new Uint32Array(ab);
    const f32 = new Float32Array(ab);
    u32[0] = decSeq; u32[1] = encSeq; u32[2] = numHeads; u32[3] = headDim;
    f32[4] = 1.0 / Math.sqrt(headDim); u32[5] = 0;
    const paramsBuffer = this.device.createBuffer({ size: 24, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    this.device.queue.writeBuffer(paramsBuffer, 0, ab);

    const bindGroup = this.device.createBindGroup({
      layout: this.crossAttnBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: qBuf } }, { binding: 1, resource: { buffer: kBuf } },
        { binding: 2, resource: { buffer: vBuf } }, { binding: 3, resource: { buffer: maskBuf } },
        { binding: 4, resource: { buffer: outBuf } }, { binding: 5, resource: { buffer: paramsBuffer } },
      ],
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(this.pipelines.crossAttention);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(numHeads, batch, decSeq);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
    paramsBuffer.destroy();
  }

  _dispatchRmsNorm(inputId, weightId, outId, totalRows, dim, epsBits) {
    const inputBuf = this.buffers.get(inputId), weightBuf = this.buffers.get(weightId);
    const outBuf = this.buffers.get(outId);
    if (!inputBuf || !weightBuf || !outBuf) return;

    const paramsData = new Uint32Array([totalRows, dim, epsBits, 0]);
    const paramsBuffer = this.device.createBuffer({ size: 16, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.matmulBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } }, { binding: 1, resource: { buffer: weightBuf } },
        { binding: 2, resource: { buffer: outBuf } }, { binding: 3, resource: { buffer: paramsBuffer } },
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
    const inputBuf = this.buffers.get(inputId), gammaBuf = this.buffers.get(gammaId);
    const betaBuf = this.buffers.get(betaId), outBuf = this.buffers.get(outId);
    if (!inputBuf || !gammaBuf || !betaBuf || !outBuf) return;

    const paramsData = new Uint32Array([totalRows, dim, epsBits, 0]);
    const paramsBuffer = this.device.createBuffer({ size: 16, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    this.device.queue.writeBuffer(paramsBuffer, 0, paramsData);

    const bindGroup = this.device.createBindGroup({
      layout: this.normBindGroupLayout,
      entries: [
        { binding: 0, resource: { buffer: inputBuf } }, { binding: 1, resource: { buffer: gammaBuf } },
        { binding: 2, resource: { buffer: betaBuf } }, { binding: 3, resource: { buffer: outBuf } },
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
   * @returns {Promise<void>}
   */
  async flush() {
    await this.device.queue.onSubmittedWorkDone();

    if (this._pendingDownload) {
      const { staging, ptr, sizeBytes, memory } = this._pendingDownload;
      this._pendingDownload = null;

      await staging.mapAsync(GPUMapMode.READ);
      const mapped = new Uint8Array(staging.getMappedRange());
      new Uint8Array(memory.buffer, ptr, sizeBytes).set(mapped);
      staging.unmap();
      staging.destroy();
    }
  }

  destroy() {
    for (const buf of this.buffers.values()) buf.destroy();
    this.buffers.clear();
    this.device?.destroy();
    this.device = null;
  }
}
