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

// Termite: ML inference service for embeddings, chunking, and reranking.
// Zig implementation with ONNX Runtime, MLX, and native backends.

const build_options = @import("build_options");

pub const backends = @import("backends/backends.zig");
pub const sentencepiece = @import("inference_tokenizer").sentencepiece;
pub const hf_tokenizer = @import("inference_hf_tokenizer");
pub const tokenizer = @import("inference_tokenizer");
pub const audio = @import("inference_audio");
pub const chunker = @import("inference_chunker");
pub const pipelines = @import("pipelines/pipelines.zig");
pub const extractors = @import("extractors/mod.zig");
pub const server = if (build_options.skip_openapi) struct {} else @import("server/server.zig");
pub const cache = @import("cache/cache.zig");
pub const singleflight = @import("cache/singleflight.zig");
pub const registry = @import("registry/registry.zig");
pub const models = @import("models/models.zig");
pub const gguf = @import("gguf/root.zig");
pub const runtime = @import("runtime/root.zig");
pub const util = @import("util/util.zig");
pub const ops = @import("ops/ops.zig");
pub const io = @import("io/io.zig");
pub const codecs = @import("codecs/codecs.zig");
pub const compiled_artifact = @import("compiled_artifact.zig");
pub const graph = @import("graph/root.zig");
pub const architectures = struct {
    pub const deberta_graph = @import("architectures/deberta_graph.zig");
};
pub const finetune = @import("finetune/root.zig");
pub const finetune_cli = @import("finetune/cli/root.zig");
pub const run = @import("run/root.zig");
pub const quantize = @import("quantize/root.zig");
pub const client = if (build_options.skip_openapi) struct {} else @import("inference_client");
pub const linalg = @import("inference_linalg");
pub const native_generate = @import("native_generate.zig");
pub const native_compile = @import("native_compile.zig");
pub const native_export = @import("native_export.zig");
pub const native_quantize = @import("native_quantize.zig");
pub const native_export_gguf = @import("native_export_gguf.zig");
pub const native_export_safetensors = @import("native_export_safetensors.zig");
pub const native_run_artifact = @import("native_run_artifact.zig");
pub const native_embed = @import("native_embed.zig");
pub const native_classify = @import("native_classify.zig");
pub const native_transcribe = @import("native_transcribe.zig");
pub const native_read = @import("native_read.zig");
pub const scraping = @import("antfly_scraping");
pub const native_recognize = @import("native_recognize.zig");
pub const native_extract = @import("native_extract.zig");
pub const compare_generate = @import("cli/compare_generate.zig");
pub const native_smoke = @import("native_smoke.zig");
pub const cuda_info = @import("cuda_info.zig");
pub const cuda_microbench = @import("bench/cuda_microbench.zig");
pub const enable_mlx = build_options.enable_mlx;
pub const metal_runtime = @import("backends/metal_runtime.zig");
pub const native_compute = struct {
    pub const native = @import("ops/native_compute.zig");
    pub const gpu_hosted_store = @import("ops/gpu_hosted_store.zig");
    pub const metal = if (build_options.enable_metal) @import("ops/metal_compute.zig") else struct {};
    pub const mlx = if (build_options.enable_mlx) @import("ops/mlx_compute.zig") else struct {};
    pub const cuda = if (build_options.enable_cuda) @import("ops/cuda/cuda_compute.zig") else struct {};
    pub const wasm = if (build_options.enable_wasm) @import("ops/wasm_compute.zig") else struct {};
};

test {
    _ = backends;
    _ = sentencepiece;
    _ = hf_tokenizer;
    _ = tokenizer;
    _ = audio;
    _ = chunker;
    _ = pipelines;
    _ = extractors;
    _ = server;
    _ = cache;
    _ = singleflight;
    _ = registry;
    _ = models;
    _ = gguf;
    _ = runtime;
    _ = util;
    _ = ops;
    _ = io;
    _ = codecs;
    _ = compiled_artifact;
    _ = linalg;
    _ = graph;
    _ = architectures;
    _ = finetune;
    _ = finetune_cli;
    _ = run;
    _ = quantize;
    _ = client;
    _ = scraping;
    _ = native_generate;
    _ = native_compile;
    _ = native_export;
    _ = native_quantize;
    _ = native_export_gguf;
    _ = native_export_safetensors;
    _ = native_run_artifact;
    _ = native_embed;
    _ = native_classify;
    _ = native_transcribe;
    _ = native_read;
    _ = native_recognize;
    _ = native_extract;
    _ = compare_generate;
    _ = native_smoke;
    _ = cuda_info;
    _ = cuda_microbench;
    _ = native_compute;
    if (build_options.enable_cuda) {
        _ = native_compute.cuda;
        _ = @import("ops/cuda/kernels.zig");
    }
    _ = @import("ml");
}
