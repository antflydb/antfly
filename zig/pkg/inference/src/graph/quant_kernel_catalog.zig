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

//! Checked-in AOT specializations selected by operation semantics and runtime
//! shape. Model names and layer indices are deliberately absent from this file.

const std = @import("std");
const compiler = @import("quant_kernel_compiler.zig");
const cuda_renderer = @import("quant_kernel_cuda_renderer.zig");
const op = @import("quant_kernel_op.zig");

pub const cuda_sm89_target = compiler.cuda_sm89_promotion_target;

/// Sub-kernel inventory for the composite score-prework artifacts. These are
/// not standalone `CatalogEntry` operations: both consumers require the same
/// score producer and workspace. Keeping distinct candidate IDs here lets
/// release evidence and telemetry name the tiled topology without pretending
/// it is an independently dispatchable attention operation.
pub const AttentionConsumerCandidate = struct {
    candidate_id: []const u8,
    head_dim: u16,
    score_kernel_id: []const u8,
    consumer_kernel_id: []const u8,
    source_fingerprint: u64,
    threads_per_block: u16,
    output_cols_per_block: u16,
    max_kv_tokens: u16,
    static_shared_memory_bytes: u32,
    target: compiler.Target,
    production_enabled: bool,
};

pub const cuda_score_prework_tiled64_candidate_id = "cuda_gqa_score_prework_tiled64";

pub const score_prework_tiled64_candidates = [_]AttentionConsumerCandidate{
    .{
        .candidate_id = cuda_score_prework_tiled64_candidate_id ++ "/hd256/cap512/sm89",
        .head_dim = 256,
        .score_kernel_id = cuda_renderer.generated_attention_hd256_score_prework_kernel_id,
        .consumer_kernel_id = cuda_renderer.generated_attention_hd256_score_prework_tiled64_kernel_id,
        .source_fingerprint = compiler.first_decode_attention_1x_cuda_score_prework_hd256_source_fingerprint,
        .threads_per_block = cuda_renderer.generated_attention_score_prework_tiled64_tile_size,
        .output_cols_per_block = cuda_renderer.generated_attention_score_prework_tiled64_tile_size,
        .max_kv_tokens = cuda_renderer.generated_attention_score_prework_tiled64_hd256_max_kv_tokens,
        .static_shared_memory_bytes = cuda_renderer.generatedAttentionScorePreworkTiled64SharedBytes(256).?,
        .target = cuda_sm89_target,
        .production_enabled = false,
    },
    .{
        .candidate_id = cuda_score_prework_tiled64_candidate_id ++ "/hd512/cap4096/sm89",
        .head_dim = 512,
        .score_kernel_id = cuda_renderer.generated_attention_hd512_score_prework_kernel_id,
        .consumer_kernel_id = cuda_renderer.generated_attention_hd512_score_prework_tiled64_kernel_id,
        .source_fingerprint = compiler.first_decode_attention_1x_cuda_score_prework_hd512_source_fingerprint,
        .threads_per_block = cuda_renderer.generated_attention_score_prework_tiled64_tile_size,
        .output_cols_per_block = cuda_renderer.generated_attention_score_prework_tiled64_tile_size,
        .max_kv_tokens = cuda_renderer.generated_attention_score_prework_tiled64_hd512_max_kv_tokens,
        .static_shared_memory_bytes = cuda_renderer.generatedAttentionScorePreworkTiled64SharedBytes(512).?,
        .target = cuda_sm89_target,
        .production_enabled = false,
    },
};

const RegistryBinding = struct {
    artifact: compiler.GeneratedArtifact,
    source_fingerprint: u64,
    production_enabled: bool,
};

fn registryBinding(comptime kernel_id: []const u8) RegistryBinding {
    const artifact = compiler.generatedRegistryArtifactForKernel(.cuda, kernel_id) orelse
        @compileError("AOT catalog kernel is missing from first_generated_artifacts: " ++ kernel_id);
    const source_fingerprint = compiler.artifactSourceFingerprint(artifact);
    if (source_fingerprint == 0) {
        @compileError("AOT catalog kernel has no generated source: " ++ kernel_id);
    }
    return .{
        .artifact = artifact,
        .source_fingerprint = source_fingerprint,
        .production_enabled = artifact.production_enabled,
    };
}

fn q4MmvEntry(input_dim: u32, output_dim: u32) compiler.CatalogEntry {
    const binding = registryBinding(compiler.first_general_cuda_q4_0_mmv_kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .none,
        } },
        .runtime = .exactMatmul(1, input_dim, output_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q4_0_mmv_c4_w8",
            .threads_per_block = 256,
            .cols_per_block = 4,
            .resources = .{ .static_shared_memory_bytes = 128 },
        },
        .kernel_id = compiler.first_general_cuda_q4_0_mmv_kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn q4KMmvEntry(input_dim: u32, output_dim: u32) compiler.CatalogEntry {
    const binding = registryBinding(compiler.first_general_cuda_q4_k_mmv_kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_k,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .none,
        } },
        .runtime = .exactMatmul(1, input_dim, output_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q4_k_mmv_c1_w8",
            .threads_per_block = 256,
            .cols_per_block = 1,
            .resources = .{ .static_shared_memory_bytes = 1024 },
        },
        .kernel_id = compiler.first_general_cuda_q4_k_mmv_kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn q6KQ8ArgmaxEntry(
    input_dim: u32,
    output_dim: u32,
    threads: u16,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q6_k,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .argmax,
            .activation = .q8_1,
            .output = .i32,
        } },
        .runtime = .exactMatmul(1, input_dim, output_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q6_k_q8_1_argmax_tile8",
            .threads_per_block = threads,
            .cols_per_block = 8,
            .vector_width = 4,
            .resources = .{ .static_shared_memory_bytes = threads },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn q4Q8ArgmaxEntry(input_dim: u32, output_dim: u32) compiler.CatalogEntry {
    const binding = registryBinding(compiler.first_e2b_cuda_q4_0_q8_1_argmax_kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .argmax,
            .activation = .q8_1,
            .output = .i32,
        } },
        .runtime = .exactMatmul(1, input_dim, output_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q4_0_q8_1_argmax_tile8",
            .threads_per_block = 96,
            .cols_per_block = 8,
            .vector_width = 4,
            .resources = .{ .static_shared_memory_bytes = 96 },
        },
        .kernel_id = compiler.first_e2b_cuda_q4_0_q8_1_argmax_kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn pairQ8Entry(
    hidden_dim: u32,
    intermediate_dim: u32,
    threads: u16,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .pair_activation,
            .activation = .q8_1,
            .function = .gelu_new,
            .output = .q8_1,
        } },
        .runtime = .exactMatmul(1, hidden_dim, intermediate_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q4_0_pair_activation_q8_1",
            .threads_per_block = threads,
            .cols_per_block = 32,
            .vector_width = 4,
            .resources = .{ .static_shared_memory_bytes = @as(u32, threads / 128) * 128 + 128 },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn downQ8Entry(
    intermediate_dim: u32,
    hidden_dim: u32,
    threads: u16,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .gated_down,
            .activation = .q8_1,
        } },
        .runtime = .exactMatmul(1, intermediate_dim, hidden_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q4_0_down_q8_1_c4",
            .threads_per_block = threads,
            .cols_per_block = 4,
            .vector_width = 4,
            .resources = .{ .static_shared_memory_bytes = @as(u32, threads / 32) * 4 * @sizeOf(f32) },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn pairGgmlQ8_1Entry(
    hidden_dim: u32,
    intermediate_dim: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    var entry = pairQ8Entry(hidden_dim, intermediate_dim, 384, kernel_id);
    entry.schedule.family = "q4_0_pair_activation_ggml_q8_1_c32_sm89";
    return entry;
}

fn downGgmlQ8_1Entry(
    intermediate_dim: u32,
    hidden_dim: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    var entry = downQ8Entry(intermediate_dim, hidden_dim, 128, kernel_id);
    entry.schedule.family = "q4_0_down_ggml_q8_1_mmvq_c1_w4_sm89";
    entry.schedule.cols_per_block = 1;
    entry.schedule.resources.static_shared_memory_bytes = 4 * @sizeOf(f32);
    return entry;
}

/// The exact F32 candidates intentionally model the handwritten FFN ABI rather
/// than the Q8_1 intermediate route. They are cataloged for inventory and
/// validation only; both artifacts remain production-disabled.
fn pairF32ExactEntry(
    hidden_dim: u32,
    intermediate_dim: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .pair_activation,
            .activation = .f32,
            .function = .gelu_new,
            .output = .f32,
        } },
        .runtime = .exactMatmul(1, hidden_dim, intermediate_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q4_0_pair_activation_f32_exact_c4_w4",
            .threads_per_block = 128,
            .cols_per_block = 4,
            .vector_width = 4,
            .resources = .{ .static_shared_memory_bytes = 128 },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn downF32ExactEntry(
    intermediate_dim: u32,
    hidden_dim: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .gated_down,
            .activation = .f32,
            .output = .f32,
        } },
        .runtime = .exactMatmul(1, intermediate_dim, hidden_dim),
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "q4_0_down_f32_exact_c4_w8",
            .threads_per_block = 256,
            .cols_per_block = 4,
            .vector_width = 4,
            .resources = .{ .static_shared_memory_bytes = 128 },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn attentionEntry(
    head_dim: u32,
    query_heads: u32,
    kv_heads: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .attention = .{ .kind = .decode_1x } },
        .runtime = .{
            .rows = .exact(1),
            .head_dim = .exact(head_dim),
            .query_heads = .exact(query_heads),
            .kv_heads = .exact(kv_heads),
            .sequence_length = .{ .min = 1, .max = 262_144 },
        },
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "attention_query_head_split8",
            .threads_per_block = @intCast(head_dim),
            .cols_per_block = @intCast(head_dim),
            .split_count = 8,
            .resources = .{ .static_shared_memory_bytes = if (head_dim == 256) 384 else 640 },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn scorePreworkDecodeEntry(
    head_dim: u32,
    query_heads: u32,
    kv_heads: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .attention = .{
            .kind = .decode_1x,
            .query = .f32,
            .key = .paged_f16_or_polar4,
            .value = .paged_f16_or_f32,
            .output = .f32,
        } },
        .runtime = .{
            .rows = .exact(1),
            .head_dim = .exact(head_dim),
            .query_heads = .exact(query_heads),
            .kv_heads = .exact(kv_heads),
            // RuntimeShape carries total logical sequence length. The typed
            // runtime selector additionally owns the sliding-window/global
            // pairing, the F16 storage checks, and the SM89 512-token
            // automatic crossover; explicit mode may engage the route below
            // that crossover on other CUDA >= 7.5 targets.
            .sequence_length = .{
                .min = 1,
                .max = cuda_renderer.generated_attention_score_prework_max_kv_tokens,
            },
        },
        .target = cuda_sm89_target,
        .schedule = .{
            .family = "attention_score_prework_chunk128",
            .threads_per_block = @intCast(head_dim),
            .cols_per_block = 1,
            .split_count = cuda_renderer.generated_attention_score_prework_chunks,
            .resources = .{ .static_shared_memory_bytes = if (head_dim == 256) 32 else 64 },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn splitkOnlineDecodeEntry(
    head_dim: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    return .{
        .signature = .{ .attention = .{
            .kind = .decode_1x,
            .query = .f32,
            .key = .paged_f16,
            .value = .paged_f16,
            .output = .f32,
        } },
        .runtime = .{
            .rows = .exact(1),
            .head_dim = .exact(head_dim),
            .query_heads = .exact(8),
            .kv_heads = .exact(1),
            // RuntimeShape carries total logical sequence length. HD256 still
            // limits the visible interval to SWA512 in its typed lowering;
            // it must remain catalog-resolvable for the same 2k+ contexts as
            // the HD512 global layers.
            .sequence_length = .{
                .min = 1,
                .max = cuda_renderer.generated_splitk_online_decode_max_sequence_tokens,
            },
        },
        .target = cuda_sm89_target,
        .schedule = .{
            .family = if (head_dim == 256)
                "gqa_decode_splitk_online_sm89_hd256_swa512_f16"
            else
                "gqa_decode_splitk_online_sm89_hd512_global_f16",
            .threads_per_block = cuda_renderer.generated_splitk_online_decode_threads,
            .rows_per_block = 1,
            .cols_per_block = @intCast(head_dim),
            .split_count = cuda_renderer.generated_splitk_online_decode_splits,
            .resources = .{
                .static_shared_memory_bytes = cuda_renderer.generated_splitk_online_decode_static_shared_memory_bytes,
            },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

fn flashPrefillSequenceConstraint(query_len: u32) op.RuntimeDimensionConstraint {
    return switch (query_len) {
        // The qualified q512 kernel accepts prefixes 0/512/1024/1536. The
        // catalog carries total sequence length, so encode those four points
        // as the exact aligned set 512/1024/1536/2048.
        512 => .{ .min = 512, .max = 2048, .multiple_of = 512 },
        // The long-context fixture ends with a three-token tail at prefix 2048.
        3 => .exact(2051),
        else => unreachable,
    };
}

fn flashPrefillEntry(
    head_dim: u32,
    query_len: u32,
    comptime kernel_id: []const u8,
) compiler.CatalogEntry {
    const binding = registryBinding(kernel_id);
    const family = switch (head_dim) {
        256 => "gqa_flash_prefill_f16_sm89_hd256_swa512",
        512 => "gqa_flash_prefill_f16_sm89_hd512_global",
        else => unreachable,
    };
    return .{
        .signature = .{ .attention = .{
            .kind = .prefill_flash,
            .query = .f32,
            .key = .paged_f16,
            .value = .paged_f16,
            .output = .f32,
        } },
        .runtime = .{
            .rows = .exact(query_len),
            .head_dim = .exact(head_dim),
            .query_heads = .exact(8),
            .kv_heads = .exact(1),
            .sequence_length = flashPrefillSequenceConstraint(query_len),
        },
        .target = cuda_sm89_target,
        .schedule = .{
            .family = family,
            .threads_per_block = cuda_renderer.generated_flash_prefill_threads,
            .rows_per_block = cuda_renderer.generated_flash_prefill_query_tile,
            .cols_per_block = @intCast(head_dim),
            .resources = .{
                .dynamic_shared_memory_bytes = cuda_renderer.generatedFlashPrefillDynamicSharedBytes(@intCast(head_dim)).?,
            },
        },
        .kernel_id = kernel_id,
        .source_fingerprint = binding.source_fingerprint,
        .production_enabled = binding.production_enabled,
    };
}

pub const entries = [_]compiler.CatalogEntry{
    // Q4_0 projection signatures observed in the E2B QAT checkpoint.
    q4MmvEntry(256, 1536),
    q4MmvEntry(2048, 1536),
    q4MmvEntry(12_288, 1536),
    q4MmvEntry(1536, 8960),
    q4MmvEntry(1536, 256),
    q4MmvEntry(1536, 512),
    q4MmvEntry(1536, 2048),
    q4MmvEntry(1536, 4096),
    q4MmvEntry(1536, 6144),
    q4MmvEntry(1536, 12_288),
    q4MmvEntry(4096, 1536),
    q4MmvEntry(6144, 1536),

    // Q4_K decode shapes used by dense E4B and 12B checkpoints. One generated
    // body serves every entry; exact AOT rows keep selection deterministic.
    q4KMmvEntry(2560, 2048),
    q4KMmvEntry(2560, 4096),
    q4KMmvEntry(2560, 512),
    q4KMmvEntry(2560, 1024),
    q4KMmvEntry(2048, 2560),
    q4KMmvEntry(4096, 2560),
    q4KMmvEntry(2560, 10_240),
    q4KMmvEntry(10_240, 2560),
    q4KMmvEntry(3840, 4096),
    q4KMmvEntry(3840, 2048),
    q4KMmvEntry(3840, 512),
    q4KMmvEntry(4096, 3840),
    q4KMmvEntry(8192, 3840),
    q4KMmvEntry(3840, 15_360),
    q4KMmvEntry(15_360, 3840),
    q4KMmvEntry(2560, 256),
    q4KMmvEntry(256, 2560),
    q4KMmvEntry(3840, 8192),

    // Greedy LM-head stage 1. Semantic keys record Q8_1 input and argmax output
    // so neither candidate can resolve as a general logits projection.
    q4Q8ArgmaxEntry(1536, 262_144),
    q6KQ8ArgmaxEntry(2560, 262_144, 160, compiler.first_cuda_q6_k_q8_1_argmax_k2560_kernel_id),
    q6KQ8ArgmaxEntry(3840, 262_144, 256, compiler.first_cuda_q6_k_q8_1_argmax_k3840_kernel_id),

    // Fused FFN signatures. The 2560x10240 route is already production;
    // 1536-wide candidates remain disabled pending model-level evidence.
    pairQ8Entry(2560, 10_240, 640, compiler.first_general_cuda_q4_0_pair_q8_kernel_id),
    downQ8Entry(10_240, 2560, 256, compiler.first_general_cuda_q4_0_down_q8_kernel_id),
    pairQ8Entry(1536, 6144, 384, compiler.first_e2b_cuda_q4_0_pair_q8_6144_kernel_id),
    pairQ8Entry(1536, 12_288, 384, compiler.first_e2b_cuda_q4_0_pair_q8_12288_kernel_id),
    downQ8Entry(6144, 1536, 128, compiler.first_e2b_cuda_q4_0_down_q8_6144_kernel_id),
    downQ8Entry(12_288, 1536, 256, compiler.first_e2b_cuda_q4_0_down_q8_12288_kernel_id),

    // Exact F32 E2B FFN candidates retain the production activation boundary.
    // They are optional semantic entries; the runtime direct route remains
    // gated by ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT.
    pairF32ExactEntry(1536, 6144, compiler.first_e2b_cuda_q4_0_pair_f32_6144_exact_kernel_id),
    pairF32ExactEntry(1536, 12_288, compiler.first_e2b_cuda_q4_0_pair_f32_12288_exact_kernel_id),
    downF32ExactEntry(6144, 1536, compiler.first_e2b_cuda_q4_0_down_f32_6144_exact_kernel_id),
    downF32ExactEntry(12_288, 1536, compiler.first_e2b_cuda_q4_0_down_f32_12288_exact_kernel_id),

    // Gemma 4 E2B, E4B, and 12B sliding/global attention topologies.
    attentionEntry(256, 8, 1, compiler.first_decode_attention_1x_cuda_kernel_id),
    attentionEntry(512, 8, 1, compiler.first_decode_attention_1x_cuda_hd512_kernel_id),
    attentionEntry(256, 8, 2, compiler.first_decode_attention_1x_cuda_kernel_id),
    attentionEntry(512, 8, 2, compiler.first_decode_attention_1x_cuda_hd512_kernel_id),
    attentionEntry(256, 16, 8, compiler.first_decode_attention_1x_cuda_kernel_id),
    attentionEntry(512, 16, 1, compiler.first_decode_attention_1x_cuda_hd512_kernel_id),

    // Promoted paged exact score-prework decode composites for the qualified
    // SM89 Gemma 4 F16 geometry (8 query heads over 1 or 2 KV heads). The
    // production runtime engages them automatically at the 512-token KV
    // crossover; the entries carry the composite score/serial/tiled64
    // schedule and the paged K/V storage contract.
    scorePreworkDecodeEntry(256, 8, 1, compiler.first_decode_attention_1x_cuda_score_prework_hd256_kernel_id),
    scorePreworkDecodeEntry(256, 8, 2, compiler.first_decode_attention_1x_cuda_score_prework_hd256_kernel_id),
    scorePreworkDecodeEntry(512, 8, 1, compiler.first_decode_attention_1x_cuda_score_prework_hd512_kernel_id),
    scorePreworkDecodeEntry(512, 8, 2, compiler.first_decode_attention_1x_cuda_score_prework_hd512_kernel_id),

    // Qualified SM89 Gemma 4 q=1 split-K online-softmax candidates. They are
    // separate catalog identities from the portable generated decode family.
    splitkOnlineDecodeEntry(256, compiler.first_decode_splitk_online_cuda_hd256_kernel_id),
    splitkOnlineDecodeEntry(512, compiler.first_decode_splitk_online_cuda_hd512_kernel_id),

    // Promoted single-launch Flash-prefill topology for the qualified SM89
    // Gemma 4 F16 geometry. The production runtime attempts it automatically
    // whenever the full eligibility contract holds (rollback:
    // ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off). Runtime constraints
    // express query length and total-length/prefix buckets; the exact page-16,
    // sliding/global policy, format tags, and SM89 contract remain owned by the
    // typed renderer plan and the fail-closed runtime selector.
    flashPrefillEntry(256, 512, compiler.first_prefill_flash_cuda_hd256_kernel_id),
    flashPrefillEntry(256, 3, compiler.first_prefill_flash_cuda_hd256_kernel_id),
    flashPrefillEntry(512, 512, compiler.first_prefill_flash_cuda_hd512_kernel_id),
    flashPrefillEntry(512, 3, compiler.first_prefill_flash_cuda_hd512_kernel_id),
};

pub const catalog = compiler.Catalog{ .entries = &entries };

/// Layout-qualified candidate suite. The generic Q8_1 semantic ABI predates
/// explicit activation-layout typing, so these entries live in a dedicated
/// resolver rather than making the primary catalog ambiguous with the legacy
/// zero-sum routes at identical shapes.
pub const ggml_q8_1_entries = [_]compiler.CatalogEntry{
    pairGgmlQ8_1Entry(1536, 6144, compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_kernel_id),
    pairGgmlQ8_1Entry(1536, 12_288, compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_kernel_id),
    downGgmlQ8_1Entry(6144, 1536, compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_6144_kernel_id),
    downGgmlQ8_1Entry(12_288, 1536, compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_12288_kernel_id),
};

pub const ggml_q8_1_catalog = compiler.Catalog{ .entries = &ggml_q8_1_entries };

fn rendererDimensionMatches(constraint: anytype, value: u32) bool {
    if (value == 0) return false;
    if (constraint.fixed != 0 and value != constraint.fixed) return false;
    if (value < constraint.min) return false;
    if (constraint.max != 0 and value > constraint.max) return false;
    return value % constraint.multiple_of == 0;
}

fn expectedCudaVectorWidth(plan: anytype) u8 {
    return switch (plan.lowering) {
        .q4_k_small_batch, .q4_k_projection, .q4_0_projection => 1,
        .q4_0_pair_activation_q8_1,
        .q4_0_down_q8_1,
        .q4_0_pair_activation_f32_exact,
        .q4_0_down_f32_exact,
        .q4_0_q8_1_argmax,
        .q6_k_q8_1_argmax,
        => 4,
    };
}

fn validateCudaLaunchSchedule(entry: compiler.CatalogEntry, plan: anytype) !void {
    const rows = entry.runtime.rows.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const input_dim = entry.runtime.input_dim.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const output_dim = entry.runtime.output_dim.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    if (!rendererDimensionMatches(plan.launch.rows, rows) or
        !rendererDimensionMatches(plan.launch.input_dim, input_dim) or
        !rendererDimensionMatches(plan.launch.output_dim, output_dim))
    {
        return error.CatalogRuntimeShapeMismatch;
    }
    if (entry.schedule.threads_per_block != plan.launch.threads_per_block or
        entry.schedule.rows_per_block != plan.launch.output_rows_per_block or
        entry.schedule.cols_per_block != plan.launch.output_cols_per_block or
        entry.schedule.vector_width != expectedCudaVectorWidth(plan) or
        entry.schedule.split_count != 1 or
        entry.schedule.resources.static_shared_memory_bytes != plan.launch.static_shared_memory_bytes or
        entry.schedule.resources.dynamic_shared_memory_bytes != plan.launch.dynamic_shared_memory_bytes)
    {
        return error.CatalogCudaLaunchScheduleMismatch;
    }
}

fn validateMatmulSemanticAbi(
    signature: op.MatmulSpecialization,
    artifact_op: compiler.MatmulArtifactOp,
) !void {
    if (signature.format != artifact_op.format or
        signature.row_bucket != artifact_op.row_bucket or
        signature.epilogue != artifact_op.epilogue or
        signature.activation != artifact_op.activation or
        signature.output != artifact_op.output)
    {
        return error.CatalogArtifactSemanticAbiMismatch;
    }
    if (artifact_op.function) |function| {
        if (signature.function != function) return error.CatalogArtifactSemanticAbiMismatch;
    } else if (signature.epilogue != .pair_activation and signature.function != .none) {
        return error.CatalogArtifactSemanticAbiMismatch;
    }
}

fn activationEncodingForAttentionStorage(storage: anytype) op.ActivationEncoding {
    return switch (storage) {
        .f32 => .f32,
        .f16 => .f16,
        .bf16 => .bf16,
        .paged_f16 => .paged_f16,
        .paged_f16_or_polar4 => .paged_f16_or_polar4,
        .paged_f16_or_f32 => .paged_f16_or_f32,
    };
}

fn outputEncodingForAttentionStorage(storage: anytype) op.OutputEncoding {
    return switch (storage) {
        .f32 => .f32,
        .f16 => .f16,
        .bf16 => .bf16,
        .paged_f16, .paged_f16_or_polar4, .paged_f16_or_f32 => unreachable,
    };
}

fn validateFlashPrefillSemanticAndSchedule(
    entry: compiler.CatalogEntry,
    signature: op.AttentionSpecialization,
    artifact: compiler.GeneratedArtifact,
) !void {
    const artifact_op = artifact.attentionOp() orelse return error.CatalogArtifactOperationKindMismatch;
    const plan = compiler.cudaFlashPrefillRenderPlanForArtifact(artifact) orelse
        return error.CatalogArtifactCudaRenderPlanMissing;
    if (signature.kind != artifact_op.kind or
        signature.query != activationEncodingForAttentionStorage(plan.lowering.query_storage) or
        signature.key != activationEncodingForAttentionStorage(plan.lowering.key_storage) or
        signature.value != activationEncodingForAttentionStorage(plan.lowering.value_storage) or
        signature.output != outputEncodingForAttentionStorage(plan.lowering.output_storage))
    {
        return error.CatalogArtifactSemanticAbiMismatch;
    }

    const query_len = entry.runtime.rows.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const head_dim = entry.runtime.head_dim.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const query_heads = entry.runtime.query_heads.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const kv_heads = entry.runtime.kv_heads.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const policy_matches = switch (query_len) {
        512 => blk: {
            for ([_]usize{ 0, 512, 1024, 1536 }) |prefix| {
                if (!plan.lowering.query_length_policy.accepts(512, prefix)) break :blk false;
            }
            break :blk !plan.lowering.query_length_policy.accepts(512, 2048);
        },
        3 => plan.lowering.query_length_policy.accepts(3, 2048) and
            !plan.lowering.query_length_policy.accepts(3, 1536),
        else => false,
    };
    if ((query_len != 512 and query_len != 3) or
        !std.meta.eql(entry.runtime.sequence_length, flashPrefillSequenceConstraint(query_len)) or
        !policy_matches or
        !rendererDimensionMatches(plan.launch.rows, query_len) or
        !rendererDimensionMatches(plan.launch.input_dim, head_dim) or
        head_dim != plan.lowering.head_dim or
        query_heads != plan.lowering.query_heads or
        kv_heads != plan.lowering.kv_heads or
        plan.lowering.page_size_tokens != cuda_renderer.generated_flash_prefill_page_size_tokens or
        plan.lowering.sliding_window != @as(u16, if (head_dim == 256) 512 else 0) or
        plan.lowering.query_length_policy != .gemma4_q512_or_q3_v1 or
        plan.lowering.format != cuda_renderer.generated_flash_prefill_format_f16 or
        plan.lowering.value_format != cuda_renderer.generated_flash_prefill_format_f16 or
        plan.lowering.required_compute_major != 8 or
        plan.lowering.required_compute_minor != 9 or
        compiler.targetFingerprint(entry.target) != compiler.targetFingerprint(cuda_sm89_target))
    {
        return error.CatalogRuntimeShapeMismatch;
    }

    const expected_family: []const u8 = switch (head_dim) {
        256 => "gqa_flash_prefill_f16_sm89_hd256_swa512",
        512 => "gqa_flash_prefill_f16_sm89_hd512_global",
        else => return error.CatalogRuntimeShapeMismatch,
    };
    if (!std.mem.eql(u8, entry.schedule.family, expected_family) or
        entry.schedule.threads_per_block != plan.launch.threads_per_block or
        entry.schedule.rows_per_block != plan.launch.output_rows_per_block or
        entry.schedule.cols_per_block != plan.launch.output_cols_per_block or
        entry.schedule.vector_width != 1 or
        entry.schedule.split_count != 1 or
        entry.schedule.resources.static_shared_memory_bytes != plan.launch.static_shared_memory_bytes or
        entry.schedule.resources.dynamic_shared_memory_bytes != plan.launch.dynamic_shared_memory_bytes)
    {
        return error.CatalogCudaLaunchScheduleMismatch;
    }
}

fn validateSplitkOnlineDecodeSemanticAndSchedule(
    entry: compiler.CatalogEntry,
    signature: op.AttentionSpecialization,
    artifact: compiler.GeneratedArtifact,
) !void {
    const artifact_op = artifact.attentionOp() orelse return error.CatalogArtifactOperationKindMismatch;
    const plan = compiler.cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact) orelse
        return error.CatalogArtifactCudaRenderPlanMissing;
    if (signature.kind != artifact_op.kind or
        signature.query != activationEncodingForAttentionStorage(plan.lowering.query_storage) or
        signature.key != activationEncodingForAttentionStorage(plan.lowering.key_storage) or
        signature.value != activationEncodingForAttentionStorage(plan.lowering.value_storage) or
        signature.output != outputEncodingForAttentionStorage(plan.lowering.output_storage))
    {
        return error.CatalogArtifactSemanticAbiMismatch;
    }

    const rows = entry.runtime.rows.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const head_dim = entry.runtime.head_dim.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const query_heads = entry.runtime.query_heads.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const kv_heads = entry.runtime.kv_heads.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const expected_sequence = op.RuntimeDimensionConstraint{
        .min = 1,
        .max = cuda_renderer.generated_splitk_online_decode_max_sequence_tokens,
    };
    if (rows != 1 or
        !rendererDimensionMatches(plan.launch.rows, rows) or
        !rendererDimensionMatches(plan.launch.input_dim, head_dim) or
        head_dim != plan.lowering.head_dim or
        query_heads != plan.lowering.query_heads or
        kv_heads != plan.lowering.kv_heads or
        !std.meta.eql(entry.runtime.sequence_length, expected_sequence) or
        plan.lowering.kv_splits != cuda_renderer.generated_splitk_online_decode_splits or
        plan.lowering.page_size_tokens != cuda_renderer.generated_splitk_online_decode_page_size_tokens or
        plan.lowering.sliding_window != @as(u16, if (head_dim == 256) 512 else 0) or
        plan.lowering.format != cuda_renderer.generated_splitk_online_decode_format_f16 or
        plan.lowering.value_format != cuda_renderer.generated_splitk_online_decode_format_f16 or
        plan.lowering.required_compute_major != 8 or
        plan.lowering.required_compute_minor != 9 or
        compiler.targetFingerprint(entry.target) != compiler.targetFingerprint(cuda_sm89_target))
    {
        return error.CatalogRuntimeShapeMismatch;
    }

    const expected_family: []const u8 = if (head_dim == 256)
        "gqa_decode_splitk_online_sm89_hd256_swa512_f16"
    else
        "gqa_decode_splitk_online_sm89_hd512_global_f16";
    if (!std.mem.eql(u8, entry.schedule.family, expected_family) or
        entry.schedule.threads_per_block != plan.launch.threads_per_block or
        entry.schedule.rows_per_block != plan.launch.output_rows_per_block or
        entry.schedule.cols_per_block != plan.launch.output_cols_per_block or
        entry.schedule.vector_width != 1 or
        entry.schedule.split_count != plan.lowering.kv_splits or
        entry.schedule.resources.static_shared_memory_bytes != plan.launch.static_shared_memory_bytes or
        entry.schedule.resources.dynamic_shared_memory_bytes != plan.launch.dynamic_shared_memory_bytes)
    {
        return error.CatalogCudaLaunchScheduleMismatch;
    }
}

fn validateAttentionSemanticAndSchedule(
    entry: compiler.CatalogEntry,
    signature: op.AttentionSpecialization,
    artifact: compiler.GeneratedArtifact,
) !void {
    if (artifact.cuda_splitk_online_decode_kernel != null) {
        return validateSplitkOnlineDecodeSemanticAndSchedule(entry, signature, artifact);
    }
    if (signature.kind == .prefill_flash) {
        return validateFlashPrefillSemanticAndSchedule(entry, signature, artifact);
    }
    const artifact_op = artifact.attentionOp() orelse return error.CatalogArtifactOperationKindMismatch;
    const plan = compiler.cudaAttentionRenderPlanForArtifact(artifact) orelse
        return error.CatalogArtifactCudaRenderPlanMissing;
    if (signature.kind != artifact_op.kind or
        signature.query != activationEncodingForAttentionStorage(plan.lowering.query_storage) or
        signature.key != activationEncodingForAttentionStorage(plan.lowering.key_storage) or
        signature.value != activationEncodingForAttentionStorage(plan.lowering.value_storage) or
        signature.output != outputEncodingForAttentionStorage(plan.lowering.output_storage))
    {
        return error.CatalogArtifactSemanticAbiMismatch;
    }

    const rows = entry.runtime.rows.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const head_dim = entry.runtime.head_dim.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const query_heads = entry.runtime.query_heads.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    const kv_heads = entry.runtime.kv_heads.exactValue() orelse return error.CatalogRuntimeShapeMismatch;
    if (!rendererDimensionMatches(plan.launch.rows, rows) or
        !rendererDimensionMatches(plan.launch.input_dim, head_dim) or
        head_dim != plan.lowering.head_dim or
        query_heads == 0 or kv_heads == 0 or query_heads % kv_heads != 0 or
        query_heads > cuda_renderer.generated_attention_workspace_max_query_heads or
        query_heads / kv_heads > plan.lowering.query_heads_per_kv_head)
    {
        return error.CatalogRuntimeShapeMismatch;
    }
    if (entry.schedule.threads_per_block != plan.launch.threads_per_block or
        entry.schedule.rows_per_block != plan.launch.output_rows_per_block or
        entry.schedule.cols_per_block != plan.launch.output_cols_per_block or
        entry.schedule.vector_width != 1 or
        entry.schedule.split_count != plan.lowering.kv_splits or
        entry.schedule.resources.static_shared_memory_bytes != plan.launch.static_shared_memory_bytes or
        entry.schedule.resources.dynamic_shared_memory_bytes != plan.launch.dynamic_shared_memory_bytes)
    {
        return error.CatalogCudaLaunchScheduleMismatch;
    }
}

/// A promoted decode-attention composite must expose the complete contract the
/// host relies on: the score producer plus serial and tiled64 consumer entry
/// points, the typed score-capacity/threshold rules and paged K/V storage in
/// its checked-in schedule, runtime linkage, and a tiled64 consumer inventory
/// record bound to the same generated source and target. Only the exact
/// score-prework composites satisfy this; dense split candidates keep their
/// host-owned rules outside the catalog and stay unpromotable.
fn decodeCompositeContractComplete(
    entry: compiler.CatalogEntry,
    artifact: compiler.GeneratedArtifact,
) bool {
    if (!compiler.cudaAttentionArtifactRuntimeWired(artifact)) return false;
    const attention = artifact.attentionOp() orelse return false;
    const plan = compiler.cudaAttentionRenderPlanForArtifact(artifact) orelse return false;
    if (plan.lowering.split_variant != .score_prework) return false;
    const schedule = attention.schedule;
    if (schedule.attention_serial_threads_per_threadgroup == 0 or
        schedule.attention_stage2_threads_per_threadgroup == 0 or
        schedule.attention_tiled64_threads_per_threadgroup == 0 or
        schedule.attention_max_kv_tokens == 0 or
        schedule.attention_tiled64_max_kv_tokens == 0 or
        schedule.attention_key_storage != .paged_f16_or_polar4 or
        schedule.attention_value_storage != .paged_f16_or_f32)
    {
        return false;
    }
    const source_fingerprint = compiler.artifactSourceFingerprint(artifact);
    for (score_prework_tiled64_candidates) |candidate| {
        if (candidate.head_dim == attention.head_dim and
            candidate.source_fingerprint == source_fingerprint and
            candidate.target.backend == entry.target.backend and
            std.mem.eql(u8, candidate.target.architecture, entry.target.architecture))
        {
            return true;
        }
    }
    return false;
}

fn validateEntryAgainstArtifact(
    entry: compiler.CatalogEntry,
    artifact: compiler.GeneratedArtifact,
) !void {
    try entry.validate();
    if (entry.target.backend != artifact.backend) return error.CatalogArtifactBackendMismatch;
    if (!std.mem.eql(u8, entry.kernel_id, artifact.kernel_id)) return error.CatalogArtifactKernelIdMismatch;

    // Decode attention contains three entry points and host-owned
    // workspace/threshold rules. A catalog record may promote that composite
    // route only when those contracts are represented: the typed score-prework
    // plan carries the paged K/V storage, score capacity, and consumer
    // thresholds in its checked-in schedule, and the tiled64 consumer is
    // inventoried against the same generated source. Stage-1 dense split
    // candidates carry none of that and remain unpromotable. Flash prefill is
    // a dedicated one-launch plan and may be promoted only after its
    // explicit-profile runtime wiring is present.
    if (entry.production_enabled and artifact.opKind() == .attention) {
        const attention = artifact.attentionOp() orelse return error.CatalogArtifactOperationKindMismatch;
        switch (attention.kind) {
            .decode_1x => if (!decodeCompositeContractComplete(entry, artifact)) {
                return error.ProductionAttentionCompositeContractIncomplete;
            },
            .prefill_flash => if (!compiler.cudaFlashPrefillArtifactRuntimeWired(artifact)) {
                return error.ProductionCatalogArtifactNotRuntimeWired;
            },
        }
    }
    if (entry.production_enabled) switch (entry.signature) {
        .small_batch_matmul => |signature| if (signature.epilogue == .argmax) {
            return error.ProductionArgmaxCompositeContractIncomplete;
        },
        else => {},
    };
    if (entry.production_enabled != artifact.production_enabled) {
        return error.CatalogArtifactProductionStateMismatch;
    }
    if (entry.production_enabled and artifact.matmulOp() != null and
        !compiler.artifactRuntimeWired(compiler.matmulArtifactView(artifact)))
    {
        return error.ProductionCatalogArtifactNotRuntimeWired;
    }
    const source_fingerprint = compiler.artifactSourceFingerprint(artifact);
    if (source_fingerprint == 0 or entry.source_fingerprint != source_fingerprint) {
        return error.CatalogArtifactSourceFingerprintMismatch;
    }

    switch (entry.signature) {
        .small_batch_matmul => |signature| {
            const artifact_op = artifact.matmulOp() orelse return error.CatalogArtifactOperationKindMismatch;
            try validateMatmulSemanticAbi(signature, artifact_op);
            const plan = compiler.cudaRenderPlanForArtifact(artifact) orelse
                return error.CatalogArtifactCudaRenderPlanMissing;
            try validateCudaLaunchSchedule(entry, plan);
        },
        .attention => |signature| try validateAttentionSemanticAndSchedule(entry, signature, artifact),
        .microkernel => return error.CatalogArtifactOperationKindMismatch,
    }

    if (!entry.production_enabled) return;
    if (artifact.runtime_evidence_command.len == 0) return error.ProductionCatalogArtifactMissingRuntimeEvidence;
    if (artifact.promotion_evidence_command.len == 0) return error.ProductionCatalogArtifactMissingPromotionEvidence;
    const maybe_evidence_target = if (artifact.matmulOp() != null) target: {
        const matmul = compiler.matmulArtifactView(artifact);
        if (!compiler.artifactHasPromotionEvidence(matmul)) return error.ProductionCatalogArtifactPromotionEvidenceRejected;
        break :target compiler.artifactPromotionTargetFingerprint(matmul);
    } else compiler.attentionArtifactPromotionTargetFingerprint(artifact);
    const evidence_target = maybe_evidence_target orelse
        return error.ProductionCatalogArtifactMissingTargetEvidence;
    if (evidence_target != compiler.targetFingerprint(entry.target)) {
        return error.ProductionCatalogArtifactTargetEvidenceMismatch;
    }
}

pub fn validateEntryAgainstRegistry(entry: compiler.CatalogEntry) !void {
    const artifact = compiler.generatedRegistryArtifactForKernel(entry.target.backend, entry.kernel_id) orelse
        return error.CatalogArtifactMissing;
    try validateEntryAgainstArtifact(entry, artifact);
}

pub fn validateCheckedInCatalog() !void {
    try catalog.validate();
    for (entries) |entry| try validateEntryAgainstRegistry(entry);
    try ggml_q8_1_catalog.validate();
    for (ggml_q8_1_entries) |entry| try validateEntryAgainstRegistry(entry);
}

comptime {
    @setEvalBranchQuota(200_000_000);
    validateCheckedInCatalog() catch |err| @compileError(@errorName(err));
}

pub fn targetForComputeCapability(major: i32, minor: i32) ?compiler.Target {
    if (major == 8 and minor == 9) return cuda_sm89_target;
    return null;
}

pub fn resolve(
    signature: op.SpecializationSignature,
    shape: op.RuntimeShape,
    major: i32,
    minor: i32,
) !?compiler.CatalogEntry {
    const target = targetForComputeCapability(major, minor) orelse return null;
    return catalog.resolve(.{ .signature = signature, .runtime = shape, .target = target });
}

pub fn resolveGgmlQ8_1(
    signature: op.SpecializationSignature,
    shape: op.RuntimeShape,
    major: i32,
    minor: i32,
) !?compiler.CatalogEntry {
    const target = targetForComputeCapability(major, minor) orelse return null;
    return ggml_q8_1_catalog.resolve(.{ .signature = signature, .runtime = shape, .target = target });
}

test "checked-in CUDA AOT catalog is unambiguous" {
    try validateCheckedInCatalog();
    for (entries) |entry| {
        const id = try entry.id(std.testing.allocator);
        defer std.testing.allocator.free(id);
        try std.testing.expect(std.mem.startsWith(u8, id, "antfly.kernel.aot.v1/cuda/sm_89/"));
        try std.testing.expect(entry.source_fingerprint != 0);
        const artifact = compiler.generatedRegistryArtifactForKernel(entry.target.backend, entry.kernel_id) orelse
            return error.CatalogArtifactMissing;
        try std.testing.expectEqual(artifact.production_enabled, entry.production_enabled);
        try std.testing.expectEqual(compiler.artifactSourceFingerprint(artifact), entry.source_fingerprint);
    }
}

test "catalog rejects registry provenance promotion ABI and schedule drift" {
    const production_entry = entries[0];
    const production_artifact = compiler.generatedRegistryArtifactForKernel(
        production_entry.target.backend,
        production_entry.kernel_id,
    ) orelse return error.CatalogArtifactMissing;
    try std.testing.expect(production_entry.production_enabled);

    var bad_source = production_entry;
    bad_source.source_fingerprint ^= 1;
    try std.testing.expectError(
        error.CatalogArtifactSourceFingerprintMismatch,
        validateEntryAgainstArtifact(bad_source, production_artifact),
    );

    var bad_promotion = production_entry;
    bad_promotion.production_enabled = false;
    try std.testing.expectError(
        error.CatalogArtifactProductionStateMismatch,
        validateEntryAgainstArtifact(bad_promotion, production_artifact),
    );

    var bad_schedule = production_entry;
    bad_schedule.schedule.threads_per_block /= 2;
    try std.testing.expectError(
        error.CatalogCudaLaunchScheduleMismatch,
        validateEntryAgainstArtifact(bad_schedule, production_artifact),
    );

    var bad_abi = production_entry;
    bad_abi.signature.small_batch_matmul.output = .q8_1;
    try std.testing.expectError(
        error.CatalogArtifactSemanticAbiMismatch,
        validateEntryAgainstArtifact(bad_abi, production_artifact),
    );

    var wrong_backend = production_entry;
    wrong_backend.target.backend = .metal;
    try std.testing.expectError(
        error.CatalogArtifactBackendMismatch,
        validateEntryAgainstArtifact(wrong_backend, production_artifact),
    );

    var wrong_target = production_entry;
    wrong_target.target.architecture = "sm_80";
    try std.testing.expectError(
        error.ProductionCatalogArtifactTargetEvidenceMismatch,
        validateEntryAgainstArtifact(wrong_target, production_artifact),
    );

    var wrong_kernel = production_entry;
    wrong_kernel.kernel_id = compiler.first_general_cuda_q4_k_mmv_kernel_id;
    try std.testing.expectError(
        error.CatalogArtifactKernelIdMismatch,
        validateEntryAgainstArtifact(wrong_kernel, production_artifact),
    );

    var missing = production_entry;
    missing.kernel_id = "antfly_missing_catalog_kernel_v1";
    try std.testing.expectError(error.CatalogArtifactMissing, validateEntryAgainstRegistry(missing));
}

test "catalog enforces production evidence and runtime wiring policy" {
    const entry = entries[0];
    const artifact = compiler.generatedRegistryArtifactForKernel(entry.target.backend, entry.kernel_id) orelse
        return error.CatalogArtifactMissing;

    var missing_runtime_evidence = artifact;
    missing_runtime_evidence.runtime_evidence_command = "";
    try std.testing.expectError(
        error.ProductionCatalogArtifactMissingRuntimeEvidence,
        validateEntryAgainstArtifact(entry, missing_runtime_evidence),
    );

    var missing_promotion_evidence = artifact;
    missing_promotion_evidence.promotion_evidence_command = "";
    try std.testing.expectError(
        error.ProductionCatalogArtifactMissingPromotionEvidence,
        validateEntryAgainstArtifact(entry, missing_promotion_evidence),
    );

    var rejected_evidence = artifact;
    rejected_evidence.source_path = "src/ops/cuda/generated/not_the_promoted_source.cu";
    try std.testing.expectError(
        error.ProductionCatalogArtifactPromotionEvidenceRejected,
        validateEntryAgainstArtifact(entry, rejected_evidence),
    );

    var q4_k_entry_value: ?compiler.CatalogEntry = null;
    for (entries) |candidate| {
        if (std.mem.eql(u8, candidate.kernel_id, compiler.first_general_cuda_q4_k_mmv_kernel_id)) {
            q4_k_entry_value = candidate;
            break;
        }
    }
    const q4_k_entry = q4_k_entry_value orelse return error.MissingCatalogEntry;
    var q4_k_artifact = compiler.generatedRegistryArtifactForKernel(
        q4_k_entry.target.backend,
        q4_k_entry.kernel_id,
    ) orelse return error.CatalogArtifactMissing;
    try std.testing.expect(!q4_k_entry.production_enabled);
    var unwired_production = q4_k_entry;
    unwired_production.production_enabled = true;
    q4_k_artifact.production_enabled = true;
    try std.testing.expectError(
        error.ProductionCatalogArtifactNotRuntimeWired,
        validateEntryAgainstArtifact(unwired_production, q4_k_artifact),
    );

    var attention_entry_value: ?compiler.CatalogEntry = null;
    for (entries) |candidate| switch (candidate.signature) {
        .attention => {
            attention_entry_value = candidate;
            break;
        },
        else => {},
    };
    var attention_entry = attention_entry_value orelse return error.MissingCatalogEntry;
    var attention_artifact = compiler.generatedRegistryArtifactForKernel(
        attention_entry.target.backend,
        attention_entry.kernel_id,
    ) orelse return error.CatalogArtifactMissing;
    try std.testing.expectEqual(compiler.OpKind.attention, attention_artifact.opKind());
    attention_entry.production_enabled = true;
    attention_artifact.production_enabled = true;
    try std.testing.expectError(
        error.ProductionAttentionCompositeContractIncomplete,
        validateEntryAgainstArtifact(attention_entry, attention_artifact),
    );

    var argmax_entry_value: ?compiler.CatalogEntry = null;
    for (entries) |candidate| switch (candidate.signature) {
        .small_batch_matmul => |signature| if (signature.epilogue == .argmax) {
            argmax_entry_value = candidate;
            break;
        },
        else => {},
    };
    var argmax_entry = argmax_entry_value orelse return error.MissingCatalogEntry;
    var argmax_artifact = compiler.generatedRegistryArtifactForKernel(
        argmax_entry.target.backend,
        argmax_entry.kernel_id,
    ) orelse return error.CatalogArtifactMissing;
    argmax_entry.production_enabled = true;
    argmax_artifact.production_enabled = true;
    try std.testing.expectError(
        error.ProductionArgmaxCompositeContractIncomplete,
        validateEntryAgainstArtifact(argmax_entry, argmax_artifact),
    );
}

test "catalog validates Q8_1 activation and output ABI from typed artifacts" {
    var pair_entry: ?compiler.CatalogEntry = null;
    var argmax_entry: ?compiler.CatalogEntry = null;
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.kernel_id, compiler.first_general_cuda_q4_0_pair_q8_kernel_id)) pair_entry = entry;
        if (std.mem.eql(u8, entry.kernel_id, compiler.first_cuda_q6_k_q8_1_argmax_k2560_kernel_id)) argmax_entry = entry;
    }

    const pair = pair_entry orelse return error.MissingCatalogEntry;
    const pair_artifact = compiler.generatedRegistryArtifactForKernel(pair.target.backend, pair.kernel_id) orelse
        return error.CatalogArtifactMissing;
    try validateEntryAgainstArtifact(pair, pair_artifact);
    var bad_pair = pair;
    bad_pair.signature.small_batch_matmul.activation = .f32;
    try std.testing.expectError(
        error.CatalogArtifactSemanticAbiMismatch,
        validateEntryAgainstArtifact(bad_pair, pair_artifact),
    );

    const argmax = argmax_entry orelse return error.MissingCatalogEntry;
    const argmax_artifact = compiler.generatedRegistryArtifactForKernel(argmax.target.backend, argmax.kernel_id) orelse
        return error.CatalogArtifactMissing;
    try validateEntryAgainstArtifact(argmax, argmax_artifact);
    var bad_argmax = argmax;
    bad_argmax.signature.small_batch_matmul.output = .f32;
    try std.testing.expectError(
        error.CatalogArtifactSemanticAbiMismatch,
        validateEntryAgainstArtifact(bad_argmax, argmax_artifact),
    );
}

test "catalog resolves exact F32 E2B FFN candidates without promotion" {
    const pair_signature = op.SpecializationSignature{ .small_batch_matmul = .{
        .format = .q4_0,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .pair_activation,
        .activation = .f32,
        .function = .gelu_new,
        .output = .f32,
    } };
    const down_signature = op.SpecializationSignature{ .small_batch_matmul = .{
        .format = .q4_0,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .gated_down,
        .activation = .f32,
        .output = .f32,
    } };
    const expected = [_]struct {
        signature: op.SpecializationSignature,
        input_dim: u32,
        output_dim: u32,
        kernel_id: []const u8,
        threads: u16,
        family: []const u8,
    }{
        .{
            .signature = pair_signature,
            .input_dim = 1536,
            .output_dim = 6144,
            .kernel_id = compiler.first_e2b_cuda_q4_0_pair_f32_6144_exact_kernel_id,
            .threads = 128,
            .family = "q4_0_pair_activation_f32_exact_c4_w4",
        },
        .{
            .signature = pair_signature,
            .input_dim = 1536,
            .output_dim = 12_288,
            .kernel_id = compiler.first_e2b_cuda_q4_0_pair_f32_12288_exact_kernel_id,
            .threads = 128,
            .family = "q4_0_pair_activation_f32_exact_c4_w4",
        },
        .{
            .signature = down_signature,
            .input_dim = 6144,
            .output_dim = 1536,
            .kernel_id = compiler.first_e2b_cuda_q4_0_down_f32_6144_exact_kernel_id,
            .threads = 256,
            .family = "q4_0_down_f32_exact_c4_w8",
        },
        .{
            .signature = down_signature,
            .input_dim = 12_288,
            .output_dim = 1536,
            .kernel_id = compiler.first_e2b_cuda_q4_0_down_f32_12288_exact_kernel_id,
            .threads = 256,
            .family = "q4_0_down_f32_exact_c4_w8",
        },
    };
    for (expected) |item| {
        const resolved = (try resolve(item.signature, .{
            .rows = 1,
            .input_dim = item.input_dim,
            .output_dim = item.output_dim,
        }, 8, 9)) orelse return error.MissingCatalogEntry;
        try std.testing.expectEqualStrings(item.kernel_id, resolved.kernel_id);
        try std.testing.expect(!resolved.production_enabled);
        try std.testing.expectEqual(item.threads, resolved.schedule.threads_per_block);
        try std.testing.expectEqual(@as(u8, 4), resolved.schedule.cols_per_block);
        try std.testing.expectEqual(@as(u8, 4), resolved.schedule.vector_width);
        try std.testing.expectEqual(@as(u32, 128), resolved.schedule.resources.static_shared_memory_bytes);
        try std.testing.expectEqualStrings(item.family, resolved.schedule.family);
    }

    // The F32 entries are semantically disjoint from the Q8_1 route used by
    // the catalog-driven FFN selector, so merely cataloging them cannot alter
    // that selector's resolution.
    var q8_pair_signature = pair_signature;
    q8_pair_signature.small_batch_matmul.activation = .q8_1;
    q8_pair_signature.small_batch_matmul.output = .q8_1;
    const q8_pair = (try resolve(q8_pair_signature, .{
        .rows = 1,
        .input_dim = 1536,
        .output_dim = 6144,
    }, 8, 9)) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(compiler.first_e2b_cuda_q4_0_pair_q8_6144_kernel_id, q8_pair.kernel_id);
}

test "catalog resolves llama CUDA Q8_1 E2B candidates only through typed catalog" {
    const pair_signature = op.SpecializationSignature{ .small_batch_matmul = .{
        .format = .q4_0,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .pair_activation,
        .activation = .q8_1,
        .function = .gelu_new,
        .output = .q8_1,
    } };
    const down_signature = op.SpecializationSignature{ .small_batch_matmul = .{
        .format = .q4_0,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .gated_down,
        .activation = .q8_1,
    } };
    const pair = (try resolveGgmlQ8_1(
        pair_signature,
        .{ .rows = 1, .input_dim = 1536, .output_dim = 6144 },
        8,
        9,
    )) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(
        compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_kernel_id,
        pair.kernel_id,
    );
    try std.testing.expectEqualStrings("q4_0_pair_activation_ggml_q8_1_c32_sm89", pair.schedule.family);
    try std.testing.expect(!pair.production_enabled);

    const down = (try resolveGgmlQ8_1(
        down_signature,
        .{ .rows = 1, .input_dim = 12_288, .output_dim = 1536 },
        8,
        9,
    )) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(
        compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_12288_kernel_id,
        down.kernel_id,
    );
    try std.testing.expectEqual(@as(u16, 128), down.schedule.threads_per_block);
    try std.testing.expectEqual(@as(u16, 1), down.schedule.cols_per_block);
    try std.testing.expect(!down.production_enabled);

    try std.testing.expect((try resolveGgmlQ8_1(
        pair_signature,
        .{ .rows = 2, .input_dim = 1536, .output_dim = 6144 },
        8,
        9,
    )) == null);
}

test "catalog resolves model-neutral Gemma 4 attention topologies" {
    const signature = op.SpecializationSignature{ .attention = .{ .kind = .decode_1x } };
    const resolved = (try resolve(signature, .{
        .rows = 1,
        .head_dim = 512,
        .query_heads = 16,
        .kv_heads = 1,
        .sequence_length = 256,
    }, 8, 9)) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(compiler.first_decode_attention_1x_cuda_hd512_kernel_id, resolved.kernel_id);
    try std.testing.expect(!resolved.production_enabled);

    const artifact = compiler.generatedRegistryArtifactForKernel(resolved.target.backend, resolved.kernel_id) orelse
        return error.CatalogArtifactMissing;
    var workspace_overflow = resolved;
    workspace_overflow.runtime.query_heads = .exact(64);
    workspace_overflow.runtime.kv_heads = .exact(4);
    try std.testing.expectError(
        error.CatalogRuntimeShapeMismatch,
        validateEntryAgainstArtifact(workspace_overflow, artifact),
    );

    try std.testing.expect((try resolve(signature, .{
        .rows = 1,
        .head_dim = 128,
        .query_heads = 8,
        .kv_heads = 1,
        .sequence_length = 256,
    }, 8, 9)) == null);
    try std.testing.expect((try resolve(signature, .{
        .rows = 1,
        .head_dim = 512,
        .query_heads = 16,
        .kv_heads = 1,
        .sequence_length = 256,
    }, 8, 0)) == null);
}

test "catalog resolves long-context SM89 split-K online decode without widening visibility" {
    const signature = op.SpecializationSignature{ .attention = .{
        .kind = .decode_1x,
        .query = .f32,
        .key = .paged_f16,
        .value = .paged_f16,
        .output = .f32,
    } };
    const expected = [_]struct {
        head_dim: u32,
        kernel_id: []const u8,
        sliding_window: u16,
        max_visible_tokens: u16,
    }{
        .{ .head_dim = 256, .kernel_id = compiler.first_decode_splitk_online_cuda_hd256_kernel_id, .sliding_window = 512, .max_visible_tokens = 512 },
        .{ .head_dim = 512, .kernel_id = compiler.first_decode_splitk_online_cuda_hd512_kernel_id, .sliding_window = 0, .max_visible_tokens = 4096 },
    };
    for (expected) |item| {
        const resolved = (try resolve(signature, .{
            .rows = 1,
            .head_dim = item.head_dim,
            .query_heads = 8,
            .kv_heads = 1,
            .sequence_length = 2432,
        }, 8, 9)) orelse return error.MissingCatalogEntry;
        try std.testing.expectEqualStrings(item.kernel_id, resolved.kernel_id);
        try std.testing.expectEqual(@as(u32, 4096), resolved.runtime.sequence_length.max);
        const artifact = compiler.generatedRegistryArtifactForKernel(resolved.target.backend, resolved.kernel_id) orelse
            return error.CatalogArtifactMissing;
        const plan = compiler.cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact) orelse
            return error.CatalogArtifactCudaRenderPlanMissing;
        try std.testing.expectEqual(item.sliding_window, plan.lowering.sliding_window);
        try std.testing.expectEqual(item.max_visible_tokens, plan.lowering.max_visible_tokens);
        try std.testing.expect(compiler.cudaSplitkOnlineDecodeArtifactRuntimeWired(artifact));
        try std.testing.expect(!artifact.production_enabled);
        try validateEntryAgainstArtifact(resolved, artifact);
    }

    var outside = op.RuntimeShape{
        .rows = 1,
        .head_dim = 512,
        .query_heads = 8,
        .kv_heads = 1,
        .sequence_length = 4097,
    };
    try std.testing.expect((try resolve(signature, outside, 8, 9)) == null);
    outside.sequence_length = 2432;
    outside.query_heads = 16;
    try std.testing.expect((try resolve(signature, outside, 8, 9)) == null);
    outside.query_heads = 8;
    try std.testing.expect((try resolve(signature, outside, 8, 0)) == null);
}

test "catalog resolves qualified SM89 F16 Flash-prefill buckets" {
    const signature = op.SpecializationSignature{ .attention = .{
        .kind = .prefill_flash,
        .query = .f32,
        .key = .paged_f16,
        .value = .paged_f16,
        .output = .f32,
    } };
    const expected = [_]struct {
        head_dim: u32,
        query_len: u32,
        total_len: u32,
        kernel_id: []const u8,
        family: []const u8,
        dynamic_shared_bytes: u32,
    }{
        .{ .head_dim = 256, .query_len = 512, .total_len = 2048, .kernel_id = compiler.first_prefill_flash_cuda_hd256_kernel_id, .family = "gqa_flash_prefill_f16_sm89_hd256_swa512", .dynamic_shared_bytes = 26_564 },
        .{ .head_dim = 256, .query_len = 3, .total_len = 2051, .kernel_id = compiler.first_prefill_flash_cuda_hd256_kernel_id, .family = "gqa_flash_prefill_f16_sm89_hd256_swa512", .dynamic_shared_bytes = 26_564 },
        .{ .head_dim = 512, .query_len = 512, .total_len = 2048, .kernel_id = compiler.first_prefill_flash_cuda_hd512_kernel_id, .family = "gqa_flash_prefill_f16_sm89_hd512_global", .dynamic_shared_bytes = 42_948 },
        .{ .head_dim = 512, .query_len = 3, .total_len = 2051, .kernel_id = compiler.first_prefill_flash_cuda_hd512_kernel_id, .family = "gqa_flash_prefill_f16_sm89_hd512_global", .dynamic_shared_bytes = 42_948 },
    };

    var flash_entry_count: usize = 0;
    for (entries) |entry| switch (entry.signature) {
        .attention => |attention| if (attention.kind == .prefill_flash) {
            flash_entry_count += 1;
        },
        else => {},
    };
    try std.testing.expectEqual(@as(usize, 4), flash_entry_count);

    for (expected) |item| {
        const resolved = (try resolve(signature, .{
            .rows = item.query_len,
            .head_dim = item.head_dim,
            .query_heads = 8,
            .kv_heads = 1,
            .sequence_length = item.total_len,
        }, 8, 9)) orelse return error.MissingCatalogEntry;
        try std.testing.expectEqualStrings(item.kernel_id, resolved.kernel_id);
        try std.testing.expectEqualStrings(item.family, resolved.schedule.family);
        try std.testing.expectEqual(cuda_renderer.generated_flash_prefill_threads, resolved.schedule.threads_per_block);
        try std.testing.expectEqual(cuda_renderer.generated_flash_prefill_query_tile, resolved.schedule.rows_per_block);
        try std.testing.expectEqual(@as(u16, @intCast(item.head_dim)), resolved.schedule.cols_per_block);
        try std.testing.expectEqual(item.dynamic_shared_bytes, resolved.schedule.resources.dynamic_shared_memory_bytes);
        try std.testing.expect(resolved.production_enabled);

        const artifact = compiler.generatedRegistryArtifactForKernel(resolved.target.backend, resolved.kernel_id) orelse
            return error.CatalogArtifactMissing;
        try std.testing.expect(artifact.production_enabled);
        try std.testing.expect(artifact.runtime_default_enabled);
        try std.testing.expect(compiler.cudaFlashPrefillArtifactRuntimeWired(artifact));
        try validateEntryAgainstArtifact(resolved, artifact);
    }

    // The q512 entry represents exactly the four qualified prefix buckets.
    for ([_]u32{ 512, 1024, 1536, 2048 }) |total_len| {
        try std.testing.expect((try resolve(signature, .{
            .rows = 512,
            .head_dim = 256,
            .query_heads = 8,
            .kv_heads = 1,
            .sequence_length = total_len,
        }, 8, 9)) != null);
    }
}

test "catalog rejects unqualified Flash-prefill shapes targets and storage" {
    const signature = op.SpecializationSignature{ .attention = .{
        .kind = .prefill_flash,
        .query = .f32,
        .key = .paged_f16,
        .value = .paged_f16,
        .output = .f32,
    } };
    const base_shape = op.RuntimeShape{
        .rows = 512,
        .head_dim = 256,
        .query_heads = 8,
        .kv_heads = 1,
        .sequence_length = 2048,
    };

    var shape = base_shape;
    shape.rows = 1;
    try std.testing.expect((try resolve(signature, shape, 8, 9)) == null);
    shape = base_shape;
    shape.sequence_length = 2560;
    try std.testing.expect((try resolve(signature, shape, 8, 9)) == null);
    shape = base_shape;
    shape.query_heads = 16;
    try std.testing.expect((try resolve(signature, shape, 8, 9)) == null);
    try std.testing.expect((try resolve(signature, base_shape, 8, 0)) == null);

    var broad_storage = signature;
    broad_storage.attention.key = .paged_f16_or_polar4;
    broad_storage.attention.value = .paged_f16_or_f32;
    try std.testing.expect((try resolve(broad_storage, base_shape, 8, 9)) == null);

    const resolved = (try resolve(signature, base_shape, 8, 9)) orelse return error.MissingCatalogEntry;
    const artifact = compiler.generatedRegistryArtifactForKernel(resolved.target.backend, resolved.kernel_id) orelse
        return error.CatalogArtifactMissing;
    var bad_policy = resolved;
    bad_policy.runtime.sequence_length.max = 2560;
    try std.testing.expectError(
        error.CatalogRuntimeShapeMismatch,
        validateEntryAgainstArtifact(bad_policy, artifact),
    );
    var bad_schedule = resolved;
    bad_schedule.schedule.resources.dynamic_shared_memory_bytes -= 4;
    try std.testing.expectError(
        error.CatalogCudaLaunchScheduleMismatch,
        validateEntryAgainstArtifact(bad_schedule, artifact),
    );
}

test "catalog resolves promoted SM89 score-prework decode composites" {
    const signature = op.SpecializationSignature{ .attention = .{
        .kind = .decode_1x,
        .query = .f32,
        .key = .paged_f16_or_polar4,
        .value = .paged_f16_or_f32,
        .output = .f32,
    } };
    const local = (try resolve(signature, .{
        .rows = 1,
        .head_dim = 256,
        .query_heads = 8,
        .kv_heads = 2,
        .sequence_length = 2048,
    }, 8, 9)) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(
        compiler.first_decode_attention_1x_cuda_score_prework_hd256_kernel_id,
        local.kernel_id,
    );
    try std.testing.expect(local.production_enabled);
    try std.testing.expectEqual(
        @as(u16, cuda_renderer.generated_attention_score_prework_chunks),
        local.schedule.split_count,
    );

    const global = (try resolve(signature, .{
        .rows = 1,
        .head_dim = 512,
        .query_heads = 8,
        .kv_heads = 1,
        .sequence_length = cuda_renderer.generated_attention_score_prework_max_kv_tokens,
    }, 8, 9)) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(
        compiler.first_decode_attention_1x_cuda_score_prework_hd512_kernel_id,
        global.kernel_id,
    );
    try std.testing.expect(global.production_enabled);

    // Beyond the typed score capacity, and away from the qualified Gemma 4
    // topology, the composite must not resolve.
    try std.testing.expect((try resolve(signature, .{
        .rows = 1,
        .head_dim = 512,
        .query_heads = 8,
        .kv_heads = 1,
        .sequence_length = cuda_renderer.generated_attention_score_prework_max_kv_tokens + 1,
    }, 8, 9)) == null);
    try std.testing.expect((try resolve(signature, .{
        .rows = 1,
        .head_dim = 256,
        .query_heads = 16,
        .kv_heads = 8,
        .sequence_length = 2048,
    }, 8, 9)) == null);
}

test "catalog inventories tiled64 score-prework consumers as composite candidates" {
    try std.testing.expectEqual(@as(usize, 2), score_prework_tiled64_candidates.len);
    for (score_prework_tiled64_candidates) |candidate| {
        try std.testing.expect(std.mem.startsWith(u8, candidate.candidate_id, cuda_score_prework_tiled64_candidate_id));
        try std.testing.expect(candidate.head_dim == 256 or candidate.head_dim == 512);
        try std.testing.expectEqual(cuda_renderer.generated_attention_score_prework_tiled64_tile_size, candidate.threads_per_block);
        try std.testing.expectEqual(candidate.threads_per_block, candidate.output_cols_per_block);
        try std.testing.expectEqualStrings(cuda_sm89_target.architecture, candidate.target.architecture);
        try std.testing.expectEqual(cuda_sm89_target.backend, candidate.target.backend);
        try std.testing.expect(candidate.source_fingerprint != 0);
        try std.testing.expect(!candidate.production_enabled);
        try std.testing.expect(std.mem.containsAtLeast(u8, candidate.consumer_kernel_id, 1, "score_prework_tiled64"));
    }
}

test "catalog resolves exact SM89 Q6_K Q8_1 argmax family shapes" {
    const signature = op.SpecializationSignature{ .small_batch_matmul = .{
        .format = .q6_k,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .argmax,
        .activation = .q8_1,
        .output = .i32,
    } };
    const resolved = (try resolve(signature, .{
        .rows = 1,
        .input_dim = 2560,
        .output_dim = 262_144,
    }, 8, 9)) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(compiler.first_cuda_q6_k_q8_1_argmax_k2560_kernel_id, resolved.kernel_id);
    try std.testing.expectEqual(@as(u16, 160), resolved.schedule.threads_per_block);
    try std.testing.expect(!resolved.production_enabled);

    const resolved_k3840 = (try resolve(signature, .{
        .rows = 1,
        .input_dim = 3840,
        .output_dim = 262_144,
    }, 8, 9)) orelse return error.MissingCatalogEntry;
    try std.testing.expectEqualStrings(compiler.first_cuda_q6_k_q8_1_argmax_k3840_kernel_id, resolved_k3840.kernel_id);
    try std.testing.expectEqual(@as(u16, 256), resolved_k3840.schedule.threads_per_block);
    try std.testing.expectEqual(@as(u32, 256), resolved_k3840.schedule.resources.static_shared_memory_bytes);
    try std.testing.expect(!resolved_k3840.production_enabled);

    try std.testing.expect((try resolve(signature, .{
        .rows = 1,
        .input_dim = 4096,
        .output_dim = 262_144,
    }, 8, 9)) == null);
    try std.testing.expect((try resolve(signature, .{
        .rows = 1,
        .input_dim = 2560,
        .output_dim = 262_144,
    }, 8, 0)) == null);

    var f32_signature = signature;
    f32_signature.small_batch_matmul.activation = .f32;
    f32_signature.small_batch_matmul.output = .f32;
    try std.testing.expect((try resolve(f32_signature, .{
        .rows = 1,
        .input_dim = 2560,
        .output_dim = 262_144,
    }, 8, 9)) == null);
}
