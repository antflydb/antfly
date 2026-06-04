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

const build_options = @import("build_options");
const std = @import("std");
const backends = @import("../backends/backends.zig");
const decoder_bitnet_runtime = @import("../backends/decoder_bitnet_runtime.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const decoder_rms_runtime = @import("../backends/decoder_rms_runtime.zig");
const decoder_tail_runtime = @import("../backends/decoder_tail_runtime.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const cache_mod = @import("cache.zig");
const session_factory = @import("../architectures/session_factory.zig");
const gemma4_runtime = @import("../architectures/gemma4_runtime.zig");
const generation = @import("../pipelines/generation.zig");
const debug_timing = @import("../debug_timing.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const runtime = @import("../runtime/root.zig");
const contracts = @import("backend_contracts.zig");
const model_runtime = @import("model_runtime.zig");

const c_std = @import("../util/c_env.zig");

pub const TimingStats = model_runtime.RuntimeDebugTimingStats;

var timing_stats = TimingStats{};

pub fn resetTimingStats() void {
    timing_stats = .{};
}

pub fn getTimingStats() TimingStats {
    return timing_stats;
}

fn printRuntimeDebugTimingStats(metal_stats: model_runtime.RuntimeDebugTimingStats, has_runtime: bool) void {
    std.debug.print(
        "metal_executor_ms: runtime_prepare_calls={d} runtime_prepare={d} runtime_prepare_family={d} runtime_prepare_greedy={d} runtime_prepare_fast_hits={d} prefill_calls={d} prefill_prepare={d} prefill_direct_last={d} prefill_direct_family={d} prefill_family_project={d} prefill_family_span_prep={d} prefill_family_quant_attn={d} prefill_family_block_apply={d} prefill_family_frame_wait={d} prefill_family_frame_gpu={d} prefill_fallback={d} decode_begin={d} sample_calls={d} sample_direct={d} sample_fallback={d} greedy_calls={d} greedy_direct={d} greedy_fallback={d} ensure_prepared_calls={d} ensure_prepared={d} ensure_sync={d} ensure_family={d} ensure_greedy={d} ensure_fast_hits={d}\n",
        .{
            metal_stats.runtime_prepare_calls,
            @divTrunc(metal_stats.runtime_prepare_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.runtime_prepare_family_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.runtime_prepare_greedy_nanos, std.time.ns_per_ms),
            metal_stats.runtime_prepare_fast_hits,
            metal_stats.prefill_calls,
            @divTrunc(metal_stats.prefill_prepare_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_last_logits_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_family_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_family_project_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_family_span_prep_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_family_quant_attn_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_family_block_apply_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_family_frame_wait_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_direct_family_frame_gpu_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.prefill_fallback_logits_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.decode_begin_step_nanos, std.time.ns_per_ms),
            metal_stats.decode_sample_calls,
            @divTrunc(metal_stats.decode_sample_direct_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.decode_sample_fallback_nanos, std.time.ns_per_ms),
            metal_stats.decode_greedy_calls,
            @divTrunc(metal_stats.decode_greedy_direct_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.decode_greedy_fallback_nanos, std.time.ns_per_ms),
            metal_stats.ensure_prepared_calls,
            @divTrunc(metal_stats.ensure_prepared_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.ensure_prepared_sync_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.ensure_prepared_family_nanos, std.time.ns_per_ms),
            @divTrunc(metal_stats.ensure_prepared_greedy_nanos, std.time.ns_per_ms),
            metal_stats.ensure_prepared_fast_hits,
        },
    );
    if (build_options.enable_mlx and has_runtime) {
        debug_timing.printBackendTimingDetails(
            .metal,
            metal_stats.backend,
            metal_stats.decoder_runtime_ready,
            metal_stats.decoder_runtime_absolute_embeddings_prepared,
        );
    }
    const decoder_rms_stats = decoder_rms_runtime.getTimingStats();
    std.debug.print(
        "decoder_rms_prepare_ms: embed_calls={d} embed_lookup={d} embed_gather={d} embed_scale={d} rms_calls={d} rms={d} linear_calls={d} linear={d} linear_quantized_calls={d} linear_quantized={d} linear_dense_calls={d} linear_dense={d}\n",
        .{
            decoder_rms_stats.embed_calls,
            @divTrunc(decoder_rms_stats.embed_lookup_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_rms_stats.embed_gather_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_rms_stats.embed_scale_nanos, std.time.ns_per_ms),
            decoder_rms_stats.rms_norm_calls,
            @divTrunc(decoder_rms_stats.rms_norm_nanos, std.time.ns_per_ms),
            decoder_rms_stats.linear_calls,
            @divTrunc(decoder_rms_stats.linear_nanos, std.time.ns_per_ms),
            decoder_rms_stats.linear_quantized_calls,
            @divTrunc(decoder_rms_stats.linear_quantized_nanos, std.time.ns_per_ms),
            decoder_rms_stats.linear_dense_calls,
            @divTrunc(decoder_rms_stats.linear_dense_nanos, std.time.ns_per_ms),
        },
    );
    const decoder_gated_stats = decoder_gated_runtime.getTimingStats();
    const prefill_named_block_nanos =
        decoder_gated_stats.prefill_block_attention_qkv_nanos +
        decoder_gated_stats.prefill_block_attention_head_norm_nanos +
        decoder_gated_stats.prefill_block_attention_apply_nanos +
        decoder_gated_stats.prefill_block_attention_project_nanos +
        decoder_gated_stats.prefill_block_attention_fused_residual_nanos +
        decoder_gated_stats.prefill_block_ffn_norm_nanos +
        decoder_gated_stats.prefill_block_ffn_fused_nanos +
        decoder_gated_stats.prefill_block_ple_nanos +
        decoder_gated_stats.prefill_block_output_scale_nanos +
        decoder_gated_stats.prefill_frame_begin_nanos +
        decoder_gated_stats.prefill_frame_layer_spec_nanos +
        decoder_gated_stats.prefill_frame_plan_nanos +
        decoder_gated_stats.prefill_frame_execute_nanos;
    const prefill_block_unattributed_nanos = if (decoder_gated_stats.prefill_block_nanos > prefill_named_block_nanos)
        decoder_gated_stats.prefill_block_nanos - prefill_named_block_nanos
    else
        0;
    std.debug.print(
        "decoder_gated_prepare_ms: calls={d} greedy={d} lookup={d} norm_prep={d} linear_prep={d} final_lookup={d} final_prep={d} greedy_fail={d} attn_pre_fail={d} attn_post_fail={d} ffn_pre_fail={d} ffn_post_fail={d} q_fail={d} k_fail={d} v_fail={d} out_fail={d} gate_fail={d} up_fail={d} down_fail={d} final_fail={d} out_shape_rank={d} out_shape0={d} out_shape1={d}\n",
        .{
            decoder_gated_stats.prepare_calls,
            @divTrunc(decoder_gated_stats.prepare_greedy_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.lookup_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.norm_prep_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.linear_prep_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.final_lookup_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.final_prep_nanos, std.time.ns_per_ms),
            decoder_gated_stats.prepare_greedy_failures,
            decoder_gated_stats.prepare_attn_pre_norm_failures,
            decoder_gated_stats.prepare_attn_post_norm_failures,
            decoder_gated_stats.prepare_ffn_pre_norm_failures,
            decoder_gated_stats.prepare_ffn_post_norm_failures,
            decoder_gated_stats.prepare_attn_q_failures,
            decoder_gated_stats.prepare_attn_k_failures,
            decoder_gated_stats.prepare_attn_v_failures,
            decoder_gated_stats.prepare_attn_out_failures,
            decoder_gated_stats.prepare_mlp_gate_failures,
            decoder_gated_stats.prepare_mlp_up_failures,
            decoder_gated_stats.prepare_mlp_down_failures,
            decoder_gated_stats.prepare_final_norm_failures,
            decoder_gated_stats.prepare_attn_out_first_rank,
            decoder_gated_stats.prepare_attn_out_first_dim0,
            decoder_gated_stats.prepare_attn_out_first_dim1,
        },
    );
    std.debug.print(
        "decoder_gated_prefill_ms: calls={d} embed={d} ple_prepare={d} block={d} tail={d} compare={d} sync={d} last_slice={d}\n",
        .{
            decoder_gated_stats.prefill_calls,
            @divTrunc(decoder_gated_stats.prefill_embed_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_ple_prepare_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_tail_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_compare_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_sync_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_last_hidden_slice_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_preplan_frame_ms: layer_specs={d} plan={d}\n",
        .{
            @divTrunc(decoder_gated_stats.preplan_frame_layer_spec_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.preplan_frame_plan_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_prefill_ple_ms: lookup={d} embedding={d} model_proj={d} norm={d} combine={d} fallback={d}\n",
        .{
            @divTrunc(decoder_gated_stats.prefill_ple_lookup_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_ple_embedding_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_ple_model_proj_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_ple_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_ple_combine_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_ple_fallback_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_prefill_frame_ms: begin={d} layer_specs={d} plan={d} execute={d} finish={d}\n",
        .{
            @divTrunc(decoder_gated_stats.prefill_frame_begin_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_frame_layer_spec_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_frame_plan_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_frame_execute_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_frame_finish_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_prefill_breakdown_ms: named_block={d} unattributed_block={d} qkv={d} head_norm={d} attn_apply={d} attn_project={d} attn_fused_residual={d} ffn_norm={d} ffn_fused={d} ple={d} output_scale={d}\n",
        .{
            @divTrunc(prefill_named_block_nanos, std.time.ns_per_ms),
            @divTrunc(prefill_block_unattributed_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_qkv_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_head_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_apply_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_project_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_fused_residual_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_ffn_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_ffn_fused_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_ple_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_output_scale_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_prefill_ops: tokens={d} layers={d} attn_norm={d} q_linear={d} qkv={d} qkv_fused={d} qkv_split={d} kv_pair={d} head_norm={d} kv_seed={d} attn_apply={d} attn_out_linear={d} attn_post_norm={d} attn_residual_add={d}\n",
        .{
            decoder_gated_stats.prefill_query_tokens,
            decoder_gated_stats.prefill_layers,
            decoder_gated_stats.prefill_attn_norm_ops,
            decoder_gated_stats.prefill_q_linear_ops,
            decoder_gated_stats.prefill_qkv_ops,
            decoder_gated_stats.prefill_qkv_fused_ops,
            decoder_gated_stats.prefill_qkv_split_ops,
            decoder_gated_stats.prefill_kv_pair_ops,
            decoder_gated_stats.prefill_head_norm_ops,
            decoder_gated_stats.prefill_kv_seed_ops,
            decoder_gated_stats.prefill_attention_apply_ops,
            decoder_gated_stats.prefill_attn_out_linear_ops,
            decoder_gated_stats.prefill_attn_post_norm_ops,
            decoder_gated_stats.prefill_attn_residual_add_ops,
        },
    );
    std.debug.print(
        "decoder_gated_prefill_ffn_ops: ffn_norm={d} ffn_fused={d} ffn_split={d} ffn_pair={d} ffn_activation={d} ffn_multiply={d} ffn_down_linear={d} ffn_post_norm={d} ffn_residual_add={d} ple={d} ple_direct={d} ple_fallback={d} output_scale={d} tail_logits={d}\n",
        .{
            decoder_gated_stats.prefill_ffn_norm_ops,
            decoder_gated_stats.prefill_ffn_fused_ops,
            decoder_gated_stats.prefill_ffn_split_ops,
            decoder_gated_stats.prefill_ffn_pair_ops,
            decoder_gated_stats.prefill_ffn_activation_ops,
            decoder_gated_stats.prefill_ffn_multiply_ops,
            decoder_gated_stats.prefill_ffn_down_linear_ops,
            decoder_gated_stats.prefill_ffn_post_norm_ops,
            decoder_gated_stats.prefill_ffn_residual_add_ops,
            decoder_gated_stats.prefill_ple_ops,
            decoder_gated_stats.prefill_ple_direct_hits,
            decoder_gated_stats.prefill_ple_fallbacks,
            decoder_gated_stats.prefill_output_scale_ops,
            decoder_gated_stats.prefill_tail_logits_ops,
        },
    );
    std.debug.print(
        "decoder_gated_block_ms: prefill_attn={d} prefill_attn_project={d} prefill_attn_apply={d} prefill_ffn={d} prefill_ple={d} prefill_scale={d} greedy_attn={d} greedy_attn_project={d} greedy_attn_apply={d} greedy_ffn={d} greedy_ple={d} greedy_scale={d} sampled_attn={d} sampled_attn_project={d} sampled_attn_apply={d} sampled_ffn={d} sampled_ple={d} sampled_scale={d}\n",
        .{
            @divTrunc(decoder_gated_stats.prefill_block_attention_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_project_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_apply_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_ffn_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_ple_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_output_scale_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_attention_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_attention_project_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_attention_apply_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_ffn_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_ple_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_output_scale_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_attention_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_attention_project_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_attention_apply_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_ffn_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_ple_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_output_scale_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_attn_ms: prefill_qkv={d} prefill_head_norm={d} prefill_fused_residual={d} greedy_qkv={d} greedy_head_norm={d} greedy_fused_residual={d} sampled_qkv={d} sampled_head_norm={d} sampled_fused_residual={d}\n",
        .{
            @divTrunc(decoder_gated_stats.prefill_block_attention_qkv_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_head_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_attention_fused_residual_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_attention_qkv_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_attention_head_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_attention_fused_residual_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_attention_qkv_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_attention_head_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_attention_fused_residual_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_ffn_ms: prefill_norm={d} prefill_fused={d} greedy_norm={d} greedy_fused={d} sampled_norm={d} sampled_fused={d}\n",
        .{
            @divTrunc(decoder_gated_stats.prefill_block_ffn_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.prefill_block_ffn_fused_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_ffn_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_ffn_fused_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_ffn_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_ffn_fused_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_gated_qkv: gemma_fused_hits={d} gemma_fused_fallbacks={d} gemma_fused_attn_residual_hits={d} gemma_fused_attn_residual_fallbacks={d} gemma_fused_ffn_hits={d} gemma_fused_ffn_fallbacks={d}\n",
        .{
            decoder_gated_stats.gemma_fused_qkv_hits,
            decoder_gated_stats.gemma_fused_qkv_fallbacks,
            decoder_gated_stats.gemma_fused_attn_residual_hits,
            decoder_gated_stats.gemma_fused_attn_residual_fallbacks,
            decoder_gated_stats.gemma_fused_ffn_hits,
            decoder_gated_stats.gemma_fused_ffn_fallbacks,
        },
    );
    std.debug.print(
        "decoder_gated_decode_ms: greedy_calls={d} greedy_embed={d} greedy_block={d} greedy_tail={d} sampled_calls={d} sampled_embed={d} sampled_block={d} sampled_tail={d}\n",
        .{
            decoder_gated_stats.greedy_calls,
            @divTrunc(decoder_gated_stats.greedy_embed_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_block_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.greedy_tail_nanos, std.time.ns_per_ms),
            decoder_gated_stats.sampled_calls,
            @divTrunc(decoder_gated_stats.sampled_embed_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_block_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_gated_stats.sampled_tail_nanos, std.time.ns_per_ms),
        },
    );
    const decoder_bitnet_stats = decoder_bitnet_runtime.getTimingStats();
    std.debug.print(
        "decoder_bitnet_prepare_ms: calls={d} greedy={d} lookup={d} norm_prep={d} linear_prep={d} final_lookup={d} final_prep={d}\n",
        .{
            decoder_bitnet_stats.prepare_calls,
            @divTrunc(decoder_bitnet_stats.prepare_greedy_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.lookup_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.norm_prep_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.linear_prep_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.final_lookup_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.final_prep_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_bitnet_prefill_ms: calls={d} embed={d} block={d} tail={d}\n",
        .{
            decoder_bitnet_stats.prefill_calls,
            @divTrunc(decoder_bitnet_stats.prefill_embed_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.prefill_block_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.prefill_tail_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "decoder_bitnet_decode_ms: greedy_calls={d} greedy_embed={d} greedy_block={d} greedy_tail={d} sampled_calls={d} sampled_embed={d} sampled_block={d} sampled_tail={d}\n",
        .{
            decoder_bitnet_stats.greedy_calls,
            @divTrunc(decoder_bitnet_stats.greedy_embed_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.greedy_block_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.greedy_tail_nanos, std.time.ns_per_ms),
            decoder_bitnet_stats.sampled_calls,
            @divTrunc(decoder_bitnet_stats.sampled_embed_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.sampled_block_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_bitnet_stats.sampled_tail_nanos, std.time.ns_per_ms),
        },
    );
    const decoder_tail_stats = decoder_tail_runtime.getTimingStats();
    std.debug.print(
        "decoder_tail_ms: logits_calls={d} logits_norm={d} logits_lm_head={d} logits_linear={d} sampled_calls={d} sampled_logits={d} sampled_device={d} sampled_host={d}\n",
        .{
            decoder_tail_stats.logits_calls,
            @divTrunc(decoder_tail_stats.logits_norm_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_tail_stats.logits_lm_head_lookup_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_tail_stats.logits_linear_nanos, std.time.ns_per_ms),
            decoder_tail_stats.sampled_calls,
            @divTrunc(decoder_tail_stats.sampled_logits_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_tail_stats.sampled_device_sample_nanos, std.time.ns_per_ms),
            @divTrunc(decoder_tail_stats.sampled_host_sample_nanos, std.time.ns_per_ms),
        },
    );
    const provider_stats = metal_stats.backend.provider;
    std.debug.print(
        "metal_memory: tensor_device_live_mb={d} tensor_device_peak_mb={d} tensor_device_created={d} tensor_device_released={d} host_mirror_live_mb={d} host_mirror_peak_mb={d} host_mirror_download_mb={d} host_mirror_allocs={d} host_mirror_frees={d} to_host_device_calls={d} quant_slots={d} quant_raw_ref_mb={d} quant_raw_owned_mb={d} quant_prepared_mb={d} quant_runtime_slots={d} quant_runtime_mb={d} dense_host_mb={d} norm_host_mb={d} gathered_entries={d} gathered_device_mb={d} gathered_encoded_host_mb={d}\n",
        .{
            provider_stats.metal_tensor_device_owned_live_bytes / (1024 * 1024),
            provider_stats.metal_tensor_device_owned_peak_live_bytes / (1024 * 1024),
            provider_stats.metal_tensor_device_owned_buffers_created,
            provider_stats.metal_tensor_device_owned_buffers_released,
            provider_stats.metal_tensor_host_mirror_live_bytes / (1024 * 1024),
            provider_stats.metal_tensor_host_mirror_peak_live_bytes / (1024 * 1024),
            provider_stats.metal_tensor_host_mirror_download_bytes / (1024 * 1024),
            provider_stats.metal_tensor_host_mirror_allocations,
            provider_stats.metal_tensor_host_mirror_frees,
            provider_stats.metal_tensor_to_host_device_calls,
            provider_stats.metal_provider_quantized_slots,
            provider_stats.metal_provider_quantized_raw_bytes / (1024 * 1024),
            provider_stats.metal_provider_quantized_raw_owned_bytes / (1024 * 1024),
            provider_stats.metal_provider_quantized_prepared_bytes / (1024 * 1024),
            provider_stats.metal_provider_quantized_runtime_prepared_slots,
            provider_stats.metal_provider_quantized_runtime_prepared_bytes / (1024 * 1024),
            provider_stats.metal_provider_dense_slot_host_bytes / (1024 * 1024),
            provider_stats.metal_provider_norm_slot_host_bytes / (1024 * 1024),
            provider_stats.metal_provider_gathered_span_entries,
            provider_stats.metal_provider_gathered_span_device_bytes / (1024 * 1024),
            provider_stats.metal_provider_gathered_span_encoded_host_bytes / (1024 * 1024),
        },
    );
    std.debug.print(
        "metal_quant_runtime_prepare: private_slots={d} private_mb={d} private_ms={d} mapped_slots={d} mapped_mb={d} mapped_ms={d} mapped_attempts={d} mapped_fallbacks={d} mapped_failures={d}\n",
        .{
            provider_stats.metal_provider_quantized_runtime_private_slots,
            provider_stats.metal_provider_quantized_runtime_private_bytes / (1024 * 1024),
            @divTrunc(provider_stats.metal_provider_quantized_runtime_private_nanos, std.time.ns_per_ms),
            provider_stats.metal_provider_quantized_runtime_mapped_slots,
            provider_stats.metal_provider_quantized_runtime_mapped_bytes / (1024 * 1024),
            @divTrunc(provider_stats.metal_provider_quantized_runtime_mapped_nanos, std.time.ns_per_ms),
            provider_stats.metal_provider_quantized_runtime_mapped_attempts,
            provider_stats.metal_provider_quantized_runtime_mapped_fallbacks,
            provider_stats.metal_provider_quantized_runtime_mapped_failures,
        },
    );
    std.debug.print(
        "metal_runtime_memory: buffers={d} total_mb={d} embedding_mb={d} norm_mb={d} dense_linear_mb={d} dense_linear_buffers={d} dense_largest_slot={d} dense_largest_mb={d} dense_largest_in={d} dense_largest_out={d} dense_weight_mb={d} dense_f32_mb={d} dense_bf16_mb={d} dense_f32_slots={d} dense_bf16_slots={d} quant_linear_mb={d} scratch_mb={d} scratch_pool_mb={d} scratch_pool_slots={d} scratch_pool_in_use={d} scratch_pool_pending={d} attention_span_mb={d} hidden_state_mb={d} frame_retained_mb={d} graph_plan_mb={d} graph_plan_slots={d} graph_plan_active={d} graph_plan_count={d} graph_plan_allocs={d} graph_plan_reuses={d}\n",
        .{
            provider_stats.metal_runtime_buffer_count,
            provider_stats.metal_runtime_total_bytes / (1024 * 1024),
            provider_stats.metal_runtime_embedding_bytes / (1024 * 1024),
            provider_stats.metal_runtime_norm_bytes / (1024 * 1024),
            provider_stats.metal_runtime_dense_linear_bytes / (1024 * 1024),
            provider_stats.metal_runtime_dense_linear_buffer_count,
            provider_stats.metal_runtime_dense_linear_largest_slot,
            provider_stats.metal_runtime_dense_linear_largest_bytes / (1024 * 1024),
            provider_stats.metal_runtime_dense_linear_largest_in_dim,
            provider_stats.metal_runtime_dense_linear_largest_out_dim,
            provider_stats.metal_runtime_dense_linear_weight_bytes / (1024 * 1024),
            provider_stats.metal_runtime_dense_linear_f32_weight_bytes / (1024 * 1024),
            provider_stats.metal_runtime_dense_linear_bf16_weight_bytes / (1024 * 1024),
            provider_stats.metal_runtime_dense_linear_f32_slots,
            provider_stats.metal_runtime_dense_linear_bf16_slots,
            provider_stats.metal_runtime_quant_linear_bytes / (1024 * 1024),
            provider_stats.metal_runtime_scratch_bytes / (1024 * 1024),
            provider_stats.metal_runtime_scratch_pool_bytes / (1024 * 1024),
            provider_stats.metal_runtime_scratch_pool_slots,
            provider_stats.metal_runtime_scratch_pool_in_use_slots,
            provider_stats.metal_runtime_scratch_pool_pending_slots,
            provider_stats.metal_runtime_attention_span_bytes / (1024 * 1024),
            provider_stats.metal_runtime_hidden_state_bytes / (1024 * 1024),
            provider_stats.metal_runtime_frame_retained_bytes / (1024 * 1024),
            provider_stats.metal_runtime_graph_plan_bytes / (1024 * 1024),
            provider_stats.metal_runtime_graph_plan_slots,
            provider_stats.metal_runtime_graph_plan_active,
            provider_stats.metal_runtime_graph_plan_count,
            provider_stats.metal_runtime_graph_plan_allocations,
            provider_stats.metal_runtime_graph_plan_reuses,
        },
    );
    std.debug.print(
        "metal_runtime_encoders: compute={d} blit={d} last_frame_compute={d} last_frame_blit={d} planned_scopes={d} planned_barriers={d}\n",
        .{
            provider_stats.metal_runtime_compute_encoder_count,
            provider_stats.metal_runtime_blit_encoder_count,
            provider_stats.metal_runtime_last_frame_compute_encoder_count,
            provider_stats.metal_runtime_last_frame_blit_encoder_count,
            provider_stats.metal_runtime_last_frame_planned_compute_scope_count,
            provider_stats.metal_runtime_last_frame_planned_barrier_count,
        },
    );
    std.debug.print(
        "metal_runtime_blit_sources: upload={d} copy={d} slice={d} attention_span={d} ffn_copy={d} embedding={d} other={d}\n",
        .{
            provider_stats.metal_runtime_last_frame_blit_buffer_upload_count,
            provider_stats.metal_runtime_last_frame_blit_buffer_copy_count,
            provider_stats.metal_runtime_last_frame_blit_buffer_slice_count,
            provider_stats.metal_runtime_last_frame_blit_attention_span_count,
            provider_stats.metal_runtime_last_frame_blit_ffn_copy_count,
            provider_stats.metal_runtime_last_frame_blit_embedding_count,
            provider_stats.metal_runtime_last_frame_blit_other_count,
        },
    );
    std.debug.print(
        "metal_runtime_compute_sources: quant_linear={d} quant_qkv={d} quant_pair_act={d} attention={d} rms_norm={d} head_rope={d} ffn={d} ple={d} tail={d} embedding={d} dense_linear={d} layer={d} other={d}\n",
        .{
            provider_stats.metal_runtime_last_frame_compute_quant_linear_count,
            provider_stats.metal_runtime_last_frame_compute_quant_qkv_count,
            provider_stats.metal_runtime_last_frame_compute_quant_pair_act_count,
            provider_stats.metal_runtime_last_frame_compute_attention_count,
            provider_stats.metal_runtime_last_frame_compute_rms_norm_count,
            provider_stats.metal_runtime_last_frame_compute_head_rope_count,
            provider_stats.metal_runtime_last_frame_compute_ffn_count,
            provider_stats.metal_runtime_last_frame_compute_ple_count,
            provider_stats.metal_runtime_last_frame_compute_tail_count,
            provider_stats.metal_runtime_last_frame_compute_embedding_count,
            provider_stats.metal_runtime_last_frame_compute_dense_linear_count,
            provider_stats.metal_runtime_last_frame_compute_layer_count,
            provider_stats.metal_runtime_last_frame_compute_other_count,
        },
    );
    std.debug.print(
        "metal_runtime_compute_regions: attention={d} attention_project={d} ffn_norm={d} ffn={d} ple={d} tail={d} embedding={d} layer={d} other={d}\n",
        .{
            provider_stats.metal_runtime_last_frame_compute_region_attention_count,
            provider_stats.metal_runtime_last_frame_compute_region_attention_project_count,
            provider_stats.metal_runtime_last_frame_compute_region_ffn_norm_count,
            provider_stats.metal_runtime_last_frame_compute_region_ffn_count,
            provider_stats.metal_runtime_last_frame_compute_region_ple_count,
            provider_stats.metal_runtime_last_frame_compute_region_tail_count,
            provider_stats.metal_runtime_last_frame_compute_region_embedding_count,
            provider_stats.metal_runtime_last_frame_compute_region_layer_count,
            provider_stats.metal_runtime_last_frame_compute_region_other_count,
        },
    );
    const command_ops = provider_stats.metal_runtime_last_frame_planned_command_op_kind_counts;
    const command_operators = provider_stats.metal_runtime_last_frame_planned_command_operator_counts;
    const command_dispatch = provider_stats.metal_runtime_last_frame_planned_command_quant_dispatch_counts;
    std.debug.print(
        "metal_runtime_command_ops: total={d} attention_pre_norm={d} qkv_linear={d} q_head_norm_rope={d} k_head_norm_rope={d} v_norm={d} kv_seed={d} attention={d} attention_output_linear={d} attention_post_norm_residual={d} ffn_pre_norm_scale={d} ffn_gate_up_activation={d} ffn_down_linear={d} ffn_post_norm_residual={d} ple_gate_activation={d} ple_projection={d} ple_post_norm_residual={d} tail_final_norm={d} tail_lm_head={d} tail_argmax={d}\n",
        .{
            provider_stats.metal_runtime_last_frame_planned_command_op_count,
            command_ops[1],
            command_ops[2],
            command_ops[3],
            command_ops[4],
            command_ops[5],
            command_ops[6],
            command_ops[7],
            command_ops[8],
            command_ops[9],
            command_ops[10],
            command_ops[11],
            command_ops[12],
            command_ops[13],
            command_ops[14],
            command_ops[15],
            command_ops[16],
            command_ops[17],
            command_ops[18],
            command_ops[19],
        },
    );
    std.debug.print(
        "metal_runtime_command_operators: fallback={d} mul_mv={d} mul_mv_ext={d} mul_mm={d} get_rows={d} set_rows={d} cpy_q_to_f32={d} cpy_f32_to_q={d} attention_flash={d} attention_paged={d} attention_quantized_kv={d} dispatch_scalar={d} dispatch_mmv={d} dispatch_small_batch={d} dispatch_mm={d}\n",
        .{
            command_operators[0],
            command_operators[1],
            command_operators[2],
            command_operators[3],
            command_operators[4],
            command_operators[5],
            command_operators[6],
            command_operators[7],
            command_operators[8],
            command_operators[9],
            command_operators[10],
            command_dispatch[0],
            command_dispatch[1],
            command_dispatch[2],
            command_dispatch[3],
        },
    );
    std.debug.print(
        "metal_prefill_frame_executor_ms: layer_runtime={d} setup={d} block={d} staged={d} full_frames={d} windows={d} layer_contracts={d} tail_contracts={d}\n",
        .{
            @divTrunc(provider_stats.prefill_frame_executor_layer_runtime_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.prefill_frame_executor_layer_setup_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.prefill_frame_executor_layer_block_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.prefill_frame_executor_layer_staged_nanos, std.time.ns_per_ms),
            provider_stats.prefill_frame_contract_full_frames,
            provider_stats.prefill_frame_contract_windows,
            provider_stats.prefill_frame_executor_layer_contracts,
            provider_stats.prefill_frame_executor_tail_contracts,
        },
    );
    std.debug.print(
        "metal_q8_0_dispatch: scalar={d} mmv={d} small_batch={d} mm={d} rows_1={d} rows_2_8={d} rows_9_64={d} rows_65_plus={d} pair_act_mm_out_f16={d} linear_mm_in_f16={d} pair_act_rms_mmv_out_f16={d} linear_mmv_in_f16={d}\n",
        .{
            provider_stats.metal_runtime_q8_0_linear_dispatch_scalar,
            provider_stats.metal_runtime_q8_0_linear_dispatch_mmv,
            provider_stats.metal_runtime_q8_0_linear_dispatch_small_batch,
            provider_stats.metal_runtime_q8_0_linear_dispatch_mm,
            provider_stats.metal_runtime_q8_0_linear_rows_1,
            provider_stats.metal_runtime_q8_0_linear_rows_2_8,
            provider_stats.metal_runtime_q8_0_linear_rows_9_64,
            provider_stats.metal_runtime_q8_0_linear_rows_65_plus,
            provider_stats.metal_runtime_q8_0_pair_activation_mm_f16_output,
            provider_stats.metal_runtime_q8_0_linear_mm_f16_input,
            provider_stats.metal_runtime_q8_0_pair_activation_rms_scale_mmv_f16_output,
            provider_stats.metal_runtime_q8_0_linear_mmv_f16_input,
        },
    );
    const q8_family_dispatch = provider_stats.metal_runtime_q8_0_linear_family_dispatch_counts;
    std.debug.print(
        "metal_q8_0_dispatch_families: none={d}/{d}/{d}/{d} pair_act={d}/{d}/{d}/{d} pair_act_rms={d}/{d}/{d}/{d} act_rhs={d}/{d}/{d}/{d} pair={d}/{d}/{d}/{d} qkv={d}/{d}/{d}/{d}\n",
        .{
            q8_family_dispatch[0][0],
            q8_family_dispatch[0][1],
            q8_family_dispatch[0][2],
            q8_family_dispatch[0][3],
            q8_family_dispatch[1][0],
            q8_family_dispatch[1][1],
            q8_family_dispatch[1][2],
            q8_family_dispatch[1][3],
            q8_family_dispatch[2][0],
            q8_family_dispatch[2][1],
            q8_family_dispatch[2][2],
            q8_family_dispatch[2][3],
            q8_family_dispatch[3][0],
            q8_family_dispatch[3][1],
            q8_family_dispatch[3][2],
            q8_family_dispatch[3][3],
            q8_family_dispatch[4][0],
            q8_family_dispatch[4][1],
            q8_family_dispatch[4][2],
            q8_family_dispatch[4][3],
            q8_family_dispatch[5][0],
            q8_family_dispatch[5][1],
            q8_family_dispatch[5][2],
            q8_family_dispatch[5][3],
        },
    );
    std.debug.print(
        "metal_q8_0_dispatch_families_ext: rms_scale={d}/{d}/{d}/{d} attention_out={d}/{d}/{d}/{d} ffn_down={d}/{d}/{d}/{d} ple_projection={d}/{d}/{d}/{d} tail={d}/{d}/{d}/{d} ple_gate={d}/{d}/{d}/{d}\n",
        .{
            q8_family_dispatch[6][0],
            q8_family_dispatch[6][1],
            q8_family_dispatch[6][2],
            q8_family_dispatch[6][3],
            q8_family_dispatch[7][0],
            q8_family_dispatch[7][1],
            q8_family_dispatch[7][2],
            q8_family_dispatch[7][3],
            q8_family_dispatch[8][0],
            q8_family_dispatch[8][1],
            q8_family_dispatch[8][2],
            q8_family_dispatch[8][3],
            q8_family_dispatch[9][0],
            q8_family_dispatch[9][1],
            q8_family_dispatch[9][2],
            q8_family_dispatch[9][3],
            q8_family_dispatch[10][0],
            q8_family_dispatch[10][1],
            q8_family_dispatch[10][2],
            q8_family_dispatch[10][3],
            q8_family_dispatch[11][0],
            q8_family_dispatch[11][1],
            q8_family_dispatch[11][2],
            q8_family_dispatch[11][3],
        },
    );
    const gpt_stats = gpt_arch.getDebugTimingStats();
    std.debug.print(
        "gpt_block_counts: dense_attempts={d} dense_successes={d} gated_attempts={d} gated_successes={d} gated_input_attempts={d} gated_input_successes={d} gated_input_prefill={d} gated_input_decode={d} gated_qkv_attempts={d} gated_qkv_successes={d}\n",
        .{
            gpt_stats.dense_block_attempts,
            gpt_stats.dense_block_successes,
            gpt_stats.gated_block_attempts,
            gpt_stats.gated_block_successes,
            gpt_stats.gated_block_input_attempts,
            gpt_stats.gated_block_input_successes,
            @divTrunc(gpt_stats.gated_block_input_prefill_nanos, std.time.ns_per_ms),
            @divTrunc(gpt_stats.gated_block_input_decode_nanos, std.time.ns_per_ms),
            gpt_stats.gated_block_qkv_attempts,
            gpt_stats.gated_block_qkv_successes,
        },
    );
}

pub fn printRuntimeDebugTiming(runtime_opt: ?*const model_runtime.ModelRuntime) void {
    const metal_stats = if (runtime_opt) |runtime_model|
        runtime_model.debugTimingStats()
    else
        model_runtime.RuntimeDebugTimingStats{};
    printRuntimeDebugTimingStats(metal_stats, runtime_opt != null);
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

/// Metal-backed whole-model executor.
///
/// The first slice keeps prefill on the existing MLX runtime but routes
/// qLen=1 supported decoder-family decode through the raw whole-token
/// Metal-owned input/override path that was previously only reachable from
/// `generation.zig`.
fn disablePagedKvDebug() bool {
    const value = c_std.getenv("TERMITE_DISABLE_PAGED_KV") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

const ExecutorContext = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    gpt_config: gpt_mod.Config,
    kv_dtype: ?runtime.kv.pool.KvDType = null,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
};

const DecoderRuntimeSpanState = struct {
    kv_tokens: usize,
    kv_position_offset: usize,
};

const ExecutorKvMetadataLayer = struct {
    allocator: std.mem.Allocator,
    pool_id: runtime.kv.block.KvPoolId,
    sequence_id: ?runtime.kv.manager.SequenceId = null,
    total_token_count: usize = 0,
    position_offset: usize = 0,
    tail_tokens: u16 = 0,
    kv_view: ?generation.KvView = null,
    compacted: bool = false,
    logical_blocks: std.ArrayListUnmanaged(runtime.kv.block.KvBlockId) = .empty,

    fn init(allocator: std.mem.Allocator, pool_id: runtime.kv.block.KvPoolId) ExecutorKvMetadataLayer {
        return .{ .allocator = allocator, .pool_id = pool_id };
    }

    fn deinit(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) void {
        self.releaseOwnedSequence(storage) catch {};
        self.logical_blocks.deinit(self.allocator);
        self.* = undefined;
    }

    fn reset(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) !void {
        try self.releaseOwnedSequence(storage);
        self.total_token_count = 0;
        self.position_offset = 0;
        self.tail_tokens = 0;
        self.kv_view = null;
        self.compacted = false;
        self.logical_blocks.clearRetainingCapacity();
    }

    fn tokenCount(self: *const ExecutorKvMetadataLayer, storage: *const runtime.kv.storage_runtime.KvStorageRuntime) usize {
        if (self.logical_blocks.items.len == 0) return 0;
        const pool = storage.getPool(self.pool_id) orelse return 0;
        const page_size = pool.config.page_size_tokens;
        return (self.logical_blocks.items.len - 1) * page_size + self.tail_tokens;
    }

    fn updateView(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) void {
        const sequence_id = self.sequence_id orelse {
            self.kv_view = null;
            return;
        };
        const retained_tokens = self.tokenCount(storage);
        if (retained_tokens == 0) {
            self.kv_view = null;
            return;
        }
        self.kv_view = .{
            .sequence_id = sequence_id,
            .pool_id = self.pool_id,
            .logical_block_count = self.logical_blocks.items.len,
            .tail_tokens = self.tail_tokens,
            .token_count = retained_tokens,
            .position_offset = self.position_offset,
            .logical_blocks = self.logical_blocks.items,
            .kv_storage = storage,
        };
    }

    fn syncSequenceState(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) !void {
        const sequence_id = self.sequence_id orelse return;
        const seq_state = try storage.sequenceMut(sequence_id);
        seq_state.compacted = self.compacted;
        seq_state.block_table.shared_prefix_blocks = 0;
        try seq_state.block_table.blocks.resize(self.allocator, self.logical_blocks.items.len);
        @memcpy(seq_state.block_table.blocks.items, self.logical_blocks.items);
        seq_state.block_table.tail_tokens = self.tail_tokens;
        self.updateView(storage);
        try self.reserveDeviceCapacity(storage);
    }

    fn reserveDeviceCapacity(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) !void {
        if (disableDeviceKvReserveRequested()) return;
        const sequence_id = self.sequence_id orelse return;
        const pool = storage.getPool(self.pool_id) orelse return error.InvalidPoolId;
        const retained_tokens = self.tokenCount(storage);
        if (retained_tokens == 0) return;
        for (0..pool.config.num_layers_packed) |layer_index| {
            storage.reserveLayerKvDeviceCapacity(
                sequence_id,
                layer_index,
                retained_tokens,
                self.position_offset,
                pool.config.num_kv_heads,
                pool.config.head_dim,
            ) catch |err| switch (err) {
                error.DeviceWriteUnsupported,
                error.DeviceWriteFormatUnsupported,
                error.DeviceWriteFallback,
                => return,
                else => return err,
            };
        }
    }

    fn ensureAttached(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) !void {
        if (self.sequence_id != null) return;
        self.sequence_id = try storage.attachSequence(self.pool_id);
        try self.syncSequenceState(storage);
    }

    fn releaseOwnedSequence(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) !void {
        const sequence_id = self.sequence_id orelse return;
        try storage.releaseSequence(sequence_id);
        self.sequence_id = null;
        self.total_token_count = 0;
        self.position_offset = 0;
        self.tail_tokens = 0;
        self.compacted = false;
        self.logical_blocks.clearRetainingCapacity();
        self.kv_view = null;
    }

    fn reserveTailBlock(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) !runtime.kv.block.KvBlockId {
        const pool = storage.getPoolMut(self.pool_id) orelse return error.InvalidPoolId;
        const page_size = pool.config.page_size_tokens;
        if (self.logical_blocks.items.len > 0 and self.tail_tokens < page_size) {
            return self.logical_blocks.items[self.logical_blocks.items.len - 1];
        }
        const id = try pool.acquire(self.allocator);
        try self.logical_blocks.append(self.allocator, id);
        self.tail_tokens = 0;
        return id;
    }

    fn appendTokensNoTrim(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime, count: usize) !void {
        try self.ensureAttached(storage);
        const pool = storage.getPoolMut(self.pool_id) orelse return error.InvalidPoolId;
        const page_size = pool.config.page_size_tokens;
        var remaining: usize = count;
        while (remaining > 0) {
            _ = try self.reserveTailBlock(storage);
            const space = page_size - self.tail_tokens;
            const consumed = @min(space, remaining);
            self.tail_tokens += @intCast(consumed);
            remaining -= consumed;
        }
    }

    fn fullBlockCount(self: *const ExecutorKvMetadataLayer, storage: *const runtime.kv.storage_runtime.KvStorageRuntime) usize {
        if (self.logical_blocks.items.len == 0) return 0;
        const pool = storage.getPool(self.pool_id) orelse return 0;
        const page_size = pool.config.page_size_tokens;
        if (self.tail_tokens == page_size) return self.logical_blocks.items.len;
        return self.logical_blocks.items.len - 1;
    }

    fn trimToSlidingWindow(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime) !usize {
        if (self.compacted) return 0;
        const pool = storage.getPoolMut(self.pool_id) orelse return error.InvalidPoolId;
        const keep_tokens = pool.config.sliding_window_size orelse return 0;
        const current_tokens = self.tokenCount(storage);
        if (keep_tokens >= current_tokens) return 0;
        const page_size = pool.config.page_size_tokens;
        const excess_tokens = current_tokens - keep_tokens;
        const droppable_blocks = @min(excess_tokens / page_size, self.fullBlockCount(storage));
        if (droppable_blocks == 0) return 0;
        for (self.logical_blocks.items[0..droppable_blocks]) |block_id| {
            _ = try pool.releaseRef(self.allocator, block_id);
        }
        std.mem.copyForwards(runtime.kv.block.KvBlockId, self.logical_blocks.items[0 .. self.logical_blocks.items.len - droppable_blocks], self.logical_blocks.items[droppable_blocks..]);
        self.logical_blocks.items.len -= droppable_blocks;
        return droppable_blocks * page_size;
    }

    fn notePrefill(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime, token_count: usize) !generation.KvMutationResult {
        const before_view = self.kv_view;
        const before_sequence_id = self.sequence_id;
        self.total_token_count = token_count;
        self.position_offset = 0;
        self.compacted = false;
        try self.appendTokensNoTrim(storage, token_count);
        try self.syncSequenceState(storage);
        return .{
            .token_count = self.total_token_count,
            .kv_view = self.kv_view,
            .compacted = self.compacted,
            .delta = .{
                .sequence_replaced = before_sequence_id != self.sequence_id,
                .logical_block_count_before = if (before_view) |view| view.logical_block_count else 0,
                .logical_block_count_after = if (self.kv_view) |view| view.logical_block_count else 0,
                .position_offset_before = if (before_view) |view| view.position_offset else 0,
                .position_offset_after = self.position_offset,
            },
        };
    }

    fn appendPrefillChunk(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime, token_count: usize) !generation.KvMutationResult {
        const before_view = self.kv_view;
        const before_sequence_id = self.sequence_id;
        self.total_token_count += token_count;
        self.position_offset = 0;
        self.compacted = false;
        try self.appendTokensNoTrim(storage, token_count);
        try self.syncSequenceState(storage);
        return .{
            .token_count = self.total_token_count,
            .kv_view = self.kv_view,
            .compacted = self.compacted,
            .delta = .{
                .sequence_replaced = before_sequence_id != self.sequence_id,
                .logical_block_count_before = if (before_view) |view| view.logical_block_count else 0,
                .logical_block_count_after = if (self.kv_view) |view| view.logical_block_count else 0,
                .position_offset_before = if (before_view) |view| view.position_offset else 0,
                .position_offset_after = self.position_offset,
            },
        };
    }

    fn appendGeneratedTokens(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime, count: usize) !generation.KvMutationResult {
        const before_view = self.kv_view;
        const before_sequence_id = self.sequence_id;
        self.total_token_count += count;
        try self.appendTokensNoTrim(storage, count);
        const retained_tokens_before_trim = self.tokenCount(storage);
        _ = try self.trimToSlidingWindow(storage);
        const retained_tokens = self.tokenCount(storage);
        self.position_offset = self.total_token_count - retained_tokens;
        try self.syncSequenceState(storage);
        return .{
            .token_count = self.total_token_count,
            .kv_view = self.kv_view,
            .compacted = self.compacted,
            .delta = .{
                .sequence_replaced = before_sequence_id != self.sequence_id,
                .logical_block_count_before = if (before_view) |view| view.logical_block_count else 0,
                .logical_block_count_after = if (self.kv_view) |view| view.logical_block_count else 0,
                .position_offset_before = if (before_view) |view| view.position_offset else 0,
                .position_offset_after = self.position_offset,
                .retained_tokens = retained_tokens_before_trim,
            },
        };
    }

    fn truncateTokens(self: *ExecutorKvMetadataLayer, storage: *runtime.kv.storage_runtime.KvStorageRuntime, count: usize) !generation.KvMutationResult {
        const before_view = self.kv_view;
        const before_sequence_id = self.sequence_id;
        if (count > self.total_token_count) return error.TruncateBeyondStart;
        self.total_token_count -= count;
        const pool = storage.getPoolMut(self.pool_id) orelse return error.InvalidPoolId;
        const page_size = pool.config.page_size_tokens;
        const current = self.tokenCount(storage);
        if (count >= current) {
            const dropped = try self.allocator.dupe(runtime.kv.block.KvBlockId, self.logical_blocks.items);
            defer self.allocator.free(dropped);
            self.logical_blocks.clearRetainingCapacity();
            self.tail_tokens = 0;
            for (dropped) |block_id| _ = try pool.releaseRef(self.allocator, block_id);
        } else if (count > 0) {
            const old_len = self.logical_blocks.items.len;
            const target = current - count;
            const needed_blocks = (target + page_size - 1) / page_size;
            const excess_blocks = if (old_len > needed_blocks) old_len - needed_blocks else 0;
            if (excess_blocks > 0) {
                const dropped = try self.allocator.dupe(runtime.kv.block.KvBlockId, self.logical_blocks.items[old_len - excess_blocks .. old_len]);
                defer self.allocator.free(dropped);
                self.logical_blocks.items.len -= excess_blocks;
                for (dropped) |block_id| _ = try pool.releaseRef(self.allocator, block_id);
            }
            const rem: u16 = @intCast(target % page_size);
            self.tail_tokens = if (rem == 0) page_size else rem;
        }
        const retained_tokens = self.tokenCount(storage);
        self.position_offset = self.total_token_count - retained_tokens;
        try self.syncSequenceState(storage);
        return .{
            .token_count = self.total_token_count,
            .kv_view = self.kv_view,
            .compacted = self.compacted,
            .delta = .{
                .sequence_replaced = before_sequence_id != self.sequence_id,
                .logical_block_count_before = if (before_view) |view| view.logical_block_count else 0,
                .logical_block_count_after = if (self.kv_view) |view| view.logical_block_count else 0,
                .position_offset_before = if (before_view) |view| view.position_offset else 0,
                .position_offset_after = self.position_offset,
            },
        };
    }
};

const RuntimeContext = struct {
    allocator: std.mem.Allocator,
    cb: ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    use_decoder_runtime_executor: bool,
    kv_storage: runtime.kv.storage_runtime.KvStorageRuntime,
    pool_id: runtime.kv.block.KvPoolId,
    moe_runtime: runtime.moe.runtime.MoeRuntime,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
    kv_metadata: ExecutorKvMetadataLayer,
    mirrored_token_count: usize,
    mirrored_kv_view: ?generation.KvView,
    mirrored_kv_compacted: bool,
    raw_span_state: ?DecoderRuntimeSpanState,

    fn init(
        allocator: std.mem.Allocator,
        session: backends.Session,
        gpt_config: gpt_mod.Config,
        kv_dtype_override: ?runtime.kv.pool.KvDType,
        shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
    ) !*RuntimeContext {
        const cb = try session_factory.getComputeBackend(session, allocator);
        errdefer {
            var cb_mut = cb;
            cb_mut.deinit();
        }

        const session_backend = session.backend();
        const kv_dtype = kv_dtype_override orelse session_factory.recommendedKvDTypeForSession(session, .metal);
        const needs_cpu_kv_bridge = switch (session_backend) {
            .metal => false,
            else => false,
        };
        const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
            null
        else if (gpt_config.sliding_window > 0)
            gpt_config.sliding_window
        else if (gpt_config.max_position_embeddings > 0)
            gpt_config.max_position_embeddings
        else
            null;

        var kv_storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, .{
            .backend = .metal,
            .dtype = kv_dtype,
            .page_size_tokens = 16,
            .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
            .num_kv_heads = gpt_config.maxKvHeads(),
            .head_dim = gpt_config.maxHeadDim(),
            .sliding_window_size = sliding_window_size,
            .store_cpu_bytes = needs_cpu_kv_bridge,
        });
        errdefer kv_storage.deinit();
        // Phase 6 wiring: ask the Metal compute backend to install its device
        // write hook if the KV dtype matches the fast path. When unsupported
        // (e.g. f32/int8 KVs, or no decoder runtime), this is a no-op and
        // writes continue through the host path.
        cb.provisionKvDeviceWriteHook(&kv_storage) catch {};

        const pool_id = kv_storage.poolId();

        const ctx = try allocator.create(RuntimeContext);
        ctx.* = .{
            .allocator = allocator,
            .cb = cb,
            .gpt_config = gpt_config,
            .use_decoder_runtime_executor = session.backend() == .metal,
            .kv_storage = kv_storage,
            .pool_id = pool_id,
            .moe_runtime = runtime.moe.runtime.MoeRuntime.init(allocator, shared_moe_cache),
            .shared_moe_cache = shared_moe_cache,
            .kv_metadata = ExecutorKvMetadataLayer.init(allocator, pool_id),
            .mirrored_token_count = 0,
            .mirrored_kv_view = null,
            .mirrored_kv_compacted = false,
            .raw_span_state = null,
        };
        return ctx;
    }

    fn resetState(self: *RuntimeContext) !void {
        try self.invalidateDecoderRuntimeSpanState();
        self.moe_runtime.deinit();
        self.moe_runtime = runtime.moe.runtime.MoeRuntime.init(self.allocator, self.shared_moe_cache);
        try self.kv_metadata.reset(&self.kv_storage);
        self.mirrored_token_count = 0;
        self.mirrored_kv_view = null;
        self.mirrored_kv_compacted = false;
    }

    fn deinit(self: *RuntimeContext) void {
        self.moe_runtime.deinit();
        self.kv_metadata.deinit(&self.kv_storage);
        self.kv_storage.deinit();
        self.cb.deinit();
        self.allocator.destroy(self);
    }

    fn invalidateDecoderRuntimeSpanState(self: *RuntimeContext) !void {
        try self.cb.decoderRuntimeResetState();
        self.raw_span_state = null;
    }

    fn applyKvMutationResult(self: *RuntimeContext, result: generation.KvMutationResult) !void {
        _ = result;
        self.mirrored_token_count = self.kv_metadata.total_token_count;
        self.mirrored_kv_view = self.kv_metadata.kv_view;
        self.mirrored_kv_compacted = self.kv_metadata.compacted;
    }

    fn decoderRuntimeConfiguredLayerCount(self: *const RuntimeContext) usize {
        return self.gpt_config.num_hidden_layers;
    }

    fn decoderRuntimeExecutorEnabled(self: *const RuntimeContext) bool {
        return self.use_decoder_runtime_executor;
    }

    fn notePrefill(self: *RuntimeContext, token_count: usize) !void {
        try self.invalidateDecoderRuntimeSpanState();
        const result = try self.kv_metadata.notePrefill(&self.kv_storage, token_count);
        try self.applyKvMutationResult(result);
    }

    fn appendPrefillChunk(self: *RuntimeContext, token_count: usize) !void {
        try self.invalidateDecoderRuntimeSpanState();
        const result = try self.kv_metadata.appendPrefillChunk(&self.kv_storage, token_count);
        try self.applyKvMutationResult(result);
    }

    fn appendGeneratedToken(self: *RuntimeContext) !usize {
        const result = try self.kv_metadata.appendGeneratedTokens(&self.kv_storage, 1);
        try self.applyKvMutationResult(result);
        return self.currentTokenCount();
    }

    fn appendGeneratedTokens(self: *RuntimeContext, count: usize) !usize {
        const result = try self.kv_metadata.appendGeneratedTokens(&self.kv_storage, count);
        try self.applyKvMutationResult(result);
        return self.currentTokenCount();
    }

    fn truncateGeneratedTokens(self: *RuntimeContext, count: usize) !void {
        try self.invalidateDecoderRuntimeSpanState();
        const result = try self.kv_metadata.truncateTokens(&self.kv_storage, count);
        try self.applyKvMutationResult(result);
    }

    fn compactKvCache(self: *RuntimeContext, config: runtime.kv.compaction.CompactionConfig) !void {
        try self.invalidateDecoderRuntimeSpanState();
        const sequence_id = self.kv_metadata.sequence_id orelse return;
        const kv_view = self.kv_metadata.kv_view orelse return error.InvalidPagedKvState;
        const ops_kv_view = self.kvCacheView(kv_view);
        var compacted = blk: {
            if (try self.cb.gatherPagedKvLayerCache(self.allocator, ops_kv_view, kv_view.token_count, 0)) |first_gathered| {
                const pool = self.kv_storage.getPool(self.pool_id) orelse return error.InvalidPoolId;
                const num_layers = pool.config.num_layers_packed;
                const gathered_layers = try self.allocator.alloc(runtime.kv.compaction.GatheredLayerKv, num_layers);
                defer self.allocator.free(gathered_layers);
                gathered_layers[0] = .{ .k = first_gathered.k, .v = first_gathered.v };
                var gathered_count: usize = 1;
                defer {
                    for (gathered_layers[0..gathered_count]) |layer_data| {
                        self.allocator.free(layer_data.k);
                        self.allocator.free(layer_data.v);
                    }
                }
                for (1..num_layers) |layer| {
                    const gathered = (try self.cb.gatherPagedKvLayerCache(
                        self.allocator,
                        ops_kv_view,
                        kv_view.token_count,
                        layer,
                    )) orelse return error.InvalidPagedKvState;
                    gathered_layers[layer] = .{ .k = gathered.k, .v = gathered.v };
                    gathered_count += 1;
                }
                break :blk try runtime.kv.compaction.compactGatheredSequence(
                    self.allocator,
                    gathered_layers,
                    kv_view.token_count,
                    pool.config.num_kv_heads,
                    pool.config.head_dim,
                    config,
                );
            }
            break :blk try runtime.kv.compaction.compactStorageSequence(
                self.allocator,
                &self.kv_storage,
                sequence_id,
                config,
            );
        };
        defer compacted.deinit();

        var replacement = ExecutorKvMetadataLayer.init(self.allocator, self.pool_id);
        replacement.sequence_id = try self.kv_storage.attachSequence(self.pool_id);
        errdefer replacement.releaseOwnedSequence(&self.kv_storage) catch {};
        replacement.total_token_count = self.kv_metadata.total_token_count;
        replacement.position_offset = replacement.total_token_count - compacted.retained_count;
        replacement.compacted = true;
        try replacement.appendTokensNoTrim(&self.kv_storage, compacted.retained_count);
        try replacement.syncSequenceState(&self.kv_storage);
        replacement.updateView(&self.kv_storage);
        const pool = self.kv_storage.getPool(self.pool_id) orelse return error.InvalidPoolId;
        if (replacement.kv_view) |replacement_view| {
            const replacement_ops_view = self.kvCacheView(replacement_view);
            if (try self.cb.seedPagedKvLayerCache(
                replacement_ops_view,
                replacement_view.token_count,
                0,
                compacted.k_per_layer[0],
                compacted.v_per_layer[0],
            )) {
                for (1..pool.config.num_layers_packed) |layer| {
                    _ = try self.cb.seedPagedKvLayerCache(
                        replacement_ops_view,
                        replacement_view.token_count,
                        layer,
                        compacted.k_per_layer[layer],
                        compacted.v_per_layer[layer],
                    );
                }
            } else {
                for (0..pool.config.num_layers_packed) |layer| {
                    try self.kv_storage.writeFullLayerKv(
                        replacement.sequence_id.?,
                        layer,
                        compacted.retained_count,
                        compacted.k_per_layer[layer],
                        compacted.v_per_layer[layer],
                    );
                }
            }
        } else {
            for (0..pool.config.num_layers_packed) |layer| {
                try self.kv_storage.writeFullLayerKv(
                    replacement.sequence_id.?,
                    layer,
                    compacted.retained_count,
                    compacted.k_per_layer[layer],
                    compacted.v_per_layer[layer],
                );
            }
        }

        const before_view = self.kv_metadata.kv_view;
        const before_sequence_id = self.kv_metadata.sequence_id;
        try self.kv_metadata.releaseOwnedSequence(&self.kv_storage);
        self.kv_metadata.logical_blocks.deinit(self.allocator);
        self.kv_metadata = replacement;
        try self.applyKvMutationResult(.{
            .token_count = self.kv_metadata.total_token_count,
            .kv_view = self.kv_metadata.kv_view,
            .compacted = self.kv_metadata.compacted,
            .delta = .{
                .sequence_replaced = before_sequence_id != self.kv_metadata.sequence_id,
                .logical_block_count_before = if (before_view) |view| view.logical_block_count else 0,
                .logical_block_count_after = if (self.kv_metadata.kv_view) |view| view.logical_block_count else 0,
                .position_offset_before = if (before_view) |view| view.position_offset else 0,
                .position_offset_after = self.kv_metadata.position_offset,
                .retained_tokens = compacted.retained_count,
            },
        });
    }

    fn currentTokenCount(self: *const RuntimeContext) usize {
        return self.kv_metadata.total_token_count;
    }

    fn kvView(self: *const RuntimeContext) ?generation.KvView {
        return self.kv_metadata.kv_view;
    }

    fn kvCacheView(_: *const RuntimeContext, kv_view: generation.KvView) contracts.KvCacheView {
        return .{
            .sequence_id = kv_view.sequence_id,
            .pool_id = kv_view.pool_id,
            .logical_block_count = kv_view.logical_block_count,
            .tail_tokens = kv_view.tail_tokens,
            .position_offset = kv_view.position_offset,
            .logical_blocks = kv_view.logical_blocks,
            .kv_storage = kv_view.kv_storage,
        };
    }

    fn validateDecodePosition(self: *const RuntimeContext, position: usize) !void {
        if (self.currentTokenCount() != position) return error.InvalidDecodePosition;
    }

    fn syncDecoderRuntimeStateForView(self: *RuntimeContext, kv_view: generation.KvView) !void {
        const prior = self.raw_span_state orelse return;
        const prior_end = prior.kv_position_offset + prior.kv_tokens;
        const current_end = kv_view.position_offset + kv_view.token_count;
        const regressed = kv_view.position_offset < prior.kv_position_offset or current_end < prior_end;
        if (regressed) {
            try self.invalidateDecoderRuntimeSpanState();
        }
    }

    fn noteDecoderRuntimeState(self: *RuntimeContext, kv_view: generation.KvView) void {
        self.raw_span_state = .{
            .kv_tokens = kv_view.token_count,
            .kv_position_offset = kv_view.position_offset,
        };
    }

    fn noteDecoderRuntimeStateFromCurrentView(self: *RuntimeContext) void {
        if (self.kvView()) |kv_view| self.noteDecoderRuntimeState(kv_view);
    }

    fn applyDecoderRuntimeSpanHints(self: *RuntimeContext, decode_context: *gpt_arch.DecodeContext) void {
        const prior = self.raw_span_state orelse return;
        decode_context.decoder_runtime_resident_kv_sequence_len = prior.kv_tokens;
        decode_context.decoder_runtime_resident_kv_position_offset = prior.kv_position_offset;
    }

    fn makeDecodeContext(
        self: *RuntimeContext,
        seq_len: usize,
        query_seq_len: usize,
        attention_mode: cache_mod.AttentionMode,
    ) gpt_arch.DecodeContext {
        var decode_context: gpt_arch.DecodeContext = undefined;
        if (disablePagedKvDebug()) {
            decode_context = .{
                .attention_mode = .full_recompute,
                .total_sequence_len = seq_len,
                .query_sequence_len = query_seq_len,
                .kv_sequence_len = seq_len,
                .kv_position_offset = 0,
                .moe_runtime = &self.moe_runtime,
            };
        } else {
            const kv_view = self.kvView();
            const resolved_attention_mode: gpt_arch.DecodeContext.AttentionMode = switch (attention_mode) {
                .full_recompute => .full_recompute,
                .paged_prefill => .paged_prefill,
                .paged_decode => .paged_decode,
            };
            decode_context = .{
                .attention_mode = if (kv_view != null) resolved_attention_mode else .full_recompute,
                .total_sequence_len = seq_len,
                .query_sequence_len = query_seq_len,
                .kv_sequence_len = if (kv_view) |view| view.token_count else seq_len,
                .kv_position_offset = if (kv_view) |view| view.position_offset else 0,
                .kv_storage = &self.kv_storage,
                .moe_runtime = &self.moe_runtime,
                .kv_cache = if (kv_view) |view|
                    .{
                        .sequence_id = view.sequence_id,
                        .pool_id = view.pool_id,
                        .logical_block_count = view.logical_block_count,
                        .tail_tokens = view.tail_tokens,
                        .position_offset = view.position_offset,
                        .logical_blocks = view.logical_blocks,
                        .kv_storage = view.kv_storage,
                    }
                else
                    null,
            };
        }
        self.applyDecoderRuntimeSpanHints(&decode_context);
        return decode_context;
    }

    fn preparePrefill(self: *RuntimeContext, seq_len: usize, query_seq_len: usize, attention_mode: cache_mod.AttentionMode) !gpt_arch.DecodeContext {
        if (self.currentTokenCount() == 0) {
            if (seq_len != query_seq_len) return error.UnsupportedShape;
            try self.notePrefill(query_seq_len);
        } else {
            const expected_prior = seq_len - query_seq_len;
            if (self.currentTokenCount() != expected_prior) return error.InvalidPrefillSequence;
            try self.appendPrefillChunk(query_seq_len);
        }
        return self.makeDecodeContext(seq_len, query_seq_len, attention_mode);
    }

    fn beginDecodeStep(self: *RuntimeContext, position: usize, attention_mode: cache_mod.AttentionMode) !struct {
        seq_len: usize,
        decode_context: gpt_arch.DecodeContext,
    } {
        try self.validateDecodePosition(position);
        const seq_len = try self.appendGeneratedToken();
        return .{
            .seq_len = seq_len,
            .decode_context = self.makeDecodeContext(seq_len, 1, attention_mode),
        };
    }

    fn ensureDecoderRuntimePrepared(self: *RuntimeContext) !bool {
        timing_stats.ensure_prepared_calls += 1;
        const started_at = monotonicNowNs();
        defer timing_stats.ensure_prepared_nanos += @intCast(monotonicNowNs() - started_at);
        const kv_view = self.kvView() orelse {
            if (decoderRuntimePrepareTraceRequested()) {
                std.debug.print("prepare-trace: no kv view available before runtime prepare\n", .{});
            }
            return false;
        };
        const sync_started_at = monotonicNowNs();
        try self.syncDecoderRuntimeStateForView(kv_view);
        timing_stats.ensure_prepared_sync_nanos += @intCast(monotonicNowNs() - sync_started_at);
        const configured_layer_count = self.decoderRuntimeConfiguredLayerCount();
        const prepare_started_at = monotonicNowNs();
        const prepare = try self.cb.decoderRuntimePrepareOrReuseFamily(
            self.allocator,
            self.gpt_config,
            kv_view.token_count,
            configured_layer_count,
        );
        if (decoderRuntimePrepareTraceRequested()) {
            std.debug.print(
                "prepare-trace: kv_tokens={d} configured_layers={d} prepared={} fast_hit={} used_greedy={} runtime_ready={}\n",
                .{
                    kv_view.token_count,
                    configured_layer_count,
                    prepare.prepared,
                    prepare.fast_hit,
                    prepare.used_greedy,
                    self.cb.decoderRuntimeReady(),
                },
            );
        }
        if (prepare.fast_hit) {
            timing_stats.ensure_prepared_fast_hits += 1;
        } else if (prepare.used_greedy) {
            timing_stats.ensure_prepared_greedy_nanos += @intCast(monotonicNowNs() - prepare_started_at);
        } else {
            timing_stats.ensure_prepared_family_nanos += @intCast(monotonicNowNs() - prepare_started_at);
        }
        return prepare.prepared;
    }

    fn forwardDecoderRuntimeLastLogits(
        self: *RuntimeContext,
        allocator: std.mem.Allocator,
        token_id: i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !?model_runtime.ModelOutput {
        if (!(try self.ensureDecoderRuntimePrepared())) return null;
        const configured_layer_count = self.decoderRuntimeConfiguredLayerCount();
        const output = if (try metal_runtime.forwardLastLogitsTensorFamily(
            &self.cb,
            allocator,
            self.gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            decode_context,
        )) |logits| model_runtime.ModelOutput{
            .device_logits = logits,
            .device_logits_backend = self.cb,
            .final_logit_softcap = self.gpt_config.final_logit_softcapping,
        } else null;
        if (output != null) {
            self.noteDecoderRuntimeStateFromCurrentView();
        }
        return output;
    }

    fn forwardDecoderRuntimeGreedyToken(
        self: *RuntimeContext,
        allocator: std.mem.Allocator,
        token_id: i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !?i64 {
        if (!(try self.ensureDecoderRuntimePrepared())) return null;
        const configured_layer_count = self.decoderRuntimeConfiguredLayerCount();
        const token = try metal_runtime.forwardGreedyTokenFamily(
            &self.cb,
            allocator,
            self.gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            decode_context,
        );
        if (token != null) {
            self.noteDecoderRuntimeStateFromCurrentView();
        }
        return token;
    }

    fn forwardDecoderRuntimeSampledToken(
        self: *RuntimeContext,
        allocator: std.mem.Allocator,
        token_id: i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        sampling: model_runtime.SamplingConfig,
        token_history: []const i64,
    ) !?i64 {
        if (!(try self.ensureDecoderRuntimePrepared())) return null;
        const configured_layer_count = self.decoderRuntimeConfiguredLayerCount();
        const token = try metal_runtime.forwardSampledTokenFamily(
            &self.cb,
            allocator,
            self.gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            decode_context,
            sampling,
            token_history,
        );
        if (token != null) {
            self.noteDecoderRuntimeStateFromCurrentView();
        }
        return token;
    }
};

const runtime_vtable = model_runtime.ModelRuntime.VTable{
    .capabilities = runtimeCapabilities,
    .prepare = runtimePrepare,
    .prefill = runtimePrefill,
    .decode = runtimeDecode,
    .decode_sample = runtimeDecodeSample,
    .decode_greedy = runtimeDecodeGreedy,
    .deinit = runtimeDeinit,
    .reset = runtimeReset,
    .debug_timing_stats = runtimeDebugTimingStats,
    .reset_debug_timing_stats = runtimeResetDebugTimingStats,
    .print_debug_timing = runtimePrintDebugTiming,
};

const executor_vtable = model_runtime.ModelExecutor.VTable{
    .create_runtime = createRuntime,
    .deinit = executorDeinit,
};

fn envFlag(name: [:0]const u8) bool {
    const value = c_std.getenv(name) orelse return false;
    const slice = std.mem.span(value);
    return slice.len > 0 and !std.mem.eql(u8, slice, "0");
}

fn decoderRuntimePrefillAfterPrepareRequested() bool {
    return envFlag("TERMITE_METAL_PREFILL_DIRECT_AFTER_PREPARE");
}

fn decoderRuntimePrefillTraceRequested() bool {
    return envFlag("TERMITE_METAL_PREFILL_TRACE");
}

fn decoderRuntimePrepareTraceRequested() bool {
    return envFlag("TERMITE_METAL_PREPARE_TRACE");
}

fn disableDeviceKvReserveRequested() bool {
    return envFlag("TERMITE_METAL_DISABLE_DEVICE_KV_RESERVE");
}

pub fn supportsSession(session: backends.Session) bool {
    const gpt_config = session_factory.getGptConfig(session) orelse return false;
    if (!session.backend().usesGpuHostedSession()) return false;
    return metal_runtime.supportsDecoderRuntimeConfig(gpt_config);
}

fn prewarmEmbeddingWeight(cb: *const ops.ComputeBackend, gpt_config: gpt_mod.Config) !void {
    const embed_w = try gpt_arch.getEmbeddingWeight(cb, gpt_config);
    defer cb.free(embed_w);
}

pub fn prewarmSharedDecoderRuntime(
    allocator: std.mem.Allocator,
    session: backends.Session,
    gpt_config: gpt_mod.Config,
) !bool {
    if (!supportsSession(session)) return false;
    if (gemma4_runtime.shouldSkipSharedDecoderPrewarm(gpt_config)) return false;

    var cb = try session_factory.getComputeBackend(session, allocator);
    defer cb.deinit();

    const configured_layer_count = gpt_config.num_hidden_layers;
    const prepare = try cb.decoderRuntimePrepareOrReuseFamily(
        allocator,
        gpt_config,
        0,
        configured_layer_count,
    );
    if (prepare.prepared) {
        try prewarmEmbeddingWeight(&cb, gpt_config);
    }
    return prepare.prepared;
}

pub fn createModelExecutor(
    allocator: std.mem.Allocator,
    session: backends.Session,
    gpt_config: gpt_mod.Config,
    kv_dtype: ?runtime.kv.pool.KvDType,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
) !model_runtime.ModelExecutor {
    if (!supportsSession(session)) return error.UnsupportedCompileBackend;
    const ctx = try allocator.create(ExecutorContext);
    ctx.* = .{
        .allocator = allocator,
        .session = session,
        .gpt_config = gpt_config,
        .kv_dtype = kv_dtype,
        .shared_moe_cache = shared_moe_cache,
    };
    return .{ .ptr = ctx, .vtable = &executor_vtable };
}

fn createRuntime(ctx: *anyopaque, allocator: std.mem.Allocator) !model_runtime.ModelRuntime {
    const exec_ctx: *ExecutorContext = @ptrCast(@alignCast(ctx));
    const runtime_ctx = try RuntimeContext.init(
        allocator,
        exec_ctx.session,
        exec_ctx.gpt_config,
        exec_ctx.kv_dtype,
        exec_ctx.shared_moe_cache,
    );
    return .{ .ptr = runtime_ctx, .vtable = &runtime_vtable };
}

fn executorDeinit(ctx: *anyopaque) void {
    const exec_ctx: *ExecutorContext = @ptrCast(@alignCast(ctx));
    exec_ctx.allocator.destroy(exec_ctx);
}

fn runtimeCapabilities(ctx: *anyopaque) model_runtime.RuntimeCapabilities {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    _ = runtime_ctx;
    // Decode context assembly and hot-path token bookkeeping are executor-owned
    // now, but KV mutation/storage still depends on the host-backed block
    // manager and paged cache metadata. Keep this at runtime_owned_host_cache
    // until that metadata and mutation policy move fully behind the backend.
    return .{
        .supports_decode = true,
        .supports_sample_decode = true,
        .supports_greedy_decode = true,
        .state_ownership = .runtime_owned_host_cache,
    };
}

fn runtimePrepare(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.PrepareRequest,
) !bool {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    if (!runtime_ctx.decoderRuntimeExecutorEnabled()) return false;

    timing_stats.runtime_prepare_calls += 1;
    const started_at = monotonicNowNs();
    defer timing_stats.runtime_prepare_nanos += @intCast(monotonicNowNs() - started_at);

    const configured_layer_count = runtime_ctx.decoderRuntimeConfiguredLayerCount();
    const prepare_started_at = monotonicNowNs();
    const prepare = try runtime_ctx.cb.decoderRuntimePrepareOrReuseFamily(
        allocator,
        runtime_ctx.gpt_config,
        request.kv_tokens_hint,
        configured_layer_count,
    );
    if (prepare.fast_hit) {
        timing_stats.runtime_prepare_fast_hits += 1;
    } else if (prepare.used_greedy) {
        timing_stats.runtime_prepare_greedy_nanos += @intCast(monotonicNowNs() - prepare_started_at);
    } else {
        timing_stats.runtime_prepare_family_nanos += @intCast(monotonicNowNs() - prepare_started_at);
    }
    if (prepare.prepared) {
        prewarmEmbeddingWeight(&runtime_ctx.cb, runtime_ctx.gpt_config) catch {};
        _ = decoder_gated_runtime.preplanPrefillFrame(
            &runtime_ctx.cb,
            allocator,
            runtime_ctx.gpt_config,
            configured_layer_count,
            request.kv_tokens_hint,
        ) catch false;
        _ = decoder_gated_runtime.prewarmPleTokenEmbedding(
            &runtime_ctx.cb,
            runtime_ctx.gpt_config,
        ) catch false;
    }
    return prepare.prepared;
}

fn runtimeReset(ctx: *anyopaque) !void {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    try runtime_ctx.resetState();
}

fn runtimeDebugTimingStats(ctx: *anyopaque) model_runtime.RuntimeDebugTimingStats {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    var stats = getTimingStats();
    stats.backend = runtime_ctx.cb.debugTimingSnapshot();
    stats.decoder_runtime_ready = runtime_ctx.cb.decoderRuntimeReady();
    stats.decoder_runtime_absolute_embeddings_prepared = runtime_ctx.cb.decoderRuntimeAbsoluteEmbeddingsPrepared();
    return stats;
}

fn runtimeResetDebugTimingStats(ctx: *anyopaque) void {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    resetTimingStats();
    runtime_ctx.cb.resetDebugTimingStats();
    decoder_rms_runtime.resetTimingStats();
    decoder_gated_runtime.resetTimingStats();
    decoder_bitnet_runtime.resetTimingStats();
    decoder_tail_runtime.resetTimingStats();
}

fn runtimePrintDebugTiming(ctx: *anyopaque) void {
    printRuntimeDebugTimingStats(runtimeDebugTimingStats(ctx), true);
}

fn runtimeDeinit(ctx: *anyopaque) void {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    runtime_ctx.deinit();
}

fn runtimePrefill(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.PrefillRequest,
) !model_runtime.ModelOutput {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    if (request.input_ids.len == 0 or request.query_seq_len == 0) return error.EmptyPrompt;
    if (request.input_ids.len != request.query_seq_len) return error.UnsupportedShape;
    if (request.query_seq_len > request.seq_len) return error.UnsupportedShape;

    timing_stats.prefill_calls += 1;
    const prepare_started_at = monotonicNowNs();
    const decode_context = try runtime_ctx.preparePrefill(request.seq_len, request.query_seq_len, request.attention_mode);
    timing_stats.prefill_prepare_nanos += @intCast(monotonicNowNs() - prepare_started_at);
    const decoder_runtime_ready = if (decoderRuntimePrefillAfterPrepareRequested())
        runtime_ctx.decoderRuntimeExecutorEnabled() and (try runtime_ctx.ensureDecoderRuntimePrepared())
    else blk: {
        _ = runtime_ctx.ensureDecoderRuntimePrepared() catch false;
        break :blk runtime_ctx.decoderRuntimeExecutorEnabled();
    };
    if (decoderRuntimePrefillTraceRequested()) {
        std.debug.print("prefill-trace: runtimePrefill qlen={d} seq={d} ready={}\n", .{ request.query_seq_len, request.seq_len, decoder_runtime_ready });
    }
    if (decoder_runtime_ready and request.query_seq_len == 1) {
        const token_id = request.input_ids[request.input_ids.len - 1];
        const direct_started_at = monotonicNowNs();
        if (try runtime_ctx.forwardDecoderRuntimeLastLogits(allocator, token_id, request.seq_len, &decode_context)) |output| {
            timing_stats.prefill_direct_last_logits_nanos += @intCast(monotonicNowNs() - direct_started_at);
            return output;
        }
        timing_stats.prefill_direct_last_logits_nanos += @intCast(monotonicNowNs() - direct_started_at);
    }
    if (decoder_runtime_ready) {
        if (decoderRuntimePrefillTraceRequested()) {
            std.debug.print("prefill-trace: runtimePrefill entering direct family path\n", .{});
        }
        const configured_layer_count = runtime_ctx.decoderRuntimeConfiguredLayerCount();
        const timing_before = runtime_ctx.cb.directFamilyTimingSnapshot();
        const direct_started_at = monotonicNowNs();
        if (try metal_runtime.forwardPrefillLastPreparedTailFamily(
            &runtime_ctx.cb,
            allocator,
            runtime_ctx.gpt_config,
            configured_layer_count,
            request.input_ids,
            request.seq_len,
            &decode_context,
        )) |tail| {
            const timing_after = runtime_ctx.cb.directFamilyTimingSnapshot();
            const delta = timing_after.delta(timing_before);
            timing_stats.prefill_direct_family_nanos += @intCast(monotonicNowNs() - direct_started_at);
            timing_stats.prefill_direct_family_project_nanos += delta.project_nanos;
            timing_stats.prefill_direct_family_span_prep_nanos += delta.span_prep_nanos;
            timing_stats.prefill_direct_family_quant_attn_nanos += delta.quant_attn_nanos;
            timing_stats.prefill_direct_family_block_apply_nanos += delta.block_apply_nanos;
            timing_stats.prefill_direct_family_frame_wait_nanos += delta.frame_wait_nanos;
            timing_stats.prefill_direct_family_frame_gpu_nanos += delta.frame_gpu_nanos;
            runtime_ctx.noteDecoderRuntimeStateFromCurrentView();
            return .{ .prepared_tail = .{
                .final_hidden = tail.final_hidden,
                .backend = runtime_ctx.cb,
                .norm = .rms,
                .norm_slot = tail.final_norm_slot,
                .lm_head_slot = tail.final_lm_head_slot,
                .hidden_size = tail.hidden_size,
                .vocab_size = tail.vocab_size,
                .eps = tail.norm_eps,
                .final_logit_softcap = runtime_ctx.gpt_config.final_logit_softcapping,
            } };
        }
        const timing_after = runtime_ctx.cb.directFamilyTimingSnapshot();
        const delta = timing_after.delta(timing_before);
        timing_stats.prefill_direct_family_nanos += @intCast(monotonicNowNs() - direct_started_at);
        timing_stats.prefill_direct_family_project_nanos += delta.project_nanos;
        timing_stats.prefill_direct_family_span_prep_nanos += delta.span_prep_nanos;
        timing_stats.prefill_direct_family_quant_attn_nanos += delta.quant_attn_nanos;
        timing_stats.prefill_direct_family_block_apply_nanos += delta.block_apply_nanos;
    }
    const fallback_started_at = monotonicNowNs();
    const logits = try forwardLastLogits(
        runtime_ctx,
        allocator,
        request.input_ids,
        request.seq_len,
        request.query_seq_len,
        &decode_context,
    );
    timing_stats.prefill_fallback_logits_nanos += @intCast(monotonicNowNs() - fallback_started_at);
    return .{
        .logits = logits,
    };
}

fn runtimeDecode(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.DecodeRequest,
) !model_runtime.ModelOutput {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    const step = try runtime_ctx.beginDecodeStep(request.position, request.attention_mode);

    if (runtime_ctx.decoderRuntimeExecutorEnabled()) {
        if (try runtime_ctx.forwardDecoderRuntimeLastLogits(allocator, request.token_id, step.seq_len, &step.decode_context)) |output| {
            return output;
        }
    }

    const input_ids = [_]i64{request.token_id};
    return .{
        .logits = try forwardLastLogits(runtime_ctx, allocator, input_ids[0..], step.seq_len, 1, &step.decode_context),
    };
}

fn runtimeDecodeSample(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.SampledDecodeRequest,
) !model_runtime.SampledDecodeOutput {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    if (request.sampling.isPureGreedy()) {
        const greedy = try runtimeDecodeGreedy(ctx, allocator, request.decode);
        return .{ .token_id = greedy.token_id };
    }

    timing_stats.decode_sample_calls += 1;
    const begin_started_at = monotonicNowNs();
    const step = try runtime_ctx.beginDecodeStep(request.decode.position, request.decode.attention_mode);
    timing_stats.decode_begin_step_nanos += @intCast(monotonicNowNs() - begin_started_at);
    if (runtime_ctx.decoderRuntimeExecutorEnabled()) {
        const direct_started_at = monotonicNowNs();
        if (try runtime_ctx.forwardDecoderRuntimeSampledToken(
            allocator,
            request.decode.token_id,
            step.seq_len,
            &step.decode_context,
            request.sampling,
            request.token_history,
        )) |token_id| {
            timing_stats.decode_sample_direct_nanos += @intCast(monotonicNowNs() - direct_started_at);
            return .{ .token_id = token_id };
        }
        timing_stats.decode_sample_direct_nanos += @intCast(monotonicNowNs() - direct_started_at);
    }

    var output: model_runtime.ModelOutput = undefined;
    const fallback_started_at = monotonicNowNs();
    if (runtime_ctx.decoderRuntimeExecutorEnabled()) {
        if (try runtime_ctx.forwardDecoderRuntimeLastLogits(allocator, request.decode.token_id, step.seq_len, &step.decode_context)) |raw_output| {
            output = raw_output;
        } else {
            const input_ids = [_]i64{request.decode.token_id};
            output = .{
                .logits = try forwardLastLogits(runtime_ctx, allocator, input_ids[0..], step.seq_len, 1, &step.decode_context),
            };
        }
    } else {
        const input_ids = [_]i64{request.decode.token_id};
        output = .{
            .logits = try forwardLastLogits(runtime_ctx, allocator, input_ids[0..], step.seq_len, 1, &step.decode_context),
        };
    }
    timing_stats.decode_sample_fallback_nanos += @intCast(monotonicNowNs() - fallback_started_at);
    defer output.deinit(allocator);
    if (output.final_logit_softcap <= 0.0) {
        if (output.device_logits) |device_logits| {
            const backend = output.device_logits_backend orelse return error.InvalidModelOutput;
            if (try backend.sampleLastRow(&.{
                .tensor = device_logits,
                .rows = 1,
                .dim = runtime_ctx.gpt_config.vocab_size,
                .temperature = request.sampling.temperature,
                .top_k = @intCast(@max(request.sampling.top_k, 0)),
                .top_p = request.sampling.top_p,
                .min_p = request.sampling.min_p,
                .repetition_penalty = request.sampling.repetition_penalty,
                .frequency_penalty = request.sampling.frequency_penalty,
                .presence_penalty = request.sampling.presence_penalty,
                .token_history = request.token_history,
            })) |token_id| {
                return .{ .token_id = token_id };
            }
        }
    }
    return .{
        .token_id = @intCast(model_runtime.sampleTokenFromLogits(
            allocator,
            try output.hostLogits(allocator),
            request.sampling,
            request.token_history,
        )),
    };
}

fn runtimeDecodeGreedy(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.DecodeRequest,
) !model_runtime.GreedyDecodeOutput {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    timing_stats.decode_greedy_calls += 1;
    const begin_started_at = monotonicNowNs();
    const step = try runtime_ctx.beginDecodeStep(request.position, request.attention_mode);
    timing_stats.decode_begin_step_nanos += @intCast(monotonicNowNs() - begin_started_at);

    const direct_started_at = monotonicNowNs();
    if (try runtime_ctx.forwardDecoderRuntimeGreedyToken(allocator, request.token_id, step.seq_len, &step.decode_context)) |token_id| {
        timing_stats.decode_greedy_direct_nanos += @intCast(monotonicNowNs() - direct_started_at);
        return .{ .token_id = token_id };
    }
    if (try runtime_ctx.forwardDecoderRuntimeLastLogits(allocator, request.token_id, step.seq_len, &step.decode_context)) |output| {
        var owned_output = output;
        defer owned_output.deinit(allocator);
        if (owned_output.device_logits) |device_logits| {
            const backend = owned_output.device_logits_backend orelse return error.InvalidModelOutput;
            if (try backend.argmaxLastRow(device_logits, 1, runtime_ctx.gpt_config.vocab_size)) |token_id| {
                timing_stats.decode_greedy_direct_nanos += @intCast(monotonicNowNs() - direct_started_at);
                return .{ .token_id = @intCast(token_id) };
            }
        }
        const logits = try owned_output.hostLogits(allocator);
        var best_idx: usize = 0;
        for (logits[1..], 1..) |value, idx| {
            if (value > logits[best_idx]) best_idx = idx;
        }
        timing_stats.decode_greedy_direct_nanos += @intCast(monotonicNowNs() - direct_started_at);
        return .{ .token_id = @intCast(best_idx) };
    }
    timing_stats.decode_greedy_direct_nanos += @intCast(monotonicNowNs() - direct_started_at);

    const input_ids = [_]i64{request.token_id};
    const fallback_started_at = monotonicNowNs();
    const token_id = try gpt_arch.forwardGreedyLastToken(
        &runtime_ctx.cb,
        allocator,
        runtime_ctx.gpt_config,
        input_ids[0..],
        1,
        step.seq_len,
        &step.decode_context,
    );
    timing_stats.decode_greedy_fallback_nanos += @intCast(monotonicNowNs() - fallback_started_at);
    return .{
        .token_id = @intCast(token_id),
    };
}

fn forwardLastLogits(
    runtime_ctx: *RuntimeContext,
    allocator: std.mem.Allocator,
    input_ids: []const i64,
    seq_len: usize,
    query_seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) ![]f32 {
    const logits = try gpt_arch.forward(
        &runtime_ctx.cb,
        allocator,
        runtime_ctx.gpt_config,
        input_ids,
        1,
        seq_len,
        decode_context,
    );
    defer allocator.free(logits);
    const vocab_size: usize = @intCast(runtime_ctx.gpt_config.vocab_size);
    const last_pos_offset = (query_seq_len - 1) * vocab_size;
    return allocator.dupe(f32, logits[last_pos_offset..][0..vocab_size]);
}
