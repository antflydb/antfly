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
const decoder_bitnet_runtime = if (build_options.enable_mlx) @import("backends/decoder_bitnet_runtime.zig") else struct {};
const decoder_gated_runtime = if (build_options.enable_mlx) @import("backends/decoder_gated_runtime.zig") else struct {};
const decoder_rms_runtime = if (build_options.enable_mlx) @import("backends/decoder_rms_runtime.zig") else struct {};
const ops = @import("ops/ops.zig");
const mlx_compute = if (build_options.enable_mlx) @import("ops/mlx_compute.zig") else struct {};
const mlx_quant = if (build_options.enable_mlx) @import("backends/mlx_quant.zig") else struct {};
const gpt_arch = @import("architectures/gpt.zig");

pub fn fallbackMlxTimingSnapshot() ops.BackendDebugTimingSnapshot {
    if (!build_options.enable_mlx) return .{};
    return .{
        .native_quant_null = false,
        .provider = mlx_quant.getTimingStats(),
        .quant = mlx_compute.getQuantExecutionTimingStats(),
    };
}

pub fn resetLiveMlxTimingStats(cb: *const ops.ComputeBackend) void {
    if (!build_options.enable_mlx) return;
    cb.resetDebugTimingStats();
    decoder_rms_runtime.resetTimingStats();
    decoder_gated_runtime.resetTimingStats();
    decoder_bitnet_runtime.resetTimingStats();
}

pub fn printBackendTimingDetails(
    backend_kind: ops.BackendKind,
    backend_stats: ops.BackendDebugTimingSnapshot,
    decoder_runtime_runtime_ready: bool,
    decoder_runtime_embeddings_prepared: bool,
) void {
    const prefix = switch (backend_kind) {
        .metal => "metal",
        .mlx => "mlx",
        else => "backend",
    };
    const provider_stats = backend_stats.provider;
    const quant_stats = backend_stats.quant;
    const gpt_stats = gpt_arch.getDebugTimingStats();
    const native_quant_null = backend_stats.native_quant_null;
    std.debug.print(
        "{s}_quant_counts: native_quant_null={} decoder_runtime_runtime_ready={} decoder_runtime_embeddings_prepared={} provider_calls={d} provider_pair_calls={d} provider_grouped_calls={d} prepare_calls={d} prepare_cache_hits={d} decoder_runtime_prepare_calls={d} decoder_runtime_embed_calls={d} decoder_runtime_prepare_layer_norm_calls={d} decoder_runtime_apply_layer_norm_calls={d} decoder_runtime_prepare_linear_calls={d} decoder_runtime_prepare_linear_ready_fail={d} decoder_runtime_prepare_linear_bias_fail={d} decoder_runtime_prepare_linear_packed_fail={d} decoder_runtime_prepare_linear_weight_dtype_fail={d} decoder_runtime_prepare_linear_weight_ndim_fail={d} decoder_runtime_prepare_linear_weight_shape_fail={d} decoder_runtime_apply_linear_calls={d} decoder_runtime_apply_activation_calls={d} decoder_runtime_apply_add_calls={d} decoder_runtime_attention_span_calls={d} slice_calls={d} prepared_view_cache_hits={d} prepared_view_cache_misses={d} prepared_view_owned_materializations={d} device_native_moe_grouped_calls={d} device_native_packed_backend_weight_calls={d} device_native_packed_prepared_view_calls={d}\n",
        .{
            prefix,
            native_quant_null,
            decoder_runtime_runtime_ready,
            decoder_runtime_embeddings_prepared,
            provider_stats.calls,
            provider_stats.pair_calls,
            provider_stats.grouped_calls,
            provider_stats.prepare_calls,
            provider_stats.prepare_cache_hits,
            provider_stats.decoder_runtime_prepare_calls,
            provider_stats.decoder_runtime_embed_calls,
            provider_stats.decoder_runtime_prepare_layer_norm_calls,
            provider_stats.decoder_runtime_apply_layer_norm_calls,
            provider_stats.decoder_runtime_prepare_linear_calls,
            provider_stats.decoder_runtime_prepare_linear_runtime_not_ready,
            provider_stats.decoder_runtime_prepare_linear_bias_shape_failures,
            provider_stats.decoder_runtime_prepare_linear_packed_expert_failures,
            provider_stats.decoder_runtime_prepare_linear_weight_dtype_failures,
            provider_stats.decoder_runtime_prepare_linear_weight_ndim_failures,
            provider_stats.decoder_runtime_prepare_linear_weight_shape_failures,
            provider_stats.decoder_runtime_apply_linear_calls,
            provider_stats.decoder_runtime_apply_activation_calls,
            provider_stats.decoder_runtime_apply_add_calls,
            provider_stats.decoder_runtime_attention_span_calls,
            provider_stats.slice_calls,
            provider_stats.prepared_view_cache_hits,
            provider_stats.prepared_view_cache_misses,
            provider_stats.prepared_view_owned_materializations,
            quant_stats.device_native_moe_grouped_calls,
            quant_stats.device_native_packed_backend_weight_calls,
            quant_stats.device_native_packed_prepared_view_calls,
        },
    );
    std.debug.print(
        "{s}_quant_pair_path: direct_ok={d} direct_fail={d} backend_fallbacks={d} non_i2s={d}\n",
        .{
            prefix,
            provider_stats.decoder_runtime_pair_direct_successes,
            provider_stats.decoder_runtime_pair_direct_failures,
            provider_stats.decoder_runtime_pair_backend_fallbacks,
            provider_stats.decoder_runtime_pair_non_i2s,
        },
    );
    std.debug.print(
        "{s}_quant_linear_nulls: prepare_provider=0x{x} apply_provider=0x{x} apply_not_prepared={d} first_apply_slot={d} apply_dim={d} apply_shape={d} apply_quant_storage={d} apply_quant_weight={d} apply_dense_cache={d} pair_not_prepared={d} first_pair_slots={d}/{d} pair_dim={d} pair_shape={d} pair_dense_direct_ok={d} pair_dense_direct_fail={d} pair_delegate={d}\n",
        .{
            prefix,
            provider_stats.decoder_runtime_prepare_linear_first_provider_ptr,
            provider_stats.decoder_runtime_apply_linear_first_provider_ptr,
            provider_stats.decoder_runtime_apply_linear_not_prepared,
            provider_stats.decoder_runtime_apply_linear_first_not_prepared_slot,
            provider_stats.decoder_runtime_apply_linear_dim_mismatch,
            provider_stats.decoder_runtime_apply_linear_input_shape_failures,
            provider_stats.decoder_runtime_apply_linear_quantized_storage_nulls,
            provider_stats.decoder_runtime_apply_linear_quantized_weight_nulls,
            provider_stats.decoder_runtime_apply_linear_dense_cache_nulls,
            provider_stats.decoder_runtime_apply_linear_pair_not_prepared,
            provider_stats.decoder_runtime_apply_linear_pair_first_not_prepared_slot_a,
            provider_stats.decoder_runtime_apply_linear_pair_first_not_prepared_slot_b,
            provider_stats.decoder_runtime_apply_linear_pair_dim_mismatch,
            provider_stats.decoder_runtime_apply_linear_pair_input_shape_failures,
            provider_stats.decoder_runtime_apply_linear_pair_dense_direct_successes,
            provider_stats.decoder_runtime_apply_linear_pair_dense_direct_failures,
            provider_stats.decoder_runtime_apply_linear_pair_dense_delegate_failures,
        },
    );
    std.debug.print(
        "{s}_quant_span_cache: cold_miss_nulls={d} seed_calls={d} seed_successes={d} decode_cache_hits={d} decode_cache_misses={d} same_span_hits={d} append_hits={d} offset_regressions={d} prefix_token_mismatches={d} prefix_mismatch_resets={d} reset_rebuilds={d} prefill_source=0x{x} decode_source=0x{x}\n",
        .{
            prefix,
            provider_stats.gathered_span_cold_miss_nulls,
            provider_stats.gathered_span_seed_calls,
            provider_stats.gathered_span_seed_successes,
            provider_stats.gathered_span_decode_cache_hits,
            provider_stats.gathered_span_decode_cache_misses,
            provider_stats.gathered_span_same_span_hits,
            provider_stats.gathered_span_append_hits,
            provider_stats.gathered_span_offset_regressions,
            provider_stats.gathered_span_prefix_token_mismatches,
            provider_stats.gathered_span_prefix_mismatch_resets,
            provider_stats.gathered_span_reset_rebuilds,
            provider_stats.gathered_span_first_prefill_source_ptr,
            provider_stats.gathered_span_first_decode_source_ptr,
        },
    );
    std.debug.print(
        "{s}_quant_prepare_ms: embed={d} layer_norm={d} linear={d}\n",
        .{
            prefix,
            @divTrunc(provider_stats.decoder_runtime_prepare_embed_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.decoder_runtime_prepare_layer_norm_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.decoder_runtime_prepare_linear_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_quant_grouped_ms: q5_calls={d} q5_expand={d} q5_down={d} q6_calls={d} q6_expand={d} q6_down={d} weight_setup={d} ids_setup={d} apply={d}\n",
        .{
            prefix,
            provider_stats.grouped_q5_k_calls,
            provider_stats.grouped_q5_k_expand_calls,
            provider_stats.grouped_q5_k_down_calls,
            provider_stats.grouped_q6_k_calls,
            provider_stats.grouped_q6_k_expand_calls,
            provider_stats.grouped_q6_k_down_calls,
            @divTrunc(provider_stats.grouped_weight_setup_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.grouped_ids_setup_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.grouped_apply_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_quant_block_ms: dense_calls={d} gated_calls={d} project={d} span_prep={d} encode={d} apply={d}\n",
        .{
            prefix,
            provider_stats.compressed_block_dense_calls,
            provider_stats.compressed_block_gated_calls,
            @divTrunc(provider_stats.compressed_block_project_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_span_prep_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_encode_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_apply_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_quant_block_apply_ms: replace_span={d} attention_span={d} attention_prefix={d} gated_ffn={d} command_wait={d} gpu={d}\n",
        .{
            prefix,
            @divTrunc(provider_stats.compressed_block_replace_span_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_attention_span_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_attention_prefix_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_gated_ffn_residual_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_command_wait_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_block_gpu_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_quant_block_us: quant_attn_calls={d} quant_attn={d} quant_ffn={d} gated_quant_branch={d} gated_quant_attn_nulls={d} gated_quant_attn_prefill_nulls={d} gated_quant_attn_decode_nulls={d} gated_quant_norm_nulls={d} compressed_f32_reroutes={d} active_bootstrap_misses={d} gated_direct_ok={d} gated_direct_runtime_fail={d} gated_direct_fail_replace={d} gated_direct_fail_attn={d} gated_direct_fail_prefix={d} gated_direct_fail_ffn={d} gated_direct_first_code={d} gated_ffn_direct_ok={d} gated_ffn_direct_fallbacks={d} gated_ffn_backend_fallbacks={d} gated_ffn_backend_mixed_kind={d} gated_ffn_backend_unsupported_kind={d} gated_ffn_type_mismatch={d} gated_ffn_unsupported={d} gated_ffn_runtime_fail={d}\n",
        .{
            prefix,
            provider_stats.compressed_block_quantized_attention_calls,
            @divTrunc(provider_stats.compressed_block_quantized_attention_nanos, std.time.ns_per_us),
            @divTrunc(provider_stats.compressed_block_quantized_ffn_nanos, std.time.ns_per_us),
            provider_stats.compressed_block_gated_quantized_branch_calls,
            provider_stats.compressed_block_gated_quantized_attention_nulls,
            provider_stats.compressed_block_gated_quantized_attention_prefill_nulls,
            provider_stats.compressed_block_gated_quantized_attention_decode_nulls,
            provider_stats.compressed_block_gated_quantized_norm_nulls,
            provider_stats.compressed_block_active_frame_f32_reroutes,
            provider_stats.compressed_block_active_frame_bootstrap_misses,
            provider_stats.compressed_block_gated_direct_successes,
            provider_stats.compressed_block_gated_direct_runtime_failures,
            provider_stats.compressed_block_gated_direct_fail_replace_span,
            provider_stats.compressed_block_gated_direct_fail_attention_span,
            provider_stats.compressed_block_gated_direct_fail_attention_prefix,
            provider_stats.compressed_block_gated_direct_fail_gated_ffn,
            provider_stats.compressed_block_gated_direct_first_failure_code,
            provider_stats.quantized_gated_ffn_direct_successes,
            provider_stats.quantized_gated_ffn_direct_fallbacks,
            provider_stats.quantized_gated_ffn_backend_fallbacks,
            provider_stats.quantized_gated_ffn_backend_mixed_kind_fallbacks,
            provider_stats.quantized_gated_ffn_backend_unsupported_kind_fallbacks,
            provider_stats.quantized_gated_ffn_type_mismatches,
            provider_stats.quantized_gated_ffn_unsupported_types,
            provider_stats.quantized_gated_ffn_runtime_failures,
        },
    );
    std.debug.print(
        "{s}_quant_attn_residual_counts: attn_fused_ok={d} attn_update_fail={d} attn_span_fail={d} attn_post_linear_ok={d} attn_post_linear_fail={d} multi_row_attn_calls={d} multi_row_attn_ok={d}\n",
        .{
            prefix,
            provider_stats.compressed_attention_residual_fused_successes,
            provider_stats.compressed_attention_residual_update_span_failures,
            provider_stats.compressed_attention_residual_attention_span_failures,
            provider_stats.compressed_attention_residual_post_linear_successes,
            provider_stats.compressed_attention_residual_post_linear_failures,
            provider_stats.compressed_attention_residual_multi_row_calls,
            provider_stats.compressed_attention_residual_multi_row_successes,
        },
    );
    std.debug.print(
        "{s}_quant_attn_residual_ms: input_eval={d} raw_runtime={d}\n",
        .{
            prefix,
            @divTrunc(provider_stats.compressed_attention_residual_input_eval_nanos, std.time.ns_per_ms),
            @divTrunc(provider_stats.compressed_attention_residual_raw_runtime_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_quant_gated_ffn_us: pair={d} activation_mul={d} post_gate_norm={d} down={d} add={d}\n",
        .{
            prefix,
            @divTrunc(provider_stats.quantized_gated_pair_nanos, std.time.ns_per_us),
            @divTrunc(provider_stats.quantized_gated_activation_multiply_nanos, std.time.ns_per_us),
            @divTrunc(provider_stats.quantized_gated_post_gate_norm_nanos, std.time.ns_per_us),
            @divTrunc(provider_stats.quantized_gated_down_nanos, std.time.ns_per_us),
            @divTrunc(provider_stats.quantized_gated_add_nanos, std.time.ns_per_us),
        },
    );
    std.debug.print(
        "{s}_quant_prefetch: packed_backend_prefetch_attempts={d} packed_backend_prefetch_successes={d} packed_backend_prefetch_denials={d}\n",
        .{
            prefix,
            quant_stats.packed_backend_prefetch_attempts,
            quant_stats.packed_backend_prefetch_successes,
            quant_stats.packed_backend_prefetch_denials,
        },
    );
    std.debug.print(
        "{s}_quant_exec_classes: attn_device_native={d} attn_backend_dense={d} attn_wrapper={d} attn_dense_lazy={d} router_device_native={d} router_backend_dense={d} router_wrapper={d} lm_head_device_native={d} lm_head_backend_dense={d} lm_head_wrapper={d}\n",
        .{
            prefix,
            quant_stats.attn_quant_device_native_calls,
            quant_stats.attn_quant_backend_dense_calls,
            quant_stats.attn_quant_wrapper_calls,
            quant_stats.attn_dense_lazy_calls,
            quant_stats.router_quant_device_native_calls,
            quant_stats.router_quant_backend_dense_calls,
            quant_stats.router_quant_wrapper_calls,
            quant_stats.lm_head_quant_device_native_calls,
            quant_stats.lm_head_quant_backend_dense_calls,
            quant_stats.lm_head_quant_wrapper_calls,
        },
    );
    std.debug.print(
        "{s}_paged_decode: calls={d} blocks={d} update_kv={d} cache_lookup={d} block_setup={d} mask={d} block_apply={d} total={d}\n",
        .{
            prefix,
            quant_stats.paged_decode_calls,
            quant_stats.paged_decode_blocks,
            @divTrunc(quant_stats.paged_update_kv_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.paged_cache_lookup_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.paged_block_setup_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.paged_mask_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.paged_block_apply_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.paged_decode_total_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_block_fastpath: dense_attempts={d} dense_no_blocks={d} dense_unsupported_format={d} gated_attempts={d} gated_no_blocks={d} gated_unsupported_format={d}\n",
        .{
            prefix,
            quant_stats.dense_block_fast_attempts,
            quant_stats.dense_block_fast_no_blocks,
            quant_stats.dense_block_fast_unsupported_format,
            quant_stats.gated_block_fast_attempts,
            quant_stats.gated_block_fast_no_blocks,
            quant_stats.gated_block_fast_unsupported_format,
        },
    );
    std.debug.print(
        "{s}_gated_block_ms: input_project={d} input_attention={d} input_ffn_norm={d} input_ffn={d} qkv_attention={d} qkv_ffn_norm={d} qkv_ffn={d}\n",
        .{
            prefix,
            @divTrunc(quant_stats.gated_input_projection_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.gated_input_attention_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.gated_input_ffn_norm_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.gated_input_ffn_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.gated_qkv_attention_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.gated_qkv_ffn_norm_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.gated_qkv_ffn_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_attention_residual_ms: core={d} post_linear_fast={d} post_linear_fallback={d}\n",
        .{
            prefix,
            @divTrunc(quant_stats.attention_residual_core_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.attention_residual_post_linear_fast_nanos, std.time.ns_per_ms),
            @divTrunc(quant_stats.attention_residual_post_linear_fallback_nanos, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "{s}_gated_block_failures: input_project={d} input_attn={d} input_ffn_norm={d} input_ffn={d} qkv_attn={d} qkv_ffn_norm={d} qkv_ffn={d}\n",
        .{
            prefix,
            quant_stats.gated_input_project_failures,
            quant_stats.gated_input_attention_failures,
            quant_stats.gated_input_ffn_norm_failures,
            quant_stats.gated_input_ffn_failures,
            quant_stats.gated_qkv_attention_failures,
            quant_stats.gated_qkv_ffn_norm_failures,
            quant_stats.gated_qkv_ffn_failures,
        },
    );
    std.debug.print(
        "gpt_pair_counts: attn_project_pair_calls={d} ffn_project_pair_calls={d}\n",
        .{
            gpt_stats.attention_project_pair_calls,
            gpt_stats.ffn_project_pair_calls,
        },
    );
}
