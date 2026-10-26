// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical scalar kernel layouts for C, runtime MSL and standalone lowering.
//! Only declaration text is generated; dispatch and arithmetic stay native.
const std = @import("std");
const Emitter = @import("quant_kernel_native_source.zig").Emitter;
const Scalar = enum { uint32, int32, float32 };
const Group = union(Scalar) { uint32: []const u8, int32: []const u8, float32: []const u8 };
const Layout = struct { name: []const u8, groups: []const Group, note: []const u8 = "" };
const Spelling = struct { tag: []const u8, c: []const u8, metal: []const u8 };
const spellings = [_]Spelling{
    .{ .tag = "u32", .c = "uint32_t", .metal = "uint" },
    .{ .tag = "i32", .c = "int32_t", .metal = "int" },
    .{ .tag = "f32", .c = "float", .metal = "float" },
};
fn spelling(group: Group) Spelling {
    return spellings[@backingInt(std.meta.activeTag(group))];
}
fn names(group: Group) []const u8 {
    return switch (group) {
        inline else => |value| value,
    };
}
fn layout(comptime name: []const u8) Layout {
    inline for (layouts) |value| if (comptime std.mem.eql(u8, name, value.name)) return value;
    @compileError("Unknown Metal parameter layout");
}
pub fn fields(comptime name: []const u8) []const u8 {
    @setEvalBranchQuota(1_000_000);
    return comptime blk: {
        var result: []const u8 = "";
        for (layout(name).groups) |group| {
            var words = std.mem.tokenizeScalar(u8, names(group), ' ');
            while (words.next()) |field| result = result ++ spelling(group).metal ++ " " ++ field ++ "; ";
        }
        break :blk std.mem.trimEnd(u8, result, " ");
    };
}
pub fn declaration(comptime name: []const u8) []const u8 {
    return comptime "struct termite_metal_" ++ name ++ "_params { " ++ fields(name) ++ " };";
}
pub const header_path = "src/backends/metal_param_abi.h";
pub fn renderHeader(allocator: std.mem.Allocator) ![]u8 {
    var e = Emitter{ .allocator = allocator };
    errdefer e.bytes.deinit(allocator);
    try e.raw("// Copyright 2026 Antfly, Inc.\n// SPDX-License-Identifier: Apache-2.0\n#pragma once\n\n// quant-kernel-codegen:begin scalar Metal ABI (do not edit)\n");
    for (spellings) |s| try e.add("#define TERMITE_METAL_ABI_C_{s} {s}\n", .{ s.tag, s.c });
    for (spellings) |s| try e.add("#define TERMITE_METAL_ABI_MSL_{s} \"{s}\"\n", .{ s.tag, s.metal });
    try e.raw(factory);
    for (layouts) |record| {
        if (record.note.len != 0) try e.add("{s}\n", .{record.note});
        try e.add("#define TERMITE_METAL_ABI_FIELDS_{s}(F) \\\n    ", .{record.name});
        var column: usize = 4;
        for (record.groups) |group| {
            var words = std.mem.tokenizeScalar(u8, names(group), ' ');
            while (words.next()) |field| {
                const item = try std.fmt.allocPrint(allocator, "F({s}, {s})", .{ spelling(group).tag, field });
                defer allocator.free(item);
                if (column > 4 and column + 1 + item.len > 112) {
                    try e.raw(" \\\n    ");
                    column = 4;
                } else if (column > 4) {
                    try e.raw(" ");
                    column += 1;
                }
                try e.raw(item);
                column += item.len;
            }
        }
        try e.raw("\n\n");
    }
    try e.raw("// quant-kernel-codegen:end scalar Metal ABI\n");
    return e.bytes.toOwnedSlice(allocator);
}
const factory =
    \\#define TERMITE_METAL_ABI_FIELD_C(Type, Field) TERMITE_METAL_ABI_C_##Type Field;
    \\#define TERMITE_METAL_ABI_FIELD_MSL(Type, Field) TERMITE_METAL_ABI_MSL_##Type " " #Field "; "
    \\#define TERMITE_METAL_ABI_FIELDS(Name, F) TERMITE_METAL_ABI_FIELDS_##Name(F)
    \\#define TERMITE_METAL_ABI_DECLARE(Name) \
    \\    typedef struct { TERMITE_METAL_ABI_FIELDS(Name, TERMITE_METAL_ABI_FIELD_C) } termite_metal_##Name##_params;
    \\#define TERMITE_METAL_ABI_DECLARE_TAGGED(Name) \
    \\    typedef struct termite_metal_##Name##_params { TERMITE_METAL_ABI_FIELDS(Name, TERMITE_METAL_ABI_FIELD_C) } termite_metal_##Name##_params;
    \\#define TERMITE_METAL_ABI_MSL(Name) \
    \\    "struct termite_metal_" #Name "_params { " \
    \\    TERMITE_METAL_ABI_FIELDS(Name, TERMITE_METAL_ABI_FIELD_MSL) "};\n"
    \\
;

const layouts = [_]Layout{
    .{ .name = "sample_logits", .groups = &.{
        .{ .uint32 = "out_dim" },
        .{ .float32 = "temperature" },
        .{ .uint32 = "top_k" },
        .{ .float32 = "top_p min_p repetition_penalty frequency_penalty presence_penalty" },
        .{ .uint32 = "penalty_count seed" },
        .{ .float32 = "final_logit_softcap" },
        .{ .uint32 = "defer_monotonic_max" },
    } },
    .{ .name = "linear_id", .groups = &.{
        .{ .uint32 = "rows in_dim out_dim row_blocks source_out_dim row_offset expert_count reserved" },
    } },
    .{ .name = "a4b_down_reduce", .groups = &.{
        .{ .uint32 = "has_expert_scale reserved0 reserved1 reserved2" },
    } },
    .{ .name = "moe_scatter", .groups = &.{
        .{ .uint32 = "base_rows route_rows dim reserved" },
    } },
    .{ .name = "key_scores", .groups = &.{
        .{ .uint32 = "q_len block_tokens num_heads num_kv_heads head_dim key_row_bytes" },
    } },
    .{ .name = "tl1", .groups = &.{
        .{ .uint32 = "rows in_dim out_dim packed_len bm cfg_by bmm" },
    } },
    .{ .name = "tl2", .groups = &.{
        .{ .uint32 = "rows in_dim out_dim scale_off three_value_len three_sign_len bm cfg_by bmm three_cols two_cols" },
    } },
    .{ .name = "embed_absolute_position", .groups = &.{
        .{ .uint32 = "token_id position_id hidden_size" },
    } },
    .{ .name = "embedding_lookup", .groups = &.{
        .{ .uint32 = "total dim" },
    } },
    .{ .name = "quant_rows", .groups = &.{
        .{ .uint32 = "total dim source_rows row_offset" },
        .{ .float32 = "scale" },
    } },
    .{ .name = "rope", .groups = &.{
        .{ .uint32 = "total_chunks head_dim rope_dim" },
        .{ .float32 = "theta freq_scale" },
        .{ .uint32 = "consecutive_pairs" },
    } },
    .{ .name = "mrope", .groups = &.{
        .{ .uint32 = "total_chunks token_count head_dim heads_per_token section_t section_h section_w mode" },
        .{ .float32 = "theta freq_scale" },
    } },
    .{ .name = "attention_f32", .groups = &.{
        .{ .uint32 = "q_len kv_len num_heads num_kv_heads head_dim query_position_offset kv_position_offset sliding_window total_sequence_len has_bias has_mask batch reserved0" },
    } },
    .{ .name = "florence_window", .groups = &.{
        .{ .uint32 = "batch height width dim window_size padded_h padded_w window_area window_count reserved0 reserved1 reserved2" },
    } },
    .{ .name = "florence_channel", .groups = &.{
        .{ .uint32 = "batch seq_len dim groups channels_per_group reserved0 reserved1 reserved2" },
    } },
    .{ .name = "compressed_attention_store_local", .groups = &.{
        .{ .uint32 = "query_rows query_abs_start head_dim reserved" },
    } },
    .{ .name = "compressed_attention_component", .groups = &.{
        .{ .uint32 = "query_rows query_abs_start total_tokens compress_rate row_dim gate_width row_count rope_dim" },
        .{ .float32 = "theta freq_scale eps" },
        .{ .uint32 = "consecutive_pairs bias_rows reserved0 reserved1 reserved2" },
    } },
    .{ .name = "compressed_attention", .groups = &.{
        .{ .uint32 = "query_abs_start query_rows token_count compressed_rows num_heads head_dim sliding_window top_k has_indexer index_rows index_heads index_head_dim has_sinks reserved0" },
        .{ .float32 = "scale reserved1" },
    } },
    .{ .name = "layer_norm_bwd", .groups = &.{
        .{ .uint32 = "hidden_size rows" },
        .{ .float32 = "eps" },
        .{ .uint32 = "reserved0" },
    } },
    .{ .name = "apply_layer_norm_scale", .groups = &.{
        .{ .uint32 = "hidden_size" },
        .{ .float32 = "eps scale" },
        .{ .uint32 = "reserved" },
    } },
    .{ .name = "apply_row_norm", .groups = &.{
        .{ .uint32 = "rows hidden_size" },
        .{ .float32 = "eps" },
    } },
    .{ .name = "apply_row_norm_scale", .groups = &.{
        .{ .uint32 = "rows hidden_size" },
        .{ .float32 = "eps scale" },
    } },
    .{ .name = "apply_linear", .groups = &.{
        .{ .uint32 = "in_dim out_dim" },
    } },
    .{ .name = "quantize_rows", .groups = &.{
        .{ .uint32 = "rows dim" },
    } },
    .{ .name = "topk_reduce", .groups = &.{
        .{ .uint32 = "candidate_count top_k" },
        .{ .float32 = "top_p min_p" },
        .{ .uint32 = "seed" },
    } },
    .{ .name = "apply_activation", .groups = &.{
        .{ .uint32 = "activation_kind rows dim" },
    } },
    .{ .name = "apply_add", .groups = &.{
        .{ .uint32 = "rows dim flags reserved" },
    } },
    .{ .name = "norm_residual", .groups = &.{
        .{ .uint32 = "dim" },
        .{ .float32 = "eps norm_scale output_scale" },
    } },
    .{ .name = "gemma4_audio_clamp", .groups = &.{
        .{ .uint32 = "total flags" },
        .{ .float32 = "min_value max_value" },
    } },
    .{ .name = "gemma4_audio_rows", .groups = &.{
        .{ .uint32 = "rows dim extra reserved" },
    } },
    .{ .name = "gemma4_audio_channel_norm", .groups = &.{
        .{ .uint32 = "channels positions" },
        .{ .float32 = "eps" },
        .{ .uint32 = "reserved" },
    } },
    .{ .name = "training_accumulate", .groups = &.{
        .{ .uint32 = "elem_count" },
        .{ .float32 = "scale" },
        .{ .uint32 = "first reserved" },
    } },
    .{ .name = "training_accumulate_into", .groups = &.{
        .{ .uint32 = "elem_count" },
        .{ .float32 = "divisor renormalize" },
        .{ .uint32 = "flags" },
    } },
    .{ .name = "training_adamw", .groups = &.{
        .{ .uint32 = "elem_count" },
        .{ .float32 = "lr beta1 beta2 eps weight_decay bias_correction1 bias_correction2 grad_scale" },
        .{ .uint32 = "reserved0 reserved1 reserved2" },
    } },
    .{ .name = "training_sumsq", .groups = &.{
        .{ .uint32 = "elem_count reserved0 reserved1 reserved2" },
    } },
    .{ .name = "apply_softmax", .groups = &.{
        .{ .uint32 = "rows dim log_softmax reserved" },
    } },
    .{ .name = "moe_route_select", .groups = &.{
        .{ .uint32 = "rows num_experts top_k" },
        .{ .float32 = "logit_scale" },
    } },
    .{ .name = "moe_route_map_slots", .groups = &.{
        .{ .uint32 = "rows num_experts top_k reserved" },
    } },
    .{ .name = "moe_slot_q4_0_gate_up", .groups = &.{
        .{ .uint32 = "route_rows total_rows in_dim out_dim row_blocks gate_offset up_offset expert_stride slot_count top_k" },
    } },
    .{ .name = "moe_slot_q4_0_down", .groups = &.{
        .{ .uint32 = "route_rows in_dim out_dim row_blocks down_offset expert_stride slot_count reserved" },
    } },
    .{ .name = "moe_slot_reduce", .groups = &.{
        .{ .uint32 = "rows top_k dim has_expert_scale" },
    } },
    .{ .name = "mapped_page_touch", .groups = &.{
        .{ .uint32 = "page_size page_count" },
    } },
    .{ .name = "mapped_selected_expert_page_touch", .groups = &.{
        .{ .uint32 = "route_count expert_count source_out_dim row_offset logical_out_dim row_stride page_size max_pages_per_route weight_view_offset" },
    } },
    .{ .name = "reduce_last_dim", .groups = &.{
        .{ .uint32 = "rows dim kind reserved" },
    } },
    .{ .name = "nomic_pool", .groups = &.{
        .{ .uint32 = "batch seq_len hidden_size normalize" },
    } },
    .{ .name = "masked_bce", .groups = &.{
        .{ .uint32 = "elem_count mean_reduction" },
        .{ .float32 = "positive_weight negative_weight eps" },
        .{ .uint32 = "reserved0 reserved1 reserved2" },
    } },
    .{ .name = "broadcast_last_dim", .groups = &.{
        .{ .uint32 = "rows in_dim out_dim reserved" },
    } },
    .{ .name = "paged_attention", .groups = &.{
        .{ .uint32 = "q_len kv_tokens num_heads num_kv_heads head_dim key_row_bytes base_key_row_bytes query_position_offset kv_position_offset sliding_window v_row_stride page_size block_count contiguous_base_token contiguous_blocks format v_element_bytes has_sinks" },
        .{ .float32 = "softcap" },
        .{ .uint32 = "swa_scan_clamp has_explicit_positions" },
    } },
    .{ .name = "decode_gqa_split", .groups = &.{
        .{ .uint32 = "split_count max_splits key_chunk reduce_simdgroups" },
    } },
    .{ .name = "encode_key", .groups = &.{
        .{ .uint32 = "rows num_kv_heads head_dim key_row_bytes base_key_row_bytes" },
    } },
    .{ .name = "paged_f32_kv_seed", .groups = &.{
        .{ .uint32 = "total_tokens suffix_tokens values_per_row page_size key_row_floats v_row_stride" },
    } },
    .{ .name = "slice_last_dim_f32_2d", .groups = &.{
        .{ .uint32 = "rows cols start out_cols" },
    } },
    .{ .name = "gather_axis0_f32_2d", .groups = &.{
        .{ .uint32 = "rows cols index_count reserved" },
    } },
    .{ .name = "concat_lastdim_f32_2d", .groups = &.{
        .{ .uint32 = "rows dim_a dim_b reserved" },
    } },
    .{ .name = "dense_qkv_split_bias", .groups = &.{
        .{ .uint32 = "rows q_out_dim kv_out_dim total_out_dim" },
    } },
    .{ .name = "dense_pair_split_bias", .groups = &.{
        .{ .uint32 = "rows out_dim total_out_dim reserved" },
    } },
    .{ .name = "dense_qkv_direct", .groups = &.{
        .{ .uint32 = "rows in_dim q_out_dim kv_out_dim" },
    } },
    .{ .name = "dense_pair_direct", .groups = &.{
        .{ .uint32 = "rows in_dim out_dim reserved" },
    } },
    .{ .name = "deberta_embeddings_f32", .groups = &.{
        .{ .uint32 = "total rows dim" },
        .{ .float32 = "eps" },
    } },
    .{ .name = "gliner_word_embeddings_f32", .groups = &.{
        .{ .uint32 = "batch seq_len hidden_size num_words" },
    } },
    .{ .name = "gliner_gru_combine_f32", .groups = &.{
        .{ .uint32 = "rows dim reserved0 reserved1" },
    } },
    .{ .name = "argmax_axis_f32", .groups = &.{
        .{ .uint32 = "outer axis_dim inner reserved" },
    } },
    .{ .name = "convert_dtype_f32", .groups = &.{
        .{ .uint32 = "elem_count kind reserved0 reserved1" },
    } },
    .{ .name = "sdpa_f32", .groups = &.{
        .{ .uint32 = "batch seq_len num_heads head_dim bias_mode has_mask layout reserved1" },
    } },
    .{ .name = "disentangled_relative_attention_f32", .groups = &.{
        .{ .uint32 = "batch seq_len num_heads head_dim has_mask reserved0 reserved1 reserved2" },
    } },
    .{ .name = "deberta_relative_score_gemm_f32", .groups = &.{
        .{ .uint32 = "batch seq_len rel_len num_heads head_dim mode reserved0 reserved1" },
    } },
    .{ .name = "transpose_last2", .groups = &.{
        .{ .uint32 = "batch rows cols reserved0" },
    } },
    .{ .name = "dot_general_2d_f32", .groups = &.{
        .{ .uint32 = "m n k rhs_contract_axis" },
    } },
    .{ .name = "dot_general_batched_f32", .groups = &.{
        .{ .uint32 = "batch_count m n k rhs_contract_axis reserved0 reserved1 reserved2" },
    } },
    .{ .name = "lora_linear_f32", .groups = &.{
        .{ .uint32 = "rows in_dim rank out_dim" },
        .{ .float32 = "scale" },
        .{ .uint32 = "reserved0 reserved1 reserved2" },
    } },
    .{ .name = "scatter_add_axis0_f32", .groups = &.{
        .{ .uint32 = "out_rows value_rows dim reserved0" },
    } },
    .{ .name = "conv1d_f32", .groups = &.{
        .{ .uint32 = "batch in_channels out_channels time_steps kernel_size stride padding out_time has_bias reserved0 reserved1 reserved2" },
    } },
    .{ .name = "conv1d_im2col", .groups = &.{
        .{ .uint32 = "batch in_channels time_steps kernel_size stride padding out_time time_major" },
    } },
    .{ .name = "conv2d_f32", .groups = &.{
        .{ .uint32 = "batch in_channels out_channels height width kernel_h kernel_w stride_h stride_w padding_h padding_w groups out_h out_w has_bias reserved0" },
    } },
};
