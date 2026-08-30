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

const std = @import("std");
const builtin = @import("builtin");
const quant_kernel_compiler = @import("graph/quant_kernel_compiler.zig");
const quant_kernel_metal_renderer = @import("graph/quant_kernel_metal_renderer.zig");
const quant_matmul = @import("graph/quant_matmul.zig");
const quant_codec = @import("gguf/quant_codec.zig");
const compat = @import("io/compat.zig");
const metal_runtime = @import("backends/metal_runtime.zig");

const RawMetalProvider = opaque {};
const RawMetalDecodeRuntime = opaque {};
const metal_quant_format_q2_k: u32 = 4;
const metal_quant_format_q3_k: u32 = 5;
const metal_quant_format_q4_0: u32 = 6;
const metal_quant_format_q4_1: u32 = 7;
const metal_quant_format_q4_k: u32 = 8;
const metal_quant_format_q5_0: u32 = 9;
const metal_quant_format_q5_1: u32 = 10;
const metal_quant_format_q5_k: u32 = 11;
const metal_quant_format_q6_k: u32 = 12;
const metal_quant_format_q8_0: u32 = 13;
const metal_quant_format_q8_1: u32 = 14;
const metal_quant_format_q8_k: u32 = 15;
const metal_storage_private: c_int = 1;
const metal_quant_evidence_contract = "antfly.quant_kernel_metal_evidence.v1";
const metal_runtime_evidence_schema = "antfly.quant_kernel_metal_runtime_evidence.v11";
const metal_runtime_evidence_provenance_local = "local_unattested";
const metal_runtime_evidence_provenance_attested = "attested_v1";
const metal_runtime_evidence_provenance_missing = "missing_reproducible_provenance";
const clean_source_status_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern fn unsetenv(name: [*:0]const u8) c_int;
extern fn termite_metal_device_available() c_int;
extern fn termite_metal_copy_device_name(buffer: [*c]u8, capacity: usize) usize;
extern fn termite_metal_provider_create() ?*RawMetalProvider;
extern fn termite_metal_provider_destroy(provider: ?*RawMetalProvider) void;
extern fn termite_metal_provider_linear_q2_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q4_k(
    provider: ?*RawMetalProvider,
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q3_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q5_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q6_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q5_0(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q5_1(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q8_1(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q8_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q8_0_planned(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    planned_dispatch: u8,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q8_0_bias(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q8_0_bias_gelu(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q8_0_relu(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q2_k_bias(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q2_k_bias_gelu(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q3_k_bias(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q3_k_bias_gelu(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q4_k_bias(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q4_k_bias_gelu(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q5_k_bias(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q5_k_bias_gelu(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q6_k_bias(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_provider_linear_q6_k_bias_gelu(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    bias: [*c]const f32,
    output: [*c]f32,
) c_int;
extern fn termite_metal_decode_runtime_create() ?*RawMetalDecodeRuntime;
extern fn termite_metal_decode_runtime_destroy(runtime: ?*RawMetalDecodeRuntime) void;
extern fn termite_metal_decode_runtime_ready(runtime: ?*RawMetalDecodeRuntime) c_int;
extern fn termite_metal_decode_runtime_begin_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
extern fn termite_metal_decode_runtime_submit_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
extern fn termite_metal_decode_runtime_wait_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
extern fn termite_metal_decode_runtime_reset_attention_span_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
) c_int;
extern fn termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    format: u32,
    token_capacity: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
) c_int;
extern fn termite_metal_decode_runtime_attention_span_slot_info(
    runtime: ?*const RawMetalDecodeRuntime,
    slot: usize,
    encoded_key_handle_out: ?*?*anyopaque,
    encoded_key_capacity_out: ?*usize,
    v_handle_out: ?*?*anyopaque,
    v_capacity_out: ?*usize,
    tokens_out: ?*usize,
    key_row_bytes_out: ?*usize,
    v_row_stride_out: ?*usize,
    position_offset_out: ?*usize,
) c_int;
extern fn termite_metal_decode_runtime_update_attention_paged_from_f32_key_device_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    format: u32,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    total_tokens: usize,
    suffix_tokens: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    v_row_stride: usize,
    kv_position_offset: usize,
    block_table: [*c]const u32,
    block_count: usize,
    page_size: usize,
) c_int;
extern fn termite_metal_decode_runtime_attention_paged_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    format: u32,
    q_handle: ?*anyopaque,
    q_offset: usize,
    block_table: [*c]const u32,
    block_count: usize,
    page_size: usize,
    q_len: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    softcap: f32,
    sinks: ?[*]const f32,
    sink_count: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_decode_gqa_split_calls(runtime: ?*const RawMetalDecodeRuntime) u64;
extern fn termite_metal_decode_gqa_split_policy_probe(
    requested_variant: u32,
    q_len: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    sliding_window: usize,
    resolved_variant_out: *u32,
    split_count_out: *u32,
    scratch_bytes_out: *usize,
) c_int;
extern fn termite_metal_decode_runtime_decode_gqa_split_schedule_snapshot(
    runtime: ?*const RawMetalDecodeRuntime,
    snapshot: *metal_runtime.RawDecodeGqaSplitScheduleStats,
) c_int;
extern fn termite_metal_decode_runtime_prepare_quantized_linear_slot(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    weight_raw: [*]const u8,
    weight_bytes: usize,
    in_dim: usize,
    out_dim: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q2_k_bias_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q2_k_bias_gelu_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q3_k_bias_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q3_k_bias_gelu_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_gelu_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_gelu_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_gelu_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_gelu_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q8_0_relu_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_gelu_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_gelu_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_gelu_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_gelu_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_split_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
extern fn termite_metal_decode_runtime_memory_snapshot(
    runtime: ?*RawMetalDecodeRuntime,
    snapshot: *metal_runtime.RawRuntimeMemoryStats,
) c_int;
extern fn termite_metal_buffer_alloc(runtime: ?*RawMetalDecodeRuntime, length: usize, storage_mode: c_int) ?*anyopaque;
extern fn termite_metal_buffer_release(handle: ?*anyopaque) void;
extern fn termite_metal_buffer_upload(
    runtime: ?*RawMetalDecodeRuntime,
    handle: ?*anyopaque,
    offset: usize,
    src: ?*const anyopaque,
    length: usize,
) c_int;
extern fn termite_metal_buffer_download(
    runtime: ?*RawMetalDecodeRuntime,
    handle: ?*anyopaque,
    offset: usize,
    dst: ?*anyopaque,
    length: usize,
) c_int;
extern fn termite_metal_run_generated_quant_kernel_check(
    source: [*]const u8,
    source_len: usize,
    kernel_name: [*:0]const u8,
    input: [*]const f32,
    input_count: usize,
    weight: [*]const u8,
    weight_count: usize,
    bias: ?[*]const f32,
    bias_count: usize,
    output: [*]f32,
    output_count: usize,
    rows: c_int,
    in_dim: c_int,
    out_dim: c_int,
    threads_per_threadgroup: u32,
    cols_per_threadgroup: u32,
    warmup_iters: u32,
    measure_iters: u32,
    elapsed_nanos: *u64,
) c_int;
extern fn termite_metal_run_generated_microkernel_check(
    source: [*]const u8,
    source_len: usize,
    kernel_name: [*:0]const u8,
    input: [*]const f32,
    input_count: usize,
    weight: [*]const f32,
    weight_count: usize,
    output: [*]f32,
    output_count: usize,
    rows: c_int,
    hidden_size: c_int,
    eps: f32,
    threads_per_threadgroup: u32,
    warmup_iters: u32,
    measure_iters: u32,
    elapsed_nanos: *u64,
) c_int;
// Sister runner for generated `op_kind = .attention` kernels. Binds the paged
// attention buffer layout (q f32, encoded_key f16, v f16, block_table, output),
// constructs the params struct for the "simplest case" (f16 KV / format==3, no
// sinks, softcap 0, single contiguous block), dispatches grid (q_len, num_heads)
// x threads, and returns the output for host comparison. f16 buffers are passed
// as raw u16 bits so the host and GPU read identical half values.
extern fn termite_metal_run_generated_attention_check(
    source: [*]const u8,
    source_len: usize,
    kernel_name: [*:0]const u8,
    q: [*]const f32,
    q_count: usize,
    encoded_key: [*]const u16,
    encoded_key_count: usize,
    v_bytes: [*]const u16,
    v_bytes_count: usize,
    block_table: [*]const u32,
    block_count: usize,
    output: [*]f32,
    output_count: usize,
    q_len: u32,
    kv_tokens: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    query_position_offset: u32,
    kv_position_offset: u32,
    sliding_window: u32,
    page_size: u32,
    contiguous_base_token: u32,
    contiguous_blocks: u32,
    threads_per_threadgroup: u32,
    warmup_iters: u32,
    measure_iters: u32,
    elapsed_nanos: *u64,
) c_int;

// Sister runner for the generated flash-prefill attention route. Same buffer
// order + f16-KV / single-contiguous-block simplest case as the decode-1x runner,
// but dispatches the flash grid ((q_len+7)/8, num_heads) with 128 threads and the
// `key_chunk`-parameterized flash threadgroup-memory layout. Returns the output
// (host comparison vs the multi-query prefill oracle) + elapsed nanos (sweep A/B).
extern fn termite_metal_run_generated_flash_prefill_check(
    source: [*]const u8,
    source_len: usize,
    kernel_name: [*:0]const u8,
    q: [*]const f32,
    q_count: usize,
    encoded_key: [*]const u16,
    encoded_key_count: usize,
    v_bytes: [*]const u16,
    v_bytes_count: usize,
    block_table: [*]const u32,
    block_count: usize,
    output: [*]f32,
    output_count: usize,
    q_len: u32,
    kv_tokens: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    query_position_offset: u32,
    kv_position_offset: u32,
    sliding_window: u32,
    page_size: u32,
    contiguous_base_token: u32,
    contiguous_blocks: u32,
    key_chunk: u32,
    threads_per_threadgroup: u32,
    threadgroup_memory_bytes: u32,
    warmup_iters: u32,
    measure_iters: u32,
    elapsed_nanos: *u64,
) c_int;

const CheckCase = struct {
    name: []const u8,
    source: []const u8,
    kernel_name: []const u8,
    format: quant_matmul.Format,
    epilogue: quant_kernel_compiler.Epilogue,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    threads_per_threadgroup: u32,
    cols_per_threadgroup: u32,
    tolerance: f32,
    // Op-kind routing. Defaults keep every matmul CheckCase construction (and the
    // whole route/evidence/production machinery) byte-identical; the microkernel
    // fields below are only read when `op_kind == .microkernel`.
    op_kind: quant_kernel_compiler.OpKind = .small_batch_matmul,
    // Microkernel params (RMSNorm): rows=n rows, in_dim=out_dim=d hidden size,
    // eps epsilon. `eps` is meaningless for matmul routes.
    eps: f32 = 0,
    warmup_iters: u32 = default_warmup_iters,
    measure_iters: u32 = default_measure_iters,
};

const default_warmup_iters: u32 = 5;
const default_measure_iters: u32 = 25;
const repeated_check_warmup_runs: u32 = quant_kernel_compiler.metal_promotion_warmup_repeat_runs;

const RuntimeCheckShape = quant_kernel_compiler.MetalBenchmarkShape;
const RuntimeCheckDims = quant_kernel_compiler.MetalBenchmarkDims;

const metal_runtime_check_count = metalRuntimeCheckCount();
const metal_runtime_checks = buildMetalRuntimeChecks();

// ---- Microkernel (non-matmul) conformance --------------------------------
// Microkernel checks live in their own array + their own correctness pass so
// the matmul `metal_runtime_checks`, its coverage test, the evidence writer,
// and every route/production filter stay byte-identical. Each generated
// microkernel artifact is exercised across a representative shape set; the
// CPU oracle is the same `activations.rmsNorm` used by native inference.

const RmsNormShape = struct { n: usize, d: usize };

/// Representative (n, d) shapes: n small (decode-ish batch) x d spanning the
/// tiny-to-large hidden sizes real models use. d values need not be powers of
/// two — the kernel's strided lane loop handles any d.
const rms_norm_shapes = [_]RmsNormShape{
    .{ .n = 1, .d = 64 },
    .{ .n = 2, .d = 128 },
    .{ .n = 4, .d = 512 },
    .{ .n = 3, .d = 2048 },
    .{ .n = 2, .d = 4096 },
};

const microkernel_runtime_check_count = microkernelRuntimeCheckCount();
const microkernel_runtime_checks = buildMicrokernelRuntimeChecks();

fn microkernelRuntimeCheckCount() comptime_int {
    var count: comptime_int = 0;
    for (quant_kernel_compiler.first_generated_microkernel_artifacts) |artifact| {
        if (artifact.backend == .metal) count += rms_norm_shapes.len;
    }
    return count;
}

fn buildMicrokernelRuntimeChecks() [microkernel_runtime_check_count]CheckCase {
    var checks: [microkernel_runtime_check_count]CheckCase = undefined;
    var index: usize = 0;
    inline for (quant_kernel_compiler.first_generated_microkernel_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        inline for (rms_norm_shapes) |shape| {
            checks[index] = microkernelRuntimeCheck(artifact, shape);
            index += 1;
        }
    }
    return checks;
}

fn microkernelRuntimeCheck(comptime artifact: quant_kernel_compiler.GeneratedArtifact, comptime shape: RmsNormShape) CheckCase {
    const op = artifact.microkernelOp() orelse @compileError("expected a generated microkernel artifact");
    return .{
        .name = std.fmt.comptimePrint("{s}_n{d}_d{d}", .{ artifact.kernel_id, shape.n, shape.d }),
        .source = quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse @compileError("missing generated microkernel source"),
        .kernel_name = artifact.kernel_id,
        .format = .f32,
        .epilogue = .none,
        .op_kind = artifact.opKind(),
        // rows=n, in_dim=out_dim=d so input/output are both n*d and the weight is d.
        .rows = shape.n,
        .in_dim = shape.d,
        .out_dim = shape.d,
        .threads_per_threadgroup = @intCast(op.schedule.threads_per_threadgroup),
        .cols_per_threadgroup = 1,
        // f32 RMSNorm: near bit-exact vs the CPU oracle; the only divergence is
        // GPU tree-reduction vs CPU sequential summation order. Measured on-device
        // max_abs_error grows with d: ~0 at d=64 up to ~3.3e-6 at d=4096. 1e-4 abs
        // keeps a ~30x safety margin (outputs are O(1)) without being flaky.
        .eps = 1e-6,
        .tolerance = 1e-4,
    };
}

/// CPU oracle for the RMSNorm microkernel. Self-contained (the runtime-check exe
/// does not link the `inference_linalg` module `backends/activations.zig` needs)
/// but computes exactly what `activations.rmsNorm` does — the reference native
/// inference uses: `out[i] = in[i] * (1/sqrt(mean(in^2)+eps)) * weight[i]`, sum
/// of squares accumulated in f32, no mean subtraction, plain (non-Gemma) weight.
fn referenceRmsNorm(allocator: std.mem.Allocator, input: []const f32, weight: []const f32, n: usize, d: usize, eps: f32, out: []f32) !void {
    _ = allocator;
    if (input.len != n * d or out.len != n * d or weight.len != d) return error.InvalidArgument;
    for (0..n) |row| {
        const base = row * d;
        var sum_sq: f32 = 0;
        for (0..d) |i| {
            const x = input[base + i];
            sum_sq += x * x;
        }
        const rms = @sqrt(sum_sq / @as(f32, @floatFromInt(d)) + eps);
        const inv_rms: f32 = 1.0 / rms;
        for (0..d) |i| {
            out[base + i] = input[base + i] * inv_rms * weight[i];
        }
    }
}

fn runMicrokernelCheck(allocator: std.mem.Allocator, check: CheckCase) !CheckResult {
    const n = check.rows;
    const d = check.in_dim;

    const input = try allocator.alloc(f32, n * d);
    defer allocator.free(input);
    for (input, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 19)) - 9)) / 48.0;
    }

    const weight = try allocator.alloc(f32, d);
    defer allocator.free(weight);
    for (weight, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + 3) % 23)) - 11)) / 16.0;
    }

    const expected = try allocator.alloc(f32, n * d);
    defer allocator.free(expected);
    try referenceRmsNorm(allocator, input, weight, n, d, check.eps, expected);

    const actual = try allocator.alloc(f32, n * d);
    defer allocator.free(actual);
    @memset(actual, 0);

    const kernel_name_z = try allocator.dupeSentinel(u8, check.kernel_name, 0);
    defer allocator.free(kernel_name_z);
    var elapsed_nanos: u64 = 0;
    const rc = termite_metal_run_generated_microkernel_check(
        check.source.ptr,
        check.source.len,
        kernel_name_z.ptr,
        input.ptr,
        input.len,
        weight.ptr,
        weight.len,
        actual.ptr,
        actual.len,
        @intCast(n),
        @intCast(d),
        check.eps,
        check.threads_per_threadgroup,
        check.warmup_iters,
        check.measure_iters,
        &elapsed_nanos,
    );
    if (rc != 0) {
        std.debug.print("generated Metal microkernel {s} failed rc={d}\n", .{ check.name, rc });
        return error.GeneratedMetalKernelFailed;
    }

    var max_error: f32 = 0.0;
    for (actual, expected, 0..) |got, want, index| {
        if (!std.math.isFinite(got)) {
            std.debug.print("generated Metal microkernel {s} nonfinite at {d}: got={d} want={d}\n", .{ check.name, index, got, want });
            return error.GeneratedMetalKernelMismatch;
        }
        const diff = @abs(got - want);
        max_error = @max(max_error, diff);
        if (diff > check.tolerance) {
            std.debug.print("generated Metal microkernel {s} mismatch at {d}: got={d} want={d} diff={d}\n", .{ check.name, index, got, want, diff });
            return error.GeneratedMetalKernelMismatch;
        }
    }
    return .{ .max_error = max_error, .measure_iters = check.measure_iters, .elapsed_nanos = elapsed_nanos };
}

// ---- Attention (paged decode) conformance --------------------------------
// Like the microkernel checks, attention checks live in their own array + pass
// so the matmul machinery is untouched. This isolated float compare is a fast
// pre-check for compile/dispatch/gross-logic errors ONLY — softmax is too
// summation-sensitive for a tight isolated gate, so the real acceptance gate is
// bit-identical *model tokens* (scripts/gemma4/compare_metal_gemma4_e4b_qat.sh with the
// generated route enabled). Every case is the "simplest case": q_len=1 decode,
// f16 KV, single contiguous block, no sinks, softcap 0 — exercising GQA and the
// causal + sliding-window masks against a self-contained CPU oracle.

const AttentionShape = struct {
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    /// Position of the (single) decode query. Set to `kv_tokens - 1` so the
    /// query is causally after every KV token (the real decode invariant).
    query_position_offset: usize,
    /// 0 = full causal attention; W>0 masks KV tokens older than W (iSWA).
    sliding_window: usize,
};

/// Representative decode shapes: GQA ratios (MHA, 2:1, 4:1), head_dims spanning
/// 64/128/256, with and without a sliding window. All single contiguous block.
const attention_shapes = [_]AttentionShape{
    .{ .kv_tokens = 8, .num_heads = 2, .num_kv_heads = 1, .head_dim = 64, .query_position_offset = 7, .sliding_window = 0 },
    .{ .kv_tokens = 16, .num_heads = 4, .num_kv_heads = 2, .head_dim = 128, .query_position_offset = 15, .sliding_window = 0 },
    .{ .kv_tokens = 32, .num_heads = 4, .num_kv_heads = 1, .head_dim = 64, .query_position_offset = 31, .sliding_window = 8 },
    .{ .kv_tokens = 64, .num_heads = 8, .num_kv_heads = 8, .head_dim = 128, .query_position_offset = 63, .sliding_window = 0 },
    .{ .kv_tokens = 48, .num_heads = 4, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 47, .sliding_window = 16 },
};

const AttentionCheckCase = struct {
    name: []const u8,
    source: []const u8,
    kernel_name: []const u8,
    threads_per_threadgroup: u32,
    tolerance: f32,
    shape: AttentionShape,
    warmup_iters: u32 = default_warmup_iters,
    measure_iters: u32 = default_measure_iters,
};

const attention_runtime_check_count = attentionRuntimeCheckCount();
const attention_runtime_checks = buildAttentionRuntimeChecks();

/// The decode-1x conformance harness handles only the scalar decode route: its
/// oracle assumes q_len=1 and its runner dispatches the decode grid / 256 threads
/// / decode shmem. The flash-prefill route (different grid / 128 threads / shmem)
/// has its own oracle + runner, so it is excluded here.
fn isDecodeAttentionArtifact(comptime artifact: quant_kernel_compiler.GeneratedArtifact) bool {
    const op = artifact.attentionOp() orelse return false;
    return artifact.backend == .metal and op.kind == .decode_1x;
}

fn attentionRuntimeCheckCount() comptime_int {
    var count: comptime_int = 0;
    for (quant_kernel_compiler.first_generated_attention_artifacts) |artifact| {
        if (isDecodeAttentionArtifact(artifact)) count += attention_shapes.len;
    }
    return count;
}

fn buildAttentionRuntimeChecks() [attention_runtime_check_count]AttentionCheckCase {
    var checks: [attention_runtime_check_count]AttentionCheckCase = undefined;
    var index: usize = 0;
    inline for (quant_kernel_compiler.first_generated_attention_artifacts) |artifact| {
        if (!isDecodeAttentionArtifact(artifact)) continue;
        inline for (attention_shapes) |shape| {
            checks[index] = attentionRuntimeCheck(artifact, shape);
            index += 1;
        }
    }
    return checks;
}

fn attentionRuntimeCheck(comptime artifact: quant_kernel_compiler.GeneratedArtifact, comptime shape: AttentionShape) AttentionCheckCase {
    const op = artifact.attentionOp() orelse @compileError("expected a generated attention artifact");
    return .{
        .name = std.fmt.comptimePrint("{s}_kv{d}_h{d}_kvh{d}_hd{d}_sw{d}", .{ artifact.kernel_id, shape.kv_tokens, shape.num_heads, shape.num_kv_heads, shape.head_dim, shape.sliding_window }),
        .source = quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse @compileError("missing generated attention source"),
        .kernel_name = artifact.kernel_id,
        .threads_per_threadgroup = @intCast(op.schedule.threads_per_threadgroup),
        // f16 KV + f32 accumulation: kernel and oracle read the same half values,
        // so only GPU tree-reduction vs sequential summation order differs. This
        // is a loose sanity gate (the model-token gate is the real one); 2e-2 abs
        // keeps a wide margin above the ~1e-3 summation-order noise on O(1)
        // outputs while still catching NaNs / masking / paging / GQA bugs.
        .tolerance = 2e-2,
        .shape = shape,
    };
}

fn halfBitsToF32(bits: u16) f32 {
    return @floatCast(@as(f16, @bitCast(bits)));
}

fn f32ToHalfBits(value: f32) u16 {
    return @bitCast(@as(f16, @floatCast(value)));
}

/// Self-contained CPU oracle for the decode-1x paged attention kernel, for the
/// simplest case (single contiguous block => physical_token == logical_token).
/// Computes exactly what the kernel computes: per (query, head), a causal +
/// sliding-window masked, GQA-mapped, f16-KV dot-product score scaled by
/// 1/sqrt(head_dim), a numerically-stable softmax, then the P·V combination.
fn referenceGqaAttention1x(
    allocator: std.mem.Allocator,
    q: []const f32,
    encoded_key: []const u16,
    v_bytes: []const u16,
    shape: AttentionShape,
    out: []f32,
) !void {
    const q_len: usize = 1;
    const kv = shape.kv_tokens;
    const nh = shape.num_heads;
    const nkv = shape.num_kv_heads;
    const hd = shape.head_dim;
    if (nkv == 0 or nh % nkv != 0) return error.InvalidArgument;
    if (q.len != q_len * nh * hd or out.len != q_len * nh * hd) return error.InvalidArgument;
    if (encoded_key.len != kv * nkv * hd or v_bytes.len != kv * nkv * hd) return error.InvalidArgument;
    const heads_per_group = nh / nkv;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(hd)));
    const neg_inf: f32 = -3.402823466e+38;

    var scores = try allocator.alloc(f32, kv);
    defer allocator.free(scores);

    const qi: usize = 0;
    const query_pos = shape.query_position_offset + qi;
    for (0..nh) |h| {
        const kv_h = h / heads_per_group;
        const q_base = qi * nh * hd + h * hd;
        const kv_head_base = kv_h * hd;

        var best: f32 = neg_inf;
        for (0..kv) |ki| {
            const key_pos = ki; // kv_position_offset = 0
            var allowed = key_pos <= query_pos;
            if (shape.sliding_window != 0 and allowed) allowed = (query_pos - key_pos) < shape.sliding_window;
            if (!allowed) {
                scores[ki] = neg_inf;
                continue;
            }
            const key_row = ki * nkv * hd + kv_head_base;
            var acc: f32 = 0;
            for (0..hd) |d| acc += q[q_base + d] * halfBitsToF32(encoded_key[key_row + d]);
            const score = acc * scale;
            scores[ki] = if (std.math.isFinite(score)) score else neg_inf;
            if (scores[ki] > best) best = scores[ki];
        }

        var sum: f32 = 0;
        for (0..kv) |ki| {
            var e: f32 = if (best > -3.0e+38 and scores[ki] > -3.0e+38) @exp(scores[ki] - best) else 0;
            if (!std.math.isFinite(e)) e = 0;
            scores[ki] = e;
            sum += e;
        }
        const inv_sum: f32 = if (sum > 0) 1.0 / sum else 0;
        for (0..hd) |d| {
            var value: f32 = 0;
            for (0..kv) |ki| {
                const v_index = ki * nkv * hd + kv_head_base + d; // v_row_stride = nkv*hd
                value += scores[ki] * inv_sum * halfBitsToF32(v_bytes[v_index]);
            }
            out[q_base + d] = value;
        }
    }
}

fn runAttentionCheck(allocator: std.mem.Allocator, check: AttentionCheckCase) !CheckResult {
    const shape = check.shape;
    const q_len: usize = 1;
    const nh = shape.num_heads;
    const nkv = shape.num_kv_heads;
    const hd = shape.head_dim;
    const kv = shape.kv_tokens;

    const q = try allocator.alloc(f32, q_len * nh * hd);
    defer allocator.free(q);
    for (q, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 17)) - 8)) / 32.0;
    }

    // Key/value are f16 (format==3): quantize deterministic small f32 patterns to
    // half so the host oracle and the GPU read identical values.
    const key = try allocator.alloc(u16, kv * nkv * hd);
    defer allocator.free(key);
    for (key, 0..) |*value, i| {
        value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + 1) % 23)) - 11)) / 40.0);
    }
    const v = try allocator.alloc(u16, kv * nkv * hd);
    defer allocator.free(v);
    for (v, 0..) |*value, i| {
        value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast((i * 3 + 5) % 19)) - 9)) / 24.0);
    }

    // Single contiguous block: block_table = {0}, page_size >= kv_tokens.
    var block_table = [_]u32{0};

    const expected = try allocator.alloc(f32, q_len * nh * hd);
    defer allocator.free(expected);
    try referenceGqaAttention1x(allocator, q, key, v, shape, expected);

    const actual = try allocator.alloc(f32, q_len * nh * hd);
    defer allocator.free(actual);
    @memset(actual, 0);

    const kernel_name_z = try allocator.dupeSentinel(u8, check.kernel_name, 0);
    defer allocator.free(kernel_name_z);
    var elapsed_nanos: u64 = 0;
    const rc = termite_metal_run_generated_attention_check(
        check.source.ptr,
        check.source.len,
        kernel_name_z.ptr,
        q.ptr,
        q.len,
        key.ptr,
        key.len,
        v.ptr,
        v.len,
        &block_table,
        block_table.len,
        actual.ptr,
        actual.len,
        @intCast(q_len),
        @intCast(kv),
        @intCast(nh),
        @intCast(nkv),
        @intCast(hd),
        @intCast(shape.query_position_offset),
        0, // kv_position_offset
        @intCast(shape.sliding_window),
        @intCast(kv), // page_size (single block spans all kv tokens)
        0, // contiguous_base_token
        1, // contiguous_blocks
        check.threads_per_threadgroup,
        check.warmup_iters,
        check.measure_iters,
        &elapsed_nanos,
    );
    if (rc != 0) {
        std.debug.print("generated Metal attention {s} failed rc={d}\n", .{ check.name, rc });
        return error.GeneratedMetalKernelFailed;
    }

    var max_error: f32 = 0.0;
    for (actual, expected, 0..) |got, want, index| {
        if (!std.math.isFinite(got)) {
            std.debug.print("generated Metal attention {s} nonfinite at {d}: got={d} want={d}\n", .{ check.name, index, got, want });
            return error.GeneratedMetalKernelMismatch;
        }
        const diff = @abs(got - want);
        max_error = @max(max_error, diff);
        if (diff > check.tolerance) {
            std.debug.print("generated Metal attention {s} mismatch at {d}: got={d} want={d} diff={d}\n", .{ check.name, index, got, want, diff });
            return error.GeneratedMetalKernelMismatch;
        }
    }
    return .{ .max_error = max_error, .measure_iters = check.measure_iters, .elapsed_nanos = elapsed_nanos };
}

// ---- Flash-prefill (multi-query) attention conformance -------------------
// The flash-prefill route dispatches a different grid / thread count / shmem
// layout than decode-1x, so it has its own oracle + runner + shape set. Every
// case uses f16 KV, no sinks, and softcap 0, with dense, permuted, and offset
// physical pages exercising multi-query prefill against the causal +
// sliding-window masks. Isolated float parity is a loose compile/dispatch sanity check (the
// flash online-softmax + f16 Q/P rounding diverge from a plain-f32 reference);
// the real gate is bit-identical model tokens with the generated route enabled.

const FlashPrefillShape = struct {
    q_len: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    /// Position of the first query in the tile; query qi sits at qpo+qi.
    query_position_offset: usize,
    /// 0 = full causal; W>0 masks KV tokens older than W (iSWA).
    sliding_window: usize,
    page_size: usize = 0,
    permuted_pages: bool = false,
    physical_page_bias: usize = 0,
    /// Cyclic physical-page rotation used to model a paged-KV ring wrap.
    /// Mutually exclusive with `permuted_pages` in the focused fixtures.
    physical_page_rotation: usize = 0,
};

fn physicalPageWithinSpan(shape: FlashPrefillShape, page_count: usize, logical_page: usize) usize {
    std.debug.assert(page_count != 0 and logical_page < page_count);
    if (shape.permuted_pages) return page_count - 1 - logical_page;
    return (logical_page + shape.physical_page_rotation % page_count) % page_count;
}

/// Representative Gemma4 local-attention tiles for the exact head_dim=256
/// specialization, including sliding, permuted, and ragged-final-page cases.
const flash_prefill_shapes = [_]FlashPrefillShape{
    .{ .q_len = 12, .kv_tokens = 48, .num_heads = 4, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 36, .sliding_window = 16 },
    .{ .q_len = 8, .kv_tokens = 64, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 0, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 },
    .{ .q_len = 13, .kv_tokens = 50, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 37, .sliding_window = 0, .page_size = 16, .permuted_pages = true },
    .{ .q_len = 256, .kv_tokens = 512, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 256, .sliding_window = 512, .page_size = 16, .permuted_pages = true },
    // Production Gemma4 26B-A4B local attention (16Q/8KV, 2:1 GQA). The
    // 1,040-token span crosses the first 1,024-token sliding-window boundary;
    // page reversal plus a leading gap makes masked and mapped reads observable.
    .{ .q_len = 16, .kv_tokens = 1040, .num_heads = 16, .num_kv_heads = 8, .head_dim = 256, .query_position_offset = 1024, .sliding_window = 1024, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 },
};

const flash_prefill_hd512_shapes = [_]FlashPrefillShape{
    // Production Gemma4 E4B global-attention geometry (8Q/2KV, 4:1 GQA).
    .{ .q_len = 8, .kv_tokens = 64, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 0, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 },
    .{ .q_len = 8, .kv_tokens = 64, .num_heads = 16, .num_kv_heads = 1, .head_dim = 512, .query_position_offset = 0, .sliding_window = 0, .page_size = 16, .permuted_pages = true },
    .{ .q_len = 8, .kv_tokens = 32, .num_heads = 16, .num_kv_heads = 1, .head_dim = 512, .query_position_offset = 24, .sliding_window = 0, .page_size = 16, .permuted_pages = true },
    .{ .q_len = 12, .kv_tokens = 48, .num_heads = 16, .num_kv_heads = 1, .head_dim = 512, .query_position_offset = 36, .sliding_window = 0, .page_size = 16, .permuted_pages = true },
    .{ .q_len = 13, .kv_tokens = 50, .num_heads = 16, .num_kv_heads = 1, .head_dim = 512, .query_position_offset = 37, .sliding_window = 0, .page_size = 16, .permuted_pages = true },
    .{ .q_len = 256, .kv_tokens = 512, .num_heads = 16, .num_kv_heads = 1, .head_dim = 512, .query_position_offset = 0, .sliding_window = 0, .page_size = 16, .permuted_pages = true },
};

const SplitGqaCheckCase = struct {
    name: []const u8,
    shape: FlashPrefillShape,
};

const SplitGqaVariant = metal_runtime.DecodeGqaSplitVariant;
const split_gqa_concrete_variants = [_]SplitGqaVariant{
    .s8_k32_r256,
    .s16_k32_r256,
    .s24_k32_r256,
    .s32_k32_r256,
};
const split_gqa_policy_variants = [_]SplitGqaVariant{
    .auto,
    .s8_k32_r256,
    .s16_k32_r256,
    .s24_k32_r256,
    .s32_k32_r256,
};
const split_gqa_policy_kv_boundaries = [_]usize{ 511, 512, 513, 1023, 1024, 2003, 4095, 4096, 8191 };

// This is an isolated tensor-oracle gate, not the final token-parity gate.
// Split-GQA rounds staged Q and softmax probabilities through f16, but 1e-2
// still leaves a useful margin over those expected differences while being 5x
// tighter than the former ad-hoc 5e-2 bound.
const split_gqa_tensor_tolerance: f32 = 1e-2;

// Production Gemma4 E2B/E4B/A4B decode geometries. 513 tokens leaves one valid row in
// the final 8-row V tile; reversed pages plus a leading physical-page gap make
// every masked lane observable when the unused private storage is NaN-poisoned.
// The 2,003-token global case forces every split-cap candidate to execute
// repeated 32-token chunks instead of validating only compact-stride plumbing.
const split_gqa_checks = [_]SplitGqaCheckCase{
    .{ .name = "decode_gqa_split_e2b_q1_hd256_swa512", .shape = .{ .q_len = 1, .kv_tokens = 513, .num_heads = 8, .num_kv_heads = 1, .head_dim = 256, .query_position_offset = 512, .sliding_window = 512, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_e2b_q1_kv2003_hd512_global", .shape = .{ .q_len = 1, .kv_tokens = 2003, .num_heads = 8, .num_kv_heads = 1, .head_dim = 512, .query_position_offset = 2002, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_q1_hd256_swa512", .shape = .{ .q_len = 1, .kv_tokens = 513, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 512, .sliding_window = 512, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_q2_hd256_swa512", .shape = .{ .q_len = 2, .kv_tokens = 513, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 511, .sliding_window = 512, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_q1_hd512_global", .shape = .{ .q_len = 1, .kv_tokens = 513, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 512, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_q2_hd512_global", .shape = .{ .q_len = 2, .kv_tokens = 513, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 511, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_q1_kv2003_hd512_global", .shape = .{ .q_len = 1, .kv_tokens = 2003, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 2002, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_a4b_q1_hd256_swa1024", .shape = .{ .q_len = 1, .kv_tokens = 513, .num_heads = 16, .num_kv_heads = 8, .head_dim = 256, .query_position_offset = 512, .sliding_window = 1024, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
    .{ .name = "decode_gqa_split_a4b_q1_hd512_global", .shape = .{ .q_len = 1, .kv_tokens = 513, .num_heads = 16, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 512, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } },
};

// The production local-attention fallback at the first ragged SWA boundary.
// A leading physical-page gap stays NaN-poisoned so masked V reads cannot hide.
const paged_local_ragged_check = SplitGqaCheckCase{
    .name = "paged_local_q1_kv513_hd256_swa512",
    .shape = .{ .q_len = 1, .kv_tokens = 513, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 512, .sliding_window = 512, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 },
};

const SplitGqaSelectionCheck = struct {
    check: SplitGqaCheckCase,
    expect_route: bool,
};

// Production defaults use split GQA at and above the validated 512-token
// crossover; the explicit disable flag remains the rollback path.
const split_gqa_production_selection_checks = [_]SplitGqaSelectionCheck{
    .{ .check = .{ .name = "decode_gqa_default_e2b_local_kv512", .shape = .{ .q_len = 1, .kv_tokens = 512, .num_heads = 8, .num_kv_heads = 1, .head_dim = 256, .query_position_offset = 511, .sliding_window = 512, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    .{ .check = .{ .name = "decode_gqa_default_e2b_global_kv2003", .shape = .{ .q_len = 1, .kv_tokens = 2003, .num_heads = 8, .num_kv_heads = 1, .head_dim = 512, .query_position_offset = 2002, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    .{ .check = .{ .name = "decode_gqa_default_global_kv511", .shape = .{ .q_len = 1, .kv_tokens = 511, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 510, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = false },
    .{ .check = .{ .name = "decode_gqa_default_global_kv512", .shape = .{ .q_len = 1, .kv_tokens = 512, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 511, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    .{ .check = .{ .name = "decode_gqa_default_global_kv513", .shape = .{ .q_len = 2, .kv_tokens = 513, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 511, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    // Preserve the established long-global tensor checks in addition to the
    // pure schedule-policy matrix. These exercise real staged/reducer output
    // across the former 4K decode boundary rather than only selection math.
    .{ .check = .{ .name = "decode_gqa_default_global_kv4095", .shape = .{ .q_len = 1, .kv_tokens = 4095, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 4094, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    .{ .check = .{ .name = "decode_gqa_default_global_kv4096", .shape = .{ .q_len = 1, .kv_tokens = 4096, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 4095, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    .{ .check = .{ .name = "decode_gqa_default_global_kv4097", .shape = .{ .q_len = 1, .kv_tokens = 4097, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 4096, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    .{ .check = .{ .name = "decode_gqa_default_local_split", .shape = .{ .q_len = 1, .kv_tokens = 512, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 511, .sliding_window = 512, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = true },
    // The 2,003-token history is clamped to the live 512-token SWA span. A
    // 20-page physical rotation puts the retained page-table suffix across the
    // ring boundary while keeping the allocation a bounded page permutation.
    .{ .check = .{ .name = "decode_gqa_default_local_kv2003_ring_wrap", .shape = .{ .q_len = 1, .kv_tokens = 2003, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 2002, .sliding_window = 512, .page_size = 16, .physical_page_bias = 1, .physical_page_rotation = 20 } }, .expect_route = true },
    // A4B shapes require the dedicated enable or high-memory umbrella; the
    // default compact lane must stay on its established paged-1x route.
    .{ .check = .{ .name = "decode_gqa_default_a4b_local_isolated", .shape = .{ .q_len = 1, .kv_tokens = 512, .num_heads = 16, .num_kv_heads = 8, .head_dim = 256, .query_position_offset = 511, .sliding_window = 1024, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = false },
    .{ .check = .{ .name = "decode_gqa_default_a4b_global_isolated", .shape = .{ .q_len = 1, .kv_tokens = 512, .num_heads = 16, .num_kv_heads = 2, .head_dim = 512, .query_position_offset = 511, .sliding_window = 0, .page_size = 16, .permuted_pages = true, .physical_page_bias = 1 } }, .expect_route = false },
};

const FlashPrefillCheckCase = struct {
    name: []const u8,
    source: []const u8,
    kernel_name: []const u8,
    key_chunk: u32,
    threads_per_threadgroup: u32,
    threadgroup_memory_bytes: u32,
    tolerance: f32,
    shape: FlashPrefillShape,
    warmup_iters: u32 = default_warmup_iters,
    measure_iters: u32 = default_measure_iters,
};

const flash_prefill_check_count = flashPrefillRuntimeCheckCount();
const flash_prefill_checks = buildFlashPrefillRuntimeChecks();

fn isFlashPrefillArtifact(comptime artifact: quant_kernel_compiler.GeneratedArtifact) bool {
    const op = artifact.attentionOp() orelse return false;
    return artifact.backend == .metal and op.kind == .prefill_flash;
}

fn flashPrefillRuntimeCheckCount() comptime_int {
    var count: comptime_int = 0;
    for (quant_kernel_compiler.first_generated_attention_artifacts) |artifact| {
        if (!isFlashPrefillArtifact(artifact)) continue;
        count += if (artifact.attentionOp().?.head_dim == 512) flash_prefill_hd512_shapes.len else flash_prefill_shapes.len;
    }
    return count;
}

fn buildFlashPrefillRuntimeChecks() [flash_prefill_check_count]FlashPrefillCheckCase {
    var checks: [flash_prefill_check_count]FlashPrefillCheckCase = undefined;
    var index: usize = 0;
    inline for (quant_kernel_compiler.first_generated_attention_artifacts) |artifact| {
        if (!isFlashPrefillArtifact(artifact)) continue;
        if (artifact.attentionOp().?.head_dim == 512) {
            inline for (flash_prefill_hd512_shapes) |shape| {
                checks[index] = flashPrefillRuntimeCheck(artifact, shape);
                index += 1;
            }
        } else {
            inline for (flash_prefill_shapes) |shape| {
                checks[index] = flashPrefillRuntimeCheck(artifact, shape);
                index += 1;
            }
        }
    }
    return checks;
}

fn flashPrefillRuntimeCheck(comptime artifact: quant_kernel_compiler.GeneratedArtifact, comptime shape: FlashPrefillShape) FlashPrefillCheckCase {
    const op = artifact.attentionOp() orelse @compileError("expected a generated flash-prefill artifact");
    return .{
        .name = std.fmt.comptimePrint("{s}_q{d}_kv{d}_h{d}_kvh{d}_hd{d}_sw{d}", .{ artifact.kernel_id, shape.q_len, shape.kv_tokens, shape.num_heads, shape.num_kv_heads, shape.head_dim, shape.sliding_window }),
        .source = quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse @compileError("missing generated flash prefill source"),
        .kernel_name = artifact.kernel_id,
        .key_chunk = @intCast(op.schedule.key_chunk),
        .threads_per_threadgroup = @intCast(op.schedule.threads_per_threadgroup),
        .threadgroup_memory_bytes = if (op.head_dim == 512)
            13_888
        else
            @intCast(24 * shape.head_dim + 52 * op.schedule.key_chunk + 352),
        // Loose sanity bound: the flash online softmax rounds Q and the P weights
        // through f16 and accumulates in a different order than the f32 reference,
        // so the divergence on O(0.3) outputs runs a few 1e-2. 5e-2 keeps a wide
        // margin above that while still catching NaNs / masking / paging / GQA
        // bugs (the real gate is bit-identical model tokens).
        .tolerance = 5e-2,
        .shape = shape,
    };
}

/// Self-contained CPU oracle for the flash-prefill kernel (single contiguous
/// block => physical_token == logical_token). Mirrors the kernel's numerics that
/// matter: Q is pre-scaled by 1/sqrt(head_dim) and rounded to f16 (the kernel
/// stages `half(q*scale)` in shmem), K/V are read as f16; the softmax runs in f32
/// (the online running-max form is algebraically the same up to f32 rounding).
fn referenceFlashPrefill(
    allocator: std.mem.Allocator,
    q: []const f32,
    encoded_key: []const u16,
    v_bytes: []const u16,
    shape: FlashPrefillShape,
    out: []f32,
) !void {
    const q_len = shape.q_len;
    const kv = shape.kv_tokens;
    const nh = shape.num_heads;
    const nkv = shape.num_kv_heads;
    const hd = shape.head_dim;
    if (nkv == 0 or nh % nkv != 0) return error.InvalidArgument;
    if (q.len != q_len * nh * hd or out.len != q_len * nh * hd) return error.InvalidArgument;
    if (encoded_key.len != kv * nkv * hd or v_bytes.len != kv * nkv * hd) return error.InvalidArgument;
    const heads_per_group = nh / nkv;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(hd)));
    const neg_inf: f32 = -3.402823466e+38;

    var scores = try allocator.alloc(f32, kv);
    defer allocator.free(scores);

    for (0..q_len) |qi| {
        const query_pos = shape.query_position_offset + qi;
        for (0..nh) |h| {
            const kv_h = h / heads_per_group;
            const q_base = qi * nh * hd + h * hd;
            const kv_head_base = kv_h * hd;

            var best: f32 = neg_inf;
            for (0..kv) |ki| {
                const key_pos = ki; // kv_position_offset = 0
                var allowed = key_pos <= query_pos;
                if (shape.sliding_window != 0 and allowed) allowed = (query_pos - key_pos) < shape.sliding_window;
                if (!allowed) {
                    scores[ki] = neg_inf;
                    continue;
                }
                const key_row = ki * nkv * hd + kv_head_base;
                var acc: f32 = 0;
                for (0..hd) |d| {
                    // Q pre-scaled then rounded to f16 in shmem; K already f16.
                    const qh = halfBitsToF32(f32ToHalfBits(q[q_base + d] * scale));
                    acc += qh * halfBitsToF32(encoded_key[key_row + d]);
                }
                scores[ki] = if (std.math.isFinite(acc)) acc else neg_inf;
                if (scores[ki] > best) best = scores[ki];
            }

            var sum: f32 = 0;
            for (0..kv) |ki| {
                var e = @exp(scores[ki] - best);
                if (!std.math.isFinite(e)) e = 0;
                scores[ki] = e;
                sum += e;
            }
            const inv_sum: f32 = if (sum > 0) 1.0 / sum else 0;
            for (0..hd) |d| {
                var value: f32 = 0;
                for (0..kv) |ki| {
                    const v_index = ki * nkv * hd + kv_head_base + d;
                    value += scores[ki] * inv_sum * halfBitsToF32(v_bytes[v_index]);
                }
                out[q_base + d] = value;
            }
        }
    }
}

fn runFlashPrefillCheck(allocator: std.mem.Allocator, check: FlashPrefillCheckCase) !CheckResult {
    const shape = check.shape;
    const q_len = shape.q_len;
    const nh = shape.num_heads;
    const nkv = shape.num_kv_heads;
    const hd = shape.head_dim;
    const kv = shape.kv_tokens;

    const q = try allocator.alloc(f32, q_len * nh * hd);
    defer allocator.free(q);
    for (q, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 17)) - 8)) / 32.0;
    }
    const logical_key = try allocator.alloc(u16, kv * nkv * hd);
    defer allocator.free(logical_key);
    for (logical_key, 0..) |*value, i| {
        value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + 1) % 23)) - 11)) / 40.0);
    }
    const logical_v = try allocator.alloc(u16, kv * nkv * hd);
    defer allocator.free(logical_v);
    for (logical_v, 0..) |*value, i| {
        value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast((i * 3 + 5) % 19)) - 9)) / 24.0);
    }

    const page_size = if (shape.page_size == 0) kv else shape.page_size;
    if (page_size == 0) return error.InvalidArgument;
    const page_count = (kv + page_size - 1) / page_size;
    const block_table = try allocator.alloc(u32, page_count);
    defer allocator.free(block_table);
    for (block_table, 0..) |*offset, logical_page| {
        const page_within_span = physicalPageWithinSpan(shape, page_count, logical_page);
        const physical_page = shape.physical_page_bias + page_within_span;
        offset.* = @intCast(physical_page * page_size);
    }
    const physical_tokens = (shape.physical_page_bias + page_count) * page_size;
    const key = try allocator.alloc(u16, physical_tokens * nkv * hd);
    defer allocator.free(key);
    @memset(key, 0x7e00); // Poison unwritten page padding: masked lanes must never leak NaNs.
    const v = try allocator.alloc(u16, physical_tokens * nkv * hd);
    defer allocator.free(v);
    @memset(v, 0x7e00);
    const row_values = nkv * hd;
    for (0..kv) |logical_token| {
        const logical_page = logical_token / page_size;
        const physical_token = @as(usize, block_table[logical_page]) + logical_token % page_size;
        @memcpy(key[physical_token * row_values ..][0..row_values], logical_key[logical_token * row_values ..][0..row_values]);
        @memcpy(v[physical_token * row_values ..][0..row_values], logical_v[logical_token * row_values ..][0..row_values]);
    }

    const expected = try allocator.alloc(f32, q_len * nh * hd);
    defer allocator.free(expected);
    try referenceFlashPrefill(allocator, q, logical_key, logical_v, shape, expected);

    const actual = try allocator.alloc(f32, q_len * nh * hd);
    defer allocator.free(actual);
    @memset(actual, 0);

    const kernel_name_z = try allocator.dupeSentinel(u8, check.kernel_name, 0);
    defer allocator.free(kernel_name_z);
    var elapsed_nanos: u64 = 0;
    const rc = termite_metal_run_generated_flash_prefill_check(
        check.source.ptr,
        check.source.len,
        kernel_name_z.ptr,
        q.ptr,
        q.len,
        key.ptr,
        key.len,
        v.ptr,
        v.len,
        block_table.ptr,
        block_table.len,
        actual.ptr,
        actual.len,
        @intCast(q_len),
        @intCast(kv),
        @intCast(nh),
        @intCast(nkv),
        @intCast(hd),
        @intCast(shape.query_position_offset),
        0, // kv_position_offset
        @intCast(shape.sliding_window),
        @intCast(page_size),
        0, // contiguous_base_token
        @intFromBool(!shape.permuted_pages and shape.physical_page_rotation % page_count == 0),
        check.key_chunk,
        check.threads_per_threadgroup,
        check.threadgroup_memory_bytes,
        check.warmup_iters,
        check.measure_iters,
        &elapsed_nanos,
    );
    if (rc != 0) {
        std.debug.print("generated Metal flash prefill {s} failed rc={d}\n", .{ check.name, rc });
        return error.GeneratedMetalKernelFailed;
    }

    var max_error: f32 = 0.0;
    for (actual, expected, 0..) |got, want, index| {
        if (!std.math.isFinite(got)) {
            std.debug.print("generated Metal flash prefill {s} nonfinite at {d}: got={d} want={d}\n", .{ check.name, index, got, want });
            return error.GeneratedMetalKernelMismatch;
        }
        const diff = @abs(got - want);
        max_error = @max(max_error, diff);
        if (diff > check.tolerance) {
            std.debug.print("generated Metal flash prefill {s} mismatch at {d}: got={d} want={d} diff={d}\n", .{ check.name, index, got, want, diff });
            return error.GeneratedMetalKernelMismatch;
        }
    }
    return .{ .max_error = max_error, .measure_iters = check.measure_iters, .elapsed_nanos = elapsed_nanos };
}

fn splitGqaVariantCap(variant: SplitGqaVariant) usize {
    return switch (variant) {
        .auto, .s32_k32_r256 => 32,
        .s8_k32_r256 => 8,
        .s16_k32_r256 => 16,
        .s24_k32_r256 => 24,
    };
}

fn splitGqaResolvedVariant(variant: SplitGqaVariant) SplitGqaVariant {
    return if (variant == .auto) .s32_k32_r256 else variant;
}

fn splitGqaVariantEnvValue(variant: SplitGqaVariant) [*:0]const u8 {
    return switch (variant) {
        .auto => "auto",
        .s8_k32_r256 => "s8-k32-r256",
        .s16_k32_r256 => "s16-k32-r256",
        .s24_k32_r256 => "s24-k32-r256",
        .s32_k32_r256 => "s32-k32-r256",
    };
}

fn runSplitGqaPolicyProbeChecks() !void {
    const shapes = [_]struct { head_dim: usize, sliding_window: usize }{
        .{ .head_dim = 256, .sliding_window = 512 },
        .{ .head_dim = 512, .sliding_window = 0 },
    };
    var checked: usize = 0;
    for (split_gqa_policy_variants) |requested| {
        for (shapes) |shape| {
            for ([_]usize{ 1, 2 }) |q_len| {
                for ([_]usize{ 1, 2 }) |num_kv_heads| {
                    for (split_gqa_policy_kv_boundaries) |kv_tokens| {
                        var resolved: u32 = 99;
                        var split_count: u32 = 99;
                        var scratch_bytes: usize = 99;
                        const rc = termite_metal_decode_gqa_split_policy_probe(
                            @backingInt(requested),
                            q_len,
                            kv_tokens,
                            8,
                            num_kv_heads,
                            shape.head_dim,
                            shape.sliding_window,
                            &resolved,
                            &split_count,
                            &scratch_bytes,
                        );
                        if (kv_tokens < 512) {
                            if (rc != 0 or resolved != @backingInt(SplitGqaVariant.auto) or
                                split_count != 0 or scratch_bytes != 0)
                            {
                                std.debug.print(
                                    "split GQA policy unsupported mismatch variant={s} q={d} kv={d} hd={d} rc={d} resolved={d} splits={d} scratch={d}\n",
                                    .{ @tagName(requested), q_len, kv_tokens, shape.head_dim, rc, resolved, split_count, scratch_bytes },
                                );
                                return error.GeneratedMetalKernelMismatch;
                            }
                        } else {
                            const expected_variant = splitGqaResolvedVariant(requested);
                            const expected_splits: usize = @min(splitGqaVariantCap(requested), (kv_tokens + 31) / 32);
                            const expected_scratch = q_len * 8 * expected_splits * (shape.head_dim + 2) * @sizeOf(f32);
                            if (rc != 1 or resolved != @backingInt(expected_variant) or
                                split_count != @as(u32, @intCast(expected_splits)) or scratch_bytes != expected_scratch)
                            {
                                std.debug.print(
                                    "split GQA policy mismatch variant={s} q={d} kv={d} hd={d} rc={d} resolved={d}/{d} splits={d}/{d} scratch={d}/{d}\n",
                                    .{ @tagName(requested), q_len, kv_tokens, shape.head_dim, rc, resolved, @backingInt(expected_variant), split_count, expected_splits, scratch_bytes, expected_scratch },
                                );
                                return error.GeneratedMetalKernelMismatch;
                            }
                        }
                        checked += 1;
                    }
                }
            }
        }
    }

    const a4b_shapes = [_]struct { num_kv_heads: usize, head_dim: usize, sliding_window: usize }{
        .{ .num_kv_heads = 8, .head_dim = 256, .sliding_window = 1024 },
        .{ .num_kv_heads = 2, .head_dim = 512, .sliding_window = 0 },
    };
    for (split_gqa_policy_variants) |requested| {
        for (a4b_shapes) |shape| {
            for ([_]usize{ 1, 2 }) |q_len| {
                for (split_gqa_policy_kv_boundaries) |kv_tokens| {
                    var resolved: u32 = 99;
                    var split_count: u32 = 99;
                    var scratch_bytes: usize = 99;
                    const rc = termite_metal_decode_gqa_split_policy_probe(
                        @backingInt(requested),
                        q_len,
                        kv_tokens,
                        16,
                        shape.num_kv_heads,
                        shape.head_dim,
                        shape.sliding_window,
                        &resolved,
                        &split_count,
                        &scratch_bytes,
                    );
                    if (kv_tokens < 512) {
                        if (rc != 0 or resolved != @backingInt(SplitGqaVariant.auto) or
                            split_count != 0 or scratch_bytes != 0)
                        {
                            return error.GeneratedMetalKernelMismatch;
                        }
                    } else {
                        const expected_variant = splitGqaResolvedVariant(requested);
                        const expected_splits: usize = @min(splitGqaVariantCap(requested), (kv_tokens + 31) / 32);
                        const expected_scratch = q_len * 16 * expected_splits * (shape.head_dim + 2) * @sizeOf(f32);
                        if (rc != 1 or resolved != @backingInt(expected_variant) or
                            split_count != @as(u32, @intCast(expected_splits)) or scratch_bytes != expected_scratch)
                        {
                            std.debug.print(
                                "split GQA A4B policy mismatch variant={s} q={d} kv={d} hd={d} kv_heads={d} rc={d} resolved={d}/{d} splits={d}/{d} scratch={d}/{d}\n",
                                .{ @tagName(requested), q_len, kv_tokens, shape.head_dim, shape.num_kv_heads, rc, resolved, @backingInt(expected_variant), split_count, expected_splits, scratch_bytes, expected_scratch },
                            );
                            return error.GeneratedMetalKernelMismatch;
                        }
                    }
                    checked += 1;
                }
            }
        }
    }

    const unsupported = [_]struct {
        q_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        sliding_window: usize,
    }{
        .{ .q_len = 0, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .sliding_window = 0 },
        .{ .q_len = 3, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .sliding_window = 0 },
        .{ .q_len = 1, .num_heads = 16, .num_kv_heads = 4, .head_dim = 512, .sliding_window = 0 },
        .{ .q_len = 1, .num_heads = 8, .num_kv_heads = 4, .head_dim = 512, .sliding_window = 0 },
        .{ .q_len = 1, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .sliding_window = 0 },
        .{ .q_len = 1, .num_heads = 8, .num_kv_heads = 2, .head_dim = 512, .sliding_window = 512 },
        .{ .q_len = 1, .num_heads = 8, .num_kv_heads = 2, .head_dim = 384, .sliding_window = 0 },
    };
    for (unsupported) |shape| {
        var resolved: u32 = 99;
        var split_count: u32 = 99;
        var scratch_bytes: usize = 99;
        const rc = termite_metal_decode_gqa_split_policy_probe(
            @backingInt(SplitGqaVariant.auto),
            shape.q_len,
            512,
            shape.num_heads,
            shape.num_kv_heads,
            shape.head_dim,
            shape.sliding_window,
            &resolved,
            &split_count,
            &scratch_bytes,
        );
        if (rc != 0 or resolved != @backingInt(SplitGqaVariant.auto) or
            split_count != 0 or scratch_bytes != 0) return error.GeneratedMetalKernelMismatch;
    }

    var invalid_resolved: u32 = 99;
    var invalid_split_count: u32 = 99;
    var invalid_scratch_bytes: usize = 99;
    const invalid_rc = termite_metal_decode_gqa_split_policy_probe(
        99,
        1,
        512,
        8,
        2,
        512,
        0,
        &invalid_resolved,
        &invalid_split_count,
        &invalid_scratch_bytes,
    );
    if (invalid_rc != -2 or invalid_resolved != @backingInt(SplitGqaVariant.auto) or
        invalid_split_count != 0 or invalid_scratch_bytes != 0) return error.GeneratedMetalKernelMismatch;

    std.debug.print(
        "quant-kernel-metal-runtime-check decode_gqa_split_policy_probe ok cases={d} unsupported={d} tolerance={d:.4} op_kind=attention_decode_gqa_split_policy\n",
        .{ checked, unsupported.len + 1, split_gqa_tensor_tolerance },
    );
}

fn splitGqaShapeIndex(shape: FlashPrefillShape) !usize {
    if (shape.head_dim == 256 and (shape.sliding_window == 512 or shape.sliding_window == 1024)) return 0;
    if (shape.head_dim == 512 and shape.sliding_window == 0) return 1;
    return error.InvalidArgument;
}

fn splitGqaScheduleSnapshot(
    runtime: *RawMetalDecodeRuntime,
) !metal_runtime.RawDecodeGqaSplitScheduleStats {
    var snapshot: metal_runtime.RawDecodeGqaSplitScheduleStats = .{};
    if (termite_metal_decode_runtime_decode_gqa_split_schedule_snapshot(runtime, &snapshot) != 0) {
        return error.MetalRuntimeUnavailable;
    }
    return snapshot;
}

fn validateSplitGqaScheduleDelta(
    check: SplitGqaCheckCase,
    before: metal_runtime.RawDecodeGqaSplitScheduleStats,
    after: metal_runtime.RawDecodeGqaSplitScheduleStats,
    expected_variant: ?SplitGqaVariant,
) !void {
    if (after.fallback_calls != before.fallback_calls or
        after.invalid_override_count != before.invalid_override_count)
    {
        std.debug.print(
            "split GQA schedule diagnostics changed during {s}: fallbacks={d}->{d} invalid={d}->{d}\n",
            .{ check.name, before.fallback_calls, after.fallback_calls, before.invalid_override_count, after.invalid_override_count },
        );
        return error.GeneratedMetalKernelMismatch;
    }
    const expected_shape = try splitGqaShapeIndex(check.shape);
    const expected_variant_index: ?usize = if (expected_variant) |variant|
        @as(usize, @intCast(@backingInt(splitGqaResolvedVariant(variant)) - 1))
    else
        null;
    var total_delta: u64 = 0;
    for (0..2) |shape_index| {
        for (0..4) |variant_index| {
            const old = before.calls[shape_index][variant_index];
            const new = after.calls[shape_index][variant_index];
            if (new < old) return error.GeneratedMetalKernelMismatch;
            const delta = new - old;
            const expected_delta: u64 = @intFromBool(
                expected_variant_index != null and
                    shape_index == expected_shape and
                    variant_index == expected_variant_index.?,
            );
            if (delta != expected_delta) {
                std.debug.print(
                    "split GQA schedule one-hot mismatch {s}: shape={d} variant={d} delta={d} expected={d}\n",
                    .{ check.name, shape_index, variant_index, delta, expected_delta },
                );
                return error.GeneratedMetalKernelMismatch;
            }
            total_delta += delta;
        }
    }
    if (total_delta != @as(u64, @intFromBool(expected_variant != null))) {
        return error.GeneratedMetalKernelMismatch;
    }
}

const SplitGqaCheckResult = struct {
    max_error: f32,
    route_calls: u64,
};

fn runSplitGqaCheck(
    allocator: std.mem.Allocator,
    runtime: *RawMetalDecodeRuntime,
    check: SplitGqaCheckCase,
    expected_variant: ?SplitGqaVariant,
) !SplitGqaCheckResult {
    const shape = check.shape;
    const q_len = shape.q_len;
    const kv = shape.kv_tokens;
    const nh = shape.num_heads;
    const nkv = shape.num_kv_heads;
    const hd = shape.head_dim;
    const q_width = nh * hd;
    const kv_width = nkv * hd;
    const page_size = shape.page_size;
    const f16_kv_format: u32 = 3;
    const slot: usize = 0;
    const key_row_bytes = kv_width * @sizeOf(u16);

    const q = try allocator.alloc(f32, q_len * q_width);
    defer allocator.free(q);
    for (q, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 17)) - 8)) / 32.0;
    }

    const logical_key = try allocator.alloc(u16, kv * kv_width);
    defer allocator.free(logical_key);
    const key_source = try allocator.alloc(f32, logical_key.len);
    defer allocator.free(key_source);
    for (logical_key, key_source, 0..) |*bits, *source, i| {
        bits.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + 1) % 23)) - 11)) / 40.0);
        source.* = halfBitsToF32(bits.*);
    }
    const logical_v = try allocator.alloc(u16, kv * kv_width);
    defer allocator.free(logical_v);
    const v_source = try allocator.alloc(f32, logical_v.len);
    defer allocator.free(v_source);
    for (logical_v, v_source, 0..) |*bits, *source, i| {
        bits.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast((i * 3 + 5) % 19)) - 9)) / 24.0);
        source.* = halfBitsToF32(bits.*);
    }

    const page_count = (kv + page_size - 1) / page_size;
    const block_table = try allocator.alloc(u32, page_count);
    defer allocator.free(block_table);
    var physical_tokens: usize = 0;
    for (block_table, 0..) |*offset, logical_page| {
        const page_within_span = physicalPageWithinSpan(shape, page_count, logical_page);
        const physical_page = shape.physical_page_bias + page_within_span;
        const physical_offset = physical_page * page_size;
        offset.* = @intCast(physical_offset);
        physical_tokens = @max(physical_tokens, physical_offset + page_size);
    }

    const expected = try allocator.alloc(f32, q.len);
    defer allocator.free(expected);
    try referenceFlashPrefill(allocator, q, logical_key, logical_v, shape, expected);
    const actual = try allocator.alloc(f32, q.len);
    defer allocator.free(actual);

    const q_buffer = termite_metal_buffer_alloc(runtime, q.len * @sizeOf(f32), metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(q_buffer);
    const key_source_buffer = termite_metal_buffer_alloc(runtime, key_source.len * @sizeOf(f32), metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(key_source_buffer);
    const v_source_buffer = termite_metal_buffer_alloc(runtime, v_source.len * @sizeOf(f32), metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(v_source_buffer);
    const output_buffer = termite_metal_buffer_alloc(runtime, actual.len * @sizeOf(f32), metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(output_buffer);
    if (termite_metal_buffer_upload(runtime, q_buffer, 0, q.ptr, q.len * @sizeOf(f32)) != 0 or
        termite_metal_buffer_upload(runtime, key_source_buffer, 0, key_source.ptr, key_source.len * @sizeOf(f32)) != 0 or
        termite_metal_buffer_upload(runtime, v_source_buffer, 0, v_source.ptr, v_source.len * @sizeOf(f32)) != 0)
    {
        return error.GeneratedMetalKernelFailed;
    }

    if (termite_metal_decode_runtime_reset_attention_span_slot(runtime, slot) != 0 or
        termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
            runtime,
            slot,
            f16_kv_format,
            physical_tokens,
            key_row_bytes,
            kv_width,
        ) != 0)
    {
        return error.GeneratedMetalKernelFailed;
    }

    var encoded_key_handle: ?*anyopaque = null;
    var encoded_key_capacity: usize = 0;
    var v_handle: ?*anyopaque = null;
    var v_capacity: usize = 0;
    if (termite_metal_decode_runtime_attention_span_slot_info(
        runtime,
        slot,
        &encoded_key_handle,
        &encoded_key_capacity,
        &v_handle,
        &v_capacity,
        null,
        null,
        null,
        null,
    ) != 0 or encoded_key_handle == null or v_handle == null) {
        return error.GeneratedMetalKernelFailed;
    }

    const poison = try allocator.alloc(u16, physical_tokens * kv_width);
    defer allocator.free(poison);
    @memset(poison, 0x7e00);
    const poison_bytes = poison.len * @sizeOf(u16);
    if (encoded_key_capacity < poison_bytes or v_capacity < poison_bytes or
        termite_metal_buffer_upload(runtime, encoded_key_handle, 0, poison.ptr, poison_bytes) != 0 or
        termite_metal_buffer_upload(runtime, v_handle, 0, poison.ptr, poison_bytes) != 0)
    {
        return error.GeneratedMetalKernelFailed;
    }

    const seed_rc = termite_metal_decode_runtime_update_attention_paged_from_f32_key_device_slot(
        runtime,
        slot,
        f16_kv_format,
        key_source_buffer,
        0,
        v_source_buffer,
        0,
        kv,
        kv,
        nkv,
        hd,
        key_row_bytes,
        key_row_bytes,
        kv_width,
        0,
        block_table.ptr,
        block_table.len,
        page_size,
    );
    if (seed_rc != 0) {
        std.debug.print("split GQA seed failed {s} rc={d}\n", .{ check.name, seed_rc });
        return error.GeneratedMetalKernelFailed;
    }

    const calls_before = termite_metal_decode_runtime_decode_gqa_split_calls(runtime);
    const schedule_before = try splitGqaScheduleSnapshot(runtime);
    const attention_rc = termite_metal_decode_runtime_attention_paged_slot_device(
        runtime,
        slot,
        f16_kv_format,
        q_buffer,
        0,
        block_table.ptr,
        block_table.len,
        page_size,
        q_len,
        kv,
        nh,
        nkv,
        hd,
        key_row_bytes,
        key_row_bytes,
        shape.query_position_offset,
        0,
        shape.sliding_window,
        0.0,
        null,
        0,
        output_buffer,
        0,
    );
    if (attention_rc != 0) {
        std.debug.print("split GQA attention failed {s} rc={d}\n", .{ check.name, attention_rc });
        return error.GeneratedMetalKernelFailed;
    }
    const calls_after = termite_metal_decode_runtime_decode_gqa_split_calls(runtime);
    const schedule_after = try splitGqaScheduleSnapshot(runtime);
    try validateSplitGqaScheduleDelta(check, schedule_before, schedule_after, expected_variant);
    const expected_calls: u64 = @intFromBool(expected_variant != null);
    if (calls_after != calls_before + expected_calls) {
        std.debug.print("split GQA route mismatch {s}: expected={d} before={d} after={d}\n", .{ check.name, expected_calls, calls_before, calls_after });
        return error.GeneratedMetalKernelMismatch;
    }
    if (termite_metal_buffer_download(runtime, output_buffer, 0, actual.ptr, actual.len * @sizeOf(f32)) != 0) {
        return error.GeneratedMetalKernelFailed;
    }

    var max_error: f32 = 0.0;
    for (actual, expected, 0..) |got, want, index| {
        if (!std.math.isFinite(got)) {
            std.debug.print("split GQA {s} nonfinite at {d}: got={d} want={d}\n", .{ check.name, index, got, want });
            return error.GeneratedMetalKernelMismatch;
        }
        const diff = @abs(got - want);
        max_error = @max(max_error, diff);
        if (diff > split_gqa_tensor_tolerance) {
            std.debug.print("split GQA {s} mismatch at {d}: got={d} want={d} diff={d}\n", .{ check.name, index, got, want, diff });
            return error.GeneratedMetalKernelMismatch;
        }
    }
    return .{ .max_error = max_error, .route_calls = calls_after - calls_before };
}

fn runSplitGqaConcreteVariantChecks(
    allocator: std.mem.Allocator,
    variant: SplitGqaVariant,
) !void {
    const runtime = termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.MetalRuntimeUnavailable;
    for (split_gqa_checks) |check| {
        const result = try runSplitGqaCheck(allocator, runtime, check, variant);
        std.debug.print(
            "quant-kernel-metal-runtime-check {s} ok variant={s} max_abs_error={d:.7} tolerance={d:.4} route_calls={d} op_kind=attention_decode_gqa_split\n",
            .{ check.name, @tagName(variant), result.max_error, split_gqa_tensor_tolerance, result.route_calls },
        );
    }
}

fn runSplitGqaChecks(allocator: std.mem.Allocator) !void {
    const enable_env = "TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT";
    const disable_env = "TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT";
    const swa_variant_env = "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT";
    const global_variant_env = "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT";
    const a4b_enable_env = "TERMITE_METAL_ENABLE_A4B_DECODE_GQA_SPLIT";
    const old_enable = if (std.c.getenv(enable_env)) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_enable) |value| allocator.free(value);
    const old_disable = if (std.c.getenv(disable_env)) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_disable) |value| allocator.free(value);
    const old_swa_variant = if (std.c.getenv(swa_variant_env)) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_swa_variant) |value| allocator.free(value);
    const old_global_variant = if (std.c.getenv(global_variant_env)) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_global_variant) |value| allocator.free(value);
    const old_a4b_enable = if (std.c.getenv(a4b_enable_env)) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_a4b_enable) |value| allocator.free(value);
    defer if (old_enable) |value| {
        _ = setenv(enable_env, value.ptr, 1);
    } else {
        _ = unsetenv(enable_env);
    };
    defer if (old_disable) |value| {
        _ = setenv(disable_env, value.ptr, 1);
    } else {
        _ = unsetenv(disable_env);
    };
    defer if (old_swa_variant) |value| {
        _ = setenv(swa_variant_env, value.ptr, 1);
    } else {
        _ = unsetenv(swa_variant_env);
    };
    defer if (old_global_variant) |value| {
        _ = setenv(global_variant_env, value.ptr, 1);
    } else {
        _ = unsetenv(global_variant_env);
    };
    defer if (old_a4b_enable) |value| {
        _ = setenv(a4b_enable_env, value.ptr, 1);
    } else {
        _ = unsetenv(a4b_enable_env);
    };

    try runSplitGqaPolicyProbeChecks();
    if (setenv(enable_env, "1", 1) != 0 or unsetenv(disable_env) != 0 or
        setenv(a4b_enable_env, "1", 1) != 0) return error.MetalRuntimeUnavailable;
    for (split_gqa_concrete_variants) |variant| {
        const env_value = splitGqaVariantEnvValue(variant);
        if (setenv(swa_variant_env, env_value, 1) != 0 or
            setenv(global_variant_env, env_value, 1) != 0) return error.MetalRuntimeUnavailable;
        try runSplitGqaConcreteVariantChecks(allocator, variant);
    }

    // Invalid shape-specific overrides are captured once at runtime creation,
    // counted, and resolved conservatively to AUTO (the production s32 route).
    if (setenv(swa_variant_env, "not-a-split-schedule", 1) != 0 or
        setenv(global_variant_env, "auto", 1) != 0) return error.MetalRuntimeUnavailable;
    {
        const invalid_override_runtime = termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
        defer termite_metal_decode_runtime_destroy(invalid_override_runtime);
        if (termite_metal_decode_runtime_ready(invalid_override_runtime) == 0) return error.MetalRuntimeUnavailable;
        const before = try splitGqaScheduleSnapshot(invalid_override_runtime);
        if (before.invalid_override_count != 1 or before.fallback_calls != 0) {
            std.debug.print(
                "split GQA invalid override capture mismatch: invalid={d} fallbacks={d}\n",
                .{ before.invalid_override_count, before.fallback_calls },
            );
            return error.GeneratedMetalKernelMismatch;
        }
        const invalid_override_check = split_gqa_checks[0];
        const result = try runSplitGqaCheck(
            allocator,
            invalid_override_runtime,
            invalid_override_check,
            .s32_k32_r256,
        );
        const after = try splitGqaScheduleSnapshot(invalid_override_runtime);
        if (after.invalid_override_count != 1 or after.fallback_calls != 0) {
            return error.GeneratedMetalKernelMismatch;
        }
        std.debug.print(
            "quant-kernel-metal-runtime-check decode_gqa_split_invalid_override_auto ok max_abs_error={d:.7} invalid_overrides={d} route_calls={d} resolved_variant=s32 op_kind=attention_decode_gqa_split_policy\n",
            .{ result.max_error, after.invalid_override_count, result.route_calls },
        );
    }

    if (unsetenv(enable_env) != 0 or unsetenv(disable_env) != 0 or
        unsetenv(swa_variant_env) != 0 or unsetenv(global_variant_env) != 0 or
        unsetenv(a4b_enable_env) != 0)
    {
        return error.MetalRuntimeUnavailable;
    }
    const production_runtime = termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_decode_runtime_destroy(production_runtime);
    if (termite_metal_decode_runtime_ready(production_runtime) == 0) return error.MetalRuntimeUnavailable;
    for (split_gqa_production_selection_checks) |selection| {
        const expected_variant: ?SplitGqaVariant = if (selection.expect_route) .s32_k32_r256 else null;
        const result = try runSplitGqaCheck(allocator, production_runtime, selection.check, expected_variant);
        std.debug.print(
            "quant-kernel-metal-runtime-check {s} ok max_abs_error={d:.7} route_calls={d} expected_route={} op_kind=attention_decode_gqa_split_policy\n",
            .{ selection.check.name, result.max_error, result.route_calls, selection.expect_route },
        );
    }

    if (setenv(enable_env, "1", 1) != 0 or setenv(disable_env, "1", 1) != 0 or
        setenv(swa_variant_env, "s8-k32-r256", 1) != 0 or
        setenv(global_variant_env, "s32-k32-r256", 1) != 0) return error.MetalRuntimeUnavailable;
    const disabled_runtime = termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_decode_runtime_destroy(disabled_runtime);
    if (termite_metal_decode_runtime_ready(disabled_runtime) == 0) return error.MetalRuntimeUnavailable;
    const disabled_check = split_gqa_checks[2];
    const disabled_result = try runSplitGqaCheck(allocator, disabled_runtime, disabled_check, null);
    std.debug.print(
        "quant-kernel-metal-runtime-check decode_gqa_split_disable_rollback ok max_abs_error={d:.7} route_calls={d} expected_route=false op_kind=attention_decode_gqa_split_policy\n",
        .{ disabled_result.max_error, disabled_result.route_calls },
    );
    const paged_result = try runSplitGqaCheck(allocator, disabled_runtime, paged_local_ragged_check, null);
    std.debug.print(
        "quant-kernel-metal-runtime-check {s} ok max_abs_error={d:.7} route_calls={d} expected_route=false op_kind=attention_paged_1x\n",
        .{ paged_local_ragged_check.name, paged_result.max_error, paged_result.route_calls },
    );
}

fn metalRuntimeCheckCount() comptime_int {
    var count: comptime_int = 0;
    for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == .metal) count += 2;
    }
    return count;
}

fn buildMetalRuntimeChecks() [metal_runtime_check_count]CheckCase {
    var checks: [metal_runtime_check_count]CheckCase = undefined;
    var index: usize = 0;
    inline for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == .metal) {
            checks[index] = metalRuntimeCheckForArtifactShape(artifact, .small);
            index += 1;
            checks[index] = metalRuntimeCheckForArtifactShape(artifact, .wide);
            index += 1;
        }
    }
    return checks;
}

fn metalRuntimeCheckForArtifactShape(comptime artifact: quant_kernel_compiler.GeneratedMatmulArtifact, comptime shape: RuntimeCheckShape) CheckCase {
    const dims = metalRuntimeDimsForArtifact(artifact, shape);
    return .{
        .name = metalRuntimeCheckName(artifact, shape),
        .source = quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse @compileError("missing generated Metal source"),
        .kernel_name = artifact.kernel_id,
        .format = artifact.format,
        .epilogue = artifact.epilogue,
        .rows = dims.rows,
        .in_dim = dims.in_dim,
        .out_dim = dims.out_dim,
        .threads_per_threadgroup = @intCast(quant_kernel_compiler.metalGeneratedThreadsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue)),
        .cols_per_threadgroup = @intCast(quant_kernel_compiler.metalGeneratedColsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue)),
        .tolerance = dims.tolerance_abs,
    };
}

fn metalRuntimeCheckName(comptime artifact: quant_kernel_compiler.GeneratedMatmulArtifact, comptime shape: RuntimeCheckShape) []const u8 {
    return quant_kernel_compiler.metalBenchmarkCaseName(artifact, shape);
}

fn metalRuntimeDimsForArtifact(comptime artifact: quant_kernel_compiler.GeneratedMatmulArtifact, comptime shape: RuntimeCheckShape) RuntimeCheckDims {
    return quant_kernel_compiler.metalBenchmarkDimsForArtifact(artifact, shape);
}

test "quant kernel metal runtime checks cover generated Metal artifacts" {
    var artifact_count: usize = 0;
    for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        artifact_count += 1;

        var match_count: usize = 0;
        var has_small = false;
        var has_wide = false;
        for (metal_runtime_checks) |check| {
            if (!std.mem.eql(u8, artifact.kernel_id, check.kernel_name)) continue;
            match_count += 1;
            if (check.rows == 3 or check.rows == 4) has_small = true;
            if (check.rows == 8 and check.out_dim == 7) has_wide = true;
            try std.testing.expectEqual(artifact.format, check.format);
            try std.testing.expectEqual(artifact.row_bucket, quant_matmul.rowBucket(check.rows));
            try std.testing.expectEqual(artifact.epilogue, check.epilogue);
            try std.testing.expectEqual(quant_kernel_compiler.metalGeneratedThreadsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue), check.threads_per_threadgroup);
            try std.testing.expectEqual(quant_kernel_compiler.metalGeneratedColsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue), check.cols_per_threadgroup);
            const evidence_source_path = generatedSourcePathFor(check);
            const evidence_check_command = metalCheckCommandFor(check);
            if (quant_kernel_compiler.artifactHasPromotionEvidence(artifact)) {
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, artifact.source_path));
                try std.testing.expect(std.mem.containsAtLeast(u8, evidence_check_command, 1, evidence_source_path));
            } else {
                try std.testing.expectEqualStrings(artifact.source_path, evidence_source_path);
                try std.testing.expectEqualStrings(artifact.check_command, evidence_check_command);
            }
            try std.testing.expectEqualStrings(quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse return error.MissingGeneratedSource, check.source);
        }
        try std.testing.expectEqual(@as(usize, 2), match_count);
        try std.testing.expect(has_small);
        try std.testing.expect(has_wide);
    }
    try std.testing.expect(artifact_count > 0);
    try std.testing.expectEqual(artifact_count * 2, metal_runtime_checks.len);

    for (metal_runtime_checks) |check| {
        var found = false;
        for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
            if (artifact.backend != .metal) continue;
            if (!std.mem.eql(u8, artifact.kernel_id, check.kernel_name)) continue;
            found = true;
            try std.testing.expectEqual(artifact.format, check.format);
            try std.testing.expectEqual(artifact.row_bucket, quant_matmul.rowBucket(check.rows));
            try std.testing.expectEqual(artifact.epilogue, check.epilogue);
            try std.testing.expectEqual(quant_kernel_compiler.metalGeneratedThreadsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue), check.threads_per_threadgroup);
            try std.testing.expectEqual(quant_kernel_compiler.metalGeneratedColsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue), check.cols_per_threadgroup);
            const evidence_source_path = generatedSourcePathFor(check);
            const evidence_check_command = metalCheckCommandFor(check);
            if (quant_kernel_compiler.artifactHasPromotionEvidence(artifact)) {
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, artifact.source_path));
                try std.testing.expect(std.mem.containsAtLeast(u8, evidence_check_command, 1, evidence_source_path));
            } else {
                try std.testing.expectEqualStrings(artifact.source_path, evidence_source_path);
                try std.testing.expectEqualStrings(artifact.check_command, evidence_check_command);
            }
            try std.testing.expectEqualStrings(quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse return error.MissingGeneratedSource, check.source);
        }
        try std.testing.expect(found);
    }
}

test "quant kernel metal runtime microkernel checks cover generated microkernel artifacts" {
    // The matmul array stays microkernel-free; microkernel checks are their own
    // set (one per shape) and never leak into `metal_runtime_checks`.
    for (metal_runtime_checks) |check| {
        try std.testing.expectEqual(quant_kernel_compiler.OpKind.small_batch_matmul, check.op_kind);
    }
    try std.testing.expect(microkernel_runtime_checks.len > 0);
    try std.testing.expectEqual(
        quant_kernel_compiler.first_generated_microkernel_artifacts.len * rms_norm_shapes.len,
        microkernel_runtime_checks.len,
    );
    for (quant_kernel_compiler.first_generated_microkernel_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        var match_count: usize = 0;
        for (microkernel_runtime_checks) |check| {
            if (!std.mem.eql(u8, artifact.kernel_id, check.kernel_name)) continue;
            match_count += 1;
            try std.testing.expectEqual(quant_kernel_compiler.OpKind.microkernel, check.op_kind);
            // rows=n, in_dim=out_dim=d so input/output are both n*d and weight is d.
            try std.testing.expectEqual(check.in_dim, check.out_dim);
            try std.testing.expect(check.threads_per_threadgroup >= 2);
            try std.testing.expectEqual(@as(u32, 1), check.cols_per_threadgroup);
            try std.testing.expect(check.eps > 0.0);
            try std.testing.expectEqualStrings(
                quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse return error.MissingGeneratedSource,
                check.source,
            );
        }
        try std.testing.expectEqual(rms_norm_shapes.len, match_count);
    }
}

test "quant kernel metal runtime RMSNorm CPU oracle matches activations.rmsNorm" {
    const allocator = std.testing.allocator;
    const n: usize = 2;
    const d: usize = 8;
    var input = [_]f32{ 0.1, -0.2, 0.3, -0.4, 0.5, -0.6, 0.7, -0.8, 1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3 };
    const weight = [_]f32{ 1.0, 0.5, 2.0, 1.5, 0.25, 1.25, 0.75, 1.75 };
    const eps: f32 = 1e-6;

    var out = @as([(n * d)]f32, @splat(0));
    try referenceRmsNorm(allocator, &input, &weight, n, d, eps, &out);

    // Independent scalar reference: out[r,i] = in[r,i] * rsqrt(mean(in^2)+eps) * w[i].
    for (0..n) |r| {
        var sum_sq: f32 = 0;
        for (0..d) |i| sum_sq += input[r * d + i] * input[r * d + i];
        const inv = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(d)) + eps);
        for (0..d) |i| {
            const want = input[r * d + i] * inv * weight[i];
            try std.testing.expectApproxEqAbs(want, out[r * d + i], 1e-6);
        }
    }
}

test "quant kernel metal runtime attention checks cover generated decode attention artifacts" {
    try std.testing.expect(attention_runtime_checks.len > 0);
    const decode_artifacts = comptime blk: {
        var count: usize = 0;
        for (quant_kernel_compiler.first_generated_attention_artifacts) |artifact| {
            if (isDecodeAttentionArtifact(artifact)) count += 1;
        }
        break :blk count;
    };
    // The decode harness covers only the decode-1x route; flash-prefill is covered
    // by the separate flash harness (different grid / threads / shmem).
    try std.testing.expectEqual(decode_artifacts * attention_shapes.len, attention_runtime_checks.len);
    inline for (quant_kernel_compiler.first_generated_attention_artifacts) |artifact| {
        if (comptime isDecodeAttentionArtifact(artifact)) {
            var match_count: usize = 0;
            for (attention_runtime_checks) |check| {
                if (!std.mem.eql(u8, artifact.kernel_id, check.kernel_name)) continue;
                match_count += 1;
                // NT must match the schedule (and the hand-written dispatch); the
                // shape must be GQA-valid and never all-masked (query after all KV).
                try std.testing.expectEqual(@as(u32, 256), check.threads_per_threadgroup);
                try std.testing.expect(check.shape.num_kv_heads > 0);
                try std.testing.expectEqual(@as(usize, 0), check.shape.num_heads % check.shape.num_kv_heads);
                try std.testing.expect(check.shape.query_position_offset + 1 >= check.shape.kv_tokens);
                try std.testing.expectEqualStrings(
                    quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse return error.MissingGeneratedSource,
                    check.source,
                );
            }
            try std.testing.expectEqual(attention_shapes.len, match_count);
        }
    }
}

test "quant kernel metal runtime flash prefill checks cover the generated flash artifact" {
    try std.testing.expect(flash_prefill_checks.len > 0);
    try std.testing.expectEqual(@as(usize, flashPrefillRuntimeCheckCount()), flash_prefill_checks.len);
    for (flash_prefill_checks) |check| {
        // Every case is a valid multi-query GQA prefill tile that never masks all
        // KV (the first query at qpo must see at least token 0).
        try std.testing.expect(check.shape.num_kv_heads > 0);
        try std.testing.expectEqual(@as(usize, 0), check.shape.num_heads % check.shape.num_kv_heads);
        try std.testing.expect(check.shape.head_dim % 32 == 0);
        if (check.shape.head_dim == 512) {
            try std.testing.expectEqual(@as(u32, quant_kernel_compiler.first_prefill_flash_hd512_metal_schedule.key_chunk), check.key_chunk);
            try std.testing.expectEqual(@as(u32, 256), check.threads_per_threadgroup);
            try std.testing.expectEqual(@as(u32, 13_888), check.threadgroup_memory_bytes);
            try std.testing.expectEqual(@as(usize, 16), check.shape.page_size);
            try std.testing.expect(check.shape.permuted_pages);
        } else {
            try std.testing.expectEqual(@as(u32, quant_kernel_compiler.first_prefill_flash_metal_schedule.key_chunk), check.key_chunk);
            try std.testing.expectEqual(@as(u32, 128), check.threads_per_threadgroup);
        }
    }
}

test "quant kernel metal runtime split GQA checks cover production shapes and poisoned ragged pages" {
    try std.testing.expectEqual(@as(usize, 9), split_gqa_checks.len);
    try std.testing.expectEqual(@as(usize, 4), split_gqa_concrete_variants.len);
    try std.testing.expectEqual(@as(usize, 5), split_gqa_policy_variants.len);
    try std.testing.expectEqual(@as(usize, 9), split_gqa_policy_kv_boundaries.len);
    try std.testing.expectEqual(@as(usize, 36), split_gqa_concrete_variants.len * split_gqa_checks.len);
    try std.testing.expectEqual(@as(usize, 540), split_gqa_policy_variants.len * 2 * 2 * 2 * split_gqa_policy_kv_boundaries.len + split_gqa_policy_variants.len * 2 * 2 * split_gqa_policy_kv_boundaries.len);
    try std.testing.expect(split_gqa_tensor_tolerance < 5e-2);
    try std.testing.expectEqual(@as(f32, 1e-2), split_gqa_tensor_tolerance);
    var e2b_count: usize = 0;
    var q1_count: usize = 0;
    var q2_count: usize = 0;
    var hd256_count: usize = 0;
    var hd512_count: usize = 0;
    var long_global_count: usize = 0;
    var a4b_count: usize = 0;
    for (split_gqa_checks) |check| {
        const shape = check.shape;
        if (shape.kv_tokens == 2003) {
            long_global_count += 1;
            try std.testing.expectEqual(@as(usize, 1), shape.q_len);
            try std.testing.expectEqual(@as(usize, 512), shape.head_dim);
            try std.testing.expectEqual(@as(usize, 0), shape.sliding_window);
        } else {
            try std.testing.expectEqual(@as(usize, 513), shape.kv_tokens);
        }
        if (shape.num_heads == 16) {
            a4b_count += 1;
            try std.testing.expect(shape.num_kv_heads == 2 or shape.num_kv_heads == 8);
        } else {
            try std.testing.expectEqual(@as(usize, 8), shape.num_heads);
        }
        if (shape.num_kv_heads == 1) {
            e2b_count += 1;
            try std.testing.expectEqual(@as(usize, 1), shape.q_len);
        } else if (shape.num_heads == 8) {
            try std.testing.expectEqual(@as(usize, 2), shape.num_kv_heads);
        }
        try std.testing.expectEqual(@as(usize, 16), shape.page_size);
        try std.testing.expect(shape.permuted_pages);
        try std.testing.expectEqual(@as(usize, 1), shape.physical_page_bias);
        try std.testing.expectEqual(@as(usize, 0), shape.physical_page_rotation);
        if (shape.q_len == 1) q1_count += 1 else if (shape.q_len == 2) q2_count += 1 else return error.TestUnexpectedResult;
        if (shape.head_dim == 256) {
            hd256_count += 1;
            try std.testing.expectEqual(@as(usize, if (shape.num_heads == 16) 1024 else 512), shape.sliding_window);
        } else if (shape.head_dim == 512) {
            hd512_count += 1;
            try std.testing.expectEqual(@as(usize, 0), shape.sliding_window);
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(shape.kv_tokens - shape.q_len, shape.query_position_offset);
    }
    try std.testing.expectEqual(@as(usize, 2), e2b_count);
    try std.testing.expectEqual(@as(usize, 7), q1_count);
    try std.testing.expectEqual(@as(usize, 2), q2_count);
    try std.testing.expectEqual(@as(usize, 4), hd256_count);
    try std.testing.expectEqual(@as(usize, 5), hd512_count);
    try std.testing.expectEqual(@as(usize, 2), long_global_count);
    try std.testing.expectEqual(@as(usize, 2), a4b_count);
}

test "quant kernel metal runtime split GQA production policy starts at 512 tokens" {
    try std.testing.expectEqual(@as(usize, 12), split_gqa_production_selection_checks.len);
    const e2b_local = split_gqa_production_selection_checks[0];
    try std.testing.expectEqual(@as(usize, 1), e2b_local.check.shape.num_kv_heads);
    try std.testing.expectEqual(@as(usize, 256), e2b_local.check.shape.head_dim);
    try std.testing.expectEqual(@as(usize, 512), e2b_local.check.shape.sliding_window);
    try std.testing.expect(e2b_local.expect_route);
    const e2b_global = split_gqa_production_selection_checks[1];
    try std.testing.expectEqual(@as(usize, 1), e2b_global.check.shape.num_kv_heads);
    try std.testing.expectEqual(@as(usize, 2003), e2b_global.check.shape.kv_tokens);
    try std.testing.expectEqual(@as(usize, 512), e2b_global.check.shape.head_dim);
    try std.testing.expectEqual(@as(usize, 0), e2b_global.check.shape.sliding_window);
    try std.testing.expect(e2b_global.expect_route);
    const expected_kv = [_]usize{ 511, 512, 513, 4095, 4096, 4097 };
    for (expected_kv) |kv_tokens| {
        var found = false;
        for (split_gqa_production_selection_checks) |selection| {
            if (selection.check.shape.num_heads == 8 and selection.check.shape.head_dim == 512 and selection.check.shape.kv_tokens == kv_tokens) {
                found = true;
                try std.testing.expectEqual(kv_tokens >= 512, selection.expect_route);
            }
        }
        try std.testing.expect(found);
    }
    const ring_wrap = split_gqa_production_selection_checks[9];
    try std.testing.expectEqual(@as(usize, 2003), ring_wrap.check.shape.kv_tokens);
    try std.testing.expectEqual(@as(usize, 256), ring_wrap.check.shape.head_dim);
    try std.testing.expectEqual(@as(usize, 512), ring_wrap.check.shape.sliding_window);
    try std.testing.expectEqual(@as(usize, 20), ring_wrap.check.shape.physical_page_rotation);
    try std.testing.expect(!ring_wrap.check.shape.permuted_pages);
    try std.testing.expect(ring_wrap.expect_route);
    const ring_page_count = (ring_wrap.check.shape.kv_tokens + ring_wrap.check.shape.page_size - 1) /
        ring_wrap.check.shape.page_size;
    // The live SWA clamp drops 93 full pages. The retained suffix then crosses
    // the rotated physical ring between logical pages 105 and 106.
    try std.testing.expectEqual(@as(usize, 126), ring_page_count);
    try std.testing.expectEqual(@as(usize, 125), physicalPageWithinSpan(ring_wrap.check.shape, ring_page_count, 105));
    try std.testing.expectEqual(@as(usize, 0), physicalPageWithinSpan(ring_wrap.check.shape, ring_page_count, 106));
    for (split_gqa_production_selection_checks[10..]) |selection| {
        try std.testing.expectEqual(@as(usize, 16), selection.check.shape.num_heads);
        try std.testing.expect(!selection.expect_route);
    }
    try std.testing.expectEqualSlices(
        usize,
        &.{ 511, 512, 513, 1023, 1024, 2003, 4095, 4096, 8191 },
        &split_gqa_policy_kv_boundaries,
    );
}

test "quant kernel metal runtime GQA attention CPU oracle matches an independent reference" {
    const allocator = std.testing.allocator;
    // 4 query heads share 2 KV heads (GQA 2:1), head_dim 4, 3 KV tokens, causal.
    const shape = AttentionShape{ .kv_tokens = 3, .num_heads = 4, .num_kv_heads = 2, .head_dim = 4, .query_position_offset = 2, .sliding_window = 0 };
    const nh = shape.num_heads;
    const nkv = shape.num_kv_heads;
    const hd = shape.head_dim;
    const kv = shape.kv_tokens;

    var q: [nh * hd]f32 = undefined;
    for (&q, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 5)) - 2)) / 8.0;
    var key: [3 * 2 * 4]u16 = undefined;
    for (&key, 0..) |*value, i| value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast(i % 7)) - 3)) / 10.0);
    var v: [3 * 2 * 4]u16 = undefined;
    for (&v, 0..) |*value, i| value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast(i % 6)) - 3)) / 6.0);

    var out: [nh * hd]f32 = undefined;
    try referenceGqaAttention1x(allocator, &q, &key, &v, shape, &out);

    // Independent recomputation (softmax over causal-allowed KV, GQA-mapped, f16).
    const heads_per_group = nh / nkv;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(hd)));
    for (0..nh) |h| {
        const kv_h = h / heads_per_group;
        var s: [kv]f32 = undefined;
        var best: f32 = -3.402823466e+38;
        for (0..kv) |ki| {
            var acc: f32 = 0;
            for (0..hd) |d| acc += q[h * hd + d] * halfBitsToF32(key[ki * nkv * hd + kv_h * hd + d]);
            s[ki] = acc * scale; // all causally allowed (query_pos = kv-1 >= every key_pos)
            best = @max(best, s[ki]);
        }
        var sum: f32 = 0;
        for (0..kv) |ki| {
            s[ki] = @exp(s[ki] - best);
            sum += s[ki];
        }
        for (0..hd) |d| {
            var want: f32 = 0;
            for (0..kv) |ki| want += (s[ki] / sum) * halfBitsToF32(v[ki * nkv * hd + kv_h * hd + d]);
            try std.testing.expectApproxEqAbs(want, out[h * hd + d], 1e-5);
        }
    }
}

test "quant kernel metal runtime GQA attention CPU oracle honours the sliding window" {
    const allocator = std.testing.allocator;
    // Window of 2 over 5 KV tokens with the query at position 4: only tokens
    // 3 and 4 are in-window (query_pos - key_pos < 2), so 0..2 are masked out.
    const shape = AttentionShape{ .kv_tokens = 5, .num_heads = 1, .num_kv_heads = 1, .head_dim = 2, .query_position_offset = 4, .sliding_window = 2 };
    var q = [_]f32{ 0.5, -0.25 };
    var key: [5 * 2]u16 = undefined;
    for (&key, 0..) |*value, i| value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast(i)) - 5)) / 8.0);
    var v: [5 * 2]u16 = undefined;
    for (&v, 0..) |*value, i| value.* = f32ToHalfBits(@as(f32, @floatFromInt(@as(i32, @intCast(i)) - 5)) / 4.0);

    var out: [2]f32 = undefined;
    try referenceGqaAttention1x(allocator, &q, &key, &v, shape, &out);

    // Only ki in {3,4} contribute.
    const scale: f32 = 1.0 / @sqrt(@as(f32, 2.0));
    var s3: f32 = 0;
    var s4: f32 = 0;
    for (0..2) |d| {
        s3 += q[d] * halfBitsToF32(key[3 * 2 + d]);
        s4 += q[d] * halfBitsToF32(key[4 * 2 + d]);
    }
    s3 *= scale;
    s4 *= scale;
    const best = @max(s3, s4);
    const e3 = @exp(s3 - best);
    const e4 = @exp(s4 - best);
    const sum = e3 + e4;
    for (0..2) |d| {
        const want = (e3 / sum) * halfBitsToF32(v[3 * 2 + d]) + (e4 / sum) * halfBitsToF32(v[4 * 2 + d]);
        try std.testing.expectApproxEqAbs(want, out[d], 1e-5);
    }
}

test "quant kernel metal runtime generated counter snapshot order matches C" {
    const Stats = metal_runtime.RawRuntimeMemoryStats;
    const Counts = @FieldType(Stats, "antfly_generated_dispatch_counts");

    // The snapshot mirror must use the shared [format][epilogue] array shape so
    // the extern layout matches uint64_t antfly_generated_dispatch_counts[12][4]
    // in termite_metal_decode_runtime_memory_stats (metal_kernels.m).
    try std.testing.expectEqual(quant_matmul.GeneratedQuantDispatchCounts, Counts);
    try std.testing.expectEqual(
        @as(usize, quant_matmul.generated_quant_format_count * quant_matmul.generated_quant_epilogue_count) * @sizeOf(u64),
        @sizeOf(Counts),
    );
    try std.testing.expectEqual(
        @offsetOf(Stats, "q6_k_linear_reduce_f16_input") + @sizeOf(u64),
        @offsetOf(Stats, "antfly_generated_dispatch_counts"),
    );
    try std.testing.expectEqual(
        @offsetOf(Stats, "antfly_generated_dispatch_counts") + @sizeOf(Counts),
        @offsetOf(Stats, "rms_norm_add_sumsq"),
    );

    // Every wired counter must map to a unique in-range (format, epilogue) cell.
    var seen = @as(
        [quant_matmul.generated_quant_format_count][quant_matmul.generated_quant_epilogue_count]bool,
        @splat(@as([quant_matmul.generated_quant_epilogue_count]bool, @splat(false))),
    );
    for (quant_matmul.generated_quant_counter_names) |counter| {
        const format_index: usize = @backingInt(counter.format);
        const epilogue_index: usize = @backingInt(counter.epilogue);
        try std.testing.expect(format_index < quant_matmul.generated_quant_format_count);
        try std.testing.expect(epilogue_index < quant_matmul.generated_quant_epilogue_count);
        try std.testing.expect(!seen[format_index][epilogue_index]);
        seen[format_index][epilogue_index] = true;
    }
}

test "quant kernel metal runtime production benchmark cases match compiler manifest" {
    var index: usize = 0;
    for (metal_runtime_checks) |check| {
        if (!productionMetalRuntimeCheck(check)) continue;
        const manifest_case = quant_kernel_compiler.first_metal_production_benchmark_cases[index];
        try std.testing.expectEqualStrings(manifest_case.name, check.name);
        try std.testing.expectEqualStrings(manifest_case.kernel_id, check.kernel_name);
        try std.testing.expectEqual(manifest_case.format, check.format);
        try std.testing.expectEqual(manifest_case.row_bucket, quant_matmul.rowBucket(check.rows));
        try std.testing.expectEqual(manifest_case.epilogue, check.epilogue);
        try std.testing.expectEqual(manifest_case.rows, check.rows);
        try std.testing.expectEqual(manifest_case.in_dim, check.in_dim);
        try std.testing.expectEqual(manifest_case.out_dim, check.out_dim);
        try std.testing.expectEqual(manifest_case.threads_per_threadgroup, check.threads_per_threadgroup);
        try std.testing.expectEqual(manifest_case.cols_per_threadgroup, check.cols_per_threadgroup);
        try std.testing.expectEqual(manifest_case.tolerance_abs, check.tolerance);
        try std.testing.expectEqualStrings(manifest_case.generated_source_path, generatedSourcePathFor(check));
        try std.testing.expectEqualStrings(manifest_case.check_command, metalCheckCommandFor(check));
        try std.testing.expectEqualStrings(manifest_case.production_kernel_id, check.kernel_name);
        try std.testing.expectEqualStrings(quant_kernel_compiler.first_metal_production_regression_evidence_command, manifest_case.benchmark_command);
        index += 1;
    }
    try std.testing.expectEqual(quant_kernel_compiler.first_metal_production_benchmark_case_count, index);
}

test "quant kernel metal runtime enables qualified opt-in production routes" {
    var qualified_count: usize = 0;
    for (metal_runtime_checks) |check| {
        if (!productionMetalRuntimeCheck(check)) continue;
        const artifact = metalArtifactForCheck(check) orelse return error.MissingGeneratedArtifact;
        if (artifact.runtime_default_enabled) continue;
        const override = productionRouteGateOverride(check) orelse return error.MissingRuntimeGate;
        try std.testing.expect(override.enable);
        try std.testing.expect(std.mem.startsWith(u8, std.mem.span(override.name), "TERMITE_METAL_ENABLE_"));
        qualified_count += 1;
    }
    try std.testing.expect(qualified_count > 0);
}

const Config = struct {
    evidence_out_path: ?[]const u8 = null,
    check_evidence_path: ?[]const u8 = null,
    attest_provenance: bool = false,
    require_promotion_ready: bool = false,
    require_runtime_route_all: bool = false,
    require_kernel: ?[]const u8 = null,
    require_evidence_kernel: ?[]const u8 = null,
    check_blocker_evidence: bool = false,
    refresh_blocker_evidence: bool = false,
    blocker_evidence_dir: ?[]const u8 = null,
    confirm_cleared_blockers: bool = false,
    fail_on_cleared_blocker: bool = false,
    promotion_ready_kernel: ?[]const u8 = null,
    runtime_route_kernel: ?[]const u8 = null,
    runtime_route_all: bool = false,
    production_regression_check: bool = false,
    split_gqa_only: bool = false,
    v2_conformance: bool = false,
    sweep: bool = false,
    sweep_route: ?[]const u8 = null,
    sweep_evidence_out: ?[]const u8 = null,
    repeat_runs: u32 = 1,
    measure_iters: ?u32 = null,
};

const AttestedProvenance = struct {
    source_commit: []const u8,
    source_tree_clean: bool,
    source_status_sha256: []const u8,
    host_os: []const u8,
    host_arch: []const u8,
    accelerator_name: []const u8,
    metal_compiler_version: []const u8,
    zig_version: []const u8,
    recorded_at_utc: []const u8,
};

const CollectedAttestedProvenance = struct {
    value: AttestedProvenance,

    fn deinit(self: *CollectedAttestedProvenance, allocator: std.mem.Allocator) void {
        allocator.free(self.value.source_commit);
        allocator.free(self.value.source_status_sha256);
        allocator.free(self.value.host_os);
        allocator.free(self.value.host_arch);
        allocator.free(self.value.accelerator_name);
        allocator.free(self.value.metal_compiler_version);
        allocator.free(self.value.zig_version);
        allocator.free(self.value.recorded_at_utc);
        self.* = undefined;
    }
};

fn runAttestationCommandRaw(allocator: std.mem.Allocator, io: std.Io, argv: []const []const u8, stdout_limit: usize) ![]u8 {
    const result = try std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(stdout_limit),
        .stderr_limit = .limited(64 * 1024),
        .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
    });
    defer allocator.free(result.stderr);
    errdefer allocator.free(result.stdout);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.MetalAttestationCommandFailed,
        else => return error.MetalAttestationCommandFailed,
    }
    return result.stdout;
}

fn runAttestationCommandTrimmed(allocator: std.mem.Allocator, io: std.Io, argv: []const []const u8, stdout_limit: usize) ![]u8 {
    const raw = try runAttestationCommandRaw(allocator, io, argv, stdout_limit);
    defer allocator.free(raw);
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (trimmed.len == 0) return error.MetalAttestationMetadataMissing;
    return allocator.dupe(u8, trimmed);
}

fn cleanSourceStatusSha256Alloc(allocator: std.mem.Allocator, status: []const u8) ![]u8 {
    if (status.len != 0) return error.MetalEvidenceDirtySourceTree;
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(status, &digest, .{});
    return std.fmt.allocPrint(allocator, "{s}", .{std.fmt.bytesToHex(digest, .lower)});
}

fn collectMetalDeviceName(allocator: std.mem.Allocator) ![]u8 {
    // The broad inference unit-test binary intentionally does not link the
    // Objective-C Metal provider. Pure provenance parsing/matching is tested
    // below; live device collection remains fail-closed in that test build.
    if (builtin.is_test) return error.MetalAttestationDeviceIdentityMissing;
    const length = termite_metal_copy_device_name(null, 0);
    if (length == 0 or length > 4096) return error.MetalAttestationDeviceIdentityMissing;
    const name = try allocator.alloc(u8, length);
    errdefer allocator.free(name);
    if (termite_metal_copy_device_name(name.ptr, name.len) != length) return error.MetalAttestationDeviceIdentityMissing;
    return name;
}

fn collectAttestedProvenance(allocator: std.mem.Allocator, io: std.Io) !CollectedAttestedProvenance {
    const source_commit = try runAttestationCommandTrimmed(allocator, io, &.{ "/usr/bin/git", "rev-parse", "--verify", "HEAD" }, 1024);
    errdefer allocator.free(source_commit);

    const source_status = try runAttestationCommandRaw(allocator, io, &.{ "/usr/bin/git", "status", "--porcelain=v1", "--untracked-files=all" }, 1024 * 1024);
    defer allocator.free(source_status);
    const source_status_sha256 = try cleanSourceStatusSha256Alloc(allocator, source_status);
    errdefer allocator.free(source_status_sha256);

    const accelerator_name = try collectMetalDeviceName(allocator);
    errdefer allocator.free(accelerator_name);
    const metal_compiler_version = try runAttestationCommandTrimmed(allocator, io, &.{ "/usr/bin/xcrun", "--toolchain", "Metal", "metal", "--version" }, 64 * 1024);
    errdefer allocator.free(metal_compiler_version);
    const recorded_at_utc = try runAttestationCommandTrimmed(allocator, io, &.{ "/bin/date", "-u", "+%Y-%m-%dT%H:%M:%SZ" }, 1024);
    errdefer allocator.free(recorded_at_utc);
    const host_os = try runAttestationCommandTrimmed(allocator, io, &.{ "/usr/bin/uname", "-s" }, 1024);
    errdefer allocator.free(host_os);
    const host_arch = try runAttestationCommandTrimmed(allocator, io, &.{ "/usr/bin/uname", "-m" }, 1024);
    errdefer allocator.free(host_arch);
    const zig_version = try allocator.dupe(u8, builtin.zig_version_string);
    errdefer allocator.free(zig_version);

    return .{ .value = .{
        .source_commit = source_commit,
        .source_tree_clean = true,
        .source_status_sha256 = source_status_sha256,
        .host_os = host_os,
        .host_arch = host_arch,
        .accelerator_name = accelerator_name,
        .metal_compiler_version = metal_compiler_version,
        .zig_version = zig_version,
        .recorded_at_utc = recorded_at_utc,
    } };
}

const promotion_min_repeat_runs: usize = quant_kernel_compiler.metal_promotion_repeat_runs;
const max_evidence_repeat_runs: usize = 31;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const cfg = try parseArgs(args);

    if (cfg.check_blocker_evidence) {
        const summary = try checkBlockerEvidence(allocator, cfg.blocker_evidence_dir, cfg.confirm_cleared_blockers);
        printBlockerEvidenceAuditSummary(summary);
        try enforceClearedBlockerPolicy(summary, cfg.fail_on_cleared_blocker);
        return;
    }

    if (cfg.check_evidence_path) |path| {
        const required_evidence_kernel = cfg.require_kernel orelse cfg.require_evidence_kernel;
        const summary = checkEvidenceFileWithSummary(allocator, path, cfg.require_promotion_ready, cfg.require_runtime_route_all, required_evidence_kernel) catch |err| {
            if (err == error.MetalEvidencePromotionNotReady or err == error.MetalEvidenceReproducibleProvenanceMissing) {
                if (checkEvidenceFileWithSummary(allocator, path, false, cfg.require_runtime_route_all, required_evidence_kernel)) |summary| {
                    printEvidenceSummary(path, "not_ready", summary);
                } else |_| {}
            }
            return err;
        };
        printEvidenceSummary(path, "ok", summary);
        return;
    }

    if (termite_metal_device_available() == 0) return error.MetalDeviceUnavailable;

    if (cfg.split_gqa_only) {
        try runSplitGqaChecks(allocator);
        return;
    }

    if (cfg.v2_conformance) {
        try runV2Conformance(allocator);
        return;
    }

    if (cfg.sweep) {
        try runSweep(allocator, cfg);
        return;
    }

    if (cfg.refresh_blocker_evidence) {
        const summary = try refreshBlockerEvidence(allocator, cfg.blocker_evidence_dir, cfg.confirm_cleared_blockers);
        printBlockerEvidenceAuditSummary(summary);
        try enforceClearedBlockerPolicy(summary, cfg.fail_on_cleared_blocker);
        return;
    }

    var checks_storage = metal_runtime_checks;
    if (cfg.measure_iters) |measure_iters| {
        for (&checks_storage) |*check| check.measure_iters = measure_iters;
    }

    var selected_checks_storage: [metal_runtime_checks.len]CheckCase = undefined;
    var checks: []const CheckCase = checks_storage[0..];
    const selected_kernel = cfg.promotion_ready_kernel orelse cfg.runtime_route_kernel;
    if (cfg.runtime_route_all or cfg.production_regression_check) {
        var selected_count: usize = 0;
        for (checks_storage) |check| {
            if ((cfg.runtime_route_all and runtimeRouteAllSupported(check)) or
                (cfg.production_regression_check and productionMetalRuntimeCheck(check)))
            {
                selected_checks_storage[selected_count] = check;
                selected_count += 1;
            }
        }
        if (selected_count == 0 and !cfg.production_regression_check) return error.InvalidArgument;
        checks = selected_checks_storage[0..selected_count];
    } else if (selected_kernel) |kernel| {
        var selected_count: usize = 0;
        for (checks_storage) |check| {
            if (std.mem.eql(u8, check.kernel_name, kernel)) {
                selected_checks_storage[selected_count] = check;
                selected_count += 1;
            }
        }
        if (selected_count == 0) return error.InvalidArgument;
        checks = selected_checks_storage[0..selected_count];
    }

    const results = try allocator.alloc(CheckResult, checks.len);
    defer allocator.free(results);
    for (checks, 0..) |check, i| {
        const runtime_route_kernel = if (cfg.runtime_route_all or cfg.production_regression_check) check.kernel_name else selected_kernel;
        const result = try runRepeatedCheck(allocator, check, cfg.repeat_runs, runtime_route_kernel, cfg.promotion_ready_kernel);
        results[i] = result;
        const avg_us = @as(f64, @floatFromInt(result.elapsed_nanos)) / @as(f64, @floatFromInt(result.measure_iters)) / 1000.0;
        if (result.handwritten_elapsed_nanos) |handwritten_elapsed_nanos| {
            const handwritten_avg_us = @as(f64, @floatFromInt(handwritten_elapsed_nanos)) / @as(f64, @floatFromInt(result.measure_iters)) / 1000.0;
            std.debug.print(
                "quant-kernel-metal-runtime-check {s} ok max_abs_error={d:.7} measure_iters={d} generated_avg_us={d:.3} handwritten_avg_us={d:.3} generated_speedup={d:.3}\n",
                .{ check.name, result.max_error, result.measure_iters, avg_us, handwritten_avg_us, handwritten_avg_us / avg_us },
            );
        } else {
            std.debug.print(
                "quant-kernel-metal-runtime-check {s} ok max_abs_error={d:.7} measure_iters={d} generated_avg_us={d:.3} handwritten_baseline={s}\n",
                .{ check.name, result.max_error, result.measure_iters, avg_us, handwrittenBaselineFallbackReason(check) },
            );
        }
    }

    // Microkernel (non-matmul) conformance runs in the plain correctness mode
    // only — it is a distinct concern from the matmul route/evidence/production
    // machinery, so it is skipped whenever a matmul route/promotion/evidence
    // selection is active (those flags never target a microkernel).
    if (cfg.evidence_out_path == null and !cfg.runtime_route_all and !cfg.production_regression_check and selected_kernel == null) {
        for (microkernel_runtime_checks) |check| {
            var micro_check = check;
            if (cfg.measure_iters) |measure_iters| micro_check.measure_iters = measure_iters;
            const result = try runMicrokernelCheck(allocator, micro_check);
            const avg_us = @as(f64, @floatFromInt(result.elapsed_nanos)) / @as(f64, @floatFromInt(result.measure_iters)) / 1000.0;
            std.debug.print(
                "quant-kernel-metal-runtime-check {s} ok max_abs_error={d:.7} measure_iters={d} generated_avg_us={d:.3} op_kind=microkernel\n",
                .{ micro_check.name, result.max_error, result.measure_iters, avg_us },
            );
        }
    }

    // Attention (non-matmul) conformance — same isolation as the microkernel
    // pass. This is the fast compile/dispatch/gross-logic pre-check only; the
    // real gate is bit-identical model tokens with the generated route enabled.
    if (cfg.evidence_out_path == null and !cfg.runtime_route_all and !cfg.production_regression_check and selected_kernel == null) {
        for (attention_runtime_checks) |check| {
            var attn_check = check;
            if (cfg.measure_iters) |measure_iters| attn_check.measure_iters = measure_iters;
            const result = try runAttentionCheck(allocator, attn_check);
            const avg_us = @as(f64, @floatFromInt(result.elapsed_nanos)) / @as(f64, @floatFromInt(result.measure_iters)) / 1000.0;
            std.debug.print(
                "quant-kernel-metal-runtime-check {s} ok max_abs_error={d:.7} measure_iters={d} generated_avg_us={d:.3} op_kind=attention\n",
                .{ attn_check.name, result.max_error, result.measure_iters, avg_us },
            );
        }
        // Flash-prefill conformance (multi-query prefill tile, flash grid/shmem).
        for (flash_prefill_checks) |check| {
            var flash_check = check;
            if (cfg.measure_iters) |measure_iters| flash_check.measure_iters = measure_iters;
            const result = try runFlashPrefillCheck(allocator, flash_check);
            const avg_us = @as(f64, @floatFromInt(result.elapsed_nanos)) / @as(f64, @floatFromInt(result.measure_iters)) / 1000.0;
            std.debug.print(
                "quant-kernel-metal-runtime-check {s} ok max_abs_error={d:.7} measure_iters={d} generated_avg_us={d:.3} op_kind=attention_flash\n",
                .{ flash_check.name, result.max_error, result.measure_iters, avg_us },
            );
        }
        // Real production-route split-GQA checks. The harness covers explicit
        // all-layer, production global-only, and disabled rollback policies,
        // poisons private page padding, and proves selection with route counters.
        try runSplitGqaChecks(allocator);
    }

    if (cfg.evidence_out_path) |path| {
        var collected_provenance: ?CollectedAttestedProvenance = if (cfg.attest_provenance)
            try collectAttestedProvenance(allocator, compat.io())
        else
            null;
        defer if (collected_provenance) |*provenance| provenance.deinit(allocator);
        try writeEvidence(
            allocator,
            path,
            checks,
            results,
            cfg.repeat_runs,
            cfg.promotion_ready_kernel,
            cfg.runtime_route_kernel,
            cfg.runtime_route_all,
            cfg.production_regression_check,
            true,
            if (collected_provenance) |provenance| provenance.value else null,
        );
        std.debug.print("quant-kernel-metal-runtime-check evidence_out={s}\n", .{path});
        if (cfg.production_regression_check) {
            const summary = try checkEvidenceFileWithSummary(allocator, path, false, false, null);
            const status = productionRegressionEvidenceStatus(summary);
            printEvidenceSummary(path, status, summary);
            if (productionRegressionEvidenceHasHardBlocker(summary)) {
                return error.MetalEvidencePromotionNotReady;
            }
        }
    }
}

const CheckResult = struct {
    max_error: f32,
    measure_iters: u32,
    elapsed_nanos: u64,
    generated_timing_route: GeneratedTimingRoute = .standalone_generated,
    handwritten_elapsed_nanos: ?u64 = null,
    minimum_repeat_speedup: ?f64 = null,
    repeat_generated_ns: [max_evidence_repeat_runs]u64 = @as([max_evidence_repeat_runs]u64, @splat(0)),
    repeat_handwritten_ns: [max_evidence_repeat_runs]u64 = @as([max_evidence_repeat_runs]u64, @splat(0)),
    repeat_speedups: [max_evidence_repeat_runs]f64 = @as([max_evidence_repeat_runs]f64, @splat(0.0)),
    repeat_timing_count: u32 = 0,
    repeat_handwritten_count: u32 = 0,
    generated_route_checked: bool = false,
    provider_route_checked: bool = false,
    repeat_runs: u32 = 1,
};

const GeneratedTimingRoute = enum {
    standalone_generated,
    decode_runtime_generated,
};

fn generatedTimingRouteName(route: GeneratedTimingRoute) []const u8 {
    return switch (route) {
        .standalone_generated => "standalone_generated",
        .decode_runtime_generated => "decode_runtime_generated",
    };
}

fn generatedTimingScopeName(route: GeneratedTimingRoute) []const u8 {
    return switch (route) {
        .standalone_generated => "standalone_command_buffer",
        .decode_runtime_generated => "decode_runtime_active_frame_batch",
    };
}

fn parseArgs(args: []const [:0]const u8) !Config {
    var cfg: Config = .{};
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg: []const u8 = args[i];
        if (std.mem.eql(u8, arg, "--evidence-out")) {
            if (cfg.evidence_out_path != null) return error.DuplicateEvidenceOut;
            i += 1;
            if (i >= args.len) return error.MissingEvidenceOutPath;
            cfg.evidence_out_path = args[i];
        } else if (std.mem.eql(u8, arg, "--attest-provenance")) {
            if (cfg.attest_provenance) return error.DuplicateAttestProvenance;
            cfg.attest_provenance = true;
        } else if (std.mem.eql(u8, arg, "--check-evidence")) {
            if (cfg.check_evidence_path != null) return error.DuplicateCheckEvidence;
            i += 1;
            if (i >= args.len) return error.MissingCheckEvidencePath;
            cfg.check_evidence_path = args[i];
        } else if (std.mem.eql(u8, arg, "--require-promotion-ready")) {
            if (cfg.require_promotion_ready) return error.DuplicateRequirePromotionReady;
            cfg.require_promotion_ready = true;
        } else if (std.mem.eql(u8, arg, "--require-runtime-route-all")) {
            if (cfg.require_runtime_route_all) return error.DuplicateRequireRuntimeRouteAll;
            cfg.require_runtime_route_all = true;
        } else if (std.mem.eql(u8, arg, "--require-kernel")) {
            if (cfg.require_kernel != null) return error.DuplicateRequireKernel;
            i += 1;
            if (i >= args.len) return error.MissingRequireKernel;
            cfg.require_kernel = args[i];
        } else if (std.mem.eql(u8, arg, "--require-evidence-kernel")) {
            if (cfg.require_evidence_kernel != null) return error.DuplicateRequireEvidenceKernel;
            i += 1;
            if (i >= args.len) return error.MissingRequireEvidenceKernel;
            cfg.require_evidence_kernel = args[i];
        } else if (std.mem.eql(u8, arg, "--check-blocker-evidence")) {
            if (cfg.check_blocker_evidence) return error.DuplicateCheckBlockerEvidence;
            cfg.check_blocker_evidence = true;
        } else if (std.mem.eql(u8, arg, "--refresh-blocker-evidence")) {
            if (cfg.refresh_blocker_evidence) return error.DuplicateRefreshBlockerEvidence;
            cfg.refresh_blocker_evidence = true;
        } else if (std.mem.eql(u8, arg, "--blocker-evidence-dir")) {
            if (cfg.blocker_evidence_dir != null) return error.DuplicateBlockerEvidenceDir;
            i += 1;
            if (i >= args.len) return error.MissingBlockerEvidenceDir;
            cfg.blocker_evidence_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--confirm-cleared-blockers")) {
            if (cfg.confirm_cleared_blockers) return error.DuplicateConfirmClearedBlockers;
            cfg.confirm_cleared_blockers = true;
        } else if (std.mem.eql(u8, arg, "--fail-on-cleared-blocker")) {
            if (cfg.fail_on_cleared_blocker) return error.DuplicateFailOnClearedBlocker;
            cfg.fail_on_cleared_blocker = true;
        } else if (std.mem.eql(u8, arg, "--promotion-ready-kernel")) {
            if (cfg.promotion_ready_kernel != null) return error.DuplicatePromotionReadyKernel;
            i += 1;
            if (i >= args.len) return error.MissingPromotionReadyKernel;
            cfg.promotion_ready_kernel = args[i];
        } else if (std.mem.eql(u8, arg, "--runtime-route-kernel")) {
            if (cfg.runtime_route_kernel != null) return error.DuplicateRuntimeRouteKernel;
            i += 1;
            if (i >= args.len) return error.MissingRuntimeRouteKernel;
            cfg.runtime_route_kernel = args[i];
        } else if (std.mem.eql(u8, arg, "--runtime-route-all")) {
            if (cfg.runtime_route_all) return error.DuplicateRuntimeRouteAll;
            cfg.runtime_route_all = true;
        } else if (std.mem.eql(u8, arg, "--production-regression-check")) {
            if (cfg.production_regression_check) return error.DuplicateProductionRegressionCheck;
            cfg.production_regression_check = true;
        } else if (std.mem.eql(u8, arg, "--split-gqa-only")) {
            if (cfg.split_gqa_only) return error.DuplicateSplitGqaOnly;
            cfg.split_gqa_only = true;
        } else if (std.mem.eql(u8, arg, "--v2-conformance")) {
            if (cfg.v2_conformance) return error.DuplicateV2Conformance;
            cfg.v2_conformance = true;
        } else if (std.mem.eql(u8, arg, "--sweep")) {
            if (cfg.sweep) return error.DuplicateSweep;
            cfg.sweep = true;
        } else if (std.mem.eql(u8, arg, "--sweep-route")) {
            if (cfg.sweep_route != null) return error.DuplicateSweepRoute;
            i += 1;
            if (i >= args.len) return error.MissingSweepRoute;
            cfg.sweep_route = args[i];
        } else if (std.mem.eql(u8, arg, "--sweep-evidence-out")) {
            if (cfg.sweep_evidence_out != null) return error.DuplicateSweepEvidenceOut;
            i += 1;
            if (i >= args.len) return error.MissingSweepEvidenceOut;
            cfg.sweep_evidence_out = args[i];
        } else if (std.mem.eql(u8, arg, "--repeat-runs")) {
            if (cfg.repeat_runs != 1) return error.DuplicateRepeatRuns;
            i += 1;
            if (i >= args.len) return error.MissingRepeatRuns;
            cfg.repeat_runs = try parseRepeatRuns(args[i]);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            if (cfg.measure_iters != null) return error.DuplicateMeasureIters;
            i += 1;
            if (i >= args.len) return error.MissingMeasureIters;
            cfg.measure_iters = try parseMeasureIters(args[i]);
        } else {
            std.debug.print("unknown quant-kernel-metal-runtime-check argument: {s}\n", .{arg});
            return error.UnknownArgument;
        }
    }
    if (cfg.check_evidence_path != null and cfg.evidence_out_path != null) return error.CheckEvidenceConflictsWithEvidenceOut;
    if (cfg.split_gqa_only and (cfg.evidence_out_path != null or cfg.check_evidence_path != null or
        cfg.attest_provenance or cfg.require_promotion_ready or cfg.require_runtime_route_all or
        cfg.require_kernel != null or cfg.require_evidence_kernel != null or cfg.check_blocker_evidence or
        cfg.refresh_blocker_evidence or cfg.blocker_evidence_dir != null or cfg.confirm_cleared_blockers or
        cfg.fail_on_cleared_blocker or cfg.promotion_ready_kernel != null or cfg.runtime_route_kernel != null or
        cfg.runtime_route_all or cfg.production_regression_check or cfg.v2_conformance or cfg.sweep or
        cfg.sweep_route != null or cfg.sweep_evidence_out != null or cfg.repeat_runs != 1 or
        cfg.measure_iters != null)) return error.SplitGqaOnlyConflictsWithOtherMode;
    if (cfg.attest_provenance and cfg.evidence_out_path == null) return error.AttestProvenanceRequiresEvidenceOut;
    if (cfg.check_evidence_path != null and cfg.repeat_runs != 1) return error.CheckEvidenceConflictsWithRepeatRuns;
    if (cfg.check_evidence_path != null and cfg.measure_iters != null) return error.CheckEvidenceConflictsWithMeasureIters;
    if (cfg.check_blocker_evidence and cfg.evidence_out_path != null) return error.CheckBlockerEvidenceConflictsWithEvidenceOut;
    if (cfg.check_blocker_evidence and cfg.check_evidence_path != null) return error.CheckBlockerEvidenceConflictsWithCheckEvidence;
    if (cfg.check_blocker_evidence and cfg.repeat_runs != 1) return error.CheckBlockerEvidenceConflictsWithRepeatRuns;
    if (cfg.check_blocker_evidence and cfg.measure_iters != null) return error.CheckBlockerEvidenceConflictsWithMeasureIters;
    if (cfg.check_blocker_evidence and (cfg.require_promotion_ready or cfg.require_runtime_route_all or cfg.require_kernel != null or cfg.require_evidence_kernel != null or cfg.promotion_ready_kernel != null or cfg.runtime_route_kernel != null or cfg.runtime_route_all or cfg.production_regression_check)) return error.CheckBlockerEvidenceConflictsWithRuntimeMode;
    if (cfg.refresh_blocker_evidence and cfg.evidence_out_path != null) return error.RefreshBlockerEvidenceConflictsWithEvidenceOut;
    if (cfg.refresh_blocker_evidence and cfg.check_evidence_path != null) return error.RefreshBlockerEvidenceConflictsWithCheckEvidence;
    if (cfg.refresh_blocker_evidence and cfg.check_blocker_evidence) return error.RefreshBlockerEvidenceConflictsWithCheckBlockerEvidence;
    if (cfg.refresh_blocker_evidence and cfg.repeat_runs != 1) return error.RefreshBlockerEvidenceConflictsWithRepeatRuns;
    if (cfg.refresh_blocker_evidence and cfg.measure_iters != null) return error.RefreshBlockerEvidenceConflictsWithMeasureIters;
    if (cfg.refresh_blocker_evidence and (cfg.require_promotion_ready or cfg.require_runtime_route_all or cfg.require_kernel != null or cfg.require_evidence_kernel != null or cfg.promotion_ready_kernel != null or cfg.runtime_route_kernel != null or cfg.runtime_route_all or cfg.production_regression_check)) return error.RefreshBlockerEvidenceConflictsWithRuntimeMode;
    if (cfg.blocker_evidence_dir != null and !cfg.check_blocker_evidence and !cfg.refresh_blocker_evidence) return error.BlockerEvidenceDirRequiresBlockerEvidence;
    if (cfg.confirm_cleared_blockers and !cfg.check_blocker_evidence and !cfg.refresh_blocker_evidence) return error.ConfirmClearedBlockersRequiresBlockerEvidence;
    if (cfg.fail_on_cleared_blocker and !cfg.check_blocker_evidence and !cfg.refresh_blocker_evidence) return error.FailOnClearedBlockerRequiresBlockerEvidence;
    if (cfg.require_promotion_ready and cfg.check_evidence_path == null) return error.PromotionReadyRequiresCheckEvidence;
    if (cfg.require_runtime_route_all and cfg.check_evidence_path == null) return error.RuntimeRouteAllRequiresCheckEvidence;
    if (cfg.require_runtime_route_all and cfg.require_promotion_ready) return error.RequireRuntimeRouteAllConflictsWithPromotionReady;
    if (cfg.require_kernel != null and !cfg.require_promotion_ready) return error.RequireKernelRequiresPromotionReady;
    if (cfg.require_evidence_kernel != null and cfg.check_evidence_path == null) return error.RequireEvidenceKernelRequiresCheckEvidence;
    if (cfg.require_kernel != null and cfg.require_evidence_kernel != null) return error.RequireKernelConflictsWithRequireEvidenceKernel;
    if (cfg.promotion_ready_kernel != null and cfg.evidence_out_path == null) return error.PromotionReadyKernelRequiresEvidenceOut;
    if (cfg.promotion_ready_kernel != null and !hasPromotionRepeatRuns(cfg.repeat_runs)) return error.PromotionReadyKernelRequiresRepeatRuns;
    if (cfg.promotion_ready_kernel != null and cfg.measure_iters != quant_kernel_compiler.metal_promotion_measure_iters) return error.PromotionReadyKernelRequiresMeasureIters;
    if (cfg.promotion_ready_kernel != null and cfg.runtime_route_kernel != null) return error.RuntimeRouteKernelConflictsWithPromotionReadyKernel;
    if (cfg.promotion_ready_kernel != null and cfg.runtime_route_all) return error.RuntimeRouteAllConflictsWithPromotionReadyKernel;
    if (cfg.runtime_route_kernel != null and cfg.runtime_route_all) return error.RuntimeRouteAllConflictsWithRuntimeRouteKernel;
    if (cfg.production_regression_check and cfg.evidence_out_path == null) return error.ProductionRegressionCheckRequiresEvidenceOut;
    if (cfg.production_regression_check and !hasPromotionRepeatRuns(cfg.repeat_runs)) return error.ProductionRegressionCheckRequiresRepeatRuns;
    if (cfg.production_regression_check and cfg.promotion_ready_kernel != null) return error.ProductionRegressionCheckConflictsWithPromotionReadyKernel;
    if (cfg.production_regression_check and cfg.runtime_route_kernel != null) return error.ProductionRegressionCheckConflictsWithRuntimeRouteKernel;
    if (cfg.production_regression_check and cfg.runtime_route_all) return error.ProductionRegressionCheckConflictsWithRuntimeRouteAll;
    if (cfg.production_regression_check and cfg.measure_iters != quant_kernel_compiler.metal_promotion_measure_iters) return error.ProductionRegressionCheckRequiresMeasureIters;
    if (cfg.promotion_ready_kernel) |kernel| {
        if (!std.mem.containsAtLeast(u8, cfg.evidence_out_path.?, 1, kernel)) return error.PromotionReadyKernelRequiresKernelEvidencePath;
    }
    return cfg;
}

test "quant kernel metal runtime check parses sweep flags" {
    const cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--sweep", "--sweep-route", "q6_k/bias", "--sweep-evidence-out", "/private/tmp/sweep.json" });
    try std.testing.expect(cfg.sweep);
    try std.testing.expectEqualStrings("q6_k/bias", cfg.sweep_route.?);
    try std.testing.expectEqualStrings("/private/tmp/sweep.json", cfg.sweep_evidence_out.?);
    try std.testing.expectError(error.MissingSweepRoute, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--sweep", "--sweep-route" }));
    try std.testing.expectError(error.MissingSweepEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--sweep", "--sweep-evidence-out" }));
    try std.testing.expectError(error.DuplicateSweep, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--sweep", "--sweep" }));

    // Route filter matching: "<fmt>/<epi>" and format-only.
    try std.testing.expect(sweepRouteMatches("q6_k/bias", .q6_k, .bias));
    try std.testing.expect(!sweepRouteMatches("q6_k/bias", .q6_k, .none));
    try std.testing.expect(sweepRouteMatches("q6_k", .q6_k, .none));
    try std.testing.expect(!sweepRouteMatches("q6_k", .q5_k, .none));
}

test "quant kernel metal runtime sweep enumerates valid variants" {
    var buf: [quant_kernel_compiler.metal_schedule_candidate_capacity]quant_kernel_compiler.KernelSchedule = undefined;
    // 32-value format: only single-simdgroup variants (cols 1/2).
    const q8_0_count = quant_kernel_compiler.metalScheduleCandidates(.q8_0, .none, &buf);
    try std.testing.expectEqual(@as(usize, 2), q8_0_count);
    for (buf[0..q8_0_count]) |schedule| {
        try std.testing.expectEqual(@as(u16, 32), schedule.threads_per_threadgroup);
        try schedule.validate(32);
    }
    // 256-value format: 32(c1,c2) + 64/128/256(tree,hybrid) = 8 variants.
    const q6_k_count = quant_kernel_compiler.metalScheduleCandidates(.q6_k, .none, &buf);
    try std.testing.expectEqual(@as(usize, 8), q6_k_count);
    for (buf[0..q6_k_count]) |schedule| {
        try schedule.validate(256);
        try std.testing.expect(schedule.threads_per_threadgroup <= 256);
    }
}

test "quant kernel metal runtime check isolates split GQA conformance mode" {
    const cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--split-gqa-only" });
    try std.testing.expect(cfg.split_gqa_only);
    try std.testing.expectError(
        error.DuplicateSplitGqaOnly,
        parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--split-gqa-only", "--split-gqa-only" }),
    );
    try std.testing.expectError(
        error.SplitGqaOnlyConflictsWithOtherMode,
        parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--split-gqa-only", "--measure-iters", "2" }),
    );
}

test "quant kernel metal runtime check parses evidence output flag" {
    const cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json" });
    try std.testing.expectEqualStrings("/tmp/evidence.json", cfg.evidence_out_path.?);
    const attested_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--attest-provenance" });
    try std.testing.expect(attested_cfg.attest_provenance);
    try std.testing.expectError(error.MissingEvidenceOutPath, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out" }));
    try std.testing.expectError(error.AttestProvenanceRequiresEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--attest-provenance" }));
    try std.testing.expectError(error.DuplicateAttestProvenance, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--attest-provenance", "--attest-provenance" }));

    const check_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--require-promotion-ready", "--require-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id });
    try std.testing.expectEqualStrings("/tmp/evidence.json", check_cfg.check_evidence_path.?);
    try std.testing.expect(check_cfg.require_promotion_ready);
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_general_metal_q4_kernel_id, check_cfg.require_kernel.?);
    const evidence_kernel_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--require-evidence-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id });
    try std.testing.expectEqualStrings("/tmp/evidence.json", evidence_kernel_cfg.check_evidence_path.?);
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_general_metal_q4_kernel_id, evidence_kernel_cfg.require_evidence_kernel.?);
    const promotion_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/" ++ quant_kernel_compiler.first_general_metal_q4_kernel_id ++ "-evidence.json", "--repeat-runs", "5", "--measure-iters", "500", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id });
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_general_metal_q4_kernel_id, promotion_cfg.promotion_ready_kernel.?);
    try std.testing.expectError(error.MissingCheckEvidencePath, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence" }));
    try std.testing.expectError(error.MissingRequireKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--require-kernel" }));
    try std.testing.expectError(error.MissingRequireEvidenceKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--require-evidence-kernel" }));
    try std.testing.expectError(error.MissingPromotionReadyKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--promotion-ready-kernel" }));
    try std.testing.expectError(error.PromotionReadyRequiresCheckEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--require-promotion-ready" }));
    try std.testing.expectError(error.RequireKernelRequiresPromotionReady, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--require-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.RequireEvidenceKernelRequiresCheckEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--require-evidence-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.RequireKernelConflictsWithRequireEvidenceKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--require-promotion-ready", "--require-kernel", "a", "--require-evidence-kernel", "a" }));
    try std.testing.expectError(error.PromotionReadyKernelRequiresEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id, "--repeat-runs", "3" }));
    try std.testing.expectError(error.PromotionReadyKernelRequiresRepeatRuns, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "3", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.PromotionReadyKernelRequiresRepeatRuns, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.PromotionReadyKernelRequiresMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.PromotionReadyKernelRequiresMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--measure-iters", "100", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.PromotionReadyKernelRequiresKernelEvidencePath, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--measure-iters", "500", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.CheckEvidenceConflictsWithEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--evidence-out", "/tmp/out.json" }));
    try std.testing.expectError(error.CheckEvidenceConflictsWithRepeatRuns, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--repeat-runs", "3" }));
    try std.testing.expectError(error.DuplicateCheckEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/a.json", "--check-evidence", "/tmp/b.json" }));
    try std.testing.expectError(error.DuplicateRequirePromotionReady, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/a.json", "--require-promotion-ready", "--require-promotion-ready" }));
    try std.testing.expectError(error.DuplicateRequireKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/a.json", "--require-promotion-ready", "--require-kernel", "a", "--require-kernel", "b" }));
    try std.testing.expectError(error.DuplicateRequireEvidenceKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/a.json", "--require-evidence-kernel", "a", "--require-evidence-kernel", "b" }));
    try std.testing.expectError(error.DuplicatePromotionReadyKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "3", "--promotion-ready-kernel", "a", "--promotion-ready-kernel", "b" }));

    const repeat_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "3" });
    try std.testing.expectEqual(@as(u32, 3), repeat_cfg.repeat_runs);
    const measure_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--measure-iters", "100" });
    try std.testing.expectEqual(@as(u32, 100), measure_cfg.measure_iters.?);
    const route_all_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--runtime-route-all" });
    try std.testing.expect(route_all_cfg.runtime_route_all);
    const production_regression_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--measure-iters", "500", "--production-regression-check" });
    try std.testing.expect(production_regression_cfg.production_regression_check);
    try std.testing.expectEqual(quant_kernel_compiler.metal_promotion_measure_iters, production_regression_cfg.measure_iters.?);
    try std.testing.expectError(error.ProductionRegressionCheckRequiresMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--production-regression-check" }));
    try std.testing.expectError(error.ProductionRegressionCheckRequiresMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--measure-iters", "25", "--production-regression-check" }));
    const route_all_check_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--require-runtime-route-all" });
    try std.testing.expect(route_all_check_cfg.require_runtime_route_all);
    const blocker_check_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--blocker-evidence-dir", "/tmp/blocker-evidence" });
    try std.testing.expect(blocker_check_cfg.check_blocker_evidence);
    try std.testing.expectEqualStrings("/tmp/blocker-evidence", blocker_check_cfg.blocker_evidence_dir.?);
    const blocker_refresh_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--refresh-blocker-evidence", "--blocker-evidence-dir", "/tmp/blocker-evidence" });
    try std.testing.expect(blocker_refresh_cfg.refresh_blocker_evidence);
    const strict_blocker_check_cfg = try parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--confirm-cleared-blockers", "--fail-on-cleared-blocker" });
    try std.testing.expect(strict_blocker_check_cfg.check_blocker_evidence);
    try std.testing.expect(strict_blocker_check_cfg.confirm_cleared_blockers);
    try std.testing.expect(strict_blocker_check_cfg.fail_on_cleared_blocker);
    try std.testing.expectError(error.MissingRepeatRuns, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--repeat-runs" }));
    try std.testing.expectError(error.InvalidRepeatRuns, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--repeat-runs", "0" }));
    try std.testing.expectError(error.MissingMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--measure-iters" }));
    try std.testing.expectError(error.InvalidMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--measure-iters", "0" }));
    try std.testing.expectError(error.CheckEvidenceConflictsWithMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--measure-iters", "100" }));
    try std.testing.expectError(error.DuplicateEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/a.json", "--evidence-out", "/tmp/b.json" }));
    try std.testing.expectError(error.DuplicateRepeatRuns, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--repeat-runs", "2", "--repeat-runs", "3" }));
    try std.testing.expectError(error.DuplicateMeasureIters, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--measure-iters", "100", "--measure-iters", "200" }));
    try std.testing.expectError(error.DuplicateRuntimeRouteAll, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--runtime-route-all", "--runtime-route-all" }));
    try std.testing.expectError(error.DuplicateCheckBlockerEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--check-blocker-evidence" }));
    try std.testing.expectError(error.DuplicateConfirmClearedBlockers, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--confirm-cleared-blockers", "--confirm-cleared-blockers" }));
    try std.testing.expectError(error.DuplicateFailOnClearedBlocker, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--fail-on-cleared-blocker", "--fail-on-cleared-blocker" }));
    try std.testing.expectError(error.ConfirmClearedBlockersRequiresBlockerEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--confirm-cleared-blockers" }));
    try std.testing.expectError(error.FailOnClearedBlockerRequiresBlockerEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--fail-on-cleared-blocker" }));
    try std.testing.expectError(error.CheckBlockerEvidenceConflictsWithEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--evidence-out", "/tmp/evidence.json" }));
    try std.testing.expectError(error.CheckBlockerEvidenceConflictsWithCheckEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--check-evidence", "/tmp/evidence.json" }));
    try std.testing.expectError(error.CheckBlockerEvidenceConflictsWithRuntimeMode, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--runtime-route-all" }));
    try std.testing.expectError(error.DuplicateRefreshBlockerEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--refresh-blocker-evidence", "--refresh-blocker-evidence" }));
    try std.testing.expectError(error.RefreshBlockerEvidenceConflictsWithEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--refresh-blocker-evidence", "--evidence-out", "/tmp/evidence.json" }));
    try std.testing.expectError(error.RefreshBlockerEvidenceConflictsWithCheckEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--refresh-blocker-evidence", "--check-evidence", "/tmp/evidence.json" }));
    try std.testing.expectError(error.RefreshBlockerEvidenceConflictsWithCheckBlockerEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--refresh-blocker-evidence", "--check-blocker-evidence" }));
    try std.testing.expectError(error.RefreshBlockerEvidenceConflictsWithRuntimeMode, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--refresh-blocker-evidence", "--runtime-route-all" }));
    try std.testing.expectError(error.MissingBlockerEvidenceDir, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--blocker-evidence-dir" }));
    try std.testing.expectError(error.DuplicateBlockerEvidenceDir, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-blocker-evidence", "--blocker-evidence-dir", "/tmp/a", "--blocker-evidence-dir", "/tmp/b" }));
    try std.testing.expectError(error.BlockerEvidenceDirRequiresBlockerEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--blocker-evidence-dir", "/tmp/blocker-evidence" }));
    try std.testing.expectError(error.DuplicateProductionRegressionCheck, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--production-regression-check", "--production-regression-check" }));
    try std.testing.expectError(error.DuplicateRequireRuntimeRouteAll, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--require-runtime-route-all", "--require-runtime-route-all" }));
    try std.testing.expectError(error.RuntimeRouteAllRequiresCheckEvidence, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--require-runtime-route-all" }));
    try std.testing.expectError(error.RequireRuntimeRouteAllConflictsWithPromotionReady, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--check-evidence", "/tmp/evidence.json", "--require-runtime-route-all", "--require-promotion-ready" }));
    try std.testing.expectError(error.RuntimeRouteAllConflictsWithRuntimeRouteKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--runtime-route-all", "--runtime-route-kernel", quant_kernel_compiler.first_general_metal_q8_bias_gelu_kernel_id }));
    try std.testing.expectError(error.RuntimeRouteAllConflictsWithPromotionReadyKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--measure-iters", "500", "--runtime-route-all", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.ProductionRegressionCheckRequiresEvidenceOut, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--repeat-runs", "5", "--production-regression-check" }));
    try std.testing.expectError(error.ProductionRegressionCheckRequiresRepeatRuns, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--production-regression-check" }));
    try std.testing.expectError(error.ProductionRegressionCheckConflictsWithPromotionReadyKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--measure-iters", "500", "--production-regression-check", "--promotion-ready-kernel", quant_kernel_compiler.first_general_metal_q4_kernel_id }));
    try std.testing.expectError(error.ProductionRegressionCheckConflictsWithRuntimeRouteKernel, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--production-regression-check", "--runtime-route-kernel", quant_kernel_compiler.first_general_metal_q8_bias_gelu_kernel_id }));
    try std.testing.expectError(error.ProductionRegressionCheckConflictsWithRuntimeRouteAll, parseArgs(&.{ "antfly-quant-kernel-metal-runtime-check", "--evidence-out", "/tmp/evidence.json", "--repeat-runs", "5", "--production-regression-check", "--runtime-route-all" }));
}

test "quant kernel metal runtime blocker evidence path can be build owned" {
    const path = try blockerEvidencePath(std.testing.allocator, quant_kernel_compiler.first_general_metal_q4_0_promotion_evidence_path, "/tmp/build-owned");
    defer std.testing.allocator.free(path);
    try std.testing.expectEqualStrings("/tmp/build-owned/antfly-quant-metal-antfly_q4_0_small_batch_msl_v1-promotion-evidence.json", path);
}

fn parseRepeatRuns(text: []const u8) !u32 {
    const runs = try std.fmt.parseUnsigned(u32, text, 10);
    if (runs == 0 or runs > 31) return error.InvalidRepeatRuns;
    return runs;
}

fn parseMeasureIters(text: []const u8) !u32 {
    const iters = try std.fmt.parseUnsigned(u32, text, 10);
    if (iters == 0 or iters > 100_000) return error.InvalidMeasureIters;
    return iters;
}

fn runRepeatedCheck(
    allocator: std.mem.Allocator,
    check: CheckCase,
    repeat_runs: u32,
    route_kernel: ?[]const u8,
    promotion_ready_kernel: ?[]const u8,
) !CheckResult {
    if (repeat_runs == 0) return error.InvalidRepeatRuns;
    if (repeat_runs == 1) return runCheck(allocator, check, route_kernel, promotion_ready_kernel);

    const run_count: usize = @intCast(repeat_runs);
    if (run_count > max_evidence_repeat_runs) return error.InvalidRepeatRuns;
    const runs = try allocator.alloc(CheckResult, run_count);
    defer allocator.free(runs);
    const generated_ns = try allocator.alloc(u64, run_count);
    defer allocator.free(generated_ns);
    const handwritten_ns = try allocator.alloc(u64, run_count);
    defer allocator.free(handwritten_ns);

    for (0..repeated_check_warmup_runs) |_| {
        _ = try runCheck(allocator, check, route_kernel, promotion_ready_kernel);
    }

    var max_error: f32 = 0.0;
    var handwritten_count: usize = 0;
    var repeat_speedups: [max_evidence_repeat_runs]f64 = @as([max_evidence_repeat_runs]f64, @splat(0.0));
    for (runs, 0..) |*run, i| {
        run.* = try runCheck(allocator, check, route_kernel, promotion_ready_kernel);
        max_error = @max(max_error, run.max_error);
        generated_ns[i] = run.elapsed_nanos;
        if (run.handwritten_elapsed_nanos) |elapsed| {
            handwritten_ns[handwritten_count] = elapsed;
            handwritten_count += 1;
            const run_speedup = speedup(elapsed, run.elapsed_nanos);
            repeat_speedups[i] = run_speedup;
        }
    }

    const generated_route_checked = runs[0].generated_route_checked;
    const provider_route_checked = runs[0].provider_route_checked;
    const generated_timing_route = runs[0].generated_timing_route;
    for (runs[1..]) |run| {
        if (run.generated_route_checked != generated_route_checked) return error.InvalidArgument;
        if (run.provider_route_checked != provider_route_checked) return error.InvalidArgument;
        if (run.generated_timing_route != generated_timing_route) return error.InvalidArgument;
    }

    var result = CheckResult{
        .max_error = max_error,
        .measure_iters = runs[0].measure_iters,
        .elapsed_nanos = medianU64Const(generated_ns),
        .generated_timing_route = generated_timing_route,
        .handwritten_elapsed_nanos = if (handwritten_count == run_count) medianU64Const(handwritten_ns[0..handwritten_count]) else null,
        .minimum_repeat_speedup = if (handwritten_count == run_count) repeatGateSpeedup(repeat_speedups[0..run_count]) else null,
        .repeat_timing_count = repeat_runs,
        .repeat_handwritten_count = @intCast(handwritten_count),
        .generated_route_checked = generated_route_checked,
        .provider_route_checked = provider_route_checked,
        .repeat_runs = repeat_runs,
    };
    @memcpy(result.repeat_generated_ns[0..run_count], generated_ns);
    @memcpy(result.repeat_handwritten_ns[0..handwritten_count], handwritten_ns[0..handwritten_count]);
    @memcpy(result.repeat_speedups[0..run_count], repeat_speedups[0..run_count]);
    return result;
}

fn medianU64(values: []u64) u64 {
    std.mem.sort(u64, values, {}, std.sort.asc(u64));
    return values[values.len / 2];
}

fn medianU64Const(values: []const u64) u64 {
    var copy: [max_evidence_repeat_runs]u64 = undefined;
    @memcpy(copy[0..values.len], values);
    return medianU64(copy[0..values.len]);
}

fn repeatGateSpeedup(values: []const f64) f64 {
    var copy: [max_evidence_repeat_runs]f64 = undefined;
    @memcpy(copy[0..values.len], values);
    std.mem.sort(f64, copy[0..values.len], {}, std.sort.asc(f64));
    return copy[0];
}

// Renders every metal_production_schedules route through the descriptor-driven
// renderer and runs it on-device against the CPU reference, reusing the v1
// benchmark shapes/tolerances. Proves the v2 kernels are numerically correct on
// the GPU (not just that they compile). Non-production: nothing is promoted.
fn runV2Conformance(allocator: std.mem.Allocator) !void {
    var checked: usize = 0;
    var worst_error: f32 = 0.0;
    for (metal_runtime_checks) |v1_check| {
        const decoder = quant_kernel_metal_renderer.decoderFor(v1_check.format) orelse return error.MissingV2Decoder;
        const schedule = quant_kernel_compiler.metalRouteScheduleFor(v1_check.format, .rows_2_8, v1_check.epilogue) orelse return error.MissingV2Schedule;
        const suffix = switch (v1_check.epilogue) {
            .none => "",
            .bias => "_bias",
            .bias_gelu => "_bias_gelu",
            .relu => "_relu",
            else => return error.UnsupportedV2Epilogue,
        };
        const kernel_id = try std.fmt.allocPrint(allocator, "antfly_{s}_small_batch{s}_msl_v2", .{ @tagName(v1_check.format), suffix });
        defer allocator.free(kernel_id);
        const body = try quant_kernel_metal_renderer.renderKernel(allocator, kernel_id, decoder, schedule, v1_check.epilogue);
        defer allocator.free(body);
        const source = try std.fmt.allocPrint(allocator, "#include <metal_stdlib>\nusing namespace metal;\n{s}", .{body});
        defer allocator.free(source);

        var v2_check = v1_check;
        v2_check.source = source;
        v2_check.kernel_name = kernel_id;
        v2_check.threads_per_threadgroup = schedule.threads_per_threadgroup;
        v2_check.cols_per_threadgroup = schedule.cols_per_threadgroup;
        v2_check.measure_iters = 1; // conformance only; timing irrelevant

        const result = runCheckImpl(allocator, v2_check, null, null, true) catch |err| {
            std.debug.print("quant-kernel-metal-v2-conformance {s} FAILED err={s}\n", .{ v1_check.name, @errorName(err) });
            return err;
        };
        if (result.max_error > worst_error) worst_error = result.max_error;
        std.debug.print("quant-kernel-metal-v2-conformance {s} ok max_abs_error={d:.7} tolerance={d:.7}\n", .{ v1_check.name, result.max_error, v1_check.tolerance });
        checked += 1;
    }
    std.debug.print("quant-kernel-metal-v2-conformance done: {d} cases, worst_abs_error={d:.7}\n", .{ checked, worst_error });
}

fn runCheck(
    allocator: std.mem.Allocator,
    check: CheckCase,
    route_kernel: ?[]const u8,
    promotion_ready_kernel: ?[]const u8,
) !CheckResult {
    return runCheckImpl(allocator, check, route_kernel, promotion_ready_kernel, false);
}

const SweepVariantRecord = struct {
    schedule: quant_kernel_compiler.KernelSchedule,
    correctness_passed: bool,
    max_error: f32,
    min_speedup: f64, // worst-shape speedup vs the production baseline
    is_baseline_schedule: bool,
};

const SweepRouteRecord = struct {
    format: quant_matmul.Format,
    epilogue: quant_kernel_compiler.Epilogue,
    baseline_kernel_id: []const u8,
    baseline_schedule: quant_kernel_compiler.KernelSchedule,
    variants: []SweepVariantRecord,
    winner_index: ?usize,
};

// Runs the schedule sweep for one route (all its benchmark shapes) and returns
// an owned SweepRouteRecord. Times the production baseline once per shape, then
// each rendered variant, computing the worst-shape speedup.
fn runSweepRoute(
    allocator: std.mem.Allocator,
    format: quant_matmul.Format,
    epilogue: quant_kernel_compiler.Epilogue,
    measure_iters: u32,
    repeat_runs: u32,
) !SweepRouteRecord {
    // Collect the v1 production CheckCases (small + wide) for this route.
    var shape_checks: [4]CheckCase = undefined;
    var shape_count: usize = 0;
    for (metal_runtime_checks) |check| {
        if (check.format == format and check.epilogue == epilogue) {
            if (shape_count >= shape_checks.len) break;
            shape_checks[shape_count] = check;
            shape_count += 1;
        }
    }
    if (shape_count == 0) return error.SweepRouteHasNoChecks;
    const baseline_kernel_id = shape_checks[0].kernel_name;
    const baseline_schedule = quant_kernel_compiler.metalRouteScheduleFor(format, .rows_2_8, epilogue) orelse return error.MissingSweepSchedule;

    // Baseline timing per shape (production source at production schedule).
    var baseline_ns: [4]u64 = .{ 0, 0, 0, 0 };
    for (shape_checks[0..shape_count], 0..) |check, si| {
        const timed = try timeSourceMinNs(allocator, check, check.source, check.kernel_name, baseline_schedule, measure_iters, repeat_runs) orelse return error.BaselineConformanceFailed;
        baseline_ns[si] = timed.ns;
    }

    const decoder = quant_kernel_metal_renderer.decoderFor(format) orelse return error.MissingSweepDecoder;
    var variant_schedules: [quant_kernel_compiler.metal_schedule_candidate_capacity]quant_kernel_compiler.KernelSchedule = undefined;
    const variant_count = quant_kernel_compiler.metalScheduleCandidates(format, epilogue, &variant_schedules);

    var records = try allocator.alloc(SweepVariantRecord, variant_count);
    var winner_index: ?usize = null;
    var winner_speedup: f64 = 0.0;
    const suffix = epilogueIdSuffix(epilogue);
    for (variant_schedules[0..variant_count], 0..) |schedule, vi| {
        const kernel_id = try std.fmt.allocPrint(allocator, "antfly_{s}_small_batch{s}_msl_v2_t{d}c{d}_{s}", .{
            @tagName(format), suffix, schedule.threads_per_threadgroup, schedule.cols_per_threadgroup, reductionName(schedule.reduction),
        });
        defer allocator.free(kernel_id);
        const body = try quant_kernel_metal_renderer.renderKernel(allocator, kernel_id, decoder, schedule, epilogue);
        defer allocator.free(body);
        const source = try std.fmt.allocPrint(allocator, "#include <metal_stdlib>\nusing namespace metal;\n{s}", .{body});
        defer allocator.free(source);

        var ok = true;
        var max_error: f32 = 0.0;
        var min_speedup: f64 = std.math.floatMax(f64);
        for (shape_checks[0..shape_count], 0..) |check, si| {
            const timed = try timeSourceMinNs(allocator, check, source, kernel_id, schedule, measure_iters, repeat_runs);
            if (timed == null) {
                ok = false;
                break;
            }
            if (timed.?.max_error > max_error) max_error = timed.?.max_error;
            const shape_speedup = @as(f64, @floatFromInt(baseline_ns[si])) / @as(f64, @floatFromInt(@max(timed.?.ns, 1)));
            if (shape_speedup < min_speedup) min_speedup = shape_speedup;
        }
        const is_baseline = schedule.threads_per_threadgroup == baseline_schedule.threads_per_threadgroup and
            schedule.cols_per_threadgroup == baseline_schedule.cols_per_threadgroup and
            schedule.reduction == baseline_schedule.reduction;
        records[vi] = .{
            .schedule = schedule,
            .correctness_passed = ok,
            .max_error = max_error,
            .min_speedup = if (ok) min_speedup else 0.0,
            .is_baseline_schedule = is_baseline,
        };
        if (ok and min_speedup > winner_speedup) {
            winner_speedup = min_speedup;
            winner_index = vi;
        }
    }

    return .{
        .format = format,
        .epilogue = epilogue,
        .baseline_kernel_id = baseline_kernel_id,
        .baseline_schedule = baseline_schedule,
        .variants = records,
        .winner_index = winner_index,
    };
}

fn epilogueIdSuffix(epilogue: quant_kernel_compiler.Epilogue) []const u8 {
    return switch (epilogue) {
        .none => "",
        .bias => "_bias",
        .bias_gelu => "_bias_gelu",
        .relu => "_relu",
        else => "_x",
    };
}

// Drives the sweep across all routes (or a single --sweep-route fmt/epilogue),
// prints a per-route summary, and optionally writes sweep evidence JSON.
fn runSweep(allocator: std.mem.Allocator, cfg: Config) !void {
    const measure_iters = cfg.measure_iters orelse 200;
    const repeat_runs = cfg.repeat_runs;
    var route_records = std.ArrayListUnmanaged(SweepRouteRecord).empty;
    defer {
        for (route_records.items) |record| allocator.free(record.variants);
        route_records.deinit(allocator);
    }

    for (quant_kernel_compiler.metal_production_schedules) |entry| {
        if (cfg.sweep_route) |filter| {
            if (!sweepRouteMatches(filter, entry.format, entry.epilogue)) continue;
        }
        const record = try runSweepRoute(allocator, entry.format, entry.epilogue, measure_iters, repeat_runs);
        try route_records.append(allocator, record);
        const route_name = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ @tagName(entry.format), @tagName(entry.epilogue) });
        defer allocator.free(route_name);
        if (record.winner_index) |wi| {
            const w = record.variants[wi];
            std.debug.print(
                "quant-kernel-metal-sweep {s} winner threads={d} cols={d} reduction={s} min_speedup={d:.3}{s}\n",
                .{ route_name, w.schedule.threads_per_threadgroup, w.schedule.cols_per_threadgroup, reductionName(w.schedule.reduction), w.min_speedup, if (w.is_baseline_schedule) " (== current)" else "" },
            );
        } else {
            std.debug.print("quant-kernel-metal-sweep {s} no correct variant\n", .{route_name});
        }
    }

    // Attention `op_kind` route(s): swept on a parallel path (flash knobs / flash
    // runner) but folded into the same evidence. Runs when no route filter is set
    // or the filter selects the attention family.
    var attention_records = std.ArrayListUnmanaged(AttentionSweepRouteRecord).empty;
    defer {
        for (attention_records.items) |record| allocator.free(record.variants);
        attention_records.deinit(allocator);
    }
    const run_attention = cfg.sweep_route == null or sweepRouteMatchesAttention(cfg.sweep_route.?);
    if (run_attention) {
        const record = try runAttentionSweepRoute(allocator, measure_iters, repeat_runs);
        try attention_records.append(allocator, record);
        if (record.winner_index) |wi| {
            const w = record.variants[wi];
            std.debug.print(
                "quant-kernel-metal-sweep {s} winner key_chunk={d} skip_rescale={s} min_speedup={d:.3}{s}\n",
                .{ record.route_name, w.schedule.key_chunk, if (w.schedule.skip_rescale) "true" else "false", w.min_speedup, if (w.is_baseline_schedule) " (== current)" else "" },
            );
        } else {
            std.debug.print("quant-kernel-metal-sweep {s} no correct variant\n", .{record.route_name});
        }
    }

    if (cfg.sweep_evidence_out) |path| {
        try writeSweepEvidence(allocator, path, route_records.items, attention_records.items);
        std.debug.print("quant-kernel-metal-sweep evidence_out={s} routes={d}\n", .{ path, route_records.items.len + attention_records.items.len });
    }
    std.debug.print("quant-kernel-metal-sweep done: {d} matmul routes + {d} attention routes\n", .{ route_records.items.len, attention_records.items.len });
}

fn sweepRouteMatches(filter: []const u8, format: quant_matmul.Format, epilogue: quant_kernel_compiler.Epilogue) bool {
    // filter form "<format>/<epilogue>" or just "<format>".
    var it = std.mem.splitScalar(u8, filter, '/');
    const fmt_part = it.next() orelse return false;
    if (!std.mem.eql(u8, fmt_part, @tagName(format))) return false;
    const epi_part = it.next() orelse return true; // format-only matches all epilogues
    return std.mem.eql(u8, epi_part, @tagName(epilogue));
}

fn writeSweepEvidence(
    allocator: std.mem.Allocator,
    path: []const u8,
    records: []const SweepRouteRecord,
    attention_records: []const AttentionSweepRouteRecord,
) !void {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try out.appendSlice(allocator, "{\n  \"schema\": \"antfly.quant_kernel_metal_sweep.v1\",\n  \"routes\": [\n");
    const total = records.len + attention_records.len;
    var emitted: usize = 0;
    for (records) |record| {
        try appendSweepRouteJson(allocator, &out, record);
        emitted += 1;
        try out.appendSlice(allocator, if (emitted == total) "\n" else ",\n");
    }
    // Attention routes ride in the same "routes" array; each object self-describes
    // its schedule (the flash key_chunk/skip_rescale knobs distinguish variants
    // that share the matmul threads/cols/reduction fields).
    for (attention_records) |record| {
        try appendAttentionSweepRouteJson(allocator, &out, record);
        emitted += 1;
        try out.appendSlice(allocator, if (emitted == total) "\n" else ",\n");
    }
    try out.appendSlice(allocator, "  ]\n}\n");
    try writeFileCreatingParent(compat.io(), path, out.items);
}

fn appendSweepScheduleJson(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), schedule: quant_kernel_compiler.KernelSchedule) !void {
    const chunk = try std.fmt.allocPrint(allocator, "{{\"threads\": {d}, \"cols\": {d}, \"reduction\": \"{s}\"}}", .{
        schedule.threads_per_threadgroup, schedule.cols_per_threadgroup, reductionName(schedule.reduction),
    });
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

// Flash schedule JSON carries the attention knobs the matmul schedule JSON omits.
fn appendFlashScheduleJson(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), schedule: quant_kernel_compiler.KernelSchedule) !void {
    const chunk = try std.fmt.allocPrint(allocator, "{{\"threads\": {d}, \"cols\": {d}, \"reduction\": \"{s}\", \"key_chunk\": {d}, \"skip_rescale\": {s}}}", .{
        schedule.threads_per_threadgroup, schedule.cols_per_threadgroup,                  reductionName(schedule.reduction),
        schedule.key_chunk,               if (schedule.skip_rescale) "true" else "false",
    });
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

fn appendAttentionSweepRouteJson(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), record: AttentionSweepRouteRecord) !void {
    const header = try std.fmt.allocPrint(allocator, "    {{\n      \"format\": \"{s}\",\n      \"epilogue\": \"none\",\n      \"baseline_kernel_id\": \"{s}\",\n      \"baseline_schedule\": ", .{
        record.route_name, record.baseline_kernel_id,
    });
    defer allocator.free(header);
    try out.appendSlice(allocator, header);
    try appendFlashScheduleJson(allocator, out, record.baseline_schedule);
    try out.appendSlice(allocator, ",\n      \"variants\": [\n");
    for (record.variants, 0..) |variant, vi| {
        try out.appendSlice(allocator, "        {\"schedule\": ");
        try appendFlashScheduleJson(allocator, out, variant.schedule);
        const tail = try std.fmt.allocPrint(allocator, ", \"correctness_passed\": {s}, \"max_abs_error\": {d:.7}, \"min_speedup_vs_baseline\": {d:.6}, \"is_current_schedule\": {s}}}", .{
            if (variant.correctness_passed) "true" else "false",
            variant.max_error,
            variant.min_speedup,
            if (variant.is_baseline_schedule) "true" else "false",
        });
        defer allocator.free(tail);
        try out.appendSlice(allocator, tail);
        try out.appendSlice(allocator, if (vi + 1 == record.variants.len) "\n" else ",\n");
    }
    try out.appendSlice(allocator, "      ],\n      \"winner\": ");
    if (record.winner_index) |wi| {
        try appendFlashScheduleJson(allocator, out, record.variants[wi].schedule);
    } else {
        try out.appendSlice(allocator, "null");
    }
    try out.appendSlice(allocator, "\n    }");
}

fn appendSweepRouteJson(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), record: SweepRouteRecord) !void {
    const header = try std.fmt.allocPrint(allocator, "    {{\n      \"format\": \"{s}\",\n      \"epilogue\": \"{s}\",\n      \"baseline_kernel_id\": \"{s}\",\n      \"baseline_schedule\": ", .{
        @tagName(record.format), @tagName(record.epilogue), record.baseline_kernel_id,
    });
    defer allocator.free(header);
    try out.appendSlice(allocator, header);
    try appendSweepScheduleJson(allocator, out, record.baseline_schedule);
    try out.appendSlice(allocator, ",\n      \"variants\": [\n");
    for (record.variants, 0..) |variant, vi| {
        try out.appendSlice(allocator, "        {\"schedule\": ");
        try appendSweepScheduleJson(allocator, out, variant.schedule);
        const tail = try std.fmt.allocPrint(allocator, ", \"correctness_passed\": {s}, \"max_abs_error\": {d:.7}, \"min_speedup_vs_baseline\": {d:.6}, \"is_current_schedule\": {s}}}", .{
            if (variant.correctness_passed) "true" else "false",
            variant.max_error,
            variant.min_speedup,
            if (variant.is_baseline_schedule) "true" else "false",
        });
        defer allocator.free(tail);
        try out.appendSlice(allocator, tail);
        try out.appendSlice(allocator, if (vi + 1 == record.variants.len) "\n" else ",\n");
    }
    try out.appendSlice(allocator, "      ],\n      \"winner\": ");
    if (record.winner_index) |wi| {
        try appendSweepScheduleJson(allocator, out, record.variants[wi].schedule);
    } else {
        try out.appendSlice(allocator, "null");
    }
    try out.appendSlice(allocator, "\n    }");
}

// ---- Schedule sweep (autotune) ------------------------------------------
//
// For each generated Metal route, render every valid schedule variant through
// the descriptor-driven renderer and benchmark it on-device against the current
// production kernel for that route, reusing the promotion shapes/tolerances. The
// winner is the variant with the best worst-shape speedup that also passes CPU-
// reference correctness. Variants are rendered in memory and never checked in;
// the winning schedule is fed back by editing metal_production_schedules (a
// human-reviewed step, Phase 4). Nothing here promotes or changes production.

fn reductionName(reduction: quant_kernel_compiler.ReductionKind) []const u8 {
    return switch (reduction) {
        .simd_sum => "simd_sum",
        .threadgroup_tree => "threadgroup_tree",
        .hybrid_simd => "hybrid_simd",
        .simdgroup_tiled => "simdgroup_tiled",
        .simdgroup_matrix => "simdgroup_matrix",
    };
}

// ---- Attention schedule sweep (autotune) ---------------------------------
//
// The attention `op_kind` routes have their own knobs (flash `key_chunk` /
// `skip_rescale`) and their own on-device runner (flash grid / 128 threads /
// key_chunk-sized shmem), so they sweep on a parallel path to the matmul routes
// but emit into the same `antfly.quant_kernel_metal_sweep.v1` evidence. Like the
// matmul sweep, this is a directional filter only: variants are rendered in
// memory, benchmarked against the checked-in baseline, and never promoted here.
// The real arbiter is a prefill decode-runtime A/B on the model (the sweep
// under-reports — it times an isolated dispatch, not a full-layer prefill).

const max_attention_sweep_variants = 4;

/// Prefill tiles used to benchmark the flash sweep. The page-local K/V lowering
/// keeps every variant under the 32 KB threadgroup limit, including Gemma4's
/// head_dim=256 shape. Larger kv_tokens exercise the chunk-count lever
/// (key_chunk=64 halves the flash chunks and their barriers).
const flash_sweep_shapes = [_]FlashPrefillShape{
    .{ .q_len = 128, .kv_tokens = 256, .num_heads = 8, .num_kv_heads = 2, .head_dim = 128, .query_position_offset = 128, .sliding_window = 0 },
    .{ .q_len = 256, .kv_tokens = 256, .num_heads = 8, .num_kv_heads = 2, .head_dim = 128, .query_position_offset = 0, .sliding_window = 0 },
    .{ .q_len = 128, .kv_tokens = 512, .num_heads = 4, .num_kv_heads = 1, .head_dim = 64, .query_position_offset = 384, .sliding_window = 0 },
    .{ .q_len = 256, .kv_tokens = 512, .num_heads = 8, .num_kv_heads = 2, .head_dim = 256, .query_position_offset = 256, .sliding_window = 512 },
};

const flash_sweep_tolerance: f32 = 5e-2;

const AttentionSweepVariantRecord = struct {
    schedule: quant_kernel_compiler.KernelSchedule,
    correctness_passed: bool,
    max_error: f32,
    min_speedup: f64, // worst-shape speedup vs the checked-in baseline schedule
    is_baseline_schedule: bool,
};

const AttentionSweepRouteRecord = struct {
    route_name: []const u8,
    baseline_kernel_id: []const u8,
    baseline_schedule: quant_kernel_compiler.KernelSchedule,
    variants: []AttentionSweepVariantRecord,
    winner_index: ?usize,
};

/// Enumerates the attention sweep variants for a route. Flash-prefill sweeps the
/// `key_chunk ∈ {32,64}` × `skip_rescale ∈ {false,true}` grid (4 variants); the
/// structural 128-thread / 4-simdgroup / 8-query-tile shape is fixed. decode-1x
/// has no tunable knob wired (its 256/NSG shape is structural), so it sweeps to 0.
fn enumerateAttentionSweepVariants(
    kind: quant_kernel_metal_renderer.AttentionKind,
    out: *[max_attention_sweep_variants]quant_kernel_compiler.KernelSchedule,
) usize {
    switch (kind) {
        .decode_1x => return 0,
        .prefill_flash => {
            var count: usize = 0;
            const key_chunks = [_]u16{ 32, 64 };
            const skips = [_]bool{ false, true };
            for (key_chunks) |kc| {
                for (skips) |sk| {
                    out[count] = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = kc, .skip_rescale = sk };
                    count += 1;
                }
            }
            return count;
        },
    }
}

// Times one rendered flash source on one prefill shape, repeated `repeat_runs`
// times, returning the min elapsed nanos (noise floor) + max abs error. Returns
// null on a correctness mismatch or dispatch failure (variant dropped from the
// sweep — e.g. key_chunk=64 that overflows shmem for the shape's head_dim).
fn timeFlashSourceMinNs(
    allocator: std.mem.Allocator,
    shape: FlashPrefillShape,
    source: []const u8,
    kernel_id: []const u8,
    key_chunk: u32,
    measure_iters: u32,
    repeat_runs: u32,
) !?struct { ns: u64, max_error: f32 } {
    var min_ns: u64 = std.math.maxInt(u64);
    var max_error: f32 = 0.0;
    var run: u32 = 0;
    while (run < repeat_runs) : (run += 1) {
        const check = FlashPrefillCheckCase{
            .name = "flash_sweep",
            .source = source,
            .kernel_name = kernel_id,
            .key_chunk = key_chunk,
            .threads_per_threadgroup = 128,
            .threadgroup_memory_bytes = @intCast(24 * shape.head_dim + 52 * key_chunk + 352),
            .tolerance = flash_sweep_tolerance,
            .shape = shape,
            .measure_iters = measure_iters,
        };
        const result = runFlashPrefillCheck(allocator, check) catch |err| {
            if (err == error.GeneratedMetalKernelMismatch or err == error.GeneratedMetalKernelFailed) return null;
            return err;
        };
        if (result.elapsed_nanos < min_ns) min_ns = result.elapsed_nanos;
        if (result.max_error > max_error) max_error = result.max_error;
    }
    return .{ .ns = min_ns, .max_error = max_error };
}

// Runs the flash-prefill schedule sweep (all `flash_sweep_shapes`) and returns an
// owned AttentionSweepRouteRecord. Times the checked-in baseline (key_chunk=32,
// skip_rescale=false) once per shape, then each rendered variant, computing the
// worst-shape speedup and dropping variants that fail correctness on any shape.
fn runAttentionSweepRoute(
    allocator: std.mem.Allocator,
    measure_iters: u32,
    repeat_runs: u32,
) !AttentionSweepRouteRecord {
    const kind = quant_kernel_metal_renderer.AttentionKind.prefill_flash;
    var variant_schedules: [max_attention_sweep_variants]quant_kernel_compiler.KernelSchedule = undefined;
    const variant_count = enumerateAttentionSweepVariants(kind, &variant_schedules);
    if (variant_count == 0) return error.AttentionSweepNoVariants;
    const baseline_schedule = variant_schedules[0]; // key_chunk=32, skip_rescale=false

    // Baseline timing per shape (rendered baseline schedule).
    const baseline_kernel_id = "antfly_paged_attention_prefill_flash_sweep_baseline";
    const baseline_body = try quant_kernel_metal_renderer.renderPrefillFlashKernel(allocator, baseline_kernel_id, baseline_schedule);
    defer allocator.free(baseline_body);
    const baseline_source = try std.fmt.allocPrint(allocator, "#include <metal_stdlib>\nusing namespace metal;\n{s}", .{baseline_body});
    defer allocator.free(baseline_source);
    var baseline_ns: [flash_sweep_shapes.len]u64 = undefined;
    for (flash_sweep_shapes, 0..) |shape, si| {
        const timed = try timeFlashSourceMinNs(allocator, shape, baseline_source, baseline_kernel_id, @intCast(baseline_schedule.key_chunk), measure_iters, repeat_runs) orelse return error.AttentionSweepBaselineFailed;
        baseline_ns[si] = timed.ns;
    }

    var records = try allocator.alloc(AttentionSweepVariantRecord, variant_count);
    var winner_index: ?usize = null;
    var winner_speedup: f64 = 0.0;
    for (variant_schedules[0..variant_count], 0..) |schedule, vi| {
        const kernel_id = try std.fmt.allocPrint(allocator, "antfly_paged_attention_prefill_flash_sweep_kc{d}_{s}", .{
            schedule.key_chunk, if (schedule.skip_rescale) "skip" else "rescale",
        });
        defer allocator.free(kernel_id);
        const body = try quant_kernel_metal_renderer.renderPrefillFlashKernel(allocator, kernel_id, schedule);
        defer allocator.free(body);
        const source = try std.fmt.allocPrint(allocator, "#include <metal_stdlib>\nusing namespace metal;\n{s}", .{body});
        defer allocator.free(source);

        var ok = true;
        var max_error: f32 = 0.0;
        var min_speedup: f64 = std.math.floatMax(f64);
        for (flash_sweep_shapes, 0..) |shape, si| {
            const timed = try timeFlashSourceMinNs(allocator, shape, source, kernel_id, @intCast(schedule.key_chunk), measure_iters, repeat_runs);
            if (timed == null) {
                ok = false;
                break;
            }
            if (timed.?.max_error > max_error) max_error = timed.?.max_error;
            const shape_speedup = @as(f64, @floatFromInt(baseline_ns[si])) / @as(f64, @floatFromInt(@max(timed.?.ns, 1)));
            if (shape_speedup < min_speedup) min_speedup = shape_speedup;
        }
        const is_baseline = schedule.key_chunk == baseline_schedule.key_chunk and schedule.skip_rescale == baseline_schedule.skip_rescale;
        records[vi] = .{
            .schedule = schedule,
            .correctness_passed = ok,
            .max_error = max_error,
            .min_speedup = if (ok) min_speedup else 0.0,
            .is_baseline_schedule = is_baseline,
        };
        if (ok and min_speedup > winner_speedup) {
            winner_speedup = min_speedup;
            winner_index = vi;
        }
    }

    return .{
        .route_name = "attention/prefill_flash",
        .baseline_kernel_id = quant_kernel_compiler.first_prefill_flash_metal_kernel_id,
        .baseline_schedule = baseline_schedule,
        .variants = records,
        .winner_index = winner_index,
    };
}

fn sweepRouteMatchesAttention(filter: []const u8) bool {
    // filter form "attention" or "attention/prefill_flash".
    var it = std.mem.splitScalar(u8, filter, '/');
    const head = it.next() orelse return false;
    if (!std.mem.eql(u8, head, "attention")) return false;
    const tail = it.next() orelse return true; // "attention" matches the whole family
    return std.mem.eql(u8, tail, "prefill_flash");
}

test "quant kernel metal attention sweep enumerates the flash key_chunk x skip_rescale grid" {
    var out: [max_attention_sweep_variants]quant_kernel_compiler.KernelSchedule = undefined;
    const n = enumerateAttentionSweepVariants(.prefill_flash, &out);
    try std.testing.expectEqual(@as(usize, 4), n);
    // The baseline (key_chunk=32, skip_rescale=false) leads so the sweep times it first.
    try std.testing.expectEqual(@as(u16, 32), out[0].key_chunk);
    try std.testing.expect(!out[0].skip_rescale);
    var seen_kc64_skip = false;
    for (out[0..n]) |s| {
        // The structural flash shape is fixed; only the two knobs vary.
        try std.testing.expectEqual(@as(u16, 128), s.threads_per_threadgroup);
        try std.testing.expectEqual(@as(u8, 1), s.cols_per_threadgroup);
        try std.testing.expect(s.key_chunk == 32 or s.key_chunk == 64);
        if (s.key_chunk == 64 and s.skip_rescale) seen_kc64_skip = true;
    }
    try std.testing.expect(seen_kc64_skip);
    // decode-1x has no tunable knob wired.
    try std.testing.expectEqual(@as(usize, 0), enumerateAttentionSweepVariants(.decode_1x, &out));
    // Route filter selects the attention family.
    try std.testing.expect(sweepRouteMatchesAttention("attention"));
    try std.testing.expect(sweepRouteMatchesAttention("attention/prefill_flash"));
    try std.testing.expect(!sweepRouteMatchesAttention("q4_k/none"));
}

const SweepShapeResult = struct {
    baseline_ns: u64,
    variant_ns: u64,
    speedup: f64,
    max_error: f32,
};

// Times one source on one CheckCase, repeated `repeat_runs` times, returning the
// min elapsed nanos (noise floor) and the max abs error. Returns null on a
// correctness mismatch (variant is dropped from the sweep).
fn timeSourceMinNs(
    allocator: std.mem.Allocator,
    base_check: CheckCase,
    source: []const u8,
    kernel_name: []const u8,
    schedule: quant_kernel_compiler.KernelSchedule,
    measure_iters: u32,
    repeat_runs: u32,
) !?struct { ns: u64, max_error: f32 } {
    var check = base_check;
    check.source = source;
    check.kernel_name = kernel_name;
    check.threads_per_threadgroup = schedule.threads_per_threadgroup;
    check.cols_per_threadgroup = schedule.cols_per_threadgroup;
    check.measure_iters = measure_iters;
    var min_ns: u64 = std.math.maxInt(u64);
    var max_error: f32 = 0.0;
    var run: u32 = 0;
    while (run < repeat_runs) : (run += 1) {
        const result = runCheckImpl(allocator, check, null, null, true) catch |err| {
            if (err == error.GeneratedMetalKernelMismatch or err == error.GeneratedMetalKernelFailed) return null;
            return err;
        };
        if (result.elapsed_nanos < min_ns) min_ns = result.elapsed_nanos;
        if (result.max_error > max_error) max_error = result.max_error;
    }
    return .{ .ns = min_ns, .max_error = max_error };
}

fn runCheckImpl(
    allocator: std.mem.Allocator,
    check: CheckCase,
    route_kernel: ?[]const u8,
    promotion_ready_kernel: ?[]const u8,
    conformance_only: bool,
) !CheckResult {
    const dense_weight = try allocator.alloc(f32, check.in_dim * check.out_dim);
    defer allocator.free(dense_weight);
    for (dense_weight, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + 3) % 23)) - 11)) / 16.0;
    }

    const raw_weight = switch (check.format) {
        .q4_0 => try quant_codec.quantizeQ4_0FromF32(allocator, dense_weight),
        .q4_1 => try quant_codec.quantizeQ4_1FromF32(allocator, dense_weight),
        .q5_0 => try quant_codec.quantizeQ5_0FromF32(allocator, dense_weight),
        .q5_1 => try quant_codec.quantizeQ5_1FromF32(allocator, dense_weight),
        .q2_k => try quant_codec.quantizeQ2_KFromF32(allocator, dense_weight),
        .q3_k => try quant_codec.quantizeQ3_KFromF32(allocator, dense_weight),
        .q4_k => try quant_codec.quantizeQ4_KFromF32(allocator, dense_weight),
        .q5_k => try quant_codec.quantizeQ5_KFromF32(allocator, dense_weight),
        .q6_k => try quant_codec.quantizeQ6_KFromF32(allocator, dense_weight),
        .q8_0 => try quant_codec.quantizeQ8_0FromF32(allocator, dense_weight),
        .q8_1 => try quant_codec.quantizeQ8_1FromF32(allocator, dense_weight),
        .q8_k => try quant_codec.quantizeQ8_KFromF32(allocator, dense_weight),
        else => return error.UnsupportedFormat,
    };
    defer allocator.free(raw_weight);

    const input = try allocator.alloc(f32, check.rows * check.in_dim);
    defer allocator.free(input);
    for (input, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 19)) - 9)) / 48.0;
    }

    const bias = try allocator.alloc(f32, check.out_dim);
    defer allocator.free(bias);
    for (bias, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 1)) / 8.0;
    }
    // Exercise the same saturation path used by the resident BERT FFN. The
    // unfused Metal GELU maps non-finite inputs to zero, so fused candidates
    // must not turn a negative infinity into NaN via `x * (1 + tanh(x))`.
    if (check.epilogue == .bias_gelu and bias.len > 0) bias[0] = -std.math.inf(f32);

    const expected = try allocator.alloc(f32, check.rows * check.out_dim);
    defer allocator.free(expected);
    try quant_kernel_compiler.referenceMatmulEpilogue(
        allocator,
        check.format,
        raw_weight,
        input,
        if (check.epilogue == .none) null else bias,
        check.rows,
        check.in_dim,
        check.out_dim,
        check.epilogue,
        expected,
    );

    const actual = try allocator.alloc(f32, expected.len);
    defer allocator.free(actual);
    @memset(actual, 0);

    const kernel_name_z = try allocator.dupeSentinel(u8, check.kernel_name, 0);
    defer allocator.free(kernel_name_z);
    const needs_bias = epilogueNeedsBias(check.epilogue);
    const bias_ptr: ?[*]const f32 = if (needs_bias) bias.ptr else null;
    const bias_count: usize = if (needs_bias) bias.len else 0;
    var elapsed_nanos: u64 = 0;
    const rc = termite_metal_run_generated_quant_kernel_check(
        check.source.ptr,
        check.source.len,
        kernel_name_z.ptr,
        input.ptr,
        input.len,
        raw_weight.ptr,
        raw_weight.len,
        bias_ptr,
        bias_count,
        actual.ptr,
        actual.len,
        @intCast(check.rows),
        @intCast(check.in_dim),
        @intCast(check.out_dim),
        check.threads_per_threadgroup,
        check.cols_per_threadgroup,
        check.warmup_iters,
        check.measure_iters,
        &elapsed_nanos,
    );
    if (rc != 0) {
        std.debug.print("generated Metal kernel {s} failed rc={d}\n", .{ check.name, rc });
        return error.GeneratedMetalKernelFailed;
    }

    var max_error: f32 = 0.0;
    for (actual, expected, 0..) |got, want, index| {
        if (!std.math.isFinite(got)) {
            std.debug.print("generated Metal kernel {s} nonfinite at {d}: got={d} want={d}\n", .{ check.name, index, got, want });
            return error.GeneratedMetalKernelMismatch;
        }
        const diff = @abs(got - want);
        max_error = @max(max_error, diff);
        if (diff > check.tolerance) {
            std.debug.print("generated Metal kernel {s} mismatch at {d}: got={d} want={d} diff={d}\n", .{ check.name, index, got, want, diff });
            return error.GeneratedMetalKernelMismatch;
        }
    }
    // Conformance-only callers (rendered v2 kernels) want just the standalone
    // source-vs-CPU-reference correctness result; skip all baseline/route timing
    // (which asserts production route selection and would trip on promoted routes).
    if (conformance_only) {
        return .{ .max_error = max_error, .measure_iters = check.measure_iters, .elapsed_nanos = elapsed_nanos };
    }
    const selected_for_route = route_kernel != null and std.mem.eql(u8, route_kernel.?, check.kernel_name);
    const selected_for_promotion = promotion_ready_kernel != null and std.mem.eql(u8, promotion_ready_kernel.?, check.kernel_name);
    const runtime_bias: ?[]const f32 = if (check.epilogue == .none) null else bias;
    const generated_route_elapsed_nanos = if (selected_for_route and !generatedRouteSupported(check))
        try runGeneratedRouteForPromotion(allocator, check, raw_weight, input, runtime_bias, expected)
    else
        try runProductionRouteIfGenerated(allocator, check, raw_weight, input, runtime_bias, expected);
    const generated_route_checked = generated_route_elapsed_nanos != null;
    const provider_route_checked = try runProviderRouteIfSupported(allocator, check, raw_weight, input, bias, expected, selected_for_promotion or selected_for_route);
    const handwritten_elapsed_nanos = try runHandwrittenBaselineIfSupported(allocator, check, raw_weight, input, bias, expected);
    const route_timing_selected = (selected_for_route or selected_for_promotion) and generated_route_elapsed_nanos != null;
    return .{
        .max_error = max_error,
        .measure_iters = check.measure_iters,
        .elapsed_nanos = if (route_timing_selected) generated_route_elapsed_nanos.? else elapsed_nanos,
        .generated_timing_route = if (route_timing_selected) .decode_runtime_generated else .standalone_generated,
        .handwritten_elapsed_nanos = handwritten_elapsed_nanos,
        .generated_route_checked = generated_route_checked,
        .provider_route_checked = provider_route_checked,
    };
}

fn runHandwrittenBaselineIfSupported(
    allocator: std.mem.Allocator,
    check: CheckCase,
    raw_weight: []const u8,
    input: []const f32,
    bias: []const f32,
    expected: []const f32,
) !?u64 {
    if (!handwrittenBaselineSupported(check)) return null;
    const master_disable_env = "TERMITE_METAL_DISABLE_ANTFLY_GENERATED_QUANT";
    const old_master_disable = std.c.getenv(master_disable_env);
    const old_master_disable_copy = if (old_master_disable) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_master_disable_copy) |value| allocator.free(value);
    if (setenv(master_disable_env, "1", 1) != 0) return error.MetalRuntimeUnavailable;
    defer if (old_master_disable_copy) |value| {
        _ = setenv(master_disable_env, value.ptr, 1);
    } else {
        _ = unsetenv(master_disable_env);
    };

    if (isQuantBiasEpilogue(check)) {
        return try runQuantBiasSplitBaseline(allocator, check, raw_weight, input, bias, expected);
    }
    return try runDecodeRuntime(allocator, check, raw_weight, input, null, expected, "handwritten", false);
}

fn runQuantBiasSplitBaseline(
    allocator: std.mem.Allocator,
    check: CheckCase,
    raw_weight: []const u8,
    input: []const f32,
    bias: []const f32,
    expected: []const f32,
) !u64 {
    const runtime = termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.MetalRuntimeUnavailable;
    const format = metalFormatFor(check.format) orelse return error.MetalRuntimeUnavailable;

    const disable_env = disableEnvForSplitBaselineLinearRoute(check);
    const old_disable = if (disable_env) |env_name| std.c.getenv(env_name) else null;
    const old_disable_copy = if (old_disable) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_disable_copy) |value| allocator.free(value);
    if (disable_env) |env_name| {
        if (setenv(env_name, "1", 1) != 0) return error.MetalRuntimeUnavailable;
    }
    defer if (disable_env) |env_name| {
        if (old_disable_copy) |value| {
            _ = setenv(env_name, value.ptr, 1);
        } else {
            _ = unsetenv(env_name);
        }
    };

    const slot: usize = 0;
    const prep_rc = termite_metal_decode_runtime_prepare_quantized_linear_slot(
        runtime,
        format,
        slot,
        raw_weight.ptr,
        raw_weight.len,
        check.in_dim,
        check.out_dim,
    );
    if (prep_rc != 0) {
        std.debug.print("split Metal baseline prepare failed {s} rc={d}\n", .{ check.name, prep_rc });
        return error.HandwrittenMetalBaselineFailed;
    }

    const input_bytes = input.len * @sizeOf(f32);
    const bias_bytes = bias.len * @sizeOf(f32);
    const output_bytes = expected.len * @sizeOf(f32);
    const input_buffer = termite_metal_buffer_alloc(runtime, input_bytes, metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(input_buffer);
    const bias_buffer = termite_metal_buffer_alloc(runtime, bias_bytes, metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(bias_buffer);
    const output_buffer = termite_metal_buffer_alloc(runtime, output_bytes, metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(output_buffer);

    if (termite_metal_buffer_upload(runtime, input_buffer, 0, input.ptr, input_bytes) != 0) return error.HandwrittenMetalBaselineFailed;
    if (termite_metal_buffer_upload(runtime, bias_buffer, 0, bias.ptr, bias_bytes) != 0) return error.HandwrittenMetalBaselineFailed;

    if (check.warmup_iters != 0) {
        try beginDecodeFrame(runtime, "split baseline", check);
        for (0..check.warmup_iters) |_| try applyQuantBiasSplitBaseline(runtime, slot, input_buffer, bias_buffer, output_buffer, check);
        try submitAndWaitDecodeFrame(runtime, "split baseline", check);
    }
    try beginDecodeFrame(runtime, "split baseline", check);
    const start = nowNs();
    for (0..check.measure_iters) |_| try applyQuantBiasSplitBaseline(runtime, slot, input_buffer, bias_buffer, output_buffer, check);
    try submitAndWaitDecodeFrame(runtime, "split baseline", check);
    const elapsed = nowNs() - start;

    const actual = try allocator.alloc(f32, expected.len);
    defer allocator.free(actual);
    if (termite_metal_buffer_download(runtime, output_buffer, 0, actual.ptr, output_bytes) != 0) return error.HandwrittenMetalBaselineFailed;
    try expectClose(check.name, "split baseline", actual, expected, check.tolerance);
    return elapsed;
}

fn applyQuantBiasSplitBaseline(
    runtime: *RawMetalDecodeRuntime,
    slot: usize,
    input_buffer: ?*anyopaque,
    bias_buffer: ?*anyopaque,
    output_buffer: ?*anyopaque,
    check: CheckCase,
) !void {
    const rc = switch (check.format) {
        .q4_k => if (check.epilogue == .bias)
            termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            )
        else
            termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_gelu_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            ),
        .q5_k => if (check.epilogue == .bias)
            termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            )
        else
            termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_gelu_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            ),
        .q6_k => if (check.epilogue == .bias)
            termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            )
        else
            termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_gelu_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            ),
        .q8_0 => if (check.epilogue == .bias)
            termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            )
        else
            termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_gelu_split_slot_device(
                runtime,
                slot,
                input_buffer,
                0,
                bias_buffer,
                0,
                check.rows,
                check.in_dim,
                check.out_dim,
                output_buffer,
                0,
            ),
        else => -1,
    };
    if (rc != 0) {
        std.debug.print("split Metal baseline apply failed {s} rc={d}\n", .{ check.name, rc });
        return error.HandwrittenMetalBaselineFailed;
    }
}

fn runProductionRouteIfGenerated(
    allocator: std.mem.Allocator,
    check: CheckCase,
    raw_weight: []const u8,
    input: []const f32,
    bias: ?[]const f32,
    expected: []const f32,
) !?u64 {
    const route = quant_kernel_compiler.loweringFor(.metal, check.format, quant_matmul.rowBucket(check.rows), check.epilogue);
    if (route.production_route != .generated_production) return null;
    const gate_override = productionRouteGateOverride(check);
    const route_env_name = if (gate_override) |override| override.name else null;
    var old_value_copy: ?[:0]u8 = null;
    defer if (old_value_copy) |value| allocator.free(value);
    defer if (route_env_name) |env_name| {
        if (old_value_copy) |value| {
            _ = setenv(env_name, value.ptr, 1);
        } else {
            _ = unsetenv(env_name);
        }
    };
    if (route_env_name) |env_name| {
        const old_value = std.c.getenv(env_name);
        old_value_copy = if (old_value) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
        if (gate_override.?.enable) {
            if (setenv(env_name, "1", 1) != 0) return error.MetalRuntimeUnavailable;
        } else if (unsetenv(env_name) != 0) {
            return error.MetalRuntimeUnavailable;
        }
    }
    return try runDecodeRuntime(allocator, check, raw_weight, input, bias, expected, "production", true);
}

fn runGeneratedRouteForPromotion(
    allocator: std.mem.Allocator,
    check: CheckCase,
    raw_weight: []const u8,
    input: []const f32,
    bias: ?[]const f32,
    expected: []const f32,
) !?u64 {
    if (try runProductionRouteIfGenerated(allocator, check, raw_weight, input, bias, expected)) |elapsed| return elapsed;
    const env_name = enableEnvForGeneratedCandidateRoute(check) orelse return null;
    const old_value = std.c.getenv(env_name);
    const old_value_copy = if (old_value) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (old_value_copy) |value| allocator.free(value);
    if (setenv(env_name, "1", 1) != 0) return error.MetalRuntimeUnavailable;
    defer {
        if (old_value_copy) |value| {
            _ = setenv(env_name, value.ptr, 1);
        } else {
            _ = unsetenv(env_name);
        }
    }
    return try runDecodeRuntime(allocator, check, raw_weight, input, bias, expected, "promotion", true);
}

fn runProviderRouteIfSupported(
    allocator: std.mem.Allocator,
    check: CheckCase,
    raw_weight: []const u8,
    input: []const f32,
    bias: []const f32,
    expected: []const f32,
    selected_for_generated_candidate: bool,
) !bool {
    const route_supported = providerRouteSupported(check);
    const use_candidate_route = selected_for_generated_candidate and providerCandidateRouteSupported(check);
    if (!route_supported and !use_candidate_route) return false;
    var enabled_env_name: ?[*:0]const u8 = null;
    var enabled_old_value_copy: ?[:0]u8 = null;
    defer if (enabled_old_value_copy) |value| allocator.free(value);
    defer if (enabled_env_name) |env_name| {
        if (enabled_old_value_copy) |value| {
            _ = setenv(env_name, value.ptr, 1);
        } else {
            _ = unsetenv(env_name);
        }
    };
    if (use_candidate_route) {
        const env_name = enableEnvForGeneratedCandidateRoute(check) orelse return false;
        const old_value = std.c.getenv(env_name);
        enabled_old_value_copy = if (old_value) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
        enabled_env_name = env_name;
        if (setenv(env_name, "1", 1) != 0) return error.MetalRuntimeUnavailable;
    }
    var disabled_env_name: ?[*:0]const u8 = null;
    var disabled_old_value_copy: ?[:0]u8 = null;
    defer if (disabled_old_value_copy) |value| allocator.free(value);
    defer if (disabled_env_name) |env_name| {
        if (disabled_old_value_copy) |value| {
            _ = setenv(env_name, value.ptr, 1);
        } else {
            _ = unsetenv(env_name);
        }
    };
    if (disableEnvForGeneratedProductionRoute(check)) |env_name| {
        const old_value = std.c.getenv(env_name);
        disabled_old_value_copy = if (old_value) |value| try allocator.dupeSentinel(u8, std.mem.span(value), 0) else null;
        disabled_env_name = env_name;
        if (unsetenv(env_name) != 0) return error.MetalRuntimeUnavailable;
    }

    const provider = termite_metal_provider_create() orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_provider_destroy(provider);

    const actual = try allocator.alloc(f32, expected.len);
    defer allocator.free(actual);
    @memset(actual, 0);
    const rc = switch (check.format) {
        .q8_0 => switch (check.epilogue) {
            .bias => termite_metal_provider_linear_q8_0_bias(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            .bias_gelu => termite_metal_provider_linear_q8_0_bias_gelu(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            .relu => termite_metal_provider_linear_q8_0_relu(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
            else => termite_metal_provider_linear_q8_0_planned(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, @backingInt(quant_matmul.DispatchKind.small_batch), actual.ptr),
        },
        .q8_1 => termite_metal_provider_linear_q8_1(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        .q8_k => termite_metal_provider_linear_q8_k(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        .q2_k => switch (check.epilogue) {
            .bias => termite_metal_provider_linear_q2_k_bias(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            .bias_gelu => termite_metal_provider_linear_q2_k_bias_gelu(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            else => termite_metal_provider_linear_q2_k(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        },
        .q5_0 => termite_metal_provider_linear_q5_0(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        .q5_1 => termite_metal_provider_linear_q5_1(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        .q3_k => switch (check.epilogue) {
            .bias => termite_metal_provider_linear_q3_k_bias(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            .bias_gelu => termite_metal_provider_linear_q3_k_bias_gelu(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            else => termite_metal_provider_linear_q3_k(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        },
        .q4_k => switch (check.epilogue) {
            .bias => termite_metal_provider_linear_q4_k_bias(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            .bias_gelu => termite_metal_provider_linear_q4_k_bias_gelu(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            else => termite_metal_provider_linear_q4_k(provider, null, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        },
        .q5_k => switch (check.epilogue) {
            .bias => termite_metal_provider_linear_q5_k_bias(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            .bias_gelu => termite_metal_provider_linear_q5_k_bias_gelu(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            else => termite_metal_provider_linear_q5_k(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        },
        .q6_k => switch (check.epilogue) {
            .bias => termite_metal_provider_linear_q6_k_bias(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            .bias_gelu => termite_metal_provider_linear_q6_k_bias_gelu(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, bias.ptr, actual.ptr),
            else => termite_metal_provider_linear_q6_k(provider, input.ptr, check.rows, check.in_dim, raw_weight.ptr, check.out_dim, actual.ptr),
        },
        else => unreachable,
    };
    if (rc != 0) {
        std.debug.print("provider Metal route failed {s} rc={d}\n", .{ check.name, rc });
        return error.ProviderMetalRouteFailed;
    }
    try expectClose(check.name, "provider", actual, expected, check.tolerance);
    return true;
}

fn providerRouteSupported(check: CheckCase) bool {
    const route = loweringForCheck(check);
    const artifact = metalArtifactForCheck(check) orelse return false;
    return route.production_route == .generated_production and
        quant_kernel_compiler.artifactHasMetalProviderRouteEvidence(artifact);
}

fn providerCandidateRouteSupported(check: CheckCase) bool {
    const artifact = metalArtifactForCheck(check) orelse return false;
    return enableEnvForGeneratedCandidateRoute(check) != null and
        quant_kernel_compiler.artifactHasMetalProviderRouteEvidence(artifact);
}

fn runtimeRouteAllSupported(check: CheckCase) bool {
    const artifact = metalArtifactForCheck(check) orelse return false;
    return quant_kernel_compiler.artifactRuntimeWired(artifact);
}

fn enableEnvForGeneratedCandidateRoute(check: CheckCase) ?[*:0]const u8 {
    const artifact = metalArtifactForCheck(check) orelse return null;
    const env_name = quant_kernel_compiler.artifactRuntimeGateEnv(artifact) orelse return null;
    return if (std.mem.startsWith(u8, std.mem.span(env_name), "TERMITE_METAL_ENABLE_")) env_name else null;
}

fn disableEnvForGeneratedProductionRoute(check: CheckCase) ?[*:0]const u8 {
    const artifact = metalArtifactForCheck(check) orelse return null;
    const env_name = quant_kernel_compiler.artifactRuntimeGateEnv(artifact) orelse return null;
    return if (std.mem.startsWith(u8, std.mem.span(env_name), "TERMITE_METAL_DISABLE_")) env_name else null;
}

const ProductionRouteGateOverride = struct {
    name: [*:0]const u8,
    enable: bool,
};

fn productionRouteGateOverride(check: CheckCase) ?ProductionRouteGateOverride {
    if (disableEnvForGeneratedProductionRoute(check)) |name| return .{ .name = name, .enable = false };
    if (enableEnvForGeneratedCandidateRoute(check)) |name| return .{ .name = name, .enable = true };
    return null;
}

fn disableEnvForSplitBaselineLinearRoute(check: CheckCase) ?[*:0]const u8 {
    if (!isQuantBiasEpilogue(check)) return null;
    var linear_check = check;
    linear_check.epilogue = .none;
    return disableEnvForGeneratedProductionRoute(linear_check);
}

fn runtimeMemorySnapshot(runtime: ?*RawMetalDecodeRuntime) metal_runtime.RawRuntimeMemoryStats {
    var snapshot: metal_runtime.RawRuntimeMemoryStats = .{};
    _ = termite_metal_decode_runtime_memory_snapshot(runtime, &snapshot);
    return snapshot;
}

fn generatedDispatchCount(check: CheckCase, snapshot: metal_runtime.RawRuntimeMemoryStats) ?u64 {
    if (check.rows < 2 or check.rows > 8) return null;
    // Only the wired (format, epilogue) pairs from the shared counter-name
    // table have a dispatch counter; everything else reports null so callers
    // can distinguish "no generated route" from "zero dispatches".
    for (quant_matmul.generated_quant_counter_names) |counter| {
        if (!std.mem.eql(u8, @tagName(counter.format), @tagName(check.format))) continue;
        if (!std.mem.eql(u8, @tagName(counter.epilogue), @tagName(check.epilogue))) continue;
        return quant_matmul.generatedQuantDispatchCount(&snapshot.antfly_generated_dispatch_counts, counter.format, counter.epilogue);
    }
    return null;
}

fn generatedRouteSupported(check: CheckCase) bool {
    return quant_kernel_compiler.loweringFor(.metal, check.format, quant_matmul.rowBucket(check.rows), check.epilogue).production_route == .generated_production;
}

fn beginDecodeFrame(runtime: *RawMetalDecodeRuntime, label: []const u8, check: CheckCase) !void {
    const rc = termite_metal_decode_runtime_begin_frame(runtime);
    if (rc != 0) {
        std.debug.print("{s} Metal runtime begin frame failed {s} rc={d}\n", .{ label, check.name, rc });
        return error.MetalRuntimeUnavailable;
    }
}

fn submitAndWaitDecodeFrame(runtime: *RawMetalDecodeRuntime, label: []const u8, check: CheckCase) !void {
    const submit_rc = termite_metal_decode_runtime_submit_frame(runtime);
    if (submit_rc != 0) {
        std.debug.print("{s} Metal runtime submit frame failed {s} rc={d}\n", .{ label, check.name, submit_rc });
        return error.HandwrittenMetalBaselineFailed;
    }
    const wait_rc = termite_metal_decode_runtime_wait_frame(runtime);
    if (wait_rc != 0) {
        std.debug.print("{s} Metal runtime wait frame failed {s} rc={d}\n", .{ label, check.name, wait_rc });
        return error.HandwrittenMetalBaselineFailed;
    }
}

fn runDecodeRuntime(
    allocator: std.mem.Allocator,
    check: CheckCase,
    raw_weight: []const u8,
    input: []const f32,
    bias: ?[]const f32,
    expected: []const f32,
    label: []const u8,
    require_generated_route: bool,
) !u64 {
    const format = metalFormatFor(check.format) orelse return error.MetalRuntimeUnavailable;
    const runtime = termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.MetalRuntimeUnavailable;

    const slot: usize = 0;
    const prep_rc = termite_metal_decode_runtime_prepare_quantized_linear_slot(
        runtime,
        format,
        slot,
        raw_weight.ptr,
        raw_weight.len,
        check.in_dim,
        check.out_dim,
    );
    if (prep_rc != 0) {
        std.debug.print("{s} Metal runtime prepare failed {s} rc={d}\n", .{ label, check.name, prep_rc });
        return error.HandwrittenMetalBaselineFailed;
    }

    const input_bytes = input.len * @sizeOf(f32);
    const output_bytes = expected.len * @sizeOf(f32);
    const input_buffer = termite_metal_buffer_alloc(runtime, input_bytes, metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(input_buffer);
    const output_buffer = termite_metal_buffer_alloc(runtime, output_bytes, metal_storage_private) orelse return error.MetalRuntimeUnavailable;
    defer termite_metal_buffer_release(output_buffer);
    const bias_buffer = if (bias) |values| termite_metal_buffer_alloc(runtime, values.len * @sizeOf(f32), metal_storage_private) orelse return error.MetalRuntimeUnavailable else null;
    defer if (bias_buffer) |buffer| termite_metal_buffer_release(buffer);

    const upload_rc = termite_metal_buffer_upload(runtime, input_buffer, 0, input.ptr, input_bytes);
    if (upload_rc != 0) {
        std.debug.print("{s} Metal runtime input upload failed {s} rc={d}\n", .{ label, check.name, upload_rc });
        return error.HandwrittenMetalBaselineFailed;
    }
    if (bias) |values| {
        const bias_upload_rc = termite_metal_buffer_upload(runtime, bias_buffer, 0, values.ptr, values.len * @sizeOf(f32));
        if (bias_upload_rc != 0) {
            std.debug.print("{s} Metal runtime bias upload failed {s} rc={d}\n", .{ label, check.name, bias_upload_rc });
            return error.HandwrittenMetalBaselineFailed;
        }
    }

    if (check.warmup_iters != 0) {
        try beginDecodeFrame(runtime, label, check);
        for (0..check.warmup_iters) |_| try applyDecodeRuntimeLinear(runtime, format, slot, input_buffer, bias_buffer, output_buffer, check, label);
        try submitAndWaitDecodeFrame(runtime, label, check);
    }
    try beginDecodeFrame(runtime, label, check);
    const start = nowNs();
    for (0..check.measure_iters) |_| try applyDecodeRuntimeLinear(runtime, format, slot, input_buffer, bias_buffer, output_buffer, check, label);
    try submitAndWaitDecodeFrame(runtime, label, check);
    const elapsed = nowNs() - start;
    const snapshot = runtimeMemorySnapshot(runtime);
    if (require_generated_route) {
        const count = generatedDispatchCount(check, snapshot) orelse {
            std.debug.print("production Metal route has no generated dispatch counter {s}\n", .{check.name});
            return error.GeneratedMetalProductionRouteNotUsed;
        };
        const expected_count = @as(u64, check.warmup_iters) + @as(u64, check.measure_iters);
        if (count != expected_count) {
            std.debug.print("production Metal route used generated kernel {s} {d}/{d} times\n", .{ check.name, count, expected_count });
            return error.GeneratedMetalProductionRouteNotUsed;
        }
    } else if (generatedDispatchCount(check, snapshot)) |count| {
        if (count != 0) {
            std.debug.print("default Metal route unexpectedly used generated kernel {s} {d} times\n", .{ check.name, count });
            return error.GeneratedMetalCandidateUnexpectedlyUsed;
        }
    }

    const actual = try allocator.alloc(f32, expected.len);
    defer allocator.free(actual);
    const download_rc = termite_metal_buffer_download(runtime, output_buffer, 0, actual.ptr, output_bytes);
    if (download_rc != 0) {
        std.debug.print("{s} Metal runtime output download failed {s} rc={d}\n", .{ label, check.name, download_rc });
        return error.HandwrittenMetalBaselineFailed;
    }
    try expectClose(check.name, label, actual, expected, check.tolerance);
    return elapsed;
}

fn applyDecodeRuntimeLinear(
    runtime: *RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    input_buffer: ?*anyopaque,
    bias_buffer: ?*anyopaque,
    output_buffer: ?*anyopaque,
    check: CheckCase,
    label: []const u8,
) !void {
    const rc = if (check.format == .q2_k and check.epilogue == .bias)
        termite_metal_decode_runtime_apply_quantized_linear_q2_k_bias_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q2_k and check.epilogue == .bias_gelu)
        termite_metal_decode_runtime_apply_quantized_linear_q2_k_bias_gelu_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q3_k and check.epilogue == .bias)
        termite_metal_decode_runtime_apply_quantized_linear_q3_k_bias_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q3_k and check.epilogue == .bias_gelu)
        termite_metal_decode_runtime_apply_quantized_linear_q3_k_bias_gelu_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q4_k and check.epilogue == .bias)
        termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q4_k and check.epilogue == .bias_gelu)
        termite_metal_decode_runtime_apply_quantized_linear_q4_k_bias_gelu_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q5_k and check.epilogue == .bias)
        termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q5_k and check.epilogue == .bias_gelu)
        termite_metal_decode_runtime_apply_quantized_linear_q5_k_bias_gelu_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q6_k and check.epilogue == .bias)
        termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q6_k and check.epilogue == .bias_gelu)
        termite_metal_decode_runtime_apply_quantized_linear_q6_k_bias_gelu_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q8_0 and check.epilogue == .bias)
        termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q8_0 and check.epilogue == .bias_gelu)
        termite_metal_decode_runtime_apply_quantized_linear_q8_0_bias_gelu_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            bias_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else if (check.format == .q8_0 and check.epilogue == .relu)
        termite_metal_decode_runtime_apply_quantized_linear_q8_0_relu_slot_device(
            runtime,
            slot,
            input_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        )
    else
        termite_metal_decode_runtime_apply_quantized_linear_slot_device(
            runtime,
            format,
            slot,
            input_buffer,
            0,
            check.rows,
            check.in_dim,
            check.out_dim,
            output_buffer,
            0,
        );
    if (rc != 0) {
        std.debug.print("{s} Metal runtime apply failed {s} rc={d}\n", .{ label, check.name, rc });
        return error.HandwrittenMetalBaselineFailed;
    }
}

fn metalFormatFor(format: quant_matmul.Format) ?u32 {
    return switch (format) {
        .q2_k => metal_quant_format_q2_k,
        .q3_k => metal_quant_format_q3_k,
        .q4_0 => metal_quant_format_q4_0,
        .q4_1 => metal_quant_format_q4_1,
        .q5_0 => metal_quant_format_q5_0,
        .q5_1 => metal_quant_format_q5_1,
        .q4_k => metal_quant_format_q4_k,
        .q5_k => metal_quant_format_q5_k,
        .q6_k => metal_quant_format_q6_k,
        .q8_0 => metal_quant_format_q8_0,
        .q8_1 => metal_quant_format_q8_1,
        .q8_k => metal_quant_format_q8_k,
        else => null,
    };
}

fn expectClose(name: []const u8, label: []const u8, actual: []const f32, expected: []const f32, tolerance: f32) !void {
    for (actual, expected, 0..) |got, want, index| {
        if (!std.math.isFinite(got)) {
            std.debug.print("{s} Metal kernel {s} nonfinite at {d}: got={d} want={d}\n", .{ label, name, index, got, want });
            return error.GeneratedMetalKernelMismatch;
        }
        const diff = @abs(got - want);
        if (diff > tolerance) {
            std.debug.print("{s} Metal kernel {s} mismatch at {d}: got={d} want={d} diff={d}\n", .{ label, name, index, got, want, diff });
            return error.GeneratedMetalKernelMismatch;
        }
    }
}

fn writeEvidence(
    allocator: std.mem.Allocator,
    path: []const u8,
    checks: []const CheckCase,
    results: []const CheckResult,
    repeat_runs: u32,
    promotion_ready_kernel: ?[]const u8,
    runtime_route_kernel: ?[]const u8,
    runtime_route_all: bool,
    production_regression_check: bool,
    emit_promotion_diagnostics: bool,
    attested_provenance: ?AttestedProvenance,
) !void {
    if (checks.len != results.len) return error.InvalidArgument;
    if (promotion_ready_kernel) |kernel| {
        if (!hasPromotionRepeatRuns(repeat_runs) or !checksContainKernel(checks, kernel)) return error.InvalidArgument;
    } else if (runtime_route_kernel) |kernel| {
        if (!checksContainKernel(checks, kernel)) return error.InvalidArgument;
    } else if (runtime_route_all) {
        for (checks) |check| {
            if (!runtimeRouteAllSupported(check)) return error.InvalidArgument;
        }
    } else if (production_regression_check) {
        if (!hasPromotionRepeatRuns(repeat_runs)) return error.InvalidArgument;
        for (checks) |check| {
            if (!productionMetalRuntimeCheck(check)) return error.InvalidArgument;
            if (check.measure_iters != quant_kernel_compiler.metal_promotion_measure_iters) return error.InvalidArgument;
        }
    }
    if (production_regression_check and (promotion_ready_kernel != null or runtime_route_kernel != null or runtime_route_all)) return error.InvalidArgument;

    const promotion_arg = if (promotion_ready_kernel) |kernel|
        try std.fmt.allocPrint(allocator, " --promotion-ready-kernel {s}", .{kernel})
    else if (production_regression_check)
        " --production-regression-check"
    else
        "";
    defer if (promotion_ready_kernel != null) allocator.free(promotion_arg);
    const runtime_route_arg = if (runtime_route_kernel) |kernel|
        try std.fmt.allocPrint(allocator, " --runtime-route-kernel {s}", .{kernel})
    else if (runtime_route_all)
        " --runtime-route-all"
    else
        "";
    defer if (runtime_route_kernel != null) allocator.free(runtime_route_arg);
    const route_args = try std.fmt.allocPrint(allocator, "{s}{s}", .{ promotion_arg, runtime_route_arg });
    defer allocator.free(route_args);
    const attestation_arg = if (attested_provenance != null) " --attest-provenance" else "";
    const measure_iters = if (checks.len == 0) default_measure_iters else checks[0].measure_iters;
    if (production_regression_check and measure_iters != quant_kernel_compiler.metal_promotion_measure_iters) return error.InvalidArgument;
    const measure_iters_arg = if (measure_iters != default_measure_iters)
        try std.fmt.allocPrint(allocator, " --measure-iters {d}", .{measure_iters})
    else
        "";
    defer if (measure_iters != default_measure_iters) allocator.free(measure_iters_arg);
    const benchmark_command = if (repeat_runs == 1)
        try std.fmt.allocPrint(
            allocator,
            "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out {s}{s}{s}{s}",
            .{ path, measure_iters_arg, attestation_arg, route_args },
        )
    else
        try std.fmt.allocPrint(
            allocator,
            "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out {s} --repeat-runs {d}{s}{s}{s}",
            .{ path, repeat_runs, measure_iters_arg, attestation_arg, route_args },
        );
    defer allocator.free(benchmark_command);
    const production_enabled = promotion_ready_kernel != null or production_regression_check;
    const timing_aggregation = timingAggregationName(repeat_runs);
    const warmup_repeat_runs: u32 = if (repeat_runs > 1) repeated_check_warmup_runs else 0;
    const runtime_route_kernel_json = if (runtime_route_kernel) |kernel|
        try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(kernel, .{})})
    else
        "null";
    defer if (runtime_route_kernel != null) allocator.free(runtime_route_kernel_json);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try appendJsonFmt(allocator, &out,
        \\{{
        \\"evidence_contract":"{s}",
        \\"schema":"{s}",
        \\"provenance_status":{f},
        \\"provenance_blocker":{f},
        \\"source_commit":{f},
        \\"source_tree_clean":{s},
        \\"source_status_sha256":{f},
        \\"host_os":{f},
        \\"host_arch":{f},
        \\"accelerator_name":{f},
        \\"metal_compiler_version":{f},
        \\"zig_version":{f},
        \\"recorded_at_utc":{f},
        \\"benchmark_command":{f},
        \\"benchmark_mode":"sequential",
        \\"repeat_runs":{d},
        \\"warmup_repeat_runs":{d},
        \\"timing_aggregation":{f},
        \\"minimum_speedup":{d:.6},
        \\"minimum_speedup_tolerance":{d:.6},
        \\"production_enabled":{s},
        \\"production_regression_check":{s},
        \\"compiler_benchmark_manifest_schema":{f},
        \\"compiler_benchmark_manifest_case_count":{d},
        \\"compiler_benchmark_manifest_case_fingerprint":{d},
        \\"runtime_route_kernel":{s},
        \\"runtime_route_all":{s},
        \\"case_count":{d},
        \\"cases":[
        \\
    , .{
        metal_quant_evidence_contract,
        metal_runtime_evidence_schema,
        std.json.fmt(if (attested_provenance != null) metal_runtime_evidence_provenance_attested else metal_runtime_evidence_provenance_local, .{}),
        std.json.fmt(if (attested_provenance != null) "" else metal_runtime_evidence_provenance_missing, .{}),
        std.json.fmt(if (attested_provenance) |provenance| provenance.source_commit else "", .{}),
        jsonBool(if (attested_provenance) |provenance| provenance.source_tree_clean else false),
        std.json.fmt(if (attested_provenance) |provenance| provenance.source_status_sha256 else "", .{}),
        std.json.fmt(if (attested_provenance) |provenance| provenance.host_os else "", .{}),
        std.json.fmt(if (attested_provenance) |provenance| provenance.host_arch else "", .{}),
        std.json.fmt(if (attested_provenance) |provenance| provenance.accelerator_name else "", .{}),
        std.json.fmt(if (attested_provenance) |provenance| provenance.metal_compiler_version else "", .{}),
        std.json.fmt(if (attested_provenance) |provenance| provenance.zig_version else "", .{}),
        std.json.fmt(if (attested_provenance) |provenance| provenance.recorded_at_utc else "", .{}),
        std.json.fmt(benchmark_command, .{}),
        repeat_runs,
        warmup_repeat_runs,
        std.json.fmt(timing_aggregation, .{}),
        quant_kernel_compiler.metal_promotion_min_speedup,
        quant_kernel_compiler.metal_promotion_speedup_tolerance,
        jsonBool(production_enabled),
        jsonBool(production_regression_check),
        std.json.fmt(quant_kernel_compiler.first_benchmark_manifest_schema, .{}),
        quant_kernel_compiler.first_metal_production_benchmark_case_count,
        quant_kernel_compiler.metalProductionBenchmarkCaseManifestFingerprint(),
        runtime_route_kernel_json,
        jsonBool(runtime_route_all),
        checks.len,
    });

    var promotion_case_count: usize = 0;
    var promotion_ready_count: usize = 0;
    var runtime_route_checked_count: usize = 0;
    var provider_route_checked_count: usize = 0;
    var candidate_route_ready_count: usize = 0;
    var candidate_benchmark_ready_count: usize = 0;
    var promotion_worst_repeat_speedup: ?f64 = null;
    var promotion_worst_repeat_case: ?[]const u8 = null;
    var benchmark_speedups = BenchmarkSpeedupSummary{};
    var blocker_counts = PromotionBlockerCounts{};
    for (checks, results, 0..) |check, result, i| {
        if (result.repeat_runs != repeat_runs) return error.InvalidArgument;
        if (i != 0) try out.appendSlice(allocator, ",\n");
        const source_path = generatedSourcePathFor(check);
        const check_command = metalCheckCommandFor(check);
        const lowering = loweringForCheck(check);
        const plan_id = try quant_kernel_compiler.planIdName(allocator, lowering.plan_id);
        defer allocator.free(plan_id);
        const route_checked_json = try std.fmt.allocPrint(
            allocator,
            "\"standalone_generated_checked\":true,\"decode_runtime_route_checked\":{s},\"generated_route_checked\":{s},\"provider_route_checked\":{s}",
            .{ jsonBool(result.generated_route_checked), jsonBool(result.generated_route_checked), jsonBool(result.provider_route_checked) },
        );
        defer allocator.free(route_checked_json);
        if (result.generated_route_checked) runtime_route_checked_count += 1;
        if (result.provider_route_checked) provider_route_checked_count += 1;
        const provider_route_needed = quant_kernel_compiler.metalProviderRouteRequiredForKernel(check.kernel_name);
        const route_evidence_passed = result.max_error <= check.tolerance and result.generated_route_checked and (!provider_route_needed or result.provider_route_checked);
        if (route_evidence_passed) candidate_route_ready_count += 1;
        const generated_avg_us = averageUs(result.elapsed_nanos, result.measure_iters);
        const timing_metadata_json = try std.fmt.allocPrint(
            allocator,
            "\"timing_aggregation\":{f},\"generated_timing_route\":{f},\"generated_timing_scope\":{f}",
            .{
                std.json.fmt(timing_aggregation, .{}),
                std.json.fmt(generatedTimingRouteName(result.generated_timing_route), .{}),
                std.json.fmt(generatedTimingScopeName(result.generated_timing_route), .{}),
            },
        );
        defer allocator.free(timing_metadata_json);
        const baseline_supported = handwrittenBaselineSupported(check);
        if ((result.handwritten_elapsed_nanos != null) != baseline_supported) return error.InvalidArgument;
        if (result.handwritten_elapsed_nanos) |handwritten_elapsed_nanos| {
            const handwritten_avg_us = averageUs(handwritten_elapsed_nanos, result.measure_iters);
            const measured_speedup = speedup(handwritten_elapsed_nanos, result.elapsed_nanos);
            const minimum_repeat_speedup = result.minimum_repeat_speedup orelse measured_speedup;
            const benchmark_blocker = quant_kernel_compiler.metalPromotionSpeedupBlocker(measured_speedup, minimum_repeat_speedup);
            const benchmark_passed = std.mem.eql(u8, benchmark_blocker, quant_kernel_compiler.metal_blocker_none);
            benchmark_speedups.add(check.name, measured_speedup, benchmark_passed);
            const selected_for_promotion = (promotion_ready_kernel != null and std.mem.eql(u8, promotion_ready_kernel.?, check.kernel_name)) or
                (production_regression_check and productionMetalRuntimeCheck(check));
            const selected_for_runtime_route = if (runtime_route_all)
                runtimeRouteAllSupported(check)
            else if (production_regression_check)
                productionMetalRuntimeCheck(check)
            else
                runtime_route_kernel != null and std.mem.eql(u8, runtime_route_kernel.?, check.kernel_name);
            const generated_production_route = lowering.production_route == .generated_production and
                std.mem.eql(u8, lowering.production_kernel_id, check.kernel_name);
            const generated_production_route_ready = generated_production_route and selected_for_runtime_route and benchmark_passed and route_evidence_passed;
            if (benchmark_passed) candidate_benchmark_ready_count += 1;
            const promotion_ready = selected_for_promotion and benchmark_passed and route_evidence_passed;
            if (selected_for_promotion) {
                if (promotion_worst_repeat_speedup == null or minimum_repeat_speedup < promotion_worst_repeat_speedup.?) {
                    promotion_worst_repeat_speedup = minimum_repeat_speedup;
                    promotion_worst_repeat_case = check.name;
                }
            }
            const promotion_blocker = if (promotion_ready)
                ""
            else if (generated_production_route_ready)
                ""
            else if (!benchmark_passed)
                benchmark_blocker
            else if ((selected_for_promotion or selected_for_runtime_route) and !result.generated_route_checked)
                quant_kernel_compiler.metal_blocker_missing_generated_route
            else if (selected_for_promotion and provider_route_needed and !result.provider_route_checked)
                quant_kernel_compiler.metal_blocker_missing_provider_route
            else if (selected_for_runtime_route)
                quant_kernel_compiler.metal_blocker_runtime_route_only
            else
                quant_kernel_compiler.metal_blocker_dev_only_candidate;
            if (!blocker_counts.add(promotion_blocker)) return error.InvalidArgument;
            if (selected_for_promotion) {
                promotion_case_count += 1;
                if (promotion_ready) {
                    promotion_ready_count += 1;
                } else if (emit_promotion_diagnostics) {
                    if (repeatGateIndex(result)) |repeat_index| {
                        std.debug.print(
                            "quant-kernel-metal-runtime-check promotion not ready kernel={s} case={s} blocker={s} repeat_gate={d} generated_avg_us={d:.3} handwritten_avg_us={d:.3} repeat_speedup={d:.3} required={d:.3}\n",
                            .{
                                check.kernel_name,
                                check.name,
                                promotion_blocker,
                                repeat_index,
                                averageUs(result.repeat_generated_ns[repeat_index], result.measure_iters),
                                averageUs(result.repeat_handwritten_ns[repeat_index], result.measure_iters),
                                result.repeat_speedups[repeat_index],
                                quant_kernel_compiler.metal_promotion_min_speedup,
                            },
                        );
                    } else {
                        std.debug.print(
                            "quant-kernel-metal-runtime-check promotion not ready kernel={s} case={s} blocker={s} minimum_repeat_speedup={d:.3} required={d:.3}\n",
                            .{
                                check.kernel_name,
                                check.name,
                                promotion_blocker,
                                minimum_repeat_speedup,
                                quant_kernel_compiler.metal_promotion_min_speedup,
                            },
                        );
                    }
                }
            }
            try appendJsonFmt(allocator, &out,
                \\{{"name":{f},"backend":"metal","plan_id":{f},"production_route":{f},"candidate_route":{f},"production_kernel_id":{f},"candidate_kernel_id":{f},"route_fallback_reason":{f},"kernel_id":{f},"generated_source_path":{f},"generated_source_fingerprint":{d},"metal_check_command":{f},"format":{f},"row_bucket":{f},"epilogue":{f},"rows":{d},"in_dim":{d},"out_dim":{d},"threads_per_threadgroup":{d},"cols_per_threadgroup":{d},"correctness_passed":true,{s},"max_abs_error":{d:.7},"tolerance_abs":{d:.7},"warmup_iters":{d},"measure_iters":{d},"repeat_runs":{d},{s},"generated_ns":{d},"generated_avg_us":{d:.3},"handwritten_baseline_supported":true,"handwritten_baseline":{f},"handwritten_ns":{d},"handwritten_avg_us":{d:.3},"measured_speedup":{d:.6}
            , .{
                std.json.fmt(check.name, .{}),
                std.json.fmt(plan_id, .{}),
                std.json.fmt(quant_kernel_compiler.loweringRouteName(lowering.production_route), .{}),
                std.json.fmt(quant_kernel_compiler.loweringRouteName(lowering.candidate_route), .{}),
                std.json.fmt(lowering.production_kernel_id, .{}),
                std.json.fmt(lowering.kernel_id, .{}),
                std.json.fmt(quant_kernel_compiler.fallbackReasonName(lowering.fallback_reason), .{}),
                std.json.fmt(check.kernel_name, .{}),
                std.json.fmt(source_path, .{}),
                std.hash.Wyhash.hash(0, check.source),
                std.json.fmt(check_command, .{}),
                std.json.fmt(@tagName(check.format), .{}),
                std.json.fmt(rowBucketName(check.rows), .{}),
                std.json.fmt(@tagName(check.epilogue), .{}),
                check.rows,
                check.in_dim,
                check.out_dim,
                check.threads_per_threadgroup,
                check.cols_per_threadgroup,
                route_checked_json,
                result.max_error,
                check.tolerance,
                check.warmup_iters,
                result.measure_iters,
                result.repeat_runs,
                timing_metadata_json,
                result.elapsed_nanos,
                generated_avg_us,
                std.json.fmt(handwrittenBaselineName(check), .{}),
                handwritten_elapsed_nanos,
                handwritten_avg_us,
                measured_speedup,
            });
            try appendRepeatTimingFields(allocator, &out, result, true);
            try appendJsonFmt(allocator, &out,
                \\,"minimum_repeat_speedup":{d:.6},"benchmark_passed":{s},"promotion_ready":{s},"promotion_blocker":{f}}}
            , .{
                minimum_repeat_speedup,
                jsonBool(benchmark_passed),
                jsonBool(promotion_ready),
                std.json.fmt(promotion_blocker, .{}),
            });
        } else {
            const fallback_reason = handwrittenBaselineFallbackReason(check);
            if ((promotion_ready_kernel != null and std.mem.eql(u8, promotion_ready_kernel.?, check.kernel_name)) or
                (production_regression_check and productionMetalRuntimeCheck(check)))
            {
                promotion_case_count += 1;
                if (emit_promotion_diagnostics) std.debug.print(
                    "quant-kernel-metal-runtime-check promotion not ready kernel={s} case={s} blocker=unsupported_handwritten_baseline fallback_reason={s}\n",
                    .{ check.kernel_name, check.name, fallback_reason },
                );
            }
            if (!blocker_counts.add(quant_kernel_compiler.metal_blocker_unsupported_handwritten)) return error.InvalidArgument;
            try appendJsonFmt(allocator, &out,
                \\{{"name":{f},"backend":"metal","plan_id":{f},"production_route":{f},"candidate_route":{f},"production_kernel_id":{f},"candidate_kernel_id":{f},"route_fallback_reason":{f},"kernel_id":{f},"generated_source_path":{f},"generated_source_fingerprint":{d},"metal_check_command":{f},"format":{f},"row_bucket":{f},"epilogue":{f},"rows":{d},"in_dim":{d},"out_dim":{d},"threads_per_threadgroup":{d},"cols_per_threadgroup":{d},"correctness_passed":true,{s},"max_abs_error":{d:.7},"tolerance_abs":{d:.7},"warmup_iters":{d},"measure_iters":{d},"repeat_runs":{d},{s},"generated_ns":{d},"generated_avg_us":{d:.3},"handwritten_baseline_supported":false,"handwritten_baseline":"none","fallback_reason":{f},"benchmark_passed":false,"promotion_ready":false,"promotion_blocker":"unsupported_handwritten_baseline"}}
            , .{
                std.json.fmt(check.name, .{}),
                std.json.fmt(plan_id, .{}),
                std.json.fmt(quant_kernel_compiler.loweringRouteName(lowering.production_route), .{}),
                std.json.fmt(quant_kernel_compiler.loweringRouteName(lowering.candidate_route), .{}),
                std.json.fmt(lowering.production_kernel_id, .{}),
                std.json.fmt(lowering.kernel_id, .{}),
                std.json.fmt(quant_kernel_compiler.fallbackReasonName(lowering.fallback_reason), .{}),
                std.json.fmt(check.kernel_name, .{}),
                std.json.fmt(source_path, .{}),
                std.hash.Wyhash.hash(0, check.source),
                std.json.fmt(check_command, .{}),
                std.json.fmt(@tagName(check.format), .{}),
                std.json.fmt(rowBucketName(check.rows), .{}),
                std.json.fmt(@tagName(check.epilogue), .{}),
                check.rows,
                check.in_dim,
                check.out_dim,
                check.threads_per_threadgroup,
                check.cols_per_threadgroup,
                route_checked_json,
                result.max_error,
                check.tolerance,
                check.warmup_iters,
                result.measure_iters,
                result.repeat_runs,
                timing_metadata_json,
                result.elapsed_nanos,
                generated_avg_us,
                std.json.fmt(fallback_reason, .{}),
            });
        }
    }

    try out.appendSlice(allocator,
        \\],
        \\
    );
    if (runtime_route_all) {
        try appendJsonFmt(allocator, &out,
            \\"runtime_route_all_status":{f},
            \\
        , .{std.json.fmt(runtimeRouteAllEvidenceStatus(.{
            .case_count = checks.len,
            .promotion_case_count = promotion_case_count,
            .promotion_ready_count = promotion_ready_count,
            .runtime_route_checked_count = runtime_route_checked_count,
            .provider_route_checked_count = provider_route_checked_count,
            .candidate_route_ready_count = candidate_route_ready_count,
            .candidate_benchmark_ready_count = candidate_benchmark_ready_count,
            .blocker_counts = blocker_counts,
        }), .{})});
    }
    if (production_regression_check) {
        try appendJsonFmt(allocator, &out,
            \\"production_regression_status":{f},
            \\
        , .{std.json.fmt(productionRegressionEvidenceStatus(.{
            .case_count = checks.len,
            .promotion_case_count = promotion_case_count,
            .promotion_ready_count = promotion_ready_count,
            .runtime_route_checked_count = runtime_route_checked_count,
            .provider_route_checked_count = provider_route_checked_count,
            .candidate_route_ready_count = candidate_route_ready_count,
            .candidate_benchmark_ready_count = candidate_benchmark_ready_count,
            .blocker_counts = blocker_counts,
        }), .{})});
    }
    const worst_repeat_speedup_json = if (promotion_worst_repeat_speedup) |value|
        try std.fmt.allocPrint(allocator, "{d:.6}", .{value})
    else
        "null";
    defer if (promotion_worst_repeat_speedup != null) allocator.free(worst_repeat_speedup_json);
    const worst_repeat_case_json = if (promotion_worst_repeat_case) |name|
        try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(name, .{})})
    else
        "null";
    defer if (promotion_worst_repeat_case != null) allocator.free(worst_repeat_case_json);
    const benchmark_min_speedup_json = try nullableF64Json(allocator, benchmark_speedups.min_speedup);
    defer allocator.free(benchmark_min_speedup_json);
    const benchmark_min_case_json = try nullableStringJson(allocator, benchmark_speedups.min_case);
    defer allocator.free(benchmark_min_case_json);
    const benchmark_max_speedup_json = try nullableF64Json(allocator, benchmark_speedups.max_speedup);
    defer allocator.free(benchmark_max_speedup_json);
    const benchmark_max_case_json = try nullableStringJson(allocator, benchmark_speedups.max_case);
    defer allocator.free(benchmark_max_case_json);
    const benchmark_avg_speedup_json = try nullableF64Json(allocator, benchmark_speedups.average());
    defer allocator.free(benchmark_avg_speedup_json);
    try appendJsonFmt(allocator, &out,
        \\"promotion_worst_repeat_speedup":{s},
        \\"promotion_worst_repeat_case":{s},
        \\"promotion_case_count":{d},
        \\"promotion_ready_count":{d},
        \\"runtime_route_checked_count":{d},
        \\"provider_route_checked_count":{d},
        \\"candidate_route_ready_count":{d},
        \\"candidate_benchmark_ready_count":{d},
        \\"benchmark_supported_count":{d},
        \\"benchmark_speedup_pass_count":{d},
        \\"benchmark_speedup_min":{s},
        \\"benchmark_speedup_min_case":{s},
        \\"benchmark_speedup_max":{s},
        \\"benchmark_speedup_max_case":{s},
        \\"benchmark_speedup_avg":{s},
        \\"promotion_blocker_speedup_gate_missing_count":{d},
        \\"promotion_blocker_unstable_benchmark_timing_count":{d},
        \\"promotion_blocker_runtime_route_only_count":{d},
        \\"promotion_blocker_missing_generated_route_count":{d},
        \\"promotion_blocker_missing_provider_route_count":{d},
        \\"promotion_blocker_unsupported_handwritten_count":{d},
        \\"promotion_blocker_dev_only_candidate_count":{d},
        \\"slow_fallback_count":{d},
        \\"top_slow_fallback_reason":{f},
        \\"top_slow_fallback_count":{d}
        \\
    , .{
        worst_repeat_speedup_json,
        worst_repeat_case_json,
        promotion_case_count,
        promotion_ready_count,
        runtime_route_checked_count,
        provider_route_checked_count,
        candidate_route_ready_count,
        candidate_benchmark_ready_count,
        benchmark_speedups.supported_count,
        benchmark_speedups.pass_count,
        benchmark_min_speedup_json,
        benchmark_min_case_json,
        benchmark_max_speedup_json,
        benchmark_max_case_json,
        benchmark_avg_speedup_json,
        blocker_counts.speedup_gate_missing,
        blocker_counts.unstable_benchmark_timing,
        blocker_counts.runtime_route_only,
        blocker_counts.missing_generated_route,
        blocker_counts.missing_provider_route,
        blocker_counts.unsupported_handwritten_baseline,
        blocker_counts.dev_only_candidate,
        blocker_counts.slowFallbackCount(),
        std.json.fmt(blocker_counts.topSlowFallbackReason(), .{}),
        blocker_counts.topSlowFallbackCount(),
    });
    try out.appendSlice(allocator,
        \\}
        \\
    );
    try writeFileCreatingParent(compat.io(), path, out.items);
    if (promotion_ready_kernel) |kernel| {
        if (emit_promotion_diagnostics and promotion_ready_count != promotion_case_count) {
            std.debug.print(
                "quant-kernel-metal-runtime-check promotion summary kernel={s} ready={d}/{d} evidence={s}\n",
                .{ kernel, promotion_ready_count, promotion_case_count, path },
            );
        }
        try checkEvidenceFile(allocator, path, false, false, kernel);
    } else if (production_regression_check) {
        if (emit_promotion_diagnostics and promotion_ready_count != promotion_case_count) {
            std.debug.print(
                "quant-kernel-metal-runtime-check production regression summary ready={d}/{d} evidence={s}\n",
                .{ promotion_ready_count, promotion_case_count, path },
            );
        }
        try checkEvidenceFile(allocator, path, false, false, null);
    } else {
        try checkEvidenceFile(allocator, path, false, false, null);
    }
}

fn timingAggregationName(repeat_runs: u32) []const u8 {
    return if (repeat_runs == 1) "single" else "median";
}

const PromotionBlockerCounts = struct {
    speedup_gate_missing: usize = 0,
    unstable_benchmark_timing: usize = 0,
    runtime_route_only: usize = 0,
    missing_generated_route: usize = 0,
    missing_provider_route: usize = 0,
    unsupported_handwritten_baseline: usize = 0,
    dev_only_candidate: usize = 0,

    fn add(self: *@This(), blocker: []const u8) bool {
        if (blocker.len == 0) return true;
        if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_speedup_gate_missing)) {
            self.speedup_gate_missing += 1;
        } else if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_unstable_benchmark_timing)) {
            self.unstable_benchmark_timing += 1;
        } else if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_runtime_route_only)) {
            self.runtime_route_only += 1;
        } else if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_missing_generated_route)) {
            self.missing_generated_route += 1;
        } else if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_missing_provider_route)) {
            self.missing_provider_route += 1;
        } else if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_unsupported_handwritten)) {
            self.unsupported_handwritten_baseline += 1;
        } else if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_dev_only_candidate)) {
            self.dev_only_candidate += 1;
        } else {
            return false;
        }
        return true;
    }

    fn eql(self: @This(), other: @This()) bool {
        return self.speedup_gate_missing == other.speedup_gate_missing and
            self.unstable_benchmark_timing == other.unstable_benchmark_timing and
            self.runtime_route_only == other.runtime_route_only and
            self.missing_generated_route == other.missing_generated_route and
            self.missing_provider_route == other.missing_provider_route and
            self.unsupported_handwritten_baseline == other.unsupported_handwritten_baseline and
            self.dev_only_candidate == other.dev_only_candidate;
    }

    fn slowFallbackCount(self: @This()) usize {
        return self.speedup_gate_missing + self.unstable_benchmark_timing;
    }

    fn topSlowFallbackReason(self: @This()) []const u8 {
        if (self.slowFallbackCount() == 0) return quant_kernel_compiler.metal_blocker_none;
        if (self.speedup_gate_missing >= self.unstable_benchmark_timing) return quant_kernel_compiler.metal_blocker_speedup_gate_missing;
        return quant_kernel_compiler.metal_blocker_unstable_benchmark_timing;
    }

    fn topSlowFallbackCount(self: @This()) usize {
        if (self.slowFallbackCount() == 0) return 0;
        return @max(self.speedup_gate_missing, self.unstable_benchmark_timing);
    }
};

fn addPromotionBlockerCounts(total: *PromotionBlockerCounts, addend: PromotionBlockerCounts) void {
    total.speedup_gate_missing += addend.speedup_gate_missing;
    total.unstable_benchmark_timing += addend.unstable_benchmark_timing;
    total.runtime_route_only += addend.runtime_route_only;
    total.missing_generated_route += addend.missing_generated_route;
    total.missing_provider_route += addend.missing_provider_route;
    total.unsupported_handwritten_baseline += addend.unsupported_handwritten_baseline;
    total.dev_only_candidate += addend.dev_only_candidate;
}

fn promotionBlockerCount(counts: PromotionBlockerCounts, blocker: []const u8) ?usize {
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_speedup_gate_missing)) return counts.speedup_gate_missing;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_unstable_benchmark_timing)) return counts.unstable_benchmark_timing;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_runtime_route_only)) return counts.runtime_route_only;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_missing_generated_route)) return counts.missing_generated_route;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_missing_provider_route)) return counts.missing_provider_route;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_unsupported_handwritten)) return counts.unsupported_handwritten_baseline;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_dev_only_candidate)) return counts.dev_only_candidate;
    return null;
}

fn promotionBlockerEvidenceMatches(counts: PromotionBlockerCounts, expected_blocker: []const u8) bool {
    if (promotionBlockerEvidenceMatchesExactly(counts, expected_blocker)) return true;
    if (std.mem.eql(u8, expected_blocker, quant_kernel_compiler.metal_blocker_unstable_benchmark_timing)) {
        return counts.speedup_gate_missing != 0;
    }
    return promotionBlockerEvidenceTimingDrifted(counts, expected_blocker);
}

fn promotionBlockerEvidenceMatchesExactly(counts: PromotionBlockerCounts, expected_blocker: []const u8) bool {
    const exact_count = promotionBlockerCount(counts, expected_blocker) orelse return false;
    return exact_count != 0;
}

fn promotionBlockerEvidenceTimingDrifted(counts: PromotionBlockerCounts, expected_blocker: []const u8) bool {
    if (std.mem.eql(u8, expected_blocker, quant_kernel_compiler.metal_blocker_speedup_gate_missing)) {
        return counts.unstable_benchmark_timing != 0;
    }
    return false;
}

fn promotionBlockerEvidenceCleared(summary: EvidenceSummary, expected_blocker: []const u8) bool {
    return (std.mem.eql(u8, expected_blocker, quant_kernel_compiler.metal_blocker_speedup_gate_missing) or
        std.mem.eql(u8, expected_blocker, quant_kernel_compiler.metal_blocker_unstable_benchmark_timing)) and
        summary.promotion_case_count != 0 and
        summary.promotion_ready_count == summary.promotion_case_count;
}

fn hasPromotionRepeatRuns(repeat_runs: usize) bool {
    return repeat_runs >= promotion_min_repeat_runs;
}

fn checksContainKernel(checks: []const CheckCase, kernel: []const u8) bool {
    for (checks) |check| {
        if (std.mem.eql(u8, check.kernel_name, kernel)) return true;
    }
    return false;
}

fn appendJsonFmt(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const chunk = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

fn nullableF64Json(allocator: std.mem.Allocator, value: ?f64) ![]const u8 {
    if (value) |actual| return try std.fmt.allocPrint(allocator, "{d:.6}", .{actual});
    return try allocator.dupe(u8, "null");
}

fn nullableStringJson(allocator: std.mem.Allocator, value: ?[]const u8) ![]const u8 {
    if (value) |actual| return try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(actual, .{})});
    return try allocator.dupe(u8, "null");
}

fn appendRepeatTimingFields(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    result: CheckResult,
    include_handwritten: bool,
) !void {
    const count: usize = @intCast(result.repeat_timing_count);
    if (count == 0) return;
    try appendU64ArrayField(allocator, out, "repeat_generated_ns", result.repeat_generated_ns[0..count]);
    if (include_handwritten and result.repeat_handwritten_count == result.repeat_timing_count) {
        try appendU64ArrayField(allocator, out, "repeat_handwritten_ns", result.repeat_handwritten_ns[0..count]);
        try appendF64ArrayField(allocator, out, "repeat_speedups", result.repeat_speedups[0..count]);
        if (minimumRepeatIndex(result)) |index| {
            try appendJsonFmt(allocator, out, ",\"minimum_repeat_index\":{d}", .{index});
        }
        if (repeatGateIndex(result)) |index| {
            try appendJsonFmt(allocator, out, ",\"repeat_gate_index\":{d}", .{index});
        }
    }
}

fn minimumRepeatIndex(result: CheckResult) ?usize {
    if (result.repeat_timing_count == 0 or result.repeat_timing_count != result.repeat_handwritten_count) return null;
    const count: usize = @intCast(result.repeat_timing_count);
    var index: usize = 0;
    var min_speedup = result.repeat_speedups[0];
    for (result.repeat_speedups[1..count], 1..) |speedup_value, i| {
        if (speedup_value < min_speedup) {
            min_speedup = speedup_value;
            index = i;
        }
    }
    return index;
}

fn repeatGateIndex(result: CheckResult) ?usize {
    if (result.repeat_timing_count == 0 or result.repeat_timing_count != result.repeat_handwritten_count) return null;
    const count: usize = @intCast(result.repeat_timing_count);
    const gate_speedup = repeatGateSpeedup(result.repeat_speedups[0..count]);
    for (result.repeat_speedups[0..count], 0..) |speedup_value, i| {
        if (approximately(speedup_value, gate_speedup, 0.000001)) return i;
    }
    return null;
}

fn appendU64ArrayField(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime name: []const u8,
    values: []const u64,
) !void {
    try appendJsonFmt(allocator, out, ",\"{s}\":[", .{name});
    for (values, 0..) |value, i| {
        if (i != 0) try out.append(allocator, ',');
        try appendJsonFmt(allocator, out, "{d}", .{value});
    }
    try out.append(allocator, ']');
}

fn appendF64ArrayField(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime name: []const u8,
    values: []const f64,
) !void {
    try appendJsonFmt(allocator, out, ",\"{s}\":[", .{name});
    for (values, 0..) |value, i| {
        if (i != 0) try out.append(allocator, ',');
        try appendJsonFmt(allocator, out, "{d:.6}", .{value});
    }
    try out.append(allocator, ']');
}

fn writeFileCreatingParent(io: std.Io, path: []const u8, data: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        if (parent.len > 0) try compat.cwd().createDirPath(io, parent);
    }
    if (std.fs.path.isAbsolute(path)) {
        var file = try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
        defer file.close(io);
        try file.writeStreamingAll(io, data);
    } else {
        try compat.cwd().writeFile(io, .{ .sub_path = path, .data = data });
    }
}

const EvidenceSummary = struct {
    case_count: usize,
    promotion_case_count: usize,
    promotion_ready_count: usize,
    runtime_route_checked_count: usize,
    provider_route_checked_count: usize,
    candidate_route_ready_count: usize = 0,
    candidate_benchmark_ready_count: usize = 0,
    blocker_counts: PromotionBlockerCounts,
    promotion_worst_repeat_speedup: ?f64 = null,
    production_regression_check: bool = false,
    compiler_benchmark_manifest_case_count: ?usize = null,
    compiler_benchmark_manifest_case_fingerprint: ?u64 = null,
};

const BenchmarkSpeedupSummary = struct {
    supported_count: usize = 0,
    pass_count: usize = 0,
    min_speedup: ?f64 = null,
    min_case: ?[]const u8 = null,
    max_speedup: ?f64 = null,
    max_case: ?[]const u8 = null,
    sum_speedup: f64 = 0.0,

    fn add(self: *@This(), case_name: []const u8, measured_speedup: f64, benchmark_passed: bool) void {
        self.supported_count += 1;
        if (benchmark_passed) self.pass_count += 1;
        self.sum_speedup += measured_speedup;
        if (self.min_speedup == null or measured_speedup < self.min_speedup.?) {
            self.min_speedup = measured_speedup;
            self.min_case = case_name;
        }
        if (self.max_speedup == null or measured_speedup > self.max_speedup.?) {
            self.max_speedup = measured_speedup;
            self.max_case = case_name;
        }
    }

    fn average(self: @This()) ?f64 {
        if (self.supported_count == 0) return null;
        return self.sum_speedup / @as(f64, @floatFromInt(self.supported_count));
    }

    fn eql(self: @This(), other: @This()) bool {
        return self.supported_count == other.supported_count and
            self.pass_count == other.pass_count and
            nullableF64Approximately(self.min_speedup, other.min_speedup, 0.000001) and
            nullableF64Approximately(self.max_speedup, other.max_speedup, 0.000001) and
            nullableF64Approximately(self.average(), other.average(), 0.000001) and
            nullableStringEql(self.min_case, other.min_case) and
            nullableStringEql(self.max_case, other.max_case);
    }
};

fn evidenceSummaryHasRouteReadyCoverage(summary: EvidenceSummary) bool {
    return summary.case_count != 0 and summary.candidate_route_ready_count == summary.case_count;
}

fn productionRegressionEvidenceHasHardBlocker(summary: EvidenceSummary) bool {
    if (summary.promotion_case_count == 0) return false;
    return !evidenceSummaryHasRouteReadyCoverage(summary) or
        (quant_kernel_compiler.first_metal_production_regression_unstable_benchmark_timing_is_hard_gate and summary.blocker_counts.unstable_benchmark_timing != 0) or
        summary.blocker_counts.speedup_gate_missing != 0 or
        summary.blocker_counts.runtime_route_only != 0 or
        summary.blocker_counts.missing_generated_route != 0 or
        summary.blocker_counts.missing_provider_route != 0 or
        summary.blocker_counts.unsupported_handwritten_baseline != 0 or
        summary.blocker_counts.dev_only_candidate != 0;
}

fn productionRegressionEvidenceStatus(summary: EvidenceSummary) []const u8 {
    if (productionRegressionEvidenceHasHardBlocker(summary)) return "production_regression_blocked";
    if (summary.promotion_case_count == 0) return "production_regression_skipped";
    if (summary.promotion_ready_count == summary.promotion_case_count) return "production_regression_ok";
    return "production_regression_timing_drift";
}

fn runtimeRouteAllEvidenceStatus(summary: EvidenceSummary) []const u8 {
    if (summary.case_count == quant_kernel_compiler.first_metal_runtime_route_all_expected_case_count and
        summary.runtime_route_checked_count == summary.case_count and
        evidenceSummaryHasRouteReadyCoverage(summary) and
        summary.provider_route_checked_count == quant_kernel_compiler.first_metal_runtime_route_all_expected_provider_route_count)
    {
        return "runtime_route_all_ok";
    }
    return "runtime_route_all_blocked";
}

fn printEvidenceSummary(path: []const u8, status: []const u8, summary: EvidenceSummary) void {
    std.debug.print(
        "quant-kernel-metal-runtime-check evidence_check={s} {s} cases={d} route_checked={d} provider_checked={d} route_ready={d}/{d} benchmark_ready={d}/{d} promotion_ready={d}/{d} blockers={{speedup_gate:{d},unstable_benchmark:{d},runtime_route_only:{d},missing_generated:{d},missing_provider:{d},unsupported_handwritten:{d},dev_only:{d}}} slow_fallbacks={d} top_slow={s}:{d}",
        .{
            path,
            status,
            summary.case_count,
            summary.runtime_route_checked_count,
            summary.provider_route_checked_count,
            summary.candidate_route_ready_count,
            summary.case_count,
            summary.candidate_benchmark_ready_count,
            summary.case_count,
            summary.promotion_ready_count,
            summary.promotion_case_count,
            summary.blocker_counts.speedup_gate_missing,
            summary.blocker_counts.unstable_benchmark_timing,
            summary.blocker_counts.runtime_route_only,
            summary.blocker_counts.missing_generated_route,
            summary.blocker_counts.missing_provider_route,
            summary.blocker_counts.unsupported_handwritten_baseline,
            summary.blocker_counts.dev_only_candidate,
            summary.blocker_counts.slowFallbackCount(),
            summary.blocker_counts.topSlowFallbackReason(),
            summary.blocker_counts.topSlowFallbackCount(),
        },
    );
    if (summary.promotion_worst_repeat_speedup) |worst| {
        std.debug.print(" worst_repeat_speedup={d:.3}", .{worst});
    }
    if (summary.production_regression_check) {
        std.debug.print(
            " benchmark_manifest={s}:{d}:{d}",
            .{
                quant_kernel_compiler.first_benchmark_manifest_schema,
                summary.compiler_benchmark_manifest_case_count orelse 0,
                summary.compiler_benchmark_manifest_case_fingerprint orelse 0,
            },
        );
    }
    std.debug.print("\n", .{});
}

const BlockerEvidenceAuditSummary = struct {
    table_entry_count: usize = 0,
    checked_path_count: usize = 0,
    skipped_no_path_count: usize = 0,
    cleared_blocker_count: usize = 0,
    confirmed_cleared_blocker_count: usize = 0,
    unconfirmed_cleared_blocker_count: usize = 0,
    production_regression_guarded_count: usize = 0,
    timing_blocker_drift_count: usize = 0,
    evidence_case_count: usize = 0,
    candidate_route_ready_count: usize = 0,
    candidate_benchmark_ready_count: usize = 0,
    promotion_case_count: usize = 0,
    promotion_ready_count: usize = 0,
    table_blocker_counts: PromotionBlockerCounts = .{},
    evidence_blocker_counts: PromotionBlockerCounts = .{},
};

fn blockerEvidenceAuditStatus(summary: BlockerEvidenceAuditSummary) []const u8 {
    if (summary.confirmed_cleared_blocker_count != 0) return "blocker_evidence_confirmed_cleared";
    if (summary.unconfirmed_cleared_blocker_count != 0) return "blocker_evidence_unconfirmed_cleared";
    if (summary.cleared_blocker_count != 0) return "blocker_evidence_cleared";
    if (summary.production_regression_guarded_count != 0) return "blocker_evidence_production_regression_guarded";
    if (summary.timing_blocker_drift_count != 0) return "blocker_evidence_timing_drift";
    return "blocker_evidence_ok";
}

fn enforceClearedBlockerPolicy(summary: BlockerEvidenceAuditSummary, fail_on_cleared_blocker: bool) !void {
    if (!fail_on_cleared_blocker) return;
    if (summary.confirmed_cleared_blocker_count != 0) return error.MetalBlockerEvidenceCleared;
    if (summary.unconfirmed_cleared_blocker_count == 0 and summary.cleared_blocker_count != 0) return error.MetalBlockerEvidenceCleared;
}

fn printBlockerEvidenceAuditSummary(summary: BlockerEvidenceAuditSummary) void {
    std.debug.print(
        "quant-kernel-metal-runtime-check blocker_evidence status={s} entries={d} checked={d} skipped_no_path={d} cleared={d} confirmed_cleared={d} unconfirmed_cleared={d} production_regression_guarded={d} timing_drift={d} readiness={{route:{d}/{d},benchmark:{d}/{d},promotion:{d}/{d}}} table_blockers={{speedup_gate:{d},unstable_benchmark:{d},runtime_route_only:{d},missing_generated:{d},missing_provider:{d},unsupported_handwritten:{d},dev_only:{d}}} evidence_blockers={{speedup_gate:{d},unstable_benchmark:{d},runtime_route_only:{d},missing_generated:{d},missing_provider:{d},unsupported_handwritten:{d},dev_only:{d}}}\n",
        .{
            blockerEvidenceAuditStatus(summary),
            summary.table_entry_count,
            summary.checked_path_count,
            summary.skipped_no_path_count,
            summary.cleared_blocker_count,
            summary.confirmed_cleared_blocker_count,
            summary.unconfirmed_cleared_blocker_count,
            summary.production_regression_guarded_count,
            summary.timing_blocker_drift_count,
            summary.candidate_route_ready_count,
            summary.evidence_case_count,
            summary.candidate_benchmark_ready_count,
            summary.evidence_case_count,
            summary.promotion_ready_count,
            summary.promotion_case_count,
            summary.table_blocker_counts.speedup_gate_missing,
            summary.table_blocker_counts.unstable_benchmark_timing,
            summary.table_blocker_counts.runtime_route_only,
            summary.table_blocker_counts.missing_generated_route,
            summary.table_blocker_counts.missing_provider_route,
            summary.table_blocker_counts.unsupported_handwritten_baseline,
            summary.table_blocker_counts.dev_only_candidate,
            summary.evidence_blocker_counts.speedup_gate_missing,
            summary.evidence_blocker_counts.unstable_benchmark_timing,
            summary.evidence_blocker_counts.runtime_route_only,
            summary.evidence_blocker_counts.missing_generated_route,
            summary.evidence_blocker_counts.missing_provider_route,
            summary.evidence_blocker_counts.unsupported_handwritten_baseline,
            summary.evidence_blocker_counts.dev_only_candidate,
        },
    );
}

fn refreshBlockerEvidence(allocator: std.mem.Allocator, evidence_dir: ?[]const u8, confirm_cleared_blockers: bool) !BlockerEvidenceAuditSummary {
    var checks_storage = metal_runtime_checks;
    for (&checks_storage) |*check| {
        check.measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
    }
    const repeat_runs: u32 = @intCast(quant_kernel_compiler.metal_promotion_repeat_runs);

    for (quant_kernel_compiler.first_metal_promotion_blocker_evidence) |evidence| {
        if (evidence.evidence_path.len == 0) continue;
        const evidence_path = try blockerEvidencePath(allocator, evidence.evidence_path, evidence_dir);
        defer if (evidence_dir != null) allocator.free(evidence_path);

        var selected_checks: [metal_runtime_checks.len]CheckCase = undefined;
        var selected_results: [metal_runtime_checks.len]CheckResult = undefined;
        var selected_count: usize = 0;
        for (checks_storage) |check| {
            if (!std.mem.eql(u8, check.kernel_name, evidence.kernel_id)) continue;
            selected_checks[selected_count] = check;
            selected_results[selected_count] = try runRepeatedCheck(allocator, check, repeat_runs, evidence.kernel_id, evidence.kernel_id);
            selected_count += 1;
        }
        if (selected_count == 0) return error.InvalidArgument;
        try writeEvidence(
            allocator,
            evidence_path,
            selected_checks[0..selected_count],
            selected_results[0..selected_count],
            repeat_runs,
            evidence.kernel_id,
            null,
            false,
            false,
            true,
            null,
        );
    }
    return checkBlockerEvidenceWithDetails(allocator, evidence_dir, false, confirm_cleared_blockers);
}

fn checkBlockerEvidence(allocator: std.mem.Allocator, evidence_dir: ?[]const u8, confirm_cleared_blockers: bool) !BlockerEvidenceAuditSummary {
    return checkBlockerEvidenceWithDetails(allocator, evidence_dir, true, confirm_cleared_blockers);
}

fn blockerEvidencePath(allocator: std.mem.Allocator, path: []const u8, evidence_dir: ?[]const u8) ![]const u8 {
    const dir = evidence_dir orelse return path;
    return std.fs.path.join(allocator, &.{ dir, std.fs.path.basename(path) });
}

fn confirmClearedBlocker(allocator: std.mem.Allocator, kernel_id: []const u8) !EvidenceSummary {
    const repeat_runs: u32 = @intCast(quant_kernel_compiler.metal_promotion_repeat_runs);
    var summary = EvidenceSummary{
        .case_count = 0,
        .promotion_case_count = 0,
        .promotion_ready_count = 0,
        .runtime_route_checked_count = 0,
        .provider_route_checked_count = 0,
        .blocker_counts = .{},
    };

    for (metal_runtime_checks) |base_check| {
        if (!std.mem.eql(u8, base_check.kernel_name, kernel_id)) continue;
        var check = base_check;
        check.measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
        const result = try runRepeatedCheck(allocator, check, repeat_runs, kernel_id, kernel_id);
        summary.case_count += 1;
        summary.promotion_case_count += 1;
        if (result.generated_route_checked) summary.runtime_route_checked_count += 1;
        if (result.provider_route_checked) summary.provider_route_checked_count += 1;

        const promotion_blocker = if (result.handwritten_elapsed_nanos) |handwritten_elapsed_nanos| blocker: {
            const measured_speedup = speedup(handwritten_elapsed_nanos, result.elapsed_nanos);
            const minimum_repeat_speedup = result.minimum_repeat_speedup orelse measured_speedup;
            const benchmark_blocker = quant_kernel_compiler.metalPromotionSpeedupBlocker(measured_speedup, minimum_repeat_speedup);
            const benchmark_passed = std.mem.eql(u8, benchmark_blocker, quant_kernel_compiler.metal_blocker_none);
            const provider_route_needed = quant_kernel_compiler.metalProviderRouteRequiredForKernel(check.kernel_name);
            const route_evidence_passed = result.generated_route_checked and (!provider_route_needed or result.provider_route_checked);
            if (route_evidence_passed) summary.candidate_route_ready_count += 1;
            if (benchmark_passed) summary.candidate_benchmark_ready_count += 1;
            if (benchmark_passed and route_evidence_passed) {
                summary.promotion_ready_count += 1;
                break :blocker "";
            }
            if (!benchmark_passed) break :blocker benchmark_blocker;
            if (!result.generated_route_checked) break :blocker quant_kernel_compiler.metal_blocker_missing_generated_route;
            if (provider_route_needed and !result.provider_route_checked) break :blocker quant_kernel_compiler.metal_blocker_missing_provider_route;
            break :blocker "";
        } else unsupported: {
            const provider_route_needed = quant_kernel_compiler.metalProviderRouteRequiredForKernel(check.kernel_name);
            if (result.generated_route_checked and (!provider_route_needed or result.provider_route_checked)) {
                summary.candidate_route_ready_count += 1;
            }
            break :unsupported quant_kernel_compiler.metal_blocker_unsupported_handwritten;
        };
        if (!summary.blocker_counts.add(promotion_blocker)) return error.InvalidArgument;
    }

    if (summary.case_count == 0) return error.InvalidArgument;
    return summary;
}

fn checkBlockerEvidenceWithDetails(allocator: std.mem.Allocator, evidence_dir: ?[]const u8, print_details: bool, confirm_cleared_blockers: bool) !BlockerEvidenceAuditSummary {
    var summary = BlockerEvidenceAuditSummary{
        .table_entry_count = quant_kernel_compiler.first_metal_promotion_blocker_evidence_count,
    };
    for (quant_kernel_compiler.first_metal_promotion_blocker_evidence) |evidence| {
        if (!summary.table_blocker_counts.add(evidence.blocker)) return error.InvalidMetalEvidence;
        if (evidence.evidence_path.len == 0) {
            const route_only = std.mem.eql(u8, evidence.blocker, quant_kernel_compiler.metal_blocker_unsupported_handwritten) or
                std.mem.eql(u8, evidence.blocker, quant_kernel_compiler.metal_blocker_runtime_route_only);
            if (!route_only) {
                std.debug.print(
                    "quant-kernel-metal-runtime-check blocker_evidence missing_path kernel={s} blocker={s}\n",
                    .{ evidence.kernel_id, evidence.blocker },
                );
                return error.MetalBlockerEvidenceMissingPath;
            }
            summary.skipped_no_path_count += 1;
            continue;
        }
        const evidence_path = try blockerEvidencePath(allocator, evidence.evidence_path, evidence_dir);
        defer if (evidence_dir != null) allocator.free(evidence_path);

        const evidence_summary = checkEvidenceFileWithSummary(allocator, evidence_path, false, false, evidence.kernel_id) catch |err| {
            std.debug.print(
                "quant-kernel-metal-runtime-check blocker_evidence invalid kernel={s} blocker={s} path={s} error={s}\n",
                .{ evidence.kernel_id, evidence.blocker, evidence_path, @errorName(err) },
            );
            return err;
        };
        addPromotionBlockerCounts(&summary.evidence_blocker_counts, evidence_summary.blocker_counts);
        summary.evidence_case_count += evidence_summary.case_count;
        summary.candidate_route_ready_count += evidence_summary.candidate_route_ready_count;
        summary.candidate_benchmark_ready_count += evidence_summary.candidate_benchmark_ready_count;
        summary.promotion_case_count += evidence_summary.promotion_case_count;
        summary.promotion_ready_count += evidence_summary.promotion_ready_count;
        if (!evidenceSummaryHasRouteReadyCoverage(evidence_summary)) {
            std.debug.print(
                "quant-kernel-metal-runtime-check blocker_evidence missing_route_ready kernel={s} blocker={s} path={s} route_ready={d}/{d}\n",
                .{ evidence.kernel_id, evidence.blocker, evidence_path, evidence_summary.candidate_route_ready_count, evidence_summary.case_count },
            );
            return error.MetalBlockerEvidenceRouteMissing;
        }
        const blocker_cleared = promotionBlockerEvidenceCleared(evidence_summary, evidence.blocker);
        const production_regression_guarded = blocker_cleared and evidence.requires_production_regression_clear;
        if (production_regression_guarded) {
            summary.production_regression_guarded_count += 1;
            if (print_details) {
                std.debug.print(
                    "quant-kernel-metal-runtime-check blocker_evidence production_regression_guarded kernel={s} table_blocker={s} path={s} promotion_ready={d}/{d}\n",
                    .{ evidence.kernel_id, evidence.blocker, evidence_path, evidence_summary.promotion_ready_count, evidence_summary.promotion_case_count },
                );
            }
        } else if (blocker_cleared) {
            summary.cleared_blocker_count += 1;
            if (confirm_cleared_blockers) {
                const required_confirmation_rounds: usize = 3;
                var confirmation_rounds: usize = 1;
                var confirmation_summary = try confirmClearedBlocker(allocator, evidence.kernel_id);
                var confirmed_clear = promotionBlockerEvidenceCleared(confirmation_summary, evidence.blocker);
                while (confirmed_clear and confirmation_rounds < required_confirmation_rounds) : (confirmation_rounds += 1) {
                    confirmation_summary = try confirmClearedBlocker(allocator, evidence.kernel_id);
                    confirmed_clear = promotionBlockerEvidenceCleared(confirmation_summary, evidence.blocker);
                }
                if (confirmed_clear) {
                    summary.confirmed_cleared_blocker_count += 1;
                    if (print_details) {
                        std.debug.print(
                            "quant-kernel-metal-runtime-check blocker_evidence confirmed_cleared kernel={s} table_blocker={s} confirmation_rounds={d} promotion_ready={d}/{d}\n",
                            .{ evidence.kernel_id, evidence.blocker, confirmation_rounds, confirmation_summary.promotion_ready_count, confirmation_summary.promotion_case_count },
                        );
                    }
                } else {
                    summary.unconfirmed_cleared_blocker_count += 1;
                    if (print_details) {
                        std.debug.print(
                            "quant-kernel-metal-runtime-check blocker_evidence unconfirmed_cleared kernel={s} table_blocker={s} confirmation_ready={d}/{d} blockers={{speedup_gate:{d},unstable_benchmark:{d},runtime_route_only:{d},missing_generated:{d},missing_provider:{d},unsupported_handwritten:{d},dev_only:{d}}}\n",
                            .{
                                evidence.kernel_id,
                                evidence.blocker,
                                confirmation_summary.promotion_ready_count,
                                confirmation_summary.promotion_case_count,
                                confirmation_summary.blocker_counts.speedup_gate_missing,
                                confirmation_summary.blocker_counts.unstable_benchmark_timing,
                                confirmation_summary.blocker_counts.runtime_route_only,
                                confirmation_summary.blocker_counts.missing_generated_route,
                                confirmation_summary.blocker_counts.missing_provider_route,
                                confirmation_summary.blocker_counts.unsupported_handwritten_baseline,
                                confirmation_summary.blocker_counts.dev_only_candidate,
                            },
                        );
                    }
                }
            }
            if (print_details) {
                std.debug.print(
                    "quant-kernel-metal-runtime-check blocker_evidence cleared kernel={s} table_blocker={s} path={s} promotion_ready={d}/{d}\n",
                    .{ evidence.kernel_id, evidence.blocker, evidence_path, evidence_summary.promotion_ready_count, evidence_summary.promotion_case_count },
                );
            }
        } else if (!promotionBlockerEvidenceMatchesExactly(evidence_summary.blocker_counts, evidence.blocker) and
            promotionBlockerEvidenceTimingDrifted(evidence_summary.blocker_counts, evidence.blocker))
        {
            summary.timing_blocker_drift_count += 1;
            if (print_details) {
                std.debug.print(
                    "quant-kernel-metal-runtime-check blocker_evidence timing_drift kernel={s} table_blocker={s} path={s} evidence_blockers={{speedup_gate:{d},unstable_benchmark:{d}}}\n",
                    .{ evidence.kernel_id, evidence.blocker, evidence_path, evidence_summary.blocker_counts.speedup_gate_missing, evidence_summary.blocker_counts.unstable_benchmark_timing },
                );
            }
        }
        if (!blocker_cleared and !promotionBlockerEvidenceMatches(evidence_summary.blocker_counts, evidence.blocker)) {
            std.debug.print(
                "quant-kernel-metal-runtime-check blocker_evidence mismatch kernel={s} expected_blocker={s} path={s}\n",
                .{ evidence.kernel_id, evidence.blocker, evidence_path },
            );
            return error.MetalBlockerEvidenceMismatch;
        }
        if (!blocker_cleared and (evidence_summary.promotion_case_count == 0 or evidence_summary.promotion_ready_count == evidence_summary.promotion_case_count)) {
            std.debug.print(
                "quant-kernel-metal-runtime-check blocker_evidence stale_ready kernel={s} expected_blocker={s} path={s} promotion_ready={d}/{d}\n",
                .{ evidence.kernel_id, evidence.blocker, evidence_path, evidence_summary.promotion_ready_count, evidence_summary.promotion_case_count },
            );
            return error.MetalBlockerEvidenceMismatch;
        }
        summary.checked_path_count += 1;
    }
    if (summary.evidence_case_count != quant_kernel_compiler.first_metal_promotion_blocker_evidence_expected_case_count) {
        std.debug.print(
            "quant-kernel-metal-runtime-check blocker_evidence case_count_mismatch expected={d} actual={d}\n",
            .{ quant_kernel_compiler.first_metal_promotion_blocker_evidence_expected_case_count, summary.evidence_case_count },
        );
        return error.MetalBlockerEvidenceCaseCountMismatch;
    }
    if (summary.candidate_route_ready_count != quant_kernel_compiler.first_metal_promotion_blocker_evidence_expected_route_ready_count) {
        std.debug.print(
            "quant-kernel-metal-runtime-check blocker_evidence route_ready_mismatch expected={d} actual={d}\n",
            .{ quant_kernel_compiler.first_metal_promotion_blocker_evidence_expected_route_ready_count, summary.candidate_route_ready_count },
        );
        return error.MetalBlockerEvidenceRouteMissing;
    }
    return summary;
}

fn checkEvidenceFile(allocator: std.mem.Allocator, path: []const u8, require_promotion_ready: bool, require_runtime_route_all: bool, require_kernel: ?[]const u8) !void {
    _ = try checkEvidenceFileWithSummary(allocator, path, require_promotion_ready, require_runtime_route_all, require_kernel);
}

fn checkEvidenceFileWithSummary(allocator: std.mem.Allocator, path: []const u8, require_promotion_ready: bool, require_runtime_route_all: bool, require_kernel: ?[]const u8) !EvidenceSummary {
    return checkEvidenceFileWithSummaryExpected(allocator, path, require_promotion_ready, require_runtime_route_all, require_kernel, null);
}

fn checkEvidenceFileWithSummaryExpected(
    allocator: std.mem.Allocator,
    path: []const u8,
    require_promotion_ready: bool,
    require_runtime_route_all: bool,
    require_kernel: ?[]const u8,
    expected_provenance_override: ?AttestedProvenance,
) !EvidenceSummary {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(compat.io(), path, allocator, .limited(1024 * 1024));
    defer allocator.free(bytes);
    try checkEvidenceJson(allocator, bytes, require_promotion_ready, require_runtime_route_all, require_kernel);
    if (require_promotion_ready) {
        if (expected_provenance_override) |expected_provenance| {
            try checkEvidenceProvenanceMatches(allocator, bytes, expected_provenance);
        } else {
            var expected_provenance = try collectAttestedProvenance(allocator, compat.io());
            defer expected_provenance.deinit(allocator);
            try checkEvidenceProvenanceMatches(allocator, bytes, expected_provenance.value);
        }
    }
    try checkEvidenceJsonCommandPath(allocator, bytes, path);
    return evidenceSummaryFromJson(allocator, bytes);
}

fn evidenceSummaryFromJson(allocator: std.mem.Allocator, bytes: []const u8) !EvidenceSummary {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidMetalEvidence;
    var promotion_worst_repeat_speedup: ?f64 = null;
    if (!jsonNullableF64(root.object.get("promotion_worst_repeat_speedup") orelse return error.InvalidMetalEvidence, &promotion_worst_repeat_speedup)) return error.InvalidMetalEvidence;
    const blocker_counts = PromotionBlockerCounts{
        .speedup_gate_missing = jsonUsize(root.object.get("promotion_blocker_speedup_gate_missing_count")) orelse return error.InvalidMetalEvidence,
        .unstable_benchmark_timing = jsonUsize(root.object.get("promotion_blocker_unstable_benchmark_timing_count")) orelse return error.InvalidMetalEvidence,
        .runtime_route_only = jsonUsize(root.object.get("promotion_blocker_runtime_route_only_count")) orelse return error.InvalidMetalEvidence,
        .missing_generated_route = jsonUsize(root.object.get("promotion_blocker_missing_generated_route_count")) orelse return error.InvalidMetalEvidence,
        .missing_provider_route = jsonUsize(root.object.get("promotion_blocker_missing_provider_route_count")) orelse return error.InvalidMetalEvidence,
        .unsupported_handwritten_baseline = jsonUsize(root.object.get("promotion_blocker_unsupported_handwritten_count")) orelse return error.InvalidMetalEvidence,
        .dev_only_candidate = jsonUsize(root.object.get("promotion_blocker_dev_only_candidate_count")) orelse return error.InvalidMetalEvidence,
    };
    try validateSlowFallbackSummary(root, blocker_counts);
    _ = try benchmarkSpeedupSummaryFromRoot(root);
    const production_regression_check = jsonBoolValue(root.object.get("production_regression_check")) orelse false;
    const compiler_manifest_case_count = jsonUsize(root.object.get("compiler_benchmark_manifest_case_count"));
    const compiler_manifest_case_fingerprint = jsonU64(root.object.get("compiler_benchmark_manifest_case_fingerprint"));
    return .{
        .case_count = jsonUsize(root.object.get("case_count")) orelse return error.InvalidMetalEvidence,
        .promotion_case_count = jsonUsize(root.object.get("promotion_case_count")) orelse return error.InvalidMetalEvidence,
        .promotion_ready_count = jsonUsize(root.object.get("promotion_ready_count")) orelse return error.InvalidMetalEvidence,
        .runtime_route_checked_count = jsonUsize(root.object.get("runtime_route_checked_count")) orelse return error.InvalidMetalEvidence,
        .provider_route_checked_count = jsonUsize(root.object.get("provider_route_checked_count")) orelse return error.InvalidMetalEvidence,
        .candidate_route_ready_count = jsonUsize(root.object.get("candidate_route_ready_count")) orelse return error.InvalidMetalEvidence,
        .candidate_benchmark_ready_count = jsonUsize(root.object.get("candidate_benchmark_ready_count")) orelse return error.InvalidMetalEvidence,
        .promotion_worst_repeat_speedup = promotion_worst_repeat_speedup,
        .production_regression_check = production_regression_check,
        .compiler_benchmark_manifest_case_count = compiler_manifest_case_count,
        .compiler_benchmark_manifest_case_fingerprint = compiler_manifest_case_fingerprint,
        .blocker_counts = blocker_counts,
    };
}

fn validateSlowFallbackSummary(root: std.json.Value, blocker_counts: PromotionBlockerCounts) !void {
    const slow_fallback_count = jsonUsize(root.object.get("slow_fallback_count")) orelse return error.InvalidMetalEvidence;
    if (slow_fallback_count != blocker_counts.slowFallbackCount()) return error.InvalidMetalEvidence;
    const top_slow_reason = jsonString(root.object.get("top_slow_fallback_reason")) orelse return error.InvalidMetalEvidence;
    if (!std.mem.eql(u8, top_slow_reason, blocker_counts.topSlowFallbackReason())) return error.InvalidMetalEvidence;
    const top_slow_count = jsonUsize(root.object.get("top_slow_fallback_count")) orelse return error.InvalidMetalEvidence;
    if (top_slow_count != blocker_counts.topSlowFallbackCount()) return error.InvalidMetalEvidence;
}

fn benchmarkSpeedupSummaryFromRoot(root: std.json.Value) !BenchmarkSpeedupSummary {
    if (root != .object) return error.InvalidMetalEvidence;
    const supported_count = jsonUsize(root.object.get("benchmark_supported_count")) orelse return error.InvalidMetalEvidence;
    const pass_count = jsonUsize(root.object.get("benchmark_speedup_pass_count")) orelse return error.InvalidMetalEvidence;
    if (pass_count > supported_count) return error.InvalidMetalEvidence;

    var min_speedup: ?f64 = null;
    if (!jsonNullableF64(root.object.get("benchmark_speedup_min") orelse return error.InvalidMetalEvidence, &min_speedup)) return error.InvalidMetalEvidence;
    var max_speedup: ?f64 = null;
    if (!jsonNullableF64(root.object.get("benchmark_speedup_max") orelse return error.InvalidMetalEvidence, &max_speedup)) return error.InvalidMetalEvidence;
    var avg_speedup: ?f64 = null;
    if (!jsonNullableF64(root.object.get("benchmark_speedup_avg") orelse return error.InvalidMetalEvidence, &avg_speedup)) return error.InvalidMetalEvidence;

    var min_case: ?[]const u8 = null;
    if (!jsonNullableString(root.object.get("benchmark_speedup_min_case") orelse return error.InvalidMetalEvidence, &min_case)) return error.InvalidMetalEvidence;
    var max_case: ?[]const u8 = null;
    if (!jsonNullableString(root.object.get("benchmark_speedup_max_case") orelse return error.InvalidMetalEvidence, &max_case)) return error.InvalidMetalEvidence;

    if (supported_count == 0) {
        if (pass_count != 0 or min_speedup != null or max_speedup != null or avg_speedup != null or min_case != null or max_case != null) return error.InvalidMetalEvidence;
    } else {
        if (min_speedup == null or max_speedup == null or avg_speedup == null or min_case == null or max_case == null) return error.InvalidMetalEvidence;
        if (min_speedup.? > max_speedup.?) return error.InvalidMetalEvidence;
    }

    return .{
        .supported_count = supported_count,
        .pass_count = pass_count,
        .min_speedup = min_speedup,
        .min_case = min_case,
        .max_speedup = max_speedup,
        .max_case = max_case,
        .sum_speedup = if (avg_speedup) |avg| avg * @as(f64, @floatFromInt(supported_count)) else 0.0,
    };
}

fn checkEvidenceJsonCommandPath(allocator: std.mem.Allocator, bytes: []const u8, path: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidMetalEvidence;
    const command = jsonString(root.object.get("benchmark_command")) orelse return error.InvalidMetalEvidence;
    if (!try commandEvidenceOutMatches(allocator, command, path)) return error.InvalidMetalEvidence;
}

fn commandEvidenceOutMatches(allocator: std.mem.Allocator, command: []const u8, path: []const u8) !bool {
    const actual = commandArgValue(command, "--evidence-out") orelse return false;
    if (std.mem.eql(u8, actual, path)) return true;
    const actual_real = compat.cwd().realPathFileAlloc(compat.io(), actual, allocator) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer allocator.free(actual_real);
    const expected_real = compat.cwd().realPathFileAlloc(compat.io(), path, allocator) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer allocator.free(expected_real);
    return std.mem.eql(u8, actual_real, expected_real);
}

fn commandHasToken(command: []const u8, expected: []const u8) bool {
    var tokens = std.mem.tokenizeScalar(u8, command, ' ');
    while (tokens.next()) |token| {
        if (std.mem.eql(u8, token, expected)) return true;
    }
    return false;
}

fn commandHasArgValue(command: []const u8, arg: []const u8, value: []const u8) bool {
    const actual = commandArgValue(command, arg) orelse return false;
    return std.mem.eql(u8, actual, value);
}

fn commandArgValue(command: []const u8, arg: []const u8) ?[]const u8 {
    var tokens = std.mem.tokenizeScalar(u8, command, ' ');
    while (tokens.next()) |token| {
        if (std.mem.eql(u8, token, arg)) {
            return tokens.next();
        }
    }
    return null;
}

fn isLowerHex(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |byte| {
        if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return false;
    }
    return true;
}

fn isUtcSecondTimestamp(value: []const u8) bool {
    if (value.len != 20 or value[4] != '-' or value[7] != '-' or value[10] != 'T' or value[13] != ':' or value[16] != ':' or value[19] != 'Z') return false;
    for (value, 0..) |byte, index| {
        if (index == 4 or index == 7 or index == 10 or index == 13 or index == 16 or index == 19) continue;
        if (!std.ascii.isDigit(byte)) return false;
    }
    const month = std.fmt.parseInt(u8, value[5..7], 10) catch return false;
    const day = std.fmt.parseInt(u8, value[8..10], 10) catch return false;
    const hour = std.fmt.parseInt(u8, value[11..13], 10) catch return false;
    const minute = std.fmt.parseInt(u8, value[14..16], 10) catch return false;
    const second = std.fmt.parseInt(u8, value[17..19], 10) catch return false;
    return month >= 1 and month <= 12 and day >= 1 and day <= 31 and hour <= 23 and minute <= 59 and second <= 60;
}

fn parseEvidenceProvenance(root: std.json.Value, benchmark_command: []const u8, require_promotion_ready: bool) !?AttestedProvenance {
    const status = jsonString(root.object.get("provenance_status")) orelse return error.InvalidMetalEvidence;
    const blocker = jsonString(root.object.get("provenance_blocker")) orelse return error.InvalidMetalEvidence;
    const source_commit = jsonString(root.object.get("source_commit")) orelse return error.InvalidMetalEvidence;
    const source_tree_clean = jsonBoolValue(root.object.get("source_tree_clean")) orelse return error.InvalidMetalEvidence;
    const source_status_sha256 = jsonString(root.object.get("source_status_sha256")) orelse return error.InvalidMetalEvidence;
    const host_os = jsonString(root.object.get("host_os")) orelse return error.InvalidMetalEvidence;
    const host_arch = jsonString(root.object.get("host_arch")) orelse return error.InvalidMetalEvidence;
    const accelerator_name = jsonString(root.object.get("accelerator_name")) orelse return error.InvalidMetalEvidence;
    const metal_compiler_version = jsonString(root.object.get("metal_compiler_version")) orelse return error.InvalidMetalEvidence;
    const zig_version = jsonString(root.object.get("zig_version")) orelse return error.InvalidMetalEvidence;
    const recorded_at_utc = jsonString(root.object.get("recorded_at_utc")) orelse return error.InvalidMetalEvidence;

    if (std.mem.eql(u8, status, metal_runtime_evidence_provenance_local)) {
        if (!std.mem.eql(u8, blocker, metal_runtime_evidence_provenance_missing) or
            source_commit.len != 0 or source_tree_clean or source_status_sha256.len != 0 or
            host_os.len != 0 or host_arch.len != 0 or accelerator_name.len != 0 or
            metal_compiler_version.len != 0 or zig_version.len != 0 or recorded_at_utc.len != 0 or
            commandHasToken(benchmark_command, "--attest-provenance"))
        {
            return error.InvalidMetalEvidence;
        }
        if (require_promotion_ready) return error.MetalEvidenceReproducibleProvenanceMissing;
        return null;
    }

    if (!std.mem.eql(u8, status, metal_runtime_evidence_provenance_attested) or blocker.len != 0) return error.InvalidMetalEvidence;
    if (!commandHasToken(benchmark_command, "--attest-provenance")) return error.InvalidMetalEvidence;
    if (!source_tree_clean or !std.mem.eql(u8, source_status_sha256, clean_source_status_sha256)) return error.InvalidMetalEvidence;
    if ((source_commit.len != 40 and source_commit.len != 64) or !isLowerHex(source_commit)) return error.InvalidMetalEvidence;
    if (host_os.len == 0 or host_arch.len == 0 or accelerator_name.len == 0 or metal_compiler_version.len == 0 or zig_version.len == 0) return error.InvalidMetalEvidence;
    if (!isUtcSecondTimestamp(recorded_at_utc)) return error.InvalidMetalEvidence;

    return .{
        .source_commit = source_commit,
        .source_tree_clean = source_tree_clean,
        .source_status_sha256 = source_status_sha256,
        .host_os = host_os,
        .host_arch = host_arch,
        .accelerator_name = accelerator_name,
        .metal_compiler_version = metal_compiler_version,
        .zig_version = zig_version,
        .recorded_at_utc = recorded_at_utc,
    };
}

fn attestedProvenanceMatches(actual: AttestedProvenance, expected: AttestedProvenance) bool {
    return actual.source_tree_clean == expected.source_tree_clean and
        std.mem.eql(u8, actual.source_commit, expected.source_commit) and
        std.mem.eql(u8, actual.source_status_sha256, expected.source_status_sha256) and
        std.mem.eql(u8, actual.host_os, expected.host_os) and
        std.mem.eql(u8, actual.host_arch, expected.host_arch) and
        std.mem.eql(u8, actual.accelerator_name, expected.accelerator_name) and
        std.mem.eql(u8, actual.metal_compiler_version, expected.metal_compiler_version) and
        std.mem.eql(u8, actual.zig_version, expected.zig_version);
}

fn checkEvidenceProvenanceMatches(allocator: std.mem.Allocator, bytes: []const u8, expected: AttestedProvenance) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidMetalEvidence;
    const benchmark_command = jsonString(root.object.get("benchmark_command")) orelse return error.InvalidMetalEvidence;
    const actual = (try parseEvidenceProvenance(root, benchmark_command, true)) orelse return error.MetalEvidenceReproducibleProvenanceMissing;
    if (!attestedProvenanceMatches(actual, expected)) return error.MetalEvidenceProvenanceMismatch;
}

fn checkEvidenceJson(allocator: std.mem.Allocator, bytes: []const u8, require_promotion_ready: bool, require_runtime_route_all: bool, require_kernel: ?[]const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidMetalEvidence;

    const contract = jsonString(root.object.get("evidence_contract")) orelse return error.InvalidMetalEvidence;
    if (!std.mem.eql(u8, contract, metal_quant_evidence_contract)) return error.InvalidMetalEvidence;
    const schema = jsonString(root.object.get("schema")) orelse return error.InvalidMetalEvidence;
    if (!std.mem.eql(u8, schema, metal_runtime_evidence_schema)) return error.InvalidMetalEvidence;
    const benchmark_command = jsonString(root.object.get("benchmark_command")) orelse return error.InvalidMetalEvidence;
    if (!std.mem.startsWith(u8, benchmark_command, "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out ")) return error.InvalidMetalEvidence;
    _ = try parseEvidenceProvenance(root, benchmark_command, require_promotion_ready);
    const benchmark_mode = jsonString(root.object.get("benchmark_mode")) orelse return error.InvalidMetalEvidence;
    if (!std.mem.eql(u8, benchmark_mode, "sequential")) return error.InvalidMetalEvidence;
    const repeat_runs = jsonUsize(root.object.get("repeat_runs")) orelse return error.InvalidMetalEvidence;
    if (repeat_runs == 0 or repeat_runs > 31) return error.InvalidMetalEvidence;
    const warmup_repeat_runs = jsonUsize(root.object.get("warmup_repeat_runs")) orelse return error.InvalidMetalEvidence;
    if (warmup_repeat_runs != if (repeat_runs > 1) repeated_check_warmup_runs else 0) return error.InvalidMetalEvidence;
    if (require_promotion_ready and !hasPromotionRepeatRuns(repeat_runs)) return error.MetalEvidencePromotionNotReady;
    const timing_aggregation = jsonString(root.object.get("timing_aggregation")) orelse return error.InvalidMetalEvidence;
    if (!std.mem.eql(u8, timing_aggregation, timingAggregationName(@intCast(repeat_runs)))) return error.InvalidMetalEvidence;
    const minimum_speedup = jsonF64(root.object.get("minimum_speedup")) orelse return error.InvalidMetalEvidence;
    if (!approximately(minimum_speedup, quant_kernel_compiler.metal_promotion_min_speedup, 0.000001)) return error.InvalidMetalEvidence;
    const minimum_speedup_tolerance = jsonF64(root.object.get("minimum_speedup_tolerance")) orelse return error.InvalidMetalEvidence;
    if (!approximately(minimum_speedup_tolerance, quant_kernel_compiler.metal_promotion_speedup_tolerance, 0.000001)) return error.InvalidMetalEvidence;
    if (repeat_runs > 1) {
        const repeat_arg = try std.fmt.allocPrint(allocator, " --repeat-runs {d}", .{repeat_runs});
        defer allocator.free(repeat_arg);
        if (!std.mem.containsAtLeast(u8, benchmark_command, 1, repeat_arg)) return error.InvalidMetalEvidence;
    }
    const measure_iters_override = if (commandArgValue(benchmark_command, "--measure-iters")) |value|
        parseMeasureIters(value) catch return error.InvalidMetalEvidence
    else
        null;
    if (measure_iters_override == null and commandHasToken(benchmark_command, "--measure-iters")) return error.InvalidMetalEvidence;
    const production_enabled = jsonBoolValue(root.object.get("production_enabled")) orelse return error.InvalidMetalEvidence;
    const production_regression_check = jsonBoolValue(root.object.get("production_regression_check")) orelse false;
    if (production_regression_check) {
        if (!commandHasToken(benchmark_command, "--production-regression-check")) return error.InvalidMetalEvidence;
        if (!hasPromotionRepeatRuns(repeat_runs)) return error.InvalidMetalEvidence;
        if (measure_iters_override != quant_kernel_compiler.metal_promotion_measure_iters) return error.InvalidMetalEvidence;
        const compiler_manifest_schema = jsonString(root.object.get("compiler_benchmark_manifest_schema")) orelse return error.InvalidMetalEvidence;
        if (!std.mem.eql(u8, compiler_manifest_schema, quant_kernel_compiler.first_benchmark_manifest_schema)) return error.InvalidMetalEvidence;
        const compiler_manifest_case_count = jsonUsize(root.object.get("compiler_benchmark_manifest_case_count")) orelse return error.InvalidMetalEvidence;
        if (compiler_manifest_case_count != quant_kernel_compiler.first_metal_production_benchmark_case_count) return error.InvalidMetalEvidence;
        const compiler_manifest_case_fingerprint = jsonU64(root.object.get("compiler_benchmark_manifest_case_fingerprint")) orelse return error.InvalidMetalEvidence;
        if (compiler_manifest_case_fingerprint != quant_kernel_compiler.metalProductionBenchmarkCaseManifestFingerprint()) return error.InvalidMetalEvidence;
    } else if (commandHasToken(benchmark_command, "--production-regression-check")) {
        return error.InvalidMetalEvidence;
    }
    const runtime_route_kernel_value = root.object.get("runtime_route_kernel") orelse return error.InvalidMetalEvidence;
    const runtime_route_kernel: ?[]const u8 = switch (runtime_route_kernel_value) {
        .null => null,
        else => jsonString(runtime_route_kernel_value) orelse return error.InvalidMetalEvidence,
    };
    if (runtime_route_kernel) |kernel| {
        if (!commandHasArgValue(benchmark_command, "--runtime-route-kernel", kernel)) return error.InvalidMetalEvidence;
    } else if (commandHasToken(benchmark_command, "--runtime-route-kernel")) {
        return error.InvalidMetalEvidence;
    }
    const runtime_route_all = jsonBoolValue(root.object.get("runtime_route_all")) orelse false;
    if (require_runtime_route_all and !runtime_route_all) return error.MetalEvidenceRuntimeRouteAllMissing;
    if (runtime_route_all) {
        if (!commandHasToken(benchmark_command, "--runtime-route-all")) return error.InvalidMetalEvidence;
    } else if (commandHasToken(benchmark_command, "--runtime-route-all")) {
        return error.InvalidMetalEvidence;
    }
    var promoted_kernel: ?[]const u8 = null;
    if (production_enabled) {
        if (production_regression_check) {
            if (commandHasToken(benchmark_command, "--promotion-ready-kernel")) return error.InvalidMetalEvidence;
        } else {
            promoted_kernel = commandArgValue(benchmark_command, "--promotion-ready-kernel") orelse return error.InvalidMetalEvidence;
            if (measure_iters_override != quant_kernel_compiler.metal_promotion_measure_iters) return error.InvalidMetalEvidence;
            const evidence_path = commandArgValue(benchmark_command, "--evidence-out") orelse return error.InvalidMetalEvidence;
            if (!std.mem.containsAtLeast(u8, evidence_path, 1, promoted_kernel.?)) return error.InvalidMetalEvidence;
            if (require_kernel) |required| {
                if (!std.mem.eql(u8, promoted_kernel.?, required)) return error.InvalidMetalEvidence;
            }
        }
    } else if (commandHasToken(benchmark_command, "--promotion-ready-kernel")) {
        return error.InvalidMetalEvidence;
    }
    if (promoted_kernel != null and runtime_route_kernel != null) return error.InvalidMetalEvidence;
    if (promoted_kernel != null and runtime_route_all) return error.InvalidMetalEvidence;
    if (runtime_route_kernel != null and runtime_route_all) return error.InvalidMetalEvidence;
    if (production_regression_check and (promoted_kernel != null or runtime_route_kernel != null or runtime_route_all or !production_enabled)) return error.InvalidMetalEvidence;
    if (require_promotion_ready) {
        if (!production_enabled) return error.MetalEvidencePromotionNotReady;
    }

    const cases_value = root.object.get("cases") orelse return error.InvalidMetalEvidence;
    if (cases_value != .array) return error.InvalidMetalEvidence;
    const case_count = jsonUsize(root.object.get("case_count")) orelse return error.InvalidMetalEvidence;
    const route_kernel = promoted_kernel orelse runtime_route_kernel;
    const expected_case_count = if (route_kernel) |kernel|
        expectedMetalEvidenceCaseCountForKernel(kernel)
    else if (production_regression_check)
        expectedProductionRegressionEvidenceCaseCount()
    else if (runtime_route_all)
        expectedRuntimeRouteAllEvidenceCaseCount()
    else
        expectedMetalEvidenceCaseCount();
    if (runtime_route_all and expected_case_count != quant_kernel_compiler.first_metal_runtime_route_all_expected_case_count) return error.InvalidMetalEvidence;
    if (case_count != cases_value.array.items.len or (case_count == 0 and !production_regression_check)) return error.InvalidMetalEvidence;
    if (case_count != expected_case_count) return error.InvalidMetalEvidence;
    if (!evidenceCasesHaveUniqueNames(cases_value.array.items)) return error.InvalidMetalEvidence;
    if (production_regression_check and !productionEvidenceCasesMatchCompilerManifest(cases_value.array.items)) return error.InvalidMetalEvidence;

    const expected_promotion_case_count = jsonUsize(root.object.get("promotion_case_count")) orelse return error.InvalidMetalEvidence;
    const expected_promotion_ready_count = jsonUsize(root.object.get("promotion_ready_count")) orelse return error.InvalidMetalEvidence;
    if (expected_promotion_ready_count > expected_promotion_case_count) return error.InvalidMetalEvidence;
    var expected_promotion_worst_repeat_speedup: ?f64 = null;
    if (!jsonNullableF64(root.object.get("promotion_worst_repeat_speedup") orelse return error.InvalidMetalEvidence, &expected_promotion_worst_repeat_speedup)) return error.InvalidMetalEvidence;
    const expected_promotion_worst_repeat_case_value = root.object.get("promotion_worst_repeat_case") orelse return error.InvalidMetalEvidence;
    const expected_promotion_worst_repeat_case: ?[]const u8 = switch (expected_promotion_worst_repeat_case_value) {
        .null => null,
        else => jsonString(expected_promotion_worst_repeat_case_value) orelse return error.InvalidMetalEvidence,
    };
    const expected_runtime_route_checked_count = jsonUsize(root.object.get("runtime_route_checked_count")) orelse return error.InvalidMetalEvidence;
    const expected_provider_route_checked_count = jsonUsize(root.object.get("provider_route_checked_count")) orelse return error.InvalidMetalEvidence;
    const expected_candidate_route_ready_count = jsonUsize(root.object.get("candidate_route_ready_count")) orelse return error.InvalidMetalEvidence;
    const expected_candidate_benchmark_ready_count = jsonUsize(root.object.get("candidate_benchmark_ready_count")) orelse return error.InvalidMetalEvidence;
    if (expected_candidate_route_ready_count > case_count or expected_candidate_benchmark_ready_count > case_count) return error.InvalidMetalEvidence;
    const expected_benchmark_speedups = try benchmarkSpeedupSummaryFromRoot(root);
    if (expected_benchmark_speedups.supported_count > case_count) return error.InvalidMetalEvidence;
    if (runtime_route_all and expected_provider_route_checked_count != quant_kernel_compiler.first_metal_runtime_route_all_expected_provider_route_count) return error.InvalidMetalEvidence;
    const expected_blocker_counts = PromotionBlockerCounts{
        .speedup_gate_missing = jsonUsize(root.object.get("promotion_blocker_speedup_gate_missing_count")) orelse return error.InvalidMetalEvidence,
        .unstable_benchmark_timing = jsonUsize(root.object.get("promotion_blocker_unstable_benchmark_timing_count")) orelse return error.InvalidMetalEvidence,
        .runtime_route_only = jsonUsize(root.object.get("promotion_blocker_runtime_route_only_count")) orelse return error.InvalidMetalEvidence,
        .missing_generated_route = jsonUsize(root.object.get("promotion_blocker_missing_generated_route_count")) orelse return error.InvalidMetalEvidence,
        .missing_provider_route = jsonUsize(root.object.get("promotion_blocker_missing_provider_route_count")) orelse return error.InvalidMetalEvidence,
        .unsupported_handwritten_baseline = jsonUsize(root.object.get("promotion_blocker_unsupported_handwritten_count")) orelse return error.InvalidMetalEvidence,
        .dev_only_candidate = jsonUsize(root.object.get("promotion_blocker_dev_only_candidate_count")) orelse return error.InvalidMetalEvidence,
    };
    try validateSlowFallbackSummary(root, expected_blocker_counts);
    if (runtime_route_all) {
        const status = jsonString(root.object.get("runtime_route_all_status")) orelse return error.InvalidMetalEvidence;
        const expected_status = runtimeRouteAllEvidenceStatus(.{
            .case_count = case_count,
            .promotion_case_count = expected_promotion_case_count,
            .promotion_ready_count = expected_promotion_ready_count,
            .runtime_route_checked_count = expected_runtime_route_checked_count,
            .provider_route_checked_count = expected_provider_route_checked_count,
            .candidate_route_ready_count = expected_candidate_route_ready_count,
            .candidate_benchmark_ready_count = expected_candidate_benchmark_ready_count,
            .blocker_counts = expected_blocker_counts,
        });
        if (!std.mem.eql(u8, status, expected_status)) return error.InvalidMetalEvidence;
    } else if (root.object.get("runtime_route_all_status") != null) {
        return error.InvalidMetalEvidence;
    }
    if (production_regression_check) {
        const status = jsonString(root.object.get("production_regression_status")) orelse return error.InvalidMetalEvidence;
        const expected_status = productionRegressionEvidenceStatus(.{
            .case_count = case_count,
            .promotion_case_count = expected_promotion_case_count,
            .promotion_ready_count = expected_promotion_ready_count,
            .runtime_route_checked_count = expected_runtime_route_checked_count,
            .provider_route_checked_count = expected_provider_route_checked_count,
            .candidate_route_ready_count = expected_candidate_route_ready_count,
            .candidate_benchmark_ready_count = expected_candidate_benchmark_ready_count,
            .blocker_counts = expected_blocker_counts,
        });
        if (!std.mem.eql(u8, status, expected_status)) return error.InvalidMetalEvidence;
    } else if (root.object.get("production_regression_status") != null) {
        return error.InvalidMetalEvidence;
    }

    for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal or artifact.runtime_evidence_command.len == 0) continue;
        if (route_kernel) |kernel| {
            if (!std.mem.eql(u8, artifact.kernel_id, kernel)) continue;
        } else if (production_regression_check) {
            if (!productionMetalRuntimeArtifact(artifact)) continue;
        } else if (runtime_route_all) {
            if (!runtimeRouteArtifactSupported(artifact)) continue;
        }
        var found = false;
        for (cases_value.array.items) |case_value| {
            if (evidenceCaseMatchesArtifact(allocator, case_value, artifact)) {
                found = true;
                break;
            }
        }
        if (!found) return error.InvalidMetalEvidence;
    }

    for (metal_runtime_checks) |base_check| {
        if (route_kernel) |kernel| {
            if (!std.mem.eql(u8, base_check.kernel_name, kernel)) continue;
        } else if (production_regression_check) {
            if (!productionMetalRuntimeCheck(base_check)) continue;
        } else if (runtime_route_all) {
            if (!runtimeRouteAllSupported(base_check)) continue;
        }
        var check = base_check;
        if (measure_iters_override) |measure_iters| check.measure_iters = measure_iters;
        var found = false;
        for (cases_value.array.items) |case_value| {
            if (evidenceCaseMatchesCheck(allocator, case_value, check, route_kernel, promoted_kernel, runtime_route_all, production_regression_check)) {
                found = true;
                break;
            }
        }
        if (!found) return error.InvalidMetalEvidence;
    }

    var found_required_kernel = require_kernel == null;
    var actual_promotion_case_count: usize = 0;
    var actual_promotion_ready_count: usize = 0;
    var actual_runtime_route_checked_count: usize = 0;
    var actual_provider_route_checked_count: usize = 0;
    var actual_candidate_route_ready_count: usize = 0;
    var actual_candidate_benchmark_ready_count: usize = 0;
    var actual_promotion_worst_repeat_speedup: ?f64 = null;
    var actual_promotion_worst_repeat_case: ?[]const u8 = null;
    var actual_benchmark_speedups = BenchmarkSpeedupSummary{};
    var actual_blocker_counts = PromotionBlockerCounts{};
    for (cases_value.array.items) |case_value| {
        if (case_value != .object) return error.InvalidMetalEvidence;
        const case_name = jsonString(case_value.object.get("name")) orelse return error.InvalidMetalEvidence;
        const kernel_id = jsonString(case_value.object.get("kernel_id")) orelse return error.InvalidMetalEvidence;
        const kernel_required = if (require_kernel) |required| std.mem.eql(u8, kernel_id, required) else true;
        if (kernel_required) found_required_kernel = true;
        const correctness_passed = jsonBoolValue(case_value.object.get("correctness_passed")) orelse return error.InvalidMetalEvidence;
        if (!correctness_passed) return error.MetalEvidenceCorrectnessFailed;
        const standalone_generated_checked = jsonBoolValue(case_value.object.get("standalone_generated_checked")) orelse return error.InvalidMetalEvidence;
        if (!standalone_generated_checked) return error.MetalEvidenceCorrectnessFailed;
        const decode_runtime_route_checked = jsonBoolValue(case_value.object.get("decode_runtime_route_checked")) orelse return error.InvalidMetalEvidence;
        const generated_route_checked = jsonBoolValue(case_value.object.get("generated_route_checked")) orelse return error.InvalidMetalEvidence;
        if (decode_runtime_route_checked != generated_route_checked) return error.InvalidMetalEvidence;
        const provider_route_checked = jsonBoolValue(case_value.object.get("provider_route_checked")) orelse return error.InvalidMetalEvidence;
        if (generated_route_checked) actual_runtime_route_checked_count += 1;
        if (provider_route_checked) actual_provider_route_checked_count += 1;
        if (!evidenceCaseHasValidError(case_value)) return error.InvalidMetalEvidence;
        const benchmark_passed = jsonBoolValue(case_value.object.get("benchmark_passed")) orelse return error.InvalidMetalEvidence;
        if (!evidenceCaseHasTiming(case_value)) return error.InvalidMetalEvidence;
        if (!evidenceCaseHasConsistentBenchmarkMath(case_value)) return error.InvalidMetalEvidence;
        const case_handwritten_supported = jsonBoolValue(case_value.object.get("handwritten_baseline_supported")) orelse return error.InvalidMetalEvidence;
        if (case_handwritten_supported) {
            const measured_speedup = jsonF64(case_value.object.get("measured_speedup")) orelse return error.InvalidMetalEvidence;
            actual_benchmark_speedups.add(case_name, measured_speedup, benchmark_passed);
        }
        if (generated_route_checked and (!quant_kernel_compiler.metalProviderRouteRequiredForKernel(kernel_id) or provider_route_checked)) {
            actual_candidate_route_ready_count += 1;
        }
        if (benchmark_passed) actual_candidate_benchmark_ready_count += 1;
        const case_repeat_runs = jsonUsize(case_value.object.get("repeat_runs")) orelse return error.InvalidMetalEvidence;
        if (case_repeat_runs != repeat_runs) return error.InvalidMetalEvidence;
        const case_timing_aggregation = jsonString(case_value.object.get("timing_aggregation")) orelse return error.InvalidMetalEvidence;
        if (!std.mem.eql(u8, case_timing_aggregation, timing_aggregation)) return error.InvalidMetalEvidence;
        const promotion_ready = jsonBoolValue(case_value.object.get("promotion_ready")) orelse return error.InvalidMetalEvidence;
        if (promotion_ready and promoted_kernel == null and !production_regression_check) return error.InvalidMetalEvidence;
        if (promotion_ready and promoted_kernel != null and !std.mem.eql(u8, kernel_id, promoted_kernel.?)) return error.InvalidMetalEvidence;
        const selected_for_promotion = if (promoted_kernel) |kernel|
            std.mem.eql(u8, kernel_id, kernel)
        else
            production_regression_check and productionMetalRuntimeKernel(kernel_id);
        if (selected_for_promotion) {
            actual_promotion_case_count += 1;
            const handwritten_supported = jsonBoolValue(case_value.object.get("handwritten_baseline_supported")) orelse return error.InvalidMetalEvidence;
            if (handwritten_supported) {
                const minimum_repeat_speedup = jsonF64(case_value.object.get("minimum_repeat_speedup")) orelse return error.InvalidMetalEvidence;
                if (actual_promotion_worst_repeat_speedup == null or minimum_repeat_speedup < actual_promotion_worst_repeat_speedup.?) {
                    actual_promotion_worst_repeat_speedup = minimum_repeat_speedup;
                    actual_promotion_worst_repeat_case = case_name;
                }
            }
        }
        if (promotion_ready) actual_promotion_ready_count += 1;
        if (require_promotion_ready and require_kernel != null and !kernel_required and promotion_ready) return error.InvalidMetalEvidence;
        if (!evidenceCaseHasConsistentPromotionBlocker(case_value)) return error.InvalidMetalEvidence;
        const blocker = jsonString(case_value.object.get("promotion_blocker")) orelse return error.InvalidMetalEvidence;
        if (!actual_blocker_counts.add(blocker)) return error.InvalidMetalEvidence;
        if (require_promotion_ready and kernel_required) {
            if (!generated_route_checked) return error.MetalEvidencePromotionNotReady;
            if (quant_kernel_compiler.metalProviderRouteRequiredForKernel(kernel_id) and !provider_route_checked) return error.MetalEvidencePromotionNotReady;
        }
        if (require_promotion_ready and kernel_required and !promotion_ready) {
            return error.MetalEvidencePromotionNotReady;
        }
    }
    if (!found_required_kernel) return error.InvalidMetalEvidence;
    if (expected_promotion_case_count != actual_promotion_case_count) return error.InvalidMetalEvidence;
    if (expected_promotion_ready_count != actual_promotion_ready_count) return error.InvalidMetalEvidence;
    if (expected_promotion_worst_repeat_speedup) |expected_worst| {
        const actual_worst = actual_promotion_worst_repeat_speedup orelse return error.InvalidMetalEvidence;
        if (!approximately(expected_worst, actual_worst, 0.000001)) return error.InvalidMetalEvidence;
        const expected_case = expected_promotion_worst_repeat_case orelse return error.InvalidMetalEvidence;
        const actual_case = actual_promotion_worst_repeat_case orelse return error.InvalidMetalEvidence;
        if (!std.mem.eql(u8, expected_case, actual_case)) return error.InvalidMetalEvidence;
    } else if (actual_promotion_worst_repeat_speedup != null or expected_promotion_worst_repeat_case != null) {
        return error.InvalidMetalEvidence;
    }
    if (expected_runtime_route_checked_count != actual_runtime_route_checked_count) return error.InvalidMetalEvidence;
    if (expected_provider_route_checked_count != actual_provider_route_checked_count) return error.InvalidMetalEvidence;
    if (expected_candidate_route_ready_count != actual_candidate_route_ready_count) return error.InvalidMetalEvidence;
    if (expected_candidate_benchmark_ready_count != actual_candidate_benchmark_ready_count) return error.InvalidMetalEvidence;
    if (!actual_benchmark_speedups.eql(expected_benchmark_speedups)) return error.InvalidMetalEvidence;
    if (!expected_blocker_counts.eql(actual_blocker_counts)) return error.InvalidMetalEvidence;
    if (production_enabled and expected_promotion_case_count == 0 and !production_regression_check) return error.InvalidMetalEvidence;
}

fn expectedMetalEvidenceCaseCount() usize {
    return metal_runtime_checks.len;
}

fn expectedMetalEvidenceCaseCountForKernel(kernel: []const u8) usize {
    var count: usize = 0;
    for (metal_runtime_checks) |check| {
        if (std.mem.eql(u8, check.kernel_name, kernel)) count += 1;
    }
    return count;
}

fn expectedRuntimeRouteAllEvidenceCaseCount() usize {
    var count: usize = 0;
    for (metal_runtime_checks) |check| {
        if (runtimeRouteAllSupported(check)) count += 1;
    }
    return count;
}

fn expectedProductionRegressionEvidenceCaseCount() usize {
    var count: usize = 0;
    for (metal_runtime_checks) |check| {
        if (productionMetalRuntimeCheck(check)) count += 1;
    }
    return count;
}

fn runtimeRouteArtifactSupported(artifact: quant_kernel_compiler.GeneratedMatmulArtifact) bool {
    const check = metalRuntimeCheckForArtifact(artifact) orelse return false;
    return runtimeRouteAllSupported(check);
}

fn productionMetalRuntimeArtifact(artifact: quant_kernel_compiler.GeneratedMatmulArtifact) bool {
    if (artifact.backend != .metal or !quant_kernel_compiler.artifactHasPromotionEvidence(artifact)) return false;
    return metalRuntimeCheckForArtifact(artifact) != null;
}

fn productionMetalRuntimeCheck(check: CheckCase) bool {
    const artifact = metalArtifactForCheck(check) orelse return false;
    return productionMetalRuntimeArtifact(artifact);
}

fn productionMetalRuntimeKernel(kernel_id: []const u8) bool {
    for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == .metal and quant_kernel_compiler.artifactHasPromotionEvidence(artifact) and std.mem.eql(u8, artifact.kernel_id, kernel_id)) return true;
    }
    return false;
}

fn evidenceCasesHaveUniqueNames(cases: []const std.json.Value) bool {
    for (cases, 0..) |case_value, index| {
        if (case_value != .object) return false;
        const name = jsonString(case_value.object.get("name")) orelse return false;
        for (cases[index + 1 ..]) |other_value| {
            if (other_value != .object) return false;
            const other_name = jsonString(other_value.object.get("name")) orelse return false;
            if (std.mem.eql(u8, name, other_name)) return false;
        }
    }
    return true;
}

fn productionEvidenceCasesMatchCompilerManifest(cases: []const std.json.Value) bool {
    if (cases.len != quant_kernel_compiler.first_metal_production_benchmark_case_count) return false;
    for (cases, quant_kernel_compiler.first_metal_production_benchmark_cases) |case_value, expected| {
        if (!productionEvidenceCaseMatchesCompilerManifest(case_value, expected)) return false;
    }
    return true;
}

fn productionEvidenceCaseMatchesCompilerManifest(case_value: std.json.Value, expected: quant_kernel_compiler.MetalProductionBenchmarkCase) bool {
    if (case_value != .object) return false;
    const name = jsonString(case_value.object.get("name")) orelse return false;
    if (!std.mem.eql(u8, name, expected.name)) return false;
    const backend = jsonString(case_value.object.get("backend")) orelse return false;
    if (!std.mem.eql(u8, backend, "metal")) return false;
    const kernel_id = jsonString(case_value.object.get("kernel_id")) orelse return false;
    if (!std.mem.eql(u8, kernel_id, expected.kernel_id)) return false;
    const production_kernel_id = jsonString(case_value.object.get("production_kernel_id")) orelse return false;
    if (!std.mem.eql(u8, production_kernel_id, expected.production_kernel_id)) return false;
    const source_path = jsonString(case_value.object.get("generated_source_path")) orelse return false;
    if (!std.mem.eql(u8, source_path, expected.generated_source_path)) return false;
    const source_fingerprint = jsonU64(case_value.object.get("generated_source_fingerprint")) orelse return false;
    if (source_fingerprint != expected.generated_source_fingerprint) return false;
    const check_command = jsonString(case_value.object.get("metal_check_command")) orelse return false;
    if (!std.mem.eql(u8, check_command, expected.check_command)) return false;
    const format = jsonString(case_value.object.get("format")) orelse return false;
    if (!std.mem.eql(u8, format, @tagName(expected.format))) return false;
    const row_bucket = jsonString(case_value.object.get("row_bucket")) orelse return false;
    if (!std.mem.eql(u8, row_bucket, @tagName(expected.row_bucket))) return false;
    const epilogue = jsonString(case_value.object.get("epilogue")) orelse return false;
    if (!std.mem.eql(u8, epilogue, @tagName(expected.epilogue))) return false;
    const rows = jsonUsize(case_value.object.get("rows")) orelse return false;
    if (rows != expected.rows) return false;
    const in_dim = jsonUsize(case_value.object.get("in_dim")) orelse return false;
    if (in_dim != expected.in_dim) return false;
    const out_dim = jsonUsize(case_value.object.get("out_dim")) orelse return false;
    if (out_dim != expected.out_dim) return false;
    const threads_per_threadgroup = jsonUsize(case_value.object.get("threads_per_threadgroup")) orelse return false;
    if (threads_per_threadgroup != expected.threads_per_threadgroup) return false;
    const cols_per_threadgroup = jsonUsize(case_value.object.get("cols_per_threadgroup")) orelse return false;
    if (cols_per_threadgroup != expected.cols_per_threadgroup) return false;
    const tolerance = jsonF64(case_value.object.get("tolerance_abs")) orelse return false;
    if (!approximately(tolerance, @as(f64, @floatCast(expected.tolerance_abs)), 0.0000001)) return false;
    return true;
}

fn evidenceCaseMatchesArtifact(allocator: std.mem.Allocator, case_value: std.json.Value, artifact: quant_kernel_compiler.GeneratedMatmulArtifact) bool {
    if (case_value != .object) return false;
    const backend = jsonString(case_value.object.get("backend")) orelse return false;
    if (!std.mem.eql(u8, backend, @tagName(artifact.backend))) return false;
    const check = metalRuntimeCheckForArtifact(artifact) orelse return false;
    const source_path = jsonString(case_value.object.get("generated_source_path")) orelse return false;
    if (!std.mem.eql(u8, source_path, generatedSourcePathFor(check))) return false;
    const kernel_id = jsonString(case_value.object.get("kernel_id")) orelse return false;
    if (!std.mem.eql(u8, kernel_id, artifact.kernel_id)) return false;
    const check_command = jsonString(case_value.object.get("metal_check_command")) orelse return false;
    if (!std.mem.eql(u8, check_command, metalCheckCommandFor(check))) return false;
    const format = jsonString(case_value.object.get("format")) orelse return false;
    if (!std.mem.eql(u8, format, @tagName(artifact.format))) return false;
    const row_bucket = jsonString(case_value.object.get("row_bucket")) orelse return false;
    if (!std.mem.eql(u8, row_bucket, @tagName(artifact.row_bucket))) return false;
    const epilogue = jsonString(case_value.object.get("epilogue")) orelse return false;
    if (!std.mem.eql(u8, epilogue, @tagName(artifact.epilogue))) return false;
    const fingerprint = jsonU64(case_value.object.get("generated_source_fingerprint")) orelse return false;
    if (fingerprint != std.hash.Wyhash.hash(0, check.source)) return false;
    const threads_per_threadgroup = jsonUsize(case_value.object.get("threads_per_threadgroup")) orelse return false;
    if (threads_per_threadgroup != quant_kernel_compiler.metalGeneratedThreadsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue)) return false;
    const cols_per_threadgroup = jsonUsize(case_value.object.get("cols_per_threadgroup")) orelse return false;
    if (cols_per_threadgroup != quant_kernel_compiler.metalGeneratedColsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue)) return false;

    const dispatch = dispatchForRowBucket(artifact.row_bucket) orelse return false;
    const lowering = quant_kernel_compiler.registryLoweringFor(artifact.backend, artifact.format, artifact.row_bucket, artifact.epilogue, dispatch);
    const plan_id = quant_kernel_compiler.planIdName(allocator, lowering.plan_id) catch return false;
    defer allocator.free(plan_id);

    const actual_plan_id = jsonString(case_value.object.get("plan_id")) orelse return false;
    if (!std.mem.eql(u8, actual_plan_id, plan_id)) return false;
    const production_route = jsonString(case_value.object.get("production_route")) orelse return false;
    if (!std.mem.eql(u8, production_route, quant_kernel_compiler.loweringRouteName(lowering.production_route))) return false;
    const candidate_route = jsonString(case_value.object.get("candidate_route")) orelse return false;
    if (!std.mem.eql(u8, candidate_route, quant_kernel_compiler.loweringRouteName(lowering.candidate_route))) return false;
    const production_kernel_id = jsonString(case_value.object.get("production_kernel_id")) orelse return false;
    if (!std.mem.eql(u8, production_kernel_id, lowering.production_kernel_id)) return false;
    const candidate_kernel_id = jsonString(case_value.object.get("candidate_kernel_id")) orelse return false;
    if (!std.mem.eql(u8, candidate_kernel_id, lowering.kernel_id)) return false;
    const route_fallback_reason = jsonString(case_value.object.get("route_fallback_reason")) orelse return false;
    return std.mem.eql(u8, route_fallback_reason, quant_kernel_compiler.fallbackReasonName(lowering.fallback_reason));
}

fn metalRuntimeCheckForArtifact(artifact: quant_kernel_compiler.GeneratedMatmulArtifact) ?CheckCase {
    for (metal_runtime_checks) |check| {
        if (std.mem.eql(u8, check.kernel_name, artifact.kernel_id) and
            check.format == artifact.format and
            quant_matmul.rowBucket(check.rows) == artifact.row_bucket and
            check.epilogue == artifact.epilogue)
        {
            return check;
        }
    }
    return null;
}

fn evidenceCaseHasValidError(case_value: std.json.Value) bool {
    if (case_value != .object) return false;
    const max_error = jsonF64(case_value.object.get("max_abs_error")) orelse return false;
    const tolerance = jsonF64(case_value.object.get("tolerance_abs")) orelse return false;
    return std.math.isFinite(max_error) and std.math.isFinite(tolerance) and tolerance > 0.0 and max_error <= tolerance;
}

fn evidenceCaseMatchesCheck(
    allocator: std.mem.Allocator,
    case_value: std.json.Value,
    check: CheckCase,
    route_kernel: ?[]const u8,
    promoted_kernel: ?[]const u8,
    runtime_route_all: bool,
    production_regression_check: bool,
) bool {
    if (case_value != .object) return false;
    const name = jsonString(case_value.object.get("name")) orelse return false;
    if (!std.mem.eql(u8, name, check.name)) return false;
    const kernel_id = jsonString(case_value.object.get("kernel_id")) orelse return false;
    if (!std.mem.eql(u8, kernel_id, check.kernel_name)) return false;
    const lowering = loweringForCheck(check);
    const expected_plan_id = quant_kernel_compiler.planIdName(allocator, lowering.plan_id) catch return false;
    defer allocator.free(expected_plan_id);
    const plan_id = jsonString(case_value.object.get("plan_id")) orelse return false;
    if (!std.mem.eql(u8, plan_id, expected_plan_id)) return false;
    const production_route = jsonString(case_value.object.get("production_route")) orelse return false;
    if (!std.mem.eql(u8, production_route, quant_kernel_compiler.loweringRouteName(lowering.production_route))) return false;
    const candidate_route = jsonString(case_value.object.get("candidate_route")) orelse return false;
    if (!std.mem.eql(u8, candidate_route, quant_kernel_compiler.loweringRouteName(lowering.candidate_route))) return false;
    const production_kernel_id = jsonString(case_value.object.get("production_kernel_id")) orelse return false;
    if (!std.mem.eql(u8, production_kernel_id, lowering.production_kernel_id)) return false;
    const candidate_kernel_id = jsonString(case_value.object.get("candidate_kernel_id")) orelse return false;
    if (!std.mem.eql(u8, candidate_kernel_id, lowering.kernel_id)) return false;
    const route_fallback_reason = jsonString(case_value.object.get("route_fallback_reason")) orelse return false;
    if (!std.mem.eql(u8, route_fallback_reason, quant_kernel_compiler.fallbackReasonName(lowering.fallback_reason))) return false;
    const source_path = jsonString(case_value.object.get("generated_source_path")) orelse return false;
    if (!std.mem.eql(u8, source_path, generatedSourcePathFor(check))) return false;
    const check_command = jsonString(case_value.object.get("metal_check_command")) orelse return false;
    if (!std.mem.eql(u8, check_command, metalCheckCommandFor(check))) return false;
    const source_fingerprint = jsonU64(case_value.object.get("generated_source_fingerprint")) orelse return false;
    if (source_fingerprint != std.hash.Wyhash.hash(0, check.source)) return false;
    const format = jsonString(case_value.object.get("format")) orelse return false;
    if (!std.mem.eql(u8, format, @tagName(check.format))) return false;
    const epilogue = jsonString(case_value.object.get("epilogue")) orelse return false;
    if (!std.mem.eql(u8, epilogue, @tagName(check.epilogue))) return false;
    const row_bucket = jsonString(case_value.object.get("row_bucket")) orelse return false;
    if (!std.mem.eql(u8, row_bucket, rowBucketName(check.rows))) return false;
    const rows = jsonUsize(case_value.object.get("rows")) orelse return false;
    if (rows != check.rows) return false;
    const in_dim = jsonUsize(case_value.object.get("in_dim")) orelse return false;
    if (in_dim != check.in_dim) return false;
    const out_dim = jsonUsize(case_value.object.get("out_dim")) orelse return false;
    if (out_dim != check.out_dim) return false;
    const threads_per_threadgroup = jsonUsize(case_value.object.get("threads_per_threadgroup")) orelse return false;
    if (threads_per_threadgroup != check.threads_per_threadgroup) return false;
    const cols_per_threadgroup = jsonUsize(case_value.object.get("cols_per_threadgroup")) orelse return false;
    if (cols_per_threadgroup != check.cols_per_threadgroup) return false;
    const warmup_iters = jsonUsize(case_value.object.get("warmup_iters")) orelse return false;
    if (warmup_iters != check.warmup_iters) return false;
    const measure_iters = jsonUsize(case_value.object.get("measure_iters")) orelse return false;
    if (measure_iters != check.measure_iters) return false;
    const tolerance = jsonF64(case_value.object.get("tolerance_abs")) orelse return false;
    if (!approximately(tolerance, @as(f64, @floatCast(check.tolerance)), 0.0000001)) return false;
    const selected_for_route = if (runtime_route_all)
        runtimeRouteAllSupported(check)
    else if (production_regression_check)
        productionMetalRuntimeCheck(check)
    else
        route_kernel != null and std.mem.eql(u8, route_kernel.?, check.kernel_name);
    const selected_for_promotion = (promoted_kernel != null and std.mem.eql(u8, promoted_kernel.?, check.kernel_name)) or
        (production_regression_check and productionMetalRuntimeCheck(check));
    const provider_route_checked = jsonBoolValue(case_value.object.get("provider_route_checked")) orelse return false;
    const provider_route_expected = providerRouteSupported(check) or ((selected_for_promotion or selected_for_route) and providerCandidateRouteSupported(check));
    if (provider_route_checked != provider_route_expected) return false;
    const generated_route_checked = jsonBoolValue(case_value.object.get("generated_route_checked")) orelse return false;
    const standalone_generated_checked = jsonBoolValue(case_value.object.get("standalone_generated_checked")) orelse return false;
    if (!standalone_generated_checked) return false;
    const decode_runtime_route_checked = jsonBoolValue(case_value.object.get("decode_runtime_route_checked")) orelse return false;
    if (decode_runtime_route_checked != generated_route_checked) return false;
    const generated_route_expected = generatedRouteSupported(check) or (selected_for_route and enableEnvForGeneratedCandidateRoute(check) != null);
    if (generated_route_checked != generated_route_expected) return false;
    const generated_timing_route = jsonString(case_value.object.get("generated_timing_route")) orelse return false;
    const expected_timing_route = if ((selected_for_route or selected_for_promotion) and generated_route_expected)
        "decode_runtime_generated"
    else
        "standalone_generated";
    if (!std.mem.eql(u8, generated_timing_route, expected_timing_route)) return false;
    const generated_timing_scope = jsonString(case_value.object.get("generated_timing_scope")) orelse return false;
    const expected_timing_scope = if ((selected_for_route or selected_for_promotion) and generated_route_expected)
        "decode_runtime_active_frame_batch"
    else
        "standalone_command_buffer";
    if (!std.mem.eql(u8, generated_timing_scope, expected_timing_scope)) return false;
    const baseline_supported = jsonBoolValue(case_value.object.get("handwritten_baseline_supported")) orelse return false;
    if (baseline_supported != handwrittenBaselineSupported(check)) return false;
    if (!baseline_supported) {
        const fallback_reason = jsonString(case_value.object.get("fallback_reason")) orelse return false;
        if (!std.mem.eql(u8, fallback_reason, handwrittenBaselineFallbackReason(check))) return false;
    }
    return true;
}

fn evidenceCaseHasTiming(case_value: std.json.Value) bool {
    if (case_value != .object) return false;
    const measure_iters = jsonUsize(case_value.object.get("measure_iters")) orelse return false;
    if (measure_iters == 0) return false;
    const generated_ns = jsonU64(case_value.object.get("generated_ns")) orelse return false;
    if (generated_ns == 0) return false;
    const handwritten_supported = jsonBoolValue(case_value.object.get("handwritten_baseline_supported")) orelse return false;
    if (!handwritten_supported) return true;
    const handwritten_ns = jsonU64(case_value.object.get("handwritten_ns")) orelse return false;
    return handwritten_ns != 0;
}

fn evidenceCaseHasConsistentBenchmarkMath(case_value: std.json.Value) bool {
    if (case_value != .object) return false;
    const measure_iters = jsonUsize(case_value.object.get("measure_iters")) orelse return false;
    if (measure_iters == 0) return false;
    const generated_ns = jsonU64(case_value.object.get("generated_ns")) orelse return false;
    const generated_avg_us = jsonF64(case_value.object.get("generated_avg_us")) orelse return false;
    if (!approximately(generated_avg_us, averageUsFromUsize(generated_ns, measure_iters), 0.0005)) return false;

    const benchmark_passed = jsonBoolValue(case_value.object.get("benchmark_passed")) orelse return false;
    const handwritten_supported = jsonBoolValue(case_value.object.get("handwritten_baseline_supported")) orelse return false;
    if (!handwritten_supported) return !benchmark_passed;

    const handwritten_ns = jsonU64(case_value.object.get("handwritten_ns")) orelse return false;
    const handwritten_avg_us = jsonF64(case_value.object.get("handwritten_avg_us")) orelse return false;
    if (!approximately(handwritten_avg_us, averageUsFromUsize(handwritten_ns, measure_iters), 0.0005)) return false;
    const measured_speedup = jsonF64(case_value.object.get("measured_speedup")) orelse return false;
    const expected_speedup = speedup(handwritten_ns, generated_ns);
    if (!approximately(measured_speedup, expected_speedup, 0.000001)) return false;
    const minimum_repeat_speedup = jsonF64(case_value.object.get("minimum_repeat_speedup")) orelse return false;
    const repeat_runs = jsonUsize(case_value.object.get("repeat_runs")) orelse return false;
    if (repeat_runs == 1 and !approximately(minimum_repeat_speedup, expected_speedup, 0.000001)) return false;
    if (!evidenceCaseHasConsistentRepeatTimings(case_value, repeat_runs, generated_ns, handwritten_ns, minimum_repeat_speedup)) return false;
    return benchmark_passed == std.mem.eql(u8, quant_kernel_compiler.metalPromotionSpeedupBlocker(measured_speedup, minimum_repeat_speedup), quant_kernel_compiler.metal_blocker_none);
}

fn evidenceCaseHasConsistentRepeatTimings(
    case_value: std.json.Value,
    repeat_runs: usize,
    generated_ns: u64,
    handwritten_ns: u64,
    minimum_repeat_speedup: f64,
) bool {
    const generated_value = case_value.object.get("repeat_generated_ns") orelse return repeat_runs == 1;
    var generated_values: [max_evidence_repeat_runs]u64 = undefined;
    const generated = jsonU64Array(generated_value, &generated_values) orelse return false;
    if (generated.len != repeat_runs) return false;
    if (medianU64Const(generated) != generated_ns) return false;

    var handwritten_values: [max_evidence_repeat_runs]u64 = undefined;
    const handwritten_value = case_value.object.get("repeat_handwritten_ns") orelse return false;
    const handwritten = jsonU64Array(handwritten_value, &handwritten_values) orelse return false;
    if (handwritten.len != repeat_runs) return false;
    if (medianU64Const(handwritten) != handwritten_ns) return false;

    var speedup_values: [max_evidence_repeat_runs]f64 = undefined;
    const speedups_value = case_value.object.get("repeat_speedups") orelse return false;
    const speedups = jsonF64Array(speedups_value, &speedup_values) orelse return false;
    if (speedups.len != repeat_runs) return false;

    var minimum_index: usize = 0;
    for (speedups, generated, handwritten, 0..) |actual_speedup, generated_elapsed, handwritten_elapsed, i| {
        const expected_speedup = speedup(handwritten_elapsed, generated_elapsed);
        if (!approximately(actual_speedup, expected_speedup, 0.000001)) return false;
        if (actual_speedup < speedups[minimum_index]) minimum_index = i;
    }
    const recorded_minimum_index = jsonUsize(case_value.object.get("minimum_repeat_index")) orelse return false;
    if (recorded_minimum_index >= speedups.len) return false;
    if (!approximately(speedups[recorded_minimum_index], speedups[minimum_index], 0.000001)) return false;

    const gate_index = jsonUsize(case_value.object.get("repeat_gate_index")) orelse return false;
    if (gate_index >= speedups.len) return false;
    const gate_speedup = repeatGateSpeedup(speedups);
    if (!approximately(speedups[gate_index], gate_speedup, 0.000001)) return false;
    return approximately(speedups[gate_index], minimum_repeat_speedup, 0.000001);
}

fn evidenceCaseHasConsistentPromotionBlocker(case_value: std.json.Value) bool {
    if (case_value != .object) return false;
    const promotion_ready = jsonBoolValue(case_value.object.get("promotion_ready")) orelse return false;
    const blocker = jsonString(case_value.object.get("promotion_blocker")) orelse return false;
    const benchmark_passed = jsonBoolValue(case_value.object.get("benchmark_passed")) orelse return false;
    if (promotion_ready) return benchmark_passed and blocker.len == 0;

    const handwritten_supported = jsonBoolValue(case_value.object.get("handwritten_baseline_supported")) orelse return false;
    if (!handwritten_supported) return std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_unsupported_handwritten);

    if (!benchmark_passed) {
        const measured_speedup = jsonF64(case_value.object.get("measured_speedup")) orelse return false;
        const minimum_repeat_speedup = jsonF64(case_value.object.get("minimum_repeat_speedup")) orelse return false;
        return std.mem.eql(u8, blocker, quant_kernel_compiler.metalPromotionSpeedupBlocker(measured_speedup, minimum_repeat_speedup));
    }
    if (blocker.len == 0) {
        const production_route = jsonString(case_value.object.get("production_route")) orelse return false;
        if (!std.mem.eql(u8, production_route, quant_kernel_compiler.loweringRouteName(.generated_production))) return false;
        const kernel_id = jsonString(case_value.object.get("kernel_id")) orelse return false;
        const generated_route_checked = jsonBoolValue(case_value.object.get("generated_route_checked")) orelse return false;
        const provider_route_checked = jsonBoolValue(case_value.object.get("provider_route_checked")) orelse return false;
        return generated_route_checked and (!quant_kernel_compiler.metalProviderRouteRequiredForKernel(kernel_id) or provider_route_checked);
    }
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_missing_generated_route)) return true;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_missing_provider_route)) return true;
    if (std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_runtime_route_only)) {
        return jsonBoolValue(case_value.object.get("generated_route_checked")) orelse false;
    }

    return std.mem.eql(u8, blocker, quant_kernel_compiler.metal_blocker_dev_only_candidate);
}

fn averageUsFromUsize(elapsed_nanos: u64, measure_iters: usize) f64 {
    if (measure_iters == 0) return 0;
    return @as(f64, @floatFromInt(elapsed_nanos)) / @as(f64, @floatFromInt(measure_iters)) / 1000.0;
}

fn approximately(actual: f64, expected: f64, tolerance: f64) bool {
    return std.math.isFinite(actual) and std.math.isFinite(expected) and @abs(actual - expected) <= tolerance;
}

fn nullableF64Approximately(actual: ?f64, expected: ?f64, tolerance: f64) bool {
    if (actual == null or expected == null) return actual == null and expected == null;
    return approximately(actual.?, expected.?, tolerance);
}

fn nullableStringEql(actual: ?[]const u8, expected: ?[]const u8) bool {
    if (actual == null or expected == null) return actual == null and expected == null;
    return std.mem.eql(u8, actual.?, expected.?);
}

fn jsonString(value: ?std.json.Value) ?[]const u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .string => |text| text,
        else => null,
    };
}

fn jsonBoolValue(value: ?std.json.Value) ?bool {
    const actual = value orelse return null;
    return switch (actual) {
        .bool => |val| val,
        else => null,
    };
}

fn jsonUsize(value: ?std.json.Value) ?usize {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |val| if (val >= 0) @intCast(val) else null,
        else => null,
    };
}

fn jsonU64(value: ?std.json.Value) ?u64 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |val| if (val >= 0) @intCast(val) else null,
        .number_string => |text| std.fmt.parseUnsigned(u64, text, 10) catch null,
        else => null,
    };
}

fn jsonF64(value: ?std.json.Value) ?f64 {
    const actual = value orelse return null;
    return switch (actual) {
        .float => |val| val,
        .integer => |val| @floatFromInt(val),
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
}

fn jsonNullableF64(value: std.json.Value, out: *?f64) bool {
    switch (value) {
        .null => {
            out.* = null;
            return true;
        },
        else => {
            out.* = jsonF64(value) orelse return false;
            return true;
        },
    }
}

fn jsonNullableString(value: std.json.Value, out: *?[]const u8) bool {
    switch (value) {
        .null => {
            out.* = null;
            return true;
        },
        .string => |text| {
            out.* = text;
            return true;
        },
        else => return false,
    }
}

fn jsonU64Array(value: std.json.Value, out: *[max_evidence_repeat_runs]u64) ?[]u64 {
    if (value != .array or value.array.items.len == 0 or value.array.items.len > out.len) return null;
    for (value.array.items, 0..) |item, i| {
        out[i] = jsonU64(item) orelse return null;
    }
    return out[0..value.array.items.len];
}

fn jsonF64Array(value: std.json.Value, out: *[max_evidence_repeat_runs]f64) ?[]f64 {
    if (value != .array or value.array.items.len == 0 or value.array.items.len > out.len) return null;
    for (value.array.items, 0..) |item, i| {
        out[i] = jsonF64(item) orelse return null;
    }
    return out[0..value.array.items.len];
}

fn epilogueNeedsBias(epilogue: quant_kernel_compiler.Epilogue) bool {
    return epilogue == .bias or epilogue == .bias_gelu;
}

fn generatedSourcePathFor(check: CheckCase) []const u8 {
    const artifact = metalArtifactForCheck(check) orelse return "";
    if (quant_kernel_compiler.artifactHasPromotionEvidence(artifact)) return quant_kernel_compiler.generatedMetalSourcePathForKernel(check.kernel_name) orelse "";
    return artifact.source_path;
}

fn metalArtifactForCheck(check: CheckCase) ?quant_kernel_compiler.GeneratedMatmulArtifact {
    const artifact = quant_kernel_compiler.generatedArtifactForCandidate(.metal, check.format, quant_matmul.rowBucket(check.rows), check.epilogue) orelse return null;
    return if (std.mem.eql(u8, artifact.kernel_id, check.kernel_name)) artifact else null;
}

fn metalCheckCommandFor(check: CheckCase) []const u8 {
    const artifact = metalArtifactForCheck(check) orelse return "";
    if (quant_kernel_compiler.artifactHasPromotionEvidence(artifact)) return quant_kernel_compiler.generatedMetalCheckCommandForKernel(check.kernel_name) orelse "";
    return artifact.check_command;
}

fn handwrittenBaselineFallbackReason(check: CheckCase) []const u8 {
    if (handwrittenBaselineSupported(check)) return "none";
    if (check.epilogue != .none) return "unsupported_epilogue";
    if (metalFormatFor(check.format) == null) return "unsupported_format";
    return "unavailable";
}

fn isQuantBiasEpilogue(check: CheckCase) bool {
    return (check.format == .q8_0 or check.format == .q4_k or check.format == .q5_k or check.format == .q6_k) and
        (check.epilogue == .bias or check.epilogue == .bias_gelu) and
        check.rows >= 2 and check.rows <= 8;
}

fn handwrittenBaselineSupported(check: CheckCase) bool {
    if (isQuantBiasEpilogue(check)) return true;
    return check.epilogue == .none and metalFormatFor(check.format) != null;
}

fn handwrittenBaselineName(check: CheckCase) []const u8 {
    if (check.format == .q8_0 and check.epilogue == .bias) return "termite_metal_decode_runtime_q8_0_linear_then_bias";
    if (check.format == .q8_0 and check.epilogue == .bias_gelu) return "termite_metal_decode_runtime_q8_0_linear_then_bias_gelu";
    if (check.format == .q4_k and check.epilogue == .bias) return "termite_metal_decode_runtime_q4_k_linear_then_bias";
    if (check.format == .q4_k and check.epilogue == .bias_gelu) return "termite_metal_decode_runtime_q4_k_linear_then_bias_gelu";
    if (check.format == .q5_k and check.epilogue == .bias) return "termite_metal_decode_runtime_q5_k_linear_then_bias";
    if (check.format == .q5_k and check.epilogue == .bias_gelu) return "termite_metal_decode_runtime_q5_k_linear_then_bias_gelu";
    if (check.format == .q6_k and check.epilogue == .bias) return "termite_metal_decode_runtime_q6_k_linear_then_bias";
    if (check.format == .q6_k and check.epilogue == .bias_gelu) return "termite_metal_decode_runtime_q6_k_linear_then_bias_gelu";
    return "termite_metal_decode_runtime_apply_quantized_linear_slot_device";
}

test "quant kernel metal runtime check exposes quant bias split baselines" {
    var found_q8_bias = false;
    var found_q8_bias_gelu = false;
    var found_q4_bias = false;
    var found_q4_bias_gelu = false;
    var found_q5_bias = false;
    var found_q5_bias_gelu = false;
    var found_q6_bias = false;
    var found_q6_bias_gelu = false;
    for (metal_runtime_checks) |check| {
        if (check.format == .q8_0 and check.epilogue == .bias) {
            found_q8_bias = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q8_0_linear_then_bias",
                handwrittenBaselineName(check),
            );
        } else if (check.format == .q8_0 and check.epilogue == .bias_gelu) {
            found_q8_bias_gelu = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q8_0_linear_then_bias_gelu",
                handwrittenBaselineName(check),
            );
        } else if (check.format == .q4_k and check.epilogue == .bias) {
            found_q4_bias = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q4_k_linear_then_bias",
                handwrittenBaselineName(check),
            );
        } else if (check.format == .q4_k and check.epilogue == .bias_gelu) {
            found_q4_bias_gelu = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q4_k_linear_then_bias_gelu",
                handwrittenBaselineName(check),
            );
        } else if (check.format == .q5_k and check.epilogue == .bias) {
            found_q5_bias = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q5_k_linear_then_bias",
                handwrittenBaselineName(check),
            );
        } else if (check.format == .q5_k and check.epilogue == .bias_gelu) {
            found_q5_bias_gelu = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q5_k_linear_then_bias_gelu",
                handwrittenBaselineName(check),
            );
        } else if (check.format == .q6_k and check.epilogue == .bias) {
            found_q6_bias = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q6_k_linear_then_bias",
                handwrittenBaselineName(check),
            );
        } else if (check.format == .q6_k and check.epilogue == .bias_gelu) {
            found_q6_bias_gelu = true;
            try std.testing.expectEqualStrings(
                "termite_metal_decode_runtime_q6_k_linear_then_bias_gelu",
                handwrittenBaselineName(check),
            );
        } else continue;
        try std.testing.expect(handwrittenBaselineSupported(check));
        try std.testing.expectEqualStrings("none", handwrittenBaselineFallbackReason(check));
    }
    try std.testing.expect(found_q8_bias);
    try std.testing.expect(found_q8_bias_gelu);
    try std.testing.expect(found_q4_bias);
    try std.testing.expect(found_q4_bias_gelu);
    try std.testing.expect(found_q5_bias);
    try std.testing.expect(found_q5_bias_gelu);
    try std.testing.expect(found_q6_bias);
    try std.testing.expect(found_q6_bias_gelu);

    const unsupported_epilogue: CheckCase = .{
        .name = "q4_k_rows_2_8_pair",
        .source = "",
        .kernel_name = "",
        .format = .q4_k,
        .epilogue = .pair,
        .rows = 4,
        .in_dim = 512,
        .out_dim = 3,
        .threads_per_threadgroup = 64,
        .cols_per_threadgroup = 1,
        .tolerance = 0.0002,
    };
    try std.testing.expect(!handwrittenBaselineSupported(unsupported_epilogue));
    try std.testing.expectEqualStrings("unsupported_epilogue", handwrittenBaselineFallbackReason(unsupported_epilogue));
}

fn rowBucketName(rows: usize) []const u8 {
    return @tagName(quant_matmul.rowBucket(rows));
}

fn dispatchForRowBucket(row_bucket: quant_matmul.RowBucket) ?quant_matmul.DispatchKind {
    return switch (row_bucket) {
        .rows_0 => null,
        .rows_1 => .mmv,
        .rows_2_8 => .small_batch,
        .rows_9_64, .rows_65_plus => .mm,
    };
}

fn loweringForCheck(check: CheckCase) quant_kernel_compiler.QuantKernelLowering {
    const row_bucket = quant_matmul.rowBucket(check.rows);
    const dispatch = dispatchForRowBucket(row_bucket) orelse .scalar;
    return quant_kernel_compiler.registryLoweringFor(.metal, check.format, row_bucket, check.epilogue, dispatch);
}

fn averageUs(elapsed_nanos: u64, measure_iters: u32) f64 {
    if (measure_iters == 0) return 0;
    return @as(f64, @floatFromInt(elapsed_nanos)) / @as(f64, @floatFromInt(measure_iters)) / 1000.0;
}

fn speedup(baseline_nanos: u64, generated_nanos: u64) f64 {
    if (baseline_nanos == 0 or generated_nanos == 0) return 0;
    return @as(f64, @floatFromInt(baseline_nanos)) / @as(f64, @floatFromInt(generated_nanos));
}

fn jsonBool(value: bool) []const u8 {
    return if (value) "true" else "false";
}

test "quant kernel metal attestation rejects dirty source status" {
    try std.testing.expectError(error.MetalEvidenceDirtySourceTree, cleanSourceStatusSha256Alloc(std.testing.allocator, " M src/file.zig\n"));
    const clean_digest = try cleanSourceStatusSha256Alloc(std.testing.allocator, "");
    defer std.testing.allocator.free(clean_digest);
    try std.testing.expectEqualStrings(clean_source_status_sha256, clean_digest);
}

test "quant kernel metal runtime evidence records dev-only benchmark results" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "evidence.json" });
    defer std.testing.allocator.free(path);

    var results: [metal_runtime_checks.len]CheckResult = undefined;
    for (&results, metal_runtime_checks, 0..) |*result, check, i| {
        const generated_ns = 4_000_000 + i * 250_000;
        result.* = .{
            .max_error = if (i == 0) 0.0 else 0.0000001,
            .measure_iters = check.measure_iters,
            .elapsed_nanos = generated_ns,
            .generated_route_checked = generatedRouteSupported(check),
            .provider_route_checked = providerRouteSupported(check),
        };
        if (handwrittenBaselineSupported(check)) {
            result.handwritten_elapsed_nanos = if (std.mem.eql(u8, check.kernel_name, quant_kernel_compiler.first_general_metal_q5_kernel_id))
                (generated_ns * 9) / 10
            else
                (generated_ns * 5) / 4;
        }
    }

    try writeEvidence(std.testing.allocator, path, &metal_runtime_checks, &results, 1, null, null, false, false, false, null);
    const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(actual);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, actual, .{});
    defer parsed.deinit();

    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"evidence_contract\":\"antfly.quant_kernel_metal_evidence.v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"schema\":\"antfly.quant_kernel_metal_runtime_evidence.v11\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"provenance_status\":\"local_unattested\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"provenance_blocker\":\"missing_reproducible_provenance\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"source_commit\":\"\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"source_tree_clean\":false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"generated_timing_route\":\"standalone_generated\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"generated_timing_scope\":\"standalone_command_buffer\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_worst_repeat_speedup\":null"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_worst_repeat_case\":null"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"production_enabled\":false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_case_count\":0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_ready_count\":0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"repeat_runs\":1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"warmup_repeat_runs\":0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"timing_aggregation\":\"single\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"minimum_speedup\":1.020000"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"minimum_speedup_tolerance\":0.001000"));
    const expected_case_count = try std.fmt.allocPrint(std.testing.allocator, "\"case_count\":{d}", .{metal_runtime_checks.len});
    defer std.testing.allocator.free(expected_case_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, expected_case_count));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"fallback_reason\":\"unsupported_epilogue\""));
    var expected_generated_route_checked: usize = 0;
    var expected_provider_route_checked: usize = 0;
    var expected_candidate_route_ready: usize = 0;
    var expected_candidate_benchmark_ready: usize = 0;
    var expected_generated_production: usize = 0;
    var expected_handwritten_production: usize = 0;
    var expected_candidate_dev: usize = 0;
    var expected_candidate_unsupported: usize = 0;
    var expected_fallback_missing: usize = 0;
    var expected_fallback_runtime_not_wired: usize = 0;
    var expected_fallback_none: usize = 0;
    var expected_benchmark_speedups = BenchmarkSpeedupSummary{};
    for (metal_runtime_checks, results) |check, result| {
        const lowering = loweringForCheck(check);
        if (generatedRouteSupported(check)) expected_generated_route_checked += 1;
        if (providerRouteSupported(check)) expected_provider_route_checked += 1;
        const provider_route_needed = quant_kernel_compiler.metalProviderRouteRequiredForKernel(check.kernel_name);
        if (result.max_error <= check.tolerance and result.generated_route_checked and (!provider_route_needed or result.provider_route_checked)) {
            expected_candidate_route_ready += 1;
        }
        if (result.handwritten_elapsed_nanos) |handwritten_elapsed| {
            const measured_speedup = speedup(handwritten_elapsed, result.elapsed_nanos);
            const minimum_repeat_speedup = result.minimum_repeat_speedup orelse measured_speedup;
            const benchmark_passed = std.mem.eql(u8, quant_kernel_compiler.metalPromotionSpeedupBlocker(measured_speedup, minimum_repeat_speedup), quant_kernel_compiler.metal_blocker_none);
            if (benchmark_passed) {
                expected_candidate_benchmark_ready += 1;
            }
            expected_benchmark_speedups.add(check.name, measured_speedup, benchmark_passed);
        }
        switch (lowering.production_route) {
            .generated_production => expected_generated_production += 1,
            .handwritten_production => expected_handwritten_production += 1,
            else => {},
        }
        switch (lowering.candidate_route) {
            .generated_dev_candidate => expected_candidate_dev += 1,
            .unsupported => expected_candidate_unsupported += 1,
            else => {},
        }
        switch (lowering.fallback_reason) {
            .generated_artifact_missing => expected_fallback_missing += 1,
            .generated_runtime_not_wired => expected_fallback_runtime_not_wired += 1,
            .none => expected_fallback_none += 1,
            else => {},
        }
    }
    try std.testing.expectEqual(expected_generated_route_checked, std.mem.count(u8, actual, "\"generated_route_checked\":true"));
    try std.testing.expectEqual(metal_runtime_checks.len - expected_generated_route_checked, std.mem.count(u8, actual, "\"generated_route_checked\":false"));
    try std.testing.expectEqual(metal_runtime_checks.len, std.mem.count(u8, actual, "\"standalone_generated_checked\":true"));
    try std.testing.expectEqual(expected_generated_route_checked, std.mem.count(u8, actual, "\"decode_runtime_route_checked\":true"));
    try std.testing.expectEqual(metal_runtime_checks.len - expected_generated_route_checked, std.mem.count(u8, actual, "\"decode_runtime_route_checked\":false"));
    try std.testing.expectEqual(expected_provider_route_checked, std.mem.count(u8, actual, "\"provider_route_checked\":true"));
    try std.testing.expectEqual(metal_runtime_checks.len - expected_provider_route_checked, std.mem.count(u8, actual, "\"provider_route_checked\":false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"plan_id\":\"metal/q4_k/rows_2_8/bias_gelu/small_batch\""));
    try std.testing.expectEqual(expected_handwritten_production, std.mem.count(u8, actual, "\"production_route\":\"handwritten_production\""));
    try std.testing.expectEqual(expected_generated_production, std.mem.count(u8, actual, "\"production_route\":\"generated_production\""));
    try std.testing.expectEqual(expected_candidate_dev, std.mem.count(u8, actual, "\"candidate_route\":\"generated_dev_candidate\""));
    try std.testing.expectEqual(expected_candidate_unsupported, std.mem.count(u8, actual, "\"candidate_route\":\"unsupported\""));
    try std.testing.expectEqual(expected_fallback_missing, std.mem.count(u8, actual, "\"route_fallback_reason\":\"generated_artifact_missing\""));
    try std.testing.expectEqual(expected_fallback_runtime_not_wired, std.mem.count(u8, actual, "\"route_fallback_reason\":\"generated_runtime_not_wired\""));
    try std.testing.expectEqual(expected_fallback_none, std.mem.count(u8, actual, "\"route_fallback_reason\":\"none\""));
    try std.testing.expectEqual(expected_generated_route_checked, jsonUsize(parsed.value.object.get("runtime_route_checked_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(expected_provider_route_checked, jsonUsize(parsed.value.object.get("provider_route_checked_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(expected_candidate_route_ready, jsonUsize(parsed.value.object.get("candidate_route_ready_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(expected_candidate_benchmark_ready, jsonUsize(parsed.value.object.get("candidate_benchmark_ready_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expect(expected_benchmark_speedups.eql(try benchmarkSpeedupSummaryFromRoot(parsed.value)));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_supported_count\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_speedup_min_case\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_speedup_max_case\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_speedup_avg\":"));
    const expected_speedup_blockers = std.mem.count(u8, actual, "\"promotion_blocker\":\"speedup_gate_missing\"");
    const expected_unstable_blockers = std.mem.count(u8, actual, "\"promotion_blocker\":\"unstable_benchmark_timing\"");
    try std.testing.expectEqual(expected_speedup_blockers, jsonUsize(parsed.value.object.get("promotion_blocker_speedup_gate_missing_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(expected_unstable_blockers, jsonUsize(parsed.value.object.get("promotion_blocker_unstable_benchmark_timing_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(std.mem.count(u8, actual, "\"promotion_blocker\":\"runtime_route_only\""), jsonUsize(parsed.value.object.get("promotion_blocker_runtime_route_only_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(std.mem.count(u8, actual, "\"promotion_blocker\":\"missing_metal_generated_route_evidence\""), jsonUsize(parsed.value.object.get("promotion_blocker_missing_generated_route_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(std.mem.count(u8, actual, "\"promotion_blocker\":\"missing_metal_provider_route_evidence\""), jsonUsize(parsed.value.object.get("promotion_blocker_missing_provider_route_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(std.mem.count(u8, actual, "\"promotion_blocker\":\"unsupported_handwritten_baseline\""), jsonUsize(parsed.value.object.get("promotion_blocker_unsupported_handwritten_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(std.mem.count(u8, actual, "\"promotion_blocker\":\"dev_only_candidate\""), jsonUsize(parsed.value.object.get("promotion_blocker_dev_only_candidate_count")) orelse return error.InvalidMetalEvidence);
    const expected_slow_fallbacks = expected_speedup_blockers + expected_unstable_blockers;
    try std.testing.expectEqual(expected_slow_fallbacks, jsonUsize(parsed.value.object.get("slow_fallback_count")) orelse return error.InvalidMetalEvidence);
    const expected_top_slow_reason = if (expected_slow_fallbacks == 0)
        quant_kernel_compiler.metal_blocker_none
    else if (expected_speedup_blockers >= expected_unstable_blockers)
        quant_kernel_compiler.metal_blocker_speedup_gate_missing
    else
        quant_kernel_compiler.metal_blocker_unstable_benchmark_timing;
    const expected_top_slow_count = if (expected_slow_fallbacks == 0) 0 else @max(expected_speedup_blockers, expected_unstable_blockers);
    try std.testing.expectEqualStrings(expected_top_slow_reason, jsonString(parsed.value.object.get("top_slow_fallback_reason")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(expected_top_slow_count, jsonUsize(parsed.value.object.get("top_slow_fallback_count")) orelse return error.InvalidMetalEvidence);
    const summary = try evidenceSummaryFromJson(std.testing.allocator, actual);
    try std.testing.expectEqual(metal_runtime_checks.len, summary.case_count);
    try std.testing.expectEqual(expected_generated_route_checked, summary.runtime_route_checked_count);
    try std.testing.expectEqual(expected_provider_route_checked, summary.provider_route_checked_count);
    try std.testing.expectEqual(expected_candidate_route_ready, summary.candidate_route_ready_count);
    try std.testing.expectEqual(expected_candidate_benchmark_ready, summary.candidate_benchmark_ready_count);
    for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        const promotion_ready = quant_kernel_compiler.artifactHasPromotionEvidence(artifact);
        const source_path = if (promotion_ready)
            quant_kernel_compiler.generatedMetalSourcePathForKernel(artifact.kernel_id) orelse artifact.source_path
        else
            artifact.source_path;
        const check_command = if (promotion_ready)
            quant_kernel_compiler.generatedMetalCheckCommandForKernel(artifact.kernel_id) orelse artifact.check_command
        else
            artifact.check_command;
        try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, artifact.kernel_id));
        try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, source_path));
        try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, check_command));
    }
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_passed\":true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_ready\":false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_blocker\":\"dev_only_candidate\""));

    const route_kernel = quant_kernel_compiler.first_general_metal_q8_kernel_id;
    var route_checks: [metal_runtime_checks.len]CheckCase = undefined;
    var route_results: [metal_runtime_checks.len]CheckResult = undefined;
    var route_count: usize = 0;
    for (metal_runtime_checks, results) |check, result| {
        if (!std.mem.eql(u8, check.kernel_name, route_kernel)) continue;
        route_checks[route_count] = check;
        route_results[route_count] = result;
        route_results[route_count].generated_route_checked = true;
        route_results[route_count].generated_timing_route = .decode_runtime_generated;
        route_results[route_count].provider_route_checked = true;
        route_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), route_count);

    const route_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "q8-route-evidence.json" });
    defer std.testing.allocator.free(route_path);
    try writeEvidence(std.testing.allocator, route_path, route_checks[0..route_count], route_results[0..route_count], 1, null, route_kernel, false, false, false, null);
    const route_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, route_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(route_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, route_actual, 1, "\"runtime_route_kernel\":\"" ++ route_kernel ++ "\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, route_actual, 1, "--runtime-route-kernel " ++ route_kernel));
    try std.testing.expect(std.mem.containsAtLeast(u8, route_actual, 1, "\"generated_route_checked\":true"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, route_actual, "\"production_route\":\"generated_production\""));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, route_actual, "\"promotion_blocker\":\"\""));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, route_actual, "\"promotion_blocker\":\"runtime_route_only\""));
    try checkEvidenceFile(std.testing.allocator, route_path, false, false, null);
    try std.testing.expectError(error.MetalEvidenceReproducibleProvenanceMissing, checkEvidenceFile(std.testing.allocator, route_path, true, false, route_kernel));

    var route_all_checks: [metal_runtime_checks.len]CheckCase = undefined;
    var route_all_results: [metal_runtime_checks.len]CheckResult = undefined;
    var route_all_count: usize = 0;
    var route_all_provider_count: usize = 0;
    for (metal_runtime_checks, results) |check, result| {
        if (!runtimeRouteAllSupported(check)) continue;
        route_all_checks[route_all_count] = check;
        route_all_results[route_all_count] = result;
        route_all_results[route_all_count].generated_route_checked = true;
        route_all_results[route_all_count].generated_timing_route = .decode_runtime_generated;
        route_all_results[route_all_count].provider_route_checked = providerRouteSupported(check) or providerCandidateRouteSupported(check);
        if (route_all_results[route_all_count].provider_route_checked) route_all_provider_count += 1;
        route_all_count += 1;
    }
    try std.testing.expect(route_all_count > route_count);

    const route_all_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "all-route-evidence.json" });
    defer std.testing.allocator.free(route_all_path);
    try writeEvidence(std.testing.allocator, route_all_path, route_all_checks[0..route_all_count], route_all_results[0..route_all_count], 1, null, null, true, false, false, null);
    const route_all_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, route_all_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(route_all_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, route_all_actual, 1, "\"runtime_route_kernel\":null"));
    try std.testing.expect(std.mem.containsAtLeast(u8, route_all_actual, 1, "\"runtime_route_all\":true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, route_all_actual, 1, "\"runtime_route_all_status\":\"runtime_route_all_ok\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, route_all_actual, 1, "--runtime-route-all"));
    try std.testing.expectEqual(quant_kernel_compiler.first_metal_runtime_route_all_expected_case_count, route_all_count);
    try std.testing.expectEqual(quant_kernel_compiler.first_metal_runtime_route_all_expected_provider_route_count, route_all_provider_count);
    try std.testing.expectEqual(route_all_count, std.mem.count(u8, route_all_actual, "\"generated_route_checked\":true"));
    try std.testing.expectEqual(route_all_provider_count, std.mem.count(u8, route_all_actual, "\"provider_route_checked\":true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, route_all_actual, 1, "\"promotion_blocker\":\"runtime_route_only\""));
    const route_all_generated_production_count = std.mem.count(u8, route_all_actual, "\"production_route\":\"generated_production\"");
    try std.testing.expect(route_all_generated_production_count > 0);
    const route_all_generated_production_ready_count = std.mem.count(u8, route_all_actual, "\"promotion_blocker\":\"\"");
    try std.testing.expect(route_all_generated_production_ready_count > 0);
    try std.testing.expect(route_all_generated_production_ready_count <= route_all_generated_production_count);
    var parsed_route_all = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, route_all_actual, .{});
    defer parsed_route_all.deinit();
    try std.testing.expectEqual(route_all_count, jsonUsize(parsed_route_all.value.object.get("runtime_route_checked_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(route_all_provider_count, jsonUsize(parsed_route_all.value.object.get("provider_route_checked_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(route_all_count, jsonUsize(parsed_route_all.value.object.get("candidate_route_ready_count")) orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(std.mem.count(u8, route_all_actual, "\"promotion_blocker\":\"runtime_route_only\""), jsonUsize(parsed_route_all.value.object.get("promotion_blocker_runtime_route_only_count")) orelse return error.InvalidMetalEvidence);
    try checkEvidenceFile(std.testing.allocator, route_all_path, false, true, null);
    try std.testing.expectError(error.MetalEvidenceRuntimeRouteAllMissing, checkEvidenceFile(std.testing.allocator, route_path, false, true, null));
    const stale_route_all = try replaceOnce(std.testing.allocator, route_all_actual, "\"runtime_route_all\":true", "\"runtime_route_all\":false");
    defer std.testing.allocator.free(stale_route_all);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_route_all, false, false, null));
    const route_all_checked_count = try std.fmt.allocPrint(std.testing.allocator, "\"runtime_route_checked_count\":{d}", .{route_all_count});
    defer std.testing.allocator.free(route_all_checked_count);
    const stale_route_all_checked_count = try std.fmt.allocPrint(std.testing.allocator, "\"runtime_route_checked_count\":{d}", .{route_all_count - 1});
    defer std.testing.allocator.free(stale_route_all_checked_count);
    const stale_route_all_summary = try replaceOnce(std.testing.allocator, route_all_actual, route_all_checked_count, stale_route_all_checked_count);
    defer std.testing.allocator.free(stale_route_all_summary);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_route_all_summary, false, true, null));
    const route_all_provider_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"provider_route_checked_count\":{d}", .{route_all_provider_count});
    defer std.testing.allocator.free(route_all_provider_count_field);
    const stale_route_all_provider_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"provider_route_checked_count\":{d}", .{route_all_provider_count - 1});
    defer std.testing.allocator.free(stale_route_all_provider_count_field);
    const stale_route_all_provider_summary = try replaceOnce(std.testing.allocator, route_all_actual, route_all_provider_count_field, stale_route_all_provider_count_field);
    defer std.testing.allocator.free(stale_route_all_provider_summary);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_route_all_provider_summary, false, true, null));
    const route_all_route_ready_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"candidate_route_ready_count\":{d}", .{route_all_count});
    defer std.testing.allocator.free(route_all_route_ready_count_field);
    const stale_route_all_route_ready_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"candidate_route_ready_count\":{d}", .{route_all_count - 1});
    defer std.testing.allocator.free(stale_route_all_route_ready_count_field);
    const stale_route_all_route_ready_summary = try replaceOnce(std.testing.allocator, route_all_actual, route_all_route_ready_count_field, stale_route_all_route_ready_count_field);
    defer std.testing.allocator.free(stale_route_all_route_ready_summary);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_route_all_route_ready_summary, false, true, null));
    const stale_route_all_status = try replaceOnce(std.testing.allocator, route_all_actual, "\"runtime_route_all_status\":\"runtime_route_all_ok\"", "\"runtime_route_all_status\":\"runtime_route_all_blocked\"");
    defer std.testing.allocator.free(stale_route_all_status);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_route_all_status, false, true, null));
    const slow_fallback_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"slow_fallback_count\":{d}", .{jsonUsize(parsed_route_all.value.object.get("slow_fallback_count")) orelse return error.InvalidMetalEvidence});
    defer std.testing.allocator.free(slow_fallback_count_field);
    const stale_slow_fallback_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"slow_fallback_count\":{d}", .{(jsonUsize(parsed_route_all.value.object.get("slow_fallback_count")) orelse return error.InvalidMetalEvidence) + 1});
    defer std.testing.allocator.free(stale_slow_fallback_count_field);
    const stale_slow_fallback_summary = try replaceOnce(std.testing.allocator, route_all_actual, slow_fallback_count_field, stale_slow_fallback_count_field);
    defer std.testing.allocator.free(stale_slow_fallback_summary);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_slow_fallback_summary, false, true, null));

    const promoted_artifact = blk: {
        for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
            if (artifact.backend == .metal and artifact.production_enabled) break :blk artifact;
        }
        return error.MissingPromotedMetalArtifact;
    };
    const cases_value = parsed.value.object.get("cases").?;
    var found_promoted_artifact_case = false;
    for (cases_value.array.items) |case_value| {
        if (evidenceCaseMatchesArtifact(std.testing.allocator, case_value, promoted_artifact)) {
            found_promoted_artifact_case = true;
            break;
        }
    }
    try std.testing.expect(found_promoted_artifact_case);

    try checkEvidenceFile(std.testing.allocator, path, false, false, null);
    try checkEvidenceFile(std.testing.allocator, path, false, false, quant_kernel_compiler.first_general_metal_q4_0_kernel_id);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceFile(std.testing.allocator, path, false, false, "missing_kernel"));
    try std.testing.expectError(error.MetalEvidenceReproducibleProvenanceMissing, checkEvidenceFile(std.testing.allocator, path, true, false, null));

    const copied_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "copied-evidence.json" });
    defer std.testing.allocator.free(copied_path);
    try writeFileCreatingParent(std.testing.io, copied_path, actual);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceFile(std.testing.allocator, copied_path, false, false, null));
    try std.testing.expect(try commandEvidenceOutMatches(std.testing.allocator, "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out /tmp/a --repeat-runs 3", "/tmp/a"));
    try std.testing.expect(!try commandEvidenceOutMatches(std.testing.allocator, "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out /tmp/abc --repeat-runs 3", "/tmp/a"));
    const absolute_path = try compat.cwd().realPathFileAlloc(std.testing.io, path, std.testing.allocator);
    defer std.testing.allocator.free(absolute_path);
    try checkEvidenceJsonCommandPath(std.testing.allocator, actual, absolute_path);

    const production_enabled = try replaceOnce(std.testing.allocator, actual, "\"production_enabled\":false", "\"production_enabled\":true");
    defer std.testing.allocator.free(production_enabled);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, production_enabled, false, false, null));
    try std.testing.expectError(error.MetalEvidenceReproducibleProvenanceMissing, checkEvidenceJson(std.testing.allocator, production_enabled, true, false, null));
    const impossible_ready = try replaceOnce(
        std.testing.allocator,
        actual,
        "\"benchmark_passed\":true,\"promotion_ready\":false,\"promotion_blocker\":\"dev_only_candidate\"",
        "\"benchmark_passed\":true,\"promotion_ready\":true,\"promotion_blocker\":\"\"",
    );
    defer std.testing.allocator.free(impossible_ready);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, impossible_ready, false, false, null));
    const forged_provenance = try replaceOnce(
        std.testing.allocator,
        actual,
        "\"provenance_status\":\"local_unattested\"",
        "\"provenance_status\":\"attested_v1\"",
    );
    defer std.testing.allocator.free(forged_provenance);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, forged_provenance, false, false, null));

    const bad = try replaceOnce(
        std.testing.allocator,
        actual,
        quant_kernel_compiler.first_general_metal_q4_0_source_path,
        "src/ops/metal/generated/missing_q4_0_small_batch.metal",
    );
    defer std.testing.allocator.free(bad);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, bad, false, false, null));

    const q4_fingerprint = try std.fmt.allocPrint(
        std.testing.allocator,
        "{d}",
        .{std.hash.Wyhash.hash(0, quant_kernel_compiler.firstGeneralMetalQ40Source())},
    );
    defer std.testing.allocator.free(q4_fingerprint);
    const stale = try replaceOnce(std.testing.allocator, actual, q4_fingerprint, "0");
    defer std.testing.allocator.free(stale);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale, false, false, null));

    const parallel = try replaceOnce(std.testing.allocator, actual, "\"benchmark_mode\":\"sequential\"", "\"benchmark_mode\":\"parallel\"");
    defer std.testing.allocator.free(parallel);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, parallel, false, false, null));
    const stale_schema = try replaceOnce(std.testing.allocator, actual, "\"schema\":\"antfly.quant_kernel_metal_runtime_evidence.v11\"", "\"schema\":\"antfly.quant_kernel_metal_runtime_evidence.v3\"");
    defer std.testing.allocator.free(stale_schema);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_schema, false, false, null));

    const wrong_command = try replaceOnce(
        std.testing.allocator,
        actual,
        "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out ",
        "zig build test -- ",
    );
    defer std.testing.allocator.free(wrong_command);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, wrong_command, false, false, null));

    const stale_speedup_gate = try replaceOnce(std.testing.allocator, actual, "\"minimum_speedup\":1.020000", "\"minimum_speedup\":1.000000");
    defer std.testing.allocator.free(stale_speedup_gate);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_speedup_gate, false, false, null));

    const stale_speedup_tolerance = try replaceOnce(std.testing.allocator, actual, "\"minimum_speedup_tolerance\":0.001000", "\"minimum_speedup_tolerance\":0.100000");
    defer std.testing.allocator.free(stale_speedup_tolerance);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_speedup_tolerance, false, false, null));

    const no_timing = try replaceOnce(std.testing.allocator, actual, "\"measure_iters\":25", "\"measure_iters\":0");
    defer std.testing.allocator.free(no_timing);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, no_timing, false, false, null));

    const wrong_format = try replaceOnce(std.testing.allocator, actual, "\"format\":\"q5_k\"", "\"format\":\"q4_k\"");
    defer std.testing.allocator.free(wrong_format);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, wrong_format, false, false, null));

    const bad_error = try replaceOnce(std.testing.allocator, actual, "\"max_abs_error\":0.0000000", "\"max_abs_error\":1.0000000");
    defer std.testing.allocator.free(bad_error);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, bad_error, false, false, null));
    const loose_tolerance = try replaceOnce(std.testing.allocator, actual, "\"tolerance_abs\":0.0002000", "\"tolerance_abs\":0.1000000");
    defer std.testing.allocator.free(loose_tolerance);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, loose_tolerance, false, false, null));

    const wrong_shape = try replaceOnce(std.testing.allocator, actual, "\"rows\":4", "\"rows\":5");
    defer std.testing.allocator.free(wrong_shape);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, wrong_shape, false, false, null));

    const duplicate_name = try replaceOnce(std.testing.allocator, actual, "\"name\":\"q5_k_rows_2_8_none\"", "\"name\":\"q4_k_rows_2_8_none\"");
    defer std.testing.allocator.free(duplicate_name);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, duplicate_name, false, false, null));

    const swapped_name_tmp = try replaceOnce(std.testing.allocator, actual, "\"name\":\"q4_k_rows_2_8_bias\"", "\"name\":\"__tmp_q4_bias__\"");
    defer std.testing.allocator.free(swapped_name_tmp);
    const swapped_name_once = try replaceOnce(std.testing.allocator, swapped_name_tmp, "\"name\":\"q8_0_rows_2_8_none\"", "\"name\":\"q4_k_rows_2_8_bias\"");
    defer std.testing.allocator.free(swapped_name_once);
    const swapped_names = try replaceOnce(std.testing.allocator, swapped_name_once, "\"name\":\"__tmp_q4_bias__\"", "\"name\":\"q8_0_rows_2_8_none\"");
    defer std.testing.allocator.free(swapped_names);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, swapped_names, false, false, null));

    const bad_average = try replaceOnce(std.testing.allocator, actual, "\"generated_avg_us\":160.000", "\"generated_avg_us\":999.000");
    defer std.testing.allocator.free(bad_average);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, bad_average, false, false, null));

    const wrong_route_fallback = try replaceOnce(std.testing.allocator, actual, "\"route_fallback_reason\":\"generated_artifact_missing\"", "\"route_fallback_reason\":\"none\"");
    defer std.testing.allocator.free(wrong_route_fallback);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, wrong_route_fallback, false, false, null));

    const wrong_baseline_fallback = try replaceOnce(std.testing.allocator, actual, "\"fallback_reason\":\"unsupported_epilogue\"", "\"fallback_reason\":\"unavailable\"");
    defer std.testing.allocator.free(wrong_baseline_fallback);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, wrong_baseline_fallback, false, false, null));

    const wrong_blocker = try replaceOnce(
        std.testing.allocator,
        actual,
        "\"promotion_blocker\":\"speedup_gate_missing\"",
        "\"promotion_blocker\":\"dev_only_candidate\"",
    );
    defer std.testing.allocator.free(wrong_blocker);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, wrong_blocker, false, false, null));

    const expected_benchmark_supported_field = try std.fmt.allocPrint(std.testing.allocator, "\"benchmark_supported_count\":{d}", .{expected_benchmark_speedups.supported_count});
    defer std.testing.allocator.free(expected_benchmark_supported_field);
    const stale_benchmark_supported_field = try std.fmt.allocPrint(std.testing.allocator, "\"benchmark_supported_count\":{d}", .{expected_benchmark_speedups.supported_count + 1});
    defer std.testing.allocator.free(stale_benchmark_supported_field);
    const stale_benchmark_summary = try replaceOnce(std.testing.allocator, actual, expected_benchmark_supported_field, stale_benchmark_supported_field);
    defer std.testing.allocator.free(stale_benchmark_summary);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_benchmark_summary, false, false, null));

    const promotion_repeat_runs: u32 = @intCast(quant_kernel_compiler.metal_promotion_repeat_runs);
    var repeat_results = results;
    for (&repeat_results) |*result| {
        result.repeat_runs = promotion_repeat_runs;
        if (result.handwritten_elapsed_nanos) |handwritten_elapsed| {
            result.repeat_timing_count = promotion_repeat_runs;
            result.repeat_handwritten_count = promotion_repeat_runs;
            for (0..promotion_repeat_runs) |i| {
                result.repeat_generated_ns[i] = result.elapsed_nanos;
                result.repeat_handwritten_ns[i] = handwritten_elapsed;
                result.repeat_speedups[i] = speedup(handwritten_elapsed, result.elapsed_nanos);
            }
        }
    }

    var production_checks: [metal_runtime_checks.len]CheckCase = undefined;
    var production_results: [metal_runtime_checks.len]CheckResult = undefined;
    var production_count: usize = 0;
    for (metal_runtime_checks, repeat_results) |check, result| {
        if (!productionMetalRuntimeCheck(check)) continue;
        production_checks[production_count] = check;
        production_checks[production_count].measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
        production_results[production_count] = result;
        production_results[production_count].measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
        production_results[production_count].generated_route_checked = true;
        production_results[production_count].generated_timing_route = .decode_runtime_generated;
        production_results[production_count].provider_route_checked = providerRouteSupported(check);
        production_results[production_count].handwritten_elapsed_nanos = result.elapsed_nanos * 2;
        production_results[production_count].minimum_repeat_speedup = 2.0;
        production_results[production_count].repeat_timing_count = promotion_repeat_runs;
        production_results[production_count].repeat_handwritten_count = promotion_repeat_runs;
        for (0..promotion_repeat_runs) |i| {
            production_results[production_count].repeat_generated_ns[i] = result.elapsed_nanos;
            production_results[production_count].repeat_handwritten_ns[i] = result.elapsed_nanos * 2;
            production_results[production_count].repeat_speedups[i] = 2.0;
        }
        production_count += 1;
    }
    try std.testing.expectEqual(expectedProductionRegressionEvidenceCaseCount(), production_count);

    const production_regression_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "production-regression-evidence.json" });
    defer std.testing.allocator.free(production_regression_path);
    if (production_count != 0) {
        var short_production_checks = production_checks;
        short_production_checks[0].measure_iters = default_measure_iters;
        try std.testing.expectError(error.InvalidArgument, writeEvidence(std.testing.allocator, production_regression_path, short_production_checks[0..production_count], production_results[0..production_count], promotion_repeat_runs, null, null, false, true, false, null));
    }
    try writeEvidence(std.testing.allocator, production_regression_path, production_checks[0..production_count], production_results[0..production_count], promotion_repeat_runs, null, null, false, true, false, null);
    const production_regression_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, production_regression_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(production_regression_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"production_regression_check\":true"));
    const expected_compiler_manifest_schema = try std.fmt.allocPrint(std.testing.allocator, "\"compiler_benchmark_manifest_schema\":\"{s}\"", .{quant_kernel_compiler.first_benchmark_manifest_schema});
    defer std.testing.allocator.free(expected_compiler_manifest_schema);
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, expected_compiler_manifest_schema));
    const expected_compiler_manifest_case_count = try std.fmt.allocPrint(std.testing.allocator, "\"compiler_benchmark_manifest_case_count\":{d}", .{quant_kernel_compiler.first_metal_production_benchmark_case_count});
    defer std.testing.allocator.free(expected_compiler_manifest_case_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, expected_compiler_manifest_case_count));
    const expected_compiler_manifest_fingerprint = try std.fmt.allocPrint(std.testing.allocator, "\"compiler_benchmark_manifest_case_fingerprint\":{d}", .{quant_kernel_compiler.metalProductionBenchmarkCaseManifestFingerprint()});
    defer std.testing.allocator.free(expected_compiler_manifest_fingerprint);
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, expected_compiler_manifest_fingerprint));
    if (production_count == 0) {
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"case_count\":0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"production_regression_status\":\"production_regression_skipped\""));
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"promotion_worst_repeat_speedup\":null"));
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"promotion_worst_repeat_case\":null"));
    } else {
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"production_enabled\":true"));
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"production_regression_status\":\"production_regression_ok\""));
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"promotion_worst_repeat_speedup\":2.000000"));
        try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"promotion_worst_repeat_case\":"));
    }
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, " --production-regression-check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, " --measure-iters 500"));
    const stale_production_measure_iters = try replaceOnce(std.testing.allocator, production_regression_actual, " --measure-iters 500", " --measure-iters 25");
    defer std.testing.allocator.free(stale_production_measure_iters);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_production_measure_iters, false, false, null));
    try std.testing.expectEqual(production_count, std.mem.count(u8, production_regression_actual, "\"promotion_ready\":true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"candidate_route_ready_count\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, production_regression_actual, 1, "\"candidate_benchmark_ready_count\":"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, production_regression_actual, "\"promotion_blocker\":\"speedup_gate_missing\""));
    try checkEvidenceFile(std.testing.allocator, production_regression_path, false, false, null);
    if (production_count != 0) {
        try std.testing.expectError(error.MetalEvidenceReproducibleProvenanceMissing, checkEvidenceFile(std.testing.allocator, production_regression_path, true, false, null));
    }
    const production_summary = try checkEvidenceFileWithSummary(std.testing.allocator, production_regression_path, false, false, null);
    try std.testing.expect(production_summary.production_regression_check);
    try std.testing.expectEqual(quant_kernel_compiler.first_metal_production_benchmark_case_count, production_summary.compiler_benchmark_manifest_case_count orelse return error.InvalidMetalEvidence);
    try std.testing.expectEqual(quant_kernel_compiler.metalProductionBenchmarkCaseManifestFingerprint(), production_summary.compiler_benchmark_manifest_case_fingerprint orelse return error.InvalidMetalEvidence);
    const stale_production_manifest_case = try replaceOnce(std.testing.allocator, production_regression_actual, "\"name\":\"q6_k_rows_2_8_none\"", "\"name\":\"q6_k_rows_2_8_none_stale\"");
    defer std.testing.allocator.free(stale_production_manifest_case);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_production_manifest_case, false, false, null));
    const stale_production_manifest_count = try replaceOnce(std.testing.allocator, production_regression_actual, expected_compiler_manifest_case_count, "\"compiler_benchmark_manifest_case_count\":1");
    defer std.testing.allocator.free(stale_production_manifest_count);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_production_manifest_count, false, false, null));
    const stale_production_manifest_fingerprint = try replaceOnce(std.testing.allocator, production_regression_actual, expected_compiler_manifest_fingerprint, "\"compiler_benchmark_manifest_case_fingerprint\":0");
    defer std.testing.allocator.free(stale_production_manifest_fingerprint);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_production_manifest_fingerprint, false, false, null));
    const stale_production_regression = try replaceOnce(std.testing.allocator, production_regression_actual, "\"production_regression_check\":true", "\"production_regression_check\":false");
    defer std.testing.allocator.free(stale_production_regression);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_production_regression, false, false, null));
    const expected_status = if (production_count == 0) "production_regression_skipped" else "production_regression_ok";
    const expected_status_json = try std.fmt.allocPrint(std.testing.allocator, "\"production_regression_status\":\"{s}\"", .{expected_status});
    defer std.testing.allocator.free(expected_status_json);
    const stale_production_regression_status = try replaceOnce(
        std.testing.allocator,
        production_regression_actual,
        expected_status_json,
        "\"production_regression_status\":\"production_regression_timing_drift\"",
    );
    defer std.testing.allocator.free(stale_production_regression_status);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_production_regression_status, false, false, null));
    const expected_worst = if (production_count == 0) "\"promotion_worst_repeat_speedup\":null" else "\"promotion_worst_repeat_speedup\":2.000000";
    const stale_production_regression_worst = try replaceOnce(std.testing.allocator, production_regression_actual, expected_worst, "\"promotion_worst_repeat_speedup\":1.000000");
    defer std.testing.allocator.free(stale_production_regression_worst);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_production_regression_worst, false, false, null));

    const repeat_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "evidence-repeat.json" });
    defer std.testing.allocator.free(repeat_path);
    try writeEvidence(std.testing.allocator, repeat_path, &metal_runtime_checks, &repeat_results, promotion_repeat_runs, null, null, false, false, false, null);
    const repeat_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, repeat_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(repeat_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"repeat_runs\":5"));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"warmup_repeat_runs\":2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"timing_aggregation\":\"median\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"repeat_generated_ns\":["));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"repeat_handwritten_ns\":["));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"repeat_speedups\":["));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"minimum_repeat_index\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, "\"repeat_gate_index\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, repeat_actual, 1, " --repeat-runs 5"));
    try checkEvidenceJson(std.testing.allocator, repeat_actual, false, false, null);

    var longer_checks = metal_runtime_checks;
    for (&longer_checks) |*check| check.measure_iters = 100;
    var longer_results = repeat_results;
    for (&longer_results) |*result| result.measure_iters = 100;
    const longer_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "evidence-repeat-longer.json" });
    defer std.testing.allocator.free(longer_path);
    try writeEvidence(std.testing.allocator, longer_path, &longer_checks, &longer_results, promotion_repeat_runs, null, null, false, false, false, null);
    const longer_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, longer_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(longer_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, longer_actual, 1, " --measure-iters 100"));
    try std.testing.expect(std.mem.containsAtLeast(u8, longer_actual, 1, "\"measure_iters\":100"));
    try checkEvidenceJson(std.testing.allocator, longer_actual, false, false, null);
    const stale_longer_iters = try replaceOnce(std.testing.allocator, longer_actual, " --measure-iters 100", " --measure-iters 0");
    defer std.testing.allocator.free(stale_longer_iters);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_longer_iters, false, false, null));

    const promoted_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "antfly_q6_k_small_batch_msl_v1-promotion-evidence.json" });
    defer std.testing.allocator.free(promoted_path);
    var q6_promotion_results = repeat_results;
    for (&q6_promotion_results, metal_runtime_checks) |*result, check| {
        if (std.mem.eql(u8, check.kernel_name, quant_kernel_compiler.first_general_metal_q6_kernel_id)) {
            result.generated_route_checked = true;
            result.generated_timing_route = .decode_runtime_generated;
            result.provider_route_checked = true;
        }
    }
    var q6_promotion_checks: [metal_runtime_checks.len]CheckCase = undefined;
    var q6_promotion_results_scoped: [metal_runtime_checks.len]CheckResult = undefined;
    var q6_promotion_count: usize = 0;
    for (metal_runtime_checks, q6_promotion_results) |check, result| {
        if (std.mem.eql(u8, check.kernel_name, quant_kernel_compiler.first_general_metal_q6_kernel_id)) {
            q6_promotion_checks[q6_promotion_count] = check;
            q6_promotion_checks[q6_promotion_count].measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
            q6_promotion_results_scoped[q6_promotion_count] = result;
            q6_promotion_results_scoped[q6_promotion_count].measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
            q6_promotion_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), q6_promotion_count);
    try writeEvidence(std.testing.allocator, promoted_path, q6_promotion_checks[0..q6_promotion_count], q6_promotion_results_scoped[0..q6_promotion_count], promotion_repeat_runs, quant_kernel_compiler.first_general_metal_q6_kernel_id, null, false, false, false, null);
    const promoted_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, promoted_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(promoted_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"production_enabled\":true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"promotion_case_count\":2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"promotion_ready_count\":2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"candidate_route_ready_count\":2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"candidate_benchmark_ready_count\":2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"promotion_worst_repeat_speedup\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"promotion_worst_repeat_case\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "--promotion-ready-kernel " ++ quant_kernel_compiler.first_general_metal_q6_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, promoted_actual, 1, "\"promotion_ready\":true,\"promotion_blocker\":\"\""));
    try checkEvidenceJson(std.testing.allocator, promoted_actual, false, false, null);
    try std.testing.expectError(error.MetalEvidenceReproducibleProvenanceMissing, checkEvidenceJson(std.testing.allocator, promoted_actual, true, false, null));
    try std.testing.expectError(error.MetalEvidenceReproducibleProvenanceMissing, checkEvidenceJson(std.testing.allocator, promoted_actual, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id));

    const test_attested_provenance = AttestedProvenance{
        .source_commit = "0123456789abcdef0123456789abcdef01234567",
        .source_tree_clean = true,
        .source_status_sha256 = clean_source_status_sha256,
        .host_os = "macos",
        .host_arch = "aarch64",
        .accelerator_name = "Apple Test Metal Device",
        .metal_compiler_version = "Apple metal version test",
        .zig_version = "0.16.0",
        .recorded_at_utc = "2026-07-13T12:34:56Z",
    };
    const attested_promoted_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "antfly_q6_k_small_batch_msl_v1-attested-promotion-evidence.json" });
    defer std.testing.allocator.free(attested_promoted_path);
    try writeEvidence(
        std.testing.allocator,
        attested_promoted_path,
        q6_promotion_checks[0..q6_promotion_count],
        q6_promotion_results_scoped[0..q6_promotion_count],
        promotion_repeat_runs,
        quant_kernel_compiler.first_general_metal_q6_kernel_id,
        null,
        false,
        false,
        false,
        test_attested_provenance,
    );
    const attested_promoted_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, attested_promoted_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(attested_promoted_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, attested_promoted_actual, 1, "\"provenance_status\":\"attested_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, attested_promoted_actual, 1, " --attest-provenance"));
    try checkEvidenceJson(std.testing.allocator, attested_promoted_actual, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id);
    try checkEvidenceProvenanceMatches(std.testing.allocator, attested_promoted_actual, test_attested_provenance);
    _ = try checkEvidenceFileWithSummaryExpected(
        std.testing.allocator,
        attested_promoted_path,
        true,
        false,
        quant_kernel_compiler.first_general_metal_q6_kernel_id,
        test_attested_provenance,
    );

    const missing_attested_field = try replaceOnce(
        std.testing.allocator,
        attested_promoted_actual,
        "\"host_arch\":\"aarch64\"",
        "\"missing_host_arch\":\"aarch64\"",
    );
    defer std.testing.allocator.free(missing_attested_field);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, missing_attested_field, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id));
    try writeFileCreatingParent(std.testing.io, attested_promoted_path, missing_attested_field);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceFileWithSummaryExpected(std.testing.allocator, attested_promoted_path, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id, test_attested_provenance));

    const bad_clean_status_digest = try replaceOnce(
        std.testing.allocator,
        attested_promoted_actual,
        clean_source_status_sha256,
        "0000000000000000000000000000000000000000000000000000000000000000",
    );
    defer std.testing.allocator.free(bad_clean_status_digest);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, bad_clean_status_digest, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id));
    try writeFileCreatingParent(std.testing.io, attested_promoted_path, bad_clean_status_digest);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceFileWithSummaryExpected(std.testing.allocator, attested_promoted_path, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id, test_attested_provenance));

    const forged_attested_device = try replaceOnce(
        std.testing.allocator,
        attested_promoted_actual,
        "Apple Test Metal Device",
        "Forged Metal Device",
    );
    defer std.testing.allocator.free(forged_attested_device);
    try checkEvidenceJson(std.testing.allocator, forged_attested_device, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id);
    try std.testing.expectError(error.MetalEvidenceProvenanceMismatch, checkEvidenceProvenanceMatches(std.testing.allocator, forged_attested_device, test_attested_provenance));
    try writeFileCreatingParent(std.testing.io, attested_promoted_path, forged_attested_device);
    try std.testing.expectError(error.MetalEvidenceProvenanceMismatch, checkEvidenceFileWithSummaryExpected(std.testing.allocator, attested_promoted_path, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id, test_attested_provenance));
    try writeFileCreatingParent(std.testing.io, attested_promoted_path, attested_promoted_actual);

    const stale_promoted_ready_count = try replaceOnce(std.testing.allocator, promoted_actual, "\"promotion_ready_count\":2", "\"promotion_ready_count\":1");
    defer std.testing.allocator.free(stale_promoted_ready_count);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_promoted_ready_count, false, false, null));
    const stale_promoted_benchmark_count = try replaceOnce(std.testing.allocator, promoted_actual, "\"candidate_benchmark_ready_count\":2", "\"candidate_benchmark_ready_count\":1");
    defer std.testing.allocator.free(stale_promoted_benchmark_count);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_promoted_benchmark_count, false, false, null));
    const wrong_promoted_case_kernel = try replaceOnce(
        std.testing.allocator,
        attested_promoted_actual,
        "\"kernel_id\":\"antfly_q6_k_small_batch_msl_v1\"",
        "\"kernel_id\":\"antfly_q5_k_small_batch_msl_v1\"",
    );
    defer std.testing.allocator.free(wrong_promoted_case_kernel);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, wrong_promoted_case_kernel, true, false, quant_kernel_compiler.first_general_metal_q6_kernel_id));
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, attested_promoted_actual, true, false, "missing_kernel"));

    const shared_promoted_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "evidence-promoted.json" });
    defer std.testing.allocator.free(shared_promoted_path);
    try std.testing.expectError(error.InvalidMetalEvidence, writeEvidence(std.testing.allocator, shared_promoted_path, &metal_runtime_checks, &q6_promotion_results, promotion_repeat_runs, quant_kernel_compiler.first_general_metal_q6_kernel_id, null, false, false, false, null));

    const slow_promoted_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "metal", "antfly_q5_k_small_batch_msl_v1-promotion-evidence.json" });
    defer std.testing.allocator.free(slow_promoted_path);
    var q5_promotion_results = repeat_results;
    for (&q5_promotion_results, metal_runtime_checks) |*result, check| {
        if (std.mem.eql(u8, check.kernel_name, quant_kernel_compiler.first_general_metal_q5_kernel_id)) {
            result.generated_route_checked = true;
            result.generated_timing_route = .decode_runtime_generated;
            result.provider_route_checked = true;
        }
    }
    var q5_promotion_checks: [metal_runtime_checks.len]CheckCase = undefined;
    var q5_promotion_results_scoped: [metal_runtime_checks.len]CheckResult = undefined;
    var q5_promotion_count: usize = 0;
    for (metal_runtime_checks, q5_promotion_results) |check, result| {
        if (std.mem.eql(u8, check.kernel_name, quant_kernel_compiler.first_general_metal_q5_kernel_id)) {
            q5_promotion_checks[q5_promotion_count] = check;
            q5_promotion_checks[q5_promotion_count].measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
            q5_promotion_results_scoped[q5_promotion_count] = result;
            q5_promotion_results_scoped[q5_promotion_count].measure_iters = quant_kernel_compiler.metal_promotion_measure_iters;
            q5_promotion_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), q5_promotion_count);
    try writeEvidence(std.testing.allocator, slow_promoted_path, q5_promotion_checks[0..q5_promotion_count], q5_promotion_results_scoped[0..q5_promotion_count], promotion_repeat_runs, quant_kernel_compiler.first_general_metal_q5_kernel_id, null, false, false, false, null);
    const slow_promoted_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, slow_promoted_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(slow_promoted_actual);
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_promoted_actual, 1, "\"promotion_case_count\":2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_promoted_actual, 1, "\"promotion_ready_count\":0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_promoted_actual, 1, "\"candidate_route_ready_count\":2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_promoted_actual, 1, "\"candidate_benchmark_ready_count\":0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_promoted_actual, 1, "\"promotion_worst_repeat_speedup\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_promoted_actual, 1, "\"promotion_worst_repeat_case\":"));
    const slow_summary = try evidenceSummaryFromJson(std.testing.allocator, slow_promoted_actual);
    try std.testing.expectEqual(@as(usize, 2), slow_summary.promotion_case_count);
    try std.testing.expectEqual(@as(usize, 0), slow_summary.promotion_ready_count);
    try std.testing.expectEqual(@as(usize, 2), slow_summary.candidate_route_ready_count);
    try std.testing.expectEqual(@as(usize, 0), slow_summary.candidate_benchmark_ready_count);
    try std.testing.expectEqual(@as(usize, 2), slow_summary.blocker_counts.speedup_gate_missing);
    try checkEvidenceJson(std.testing.allocator, slow_promoted_actual, false, false, null);
    try std.testing.expectError(error.MetalEvidenceReproducibleProvenanceMissing, checkEvidenceJson(std.testing.allocator, slow_promoted_actual, true, false, quant_kernel_compiler.first_general_metal_q5_kernel_id));
    const stale_slow_case_count = try replaceOnce(std.testing.allocator, slow_promoted_actual, "\"promotion_case_count\":2", "\"promotion_case_count\":1");
    defer std.testing.allocator.free(stale_slow_case_count);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_slow_case_count, false, false, null));
    try std.testing.expectError(error.InvalidArgument, writeEvidence(std.testing.allocator, promoted_path, &metal_runtime_checks, &repeat_results, promotion_repeat_runs, "missing_kernel", null, false, false, false, null));
    try std.testing.expectError(error.InvalidArgument, writeEvidence(std.testing.allocator, promoted_path, &metal_runtime_checks, &repeat_results, 1, quant_kernel_compiler.first_general_metal_q4_kernel_id, null, false, false, false, null));

    const stale_repeat = try replaceOnce(std.testing.allocator, repeat_actual, " --repeat-runs 5", " --repeat-runs 2");
    defer std.testing.allocator.free(stale_repeat);
    try std.testing.expectError(error.InvalidMetalEvidence, checkEvidenceJson(std.testing.allocator, stale_repeat, false, false, null));
    try std.testing.expect(!hasPromotionRepeatRuns(1));
    try std.testing.expect(!hasPromotionRepeatRuns(2));
    try std.testing.expect(!hasPromotionRepeatRuns(3));
    try std.testing.expect(!hasPromotionRepeatRuns(4));
    try std.testing.expect(hasPromotionRepeatRuns(5));

    var mismatched_repeat_results = results;
    mismatched_repeat_results[0].repeat_runs = 2;
    try std.testing.expectError(error.InvalidArgument, writeEvidence(std.testing.allocator, repeat_path, &metal_runtime_checks, &mismatched_repeat_results, 1, null, null, false, false, false, null));
}

test "quant kernel metal runtime benchmark math gates on minimum repeat speedup" {
    const good =
        \\{"measure_iters":25,"generated_ns":2500,"generated_avg_us":0.100,"benchmark_passed":true,"handwritten_baseline_supported":true,"handwritten_ns":5000,"handwritten_avg_us":0.200,"measured_speedup":2.000000,"minimum_repeat_speedup":1.020000,"repeat_runs":5,"repeat_generated_ns":[2500,2500,2500,2500,2500],"repeat_handwritten_ns":[2550,2550,5000,5000,5000],"repeat_speedups":[1.020000,1.020000,2.000000,2.000000,2.000000],"minimum_repeat_index":0,"repeat_gate_index":0}
    ;
    var parsed_good = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, good, .{});
    defer parsed_good.deinit();
    try std.testing.expect(evidenceCaseHasConsistentBenchmarkMath(parsed_good.value));

    const stripped_repeat_evidence =
        \\{"measure_iters":25,"generated_ns":2500,"generated_avg_us":0.100,"benchmark_passed":true,"handwritten_baseline_supported":true,"handwritten_ns":5000,"handwritten_avg_us":0.200,"measured_speedup":2.000000,"minimum_repeat_speedup":1.020000,"repeat_runs":5}
    ;
    var parsed_stripped_repeat_evidence = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, stripped_repeat_evidence, .{});
    defer parsed_stripped_repeat_evidence.deinit();
    try std.testing.expect(!evidenceCaseHasConsistentBenchmarkMath(parsed_stripped_repeat_evidence.value));

    const borderline =
        \\{"measure_iters":25,"generated_ns":1000000,"generated_avg_us":40.000,"benchmark_passed":false,"handwritten_baseline_supported":true,"handwritten_ns":2000000,"handwritten_avg_us":80.000,"measured_speedup":2.000000,"minimum_repeat_speedup":1.019999,"repeat_runs":5,"repeat_generated_ns":[1000000,1000000,1000000,1000000,1000000],"repeat_handwritten_ns":[1019999,1019999,2000000,2000000,2000000],"repeat_speedups":[1.019999,1.019999,2.000000,2.000000,2.000000],"minimum_repeat_index":0,"repeat_gate_index":0}
    ;
    var parsed_borderline = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, borderline, .{});
    defer parsed_borderline.deinit();
    try std.testing.expect(evidenceCaseHasConsistentBenchmarkMath(parsed_borderline.value));

    const bad =
        \\{"measure_iters":25,"generated_ns":1000000,"generated_avg_us":40.000,"benchmark_passed":true,"handwritten_baseline_supported":true,"handwritten_ns":2000000,"handwritten_avg_us":80.000,"measured_speedup":2.000000,"minimum_repeat_speedup":1.018000,"repeat_runs":5,"repeat_generated_ns":[1000000,1000000,1000000,1000000,1000000],"repeat_handwritten_ns":[1018000,1018000,2000000,2000000,2000000],"repeat_speedups":[1.018000,1.018000,2.000000,2.000000,2.000000],"minimum_repeat_index":0,"repeat_gate_index":0}
    ;
    var parsed_bad = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bad, .{});
    defer parsed_bad.deinit();
    try std.testing.expect(!evidenceCaseHasConsistentBenchmarkMath(parsed_bad.value));

    const bad_average =
        \\{"measure_iters":25,"generated_ns":5000,"generated_avg_us":0.200,"benchmark_passed":true,"handwritten_baseline_supported":true,"handwritten_ns":4500,"handwritten_avg_us":0.180,"measured_speedup":0.900000,"minimum_repeat_speedup":1.020000,"repeat_runs":5}
    ;
    var parsed_bad_average = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bad_average, .{});
    defer parsed_bad_average.deinit();
    try std.testing.expect(!evidenceCaseHasConsistentBenchmarkMath(parsed_bad_average.value));
}

test "quant kernel metal runtime benchmark math accepts rounded repeat index aliases" {
    const rounded_minimum_alias =
        \\{"measure_iters":25,"generated_ns":10000000,"generated_avg_us":400.000,"benchmark_passed":true,"handwritten_baseline_supported":true,"handwritten_ns":20000000,"handwritten_avg_us":800.000,"measured_speedup":2.000000,"minimum_repeat_speedup":1.020000,"repeat_runs":5,"repeat_generated_ns":[10000000,10000000,10000000,10000000,10000000],"repeat_handwritten_ns":[10200004,10200003,20000000,20000000,20000000],"repeat_speedups":[1.020000,1.020000,2.000000,2.000000,2.000000],"minimum_repeat_index":1,"repeat_gate_index":0}
    ;
    var parsed_minimum_alias = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, rounded_minimum_alias, .{});
    defer parsed_minimum_alias.deinit();
    try std.testing.expect(evidenceCaseHasConsistentBenchmarkMath(parsed_minimum_alias.value));

    const rounded_gate_alias =
        \\{"measure_iters":25,"generated_ns":10000000,"generated_avg_us":400.000,"benchmark_passed":false,"handwritten_baseline_supported":true,"handwritten_ns":10000011,"handwritten_avg_us":400.000,"measured_speedup":1.000001,"minimum_repeat_speedup":0.500000,"repeat_runs":5,"repeat_generated_ns":[10000000,10000000,10000000,10000000,10000000],"repeat_handwritten_ns":[10000011,10000000,5000000,20000000,20000000],"repeat_speedups":[1.000001,1.000000,0.500000,2.000000,2.000000],"minimum_repeat_index":2,"repeat_gate_index":2}
    ;
    var parsed_gate_alias = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, rounded_gate_alias, .{});
    defer parsed_gate_alias.deinit();
    try std.testing.expect(evidenceCaseHasConsistentBenchmarkMath(parsed_gate_alias.value));
}

test "quant kernel metal runtime benchmark math rejects one repeat outlier" {
    const robust =
        \\{"measure_iters":25,"generated_ns":100,"generated_avg_us":0.004,"benchmark_passed":false,"handwritten_baseline_supported":true,"handwritten_ns":120,"handwritten_avg_us":0.0048,"measured_speedup":1.200000,"minimum_repeat_speedup":0.500000,"repeat_runs":5,"repeat_generated_ns":[100,100,100,100,100],"repeat_handwritten_ns":[50,120,120,120,120],"repeat_speedups":[0.500000,1.200000,1.200000,1.200000,1.200000],"minimum_repeat_index":0,"repeat_gate_index":0}
    ;
    var parsed_robust = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, robust, .{});
    defer parsed_robust.deinit();
    try std.testing.expect(evidenceCaseHasConsistentBenchmarkMath(parsed_robust.value));

    const stale_min =
        \\{"measure_iters":25,"generated_ns":100,"generated_avg_us":0.004,"benchmark_passed":false,"handwritten_baseline_supported":true,"handwritten_ns":120,"handwritten_avg_us":0.0048,"measured_speedup":1.200000,"minimum_repeat_speedup":0.500000,"repeat_runs":5,"repeat_generated_ns":[100,100,100,100,100],"repeat_handwritten_ns":[50,120,120,120,120],"repeat_speedups":[0.500000,1.200000,1.200000,1.200000,1.200000],"minimum_repeat_index":0}
    ;
    var parsed_stale_min = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, stale_min, .{});
    defer parsed_stale_min.deinit();
    try std.testing.expect(!evidenceCaseHasConsistentBenchmarkMath(parsed_stale_min.value));
}

test "quant kernel metal runtime promotion blocker reports unstable repeat timing" {
    try std.testing.expectEqualStrings("speedup_gate_missing", quant_kernel_compiler.metalPromotionSpeedupBlocker(0.9, 0.9));
    try std.testing.expectEqualStrings("unstable_benchmark_timing", quant_kernel_compiler.metalPromotionSpeedupBlocker(2.0, 1.019999));
    try std.testing.expectEqualStrings("unstable_benchmark_timing", quant_kernel_compiler.metalPromotionSpeedupBlocker(2.0, 1.018000));
    try std.testing.expect(promotionBlockerEvidenceMatchesExactly(.{ .speedup_gate_missing = 1 }, quant_kernel_compiler.metal_blocker_speedup_gate_missing));
    try std.testing.expect(promotionBlockerEvidenceMatches(.{ .unstable_benchmark_timing = 1 }, quant_kernel_compiler.metal_blocker_speedup_gate_missing));
    try std.testing.expect(promotionBlockerEvidenceTimingDrifted(.{ .unstable_benchmark_timing = 1 }, quant_kernel_compiler.metal_blocker_speedup_gate_missing));
    try std.testing.expect(promotionBlockerEvidenceMatches(.{ .speedup_gate_missing = 1 }, quant_kernel_compiler.metal_blocker_unstable_benchmark_timing));
    try std.testing.expect(!promotionBlockerEvidenceTimingDrifted(.{ .speedup_gate_missing = 1 }, quant_kernel_compiler.metal_blocker_unstable_benchmark_timing));
    try std.testing.expect(!promotionBlockerEvidenceTimingDrifted(.{ .speedup_gate_missing = 1 }, quant_kernel_compiler.metal_blocker_speedup_gate_missing));
    try std.testing.expect(!promotionBlockerEvidenceMatches(.{ .runtime_route_only = 1 }, quant_kernel_compiler.metal_blocker_speedup_gate_missing));
    try std.testing.expect(promotionBlockerEvidenceCleared(
        .{
            .case_count = 2,
            .promotion_case_count = 2,
            .promotion_ready_count = 2,
            .runtime_route_checked_count = 0,
            .provider_route_checked_count = 0,
            .blocker_counts = .{},
        },
        quant_kernel_compiler.metal_blocker_speedup_gate_missing,
    ));
    try std.testing.expect(promotionBlockerEvidenceCleared(
        .{
            .case_count = 2,
            .promotion_case_count = 2,
            .promotion_ready_count = 2,
            .runtime_route_checked_count = 0,
            .provider_route_checked_count = 0,
            .blocker_counts = .{},
        },
        quant_kernel_compiler.metal_blocker_unstable_benchmark_timing,
    ));
    try std.testing.expect(!promotionBlockerEvidenceCleared(
        .{
            .case_count = 2,
            .promotion_case_count = 2,
            .promotion_ready_count = 1,
            .runtime_route_checked_count = 0,
            .provider_route_checked_count = 0,
            .blocker_counts = .{},
        },
        quant_kernel_compiler.metal_blocker_speedup_gate_missing,
    ));
    try std.testing.expect(!promotionBlockerEvidenceCleared(
        .{
            .case_count = 2,
            .promotion_case_count = 2,
            .promotion_ready_count = 1,
            .runtime_route_checked_count = 0,
            .provider_route_checked_count = 0,
            .blocker_counts = .{},
        },
        quant_kernel_compiler.metal_blocker_unstable_benchmark_timing,
    ));
    try enforceClearedBlockerPolicy(.{ .cleared_blocker_count = 0 }, true);
    try enforceClearedBlockerPolicy(.{ .cleared_blocker_count = 1 }, false);
    try enforceClearedBlockerPolicy(.{ .cleared_blocker_count = 1, .unconfirmed_cleared_blocker_count = 1 }, true);
    try std.testing.expectError(error.MetalBlockerEvidenceCleared, enforceClearedBlockerPolicy(.{ .cleared_blocker_count = 1, .confirmed_cleared_blocker_count = 1 }, true));
    try std.testing.expectError(error.MetalBlockerEvidenceCleared, enforceClearedBlockerPolicy(.{ .cleared_blocker_count = 1 }, true));
    try std.testing.expectEqualStrings("blocker_evidence_ok", blockerEvidenceAuditStatus(.{}));
    try std.testing.expectEqualStrings("blocker_evidence_timing_drift", blockerEvidenceAuditStatus(.{ .timing_blocker_drift_count = 1 }));
    try std.testing.expectEqualStrings("blocker_evidence_production_regression_guarded", blockerEvidenceAuditStatus(.{ .production_regression_guarded_count = 1 }));
    try std.testing.expectEqualStrings("blocker_evidence_cleared", blockerEvidenceAuditStatus(.{ .cleared_blocker_count = 1 }));
    try std.testing.expectEqualStrings("blocker_evidence_unconfirmed_cleared", blockerEvidenceAuditStatus(.{
        .cleared_blocker_count = 1,
        .unconfirmed_cleared_blocker_count = 1,
        .timing_blocker_drift_count = 1,
    }));
    try std.testing.expectEqualStrings("blocker_evidence_confirmed_cleared", blockerEvidenceAuditStatus(.{
        .cleared_blocker_count = 1,
        .confirmed_cleared_blocker_count = 1,
        .unconfirmed_cleared_blocker_count = 1,
    }));
    try std.testing.expect(evidenceSummaryHasRouteReadyCoverage(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 1,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 2,
        .blocker_counts = .{ .unstable_benchmark_timing = 1 },
    }));
    try std.testing.expect(!evidenceSummaryHasRouteReadyCoverage(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 1,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 1,
        .blocker_counts = .{ .unstable_benchmark_timing = 1 },
    }));
    try std.testing.expect(!productionRegressionEvidenceHasHardBlocker(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 1,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 2,
        .blocker_counts = .{ .unstable_benchmark_timing = 1 },
    }));
    try std.testing.expectEqualStrings("production_regression_timing_drift", productionRegressionEvidenceStatus(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 1,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 2,
        .blocker_counts = .{ .unstable_benchmark_timing = 1 },
    }));
    try std.testing.expect(productionRegressionEvidenceHasHardBlocker(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 2,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 1,
        .blocker_counts = .{},
    }));
    try std.testing.expect(productionRegressionEvidenceHasHardBlocker(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 1,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 2,
        .blocker_counts = .{ .speedup_gate_missing = 1 },
    }));
    try std.testing.expectEqualStrings("production_regression_blocked", productionRegressionEvidenceStatus(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 1,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 2,
        .blocker_counts = .{ .speedup_gate_missing = 1 },
    }));
    try std.testing.expectEqualStrings("production_regression_ok", productionRegressionEvidenceStatus(.{
        .case_count = 2,
        .promotion_case_count = 2,
        .promotion_ready_count = 2,
        .runtime_route_checked_count = 2,
        .provider_route_checked_count = 0,
        .candidate_route_ready_count = 2,
        .blocker_counts = .{},
    }));
    try std.testing.expectEqualStrings("production_regression_skipped", productionRegressionEvidenceStatus(.{
        .case_count = 0,
        .promotion_case_count = 0,
        .promotion_ready_count = 0,
        .runtime_route_checked_count = 0,
        .provider_route_checked_count = 0,
        .blocker_counts = .{},
    }));

    const unstable =
        \\{"promotion_ready":false,"promotion_blocker":"unstable_benchmark_timing","benchmark_passed":false,"handwritten_baseline_supported":true,"measured_speedup":2.000000,"minimum_repeat_speedup":1.018000}
    ;
    var parsed_unstable = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, unstable, .{});
    defer parsed_unstable.deinit();
    try std.testing.expect(evidenceCaseHasConsistentPromotionBlocker(parsed_unstable.value));

    const stale =
        \\{"promotion_ready":false,"promotion_blocker":"speedup_gate_missing","benchmark_passed":false,"handwritten_baseline_supported":true,"measured_speedup":2.000000,"minimum_repeat_speedup":1.018000}
    ;
    var parsed_stale = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, stale, .{});
    defer parsed_stale.deinit();
    try std.testing.expect(!evidenceCaseHasConsistentPromotionBlocker(parsed_stale.value));
}

fn replaceOnce(allocator: std.mem.Allocator, input: []const u8, needle: []const u8, replacement: []const u8) ![]u8 {
    const index = std.mem.indexOf(u8, input, needle) orelse return error.InvalidArgument;
    return std.fmt.allocPrint(allocator, "{s}{s}{s}", .{ input[0..index], replacement, input[index + needle.len ..] });
}

fn replaceAll(allocator: std.mem.Allocator, input: []const u8, needle: []const u8, replacement: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var start: usize = 0;
    var replaced = false;
    while (std.mem.indexOf(u8, input[start..], needle)) |relative_index| {
        const index = start + relative_index;
        try out.appendSlice(allocator, input[start..index]);
        try out.appendSlice(allocator, replacement);
        start = index + needle.len;
        replaced = true;
    }
    if (!replaced) return error.InvalidArgument;
    try out.appendSlice(allocator, input[start..]);
    return out.toOwnedSlice(allocator);
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}
