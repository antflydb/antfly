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
const ops = @import("../ops.zig");
const tensor_mod = @import("../../backends/tensor.zig");
const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");
const kernels_mod = @import("kernels.zig");
const cublaslt_mod = @import("cublaslt.zig");
const scratch_mod = @import("scratch.zig");
const weight_source_mod = @import("../../models/weight_source.zig");
const tensor_store_mod = @import("../../models/tensor_store.zig");
const c_file = @import("../../util/c_file.zig");
const native_compute_mod = @import("../native_compute.zig");
const run_memory = @import("../../runtime/tier/memory.zig");
const gguf_tensor_types = @import("../../gguf/tensor_types.zig");
const quant_codec = @import("../../gguf/quant_codec.zig");
const quant_matmul = @import("../../graph/quant_matmul.zig");
const operator_plan = @import("../../graph/operator_plan.zig");
const kv_storage_runtime = @import("../../runtime/kv/storage_runtime.zig");
const prefetch_mod = @import("../../runtime/tier/prefetch.zig");
const gpt_arch = @import("../../architectures/gpt.zig");
const gpt_model = @import("../../models/gpt.zig");
const platform = @import("antfly_platform");
const linalg = @import("inference_linalg");

const CT = ops.CT;

pub const CudaTensor = struct {
    buffer: buffer_mod.DeviceBuffer,
    dtype: tensor_mod.DType,
    shape: []i64,
    elem_count: usize,
    quant_type: ?gguf_tensor_types.TensorType = null,
    tc_quant: ?CudaTensorCoreQuantBuffer = null,
    owns_buffer: bool = true,
    owns_shape: bool = true,
    owned_by_tensor: bool = true,
};

pub const CudaTensorCoreQuantLayout = enum {
    q8_0_hmma,
    q4_k_hmma,
};

pub const CudaTensorCoreQuantBuffer = struct {
    buffer: buffer_mod.DeviceBuffer,
    layout: CudaTensorCoreQuantLayout,
    row_blocks: usize,
    bytes: usize,
};

pub const CapabilityProfile = enum {
    clipclap,
    deberta_reranker,
    gliner2,
    gemma4,
};

const CudaDispatchOp = enum {
    linear,
    linear_no_bias,
    linear_gelu,
    linear_add,
    linear_quick_gelu,
    linear_relu,
    linear_pair,
    linear_pair_relu,
    linear_pair_inputs,
    tc_pack,
};

const CudaDispatchQuant = enum {
    none,
    q8_0,
    q4_0,
    q4_k,
    f32,
    bf16,
};

const CudaDispatchRoute = enum {
    tc_pack,
    q8_tc_hmma,
    q4_tc_hmma,
    q8_simt,
    q4_simt,
    q4_span_simt,
    dense_lt,
    dense_cuda,
    f32_cuda,
};

const CudaDispatchEpilogue = enum {
    none,
    bias,
    bias_gelu,
    bias_add,
    bias_quick_gelu,
    bias_relu,
    pair,
    pair_relu,
    pair_inputs,
};

const CudaDispatchFallback = enum {
    none,
    tc_not_requested,
    tc_no_packed_weight,
    tc_missing_symbol,
    tc_unsupported_shape,
    explicit_simt,
    specialized_span,
};

const cuda_dispatch_route_count = @typeInfo(CudaDispatchRoute).@"enum".fields.len;
const cuda_dispatch_max_entries: usize = 256;

const CudaDispatchEntry = struct {
    op: CudaDispatchOp,
    quant: CudaDispatchQuant,
    route: CudaDispatchRoute,
    epilogue: CudaDispatchEpilogue,
    fallback: CudaDispatchFallback,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    calls: u64 = 0,
    tc_pack_bytes: u64 = 0,

    fn matches(
        self: CudaDispatchEntry,
        op: CudaDispatchOp,
        quant: CudaDispatchQuant,
        route: CudaDispatchRoute,
        epilogue: CudaDispatchEpilogue,
        fallback: CudaDispatchFallback,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) bool {
        return self.op == op and
            self.quant == quant and
            self.route == route and
            self.epilogue == epilogue and
            self.fallback == fallback and
            self.rows == rows and
            self.in_dim == in_dim and
            self.out_dim == out_dim;
    }
};

const CudaDispatchStats = struct {
    route_counts: [cuda_dispatch_route_count]u64 = [_]u64{0} ** cuda_dispatch_route_count,
    entries: std.ArrayListUnmanaged(CudaDispatchEntry) = .empty,
    dropped_entries: u64 = 0,

    fn note(
        self: *CudaDispatchStats,
        allocator: std.mem.Allocator,
        op: CudaDispatchOp,
        quant: CudaDispatchQuant,
        route: CudaDispatchRoute,
        epilogue: CudaDispatchEpilogue,
        fallback: CudaDispatchFallback,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        tc_pack_bytes: usize,
    ) void {
        self.route_counts[@intFromEnum(route)] += 1;
        if (!cudaDispatchStatsEnabled()) return;
        for (self.entries.items) |*entry| {
            if (entry.matches(op, quant, route, epilogue, fallback, rows, in_dim, out_dim)) {
                entry.calls += 1;
                entry.tc_pack_bytes += @intCast(tc_pack_bytes);
                return;
            }
        }
        if (self.entries.items.len >= cuda_dispatch_max_entries) {
            self.dropped_entries += 1;
            return;
        }
        self.entries.append(allocator, .{
            .op = op,
            .quant = quant,
            .route = route,
            .epilogue = epilogue,
            .fallback = fallback,
            .rows = rows,
            .in_dim = in_dim,
            .out_dim = out_dim,
            .calls = 1,
            .tc_pack_bytes = @intCast(tc_pack_bytes),
        }) catch {
            self.dropped_entries += 1;
            return;
        };
    }

    fn printIfEnabled(self: *const CudaDispatchStats) void {
        if (!cudaDispatchStatsEnabled()) return;
        std.debug.print("ANTFLY_CUDA_DISPATCH_STATS {{\"routes\":{{", .{});
        inline for (@typeInfo(CudaDispatchRoute).@"enum".fields, 0..) |field, i| {
            if (i != 0) std.debug.print(",", .{});
            std.debug.print("\"{s}\":{d}", .{ field.name, self.route_counts[i] });
        }
        std.debug.print("}},\"entries\":[", .{});
        for (self.entries.items, 0..) |entry, i| {
            if (i != 0) std.debug.print(",", .{});
            std.debug.print(
                "{{\"op\":\"{s}\",\"quant\":\"{s}\",\"route\":\"{s}\",\"epilogue\":\"{s}\",\"fallback\":\"{s}\",\"rows\":{d},\"in_dim\":{d},\"out_dim\":{d},\"calls\":{d},\"tc_pack_bytes\":{d}}}",
                .{
                    @tagName(entry.op),
                    @tagName(entry.quant),
                    @tagName(entry.route),
                    @tagName(entry.epilogue),
                    @tagName(entry.fallback),
                    entry.rows,
                    entry.in_dim,
                    entry.out_dim,
                    entry.calls,
                    entry.tc_pack_bytes,
                },
            );
        }
        std.debug.print("],\"dropped_entries\":{d}}}\n", .{self.dropped_entries});
    }

    fn deinit(self: *CudaDispatchStats, allocator: std.mem.Allocator) void {
        self.entries.deinit(allocator);
        self.* = .{};
    }
};

const DenseStreamEntry = struct {
    const Kind = enum { attention, mlp };

    name: []u8,
    tensor: *CudaTensor,
    byte_len: usize,
    kind: Kind,
    last_access: u64 = 0,
};

const DenseHostRange = struct {
    path: []u8,
    byte_offset: u64,
    byte_len: usize,
    dtype: tensor_mod.DType,
    shape: []i64,

    fn deinit(self: *DenseHostRange, allocator: std.mem.Allocator) void {
        allocator.free(self.path);
        allocator.free(self.shape);
        self.* = .{
            .path = &.{},
            .byte_offset = 0,
            .byte_len = 0,
            .dtype = .f32,
            .shape = &.{},
        };
    }
};

const DenseHostPrefetchEntry = struct {
    name: []u8,
    range: DenseHostRange,
    kind: DenseStreamEntry.Kind,
    priority: u64 = 0,
    pending: bool = false,
    loading: bool = false,
    ready: bool = false,
    failed: bool = false,
    host: []u8 = &.{},
    read_ns: u64 = 0,
    last_access: u64 = 0,

    fn deinit(self: *DenseHostPrefetchEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.range.deinit(allocator);
        if (self.host.len != 0) allocator.free(self.host);
        self.* = undefined;
    }
};

const DenseHostPrefetchQueue = prefetch_mod.Queue(*DenseHostPrefetchEntry);

const TempPinnedSlot = struct {
    buffer: buffer_mod.DeviceBuffer = .{},
    requested_len: usize = 0,
    in_use: bool = false,
};

const CudaDecoderRuntimeLinearSlot = struct {
    weight: CudaTensor,
    bias: ?CudaTensor = null,
    in_dim: usize = 0,
    out_dim: usize = 0,
};

const CudaDecoderRuntimeRmsNormSlot = struct {
    weight: CudaTensor,
    hidden_size: usize = 0,
};

const CudaDecoderRuntimeFamilyState = struct {
    prepared: bool = false,
    reserve_kv_tokens: usize = 0,
    configured_layer_count: usize = 0,
    hidden_size: u32 = 0,
    intermediate_size: u32 = 0,
    num_hidden_layers: u32 = 0,
    num_attention_heads: u32 = 0,
    num_key_value_heads: u32 = 0,
    num_global_key_value_heads: u32 = 0,
    attention_head_dim: u32 = 0,
    global_head_dim: u32 = 0,
    vocab_size: u32 = 0,
    sliding_window: u32 = 0,
    sliding_window_pattern: u32 = 0,
    ple_hidden_size: u32 = 0,
};

pub const RuntimeStats = struct {
    pub const top_transfer_size_count = 8;

    quant_ops: operator_plan.Stats = .{},
    h2d_bytes: usize = 0,
    d2h_bytes: usize = 0,
    d2d_bytes: usize = 0,
    resident_weight_bytes: usize = 0,
    device_allocated_bytes: usize = 0,
    device_alloc_calls: usize = 0,
    device_free_calls: usize = 0,
    temp_buffer_hits: usize = 0,
    temp_buffer_misses: usize = 0,
    temp_buffer_releases: usize = 0,
    temp_buffer_evictions: usize = 0,
    temp_buffer_cached_bytes: usize = 0,
    deferred_free_queued: usize = 0,
    deferred_free_drains: usize = 0,
    deferred_free_forced_drains: usize = 0,
    deferred_free_pending_bytes: usize = 0,
    deferred_free_reclaimed_bytes: usize = 0,
    kernel_launches: usize = 0,
    stream_syncs: usize = 0,
    cross_backend_copies: usize = 0,
    cross_backend_copy_bytes: usize = 0,
    cross_backend_event_records: usize = 0,
    cross_backend_event_waits: usize = 0,
    cross_backend_sync_fallbacks: usize = 0,
    cuda_graph_capture_begins: usize = 0,
    cuda_graph_capture_replays: usize = 0,
    cuda_graph_capture_discards: usize = 0,
    cuda_graph_capture_instantiates: usize = 0,
    cuda_graph_capture_update_successes: usize = 0,
    cuda_graph_capture_update_failures: usize = 0,
    cuda_graph_capture_update_unavailable: usize = 0,
    cuda_graph_capture_scalar_updates: usize = 0,
    cuda_graph_capture_persistent_replays: usize = 0,
    cuda_graph_capture_capacity_skips: usize = 0,
    upload_syncs: usize = 0,
    download_syncs: usize = 0,
    eval_syncs: usize = 0,
    eval_requests: usize = 0,
    eval_skipped_eager: usize = 0,
    eval_forced_syncs: usize = 0,
    from_float32_calls: usize = 0,
    from_float32_bytes: usize = 0,
    to_float32_calls: usize = 0,
    to_float32_bytes: usize = 0,
    upload_owned_host_calls: usize = 0,
    upload_owned_host_bytes: usize = 0,
    download_alloc_calls: usize = 0,
    download_alloc_bytes: usize = 0,
    upload_bucket_le_16: usize = 0,
    upload_bucket_le_1k: usize = 0,
    upload_bucket_le_8k: usize = 0,
    upload_bucket_le_32k: usize = 0,
    upload_bucket_le_256k: usize = 0,
    upload_bucket_gt_256k: usize = 0,
    download_bucket_le_16: usize = 0,
    download_bucket_le_1k: usize = 0,
    download_bucket_le_8k: usize = 0,
    download_bucket_le_32k: usize = 0,
    download_bucket_le_256k: usize = 0,
    download_bucket_gt_256k: usize = 0,
    add_scalar_calls: usize = 0,
    rms_norm_bare_calls: usize = 0,
    upload_top_sizes: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    upload_top_counts: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    download_top_sizes: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    download_top_counts: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    launch_embedding: usize = 0,
    launch_linear: usize = 0,
    launch_linear_qkv: usize = 0,
    launch_norm: usize = 0,
    launch_norm_layer: usize = 0,
    launch_norm_add_layer: usize = 0,
    launch_norm_rms: usize = 0,
    launch_norm_rms_add: usize = 0,
    launch_norm_rms_add_mul_scalar: usize = 0,
    launch_norm_rms_add_output_scale: usize = 0,
    launch_norm_rms_bare: usize = 0,
    launch_norm_head_rope: usize = 0,
    launch_rope: usize = 0,
    launch_attention: usize = 0,
    launch_attention_gqa_decode: usize = 0,
    launch_attention_gqa_scalar: usize = 0,
    launch_elementwise: usize = 0,
    launch_scalar: usize = 0,
    launch_scalar_multiply_immediate: usize = 0,
    launch_scalar_add_immediate: usize = 0,
    launch_scalar_device_broadcast: usize = 0,
    launch_argmax: usize = 0,
    launch_other: usize = 0,
    activation_multiply_fused: usize = 0,
    add_mul_scalar_fused: usize = 0,
    rms_norm_add_fused: usize = 0,
    rms_norm_add_output_scale_fused: usize = 0,
    rms_norm_add_output_scale_fallbacks: usize = 0,
    host_attention_fallbacks: usize = 0,
    rope_host_fallbacks: usize = 0,
    rope_per_item_host_fallbacks: usize = 0,
    gqa_dense_host_fallbacks: usize = 0,
    paged_attention_host_fallbacks: usize = 0,
    device_kv_writes: usize = 0,
    device_kv_reads: usize = 0,
    device_kv_attempts: usize = 0,
    device_kv_successes: usize = 0,
    device_kv_fail_batch: usize = 0,
    device_kv_fail_no_cache: usize = 0,
    device_kv_fail_no_storage: usize = 0,
    device_kv_fail_no_hook: usize = 0,
    device_kv_fail_write: usize = 0,
    device_kv_fail_read: usize = 0,
    device_kv_fail_shape: usize = 0,
    qkv_fused_q8: usize = 0,
    qkv_fused_q4: usize = 0,
    qkv_fused_q4_q4_f32: usize = 0,
    qkv_fused_f32: usize = 0,
    linear_pair_fused_q8: usize = 0,
    linear_pair_fused_q4: usize = 0,
    linear_pair_fallbacks: usize = 0,
    lm_head_argmax_fused_q8: usize = 0,
    lm_head_argmax_fused_q4: usize = 0,
    lm_head_argmax_fallbacks: usize = 0,
    bf16_cublaslt_linear_calls: usize = 0,
    bf16_cublaslt_qkv_calls: usize = 0,
    bf16_cublaslt_activation_staging_calls: usize = 0,
    bf16_cublaslt_fallbacks: usize = 0,
    bf16_scalar_linear_calls: usize = 0,
    bf16_scalar_qkv_calls: usize = 0,
    qkv_fallback_unsupported: usize = 0,
    qkv_kernel_unavailable: usize = 0,
    q4k_decode_fast_hits: usize = 0,
    q4k_decode_fast_fallbacks: usize = 0,
    head_norm_rope_fused_hits: usize = 0,
    head_norm_rope_fused_fallbacks: usize = 0,
    decoder_runtime_linear_slot_prepares: usize = 0,
    decoder_runtime_linear_slot_prepare_misses: usize = 0,
    decoder_runtime_rms_norm_slot_prepares: usize = 0,
    decoder_runtime_rms_norm_slot_prepare_misses: usize = 0,
    decoder_runtime_linear_apply_hits: usize = 0,
    decoder_runtime_linear_apply_misses: usize = 0,
    decoder_runtime_linear_pair_apply_hits: usize = 0,
    decoder_runtime_linear_qkv_apply_hits: usize = 0,
    decoder_runtime_rms_norm_apply_hits: usize = 0,
    decoder_runtime_rms_norm_apply_misses: usize = 0,
    decoder_runtime_attention_residual_attempts: usize = 0,
    decoder_runtime_attention_residual_hits: usize = 0,
    decoder_runtime_attention_residual_misses: usize = 0,
    decoder_runtime_gated_ffn_attempts: usize = 0,
    decoder_runtime_gated_ffn_hits: usize = 0,
    decoder_runtime_gated_ffn_misses: usize = 0,
    gated_down_fused_q8: usize = 0,
    gated_down_fused_q4: usize = 0,
    gated_down_fallbacks: usize = 0,
    decode_profile_events: usize = 0,
    decode_profile_qkv_us: u64 = 0,
    decode_profile_gqa_attention_us: u64 = 0,
    decode_profile_attention_output_us: u64 = 0,
    decode_profile_attention_norm_residual_us: u64 = 0,
    decode_profile_ffn_gate_up_us: u64 = 0,
    decode_profile_ffn_gated_down_us: u64 = 0,
    decode_profile_ffn_post_norm_us: u64 = 0,
    decode_profile_lm_head_argmax_us: u64 = 0,
    decode_profile_graph_replay_us: u64 = 0,
    decoder_runtime_pinned_eviction_skips: usize = 0,
    mtp_masked_argmax_hits: usize = 0,
    mtp_masked_argmax_fallbacks: usize = 0,
    mtp_verify_commit_device_hits: usize = 0,
    mtp_verify_commit_device_fallbacks: usize = 0,
    mtp_verify_commit_result_downloads: usize = 0,
    mtp_verify_commit_choice_downloads: usize = 0,
    lazy_prefetch_enqueues: usize = 0,
    lazy_prefetch_duplicates: usize = 0,
    lazy_prefetch_missing: usize = 0,
    lazy_prefetch_cancelled_for_demand: usize = 0,
    lazy_prefetch_drain_calls: usize = 0,
    lazy_demand_loads: usize = 0,
    lazy_host_prefetch_hits: usize = 0,
    lazy_host_load_ns: u64 = 0,
    lazy_host_page_touch_ns: u64 = 0,
    lazy_upload_ns: u64 = 0,
    lazy_uploaded_bytes: usize = 0,
    ffn_stream_requests: usize = 0,
    ffn_stream_hits: usize = 0,
    ffn_stream_misses: usize = 0,
    ffn_stream_fallbacks: usize = 0,
    ffn_stream_evictions: usize = 0,
    ffn_stream_read_ns: u64 = 0,
    ffn_stream_h2d_ns: u64 = 0,
    ffn_stream_read_bytes: usize = 0,
    ffn_stream_uploaded_bytes: usize = 0,
    ffn_stream_resident_bytes: usize = 0,
    ffn_stream_fadvise_calls: usize = 0,
    dense_stream_requests: usize = 0,
    dense_stream_hits: usize = 0,
    dense_stream_misses: usize = 0,
    dense_stream_fallbacks: usize = 0,
    dense_stream_evictions: usize = 0,
    dense_stream_read_ns: u64 = 0,
    dense_stream_h2d_ns: u64 = 0,
    dense_stream_read_bytes: usize = 0,
    dense_stream_uploaded_bytes: usize = 0,
    dense_stream_resident_bytes: usize = 0,
    dense_stream_fadvise_calls: usize = 0,
    dense_stream_attention_loads: usize = 0,
    dense_stream_mlp_loads: usize = 0,
    dense_prefetch_enqueues: usize = 0,
    dense_prefetch_duplicates: usize = 0,
    dense_prefetch_ready_hits: usize = 0,
    dense_prefetch_inflight_steals: usize = 0,
    dense_prefetch_sync_reads: usize = 0,
    dense_prefetch_evictions: usize = 0,
    dense_prefetch_failures: usize = 0,
    dense_prefetch_host_read_ns: u64 = 0,
    dense_prefetch_demand_wait_ns: u64 = 0,
    dense_prefetch_upload_ns: u64 = 0,
    dense_prefetch_resident_bytes: usize = 0,
    dense_prefetch_read_bytes: usize = 0,
};

fn noteTopTransferSize(sizes: *[RuntimeStats.top_transfer_size_count]usize, counts: *[RuntimeStats.top_transfer_size_count]usize, bytes: usize) void {
    if (bytes == 0) return;
    for (sizes, counts) |size, *count| {
        if (size == bytes) {
            count.* += 1;
            return;
        }
    }
    var empty_index: ?usize = null;
    var min_index: usize = 0;
    for (counts, 0..) |count, i| {
        if (sizes[i] == 0) {
            empty_index = i;
            break;
        }
        if (count < counts[min_index]) min_index = i;
    }
    const target = empty_index orelse min_index;
    sizes[target] = bytes;
    counts[target] = 1;
}

fn noteUploadBucket(stats: *RuntimeStats, bytes: usize) void {
    if (bytes <= 16) {
        stats.upload_bucket_le_16 += 1;
    } else if (bytes <= 1024) {
        stats.upload_bucket_le_1k += 1;
    } else if (bytes <= 8192) {
        stats.upload_bucket_le_8k += 1;
    } else if (bytes <= 32768) {
        stats.upload_bucket_le_32k += 1;
    } else if (bytes <= 262144) {
        stats.upload_bucket_le_256k += 1;
    } else {
        stats.upload_bucket_gt_256k += 1;
    }
}

fn noteDownloadBucket(stats: *RuntimeStats, bytes: usize) void {
    if (bytes <= 16) {
        stats.download_bucket_le_16 += 1;
    } else if (bytes <= 1024) {
        stats.download_bucket_le_1k += 1;
    } else if (bytes <= 8192) {
        stats.download_bucket_le_8k += 1;
    } else if (bytes <= 32768) {
        stats.download_bucket_le_32k += 1;
    } else if (bytes <= 262144) {
        stats.download_bucket_le_256k += 1;
    } else {
        stats.download_bucket_gt_256k += 1;
    }
}

fn elapsedNsSince(start_ns: u64) u64 {
    const end_ns = platform.time.monotonicNs();
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

const max_cuda_graph_replay_slots = 8;

const CudaGraphReplaySlot = struct {
    exec: driver_mod.CUgraphExec = null,
    input_storage: buffer_mod.DeviceBuffer = .{},
    output_storage: buffer_mod.DeviceBuffer = .{},
    input: buffer_mod.DeviceBuffer = .{},
    output: buffer_mod.DeviceBuffer = .{},
    shape: ?[]i64 = null,
    elem_count: usize = 0,
    dtype: tensor_mod.DType = .f32,
    input_valid: bool = false,
    valid: bool = false,
    kv_replay_capacity_tokens: usize = 0,
    kv_replay_capacity_valid: bool = false,
};

pub const CudaCompute = struct {
    allocator: std.mem.Allocator,
    ctx: context_mod.CudaContext,
    kernels: kernels_mod.KernelModule,
    resident_weights: std.StringHashMapUnmanaged(CudaTensor) = .{},
    lazy_host_store: ?*native_compute_mod.WeightStore = null,
    lazy_device_epochs: std.StringHashMapUnmanaged(u64) = .{},
    lazy_device_bytes: usize = 0,
    lazy_device_budget_bytes: usize = 0,
    lazy_access_epoch: u64 = 1,
    decoder_runtime_linear_slots: std.AutoHashMapUnmanaged(usize, CudaDecoderRuntimeLinearSlot) = .empty,
    decoder_runtime_rms_norm_slots: std.AutoHashMapUnmanaged(usize, CudaDecoderRuntimeRmsNormSlot) = .empty,
    decoder_runtime_next_dynamic_slot: usize = 1,
    decoder_runtime_family_state: CudaDecoderRuntimeFamilyState = .{},
    run_budget: ?*run_memory.RunBudget = null,
    dense_stream_entries: std.ArrayListUnmanaged(DenseStreamEntry) = .empty,
    dense_stream_epoch: u64 = 1,
    dense_host_prefetch_entries: std.StringHashMapUnmanaged(*DenseHostPrefetchEntry) = .{},
    dense_host_prefetch: DenseHostPrefetchQueue = undefined,
    dense_host_prefetch_initialized: bool = false,
    dense_host_prefetch_epoch: u64 = 1,
    temp_buffers: std.ArrayListUnmanaged(buffer_mod.DeviceBuffer) = .empty,
    temp_pinned_slots: std.ArrayListUnmanaged(TempPinnedSlot) = .empty,
    deferred_device_frees: std.ArrayListUnmanaged(buffer_mod.DeviceBuffer) = .empty,
    deferred_device_free_bytes: usize = 0,
    temp_trace_seq: usize = 0,
    debug_cuda_graph_capture_active: bool = false,
    debug_cuda_graph_slots: [max_cuda_graph_replay_slots]CudaGraphReplaySlot = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots,
    debug_cuda_graph_active_slot: ?usize = null,
    debug_cuda_graph_prepared_slot: ?usize = null,
    debug_cuda_graph_next_evict_slot: usize = 0,
    debug_cuda_decode_scalars: buffer_mod.DeviceBuffer = .{},
    debug_cuda_graph_decode_kv_seq_len: usize = 0,
    temp_ids_masks: scratch_mod.DeviceScratch = .{},
    bf16_activation_scratch: scratch_mod.DeviceScratch = .{},
    cublaslt: ?cublaslt_mod.CublasLt = null,
    stats: RuntimeStats = .{},
    dispatch_stats: CudaDispatchStats = .{},
    owned_by_backend: bool = false,

    pub fn init(allocator: std.mem.Allocator) !CudaCompute {
        var ctx = try context_mod.CudaContext.initDefault();
        errdefer ctx.deinit();
        const kernels = try kernels_mod.KernelModule.load(&ctx);
        errdefer {
            var kernels_mut = kernels;
            kernels_mut.unload(&ctx);
        }
        const cublaslt = initCublasLtIfAvailable(&ctx);
        return .{
            .allocator = allocator,
            .ctx = ctx,
            .kernels = kernels,
            .cublaslt = cublaslt,
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*CudaCompute {
        const self = try allocator.create(CudaCompute);
        errdefer allocator.destroy(self);
        self.* = try CudaCompute.init(allocator);
        self.owned_by_backend = true;
        return self;
    }

    pub fn deinit(self: *CudaCompute) void {
        self.dispatch_stats.printIfEnabled();
        self.dispatch_stats.deinit(self.allocator);
        deinitDenseHostPrefetch(self);
        if (self.lazy_host_store) |store| {
            native_compute_mod.stopPrefetchWorker(store);
        }
        self.decoder_runtime_linear_slots.deinit(self.allocator);
        self.decoder_runtime_rms_norm_slots.deinit(self.allocator);
        var it = self.resident_weights.iterator();
        while (it.next()) |entry| {
            var tensor = entry.value_ptr.*;
            tensor.owns_buffer = true;
            tensor.owns_shape = true;
            freeCudaTensorStorage(self, &tensor);
            self.allocator.free(entry.key_ptr.*);
        }
        self.resident_weights.deinit(self.allocator);
        var lazy_it = self.lazy_device_epochs.iterator();
        while (lazy_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.lazy_device_epochs.deinit(self.allocator);
        deinitDenseStreamCache(self);
        for (&self.debug_cuda_graph_slots) |*slot| {
            deinitCudaGraphReplaySlot(self, slot);
        }
        self.debug_cuda_decode_scalars.free(&self.ctx);
        for (self.temp_pinned_slots.items) |*slot| slot.buffer.free(&self.ctx);
        self.temp_pinned_slots.deinit(self.allocator);
        for (self.temp_buffers.items) |*buffer| buffer.free(&self.ctx);
        self.temp_buffers.deinit(self.allocator);
        if (self.deferred_device_frees.items.len != 0) {
            synchronizeAndDrainDeferredDeviceFrees(self) catch {};
            drainDeferredDeviceFreesAfterSync(self);
        }
        self.deferred_device_frees.deinit(self.allocator);
        self.temp_ids_masks.deinit(&self.ctx);
        self.bf16_activation_scratch.deinit(&self.ctx);
        if (self.cublaslt) |*blas| {
            blas.deinit();
            self.cublaslt = null;
        }
        self.kernels.unload(&self.ctx);
        self.ctx.deinit();
    }

    pub fn computeBackend(self: *CudaCompute) ops.ComputeBackend {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn hasGemma4DecoderPrimitives(self: *const CudaCompute) bool {
        return self.kernels.hasGemma4DecoderPrimitives();
    }

    pub fn supportsProfile(self: *const CudaCompute, profile: CapabilityProfile) bool {
        return switch (profile) {
            .clipclap => self.kernels.hasClipClapPrimitives(),
            .deberta_reranker => self.kernels.hasDebertaRerankerPrimitives(),
            .gliner2 => self.kernels.hasGliner2Primitives(),
            .gemma4 => self.kernels.hasQuantMatmulMvpPrimitives() and
                self.kernels.hasBf16WeightPrimitives() and
                (self.kernels.hasGemma4DecoderPrimitives() or cudaAllowHostAttentionFallback()),
        };
    }

    pub fn requireProfile(self: *const CudaCompute, profile: CapabilityProfile) !void {
        if (!self.supportsProfile(profile)) return error.CudaKernelUnavailable;
    }

    pub fn snapshotStats(self: *const CudaCompute) RuntimeStats {
        var stats = self.stats;
        stats.kernel_launches = self.ctx.stats.kernel_launches;
        stats.stream_syncs = self.ctx.stats.stream_syncs;
        for (self.temp_buffers.items) |buffer| {
            stats.temp_buffer_cached_bytes += buffer.len;
        }
        for (self.temp_pinned_slots.items) |slot| {
            stats.temp_buffer_cached_bytes += slot.buffer.len;
        }
        stats.deferred_free_pending_bytes = self.deferred_device_free_bytes;
        return stats;
    }

    pub fn attachLazyHostStore(self: *CudaCompute, store: *native_compute_mod.WeightStore) !void {
        self.lazy_host_store = store;
        try self.resident_weights.ensureTotalCapacity(
            self.allocator,
            self.resident_weights.count() + store.lazy_weights.count() + 16,
        );
        try installCudaLazyHostPrefetch(self, store);
        try initDenseHostPrefetch(self);
    }

    pub fn configureRunBudget(self: *CudaCompute, run_budget: ?*run_memory.RunBudget) void {
        self.run_budget = run_budget;
        const default_lazy_budget: usize = 10 * 1024 * 1024 * 1024;
        self.lazy_device_budget_bytes = if (run_budget) |budget| blk: {
            const backend_limit = budget.limits.backend_limit_bytes;
            if (backend_limit == 0) break :blk default_lazy_budget;
            const reserved = budget.backend_kv_bytes + budget.backend_scratch_bytes;
            break :blk if (backend_limit > reserved) backend_limit - reserved else 0;
        } else default_lazy_budget;
    }

    fn noteDeviceBytes(self: *CudaCompute, bytes: usize) void {
        self.stats.device_allocated_bytes += bytes;
    }

    pub fn insertWeightFromLoaded(self: *CudaCompute, owned_key: []const u8, loaded: *const weight_source_mod.LoadedWeight) !void {
        if (loaded.quantized_storage) |storage| {
            if (cudaDequantizeQuantWeightsOnUpload() or cudaShouldDequantizeWeightOnUpload(owned_key, storage)) {
                const elem_count = try elementCountFromShape(storage.shape);
                const data = try self.allocator.alloc(f32, elem_count);
                defer self.allocator.free(data);
                try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, data);

                const shape = try self.allocator.dupe(i64, storage.shape);
                errdefer self.allocator.free(shape);
                var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
                errdefer device.free(&self.ctx);
                try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
                try synchronizeAndDrainDeferredDeviceFrees(self);
                self.stats.resident_weight_bytes += data.len * @sizeOf(f32);
                errdefer self.allocator.free(owned_key);
                try self.resident_weights.put(self.allocator, owned_key, .{
                    .buffer = device,
                    .dtype = .f32,
                    .shape = shape,
                    .elem_count = data.len,
                    .quant_type = null,
                    .owns_buffer = false,
                    .owns_shape = false,
                    .owned_by_tensor = false,
                });
                return;
            }
            const elem_count = try elementCountFromShape(storage.shape);
            const shape = try self.allocator.dupe(i64, storage.shape);
            errdefer self.allocator.free(shape);
            var device = try allocDeviceBuffer(self, storage.raw_bytes.len);
            errdefer device.free(&self.ctx);
            try copyFromHostTracked(self, device, storage.raw_bytes);
            var tc_quant = try self.prepareTensorCoreQuantOnUpload(storage.tensor_type, storage.shape, storage.raw_bytes);
            errdefer if (tc_quant) |*packed_quant| releaseDeviceBuffer(self, &packed_quant.buffer);
            try synchronizeAndDrainDeferredDeviceFrees(self);
            self.stats.resident_weight_bytes += storage.raw_bytes.len;
            errdefer self.allocator.free(owned_key);
            try self.resident_weights.put(self.allocator, owned_key, .{
                .buffer = device,
                .dtype = .u8,
                .shape = shape,
                .elem_count = elem_count,
                .quant_type = storage.tensor_type,
                .tc_quant = tc_quant,
                .owns_buffer = false,
                .owns_shape = false,
                .owned_by_tensor = false,
            });
            return;
        }
        if (loaded.quantized) return error.UnsupportedTensorType;
        if (loaded.tensor.dtype == .bf16 and loaded.tensor.shape.len >= 2) {
            return self.insertBf16WeightFromTensor(owned_key, &loaded.tensor);
        }
        if (loaded.tensor.dtype != .f32) {
            var converted = try weight_source_mod.convertToF32(self.allocator, &loaded.tensor);
            defer converted.deinit();
            return self.insertWeightFromTensor(owned_key, &converted);
        }
        try self.insertWeightFromTensor(owned_key, &loaded.tensor);
    }

    fn prepareTensorCoreQuantOnUpload(
        self: *CudaCompute,
        tensor_type: gguf_tensor_types.TensorType,
        shape: []const i64,
        raw_bytes: []const u8,
    ) !?CudaTensorCoreQuantBuffer {
        if (!cudaTensorCoreQuantRequested()) return null;
        if (!cudaTensorCoreQuantAvailable(self)) return null;
        if (shape.len != 2 or shape[0] <= 0 or shape[1] <= 0) return null;

        const out_dim: usize = @intCast(shape[0]);
        const in_dim: usize = @intCast(shape[1]);
        if (!isTensorCoreQuantLinearShape(in_dim, out_dim)) return null;

        const known = knownQuantTensorType(tensor_type) orelse return null;
        const packed_quant = switch (known) {
            .Q8_0 => try packQ8_0TensorCore(self.allocator, raw_bytes, in_dim, out_dim),
            .Q4_K => try packQ4_KTensorCore(self.allocator, raw_bytes, in_dim, out_dim),
            else => return null,
        };
        defer self.allocator.free(packed_quant.bytes);

        var device = try allocDeviceBuffer(self, packed_quant.bytes.len);
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, packed_quant.bytes);
        self.dispatch_stats.note(
            self.allocator,
            .tc_pack,
            switch (known) {
                .Q8_0 => .q8_0,
                .Q4_K => .q4_k,
                else => .none,
            },
            .tc_pack,
            .none,
            .none,
            0,
            in_dim,
            out_dim,
            packed_quant.bytes.len,
        );
        return .{
            .buffer = device,
            .layout = packed_quant.layout,
            .row_blocks = packed_quant.row_blocks,
            .bytes = packed_quant.bytes.len,
        };
    }

    pub fn insertBf16WeightFromTensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        if (tensor.dtype != .bf16) return error.UnsupportedTensorType;
        const elem_count = tensor.elementCount();
        if (tensor.data.len != elem_count * @sizeOf(u16)) return error.InvalidShape;
        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, tensor.data.len);
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, tensor.data);
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.resident_weight_bytes += tensor.data.len;
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .bf16,
            .shape = shape,
            .elem_count = elem_count,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }

    pub fn insertWeightFromTensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        if (tensor.dtype != .f32) return error.UnsupportedTensorType;
        const data = tensor.asFloat32();
        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.resident_weight_bytes += data.len * @sizeOf(f32);
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .f32,
            .shape = shape,
            .elem_count = data.len,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }
};

fn expectApproxSlice(actual: []const f32, expected: []const f32, tolerance: f32) !void {
    if (actual.len != expected.len) return error.CudaParityMismatch;
    for (actual, expected) |got, want| {
        if (@abs(got - want) > tolerance) return error.CudaParityMismatch;
    }
}

pub fn smokeDecoderRuntimeSlots(allocator: std.mem.Allocator) !void {
    var compute = try CudaCompute.init(allocator);
    defer compute.deinit();
    var cb = compute.computeBackend();

    const linear_shape = [_]i64{ 2, 2 };
    const linear_weight_data = [_]f32{ 1.0, 0.0, 0.0, 1.0 };
    var linear_weight = try tensor_mod.Tensor.initFloat32(allocator, "slot.linear.identity", &linear_shape, &linear_weight_data);
    defer linear_weight.deinit();
    try compute.insertWeightFromTensor(try allocator.dupe(u8, "slot.linear.identity"), &linear_weight);

    const norm_shape = [_]i64{2};
    const norm_weight_data = [_]f32{ 1.0, 1.0 };
    var norm_weight = try tensor_mod.Tensor.initFloat32(allocator, "slot.norm.ones", &norm_shape, &norm_weight_data);
    defer norm_weight.deinit();
    try compute.insertWeightFromTensor(try allocator.dupe(u8, "slot.norm.ones"), &norm_weight);

    const linear_weight_ct = try cb.getWeight("slot.linear.identity");
    const norm_weight_ct = try cb.getWeight("slot.norm.ones");
    const linear_slot = (try cb.decoderRuntimeEnsureLinearSlot(&.{
        .weight = linear_weight_ct,
        .bias = null,
        .in_dim = 2,
        .out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    const norm_slot = (try cb.decoderRuntimeEnsureRmsNormSlot(&.{
        .weight = norm_weight_ct,
        .hidden_size = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;

    const input_shape = [_]i32{ 1, 2 };
    const input_data = [_]f32{ 3.0, 4.0 };
    const input = try cb.fromFloat32Shape(&input_data, &input_shape);
    defer cb.free(input);

    const linear_out = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = linear_slot,
        .input = input,
        .in_dim = 2,
        .out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(linear_out);
    const linear_actual = try cb.toFloat32(linear_out, allocator);
    defer allocator.free(linear_actual);
    try expectApproxSlice(linear_actual, &input_data, 0.0001);

    const pair_out = (try cb.decoderRuntimeApplyLinearPair(&.{
        .slot_a = linear_slot,
        .slot_b = linear_slot,
        .input = input,
        .in_dim = 2,
        .out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(pair_out.first);
    defer cb.free(pair_out.second);
    const pair_first = try cb.toFloat32(pair_out.first, allocator);
    defer allocator.free(pair_first);
    const pair_second = try cb.toFloat32(pair_out.second, allocator);
    defer allocator.free(pair_second);
    try expectApproxSlice(pair_first, &input_data, 0.0001);
    try expectApproxSlice(pair_second, &input_data, 0.0001);

    const qkv_out = (try cb.decoderRuntimeApplyLinearQkv(&.{
        .q_slot = linear_slot,
        .k_slot = linear_slot,
        .v_slot = linear_slot,
        .input = input,
        .in_dim = 2,
        .q_out_dim = 2,
        .kv_out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(qkv_out.first);
    defer cb.free(qkv_out.second);
    defer cb.free(qkv_out.third);
    const q_actual = try cb.toFloat32(qkv_out.first, allocator);
    defer allocator.free(q_actual);
    const k_actual = try cb.toFloat32(qkv_out.second, allocator);
    defer allocator.free(k_actual);
    const v_actual = try cb.toFloat32(qkv_out.third, allocator);
    defer allocator.free(v_actual);
    try expectApproxSlice(q_actual, &input_data, 0.0001);
    try expectApproxSlice(k_actual, &input_data, 0.0001);
    try expectApproxSlice(v_actual, &input_data, 0.0001);

    const norm_input_data = [_]f32{ 1.0, 1.0 };
    const norm_input = try cb.fromFloat32Shape(&norm_input_data, &input_shape);
    defer cb.free(norm_input);
    const norm_out = (try cb.decoderRuntimeApplyRmsNorm(&.{
        .slot = norm_slot,
        .input = norm_input,
        .hidden_size = 2,
        .eps = 0.000001,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(norm_out);
    const norm_actual = try cb.toFloat32(norm_out, allocator);
    defer allocator.free(norm_actual);
    try expectApproxSlice(norm_actual, &norm_input_data, 0.0001);

    const stats = compute.snapshotStats();
    if (stats.decoder_runtime_linear_slot_prepares == 0 or
        stats.decoder_runtime_rms_norm_slot_prepares == 0 or
        stats.decoder_runtime_linear_apply_hits < 6 or
        stats.decoder_runtime_rms_norm_apply_hits == 0)
    {
        return error.CudaRuntimeSlotUnavailable;
    }
}

const CudaKvLayer = struct {
    k: buffer_mod.DeviceBuffer = .{},
    v: buffer_mod.DeviceBuffer = .{},
    capacity_tokens: usize = 0,
    token_count: usize = 0,
    row_width: usize = 0,
    position_offset: usize = 0,

    fn deinit(self: *CudaKvLayer, compute: *CudaCompute) void {
        self.k.free(&compute.ctx);
        self.v.free(&compute.ctx);
        self.* = .{};
    }
};

fn traceCudaDeviceKvGatherFailure(
    reason: []const u8,
    gather: kv_storage_runtime.DeviceKvLayerGather,
    requested_row_width: usize,
    layer: ?*const CudaKvLayer,
) void {
    if (!platform.env.getenvBool("ANTFLY_CUDA_TRACE_DEVICE_KV")) return;
    if (layer) |existing| {
        std.debug.print(
            "cuda_device_kv_trace: gather_failed reason={s} sequence={d} layer={d} requested_tokens={d} requested_row_width={d} existing_tokens={d} existing_row_width={d} existing_capacity={d} existing_position_offset={d}\n",
            .{
                reason,
                gather.sequence_id,
                gather.layer_index,
                gather.token_count,
                requested_row_width,
                existing.token_count,
                existing.row_width,
                existing.capacity_tokens,
                existing.position_offset,
            },
        );
    } else {
        std.debug.print(
            "cuda_device_kv_trace: gather_failed reason={s} sequence={d} layer={d} requested_tokens={d} requested_row_width={d} existing_tokens=0 existing_row_width=0 existing_capacity=0 existing_position_offset=0\n",
            .{ reason, gather.sequence_id, gather.layer_index, gather.token_count, requested_row_width },
        );
    }
}

const CudaKvDeviceStorage = struct {
    allocator: std.mem.Allocator,
    compute: *CudaCompute,
    layers: std.AutoHashMapUnmanaged(u64, CudaKvLayer) = .{},

    fn create(allocator: std.mem.Allocator, compute: *CudaCompute) !*CudaKvDeviceStorage {
        const self = try allocator.create(CudaKvDeviceStorage);
        self.* = .{
            .allocator = allocator,
            .compute = compute,
        };
        return self;
    }

    fn deinit(self: *CudaKvDeviceStorage) void {
        var it = self.layers.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.compute);
        }
        self.layers.deinit(self.allocator);
    }

    fn key(sequence_id: kv_storage_runtime.SequenceId, layer_index: usize) !u64 {
        if (layer_index > std.math.maxInt(u32)) return error.InvalidPagedKvState;
        return (@as(u64, sequence_id) << 32) | @as(u64, @intCast(layer_index));
    }

    fn ensureLayer(self: *CudaKvDeviceStorage, write: kv_storage_runtime.KvSuffixWrite) !*CudaKvLayer {
        const row_width = try checkedMul(@as(usize, write.num_kv_heads), @as(usize, write.head_dim));
        const layer_key = try key(write.sequence_id, write.layer_index);
        const gop = try self.layers.getOrPut(self.allocator, layer_key);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        const layer = gop.value_ptr;
        if (layer.row_width != 0 and layer.row_width != row_width) return error.InvalidKvShape;
        layer.row_width = row_width;
        layer.position_offset = write.position_offset;

        const required_capacity = if (cudaDebugGraphPersistentReplayEnabled())
            try std.math.add(usize, write.total_token_count, 256)
        else
            write.total_token_count;
        if (layer.capacity_tokens < required_capacity) {
            const new_capacity = @max(required_capacity, @max(@as(usize, 16), layer.capacity_tokens * 2));
            const bytes = try checkedMul(try checkedMul(new_capacity, row_width), @sizeOf(f32));
            var new_k = try buffer_mod.DeviceBuffer.alloc(&self.compute.ctx, bytes);
            errdefer new_k.free(&self.compute.ctx);
            var new_v = try buffer_mod.DeviceBuffer.alloc(&self.compute.ctx, bytes);
            errdefer new_v.free(&self.compute.ctx);
            self.compute.noteDeviceBytes(bytes * 2);
            if (layer.token_count != 0) {
                const old_bytes = try checkedMul(try checkedMul(layer.token_count, row_width), @sizeOf(f32));
                try new_k.copyFromDevice(&self.compute.ctx, layer.k, old_bytes);
                try new_v.copyFromDevice(&self.compute.ctx, layer.v, old_bytes);
            }
            layer.k.free(&self.compute.ctx);
            layer.v.free(&self.compute.ctx);
            layer.k = new_k;
            layer.v = new_v;
            layer.capacity_tokens = new_capacity;
        }
        if (cudaDebugGraphPersistentReplayEnabled() and self.compute.debug_cuda_graph_capture_active) {
            const replay_capacity = if (cudaDebugGraphForcedKvReplayCapacityTokens()) |forced|
                @min(layer.capacity_tokens, forced)
            else
                layer.capacity_tokens;
            if (self.compute.debug_cuda_graph_active_slot) |slot_idx| {
                const slot = &self.compute.debug_cuda_graph_slots[slot_idx];
                if (!slot.kv_replay_capacity_valid or replay_capacity < slot.kv_replay_capacity_tokens) {
                    slot.kv_replay_capacity_tokens = replay_capacity;
                    slot.kv_replay_capacity_valid = true;
                }
            }
        }
        return layer;
    }

    fn writeLayerKvSuffix(
        ctx: *anyopaque,
        write: kv_storage_runtime.KvSuffixWrite,
        k_ref: kv_storage_runtime.DeviceKvRef,
        v_ref: kv_storage_runtime.DeviceKvRef,
    ) anyerror!void {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        if (write.suffix_token_count == 0) return;
        if (write.suffix_token_count > write.total_token_count) return error.InvalidKvShape;
        const row_width = try checkedMul(@as(usize, write.num_kv_heads), @as(usize, write.head_dim));
        const suffix_bytes = try checkedMul(try checkedMul(write.suffix_token_count, row_width), @sizeOf(f32));
        if (k_ref.byte_len < suffix_bytes or v_ref.byte_len < suffix_bytes) return error.InvalidKvShape;
        const layer = try self.ensureLayer(write);
        const token_start = write.total_token_count - write.suffix_token_count;
        const dst_offset = try checkedMul(try checkedMul(token_start, row_width), @sizeOf(f32));
        const k_src = buffer_mod.DeviceBuffer{ .ptr = @as(@TypeOf(layer.k.ptr), @intCast(@intFromPtr(k_ref.handle) + k_ref.byte_offset)), .len = suffix_bytes };
        const v_src = buffer_mod.DeviceBuffer{ .ptr = @as(@TypeOf(layer.v.ptr), @intCast(@intFromPtr(v_ref.handle) + v_ref.byte_offset)), .len = suffix_bytes };
        if (cudaDebugDecodeScalarsReady(self.compute) and write.suffix_token_count == 1) {
            try self.compute.kernels.launchKvWriteSuffixDecodeScalarsF32(
                &self.compute.ctx,
                layer.k,
                layer.v,
                k_src,
                v_src,
                self.compute.debug_cuda_decode_scalars,
                write.suffix_token_count,
                row_width,
                write.total_token_count,
            );
            layer.token_count = write.total_token_count;
            layer.position_offset = write.position_offset;
            self.compute.stats.device_kv_writes += 1;
            return;
        }
        const k_dst = buffer_mod.DeviceBuffer{ .ptr = layer.k.ptr + dst_offset, .len = layer.k.len - dst_offset };
        const v_dst = buffer_mod.DeviceBuffer{ .ptr = layer.v.ptr + dst_offset, .len = layer.v.len - dst_offset };
        try k_dst.copyFromDevice(&self.compute.ctx, k_src, suffix_bytes);
        try v_dst.copyFromDevice(&self.compute.ctx, v_src, suffix_bytes);
        layer.token_count = write.total_token_count;
        layer.position_offset = write.position_offset;
        self.compute.stats.device_kv_writes += 1;
    }

    fn gatherLayerKvDevice(
        ctx: *anyopaque,
        gather: kv_storage_runtime.DeviceKvLayerGather,
    ) anyerror!kv_storage_runtime.DeviceKvLayer {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        const layer_key = try key(gather.sequence_id, gather.layer_index);
        const row_width = try checkedMul(@as(usize, gather.num_kv_heads), @as(usize, gather.head_dim));
        const layer = self.layers.getPtr(layer_key) orelse {
            traceCudaDeviceKvGatherFailure("missing_layer", gather, row_width, null);
            return error.DeviceReadFallback;
        };
        if (layer.row_width != row_width or layer.token_count < gather.token_count) {
            traceCudaDeviceKvGatherFailure("shape_or_tokens", gather, row_width, layer);
            return error.DeviceReadFallback;
        }
        const bytes = try checkedMul(try checkedMul(gather.token_count, row_width), @sizeOf(f32));
        self.compute.stats.device_kv_reads += 1;
        return .{
            .runtime = self,
            .k = .{ .handle = @ptrFromInt(layer.k.ptr), .byte_offset = 0, .byte_len = bytes },
            .v = .{ .handle = @ptrFromInt(layer.v.ptr), .byte_offset = 0, .byte_len = bytes },
            .token_count = gather.token_count,
            .row_width = row_width,
            .position_offset = layer.position_offset,
            .value_element_bytes = @sizeOf(f32),
        };
    }

    fn releaseSequence(ctx: *anyopaque, sequence_id: kv_storage_runtime.SequenceId) void {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        var doomed: std.ArrayList(u64) = .empty;
        defer doomed.deinit(self.allocator);
        var it = self.layers.iterator();
        while (it.next()) |entry| {
            if (@as(u32, @intCast(entry.key_ptr.* >> 32)) == sequence_id) {
                doomed.append(self.allocator, entry.key_ptr.*) catch {};
            }
        }
        for (doomed.items) |layer_key| {
            if (self.layers.fetchRemove(layer_key)) |entry| {
                var layer = entry.value;
                layer.deinit(self.compute);
            }
        }
    }

    fn hookDeinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        self.deinit();
        allocator.destroy(self);
    }

    const hook_vtable = kv_storage_runtime.DeviceWriteHook.VTable{
        .writeLayerKvSuffix = writeLayerKvSuffix,
        .gatherLayerKvDevice = gatherLayerKvDevice,
        .releaseSequence = releaseSequence,
        .deinit = hookDeinit,
    };

    fn hook(self: *CudaKvDeviceStorage) kv_storage_runtime.DeviceWriteHook {
        return .{ .ctx = self, .vtable = &hook_vtable };
    }
};

fn cudaDequantizeQuantWeightsOnUpload() bool {
    return platform.env.getenvBoolDefault("TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS", false);
}

fn cudaCublasLtEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CUBLASLT", true);
}

fn cudaFfnStreamEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_FFN_STREAM");
}

fn cudaFfnStreamProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_FFN_STREAM_PROFILE");
}

fn cudaAutoDenseStreamEnabled(self: *const CudaCompute) bool {
    const store = self.lazy_host_store orelse return false;
    const tensor_store = store.tensor_store orelse return false;
    return tensor_store.kind() == .safetensors and store.lazy_weights.count() >= 128;
}

fn cudaDenseStreamEnabled(self: *const CudaCompute) bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DENSE_STREAM", cudaAutoDenseStreamEnabled(self)) or cudaFfnStreamEnabled();
}

fn cudaDenseStreamProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DENSE_STREAM_PROFILE") or cudaFfnStreamProfileEnabled();
}

fn cudaDenseStreamBudgetBytes(self: *const CudaCompute) usize {
    const mib: usize = 1024 * 1024;
    if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DENSE_STREAM_BUDGET_MB")) |requested_mb| {
        const budget_mb = @max(@as(usize, 512), requested_mb);
        return budget_mb * mib;
    }

    const min_auto_mb: usize = 2048;
    const max_auto_mb: usize = 12000;
    const budget = self.run_budget orelse return min_auto_mb * mib;
    const backend_limit = budget.limits.backend_limit_bytes;
    if (backend_limit == 0) return min_auto_mb * mib;
    const reserved = budget.backend_kv_bytes + budget.backend_scratch_bytes;
    if (backend_limit <= reserved) return min_auto_mb * mib;
    const available_mb = (backend_limit - reserved) / mib;
    const derived_mb = (available_mb * 2) / 3;
    const budget_mb = @min(max_auto_mb, @max(min_auto_mb, derived_mb));
    return budget_mb * mib;
}

fn cudaDensePrefetchEnabled(self: *const CudaCompute) bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DENSE_PREFETCH", cudaAutoDenseStreamEnabled(self));
}

fn cudaDensePrefetchProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DENSE_PREFETCH_PROFILE") or cudaDenseStreamProfileEnabled();
}

fn cudaDenseHostPrefetchBudgetBytes() usize {
    const requested_mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DENSE_HOST_PREFETCH_MB") orelse 4096;
    const budget_mb = @max(@as(usize, 512), requested_mb);
    return budget_mb * 1024 * 1024;
}

fn initCublasLtIfAvailable(ctx: *const context_mod.CudaContext) ?cublaslt_mod.CublasLt {
    if (!cudaCublasLtEnabled()) return null;
    if (ctx.info.compute_major < 8) return null;
    return cublaslt_mod.CublasLt.open() catch null;
}

fn cudaShouldDequantizeWeightOnUpload(name: []const u8, storage: weight_source_mod.QuantizedStorage) bool {
    // Matrix weights stay quantized for CUDA matmul kernels. Small affine
    // parameters are consumed by f32 norm/elementwise kernels, so upload them
    // as f32 even if the bundle stored them in a quantized GGUF block.
    if (isKnownQuantStorage(storage, .Q6_K)) return true;
    if (storage.shape.len < 2) return true;
    if (std.mem.endsWith(u8, name, ".bias")) return true;
    if (std.mem.eql(u8, name, "count_embed.pos_embedding.weight")) return true;
    if (std.mem.eql(u8, name, "encoder.rel_embeddings.weight")) return true;
    if (std.mem.indexOf(u8, name, ".norm") != null) return true;
    if (std.mem.indexOf(u8, name, "layer_norm") != null) return true;
    return false;
}

fn isKnownQuantStorage(storage: weight_source_mod.QuantizedStorage, known: gguf_tensor_types.KnownTensorType) bool {
    return switch (storage.tensor_type) {
        .known => |actual| actual == known,
        else => false,
    };
}

fn cudaGlinerSpanQ4KernelsEnabled() bool {
    if (platform.env.getenvBool("TERMITE_CUDA_DISABLE_GLINER_SPAN_Q4_KERNELS")) return false;
    return platform.env.getenvBoolDefault("TERMITE_CUDA_ENABLE_GLINER_SPAN_Q4_KERNELS", true);
}

fn cudaDispatchStatsEnabled() bool {
    return platform.env.getenvBool("ANTFLY_CUDA_DISPATCH_STATS");
}

fn cudaQ8TiledKernelsEnabled() bool {
    return !platform.env.getenvBool("ANTFLY_CUDA_DISABLE_Q8_TILED");
}

fn cudaQ4FusionKernelsEnabled() bool {
    if (platform.env.getenvBool("ANTFLY_CUDA_DISABLE_Q4_FUSIONS")) return false;
    return platform.env.getenvBool("ANTFLY_CUDA_ENABLE_Q4_FUSIONS");
}

const QMatmulVariantChoice = enum {
    auto,
    legacy,
    fast_r2c4,
    fast_r2c8,
    fast_r4c4,
    tc_hmma,
};

fn cudaQMatmulVariantChoice() QMatmulVariantChoice {
    const raw = platform.env.getenv("ANTFLY_CUDA_QMATMUL_VARIANT") orelse return .auto;
    if (std.ascii.eqlIgnoreCase(raw, "legacy")) return .legacy;
    if (std.ascii.eqlIgnoreCase(raw, "fast_r2c4")) return .fast_r2c4;
    if (std.ascii.eqlIgnoreCase(raw, "r2c4")) return .fast_r2c4;
    if (std.ascii.eqlIgnoreCase(raw, "fast_r2c8")) return .fast_r2c8;
    if (std.ascii.eqlIgnoreCase(raw, "r2c8")) return .fast_r2c8;
    if (std.ascii.eqlIgnoreCase(raw, "fast_r4c4")) return .fast_r4c4;
    if (std.ascii.eqlIgnoreCase(raw, "r4c4")) return .fast_r4c4;
    if (std.ascii.eqlIgnoreCase(raw, "tc_hmma")) return .tc_hmma;
    if (std.ascii.eqlIgnoreCase(raw, "hmma")) return .tc_hmma;
    if (std.ascii.eqlIgnoreCase(raw, "tensor_core")) return .tc_hmma;
    return .auto;
}

fn cudaQMatmulKernelVariant() ?kernels_mod.QMatmulVariant {
    return switch (cudaQMatmulVariantChoice()) {
        .legacy => null,
        .auto => .tc_hmma,
        .fast_r2c4 => .fast_r2c4,
        .fast_r2c8 => .fast_r2c8,
        .fast_r4c4 => .fast_r4c4,
        .tc_hmma => .tc_hmma,
    };
}

fn isApprovedQuantLinearShape(in_dim: usize, out_dim: usize) bool {
    if (in_dim == 512 and out_dim == 128) return true;
    if (in_dim == 256 and out_dim == 256) return true;
    if (in_dim == 256 and out_dim == 1024) return true;
    if (in_dim == 512 and out_dim == 512) return true;
    if (in_dim == 512 and out_dim == 1024) return true;
    if (in_dim == 512 and out_dim == 2048) return true;
    if (in_dim == 768 and out_dim == 768) return true;
    if (in_dim == 768 and out_dim == 3072) return true;
    if (in_dim == 1024 and out_dim == 256) return true;
    if (in_dim == 1024 and out_dim == 512) return true;
    if (in_dim == 1024 and out_dim == 1024) return true;
    if (in_dim == 1024 and out_dim == 4096) return true;
    if (in_dim == 1536 and out_dim == 3072) return true;
    if (in_dim == 2048 and out_dim == 512) return true;
    if (in_dim == 2048 and out_dim == 1024) return true;
    if (in_dim == 3072 and out_dim == 768) return true;
    if (in_dim == 3072 and out_dim == 1536) return true;
    if (in_dim == 4096 and out_dim == 1024) return true;
    return false;
}

fn isTensorCoreQuantLinearShape(in_dim: usize, out_dim: usize) bool {
    if (out_dim == 1) return false;
    if (in_dim == 0 or in_dim % 256 != 0) return false;
    return isApprovedQuantLinearShape(in_dim, out_dim);
}

fn useMxbaiQ8TiledKernel(rows: usize, in_dim: usize, out_dim: usize) bool {
    return cudaQ8TiledKernelsEnabled() and rows > 0 and in_dim % 256 == 0 and isApprovedQuantLinearShape(in_dim, out_dim);
}

fn mxbaiQ8Variant(rows: usize, in_dim: usize, out_dim: usize) ?kernels_mod.QMatmulVariant {
    if (!useMxbaiQ8TiledKernel(rows, in_dim, out_dim)) return null;
    const variant = cudaQMatmulKernelVariant() orelse return null;
    if (variant == .fast_r2c8) return null;
    return variant;
}

fn useMxbaiQ4FusionKernel(rows: usize, in_dim: usize, out_dim: usize) bool {
    return (cudaQ4FusionKernelsEnabled() or cudaTensorCoreQuantRequested()) and rows >= 2 and in_dim % 256 == 0 and isApprovedQuantLinearShape(in_dim, out_dim);
}

fn mxbaiQ4Variant(rows: usize, in_dim: usize, out_dim: usize) ?kernels_mod.QMatmulVariant {
    if (!useMxbaiQ4FusionKernel(rows, in_dim, out_dim)) return null;
    return cudaQMatmulKernelVariant();
}

fn mxbaiQ4TiledVariant(rows: usize, in_dim: usize, out_dim: usize) ?kernels_mod.QMatmulVariant {
    if (rows < 2 or in_dim % 256 != 0 or !isApprovedQuantLinearShape(in_dim, out_dim)) return null;
    return cudaQMatmulKernelVariant();
}

fn cudaTensorCoreQuantRequested() bool {
    return switch (cudaQMatmulVariantChoice()) {
        .auto, .tc_hmma => true,
        .legacy, .fast_r2c4, .fast_r2c8, .fast_r4c4 => false,
    };
}

fn cudaTensorCoreQuantAvailable(self: *const CudaCompute) bool {
    return self.ctx.info.compute_major >= 8;
}

fn knownQuantTensorType(tensor_type: gguf_tensor_types.TensorType) ?gguf_tensor_types.KnownTensorType {
    return switch (tensor_type) {
        .known => |known| known,
        else => null,
    };
}

const Q8_0_VALUES_PER_BLOCK: usize = 32;
const Q8_0_BLOCK_BYTES: usize = 34;
const Q8_0_TC_SCALE_BYTES: usize = 2;
const Q8_0_TC_Q_BYTES: usize = 32;
const Q4_K_VALUES_PER_BLOCK: usize = 256;
const Q4_K_BLOCK_BYTES: usize = 144;
const Q4_K_TC_META_BYTES: usize = 20;
const Q4_K_TC_Q_BYTES: usize = 128;
const Q4_K_TC_BLOCK_BYTES: usize = Q4_K_TC_META_BYTES + Q4_K_TC_Q_BYTES;

const PackedTensorCoreQuant = struct {
    layout: CudaTensorCoreQuantLayout,
    row_blocks: usize,
    bytes: []u8,
};

fn packQ8_0TensorCore(allocator: std.mem.Allocator, raw: []const u8, in_dim: usize, out_dim: usize) !PackedTensorCoreQuant {
    if (in_dim == 0 or in_dim % Q8_0_VALUES_PER_BLOCK != 0) return error.InvalidShape;
    const row_blocks = in_dim / Q8_0_VALUES_PER_BLOCK;
    const block_count = try checkedMul(out_dim, row_blocks);
    const expected_raw = try checkedMul(block_count, Q8_0_BLOCK_BYTES);
    if (raw.len != expected_raw) return error.InvalidShape;

    const scales_bytes = try checkedMul(block_count, Q8_0_TC_SCALE_BYTES);
    const q_bytes = try checkedMul(block_count, Q8_0_TC_Q_BYTES);
    const out = try allocator.alloc(u8, try checkedAdd(scales_bytes, q_bytes));
    errdefer allocator.free(out);

    const q_base = scales_bytes;
    for (0..block_count) |block| {
        const src = raw[block * Q8_0_BLOCK_BYTES ..][0..Q8_0_BLOCK_BYTES];
        @memcpy(out[block * Q8_0_TC_SCALE_BYTES ..][0..Q8_0_TC_SCALE_BYTES], src[0..2]);
        @memcpy(out[q_base + block * Q8_0_TC_Q_BYTES ..][0..Q8_0_TC_Q_BYTES], src[2..34]);
    }

    return .{
        .layout = .q8_0_hmma,
        .row_blocks = row_blocks,
        .bytes = out,
    };
}

fn packQ4_KTensorCore(allocator: std.mem.Allocator, raw: []const u8, in_dim: usize, out_dim: usize) !PackedTensorCoreQuant {
    if (in_dim == 0 or in_dim % Q4_K_VALUES_PER_BLOCK != 0) return error.InvalidShape;
    const row_blocks = in_dim / Q4_K_VALUES_PER_BLOCK;
    const block_count = try checkedMul(out_dim, row_blocks);
    const expected_raw = try checkedMul(block_count, Q4_K_BLOCK_BYTES);
    if (raw.len != expected_raw) return error.InvalidShape;

    const meta_bytes = try checkedMul(block_count, Q4_K_TC_META_BYTES);
    const q_bytes = try checkedMul(block_count, Q4_K_TC_Q_BYTES);
    const out = try allocator.alloc(u8, try checkedAdd(meta_bytes, q_bytes));
    errdefer allocator.free(out);

    const q_base = meta_bytes;
    for (0..block_count) |block| {
        const src = raw[block * Q4_K_BLOCK_BYTES ..][0..Q4_K_BLOCK_BYTES];
        const meta = out[block * Q4_K_TC_META_BYTES ..][0..Q4_K_TC_META_BYTES];
        @memcpy(meta[0..4], src[0..4]);
        const scales = src[4..16];
        for (0..8) |sub| {
            meta[4 + sub] = q4KScale(scales, sub);
            meta[12 + sub] = q4KMin(scales, sub);
        }
        @memcpy(out[q_base + block * Q4_K_TC_Q_BYTES ..][0..Q4_K_TC_Q_BYTES], src[16..144]);
    }

    return .{
        .layout = .q4_k_hmma,
        .row_blocks = row_blocks,
        .bytes = out,
    };
}

fn q4KScale(scales: []const u8, sub: usize) u8 {
    std.debug.assert(scales.len >= 12);
    if (sub < 4) return scales[sub] & 63;
    return (scales[sub + 4] & 0x0f) | ((scales[sub - 4] >> 6) << 4);
}

fn q4KMin(scales: []const u8, sub: usize) u8 {
    std.debug.assert(scales.len >= 12);
    if (sub < 4) return scales[sub + 4] & 63;
    return (scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4);
}

fn isGlinerSpanQ4Shape(rows: usize, in_dim: usize, out_dim: usize) bool {
    if (rows < 2) return false;
    if (in_dim == 768 and out_dim == 3072) return true;
    if (in_dim == 3072 and out_dim == 768) return true;
    if (in_dim == 1536 and out_dim == 3072) return true;
    return false;
}

fn useGlinerSpanQ4Kernel(self: *const CudaCompute, rows: usize, in_dim: usize, out_dim: usize) bool {
    return cudaGlinerSpanQ4KernelsEnabled() and
        self.kernels.hasGlinerSpanQ4KPrimitives() and
        isGlinerSpanQ4Shape(rows, in_dim, out_dim);
}

fn tensorFromCt(tensor: CT) *CudaTensor {
    return @ptrCast(@alignCast(tensor));
}

fn unsupportedCt() anyerror!CT {
    return error.CudaOpUnsupported;
}

fn backendKind(_: *anyopaque) ops.BackendKind {
    return .cuda;
}

fn provisionKvDeviceWriteHook(ctx: *anyopaque, storage: *kv_storage_runtime.KvStorageRuntime) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (storage.device_write_hook != null) return;
    const config = storage.storage.config;
    if (config.dtype != .f32) return;
    if (config.num_kv_heads == 0 or config.head_dim == 0) return;
    const device_storage = try CudaKvDeviceStorage.create(self.allocator, self);
    errdefer {
        device_storage.deinit();
        self.allocator.destroy(device_storage);
    }
    storage.setDeviceWriteHook(device_storage.hook());
}

fn deinitBackend(ctx: *anyopaque) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const owned = self.owned_by_backend;
    if (owned) {
        self.deinit();
        self.allocator.destroy(self);
    }
}

fn freeCudaTensorStorage(self: *CudaCompute, cuda_tensor: *CudaTensor) void {
    if (cuda_tensor.tc_quant) |*tc_quant| {
        var packed_buffer = tc_quant.buffer;
        releaseDeviceBuffer(self, &packed_buffer);
        cuda_tensor.tc_quant = null;
    }
    if (cuda_tensor.owns_buffer) releaseDeviceBuffer(self, &cuda_tensor.buffer);
    if (cuda_tensor.owns_shape) self.allocator.free(cuda_tensor.shape);
}

fn freeCudaTensorStorageUncached(self: *CudaCompute, cuda_tensor: *CudaTensor) void {
    if (cuda_tensor.tc_quant) |*tc_quant| {
        if (tc_quant.buffer.ptr != 0) {
            self.stats.device_free_calls += 1;
            tc_quant.buffer.free(&self.ctx);
        }
        cuda_tensor.tc_quant = null;
    }
    if (cuda_tensor.owns_buffer and cuda_tensor.buffer.ptr != 0) {
        self.stats.device_free_calls += 1;
        cuda_tensor.buffer.free(&self.ctx);
    }
    if (cuda_tensor.owns_shape) self.allocator.free(cuda_tensor.shape);
}

fn deinitDenseStreamCache(self: *CudaCompute) void {
    for (self.dense_stream_entries.items) |*entry| {
        freeCudaTensorStorageUncached(self, entry.tensor);
        self.allocator.destroy(entry.tensor);
        self.allocator.free(entry.name);
    }
    self.dense_stream_entries.deinit(self.allocator);
    self.stats.ffn_stream_resident_bytes = 0;
    self.stats.dense_stream_resident_bytes = 0;
}

fn initDenseHostPrefetch(self: *CudaCompute) !void {
    if (!cudaDensePrefetchEnabled(self)) return;
    if (self.dense_host_prefetch_initialized) return;
    if (self.lazy_host_store == null) return;
    self.dense_host_prefetch = DenseHostPrefetchQueue.initWithPriorityUnlocked(
        self.allocator,
        self,
        &denseHostPrefetchProcess,
        &denseHostPrefetchPriority,
    );
    self.dense_host_prefetch_initialized = true;
    if (!disableCudaLazyHostPrefetchWorker()) {
        try self.dense_host_prefetch.start();
    }
}

fn deinitDenseHostPrefetch(self: *CudaCompute) void {
    if (self.dense_host_prefetch_initialized) {
        self.dense_host_prefetch.deinit();
        self.dense_host_prefetch_initialized = false;
    }
    var it = self.dense_host_prefetch_entries.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.*.deinit(self.allocator);
        self.allocator.destroy(entry.value_ptr.*);
    }
    self.dense_host_prefetch_entries.deinit(self.allocator);
    self.stats.dense_prefetch_resident_bytes = 0;
}

fn cloneDenseHostRange(allocator: std.mem.Allocator, range: tensor_store_mod.TensorRangeRef) !DenseHostRange {
    const path = try allocator.dupe(u8, range.path);
    errdefer allocator.free(path);
    const shape = try allocator.dupe(i64, range.shape);
    errdefer allocator.free(shape);
    return .{
        .path = path,
        .byte_offset = range.byte_offset,
        .byte_len = range.byte_len,
        .dtype = range.dtype,
        .shape = shape,
    };
}

fn createDenseHostPrefetchEntry(self: *CudaCompute, name: []const u8, priority: u64) !*DenseHostPrefetchEntry {
    const store = self.lazy_host_store orelse return error.MissingWeight;
    const tensor_store = store.tensor_store orelse return error.MissingWeight;
    var tensor_range = (try tensor_store.describeTensorRange(self.allocator, name)) orelse return error.MissingWeight;
    defer tensor_range.deinit(self.allocator);
    if (tensor_range.dtype != .bf16 or tensor_range.shape.len < 2 or tensor_range.byte_len == 0) return error.UnsupportedTensorType;

    const entry = try self.allocator.create(DenseHostPrefetchEntry);
    errdefer self.allocator.destroy(entry);
    const owned_name = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_name);
    const range = try cloneDenseHostRange(self.allocator, tensor_range);
    errdefer {
        var mutable_range = range;
        mutable_range.deinit(self.allocator);
    }
    entry.* = .{
        .name = owned_name,
        .range = range,
        .kind = if (isDenseAttentionStreamWeightName(name)) .attention else .mlp,
        .priority = priority,
        .pending = true,
        .last_access = self.dense_host_prefetch_epoch,
    };
    return entry;
}

fn denseHostPrefetchPriority(entry: *DenseHostPrefetchEntry) u64 {
    return entry.priority;
}

fn readDenseHostBytes(self: *CudaCompute, path: []const u8, byte_offset: u64, byte_len: usize) !struct { host: []u8, read_ns: u64 } {
    self.stats.dense_stream_fadvise_calls += 1;
    c_file.adviseFileRange(self.allocator, path, byte_offset, byte_len, .will_need);
    const start = platform.time.monotonicNs();
    const host = try self.allocator.alloc(u8, byte_len);
    errdefer self.allocator.free(host);
    try c_file.readRegionInto(self.allocator, path, byte_offset, host);
    const read_ns = elapsedNsSince(start);
    self.stats.dense_stream_fadvise_calls += 1;
    c_file.adviseFileRange(self.allocator, path, byte_offset, byte_len, .dont_need);
    return .{ .host = host, .read_ns = read_ns };
}

fn evictDenseHostPrefetchToBudgetLocked(self: *CudaCompute, protected_name: []const u8, incoming_bytes: usize) void {
    const budget = cudaDenseHostPrefetchBudgetBytes();
    while (self.stats.dense_prefetch_resident_bytes + incoming_bytes > budget) {
        var victim_name: ?[]const u8 = null;
        var victim_entry: ?*DenseHostPrefetchEntry = null;
        var oldest_epoch: u64 = std.math.maxInt(u64);
        var it = self.dense_host_prefetch_entries.iterator();
        while (it.next()) |map_entry| {
            const entry = map_entry.value_ptr.*;
            if (!entry.ready or entry.host.len == 0) continue;
            if (std.mem.eql(u8, entry.name, protected_name)) continue;
            if (entry.last_access < oldest_epoch) {
                oldest_epoch = entry.last_access;
                victim_name = map_entry.key_ptr.*;
                victim_entry = entry;
            }
        }
        const entry = victim_entry orelse return;
        _ = self.dense_host_prefetch_entries.remove(victim_name.?);
        self.stats.dense_prefetch_resident_bytes -|= entry.host.len;
        self.stats.dense_prefetch_evictions += 1;
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
    }
}

fn removeDenseHostPrefetchQueueItemLocked(self: *CudaCompute, entry: *DenseHostPrefetchEntry) bool {
    for (self.dense_host_prefetch.items.items, 0..) |queued, index| {
        if (queued == entry) {
            _ = self.dense_host_prefetch.items.orderedRemove(index);
            entry.pending = false;
            return true;
        }
    }
    return false;
}

fn denseHostPrefetchProcess(ctx: *anyopaque, entry: *DenseHostPrefetchEntry) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.dense_host_prefetch.lock();
    if (!entry.pending) {
        self.dense_host_prefetch.unlock();
        return;
    }
    entry.pending = false;
    entry.loading = true;
    self.dense_host_prefetch.unlock();

    const loaded = readDenseHostBytes(self, entry.range.path, entry.range.byte_offset, entry.range.byte_len) catch {
        self.dense_host_prefetch.lock();
        entry.loading = false;
        entry.failed = true;
        self.stats.dense_prefetch_failures += 1;
        self.dense_host_prefetch.unlock();
        return;
    };

    self.dense_host_prefetch.lock();
    defer self.dense_host_prefetch.unlock();
    entry.host = loaded.host;
    entry.read_ns = loaded.read_ns;
    entry.loading = false;
    entry.ready = true;
    self.stats.dense_prefetch_host_read_ns +|= loaded.read_ns;
    self.stats.dense_prefetch_read_bytes += loaded.host.len;
    evictDenseHostPrefetchToBudgetLocked(self, entry.name, loaded.host.len);
    if (self.dense_host_prefetch_entries.get(entry.name) == entry) {
        self.stats.dense_prefetch_resident_bytes += loaded.host.len;
        if (cudaDensePrefetchProfileEnabled()) {
            std.log.info("cuda_dense_prefetch: loaded name={s} mb={d} read_ms={d} resident_mb={d}", .{
                entry.name,
                loaded.host.len / (1024 * 1024),
                loaded.read_ns / 1_000_000,
                self.stats.dense_prefetch_resident_bytes / (1024 * 1024),
            });
        }
    }
}

fn isDenseFfnStreamWeightName(name: []const u8) bool {
    if (std.mem.indexOf(u8, name, ".mlp.") == null) return false;
    return std.mem.endsWith(u8, name, ".mlp.gate_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.up_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.down_proj.weight");
}

fn isDenseAttentionStreamWeightName(name: []const u8) bool {
    if (std.mem.indexOf(u8, name, ".self_attn.") == null) return false;
    return std.mem.endsWith(u8, name, ".self_attn.q_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.k_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.v_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.o_proj.weight");
}

fn isDenseStreamWeightName(self: *const CudaCompute, name: []const u8) bool {
    if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DENSE_STREAM", cudaAutoDenseStreamEnabled(self))) {
        return isDenseFfnStreamWeightName(name) or isDenseAttentionStreamWeightName(name);
    }
    return cudaFfnStreamEnabled() and isDenseFfnStreamWeightName(name);
}

fn findDenseStreamEntry(self: *CudaCompute, name: []const u8) ?*DenseStreamEntry {
    for (self.dense_stream_entries.items) |*entry| {
        if (std.mem.eql(u8, entry.name, name)) return entry;
    }
    return null;
}

fn evictOneDenseStreamEntry(self: *CudaCompute, protected_name: []const u8) !void {
    var victim_index: ?usize = null;
    var oldest_epoch: u64 = std.math.maxInt(u64);
    for (self.dense_stream_entries.items, 0..) |*entry, index| {
        if (std.mem.eql(u8, entry.name, protected_name)) continue;
        if (entry.last_access < oldest_epoch) {
            oldest_epoch = entry.last_access;
            victim_index = index;
        }
    }
    const index = victim_index orelse return error.MemoryBudgetExceeded;
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.stream_syncs += 1;
    const entry = self.dense_stream_entries.orderedRemove(index);
    const bytes = entry.byte_len;
    freeCudaTensorStorageUncached(self, entry.tensor);
    self.allocator.destroy(entry.tensor);
    if (entry.kind == .mlp) {
        self.stats.ffn_stream_resident_bytes -|= bytes;
        self.stats.ffn_stream_evictions += 1;
    }
    self.stats.dense_stream_resident_bytes -|= bytes;
    self.stats.resident_weight_bytes -|= bytes;
    self.stats.dense_stream_evictions += 1;
    self.allocator.free(entry.name);
}

fn ensureDenseStreamBudgetForInsert(self: *CudaCompute, protected_name: []const u8, byte_len: usize) !void {
    const budget = cudaDenseStreamBudgetBytes(self);
    if (byte_len > budget) return error.MemoryBudgetExceeded;
    while (self.stats.dense_stream_resident_bytes + byte_len > budget) {
        try evictOneDenseStreamEntry(self, protected_name);
    }
}

fn uploadDenseWeightFromHost(
    self: *CudaCompute,
    name: []const u8,
    dtype: tensor_mod.DType,
    shape_src: []const i64,
    byte_len: usize,
    host: []const u8,
    read_ns: u64,
    from_prefetch: bool,
) !*CudaTensor {
    if (dtype != .bf16) return error.UnsupportedTensorType;
    if (shape_src.len < 2) return error.InvalidShape;
    if (byte_len == 0 or host.len != byte_len) return error.InvalidShape;
    try ensureDenseStreamBudgetForInsert(self, name, byte_len);
    const stream_kind: DenseStreamEntry.Kind = if (isDenseAttentionStreamWeightName(name)) .attention else .mlp;

    if (!from_prefetch) {
        if (stream_kind == .mlp) self.stats.ffn_stream_read_ns +|= read_ns;
        self.stats.dense_stream_read_ns +|= read_ns;
        if (stream_kind == .mlp) self.stats.ffn_stream_read_bytes += host.len;
        self.stats.dense_stream_read_bytes += host.len;
    }

    const shape = try self.allocator.dupe(i64, shape_src);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, host.len);
    errdefer device.free(&self.ctx);
    const h2d_start = platform.time.monotonicNs();
    try copyFromHostTracked(self, device, host);
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.upload_syncs += 1;
    const h2d_ns = elapsedNsSince(h2d_start);
    if (stream_kind == .mlp) self.stats.ffn_stream_h2d_ns +|= h2d_ns;
    self.stats.dense_stream_h2d_ns +|= h2d_ns;
    if (from_prefetch) self.stats.dense_prefetch_upload_ns +|= h2d_ns;
    if (stream_kind == .mlp) self.stats.ffn_stream_uploaded_bytes += host.len;
    self.stats.dense_stream_uploaded_bytes += host.len;

    const owned_name = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_name);
    const tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(tensor);
    const elem_count = byte_len / @sizeOf(u16);
    tensor.* = .{
        .buffer = device,
        .dtype = .bf16,
        .shape = shape,
        .elem_count = elem_count,
        .quant_type = null,
        .owns_buffer = true,
        .owns_shape = true,
        .owned_by_tensor = false,
    };
    self.dense_stream_epoch +|= 1;
    try self.dense_stream_entries.append(self.allocator, .{
        .name = owned_name,
        .tensor = tensor,
        .byte_len = host.len,
        .kind = stream_kind,
        .last_access = self.dense_stream_epoch,
    });
    if (stream_kind == .mlp) self.stats.ffn_stream_resident_bytes += host.len;
    self.stats.dense_stream_resident_bytes += host.len;
    self.stats.resident_weight_bytes += host.len;
    if (stream_kind == .attention) {
        self.stats.dense_stream_attention_loads += 1;
    } else {
        self.stats.dense_stream_mlp_loads += 1;
    }
    if (cudaDenseStreamProfileEnabled()) {
        std.log.info("cuda_dense_stream: loaded name={s} mb={d} read_ms={d} h2d_ms={d} entries={d} resident_mb={d}", .{
            name,
            host.len / (1024 * 1024),
            read_ns / 1_000_000,
            h2d_ns / 1_000_000,
            self.dense_stream_entries.items.len,
            self.stats.dense_stream_resident_bytes / (1024 * 1024),
        });
    }
    return tensor;
}

fn streamDenseWeightFromRange(self: *CudaCompute, name: []const u8, range: tensor_store_mod.TensorRangeRef) !*CudaTensor {
    if (range.dtype != .bf16) return error.UnsupportedTensorType;
    if (range.shape.len < 2) return error.InvalidShape;
    if (range.byte_len == 0) return error.InvalidShape;
    const is_mlp = isDenseFfnStreamWeightName(name);
    if (is_mlp) self.stats.ffn_stream_fadvise_calls += 2;
    self.stats.dense_prefetch_sync_reads += 1;
    const loaded = try readDenseHostBytes(self, range.path, range.byte_offset, range.byte_len);
    defer self.allocator.free(loaded.host);
    return try uploadDenseWeightFromHost(self, name, range.dtype, range.shape, range.byte_len, loaded.host, loaded.read_ns, false);
}

fn takeDenseHostPrefetchedWeight(self: *CudaCompute, name: []const u8) !?*CudaTensor {
    if (!cudaDensePrefetchEnabled(self) or !self.dense_host_prefetch_initialized) return null;
    const wait_start = platform.time.monotonicNs();
    var waited = false;
    while (true) {
        self.dense_host_prefetch.lock();
        const entry = self.dense_host_prefetch_entries.get(name) orelse {
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            return null;
        };
        self.dense_host_prefetch_epoch +|= 1;
        entry.last_access = self.dense_host_prefetch_epoch;
        if (entry.ready) {
            _ = self.dense_host_prefetch_entries.remove(name);
            self.stats.dense_prefetch_resident_bytes -|= entry.host.len;
            const host = entry.host;
            entry.host = &.{};
            const read_ns = entry.read_ns;
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            self.stats.dense_prefetch_ready_hits += 1;
            defer self.allocator.free(host);
            defer {
                entry.deinit(self.allocator);
                self.allocator.destroy(entry);
            }
            return try uploadDenseWeightFromHost(self, name, entry.range.dtype, entry.range.shape, entry.range.byte_len, host, read_ns, true);
        }
        if (entry.pending) {
            _ = removeDenseHostPrefetchQueueItemLocked(self, entry);
            _ = self.dense_host_prefetch_entries.remove(name);
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            self.stats.dense_prefetch_inflight_steals += 1;
            self.stats.dense_prefetch_sync_reads += 1;
            if (entry.kind == .mlp) self.stats.ffn_stream_fadvise_calls += 2;
            const loaded = try readDenseHostBytes(self, entry.range.path, entry.range.byte_offset, entry.range.byte_len);
            defer self.allocator.free(loaded.host);
            defer {
                entry.deinit(self.allocator);
                self.allocator.destroy(entry);
            }
            return try uploadDenseWeightFromHost(self, name, entry.range.dtype, entry.range.shape, entry.range.byte_len, loaded.host, loaded.read_ns, false);
        }
        if (entry.failed) {
            _ = self.dense_host_prefetch_entries.remove(name);
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            return null;
        }
        self.dense_host_prefetch.unlock();
        waited = true;
        platform.time.yieldBriefly();
    }
}

fn enqueueDenseHostPrefetchHint(self: *CudaCompute, name: []const u8, hint: u32) void {
    if (!cudaDensePrefetchEnabled(self) or !self.dense_host_prefetch_initialized) return;
    if (self.resident_weights.contains(name)) return;
    self.dense_host_prefetch.lock();
    if (findDenseStreamEntry(self, name) != null) {
        self.dense_host_prefetch.unlock();
        return;
    }
    if (self.dense_host_prefetch_entries.get(name)) |entry| {
        entry.priority +|= hint;
        self.dense_host_prefetch_epoch +|= 1;
        entry.last_access = self.dense_host_prefetch_epoch;
        self.stats.dense_prefetch_duplicates += 1;
        self.dense_host_prefetch.signal();
        self.dense_host_prefetch.unlock();
        return;
    }
    self.dense_host_prefetch.unlock();

    const entry = createDenseHostPrefetchEntry(self, name, hint) catch {
        self.stats.dense_prefetch_failures += 1;
        return;
    };
    self.dense_host_prefetch.lock();
    if (self.dense_host_prefetch_entries.get(name)) |existing| {
        existing.priority +|= hint;
        self.stats.dense_prefetch_duplicates += 1;
        self.dense_host_prefetch.unlock();
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        return;
    }
    self.dense_host_prefetch_entries.put(self.allocator, entry.name, entry) catch {
        self.dense_host_prefetch.unlock();
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        self.stats.dense_prefetch_failures += 1;
        return;
    };
    self.dense_host_prefetch.items.append(self.allocator, entry) catch {
        _ = self.dense_host_prefetch_entries.remove(entry.name);
        self.dense_host_prefetch.unlock();
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        self.stats.dense_prefetch_failures += 1;
        return;
    };
    self.stats.dense_prefetch_enqueues += 1;
    self.dense_host_prefetch.signal();
    self.dense_host_prefetch.unlock();
}

fn getDenseStreamWeight(self: *CudaCompute, name: []const u8) !?*CudaTensor {
    if (!cudaDenseStreamEnabled(self)) return null;
    if (!isDenseStreamWeightName(self, name)) return null;
    const store = self.lazy_host_store orelse return null;
    const tensor_store = store.tensor_store orelse return null;
    const is_mlp = isDenseFfnStreamWeightName(name);
    if (is_mlp) self.stats.ffn_stream_requests += 1;
    self.stats.dense_stream_requests += 1;
    self.dense_stream_epoch +|= 1;
    if (findDenseStreamEntry(self, name)) |entry| {
        entry.last_access = self.dense_stream_epoch;
        if (is_mlp) self.stats.ffn_stream_hits += 1;
        self.stats.dense_stream_hits += 1;
        return entry.tensor;
    }
    if (is_mlp) self.stats.ffn_stream_misses += 1;
    self.stats.dense_stream_misses += 1;
    if (try takeDenseHostPrefetchedWeight(self, name)) |tensor| return tensor;
    var range = (try tensor_store.describeTensorRange(self.allocator, name)) orelse return null;
    defer range.deinit(self.allocator);
    return try streamDenseWeightFromRange(self, name, range);
}

fn copyFromHostTracked(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []const u8) !void {
    try device.copyFromHost(&self.ctx, bytes);
    self.stats.h2d_bytes += bytes.len;
}

fn copyToHostTracked(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []u8) !void {
    try device.copyToHost(&self.ctx, bytes);
    self.stats.d2h_bytes += bytes.len;
}

fn copyFromDeviceTracked(self: *CudaCompute, device: buffer_mod.DeviceBuffer, src: buffer_mod.DeviceBuffer, len: usize) !void {
    try device.copyFromDevice(&self.ctx, src, len);
    self.stats.d2d_bytes += len;
}

fn stageBf16ActivationForCublasLt(
    self: *CudaCompute,
    input: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
) !?buffer_mod.DeviceBuffer {
    if (!cudaCublasLtEnabled()) return null;
    if (self.ctx.info.compute_major < 8) return null;
    if (self.cublaslt == null) return null;
    const count = try checkedMul(rows, in_dim);
    const bytes = try checkedMul(count, @sizeOf(u16));
    const scratch = self.bf16_activation_scratch.acquire(&self.ctx, bytes) catch return null;
    self.kernels.launchF32ToBf16(&self.ctx, scratch, input, count) catch return null;
    self.stats.bf16_cublaslt_activation_staging_calls += 1;
    return scratch;
}

fn tryCublasLtBf16Linear(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const input_bf16 = try stageBf16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const blas = &(self.cublaslt orelse return false);
    blas.matmulBf16WeightF32Out(&self.ctx, dst, input_bf16, weight, rows, in_dim, out_dim) catch return false;
    self.stats.bf16_cublaslt_linear_calls += 1;
    return true;
}

fn tryCublasLtBf16Qkv(
    self: *CudaCompute,
    dst_q: buffer_mod.DeviceBuffer,
    dst_k: buffer_mod.DeviceBuffer,
    dst_v: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight_q: buffer_mod.DeviceBuffer,
    weight_k: buffer_mod.DeviceBuffer,
    weight_v: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
) !bool {
    const input_bf16 = try stageBf16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const blas = &(self.cublaslt orelse return false);
    blas.matmulBf16WeightF32Out(&self.ctx, dst_q, input_bf16, weight_q, rows, in_dim, q_out_dim) catch return false;
    blas.matmulBf16WeightF32Out(&self.ctx, dst_k, input_bf16, weight_k, rows, in_dim, kv_out_dim) catch return false;
    blas.matmulBf16WeightF32Out(&self.ctx, dst_v, input_bf16, weight_v, rows, in_dim, kv_out_dim) catch return false;
    self.stats.bf16_cublaslt_qkv_calls += 1;
    return true;
}

const max_temp_buffers = 256;

fn cudaTempCacheBudgetBytes() usize {
    const mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB") orelse 1024;
    return mb * 1024 * 1024;
}

fn cudaDeferredFreeEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DEFER_FREE", true);
}

fn cudaDeferredFreeBudgetBytes() usize {
    const mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DEFER_FREE_BUDGET_MB") orelse 512;
    return mb * 1024 * 1024;
}

fn cudaTempTraceEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TEMP_TRACE", false);
}

fn cudaTempTraceLimit() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_TRACE_LIMIT") orelse 4096;
}

fn cudaTempTraceSkip() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_TRACE_SKIP") orelse 0;
}

fn cudaTempStableReuseEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TEMP_STABLE_REUSE", false);
}

const max_temp_pinned_slots = 16_384;

fn cudaTempSlotPeriod() usize {
    const period = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD") orelse 0;
    if (period > max_temp_pinned_slots) return 0;
    return period;
}

fn cudaTempSlotSkip() usize {
    if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP")) |configured| return configured;
    if (cudaMtpTargetReplayEnabled()) return 2500;
    return cudaTempTraceSkip();
}

fn cudaDebugGraphCaptureMinAllocSeq() usize {
    if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_MIN_ALLOC_SEQ")) |seq| return seq;
    const period = cudaTempSlotPeriod();
    if (period != 0) return cudaTempSlotSkip() + period;
    return 0;
}

fn cudaDebugGraphCaptureUpdateExecEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC", false);
}

fn cudaMtpTargetReplayEnabled() bool {
    const raw = platform.env.getenv("ANTFLY_GEMMA4_MTP_TARGET_REPLAY") orelse return true;
    if (std.mem.eql(u8, raw, "off")) return false;
    if (std.mem.eql(u8, raw, "0")) return false;
    if (std.mem.eql(u8, raw, "false")) return false;
    return true;
}

fn cudaMtpVerifyDeviceResultEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_VERIFY_DEVICE_RESULT", false);
}

fn cudaDebugGraphPersistentReplayEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY", false) or
        cudaMtpTargetReplayEnabled();
}

fn cudaDebugGraphForcedKvReplayCapacityTokens() ?usize {
    const forced = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY") orelse return null;
    return if (forced == 0) null else forced;
}

fn cudaDecodeProfileEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PROFILE_DECODE", false);
}

const CudaDecodeProfileCategory = enum {
    qkv,
    gqa_attention,
    attention_output,
    attention_norm_residual,
    ffn_gate_up,
    ffn_gated_down,
    ffn_post_norm,
    lm_head_argmax,
    graph_replay,
};

const CudaDecodeProfileScope = struct {
    compute: *CudaCompute,
    category: CudaDecodeProfileCategory,
    pair: context_mod.ProfileEventPair,

    fn end(self: *CudaDecodeProfileScope) void {
        const elapsed_us = self.compute.ctx.endProfileEventPairUs(self.pair) catch return;
        noteDecodeProfileUs(self.compute, self.category, elapsed_us);
    }
};

fn beginDecodeProfile(self: *CudaCompute, category: CudaDecodeProfileCategory, rows: usize) ?CudaDecodeProfileScope {
    if (rows != 1) return null;
    if (!cudaDecodeProfileEnabled()) return null;
    if (self.debug_cuda_graph_capture_active) return null;
    const pair = self.ctx.beginProfileEventPair() catch return null;
    return .{ .compute = self, .category = category, .pair = pair };
}

fn noteDecodeProfileUs(self: *CudaCompute, category: CudaDecodeProfileCategory, elapsed_us: u64) void {
    self.stats.decode_profile_events += 1;
    switch (category) {
        .qkv => self.stats.decode_profile_qkv_us += elapsed_us,
        .gqa_attention => self.stats.decode_profile_gqa_attention_us += elapsed_us,
        .attention_output => self.stats.decode_profile_attention_output_us += elapsed_us,
        .attention_norm_residual => self.stats.decode_profile_attention_norm_residual_us += elapsed_us,
        .ffn_gate_up => self.stats.decode_profile_ffn_gate_up_us += elapsed_us,
        .ffn_gated_down => self.stats.decode_profile_ffn_gated_down_us += elapsed_us,
        .ffn_post_norm => self.stats.decode_profile_ffn_post_norm_us += elapsed_us,
        .lm_head_argmax => self.stats.decode_profile_lm_head_argmax_us += elapsed_us,
        .graph_replay => self.stats.decode_profile_graph_replay_us += elapsed_us,
    }
}

fn cudaDebugGraphCaptureDeviceScalarsEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS", false) or
        cudaMtpTargetReplayEnabled();
}

fn cudaDebugGraphCaptureTensorTraceEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_TENSOR_TRACE", false);
}

fn cudaDebugGraphCaptureTensorTraceLimit() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_TENSOR_TRACE_LIMIT") orelse 64;
}

fn cudaDebugDecodeScalarsReady(self: *const CudaCompute) bool {
    return cudaDebugGraphCaptureDeviceScalarsEnabled() and self.debug_cuda_graph_capture_active and self.debug_cuda_decode_scalars.ptr != 0;
}

fn cudaGraphExecUpdateResultName(result: driver_mod.CUgraphExecUpdateResult) []const u8 {
    return switch (result) {
        driver_mod.CU_GRAPH_EXEC_UPDATE_SUCCESS => "success",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR => "error",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_TOPOLOGY_CHANGED => "topology_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_NODE_TYPE_CHANGED => "node_type_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_FUNCTION_CHANGED => "function_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_PARAMETERS_CHANGED => "parameters_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_NOT_SUPPORTED => "not_supported",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_UNSUPPORTED_FUNCTION_CHANGE => "unsupported_function_change",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_ATTRIBUTES_CHANGED => "attributes_changed",
        else => "unknown",
    };
}

fn traceCudaTempAlloc(
    self: *CudaCompute,
    seq: usize,
    comptime event: []const u8,
    requested_len: usize,
    buffer: buffer_mod.DeviceBuffer,
) void {
    if (!cudaTempTraceEnabled()) return;
    const skip = cudaTempTraceSkip();
    if (seq < skip) return;
    const trace_seq = seq - skip;
    const limit = cudaTempTraceLimit();
    if (trace_seq >= limit) return;
    if (trace_seq == 0) {
        std.log.info(
            "cuda_temp_trace: begin skip={d} limit={d} temp_cache_budget_mb={d} stable_reuse={} slot_period={d} slot_skip={d}",
            .{
                skip,
                limit,
                cudaTempCacheBudgetBytes() / (1024 * 1024),
                cudaTempStableReuseEnabled(),
                cudaTempSlotPeriod(),
                cudaTempSlotSkip(),
            },
        );
    }
    std.log.info(
        "cuda_temp_trace: seq={d} trace_seq={d} event={s} requested={d} buffer_len={d} ptr=0x{x} cache_count={d}",
        .{ seq, trace_seq, event, requested_len, buffer.len, buffer.ptr, self.temp_buffers.items.len },
    );
    if (trace_seq + 1 == limit) {
        std.log.info("cuda_temp_trace: limit_reached limit={d}", .{limit});
    }
}

fn ensureTempPinnedSlots(self: *CudaCompute, period: usize) !void {
    if (self.temp_pinned_slots.items.len >= period) return;
    try self.temp_pinned_slots.ensureTotalCapacity(self.allocator, period);
    while (self.temp_pinned_slots.items.len < period) {
        self.temp_pinned_slots.appendAssumeCapacity(.{});
    }
}

fn findExactTempBufferIndex(self: *const CudaCompute, len: usize) ?usize {
    for (self.temp_buffers.items, 0..) |buffer, i| {
        if (buffer.len == len) return i;
    }
    return null;
}

fn allocPinnedTempSlot(
    self: *CudaCompute,
    seq: usize,
    len: usize,
) !?buffer_mod.DeviceBuffer {
    const period = cudaTempSlotPeriod();
    if (period == 0) return null;
    const skip = cudaTempSlotSkip();
    if (seq < skip) return null;

    try ensureTempPinnedSlots(self, period);
    const slot_index = (seq - skip) % period;
    const slot = &self.temp_pinned_slots.items[slot_index];
    if (slot.in_use) {
        traceCudaTempAlloc(self, seq, "alloc_slot_busy_fallback", len, .{});
        return null;
    }
    if (slot.requested_len != 0 and slot.requested_len != len) {
        traceCudaTempAlloc(self, seq, "alloc_slot_shape_fallback", len, .{});
        return null;
    }
    if (slot.buffer.ptr == 0) {
        slot.requested_len = len;
        if (findExactTempBufferIndex(self, len)) |i| {
            slot.buffer = self.temp_buffers.orderedRemove(i);
            self.stats.temp_buffer_hits += 1;
            slot.in_use = true;
            traceCudaTempAlloc(self, seq, "alloc_slot_seed", len, slot.buffer);
            return slot.buffer;
        }
        self.stats.temp_buffer_misses += 1;
        slot.buffer = try buffer_mod.DeviceBuffer.alloc(&self.ctx, len);
        self.noteDeviceBytes(len);
        self.stats.device_alloc_calls += 1;
        slot.in_use = true;
        traceCudaTempAlloc(self, seq, "alloc_slot_miss", len, slot.buffer);
        return slot.buffer;
    }
    slot.in_use = true;
    self.stats.temp_buffer_hits += 1;
    traceCudaTempAlloc(self, seq, "alloc_slot_hit", len, slot.buffer);
    return slot.buffer;
}

fn releasePinnedTempSlot(self: *CudaCompute, buffer: *buffer_mod.DeviceBuffer) bool {
    if (buffer.ptr == 0 or self.temp_pinned_slots.items.len == 0) return false;
    for (self.temp_pinned_slots.items) |*slot| {
        if (slot.buffer.ptr == buffer.ptr) {
            slot.in_use = false;
            buffer.* = .{};
            return true;
        }
    }
    return false;
}

fn retainPinnedTempSlot(self: *CudaCompute, buffer: buffer_mod.DeviceBuffer) bool {
    if (buffer.ptr == 0 or self.temp_pinned_slots.items.len == 0) return false;
    for (self.temp_pinned_slots.items) |*slot| {
        if (slot.buffer.ptr == buffer.ptr) {
            if (slot.in_use) return false;
            slot.in_use = true;
            return true;
        }
    }
    return false;
}

fn drainDeferredDeviceFreesAfterSync(self: *CudaCompute) void {
    if (self.deferred_device_frees.items.len == 0) return;
    var reclaimed: usize = 0;
    for (self.deferred_device_frees.items) |*buffer| {
        reclaimed += buffer.len;
        self.stats.device_free_calls += 1;
        buffer.free(&self.ctx);
    }
    self.deferred_device_frees.clearRetainingCapacity();
    self.deferred_device_free_bytes = 0;
    self.stats.deferred_free_drains += 1;
    self.stats.deferred_free_reclaimed_bytes += reclaimed;
}

fn synchronizeAndDrainDeferredDeviceFrees(self: *CudaCompute) !void {
    try self.ctx.synchronize();
    drainDeferredDeviceFreesAfterSync(self);
}

fn forceDrainDeferredDeviceFrees(self: *CudaCompute) !void {
    if (self.deferred_device_frees.items.len == 0) return;
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.deferred_free_forced_drains += 1;
}

fn disableCudaLazyHostPrefetchWorker() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_DISABLE_PREFETCH_WORKER");
}

fn installCudaLazyHostPrefetch(self: *CudaCompute, store: *native_compute_mod.WeightStore) !void {
    if (store.prefetch_initialized) {
        native_compute_mod.stopPrefetchWorker(store);
        native_compute_mod.deinitPrefetchQueue(store);
    }
    store.prefetch = @TypeOf(store.prefetch).initWithPriorityUnlocked(
        self.allocator,
        self,
        &cudaLazyHostPrefetchProcess,
        &cudaLazyHostPrefetchPriority,
    );
    store.prefetch_initialized = true;
    var lazy_it = store.lazy_weights.iterator();
    while (lazy_it.next()) |entry| {
        entry.value_ptr.guard = store.prefetch.mutexPtr();
    }
    if (store.lazy_weights.count() > 0 and !disableCudaLazyHostPrefetchWorker()) {
        try native_compute_mod.startPrefetchWorker(store);
    }
}

fn cudaLazyHostPrefetchProcess(ctx: *anyopaque, entry: *native_compute_mod.LazyWeightEntry) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const store = self.lazy_host_store orelse {
        entry.pending_prefetch = false;
        return;
    };
    var tensor_touch: []const u8 = &.{};
    var quant_touch: []const u8 = &.{};
    store.prefetch.lock();
    entry.pending_prefetch = false;
    native_compute_mod.ensureLazyWeightLoadedLocked(store, null, entry) catch {
        store.prefetch.unlock();
        return;
    };
    if (entry.loaded) |*loaded| {
        if (!loaded.tensor.owns_data) {
            tensor_touch = loaded.tensor.data;
        }
        if (loaded.quantized_storage) |*storage| {
            if (!storage.raw_owned or storage.raw_mmap_backed) {
                quant_touch = storage.raw_bytes;
            }
        }
    }
    store.prefetch.unlock();
    touchBytesForPageCache(tensor_touch);
    touchBytesForPageCache(quant_touch);
    store.prefetch.lock();
    defer store.prefetch.unlock();
    if (store.shared_prefetch) |shared_prefetch| {
        entry.prefetch_score = shared_prefetch.noteComplete(entry.tensor_ref.name) catch 0;
    } else {
        entry.prefetch_score = 0;
    }
}

fn cudaLazyHostPrefetchPriority(entry: *native_compute_mod.LazyWeightEntry) u64 {
    return entry.prefetch_score;
}

fn touchLoadedWeightPages(loaded: *const weight_source_mod.LoadedWeight) void {
    touchBytesForPageCache(loaded.tensor.data);
    if (loaded.quantized_storage) |storage| {
        touchBytesForPageCache(storage.raw_bytes);
    }
}

fn touchBytesForPageCache(bytes: []const u8) void {
    if (bytes.len == 0) return;
    const page_size = 4096;
    var checksum: u8 = 0;
    var index: usize = 0;
    while (index < bytes.len) : (index += page_size) {
        checksum +%= bytes[index];
    }
    checksum +%= bytes[bytes.len - 1];
    std.mem.doNotOptimizeAway(checksum);
}

fn allocDeviceBuffer(self: *CudaCompute, len: usize) !buffer_mod.DeviceBuffer {
    if (len == 0) return .{};
    const seq = self.temp_trace_seq;
    self.temp_trace_seq = seq + 1;
    if (try allocPinnedTempSlot(self, seq, len)) |buffer| return buffer;
    const stable_reuse = cudaTempStableReuseEnabled();
    var best_index: ?usize = null;
    var best_len: usize = std.math.maxInt(usize);
    for (self.temp_buffers.items, 0..) |buffer, i| {
        if (stable_reuse) {
            if (buffer.len == len) {
                best_index = i;
                break;
            }
        } else if (buffer.len >= len and buffer.len < best_len) {
            best_index = i;
            best_len = buffer.len;
        }
    }
    if (best_index) |i| {
        const buffer = if (stable_reuse)
            self.temp_buffers.orderedRemove(i)
        else
            self.temp_buffers.swapRemove(i);
        self.stats.temp_buffer_hits += 1;
        traceCudaTempAlloc(self, seq, "alloc_hit", len, buffer);
        return buffer;
    }
    self.stats.temp_buffer_misses += 1;
    const buffer = buffer_mod.DeviceBuffer.alloc(&self.ctx, len) catch |err| {
        if (self.deferred_device_frees.items.len != 0) {
            try forceDrainDeferredDeviceFrees(self);
            const retry = try buffer_mod.DeviceBuffer.alloc(&self.ctx, len);
            self.noteDeviceBytes(len);
            self.stats.device_alloc_calls += 1;
            traceCudaTempAlloc(self, seq, "alloc_miss_retry", len, retry);
            return retry;
        }
        return err;
    };
    self.noteDeviceBytes(len);
    self.stats.device_alloc_calls += 1;
    traceCudaTempAlloc(self, seq, "alloc_miss", len, buffer);
    return buffer;
}

fn releaseDeviceBuffer(self: *CudaCompute, buffer: *buffer_mod.DeviceBuffer) void {
    if (buffer.ptr == 0) return;
    if (releasePinnedTempSlot(self, buffer)) return;
    const cache_budget = cudaTempCacheBudgetBytes();
    var cached_bytes: usize = 0;
    for (self.temp_buffers.items) |cached| cached_bytes += cached.len;
    if (self.temp_buffers.items.len < max_temp_buffers and buffer.len <= cache_budget and cached_bytes + buffer.len <= cache_budget) {
        self.temp_buffers.append(self.allocator, buffer.*) catch {
            self.stats.temp_buffer_evictions += 1;
            self.stats.device_free_calls += 1;
            buffer.free(&self.ctx);
            return;
        };
        self.stats.temp_buffer_releases += 1;
        buffer.* = .{};
        return;
    }
    self.stats.temp_buffer_evictions += 1;
    if (cudaDeferredFreeEnabled()) {
        self.deferred_device_frees.append(self.allocator, buffer.*) catch {
            self.stats.device_free_calls += 1;
            synchronizeAndDrainDeferredDeviceFrees(self) catch {};
            buffer.free(&self.ctx);
            return;
        };
        self.deferred_device_free_bytes += buffer.len;
        self.stats.deferred_free_queued += 1;
        buffer.* = .{};
        if (self.deferred_device_free_bytes >= cudaDeferredFreeBudgetBytes()) {
            forceDrainDeferredDeviceFrees(self) catch {};
        }
        return;
    }
    self.stats.device_free_calls += 1;
    synchronizeAndDrainDeferredDeviceFrees(self) catch {};
    buffer.free(&self.ctx);
}

fn freeTensor(ctx: *anyopaque, tensor: CT) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (!cuda_tensor.owned_by_tensor) return;
    freeCudaTensorStorage(self, cuda_tensor);
    self.allocator.destroy(cuda_tensor);
}

fn borrowedSlotTensor(tensor: *const CudaTensor) CudaTensor {
    var borrowed = tensor.*;
    borrowed.owns_buffer = false;
    borrowed.owns_shape = false;
    borrowed.owned_by_tensor = false;
    return borrowed;
}

fn residentTensorForSlot(self: *CudaCompute, tensor: *const CudaTensor) ?*CudaTensor {
    var it = self.resident_weights.iterator();
    while (it.next()) |entry| {
        const resident = entry.value_ptr;
        if (resident.buffer.ptr == tensor.buffer.ptr and
            resident.buffer.len == tensor.buffer.len and
            resident.elem_count == tensor.elem_count)
        {
            return resident;
        }
    }
    return null;
}

fn decoderRuntimeSlotPinsBuffer(self: *const CudaCompute, buffer: buffer_mod.DeviceBuffer) bool {
    if (buffer.ptr == 0) return false;
    var linear_it = self.decoder_runtime_linear_slots.iterator();
    while (linear_it.next()) |entry| {
        const slot = entry.value_ptr;
        if (slot.weight.buffer.ptr == buffer.ptr) return true;
        if (slot.bias) |bias| {
            if (bias.buffer.ptr == buffer.ptr) return true;
        }
    }
    var norm_it = self.decoder_runtime_rms_norm_slots.iterator();
    while (norm_it.next()) |entry| {
        if (entry.value_ptr.weight.buffer.ptr == buffer.ptr) return true;
    }
    return false;
}

fn cudaTensorDeviceBytes(tensor: *const CudaTensor) usize {
    if (tensor.quant_type) |quant_type| {
        const block_size = gguf_tensor_types.bytesPerBlock(quant_type) orelse return tensor.buffer.len;
        const values_per_block = gguf_tensor_types.valuesPerBlock(quant_type) orelse return tensor.buffer.len;
        return ((tensor.elem_count + values_per_block - 1) / values_per_block) * block_size;
    }
    return tensor.elem_count * tensor.dtype.byteSize();
}

fn noteLazyDeviceAccess(self: *CudaCompute, name: []const u8, bytes: usize) !void {
    self.lazy_access_epoch +|= 1;
    if (self.lazy_device_epochs.getPtr(name)) |epoch| {
        epoch.* = self.lazy_access_epoch;
        return;
    }
    if (bytes == 0) return;
    const owned_key = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_key);
    try self.lazy_device_epochs.put(self.allocator, owned_key, self.lazy_access_epoch);
    self.lazy_device_bytes += bytes;
}

fn evictLazyDeviceWeightsToBudget(self: *CudaCompute, protected_name: []const u8) !void {
    if (self.lazy_device_budget_bytes == 0) return;
    const trace = cudaLazyTraceEnabled();
    var synchronized = false;
    while (self.lazy_device_bytes > self.lazy_device_budget_bytes) {
        var oldest_name: ?[]const u8 = null;
        var oldest_epoch: u64 = std.math.maxInt(u64);
        var it = self.lazy_device_epochs.iterator();
        while (it.next()) |entry| {
            const name = entry.key_ptr.*;
            if (std.mem.eql(u8, name, protected_name)) continue;
            if (self.resident_weights.getPtr(name)) |tensor| {
                if (decoderRuntimeSlotPinsBuffer(self, tensor.buffer)) {
                    self.stats.decoder_runtime_pinned_eviction_skips += 1;
                    continue;
                }
            }
            if (entry.value_ptr.* < oldest_epoch) {
                oldest_epoch = entry.value_ptr.*;
                oldest_name = name;
            }
        }
        const victim_name = oldest_name orelse break;
        if (trace) {
            std.log.info("cuda_lazy_trace: evict_start victim={s} protected={s} lazy_device_mb={d} budget_mb={d}", .{
                victim_name,
                protected_name,
                self.lazy_device_bytes / (1024 * 1024),
                self.lazy_device_budget_bytes / (1024 * 1024),
            });
        }
        if (!synchronized) {
            try synchronizeAndDrainDeferredDeviceFrees(self);
            synchronized = true;
        }
        if (self.resident_weights.fetchRemove(victim_name)) |removed| {
            var tensor = removed.value;
            tensor.owns_buffer = true;
            tensor.owns_shape = true;
            const bytes = cudaTensorDeviceBytes(&tensor);
            freeCudaTensorStorageUncached(self, &tensor);
            self.lazy_device_bytes -|= bytes;
            self.stats.resident_weight_bytes -|= bytes;
            if (trace) {
                std.log.info("cuda_lazy_trace: evicted victim={s} evicted_mb={d} lazy_device_mb={d}", .{
                    victim_name,
                    bytes / (1024 * 1024),
                    self.lazy_device_bytes / (1024 * 1024),
                });
            }
            self.allocator.free(removed.key);
        }
        if (self.lazy_device_epochs.fetchRemove(victim_name)) |removed_epoch| {
            self.allocator.free(removed_epoch.key);
        }
    }
}

fn cancelPendingLazyHostPrefetchLocked(store: *native_compute_mod.WeightStore, entry: *native_compute_mod.LazyWeightEntry) bool {
    if (!entry.pending_prefetch) return false;
    for (store.prefetch.items.items, 0..) |queued, index| {
        if (queued == entry) {
            _ = store.prefetch.items.orderedRemove(index);
            entry.pending_prefetch = false;
            return true;
        }
    }
    return false;
}

fn materializeLazyWeight(self: *CudaCompute, name: []const u8) !*CudaTensor {
    const store = self.lazy_host_store orelse return error.MissingWeight;
    const trace = cudaLazyTraceEnabled();
    const demand_start = platform.time.monotonicNs();
    if (trace) {
        std.log.info("cuda_lazy_trace: demand_start name={s} resident_mb={d} lazy_device_mb={d} budget_mb={d}", .{
            name,
            self.stats.resident_weight_bytes / (1024 * 1024),
            self.lazy_device_bytes / (1024 * 1024),
            self.lazy_device_budget_bytes / (1024 * 1024),
        });
    }
    store.prefetch.lock();
    var store_locked = true;
    errdefer if (store_locked) store.prefetch.unlock();
    const loaded = blk: {
        const entry = store.lazy_weights.getPtr(name) orelse {
            store_locked = false;
            store.prefetch.unlock();
            return error.MissingWeight;
        };
        self.stats.lazy_demand_loads += 1;
        if (entry.loaded != null) {
            self.stats.lazy_host_prefetch_hits += 1;
            if (trace) {
                std.log.info("cuda_lazy_trace: host_hit name={s} loaded_mb={d}", .{ name, entry.loaded_bytes / (1024 * 1024) });
            }
        } else {
            if (cancelPendingLazyHostPrefetchLocked(store, entry)) {
                self.stats.lazy_prefetch_cancelled_for_demand += 1;
                if (trace) std.log.info("cuda_lazy_trace: cancelled_prefetch name={s}", .{name});
            }
            const host_start = platform.time.monotonicNs();
            try native_compute_mod.ensureLazyWeightLoadedLocked(store, self.run_budget, entry);
            const host_ns = elapsedNsSince(host_start);
            self.stats.lazy_host_load_ns +|= host_ns;
            if (trace) {
                std.log.info("cuda_lazy_trace: host_loaded name={s} loaded_mb={d} host_ms={d}", .{
                    name,
                    entry.loaded_bytes / (1024 * 1024),
                    host_ns / 1_000_000,
                });
            }
        }
        const moved_loaded = entry.loaded.?;
        entry.loaded = null;
        entry.active_tier = entry.placement.spill_tier;
        if (store.tier_cache) |*tier_cache| {
            tier_cache.noteRelease(.host, entry.loaded_bytes);
        }
        store_locked = false;
        store.prefetch.unlock();
        break :blk moved_loaded;
    };
    var owned_loaded = loaded;
    defer owned_loaded.deinit();
    const page_touch_start = platform.time.monotonicNs();
    touchLoadedWeightPages(&owned_loaded);
    const page_touch_ns = elapsedNsSince(page_touch_start);
    self.stats.lazy_host_page_touch_ns +|= page_touch_ns;
    if (trace and page_touch_ns > 0) {
        std.log.info("cuda_lazy_trace: page_touched name={s} touch_ms={d}", .{
            name,
            page_touch_ns / 1_000_000,
        });
    }
    const owned_key = try self.allocator.dupe(u8, name);
    const upload_start = platform.time.monotonicNs();
    self.insertWeightFromLoaded(owned_key, &owned_loaded) catch |err| {
        self.allocator.free(owned_key);
        return err;
    };
    const tensor = self.resident_weights.getPtr(name) orelse return error.MissingWeight;
    const uploaded_bytes = cudaTensorDeviceBytes(tensor);
    const upload_ns = elapsedNsSince(upload_start);
    self.stats.lazy_upload_ns +|= upload_ns;
    self.stats.lazy_uploaded_bytes += uploaded_bytes;
    if (trace) {
        std.log.info("cuda_lazy_trace: uploaded name={s} dtype={s} uploaded_mb={d} upload_ms={d}", .{
            name,
            @tagName(tensor.dtype),
            uploaded_bytes / (1024 * 1024),
            upload_ns / 1_000_000,
        });
    }
    try noteLazyDeviceAccess(self, name, uploaded_bytes);
    try evictLazyDeviceWeightsToBudget(self, name);
    if (trace) {
        std.log.info("cuda_lazy_trace: demand_done name={s} total_ms={d} lazy_device_mb={d} resident_mb={d}", .{
            name,
            elapsedNsSince(demand_start) / 1_000_000,
            self.lazy_device_bytes / (1024 * 1024),
            self.stats.resident_weight_bytes / (1024 * 1024),
        });
    }
    return self.resident_weights.getPtr(name) orelse error.MissingWeight;
}

fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.resident_weights.getPtr(name)) |tensor| {
        if (self.lazy_device_epochs.contains(name)) {
            try noteLazyDeviceAccess(self, name, 0);
        }
        return tensor;
    }
    if (getDenseStreamWeight(self, name)) |maybe_tensor| {
        if (maybe_tensor) |tensor| return tensor;
    } else |err| {
        if (isDenseFfnStreamWeightName(name)) self.stats.ffn_stream_fallbacks += 1;
        self.stats.dense_stream_fallbacks += 1;
        if (cudaDenseStreamProfileEnabled()) {
            std.log.warn("cuda_dense_stream: fallback name={s} err={s}", .{ name, @errorName(err) });
        }
    }
    return materializeLazyWeight(self, name) catch |err| switch (err) {
        error.MissingWeight => error.WeightNotFound,
        else => err,
    };
}

fn prefetchWeightHint(ctx: *anyopaque, name: []const u8, hint: u32) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.resident_weights.contains(name)) return;
    if (cudaDenseStreamEnabled(self) and isDenseStreamWeightName(self, name)) {
        const is_mlp = isDenseFfnStreamWeightName(name);
        if (findDenseStreamEntry(self, name)) |entry| {
            self.dense_stream_epoch +|= 1;
            entry.last_access = self.dense_stream_epoch;
            if (is_mlp) self.stats.ffn_stream_hits += 1;
            self.stats.dense_stream_hits += 1;
            return;
        }
        if (cudaDensePrefetchEnabled(self) and self.dense_host_prefetch_initialized) {
            enqueueDenseHostPrefetchHint(self, name, hint);
            return;
        }
        if (self.lazy_host_store) |store| {
            if (store.tensor_store) |tensor_store| {
                if (tensor_store.describeTensorRange(self.allocator, name)) |maybe_range| {
                    if (maybe_range) |range_value| {
                        var range = range_value;
                        if (is_mlp) self.stats.ffn_stream_fadvise_calls += 1;
                        self.stats.dense_stream_fadvise_calls += 1;
                        c_file.adviseFileRange(self.allocator, range.path, range.byte_offset, range.byte_len, .will_need);
                        range.deinit(self.allocator);
                    }
                } else |_| {}
            }
        }
        return;
    }
    const store = self.lazy_host_store orelse return;
    if (!store.prefetch_initialized) return;
    store.prefetch.lock();
    defer store.prefetch.unlock();
    const entry = store.lazy_weights.getPtr(name) orelse {
        self.stats.lazy_prefetch_missing += 1;
        return;
    };
    if (store.shared_prefetch) |shared_prefetch| {
        entry.prefetch_score = shared_prefetch.noteRequest(name, hint) catch entry.prefetch_score;
    } else {
        entry.prefetch_score +|= hint;
    }
    if (entry.pending_prefetch) {
        self.stats.lazy_prefetch_duplicates += 1;
        return;
    }
    store.prefetch.appendLocked(entry) catch return;
    entry.pending_prefetch = true;
    self.stats.lazy_prefetch_enqueues += 1;
    store.prefetch.signal();
}

fn drainPrefetchBudget(ctx: *anyopaque, max_items: usize) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const store = self.lazy_host_store orelse return;
    if (!store.prefetch_initialized) return;
    self.stats.lazy_prefetch_drain_calls += 1;
    store.prefetch.drainBudget(max_items);
}

fn debugProfileCheckpoint(ctx: *anyopaque, label: []const u8, layer: usize) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_PREFILL_PROFILE")) return;
    const stats = self.stats;
    std.debug.print(
        "cuda_prefill_profile: label={s} layer={d} lazy_demand={d} lazy_hits={d} lazy_host_ms={d} lazy_touch_ms={d} lazy_upload_ms={d} lazy_uploaded_mb={d} dense_req={d} dense_hits={d} dense_misses={d} dense_fallbacks={d} dense_read_ms={d} dense_h2d_ms={d} dense_read_mb={d} dense_uploaded_mb={d} dense_resident_mb={d} dense_prefetch_enq={d} dense_prefetch_ready={d} dense_prefetch_steals={d} dense_prefetch_sync_reads={d} dense_prefetch_wait_ms={d} dense_prefetch_host_read_ms={d} dense_prefetch_upload_ms={d} syncs={d} upload_syncs={d} alloc_calls={d} free_calls={d}\n",
        .{
            label,
            layer,
            stats.lazy_demand_loads,
            stats.lazy_host_prefetch_hits,
            stats.lazy_host_load_ns / 1_000_000,
            stats.lazy_host_page_touch_ns / 1_000_000,
            stats.lazy_upload_ns / 1_000_000,
            stats.lazy_uploaded_bytes / (1024 * 1024),
            stats.dense_stream_requests,
            stats.dense_stream_hits,
            stats.dense_stream_misses,
            stats.dense_stream_fallbacks,
            stats.dense_stream_read_ns / 1_000_000,
            stats.dense_stream_h2d_ns / 1_000_000,
            stats.dense_stream_read_bytes / (1024 * 1024),
            stats.dense_stream_uploaded_bytes / (1024 * 1024),
            stats.dense_stream_resident_bytes / (1024 * 1024),
            stats.dense_prefetch_enqueues,
            stats.dense_prefetch_ready_hits,
            stats.dense_prefetch_inflight_steals,
            stats.dense_prefetch_sync_reads,
            stats.dense_prefetch_demand_wait_ns / 1_000_000,
            stats.dense_prefetch_host_read_ns / 1_000_000,
            stats.dense_prefetch_upload_ns / 1_000_000,
            stats.stream_syncs,
            stats.upload_syncs,
            stats.device_alloc_calls,
            stats.device_free_calls,
        },
    );
}

fn debugCudaGraphCaptureBegin(ctx: *anyopaque, label: []const u8) !bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    if (cudaTempSlotPeriod() == 0 and !platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_ALLOW_UNPINNED", false)) return false;
    const min_alloc_seq = cudaDebugGraphCaptureMinAllocSeq();
    if (self.temp_trace_seq < min_alloc_seq) return false;
    try self.ctx.beginStreamCapture(driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED);
    self.debug_cuda_graph_capture_active = true;
    self.debug_cuda_graph_active_slot = self.debug_cuda_graph_prepared_slot orelse 0;
    if (cudaDebugGraphPersistentReplayEnabled()) {
        const slot = &self.debug_cuda_graph_slots[self.debug_cuda_graph_active_slot.?];
        slot.kv_replay_capacity_tokens = 0;
        slot.kv_replay_capacity_valid = false;
    }
    self.stats.cuda_graph_capture_begins += 1;
    std.log.info("cuda_graph_capture_probe: begin label={s} slot={d} alloc_seq={d} min_alloc_seq={d}", .{
        label,
        self.debug_cuda_graph_active_slot.?,
        self.temp_trace_seq,
        min_alloc_seq,
    });
    return true;
}

fn debugCudaGraphPrepareDecodeScalars(
    ctx: *anyopaque,
    position_offset: usize,
    query_position_offset: usize,
    kv_seq_len: usize,
    total_sequence_len: usize,
) !bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphCaptureDeviceScalarsEnabled()) return false;
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    self.debug_cuda_graph_decode_kv_seq_len = kv_seq_len;
    if (self.debug_cuda_decode_scalars.ptr == 0) {
        self.debug_cuda_decode_scalars = try buffer_mod.DeviceBuffer.alloc(&self.ctx, 4 * @sizeOf(u32));
    }
    var scalars = [_]u32{
        std.math.cast(u32, position_offset) orelse return error.InvalidCudaState,
        std.math.cast(u32, query_position_offset) orelse return error.InvalidCudaState,
        std.math.cast(u32, kv_seq_len) orelse return error.InvalidCudaState,
        std.math.cast(u32, total_sequence_len) orelse return error.InvalidCudaState,
    };
    try copyFromHostTracked(self, self.debug_cuda_decode_scalars, std.mem.sliceAsBytes(&scalars));
    self.stats.cuda_graph_capture_scalar_updates += 1;
    std.log.info("cuda_graph_capture_probe: decode_scalars position_offset={d} query_position_offset={d} kv_seq_len={d} total_sequence_len={d} ptr=0x{x}", .{
        scalars[0],
        scalars[1],
        scalars[2],
        scalars[3],
        self.debug_cuda_decode_scalars.ptr,
    });
    return true;
}

fn debugCudaTraceTensor(ctx: *anyopaque, label: []const u8, tensor: CT) !void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.ctx.debug_graph_capture_active) return;
    if (!cudaDebugGraphCaptureTensorTraceEnabled()) return;
    const limit = cudaDebugGraphCaptureTensorTraceLimit();
    if (self.ctx.debug_graph_capture_tensor_trace_count >= limit) return;
    const index = self.ctx.debug_graph_capture_tensor_trace_count;
    self.ctx.debug_graph_capture_tensor_trace_count += 1;

    const cuda_tensor = tensorFromCt(tensor);
    std.log.info("cuda_capture_tensor_trace: capture={d} index={d} label={s} tensor=0x{x} buffer=0x{x} buffer_len={d} logical_bytes={d} dtype={s} elem_count={d} shape={any} owns_buffer={d} owns_shape={d} owned_by_tensor={d}", .{
        self.ctx.debug_graph_capture_id,
        index,
        label,
        @intFromPtr(cuda_tensor),
        cuda_tensor.buffer.ptr,
        cuda_tensor.buffer.len,
        cudaTensorDeviceBytes(cuda_tensor),
        @tagName(cuda_tensor.dtype),
        cuda_tensor.elem_count,
        cuda_tensor.shape,
        @intFromBool(cuda_tensor.owns_buffer),
        @intFromBool(cuda_tensor.owns_shape),
        @intFromBool(cuda_tensor.owned_by_tensor),
    });
}

fn resetCudaGraphReplaySlotMetadata(self: *CudaCompute, slot: *CudaGraphReplaySlot) void {
    if (slot.exec) |exec| {
        self.ctx.destroyGraphExec(exec);
        slot.exec = null;
    }
    if (slot.shape) |shape| {
        self.allocator.free(shape);
        slot.shape = null;
    }
    slot.input = .{};
    slot.output = .{};
    slot.elem_count = 0;
    slot.dtype = .f32;
    slot.input_valid = false;
    slot.valid = false;
    slot.kv_replay_capacity_tokens = 0;
    slot.kv_replay_capacity_valid = false;
}

fn deinitCudaGraphReplaySlot(self: *CudaCompute, slot: *CudaGraphReplaySlot) void {
    resetCudaGraphReplaySlotMetadata(self, slot);
    slot.input_storage.free(&self.ctx);
    slot.output_storage.free(&self.ctx);
    slot.input_storage = .{};
    slot.output_storage = .{};
}

fn invalidateDebugFinalHiddenGraph(self: *CudaCompute) void {
    for (&self.debug_cuda_graph_slots) |*slot| {
        resetCudaGraphReplaySlotMetadata(self, slot);
    }
    self.debug_cuda_graph_active_slot = null;
    self.debug_cuda_graph_prepared_slot = null;
}

fn findCudaGraphReplaySlotForByteLen(self: *CudaCompute, byte_len: usize) usize {
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (slot.input_storage.ptr != 0 and slot.input_storage.len == byte_len) return idx;
    }
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (slot.input_storage.ptr == 0) return idx;
    }
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (!slot.valid) return idx;
    }
    const idx = self.debug_cuda_graph_next_evict_slot % max_cuda_graph_replay_slots;
    self.debug_cuda_graph_next_evict_slot = (idx + 1) % max_cuda_graph_replay_slots;
    return idx;
}

fn findExistingCudaGraphReplaySlotForByteLen(self: *CudaCompute, byte_len: usize) ?usize {
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (slot.input_storage.ptr != 0 and slot.input_storage.len == byte_len and slot.valid and slot.exec != null) return idx;
    }
    return null;
}

fn ensureCudaGraphReplaySlotStorage(self: *CudaCompute, slot: *CudaGraphReplaySlot, byte_len: usize) !void {
    if (slot.input_storage.ptr != 0 and slot.input_storage.len == byte_len and
        slot.output_storage.ptr != 0 and slot.output_storage.len >= byte_len)
    {
        return;
    }
    resetCudaGraphReplaySlotMetadata(self, slot);
    if (slot.input_storage.ptr != 0) {
        self.stats.device_free_calls += 1;
        slot.input_storage.free(&self.ctx);
        slot.input_storage = .{};
    }
    if (slot.output_storage.ptr != 0) {
        self.stats.device_free_calls += 1;
        slot.output_storage.free(&self.ctx);
        slot.output_storage = .{};
    }
    slot.input_storage = try buffer_mod.DeviceBuffer.alloc(&self.ctx, byte_len);
    self.noteDeviceBytes(byte_len);
    self.stats.device_alloc_calls += 1;
    std.log.info("cuda_graph_capture_probe: persistent_input_alloc ptr=0x{x} len={d}", .{
        slot.input_storage.ptr,
        slot.input_storage.len,
    });
    slot.output_storage = try buffer_mod.DeviceBuffer.alloc(&self.ctx, byte_len);
    self.noteDeviceBytes(byte_len);
    self.stats.device_alloc_calls += 1;
    std.log.info("cuda_graph_capture_probe: persistent_output_alloc ptr=0x{x} len={d}", .{
        slot.output_storage.ptr,
        slot.output_storage.len,
    });
}

fn debugCudaGraphPrepareFinalHiddenReplayInput(ctx: *anyopaque, input: CT) !?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.debug_cuda_graph_prepared_slot = null;
    if (!cudaDebugGraphPersistentReplayEnabled()) return null;
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;

    const input_tensor = tensorFromCt(input);
    if (input_tensor.dtype != .f32 or input_tensor.quant_type != null) return null;
    const byte_len = try checkedMul(input_tensor.elem_count, @sizeOf(f32));
    if (byte_len == 0) return null;
    if (input_tensor.buffer.len < byte_len) return error.InvalidCudaState;

    const capture_possible = cudaTempSlotPeriod() != 0 or
        platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_ALLOW_UNPINNED", false);
    const slot_idx = if (capture_possible)
        findCudaGraphReplaySlotForByteLen(self, byte_len)
    else
        findExistingCudaGraphReplaySlotForByteLen(self, byte_len) orelse return null;
    self.debug_cuda_graph_prepared_slot = slot_idx;
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    try ensureCudaGraphReplaySlotStorage(self, slot, byte_len);

    try copyFromDeviceTracked(self, slot.input_storage, input_tensor.buffer, byte_len);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = slot.input_storage,
        .dtype = .f32,
        .shape = shape,
        .elem_count = input_tensor.elem_count,
        .owns_buffer = false,
    };
    return tensor;
}

fn debugCudaGraphRegisterFinalHiddenReplayInput(ctx: *anyopaque, input: CT) !void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled()) return;
    if (!self.debug_cuda_graph_capture_active) return;
    const slot_idx = self.debug_cuda_graph_active_slot orelse return;
    const slot = &self.debug_cuda_graph_slots[slot_idx];

    const input_tensor = tensorFromCt(input);
    slot.input = input_tensor.buffer;
    slot.input_valid = true;
    std.log.info("cuda_graph_capture_probe: persistent_input slot={d} input=0x{x} len={d}", .{
        slot_idx,
        input_tensor.buffer.ptr,
        input_tensor.buffer.len,
    });
}

fn debugCudaGraphRegisterFinalHiddenReplayBoundary(ctx: *anyopaque, input: CT, output: CT) !void {
    _ = input;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled()) return;
    if (!self.debug_cuda_graph_capture_active) return;
    const slot_idx = self.debug_cuda_graph_active_slot orelse return;
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    if (!slot.input_valid) return;

    const output_tensor = tensorFromCt(output);
    if (output_tensor.quant_type != null) return error.UnsupportedTensorType;
    const byte_len = try checkedMul(output_tensor.elem_count, output_tensor.dtype.byteSize());
    if (byte_len == 0) return error.InvalidTensorShape;
    if (slot.output_storage.ptr == 0 or slot.output_storage.len < byte_len) return error.InvalidCudaState;
    try copyFromDeviceTracked(self, slot.output_storage, output_tensor.buffer, byte_len);

    const shape = try dupeShape(self.allocator, output_tensor.shape);
    errdefer self.allocator.free(shape);

    if (slot.shape) |old_shape| {
        self.allocator.free(old_shape);
    }
    slot.shape = shape;
    slot.output = slot.output_storage;
    slot.elem_count = output_tensor.elem_count;
    slot.dtype = output_tensor.dtype;
    slot.valid = true;

    std.log.info("cuda_graph_capture_probe: persistent_boundary slot={d} input=0x{x} output=0x{x} temp_output=0x{x} elem_count={d} dtype={s} kv_capacity={d} shape={any}", .{
        slot_idx,
        slot.input.ptr,
        slot.output.ptr,
        output_tensor.buffer.ptr,
        output_tensor.elem_count,
        @tagName(output_tensor.dtype),
        if (slot.kv_replay_capacity_valid) slot.kv_replay_capacity_tokens else 0,
        output_tensor.shape,
    });
}

fn debugCudaGraphReplayFinalHidden(ctx: *anyopaque, input: CT) !?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled()) return null;
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    const slot_idx = self.debug_cuda_graph_prepared_slot orelse return null;
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    const exec = slot.exec orelse return null;
    if (!slot.valid) return null;
    const shape_src = slot.shape orelse return null;
    if (slot.kv_replay_capacity_valid and self.debug_cuda_graph_decode_kv_seq_len > slot.kv_replay_capacity_tokens) {
        self.stats.cuda_graph_capture_capacity_skips += 1;
        std.log.warn("cuda_graph_capture_probe: persistent_replay_kv_capacity_exceeded slot={d} kv_seq_len={d} capacity={d}", .{
            slot_idx,
            self.debug_cuda_graph_decode_kv_seq_len,
            slot.kv_replay_capacity_tokens,
        });
        return null;
    }

    const input_tensor = tensorFromCt(input);
    if (input_tensor.buffer.ptr != slot.input.ptr or input_tensor.buffer.len != slot.input.len) {
        std.log.warn("cuda_graph_capture_probe: persistent_replay_input_mismatch slot={d} expected=0x{x}/{d} actual=0x{x}/{d}", .{
            slot_idx,
            slot.input.ptr,
            slot.input.len,
            input_tensor.buffer.ptr,
            input_tensor.buffer.len,
        });
        return null;
    }

    const shape = try dupeShape(self.allocator, shape_src);
    errdefer self.allocator.free(shape);

    const output = slot.output;
    if (output.ptr == 0) return null;

    var profile_scope = beginDecodeProfile(self, .graph_replay, 1);
    defer if (profile_scope) |*scope| scope.end();
    try self.ctx.launchGraph(exec);
    self.stats.cuda_graph_capture_replays += 1;
    self.stats.cuda_graph_capture_persistent_replays += 1;
    std.log.info("cuda_graph_capture_probe: persistent_replayed slot={d} input=0x{x} output=0x{x} kv_seq_len={d} kv_capacity={d}", .{
        slot_idx,
        input_tensor.buffer.ptr,
        output.ptr,
        self.debug_cuda_graph_decode_kv_seq_len,
        if (slot.kv_replay_capacity_valid) slot.kv_replay_capacity_tokens else 0,
    });

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = output,
        .dtype = slot.dtype,
        .shape = shape,
        .elem_count = slot.elem_count,
        .owns_buffer = false,
    };
    return tensor;
}

fn replayCapturedDebugGraphOneShot(self: *CudaCompute, graph: driver_mod.CUgraph) !void {
    const exec = try self.ctx.instantiateGraph(graph);
    defer self.ctx.destroyGraphExec(exec);
    self.stats.cuda_graph_capture_instantiates += 1;
    try self.ctx.launchGraph(exec);
    self.stats.cuda_graph_capture_replays += 1;
    std.log.info("cuda_graph_capture_probe: replayed", .{});
}

fn replayCapturedDebugGraphWithUpdate(self: *CudaCompute, slot_idx: usize, graph: driver_mod.CUgraph) !void {
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    if (slot.exec) |exec| {
        const outcome = self.ctx.updateGraphExec(exec, graph) catch |err| {
            if (err == error.CudaSymbolMissing) {
                self.stats.cuda_graph_capture_update_unavailable += 1;
                std.log.warn("cuda_graph_capture_probe: update_unavailable fallback=instantiate", .{});
                return replayCapturedDebugGraphOneShot(self, graph);
            }
            return err;
        };
        if (outcome.success()) {
            self.stats.cuda_graph_capture_update_successes += 1;
            try self.ctx.launchGraph(exec);
            self.stats.cuda_graph_capture_replays += 1;
            std.log.info("cuda_graph_capture_probe: updated_replayed", .{});
            return;
        }

        self.stats.cuda_graph_capture_update_failures += 1;
        std.log.warn("cuda_graph_capture_probe: update_failed cuda={s} update_result={s} fallback=reinstantiate", .{
            self.ctx.driver.errorName(outcome.cuda_result),
            cudaGraphExecUpdateResultName(outcome.update_result),
        });
        self.ctx.destroyGraphExec(exec);
        slot.exec = null;
    }

    const exec = try self.ctx.instantiateGraph(graph);
    slot.exec = exec;
    self.stats.cuda_graph_capture_instantiates += 1;
    try self.ctx.launchGraph(exec);
    self.stats.cuda_graph_capture_replays += 1;
    std.log.info("cuda_graph_capture_probe: instantiated_cached_replayed slot={d}", .{slot_idx});
}

fn debugCudaGraphCaptureEnd(ctx: *anyopaque, replay: bool) !void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.debug_cuda_graph_capture_active) return;
    const slot_idx = self.debug_cuda_graph_active_slot orelse 0;
    self.debug_cuda_graph_capture_active = false;
    self.debug_cuda_graph_active_slot = null;
    const graph = try self.ctx.endStreamCapture();
    defer self.ctx.destroyGraph(graph);
    if (replay) {
        if (cudaDebugGraphPersistentReplayEnabled() or cudaDebugGraphCaptureUpdateExecEnabled()) {
            try replayCapturedDebugGraphWithUpdate(self, slot_idx, graph);
        } else {
            try replayCapturedDebugGraphOneShot(self, graph);
        }
    } else {
        self.stats.cuda_graph_capture_discards += 1;
        std.log.info("cuda_graph_capture_probe: discarded", .{});
    }
}

fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
    var shape = [_]i32{@intCast(data.len)};
    return fromFloat32ShapeOp(ctx, data, &shape);
}

fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var elem_count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @intCast(dim));
    }
    if (elem_count != data.len) return error.InvalidShape;

    const shape_i64 = try self.allocator.alloc(i64, shape.len);
    errdefer self.allocator.free(shape_i64);
    for (shape, 0..) |dim, i| shape_i64[i] = dim;

    var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.upload_syncs += 1;
    self.stats.from_float32_calls += 1;
    const byte_len = data.len * @sizeOf(f32);
    self.stats.from_float32_bytes += byte_len;
    noteUploadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.upload_top_sizes, &self.stats.upload_top_counts, byte_len);

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .f32,
        .shape = shape_i64,
        .elem_count = elem_count,
    };
    return tensor;
}

fn fromInt32ShapeOp(ctx: *anyopaque, data: []const i32, shape: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var elem_count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @intCast(dim));
    }
    if (elem_count != data.len) return error.InvalidShape;

    const shape_i64 = try self.allocator.alloc(i64, shape.len);
    errdefer self.allocator.free(shape_i64);
    for (shape, 0..) |dim, i| shape_i64[i] = dim;

    var device = try allocDeviceBuffer(self, data.len * @sizeOf(i32));
    errdefer device.free(&self.ctx);
    try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.upload_syncs += 1;
    self.stats.from_float32_calls += 1;
    const byte_len = data.len * @sizeOf(i32);
    self.stats.from_float32_bytes += byte_len;
    noteUploadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.upload_top_sizes, &self.stats.upload_top_counts, byte_len);

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .i32,
        .shape = shape_i64,
        .elem_count = elem_count,
    };
    return tensor;
}

fn toFloat32Op(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    const out = try allocator.alloc(f32, cuda_tensor.elem_count);
    errdefer allocator.free(out);
    switch (cuda_tensor.dtype) {
        .f32 => {
            try copyToHostTracked(self, cuda_tensor.buffer, std.mem.sliceAsBytes(out));
            try synchronizeAndDrainDeferredDeviceFrees(self);
            self.stats.download_syncs += 1;
        },
        .i32 => {
            const ints = try allocator.alloc(i32, cuda_tensor.elem_count);
            defer allocator.free(ints);
            try copyToHostTracked(self, cuda_tensor.buffer, std.mem.sliceAsBytes(ints));
            try synchronizeAndDrainDeferredDeviceFrees(self);
            self.stats.download_syncs += 1;
            for (ints, out) |value, *dst| dst.* = @floatFromInt(value);
        },
        else => return error.UnsupportedTensorType,
    }
    self.stats.to_float32_calls += 1;
    const byte_len = cuda_tensor.elem_count * cuda_tensor.dtype.byteSize();
    self.stats.to_float32_bytes += byte_len;
    noteDownloadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.download_top_sizes, &self.stats.download_top_counts, byte_len);
    return out;
}

fn tensorDTypeOp(_: *anyopaque, tensor: CT) anyerror!tensor_mod.DType {
    return tensorFromCt(tensor).dtype;
}

fn tensorShapeOp(_: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]i64 {
    return allocator.dupe(i64, tensorFromCt(tensor).shape);
}

fn copyTensorFromBackendOp(ctx: *anyopaque, src_ctx: *anyopaque, src_kind: ops.BackendKind, src_tensor_ct: CT) anyerror!?CT {
    if (src_kind != .cuda) return null;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const src_self: *CudaCompute = @ptrCast(@alignCast(src_ctx));
    const src_tensor = tensorFromCt(src_tensor_ct);
    if (src_tensor.buffer.ptr == 0 and src_tensor.buffer.len != 0) return error.InvalidTensorShape;

    const shape = try dupeShape(self.allocator, src_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, src_tensor.buffer.len);
    errdefer device.free(&self.ctx);
    if (src_self == self) {
        try copyFromDeviceTracked(self, device, src_tensor.buffer, src_tensor.buffer.len);
    } else {
        self.stats.cross_backend_copies += 1;
        self.stats.cross_backend_copy_bytes += src_tensor.buffer.len;
        if (cudaForceCrossBackendCopySync()) {
            self.stats.cross_backend_sync_fallbacks += 1;
            try synchronizeAndDrainDeferredDeviceFrees(src_self);
            try copyFromDeviceTracked(self, device, src_tensor.buffer, src_tensor.buffer.len);
        } else {
            const source_ready = try src_self.ctx.createStreamEvent();
            defer src_self.ctx.destroyEvent(source_ready);
            const copy_done = try self.ctx.createStreamEvent();
            defer self.ctx.destroyEvent(copy_done);

            try src_self.ctx.recordEvent(source_ready);
            self.stats.cross_backend_event_records += 1;
            try self.ctx.waitEvent(source_ready);
            self.stats.cross_backend_event_waits += 1;
            try copyFromDeviceTracked(self, device, src_tensor.buffer, src_tensor.buffer.len);
            try self.ctx.recordEvent(copy_done);
            self.stats.cross_backend_event_records += 1;
            try src_self.ctx.waitEvent(copy_done);
            self.stats.cross_backend_event_waits += 1;
        }
    }

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = src_tensor.dtype,
        .shape = shape,
        .elem_count = src_tensor.elem_count,
        .quant_type = src_tensor.quant_type,
    };
    return tensor;
}

fn evalTensorOp(ctx: *anyopaque, _: CT) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.stats.eval_requests += 1;
    if (!cudaForceEvalSync()) {
        self.stats.eval_skipped_eager += 1;
        return;
    }
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.eval_syncs += 1;
    self.stats.eval_forced_syncs += 1;
}

fn createTensor(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    shape: []i64,
    elem_count: usize,
) !CT {
    return createTensorWithDType(self, device, shape, elem_count, .f32);
}

fn createTensorWithDType(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    shape: []i64,
    elem_count: usize,
    dtype: tensor_mod.DType,
) !CT {
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = dtype,
        .shape = shape,
        .elem_count = elem_count,
    };
    return tensor;
}

fn uploadOwnedHost(self: *CudaCompute, data: []f32, shape_src: []const i64) !CT {
    errdefer self.allocator.free(data);
    const elem_count = data.len;
    const shape = try self.allocator.dupe(i64, shape_src);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.upload_syncs += 1;
    self.stats.upload_owned_host_calls += 1;
    const byte_len = elem_count * @sizeOf(f32);
    self.stats.upload_owned_host_bytes += byte_len;
    noteUploadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.upload_top_sizes, &self.stats.upload_top_counts, byte_len);
    self.allocator.free(data);
    return createTensor(self, device, shape, elem_count);
}

fn reshapeOp(ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    var elem_count: usize = 1;
    for (new_shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @as(usize, @intCast(dim)));
    }
    if (elem_count != input_tensor.elem_count) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape input_elems={d} requested_elems={d} new_rank={d}", .{ input_tensor.elem_count, elem_count, new_shape.len });
        return error.InvalidShape;
    }

    const shape = try self.allocator.dupe(i64, new_shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try copyFromDeviceTracked(self, device, input_tensor.buffer, input_tensor.elem_count * @sizeOf(f32));
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn reshape2DOp(
    ctx: *anyopaque,
    input: CT,
    old_rows: usize,
    old_cols: usize,
    new_rows: usize,
    new_cols: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const new_count = try std.math.mul(usize, new_rows, new_cols);
    if (new_count != input_tensor.elem_count) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape2D input_elems={d} new_rows={d} new_cols={d}", .{ input_tensor.elem_count, new_rows, new_cols });
        return error.InvalidShape;
    }
    if (old_rows != 0 or old_cols != 0) {
        const old_count = try std.math.mul(usize, old_rows, old_cols);
        if (old_count != new_count) {
            if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape2D old_rows={d} old_cols={d} new_rows={d} new_cols={d}", .{ old_rows, old_cols, new_rows, new_cols });
            return error.InvalidShape;
        }
    }
    const shape = try self.allocator.dupe(i64, &[_]i64{ @intCast(new_rows), @intCast(new_cols) });
    errdefer self.allocator.free(shape);
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = input_tensor.buffer,
        .dtype = input_tensor.dtype,
        .shape = shape,
        .elem_count = input_tensor.elem_count,
        .quant_type = input_tensor.quant_type,
        .owns_buffer = false,
        .owns_shape = true,
    };
    return tensor;
}

fn reshape2dOp(ctx: *anyopaque, input: CT, rows: usize, cols: usize) anyerror!CT {
    const input_tensor = tensorFromCt(input);
    const requested = try std.math.mul(usize, rows, cols);
    if (requested != input_tensor.elem_count) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape2d input_elems={d} rows={d} cols={d}", .{ input_tensor.elem_count, rows, cols });
        return error.InvalidShape;
    }
    return reshape2DOp(ctx, input, input_tensor.elem_count, 1, rows, cols);
}

fn sliceRows2DOp(
    ctx: *anyopaque,
    input: CT,
    start_row: usize,
    row_count: usize,
    cols: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (cols == 0) return error.InvalidShape;
    if (input_tensor.elem_count % cols != 0) return error.InvalidShape;
    const total_rows = input_tensor.elem_count / cols;
    if (start_row > total_rows or row_count > total_rows - start_row) return error.InvalidShape;

    const out_count = try checkedMul(row_count, cols);
    const byte_offset = try checkedMul(try checkedMul(start_row, cols), @sizeOf(f32));
    const byte_len = try checkedMul(out_count, @sizeOf(f32));
    if (byte_offset > input_tensor.buffer.len or byte_len > input_tensor.buffer.len - byte_offset) return error.InvalidShape;

    const shape = try allocShape2(self.allocator, row_count, cols);
    errdefer self.allocator.free(shape);
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = .{
            .ptr = input_tensor.buffer.ptr + @as(u64, @intCast(byte_offset)),
            .len = byte_len,
        },
        .dtype = input_tensor.dtype,
        .shape = shape,
        .elem_count = out_count,
        .quant_type = input_tensor.quant_type,
        .owns_buffer = false,
        .owns_shape = true,
    };
    return tensor;
}

fn sliceLastDimOp(ctx: *anyopaque, input: CT, start: usize, stop: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_tensor.shape.len != 2) return error.UnsupportedShape;
    const rows_i64 = input_tensor.shape[0];
    const cols_i64 = input_tensor.shape[1];
    if (rows_i64 < 0 or cols_i64 < 0) return error.InvalidShape;
    const rows: usize = @intCast(rows_i64);
    const cols: usize = @intCast(cols_i64);
    if (start > stop or stop > cols) return error.OutOfBounds;
    const out_cols = stop - start;
    try ensureCount(input_tensor, try checkedMul(rows, cols));

    const out_count = try checkedMul(rows, out_cols);
    const shape = try allocShape2(self.allocator, rows, out_cols);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchSliceLastDimF32(&self.ctx, device, input_tensor.buffer, rows, cols, start, out_cols);
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, out_count);
}

fn cloneTensorShapeOp(ctx: *anyopaque, input: CT, shape_i32: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    var elem_count: usize = 1;
    for (shape_i32) |dim| {
        if (dim <= 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @as(usize, @intCast(dim)));
    }
    if (elem_count != input_tensor.elem_count) return error.InvalidShape;

    const shape = try self.allocator.alloc(i64, shape_i32.len);
    errdefer self.allocator.free(shape);
    for (shape_i32, 0..) |dim, idx| shape[idx] = @intCast(dim);

    const byte_len = input_tensor.elem_count * @sizeOf(f32);
    var device = try allocDeviceBuffer(self, byte_len);
    errdefer device.free(&self.ctx);
    try copyFromDeviceTracked(self, device, input_tensor.buffer, byte_len);
    return try createTensor(self, device, shape, input_tensor.elem_count);
}

fn transposeOp(ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_shape.len == 0 or input_shape.len > 8 or perm.len != input_shape.len) return error.UnsupportedShape;

    var numel: usize = 1;
    var resolved_shape: [8]usize = undefined;
    for (input_shape, 0..) |dim, i| {
        if (dim <= 0) return error.UnsupportedShape;
        const value: usize = @intCast(dim);
        resolved_shape[i] = value;
        numel = try checkedMul(numel, value);
    }
    if (numel != input_tensor.elem_count) return error.InvalidShape;

    var seen = [_]bool{false} ** 8;
    var out_shape_usize: [8]usize = undefined;
    var out_shape_i64: [8]i64 = undefined;
    for (perm, 0..) |axis, i| {
        if (axis >= input_shape.len or seen[axis]) return error.InvalidShape;
        seen[axis] = true;
        out_shape_usize[i] = resolved_shape[axis];
        out_shape_i64[i] = @intCast(out_shape_usize[i]);
    }

    var in_strides: [8]usize = undefined;
    var out_strides: [8]usize = undefined;
    computeStridesUsize(resolved_shape[0..input_shape.len], in_strides[0..input_shape.len]);
    computeStridesUsize(out_shape_usize[0..input_shape.len], out_strides[0..input_shape.len]);

    const in_host = try downloadAlloc(self, input_tensor);
    defer self.allocator.free(in_host);
    var out_host = try self.allocator.alloc(f32, numel);
    errdefer self.allocator.free(out_host);

    for (0..numel) |flat_out| {
        var remaining = flat_out;
        var flat_in: usize = 0;
        for (0..input_shape.len) |d| {
            const coord = remaining / out_strides[d];
            remaining %= out_strides[d];
            flat_in += coord * in_strides[perm[d]];
        }
        out_host[flat_out] = in_host[flat_in];
    }

    return uploadOwnedHost(self, out_host, out_shape_i64[0..input_shape.len]);
}

fn downloadAlloc(self: *CudaCompute, tensor: *const CudaTensor) ![]f32 {
    try ensureF32(tensor);
    const out = try self.allocator.alloc(f32, tensor.elem_count);
    errdefer self.allocator.free(out);
    try copyToHostTracked(self, tensor.buffer, std.mem.sliceAsBytes(out));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    self.stats.download_alloc_calls += 1;
    const byte_len = tensor.elem_count * @sizeOf(f32);
    self.stats.download_alloc_bytes += byte_len;
    noteDownloadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.download_top_sizes, &self.stats.download_top_counts, byte_len);
    return out;
}

fn allocShape2(allocator: std.mem.Allocator, rows: usize, cols: usize) ![]i64 {
    const shape = try allocator.alloc(i64, 2);
    shape[0] = @intCast(rows);
    shape[1] = @intCast(cols);
    return shape;
}

fn dupeShape(allocator: std.mem.Allocator, shape: []const i64) ![]i64 {
    return allocator.dupe(i64, shape);
}

fn ensureF32(tensor: *const CudaTensor) !void {
    if (tensor.dtype != .f32 or tensor.quant_type != null) return error.UnsupportedTensorType;
}

fn ensureF32OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    try ensureF32(tensor);
}

fn ensureF32Bf16OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    if (tensor.dtype != .f32 and tensor.dtype != .bf16) return error.UnsupportedTensorType;
}

fn isBf16Weight(tensor: *const CudaTensor) bool {
    return tensor.dtype == .bf16 and tensor.quant_type == null;
}

fn isKnownQuant(tensor: *const CudaTensor, known: gguf_tensor_types.KnownTensorType) bool {
    const quant_type = tensor.quant_type orelse return false;
    return switch (quant_type) {
        .known => |actual| actual == known,
        else => false,
    };
}

fn quantPlanFormatForTensor(tensor: *const CudaTensor) quant_matmul.Format {
    const quant_type = tensor.quant_type orelse return .f32;
    return switch (quant_type) {
        .known => |known| switch (known) {
            .Q8_0 => .q8_0,
            .Q4_0 => .q4_0,
            .Q4_K => .q4_k,
            .Q5_K => .q5_k,
            .Q6_K => .q6_k,
            .Q8_K => .q8_k,
            else => .unknown,
        },
        else => .unknown,
    };
}

fn cudaAllowPlannedFallback() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ALLOW_PLANNED_FALLBACK", false);
}

fn cudaForceEvalSync() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_FORCE_EVAL_SYNC", false) or
        platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_PROFILE_SYNC", false);
}

fn cudaForceCrossBackendCopySync() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_CROSS_BACKEND_COPY_SYNC", false);
}

fn cudaEnableQ4KDecodeFast() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_Q4K_DECODE_FAST", true);
}

fn cudaEnableQ4KDecodeTile8() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_Q4K_DECODE_TILE8", false);
}

fn cudaDisableHeadNormRopeFusion() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DISABLE_HEAD_NORM_ROPE_FUSION", false);
}

fn cudaDisableFusedQkv() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DISABLE_FUSED_QKV", false);
}

fn recordQuantMatmulPlan(self: *CudaCompute, weight_tensor: *const CudaTensor, op_plan: operator_plan.OperatorPlan) !void {
    switch (op_plan) {
        .quant_matmul => |plan| {
            self.stats.quant_ops.add(plan.operator);
            if (plan.format != quantPlanFormatForTensor(weight_tensor)) return error.CudaPlanFormatMismatch;
            if (plan.operator == .fallback and !cudaAllowPlannedFallback()) return error.CudaPlannedFallbackDisabled;
        },
        else => {},
    }
}

fn ensureCount(tensor: *const CudaTensor, expected: usize) !void {
    if (tensor.elem_count != expected) return error.InvalidShape;
}

fn ensureLogicalWeightCount(tensor: *const CudaTensor, expected: usize) !void {
    if (tensor.quant_type == null) return ensureCount(tensor, expected);
    const logical_count = try elementCountFromShape(tensor.shape);
    if (logical_count != expected) return error.InvalidShape;
}

fn checkedMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidShape;
}

fn computeStridesUsize(shape: []const usize, out: []usize) void {
    var stride: usize = 1;
    var i = shape.len;
    while (i > 0) {
        i -= 1;
        out[i] = stride;
        stride *= shape[i];
    }
}

fn checkedAdd(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidShape;
}

fn checkedSub(a: usize, b: usize) !usize {
    return std.math.sub(usize, a, b) catch error.InvalidShape;
}

fn elementCountFromShape(shape: []const i64) !usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        count = try checkedMul(count, @intCast(dim));
    }
    return count;
}

fn sameShape(a: []const i64, b: []const i64) bool {
    return std.mem.eql(i64, a, b);
}

fn cudaLazyProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_LAZY_PROFILE");
}

fn cudaLazyTraceEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_LAZY_TRACE");
}

fn uploadTempI64(self: *CudaCompute, data: []const i64) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(i64));
    try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
    return device;
}

fn uploadTempU32(self: *CudaCompute, data: []const u32) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(u32));
    try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
    return device;
}

const TempBufferPair = struct {
    first: buffer_mod.DeviceBuffer,
    second: buffer_mod.DeviceBuffer,
};

fn uploadTempU32Pair(self: *CudaCompute, first: []const u32, second: []const u32) !TempBufferPair {
    const first_bytes = try checkedMul(first.len, @sizeOf(u32));
    const second_bytes = try checkedMul(second.len, @sizeOf(u32));
    const total_bytes = try checkedAdd(first_bytes, second_bytes);
    const device = try self.temp_ids_masks.acquire(&self.ctx, total_bytes);
    const first_device: buffer_mod.DeviceBuffer = .{ .ptr = device.ptr, .len = first_bytes };
    const second_device: buffer_mod.DeviceBuffer = .{ .ptr = device.ptr + first_bytes, .len = second_bytes };
    try copyFromHostTracked(self, first_device, std.mem.sliceAsBytes(first));
    try copyFromHostTracked(self, second_device, std.mem.sliceAsBytes(second));
    return .{ .first = first_device, .second = second_device };
}

fn uploadTempU8(self: *CudaCompute, data: []const u8) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len);
    try copyFromHostTracked(self, device, data);
    return device;
}

fn embeddingLookup(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(weight);
    try ensureF32Bf16OrQuantized(weight_tensor);
    if (ids.len != total) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding ids_len={d} total={d}", .{ ids.len, total });
        return error.InvalidShape;
    }
    if (dim == 0 or weight_tensor.elem_count % dim != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding weight_elems={d} dim={d}", .{ weight_tensor.elem_count, dim });
        return error.InvalidShape;
    }
    const vocab = weight_tensor.elem_count / dim;
    for (ids) |raw_id| {
        if (raw_id < 0) return error.InvalidTokenId;
        const id: usize = @intCast(raw_id);
        if (id >= vocab) return error.InvalidTokenId;
    }
    const ids_device = try uploadTempI64(self, ids);
    const out_count = try checkedMul(total, dim);
    const shape = try allocShape2(self.allocator, total, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => try self.kernels.launchEmbeddingLookupQ8_0F32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
                .Q4_0 => try self.kernels.launchEmbeddingLookupQ4_0F32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
                .Q4_K => try self.kernels.launchEmbeddingLookupQ4KF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else if (isBf16Weight(weight_tensor)) {
        try self.kernels.launchEmbeddingLookupBf16WeightF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim);
    } else {
        try self.kernels.launchEmbeddingLookupF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim);
    }
    self.stats.launch_embedding += 1;
    return createTensor(self, device, shape, out_count);
}

fn embeddingLookupTensor(ctx: *anyopaque, weight: CT, ids: CT, total: usize, dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(weight);
    const ids_tensor = tensorFromCt(ids);
    if (isBf16Weight(weight_tensor)) return null;
    try ensureF32OrQuantized(weight_tensor);
    if (ids_tensor.dtype != .i32 or ids_tensor.quant_type != null) return null;
    if (ids_tensor.elem_count != total) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding_tensor ids_elems={d} total={d}", .{ ids_tensor.elem_count, total });
        return error.InvalidShape;
    }
    if (dim == 0 or weight_tensor.elem_count % dim != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding_tensor weight_elems={d} dim={d}", .{ weight_tensor.elem_count, dim });
        return error.InvalidShape;
    }

    const out_count = try checkedMul(total, dim);
    const shape = try allocShape2(self.allocator, total, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => try self.kernels.launchEmbeddingLookupI32Q8_0F32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim),
                .Q4_0 => try self.kernels.launchEmbeddingLookupI32Q4_0F32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim),
                .Q4_K => try self.kernels.launchEmbeddingLookupI32Q4KF32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim),
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else {
        try self.kernels.launchEmbeddingLookupI32F32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim);
    }
    self.stats.launch_embedding += 1;
    return createTensor(self, device, shape, out_count);
}

fn takeRows(ctx: *anyopaque, request: *const ops.TakeRowsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(request.input);
    try ensureF32(input_tensor);
    if (request.dim == 0 or request.rows != request.row_ids.len) return error.InvalidShape;
    if (input_tensor.elem_count % request.dim != 0) return error.InvalidShape;
    const source_rows = input_tensor.elem_count / request.dim;
    for (request.row_ids) |row_id| {
        if (row_id >= source_rows) return error.InvalidShape;
    }
    const row_ids_device = try uploadTempU32(self, request.row_ids);

    const out_count = try checkedMul(request.rows, request.dim);
    const shape = try allocShape2(self.allocator, request.rows, request.dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchTakeRowsF32(&self.ctx, device, input_tensor.buffer, row_ids_device, source_rows, request.rows, request.dim);
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, out_count);
}

fn glinerWordEmbeddings(ctx: *anyopaque, request: *const ops.GlinerWordEmbeddingsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const hidden_tensor = tensorFromCt(request.hidden);
    try ensureF32(hidden_tensor);
    if (request.batch == 0 or request.seq_len == 0 or request.hidden_size == 0) return error.InvalidShape;
    const token_count = try checkedMul(request.batch, request.seq_len);
    if (request.words_mask.len < token_count) return error.InvalidShape;
    try ensureCount(hidden_tensor, try checkedMul(token_count, request.hidden_size));

    const out_rows = try checkedMul(request.batch, request.num_words);
    const out_count = try checkedMul(out_rows, request.hidden_size);
    const shape = try allocShape2(self.allocator, out_rows, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (out_count == 0) return try createTensor(self, device, shape, out_count);

    const mask_device = try uploadTempI64(self, request.words_mask[0..token_count]);
    try self.kernels.launchGlinerWordEmbeddingsF32(
        &self.ctx,
        device,
        hidden_tensor.buffer,
        mask_device,
        request.batch,
        request.seq_len,
        request.hidden_size,
        request.num_words,
    );
    self.stats.launch_embedding += 1;
    return try createTensor(self, device, shape, out_count);
}

fn glinerLabelGruCombined(ctx: *anyopaque, request: *const ops.GlinerLabelGruCombinedRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const label_tensor = tensorFromCt(request.label_embeddings);
    try ensureF32(label_tensor);
    if (request.num_labels == 0 or request.hidden_size == 0) return error.InvalidShape;
    try ensureCount(label_tensor, try checkedMul(request.num_labels, request.hidden_size));

    const pos_w = try getWeight(ctx, "count_embed.pos_embedding.weight");
    const pos_tensor = tensorFromCt(pos_w);
    if (pos_tensor.elem_count < request.hidden_size) return error.InvalidShape;

    const label_count = try checkedMul(request.num_labels, request.hidden_size);
    const gate_dim = try checkedMul(request.hidden_size, 3);

    const pos_ct = if (pos_tensor.quant_type == null) blk: {
        if (pos_tensor.dtype != .f32) return null;
        const pos_shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
        var pos_shape_owned = false;
        errdefer if (!pos_shape_owned) self.allocator.free(pos_shape);
        var pos_device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
        var pos_device_owned = false;
        errdefer if (!pos_device_owned) pos_device.free(&self.ctx);
        try self.kernels.launchRepeatFirstRowF32(&self.ctx, pos_device, pos_tensor.buffer, request.num_labels, request.hidden_size);
        self.stats.launch_other += 1;
        const repeated = try createTensor(self, pos_device, pos_shape, label_count);
        pos_shape_owned = true;
        pos_device_owned = true;
        break :blk repeated;
    } else blk: {
        if (!isKnownQuant(pos_tensor, .Q4_K)) return null;
        if (request.hidden_size == 0 or request.hidden_size % 256 != 0) return error.InvalidShape;
        const zero_ids = try self.allocator.alloc(i64, request.num_labels);
        defer self.allocator.free(zero_ids);
        @memset(zero_ids, 0);
        break :blk try embeddingLookup(ctx, pos_w, zero_ids, request.num_labels, request.hidden_size);
    };
    defer freeTensor(ctx, pos_ct);

    const w_ih = try getWeight(ctx, "count_embed.gru.weight_ih_l0");
    const b_ih = try getWeight(ctx, "count_embed.gru.bias_ih_l0");
    const gi = try linear(ctx, pos_ct, w_ih, b_ih, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gi);

    const w_hh = try getWeight(ctx, "count_embed.gru.weight_hh_l0");
    const b_hh = try getWeight(ctx, "count_embed.gru.bias_hh_l0");
    const gh = try linear(ctx, request.label_embeddings, w_hh, b_hh, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gh);

    const gi_tensor = tensorFromCt(gi);
    const gh_tensor = tensorFromCt(gh);
    const shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchGlinerGruCombineF32(
        &self.ctx,
        device,
        label_tensor.buffer,
        gi_tensor.buffer,
        gh_tensor.buffer,
        request.num_labels,
        request.hidden_size,
    );
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, label_count);
}

fn glinerGatherConcatRelu(ctx: *anyopaque, request: *const ops.GlinerGatherConcatReluRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.kernels.gliner_gather_concat_relu_f32 == null) return null;
    if (request.start_rows.len != request.rows or request.end_rows.len != request.rows) return error.InvalidShape;

    const start_tensor = tensorFromCt(request.start);
    const end_tensor = tensorFromCt(request.end);
    try ensureF32(start_tensor);
    try ensureF32(end_tensor);
    try ensureCount(start_tensor, try checkedMul(request.source_rows, request.dim));
    try ensureCount(end_tensor, try checkedMul(request.source_rows, request.dim));

    const row_devices = try uploadTempU32Pair(self, request.start_rows, request.end_rows);
    const out_dim = try checkedMul(request.dim, 2);
    const out_count = try checkedMul(request.rows, out_dim);
    const shape = try allocShape2(self.allocator, request.rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchGlinerGatherConcatReluF32(
        &self.ctx,
        device,
        start_tensor.buffer,
        end_tensor.buffer,
        row_devices.first,
        row_devices.second,
        request.source_rows,
        request.rows,
        request.dim,
    );
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, out_count);
}

fn tcQuantBuffer(tensor: *const CudaTensor, layout: CudaTensorCoreQuantLayout) ?buffer_mod.DeviceBuffer {
    const tc_quant = tensor.tc_quant orelse return null;
    if (tc_quant.layout != layout) return null;
    return tc_quant.buffer;
}

fn tcFallbackReason(tensor: *const CudaTensor, variant: kernels_mod.QMatmulVariant, layout: CudaTensorCoreQuantLayout) CudaDispatchFallback {
    if (variant != .tc_hmma) return .explicit_simt;
    if (tcQuantBuffer(tensor, layout) == null) return .tc_no_packed_weight;
    return .tc_missing_symbol;
}

fn tcPairFallbackReason(a: *const CudaTensor, b: *const CudaTensor, variant: kernels_mod.QMatmulVariant, layout: CudaTensorCoreQuantLayout) CudaDispatchFallback {
    if (variant != .tc_hmma) return .explicit_simt;
    if (tcQuantBuffer(a, layout) == null or tcQuantBuffer(b, layout) == null) return .tc_no_packed_weight;
    return .tc_missing_symbol;
}

fn tcUnavailableReason() CudaDispatchFallback {
    return if (cudaTensorCoreQuantRequested()) .tc_unsupported_shape else .tc_not_requested;
}

fn missingTcSymbolFallback(err: anyerror) anyerror!bool {
    return switch (err) {
        error.CudaSymbolMissing => false,
        else => err,
    };
}

fn launchQ8TcHmmaNoBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0TcHmmaF32(&self.ctx, dst, input, packed_buffer, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ8TcHmmaBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0BiasTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ8TcHmmaBiasGelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0BiasGeluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ8TcHmmaBiasAdd(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    residual: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0BiasAddTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, residual, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaNoBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KTcHmmaF32(&self.ctx, dst, input, packed_buffer, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasGelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasGeluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasAdd(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    residual: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasAddTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, residual, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasQuickGelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasQuickGeluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasRelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasReluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn simtQMatmulFallbackVariant(variant: kernels_mod.QMatmulVariant) kernels_mod.QMatmulVariant {
    return if (variant == .tc_hmma) .fast_r4c4 else variant;
}

fn linear(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32OrQuantized(weight_tensor);
    try ensureF32(bias_tensor);
    const input_expected = try checkedMul(rows, in_dim);
    if (input_tensor.elem_count != input_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear input_elems={d} rows={d} in_dim={d}", .{ input_tensor.elem_count, rows, in_dim });
        return error.InvalidShape;
    }
    const weight_expected = try checkedMul(out_dim, in_dim);
    if (weight_tensor.elem_count != weight_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear weight_elems={d} out_dim={d} in_dim={d}", .{ weight_tensor.elem_count, out_dim, in_dim });
        return error.InvalidShape;
    }
    if (bias_tensor.elem_count != out_dim) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear bias_elems={d} out_dim={d}", .{ bias_tensor.elem_count, out_dim });
        return error.InvalidShape;
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => if (mxbaiQ8Variant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ8TcHmmaBias(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear, .q8_0, .q8_tc_hmma, .bias, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ8_0BiasVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear, .q8_0, .q8_simt, .bias, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
                    }
                } else {
                    try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    try self.kernels.launchAddBiasRowsF32(&self.ctx, device, bias_tensor.buffer, rows, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q8_0, .q8_simt, .bias, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                .Q4_K => if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ4KTcHmmaBias(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_tc_hmma, .bias, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ4KBiasVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_simt, .bias, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
                    }
                } else if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim)) {
                    try self.kernels.launchLinearQ4KSpanBiasTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_span_simt, .bias, .specialized_span, rows, in_dim, out_dim, 0);
                } else if (rows >= 2) {
                    try self.kernels.launchLinearQ4KBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_simt, .bias, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                } else {
                    try self.kernels.launchLinearQ4KBiasTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_simt, .bias, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else {
        if (rows >= 2 and in_dim >= 256 and out_dim >= 4) {
            try self.kernels.launchLinearBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear, .f32, .f32_cuda, .bias, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearBiasF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear, .f32, .f32_cuda, .bias, .none, rows, in_dim, out_dim, 0);
        }
    }
    self.stats.launch_linear += 1;
    return createTensor(self, device, shape, out_count);
}

fn linearPlanned(ctx: *anyopaque, request: *const ops.LinearPlannedRequest) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    try recordQuantMatmulPlan(self, weight_tensor, request.operator_plan);
    return linear(ctx, request.input, request.weight, request.bias, request.rows, request.in_dim, request.out_dim);
}

fn linearQuickGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    if (!isKnownQuant(weight_tensor, .Q4_K)) return null;
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
        if (try launchQ4KTcHmmaBiasQuickGelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_quick_gelu, .q4_k, .q4_tc_hmma, .bias_quick_gelu, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasQuickGeluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_quick_gelu, .q4_k, .q4_simt, .bias_quick_gelu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearQ4KBiasQuickGeluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_quick_gelu, .q4_k, .q4_simt, .bias_quick_gelu, tcUnavailableReason(), rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn linearRelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (!use_q4 and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (use_q4) {
        if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
            if (try launchQ4KTcHmmaBiasRelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
                self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_tc_hmma, .bias_relu, .none, rows, in_dim, out_dim, 0);
            } else if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim)) {
                try self.kernels.launchLinearQ4KSpanBiasReluTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_span_simt, .bias_relu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
            } else {
                try self.kernels.launchLinearQ4KBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_simt, .bias_relu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
            }
        } else if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim)) {
            try self.kernels.launchLinearQ4KSpanBiasReluTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_span_simt, .bias_relu, .specialized_span, rows, in_dim, out_dim, 0);
        } else if (rows >= 2) {
            try self.kernels.launchLinearQ4KBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_simt, .bias_relu, tcUnavailableReason(), rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasReluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_simt, .bias_relu, tcUnavailableReason(), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_relu, .f32, .f32_cuda, .bias_relu, .none, rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn linearGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    const q8_variant = if (isKnownQuant(weight_tensor, .Q8_0)) mxbaiQ8Variant(rows, in_dim, out_dim) else null;
    const q4_variant = if (isKnownQuant(weight_tensor, .Q4_K)) mxbaiQ4Variant(rows, in_dim, out_dim) else null;
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (q8_variant == null and q4_variant == null and !use_dense) return null;
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (q8_variant) |variant| {
        if (try launchQ8TcHmmaBiasGelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q8_0, .q8_tc_hmma, .bias_gelu, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ8_0BiasGeluVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q8_0, .q8_simt, .bias_gelu, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
        }
    } else if (q4_variant) |variant| {
        if (try launchQ4KTcHmmaBiasGelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q4_k, .q4_tc_hmma, .bias_gelu, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasGeluVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q4_k, .q4_simt, .bias_gelu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearBiasGeluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_gelu, .f32, .f32_cuda, .bias_gelu, .none, rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn linearAdd(ctx: *anyopaque, input: CT, weight: CT, bias: CT, residual: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    const residual_tensor = tensorFromCt(residual);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureF32(residual_tensor);
    const q8_variant = if (isKnownQuant(weight_tensor, .Q8_0)) mxbaiQ8Variant(rows, in_dim, out_dim) else null;
    const q4_variant = if (isKnownQuant(weight_tensor, .Q4_K)) mxbaiQ4Variant(rows, in_dim, out_dim) else null;
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (q8_variant == null and q4_variant == null and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);
    const out_count = try checkedMul(rows, out_dim);
    try ensureCount(residual_tensor, out_count);

    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (q8_variant) |variant| {
        if (try launchQ8TcHmmaBiasAdd(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_add, .q8_0, .q8_tc_hmma, .bias_add, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ8_0BiasAddVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_add, .q8_0, .q8_simt, .bias_add, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
        }
    } else if (q4_variant) |variant| {
        if (try launchQ4KTcHmmaBiasAdd(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_add, .q4_k, .q4_tc_hmma, .bias_add, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasAddVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_add, .q4_k, .q4_simt, .bias_add, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearBiasAddTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_add, .f32, .f32_cuda, .bias_add, .none, rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn linearNoBias(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    const input_expected = try checkedMul(rows, in_dim);
    if (input_tensor.elem_count != input_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear_no_bias input_elems={d} rows={d} in_dim={d}", .{ input_tensor.elem_count, rows, in_dim });
        return error.InvalidShape;
    }
    const weight_expected = try checkedMul(out_dim, in_dim);
    if (weight_tensor.elem_count != weight_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear_no_bias weight_elems={d} out_dim={d} in_dim={d}", .{ weight_tensor.elem_count, out_dim, in_dim });
        return error.InvalidShape;
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => if (mxbaiQ8Variant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ8TcHmmaNoBias(self, variant, device, input_tensor.buffer, weight_tensor, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q8_0, .q8_tc_hmma, .none, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ8_0VariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q8_0, .q8_simt, .none, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
                    }
                } else {
                    if (rows == 1) {
                        self.kernels.launchLinearQ8_0Tile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                            else => return err,
                        };
                    } else {
                        try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    }
                    self.dispatch_stats.note(self.allocator, .linear_no_bias, .q8_0, .q8_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                .Q4_0 => {
                    try self.kernels.launchLinearQ4_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_0, .q4_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                .Q4_K => if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ4KTcHmmaNoBias(self, variant, device, input_tensor.buffer, weight_tensor, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_k, .q4_tc_hmma, .none, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_k, .q4_simt, .none, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
                    }
                } else {
                    if (rows == 1 and cudaEnableQ4KDecodeFast()) {
                        var used_fallback = false;
                        if (cudaEnableQ4KDecodeTile8()) {
                            self.kernels.launchLinearQ4KTile8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
                                error.CudaKernelUnavailable, error.InvalidCudaState => {
                                    used_fallback = true;
                                    self.stats.q4k_decode_fast_fallbacks += 1;
                                    try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                },
                                else => return err,
                            };
                        } else {
                            self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
                                error.CudaKernelUnavailable, error.InvalidCudaState => {
                                    used_fallback = true;
                                    self.stats.q4k_decode_fast_fallbacks += 1;
                                    try self.kernels.launchLinearQ4KF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                },
                                else => return err,
                            };
                        }
                        if (!used_fallback) self.stats.q4k_decode_fast_hits += 1;
                    } else {
                        try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    }
                    self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_k, .q4_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else if (isBf16Weight(weight_tensor)) {
        if (try tryCublasLtBf16Linear(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim)) {
            // cuBLASLt counters are updated by the helper.
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .bf16, .dense_lt, .none, .none, rows, in_dim, out_dim, 0);
        } else {
            self.stats.bf16_cublaslt_fallbacks += 1;
            try self.kernels.launchLinearBf16WeightF32Tiled(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
            self.stats.bf16_scalar_linear_calls += 1;
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .bf16, .dense_cuda, .none, .none, rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_no_bias, .f32, .f32_cuda, .none, .none, rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return createTensor(self, device, shape, out_count);
}

fn linearNoBiasPlanned(ctx: *anyopaque, request: *const ops.LinearNoBiasPlannedRequest) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    try recordQuantMatmulPlan(self, weight_tensor, request.operator_plan);
    return linearNoBias(ctx, request.input, request.weight, request.rows, request.in_dim, request.out_dim);
}

fn argmaxLastRow(ctx: *anyopaque, tensor: CT, rows: usize, dim: usize) anyerror!?u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (rows == 0 or dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, dim));

    var token_device = try allocDeviceBuffer(self, @sizeOf(u32));
    defer releaseDeviceBuffer(self, &token_device);
    try self.kernels.launchArgmaxLastRowF32(&self.ctx, token_device, input_tensor.buffer, rows, dim);
    self.stats.launch_argmax += 1;
    var token: u32 = 0;
    try copyToHostTracked(self, token_device, std.mem.asBytes(&token));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    return token;
}

fn argmaxRows(ctx: *anyopaque, tensor: CT, row_start: usize, row_count: usize, dim: usize, allocator: std.mem.Allocator) anyerror!?[]u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (row_count == 0 or dim == 0) return error.InvalidShape;
    const row_end = std.math.add(usize, row_start, row_count) catch return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(row_end, dim));

    var token_device = try allocDeviceBuffer(self, try checkedMul(row_count, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &token_device);
    const launched = try self.kernels.launchArgmaxRowsF32(&self.ctx, token_device, input_tensor.buffer, row_start, row_count, dim);
    if (!launched) return null;
    self.stats.launch_argmax += 1;

    const tokens = try allocator.alloc(u32, row_count);
    errdefer allocator.free(tokens);
    try copyToHostTracked(self, token_device, std.mem.sliceAsBytes(tokens));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    return tokens;
}

fn argmaxRowsSuppress(
    ctx: *anyopaque,
    tensor: CT,
    row_start: usize,
    row_count: usize,
    dim: usize,
    suppress_token_ids: []const i32,
    allocator: std.mem.Allocator,
) anyerror!?[]u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (suppress_token_ids.len == 0) return argmaxRows(ctx, tensor, row_start, row_count, dim, allocator);
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (row_count == 0 or dim == 0) return error.InvalidShape;
    const row_end = std.math.add(usize, row_start, row_count) catch return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(row_end, dim));

    var suppress_device = try allocDeviceBuffer(self, try checkedMul(suppress_token_ids.len, @sizeOf(i32)));
    defer releaseDeviceBuffer(self, &suppress_device);
    try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));

    var token_device = try allocDeviceBuffer(self, try checkedMul(row_count, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &token_device);
    const launched = try self.kernels.launchArgmaxRowsSuppressF32(
        &self.ctx,
        token_device,
        input_tensor.buffer,
        suppress_device,
        row_start,
        row_count,
        dim,
        suppress_token_ids.len,
    );
    if (!launched) return null;
    self.stats.launch_argmax += 1;

    const tokens = try allocator.alloc(u32, row_count);
    errdefer allocator.free(tokens);
    try copyToHostTracked(self, token_device, std.mem.sliceAsBytes(tokens));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    return tokens;
}

fn argmaxLastRowSuppressTensor(ctx: *anyopaque, tensor: CT, rows: usize, dim: usize, suppress_token_ids: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (suppress_token_ids.len == 0) return null;
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (rows == 0 or dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, dim));

    var suppress_device = try allocDeviceBuffer(self, suppress_token_ids.len * @sizeOf(i32));
    defer releaseDeviceBuffer(self, &suppress_device);
    try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));

    var token_device = try allocDeviceBuffer(self, @sizeOf(i32));
    errdefer token_device.free(&self.ctx);
    try self.kernels.launchArgmaxLastRowSuppressF32(
        &self.ctx,
        token_device,
        input_tensor.buffer,
        suppress_device,
        rows,
        dim,
        suppress_token_ids.len,
    );
    self.stats.launch_argmax += 1;
    const shape = try self.allocator.alloc(i64, 1);
    errdefer self.allocator.free(shape);
    shape[0] = 1;
    return createTensorWithDType(self, token_device, shape, 1, .i32);
}

fn linearNoBiasArgmaxLastRow(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?u32 {
    const logits = try linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
    defer freeTensor(ctx, logits);
    return argmaxLastRow(ctx, logits, rows, out_dim);
}

fn linearNoBiasArgmaxLastRowSuppressTensor(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize, suppress_token_ids: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    if (rows == 0 or in_dim == 0 or out_dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));

    const use_q8 = isKnownQuant(weight_tensor, .Q8_0);
    const use_q4_0 = isKnownQuant(weight_tensor, .Q4_0);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    if (!use_q8 and !use_q4_0 and !use_q4) return null;
    if (use_q8 and in_dim % 32 != 0) return null;
    if (use_q4_0 and in_dim % 32 != 0) return null;
    if (use_q4 and in_dim % 256 != 0) return null;
    if (use_q4_0) return null;

    const col_tiles = (out_dim + 3) / 4;
    var partial_values = try allocDeviceBuffer(self, try checkedMul(col_tiles, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &partial_values);
    var partial_indices = try allocDeviceBuffer(self, try checkedMul(col_tiles, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &partial_indices);

    var suppress_device: buffer_mod.DeviceBuffer = .{};
    if (suppress_token_ids.len != 0) {
        suppress_device = try allocDeviceBuffer(self, try checkedMul(suppress_token_ids.len, @sizeOf(i32)));
        try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));
    }
    defer if (suppress_device.len != 0) releaseDeviceBuffer(self, &suppress_device);

    var token_device = try allocDeviceBuffer(self, @sizeOf(i32));
    var token_device_owned = false;
    errdefer if (!token_device_owned) releaseDeviceBuffer(self, &token_device);

    if (use_q8) {
        self.kernels.launchLinearQ8_0ArgmaxTile4F32(
            &self.ctx,
            token_device,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.lm_head_argmax_fallbacks += 1;
                releaseDeviceBuffer(self, &token_device);
                token_device_owned = true;
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q8 += 1;
    } else {
        self.kernels.launchLinearQ4KArgmaxTile4F32(
            &self.ctx,
            token_device,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.lm_head_argmax_fallbacks += 1;
                releaseDeviceBuffer(self, &token_device);
                token_device_owned = true;
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q4 += 1;
    }

    self.stats.launch_linear += 1;
    self.stats.launch_argmax += 1;
    const shape = try self.allocator.alloc(i64, 1);
    errdefer self.allocator.free(shape);
    shape[0] = 1;
    const token_tensor = try createTensorWithDType(self, token_device, shape, 1, .i32);
    token_device_owned = true;
    return token_tensor;
}

fn linearNoBiasArgmaxRowsSuppressDevice(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    suppress_token_ids: []const i32,
) anyerror!?buffer_mod.DeviceBuffer {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    if (rows == 0 or in_dim == 0 or out_dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));

    const use_q8 = isKnownQuant(weight_tensor, .Q8_0);
    const use_q4_0 = isKnownQuant(weight_tensor, .Q4_0);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    if (!use_q8 and !use_q4_0 and !use_q4) return null;
    if (use_q8 and in_dim % 32 != 0) return null;
    if (use_q4_0 and in_dim % 32 != 0) return null;
    if (use_q4 and in_dim % 256 != 0) return null;

    const col_tiles = (out_dim + 3) / 4;
    const partial_count = try checkedMul(rows, col_tiles);
    var partial_values = try allocDeviceBuffer(self, try checkedMul(partial_count, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &partial_values);
    var partial_indices = try allocDeviceBuffer(self, try checkedMul(partial_count, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &partial_indices);

    var suppress_device: buffer_mod.DeviceBuffer = .{};
    if (suppress_token_ids.len != 0) {
        suppress_device = try allocDeviceBuffer(self, try checkedMul(suppress_token_ids.len, @sizeOf(i32)));
        try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));
    }
    defer if (suppress_device.len != 0) releaseDeviceBuffer(self, &suppress_device);

    var token_device = try allocDeviceBuffer(self, try checkedMul(rows, @sizeOf(u32)));
    var token_device_owned = false;
    errdefer if (!token_device_owned) releaseDeviceBuffer(self, &token_device);

    if (use_q8) {
        self.kernels.launchLinearQ8_0ArgmaxRowsTile4F32(
            &self.ctx,
            token_device,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.lm_head_argmax_fallbacks += 1;
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q8 += 1;
    } else if (use_q4_0) {
        self.kernels.launchLinearQ4_0ArgmaxRowsTile4F32(
            &self.ctx,
            token_device,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.lm_head_argmax_fallbacks += 1;
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q4 += 1;
    } else {
        self.kernels.launchLinearQ4KArgmaxRowsTile4F32(
            &self.ctx,
            token_device,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.lm_head_argmax_fallbacks += 1;
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q4 += 1;
    }

    self.stats.launch_linear += 1;
    self.stats.launch_argmax += 1;
    token_device_owned = true;
    return token_device;
}

fn linearNoBiasArgmaxRowsSuppress(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    suppress_token_ids: []const i32,
    allocator: std.mem.Allocator,
) anyerror!?[]u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var token_device = (try linearNoBiasArgmaxRowsSuppressDevice(ctx, input, weight, rows, in_dim, out_dim, suppress_token_ids)) orelse return null;
    defer releaseDeviceBuffer(self, &token_device);
    const tokens = try allocator.alloc(u32, rows);
    errdefer allocator.free(tokens);
    try copyToHostTracked(self, token_device, std.mem.sliceAsBytes(tokens));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    self.stats.mtp_verify_commit_choice_downloads += 1;
    return tokens;
}

fn linearNoBiasArgmaxLastRowTensor(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (try linearNoBiasArgmaxLastRowSuppressTensor(ctx, input, weight, rows, in_dim, out_dim, &.{})) |token| {
        return token;
    }

    const logits = try linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
    defer freeTensor(ctx, logits);

    var token_device = try allocDeviceBuffer(self, @sizeOf(i32));
    errdefer token_device.free(&self.ctx);
    try self.kernels.launchArgmaxLastRowF32(&self.ctx, token_device, tensorFromCt(logits).buffer, rows, out_dim);
    self.stats.launch_argmax += 1;
    const shape = try self.allocator.alloc(i64, 1);
    errdefer self.allocator.free(shape);
    shape[0] = 1;
    return createTensorWithDType(self, token_device, shape, 1, .i32);
}

fn gemma4MtpMaskedArgmax(ctx: *anyopaque, request: *const ops.Gemma4MtpMaskedArgmaxRequest) anyerror!?u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const logits_tensor = tensorFromCt(request.logits);
    const centroid_tensor = tensorFromCt(request.centroid_logits);
    const ordering_tensor = tensorFromCt(request.token_ordering);
    try ensureF32(logits_tensor);
    try ensureF32(centroid_tensor);
    try ensureF32(ordering_tensor);
    if (request.vocab_size == 0 or request.num_centroids == 0 or request.top_k == 0) return null;
    try ensureCount(logits_tensor, request.vocab_size);
    try ensureCount(centroid_tensor, request.num_centroids);
    try ensureCount(ordering_tensor, request.vocab_size);

    var token_device = try allocDeviceBuffer(self, @sizeOf(u32));
    defer releaseDeviceBuffer(self, &token_device);
    const launched = try self.kernels.launchGemma4MtpMaskedArgmaxF32(
        &self.ctx,
        token_device,
        logits_tensor.buffer,
        centroid_tensor.buffer,
        ordering_tensor.buffer,
        request.vocab_size,
        request.num_centroids,
        request.top_k,
        request.use_inverse_ordering,
    );
    if (!launched) {
        self.stats.mtp_masked_argmax_fallbacks += 1;
        return null;
    }
    self.stats.launch_argmax += 1;
    self.stats.mtp_masked_argmax_hits += 1;
    var token: u32 = 0;
    try copyToHostTracked(self, token_device, std.mem.asBytes(&token));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    return token;
}

fn gemma4MtpTokenIsEos(token: usize, eos_token_ids: []const i32) bool {
    for (eos_token_ids) |eos| {
        if (eos >= 0 and token == @as(usize, @intCast(eos))) return true;
    }
    return false;
}

const mtp_verify_commit_result_words = 12;

fn gemma4MtpVerifyCommitDeviceResult(
    self: *CudaCompute,
    request: *const ops.Gemma4MtpVerifyCommitRequest,
    verify_len: usize,
) anyerror!?ops.Gemma4MtpVerifyCommitResult {
    if (!cudaMtpVerifyDeviceResultEnabled()) return null;

    var target_choices_device = (try linearNoBiasArgmaxRowsSuppressDevice(
        self,
        request.input,
        request.weight,
        verify_len,
        request.in_dim,
        request.out_dim,
        request.suppress_token_ids,
    )) orelse {
        self.stats.mtp_verify_commit_device_fallbacks += 1;
        return null;
    };
    defer releaseDeviceBuffer(self, &target_choices_device);

    var draft_tokens_device = try allocDeviceBuffer(self, try checkedMul(request.draft_tokens.len, @sizeOf(i64)));
    defer releaseDeviceBuffer(self, &draft_tokens_device);
    try copyFromHostTracked(self, draft_tokens_device, std.mem.sliceAsBytes(request.draft_tokens));

    var eos_token_ids_device: buffer_mod.DeviceBuffer = .{};
    if (request.eos_token_ids.len != 0) {
        eos_token_ids_device = try allocDeviceBuffer(self, try checkedMul(request.eos_token_ids.len, @sizeOf(i32)));
        try copyFromHostTracked(self, eos_token_ids_device, std.mem.sliceAsBytes(request.eos_token_ids));
    }
    defer if (eos_token_ids_device.len != 0) releaseDeviceBuffer(self, &eos_token_ids_device);

    var result_device = try allocDeviceBuffer(self, mtp_verify_commit_result_words * @sizeOf(u32));
    defer releaseDeviceBuffer(self, &result_device);
    const launched = try self.kernels.launchGemma4MtpVerifyCommitU32(
        &self.ctx,
        result_device,
        target_choices_device,
        draft_tokens_device,
        eos_token_ids_device,
        request.draft_tokens.len,
        request.eos_token_ids.len,
        request.accept_bonus,
    );
    if (!launched) {
        self.stats.mtp_verify_commit_device_fallbacks += 1;
        return null;
    }
    self.stats.launch_other += 1;

    var words: [mtp_verify_commit_result_words]u32 = undefined;
    try copyToHostTracked(self, result_device, std.mem.sliceAsBytes(&words));
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    self.stats.mtp_verify_commit_result_downloads += 1;
    if (words[11] != 0) return error.InvalidModelOutput;

    self.stats.mtp_verify_commit_device_hits += 1;
    return .{
        .target_choices = &.{},
        .target_choices_owned = false,
        .compact_device_result = true,
        .correction_token = if (words[2] != 0) words[9] else null,
        .bonus_token = if (words[3] != 0) words[10] else null,
        .matched_drafts = @intCast(words[0]),
        .accepted = @intCast(words[1]),
        .correction_added = words[2] != 0,
        .had_bonus = words[3] != 0,
        .bonus_skipped = words[4] != 0,
        .hit_eos = words[5] != 0,
        .commit_forward_required = words[6] != 0,
        .accepted_hidden_row = if (words[7] != 0) @intCast(words[8]) else null,
    };
}

fn gemma4MtpVerifyCommit(ctx: *anyopaque, request: *const ops.Gemma4MtpVerifyCommitRequest) anyerror!?ops.Gemma4MtpVerifyCommitResult {
    if (request.draft_tokens.len == 0) return null;
    const verify_len = request.draft_tokens.len + 1;
    if (request.rows < verify_len) return error.InvalidTensorShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));

    if (try gemma4MtpVerifyCommitDeviceResult(self, request, verify_len)) |result| {
        return result;
    }

    const target_choices = (try linearNoBiasArgmaxRowsSuppress(
        ctx,
        request.input,
        request.weight,
        verify_len,
        request.in_dim,
        request.out_dim,
        request.suppress_token_ids,
        request.allocator,
    )) orelse return null;
    errdefer request.allocator.free(target_choices);
    if (target_choices.len < verify_len) return error.InvalidTensorShape;

    var matched_drafts: usize = 0;
    var accepted: usize = 0;
    var correction_added = false;
    var hit_eos = false;

    for (request.draft_tokens, 0..) |draft_token_raw, i| {
        if (draft_token_raw < 0) return error.InvalidModelOutput;
        const draft_token: usize = @intCast(draft_token_raw);
        const target_choice: usize = @intCast(target_choices[i]);
        if (target_choice == draft_token) {
            matched_drafts += 1;
            accepted += 1;
            if (gemma4MtpTokenIsEos(target_choice, request.eos_token_ids)) {
                hit_eos = true;
                break;
            }
        } else {
            correction_added = true;
            accepted = matched_drafts + 1;
            if (gemma4MtpTokenIsEos(target_choice, request.eos_token_ids)) {
                hit_eos = true;
            }
            break;
        }
    }

    const can_bonus = matched_drafts == request.draft_tokens.len and !hit_eos;
    const had_bonus = can_bonus and request.accept_bonus;
    const bonus_skipped = can_bonus and !request.accept_bonus;
    if (had_bonus) {
        const bonus_token: usize = @intCast(target_choices[request.draft_tokens.len]);
        accepted += 1;
        if (gemma4MtpTokenIsEos(bonus_token, request.eos_token_ids)) {
            hit_eos = true;
        }
    }

    const commit_forward_required = correction_added or had_bonus;
    const accepted_hidden_row: ?usize = if (accepted != 0 and !commit_forward_required)
        accepted - 1
    else
        null;

    return .{
        .target_choices = target_choices,
        .target_choices_owned = true,
        .matched_drafts = matched_drafts,
        .accepted = accepted,
        .correction_added = correction_added,
        .had_bonus = had_bonus,
        .bonus_skipped = bonus_skipped,
        .hit_eos = hit_eos,
        .commit_forward_required = commit_forward_required,
        .accepted_hidden_row = accepted_hidden_row,
    };
}

fn linearNoBiasQkv(ctx: *anyopaque, input: CT, q_weight: CT, k_weight: CT, v_weight: CT, rows: usize, in_dim: usize, q_out_dim: usize, kv_out_dim: usize) anyerror!?ops.LinearNoBiasTripleResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (cudaDisableFusedQkv()) return null;
    const input_tensor = tensorFromCt(input);
    const q_weight_tensor = tensorFromCt(q_weight);
    const k_weight_tensor = tensorFromCt(k_weight);
    const v_weight_tensor = tensorFromCt(v_weight);
    try ensureF32(input_tensor);
    const use_q8 = isKnownQuant(q_weight_tensor, .Q8_0) and isKnownQuant(k_weight_tensor, .Q8_0) and isKnownQuant(v_weight_tensor, .Q8_0);
    const use_q4 = isKnownQuant(q_weight_tensor, .Q4_K) and isKnownQuant(k_weight_tensor, .Q4_K) and isKnownQuant(v_weight_tensor, .Q4_K);
    const use_q4_q4_f32 = isKnownQuant(q_weight_tensor, .Q4_K) and isKnownQuant(k_weight_tensor, .Q4_K) and
        v_weight_tensor.dtype == .f32 and v_weight_tensor.quant_type == null;
    const use_f32 = q_weight_tensor.dtype == .f32 and q_weight_tensor.quant_type == null and
        k_weight_tensor.dtype == .f32 and k_weight_tensor.quant_type == null and
        v_weight_tensor.dtype == .f32 and v_weight_tensor.quant_type == null;
    const use_bf16 = isBf16Weight(q_weight_tensor) and isBf16Weight(k_weight_tensor) and isBf16Weight(v_weight_tensor);
    if (!use_q8 and !use_q4 and !use_q4_q4_f32 and !use_f32 and !use_bf16) {
        self.stats.qkv_fallback_unsupported += 1;
        return null;
    }
    if (use_f32) {
        try ensureF32(q_weight_tensor);
        try ensureF32(k_weight_tensor);
        try ensureF32(v_weight_tensor);
    } else if (use_q4_q4_f32) {
        try ensureF32(v_weight_tensor);
    }
    if (in_dim == 0 or q_out_dim == 0 or kv_out_dim == 0) return null;
    if (use_q8 and in_dim % 32 != 0) return null;
    if ((use_q4 or use_q4_q4_f32) and in_dim % 256 != 0) return null;
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(q_weight_tensor, try checkedMul(q_out_dim, in_dim));
    try ensureCount(k_weight_tensor, try checkedMul(kv_out_dim, in_dim));
    try ensureCount(v_weight_tensor, try checkedMul(kv_out_dim, in_dim));

    const q_count = try checkedMul(rows, q_out_dim);
    const kv_count = try checkedMul(rows, kv_out_dim);
    const q_shape = try allocShape2(self.allocator, rows, q_out_dim);
    var q_shape_owned = false;
    errdefer if (!q_shape_owned) self.allocator.free(q_shape);
    const k_shape = try allocShape2(self.allocator, rows, kv_out_dim);
    var k_shape_owned = false;
    errdefer if (!k_shape_owned) self.allocator.free(k_shape);
    const v_shape = try allocShape2(self.allocator, rows, kv_out_dim);
    var v_shape_owned = false;
    errdefer if (!v_shape_owned) self.allocator.free(v_shape);
    var q_device = try allocDeviceBuffer(self, q_count * @sizeOf(f32));
    var q_device_owned = false;
    errdefer if (!q_device_owned) q_device.free(&self.ctx);
    var k_device = try allocDeviceBuffer(self, kv_count * @sizeOf(f32));
    var k_device_owned = false;
    errdefer if (!k_device_owned) k_device.free(&self.ctx);
    var v_device = try allocDeviceBuffer(self, kv_count * @sizeOf(f32));
    var v_device_owned = false;
    errdefer if (!v_device_owned) v_device.free(&self.ctx);

    if (use_q8) {
        self.kernels.launchLinearQ8_0QkvNoBiasTile4F32(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_q8 += 1;
        self.stats.launch_linear_qkv += 1;
    } else if (use_q4) {
        self.kernels.launchLinearQ4KQkvNoBiasTiledF32(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_q4 += 1;
        self.stats.launch_linear_qkv += 1;
    } else if (use_q4_q4_f32) {
        self.kernels.launchLinearQ4KQ4KF32QkvNoBiasTiled(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_q4_q4_f32 += 1;
        self.stats.launch_linear_qkv += 1;
    } else if (use_bf16) {
        if (try tryCublasLtBf16Qkv(
            self,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        )) {
            // cuBLASLt counters are updated by the helper.
        } else {
            self.stats.bf16_cublaslt_fallbacks += 1;
            self.kernels.launchLinearBf16WeightF32QkvNoBiasTiled(
                &self.ctx,
                q_device,
                k_device,
                v_device,
                input_tensor.buffer,
                q_weight_tensor.buffer,
                k_weight_tensor.buffer,
                v_weight_tensor.buffer,
                rows,
                in_dim,
                q_out_dim,
                kv_out_dim,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable => {
                    self.stats.qkv_kernel_unavailable += 1;
                    return null;
                },
                else => return err,
            };
            self.stats.bf16_scalar_qkv_calls += 1;
        }
        self.stats.qkv_fused_f32 += 1;
        self.stats.launch_linear_qkv += 1;
    } else {
        self.kernels.launchLinearF32QkvNoBiasTiled(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_f32 += 1;
        self.stats.launch_linear_qkv += 1;
    }

    const q = try createTensor(self, q_device, q_shape, q_count);
    q_shape_owned = true;
    q_device_owned = true;
    errdefer freeTensor(ctx, q);
    const k = try createTensor(self, k_device, k_shape, kv_count);
    k_shape_owned = true;
    k_device_owned = true;
    errdefer freeTensor(ctx, k);
    const v = try createTensor(self, v_device, v_shape, kv_count);
    v_shape_owned = true;
    v_device_owned = true;
    return .{ .first = q, .second = k, .third = v };
}

fn linearTriple(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, weight_c: CT, bias_c: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearTripleResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const weight_c_tensor = tensorFromCt(weight_c);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);
    const bias_c_tensor = tensorFromCt(bias_c);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureF32(bias_c_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_c_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);
    try ensureCount(bias_c_tensor, out_dim);

    if (isKnownQuant(weight_a_tensor, .Q4_K) and isKnownQuant(weight_b_tensor, .Q4_K) and isKnownQuant(weight_c_tensor, .Q4_K)) {
        const out_count = try checkedMul(rows, out_dim);
        const shape_a = try allocShape2(self.allocator, rows, out_dim);
        var shape_a_owned = false;
        errdefer if (!shape_a_owned) self.allocator.free(shape_a);
        const shape_b = try allocShape2(self.allocator, rows, out_dim);
        var shape_b_owned = false;
        errdefer if (!shape_b_owned) self.allocator.free(shape_b);
        const shape_c = try allocShape2(self.allocator, rows, out_dim);
        var shape_c_owned = false;
        errdefer if (!shape_c_owned) self.allocator.free(shape_c);
        var device_a = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_a_owned = false;
        errdefer if (!device_a_owned) device_a.free(&self.ctx);
        var device_b = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_b_owned = false;
        errdefer if (!device_b_owned) device_b.free(&self.ctx);
        var device_c = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_c_owned = false;
        errdefer if (!device_c_owned) device_c.free(&self.ctx);
        try self.kernels.launchLinearQ4KTripleBiasTiledF32(
            &self.ctx,
            device_a,
            device_b,
            device_c,
            input_tensor.buffer,
            weight_a_tensor.buffer,
            bias_a_tensor.buffer,
            weight_b_tensor.buffer,
            bias_b_tensor.buffer,
            weight_c_tensor.buffer,
            bias_c_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        );
        self.stats.launch_linear += 1;
        const first = try createTensor(self, device_a, shape_a, out_count);
        shape_a_owned = true;
        device_a_owned = true;
        errdefer freeTensor(ctx, first);
        const second = try createTensor(self, device_b, shape_b, out_count);
        shape_b_owned = true;
        device_b_owned = true;
        errdefer freeTensor(ctx, second);
        const third = try createTensor(self, device_c, shape_c, out_count);
        shape_c_owned = true;
        device_c_owned = true;
        return .{ .first = first, .second = second, .third = third };
    }

    const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, second);
    const third = try linear(ctx, input, weight_c, bias_c, rows, in_dim, out_dim);
    return .{ .first = first, .second = second, .third = third };
}

fn linearNoBiasPair(ctx: *anyopaque, input: CT, weight_a: CT, weight_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearNoBiasPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);

    try ensureF32(input_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));

    const use_q8 = isKnownQuant(weight_a_tensor, .Q8_0) and isKnownQuant(weight_b_tensor, .Q8_0);
    const use_q4 = isKnownQuant(weight_a_tensor, .Q4_K) and isKnownQuant(weight_b_tensor, .Q4_K);
    if (!use_q8 and !use_q4) {
        self.stats.linear_pair_fallbacks += 1;
        const first = try linearNoBias(ctx, input, weight_a, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, first);
        const second = try linearNoBias(ctx, input, weight_b, rows, in_dim, out_dim);
        return .{ .first = first, .second = second };
    }
    if (use_q8 and in_dim % 32 != 0) return error.InvalidShape;
    if (use_q4 and in_dim % 256 != 0) return error.InvalidShape;

    const out_count = try checkedMul(rows, out_dim);
    const shape_a = try allocShape2(self.allocator, rows, out_dim);
    var shape_a_owned = false;
    errdefer if (!shape_a_owned) self.allocator.free(shape_a);
    const shape_b = try allocShape2(self.allocator, rows, out_dim);
    var shape_b_owned = false;
    errdefer if (!shape_b_owned) self.allocator.free(shape_b);
    var device_a = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_a_owned = false;
    errdefer if (!device_a_owned) device_a.free(&self.ctx);
    var device_b = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_b_owned = false;
    errdefer if (!device_b_owned) device_b.free(&self.ctx);

    if (use_q8) {
        self.kernels.launchLinearQ8_0PairNoBiasTile4F32(
            &self.ctx,
            device_a,
            device_b,
            input_tensor.buffer,
            weight_a_tensor.buffer,
            weight_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.linear_pair_fallbacks += 1;
                device_a.free(&self.ctx);
                device_a_owned = true;
                device_b.free(&self.ctx);
                device_b_owned = true;
                self.allocator.free(shape_a);
                shape_a_owned = true;
                self.allocator.free(shape_b);
                shape_b_owned = true;
                const first = try linearNoBias(ctx, input, weight_a, rows, in_dim, out_dim);
                errdefer freeTensor(ctx, first);
                const second = try linearNoBias(ctx, input, weight_b, rows, in_dim, out_dim);
                return .{ .first = first, .second = second };
            },
            else => return err,
        };
        self.stats.linear_pair_fused_q8 += 1;
    } else {
        self.kernels.launchLinearQ4KPairNoBiasTile4F32(
            &self.ctx,
            device_a,
            device_b,
            input_tensor.buffer,
            weight_a_tensor.buffer,
            weight_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.linear_pair_fallbacks += 1;
                device_a.free(&self.ctx);
                device_a_owned = true;
                device_b.free(&self.ctx);
                device_b_owned = true;
                self.allocator.free(shape_a);
                shape_a_owned = true;
                self.allocator.free(shape_b);
                shape_b_owned = true;
                const first = try linearNoBias(ctx, input, weight_a, rows, in_dim, out_dim);
                errdefer freeTensor(ctx, first);
                const second = try linearNoBias(ctx, input, weight_b, rows, in_dim, out_dim);
                return .{ .first = first, .second = second };
            },
            else => return err,
        };
        self.stats.linear_pair_fused_q4 += 1;
    }

    self.stats.launch_linear += 1;
    const first = try createTensor(self, device_a, shape_a, out_count);
    shape_a_owned = true;
    device_a_owned = true;
    errdefer freeTensor(ctx, first);
    const second = try createTensor(self, device_b, shape_b, out_count);
    shape_b_owned = true;
    device_b_owned = true;
    return .{ .first = first, .second = second };
}

const SpanQ4PairMode = enum {
    shared,
    shared_relu,
    separate,
};

fn spanQ4PairOp(mode: SpanQ4PairMode) CudaDispatchOp {
    return switch (mode) {
        .shared => .linear_pair,
        .shared_relu => .linear_pair_relu,
        .separate => .linear_pair_inputs,
    };
}

fn spanQ4PairEpilogue(mode: SpanQ4PairMode) CudaDispatchEpilogue {
    return switch (mode) {
        .shared => .pair,
        .shared_relu => .pair_relu,
        .separate => .pair_inputs,
    };
}

fn launchSpanQ4PairTc(
    self: *CudaCompute,
    mode: SpanQ4PairMode,
    variant: kernels_mod.QMatmulVariant,
    first_device: buffer_mod.DeviceBuffer,
    second_device: buffer_mod.DeviceBuffer,
    input_a_tensor: *const CudaTensor,
    input_b_tensor: ?*const CudaTensor,
    weight_a_tensor: *const CudaTensor,
    bias_a_tensor: *const CudaTensor,
    weight_b_tensor: *const CudaTensor,
    bias_b_tensor: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant == .tc_hmma and
        (tcQuantBuffer(weight_a_tensor, .q4_k_hmma) == null or tcQuantBuffer(weight_b_tensor, .q4_k_hmma) == null))
    {
        return false;
    }

    const first_tc = switch (mode) {
        .shared, .separate => try launchQ4KTcHmmaBias(self, variant, first_device, input_a_tensor.buffer, weight_a_tensor, bias_a_tensor.buffer, rows, in_dim, out_dim),
        .shared_relu => try launchQ4KTcHmmaBiasRelu(self, variant, first_device, input_a_tensor.buffer, weight_a_tensor, bias_a_tensor.buffer, rows, in_dim, out_dim),
    };
    if (!first_tc) return false;

    const second_input = switch (mode) {
        .shared, .shared_relu => input_a_tensor,
        .separate => input_b_tensor orelse return error.InvalidShape,
    };
    return switch (mode) {
        .shared, .separate => try launchQ4KTcHmmaBias(self, variant, second_device, second_input.buffer, weight_b_tensor, bias_b_tensor.buffer, rows, in_dim, out_dim),
        .shared_relu => try launchQ4KTcHmmaBiasRelu(self, variant, second_device, second_input.buffer, weight_b_tensor, bias_b_tensor.buffer, rows, in_dim, out_dim),
    };
}

fn linearPairSpanQ4(
    self: *CudaCompute,
    input_a_tensor: *const CudaTensor,
    input_b_tensor: ?*const CudaTensor,
    weight_a_tensor: *const CudaTensor,
    bias_a_tensor: *const CudaTensor,
    weight_b_tensor: *const CudaTensor,
    bias_b_tensor: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    mode: SpanQ4PairMode,
) anyerror!?ops.LinearPairResult {
    if (!isKnownQuant(weight_a_tensor, .Q4_K) or !isKnownQuant(weight_b_tensor, .Q4_K)) return null;

    const span_available = useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim);
    const tc_variant = mxbaiQ4TiledVariant(rows, in_dim, out_dim);
    if (tc_variant == null and !span_available) return null;

    const out_count = try checkedMul(rows, out_dim);
    const first_tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(first_tensor);
    const second_tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(second_tensor);
    const first_shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(first_shape);
    const second_shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(second_shape);
    var first_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer first_device.free(&self.ctx);
    var second_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer second_device.free(&self.ctx);

    const pair_op = spanQ4PairOp(mode);
    const pair_epilogue = spanQ4PairEpilogue(mode);
    var used_tc = false;
    var span_fallback: CudaDispatchFallback = if (tc_variant == null) tcUnavailableReason() else .specialized_span;
    if (tc_variant) |variant| {
        if (try launchSpanQ4PairTc(self, mode, variant, first_device, second_device, input_a_tensor, input_b_tensor, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim)) {
            used_tc = true;
            self.dispatch_stats.note(self.allocator, pair_op, .q4_k, .q4_tc_hmma, pair_epilogue, .none, rows, in_dim, out_dim, 0);
            self.dispatch_stats.note(self.allocator, pair_op, .q4_k, .q4_tc_hmma, pair_epilogue, .none, rows, in_dim, out_dim, 0);
        } else if (span_available) {
            span_fallback = tcPairFallbackReason(weight_a_tensor, weight_b_tensor, variant, .q4_k_hmma);
        } else {
            return null;
        }
    }

    if (!used_tc) {
        if (!span_available) return null;
        switch (mode) {
            .shared => try self.kernels.launchLinearQ4KSpanPairBiasTile8Rows2F32(
                &self.ctx,
                first_device,
                second_device,
                input_a_tensor.buffer,
                weight_a_tensor.buffer,
                bias_a_tensor.buffer,
                weight_b_tensor.buffer,
                bias_b_tensor.buffer,
                rows,
                in_dim,
                out_dim,
            ),
            .shared_relu => try self.kernels.launchLinearQ4KSpanPairBiasReluTile8Rows2F32(
                &self.ctx,
                first_device,
                second_device,
                input_a_tensor.buffer,
                weight_a_tensor.buffer,
                bias_a_tensor.buffer,
                weight_b_tensor.buffer,
                bias_b_tensor.buffer,
                rows,
                in_dim,
                out_dim,
            ),
            .separate => {
                const second_input = input_b_tensor orelse return error.InvalidShape;
                try self.kernels.launchLinearQ4KSpanPair2BiasTile8Rows2F32(
                    &self.ctx,
                    first_device,
                    second_device,
                    input_a_tensor.buffer,
                    second_input.buffer,
                    weight_a_tensor.buffer,
                    bias_a_tensor.buffer,
                    weight_b_tensor.buffer,
                    bias_b_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                );
            },
        }
        self.dispatch_stats.note(self.allocator, pair_op, .q4_k, .q4_span_simt, pair_epilogue, span_fallback, rows, in_dim, out_dim, 0);
    }

    first_tensor.* = .{
        .buffer = first_device,
        .dtype = .f32,
        .shape = first_shape,
        .elem_count = out_count,
    };
    second_tensor.* = .{
        .buffer = second_device,
        .dtype = .f32,
        .shape = second_shape,
        .elem_count = out_count,
    };
    return .{
        .first = first_tensor,
        .second = second_tensor,
    };
}

fn linearPair(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    if (try linearPairSpanQ4(self, input_tensor, null, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .shared)) |span_pair| {
        return span_pair;
    }

    // The fused Q4_K pair kernel is block-per-output. Dispatch the two linears
    // separately so each uses the row/column-tiled Q4_K path.
    const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
    return .{ .first = first, .second = second };
}

fn linearPairRelu(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    return try linearPairSpanQ4(self, input_tensor, null, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .shared_relu);
}

fn linearPairInputs(ctx: *anyopaque, input_a: CT, input_b: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_a_tensor = tensorFromCt(input_a);
    const input_b_tensor = tensorFromCt(input_b);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_a_tensor);
    try ensureF32(input_b_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_a_tensor, try checkedMul(rows, in_dim));
    try ensureCount(input_b_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    return try linearPairSpanQ4(self, input_a_tensor, input_b_tensor, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .separate);
}

fn layerNorm(ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(input_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLayerNormF32(&self.ctx, device, input_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_layer += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn addLayerNorm(ctx: *anyopaque, a: CT, b: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return null;
    if (dim == 0 or a_tensor.elem_count % dim != 0) return null;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddLayerNormF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, a_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_add_layer += 1;
    return try createTensor(self, device, shape, a_tensor.elem_count);
}

fn rmsNorm(ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=rms_norm input_elems={d} dim={d}", .{ input_tensor.elem_count, dim });
        return error.InvalidShape;
    }
    if (weight_tensor.elem_count != dim) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=rms_norm weight_elems={d} dim={d}", .{ weight_tensor.elem_count, dim });
        return error.InvalidShape;
    }

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRmsNormF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormAddMultiplyScalarTensor(ctx: *anyopaque, input: CT, weight: CT, residual: CT, scalar: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const residual_tensor = tensorFromCt(residual);
    const scalar_tensor = tensorFromCt(scalar);
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_RMSNORM_ADD_MUL_SCALAR_FUSION", false)) return null;
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(residual_tensor);
    try ensureF32(scalar_tensor);
    if (input_tensor.elem_count != residual_tensor.elem_count or !sameShape(input_tensor.shape, residual_tensor.shape)) return error.InvalidShape;
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);
    try ensureCount(scalar_tensor, 1);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRmsNormAddMulScalarF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, residual_tensor.buffer, scalar_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_add_mul_scalar += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormAddOutputScaleTensor(ctx: *anyopaque, input: CT, weight: CT, residual: CT, scalar: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_RMSNORM_ADD_OUTPUT_SCALE_FUSION", true)) {
        self.stats.rms_norm_add_output_scale_fallbacks += 1;
        return null;
    }
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const residual_tensor = tensorFromCt(residual);
    const scalar_tensor = tensorFromCt(scalar);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(residual_tensor);
    try ensureF32(scalar_tensor);
    if (input_tensor.elem_count != residual_tensor.elem_count or !sameShape(input_tensor.shape, residual_tensor.shape)) return error.InvalidShape;
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);
    try ensureCount(scalar_tensor, 1);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    self.kernels.launchRmsNormAddOutputScaleF32(
        &self.ctx,
        device,
        input_tensor.buffer,
        weight_tensor.buffer,
        residual_tensor.buffer,
        scalar_tensor.buffer,
        input_tensor.elem_count / dim,
        dim,
        eps,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            device.free(&self.ctx);
            self.allocator.free(shape);
            self.stats.rms_norm_add_output_scale_fallbacks += 1;
            return null;
        },
        else => return err,
    };
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_add_output_scale += 1;
    self.stats.rms_norm_add_output_scale_fused += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormAddTensor(ctx: *anyopaque, input: CT, weight: CT, residual: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const residual_tensor = tensorFromCt(residual);
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_RMSNORM_ADD_FUSION", true)) return null;
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(residual_tensor);
    if (input_tensor.elem_count != residual_tensor.elem_count or !sameShape(input_tensor.shape, residual_tensor.shape)) return error.InvalidShape;
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRmsNormAddF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, residual_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_add += 1;
    self.stats.rms_norm_add_fused += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormBare(ctx: *anyopaque, input: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRmsNormBareF32(&self.ctx, device, input_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.rms_norm_bare_calls += 1;
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_bare += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}
const UnaryOp = enum { gelu, relu, quick_gelu, sigmoid, tanh };

fn unaryHost(ctx: *anyopaque, input: CT, op: UnaryOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const kernel_op: kernels_mod.ElementwiseOp = switch (op) {
        .gelu => .gelu,
        .relu => .relu,
        .quick_gelu => .quick_gelu,
        .sigmoid => .sigmoid,
        .tanh => .tanh,
    };
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, .{}, input_tensor.elem_count, kernel_op);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn gelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .gelu);
}

fn relu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .relu);
}

fn quickGelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .quick_gelu);
}

fn sigmoid(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .sigmoid);
}

fn tanhAct(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .tanh);
}
fn concat(ctx: *anyopaque, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureCount(a_tensor, try checkedMul(total, dim_a));
    try ensureCount(b_tensor, try checkedMul(total, dim_b));
    const out_dim = try checkedAdd(dim_a, dim_b);
    const out_count = try checkedMul(total, out_dim);
    const shape = try allocShape2(self.allocator, total, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConcatLastDimF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, total, dim_a, dim_b);
    self.stats.launch_other += 1;
    return createTensor(self, device, shape, out_count);
}

fn splitLastDim3(ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!ops.SplitLastDim3Result {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const part_count = try checkedMul(rows, dim);
    try ensureCount(input_tensor, try checkedMul(part_count, 3));
    const shape_first = try allocShape2(self.allocator, rows, dim);
    var shape_first_owned = false;
    errdefer if (!shape_first_owned) self.allocator.free(shape_first);
    const shape_second = try allocShape2(self.allocator, rows, dim);
    var shape_second_owned = false;
    errdefer if (!shape_second_owned) self.allocator.free(shape_second);
    const shape_third = try allocShape2(self.allocator, rows, dim);
    var shape_third_owned = false;
    errdefer if (!shape_third_owned) self.allocator.free(shape_third);
    var first_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var first_device_owned = false;
    errdefer if (!first_device_owned) first_device.free(&self.ctx);
    var second_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var second_device_owned = false;
    errdefer if (!second_device_owned) second_device.free(&self.ctx);
    var third_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var third_device_owned = false;
    errdefer if (!third_device_owned) third_device.free(&self.ctx);
    try self.kernels.launchSplitLastDim3F32(&self.ctx, first_device, second_device, third_device, input_tensor.buffer, rows, dim);
    self.stats.launch_other += 1;
    const first = try createTensor(self, first_device, shape_first, part_count);
    first_device_owned = true;
    shape_first_owned = true;
    errdefer freeTensor(ctx, first);
    const second = try createTensor(self, second_device, shape_second, part_count);
    second_device_owned = true;
    shape_second_owned = true;
    errdefer freeTensor(ctx, second);
    const third = try createTensor(self, third_device, shape_third, part_count);
    third_device_owned = true;
    shape_third_owned = true;
    return .{ .first = first, .second = second, .third = third };
}

fn binaryElementwise(ctx: *anyopaque, a: CT, b: CT, op: kernels_mod.ElementwiseOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count and (op == .add or op == .multiply)) {
        const input_tensor, const scalar_tensor = if (b_tensor.elem_count == 1 and a_tensor.elem_count != 1)
            .{ a_tensor, b_tensor }
        else if (a_tensor.elem_count == 1 and b_tensor.elem_count != 1)
            .{ b_tensor, a_tensor }
        else
            return error.InvalidShape;

        const shape = try dupeShape(self.allocator, input_tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
        errdefer device.free(&self.ctx);
        try self.kernels.launchBinaryScalarF32(&self.ctx, device, input_tensor.buffer, scalar_tensor.buffer, input_tensor.elem_count, op);
        self.stats.launch_scalar += 1;
        self.stats.launch_scalar_device_broadcast += 1;
        return createTensor(self, device, shape, input_tensor.elem_count);
    }
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, a_tensor.elem_count, op);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn add(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .add);
}

fn multiply(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .multiply);
}

fn multiplyScalar(ctx: *anyopaque, input: CT, scale: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchScaleF32(&self.ctx, device, input_tensor.buffer, input_tensor.elem_count, scale);
    self.stats.launch_scalar += 1;
    self.stats.launch_scalar_multiply_immediate += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn addScalar(ctx: *anyopaque, input: CT, value: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddScalarF32(&self.ctx, device, input_tensor.buffer, input_tensor.elem_count, value);
    self.stats.add_scalar_calls += 1;
    self.stats.launch_scalar += 1;
    self.stats.launch_scalar_add_immediate += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn siluMultiply(ctx: *anyopaque, gate: CT, up: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const gate_tensor = tensorFromCt(gate);
    const up_tensor = tensorFromCt(up);
    try ensureF32(gate_tensor);
    try ensureF32(up_tensor);
    if (gate_tensor.elem_count != up_tensor.elem_count or !sameShape(gate_tensor.shape, up_tensor.shape)) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, gate_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, gate_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchSiluMultiplyF32(&self.ctx, device, gate_tensor.buffer, up_tensor.buffer, gate_tensor.elem_count);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, gate_tensor.elem_count);
}

fn activationMultiply(ctx: *anyopaque, gate: CT, up: CT, activation: ops.DecoderRuntimeActivationKind) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const gate_tensor = tensorFromCt(gate);
    const up_tensor = tensorFromCt(up);
    try ensureF32(gate_tensor);
    try ensureF32(up_tensor);
    if (gate_tensor.elem_count != up_tensor.elem_count or !sameShape(gate_tensor.shape, up_tensor.shape)) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, gate_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, gate_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchActivationMultiplyF32(&self.ctx, device, gate_tensor.buffer, up_tensor.buffer, gate_tensor.elem_count, @intFromEnum(activation));
    self.stats.launch_elementwise += 1;
    self.stats.activation_multiply_fused += 1;
    return createTensor(self, device, shape, gate_tensor.elem_count);
}

fn linearNoBiasGatedDown(
    ctx: *anyopaque,
    gate: CT,
    up: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    activation: ops.DecoderRuntimeActivationKind,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const gate_tensor = tensorFromCt(gate);
    const up_tensor = tensorFromCt(up);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(gate_tensor);
    try ensureF32(up_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    const input_expected = try checkedMul(rows, in_dim);
    if (gate_tensor.elem_count != input_expected or up_tensor.elem_count != input_expected or
        !sameShape(gate_tensor.shape, up_tensor.shape))
    {
        return error.InvalidShape;
    }
    if (weight_tensor.elem_count != try checkedMul(out_dim, in_dim)) return error.InvalidShape;

    const use_q8 = isKnownQuant(weight_tensor, .Q8_0);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    if (!use_q8 and !use_q4) {
        self.stats.gated_down_fallbacks += 1;
        return null;
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    var shape_owned = false;
    errdefer if (!shape_owned) self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_owned = false;
    errdefer if (!device_owned) device.free(&self.ctx);

    if (use_q8) {
        self.kernels.launchLinearQ8_0GatedDownTile4F32(
            &self.ctx,
            device,
            gate_tensor.buffer,
            up_tensor.buffer,
            weight_tensor.buffer,
            rows,
            in_dim,
            out_dim,
            @intFromEnum(activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.gated_down_fallbacks += 1;
                device.free(&self.ctx);
                self.allocator.free(shape);
                return null;
            },
            else => return err,
        };
        self.stats.gated_down_fused_q8 += 1;
    } else {
        self.kernels.launchLinearQ4KGatedDownTile4F32(
            &self.ctx,
            device,
            gate_tensor.buffer,
            up_tensor.buffer,
            weight_tensor.buffer,
            rows,
            in_dim,
            out_dim,
            @intFromEnum(activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.gated_down_fallbacks += 1;
                device.free(&self.ctx);
                self.allocator.free(shape);
                return null;
            },
            else => return err,
        };
        self.stats.gated_down_fused_q4 += 1;
        if (rows == 1) self.stats.q4k_decode_fast_hits += 1;
    }

    self.stats.launch_linear += 1;
    const result = try createTensor(self, device, shape, out_count);
    shape_owned = true;
    device_owned = true;
    return result;
}

fn addMultiplyScalarTensor(ctx: *anyopaque, a: CT, b: CT, scalar: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    const scalar_tensor = tensorFromCt(scalar);
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_ADD_MUL_SCALAR_FUSION", true)) return null;
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureF32(scalar_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return error.InvalidShape;
    try ensureCount(scalar_tensor, 1);

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddMulScalarF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, scalar_tensor.buffer, a_tensor.elem_count);
    self.stats.launch_elementwise += 1;
    self.stats.add_mul_scalar_fused += 1;
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn silu(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, .{}, input_tensor.elem_count, .silu);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}
fn sdpaLaunch(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: ?[]const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const token_count = try checkedMul(batch, seq_len);
    const has_mask = mask != null;
    if (mask) |mask_values| {
        if (mask_values.len < token_count) return error.InvalidShape;
    }

    const mask_device = if (mask) |mask_values| try uploadTempI64(self, mask_values) else buffer_mod.DeviceBuffer{};
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, mask_device, bias_buffer, batch, seq_len, num_heads, head_dim, false, has_mask, bias_mode, true);
    self.stats.launch_attention += 1;
    return createTensor(self, device, shape, count);
}

fn sdpa(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: []const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    return sdpaLaunch(ctx, q_ct, k_ct, v_ct, mask, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn sdpaFull(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!?CT {
    return try sdpaLaunch(ctx, q_ct, k_ct, v_ct, null, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn causalSelfAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, .{}, bias_buffer, batch, seq_len, num_heads, head_dim, true, false, bias_mode, false);
    self.stats.launch_attention += 1;
    return createTensor(self, device, shape, count);
}

fn cudaAllowHostAttentionFallback() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ALLOW_HOST_ATTENTION_FALLBACK", false);
}

fn cudaForceHostGqaAttention() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_FORCE_HOST_GQA_ATTENTION", false);
}

fn usizeSliceToU32(allocator: std.mem.Allocator, values: []const usize) ![]u32 {
    const out = try allocator.alloc(u32, values.len);
    errdefer allocator.free(out);
    for (values, 0..) |value, i| {
        if (value > std.math.maxInt(u32)) return error.InvalidShape;
        out[i] = @intCast(value);
    }
    return out;
}

fn ropeHostFallback(
    self: *CudaCompute,
    input_tensor: *const CudaTensor,
    seq_len: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    position_offset: usize,
    consecutive_pairs: bool,
) !CT {
    self.stats.host_attention_fallbacks += 1;
    self.stats.rope_host_fallbacks += 1;
    const data = try downloadAlloc(self, input_tensor);
    var data_owned = true;
    errdefer if (data_owned) self.allocator.free(data);
    const total_chunks = data.len / head_dim;
    const chunks_per_position = total_chunks / seq_len;
    const positions = try self.allocator.alloc(usize, total_chunks);
    defer self.allocator.free(positions);
    for (positions, 0..) |*position, chunk| {
        position.* = position_offset + ((chunk / chunks_per_position) % seq_len);
    }
    linalg.ropeCore(data, positions, head_dim, rope_dim, theta, freq_scale, consecutive_pairs);
    data_owned = false;
    return uploadOwnedHost(self, data, input_tensor.shape);
}

fn ropePerItemHostFallback(
    self: *CudaCompute,
    input_tensor: *const CudaTensor,
    batch: usize,
    max_seq_len: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    query_lengths: []const usize,
    position_offsets: []const usize,
    consecutive_pairs: bool,
) !CT {
    self.stats.host_attention_fallbacks += 1;
    self.stats.rope_per_item_host_fallbacks += 1;
    const data = try downloadAlloc(self, input_tensor);
    var data_owned = true;
    errdefer if (data_owned) self.allocator.free(data);
    const row_count = try checkedMul(batch, max_seq_len);
    const row_dim = data.len / row_count;
    const num_heads = row_dim / head_dim;
    const total_chunks = try checkedMul(row_count, num_heads);
    const positions = try self.allocator.alloc(usize, total_chunks);
    defer self.allocator.free(positions);
    @memset(positions, 0);
    for (0..batch) |b| {
        for (0..query_lengths[b]) |pos| {
            const row_base = (b * max_seq_len + pos) * num_heads;
            for (0..num_heads) |h| {
                positions[row_base + h] = position_offsets[b] + pos;
            }
        }
    }
    linalg.ropeCore(data, positions, head_dim, rope_dim, theta, freq_scale, consecutive_pairs);
    data_owned = false;
    return uploadOwnedHost(self, data, input_tensor.shape);
}

fn biasModeFor(bias_tensor: ?*CudaTensor, batch: usize, num_heads: usize, q_seq_len: usize, kv_seq_len: usize) !u32 {
    const tensor = bias_tensor orelse return 0;
    try ensureF32(tensor);
    const shared = try checkedMul(num_heads, try checkedMul(q_seq_len, kv_seq_len));
    const batched = try checkedMul(batch, shared);
    if (tensor.elem_count == batched) return 2;
    if (tensor.elem_count == shared) return 1;
    return error.InvalidShape;
}

fn gqaDenseAttentionHostFallback(
    self: *CudaCompute,
    q_tensor: *const CudaTensor,
    k_tensor: *const CudaTensor,
    v_tensor: *const CudaTensor,
    bias_tensor: ?*CudaTensor,
    attn_or_mask: ?[]const u8,
    sliding_window: usize,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) !CT {
    self.stats.host_attention_fallbacks += 1;
    self.stats.gqa_dense_host_fallbacks += 1;
    const q_host = try downloadAlloc(self, q_tensor);
    defer self.allocator.free(q_host);
    const k_host = try downloadAlloc(self, k_tensor);
    defer self.allocator.free(k_host);
    const v_host = try downloadAlloc(self, v_tensor);
    defer self.allocator.free(v_host);
    const bias_host = if (bias_tensor) |tensor| try downloadAlloc(self, tensor) else null;
    defer if (bias_host) |data| self.allocator.free(data);
    const output = try linalg.flashCausalAttentionHost(
        self.allocator,
        q_host,
        k_host,
        v_host,
        bias_host,
        attn_or_mask,
        sliding_window,
        batch,
        q_seq_len,
        kv_seq_len,
        query_position_offset,
        kv_position_offset,
        num_heads,
        num_kv_heads,
        head_dim,
    );
    var output_owned = true;
    errdefer if (output_owned) self.allocator.free(output);
    output_owned = false;
    return uploadOwnedHost(self, output, q_tensor.shape);
}

fn gqaDenseAttention(
    ctx: *anyopaque,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attn_or_mask: ?[]const u8,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    total_sequence_len: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    if (num_kv_heads == 0 or num_heads % num_kv_heads != 0) return error.InvalidShape;
    const q_hidden = try checkedMul(num_heads, head_dim);
    const kv_hidden = try checkedMul(num_kv_heads, head_dim);
    const q_count = try checkedMul(try checkedMul(batch, q_seq_len), q_hidden);
    const kv_count = try checkedMul(try checkedMul(batch, kv_seq_len), kv_hidden);
    try ensureCount(q_tensor, q_count);
    try ensureCount(k_tensor, kv_count);
    try ensureCount(v_tensor, kv_count);
    if (total_sequence_len < q_seq_len or query_position_offset > total_sequence_len) return error.InvalidShape;
    const query_end = try checkedAdd(query_position_offset, q_seq_len);
    const kv_end = try checkedAdd(kv_position_offset, kv_seq_len);
    const mask_sequence_len = @max(query_end, kv_end);

    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_mode = try biasModeFor(bias_tensor, batch, num_heads, q_seq_len, kv_seq_len);
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    if (cudaForceHostGqaAttention()) {
        return gqaDenseAttentionHostFallback(
            self,
            q_tensor,
            k_tensor,
            v_tensor,
            bias_tensor,
            attn_or_mask,
            sliding_window,
            batch,
            q_seq_len,
            kv_seq_len,
            query_position_offset,
            kv_position_offset,
            num_heads,
            num_kv_heads,
            head_dim,
        );
    }

    const mask_device = if (attn_or_mask) |mask| try uploadTempU8(self, mask) else buffer_mod.DeviceBuffer{};
    const mask_len = if (attn_or_mask) |mask| mask.len else 0;
    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, q_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);

    const attention_launch = launch: {
        if (cudaDebugDecodeScalarsReady(self)) {
            const scalar_launch = self.kernels.launchGqaAttentionDecodeScalarsF32(
                &self.ctx,
                device,
                q_tensor.buffer,
                k_tensor.buffer,
                v_tensor.buffer,
                mask_device,
                bias_buffer,
                self.debug_cuda_decode_scalars,
                batch,
                q_seq_len,
                kv_seq_len,
                num_heads,
                num_kv_heads,
                head_dim,
                query_position_offset,
                kv_position_offset,
                sliding_window,
                mask_sequence_len,
                mask_len,
                bias_mode,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable => null,
                else => return err,
            };
            if (scalar_launch) |kind| break :launch kind;
        }
        break :launch self.kernels.launchGqaAttentionF32(
            &self.ctx,
            device,
            q_tensor.buffer,
            k_tensor.buffer,
            v_tensor.buffer,
            mask_device,
            bias_buffer,
            batch,
            q_seq_len,
            kv_seq_len,
            num_heads,
            num_kv_heads,
            head_dim,
            query_position_offset,
            kv_position_offset,
            sliding_window,
            mask_sequence_len,
            mask_len,
            bias_mode,
        );
    } catch |err| {
        if (err == error.CudaKernelUnavailable and cudaAllowHostAttentionFallback()) {
            return gqaDenseAttentionHostFallback(
                self,
                q_tensor,
                k_tensor,
                v_tensor,
                bias_tensor,
                attn_or_mask,
                sliding_window,
                batch,
                q_seq_len,
                kv_seq_len,
                query_position_offset,
                kv_position_offset,
                num_heads,
                num_kv_heads,
                head_dim,
            );
        }
        return err;
    };
    switch (attention_launch) {
        .decode => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
        },
        .scalar => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_scalar += 1;
        },
        .none => {},
    }
    return createTensor(self, device, shape, q_count);
}

fn compactKvRows(
    allocator: std.mem.Allocator,
    data: []const f32,
    token_count: usize,
    target_width: usize,
) ![]f32 {
    if (token_count == 0 or target_width == 0) return error.InvalidShape;
    if (data.len % token_count != 0) return error.InvalidShape;
    const source_width = data.len / token_count;
    if (source_width < target_width) return error.InvalidShape;
    if (source_width == target_width) return allocator.dupe(f32, data);
    const compact = try allocator.alloc(f32, token_count * target_width);
    errdefer allocator.free(compact);
    for (0..token_count) |token_idx| {
        @memcpy(
            compact[token_idx * target_width ..][0..target_width],
            data[token_idx * source_width ..][0..target_width],
        );
    }
    return compact;
}

fn gqaPagedAttentionWithHostKv(
    ctx: *anyopaque,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attention: ops.AttentionContext,
    batch: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (gqaPagedAttentionWithDeviceKv(self, q_ct, k_ct, v_ct, attn_bias_ct, attention, batch, num_heads, num_kv_heads, head_dim)) |result| {
        return result;
    } else |err| switch (err) {
        error.DeviceReadUnsupported,
        error.DeviceReadFallback,
        error.DeviceWriteUnsupported,
        error.DeviceWriteFormatUnsupported,
        error.CudaPagedKvUnsupported,
        => {},
        else => return err,
    }

    self.stats.host_attention_fallbacks += 1;
    self.stats.paged_attention_host_fallbacks += 1;
    if (batch != 1 or attention.kv_batch != null) return error.CudaPagedKvBatchUnsupported;
    const manager = attention.kv_manager orelse return error.CudaPagedKvUnsupported;
    const kv = attention.kv_cache orelse return error.CudaPagedKvUnsupported;
    if (attention.total_sequence_len < attention.query_sequence_len) return error.InvalidShape;
    if (attention.kv_sequence_len == 0) return error.InvalidShape;
    const h_kv = try checkedMul(num_kv_heads, head_dim);

    if (!attention.skip_kv_write) {
        const k_tensor = tensorFromCt(k_ct);
        const v_tensor = tensorFromCt(v_ct);
        try ensureF32(k_tensor);
        try ensureF32(v_tensor);
        const suffix_count = try checkedMul(attention.query_sequence_len, h_kv);
        try ensureCount(k_tensor, suffix_count);
        try ensureCount(v_tensor, suffix_count);
        const k_suffix = try downloadAlloc(self, k_tensor);
        defer self.allocator.free(k_suffix);
        const v_suffix = try downloadAlloc(self, v_tensor);
        defer self.allocator.free(v_suffix);
        try manager.writeLayerKvSuffix(kv.sequence_id, attention.layer_index, attention.kv_sequence_len, attention.query_sequence_len, k_suffix, v_suffix);
    }

    const gathered = try manager.gatherLayerKv(self.allocator, kv.sequence_id, attention.layer_index, attention.kv_sequence_len);
    defer self.allocator.free(gathered.k);
    defer self.allocator.free(gathered.v);
    const compact_k = try compactKvRows(self.allocator, gathered.k, attention.kv_sequence_len, h_kv);
    defer self.allocator.free(compact_k);
    const compact_v = try compactKvRows(self.allocator, gathered.v, attention.kv_sequence_len, h_kv);
    defer self.allocator.free(compact_v);

    const kv_shape = [_]i32{ @intCast(attention.kv_sequence_len), @intCast(h_kv) };
    const gathered_k_ct = try fromFloat32ShapeOp(ctx, compact_k, &kv_shape);
    defer freeTensor(ctx, gathered_k_ct);
    const gathered_v_ct = try fromFloat32ShapeOp(ctx, compact_v, &kv_shape);
    defer freeTensor(ctx, gathered_v_ct);

    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    return gqaDenseAttention(
        ctx,
        q_ct,
        gathered_k_ct,
        gathered_v_ct,
        attn_bias_ct,
        attention.attn_or_mask,
        batch,
        attention.query_sequence_len,
        attention.kv_sequence_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        attention.kv_position_offset,
        attention.sliding_window,
        attention.total_sequence_len,
    );
}

fn gqaPagedAttentionWithDeviceKv(
    self: *CudaCompute,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attention: ops.AttentionContext,
    batch: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) anyerror!CT {
    self.stats.device_kv_attempts += 1;
    if (batch != 1 or attention.kv_batch != null) {
        self.stats.device_kv_fail_batch += 1;
        return error.CudaPagedKvBatchUnsupported;
    }
    const kv = attention.kv_cache orelse {
        self.stats.device_kv_fail_no_cache += 1;
        return error.CudaPagedKvUnsupported;
    };
    const storage = attention.kv_storage orelse {
        self.stats.device_kv_fail_no_storage += 1;
        return error.CudaPagedKvUnsupported;
    };
    const hook = storage.device_write_hook orelse {
        self.stats.device_kv_fail_no_hook += 1;
        return error.DeviceWriteUnsupported;
    };
    if (attention.total_sequence_len < attention.query_sequence_len) {
        self.stats.device_kv_fail_shape += 1;
        return error.InvalidShape;
    }
    if (attention.kv_sequence_len == 0) {
        self.stats.device_kv_fail_shape += 1;
        return error.InvalidShape;
    }
    const h_kv = try checkedMul(num_kv_heads, head_dim);

    if (!attention.skip_kv_write) {
        const k_tensor = tensorFromCt(k_ct);
        const v_tensor = tensorFromCt(v_ct);
        try ensureF32(k_tensor);
        try ensureF32(v_tensor);
        const suffix_count = try checkedMul(attention.query_sequence_len, h_kv);
        try ensureCount(k_tensor, suffix_count);
        try ensureCount(v_tensor, suffix_count);
        const suffix_bytes = try checkedMul(suffix_count, @sizeOf(f32));
        storage.writeLayerKvSuffixDevice(
            .{
                .sequence_id = kv.sequence_id,
                .layer_index = attention.layer_index,
                .total_token_count = attention.kv_sequence_len,
                .suffix_token_count = attention.query_sequence_len,
                .position_offset = attention.kv_position_offset,
                .num_kv_heads = @intCast(num_kv_heads),
                .head_dim = @intCast(head_dim),
            },
            .{ .handle = @ptrFromInt(k_tensor.buffer.ptr), .byte_offset = 0, .byte_len = suffix_bytes },
            .{ .handle = @ptrFromInt(v_tensor.buffer.ptr), .byte_offset = 0, .byte_len = suffix_bytes },
        ) catch |err| {
            self.stats.device_kv_fail_write += 1;
            return err;
        };
    }

    const gathered = hook.gatherLayerKvDevice(.{
        .sequence_id = kv.sequence_id,
        .layer_index = attention.layer_index,
        .token_count = attention.kv_sequence_len,
        .num_kv_heads = @intCast(num_kv_heads),
        .head_dim = @intCast(head_dim),
    }) catch |err| {
        self.stats.device_kv_fail_read += 1;
        return err;
    };
    if (gathered.value_element_bytes != @sizeOf(f32) or gathered.row_width < h_kv) {
        self.stats.device_kv_fail_shape += 1;
        return error.DeviceReadFallback;
    }
    const kv_count = try checkedMul(attention.kv_sequence_len, h_kv);
    const kv_bytes = try checkedMul(kv_count, @sizeOf(f32));
    if (gathered.k.byte_len < kv_bytes or gathered.v.byte_len < kv_bytes) {
        self.stats.device_kv_fail_shape += 1;
        return error.DeviceReadFallback;
    }

    const k_shape = try allocShape2(self.allocator, attention.kv_sequence_len, h_kv);
    var k_shape_owned = false;
    errdefer if (!k_shape_owned) self.allocator.free(k_shape);
    const v_shape = try allocShape2(self.allocator, attention.kv_sequence_len, h_kv);
    var v_shape_owned = false;
    errdefer if (!v_shape_owned) self.allocator.free(v_shape);
    const k_tensor = try self.allocator.create(CudaTensor);
    var k_tensor_owned = false;
    errdefer if (!k_tensor_owned) self.allocator.destroy(k_tensor);
    const q_tensor_for_ptr_type = tensorFromCt(q_ct);
    const k_device_ptr: @TypeOf(q_tensor_for_ptr_type.buffer.ptr) = @intCast(@intFromPtr(gathered.k.handle) + gathered.k.byte_offset);
    k_tensor.* = .{
        .buffer = .{ .ptr = k_device_ptr, .len = gathered.k.byte_len },
        .dtype = .f32,
        .shape = k_shape,
        .elem_count = kv_count,
        .owns_buffer = false,
    };
    k_shape_owned = true;
    const v_tensor = try self.allocator.create(CudaTensor);
    var v_tensor_owned = false;
    errdefer if (!v_tensor_owned) self.allocator.destroy(v_tensor);
    const v_device_ptr: @TypeOf(q_tensor_for_ptr_type.buffer.ptr) = @intCast(@intFromPtr(gathered.v.handle) + gathered.v.byte_offset);
    v_tensor.* = .{
        .buffer = .{ .ptr = v_device_ptr, .len = gathered.v.byte_len },
        .dtype = .f32,
        .shape = v_shape,
        .elem_count = kv_count,
        .owns_buffer = false,
    };
    v_shape_owned = true;

    k_tensor_owned = true;
    v_tensor_owned = true;
    defer freeTensor(self, k_tensor);
    defer freeTensor(self, v_tensor);

    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    const result = try gqaDenseAttention(
        self,
        q_ct,
        k_tensor,
        v_tensor,
        attn_bias_ct,
        attention.attn_or_mask,
        batch,
        attention.query_sequence_len,
        attention.kv_sequence_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        attention.kv_position_offset,
        attention.sliding_window,
        attention.total_sequence_len,
    );
    self.stats.device_kv_successes += 1;
    return result;
}
fn crossAttention(_: *anyopaque, _: CT, _: CT, _: CT, _: []const i64, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedCt();
}
fn relativePositionBias(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: bool) anyerror!CT {
    return unsupportedCt();
}
fn debertaDisentangledAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, q_r_ct: CT, k_r_ct: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    const q_r_tensor = tensorFromCt(q_r_ct);
    const k_r_tensor = tensorFromCt(k_r_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    try ensureF32(q_r_tensor);
    try ensureF32(k_r_tensor);
    if (seq_len == 0) return error.InvalidShape;
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    const rel_positions = try checkedSub(try checkedMul(2, seq_len), 1);
    const rel_count = try checkedMul(rel_positions, hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    try ensureCount(q_r_tensor, rel_count);
    try ensureCount(k_r_tensor, rel_count);
    if (mask.len < try checkedMul(batch, seq_len)) return error.InvalidShape;

    const mask_device = try uploadTempI64(self, mask);
    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchDebertaAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, q_r_tensor.buffer, k_r_tensor.buffer, mask_device, batch, seq_len, num_heads, head_dim);
    return createTensor(self, device, shape, count);
}
fn windowedSelfAttention(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedCt();
}
fn channelSelfAttention(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedCt();
}
fn tokenGridConv2d(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedCt();
}
fn conv1d(_: *anyopaque, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedCt();
}
fn conv2d(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    bias: CT,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    height: usize,
    width: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
) anyerror!CT {
    if (groups == 0 or in_channels % groups != 0 or out_channels % groups != 0) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(bias_tensor);
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const out_count = try checkedMul(try checkedMul(batch, out_channels), try checkedMul(out_h, out_w));
    try ensureCount(input_tensor, try checkedMul(try checkedMul(batch, in_channels), try checkedMul(height, width)));
    try ensureCount(weight_tensor, try checkedMul(try checkedMul(out_channels, in_channels / groups), try checkedMul(kernel_h, kernel_w)));
    try ensureCount(bias_tensor, out_channels);
    const shape = try self.allocator.dupe(i64, &.{ @as(i64, @intCast(batch)), @as(i64, @intCast(out_channels)), @as(i64, @intCast(out_h)), @as(i64, @intCast(out_w)) });
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConv2dF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups, out_h, out_w);
    return createTensor(self, device, shape, out_count);
}
fn rope(ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.InvalidShape;
    if (input_tensor.elem_count % head_dim != 0) return error.InvalidShape;
    const total_chunks = input_tensor.elem_count / head_dim;
    if (total_chunks % seq_len != 0) return error.InvalidShape;
    const chunks_per_position = total_chunks / seq_len;
    if (chunks_per_position == 0) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    self.kernels.launchRopeF32(&self.ctx, device, input_tensor.buffer, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs) catch |err| {
        if (err == error.CudaKernelUnavailable and cudaAllowHostAttentionFallback()) {
            return ropeHostFallback(self, input_tensor, seq_len, head_dim, rope_dim, theta, freq_scale, position_offset, consecutive_pairs);
        }
        return err;
    };
    self.stats.launch_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormHeadsRope(ctx: *anyopaque, input: CT, weight: CT, rows: usize, total_dim: usize, head_dim: usize, rope_dim: usize, eps: f32, theta: f32, freq_scale: f32, position_offset: usize, seq_len: usize, consecutive_pairs: bool, scale: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (cudaDisableHeadNormRopeFusion()) {
        self.stats.head_norm_rope_fused_fallbacks += 1;
        return null;
    }
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    if (rows == 0 or total_dim == 0 or head_dim == 0 or total_dim % head_dim != 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0 or seq_len == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, total_dim));
    try ensureCount(weight_tensor, head_dim);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const launch_result = if (cudaDebugDecodeScalarsReady(self))
        self.kernels.launchRmsNormHeadsRopeDecodeScalarsF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, self.debug_cuda_decode_scalars, rows, total_dim, head_dim, rope_dim, eps, theta, freq_scale, position_offset, seq_len, consecutive_pairs, scale)
    else
        self.kernels.launchRmsNormHeadsRopeF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, total_dim, head_dim, rope_dim, eps, theta, freq_scale, position_offset, seq_len, consecutive_pairs, scale);
    launch_result catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            self.stats.head_norm_rope_fused_fallbacks += 1;
            return null;
        },
        else => return err,
    };
    self.stats.head_norm_rope_fused_hits += 1;
    self.stats.launch_norm += 1;
    self.stats.launch_norm_head_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn ropeScaled(ctx: *anyopaque, input: CT, scale: f32, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.InvalidShape;
    if (input_tensor.elem_count % head_dim != 0) return error.InvalidShape;
    const total_chunks = input_tensor.elem_count / head_dim;
    if (total_chunks % seq_len != 0) return error.InvalidShape;
    const chunks_per_position = total_chunks / seq_len;
    if (chunks_per_position == 0) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    self.kernels.launchRopeScaledF32(&self.ctx, device, input_tensor.buffer, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, scale) catch |err| switch (err) {
        error.CudaKernelUnavailable => return null,
        else => return err,
    };
    self.stats.launch_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn ropePerItem(ctx: *anyopaque, input: CT, batch: usize, max_seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, query_lengths: []const usize, position_offsets: []const usize, consecutive_pairs: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (batch == 0 or max_seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.InvalidShape;
    if (query_lengths.len != batch or position_offsets.len != batch) return error.InvalidShape;
    const row_count = try checkedMul(batch, max_seq_len);
    if (input_tensor.elem_count % row_count != 0) return error.InvalidShape;
    const row_dim = input_tensor.elem_count / row_count;
    if (row_dim % head_dim != 0) return error.InvalidShape;
    const num_heads = row_dim / head_dim;
    if (num_heads == 0) return error.InvalidShape;
    for (query_lengths) |len| if (len > max_seq_len) return error.InvalidShape;

    const query_lengths_u32 = try usizeSliceToU32(self.allocator, query_lengths);
    defer self.allocator.free(query_lengths_u32);
    const position_offsets_u32 = try usizeSliceToU32(self.allocator, position_offsets);
    defer self.allocator.free(position_offsets_u32);
    const query_lengths_device = try uploadTempU32(self, query_lengths_u32);
    const position_offsets_device = try uploadTempU32(self, position_offsets_u32);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    self.kernels.launchRopePerItemF32(&self.ctx, device, input_tensor.buffer, query_lengths_device, position_offsets_device, batch, max_seq_len, num_heads, head_dim, rope_dim, theta, freq_scale, consecutive_pairs) catch |err| {
        if (err == error.CudaKernelUnavailable and cudaAllowHostAttentionFallback()) {
            return ropePerItemHostFallback(self, input_tensor, batch, max_seq_len, head_dim, rope_dim, theta, freq_scale, query_lengths, position_offsets, consecutive_pairs);
        }
        return err;
    };
    self.stats.launch_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn cudaDecoderRuntimeReserveKvTokens(config: gpt_model.Config, current_kv_tokens: usize) usize {
    if (config.sliding_window > 0) return @intCast(config.sliding_window);
    if (config.max_position_embeddings > 0) return @intCast(config.max_position_embeddings);
    return if (current_kv_tokens > 0) current_kv_tokens else 1;
}

fn cudaDecoderRuntimeOverrideLevel() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DECODER_SLOT_OVERRIDE_LEVEL") orelse 4;
}

fn cudaGemmaPreparedLayers(configured_layers: usize) usize {
    const configured = platform.env.getenvUsize("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_GATED_LAYERS") orelse configured_layers;
    return @min(configured_layers, configured);
}

fn cudaGemmaNormSlot(layer: usize, comptime kind: enum { attn_pre, attn_post, ffn_pre, ffn_post }) usize {
    return switch (kind) {
        .attn_pre => layer * 4,
        .attn_post => layer * 4 + 1,
        .ffn_pre => layer * 4 + 2,
        .ffn_post => layer * 4 + 3,
    };
}

fn cudaGemmaLinearSlot(layer: usize, comptime kind: enum { attn_q, attn_k, attn_v, attn_out_proj, mlp_gate, mlp_up, mlp_down }) usize {
    return switch (kind) {
        .attn_q => layer * 7,
        .attn_k => layer * 7 + 1,
        .attn_v => layer * 7 + 2,
        .attn_out_proj => layer * 7 + 3,
        .mlp_gate => layer * 7 + 4,
        .mlp_up => layer * 7 + 5,
        .mlp_down => layer * 7 + 6,
    };
}

fn cudaFamilyStateMatches(self: *const CudaCompute, config: gpt_model.Config, configured_layer_count: usize) bool {
    const state = self.decoder_runtime_family_state;
    return state.prepared and
        state.configured_layer_count == configured_layer_count and
        state.hidden_size == config.hidden_size and
        state.intermediate_size == config.intermediate_size and
        state.num_hidden_layers == config.num_hidden_layers and
        state.num_attention_heads == config.num_attention_heads and
        state.num_key_value_heads == config.num_key_value_heads and
        state.num_global_key_value_heads == config.num_global_key_value_heads and
        state.attention_head_dim == config.attention_head_dim and
        state.global_head_dim == config.global_head_dim and
        state.vocab_size == config.vocab_size and
        state.sliding_window == config.sliding_window and
        state.sliding_window_pattern == config.sliding_window_pattern and
        state.ple_hidden_size == config.ple_hidden_size;
}

fn cudaNoteFamilyPrepared(self: *CudaCompute, config: gpt_model.Config, reserve_kv_tokens: usize, configured_layer_count: usize) void {
    self.decoder_runtime_family_state = .{
        .prepared = true,
        .reserve_kv_tokens = reserve_kv_tokens,
        .configured_layer_count = configured_layer_count,
        .hidden_size = config.hidden_size,
        .intermediate_size = config.intermediate_size,
        .num_hidden_layers = config.num_hidden_layers,
        .num_attention_heads = config.num_attention_heads,
        .num_key_value_heads = config.num_key_value_heads,
        .num_global_key_value_heads = config.num_global_key_value_heads,
        .attention_head_dim = config.attention_head_dim,
        .global_head_dim = config.global_head_dim,
        .vocab_size = config.vocab_size,
        .sliding_window = config.sliding_window,
        .sliding_window_pattern = config.sliding_window_pattern,
        .ple_hidden_size = config.ple_hidden_size,
    };
}

fn cudaPrepareRmsNormSlotByName(
    self: *CudaCompute,
    cb: *const ops.ComputeBackend,
    config: gpt_model.Config,
    slot: usize,
    name: []const u8,
    hidden_size: usize,
) !bool {
    const weight = try gpt_arch.getModelWeight(cb, config, name);
    defer cb.free(weight);
    return decoderRuntimePrepareRmsNormOp(self, &.{
        .slot = slot,
        .weight = weight,
        .hidden_size = hidden_size,
    });
}

fn cudaPrepareLinearNoBiasSlotFromWeight(
    self: *CudaCompute,
    slot: usize,
    weight: CT,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const resident = residentTensorForSlot(self, tensorFromCt(weight)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return false;
    };
    try decoderRuntimePrepareLinearSlot(self, slot, resident, null, in_dim, out_dim);
    return true;
}

fn cudaPrepareLinearNoBiasSlotByName(
    self: *CudaCompute,
    cb: *const ops.ComputeBackend,
    config: gpt_model.Config,
    slot: usize,
    name: []const u8,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const weight = try gpt_arch.getModelWeight(cb, config, name);
    defer cb.free(weight);
    return cudaPrepareLinearNoBiasSlotFromWeight(self, slot, weight, in_dim, out_dim);
}

fn cudaPrepareGemmaDecoderRuntimeFamily(
    self: *CudaCompute,
    allocator: std.mem.Allocator,
    config: gpt_model.Config,
    configured_layer_count: usize,
) !bool {
    _ = allocator;
    if (config.family != .gemma or config.usesMoe()) return false;
    const layer_count: usize = @intCast(config.num_hidden_layers);
    if (layer_count == 0 or layer_count > 256) return false;

    const cb = self.computeBackend();
    const prepared_layer_count = cudaGemmaPreparedLayers(@min(configured_layer_count, layer_count));
    const override_level = cudaDecoderRuntimeOverrideLevel();
    if (override_level == 0) return false;

    for (0..prepared_layer_count) |layer| {
        var name_buf: [256]u8 = undefined;
        const shares_kv = config.layerSharesKv(layer);
        const layer_head_dim: usize = @intCast(config.effectiveHeadDimForLayer(layer));
        const layer_kv_heads: usize = @intCast(config.effectiveKVHeadsForLayer(layer));
        const attention_input_size: usize = @as(usize, @intCast(config.num_attention_heads)) * layer_head_dim;
        const kv_dim = layer_kv_heads * layer_head_dim;

        const attn_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.input_layernorm.weight", .{layer});
        if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, cudaGemmaNormSlot(layer, .attn_pre), attn_norm_name, config.hidden_size))) return false;

        if (override_level >= 2) {
            const q_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.q_proj.weight", .{layer});
            if (!(try cudaPrepareLinearNoBiasSlotByName(self, &cb, config, cudaGemmaLinearSlot(layer, .attn_q), q_name, config.hidden_size, attention_input_size))) return false;

            if (!shares_kv) {
                const k_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer});
                const k_weight = try gpt_arch.getModelWeight(&cb, config, k_name);
                defer cb.free(k_weight);
                if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .attn_k), k_weight, config.hidden_size, kv_dim))) return false;

                const v_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.v_proj.weight", .{layer});
                const v_weight = gpt_arch.getModelWeight(&cb, config, v_name) catch |err| switch (err) {
                    error.MissingWeight, error.WeightNotFound => if (config.layerOmitsVProj(layer)) k_weight else return err,
                    else => return err,
                };
                const owns_v_weight = v_weight != k_weight;
                defer if (owns_v_weight) cb.free(v_weight);
                if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .attn_v), v_weight, config.hidden_size, kv_dim))) return false;
            }

            const attn_out_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer});
            if (!(try cudaPrepareLinearNoBiasSlotByName(self, &cb, config, cudaGemmaLinearSlot(layer, .attn_out_proj), attn_out_name, attention_input_size, config.hidden_size))) return false;
        }

        if (override_level >= 3) {
            const attn_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer});
            if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, cudaGemmaNormSlot(layer, .attn_post), attn_post_norm_name, config.hidden_size))) return false;

            const ffn_pre_norm_name = std.fmt.bufPrint(&name_buf, "model.layers.{d}.pre_feedforward_layernorm.weight", .{layer}) catch return error.NameTooLong;
            const ffn_pre_norm = gpt_arch.getModelWeight(&cb, config, ffn_pre_norm_name) catch |err| switch (err) {
                error.MissingWeight, error.WeightNotFound => null,
                else => return err,
            };
            if (ffn_pre_norm) |weight| {
                defer cb.free(weight);
                if (!(try decoderRuntimePrepareRmsNormOp(self, &.{
                    .slot = cudaGemmaNormSlot(layer, .ffn_pre),
                    .weight = weight,
                    .hidden_size = config.hidden_size,
                }))) return false;
            }

            const ffn_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_feedforward_layernorm.weight", .{layer});
            if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, cudaGemmaNormSlot(layer, .ffn_post), ffn_post_norm_name, config.hidden_size))) return false;
        }

        if (override_level >= 4) {
            const gate_w = try gpt_arch.getFFNWeight(&cb, config, layer, "gate", &name_buf);
            defer cb.free(gate_w);
            if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .mlp_gate), gate_w, config.hidden_size, config.intermediateSize(layer)))) return false;

            const up_w = try gpt_arch.getFFNWeight(&cb, config, layer, "up", &name_buf);
            defer cb.free(up_w);
            if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .mlp_up), up_w, config.hidden_size, config.intermediateSize(layer)))) return false;

            const down_w = try gpt_arch.getFFNWeight(&cb, config, layer, "down", &name_buf);
            defer cb.free(down_w);
            if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .mlp_down), down_w, config.intermediateSize(layer), config.hidden_size))) return false;
        }
    }

    return true;
}

fn decoderRuntimePrepareGreedyOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeGreedyRequest) anyerror!bool {
    _ = ctx;
    if (request.hidden_size == 0 or request.num_layers == 0 or request.num_heads == 0 or
        request.num_kv_heads == 0 or request.head_dim == 0 or request.vocab_size == 0 or
        request.kv_tokens == 0)
    {
        return false;
    }
    return true;
}

fn decoderRuntimePrepareOrReuseFamilyOp(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    config: gpt_model.Config,
    current_kv_tokens: usize,
    configured_layer_count: usize,
) anyerror!ops.DecoderRuntimePrepareReuseResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const reserve_kv_tokens = cudaDecoderRuntimeReserveKvTokens(config, current_kv_tokens);
    if (cudaFamilyStateMatches(self, config, configured_layer_count)) {
        if (reserve_kv_tokens <= self.decoder_runtime_family_state.reserve_kv_tokens) {
            return .{
                .prepared = true,
                .reserve_kv_tokens = reserve_kv_tokens,
                .fast_hit = true,
            };
        }
        const prepared = try decoderRuntimePrepareGreedyOp(ctx, &.{
            .hidden_size = config.hidden_size,
            .intermediate_size = config.intermediate_size,
            .num_layers = config.num_hidden_layers,
            .num_heads = config.num_attention_heads,
            .num_kv_heads = config.effectiveKVHeads(),
            .head_dim = config.headDim(),
            .vocab_size = config.vocab_size,
            .kv_tokens = reserve_kv_tokens,
        });
        if (prepared) self.decoder_runtime_family_state.reserve_kv_tokens = reserve_kv_tokens;
        return .{
            .prepared = prepared,
            .reserve_kv_tokens = reserve_kv_tokens,
            .used_greedy = prepared,
        };
    }

    if (!(try decoderRuntimePrepareGreedyOp(ctx, &.{
        .hidden_size = config.hidden_size,
        .intermediate_size = config.intermediate_size,
        .num_layers = config.num_hidden_layers,
        .num_heads = config.num_attention_heads,
        .num_kv_heads = config.effectiveKVHeads(),
        .head_dim = config.headDim(),
        .vocab_size = config.vocab_size,
        .kv_tokens = reserve_kv_tokens,
    }))) {
        return .{ .reserve_kv_tokens = reserve_kv_tokens };
    }

    const prepared = try cudaPrepareGemmaDecoderRuntimeFamily(self, allocator, config, configured_layer_count);
    if (prepared) cudaNoteFamilyPrepared(self, config, reserve_kv_tokens, configured_layer_count);
    return .{
        .prepared = prepared,
        .reserve_kv_tokens = reserve_kv_tokens,
    };
}

fn decoderRuntimeReadyOp(ctx: *anyopaque) bool {
    const self: *const CudaCompute = @ptrCast(@alignCast(ctx));
    return self.decoder_runtime_family_state.prepared or
        self.decoder_runtime_linear_slots.count() != 0 or
        self.decoder_runtime_rms_norm_slots.count() != 0;
}

fn decoderRuntimePrepareRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareRmsNormRequest) anyerror!bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    const resident = residentTensorForSlot(self, weight_tensor) orelse {
        self.stats.decoder_runtime_rms_norm_slot_prepare_misses += 1;
        return false;
    };
    try ensureF32(resident);
    try ensureCount(resident, request.hidden_size);
    try self.decoder_runtime_rms_norm_slots.put(self.allocator, request.slot, .{
        .weight = borrowedSlotTensor(resident),
        .hidden_size = request.hidden_size,
    });
    self.stats.decoder_runtime_rms_norm_slot_prepares += 1;
    return true;
}

fn decoderRuntimeEnsureRmsNormSlotOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeEnsureRmsNormSlotRequest) anyerror!?usize {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    const resident = residentTensorForSlot(self, weight_tensor) orelse {
        self.stats.decoder_runtime_rms_norm_slot_prepare_misses += 1;
        return null;
    };
    try ensureF32(resident);
    try ensureCount(resident, request.hidden_size);
    var it = self.decoder_runtime_rms_norm_slots.iterator();
    while (it.next()) |entry| {
        const slot = entry.value_ptr;
        if (slot.weight.buffer.ptr == resident.buffer.ptr and slot.hidden_size == request.hidden_size) {
            return entry.key_ptr.*;
        }
    }
    const slot_id = self.decoder_runtime_next_dynamic_slot;
    self.decoder_runtime_next_dynamic_slot += 1;
    try self.decoder_runtime_rms_norm_slots.put(self.allocator, slot_id, .{
        .weight = borrowedSlotTensor(resident),
        .hidden_size = request.hidden_size,
    });
    self.stats.decoder_runtime_rms_norm_slot_prepares += 1;
    return slot_id;
}

fn decoderRuntimeApplyRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot = self.decoder_runtime_rms_norm_slots.getPtr(request.slot) orelse {
        self.stats.decoder_runtime_rms_norm_apply_misses += 1;
        return null;
    };
    if (slot.hidden_size != request.hidden_size) return error.UnexpectedOutputShape;
    self.stats.decoder_runtime_rms_norm_apply_hits += 1;
    return try rmsNorm(ctx, request.input, &slot.weight, request.hidden_size, request.eps);
}

fn decoderRuntimeInputRows(input: CT, in_dim: usize) !usize {
    if (in_dim == 0) return error.InvalidShape;
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_tensor.elem_count % in_dim != 0) return error.InvalidShape;
    const rows = input_tensor.elem_count / in_dim;
    if (rows == 0) return error.InvalidShape;
    return rows;
}

fn sameOptionalSlotBias(slot_bias: ?CudaTensor, request_bias: ?*CudaTensor) bool {
    if (slot_bias) |bias| {
        const requested = request_bias orelse return false;
        return bias.buffer.ptr == requested.buffer.ptr and bias.buffer.len == requested.buffer.len and bias.elem_count == requested.elem_count;
    }
    return request_bias == null;
}

fn decoderRuntimePrepareLinearSlot(
    self: *CudaCompute,
    slot_id: usize,
    weight: *CudaTensor,
    bias: ?*CudaTensor,
    in_dim: usize,
    out_dim: usize,
) !void {
    try ensureF32Bf16OrQuantized(weight);
    try ensureLogicalWeightCount(weight, try checkedMul(out_dim, in_dim));
    if (bias) |bias_tensor| {
        try ensureF32(bias_tensor);
        try ensureCount(bias_tensor, out_dim);
    }
    try self.decoder_runtime_linear_slots.put(self.allocator, slot_id, .{
        .weight = borrowedSlotTensor(weight),
        .bias = if (bias) |bias_tensor| borrowedSlotTensor(bias_tensor) else null,
        .in_dim = in_dim,
        .out_dim = out_dim,
    });
    self.stats.decoder_runtime_linear_slot_prepares += 1;
}

fn decoderRuntimePrepareLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareLinearRequest) anyerror!bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight = residentTensorForSlot(self, tensorFromCt(request.weight)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return false;
    };
    const bias = residentTensorForSlot(self, tensorFromCt(request.bias)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return false;
    };
    try decoderRuntimePrepareLinearSlot(self, request.slot, weight, bias, request.in_dim, request.out_dim);
    return true;
}

fn decoderRuntimeEnsureLinearSlotOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeEnsureLinearSlotRequest) anyerror!?usize {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight = residentTensorForSlot(self, tensorFromCt(request.weight)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return null;
    };
    const bias = if (request.bias) |bias_ct| blk: {
        break :blk residentTensorForSlot(self, tensorFromCt(bias_ct)) orelse {
            self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
            return null;
        };
    } else null;

    var it = self.decoder_runtime_linear_slots.iterator();
    while (it.next()) |entry| {
        const slot = entry.value_ptr;
        if (slot.weight.buffer.ptr == weight.buffer.ptr and
            slot.in_dim == request.in_dim and
            slot.out_dim == request.out_dim and
            sameOptionalSlotBias(slot.bias, bias))
        {
            return entry.key_ptr.*;
        }
    }

    const slot_id = self.decoder_runtime_next_dynamic_slot;
    self.decoder_runtime_next_dynamic_slot += 1;
    try decoderRuntimePrepareLinearSlot(self, slot_id, weight, bias, request.in_dim, request.out_dim);
    return slot_id;
}

fn decoderRuntimeApplyLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot = self.decoder_runtime_linear_slots.getPtr(request.slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (slot.in_dim != request.in_dim or slot.out_dim != request.out_dim) return error.UnexpectedOutputShape;
    self.stats.decoder_runtime_linear_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    if (slot.bias) |*bias| {
        return try linear(ctx, request.input, &slot.weight, bias, rows, request.in_dim, request.out_dim);
    }
    return try linearNoBias(ctx, request.input, &slot.weight, rows, request.in_dim, request.out_dim);
}

fn decoderRuntimeApplyLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearArgmaxRequest) anyerror!?usize {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot = self.decoder_runtime_linear_slots.getPtr(request.slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (slot.in_dim != request.in_dim or slot.out_dim != request.out_dim) return error.UnexpectedOutputShape;
    self.stats.decoder_runtime_linear_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    var profile_scope = if (request.out_dim >= 100_000)
        beginDecodeProfile(self, .lm_head_argmax, rows)
    else
        null;
    defer if (profile_scope) |*scope| scope.end();
    if (slot.bias == null) {
        const token = (try linearNoBiasArgmaxLastRow(ctx, request.input, &slot.weight, rows, request.in_dim, request.out_dim)) orelse return null;
        return @intCast(token);
    }
    const logits = try linear(ctx, request.input, &slot.weight, &slot.bias.?, rows, request.in_dim, request.out_dim);
    defer freeTensor(ctx, logits);
    const token = (try argmaxLastRow(ctx, logits, rows, request.out_dim)) orelse return null;
    return @intCast(token);
}

fn decoderRuntimeApplyLinearPairOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearPairRequest) anyerror!?ops.LinearNoBiasPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot_a = self.decoder_runtime_linear_slots.getPtr(request.slot_a) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    const slot_b = self.decoder_runtime_linear_slots.getPtr(request.slot_b) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (slot_a.in_dim != request.in_dim or slot_b.in_dim != request.in_dim or
        slot_a.out_dim != request.out_dim or slot_b.out_dim != request.out_dim)
    {
        return error.UnexpectedOutputShape;
    }
    if (slot_a.bias != null or slot_b.bias != null) return null;
    self.stats.decoder_runtime_linear_apply_hits += 2;
    self.stats.decoder_runtime_linear_pair_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    var profile_scope = beginDecodeProfile(self, .ffn_gate_up, rows);
    defer if (profile_scope) |*scope| scope.end();
    return try linearNoBiasPair(ctx, request.input, &slot_a.weight, &slot_b.weight, rows, request.in_dim, request.out_dim);
}

fn decoderRuntimeApplyLinearQkvOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearQkvRequest) anyerror!?ops.LinearNoBiasTripleResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_slot = self.decoder_runtime_linear_slots.getPtr(request.q_slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    const k_slot = self.decoder_runtime_linear_slots.getPtr(request.k_slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    const v_slot = self.decoder_runtime_linear_slots.getPtr(request.v_slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (q_slot.in_dim != request.in_dim or k_slot.in_dim != request.in_dim or v_slot.in_dim != request.in_dim or
        q_slot.out_dim != request.q_out_dim or k_slot.out_dim != request.kv_out_dim or v_slot.out_dim != request.kv_out_dim)
    {
        return error.UnexpectedOutputShape;
    }
    if (q_slot.bias != null or k_slot.bias != null or v_slot.bias != null) return null;
    self.stats.decoder_runtime_linear_apply_hits += 3;
    self.stats.decoder_runtime_linear_qkv_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    var profile_scope = beginDecodeProfile(self, .qkv, rows);
    defer if (profile_scope) |*scope| scope.end();
    if (try linearNoBiasQkv(ctx, request.input, &q_slot.weight, &k_slot.weight, &v_slot.weight, rows, request.in_dim, request.q_out_dim, request.kv_out_dim)) |fused| {
        return fused;
    }
    const q = try linearNoBias(ctx, request.input, &q_slot.weight, rows, request.in_dim, request.q_out_dim);
    errdefer freeTensor(ctx, q);
    const k = try linearNoBias(ctx, request.input, &k_slot.weight, rows, request.in_dim, request.kv_out_dim);
    errdefer freeTensor(ctx, k);
    const v = try linearNoBias(ctx, request.input, &v_slot.weight, rows, request.in_dim, request.kv_out_dim);
    return .{ .first = q, .second = k, .third = v };
}

fn decoderRuntimeApplyBlockRmsNorm(
    ctx: *anyopaque,
    input: CT,
    slot_id: ?usize,
    hidden_size: usize,
    eps: f32,
) anyerror!?CT {
    const slot = slot_id orelse return input;
    return decoderRuntimeApplyRmsNormOp(ctx, &.{
        .slot = slot,
        .input = input,
        .hidden_size = hidden_size,
        .eps = eps,
    });
}

fn runGatedFfnResidualOp(ctx: *anyopaque, request: *const ops.RunGatedFfnResidualRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.stats.decoder_runtime_gated_ffn_attempts += 1;

    const rows = decoderRuntimeInputRows(request.input, request.hidden_size) catch {
        self.stats.decoder_runtime_gated_ffn_misses += 1;
        return null;
    };
    const residual_tensor = tensorFromCt(request.residual);
    ensureF32(residual_tensor) catch {
        self.stats.decoder_runtime_gated_ffn_misses += 1;
        return null;
    };
    if (residual_tensor.elem_count != rows * request.hidden_size) {
        self.stats.decoder_runtime_gated_ffn_misses += 1;
        return null;
    }

    const gate_up = (try decoderRuntimeApplyLinearPairOp(ctx, &.{
        .slot_a = request.gate_linear_slot,
        .slot_b = request.up_linear_slot,
        .input = request.input,
        .in_dim = request.hidden_size,
        .out_dim = request.intermediate_size,
    })) orelse {
        self.stats.decoder_runtime_gated_ffn_misses += 1;
        return null;
    };
    defer freeTensor(ctx, gate_up.first);
    defer freeTensor(ctx, gate_up.second);

    var current: CT = undefined;
    var current_is_down_projection = false;
    if (request.post_gate_rms_norm_slot == null) fused_down_blk: {
        const down_slot = self.decoder_runtime_linear_slots.getPtr(request.down_linear_slot) orelse break :fused_down_blk;
        if (down_slot.in_dim != request.intermediate_size or down_slot.out_dim != request.hidden_size) return error.UnexpectedOutputShape;
        if (down_slot.bias != null) break :fused_down_blk;
        var profile_scope = beginDecodeProfile(self, .ffn_gated_down, rows);
        defer if (profile_scope) |*scope| scope.end();
        if (try linearNoBiasGatedDown(
            ctx,
            gate_up.first,
            gate_up.second,
            &down_slot.weight,
            rows,
            request.intermediate_size,
            request.hidden_size,
            request.activation,
        )) |projected| {
            self.stats.decoder_runtime_linear_apply_hits += 1;
            current = projected;
            current_is_down_projection = true;
        }
    }

    if (!current_is_down_projection) {
        current = (try activationMultiply(ctx, gate_up.first, gate_up.second, request.activation)) orelse {
            self.stats.decoder_runtime_gated_ffn_misses += 1;
            return null;
        };

        if (request.post_gate_rms_norm_slot) |slot_id| {
            const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
                .slot = slot_id,
                .input = current,
                .hidden_size = request.intermediate_size,
                .eps = request.eps,
            })) orelse {
                freeTensor(ctx, current);
                self.stats.decoder_runtime_gated_ffn_misses += 1;
                return null;
            };
            freeTensor(ctx, current);
            current = normed;
        }

        const projected = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.down_linear_slot,
            .input = current,
            .in_dim = request.intermediate_size,
            .out_dim = request.hidden_size,
        })) orelse {
            freeTensor(ctx, current);
            self.stats.decoder_runtime_gated_ffn_misses += 1;
            return null;
        };
        freeTensor(ctx, current);
        current = projected;
    }
    errdefer freeTensor(ctx, current);

    const output_scale = request.output_scale;
    if (request.post_down_rms_norm_slot) |slot_id| {
        var profile_scope = beginDecodeProfile(self, .ffn_post_norm, rows);
        defer if (profile_scope) |*scope| scope.end();
        if (self.decoder_runtime_rms_norm_slots.getPtr(slot_id)) |slot| {
            if (output_scale) |scale| {
                if (try rmsNormAddOutputScaleTensor(ctx, current, &slot.weight, request.residual, scale, request.hidden_size, request.eps)) |fused| {
                    freeTensor(ctx, current);
                    self.stats.decoder_runtime_gated_ffn_hits += 1;
                    return fused;
                }
                if (try rmsNormAddTensor(ctx, current, &slot.weight, request.residual, request.hidden_size, request.eps)) |unscaled| {
                    defer freeTensor(ctx, unscaled);
                    const scaled = try multiply(ctx, unscaled, scale);
                    freeTensor(ctx, current);
                    self.stats.decoder_runtime_gated_ffn_hits += 1;
                    return scaled;
                }
            } else if (try rmsNormAddTensor(ctx, current, &slot.weight, request.residual, request.hidden_size, request.eps)) |fused| {
                freeTensor(ctx, current);
                self.stats.decoder_runtime_gated_ffn_hits += 1;
                return fused;
            }
        }
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot_id,
            .input = current,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse {
            freeTensor(ctx, current);
            self.stats.decoder_runtime_gated_ffn_misses += 1;
            return null;
        };
        freeTensor(ctx, current);
        current = normed;
    }

    const added = try add(ctx, current, request.residual);
    freeTensor(ctx, current);
    const result = if (output_scale) |scale| scaled: {
        defer freeTensor(ctx, added);
        break :scaled try multiply(ctx, added, scale);
    } else added;
    self.stats.decoder_runtime_gated_ffn_hits += 1;
    return result;
}

fn runAttentionResidualOp(ctx: *anyopaque, request: *const ops.RunAttentionResidualRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.stats.decoder_runtime_attention_residual_attempts += 1;
    if (request.attention_sink.hasMetadata()) {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }
    const attention_input_size = request.num_heads * request.head_dim;
    var current = gqa_blk: {
        var profile_scope = beginDecodeProfile(self, .gqa_attention, 1);
        defer if (profile_scope) |*scope| scope.end();
        break :gqa_blk try gqaPagedAttention(
            ctx,
            request.q,
            request.k,
            request.v,
            null,
            request.attention,
            1,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
        );
    };
    errdefer freeTensor(ctx, current);

    if (try decoderRuntimeApplyBlockRmsNorm(ctx, current, request.pre_linear_rms_norm_slot, attention_input_size, request.eps)) |normed| {
        if (normed != current) {
            freeTensor(ctx, current);
            current = normed;
        }
    } else {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }

    const projected = projected_blk: {
        var profile_scope = beginDecodeProfile(self, .attention_output, 1);
        defer if (profile_scope) |*scope| scope.end();
        break :projected_blk (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.linear_slot,
            .input = current,
            .in_dim = attention_input_size,
            .out_dim = request.hidden_size,
        })) orelse {
            freeTensor(ctx, current);
            self.stats.decoder_runtime_attention_residual_misses += 1;
            return null;
        };
    };
    freeTensor(ctx, current);
    current = projected;

    var post_profile_scope = beginDecodeProfile(self, .attention_norm_residual, 1);
    defer if (post_profile_scope) |*scope| scope.end();
    if (request.post_linear_rms_norm_slot) |slot_id| {
        if (self.decoder_runtime_rms_norm_slots.getPtr(slot_id)) |slot| {
            if (try rmsNormAddTensor(ctx, current, &slot.weight, request.residual, request.hidden_size, request.eps)) |fused| {
                freeTensor(ctx, current);
                self.stats.decoder_runtime_attention_residual_hits += 1;
                return fused;
            }
        }
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot_id,
            .input = current,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse {
            freeTensor(ctx, current);
            self.stats.decoder_runtime_attention_residual_misses += 1;
            return null;
        };
        freeTensor(ctx, current);
        current = normed;
    }

    const result = try add(ctx, current, request.residual);
    freeTensor(ctx, current);
    self.stats.decoder_runtime_attention_residual_hits += 1;
    return result;
}

fn gqaCausalAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    if (num_heads == num_kv_heads) {
        const self: *CudaCompute = @ptrCast(@alignCast(ctx));
        if (self.kernels.gqa_attention_f32 == null) {
            return causalSelfAttention(ctx, q_ct, k_ct, v_ct, attn_bias_ct, batch, seq_len, num_heads, head_dim);
        }
    }
    return gqaDenseAttention(ctx, q_ct, k_ct, v_ct, attn_bias_ct, null, batch, seq_len, seq_len, num_heads, num_kv_heads, head_dim, 0, 0, 0, seq_len);
}

fn gqaPagedAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, attention: ops.AttentionContext, batch: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    if (attention.kv_batch != null or attention.kv_cache != null or attention.kv_manager != null or attention.kv_storage != null) {
        return gqaPagedAttentionWithHostKv(ctx, q_ct, k_ct, v_ct, attn_bias_ct, attention, batch, num_heads, num_kv_heads, head_dim);
    }
    if (attention.total_sequence_len < attention.query_sequence_len) return error.InvalidShape;
    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    return gqaDenseAttention(ctx, q_ct, k_ct, v_ct, attn_bias_ct, attention.attn_or_mask, batch, attention.query_sequence_len, attention.kv_sequence_len, num_heads, num_kv_heads, head_dim, query_position_offset, attention.kv_position_offset, attention.sliding_window, attention.total_sequence_len);
}

const vtable = ops.ComputeBackend.VTable{
    .backendKind = &backendKind,
    .deinitBackend = &deinitBackend,
    .freeTensor = &freeTensor,
    .provisionKvDeviceWriteHook = &provisionKvDeviceWriteHook,
    .getWeight = &getWeight,
    .prefetchWeightHint = &prefetchWeightHint,
    .drainPrefetchBudget = &drainPrefetchBudget,
    .debugProfileCheckpoint = &debugProfileCheckpoint,
    .debugCudaGraphCaptureBegin = &debugCudaGraphCaptureBegin,
    .debugCudaGraphPrepareDecodeScalars = &debugCudaGraphPrepareDecodeScalars,
    .debugCudaTraceTensor = &debugCudaTraceTensor,
    .debugCudaGraphRegisterFinalHiddenReplayBoundary = &debugCudaGraphRegisterFinalHiddenReplayBoundary,
    .debugCudaGraphRegisterFinalHiddenReplayInput = &debugCudaGraphRegisterFinalHiddenReplayInput,
    .debugCudaGraphPrepareFinalHiddenReplayInput = &debugCudaGraphPrepareFinalHiddenReplayInput,
    .debugCudaGraphReplayFinalHidden = &debugCudaGraphReplayFinalHidden,
    .debugCudaGraphCaptureEnd = &debugCudaGraphCaptureEnd,
    .embeddingLookup = &embeddingLookup,
    .embeddingLookupTensor = &embeddingLookupTensor,
    .takeRows = &takeRows,
    .glinerWordEmbeddings = &glinerWordEmbeddings,
    .glinerLabelGruCombined = &glinerLabelGruCombined,
    .glinerGatherConcatRelu = &glinerGatherConcatRelu,
    .linear = &linear,
    .linearQuickGelu = &linearQuickGelu,
    .linearRelu = &linearRelu,
    .linearGelu = &linearGelu,
    .linearAdd = &linearAdd,
    .linearPlanned = &linearPlanned,
    .linearNoBias = &linearNoBias,
    .linearNoBiasPlanned = &linearNoBiasPlanned,
    .linearNoBiasPair = &linearNoBiasPair,
    .argmaxLastRow = &argmaxLastRow,
    .argmaxRows = &argmaxRows,
    .argmaxRowsSuppress = &argmaxRowsSuppress,
    .argmaxLastRowSuppressTensor = &argmaxLastRowSuppressTensor,
    .linearNoBiasArgmaxLastRow = &linearNoBiasArgmaxLastRow,
    .linearNoBiasArgmaxLastRowTensor = &linearNoBiasArgmaxLastRowTensor,
    .linearNoBiasArgmaxLastRowSuppressTensor = &linearNoBiasArgmaxLastRowSuppressTensor,
    .linearNoBiasArgmaxRowsSuppress = &linearNoBiasArgmaxRowsSuppress,
    .gemma4MtpMaskedArgmax = &gemma4MtpMaskedArgmax,
    .gemma4MtpVerifyCommit = &gemma4MtpVerifyCommit,
    .linearNoBiasQkv = &linearNoBiasQkv,
    .linearPair = &linearPair,
    .linearPairRelu = &linearPairRelu,
    .linearPairInputs = &linearPairInputs,
    .linearTriple = &linearTriple,
    .decoderRuntimePrepareGreedy = &decoderRuntimePrepareGreedyOp,
    .decoderRuntimePrepareOrReuseFamily = &decoderRuntimePrepareOrReuseFamilyOp,
    .decoderRuntimeReady = &decoderRuntimeReadyOp,
    .decoderRuntimePrepareRmsNorm = &decoderRuntimePrepareRmsNormOp,
    .decoderRuntimeEnsureRmsNormSlot = &decoderRuntimeEnsureRmsNormSlotOp,
    .decoderRuntimeApplyRmsNorm = &decoderRuntimeApplyRmsNormOp,
    .decoderRuntimePrepareLinear = &decoderRuntimePrepareLinearOp,
    .decoderRuntimeEnsureLinearSlot = &decoderRuntimeEnsureLinearSlotOp,
    .decoderRuntimeApplyLinear = &decoderRuntimeApplyLinearOp,
    .decoderRuntimeApplyLinearArgmax = &decoderRuntimeApplyLinearArgmaxOp,
    .decoderRuntimeApplyLinearPair = &decoderRuntimeApplyLinearPairOp,
    .decoderRuntimeApplyLinearQkv = &decoderRuntimeApplyLinearQkvOp,
    .runGatedFfnResidual = &runGatedFfnResidualOp,
    .multiplyScalar = &multiplyScalar,
    .addScalar = &addScalar,
    .siluMultiply = &siluMultiply,
    .activationMultiply = &activationMultiply,
    .addMultiplyScalarTensor = &addMultiplyScalarTensor,
    .layerNorm = &layerNorm,
    .addLayerNorm = &addLayerNorm,
    .rmsNorm = &rmsNorm,
    .rmsNormAddMultiplyScalarTensor = &rmsNormAddMultiplyScalarTensor,
    .rmsNormAddTensor = &rmsNormAddTensor,
    .rmsNormHeadsRope = &rmsNormHeadsRope,
    .rmsNormBare = &rmsNormBare,
    .gelu = &gelu,
    .relu = &relu,
    .silu = &silu,
    .quickGelu = &quickGelu,
    .sigmoid = &sigmoid,
    .tanh_act = &tanhAct,
    .reshapeOp = &reshapeOp,
    .reshape2D = &reshape2DOp,
    .reshape2d = &reshape2dOp,
    .sliceRows2D = &sliceRows2DOp,
    .sliceLastDim = &sliceLastDimOp,
    .cloneTensorShape = &cloneTensorShapeOp,
    .transposeOp = &transposeOp,
    .splitLastDim3 = &splitLastDim3,
    .concat = &concat,
    .add = &add,
    .scaledDotProductAttention = &sdpa,
    .scaledDotProductAttentionFull = &sdpaFull,
    .causalSelfAttention = &causalSelfAttention,
    .crossAttention = &crossAttention,
    .relativePositionBias = &relativePositionBias,
    .disentangledRelativeAttention = &debertaDisentangledAttention,
    .windowedSelfAttention = &windowedSelfAttention,
    .channelSelfAttention = &channelSelfAttention,
    .tokenGridConv2d = &tokenGridConv2d,
    .multiply = &multiply,
    .conv1d = &conv1d,
    .conv2d = &conv2d,
    .rope = &rope,
    .ropeScaled = &ropeScaled,
    .ropePerItem = &ropePerItem,
    .runAttentionResidual = &runAttentionResidualOp,
    .gqaCausalAttention = &gqaCausalAttention,
    .gqaPagedAttention = &gqaPagedAttention,
    .fromFloat32 = &fromFloat32Op,
    .fromFloat32Shape = &fromFloat32ShapeOp,
    .fromInt32Shape = &fromInt32ShapeOp,
    .toFloat32 = &toFloat32Op,
    .copyTensorFromBackend = &copyTensorFromBackendOp,
    .tensorDType = &tensorDTypeOp,
    .tensorShape = &tensorShapeOp,
    .evalTensor = &evalTensorOp,
};

test "cuda compute vtable is type checked" {
    const backend_kind_fn: *const fn (*anyopaque) ops.BackendKind = &backendKind;
    const linear_fn: *const fn (*anyopaque, CT, CT, CT, usize, usize, usize) anyerror!CT = &linear;
    const linear_no_bias_fn: *const fn (*anyopaque, CT, CT, usize, usize, usize) anyerror!CT = &linearNoBias;
    const rms_norm_fn: *const fn (*anyopaque, CT, CT, usize, f32) anyerror!CT = &rmsNorm;
    const rope_per_item_fn: *const fn (*anyopaque, CT, usize, usize, usize, usize, f32, f32, []const usize, []const usize, bool) anyerror!CT = &ropePerItem;
    _ = backend_kind_fn;
    _ = linear_fn;
    _ = linear_no_bias_fn;
    _ = rms_norm_fn;
    _ = rope_per_item_fn;
    _ = vtable;
}

test "cuda shape helpers reject incompatible shapes" {
    try std.testing.expect(try checkedMul(2, 3) == 6);
    try std.testing.expect(sameShape(&.{ 2, 3 }, &.{ 2, 3 }));
    try std.testing.expect(!sameShape(&.{ 2, 3 }, &.{ 3, 2 }));
}

test "cuda lazy demand cancels pending host prefetch item" {
    const allocator = std.testing.allocator;
    var store = native_compute_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    native_compute_mod.initPrefetchQueue(&store, allocator);
    defer native_compute_mod.deinitPrefetchQueue(&store);

    var entry = native_compute_mod.LazyWeightEntry{
        .tensor_ref = .{ .name = "weight", .byte_len = 4 },
        .pending_prefetch = true,
    };
    try store.prefetch.items.append(allocator, &entry);

    try std.testing.expect(cancelPendingLazyHostPrefetchLocked(&store, &entry));
    try std.testing.expect(!entry.pending_prefetch);
    try std.testing.expectEqual(@as(usize, 0), store.prefetch.items.items.len);
    try std.testing.expect(!cancelPendingLazyHostPrefetchLocked(&store, &entry));
}

test "cuda dense stream weight classifier covers gemma attention and mlp matrices" {
    try std.testing.expect(isDenseFfnStreamWeightName("model.language_model.layers.0.mlp.gate_proj.weight"));
    try std.testing.expect(isDenseFfnStreamWeightName("model.language_model.layers.0.mlp.up_proj.weight"));
    try std.testing.expect(isDenseFfnStreamWeightName("model.language_model.layers.0.mlp.down_proj.weight"));
    try std.testing.expect(!isDenseFfnStreamWeightName("model.language_model.layers.0.self_attn.q_proj.weight"));

    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.k_proj.weight"));
    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.v_proj.weight"));
    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.o_proj.weight"));
    try std.testing.expect(!isDenseAttentionStreamWeightName("model.language_model.layers.0.input_layernorm.weight"));
}

test "cuda dense host prefetch queue removal clears pending item" {
    const allocator = std.testing.allocator;
    const Dummy = struct {
        fn process(_: *anyopaque, _: *DenseHostPrefetchEntry) void {}
    };
    var dummy_ctx: u8 = 0;
    var compute = CudaCompute{
        .allocator = allocator,
        .ctx = undefined,
        .kernels = undefined,
        .dense_host_prefetch = DenseHostPrefetchQueue.init(allocator, &dummy_ctx, &Dummy.process),
        .dense_host_prefetch_initialized = true,
    };
    defer compute.dense_host_prefetch.deinit();

    var entry = DenseHostPrefetchEntry{
        .name = try allocator.dupe(u8, "model.language_model.layers.0.self_attn.q_proj.weight"),
        .range = .{
            .path = try allocator.dupe(u8, "/tmp/model.safetensors"),
            .byte_offset = 8,
            .byte_len = 16,
            .dtype = .bf16,
            .shape = try allocator.dupe(i64, &.{ 2, 4 }),
        },
        .kind = .attention,
        .pending = true,
    };
    defer entry.deinit(allocator);

    compute.dense_host_prefetch.lock();
    defer compute.dense_host_prefetch.unlock();
    try compute.dense_host_prefetch.items.append(allocator, &entry);
    try std.testing.expect(removeDenseHostPrefetchQueueItemLocked(&compute, &entry));
    try std.testing.expect(!entry.pending);
    try std.testing.expectEqual(@as(usize, 0), compute.dense_host_prefetch.items.items.len);
    try std.testing.expect(!removeDenseHostPrefetchQueueItemLocked(&compute, &entry));
}
