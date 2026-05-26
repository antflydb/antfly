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
const build_options = @import("build_options");
const activations = @import("activations.zig");
const decoder_bitnet_runtime = @import("decoder_bitnet_runtime.zig");
const decoder_gated_runtime = @import("decoder_gated_runtime.zig");
const decoder_gpt_runtime = @import("decoder_gpt_runtime.zig");
const ops = @import("../ops/ops.zig");
const model_runtime = @import("../graph/model_runtime.zig");
const metal_command_planner = @import("../graph/metal_command_planner.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const metal_tensor = @import("metal_tensor.zig");
const native = @import("native.zig");
const runtime_root = @import("../runtime/root.zig");
const turboquant = @import("../runtime/kv/turboquant.zig");

pub const QuantizedStorage = weight_source_mod.QuantizedStorage;
pub const MetalTensor = metal_tensor.MetalTensor;

// MLX interop is only compiled under `-Dmlx=true`. Under `-Dmlx=false`, the
// stub provides enough shape to satisfy the comptime-unreachable paths.
const mlx = if (build_options.enable_mlx) @import("mlx.zig") else struct {
    pub const c = struct {
        pub const mlx_array = extern struct {
            ctx: ?*anyopaque = null,
        };
        pub const MLX_UINT8: c_int = 0;
        pub fn mlx_array_free(_: mlx_array) callconv(.c) c_int {
            unreachable;
        }
    };
    pub fn arrayFromBorrowedBytes(_: anytype, _: anytype, _: anytype) !@This().c.mlx_array {
        unreachable;
    }
};
const mlx_metal_bridge = if (build_options.enable_mlx) @import("mlx_metal_bridge.zig") else struct {
    pub fn borrowMetalTensorAsMlxArray(_: MetalTensor) c.mlx_array {
        unreachable;
    }
    pub fn borrowMlxArrayAsMetalTensor(_: c.mlx_array, _: []i32) !MetalTensor {
        unreachable;
    }
};
const c = mlx.c;

pub const RawMetalProvider = opaque {};
pub const RawMetalDecodeRuntime = opaque {};

pub const decoder_runtime_layer_norm_slot_capacity: usize = 256;
pub const decoder_runtime_rms_norm_slot_capacity: usize = 512;
pub const decoder_runtime_linear_slot_capacity: usize = 512;

pub const RawQuantizedRuntimeLinearKind = enum {
    none,
    q1_0,
    i2_s,
    i8_s,
    q4_k,
    q5_k,
    q6_k,
    q4_0,
    q4_1,
    q5_0,
    q5_1,
    q8_0,
    q8_1,
    q8_k,
    iq1_s,
    iq1_m,
    iq2_xxs,
    iq2_xs,
    iq2_s,
    iq3_xxs,
    iq3_s,
    iq4_nl,
    iq4_xs,
    q2_k,
    q3_k,
    tq1_0,
    tq2_0,
    mxfp4,
    nvfp4,
    tl1,
    tl2,
};

pub const RawQuantizedRuntimeLinearStorageMode = enum {
    none,
    private_upload,
    mapped_shared,
};

pub const MetalQuantFormat = enum(u32) {
    unsupported = 0,
    q1_0 = 1,
    i2_s = 2,
    i8_s = 3,
    q2_k = 4,
    q3_k = 5,
    q4_0 = 6,
    q4_1 = 7,
    q4_k = 8,
    q5_0 = 9,
    q5_1 = 10,
    q5_k = 11,
    q6_k = 12,
    q8_0 = 13,
    q8_1 = 14,
    q8_k = 15,
    iq1_s = 16,
    iq1_m = 17,
    iq2_xxs = 18,
    iq2_xs = 19,
    iq2_s = 20,
    iq3_xxs = 21,
    iq3_s = 22,
    iq4_nl = 23,
    iq4_xs = 24,
    tq1_0 = 25,
    tq2_0 = 26,
    mxfp4 = 27,
    nvfp4 = 28,
    tl1 = 29,
    tl2 = 30,
};

pub const PackedWeightDescriptor = struct {
    format: MetalQuantFormat,
    tensor_type: gguf_tensor_types.TensorType,
    values_per_block: usize,
    bytes_per_block: usize,
    row_blocks: usize,
    row_stride_bytes: usize,
    raw_bytes: []const u8,

    pub fn supported(self: PackedWeightDescriptor) bool {
        return self.format != .unsupported;
    }
};

pub const RawLinearSlotKind = enum(u8) {
    none,
    dense,
    quantized,
};

pub const CompressedKeyFormat = enum {
    polar4,
    turbo3,
};

pub const ComputeRegion = enum(usize) {
    attention = 0,
    attention_project = 1,
    ffn_norm = 2,
    ffn = 3,
    ple = 4,
    tail = 5,
    embedding = 6,
    layer = 7,
    other = 8,
};

pub const ComputeSource = enum(usize) {
    quant_linear = 0,
    quant_qkv = 1,
    quant_pair_act = 2,
    attention = 3,
    rms_norm = 4,
    head_rope = 5,
    ffn = 6,
    ple = 7,
    tail = 8,
    embedding = 9,
    dense_linear = 10,
    layer = 11,
    other = 12,
};

pub const ComputeRegionScope = struct {
    runtime: ?*RawMetalDecodeRuntime,
    previous: usize,
    active: bool,

    pub fn deinit(self: *ComputeRegionScope) void {
        if (!self.active) return;
        self.active = false;
        _ = termite_metal_decode_runtime_pop_compute_region(self.runtime, self.previous);
    }
};

pub const GatheredSpanKey = struct {
    source_ptr_id: usize,
    sequence_id: runtime_root.kv.manager.SequenceId,
    layer_index: usize,
};

pub const GatheredSpanEntry = struct {
    k: MetalTensor,
    v: MetalTensor,
    token_count: usize,
    position_offset: usize,
    capacity_tokens: usize = 0,
    encoded_key: ?[]u8 = null,
    encoded_format: ?CompressedKeyFormat = null,
    encoded_key_row_bytes: usize = 0,

    pub fn deinit(self: *GatheredSpanEntry) void {
        self.k.deinit();
        self.v.deinit();
        if (self.encoded_key) |encoded| std.heap.c_allocator.free(encoded);
        self.* = undefined;
    }
};

pub const GatheredSpanSuffixViews = struct {
    k: MetalTensor,
    v: MetalTensor,

    pub fn deinit(self: *GatheredSpanSuffixViews) void {
        self.k.deinit();
        self.v.deinit();
        self.* = undefined;
    }
};

const OwnedFullKv = struct {
    k: MetalTensor,
    v: MetalTensor,
};

fn getenvBool(comptime name: [*:0]const u8) bool {
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv(name) orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn getenvUsize(comptime name: [*:0]const u8) ?usize {
    if (comptime @import("builtin").os.tag == .freestanding) return null;
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv(name) orelse return null;
    const slice = std.mem.span(value);
    if (slice.len == 0) return null;
    return std.fmt.parseUnsigned(usize, slice, 10) catch null;
}

fn debugRuntimeTensorFinite(comptime label: []const u8, layer_index: usize, tensor: MetalTensor) void {
    if (!getenvBool("TERMITE_METAL_DEBUG_RUNTIME_TENSOR_FINITE")) return;
    if (getenvUsize("TERMITE_METAL_DEBUG_RUNTIME_TENSOR_LAYER")) |target| {
        if (target != layer_index) return;
    }
    var tensor_mut = tensor;
    const data = tensor_mut.toHostSlice() catch |err| {
        std.debug.print("metal-runtime-finite label={s} layer={d} error={s}\n", .{ label, layer_index, @errorName(err) });
        return;
    };
    var nonfinite: usize = 0;
    var nan_count: usize = 0;
    var inf_count: usize = 0;
    var first_bad: usize = data.len;
    var min_value: f32 = std.math.inf(f32);
    var max_value: f32 = -std.math.inf(f32);
    var max_abs: f32 = 0;
    for (data, 0..) |value, i| {
        if (!std.math.isFinite(value)) {
            if (first_bad == data.len) first_bad = i;
            nonfinite += 1;
            if (std.math.isNan(value)) nan_count += 1 else inf_count += 1;
            continue;
        }
        min_value = @min(min_value, value);
        max_value = @max(max_value, value);
        max_abs = @max(max_abs, @abs(value));
    }
    std.debug.print(
        "metal-runtime-finite label={s} layer={d} elems={d} nonfinite={d} nan={d} inf={d} first_bad={d} min={d} max={d} max_abs={d}\n",
        .{ label, layer_index, data.len, nonfinite, nan_count, inf_count, first_bad, min_value, max_value, max_abs },
    );
}

fn debugRuntimeSliceFinite(comptime label: []const u8, layer_index: usize, data: []const f32) void {
    if (!getenvBool("TERMITE_METAL_DEBUG_RUNTIME_TENSOR_FINITE")) return;
    if (getenvUsize("TERMITE_METAL_DEBUG_RUNTIME_TENSOR_LAYER")) |target| {
        if (target != layer_index) return;
    }
    var nonfinite: usize = 0;
    var nan_count: usize = 0;
    var inf_count: usize = 0;
    var first_bad: usize = data.len;
    var min_value: f32 = std.math.inf(f32);
    var max_value: f32 = -std.math.inf(f32);
    var max_abs: f32 = 0;
    for (data, 0..) |value, i| {
        if (!std.math.isFinite(value)) {
            if (first_bad == data.len) first_bad = i;
            nonfinite += 1;
            if (std.math.isNan(value)) nan_count += 1 else inf_count += 1;
            continue;
        }
        min_value = @min(min_value, value);
        max_value = @max(max_value, value);
        max_abs = @max(max_abs, @abs(value));
    }
    std.debug.print(
        "metal-runtime-finite label={s} layer={d} elems={d} nonfinite={d} nan={d} inf={d} first_bad={d} min={d} max={d} max_abs={d}\n",
        .{ label, layer_index, data.len, nonfinite, nan_count, inf_count, first_bad, min_value, max_value, max_abs },
    );
}

fn referenceQuantizedRuntimeLinearDebug() bool {
    return getenvBool("TERMITE_METAL_REFERENCE_RUNTIME_QUANT_LINEAR");
}

fn traceQuantizedGatedFfnPathRequested() bool {
    return getenvBool("TERMITE_METAL_TRACE_GATED_FFN_PATH");
}

fn traceQuantizedGatedFfnRcRequested() bool {
    return getenvBool("TERMITE_METAL_TRACE_GATED_FFN_RC");
}

fn traceQuantizedGatedFfnPath(
    comptime path_name: []const u8,
    gate_type: gguf_tensor_types.TensorType,
    up_type: gguf_tensor_types.TensorType,
    down_type: gguf_tensor_types.TensorType,
) void {
    if (!traceQuantizedGatedFfnPathRequested()) return;
    std.debug.print(
        "metal-gated-ffn-path path={s} gate={s} up={s} down={s}\n",
        .{
            path_name,
            tensorTypeLabel(gate_type),
            tensorTypeLabel(up_type),
            tensorTypeLabel(down_type),
        },
    );
}

pub const SelectedCompressedAttentionGatedQkv = struct {
    source: CompressedAttentionGatedQkvSource,
    q: MetalTensor,
    k_suffix: MetalTensor,
    v_suffix: MetalTensor,

    pub fn deinit(self: *SelectedCompressedAttentionGatedQkv) void {
        self.q.deinit();
        self.k_suffix.deinit();
        self.v_suffix.deinit();
        self.* = undefined;
    }
};

pub const CompressedAttentionGatedQkvSource = enum {
    provided_qkv,
    projected_q_only,
    projected_q_only_slot,
    projected_qkv,
};

fn retainedPlanTensor(tensor: MetalTensor) !MetalTensor {
    return tensor.retainedCopy();
}

pub const CompressedAttentionGatedAttentionPath = enum {
    direct_dense,
    quantized_residual,
};

fn tensorHostSlice(tensor: anytype) ![]f32 {
    const T = @TypeOf(tensor);
    if (T == MetalTensor) {
        var tmp = tensor;
        return tmp.toHostSlice();
    }
    if (T == *MetalTensor) return tensor.toHostSlice();
    if (T == *const MetalTensor) {
        var tmp = tensor.*;
        return tmp.toHostSlice();
    }
    @compileError("unsupported tensorHostSlice argument type");
}

fn tensorHostConstPtr(tensor: anytype) ![*c]const f32 {
    return (try tensorHostSlice(tensor)).ptr;
}

fn tensorHostPtr(tensor: anytype) ![*]f32 {
    return @ptrCast((try tensorHostSlice(tensor)).ptr);
}

pub const CompressedAttentionGatedFfnPath = enum {
    direct_dense,
    quantized_runtime,
    quantized_post_gate,
};

pub const PreparedCompressedAttentionGatedDecoderBlockPlan = struct {
    qkv: SelectedCompressedAttentionGatedQkv,
    attention_path: CompressedAttentionGatedAttentionPath,
    ffn_path: CompressedAttentionGatedFfnPath,

    pub fn deinit(self: *PreparedCompressedAttentionGatedDecoderBlockPlan) void {
        self.qkv.deinit();
        self.* = undefined;
    }
};

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn backendRequestFieldType(comptime func: anytype, comptime field_name: []const u8) type {
    const FuncType = @TypeOf(func);
    const FnType = switch (@typeInfo(FuncType)) {
        .pointer => |ptr| ptr.child,
        .@"fn" => FuncType,
        else => @compileError("expected function or function pointer"),
    };
    const fn_info = @typeInfo(FnType).@"fn";
    const request_ptr_ty = fn_info.params[1].type.?;
    const Request = std.meta.Child(request_ptr_ty);
    return @FieldType(Request, field_name);
}

fn backendRequestInputType(comptime func: anytype) type {
    return backendRequestFieldType(func, "input");
}

fn coerceBackendInput(comptime func: anytype, value: anytype) backendRequestInputType(func) {
    return coerceBackendField(func, "input", value);
}

fn coerceBackendField(comptime func: anytype, comptime field_name: []const u8, value: anytype) backendRequestFieldType(func, field_name) {
    const FieldType = backendRequestFieldType(func, field_name);
    if (FieldType == @TypeOf(value)) return value;
    if (FieldType == *anyopaque) return @ptrCast(@constCast(&value));
    @compileError("unsupported backend input handle type");
}

fn coerceMlxArrayHandle(value: anytype) c.mlx_array {
    if (@TypeOf(value) == c.mlx_array) return value;
    if (@TypeOf(value) == *anyopaque) return @as(*const c.mlx_array, @ptrCast(@alignCast(value))).*;
    @compileError("unsupported mlx array handle type");
}

fn backendOptionalPayloadType(comptime func: anytype) type {
    const FuncType = @TypeOf(func);
    const FnType = switch (@typeInfo(FuncType)) {
        .pointer => |ptr| ptr.child,
        .@"fn" => FuncType,
        else => @compileError("expected function or function pointer"),
    };
    const fn_info = @typeInfo(FnType).@"fn";
    const ReturnType = fn_info.return_type.?;
    const payload = switch (@typeInfo(ReturnType)) {
        .error_union => |eu| eu.payload,
        else => ReturnType,
    };
    return switch (@typeInfo(payload)) {
        .optional => |opt| opt.child,
        else => payload,
    };
}

fn backendUsesMlxArray(comptime func: anytype) bool {
    return backendRequestInputType(func) == c.mlx_array and backendOptionalPayloadType(func) == c.mlx_array;
}

fn backendUsesMlxArrayPair(comptime func: anytype) bool {
    const Payload = backendOptionalPayloadType(func);
    return @hasField(Payload, "first") and @hasField(Payload, "second") and
        @FieldType(Payload, "first") == c.mlx_array and
        @FieldType(Payload, "second") == c.mlx_array;
}

fn backendUsesMlxInputResidual(comptime func: anytype) bool {
    return backendRequestFieldType(func, "input") == c.mlx_array and
        backendRequestFieldType(func, "residual") == c.mlx_array and
        backendOptionalPayloadType(func) == c.mlx_array;
}

pub const SamplePenaltyEntries = struct {
    token_ids: []u32,
    counts: []u32,

    pub fn deinit(self: *SamplePenaltyEntries, allocator: std.mem.Allocator) void {
        allocator.free(self.token_ids);
        allocator.free(self.counts);
        self.* = undefined;
    }
};

pub fn buildSamplePenaltyEntries(
    allocator: std.mem.Allocator,
    token_history: []const i64,
) !SamplePenaltyEntries {
    var counts = std.AutoHashMapUnmanaged(u32, u32){};
    defer counts.deinit(allocator);

    for (token_history) |token_id| {
        if (token_id < 0 or token_id > std.math.maxInt(u32)) continue;
        const entry = try counts.getOrPut(allocator, @intCast(token_id));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }

    const entry_count = counts.count();
    const token_ids = try allocator.alloc(u32, entry_count);
    errdefer allocator.free(token_ids);
    const values = try allocator.alloc(u32, entry_count);
    errdefer allocator.free(values);

    var it = counts.iterator();
    var idx: usize = 0;
    while (it.next()) |entry| : (idx += 1) {
        token_ids[idx] = entry.key_ptr.*;
        values[idx] = entry.value_ptr.*;
    }

    return .{
        .token_ids = token_ids,
        .counts = values,
    };
}

pub fn makeSampleSeed(input_ptr: *const f32) u32 {
    const mixed = @as(u64, @truncate(@intFromPtr(input_ptr))) ^ 0x9e3779b97f4a7c15;
    var prng = std.Random.DefaultPrng.init(mixed);
    return prng.random().int(u32);
}

fn applyActivationHost(values: []f32, kind: ops.DecoderRuntimeActivationKind) void {
    switch (kind) {
        .gelu => activations.gelu(values),
        .gelu_new => for (values) |*v| {
            const x = v.*;
            const inner = 0.7978845608 * (x + 0.044715 * x * x * x);
            v.* = 0.5 * x * (1.0 + std.math.tanh(inner));
        },
        .silu => activations.silu(values),
        .relu => activations.relu(values),
        .quick_gelu => activations.quickGelu(values),
        .relu_squared => {
            activations.relu(values);
            for (values) |*v| v.* *= v.*;
        },
    }
}

fn sampleLogits(logits: []const f32, request: anytype) usize {
    return model_runtime.sampleTokenFromLogits(std.heap.c_allocator, logits, .{
        .temperature = request.temperature,
        .top_p = request.top_p,
        .top_k = @intCast(@min(request.top_k, @as(usize, @intCast(std.math.maxInt(i32))))),
        .min_p = request.min_p,
        .repetition_penalty = request.repetition_penalty,
        .frequency_penalty = request.frequency_penalty,
        .presence_penalty = request.presence_penalty,
    }, request.token_history);
}

fn tensorTypeLabel(tensor_type: gguf_tensor_types.TensorType) []const u8 {
    return switch (tensor_type) {
        .known => |known| @tagName(known),
        .bitnet_tl2 => "bitnet_tl2",
        .unknown => "unknown",
    };
}

pub fn decoderRuntimeFamilyPrepared(self: anytype) bool {
    return self.raw_decoder_family_prepared;
}

pub fn decoderRuntimePreparedKvTokens(self: anytype) usize {
    return self.raw_decoder_prepared_kv_tokens;
}

pub fn decoderRuntimeAbsoluteEmbeddingsPrepared(self: anytype) bool {
    return self.raw_absolute_embeddings_prepared;
}

pub fn decoderRuntimeRmsNormSlotPrepared(self: anytype, slot: usize, hidden_size: usize) bool {
    if (slot >= decoder_runtime_rms_norm_slot_capacity) return false;
    return self.raw_rms_norm_slots_prepared[slot] and self.raw_rms_norm_slot_hidden_sizes[slot] == hidden_size;
}

pub fn decoderRuntimeLayerNormSlotPrepared(self: anytype, slot: usize, hidden_size: usize) bool {
    if (slot >= decoder_runtime_layer_norm_slot_capacity) return false;
    return self.raw_layer_norm_slots_prepared[slot] and self.raw_layer_norm_slot_hidden_sizes[slot] == hidden_size;
}

pub fn decoderRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    if (slot >= decoder_runtime_linear_slot_capacity) return false;
    return self.raw_linear_slots_prepared[slot] and
        self.raw_linear_slot_in_dims[slot] == in_dim and
        self.raw_linear_slot_out_dims[slot] == out_dim;
}

pub fn noteDecoderRuntimeFamilyPrepared(self: anytype, kv_tokens: usize) void {
    self.raw_decoder_family_prepared = true;
    self.raw_decoder_prepared_kv_tokens = @max(self.raw_decoder_prepared_kv_tokens, kv_tokens);
}

pub fn noteDecoderRuntimeGreedyPrepared(self: anytype, kv_tokens: usize) void {
    self.raw_decoder_prepared_kv_tokens = @max(self.raw_decoder_prepared_kv_tokens, kv_tokens);
}

pub fn decoderRuntimePreparedSlotsMatchFamily(self: anytype, gpt_config: anytype) bool {
    const trace = getenvBool("TERMITE_METAL_PREPARE_TRACE");
    switch (gpt_config.family) {
        .bitnet => {
            for (0..gpt_config.num_hidden_layers) |layer| {
                if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_bitnet_runtime.normSlot(layer, .attn), gpt_config.hidden_size)) return false;
                if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_bitnet_runtime.normSlot(layer, .attn_sub), gpt_config.hidden_size)) return false;
                if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_bitnet_runtime.normSlot(layer, .ffn), gpt_config.hidden_size)) return false;
                if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_bitnet_runtime.normSlot(layer, .mlp_sub), gpt_config.intermediateSize(layer))) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.linearSlot(layer, .attn_q), gpt_config.hidden_size, gpt_config.num_attention_heads * gpt_config.headDim())) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.linearSlot(layer, .attn_k), gpt_config.hidden_size, gpt_config.effectiveKVHeads() * gpt_config.headDim())) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.linearSlot(layer, .attn_v), gpt_config.hidden_size, gpt_config.effectiveKVHeads() * gpt_config.headDim())) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.linearSlot(layer, .attn_out_proj), gpt_config.hidden_size, gpt_config.hidden_size)) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.linearSlot(layer, .mlp_gate), gpt_config.hidden_size, gpt_config.intermediateSize(layer))) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.linearSlot(layer, .mlp_up), gpt_config.hidden_size, gpt_config.intermediateSize(layer))) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.linearSlot(layer, .mlp_down), gpt_config.intermediateSize(layer), gpt_config.hidden_size)) return false;
            }
            return decoderRuntimeRmsNormSlotPrepared(self, decoder_bitnet_runtime.finalNormSlot(gpt_config.num_hidden_layers), gpt_config.hidden_size) and
                decoderRuntimeLinearSlotPrepared(self, decoder_bitnet_runtime.finalLmHeadSlot(gpt_config.num_hidden_layers), gpt_config.hidden_size, gpt_config.vocab_size);
        },
        .llama, .mistral, .qwen2, .qwen3, .gemma => {
            for (0..gpt_config.num_hidden_layers) |layer| {
                const layer_head_dim = gpt_config.effectiveHeadDimForLayer(layer);
                const layer_kv_heads = gpt_config.effectiveKVHeadsForLayer(layer);
                const attention_input_size = gpt_config.num_attention_heads * layer_head_dim;
                if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_gated_runtime.normSlot(layer, .attn_pre), gpt_config.hidden_size)) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=attn_pre_norm slot={d} hidden={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.normSlot(layer, .attn_pre), gpt_config.hidden_size });
                    return false;
                }
                if (gpt_config.family == .gemma) {
                    if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_gated_runtime.normSlot(layer, .attn_post), gpt_config.hidden_size)) {
                        if (trace) std.debug.print("prepare-trace: slot-miss family=gemma layer={d} kind=attn_post_norm slot={d} hidden={d}\n", .{ layer, decoder_gated_runtime.normSlot(layer, .attn_post), gpt_config.hidden_size });
                        return false;
                    }
                    if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_gated_runtime.normSlot(layer, .ffn_pre), gpt_config.hidden_size)) {
                        if (trace) std.debug.print("prepare-trace: slot-miss family=gemma layer={d} kind=ffn_pre_norm slot={d} hidden={d}\n", .{ layer, decoder_gated_runtime.normSlot(layer, .ffn_pre), gpt_config.hidden_size });
                        return false;
                    }
                    if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_gated_runtime.normSlot(layer, .ffn_post), gpt_config.hidden_size)) {
                        if (trace) std.debug.print("prepare-trace: slot-miss family=gemma layer={d} kind=ffn_post_norm slot={d} hidden={d}\n", .{ layer, decoder_gated_runtime.normSlot(layer, .ffn_post), gpt_config.hidden_size });
                        return false;
                    }
                } else {
                    if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_gated_runtime.normSlot(layer, .ffn_pre), gpt_config.hidden_size)) {
                        if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=ffn_pre_norm slot={d} hidden={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.normSlot(layer, .ffn_pre), gpt_config.hidden_size });
                        return false;
                    }
                }
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gated_runtime.linearSlot(layer, .attn_q), gpt_config.hidden_size, attention_input_size)) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=attn_q slot={d} in={d} out={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.linearSlot(layer, .attn_q), gpt_config.hidden_size, attention_input_size });
                    return false;
                }
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gated_runtime.linearSlot(layer, .attn_k), gpt_config.hidden_size, layer_kv_heads * layer_head_dim)) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=attn_k slot={d} in={d} out={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.linearSlot(layer, .attn_k), gpt_config.hidden_size, layer_kv_heads * layer_head_dim });
                    return false;
                }
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gated_runtime.linearSlot(layer, .attn_v), gpt_config.hidden_size, layer_kv_heads * layer_head_dim)) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=attn_v slot={d} in={d} out={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.linearSlot(layer, .attn_v), gpt_config.hidden_size, layer_kv_heads * layer_head_dim });
                    return false;
                }
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gated_runtime.linearSlot(layer, .attn_out_proj), attention_input_size, gpt_config.hidden_size)) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=attn_out_proj slot={d} in={d} out={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.linearSlot(layer, .attn_out_proj), attention_input_size, gpt_config.hidden_size });
                    return false;
                }
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gated_runtime.linearSlot(layer, .mlp_gate), gpt_config.hidden_size, gpt_config.intermediateSize(layer))) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=mlp_gate slot={d} in={d} out={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.linearSlot(layer, .mlp_gate), gpt_config.hidden_size, gpt_config.intermediateSize(layer) });
                    return false;
                }
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gated_runtime.linearSlot(layer, .mlp_up), gpt_config.hidden_size, gpt_config.intermediateSize(layer))) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=mlp_up slot={d} in={d} out={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.linearSlot(layer, .mlp_up), gpt_config.hidden_size, gpt_config.intermediateSize(layer) });
                    return false;
                }
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gated_runtime.linearSlot(layer, .mlp_down), gpt_config.intermediateSize(layer), gpt_config.hidden_size)) {
                    if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer={d} kind=mlp_down slot={d} in={d} out={d}\n", .{ @tagName(gpt_config.family), layer, decoder_gated_runtime.linearSlot(layer, .mlp_down), gpt_config.intermediateSize(layer), gpt_config.hidden_size });
                    return false;
                }
            }
            if (!decoderRuntimeRmsNormSlotPrepared(self, decoder_gated_runtime.finalNormSlot(gpt_config.num_hidden_layers), gpt_config.hidden_size)) {
                if (trace) std.debug.print("prepare-trace: slot-miss family={s} layer=final kind=final_norm slot={d} hidden={d}\n", .{ @tagName(gpt_config.family), decoder_gated_runtime.finalNormSlot(gpt_config.num_hidden_layers), gpt_config.hidden_size });
                return false;
            }
            return true;
        },
        .gpt2 => {
            if (!decoderRuntimeAbsoluteEmbeddingsPrepared(self)) return false;
            for (0..gpt_config.num_hidden_layers) |layer| {
                if (!decoderRuntimeLayerNormSlotPrepared(self, decoder_gpt_runtime.layerNormSlot(layer, false), gpt_config.hidden_size)) return false;
                if (!decoderRuntimeLayerNormSlotPrepared(self, decoder_gpt_runtime.layerNormSlot(layer, true), gpt_config.hidden_size)) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gpt_runtime.linearSlot(layer, .fused_attn), gpt_config.hidden_size, gpt_config.hidden_size * 3)) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gpt_runtime.linearSlot(layer, .attn_out_proj), gpt_config.hidden_size, gpt_config.hidden_size)) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gpt_runtime.linearSlot(layer, .mlp_fc1), gpt_config.hidden_size, gpt_config.intermediate_size)) return false;
                if (!decoderRuntimeLinearSlotPrepared(self, decoder_gpt_runtime.linearSlot(layer, .mlp_fc2), gpt_config.intermediate_size, gpt_config.hidden_size)) return false;
            }
            return decoderRuntimeLayerNormSlotPrepared(self, decoder_gpt_runtime.finalNormSlot(gpt_config.num_hidden_layers), gpt_config.hidden_size) and
                decoderRuntimeLinearSlotPrepared(self, decoder_gpt_runtime.finalLmHeadSlot(gpt_config.num_hidden_layers), gpt_config.hidden_size, gpt_config.vocab_size);
        },
        else => return false,
    }
}

pub fn supportsDecoderRuntimeConfig(gpt_config: anytype) bool {
    return switch (gpt_config.family) {
        .gpt2 => decoder_gpt_runtime.supportsConfig(gpt_config),
        .bitnet => decoder_bitnet_runtime.supportsConfig(gpt_config),
        .llama, .mistral, .qwen2, .qwen3 => decoder_gated_runtime.supportsConfig(gpt_config),
        // The Metal whole-model executor can still own text-only runs for
        // multimodal/PLE Gemma variants even when the deepest family-local
        // decoder-runtime fast path declines and falls back to the generic GPT
        // path. Do not reject those models at the top-level executor gate.
        .gemma => !gpt_config.usesMoe(),
        else => false,
    };
}

pub fn prepareDecodeRuntimeFamily(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: anytype,
    kv_tokens: usize,
    configured_layer_count: usize,
) !bool {
    return switch (gpt_config.family) {
        .gpt2 => try decoder_gpt_runtime.prepareDecodeRuntime(
            cb,
            allocator,
            gpt_config,
            kv_tokens,
            configured_layer_count,
        ),
        .bitnet => try decoder_bitnet_runtime.prepareDecodeRuntime(
            cb,
            allocator,
            gpt_config,
            kv_tokens,
            configured_layer_count,
        ),
        .llama, .mistral, .qwen2, .qwen3, .gemma => try decoder_gated_runtime.prepareDecodeRuntime(
            cb,
            allocator,
            gpt_config,
            kv_tokens,
            configured_layer_count,
        ),
        else => false,
    };
}

pub fn forwardLastLogitsTensorFamily(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: anytype,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const anyopaque,
) !?ops.CT {
    return switch (gpt_config.family) {
        .gpt2 => decoder_gpt_runtime.forwardLastLogitsTensor(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        .bitnet => decoder_bitnet_runtime.forwardLastLogitsTensor(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        .llama, .mistral, .qwen2, .gemma => decoder_gated_runtime.forwardLastLogitsTensor(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        else => null,
    };
}

pub fn forwardGreedyTokenFamily(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: anytype,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const anyopaque,
) !?i64 {
    return switch (gpt_config.family) {
        .gpt2 => decoder_gpt_runtime.forwardGreedyToken(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        .bitnet => decoder_bitnet_runtime.forwardGreedyToken(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        .llama, .mistral, .qwen2, .gemma => decoder_gated_runtime.forwardGreedyToken(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        else => null,
    };
}

pub fn forwardSampledTokenFamily(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: anytype,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const anyopaque,
    sampling: anytype,
    token_history: []const i64,
) !?i64 {
    return switch (gpt_config.family) {
        .gpt2 => decoder_gpt_runtime.forwardSampledToken(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
            sampling,
            token_history,
        ),
        .bitnet => decoder_bitnet_runtime.forwardSampledToken(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
            sampling,
            token_history,
        ),
        .llama, .mistral, .qwen2, .gemma => decoder_gated_runtime.forwardSampledToken(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            token_id,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
            sampling,
            token_history,
        ),
        else => null,
    };
}

pub fn forwardPrefillLastLogitsFamily(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: anytype,
    configured_layer_count: usize,
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const anyopaque,
) !?[]f32 {
    return switch (gpt_config.family) {
        .gpt2 => decoder_gpt_runtime.forwardPrefillLastLogits(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            input_ids,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        .bitnet => decoder_bitnet_runtime.forwardPrefillLastLogits(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            input_ids,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        .llama, .mistral, .qwen2, .gemma => decoder_gated_runtime.forwardPrefillLastLogits(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            input_ids,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        ),
        else => null,
    };
}

pub const PrefillPreparedTail = struct {
    final_hidden: ops.CT,
    final_norm_slot: usize,
    final_lm_head_slot: usize,
    hidden_size: usize,
    vocab_size: usize,
    norm_eps: f32,
};

pub fn forwardPrefillLastPreparedTailFamily(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: anytype,
    configured_layer_count: usize,
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const anyopaque,
) !?PrefillPreparedTail {
    return switch (gpt_config.family) {
        .llama, .mistral, .qwen2, .gemma => if (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            input_ids,
            seq_len,
            @ptrCast(@alignCast(decode_context)),
        )) |tail| .{
            .final_hidden = tail.final_hidden,
            .final_norm_slot = tail.final_norm_slot,
            .final_lm_head_slot = tail.final_lm_head_slot,
            .hidden_size = tail.hidden_size,
            .vocab_size = tail.vocab_size,
            .norm_eps = tail.norm_eps,
        } else null,
        else => null,
    };
}

pub fn decoderRuntimePrepareGreedy(self: anytype, request: anytype, stats: anytype) bool {
    if (self.raw_decode_runtime == null) return false;
    if (request.hidden_size == 0 or request.intermediate_size == 0 or request.num_layers == 0) return false;
    if (request.num_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0 or request.vocab_size == 0) return false;
    stats.decoder_runtime_prepare_calls += 1;
    const rc = termite_metal_decode_runtime_prepare_decoder_only_greedy(
        self.raw_decode_runtime,
        request.hidden_size,
        request.intermediate_size,
        request.num_layers,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.vocab_size,
        request.kv_tokens,
    );
    return rc == 0;
}

pub fn decoderRuntimeReservePrefillLayerScratch(
    self: anytype,
    rows: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    hidden_size: usize,
    intermediate_size: usize,
    tail_vocab_size: usize,
) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    return termite_metal_decode_runtime_reserve_prefill_layer_scratch(
        runtime,
        rows,
        num_heads,
        num_kv_heads,
        head_dim,
        hidden_size,
        intermediate_size,
        tail_vocab_size,
    ) == 0;
}

pub fn decoderRuntimeReserveGatedFfnScratch(
    self: anytype,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    return termite_metal_decode_runtime_reserve_gated_ffn_scratch(
        runtime,
        rows,
        hidden_size,
        intermediate_size,
    ) == 0;
}

pub fn decoderRuntimeReserveGreedyTailScratch(
    self: anytype,
    vocab_size: usize,
) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    return termite_metal_decode_runtime_reserve_greedy_tail_scratch(runtime, vocab_size) == 0;
}

pub fn decoderRuntimeReserveSampleTailScratch(
    self: anytype,
    vocab_size: usize,
    top_k: usize,
) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    return termite_metal_decode_runtime_reserve_sample_tail_scratch(runtime, vocab_size, top_k) == 0;
}

pub fn decoderRuntimeReserveAttentionSpanScratch(
    self: anytype,
    kv_tokens: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    return termite_metal_decode_runtime_reserve_attention_span_scratch(runtime, kv_tokens, key_row_bytes, v_row_stride) == 0;
}

pub fn decoderRuntimeResetState(self: anytype) void {
    resetGatheredSpans(self);
    const runtime = self.raw_decode_runtime orelse return;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return;
    _ = termite_metal_decode_runtime_reset_state(runtime);
}

pub fn decoderRuntimePrepareAbsoluteEmbeddings(self: anytype, request: anytype) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (request.hidden_size == 0 or request.vocab_size == 0 or request.max_position_embeddings == 0) return false;
    if (self.raw_absolute_embeddings_prepared and
        self.raw_absolute_embeddings_vocab_size == request.vocab_size and
        self.raw_absolute_embeddings_position_count == request.max_position_embeddings and
        self.raw_absolute_embeddings_hidden_size == request.hidden_size)
    {
        return true;
    }

    if (request.token_embedding.ndim() != 2 or request.position_embedding.ndim() != 2) return false;
    if (@as(usize, @intCast(request.token_embedding.dim(0))) != request.vocab_size) return false;
    if (@as(usize, @intCast(request.position_embedding.dim(0))) != request.max_position_embeddings) return false;
    if (@as(usize, @intCast(request.token_embedding.dim(1))) != request.hidden_size) return false;
    if (@as(usize, @intCast(request.position_embedding.dim(1))) != request.hidden_size) return false;

    var token_embedding = request.token_embedding;
    var position_embedding = request.position_embedding;
    const rc = termite_metal_decode_runtime_prepare_absolute_embeddings(
        runtime,
        try tensorHostConstPtr(&token_embedding),
        request.vocab_size,
        try tensorHostConstPtr(&position_embedding),
        request.max_position_embeddings,
        request.hidden_size,
    );
    if (rc != 0) return false;
    self.raw_absolute_embeddings_prepared = true;
    self.raw_absolute_embeddings_vocab_size = request.vocab_size;
    self.raw_absolute_embeddings_position_count = request.max_position_embeddings;
    self.raw_absolute_embeddings_hidden_size = request.hidden_size;
    return true;
}

pub fn decoderRuntimeEmbedAbsolutePosition(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0) return null;
    if (!self.raw_absolute_embeddings_prepared) return null;
    if (request.token_id >= self.raw_absolute_embeddings_vocab_size) return null;
    if (request.position_id >= self.raw_absolute_embeddings_position_count) return null;
    if (request.hidden_size != self.raw_absolute_embeddings_hidden_size) return null;
    const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
    errdefer std.heap.c_allocator.free(output);
    stats.decoder_runtime_embed_calls += 1;
    const rc = termite_metal_decode_runtime_embed_absolute_position(
        runtime,
        request.token_id,
        request.position_id,
        request.hidden_size,
        output.ptr,
    );
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(request.hidden_size) };
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeEmbeddingLookup(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.total == 0 or request.dim == 0) return null;
    if (request.weight.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.weight.dim(0)));
    if (rows == 0 or @as(usize, @intCast(request.weight.dim(1))) != request.dim) return null;
    if (request.ids.len != request.total) return null;

    const ids_u32 = try std.heap.c_allocator.alloc(u32, request.total);
    defer std.heap.c_allocator.free(ids_u32);
    for (request.ids, 0..) |id, i| {
        if (id < 0) return null;
        const idx: usize = @intCast(id);
        if (idx >= rows or idx > std.math.maxInt(u32)) return null;
        ids_u32[i] = @intCast(idx);
    }

    const shape = [_]i32{ @intCast(request.total), @intCast(request.dim) };
    if (request.weight.isDevice()) {
        const rc = termite_metal_decode_runtime_prepare_embedding_table_device(
            runtime,
            request.weight.deviceHandle(),
            request.weight.deviceByteOffset(),
            rows,
            request.dim,
        );
        if (rc == 0) {
            var output = try MetalTensor.deviceAllocate(runtime, request.total * request.dim * @sizeOf(f32), .private, &shape);
            errdefer output.deinit();
            const device_rc = termite_metal_decode_runtime_embedding_lookup_prepared_device(
                runtime,
                ids_u32.ptr,
                request.total,
                request.dim,
                output.deviceHandle(),
                output.deviceByteOffset(),
            );
            if (device_rc == 0) return output;
            output.deinit();
        }
    } else {
        var weight = request.weight;
        const prep_rc = termite_metal_decode_runtime_prepare_embedding_table(
            runtime,
            try tensorHostConstPtr(&weight),
            rows,
            request.dim,
        );
        if (prep_rc == 0) {
            var output = try MetalTensor.deviceAllocate(runtime, request.total * request.dim * @sizeOf(f32), .private, &shape);
            errdefer output.deinit();
            const device_rc = termite_metal_decode_runtime_embedding_lookup_prepared_device(
                runtime,
                ids_u32.ptr,
                request.total,
                request.dim,
                output.deviceHandle(),
                output.deviceByteOffset(),
            );
            if (device_rc == 0) return output;
            output.deinit();
        }
    }

    const output = try std.heap.c_allocator.alloc(f32, request.total * request.dim);
    errdefer std.heap.c_allocator.free(output);
    var weight = request.weight;
    const rc = termite_metal_decode_runtime_embedding_lookup(
        runtime,
        try tensorHostConstPtr(&weight),
        rows,
        ids_u32.ptr,
        request.total,
        request.dim,
        output.ptr,
    );
    if (rc != 0) return null;
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeDebertaEmbeddingsF32Device(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.total == 0 or request.dim == 0) return null;
    if (request.weight.ndim() != 2 or request.gamma.ndim() != 1 or request.beta.ndim() != 1) return null;
    const rows = @as(usize, @intCast(request.weight.dim(0)));
    if (rows == 0 or @as(usize, @intCast(request.weight.dim(1))) != request.dim) return null;
    if (@as(usize, @intCast(request.gamma.dim(0))) != request.dim or @as(usize, @intCast(request.beta.dim(0))) != request.dim) return null;
    if (request.ids.len != request.total or request.mask.len != request.total) return null;

    const ids_u32 = try std.heap.c_allocator.alloc(u32, request.total);
    defer std.heap.c_allocator.free(ids_u32);
    const mask_u32 = try std.heap.c_allocator.alloc(u32, request.total);
    defer std.heap.c_allocator.free(mask_u32);
    for (request.ids, 0..) |id, i| {
        if (id < 0) return null;
        const idx: usize = @intCast(id);
        if (idx >= rows or idx > std.math.maxInt(u32)) return null;
        ids_u32[i] = @intCast(idx);
    }
    for (request.mask, 0..) |value, i| {
        mask_u32[i] = if (value != 0) 1 else 0;
    }

    const shape = [_]i32{ @intCast(request.total), @intCast(request.dim) };
    var output = try MetalTensor.deviceAllocate(runtime, request.total * request.dim * @sizeOf(f32), .private, &shape);
    errdefer output.deinit();
    const rc = if (request.weight.isDevice() and request.gamma.isDevice() and request.beta.isDevice())
        termite_metal_decode_runtime_deberta_embeddings_f32_device(
            runtime,
            request.weight.deviceHandle(),
            request.weight.deviceByteOffset(),
            rows,
            request.gamma.deviceHandle(),
            request.gamma.deviceByteOffset(),
            request.beta.deviceHandle(),
            request.beta.deviceByteOffset(),
            ids_u32.ptr,
            mask_u32.ptr,
            request.total,
            request.dim,
            request.eps,
            output.deviceHandle(),
            output.deviceByteOffset(),
        )
    else blk: {
        if (request.weight.isDevice() or request.gamma.isDevice() or request.beta.isDevice()) return null;
        var weight = request.weight;
        var gamma = request.gamma;
        var beta = request.beta;
        break :blk termite_metal_decode_runtime_deberta_embeddings_f32(
            runtime,
            try tensorHostConstPtr(&weight),
            rows,
            try tensorHostConstPtr(&gamma),
            try tensorHostConstPtr(&beta),
            ids_u32.ptr,
            mask_u32.ptr,
            request.total,
            request.dim,
            request.eps,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
    };
    if (rc != 0) return null;
    return output;
}

pub fn decoderRuntimeQuantEmbeddingLookup(
    self: anytype,
    storage: *const QuantizedStorage,
    ids: []const i64,
    total: usize,
    dim: usize,
    scale: f32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const kind = quantizedRuntimeLinearKind(storage);
    const format = metalQuantFormatForKind(kind);
    if (format == .unsupported) return null;
    const layout = packedQuantBlockLayout(kind) orelse return null;
    if (total == 0 or dim == 0 or dim % layout.values_per_block != 0) return null;
    const rows = quantizedEmbeddingRows(storage, dim) orelse return null;
    if (ids.len != total) return null;

    const ids_u32 = try std.heap.c_allocator.alloc(u32, total);
    defer std.heap.c_allocator.free(ids_u32);
    for (ids, 0..) |id, i| {
        if (id < 0) return null;
        const idx: usize = @intCast(id);
        if (idx >= rows or idx > std.math.maxInt(u32)) return null;
        ids_u32[i] = @intCast(idx);
    }

    const source_bytes = storage.raw_bytes;
    const prep_rc = termite_metal_decode_runtime_prepare_quant_embedding_table(
        runtime,
        @intFromEnum(format),
        source_bytes.ptr,
        source_bytes.len,
        rows,
        dim,
    );
    if (prep_rc != 0) return null;

    const shape = [_]i32{ @intCast(total), @intCast(dim) };
    var output = try MetalTensor.deviceAllocate(runtime, total * dim * @sizeOf(f32), .private, &shape);
    errdefer output.deinit();
    const lookup_rc = termite_metal_decode_runtime_quant_embedding_lookup_prepared_device(
        runtime,
        @intFromEnum(format),
        ids_u32.ptr,
        total,
        dim,
        scale,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (lookup_rc != 0) return null;
    return output;
}

fn quantizedEmbeddingRows(storage: *const QuantizedStorage, dim: usize) ?usize {
    if (dim == 0 or storage.shape.len == 0) return null;
    const cols_i64 = storage.shape[storage.shape.len - 1];
    if (cols_i64 <= 0) return null;
    const cols: usize = @intCast(cols_i64);
    if (cols != dim) return null;

    const block_size = gguf_tensor_types.bytesPerBlock(storage.tensor_type) orelse return null;
    const values_per_block = gguf_tensor_types.valuesPerBlock(storage.tensor_type) orelse return null;
    if (dim % values_per_block != 0) return null;
    const row_bytes = (dim / values_per_block) * block_size;
    if (row_bytes == 0 or storage.raw_bytes.len % row_bytes != 0) return null;
    const rows_from_bytes = storage.raw_bytes.len / row_bytes;

    var rows_from_shape: usize = 1;
    for (storage.shape[0 .. storage.shape.len - 1]) |axis| {
        if (axis <= 0) return null;
        rows_from_shape = std.math.mul(usize, rows_from_shape, @intCast(axis)) catch return null;
    }
    if (rows_from_shape != rows_from_bytes) return null;
    return rows_from_shape;
}

pub fn decoderRuntimeNativeBf16EmbeddingLookup(
    self: anytype,
    bytes: []const u8,
    ids: []const i64,
    total: usize,
    dim: usize,
    rows: usize,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (total == 0 or dim == 0 or rows == 0 or ids.len != total) return null;
    const expected_bytes = std.math.mul(usize, try std.math.mul(usize, rows, dim), @sizeOf(u16)) catch return null;
    if (bytes.len < expected_bytes) return null;

    const ids_u32 = try std.heap.c_allocator.alloc(u32, total);
    defer std.heap.c_allocator.free(ids_u32);
    for (ids, 0..) |id, i| {
        if (id < 0) return null;
        const idx: usize = @intCast(id);
        if (idx >= rows or idx > std.math.maxInt(u32)) return null;
        ids_u32[i] = @intCast(idx);
    }

    const prep_rc = termite_metal_decode_runtime_prepare_embedding_table_bf16(
        runtime,
        bytes.ptr,
        bytes.len,
        rows,
        dim,
    );
    if (prep_rc != 0) return null;

    const shape = [_]i32{ @intCast(total), @intCast(dim) };
    var output = try MetalTensor.deviceAllocate(runtime, total * dim * @sizeOf(f32), .private, &shape);
    errdefer output.deinit();
    const lookup_rc = termite_metal_decode_runtime_embedding_lookup_bf16_prepared_device(
        runtime,
        ids_u32.ptr,
        total,
        dim,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (lookup_rc != 0) return null;
    return output;
}

pub fn decoderRuntimeApplyRope(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.head_dim == 0 or request.rope_dim == 0) return null;
    const total_values = request.input.elemCount();
    if (total_values == 0 or total_values % request.head_dim != 0) return null;
    const total_chunks = total_values / request.head_dim;
    if (request.positions.len != total_chunks) return null;

    const positions_u32 = try std.heap.c_allocator.alloc(u32, total_chunks);
    defer std.heap.c_allocator.free(positions_u32);
    for (request.positions, 0..) |pos, i| {
        if (pos > std.math.maxInt(u32)) return null;
        positions_u32[i] = @intCast(pos);
    }

    if (request.input.isDevice()) {
        var output = try MetalTensor.deviceAllocate(runtime, total_values * @sizeOf(f32), .private, request.input.shape());
        errdefer output.deinit();
        const device_rc = termite_metal_decode_runtime_apply_rope_device(
            runtime,
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            positions_u32.ptr,
            total_chunks,
            request.head_dim,
            request.rope_dim,
            request.theta,
            request.freq_scale,
            if (request.consecutive_pairs) 1 else 0,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (device_rc == 0) return output;
        output.deinit();
    }

    const output = try std.heap.c_allocator.alloc(f32, total_values);
    errdefer std.heap.c_allocator.free(output);
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_rope(
        runtime,
        try tensorHostConstPtr(&input),
        positions_u32.ptr,
        total_chunks,
        request.head_dim,
        request.rope_dim,
        request.theta,
        request.freq_scale,
        if (request.consecutive_pairs) 1 else 0,
        output.ptr,
    );
    if (rc != 0) return null;
    return MetalTensor.owned(output, request.input.shape());
}

pub fn decoderRuntimeApplyHeadRmsNormRope(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.total_heads == 0 or request.head_dim == 0 or request.rope_dim == 0) return null;
    if (request.position > std.math.maxInt(u32)) return null;
    if (!request.input.isDevice()) return null;
    const total_values = request.input.elemCount();
    if (total_values != request.total_heads * request.head_dim) return null;

    var output = try MetalTensor.deviceAllocate(runtime, total_values * @sizeOf(f32), .private, request.input.shape());
    errdefer output.deinit();
    const rc = termite_metal_decode_runtime_apply_head_rms_rope_device(
        runtime,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.slot,
        request.total_heads,
        request.head_dim,
        request.rope_dim,
        request.position,
        request.theta,
        request.freq_scale,
        request.eps,
        request.value_scale,
        if (request.consecutive_pairs) 1 else 0,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output;
}

pub fn decoderRuntimeApplyHeadRmsNormRopeBatched(self: anytype, request: anytype, heads_per_row: usize, position_period: usize) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.total_heads == 0 or request.head_dim == 0 or request.rope_dim == 0 or heads_per_row == 0 or position_period == 0) return null;
    if (request.position > std.math.maxInt(u32) or heads_per_row > std.math.maxInt(u32) or position_period > std.math.maxInt(u32)) return null;
    if (!request.input.isDevice()) return null;
    const total_values = request.input.elemCount();
    if (total_values != request.total_heads * request.head_dim) return null;

    var output = try MetalTensor.deviceAllocate(runtime, total_values * @sizeOf(f32), .private, request.input.shape());
    errdefer output.deinit();
    const rc = termite_metal_decode_runtime_apply_head_rms_rope_batched_device(
        runtime,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.slot,
        request.total_heads,
        request.head_dim,
        request.rope_dim,
        request.position,
        request.theta,
        request.freq_scale,
        request.eps,
        request.value_scale,
        if (request.consecutive_pairs) 1 else 0,
        heads_per_row,
        position_period,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output;
}

pub fn decoderRuntimeApplyHeadRmsNormRopeInto(self: anytype, request: anytype, output: MetalTensor) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (request.total_heads == 0 or request.head_dim == 0 or request.rope_dim == 0) return false;
    if (request.position > std.math.maxInt(u32)) return false;
    if (!request.input.isDevice() or !output.isDevice()) return false;
    const total_values = request.input.elemCount();
    if (total_values != request.total_heads * request.head_dim or output.elemCount() != total_values) return false;

    const rc = termite_metal_decode_runtime_apply_head_rms_rope_device(
        runtime,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.slot,
        request.total_heads,
        request.head_dim,
        request.rope_dim,
        request.position,
        request.theta,
        request.freq_scale,
        request.eps,
        request.value_scale,
        if (request.consecutive_pairs) 1 else 0,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    return rc == 0;
}

pub fn decoderRuntimeApplyHeadRmsNormRopeScratch(self: anytype, request: anytype, output_index: usize) !?MetalTensor {
    return decoderRuntimeApplyHeadRmsNormRopeRowsScratch(self, request, output_index, request.total_heads);
}

pub fn decoderRuntimeApplyHeadRmsNormRopeRowsScratch(self: anytype, request: anytype, output_index: usize, heads_per_row: usize) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.total_heads == 0 or request.head_dim == 0 or request.rope_dim == 0) return null;
    if (request.position > std.math.maxInt(u32) or heads_per_row == 0 or heads_per_row > std.math.maxInt(u32) or output_index >= 6) return null;
    if (!request.input.isDevice()) return null;
    const total_values = request.input.elemCount();
    if (total_values != request.total_heads * request.head_dim) return null;

    var output_handle: ?*anyopaque = null;
    const rc = termite_metal_decode_runtime_apply_head_rms_rope_scratch_device(
        runtime,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.slot,
        request.total_heads,
        request.head_dim,
        request.rope_dim,
        request.position,
        request.theta,
        request.freq_scale,
        request.eps,
        request.value_scale,
        if (request.consecutive_pairs) 1 else 0,
        heads_per_row,
        output_index,
        &output_handle,
    );
    if (rc != 0) return null;
    const handle = output_handle orelse return null;
    return MetalTensor.deviceBorrowed(@ptrCast(runtime), handle, 0, total_values * @sizeOf(f32), request.input.shape());
}

pub fn decoderRuntimeApplyRmsNormScratch(self: anytype, request: anytype, output_index: usize, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.slot >= decoder_runtime_layer_norm_slot_capacity) return null;
    if (!self.raw_rms_norm_slots_prepared[request.slot]) return null;
    if (self.raw_rms_norm_slot_hidden_sizes[request.slot] != request.hidden_size) return null;
    if (request.hidden_size == 0 or request.input.ndim() != 2 or output_index >= 6) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;
    if (!request.input.isDevice()) return null;

    stats.decoder_runtime_apply_layer_norm_calls += 1;
    var output_handle: ?*anyopaque = null;
    const device_rc = termite_metal_decode_runtime_apply_rms_norm_scratch_device(
        runtime,
        request.slot,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.hidden_size,
        request.eps,
        output_index,
        &output_handle,
    );
    if (device_rc != 0) return null;
    const handle = output_handle orelse return null;
    return MetalTensor.deviceBorrowed(@ptrCast(runtime), handle, 0, request.hidden_size * @sizeOf(f32), request.input.shape());
}

pub fn decoderRuntimeApplyAttentionF32(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.q.ndim() != 2 or request.k.ndim() != 2 or request.v.ndim() != 2) return null;
    if (request.q_len == 0 or request.kv_len == 0 or request.num_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0) return null;
    if (request.num_heads % request.num_kv_heads != 0) return null;
    if (@as(usize, @intCast(request.q.dim(0))) != request.q_len or
        @as(usize, @intCast(request.k.dim(0))) != request.kv_len or
        @as(usize, @intCast(request.v.dim(0))) != request.kv_len)
    {
        return null;
    }
    if (@as(usize, @intCast(request.q.dim(1))) != request.num_heads * request.head_dim or
        @as(usize, @intCast(request.k.dim(1))) != request.num_kv_heads * request.head_dim or
        @as(usize, @intCast(request.v.dim(1))) != request.num_kv_heads * request.head_dim)
    {
        return null;
    }
    if (@hasField(@TypeOf(request), "planned_layer_contract")) {
        if (!plannedContractAllowsF32Attention(request.planned_layer_contract)) return null;
    }

    const shape = [_]i32{ @intCast(request.q_len), @intCast(request.num_heads * request.head_dim) };
    const bias_tensor: ?MetalTensor = if (@hasField(@TypeOf(request), "bias")) request.bias else null;
    const attn_or_mask: ?[]const u8 = if (@hasField(@TypeOf(request), "attn_or_mask")) request.attn_or_mask else null;
    const total_sequence_len: usize = if (@hasField(@TypeOf(request), "total_sequence_len")) request.total_sequence_len else request.query_position_offset + request.q_len;

    if (request.q.isDevice() and request.k.isDevice() and request.v.isDevice()) {
        var bias_mut: ?MetalTensor = if (bias_tensor) |tensor| tensor else null;
        const bias_device_handle: ?*anyopaque = if (bias_mut) |*tensor|
            if (tensor.isDevice()) tensor.deviceHandle() else null
        else
            null;
        const bias_device_offset: usize = if (bias_mut) |*tensor|
            if (tensor.isDevice()) tensor.deviceByteOffset() else 0
        else
            0;
        const bias_host_ptr: [*c]const f32 = if (bias_mut) |*tensor|
            if (tensor.isDevice()) null else try tensorHostConstPtr(tensor)
        else
            null;
        const mask_ptr: [*c]const u8 = if (attn_or_mask) |mask|
            if (mask.len > 0) mask.ptr else null
        else
            null;
        var output = try MetalTensor.deviceAllocate(runtime, request.q_len * request.num_heads * request.head_dim * @sizeOf(f32), .private, &shape);
        errdefer output.deinit();
        const device_rc = termite_metal_decode_runtime_apply_attention_f32_device(
            runtime,
            request.q.deviceHandle(),
            request.q.deviceByteOffset(),
            request.k.deviceHandle(),
            request.k.deviceByteOffset(),
            request.v.deviceHandle(),
            request.v.deviceByteOffset(),
            request.q_len,
            request.kv_len,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.query_position_offset,
            request.kv_position_offset,
            request.sliding_window,
            bias_device_handle,
            bias_device_offset,
            bias_host_ptr,
            mask_ptr,
            total_sequence_len,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (device_rc == 0) return output;
        output.deinit();
    }

    const output = try std.heap.c_allocator.alloc(f32, request.q_len * request.num_heads * request.head_dim);
    errdefer std.heap.c_allocator.free(output);
    var q = request.q;
    var k = request.k;
    var v = request.v;
    var bias_mut: ?MetalTensor = if (bias_tensor) |tensor| tensor else null;
    const rc = termite_metal_decode_runtime_apply_attention_f32(
        runtime,
        try tensorHostConstPtr(&q),
        try tensorHostConstPtr(&k),
        try tensorHostConstPtr(&v),
        if (bias_mut) |*tensor| try tensorHostConstPtr(tensor) else null,
        if (attn_or_mask) |mask| if (mask.len > 0) mask.ptr else null else null,
        request.q_len,
        request.kv_len,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.query_position_offset,
        request.kv_position_offset,
        request.sliding_window,
        total_sequence_len,
        output.ptr,
    );
    if (rc != 0) return null;
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeApplyPagedKvAttentionSlot(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!request.q.isDevice()) return null;
    if (request.q.ndim() != 2) return null;
    if (request.kv_tokens == 0 or request.num_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0) return null;
    if (request.num_heads % request.num_kv_heads != 0) return null;

    const q_rows: usize = @intCast(request.q.dim(0));
    const attention_input_size = request.num_heads * request.head_dim;
    if (q_rows == 0 or @as(usize, @intCast(request.q.dim(1))) != attention_input_size) return null;
    if (@hasField(@TypeOf(request), "planned_layer_contract")) {
        if (!plannedContractAllowsPagedAttention(request.planned_layer_contract)) return null;
    }

    const attention_shape = [_]i32{ @intCast(q_rows), @intCast(attention_input_size) };
    var output = try MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        q_rows * attention_input_size * @sizeOf(f32),
        .private,
        &attention_shape,
    );
    errdefer output.deinit();

    const Request = @TypeOf(request);
    const page_size: usize = if (@hasField(Request, "page_size")) request.page_size else @min(request.kv_tokens, 256);
    if (page_size == 0) return null;
    var identity_blocks: ?[]u32 = null;
    defer if (identity_blocks) |blocks| std.heap.c_allocator.free(blocks);
    const block_table: []const u32 = if (@hasField(Request, "block_token_offsets"))
        request.block_token_offsets
    else blk: {
        const block_count = std.math.divCeil(usize, request.kv_tokens, page_size) catch return null;
        const blocks = try std.heap.c_allocator.alloc(u32, block_count);
        identity_blocks = blocks;
        for (blocks, 0..) |*entry, block_index| {
            const token_offset = block_index * page_size;
            if (token_offset > std.math.maxInt(u32)) return null;
            entry.* = @intCast(token_offset);
        }
        break :blk blocks;
    };
    if (block_table.len == 0) return null;
    const query_position_offset: usize = if (@hasField(Request, "query_position_offset"))
        request.query_position_offset
    else if (q_rows > 1) blk: {
        if (request.query_position + 1 < q_rows) return null;
        break :blk request.query_position + 1 - q_rows;
    } else request.query_position;
    const softcap: f32 = if (@hasField(Request, "softcap")) request.softcap else 0.0;
    const sinks: ?[]const f32 = if (@hasField(Request, "sinks")) request.sinks else null;
    const sinks_ptr: ?[*]const f32 = if (sinks) |slice| slice.ptr else null;
    const sink_count: usize = if (sinks) |slice| slice.len else 0;

    const rc = termite_metal_decode_runtime_attention_paged_slot_device(
        runtime,
        request.slot,
        request.format,
        request.q.deviceHandle(),
        request.q.deviceByteOffset(),
        block_table.ptr,
        block_table.len,
        page_size,
        q_rows,
        request.kv_tokens,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.key_row_bytes,
        request.base_key_row_bytes,
        query_position_offset,
        request.kv_position_offset,
        request.sliding_window,
        softcap,
        sinks_ptr,
        sink_count,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) {
        if (traceQuantBlockRequested()) std.debug.print(
            "metal-runtime-paged-attention-slot-null rc={d} slot={d} format={d} q_rows={d} kv_tokens={d} heads={d} kv_heads={d} head_dim={d} key_row_bytes={d} base_key_row_bytes={d} page_size={d} blocks={d} q_pos={d} kv_pos={d}\n",
            .{
                rc,
                request.slot,
                request.format,
                q_rows,
                request.kv_tokens,
                request.num_heads,
                request.num_kv_heads,
                request.head_dim,
                request.key_row_bytes,
                request.base_key_row_bytes,
                page_size,
                block_table.len,
                query_position_offset,
                request.kv_position_offset,
            },
        );
        return null;
    }

    return output;
}

fn plannedContractAllowsPagedAttention(contract: ops.PlannedLayerContract) bool {
    if (contract.command_ops.len == 0) return true;
    if (contract.start_index >= contract.command_ops.len) return false;
    const op = contract.command_ops[contract.start_index];
    if (op.kind != @intFromEnum(metal_command_planner.OpKind.attention)) return false;
    const operator = op.operator;
    if (operator != 255 and
        operator != @intFromEnum(metal_command_planner.Operator.attention_paged) and
        operator != @intFromEnum(metal_command_planner.Operator.attention_quantized_kv))
    {
        return false;
    }
    const format = op.format;
    return format == 255 or
        format == @intFromEnum(metal_command_planner.AttentionKvFormat.f32) or
        format == @intFromEnum(metal_command_planner.AttentionKvFormat.polar4) or
        format == @intFromEnum(metal_command_planner.AttentionKvFormat.turbo3) or
        format == @intFromEnum(metal_command_planner.AttentionKvFormat.quantized);
}

fn plannedContractAllowsF32Attention(contract: ops.PlannedLayerContract) bool {
    if (contract.command_ops.len == 0) return true;
    if (contract.start_index >= contract.command_ops.len) return false;
    const op = contract.command_ops[contract.start_index];
    if (op.kind != @intFromEnum(metal_command_planner.OpKind.attention)) return false;
    const operator = op.operator;
    if (operator != 255 and operator != @intFromEnum(metal_command_planner.Operator.attention_flash)) return false;
    const format = op.format;
    return format == 255 or format == @intFromEnum(metal_command_planner.AttentionKvFormat.f32);
}

pub fn decoderRuntimeApplyQuantizedKvAttention(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!request.q.isDevice() or !request.k.isDevice() or !request.v.isDevice()) return null;
    if (request.q.ndim() != 2 or request.k.ndim() != 2 or request.v.ndim() != 2) return null;
    if (request.kv_tokens == 0 or request.num_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0) return null;
    if (request.num_heads % request.num_kv_heads != 0) return null;

    const q_rows: usize = @intCast(request.q.dim(0));
    const attention_input_size = request.num_heads * request.head_dim;
    const kv_width = request.num_kv_heads * request.head_dim;
    if (q_rows == 0 or
        @as(usize, @intCast(request.q.dim(1))) != attention_input_size or
        @as(usize, @intCast(request.k.dim(0))) != request.kv_tokens or
        @as(usize, @intCast(request.v.dim(0))) != request.kv_tokens or
        @as(usize, @intCast(request.k.dim(1))) != kv_width or
        @as(usize, @intCast(request.v.dim(1))) < kv_width)
    {
        return null;
    }

    const v_row_stride: usize = @intCast(request.v.dim(1));
    if (!decoderRuntimeReserveAttentionSpanScratch(
        self,
        request.kv_tokens,
        request.key_row_bytes,
        v_row_stride,
    )) return null;

    const runtime_format: u32 = switch (request.format) {
        .polar4 => 0,
        .turbo3 => 1,
    };
    if (termite_metal_decode_runtime_update_attention_span_from_f32_key_device(
        runtime,
        runtime_format,
        request.k.deviceHandle(),
        request.k.deviceByteOffset(),
        request.v.deviceHandle(),
        request.v.deviceByteOffset(),
        request.kv_tokens,
        request.num_kv_heads,
        request.head_dim,
        request.key_row_bytes,
        request.base_key_row_bytes,
        v_row_stride,
        request.kv_position_offset,
    ) != 0) return null;

    const attention_shape = [_]i32{ @intCast(q_rows), @intCast(attention_input_size) };
    var output = try MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        q_rows * attention_input_size * @sizeOf(f32),
        .private,
        &attention_shape,
    );
    errdefer output.deinit();

    const Request = @TypeOf(request);
    const page_size: usize = if (@hasField(Request, "page_size")) request.page_size else @min(request.kv_tokens, 256);
    if (page_size == 0) return null;
    const block_count = std.math.divCeil(usize, request.kv_tokens, page_size) catch return null;
    const identity_blocks = try std.heap.c_allocator.alloc(u32, block_count);
    defer std.heap.c_allocator.free(identity_blocks);
    for (identity_blocks, 0..) |*entry, block_index| {
        const token_offset = block_index * page_size;
        if (token_offset > std.math.maxInt(u32)) return null;
        entry.* = @intCast(token_offset);
    }
    const query_position_offset: usize = if (@hasField(Request, "query_position_offset"))
        request.query_position_offset
    else if (q_rows > 1) blk: {
        if (request.query_position + 1 < q_rows) return null;
        break :blk request.query_position + 1 - q_rows;
    } else request.query_position;
    const softcap: f32 = if (@hasField(Request, "softcap")) request.softcap else 0.0;
    const sinks: ?[]const f32 = if (@hasField(Request, "sinks")) request.sinks else null;
    const sinks_ptr: ?[*]const f32 = if (sinks) |slice| slice.ptr else null;
    const sink_count: usize = if (sinks) |slice| slice.len else 0;

    if (termite_metal_decode_runtime_attention_paged_slot_device(
        runtime,
        0,
        runtime_format,
        request.q.deviceHandle(),
        request.q.deviceByteOffset(),
        identity_blocks.ptr,
        identity_blocks.len,
        page_size,
        q_rows,
        request.kv_tokens,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.key_row_bytes,
        request.base_key_row_bytes,
        query_position_offset,
        request.kv_position_offset,
        request.sliding_window,
        softcap,
        sinks_ptr,
        sink_count,
        output.deviceHandle(),
        output.deviceByteOffset(),
    ) != 0) return null;

    return output;
}

pub fn sampleLogitsDevice(self: anytype, request: anytype) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.out_dim == 0) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.out_dim) return null;

    var penalty_entries = try buildSamplePenaltyEntries(std.heap.c_allocator, request.token_history);
    defer penalty_entries.deinit(std.heap.c_allocator);
    var token_id: u32 = 0;
    if (!request.input.isDevice()) {
        var input = request.input;
        const input_host = try tensorHostSlice(&input);
        return sampleLogits(input_host, request);
    }
    if (!decoderRuntimeReserveSampleTailScratch(self, request.out_dim, request.top_k)) return null;

    const seed = makeSampleSeed(@ptrCast(@alignCast(request.input.deviceHandle().?)));
    const rc = termite_metal_decode_runtime_sample_from_logits_device(
        runtime,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.out_dim,
        request.temperature,
        request.top_k,
        request.top_p,
        request.min_p,
        request.repetition_penalty,
        request.frequency_penalty,
        request.presence_penalty,
        if (penalty_entries.token_ids.len > 0) &penalty_entries.token_ids[0] else null,
        if (penalty_entries.counts.len > 0) &penalty_entries.counts[0] else null,
        penalty_entries.token_ids.len,
        seed,
        &token_id,
    );
    if (rc != 0) return null;
    return token_id;
}

pub fn argmaxLogitsDevice(self: anytype, input: MetalTensor, out_dim: usize) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (out_dim == 0) return null;
    if (input.ndim() != 2) return null;
    if (@as(usize, @intCast(input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(input.dim(1))) != out_dim) return null;

    var token_id: u32 = 0;
    const rc = termite_metal_decode_runtime_argmax_from_logits_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        out_dim,
        &token_id,
    );
    if (rc != 0) return null;
    return token_id;
}

pub fn encodeArgmaxLogitsDevice(self: anytype, input: MetalTensor, out_dim: usize) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (!input.isDevice()) return false;
    if (out_dim == 0) return false;
    if (input.ndim() != 2) return false;
    if (@as(usize, @intCast(input.dim(0))) != 1) return false;
    if (@as(usize, @intCast(input.dim(1))) != out_dim) return false;
    return termite_metal_decode_runtime_encode_argmax_from_logits_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        out_dim,
    ) == 0;
}

pub fn decoderRuntimePrepareLayerNorm(self: anytype, request: anytype) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (request.hidden_size == 0 or request.slot >= decoder_runtime_layer_norm_slot_capacity) return false;
    if (self.raw_layer_norm_slots_prepared[request.slot] and
        self.raw_layer_norm_slot_hidden_sizes[request.slot] == request.hidden_size)
    {
        return true;
    }

    if (request.weight.ndim() != 1 or request.bias.ndim() != 1) return false;
    if (@as(usize, @intCast(request.weight.dim(0))) != request.hidden_size) return false;
    if (@as(usize, @intCast(request.bias.dim(0))) != request.hidden_size) return false;

    var weight = request.weight;
    var bias = request.bias;
    const rc = termite_metal_decode_runtime_prepare_layer_norm(
        runtime,
        request.slot,
        try tensorHostConstPtr(&weight),
        try tensorHostConstPtr(&bias),
        request.hidden_size,
    );
    if (rc != 0) return false;
    var weight_for_clone = request.weight;
    self.raw_layer_norm_slot_weights[request.slot] = try MetalTensor.ownedCloneFrom(try tensorHostSlice(&weight_for_clone), request.weight.shape());
    errdefer {
        if (self.raw_layer_norm_slot_weights[request.slot]) |*arr| {
            arr.deinit();
            self.raw_layer_norm_slot_weights[request.slot] = null;
        }
    }
    var bias_for_clone = request.bias;
    self.raw_layer_norm_slot_biases[request.slot] = try MetalTensor.ownedCloneFrom(try tensorHostSlice(&bias_for_clone), request.bias.shape());
    errdefer {
        if (self.raw_layer_norm_slot_biases[request.slot]) |*arr| {
            arr.deinit();
            self.raw_layer_norm_slot_biases[request.slot] = null;
        }
    }
    self.raw_layer_norm_slots_prepared[request.slot] = true;
    self.raw_layer_norm_slot_hidden_sizes[request.slot] = request.hidden_size;
    return true;
}

pub fn decoderRuntimePrepareRmsNorm(self: anytype, request: anytype) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (request.hidden_size == 0 or request.slot >= decoder_runtime_rms_norm_slot_capacity) return false;
    if (self.raw_rms_norm_slots_prepared[request.slot] and
        self.raw_rms_norm_slot_hidden_sizes[request.slot] == request.hidden_size)
    {
        return true;
    }

    if (request.weight.ndim() != 1) return false;
    if (@as(usize, @intCast(request.weight.dim(0))) != request.hidden_size) return false;

    if (request.weight.isDevice()) {
        if (request.weight.deviceHandle()) |handle| {
            if (request.weight.deviceByteLen() < request.hidden_size * @sizeOf(f32)) return false;
            const device_rc = termite_metal_decode_runtime_prepare_rms_norm_from_buffer(
                runtime,
                request.slot,
                handle,
                request.weight.deviceByteOffset(),
                request.hidden_size,
            );
            if (device_rc == 0) {
                if (self.raw_rms_norm_slot_weights[request.slot]) |*arr| {
                    arr.deinit();
                    self.raw_rms_norm_slot_weights[request.slot] = null;
                }
                self.raw_rms_norm_slots_prepared[request.slot] = true;
                self.raw_rms_norm_slot_hidden_sizes[request.slot] = request.hidden_size;
                return true;
            }
        }
    }

    var weight = request.weight;
    const rc = termite_metal_decode_runtime_prepare_rms_norm(
        runtime,
        request.slot,
        try tensorHostConstPtr(&weight),
        request.hidden_size,
    );
    if (rc != 0) return false;
    var weight_for_clone = request.weight;
    self.raw_rms_norm_slot_weights[request.slot] = try MetalTensor.ownedCloneFrom(try tensorHostSlice(&weight_for_clone), request.weight.shape());
    errdefer {
        if (self.raw_rms_norm_slot_weights[request.slot]) |*arr| {
            arr.deinit();
            self.raw_rms_norm_slot_weights[request.slot] = null;
        }
    }
    self.raw_rms_norm_slots_prepared[request.slot] = true;
    self.raw_rms_norm_slot_hidden_sizes[request.slot] = request.hidden_size;
    return true;
}

pub fn decoderRuntimeApplyLayerNorm(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.slot >= decoder_runtime_layer_norm_slot_capacity) return null;
    if (!self.raw_layer_norm_slots_prepared[request.slot]) return null;
    if (self.raw_layer_norm_slot_hidden_sizes[request.slot] != request.hidden_size) return null;
    if (request.input.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.input.dim(0)));
    if (rows == 0) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;
    stats.decoder_runtime_apply_layer_norm_calls += 1;
    const shape = [_]i32{ @intCast(rows), @intCast(request.hidden_size) };
    if (request.input.isDevice()) {
        var output = try MetalTensor.deviceAllocate(runtime, rows * request.hidden_size * @sizeOf(f32), .private, &shape);
        errdefer output.deinit();
        const device_rc = termite_metal_decode_runtime_apply_layer_norm_device(
            runtime,
            request.slot,
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            rows,
            request.hidden_size,
            request.eps,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (device_rc == 0) return output;
        output.deinit();
    }

    const output = try std.heap.c_allocator.alloc(f32, rows * request.hidden_size);
    errdefer std.heap.c_allocator.free(output);
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_layer_norm(
        runtime,
        request.slot,
        try tensorHostConstPtr(&input),
        rows,
        request.hidden_size,
        request.eps,
        output.ptr,
    );
    if (rc != 0) return null;
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeApplyAddLayerNorm(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.slot >= decoder_runtime_layer_norm_slot_capacity) return null;
    if (!self.raw_layer_norm_slots_prepared[request.slot]) return null;
    if (self.raw_layer_norm_slot_hidden_sizes[request.slot] != request.hidden_size) return null;
    if (!request.a.isDevice() or !request.b.isDevice()) return null;
    if (request.a.ndim() != 2 or request.b.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.a.dim(0)));
    if (rows == 0) return null;
    if (@as(usize, @intCast(request.a.dim(1))) != request.hidden_size) return null;
    if (@as(usize, @intCast(request.b.dim(0))) != rows) return null;
    if (@as(usize, @intCast(request.b.dim(1))) != request.hidden_size) return null;
    stats.decoder_runtime_apply_layer_norm_calls += 1;
    const shape = [_]i32{ @intCast(rows), @intCast(request.hidden_size) };
    var output = try MetalTensor.deviceAllocate(runtime, rows * request.hidden_size * @sizeOf(f32), .private, &shape);
    errdefer output.deinit();
    const device_rc = termite_metal_decode_runtime_apply_add_layer_norm_device(
        runtime,
        request.slot,
        request.a.deviceHandle(),
        request.a.deviceByteOffset(),
        request.b.deviceHandle(),
        request.b.deviceByteOffset(),
        rows,
        request.hidden_size,
        request.eps,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (device_rc == 0) return output;
    output.deinit();
    return null;
}

pub fn decoderRuntimeApplyRmsNorm(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.slot >= decoder_runtime_rms_norm_slot_capacity) return null;
    if (!self.raw_rms_norm_slots_prepared[request.slot]) return null;
    if (self.raw_rms_norm_slot_hidden_sizes[request.slot] != request.hidden_size) return null;
    if (request.input.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.input.dim(0)));
    if (rows == 0) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;
    if (rows != 1 and request.input.isDevice()) {
        var output = try MetalTensor.deviceAllocate(runtime, rows * request.hidden_size * @sizeOf(f32), .private, request.input.shape());
        errdefer output.deinit();
        const device_rc = termite_metal_decode_runtime_apply_rms_norm_rows_device(
            runtime,
            request.slot,
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            rows,
            request.hidden_size,
            request.eps,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (device_rc == 0) return output;
        output.deinit();
    }
    if (rows != 1) {
        var weight_tensor = self.raw_rms_norm_slot_weights[request.slot] orelse return null;
        var input = request.input;
        const input_slice = try tensorHostSlice(&input);
        const input_host = try std.heap.c_allocator.alloc(f32, input_slice.len);
        errdefer std.heap.c_allocator.free(input_host);
        @memcpy(input_host, input_slice);
        activations.rmsNorm(input_host, try tensorHostSlice(&weight_tensor), request.hidden_size, request.eps);
        const shape = [_]i32{ @intCast(rows), @intCast(request.hidden_size) };
        return MetalTensor.owned(input_host, &shape);
    }

    stats.decoder_runtime_apply_layer_norm_calls += 1;
    const shape = [_]i32{ 1, @intCast(request.hidden_size) };
    if (request.input.isDevice()) {
        var output = try MetalTensor.deviceAllocate(runtime, request.hidden_size * @sizeOf(f32), .private, &shape);
        errdefer output.deinit();
        const device_rc = termite_metal_decode_runtime_apply_rms_norm_device(
            runtime,
            request.slot,
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            request.hidden_size,
            request.eps,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (device_rc == 0) return output;
        output.deinit();
    }

    const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
    errdefer std.heap.c_allocator.free(output);
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_rms_norm(
        runtime,
        request.slot,
        try tensorHostConstPtr(&input),
        request.hidden_size,
        request.eps,
        output.ptr,
    );
    if (rc != 0) return null;
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeApplyRmsNormWeightDevice(
    self: anytype,
    input: MetalTensor,
    weight: MetalTensor,
    hidden_size: usize,
    eps: f32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or !weight.isDevice()) return null;
    if (hidden_size == 0 or input.ndim() != 2 or weight.ndim() != 1) return null;
    const rows: usize = @intCast(input.dim(0));
    if (rows == 0 or @as(usize, @intCast(input.dim(1))) != hidden_size) return null;
    if (@as(usize, @intCast(weight.dim(0))) != hidden_size) return null;

    var output = try MetalTensor.deviceAllocate(runtime, rows * hidden_size * @sizeOf(f32), .private, input.shape());
    errdefer output.deinit();
    const rc = termite_metal_decode_runtime_apply_rms_norm_weight_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        weight.deviceHandle(),
        weight.deviceByteOffset(),
        rows,
        hidden_size,
        eps,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output;
}

pub fn decoderRuntimeApplyRmsNormWeightDeviceInto(
    self: anytype,
    input: MetalTensor,
    weight: MetalTensor,
    hidden_size: usize,
    eps: f32,
    output: MetalTensor,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (!input.isDevice() or !weight.isDevice() or !output.isDevice()) return false;
    if (hidden_size == 0 or input.ndim() != 2 or weight.ndim() != 1 or output.ndim() != 2) return false;
    const rows: usize = @intCast(input.dim(0));
    if (rows == 0 or @as(usize, @intCast(input.dim(1))) != hidden_size) return false;
    if (@as(usize, @intCast(weight.dim(0))) != hidden_size) return false;
    if (@as(usize, @intCast(output.dim(0))) != rows or @as(usize, @intCast(output.dim(1))) != hidden_size) return false;

    const rc = termite_metal_decode_runtime_apply_rms_norm_weight_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        weight.deviceHandle(),
        weight.deviceByteOffset(),
        rows,
        hidden_size,
        eps,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    return rc == 0;
}

pub fn decoderRuntimeApplyRmsNormWeightDeviceScratch(
    self: anytype,
    input: MetalTensor,
    weight: MetalTensor,
    hidden_size: usize,
    eps: f32,
    output_index: usize,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or !weight.isDevice()) return null;
    if (hidden_size == 0 or input.ndim() != 2 or weight.ndim() != 1 or output_index >= 6) return null;
    const rows: usize = @intCast(input.dim(0));
    if (rows == 0 or @as(usize, @intCast(input.dim(1))) != hidden_size) return null;
    if (@as(usize, @intCast(weight.dim(0))) != hidden_size) return null;

    var output_handle: ?*anyopaque = null;
    const rc = termite_metal_decode_runtime_apply_rms_norm_weight_scratch_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        weight.deviceHandle(),
        weight.deviceByteOffset(),
        rows,
        hidden_size,
        eps,
        output_index,
        &output_handle,
    );
    if (rc != 0) return null;
    const handle = output_handle orelse return null;
    return MetalTensor.deviceBorrowed(@ptrCast(runtime), handle, 0, rows * hidden_size * @sizeOf(f32), input.shape());
}

pub fn decoderRuntimeApplyLayerNormLinearArgmax(self: anytype, request: anytype) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    if (request.hidden_size == 0 or request.out_dim == 0) return null;
    if (request.norm_slot >= decoder_runtime_layer_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_layer_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_layer_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return null;
    if (self.raw_linear_slot_kinds[request.linear_slot] != .dense) return null;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return null;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;

    if (request.input.isDevice()) {
        var norm_stats = struct {
            decoder_runtime_apply_layer_norm_calls: u64 = 0,
        }{};
        var normed = (try decoderRuntimeApplyLayerNorm(self, .{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &norm_stats)) orelse return null;
        defer normed.deinit();

        var token_id_device: u32 = 0;
        const device_rc = termite_metal_decode_runtime_apply_linear_argmax_device(
            runtime,
            request.linear_slot,
            normed.deviceHandle(),
            normed.deviceByteOffset(),
            request.hidden_size,
            request.out_dim,
            &token_id_device,
        );
        if (device_rc == 0) return token_id_device;
    }

    if (frame_active) return null;

    var token_id: u32 = 0;
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_layer_norm_linear_argmax(
        runtime,
        request.norm_slot,
        request.linear_slot,
        try tensorHostConstPtr(&input),
        request.hidden_size,
        request.eps,
        request.out_dim,
        &token_id,
    );
    if (rc != 0) return null;
    return token_id;
}

pub fn decoderRuntimeApplyLayerNormLinear(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.out_dim == 0) return null;
    if (request.norm_slot >= decoder_runtime_layer_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_layer_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_layer_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return null;
    if (self.raw_linear_slot_kinds[request.linear_slot] != .dense) return null;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return null;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;

    if (request.input.isDevice()) {
        var norm_stats = struct {
            decoder_runtime_apply_layer_norm_calls: u64 = 0,
        }{};
        var normed = (try decoderRuntimeApplyLayerNorm(self, .{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &norm_stats)) orelse return null;
        defer normed.deinit();
        return decoderRuntimeApplyLinear(self, .{
            .slot = request.linear_slot,
            .input = normed,
            .in_dim = request.hidden_size,
            .out_dim = request.out_dim,
        });
    }

    const output = try std.heap.c_allocator.alloc(f32, request.out_dim);
    errdefer std.heap.c_allocator.free(output);
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_layer_norm_linear(
        runtime,
        request.norm_slot,
        request.linear_slot,
        try tensorHostConstPtr(&input),
        request.hidden_size,
        request.eps,
        request.out_dim,
        output.ptr,
    );
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(request.out_dim) };
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeApplyLayerNormLinearSample(self: anytype, request: anytype) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.out_dim == 0) return null;
    if (request.norm_slot >= decoder_runtime_layer_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_layer_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_layer_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return null;
    if (self.raw_linear_slot_kinds[request.linear_slot] != .dense) return null;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return null;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;

    const bounded_top_p = request.top_p <= 0.0 or request.top_p >= 1.0 or
        request.top_k > 0 or request.out_dim <= 256;
    if (bounded_top_p) {
        if (decoderRuntimeReserveSampleTailScratch(self, request.out_dim, request.top_k)) {
            var penalty_entries = try buildSamplePenaltyEntries(std.heap.c_allocator, request.token_history);
            defer penalty_entries.deinit(std.heap.c_allocator);
            var input = request.input;
            const input_host = try tensorHostSlice(&input);
            var token_id: u32 = 0;
            const seed = makeSampleSeed(&input_host[0]);
            const device_rc = termite_metal_decode_runtime_apply_layer_norm_linear_sample_device(
                runtime,
                request.norm_slot,
                request.linear_slot,
                input_host.ptr,
                request.hidden_size,
                request.eps,
                request.out_dim,
                request.temperature,
                request.top_k,
                request.top_p,
                request.min_p,
                request.repetition_penalty,
                request.frequency_penalty,
                request.presence_penalty,
                if (penalty_entries.token_ids.len > 0) &penalty_entries.token_ids[0] else null,
                if (penalty_entries.counts.len > 0) &penalty_entries.counts[0] else null,
                penalty_entries.token_ids.len,
                seed,
                &token_id,
            );
            if (device_rc == 0) return token_id;
        }
    }

    const logits_host = try std.heap.c_allocator.alloc(f32, request.out_dim);
    defer std.heap.c_allocator.free(logits_host);
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_layer_norm_linear(
        runtime,
        request.norm_slot,
        request.linear_slot,
        try tensorHostConstPtr(&input),
        request.hidden_size,
        request.eps,
        request.out_dim,
        logits_host.ptr,
    );
    if (rc != 0) return null;
    return sampleLogits(logits_host, request);
}

pub fn decoderRuntimeApplyRmsNormLinearArgmax(self: anytype, request: anytype) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.out_dim == 0) return null;
    if (request.norm_slot >= decoder_runtime_rms_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_rms_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_rms_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return null;
    const linear_kind = self.raw_linear_slot_kinds[request.linear_slot];
    if (linear_kind != .dense and linear_kind != .quantized) return null;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return null;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;

    if (request.input.isDevice()) {
        var norm_stats = struct {
            decoder_runtime_apply_layer_norm_calls: u64 = 0,
        }{};
        var normed = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &norm_stats)) orelse return null;
        defer normed.deinit();

        if (linear_kind == .quantized) {
            // Keep standalone prepared-tail greedy on the materialized-logits
            // route. Directly encoding rms_norm + quantized lm_head + argmax
            // outside a planned frame triggered a 2026-05-07 SoC watchdog
            // reset under Metal API validation.
            var logits = (try decoderRuntimeApplyLinear(self, .{
                .slot = request.linear_slot,
                .input = normed,
                .in_dim = request.hidden_size,
                .out_dim = request.out_dim,
            })) orelse return null;
            defer logits.deinit();
            return argmaxSingleRowLogits(self, logits, request.out_dim);
        }

        var token_id_device: u32 = 0;
        const device_rc = termite_metal_decode_runtime_apply_linear_argmax_device(
            runtime,
            request.linear_slot,
            normed.deviceHandle(),
            normed.deviceByteOffset(),
            request.hidden_size,
            request.out_dim,
            &token_id_device,
        );
        if (device_rc == 0) return token_id_device;
    }

    if (linear_kind == .quantized) {
        var norm_stats = struct {
            decoder_runtime_apply_layer_norm_calls: u64 = 0,
        }{};
        var normed = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &norm_stats)) orelse return null;
        defer normed.deinit();
        var logits = (try decoderRuntimeApplyLinear(self, .{
            .slot = request.linear_slot,
            .input = normed,
            .in_dim = request.hidden_size,
            .out_dim = request.out_dim,
        })) orelse return null;
        defer logits.deinit();
        return argmaxSingleRowLogits(self, logits, request.out_dim);
    }

    var token_id: u32 = 0;
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_rms_norm_linear_argmax(
        runtime,
        request.norm_slot,
        request.linear_slot,
        try tensorHostConstPtr(&input),
        request.hidden_size,
        request.eps,
        request.out_dim,
        &token_id,
    );
    if (rc != 0) return null;
    return token_id;
}

pub fn decoderRuntimeEncodeRmsNormLinearArgmaxDevice(self: anytype, request: anytype) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (request.hidden_size == 0 or request.out_dim == 0) return false;
    if (request.norm_slot >= decoder_runtime_rms_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_rms_norm_slots_prepared[request.norm_slot]) return false;
    if (self.raw_rms_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return false;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return false;
    const linear_kind = self.raw_linear_slot_kinds[request.linear_slot];
    if (linear_kind != .dense and linear_kind != .quantized) return false;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return false;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return false;
    if (request.input.ndim() != 2) return false;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return false;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return false;
    if (!request.input.isDevice()) return false;
    const planned_layer_contract: ops.PlannedLayerContract = if (@hasField(@TypeOf(request), "planned_layer_contract")) request.planned_layer_contract else .{};
    const raw_planned_layer_contract = RawPlannedLayerContract.fromContract(planned_layer_contract);
    if (linear_kind == .quantized) {
        const quant_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.linear_slot, request.hidden_size, request.out_dim);
        const format = metalQuantFormatForKind(quant_kind);
        if (format == .unsupported) return false;
        return termite_metal_decode_runtime_encode_rms_norm_quantized_linear_argmax_device(
            runtime,
            request.norm_slot,
            request.linear_slot,
            @intFromEnum(format),
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            request.hidden_size,
            request.eps,
            request.out_dim,
            raw_planned_layer_contract,
        ) == 0;
    }
    return termite_metal_decode_runtime_encode_rms_norm_linear_argmax_device(
        runtime,
        request.norm_slot,
        request.linear_slot,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.hidden_size,
        request.eps,
        request.out_dim,
        raw_planned_layer_contract,
    ) == 0;
}

pub fn decoderRuntimeEncodeRmsNormLinearLogitsDevice(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.out_dim == 0) return null;
    if (request.norm_slot >= decoder_runtime_rms_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_rms_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_rms_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return null;
    if (self.raw_linear_slot_kinds[request.linear_slot] != .quantized) return null;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return null;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;
    if (!request.input.isDevice()) return null;
    const quant_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.linear_slot, request.hidden_size, request.out_dim);
    const format = metalQuantFormatForKind(quant_kind);
    if (format == .unsupported) return null;
    const planned_layer_contract: ops.PlannedLayerContract = if (@hasField(@TypeOf(request), "planned_layer_contract")) request.planned_layer_contract else .{};
    const raw_planned_layer_contract = RawPlannedLayerContract.fromContract(planned_layer_contract);
    var logits_handle: ?*anyopaque = null;
    const rc = termite_metal_decode_runtime_encode_rms_norm_quantized_linear_logits_device(
        runtime,
        request.norm_slot,
        request.linear_slot,
        @intFromEnum(format),
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.hidden_size,
        request.eps,
        request.out_dim,
        raw_planned_layer_contract,
        &logits_handle,
    );
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(request.out_dim) };
    return MetalTensor.deviceBorrowed(@ptrCast(runtime), logits_handle orelse return null, 0, request.out_dim * @sizeOf(f32), &shape);
}

fn runtimeLinearSlotSupportsDeviceApply(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    if (slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_linear_slots_prepared[slot]) return false;
    if (self.raw_linear_slot_in_dims[slot] != in_dim or self.raw_linear_slot_out_dims[slot] != out_dim) return false;
    return switch (self.raw_linear_slot_kinds[slot]) {
        .dense => true,
        .quantized => blk: {
            const kind = ensureQuantizedRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
            if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(kind)) break :blk false;
            const format = metalQuantFormatForKind(kind);
            if (format == .unsupported) break :blk false;
            if (kind == .tl1 or kind == .tl2) break :blk true;
            const storage = self.raw_linear_slot_quantized_storage[slot] orelse break :blk false;
            const descriptor = packedWeightDescriptorForMatrix(storage, in_dim, out_dim, format) orelse break :blk false;
            break :blk descriptor.supported();
        },
        .none => false,
    };
}

fn tracePleResidualRuntimeEnabled() bool {
    const raw = std.c.getenv("TERMITE_METAL_TRACE_GRAPH_FUSIONS") orelse return false;
    const value = std.mem.span(raw);
    return value.len != 0 and !std.mem.eql(u8, value, "0");
}

fn tracePleResidualRuntimeSlot(self: anytype, label: []const u8, slot: usize, in_dim: usize, out_dim: usize) void {
    if (!tracePleResidualRuntimeEnabled()) return;
    const prepared = slot < decoder_runtime_linear_slot_capacity and self.raw_linear_slots_prepared[slot];
    const kind = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_kinds[slot] else .none;
    const runtime_kind = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_runtime_prepared_kind[slot] else .none;
    const raw_in = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_in_dims[slot] else 0;
    const raw_out = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_out_dims[slot] else 0;
    const quant_kind = if (slot < decoder_runtime_linear_slot_capacity and self.raw_linear_slot_quantized_storage[slot] != null)
        quantizedRuntimeLinearKind(self.raw_linear_slot_quantized_storage[slot].?)
    else
        .none;
    const device_kernel = quantizedRuntimeLinearKindHasSingleStageDeviceKernel(quant_kind);
    const format = metalQuantFormatForKind(quant_kind);
    const descriptor_supported = blk: {
        if (slot >= decoder_runtime_linear_slot_capacity) break :blk false;
        const storage = self.raw_linear_slot_quantized_storage[slot] orelse break :blk kind == .dense;
        const descriptor = packedWeightDescriptorForMatrix(storage, in_dim, out_dim, format) orelse break :blk false;
        break :blk descriptor.supported();
    };
    std.debug.print(
        "metal_graph_fusion_trace: ple_residual runtime_slot label={s} slot={d} requested={d}x{d} prepared={} kind={s} raw_dims={d}x{d} quant_kind={s} runtime_kind={s} device_kernel={} format={s} descriptor_supported={}\n",
        .{ label, slot, in_dim, out_dim, prepared, @tagName(kind), raw_in, raw_out, @tagName(quant_kind), @tagName(runtime_kind), device_kernel, @tagName(format), descriptor_supported },
    );
}

pub fn decoderRuntimeApplyPleResidualDevice(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=no_runtime\n", .{});
        return null;
    };
    if (termite_metal_decode_runtime_ready(runtime) == 0) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=runtime_not_ready\n", .{});
        return null;
    }
    if (request.hidden_size == 0 or request.ple_hidden_size == 0) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=empty_dims hidden={d} ple={d}\n", .{ request.hidden_size, request.ple_hidden_size });
        return null;
    }
    if (request.hidden.ndim() != 2 or request.ple.ndim() != 2) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=rank hidden_rank={d} ple_rank={d}\n", .{ request.hidden.ndim(), request.ple.ndim() });
        return null;
    }
    const rows: usize = @intCast(request.hidden.dim(0));
    if (rows == 0 or
        @as(usize, @intCast(request.hidden.dim(1))) != request.hidden_size or
        @as(usize, @intCast(request.ple.dim(0))) != rows or
        @as(usize, @intCast(request.ple.dim(1))) != request.ple_hidden_size)
    {
        if (tracePleResidualRuntimeEnabled()) std.debug.print(
            "metal_graph_fusion_trace: ple_residual runtime_early reason=shape rows={d} hidden_dim={d} ple_rows={d} ple_dim={d} expected_hidden={d} expected_ple={d}\n",
            .{ rows, @as(usize, @intCast(request.hidden.dim(1))), @as(usize, @intCast(request.ple.dim(0))), @as(usize, @intCast(request.ple.dim(1))), request.hidden_size, request.ple_hidden_size },
        );
        return null;
    }
    if (!request.hidden.isDevice() or !request.ple.isDevice()) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=not_device hidden={} ple={}\n", .{ request.hidden.isDevice(), request.ple.isDevice() });
        return null;
    }
    if (request.post_norm_slot >= decoder_runtime_rms_norm_slot_capacity) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=post_norm_range slot={d}\n", .{request.post_norm_slot});
        return null;
    }
    if (!self.raw_rms_norm_slots_prepared[request.post_norm_slot]) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=post_norm_unprepared slot={d}\n", .{request.post_norm_slot});
        return null;
    }
    if (self.raw_rms_norm_slot_hidden_sizes[request.post_norm_slot] != request.hidden_size) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_early reason=post_norm_dim slot={d} got={d} expected={d}\n", .{ request.post_norm_slot, self.raw_rms_norm_slot_hidden_sizes[request.post_norm_slot], request.hidden_size });
        return null;
    }

    if (ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.ple_hidden_size) == .q8_0 and
        ensureQuantizedRuntimeLinearSlotPrepared(self, request.proj_linear_slot, request.ple_hidden_size, request.hidden_size) == .q8_0)
    {
        const out_shape = [_]i32{ @intCast(rows), @intCast(request.hidden_size) };
        var output = try MetalTensor.deviceAllocate(
            @ptrCast(runtime),
            rows * request.hidden_size * @sizeOf(f32),
            .private,
            &out_shape,
        );
        errdefer output.deinit();
        const rc = termite_metal_decode_runtime_apply_ple_residual_q8_0_device(
            runtime,
            request.hidden.deviceHandle(),
            request.hidden.deviceByteOffset(),
            request.ple.deviceHandle(),
            request.ple.deviceByteOffset(),
            request.gate_linear_slot,
            request.proj_linear_slot,
            request.post_norm_slot,
            rows,
            request.hidden_size,
            request.ple_hidden_size,
            request.eps,
            @intFromEnum(request.activation),
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (rc == 0) return output;
    }

    if (!runtimeLinearSlotSupportsDeviceApply(self, request.gate_linear_slot, request.hidden_size, request.ple_hidden_size) or
        !runtimeLinearSlotSupportsDeviceApply(self, request.proj_linear_slot, request.ple_hidden_size, request.hidden_size))
    {
        tracePleResidualRuntimeSlot(self, "gate", request.gate_linear_slot, request.hidden_size, request.ple_hidden_size);
        tracePleResidualRuntimeSlot(self, "proj", request.proj_linear_slot, request.ple_hidden_size, request.hidden_size);
        return null;
    }

    var stats = struct {
        decoder_runtime_apply_activation_calls: u64 = 0,
        decoder_runtime_apply_layer_norm_calls: u64 = 0,
        decoder_runtime_apply_add_calls: u64 = 0,
    }{};
    var gate = (try decoderRuntimeApplyLinear(self, .{
        .slot = request.gate_linear_slot,
        .input = request.hidden,
        .in_dim = request.hidden_size,
        .out_dim = request.ple_hidden_size,
    })) orelse {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=gate null\n", .{});
        return null;
    };
    defer gate.deinit();
    if (!gate.isDevice()) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=gate device=false\n", .{});
        return null;
    }

    var activated = (try decoderRuntimeApplyActivation(self, .{
        .input = gate,
        .kind = request.activation,
        .dim = request.ple_hidden_size,
    }, &stats)) orelse {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=activation null\n", .{});
        return null;
    };
    defer activated.deinit();
    if (!activated.isDevice()) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=activation device=false\n", .{});
        return null;
    }

    var gated = (try decoderRuntimeApplyMultiply(self, activated, request.ple, request.ple_hidden_size)) orelse {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=multiply null\n", .{});
        return null;
    };
    defer gated.deinit();
    if (!gated.isDevice()) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=multiply device=false\n", .{});
        return null;
    }

    var projected = (try decoderRuntimeApplyLinear(self, .{
        .slot = request.proj_linear_slot,
        .input = gated,
        .in_dim = request.ple_hidden_size,
        .out_dim = request.hidden_size,
    })) orelse {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=projection null\n", .{});
        return null;
    };
    defer projected.deinit();
    if (!projected.isDevice()) {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=projection device=false\n", .{});
        return null;
    }

    return (try decoderRuntimeApplyRmsNormAdd(self, .{
        .input = projected,
        .norm_slot = request.post_norm_slot,
        .residual = request.hidden,
        .rows = rows,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
    }, &stats)) orelse {
        if (tracePleResidualRuntimeEnabled()) std.debug.print("metal_graph_fusion_trace: ple_residual runtime_stage label=post_norm_add null\n", .{});
        return null;
    };
}

pub fn decoderRuntimeApplyAttentionOutputResidualDevice(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.rows == 0 or request.attention_input_size == 0 or request.hidden_size == 0) return null;
    if (request.attention_output.ndim() != 2 or request.residual.ndim() != 2) return null;
    if (@as(usize, @intCast(request.attention_output.dim(0))) != request.rows or
        @as(usize, @intCast(request.attention_output.dim(1))) != request.attention_input_size or
        @as(usize, @intCast(request.residual.dim(0))) != request.rows or
        @as(usize, @intCast(request.residual.dim(1))) != request.hidden_size)
    {
        return null;
    }
    if (!request.attention_output.isDevice() or !request.residual.isDevice()) return null;
    if (request.attention_output.deviceByteOffset() != 0) return null;
    const linear_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.linear_slot, request.attention_input_size, request.hidden_size);
    const linear_format = metalQuantFormatForKind(linear_kind);
    if (linear_format == .unsupported) return null;
    if (request.pre_linear_rms_norm_slot) |slot| {
        if (slot >= decoder_runtime_rms_norm_slot_capacity) return null;
        if (!self.raw_rms_norm_slots_prepared[slot]) return null;
        if (self.raw_rms_norm_slot_hidden_sizes[slot] != request.attention_input_size) return null;
    }
    if (request.post_linear_rms_norm_slot) |slot| {
        if (slot >= decoder_runtime_rms_norm_slot_capacity) return null;
        if (!self.raw_rms_norm_slots_prepared[slot]) return null;
        if (self.raw_rms_norm_slot_hidden_sizes[slot] != request.hidden_size) return null;
    }

    const out_shape = [_]i32{ @intCast(request.rows), @intCast(request.hidden_size) };
    var output = try MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        request.rows * request.hidden_size * @sizeOf(f32),
        .private,
        &out_shape,
    );
    errdefer output.deinit();
    const none = std.math.maxInt(usize);
    const rc = termite_metal_decode_runtime_apply_attention_output_residual_device(
        runtime,
        @intFromEnum(linear_format),
        request.attention_output.deviceHandle(),
        request.attention_output.deviceByteOffset(),
        request.residual.deviceHandle(),
        request.residual.deviceByteOffset(),
        request.rows,
        request.attention_input_size,
        request.hidden_size,
        request.linear_slot,
        request.pre_linear_rms_norm_slot orelse none,
        request.post_linear_rms_norm_slot orelse none,
        request.eps,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output;
}

pub fn decoderRuntimeReadTokenId(self: anytype) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    var token_id: u32 = 0;
    const rc = termite_metal_decode_runtime_read_token_id(runtime, &token_id);
    if (rc != 0) return null;
    return token_id;
}

fn argmaxSingleRowLogits(self: anytype, logits: MetalTensor, out_dim: usize) !?usize {
    if (out_dim == 0) return null;
    if (logits.ndim() != 2) return null;
    if (@as(usize, @intCast(logits.dim(0))) != 1) return null;
    if (@as(usize, @intCast(logits.dim(1))) != out_dim) return null;
    if (logits.isDevice()) {
        if (try argmaxLogitsDevice(self, logits, out_dim)) |token_id| return token_id;
    }
    var logits_mut = logits;
    const host = try tensorHostSlice(&logits_mut);
    if (host.len != out_dim) return null;
    var best_idx: usize = 0;
    var best_val = host[0];
    for (host[1..], 1..) |value, idx| {
        if (value > best_val) {
            best_val = value;
            best_idx = idx;
        }
    }
    return best_idx;
}

pub fn decoderRuntimeApplyLinearArgmax(self: anytype, request: anytype) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.in_dim == 0 or request.out_dim == 0) return null;
    if (request.slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_linear_slots_prepared[request.slot]) return null;
    if (self.raw_linear_slot_kinds[request.slot] != .dense) return null;
    if (self.raw_linear_slot_in_dims[request.slot] != request.in_dim) return null;
    if (self.raw_linear_slot_out_dims[request.slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.in_dim) return null;

    if (request.input.isDevice()) {
        var token_id_device: u32 = 0;
        const device_rc = termite_metal_decode_runtime_apply_linear_argmax_device(
            runtime,
            request.slot,
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            request.in_dim,
            request.out_dim,
            &token_id_device,
        );
        if (device_rc == 0) return token_id_device;
    }

    var token_id: u32 = 0;
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_linear_argmax(
        runtime,
        request.slot,
        try tensorHostConstPtr(&input),
        request.in_dim,
        request.out_dim,
        &token_id,
    );
    if (rc != 0) return null;
    return token_id;
}

pub fn decoderRuntimeApplyRmsNormLinearSample(self: anytype, request: anytype) !?usize {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.out_dim == 0) return null;
    if (request.norm_slot >= decoder_runtime_rms_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_rms_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_rms_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return null;
    if (self.raw_linear_slot_kinds[request.linear_slot] != .dense) return null;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return null;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;

    const bounded_top_p = request.top_p <= 0.0 or request.top_p >= 1.0 or
        request.top_k > 0 or request.out_dim <= 256;
    if (bounded_top_p) {
        if (decoderRuntimeReserveSampleTailScratch(self, request.out_dim, request.top_k)) {
            var penalty_entries = try buildSamplePenaltyEntries(std.heap.c_allocator, request.token_history);
            defer penalty_entries.deinit(std.heap.c_allocator);
            var input = request.input;
            const input_host = try tensorHostSlice(&input);
            var token_id: u32 = 0;
            const seed = makeSampleSeed(&input_host[0]);
            const device_rc = termite_metal_decode_runtime_apply_rms_norm_linear_sample_device(
                runtime,
                request.norm_slot,
                request.linear_slot,
                input_host.ptr,
                request.hidden_size,
                request.eps,
                request.out_dim,
                request.temperature,
                request.top_k,
                request.top_p,
                request.min_p,
                request.repetition_penalty,
                request.frequency_penalty,
                request.presence_penalty,
                if (penalty_entries.token_ids.len > 0) &penalty_entries.token_ids[0] else null,
                if (penalty_entries.counts.len > 0) &penalty_entries.counts[0] else null,
                penalty_entries.token_ids.len,
                seed,
                &token_id,
            );
            if (device_rc == 0) return token_id;
        }
    }

    const logits_host = try std.heap.c_allocator.alloc(f32, request.out_dim);
    defer std.heap.c_allocator.free(logits_host);
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_rms_norm_linear(
        runtime,
        request.norm_slot,
        request.linear_slot,
        try tensorHostConstPtr(&input),
        request.hidden_size,
        request.eps,
        request.out_dim,
        logits_host.ptr,
    );
    if (rc != 0) return null;
    return sampleLogits(logits_host, request);
}

pub fn decoderRuntimeApplyActivation(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.dim == 0) return null;
    if (request.input.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.input.dim(0)));
    if (rows == 0) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.dim) return null;

    if (request.input.isDevice()) {
        const elem_count = rows * request.dim;
        const shape = [_]i32{ @intCast(rows), @intCast(request.dim) };
        var output_device = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, &shape);
        errdefer output_device.deinit();
        stats.decoder_runtime_apply_activation_calls += 1;
        const device_rc = termite_metal_decode_runtime_apply_activation_device(
            runtime,
            @intFromEnum(request.kind),
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            rows,
            request.dim,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }

    if (rows != 1) {
        var input = request.input;
        const input_slice = try tensorHostSlice(&input);
        const input_host = try std.heap.c_allocator.alloc(f32, input_slice.len);
        errdefer std.heap.c_allocator.free(input_host);
        @memcpy(input_host, input_slice);
        applyActivationHost(input_host, request.kind);
        const shape = [_]i32{ @intCast(rows), @intCast(request.dim) };
        return MetalTensor.owned(input_host, &shape);
    }

    const output = try std.heap.c_allocator.alloc(f32, request.dim);
    errdefer std.heap.c_allocator.free(output);
    stats.decoder_runtime_apply_activation_calls += 1;
    var input = request.input;
    const rc = termite_metal_decode_runtime_apply_activation(
        runtime,
        @intFromEnum(request.kind),
        try tensorHostConstPtr(&input),
        request.dim,
        output.ptr,
    );
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(request.dim) };
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeApplyPrimitiveUnary(self: anytype, input: MetalTensor, activation_kind: u32) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    const dim = input.elemCount();
    if (dim == 0) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, dim * @sizeOf(f32), .private, input.shape());
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_activation_device(
        runtime,
        activation_kind,
        input.deviceHandle(),
        input.deviceByteOffset(),
        1,
        dim,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeApplySoftmaxDevice(self: anytype, input: MetalTensor, rows: usize, dim: usize, log_softmax: bool) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (rows == 0 or dim == 0) return null;
    if (rows > std.math.maxInt(usize) / dim) return null;
    if (input.elemCount() != rows * dim) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, rows * dim * @sizeOf(f32), .private, input.shape());
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_softmax_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        dim,
        @intFromBool(log_softmax),
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeReduceLastDimDevice(self: anytype, input: MetalTensor, rows: usize, dim: usize, kind: u32, output_shape: []const i32) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (rows == 0 or dim == 0 or kind > 2) return null;
    if (rows > std.math.maxInt(usize) / dim) return null;
    if (input.elemCount() != rows * dim) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, rows * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_reduce_last_dim_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        dim,
        kind,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeMultiplyReduceLastDimDevice(self: anytype, lhs: MetalTensor, rhs: MetalTensor, rows: usize, dim: usize, output_shape: []const i32) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!lhs.isDevice() or !rhs.isDevice()) return null;
    if (rows == 0 or dim == 0) return null;
    if (rows > std.math.maxInt(usize) / dim) return null;
    if (lhs.elemCount() != rows * dim or rhs.elemCount() != rows * dim) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, rows * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_multiply_reduce_last_dim_device(
        runtime,
        lhs.deviceHandle(),
        lhs.deviceByteOffset(),
        rhs.deviceHandle(),
        rhs.deviceByteOffset(),
        rows,
        dim,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeReduceAxisF32Device(
    self: anytype,
    input: MetalTensor,
    out_strides: []const u32,
    input_strides_for_out: []const u32,
    rank: usize,
    input_elems: usize,
    output_elems: usize,
    reduce_dim: usize,
    reduce_stride: usize,
    kind: u32,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (rank == 0 or rank > metal_tensor.max_dims or out_strides.len < rank or input_strides_for_out.len < rank) return null;
    if (input_elems == 0 or output_elems == 0 or reduce_dim == 0 or reduce_stride == 0 or kind > 2) return null;
    if (input.elemCount() != input_elems) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, output_elems * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_reduce_axis_f32_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        out_strides.ptr,
        input_strides_for_out.ptr,
        rank,
        input_elems,
        output_elems,
        reduce_dim,
        reduce_stride,
        kind,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeBroadcastLastDimDevice(self: anytype, input: MetalTensor, rows: usize, in_dim: usize, out_dim: usize, output_shape: []const i32) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (rows == 0 or in_dim == 0 or out_dim == 0) return null;
    if (in_dim != 1 and in_dim != out_dim) return null;
    if (rows > std.math.maxInt(usize) / in_dim or rows > std.math.maxInt(usize) / out_dim) return null;
    if (input.elemCount() != rows * in_dim) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_broadcast_last_dim_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        in_dim,
        out_dim,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeBroadcastF32Device(
    self: anytype,
    input: MetalTensor,
    out_strides: []const u32,
    input_strides_for_out: []const u32,
    rank: usize,
    input_elems: usize,
    output_elems: usize,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (rank == 0 or rank > metal_tensor.max_dims or out_strides.len < rank or input_strides_for_out.len < rank) return null;
    if (input_elems == 0 or output_elems == 0) return null;
    if (input.elemCount() != input_elems) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, output_elems * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_broadcast_f32_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        out_strides.ptr,
        input_strides_for_out.ptr,
        rank,
        input_elems,
        output_elems,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeGatherAxis0F32_2DDevice(
    self: anytype,
    input: MetalTensor,
    indices: MetalTensor,
    rows: usize,
    cols: usize,
    index_count: usize,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or !indices.isDevice()) return null;
    if (rows == 0 or cols == 0 or index_count == 0) return null;
    if (rows > std.math.maxInt(usize) / cols or index_count > std.math.maxInt(usize) / cols) return null;
    if (input.elemCount() != rows * cols or indices.elemCount() != index_count) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, index_count * cols * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_gather_axis0_f32_2d_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        indices.deviceHandle(),
        indices.deviceByteOffset(),
        rows,
        cols,
        index_count,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeGlinerWordEmbeddingsF32Device(
    self: anytype,
    hidden: MetalTensor,
    words_mask: MetalTensor,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
    num_words: usize,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!hidden.isDevice() or !words_mask.isDevice()) return null;
    if (batch == 0 or seq_len == 0 or hidden_size == 0 or num_words == 0) return null;
    const token_count = try std.math.mul(usize, batch, seq_len);
    const hidden_elems = try std.math.mul(usize, token_count, hidden_size);
    const output_rows = try std.math.mul(usize, batch, num_words);
    const output_elems = try std.math.mul(usize, output_rows, hidden_size);
    if (hidden.elemCount() != hidden_elems or words_mask.elemCount() != token_count) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, output_elems * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_gliner_word_embeddings_f32_device(
        runtime,
        hidden.deviceHandle(),
        hidden.deviceByteOffset(),
        words_mask.deviceHandle(),
        words_mask.deviceByteOffset(),
        batch,
        seq_len,
        hidden_size,
        num_words,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeConcatLastDimF32_2DDevice(
    self: anytype,
    a: MetalTensor,
    b: MetalTensor,
    rows: usize,
    dim_a: usize,
    dim_b: usize,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!a.isDevice() or !b.isDevice()) return null;
    if (rows == 0 or dim_a == 0 or dim_b == 0) return null;
    if (a.elemCount() != try std.math.mul(usize, rows, dim_a)) return null;
    if (b.elemCount() != try std.math.mul(usize, rows, dim_b)) return null;
    const out_dim = try std.math.add(usize, dim_a, dim_b);
    const output_elems = try std.math.mul(usize, rows, out_dim);
    var output_device = try MetalTensor.deviceAllocate(runtime, output_elems * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_concat_lastdim_f32_2d_device(
        runtime,
        a.deviceHandle(),
        a.deviceByteOffset(),
        b.deviceHandle(),
        b.deviceByteOffset(),
        rows,
        dim_a,
        dim_b,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeGlinerGruCombineF32Device(
    self: anytype,
    label_embeddings: MetalTensor,
    gi: MetalTensor,
    gh: MetalTensor,
    rows: usize,
    dim: usize,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!label_embeddings.isDevice() or !gi.isDevice() or !gh.isDevice()) return null;
    if (rows == 0 or dim == 0) return null;
    const count = try std.math.mul(usize, rows, dim);
    const gate_count = try std.math.mul(usize, count, 3);
    if (label_embeddings.elemCount() != count or gi.elemCount() != gate_count or gh.elemCount() != gate_count) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, count * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_gliner_gru_combine_f32_device(
        runtime,
        label_embeddings.deviceHandle(),
        label_embeddings.deviceByteOffset(),
        gi.deviceHandle(),
        gi.deviceByteOffset(),
        gh.deviceHandle(),
        gh.deviceByteOffset(),
        rows,
        dim,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeArgmaxAxisF32Device(
    self: anytype,
    input: MetalTensor,
    outer: usize,
    axis_dim: usize,
    inner: usize,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (outer == 0 or axis_dim == 0 or inner == 0) return null;
    if (outer > std.math.maxInt(usize) / axis_dim or outer * axis_dim > std.math.maxInt(usize) / inner or outer > std.math.maxInt(usize) / inner) return null;
    if (input.elemCount() != outer * axis_dim * inner) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, outer * inner * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_argmax_axis_f32_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        outer,
        axis_dim,
        inner,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeConvertDTypeF32Device(self: anytype, input: MetalTensor, kind: u32) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (input.elemCount() == 0 or kind > 2) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, input.elemCount() * @sizeOf(f32), .private, input.shape());
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_convert_dtype_f32_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        input.elemCount(),
        kind,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeSdpaF32Device(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!request.q.isDevice() or !request.k.isDevice() or !request.v.isDevice()) return null;
    if (request.batch == 0 or request.seq_len == 0 or request.num_heads == 0 or request.head_dim == 0) return null;
    const total = request.batch * request.num_heads * request.seq_len * request.head_dim;
    if (request.q.elemCount() != total or request.k.elemCount() != total or request.v.elemCount() != total) return null;
    const bias_tensor: ?MetalTensor = if (@hasField(@TypeOf(request), "bias")) request.bias else null;
    const mask_tensor: ?MetalTensor = if (@hasField(@TypeOf(request), "mask")) request.mask else null;
    const bias_mode: u32 = if (@hasField(@TypeOf(request), "bias_mode")) request.bias_mode else 0;
    if (bias_mode > 3) return null;
    if (bias_mode != 0 and (bias_tensor == null or !bias_tensor.?.isDevice())) return null;
    if (mask_tensor) |mask| {
        if (!mask.isDevice() or mask.elemCount() != request.batch * request.seq_len) return null;
    }

    var output_device = try MetalTensor.deviceAllocate(runtime, total * @sizeOf(f32), .private, request.q.shape());
    errdefer output_device.deinit();
    var bias_mut: ?MetalTensor = if (bias_tensor) |tensor| tensor else null;
    var mask_mut: ?MetalTensor = if (mask_tensor) |tensor| tensor else null;
    const rc = termite_metal_decode_runtime_sdpa_f32_device(
        runtime,
        request.q.deviceHandle(),
        request.q.deviceByteOffset(),
        request.k.deviceHandle(),
        request.k.deviceByteOffset(),
        request.v.deviceHandle(),
        request.v.deviceByteOffset(),
        if (bias_mut) |*tensor| tensor.deviceHandle() else null,
        if (bias_mut) |*tensor| tensor.deviceByteOffset() else 0,
        if (mask_mut) |*tensor| tensor.deviceHandle() else null,
        if (mask_mut) |*tensor| tensor.deviceByteOffset() else 0,
        request.batch,
        request.seq_len,
        request.num_heads,
        request.head_dim,
        bias_mode,
        if (mask_tensor != null) 1 else 0,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeDisentangledRelativeAttentionF32Device(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!request.q.isDevice() or !request.k.isDevice() or !request.v.isDevice() or !request.q_r.isDevice() or !request.k_r.isDevice()) return null;
    if (request.batch == 0 or request.seq_len == 0 or request.num_heads == 0 or request.head_dim == 0) return null;
    if (request.seq_len > std.math.maxInt(usize) / 2) return null;
    const hidden = request.num_heads * request.head_dim;
    const total = request.batch * request.seq_len * hidden;
    const rel_total = (request.seq_len * 2 - 1) * hidden;
    if (request.q.elemCount() != total or request.k.elemCount() != total or request.v.elemCount() != total) return null;
    if (request.q_r.elemCount() < rel_total or request.k_r.elemCount() < rel_total) return null;
    const mask_tensor: ?MetalTensor = if (@hasField(@TypeOf(request), "mask")) request.mask else null;
    if (mask_tensor) |mask| {
        if (!mask.isDevice() or mask.elemCount() != request.batch * request.seq_len) return null;
    }

    const output_shape = [_]i32{ @intCast(request.batch * request.seq_len), @intCast(hidden) };
    var output_device = try MetalTensor.deviceAllocate(runtime, total * @sizeOf(f32), .private, &output_shape);
    errdefer output_device.deinit();
    var mask_mut: ?MetalTensor = if (mask_tensor) |tensor| tensor else null;
    const rc = termite_metal_decode_runtime_disentangled_relative_attention_f32_device(
        runtime,
        request.q.deviceHandle(),
        request.q.deviceByteOffset(),
        request.k.deviceHandle(),
        request.k.deviceByteOffset(),
        request.v.deviceHandle(),
        request.v.deviceByteOffset(),
        request.q_r.deviceHandle(),
        request.q_r.deviceByteOffset(),
        request.k_r.deviceHandle(),
        request.k_r.deviceByteOffset(),
        if (mask_mut) |*tensor| tensor.deviceHandle() else null,
        if (mask_mut) |*tensor| tensor.deviceByteOffset() else 0,
        request.batch,
        request.seq_len,
        request.num_heads,
        request.head_dim,
        if (mask_tensor != null) 1 else 0,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeTransposeF32Device(
    self: anytype,
    input: MetalTensor,
    input_shape: []const i64,
    perm_u8: []const u8,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    const rank = input_shape.len;
    if (rank == 0 or rank > 8 or perm_u8.len != rank or output_shape.len != rank) return null;
    var dims: [8]u32 = [_]u32{1} ** 8;
    var in_strides: [8]u32 = [_]u32{0} ** 8;
    var out_strides: [8]u32 = [_]u32{0} ** 8;
    var perm: [8]u32 = [_]u32{0} ** 8;
    var seen: [8]bool = [_]bool{false} ** 8;

    var total: usize = 1;
    for (input_shape, 0..) |dim_i64, idx| {
        if (dim_i64 <= 0) return null;
        const dim: usize = std.math.cast(usize, dim_i64) orelse return null;
        if (dim > std.math.maxInt(u32)) return null;
        if (total > std.math.maxInt(usize) / dim) return null;
        total *= dim;
        dims[idx] = @intCast(dim);
    }
    if (total == 0 or input.elemCount() != total) return null;

    var stride: usize = 1;
    var axis = rank;
    while (axis > 0) {
        axis -= 1;
        if (stride > std.math.maxInt(u32)) return null;
        in_strides[axis] = @intCast(stride);
        const dim: usize = @intCast(dims[axis]);
        if (axis != 0 and stride > std.math.maxInt(usize) / dim) return null;
        stride *= dim;
    }

    stride = 1;
    axis = rank;
    while (axis > 0) {
        axis -= 1;
        if (output_shape[axis] <= 0) return null;
        if (stride > std.math.maxInt(u32)) return null;
        out_strides[axis] = @intCast(stride);
        const dim: usize = @intCast(output_shape[axis]);
        if (axis != 0 and stride > std.math.maxInt(usize) / dim) return null;
        stride *= dim;
    }
    if (stride != total) return null;

    for (perm_u8, 0..) |p, idx| {
        if (p >= rank or seen[p]) return null;
        seen[p] = true;
        perm[idx] = p;
        if (output_shape[idx] != @as(i32, @intCast(dims[p]))) return null;
    }

    var output_device = try MetalTensor.deviceAllocate(runtime, total * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_transpose_f32_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        &dims,
        &in_strides,
        &out_strides,
        &perm,
        rank,
        total,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeDotGeneral2DF32Device(
    self: anytype,
    lhs: MetalTensor,
    rhs: MetalTensor,
    m: usize,
    n: usize,
    k: usize,
    rhs_contract_axis: u32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!lhs.isDevice() or !rhs.isDevice()) return null;
    if (m == 0 or n == 0 or k == 0 or rhs_contract_axis > 1) return null;
    if (m > std.math.maxInt(usize) / k or m > std.math.maxInt(usize) / n or n > std.math.maxInt(usize) / k) return null;
    if (lhs.elemCount() != m * k or rhs.elemCount() != n * k) return null;
    const out_shape = [_]i32{ @intCast(m), @intCast(n) };
    var output_device = try MetalTensor.deviceAllocate(runtime, m * n * @sizeOf(f32), .private, &out_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_dot_general_2d_f32_device(
        runtime,
        lhs.deviceHandle(),
        lhs.deviceByteOffset(),
        rhs.deviceHandle(),
        rhs.deviceByteOffset(),
        m,
        n,
        k,
        rhs_contract_axis,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeDotGeneralBatchedF32Device(
    self: anytype,
    lhs: MetalTensor,
    rhs: MetalTensor,
    batch_count: usize,
    m: usize,
    n: usize,
    k: usize,
    rhs_contract_axis: u32,
    output_shape: []const i32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!lhs.isDevice() or !rhs.isDevice()) return null;
    if (batch_count == 0 or m == 0 or n == 0 or k == 0 or rhs_contract_axis > 1 or output_shape.len == 0) return null;
    if (batch_count > std.math.maxInt(usize) / m or batch_count * m > std.math.maxInt(usize) / k or
        batch_count > std.math.maxInt(usize) / n or batch_count * n > std.math.maxInt(usize) / k or
        batch_count * m > std.math.maxInt(usize) / n)
    {
        return null;
    }
    if (lhs.elemCount() != batch_count * m * k or rhs.elemCount() != batch_count * n * k) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, batch_count * m * n * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_dot_general_batched_f32_device(
        runtime,
        lhs.deviceHandle(),
        lhs.deviceByteOffset(),
        rhs.deviceHandle(),
        rhs.deviceByteOffset(),
        batch_count,
        m,
        n,
        k,
        rhs_contract_axis,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeConv1dF32Device(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!request.input.isDevice() or !request.weight.isDevice()) return null;
    if (request.batch == 0 or request.in_channels == 0 or request.out_channels == 0 or request.time_steps == 0 or request.kernel_size == 0 or request.stride == 0) return null;
    if (request.time_steps + 2 * request.padding < request.kernel_size) return null;
    const out_time = (request.time_steps + 2 * request.padding - request.kernel_size) / request.stride + 1;
    if (out_time == 0) return null;
    if (request.input.elemCount() != request.batch * request.in_channels * request.time_steps) return null;
    if (request.weight.elemCount() != request.out_channels * request.in_channels * request.kernel_size) return null;
    const bias_tensor: ?MetalTensor = if (@hasField(@TypeOf(request), "bias")) request.bias else null;
    if (bias_tensor) |bias| {
        if (!bias.isDevice() or bias.elemCount() != request.out_channels) return null;
    }
    const out_shape = [_]i32{ @intCast(request.batch), @intCast(request.out_channels), @intCast(out_time) };
    var output_device = try MetalTensor.deviceAllocate(runtime, request.batch * request.out_channels * out_time * @sizeOf(f32), .private, &out_shape);
    errdefer output_device.deinit();
    var bias_mut: ?MetalTensor = if (bias_tensor) |tensor| tensor else null;
    const rc = termite_metal_decode_runtime_conv1d_f32_device(
        runtime,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.weight.deviceHandle(),
        request.weight.deviceByteOffset(),
        if (bias_mut) |*tensor| tensor.deviceHandle() else null,
        if (bias_mut) |*tensor| tensor.deviceByteOffset() else 0,
        request.batch,
        request.in_channels,
        request.out_channels,
        request.time_steps,
        request.kernel_size,
        request.stride,
        request.padding,
        out_time,
        if (bias_tensor != null) 1 else 0,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeConv2dF32Device(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!request.input.isDevice() or !request.weight.isDevice()) return null;
    if (request.batch == 0 or request.in_channels == 0 or request.out_channels == 0 or request.height == 0 or request.width == 0 or request.kernel_h == 0 or request.kernel_w == 0 or request.stride_h == 0 or request.stride_w == 0 or request.groups == 0) return null;
    if (request.in_channels % request.groups != 0 or request.out_channels % request.groups != 0) return null;
    if (request.height + 2 * request.padding_h < request.kernel_h or request.width + 2 * request.padding_w < request.kernel_w) return null;
    const out_h = (request.height + 2 * request.padding_h - request.kernel_h) / request.stride_h + 1;
    const out_w = (request.width + 2 * request.padding_w - request.kernel_w) / request.stride_w + 1;
    if (out_h == 0 or out_w == 0) return null;
    const ic_per_group = request.in_channels / request.groups;
    if (request.input.elemCount() != request.batch * request.in_channels * request.height * request.width) return null;
    if (request.weight.elemCount() != request.out_channels * ic_per_group * request.kernel_h * request.kernel_w) return null;
    const bias_tensor: ?MetalTensor = if (@hasField(@TypeOf(request), "bias")) request.bias else null;
    if (bias_tensor) |bias| {
        if (!bias.isDevice() or bias.elemCount() != request.out_channels) return null;
    }
    const out_shape = [_]i32{ @intCast(request.batch), @intCast(request.out_channels), @intCast(out_h), @intCast(out_w) };
    var output_device = try MetalTensor.deviceAllocate(runtime, request.batch * request.out_channels * out_h * out_w * @sizeOf(f32), .private, &out_shape);
    errdefer output_device.deinit();
    var bias_mut: ?MetalTensor = if (bias_tensor) |tensor| tensor else null;
    const rc = termite_metal_decode_runtime_conv2d_f32_device(
        runtime,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.weight.deviceHandle(),
        request.weight.deviceByteOffset(),
        if (bias_mut) |*tensor| tensor.deviceHandle() else null,
        if (bias_mut) |*tensor| tensor.deviceByteOffset() else 0,
        request.batch,
        request.in_channels,
        request.out_channels,
        request.height,
        request.width,
        request.kernel_h,
        request.kernel_w,
        request.stride_h,
        request.stride_w,
        request.padding_h,
        request.padding_w,
        request.groups,
        out_h,
        out_w,
        if (bias_tensor != null) 1 else 0,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeApplyAdd(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.dim == 0) return null;
    if (request.lhs.elemCount() == request.rhs.elemCount() and request.lhs.elemCount() > 0 and request.lhs.isDevice() and request.rhs.isDevice()) {
        const elem_count = request.lhs.elemCount();
        var output_device = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, request.lhs.shape());
        errdefer output_device.deinit();
        stats.decoder_runtime_apply_add_calls += 1;
        const device_rc = termite_metal_decode_runtime_apply_add_device(
            runtime,
            request.lhs.deviceHandle(),
            request.lhs.deviceByteOffset(),
            request.rhs.deviceHandle(),
            request.rhs.deviceByteOffset(),
            elem_count,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }
    if (request.lhs.ndim() != 2 or request.rhs.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.lhs.dim(0)));
    if (rows == 0 or @as(usize, @intCast(request.rhs.dim(0))) != rows) return null;
    if (@as(usize, @intCast(request.lhs.dim(1))) != request.dim or @as(usize, @intCast(request.rhs.dim(1))) != request.dim) return null;
    if (rows != 1) {
        const out = try std.heap.c_allocator.alloc(f32, request.lhs.elemCount());
        errdefer std.heap.c_allocator.free(out);
        var lhs = request.lhs;
        var rhs = request.rhs;
        const lhs_slice = try tensorHostSlice(&lhs);
        const rhs_slice = try tensorHostSlice(&rhs);
        if (lhs_slice.len == 0 or rhs_slice.len == 0) return null;
        for (out, 0..) |*o, i| {
            o.* = lhs_slice[i % lhs_slice.len] + rhs_slice[i % rhs_slice.len];
        }
        const shape = [_]i32{ @intCast(rows), @intCast(request.dim) };
        return MetalTensor.owned(out, &shape);
    }

    if (request.lhs.isDevice() and request.rhs.isDevice()) {
        const shape = [_]i32{ 1, @intCast(request.dim) };
        var output_device = try MetalTensor.deviceAllocate(runtime, request.dim * @sizeOf(f32), .private, &shape);
        errdefer output_device.deinit();
        stats.decoder_runtime_apply_add_calls += 1;
        const device_rc = termite_metal_decode_runtime_apply_add_device(
            runtime,
            request.lhs.deviceHandle(),
            request.lhs.deviceByteOffset(),
            request.rhs.deviceHandle(),
            request.rhs.deviceByteOffset(),
            request.dim,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }

    const output = try std.heap.c_allocator.alloc(f32, request.dim);
    errdefer std.heap.c_allocator.free(output);
    stats.decoder_runtime_apply_add_calls += 1;
    var lhs = request.lhs;
    var rhs = request.rhs;
    const rc = termite_metal_decode_runtime_apply_add(
        runtime,
        try tensorHostConstPtr(&lhs),
        try tensorHostConstPtr(&rhs),
        request.dim,
        output.ptr,
    );
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(request.dim) };
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeApplyAddScale(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.dim == 0) return null;
    if (std.math.approxEqAbs(f32, request.scale, 1.0, 1e-6)) {
        return decoderRuntimeApplyAdd(self, .{
            .lhs = request.lhs,
            .rhs = request.rhs,
            .dim = request.dim,
        }, stats);
    }
    if (request.lhs.elemCount() == request.rhs.elemCount() and request.lhs.elemCount() > 0 and request.lhs.isDevice() and request.rhs.isDevice()) {
        const elem_count = request.lhs.elemCount();
        var output_device = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, request.lhs.shape());
        errdefer output_device.deinit();
        stats.decoder_runtime_apply_add_calls += 1;
        const device_rc = termite_metal_decode_runtime_apply_add_scale_device(
            runtime,
            request.lhs.deviceHandle(),
            request.lhs.deviceByteOffset(),
            request.rhs.deviceHandle(),
            request.rhs.deviceByteOffset(),
            elem_count,
            request.scale,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }
    var added = (try decoderRuntimeApplyAdd(self, .{
        .lhs = request.lhs,
        .rhs = request.rhs,
        .dim = request.dim,
    }, stats)) orelse return null;
    defer added.deinit();
    return decoderRuntimeApplyScale(self, added, request.scale);
}

pub fn decoderRuntimeApplyScaledAddScale(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.dim == 0) return null;
    if (request.lhs.elemCount() == request.rhs.elemCount() and request.lhs.elemCount() > 0 and request.lhs.isDevice() and request.rhs.isDevice()) {
        const elem_count = request.lhs.elemCount();
        var output_device = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, request.lhs.shape());
        errdefer output_device.deinit();
        stats.decoder_runtime_apply_add_calls += 1;
        const device_rc = termite_metal_decode_runtime_apply_scaled_add_scale_device(
            runtime,
            request.lhs.deviceHandle(),
            request.lhs.deviceByteOffset(),
            request.rhs.deviceHandle(),
            request.rhs.deviceByteOffset(),
            elem_count,
            request.lhs_scale,
            request.output_scale,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }

    var scaled_lhs = (try decoderRuntimeApplyScale(self, request.lhs, request.lhs_scale)) orelse return null;
    defer scaled_lhs.deinit();
    return decoderRuntimeApplyAddScale(self, .{
        .lhs = scaled_lhs,
        .rhs = request.rhs,
        .dim = request.dim,
        .scale = request.output_scale,
    }, stats);
}

pub fn decoderRuntimeApplyRmsNormAdd(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.rows == 0 or request.hidden_size == 0) return null;
    if (request.norm_slot >= decoder_runtime_rms_norm_slot_capacity) return null;
    if (!self.raw_rms_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_rms_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (request.input.ndim() != 2 or request.residual.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != request.rows or
        @as(usize, @intCast(request.input.dim(1))) != request.hidden_size or
        @as(usize, @intCast(request.residual.dim(0))) != request.rows or
        @as(usize, @intCast(request.residual.dim(1))) != request.hidden_size)
    {
        return null;
    }
    if (request.input.isDevice() and request.residual.isDevice()) {
        const shape = [_]i32{ @intCast(request.rows), @intCast(request.hidden_size) };
        var output_device = try MetalTensor.deviceAllocate(runtime, request.rows * request.hidden_size * @sizeOf(f32), .private, &shape);
        errdefer output_device.deinit();
        stats.decoder_runtime_apply_layer_norm_calls += 1;
        stats.decoder_runtime_apply_add_calls += 1;
        const device_rc = termite_metal_decode_runtime_apply_rms_norm_add_device(
            runtime,
            request.input.deviceHandle(),
            request.input.deviceByteOffset(),
            request.norm_slot,
            request.residual.deviceHandle(),
            request.residual.deviceByteOffset(),
            request.rows,
            request.hidden_size,
            request.eps,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }

    var normed = (try decoderRuntimeApplyRmsNorm(self, .{
        .slot = request.norm_slot,
        .input = request.input,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
    }, stats)) orelse return null;
    defer normed.deinit();
    return decoderRuntimeApplyAdd(self, .{
        .lhs = request.residual,
        .rhs = normed,
        .dim = request.hidden_size,
    }, stats);
}

pub fn decoderRuntimeApplyMultiply(
    self: anytype,
    lhs: MetalTensor,
    rhs: MetalTensor,
    dim: usize,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (dim == 0) return null;
    if (lhs.elemCount() == rhs.elemCount() and lhs.elemCount() > 0 and lhs.isDevice() and rhs.isDevice()) {
        const elem_count = lhs.elemCount();
        var output_device = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, lhs.shape());
        errdefer output_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_multiply_device(
            runtime,
            lhs.deviceHandle(),
            lhs.deviceByteOffset(),
            rhs.deviceHandle(),
            rhs.deviceByteOffset(),
            elem_count,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }
    if (lhs.ndim() != 2 or rhs.ndim() != 2) return null;
    const rows = @as(usize, @intCast(lhs.dim(0)));
    if (rows == 0 or @as(usize, @intCast(rhs.dim(0))) != rows) return null;
    if (@as(usize, @intCast(lhs.dim(1))) != dim or @as(usize, @intCast(rhs.dim(1))) != dim) return null;
    if (rows != 1) {
        const out = try std.heap.c_allocator.alloc(f32, lhs.elemCount());
        errdefer std.heap.c_allocator.free(out);
        var lhs_host = lhs;
        var rhs_host = rhs;
        const lhs_slice = try tensorHostSlice(&lhs_host);
        const rhs_slice = try tensorHostSlice(&rhs_host);
        if (lhs_slice.len == 0 or rhs_slice.len == 0) return null;
        for (out, 0..) |*o, i| {
            o.* = lhs_slice[i % lhs_slice.len] * rhs_slice[i % rhs_slice.len];
        }
        const shape = [_]i32{ @intCast(rows), @intCast(dim) };
        return MetalTensor.owned(out, &shape);
    }

    if (lhs.isDevice() and rhs.isDevice()) {
        const shape = [_]i32{ 1, @intCast(dim) };
        var output_device = try MetalTensor.deviceAllocate(runtime, dim * @sizeOf(f32), .private, &shape);
        errdefer output_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_multiply_device(
            runtime,
            lhs.deviceHandle(),
            lhs.deviceByteOffset(),
            rhs.deviceHandle(),
            rhs.deviceByteOffset(),
            dim,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }

    const output = try std.heap.c_allocator.alloc(f32, dim);
    errdefer std.heap.c_allocator.free(output);
    var lhs_mut = lhs;
    var rhs_mut = rhs;
    const rc = termite_metal_decode_runtime_apply_multiply(
        runtime,
        try tensorHostConstPtr(&lhs_mut),
        try tensorHostConstPtr(&rhs_mut),
        dim,
        output.ptr,
    );
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(dim) };
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimeApplyMultiplyInto(
    self: anytype,
    lhs: MetalTensor,
    rhs: MetalTensor,
    output: MetalTensor,
    dim: usize,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (dim == 0) return false;
    if (!lhs.isDevice() or !rhs.isDevice() or !output.isDevice()) return false;
    if (lhs.elemCount() != rhs.elemCount() or lhs.elemCount() != output.elemCount() or lhs.elemCount() == 0) return false;
    const device_rc = termite_metal_decode_runtime_apply_multiply_device(
        runtime,
        lhs.deviceHandle(),
        lhs.deviceByteOffset(),
        rhs.deviceHandle(),
        rhs.deviceByteOffset(),
        lhs.elemCount(),
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    return device_rc == 0;
}

pub fn decoderRuntimeApplyMultiplyRhsRepeat(self: anytype, lhs: MetalTensor, rhs: MetalTensor) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!lhs.isDevice() or !rhs.isDevice()) return null;
    const lhs_count = lhs.elemCount();
    const rhs_count = rhs.elemCount();
    if (lhs_count == 0 or rhs_count == 0 or lhs_count % rhs_count != 0) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, lhs_count * @sizeOf(f32), .private, lhs.shape());
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_multiply_device_rhs_repeat(
        runtime,
        lhs.deviceHandle(),
        lhs.deviceByteOffset(),
        rhs.deviceHandle(),
        rhs.deviceByteOffset(),
        lhs_count,
        rhs_count,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

fn decoderRuntimeApplyFlatBinaryDevice(
    self: anytype,
    lhs: MetalTensor,
    rhs: MetalTensor,
    comptime apply_device: fn (?*RawMetalDecodeRuntime, ?*anyopaque, usize, ?*anyopaque, usize, usize, c_int, c_int, ?*anyopaque, usize) callconv(.c) c_int,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!lhs.isDevice() or !rhs.isDevice()) return null;
    const lhs_count = lhs.elemCount();
    const rhs_count = rhs.elemCount();
    if (lhs_count == 0 or rhs_count == 0) return null;
    const lhs_scalar = lhs_count == 1 and rhs_count != 1;
    const rhs_scalar = rhs_count == 1 and lhs_count != 1;
    if (lhs_count != rhs_count and !lhs_scalar and !rhs_scalar) return null;
    const elem_count = if (lhs_scalar) rhs_count else lhs_count;
    const output_shape = if (lhs_scalar) rhs.shape() else lhs.shape();
    var output_device = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = apply_device(
        runtime,
        lhs.deviceHandle(),
        lhs.deviceByteOffset(),
        rhs.deviceHandle(),
        rhs.deviceByteOffset(),
        elem_count,
        if (lhs_scalar) 1 else 0,
        if (rhs_scalar) 1 else 0,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeApplySubtract(self: anytype, lhs: MetalTensor, rhs: MetalTensor) !?MetalTensor {
    return decoderRuntimeApplyFlatBinaryDevice(self, lhs, rhs, termite_metal_decode_runtime_apply_subtract_device_broadcast);
}

pub fn decoderRuntimeApplyDivide(self: anytype, lhs: MetalTensor, rhs: MetalTensor) !?MetalTensor {
    return decoderRuntimeApplyFlatBinaryDevice(self, lhs, rhs, termite_metal_decode_runtime_apply_divide_device_broadcast);
}

pub fn decoderRuntimeApplyDivideRhsRepeat(self: anytype, lhs: MetalTensor, rhs: MetalTensor) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!lhs.isDevice() or !rhs.isDevice()) return null;
    const lhs_count = lhs.elemCount();
    const rhs_count = rhs.elemCount();
    if (lhs_count == 0 or rhs_count == 0 or lhs_count % rhs_count != 0) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, lhs_count * @sizeOf(f32), .private, lhs.shape());
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_divide_device_rhs_repeat(
        runtime,
        lhs.deviceHandle(),
        lhs.deviceByteOffset(),
        rhs.deviceHandle(),
        rhs.deviceByteOffset(),
        lhs_count,
        rhs_count,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeApplyLessThan(self: anytype, lhs: MetalTensor, rhs: MetalTensor) !?MetalTensor {
    return decoderRuntimeApplyFlatBinaryDevice(self, lhs, rhs, termite_metal_decode_runtime_apply_less_than_device_broadcast);
}

pub fn decoderRuntimeTrainingAccumulateF32(
    self: anytype,
    accum: MetalTensor,
    grad: MetalTensor,
    elem_count: usize,
    scale: f32,
    first: bool,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (!accum.isDevice() or !grad.isDevice()) return false;
    if (elem_count == 0 or elem_count > accum.elemCount() or elem_count > grad.elemCount()) return false;
    const rc = termite_metal_decode_runtime_training_accumulate_f32(
        runtime,
        accum.deviceHandle(),
        accum.deviceByteOffset(),
        grad.deviceHandle(),
        grad.deviceByteOffset(),
        elem_count,
        scale,
        if (first) 1 else 0,
    );
    if (rc != 0) return error.MetalTrainingAccumulateFailed;
    return true;
}

pub const TrainingAdamWOptions = struct {
    lr: f32,
    beta1: f32,
    beta2: f32,
    eps: f32,
    weight_decay: f32,
    bias_correction1: f32,
    bias_correction2: f32,
    grad_scale: f32 = 1.0,
};

pub fn decoderRuntimeTrainingAdamWF32(
    self: anytype,
    weight: MetalTensor,
    grad: MetalTensor,
    m: MetalTensor,
    v: MetalTensor,
    elem_count: usize,
    opts: TrainingAdamWOptions,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (!weight.isDevice() or !grad.isDevice() or !m.isDevice() or !v.isDevice()) return false;
    if (elem_count == 0 or elem_count > weight.elemCount() or elem_count > grad.elemCount() or elem_count > m.elemCount() or elem_count > v.elemCount()) return false;
    const rc = termite_metal_decode_runtime_training_adamw_f32(
        runtime,
        weight.deviceHandle(),
        weight.deviceByteOffset(),
        grad.deviceHandle(),
        grad.deviceByteOffset(),
        m.deviceHandle(),
        m.deviceByteOffset(),
        v.deviceHandle(),
        v.deviceByteOffset(),
        elem_count,
        opts.lr,
        opts.beta1,
        opts.beta2,
        opts.eps,
        opts.weight_decay,
        opts.bias_correction1,
        opts.bias_correction2,
        opts.grad_scale,
    );
    if (rc != 0) return error.MetalTrainingAdamWFailed;
    return true;
}

pub fn decoderRuntimeTrainingSumSquaresF32(
    self: anytype,
    input: MetalTensor,
    output: MetalTensor,
    elem_count: usize,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (!input.isDevice() or !output.isDevice()) return false;
    if (elem_count == 0 or elem_count > input.elemCount() or output.elemCount() < 1) return false;
    const rc = termite_metal_decode_runtime_training_sumsq_f32(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        elem_count,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) return error.MetalTrainingSumSquaresFailed;
    return true;
}

pub fn decoderRuntimeApplyWhereSelect(self: anytype, cond: MetalTensor, on_true: MetalTensor, on_false: MetalTensor) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!cond.isDevice() or !on_true.isDevice() or !on_false.isDevice()) return null;
    const cond_count = cond.elemCount();
    const true_count = on_true.elemCount();
    const false_count = on_false.elemCount();
    if (cond_count == 0 or true_count == 0 or false_count == 0) return null;
    const elem_count = if (cond_count > 1) cond_count else if (true_count > 1) true_count else false_count;
    if ((cond_count != elem_count and cond_count != 1) or
        (true_count != elem_count and true_count != 1) or
        (false_count != elem_count and false_count != 1)) return null;
    const output_shape = if (true_count == elem_count) on_true.shape() else if (false_count == elem_count) on_false.shape() else cond.shape();
    var output_device = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, output_shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_where_select_device_broadcast(
        runtime,
        cond.deviceHandle(),
        cond.deviceByteOffset(),
        on_true.deviceHandle(),
        on_true.deviceByteOffset(),
        on_false.deviceHandle(),
        on_false.deviceByteOffset(),
        elem_count,
        if (cond_count == 1 and elem_count != 1) 1 else 0,
        if (true_count == 1 and elem_count != 1) 1 else 0,
        if (false_count == 1 and elem_count != 1) 1 else 0,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeApplyScale(
    self: anytype,
    input: MetalTensor,
    scale: f32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    const dim = input.elemCount();
    if (dim == 0) return null;
    var output_device = try MetalTensor.deviceAllocate(runtime, dim * @sizeOf(f32), .private, input.shape());
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_scale_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        dim,
        scale,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn decoderRuntimeApplyLinearActivationLinearResidual(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.hidden_size == 0 or request.intermediate_size == 0) return null;
    if (request.first_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.second_linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_linear_slots_prepared[request.first_linear_slot] or
        !self.raw_linear_slots_prepared[request.second_linear_slot]) return null;
    if (self.raw_linear_slot_kinds[request.first_linear_slot] != .dense or
        self.raw_linear_slot_kinds[request.second_linear_slot] != .dense) return null;
    if (self.raw_linear_slot_in_dims[request.first_linear_slot] != request.hidden_size or
        self.raw_linear_slot_out_dims[request.first_linear_slot] != request.intermediate_size) return null;
    if (self.raw_linear_slot_in_dims[request.second_linear_slot] != request.intermediate_size or
        self.raw_linear_slot_out_dims[request.second_linear_slot] != request.hidden_size) return null;
    if (request.input.ndim() != 2 or request.residual.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1 or
        @as(usize, @intCast(request.residual.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size or
        @as(usize, @intCast(request.residual.dim(1))) != request.hidden_size) return null;

    const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
    errdefer std.heap.c_allocator.free(output);

    var input = request.input;
    var residual = request.residual;
    const rc = termite_metal_decode_runtime_apply_linear_activation_linear_residual(
        runtime,
        request.first_linear_slot,
        request.second_linear_slot,
        try tensorHostConstPtr(&input),
        try tensorHostConstPtr(&residual),
        request.hidden_size,
        request.intermediate_size,
        @intFromEnum(request.activation),
        output.ptr,
    );
    if (rc != 0) return null;

    const shape = [_]i32{ 1, @intCast(request.hidden_size) };
    return MetalTensor.owned(output, &shape);
}

pub fn tryRawProviderQuantizedLinearHost(
    self: anytype,
    storage: *const QuantizedStorage,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: [*c]f32,
) !bool {
    return tryRawProviderQuantizedLinearHostWithDispatch(self, storage, input, rows, in_dim, out_dim, output, null);
}

pub fn tryRawProviderQuantizedLinearHostWithDispatch(
    self: anytype,
    storage: *const QuantizedStorage,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: [*c]f32,
    planned_dispatch: ?u8,
) !bool {
    const raw_provider = self.raw_provider orelse return false;
    const source_bytes = storage.raw_bytes;
    const bitnet_source_bytes = storage.preparedBytes(.row_major_blocks) orelse storage.raw_bytes;
    switch (storage.tensor_type) {
        .known => |known| switch (known) {
            .Q4_0 => return in_dim % 32 == 0 and termite_metal_provider_linear_q4_0(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q4_1 => return in_dim % 32 == 0 and termite_metal_provider_linear_q4_1(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q5_0 => return in_dim % 32 == 0 and termite_metal_provider_linear_q5_0(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q5_1 => return in_dim % 32 == 0 and termite_metal_provider_linear_q5_1(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q8_0 => {
                if (in_dim % 32 != 0) return false;
                const rc = if (planned_dispatch) |dispatch|
                    termite_metal_provider_linear_q8_0_planned(
                        raw_provider,
                        input,
                        rows,
                        in_dim,
                        source_bytes.ptr,
                        out_dim,
                        dispatch,
                        output,
                    )
                else
                    termite_metal_provider_linear_q8_0(
                        raw_provider,
                        input,
                        rows,
                        in_dim,
                        source_bytes.ptr,
                        out_dim,
                        output,
                    );
                return rc == 0;
            },
            .Q8_1 => return in_dim % 32 == 0 and termite_metal_provider_linear_q8_1(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q2_K => return in_dim % 256 == 0 and termite_metal_provider_linear_q2_k(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q3_K => return in_dim % 256 == 0 and termite_metal_provider_linear_q3_k(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q4_K => return in_dim % 256 == 0 and termite_metal_provider_linear_q4_k(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q5_K => return in_dim % 256 == 0 and termite_metal_provider_linear_q5_k(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q6_K => return in_dim % 256 == 0 and termite_metal_provider_linear_q6_k(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .IQ4_XS => return in_dim % 256 == 0 and termite_metal_provider_linear_iq4_xs(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .IQ4_NL => return in_dim % 32 == 0 and termite_metal_provider_linear_iq4_nl(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .MXFP4 => return in_dim % 32 == 0 and termite_metal_provider_linear_mxfp4(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .Q8_K => return in_dim % 256 == 0 and termite_metal_provider_linear_q8_k(
                raw_provider,
                input,
                rows,
                in_dim,
                source_bytes.ptr,
                out_dim,
                output,
            ) == 0,
            .TL1 => {
                const view = try quant_codec.bitnetTL1View(storage.shape, bitnet_source_bytes);
                if (view.cols != in_dim or view.rows != out_dim) return false;
                return termite_metal_provider_linear_tl1(
                    raw_provider,
                    input,
                    rows,
                    in_dim,
                    bitnet_source_bytes.ptr,
                    bitnet_source_bytes.len,
                    out_dim,
                    @intCast(view.packed_bytes.len),
                    @intCast(view.config.bm),
                    @intCast(view.config.by),
                    @intCast(view.config.bmm),
                    output,
                ) == 0;
            },
            else => return false,
        },
        .bitnet_tl2 => {
            const view = try quant_codec.bitnetTL2View(storage.shape, bitnet_source_bytes);
            if (view.cols != in_dim or view.rows != out_dim) return false;
            const scale_off_u64 = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(view.cols), @intCast(view.rows) }) orelse return error.UnsupportedTensorShape) - 32;
            return termite_metal_provider_linear_tl2(
                raw_provider,
                input,
                rows,
                in_dim,
                bitnet_source_bytes.ptr,
                bitnet_source_bytes.len,
                out_dim,
                @intCast(scale_off_u64),
                @intCast(view.three_values.len),
                @intCast(view.three_signs.len),
                @intCast(view.config.bm),
                @intCast(view.config.by),
                @intCast(view.config.bmm),
                @intCast(view.three_cols),
                @intCast(view.two_cols),
                output,
            ) == 0;
        },
        .unknown => return false,
    }
}

pub fn tryRawLinearHost(
    self: anytype,
    slot: usize,
    input: [*c]const f32,
    in_dim: usize,
    out_dim: usize,
    output: [*c]f32,
) !bool {
    if (slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_linear_slots_prepared[slot]) return false;
    if (self.raw_linear_slot_in_dims[slot] != in_dim or self.raw_linear_slot_out_dims[slot] != out_dim) return false;
    switch (self.raw_linear_slot_kinds[slot]) {
        .dense => {
            const runtime = self.raw_decode_runtime orelse return false;
            if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
            return termite_metal_decode_runtime_apply_linear(
                runtime,
                slot,
                input,
                in_dim,
                out_dim,
                output,
            ) == 0;
        },
        .quantized => {
            const storage = self.raw_linear_slot_quantized_storage[slot] orelse return false;
            return try tryRawProviderQuantizedLinearHost(self, storage, input, 1, in_dim, out_dim, output);
        },
        .none => return false,
    }
}

pub fn decoderRuntimeApplyLinear(self: anytype, request: anytype) !?MetalTensor {
    if (request.in_dim == 0 or request.out_dim == 0) return null;
    if (request.slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_linear_slots_prepared[request.slot]) return null;
    if (self.raw_linear_slot_in_dims[request.slot] != request.in_dim or
        self.raw_linear_slot_out_dims[request.slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.input.dim(0)));
    if (rows == 0) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.in_dim) return null;

    if (try tryApplyQuantizedRuntimeLinear(
        self,
        request.slot,
        request.input,
        rows,
        request.in_dim,
        request.out_dim,
    )) |tensor| return tensor;
    if (try tryApplyDenseRuntimeLinear(
        self,
        request.slot,
        request.input,
        rows,
        request.in_dim,
        request.out_dim,
    )) |tensor| return tensor;

    if (rows != 1) return null;
    const output = try std.heap.c_allocator.alloc(f32, request.out_dim);
    errdefer std.heap.c_allocator.free(output);
    var input = request.input;
    if (!(try tryRawLinearHost(self, request.slot, try tensorHostConstPtr(&input), request.in_dim, request.out_dim, output.ptr))) {
        return null;
    }

    const out_shape = [_]i32{ 1, @intCast(request.out_dim) };
    return MetalTensor.owned(output, &out_shape);
}

pub fn decoderRuntimeApplyLinearPair(self: anytype, request: anytype) !?RuntimeLinearPairResult {
    if (request.in_dim == 0 or request.out_dim == 0) return null;
    if (request.slot_a >= decoder_runtime_linear_slot_capacity or request.slot_b >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_linear_slots_prepared[request.slot_a] or !self.raw_linear_slots_prepared[request.slot_b]) return null;
    if (self.raw_linear_slot_in_dims[request.slot_a] != request.in_dim or
        self.raw_linear_slot_out_dims[request.slot_a] != request.out_dim or
        self.raw_linear_slot_in_dims[request.slot_b] != request.in_dim or
        self.raw_linear_slot_out_dims[request.slot_b] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    const rows = @as(usize, @intCast(request.input.dim(0)));
    if (rows == 0) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.in_dim) return null;

    if (try tryApplyQuantizedRuntimeLinearPair(
        self,
        request.slot_a,
        request.slot_b,
        request.input,
        rows,
        request.in_dim,
        request.out_dim,
    )) |pair| return pair;
    return tryApplyDenseRuntimeLinearPair(
        self,
        request.slot_a,
        request.slot_b,
        request.input,
        rows,
        request.in_dim,
        request.out_dim,
    );
}

pub fn decoderRuntimeApplyRmsNormLinear(self: anytype, request: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    if (request.hidden_size == 0 or request.out_dim == 0) return null;
    if (request.norm_slot >= decoder_runtime_rms_norm_slot_capacity or request.linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_rms_norm_slots_prepared[request.norm_slot]) return null;
    if (self.raw_rms_norm_slot_hidden_sizes[request.norm_slot] != request.hidden_size) return null;
    if (!self.raw_linear_slots_prepared[request.linear_slot]) return null;
    if (self.raw_linear_slot_in_dims[request.linear_slot] != request.hidden_size) return null;
    if (self.raw_linear_slot_out_dims[request.linear_slot] != request.out_dim) return null;
    if (request.input.ndim() != 2) return null;
    if (@as(usize, @intCast(request.input.dim(0))) != 1) return null;
    if (@as(usize, @intCast(request.input.dim(1))) != request.hidden_size) return null;

    if (self.raw_linear_slot_kinds[request.linear_slot] == .quantized) {
        if (frame_active) return null;
        const direct_output = try std.heap.c_allocator.alloc(f32, request.out_dim);
        errdefer std.heap.c_allocator.free(direct_output);
        var request_input = request.input;
        const direct_rc: c_int = switch (ensureQuantizedRuntimeLinearSlotPrepared(
            self,
            request.linear_slot,
            request.hidden_size,
            request.out_dim,
        )) {
            .q4_k => termite_metal_decode_runtime_apply_rms_norm_q4_k_linear_slot(
                runtime,
                request.norm_slot,
                request.linear_slot,
                try tensorHostConstPtr(&request_input),
                request.hidden_size,
                request.eps,
                request.out_dim,
                direct_output.ptr,
            ),
            .q5_k => termite_metal_decode_runtime_apply_rms_norm_q5_k_linear_slot(
                runtime,
                request.norm_slot,
                request.linear_slot,
                try tensorHostConstPtr(&request_input),
                request.hidden_size,
                request.eps,
                request.out_dim,
                direct_output.ptr,
            ),
            else => -1,
        };
        if (direct_rc == 0) {
            const shape = [_]i32{ 1, @intCast(request.out_dim) };
            return MetalTensor.owned(direct_output, &shape);
        }
        std.heap.c_allocator.free(direct_output);
        var norm_stats = struct {
            decoder_runtime_apply_layer_norm_calls: u64 = 0,
        }{};
        var normed_tensor = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &norm_stats)) orelse return null;
        defer normed_tensor.deinit();
        return decoderRuntimeApplyLinear(self, .{
            .slot = request.linear_slot,
            .input = normed_tensor,
            .in_dim = request.hidden_size,
            .out_dim = request.out_dim,
        });
    }

    if (request.input.isDevice()) {
        var norm_stats = struct {
            decoder_runtime_apply_layer_norm_calls: u64 = 0,
        }{};
        var normed = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &norm_stats)) orelse return null;
        defer normed.deinit();
        return decoderRuntimeApplyLinear(self, .{
            .slot = request.linear_slot,
            .input = normed,
            .in_dim = request.hidden_size,
            .out_dim = request.out_dim,
        });
    }

    const output = try std.heap.c_allocator.alloc(f32, request.out_dim);
    errdefer std.heap.c_allocator.free(output);
    var request_input = request.input;
    const rc = termite_metal_decode_runtime_apply_rms_norm_linear(
        runtime,
        request.norm_slot,
        request.linear_slot,
        try tensorHostConstPtr(&request_input),
        request.hidden_size,
        request.eps,
        request.out_dim,
        output.ptr,
    );
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(request.out_dim) };
    return MetalTensor.owned(output, &shape);
}

pub fn decoderRuntimePrepareLinear(self: anytype, request: anytype, stats: anytype) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) {
        stats.decoder_runtime_prepare_linear_runtime_not_ready += 1;
        return false;
    }
    if (request.in_dim == 0 or request.out_dim == 0 or request.slot >= decoder_runtime_linear_slot_capacity) return false;
    if (self.raw_linear_slots_prepared[request.slot] and
        self.raw_linear_slot_in_dims[request.slot] == request.in_dim and
        self.raw_linear_slot_out_dims[request.slot] == request.out_dim and
        ((request.quantized_storage != null and self.raw_linear_slot_kinds[request.slot] == .quantized) or
            (request.quantized_storage == null and self.raw_linear_slot_kinds[request.slot] == .dense)))
    {
        return true;
    }

    clearRawLinearSlot(self, request.slot);

    if (request.bias.ndim() != 1) {
        stats.decoder_runtime_prepare_linear_bias_shape_failures += 1;
        return false;
    }
    if (@as(usize, @intCast(request.bias.dim(0))) != request.out_dim) {
        stats.decoder_runtime_prepare_linear_bias_shape_failures += 1;
        return false;
    }
    var bias = request.bias;
    const bias_base: [*c]const f32 = try tensorHostConstPtr(&bias);

    if (request.quantized_storage) |storage| {
        if (storage.packed_expert != null) {
            stats.decoder_runtime_prepare_linear_packed_expert_failures += 1;
            return false;
        }
        const bias_shape = [_]i32{@intCast(request.out_dim)};
        self.raw_linear_slot_dense_biases[request.slot] = try MetalTensor.ownedCloneFrom(bias_base[0..request.out_dim], &bias_shape);
        errdefer {
            if (self.raw_linear_slot_dense_biases[request.slot]) |*bias_tensor| bias_tensor.deinit();
            self.raw_linear_slot_dense_biases[request.slot] = null;
        }
        if (termite_metal_decode_runtime_prepare_linear_bias(
            runtime,
            request.slot,
            bias_base,
            request.out_dim,
        ) != 0) return false;
        stats.decoder_runtime_prepare_linear_calls += 1;
        self.raw_linear_slot_quantized_storage[request.slot] = try dupQuantizedStorage(storage);
        self.raw_linear_slot_kinds[request.slot] = .quantized;
        self.raw_linear_slots_prepared[request.slot] = true;
        self.raw_linear_slot_in_dims[request.slot] = request.in_dim;
        self.raw_linear_slot_out_dims[request.slot] = request.out_dim;
        return true;
    }

    const dense_bf16_bytes: ?[]const u8 = if (@hasField(@TypeOf(request), "dense_bf16_bytes"))
        request.dense_bf16_bytes
    else
        @as(?[]const u8, null);
    if (dense_bf16_bytes) |bytes| {
        const expected_bytes = std.math.mul(usize, request.in_dim, request.out_dim) catch return false;
        const expected_bf16_bytes = std.math.mul(usize, expected_bytes, @sizeOf(u16)) catch return false;
        if (bytes.len < expected_bf16_bytes) return false;
        stats.decoder_runtime_prepare_linear_calls += 1;
        const use_no_copy = if (@hasField(@TypeOf(request), "dense_bf16_no_copy_safe"))
            request.dense_bf16_no_copy_safe
        else
            false;
        var rc: c_int = if (use_no_copy)
            termite_metal_decode_runtime_prepare_linear_bf16_no_copy(
                runtime,
                request.slot,
                bytes.ptr,
                bytes.len,
                bias_base,
                request.in_dim,
                request.out_dim,
            )
        else
            -1;
        if (rc != 0) {
            rc = termite_metal_decode_runtime_prepare_linear_bf16(
                runtime,
                request.slot,
                bytes.ptr,
                bytes.len,
                bias_base,
                request.in_dim,
                request.out_dim,
            );
        }
        if (rc != 0) return false;
        self.raw_linear_slots_prepared[request.slot] = true;
        self.raw_linear_slot_kinds[request.slot] = .dense;
        self.raw_linear_slot_in_dims[request.slot] = request.in_dim;
        self.raw_linear_slot_out_dims[request.slot] = request.out_dim;
        return true;
    }

    if (request.weight.ndim() != 2) {
        stats.decoder_runtime_prepare_linear_weight_ndim_failures += 1;
        return false;
    }
    const weight_dim0: usize = @intCast(request.weight.dim(0));
    const weight_dim1: usize = @intCast(request.weight.dim(1));
    const weight_is_row_major_out_in = weight_dim0 == request.out_dim and weight_dim1 == request.in_dim;
    const weight_is_row_major_in_out = weight_dim0 == request.in_dim and weight_dim1 == request.out_dim;
    if (!weight_is_row_major_out_in and !weight_is_row_major_in_out) {
        stats.decoder_runtime_prepare_linear_weight_shape_failures += 1;
        return false;
    }

    var weight = request.weight;
    const weight_base: [*c]const f32 = try tensorHostConstPtr(&weight);
    var transposed_weight_owned: ?[]f32 = null;
    defer if (transposed_weight_owned) |owned| std.heap.c_allocator.free(owned);
    const prepared_weight: [*c]const f32 = blk: {
        if (weight_is_row_major_out_in) break :blk weight_base;
        const owned = try std.heap.c_allocator.alloc(f32, request.in_dim * request.out_dim);
        transposed_weight_owned = owned;
        for (0..request.out_dim) |out_idx| {
            for (0..request.in_dim) |in_idx| {
                owned[out_idx * request.in_dim + in_idx] = weight_base[in_idx * request.out_dim + out_idx];
            }
        }
        break :blk owned.ptr;
    };

    stats.decoder_runtime_prepare_linear_calls += 1;
    const rc = termite_metal_decode_runtime_prepare_linear(
        runtime,
        request.slot,
        prepared_weight,
        bias_base,
        request.in_dim,
        request.out_dim,
    );
    if (rc != 0) return false;
    self.raw_linear_slots_prepared[request.slot] = true;
    self.raw_linear_slot_kinds[request.slot] = .dense;
    self.raw_linear_slot_in_dims[request.slot] = request.in_dim;
    self.raw_linear_slot_out_dims[request.slot] = request.out_dim;
    return true;
}

pub const RuntimeLinearPairResult = struct {
    first: MetalTensor,
    second: MetalTensor,
};

pub const RuntimeLinearTripleResult = struct {
    first: MetalTensor,
    second: MetalTensor,
    third: MetalTensor,
};

pub const RawPlannedLayerContract = extern struct {
    ops: ?[*]const u16 = null,
    op_count: usize = 0,
    barriers: ?[*]const u8 = null,
    barrier_count: usize = 0,
    quant_dispatches: ?[*]const u8 = null,
    quant_dispatch_count: usize = 0,
    command_ops: ?[*]const ops.PlannedCommandOp = null,
    command_op_count: usize = 0,
    start_index: usize = 0,

    pub fn fromContract(contract: ops.PlannedLayerContract) RawPlannedLayerContract {
        return .{
            .ops = if (contract.ops.len != 0) contract.ops.ptr else null,
            .op_count = contract.ops.len,
            .barriers = if (contract.barriers.len != 0) contract.barriers.ptr else null,
            .barrier_count = contract.barriers.len,
            .quant_dispatches = if (contract.quant_dispatches.len != 0) contract.quant_dispatches.ptr else null,
            .quant_dispatch_count = contract.quant_dispatches.len,
            .command_ops = if (contract.command_ops.len != 0) contract.command_ops.ptr else null,
            .command_op_count = contract.command_ops.len,
            .start_index = contract.start_index,
        };
    }

    pub fn fromContractRange(
        contract: ops.PlannedLayerContract,
        start_index: usize,
        end_index: usize,
    ) RawPlannedLayerContract {
        if (start_index >= end_index) return .{};
        if (end_index > contract.ops.len or
            end_index > contract.barriers.len or
            end_index > contract.quant_dispatches.len or
            end_index > contract.command_ops.len)
        {
            return .{};
        }
        var raw = fromContract(contract);
        raw.op_count = end_index;
        raw.barrier_count = end_index;
        raw.quant_dispatch_count = end_index;
        raw.command_op_count = end_index;
        raw.start_index = start_index;
        return raw;
    }
};

pub const RawAttentionGatedBlockTiming = extern struct {
    replace_span_nanos: u64 = 0,
    attention_span_nanos: u64 = 0,
    attention_prefix_nanos: u64 = 0,
    gated_ffn_residual_nanos: u64 = 0,
    command_wait_nanos: u64 = 0,
    gpu_nanos: u64 = 0,
    failure_stage: u32 = 0,
    failure_code: i32 = 0,
    attention_f32_kernels: u64 = 0,
    q8_0_linear_kernels: u64 = 0,
    q8_0_attention_linear_kernels: u64 = 0,
    q8_0_ffn_down_linear_kernels: u64 = 0,
    q8_0_ple_linear_kernels: u64 = 0,
    q8_0_pair_activation_kernels: u64 = 0,
    rms_norm_kernels: u64 = 0,
    rms_norm_add_kernels: u64 = 0,
    layer_norm_kernels: u64 = 0,
    add_kernels: u64 = 0,
    blit_copies: u64 = 0,
};

pub const RawRuntimeMemoryStats = extern struct {
    buffer_count: u64 = 0,
    total_bytes: u64 = 0,
    embedding_bytes: u64 = 0,
    norm_bytes: u64 = 0,
    dense_linear_bytes: u64 = 0,
    dense_linear_buffer_count: u64 = 0,
    dense_linear_largest_slot: u64 = 0,
    dense_linear_largest_bytes: u64 = 0,
    dense_linear_largest_in_dim: u64 = 0,
    dense_linear_largest_out_dim: u64 = 0,
    dense_linear_weight_bytes: u64 = 0,
    dense_linear_f32_weight_bytes: u64 = 0,
    dense_linear_bf16_weight_bytes: u64 = 0,
    dense_linear_f32_slots: u64 = 0,
    dense_linear_bf16_slots: u64 = 0,
    quant_linear_bytes: u64 = 0,
    scratch_bytes: u64 = 0,
    scratch_pool_bytes: u64 = 0,
    scratch_pool_slots: u64 = 0,
    scratch_pool_in_use_slots: u64 = 0,
    scratch_pool_pending_slots: u64 = 0,
    attention_span_bytes: u64 = 0,
    hidden_state_bytes: u64 = 0,
    frame_retained_bytes: u64 = 0,
    graph_plan_bytes: u64 = 0,
    graph_plan_slots: u64 = 0,
    graph_plan_active: u64 = 0,
    graph_plan_count: u64 = 0,
    graph_plan_allocations: u64 = 0,
    graph_plan_reuses: u64 = 0,
    mps_dense_linear_standalone_calls: u64 = 0,
    mps_dense_linear_active_frame_calls: u64 = 0,
    mps_dense_linear_standalone_wait_nanos: u64 = 0,
    mps_dense_linear_standalone_gpu_nanos: u64 = 0,
    last_frame_mps_dense_linear_count: u64 = 0,
    dense_qkv_packed_calls: u64 = 0,
    dense_qkv_packed_fallbacks: u64 = 0,
    dense_pair_packed_calls: u64 = 0,
    dense_pair_packed_fallbacks: u64 = 0,
    deberta_ffn_fused_calls: u64 = 0,
    deberta_ffn_fused_mps_matmuls: u64 = 0,
    deberta_ffn_fused_fallbacks: u64 = 0,
    deberta_attention_legacy_calls: u64 = 0,
    deberta_attention_gemm_calls: u64 = 0,
    deberta_attention_gemm_fallbacks: u64 = 0,
    mpsgraph_ffn_calls: u64 = 0,
    mpsgraph_ffn_fallbacks: u64 = 0,
    mpsgraph_ffn_compiles: u64 = 0,
    mpsgraph_ffn_cache_hits: u64 = 0,
    compute_encoder_count: u64 = 0,
    blit_encoder_count: u64 = 0,
    last_frame_compute_encoder_count: u64 = 0,
    last_frame_blit_encoder_count: u64 = 0,
    last_frame_planned_compute_scope_count: u64 = 0,
    last_frame_planned_barrier_count: u64 = 0,
    last_frame_compute_quant_linear_count: u64 = 0,
    last_frame_compute_quant_qkv_count: u64 = 0,
    last_frame_compute_quant_pair_act_count: u64 = 0,
    last_frame_compute_attention_count: u64 = 0,
    last_frame_compute_rms_norm_count: u64 = 0,
    last_frame_compute_head_rope_count: u64 = 0,
    last_frame_compute_ffn_count: u64 = 0,
    last_frame_compute_ple_count: u64 = 0,
    last_frame_compute_tail_count: u64 = 0,
    last_frame_compute_embedding_count: u64 = 0,
    last_frame_compute_dense_linear_count: u64 = 0,
    last_frame_compute_layer_count: u64 = 0,
    last_frame_compute_other_count: u64 = 0,
    last_frame_compute_region_attention_count: u64 = 0,
    last_frame_compute_region_attention_project_count: u64 = 0,
    last_frame_compute_region_ffn_norm_count: u64 = 0,
    last_frame_compute_region_ffn_count: u64 = 0,
    last_frame_compute_region_ple_count: u64 = 0,
    last_frame_compute_region_tail_count: u64 = 0,
    last_frame_compute_region_embedding_count: u64 = 0,
    last_frame_compute_region_layer_count: u64 = 0,
    last_frame_compute_region_other_count: u64 = 0,
    last_frame_planned_command_op_count: u64 = 0,
    last_frame_planned_command_op_kind_counts: [32]u64 = [_]u64{0} ** 32,
    last_frame_planned_command_operator_counts: [16]u64 = [_]u64{0} ** 16,
    last_frame_planned_command_quant_dispatch_counts: [4]u64 = [_]u64{0} ** 4,
    last_frame_blit_buffer_upload_count: u64 = 0,
    last_frame_blit_buffer_copy_count: u64 = 0,
    last_frame_blit_buffer_slice_count: u64 = 0,
    last_frame_blit_attention_span_count: u64 = 0,
    last_frame_blit_ffn_copy_count: u64 = 0,
    last_frame_blit_embedding_count: u64 = 0,
    last_frame_blit_other_count: u64 = 0,
    q8_0_linear_dispatch_scalar: u64 = 0,
    q8_0_linear_dispatch_mmv: u64 = 0,
    q8_0_linear_dispatch_small_batch: u64 = 0,
    q8_0_linear_dispatch_mm: u64 = 0,
    q8_0_linear_rows_1: u64 = 0,
    q8_0_linear_rows_2_8: u64 = 0,
    q8_0_linear_rows_9_64: u64 = 0,
    q8_0_linear_rows_65_plus: u64 = 0,
    q8_0_pair_activation_mm_f16_output: u64 = 0,
    q8_0_linear_mm_f16_input: u64 = 0,
    q8_0_pair_activation_rms_scale_mmv_f16_output: u64 = 0,
    q8_0_linear_mmv_f16_input: u64 = 0,
    q8_0_linear_family_dispatch_counts: [12][4]u64 = [_][4]u64{[_]u64{0} ** 4} ** 12,
};

pub extern fn termite_metal_device_available() c_int;

fn sleepMetalProbeRetry() void {
    var ts = std.c.timespec{ .sec = 0, .nsec = 50_000_000 };
    _ = std.c.nanosleep(&ts, &ts);
}

pub fn metalDeviceAvailable() bool {
    for (0..3) |attempt| {
        if (termite_metal_device_available() != 0) return true;
        if (attempt != 2) sleepMetalProbeRetry();
    }
    return false;
}

pub extern fn termite_metal_provider_create() ?*RawMetalProvider;
pub extern fn termite_metal_provider_destroy(provider: ?*RawMetalProvider) void;
pub extern fn termite_metal_decode_runtime_create() ?*RawMetalDecodeRuntime;
pub extern fn termite_metal_decode_runtime_destroy(runtime: ?*RawMetalDecodeRuntime) void;
pub extern fn termite_metal_decode_runtime_ready(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_reserve(runtime: ?*RawMetalDecodeRuntime, scratch_bytes: usize, token_bytes: usize) c_int;
pub extern fn termite_metal_decode_runtime_begin_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_submit_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_cancel_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_wait_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_flush_active_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_active_frame_has_work(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_has_active_frame(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_push_compute_region(runtime: ?*RawMetalDecodeRuntime, region: usize, previous_out: *usize) c_int;
pub extern fn termite_metal_decode_runtime_pop_compute_region(runtime: ?*RawMetalDecodeRuntime, previous: usize) c_int;
pub extern fn termite_metal_decode_runtime_begin_planned_compute_scope(runtime: ?*RawMetalDecodeRuntime, source: usize, region: usize) c_int;
pub extern fn termite_metal_decode_runtime_planned_compute_barrier(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_end_planned_compute_scope(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_frame_cb_count(runtime: ?*RawMetalDecodeRuntime) u64;
pub extern fn termite_metal_decode_runtime_last_frame_gpu_nanos(runtime: ?*RawMetalDecodeRuntime) u64;
pub extern fn termite_metal_decode_runtime_memory_snapshot(
    runtime: ?*RawMetalDecodeRuntime,
    snapshot: *RawRuntimeMemoryStats,
) c_int;
pub extern fn termite_metal_decode_runtime_begin_graph_plan(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_reserve_graph_plan_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    bytes: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_commit_graph_plan(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_buffer_alloc(runtime: ?*RawMetalDecodeRuntime, length: usize, storage_mode: c_int) ?*anyopaque;
pub extern fn termite_metal_buffer_release(handle: ?*anyopaque) void;
pub extern fn termite_metal_buffer_contents(handle: ?*anyopaque) ?*anyopaque;
pub extern fn termite_metal_buffer_upload(
    runtime: ?*RawMetalDecodeRuntime,
    handle: ?*anyopaque,
    offset: usize,
    src: ?*const anyopaque,
    length: usize,
) c_int;
pub extern fn termite_metal_buffer_download(
    runtime: ?*RawMetalDecodeRuntime,
    handle: ?*anyopaque,
    offset: usize,
    dst: ?*anyopaque,
    length: usize,
) c_int;
pub extern fn termite_metal_buffer_copy(
    runtime: ?*RawMetalDecodeRuntime,
    src_handle: ?*anyopaque,
    src_offset: usize,
    dst_handle: ?*anyopaque,
    dst_offset: usize,
    length: usize,
) c_int;
pub extern fn termite_metal_buffer_copy_pair(
    runtime: ?*RawMetalDecodeRuntime,
    src_a_handle: ?*anyopaque,
    src_a_offset: usize,
    dst_a_handle: ?*anyopaque,
    dst_a_offset: usize,
    length_a: usize,
    src_b_handle: ?*anyopaque,
    src_b_offset: usize,
    dst_b_handle: ?*anyopaque,
    dst_b_offset: usize,
    length_b: usize,
) c_int;
pub extern fn termite_metal_buffer_slice_last_dim_2d(
    runtime: ?*RawMetalDecodeRuntime,
    src_handle: ?*anyopaque,
    src_offset: usize,
    rows: usize,
    cols: usize,
    start: usize,
    out_cols: usize,
    dst_handle: ?*anyopaque,
    dst_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reserve_hidden_state(
    runtime: ?*RawMetalDecodeRuntime,
    max_prefill_rows: usize,
    hidden_size: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_hidden_state_buffer(
    runtime: ?*RawMetalDecodeRuntime,
    which: c_int,
) ?*anyopaque;
pub extern fn termite_metal_decode_runtime_hidden_state_capacity(
    runtime: ?*RawMetalDecodeRuntime,
) usize;
pub extern fn termite_metal_decode_runtime_hidden_state_max_rows(
    runtime: ?*RawMetalDecodeRuntime,
) usize;
pub extern fn termite_metal_decode_runtime_acquire_scratch(
    runtime: ?*RawMetalDecodeRuntime,
    bytes: usize,
) ?*anyopaque;
pub extern fn termite_metal_decode_runtime_release_scratch(
    runtime: ?*RawMetalDecodeRuntime,
    handle: ?*anyopaque,
) void;

pub const HiddenStateSide = enum(c_int) { front = 0, back = 1 };

pub const ScratchAcquireError = error{
    RuntimeUnavailable,
    ScratchPoolExhausted,
};

pub const HiddenStateReserveError = error{
    RuntimeUnavailable,
    InvalidHiddenStateShape,
    HiddenStateAllocFailed,
};

pub fn reserveHiddenState(
    runtime: ?*RawMetalDecodeRuntime,
    max_prefill_rows: usize,
    hidden_size: usize,
) HiddenStateReserveError!void {
    return switch (termite_metal_decode_runtime_reserve_hidden_state(runtime, max_prefill_rows, hidden_size)) {
        0 => {},
        -1 => HiddenStateReserveError.RuntimeUnavailable,
        -2 => HiddenStateReserveError.InvalidHiddenStateShape,
        else => HiddenStateReserveError.HiddenStateAllocFailed,
    };
}

pub fn hiddenStateBuffer(runtime: ?*RawMetalDecodeRuntime, side: HiddenStateSide) ?*anyopaque {
    return termite_metal_decode_runtime_hidden_state_buffer(runtime, @intFromEnum(side));
}

pub fn hiddenStateCapacity(runtime: ?*RawMetalDecodeRuntime) usize {
    return termite_metal_decode_runtime_hidden_state_capacity(runtime);
}

pub fn hiddenStateMaxRows(runtime: ?*RawMetalDecodeRuntime) usize {
    return termite_metal_decode_runtime_hidden_state_max_rows(runtime);
}

pub fn acquireScratch(runtime: ?*RawMetalDecodeRuntime, bytes: usize) ScratchAcquireError!*anyopaque {
    if (runtime == null) return ScratchAcquireError.RuntimeUnavailable;
    if (bytes == 0) return ScratchAcquireError.ScratchPoolExhausted;
    const handle = termite_metal_decode_runtime_acquire_scratch(runtime, bytes);
    return handle orelse ScratchAcquireError.ScratchPoolExhausted;
}

pub fn releaseScratch(runtime: ?*RawMetalDecodeRuntime, handle: ?*anyopaque) void {
    termite_metal_decode_runtime_release_scratch(runtime, handle);
}

pub const GraphPlanError = error{
    RuntimeUnavailable,
    InvalidSlot,
    AllocationFailed,
};

pub fn beginGraphPlan(runtime: ?*RawMetalDecodeRuntime) GraphPlanError!void {
    return switch (termite_metal_decode_runtime_begin_graph_plan(runtime)) {
        0 => {},
        else => GraphPlanError.RuntimeUnavailable,
    };
}

pub fn reserveGraphPlanSlot(runtime: ?*RawMetalDecodeRuntime, slot: usize, bytes: usize) GraphPlanError!void {
    return switch (termite_metal_decode_runtime_reserve_graph_plan_slot(runtime, slot, bytes)) {
        0 => {},
        -1 => GraphPlanError.InvalidSlot,
        else => GraphPlanError.RuntimeUnavailable,
    };
}

pub fn commitGraphPlan(runtime: ?*RawMetalDecodeRuntime) GraphPlanError!void {
    return switch (termite_metal_decode_runtime_commit_graph_plan(runtime)) {
        0 => {},
        -1 => GraphPlanError.RuntimeUnavailable,
        else => GraphPlanError.AllocationFailed,
    };
}

pub fn copyTensorInto(self: anytype, src: MetalTensor, dst: *MetalTensor) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    try src.copyInto(dst);
    return true;
}

pub const FrameError = error{
    RuntimeUnavailable,
    FrameAlreadyActive,
    FrameNotActive,
    PlannedScopeActive,
    PlannedScopeNotActive,
    SubmissionFailed,
    CommandBufferFailed,
};

pub fn beginFrame(runtime: ?*RawMetalDecodeRuntime) FrameError!void {
    return switch (termite_metal_decode_runtime_begin_frame(runtime)) {
        0 => {},
        -1, -2 => FrameError.RuntimeUnavailable,
        -3, -4 => FrameError.FrameAlreadyActive,
        else => FrameError.SubmissionFailed,
    };
}

pub fn submitFrame(runtime: ?*RawMetalDecodeRuntime) FrameError!void {
    return switch (termite_metal_decode_runtime_submit_frame(runtime)) {
        0 => {},
        -1 => FrameError.RuntimeUnavailable,
        -2 => FrameError.FrameNotActive,
        -3 => FrameError.FrameAlreadyActive,
        -4 => FrameError.PlannedScopeActive,
        else => FrameError.SubmissionFailed,
    };
}

pub fn cancelFrame(runtime: ?*RawMetalDecodeRuntime) FrameError!void {
    return switch (termite_metal_decode_runtime_cancel_frame(runtime)) {
        0 => {},
        -1 => FrameError.RuntimeUnavailable,
        -2 => FrameError.FrameNotActive,
        else => FrameError.SubmissionFailed,
    };
}

pub fn waitFrame(runtime: ?*RawMetalDecodeRuntime) FrameError!void {
    return switch (termite_metal_decode_runtime_wait_frame(runtime)) {
        0 => {},
        -1 => FrameError.RuntimeUnavailable,
        -2 => FrameError.FrameNotActive,
        else => FrameError.CommandBufferFailed,
    };
}

pub fn flushActiveFrame(runtime: ?*RawMetalDecodeRuntime) FrameError!void {
    if (traceFrameLifecycle()) {
        std.debug.print("metal_frame_lifecycle_zig: flushActiveFrame\n", .{});
    }
    return switch (termite_metal_decode_runtime_flush_active_frame(runtime)) {
        0 => {},
        -1 => FrameError.RuntimeUnavailable,
        -2 => FrameError.FrameNotActive,
        else => FrameError.CommandBufferFailed,
    };
}

fn traceFrameLifecycle() bool {
    const raw = std.c.getenv("TERMITE_METAL_TRACE_FRAME_LIFECYCLE") orelse return false;
    const value = std.mem.span(raw);
    return value.len > 0 and !std.mem.eql(u8, value, "0");
}

pub fn hasActiveFrame(runtime: ?*RawMetalDecodeRuntime) bool {
    return termite_metal_decode_runtime_has_active_frame(runtime) != 0;
}

pub fn activeFrameHasWork(runtime: ?*RawMetalDecodeRuntime) bool {
    return termite_metal_decode_runtime_active_frame_has_work(runtime) != 0;
}

pub fn pushComputeRegion(runtime: ?*RawMetalDecodeRuntime, region: ComputeRegion) ComputeRegionScope {
    var previous: usize = @intFromEnum(ComputeRegion.other);
    if (termite_metal_decode_runtime_push_compute_region(runtime, @intFromEnum(region), &previous) != 0) {
        return .{ .runtime = runtime, .previous = previous, .active = false };
    }
    return .{ .runtime = runtime, .previous = previous, .active = true };
}

pub fn beginPlannedComputeScope(runtime: ?*RawMetalDecodeRuntime, source: usize, region: ComputeRegion) FrameError!void {
    return switch (termite_metal_decode_runtime_begin_planned_compute_scope(runtime, source, @intFromEnum(region))) {
        0 => {},
        -1 => FrameError.RuntimeUnavailable,
        -2 => FrameError.FrameNotActive,
        -3 => FrameError.PlannedScopeActive,
        else => FrameError.SubmissionFailed,
    };
}

pub fn plannedComputeBarrier(runtime: ?*RawMetalDecodeRuntime) FrameError!void {
    return switch (termite_metal_decode_runtime_planned_compute_barrier(runtime)) {
        0 => {},
        -1 => FrameError.RuntimeUnavailable,
        -2 => FrameError.FrameNotActive,
        -3 => FrameError.PlannedScopeNotActive,
        else => FrameError.SubmissionFailed,
    };
}

pub fn endPlannedComputeScope(runtime: ?*RawMetalDecodeRuntime) FrameError!void {
    return switch (termite_metal_decode_runtime_end_planned_compute_scope(runtime)) {
        0 => {},
        -1 => FrameError.RuntimeUnavailable,
        -2 => FrameError.PlannedScopeNotActive,
        else => FrameError.SubmissionFailed,
    };
}

pub const PlannedComputeSequence = struct {
    runtime: ?*RawMetalDecodeRuntime,
    plan: metal_command_planner.PlanView,
    command_plan: ?metal_command_planner.GraphCommandPlanView = null,
    next_planned_op: usize = 0,
    active_scope_index: ?usize = null,
    disabled: bool = false,

    pub fn beforeNext(self: *PlannedComputeSequence) bool {
        if (self.disabled or self.next_planned_op >= self.plan.planned_ops.len) return false;
        const planned = self.plan.planned_ops[self.next_planned_op];
        if (planned.scope_index >= self.plan.scopes.len) {
            self.disable();
            return false;
        }
        const scope = self.plan.scopes[planned.scope_index];
        if (self.active_scope_index == null or self.active_scope_index.? != planned.scope_index) {
            self.endActiveScope() catch {
                self.disable();
                return false;
            };
            beginPlannedComputeScope(
                self.runtime,
                scope.source,
                @enumFromInt(scope.region),
            ) catch {
                self.disable();
                return false;
            };
            self.active_scope_index = planned.scope_index;
        }
        if (planned.barrier_before) {
            plannedComputeBarrier(self.runtime) catch {
                self.disable();
                return false;
            };
        }
        self.next_planned_op += 1;
        return true;
    }

    pub fn exportActiveContract(
        self: *const PlannedComputeSequence,
        op_storage: []u16,
        barrier_storage: []u8,
    ) ops.PlannedLayerContract {
        if (self.disabled or self.active_scope_index == null) return .{};
        if (self.plan.planned_ops.len == 0 or self.next_planned_op >= self.plan.planned_ops.len) return .{};
        if (self.plan.planned_ops.len > op_storage.len or self.plan.planned_ops.len > barrier_storage.len) return .{};
        if (self.plan.planned_ops[self.next_planned_op].scope_index != self.active_scope_index.?) return .{};

        for (self.plan.planned_ops, 0..) |planned, index| {
            op_storage[index] = @intFromEnum(planned.kind);
            barrier_storage[index] = @intFromBool(planned.barrier_before);
        }
        return .{
            .ops = op_storage[0..self.plan.planned_ops.len],
            .barriers = barrier_storage[0..self.plan.planned_ops.len],
            .start_index = self.next_planned_op,
        };
    }

    pub fn exportActiveCommandContract(
        self: *const PlannedComputeSequence,
        op_storage: []u16,
        barrier_storage: []u8,
        quant_dispatch_storage: []u8,
        command_op_storage: []ops.PlannedCommandOp,
    ) ops.PlannedLayerContract {
        if (self.disabled or self.active_scope_index == null) return .{};
        const command_plan = self.command_plan orelse return self.exportActiveContract(op_storage, barrier_storage);
        if (command_plan.planned_ops.len == 0 or self.next_planned_op >= command_plan.planned_ops.len) return .{};
        if (command_plan.planned_ops[self.next_planned_op].scope_index != self.active_scope_index.?) return .{};
        return plannedContractFromCommandPlan(
            command_plan,
            op_storage,
            barrier_storage,
            quant_dispatch_storage,
            command_op_storage,
            self.next_planned_op,
        );
    }

    pub fn disable(self: *PlannedComputeSequence) void {
        self.endActiveScope() catch {};
        self.disabled = true;
    }

    pub fn deinit(self: *PlannedComputeSequence) void {
        self.endActiveScope() catch {};
    }

    pub fn endActiveScope(self: *PlannedComputeSequence) FrameError!void {
        if (self.active_scope_index == null) return;
        try endPlannedComputeScope(self.runtime);
        self.active_scope_index = null;
    }
};

pub fn plannedContractFromPlan(
    plan: metal_command_planner.PlanView,
    op_storage: []u16,
    barrier_storage: []u8,
    start_index: usize,
) ops.PlannedLayerContract {
    if (plan.planned_ops.len == 0 or start_index >= plan.planned_ops.len) return .{};
    if (plan.planned_ops.len > op_storage.len or plan.planned_ops.len > barrier_storage.len) return .{};
    for (plan.planned_ops, 0..) |planned, index| {
        op_storage[index] = @intFromEnum(planned.kind);
        barrier_storage[index] = @intFromBool(planned.barrier_before);
    }
    return .{
        .ops = op_storage[0..plan.planned_ops.len],
        .barriers = barrier_storage[0..plan.planned_ops.len],
        .start_index = start_index,
    };
}

pub fn plannedContractFromCommandPlan(
    plan: metal_command_planner.GraphCommandPlanView,
    op_storage: []u16,
    barrier_storage: []u8,
    quant_dispatch_storage: []u8,
    command_op_storage: []ops.PlannedCommandOp,
    start_index: usize,
) ops.PlannedLayerContract {
    if (plan.planned_ops.len == 0 or start_index >= plan.planned_ops.len) return .{};
    if (plan.planned_ops.len > op_storage.len or
        plan.planned_ops.len > barrier_storage.len or
        plan.ops.len > quant_dispatch_storage.len or
        plan.ops.len > command_op_storage.len)
    {
        return .{};
    }
    @memset(quant_dispatch_storage[0..plan.ops.len], 255);
    for (plan.planned_ops, 0..) |planned, index| {
        op_storage[index] = @intFromEnum(planned.kind);
        barrier_storage[index] = @intFromBool(planned.barrier_before);
    }
    for (plan.ops, 0..) |op, index| {
        const quant_dispatch = if (op.quant_matmul) |quant|
            @intFromEnum(quant.dispatch)
        else
            255;
        const operator: u8 = if (op.operator_plan) |operator_plan|
            @intFromEnum(operator_plan.operator())
        else
            255;
        const format: u8 = if (op.operator_plan) |operator_plan|
            plannedOperatorFormat(operator_plan)
        else
            255;
        quant_dispatch_storage[index] = quant_dispatch;
        command_op_storage[index] = .{
            .kind = @intFromEnum(op.kind),
            .barrier_before = @intFromBool(op.barrier_before),
            .quant_dispatch = quant_dispatch,
            .operator = operator,
            .format = format,
            .input_dtype = @intFromEnum(op.input_dtype),
            .output_dtype = @intFromEnum(op.output_dtype),
            .source = @intCast(op.source),
            .region = @intCast(op.region),
            .scope_index = @intCast(op.scope_index),
            .resource_start = op.resource_start,
            .resource_count = op.resource_count,
        };
    }
    return .{
        .ops = op_storage[0..plan.planned_ops.len],
        .barriers = barrier_storage[0..plan.planned_ops.len],
        .quant_dispatches = quant_dispatch_storage[0..plan.ops.len],
        .command_ops = command_op_storage[0..plan.ops.len],
        .start_index = start_index,
    };
}

pub const PlannedCommandContractStorage = struct {
    ops: []u16,
    barriers: []u8,
    quant_dispatches: []u8,
    command_ops: []ops.PlannedCommandOp,
};

pub fn populatePlannedCommandContractStorage(
    plan: metal_command_planner.GraphCommandPlanView,
    storage: PlannedCommandContractStorage,
) bool {
    if (plan.planned_ops.len > storage.ops.len or
        plan.planned_ops.len > storage.barriers.len or
        plan.ops.len > storage.quant_dispatches.len or
        plan.ops.len > storage.command_ops.len)
    {
        return false;
    }
    @memset(storage.quant_dispatches[0..plan.ops.len], 255);
    for (plan.planned_ops, 0..) |planned, index| {
        storage.ops[index] = @intFromEnum(planned.kind);
        storage.barriers[index] = @intFromBool(planned.barrier_before);
    }
    for (plan.ops, 0..) |op, index| {
        const quant_dispatch = if (op.quant_matmul) |quant|
            @intFromEnum(quant.dispatch)
        else
            255;
        const operator: u8 = if (op.operator_plan) |operator_plan|
            @intFromEnum(operator_plan.operator())
        else
            255;
        const format: u8 = if (op.operator_plan) |operator_plan|
            plannedOperatorFormat(operator_plan)
        else
            255;
        storage.quant_dispatches[index] = quant_dispatch;
        storage.command_ops[index] = .{
            .kind = @intFromEnum(op.kind),
            .barrier_before = @intFromBool(op.barrier_before),
            .quant_dispatch = quant_dispatch,
            .operator = operator,
            .format = format,
            .input_dtype = @intFromEnum(op.input_dtype),
            .output_dtype = @intFromEnum(op.output_dtype),
            .source = @intCast(op.source),
            .region = @intCast(op.region),
            .scope_index = @intCast(op.scope_index),
            .resource_start = op.resource_start,
            .resource_count = op.resource_count,
        };
    }
    return true;
}

pub fn plannedContractWindowFromStorage(
    storage: PlannedCommandContractStorage,
    start_index: usize,
    end_index: usize,
) ops.PlannedLayerContract {
    if (start_index >= end_index) return .{};
    if (end_index > storage.ops.len or
        end_index > storage.barriers.len or
        end_index > storage.quant_dispatches.len or
        end_index > storage.command_ops.len)
    {
        return .{};
    }
    return .{
        .ops = storage.ops[0..end_index],
        .barriers = storage.barriers[0..end_index],
        .quant_dispatches = storage.quant_dispatches[0..end_index],
        .command_ops = storage.command_ops[0..end_index],
        .start_index = start_index,
    };
}

fn plannedOperatorFormat(operator: metal_command_planner.OperatorPlan) u8 {
    return switch (operator) {
        .quant_matmul => |plan| @intCast(@intFromEnum(plan.format)),
        .quant_row => |plan| @intCast(@intFromEnum(plan.format)),
        .quant_copy => |plan| @intCast(@intFromEnum(plan.format)),
        .attention => |plan| @intFromEnum(plan.kv_format),
    };
}

pub fn frameCommandBufferCount(runtime: ?*RawMetalDecodeRuntime) u64 {
    return termite_metal_decode_runtime_frame_cb_count(runtime);
}

pub fn lastFrameGpuNanos(runtime: ?*RawMetalDecodeRuntime) u64 {
    return termite_metal_decode_runtime_last_frame_gpu_nanos(runtime);
}

pub fn runtimeMemorySnapshot(runtime: ?*RawMetalDecodeRuntime) RawRuntimeMemoryStats {
    var snapshot: RawRuntimeMemoryStats = .{};
    _ = termite_metal_decode_runtime_memory_snapshot(runtime, &snapshot);
    return snapshot;
}

pub extern fn termite_metal_decode_runtime_prepare_decoder_only_greedy(
    runtime: ?*RawMetalDecodeRuntime,
    hidden_size: usize,
    intermediate_size: usize,
    num_layers: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    vocab_size: usize,
    kv_tokens: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reserve_prefill_layer_scratch(
    runtime: ?*RawMetalDecodeRuntime,
    rows: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    hidden_size: usize,
    intermediate_size: usize,
    tail_vocab_size: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reserve_gated_ffn_scratch(
    runtime: ?*RawMetalDecodeRuntime,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reserve_greedy_tail_scratch(
    runtime: ?*RawMetalDecodeRuntime,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reserve_sample_tail_scratch(
    runtime: ?*RawMetalDecodeRuntime,
    out_dim: usize,
    top_k: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reserve_attention_span_scratch(
    runtime: ?*RawMetalDecodeRuntime,
    kv_tokens: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reset_state(runtime: ?*RawMetalDecodeRuntime) c_int;
pub extern fn termite_metal_decode_runtime_prepare_absolute_embeddings(
    runtime: ?*RawMetalDecodeRuntime,
    token_embedding: [*c]const f32,
    vocab_size: usize,
    position_embedding: [*c]const f32,
    max_position_embeddings: usize,
    hidden_size: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_embed_absolute_position(
    runtime: ?*RawMetalDecodeRuntime,
    token_id: usize,
    position_id: usize,
    hidden_size: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_embedding_table(
    runtime: ?*RawMetalDecodeRuntime,
    weight: [*c]const f32,
    rows: usize,
    dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_embedding_table_bf16(
    runtime: ?*RawMetalDecodeRuntime,
    weight: [*c]const u8,
    weight_bytes: usize,
    rows: usize,
    dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_embedding_table_device(
    runtime: ?*RawMetalDecodeRuntime,
    src_handle: ?*anyopaque,
    src_offset: usize,
    rows: usize,
    dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_quant_embedding_table(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    rows: usize,
    dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_embedding_lookup_prepared_device(
    runtime: ?*RawMetalDecodeRuntime,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_embedding_lookup_direct_device(
    runtime: ?*RawMetalDecodeRuntime,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    rows: usize,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_deberta_embeddings_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    rows: usize,
    gamma_handle: ?*anyopaque,
    gamma_offset: usize,
    beta_handle: ?*anyopaque,
    beta_offset: usize,
    ids: [*c]const u32,
    mask: [*c]const u32,
    total: usize,
    dim: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_deberta_embeddings_f32(
    runtime: ?*RawMetalDecodeRuntime,
    weight: [*c]const f32,
    rows: usize,
    gamma: [*c]const f32,
    beta: [*c]const f32,
    ids: [*c]const u32,
    mask: [*c]const u32,
    total: usize,
    dim: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_embedding_lookup_bf16_prepared_device(
    runtime: ?*RawMetalDecodeRuntime,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_quant_embedding_lookup_prepared_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_q8_0_get_rows_linear_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_quant_get_rows_linear_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_q8_0_copy_linear_slot_to_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    row_offset: usize,
    rows: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_quant_copy_linear_slot_to_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    row_offset: usize,
    rows: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_embedding_lookup(
    runtime: ?*RawMetalDecodeRuntime,
    weight: [*c]const f32,
    rows: usize,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rope(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    positions: [*c]const u32,
    total_chunks: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    consecutive_pairs: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rope_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    positions: [*c]const u32,
    total_chunks: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    consecutive_pairs: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_head_rms_rope_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    norm_slot: usize,
    total_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    position: usize,
    theta: f32,
    freq_scale: f32,
    eps: f32,
    value_scale: f32,
    consecutive_pairs: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_head_rms_rope_batched_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    norm_slot: usize,
    total_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    position: usize,
    theta: f32,
    freq_scale: f32,
    eps: f32,
    value_scale: f32,
    consecutive_pairs: u32,
    heads_per_row: usize,
    position_period: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_head_rms_rope_scratch_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    norm_slot: usize,
    total_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    position: usize,
    theta: f32,
    freq_scale: f32,
    eps: f32,
    value_scale: f32,
    consecutive_pairs: u32,
    heads_per_row: usize,
    output_index: usize,
    output_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_f32(
    runtime: ?*RawMetalDecodeRuntime,
    q: [*c]const f32,
    k: [*c]const f32,
    v: [*c]const f32,
    bias: [*c]const f32,
    mask: [*c]const u8,
    q_len: usize,
    kv_len: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    total_sequence_len: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    q_len: usize,
    kv_len: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    bias_host: [*c]const f32,
    mask: [*c]const u8,
    total_sequence_len: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_compressed_attention_store_local_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    local_handle: ?*anyopaque,
    local_offset: usize,
    query_rows: usize,
    query_abs_start: usize,
    head_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_compressed_attention_update_component_device(
    runtime: ?*RawMetalDecodeRuntime,
    projected_handle: ?*anyopaque,
    projected_offset: usize,
    gate_handle: ?*anyopaque,
    gate_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    bias_rows: usize,
    norm_handle: ?*anyopaque,
    norm_offset: usize,
    projected_cache_handle: ?*anyopaque,
    projected_cache_offset: usize,
    gate_cache_handle: ?*anyopaque,
    gate_cache_offset: usize,
    compressed_handle: ?*anyopaque,
    compressed_offset: usize,
    positions_handle: ?*anyopaque,
    positions_offset: usize,
    query_rows: usize,
    query_abs_start: usize,
    total_tokens: usize,
    compress_rate: usize,
    row_dim: usize,
    gate_width: usize,
    row_count: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    eps: f32,
    consecutive_pairs: u32,
) c_int;
pub extern fn termite_metal_decode_runtime_compressed_attention_hybrid_attention_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_handle: ?*anyopaque,
    q_offset: usize,
    local_handle: ?*anyopaque,
    local_offset: usize,
    compressed_handle: ?*anyopaque,
    compressed_offset: usize,
    positions_handle: ?*anyopaque,
    positions_offset: usize,
    index_handle: ?*anyopaque,
    index_offset: usize,
    index_positions_handle: ?*anyopaque,
    index_positions_offset: usize,
    index_query_handle: ?*anyopaque,
    index_query_offset: usize,
    index_head_weights_handle: ?*anyopaque,
    index_head_weights_offset: usize,
    selected_indices_handle: ?*anyopaque,
    selected_indices_offset: usize,
    selected_scores_handle: ?*anyopaque,
    selected_scores_offset: usize,
    sinks_handle: ?*anyopaque,
    sinks_offset: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
    query_abs_start: usize,
    query_rows: usize,
    token_count: usize,
    compressed_rows: usize,
    num_heads: usize,
    head_dim: usize,
    sliding_window: usize,
    top_k: usize,
    index_rows: usize,
    index_heads: usize,
    index_head_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_layer_norm(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight: [*c]const f32,
    bias: [*c]const f32,
    hidden_size: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_layer_norm(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_layer_norm_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    hidden_size: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_add_layer_norm_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    a_handle: ?*anyopaque,
    a_offset: usize,
    b_handle: ?*anyopaque,
    b_offset: usize,
    rows: usize,
    hidden_size: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_rms_norm(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight: [*c]const f32,
    hidden_size: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_rms_norm_from_buffer(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    hidden_size: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    hidden_size: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_scratch_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    hidden_size: usize,
    eps: f32,
    output_index: usize,
    output_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_rows_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    hidden_size: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_weight_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    rows: usize,
    hidden_size: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_weight_scratch_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    rows: usize,
    hidden_size: usize,
    eps: f32,
    output_index: usize,
    output_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_linear(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight: [*c]const f32,
    bias: [*c]const f32,
    in_dim: usize,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_linear_bf16(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight: [*c]const u8,
    weight_bytes: usize,
    bias: [*c]const f32,
    in_dim: usize,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_linear_bf16_no_copy(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight: [*c]const u8,
    weight_bytes: usize,
    bias: [*c]const f32,
    in_dim: usize,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_linear_bias(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    bias: [*c]const f32,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_quantized_linear_slot(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    in_dim: usize,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_quantized_linear_slot_no_copy(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    in_dim: usize,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_bitnet_tl1_linear_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    in_dim: usize,
    out_dim: usize,
    packed_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
) c_int;
pub extern fn termite_metal_decode_runtime_prepare_bitnet_tl2_linear_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    in_dim: usize,
    out_dim: usize,
    scale_off: u32,
    three_value_len: u32,
    three_sign_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
    three_cols: u32,
    two_cols: u32,
) c_int;
pub extern fn termite_metal_decode_runtime_quant_copy_f32_to_linear_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    row_offset: usize,
    rows: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
    input_handle: ?*anyopaque,
    input_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_quant_set_rows_linear_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    ids: [*c]const u32,
    total: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
    input_handle: ?*anyopaque,
    input_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input: [*c]const f32,
    in_dim: usize,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_multi_row(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_multi_row_device(
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
pub extern fn termite_metal_decode_runtime_apply_dense_mlp2_device(
    runtime: ?*RawMetalDecodeRuntime,
    first_linear_slot: usize,
    second_linear_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    hidden_dim: usize,
    out_dim: usize,
    activation_kind: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_dense_ffn_layer_norm_device(
    runtime: ?*RawMetalDecodeRuntime,
    first_linear_slot: usize,
    second_linear_slot: usize,
    layer_norm_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_dense_linear_layer_norm_device(
    runtime: ?*RawMetalDecodeRuntime,
    linear_slot: usize,
    layer_norm_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    rows: usize,
    in_dim: usize,
    hidden_size: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_i2_s_linear_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_linear_slot_host(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_linear_slot_device(
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
pub extern fn termite_metal_decode_runtime_apply_linear_bias_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
    rows: usize,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_linear_pair_slots(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot_a: usize,
    slot_b: usize,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_a: [*c]f32,
    output_b: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_linear_pair_slots_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot_a: usize,
    slot_b: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_a_handle: ?*anyopaque,
    output_a_offset: usize,
    output_b_handle: ?*anyopaque,
    output_b_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_pair_slots(
    runtime: ?*RawMetalDecodeRuntime,
    slot_a: usize,
    slot_b: usize,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_a: [*c]f32,
    output_b: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_pair_slots_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot_a: usize,
    slot_b: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_a_handle: ?*anyopaque,
    output_a_offset: usize,
    output_b_handle: ?*anyopaque,
    output_b_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_qkv_slots(
    runtime: ?*RawMetalDecodeRuntime,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
    q_output: [*c]f32,
    k_output: [*c]f32,
    v_output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_qkv_slots_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
    q_output_handle: ?*anyopaque,
    q_output_offset: usize,
    k_output_handle: ?*anyopaque,
    k_output_offset: usize,
    v_output_handle: ?*anyopaque,
    v_output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_dense_linear_qkv_slots_scratch_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
    q_output_handle: *?*anyopaque,
    k_output_handle: *?*anyopaque,
    v_output_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_dense_linear_pair_slots_scratch_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot_a: usize,
    slot_b: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_a_handle: *?*anyopaque,
    output_b_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_q_kv_pair_linear_qkv_slots(
    runtime: ?*RawMetalDecodeRuntime,
    q_format: u32,
    kv_format: u32,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
    q_output: [*c]f32,
    k_output: [*c]f32,
    v_output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_linear_qkv_slots_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_format: u32,
    k_format: u32,
    v_format: u32,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
    q_output_handle: ?*anyopaque,
    q_output_offset: usize,
    k_output_handle: ?*anyopaque,
    k_output_offset: usize,
    v_output_handle: ?*anyopaque,
    v_output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_linear_slot_scratch_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_prefill_quantized_setup_device(
    runtime: ?*RawMetalDecodeRuntime,
    shares_kv: c_int,
    q_format: u32,
    k_format: u32,
    v_format: u32,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    hidden_size: usize,
    attention_input_size: usize,
    kv_dim: usize,
    q_norm_slot: usize,
    k_norm_slot: usize,
    value_norm_present: c_int,
    value_norm_weight_handle: ?*anyopaque,
    value_norm_weight_offset: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    position: usize,
    theta: f32,
    freq_scale: f32,
    eps: f32,
    query_value_scale: f32,
    consecutive_pairs: u32,
    planned_contract: RawPlannedLayerContract,
    keep_scope_open_after: c_int,
    q_ready_handle: *?*anyopaque,
    k_ready_handle: *?*anyopaque,
    v_ready_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_prefill_gated_frame_layer_q8_0_device(
    runtime: ?*RawMetalDecodeRuntime,
    shares_kv: c_int,
    q_format: u32,
    k_format: u32,
    v_format: u32,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    hidden_size: usize,
    attention_input_size: usize,
    kv_dim: usize,
    q_norm_slot: usize,
    k_norm_slot: usize,
    value_norm_present: c_int,
    value_norm_weight_handle: ?*anyopaque,
    value_norm_weight_offset: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    position: usize,
    theta: f32,
    freq_scale: f32,
    eps: f32,
    query_value_scale: f32,
    consecutive_pairs: u32,
    setup_contract: RawPlannedLayerContract,
    kv_tokens: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    total_sequence_len: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    ffn_post_down_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    ple_handle: ?*anyopaque,
    ple_offset: usize,
    ple_gate_linear_slot: usize,
    ple_proj_linear_slot: usize,
    ple_post_norm_slot: usize,
    ple_hidden_size: usize,
    output_scale_present: c_int,
    output_scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
    block_contract: RawPlannedLayerContract,
    timing: ?*RawAttentionGatedBlockTiming,
    paged_slot: usize,
    paged_format: u32,
    block_table: ?[*]const u32,
    block_count: usize,
    page_size: usize,
    paged_key_row_bytes: usize,
    paged_base_key_row_bytes: usize,
    paged_v_row_stride: usize,
    suffix_tokens: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_quantized_linear_qkv_slots_scratch_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_format: u32,
    k_format: u32,
    v_format: u32,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
    q_output_handle: *?*anyopaque,
    k_output_handle: *?*anyopaque,
    v_output_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_argmax(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input: [*c]const f32,
    in_dim: usize,
    out_dim: usize,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_argmax_device(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    in_dim: usize,
    out_dim: usize,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_argmax_from_logits_device(
    runtime: ?*RawMetalDecodeRuntime,
    logits_handle: ?*anyopaque,
    logits_offset: usize,
    out_dim: usize,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_encode_argmax_from_logits_device(
    runtime: ?*RawMetalDecodeRuntime,
    logits_handle: ?*anyopaque,
    logits_offset: usize,
    out_dim: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_layer_norm_linear_argmax(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_layer_norm_linear(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_layer_norm_linear_sample_device(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    temperature: f32,
    top_k: usize,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
    penalty_token_ids: ?*const u32,
    penalty_counts: ?*const u32,
    penalty_count: usize,
    seed: u32,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_linear_argmax(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_encode_rms_norm_linear_argmax_device(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input_handle: ?*anyopaque,
    input_offset: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    planned_contract: RawPlannedLayerContract,
) c_int;
pub extern fn termite_metal_decode_runtime_encode_rms_norm_quantized_linear_argmax_device(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    format: u32,
    input_handle: ?*anyopaque,
    input_offset: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    planned_contract: RawPlannedLayerContract,
) c_int;
pub extern fn termite_metal_decode_runtime_encode_rms_norm_quantized_linear_logits_device(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    format: u32,
    input_handle: ?*anyopaque,
    input_offset: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    planned_contract: RawPlannedLayerContract,
    logits_handle: *?*anyopaque,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_ple_residual_q8_0_device(
    runtime: ?*RawMetalDecodeRuntime,
    hidden_handle: ?*anyopaque,
    hidden_offset: usize,
    ple_handle: ?*anyopaque,
    ple_offset: usize,
    gate_linear_slot: usize,
    proj_linear_slot: usize,
    post_norm_slot: usize,
    rows: usize,
    hidden_size: usize,
    ple_hidden_size: usize,
    eps: f32,
    activation_kind: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_output_residual_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    attention_output_handle: ?*anyopaque,
    attention_output_offset: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    rows: usize,
    attention_input_size: usize,
    hidden_size: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_read_token_id(
    runtime: ?*RawMetalDecodeRuntime,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_linear(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_q4_k_linear_slot(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_q5_k_linear_slot(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_linear_sample_device(
    runtime: ?*RawMetalDecodeRuntime,
    norm_slot: usize,
    linear_slot: usize,
    input: [*c]const f32,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    temperature: f32,
    top_k: usize,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
    penalty_token_ids: ?*const u32,
    penalty_counts: ?*const u32,
    penalty_count: usize,
    seed: u32,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_sample_from_logits_device(
    runtime: ?*RawMetalDecodeRuntime,
    logits_handle: ?*anyopaque,
    logits_offset: usize,
    out_dim: usize,
    temperature: f32,
    top_k: usize,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
    penalty_token_ids: ?*const u32,
    penalty_counts: ?*const u32,
    penalty_count: usize,
    seed: u32,
    output_token_id: [*c]u32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_activation(
    runtime: ?*RawMetalDecodeRuntime,
    activation_kind: u32,
    input: [*c]const f32,
    dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_activation_device(
    runtime: ?*RawMetalDecodeRuntime,
    activation_kind: u32,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_softmax_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    dim: usize,
    log_softmax: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reduce_last_dim_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    dim: usize,
    kind: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_multiply_reduce_last_dim_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    rows: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_broadcast_last_dim_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reduce_axis_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    out_strides: [*]const u32,
    input_strides_for_out: [*]const u32,
    rank: usize,
    input_elems: usize,
    output_elems: usize,
    reduce_dim: usize,
    reduce_stride: usize,
    kind: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_broadcast_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    out_strides: [*]const u32,
    input_strides_for_out: [*]const u32,
    rank: usize,
    input_elems: usize,
    output_elems: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_gather_axis0_f32_2d_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    indices_handle: ?*anyopaque,
    indices_offset: usize,
    rows: usize,
    cols: usize,
    index_count: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_gliner_word_embeddings_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    hidden_handle: ?*anyopaque,
    hidden_offset: usize,
    words_mask_handle: ?*anyopaque,
    words_mask_offset: usize,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
    num_words: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_concat_lastdim_f32_2d_device(
    runtime: ?*RawMetalDecodeRuntime,
    a_handle: ?*anyopaque,
    a_offset: usize,
    b_handle: ?*anyopaque,
    b_offset: usize,
    rows: usize,
    dim_a: usize,
    dim_b: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_gliner_gru_combine_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    label_handle: ?*anyopaque,
    label_offset: usize,
    gi_handle: ?*anyopaque,
    gi_offset: usize,
    gh_handle: ?*anyopaque,
    gh_offset: usize,
    rows: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_argmax_axis_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    outer: usize,
    axis_dim: usize,
    inner: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_convert_dtype_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    elem_count: usize,
    kind: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_sdpa_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    mask_handle: ?*anyopaque,
    mask_offset: usize,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    bias_mode: u32,
    has_mask: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_disentangled_relative_attention_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    q_r_handle: ?*anyopaque,
    q_r_offset: usize,
    k_r_handle: ?*anyopaque,
    k_r_offset: usize,
    mask_handle: ?*anyopaque,
    mask_offset: usize,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    has_mask: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_transpose_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    dims: [*c]const u32,
    in_strides: [*c]const u32,
    out_strides: [*c]const u32,
    perm: [*c]const u32,
    rank: usize,
    total: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_dot_general_2d_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    m: usize,
    n: usize,
    k: usize,
    rhs_contract_axis: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_dot_general_batched_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    batch_count: usize,
    m: usize,
    n: usize,
    k: usize,
    rhs_contract_axis: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_conv1d_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    time_steps: usize,
    kernel_size: usize,
    stride: usize,
    padding: usize,
    out_time: usize,
    has_bias: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_conv2d_f32_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    bias_handle: ?*anyopaque,
    bias_offset: usize,
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
    out_h: usize,
    out_w: usize,
    has_bias: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_activation_multiply_device(
    runtime: ?*RawMetalDecodeRuntime,
    activation_kind: u32,
    gate_handle: ?*anyopaque,
    gate_offset: usize,
    up_handle: ?*anyopaque,
    up_offset: usize,
    rows: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_add(
    runtime: ?*RawMetalDecodeRuntime,
    lhs: [*c]const f32,
    rhs: [*c]const f32,
    dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_add_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_training_accumulate_f32(
    runtime: ?*RawMetalDecodeRuntime,
    accum_handle: ?*anyopaque,
    accum_offset: usize,
    grad_handle: ?*anyopaque,
    grad_offset: usize,
    elem_count: usize,
    scale: f32,
    first: u32,
) c_int;
pub extern fn termite_metal_decode_runtime_training_adamw_f32(
    runtime: ?*RawMetalDecodeRuntime,
    weight_handle: ?*anyopaque,
    weight_offset: usize,
    grad_handle: ?*anyopaque,
    grad_offset: usize,
    m_handle: ?*anyopaque,
    m_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    elem_count: usize,
    lr: f32,
    beta1: f32,
    beta2: f32,
    eps: f32,
    weight_decay: f32,
    bias_correction1: f32,
    bias_correction2: f32,
    grad_scale: f32,
) c_int;
pub extern fn termite_metal_decode_runtime_training_sumsq_f32(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    elem_count: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_add_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    rows: usize,
    hidden_size: usize,
    eps: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_add_scale_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_scaled_add_scale_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    lhs_scale: f32,
    output_scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_multiply(
    runtime: ?*RawMetalDecodeRuntime,
    lhs: [*c]const f32,
    rhs: [*c]const f32,
    dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_multiply_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_multiply_device_rhs_repeat(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    rhs_period: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_subtract_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_subtract_device_broadcast(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    lhs_scalar: c_int,
    rhs_scalar: c_int,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_divide_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_divide_device_broadcast(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    lhs_scalar: c_int,
    rhs_scalar: c_int,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_divide_device_rhs_repeat(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    rhs_period: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_less_than_device(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_less_than_device_broadcast(
    runtime: ?*RawMetalDecodeRuntime,
    lhs_handle: ?*anyopaque,
    lhs_offset: usize,
    rhs_handle: ?*anyopaque,
    rhs_offset: usize,
    dim: usize,
    lhs_scalar: c_int,
    rhs_scalar: c_int,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_where_select_device(
    runtime: ?*RawMetalDecodeRuntime,
    cond_handle: ?*anyopaque,
    cond_offset: usize,
    true_handle: ?*anyopaque,
    true_offset: usize,
    false_handle: ?*anyopaque,
    false_offset: usize,
    dim: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_where_select_device_broadcast(
    runtime: ?*RawMetalDecodeRuntime,
    cond_handle: ?*anyopaque,
    cond_offset: usize,
    true_handle: ?*anyopaque,
    true_offset: usize,
    false_handle: ?*anyopaque,
    false_offset: usize,
    dim: usize,
    cond_scalar: c_int,
    true_scalar: c_int,
    false_scalar: c_int,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_scale_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    dim: usize,
    scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_activation_linear_residual(
    runtime: ?*RawMetalDecodeRuntime,
    first_linear_slot: usize,
    second_linear_slot: usize,
    input: [*c]const f32,
    residual: [*c]const f32,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_linear_pair_activation_multiply_linear_residual(
    runtime: ?*RawMetalDecodeRuntime,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    down_linear_slot: usize,
    post_down_rms_norm_slot: usize,
    input: [*c]const f32,
    residual: [*c]const f32,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_update_attention_span(
    runtime: ?*RawMetalDecodeRuntime,
    encoded_key: [*c]const u8,
    v: [*c]const f32,
    kv_tokens: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    position_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_append_attention_span(
    runtime: ?*RawMetalDecodeRuntime,
    encoded_key_suffix: [*c]const u8,
    v_suffix: [*c]const f32,
    kv_tokens: usize,
    suffix_tokens: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    position_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_update_attention_span_from_f32_key_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    kv_tokens: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    v_row_stride: usize,
    kv_position_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_update_attention_span_from_f32_key_device_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    format: u32,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    kv_tokens: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    v_row_stride: usize,
    kv_position_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_update_attention_paged_from_f32_key_device_slot(
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
pub extern fn termite_metal_decode_runtime_attention_span_slot_info(
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
pub extern fn termite_metal_decode_runtime_reset_attention_span_slot(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
    runtime: ?*RawMetalDecodeRuntime,
    slot: usize,
    format: u32,
    token_capacity: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
) c_int;

/// Mirror of the C-side `TERMITE_METAL_ATTENTION_SPAN_SLOT_CAPACITY` used to
/// bound slot indices on the Zig side. Keep in sync with metal_kernels.m.
pub const attention_span_slot_capacity: usize = 256;
pub extern fn termite_metal_decode_runtime_attention_span(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_attention_span_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q_handle: ?*anyopaque,
    q_offset: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_attention_paged_slot_device(
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
pub extern fn termite_metal_decode_runtime_apply_attention_dense_block(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q: [*c]const f32,
    encoded_key: [*c]const u8,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    first_ffn_linear_slot: usize,
    second_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_dense_block_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q_handle: ?*anyopaque,
    q_offset: usize,
    encoded_key: [*c]const u8,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    first_ffn_linear_slot: usize,
    second_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_dense_block_device_kv_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    first_ffn_linear_slot: usize,
    second_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_gated_block(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q: [*c]const f32,
    k: [*c]const f32,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    output: [*c]f32,
    timing: ?*RawAttentionGatedBlockTiming,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_gated_block_q_slot(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    attention_input: [*c]const f32,
    q_linear_slot: usize,
    k: [*c]const f32,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    hidden_size: usize,
    attention_input_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    output: [*c]f32,
    timing: ?*RawAttentionGatedBlockTiming,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_gated_block_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k: [*c]const f32,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
    timing: ?*RawAttentionGatedBlockTiming,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_gated_block_device_kv_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
    timing: ?*RawAttentionGatedBlockTiming,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_gated_block_q8_0_device_kv_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    ffn_post_down_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
    timing: ?*RawAttentionGatedBlockTiming,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_f32_gated_block_q8_0_device_kv_device(
    runtime: ?*RawMetalDecodeRuntime,
    q_handle: ?*anyopaque,
    q_offset: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    q_len: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    total_sequence_len: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    ffn_post_down_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    ple_handle: ?*anyopaque,
    ple_offset: usize,
    ple_gate_linear_slot: usize,
    ple_proj_linear_slot: usize,
    ple_post_norm_slot: usize,
    ple_hidden_size: usize,
    output_scale_present: c_int,
    output_scale: f32,
    output_handle: ?*anyopaque,
    output_offset: usize,
    planned_contract: RawPlannedLayerContract,
    timing: ?*RawAttentionGatedBlockTiming,
    paged_slot: usize,
    paged_format: u32,
    block_table: ?[*]const u32,
    block_count: usize,
    page_size: usize,
    paged_key_row_bytes: usize,
    paged_base_key_row_bytes: usize,
    paged_v_row_stride: usize,
    suffix_tokens: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_gated_block_q_slot_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    attention_input_handle: ?*anyopaque,
    attention_input_offset: usize,
    q_linear_slot: usize,
    k: [*c]const f32,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    hidden_size: usize,
    attention_input_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
    timing: ?*RawAttentionGatedBlockTiming,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_gated_block_q_slot_device_kv_device(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    attention_input_handle: ?*anyopaque,
    attention_input_offset: usize,
    q_linear_slot: usize,
    k_handle: ?*anyopaque,
    k_offset: usize,
    v_handle: ?*anyopaque,
    v_offset: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    hidden_size: usize,
    attention_input_size: usize,
    eps: f32,
    ffn_layer_norm_slot: usize,
    ffn_rms_norm_slot: usize,
    ffn_post_gate_rms_norm_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation_kind: u32,
    layer_index: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
    timing: ?*RawAttentionGatedBlockTiming,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_residual_tl1(
    runtime: ?*RawMetalDecodeRuntime,
    attention_input: [*c]const f32,
    attention_pre_linear_rms_norm_slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    packed_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_residual_tl2(
    runtime: ?*RawMetalDecodeRuntime,
    attention_input: [*c]const f32,
    attention_pre_linear_rms_norm_slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    scale_off: u32,
    three_value_len: u32,
    three_sign_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
    three_cols: u32,
    two_cols: u32,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_residual_i2_s_slot(
    runtime: ?*RawMetalDecodeRuntime,
    attention_input: [*c]const f32,
    attention_pre_linear_rms_norm_slot: usize,
    attention_linear_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_residual_q4_k_slot(
    runtime: ?*RawMetalDecodeRuntime,
    attention_input: [*c]const f32,
    attention_pre_linear_rms_norm_slot: usize,
    attention_linear_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_residual_q5_k_slot(
    runtime: ?*RawMetalDecodeRuntime,
    attention_input: [*c]const f32,
    attention_pre_linear_rms_norm_slot: usize,
    attention_linear_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_residual_q8_0_slot(
    runtime: ?*RawMetalDecodeRuntime,
    attention_input: [*c]const f32,
    attention_pre_linear_rms_norm_slot: usize,
    attention_linear_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_span_residual_tl1(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q: [*c]const f32,
    encoded_key: [*c]const u8,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_pre_linear_rms_norm_slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    packed_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_span_residual_i2_s_slot(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q: [*c]const f32,
    encoded_key: [*c]const u8,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_linear_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_f32_span_residual_q8_0_slot(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q: [*c]const f32,
    k: [*c]const f32,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_pre_linear_rms_norm_slot: usize,
    attention_linear_slot: usize,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    layer_index: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_attention_span_residual_tl2(
    runtime: ?*RawMetalDecodeRuntime,
    format: u32,
    q: [*c]const f32,
    encoded_key: [*c]const u8,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    attention_pre_linear_rms_norm_slot: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    scale_off: u32,
    three_value_len: u32,
    three_sign_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
    three_cols: u32,
    two_cols: u32,
    attention_post_linear_rms_norm_slot: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_tl1(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_weight_raw: [*c]const u8,
    gate_weight_bytes: usize,
    gate_packed_len: u32,
    gate_bm: u32,
    gate_cfg_by: u32,
    gate_bmm: u32,
    up_weight_raw: [*c]const u8,
    up_weight_bytes: usize,
    up_packed_len: u32,
    up_bm: u32,
    up_cfg_by: u32,
    up_bmm: u32,
    post_gate_rms_norm_slot: usize,
    down_weight_raw: [*c]const u8,
    down_weight_bytes: usize,
    down_packed_len: u32,
    down_bm: u32,
    down_cfg_by: u32,
    down_bmm: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_tl2(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_weight_raw: [*c]const u8,
    gate_weight_bytes: usize,
    gate_scale_off: u32,
    gate_three_value_len: u32,
    gate_three_sign_len: u32,
    gate_bm: u32,
    gate_cfg_by: u32,
    gate_bmm: u32,
    gate_three_cols: u32,
    gate_two_cols: u32,
    up_weight_raw: [*c]const u8,
    up_weight_bytes: usize,
    up_scale_off: u32,
    up_three_value_len: u32,
    up_three_sign_len: u32,
    up_bm: u32,
    up_cfg_by: u32,
    up_bmm: u32,
    up_three_cols: u32,
    up_two_cols: u32,
    post_gate_rms_norm_slot: usize,
    down_weight_raw: [*c]const u8,
    down_weight_bytes: usize,
    down_scale_off: u32,
    down_three_value_len: u32,
    down_three_sign_len: u32,
    down_bm: u32,
    down_cfg_by: u32,
    down_bmm: u32,
    down_three_cols: u32,
    down_two_cols: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_i2_s(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_weight_raw: [*c]const u8,
    gate_weight_bytes: usize,
    up_weight_raw: [*c]const u8,
    up_weight_bytes: usize,
    post_gate_rms_norm_slot: usize,
    down_weight_raw: [*c]const u8,
    down_weight_bytes: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_i2_s_slots(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_slots(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q5_k_down_slots(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q6_k_down_slots(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q6_k_slots(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q4_0_pair_q8_0_down_slots(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q8_0_slots(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    eps: f32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q8_0_slots_device(
    runtime: ?*RawMetalDecodeRuntime,
    input_handle: ?*anyopaque,
    input_offset: usize,
    residual_handle: ?*anyopaque,
    residual_offset: usize,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    eps: f32,
    down_linear_slot: usize,
    output_handle: ?*anyopaque,
    output_offset: usize,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_gated_ffn_residual_q8_0(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation_kind: u32,
    gate_weight_raw: [*c]const u8,
    gate_weight_bytes: usize,
    up_weight_raw: [*c]const u8,
    up_weight_bytes: usize,
    post_gate_rms_norm_slot: usize,
    post_down_rms_norm_slot: usize,
    down_weight_raw: [*c]const u8,
    down_weight_bytes: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_i2_s_linear_residual(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    intermediate_size: usize,
    hidden_size: usize,
    post_gate_rms_norm_slot: usize,
    down_weight_raw: [*c]const u8,
    down_weight_bytes: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_decode_runtime_apply_rms_norm_i2_s_linear_residual_slot(
    runtime: ?*RawMetalDecodeRuntime,
    input: [*c]const f32,
    residual: [*c]const f32,
    intermediate_size: usize,
    hidden_size: usize,
    post_gate_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q4_0(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q4_1(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q5_0(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q5_1(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q8_0(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q8_1(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q8_0_planned(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    planned_dispatch: u8,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q2_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q3_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q4_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q5_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q6_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_iq4_xs(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_iq4_nl(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_mxfp4(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_q8_k(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    out_dim: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_tl1(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    out_dim: usize,
    packed_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_linear_tl2(
    provider: ?*RawMetalProvider,
    input: [*c]const f32,
    rows: usize,
    in_dim: usize,
    weight_raw: [*c]const u8,
    weight_bytes: usize,
    out_dim: usize,
    scale_off: u32,
    three_value_len: u32,
    three_sign_len: u32,
    bm: u32,
    cfg_by: u32,
    bmm: u32,
    three_cols: u32,
    two_cols: u32,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_compressed_key_scores_polar4(
    provider: ?*RawMetalProvider,
    q: [*c]const f32,
    q_len: usize,
    encoded_key: [*c]const u8,
    block_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    output: [*c]f32,
) c_int;
pub extern fn termite_metal_provider_compressed_key_scores_turbo3(
    provider: ?*RawMetalProvider,
    q: [*c]const f32,
    q_len: usize,
    encoded_key: [*c]const u8,
    block_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    output: [*c]f32,
) c_int;

pub fn quantizedRuntimeLinearKind(storage: *const QuantizedStorage) RawQuantizedRuntimeLinearKind {
    return switch (storage.tensor_type) {
        .known => |known| switch (known) {
            .Q1_0 => .q1_0,
            .I2_S => .i2_s,
            .I8_S => .i8_s,
            .Q2_K => .q2_k,
            .Q3_K => .q3_k,
            .Q4_K => .q4_k,
            .Q5_K => .q5_k,
            .Q6_K => .q6_k,
            .Q4_0 => .q4_0,
            .Q4_1 => .q4_1,
            .Q5_0 => .q5_0,
            .Q5_1 => .q5_1,
            .Q8_0 => .q8_0,
            .Q8_1 => .q8_1,
            .Q8_K => .q8_k,
            .IQ1_S => .iq1_s,
            .IQ1_M => .iq1_m,
            .IQ2_XXS => .iq2_xxs,
            .IQ2_XS => .iq2_xs,
            .IQ2_S => .iq2_s,
            .IQ3_XXS => .iq3_xxs,
            .IQ3_S => .iq3_s,
            .IQ4_NL => .iq4_nl,
            .IQ4_XS => .iq4_xs,
            .TQ1_0 => .tq1_0,
            .TQ2_0 => .tq2_0,
            .MXFP4 => .mxfp4,
            .NVFP4 => .nvfp4,
            .TL1 => .tl1,
            else => .none,
        },
        .bitnet_tl2 => .tl2,
        else => .none,
    };
}

pub fn metalQuantFormatForKind(kind: RawQuantizedRuntimeLinearKind) MetalQuantFormat {
    return switch (kind) {
        .none => .unsupported,
        .q1_0 => .q1_0,
        .i2_s => .i2_s,
        .i8_s => .i8_s,
        .q2_k => .q2_k,
        .q3_k => .q3_k,
        .q4_0 => .q4_0,
        .q4_1 => .q4_1,
        .q4_k => .q4_k,
        .q5_0 => .q5_0,
        .q5_1 => .q5_1,
        .q5_k => .q5_k,
        .q6_k => .q6_k,
        .q8_0 => .q8_0,
        .q8_1 => .q8_1,
        .q8_k => .q8_k,
        .iq1_s => .iq1_s,
        .iq1_m => .iq1_m,
        .iq2_xxs => .iq2_xxs,
        .iq2_xs => .iq2_xs,
        .iq2_s => .iq2_s,
        .iq3_xxs => .iq3_xxs,
        .iq3_s => .iq3_s,
        .iq4_nl => .iq4_nl,
        .iq4_xs => .iq4_xs,
        .tq1_0 => .tq1_0,
        .tq2_0 => .tq2_0,
        .mxfp4 => .mxfp4,
        .nvfp4 => .nvfp4,
        .tl1 => .tl1,
        .tl2 => .tl2,
    };
}

pub fn packedWeightDescriptor(storage: *const QuantizedStorage, in_dim: usize) ?PackedWeightDescriptor {
    if (in_dim == 0) return null;
    const values_per_block = gguf_tensor_types.valuesPerBlock(storage.tensor_type) orelse return null;
    const bytes_per_block = gguf_tensor_types.bytesPerBlock(storage.tensor_type) orelse return null;
    if (values_per_block == 0 or bytes_per_block == 0) return null;
    if (in_dim % values_per_block != 0) return null;
    const row_blocks = in_dim / values_per_block;
    const row_stride_bytes = row_blocks * bytes_per_block;
    return .{
        .format = metalQuantFormatForKind(quantizedRuntimeLinearKind(storage)),
        .tensor_type = storage.tensor_type,
        .values_per_block = values_per_block,
        .bytes_per_block = bytes_per_block,
        .row_blocks = row_blocks,
        .row_stride_bytes = row_stride_bytes,
        .raw_bytes = storage.raw_bytes,
    };
}

pub fn packedWeightDescriptorForMatrix(
    storage: *const QuantizedStorage,
    in_dim: usize,
    out_dim: usize,
    expected_format: MetalQuantFormat,
) ?PackedWeightDescriptor {
    const descriptor = packedWeightDescriptor(storage, in_dim) orelse return null;
    if (descriptor.format != expected_format) return null;
    if (!descriptor.supported()) return null;
    if (descriptor.row_stride_bytes * out_dim > descriptor.raw_bytes.len) return null;
    return descriptor;
}

const RuntimeQuantSlotDescriptor = struct {
    runtime: *RawMetalDecodeRuntime,
    storage: *const QuantizedStorage,
    descriptor: PackedWeightDescriptor,
};

const PackedQuantBlockLayout = struct {
    format: MetalQuantFormat,
    values_per_block: usize,
    bytes_per_block: usize,
};

fn packedQuantBlockLayout(kind: RawQuantizedRuntimeLinearKind) ?PackedQuantBlockLayout {
    return switch (kind) {
        .q1_0 => .{ .format = .q1_0, .values_per_block = 128, .bytes_per_block = 18 },
        .i2_s => .{ .format = .i2_s, .values_per_block = 128, .bytes_per_block = 32 },
        .i8_s => .{ .format = .i8_s, .values_per_block = 1, .bytes_per_block = 1 },
        .q2_k => .{ .format = .q2_k, .values_per_block = 256, .bytes_per_block = 84 },
        .q3_k => .{ .format = .q3_k, .values_per_block = 256, .bytes_per_block = 110 },
        .q4_0 => .{ .format = .q4_0, .values_per_block = 32, .bytes_per_block = 18 },
        .q4_1 => .{ .format = .q4_1, .values_per_block = 32, .bytes_per_block = 20 },
        .q4_k => .{ .format = .q4_k, .values_per_block = 256, .bytes_per_block = 144 },
        .q5_0 => .{ .format = .q5_0, .values_per_block = 32, .bytes_per_block = 22 },
        .q5_1 => .{ .format = .q5_1, .values_per_block = 32, .bytes_per_block = 24 },
        .q5_k => .{ .format = .q5_k, .values_per_block = 256, .bytes_per_block = 176 },
        .q6_k => .{ .format = .q6_k, .values_per_block = 256, .bytes_per_block = 210 },
        .q8_0 => .{ .format = .q8_0, .values_per_block = 32, .bytes_per_block = 34 },
        .q8_1 => .{ .format = .q8_1, .values_per_block = 32, .bytes_per_block = 36 },
        .q8_k => .{ .format = .q8_k, .values_per_block = 256, .bytes_per_block = 292 },
        .iq4_nl => .{ .format = .iq4_nl, .values_per_block = 32, .bytes_per_block = 18 },
        .iq4_xs => .{ .format = .iq4_xs, .values_per_block = 256, .bytes_per_block = 136 },
        .mxfp4 => .{ .format = .mxfp4, .values_per_block = 32, .bytes_per_block = 17 },
        .nvfp4 => .{ .format = .nvfp4, .values_per_block = 64, .bytes_per_block = 36 },
        .iq2_xs => .{ .format = .iq2_xs, .values_per_block = 256, .bytes_per_block = 74 },
        else => null,
    };
}

fn runtimeQuantSlotDescriptor(
    self: anytype,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
    expected_format: MetalQuantFormat,
    expected_values_per_block: usize,
    expected_bytes_per_block: usize,
) ?RuntimeQuantSlotDescriptor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_linear_slots_prepared[slot]) return null;
    if (self.raw_linear_slot_kinds[slot] != .quantized) return null;
    if (self.raw_linear_slot_in_dims[slot] != in_dim or self.raw_linear_slot_out_dims[slot] != out_dim) return null;
    const storage = self.raw_linear_slot_quantized_storage[slot] orelse return null;
    const descriptor = packedWeightDescriptorForMatrix(storage, in_dim, out_dim, expected_format) orelse return null;
    if (descriptor.values_per_block != expected_values_per_block or
        descriptor.bytes_per_block != expected_bytes_per_block) return null;
    return .{
        .runtime = runtime,
        .storage = storage,
        .descriptor = descriptor,
    };
}

fn typeHasField(comptime T: type, comptime field_name: []const u8) bool {
    return switch (@typeInfo(T)) {
        .pointer => |ptr| @hasField(ptr.child, field_name),
        else => @hasField(T, field_name),
    };
}

fn setRuntimeQuantPrepareMode(self: anytype, slot: usize, mode: RawQuantizedRuntimeLinearStorageMode) void {
    if (comptime typeHasField(@TypeOf(self), "raw_linear_slot_runtime_prepared_modes")) {
        if (slot < @field(self, "raw_linear_slot_runtime_prepared_modes").len) {
            @field(self, "raw_linear_slot_runtime_prepared_modes")[slot] = mode;
        }
    }
}

fn runtimeQuantPrepareMode(self: anytype, slot: usize) RawQuantizedRuntimeLinearStorageMode {
    if (comptime typeHasField(@TypeOf(self), "raw_linear_slot_runtime_prepared_modes")) {
        if (slot < @field(self, "raw_linear_slot_runtime_prepared_modes").len) {
            return @field(self, "raw_linear_slot_runtime_prepared_modes")[slot];
        }
    }
    return .none;
}

fn addRuntimeQuantField(self: anytype, comptime field_name: []const u8, value: anytype) void {
    if (comptime typeHasField(@TypeOf(self), field_name)) {
        @field(self, field_name) += value;
    }
}

fn addRuntimeQuantPrivatePrepareNanos(self: anytype, nanos: u64) void {
    addRuntimeQuantField(self, "raw_quant_runtime_private_prepare_nanos", nanos);
}

fn addRuntimeQuantMappedPrepareNanos(self: anytype, nanos: u64) void {
    addRuntimeQuantField(self, "raw_quant_runtime_mapped_prepare_nanos", nanos);
}

fn incrementRuntimeQuantMappedAttempts(self: anytype) void {
    addRuntimeQuantField(self, "raw_quant_runtime_mapped_attempts", @as(u64, 1));
}

fn incrementRuntimeQuantMappedFallbacks(self: anytype) void {
    addRuntimeQuantField(self, "raw_quant_runtime_mapped_fallbacks", @as(u64, 1));
}

fn incrementRuntimeQuantMappedFailures(self: anytype) void {
    addRuntimeQuantField(self, "raw_quant_runtime_mapped_failures", @as(u64, 1));
}

fn quantMappedWeightsDisabled() bool {
    return getenvBool("TERMITE_METAL_DISABLE_MAPPED_QUANT_WEIGHTS");
}

fn quantMappedWeightsForced() bool {
    return getenvBool("TERMITE_METAL_FORCE_MAPPED_QUANT_WEIGHTS");
}

fn quantStorageEligibleForMappedRuntime(storage: *const QuantizedStorage, descriptor: PackedWeightDescriptor) bool {
    // Metal's newBufferWithBytesNoCopy requires page-shaped host memory. A
    // borrowed quantized tensor may be stack/heap/synthetic data; only mmap
    // storage advertises a lifetime and backing suitable for this fast path.
    const page_size = std.heap.page_size_min;
    const ptr_value = @intFromPtr(descriptor.raw_bytes.ptr);
    return !storage.raw_owned and
        storage.raw_mmap_backed and
        descriptor.raw_bytes.len != 0 and
        ptr_value % page_size == 0 and
        descriptor.raw_bytes.len % page_size == 0 and
        storage.preparedBytes(.row_major_blocks) == null and
        storage.preparedBytes(.panel4) == null and
        storage.preparedBytes(.panel8) == null and
        descriptor.raw_bytes.ptr == storage.raw_bytes.ptr and
        descriptor.raw_bytes.len == storage.raw_bytes.len;
}

fn ensureRuntimeQuantSlotPrepared(
    self: anytype,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
    kind: RawQuantizedRuntimeLinearKind,
) bool {
    const layout = packedQuantBlockLayout(kind) orelse return false;
    const slot_descriptor = runtimeQuantSlotDescriptor(
        self,
        slot,
        in_dim,
        out_dim,
        layout.format,
        layout.values_per_block,
        layout.bytes_per_block,
    ) orelse return false;
    const prepared_kind = self.raw_linear_slot_runtime_prepared_kind[slot];
    if (prepared_kind == kind) return true;
    if (prepared_kind != .none) return false;
    {
        const descriptor = slot_descriptor.descriptor;
        const mapped_forced = quantMappedWeightsForced();
        const mapped_disabled = quantMappedWeightsDisabled();
        const mapped_eligible = !mapped_disabled and quantStorageEligibleForMappedRuntime(slot_descriptor.storage, descriptor);
        if (mapped_forced and !mapped_eligible) {
            incrementRuntimeQuantMappedFailures(self);
            return false;
        }
        if (mapped_eligible) {
            incrementRuntimeQuantMappedAttempts(self);
            const mapped_started_at = monotonicNowNs();
            const mapped_rc = termite_metal_decode_runtime_prepare_quantized_linear_slot_no_copy(
                slot_descriptor.runtime,
                @intFromEnum(layout.format),
                slot,
                descriptor.raw_bytes.ptr,
                descriptor.raw_bytes.len,
                in_dim,
                out_dim,
            );
            addRuntimeQuantMappedPrepareNanos(self, monotonicNowNs() - mapped_started_at);
            if (mapped_rc == 0) {
                self.raw_linear_slot_runtime_prepared_kind[slot] = kind;
                setRuntimeQuantPrepareMode(self, slot, .mapped_shared);
                return true;
            }
            incrementRuntimeQuantMappedFailures(self);
            if (mapped_forced) return false;
            incrementRuntimeQuantMappedFallbacks(self);
        }
        const private_started_at = monotonicNowNs();
        if (termite_metal_decode_runtime_prepare_quantized_linear_slot(
            slot_descriptor.runtime,
            @intFromEnum(layout.format),
            slot,
            descriptor.raw_bytes.ptr,
            descriptor.raw_bytes.len,
            in_dim,
            out_dim,
        ) != 0) return false;
        addRuntimeQuantPrivatePrepareNanos(self, monotonicNowNs() - private_started_at);
        self.raw_linear_slot_runtime_prepared_kind[slot] = kind;
        setRuntimeQuantPrepareMode(self, slot, .private_upload);
    }
    return true;
}

fn ensureRuntimeQuantSlotPreparedPrivateWritable(
    self: anytype,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
    kind: RawQuantizedRuntimeLinearKind,
) bool {
    const layout = packedQuantBlockLayout(kind) orelse return false;
    const slot_descriptor = runtimeQuantSlotDescriptor(
        self,
        slot,
        in_dim,
        out_dim,
        layout.format,
        layout.values_per_block,
        layout.bytes_per_block,
    ) orelse return false;
    const prepared_kind = self.raw_linear_slot_runtime_prepared_kind[slot];
    if (prepared_kind == kind and runtimeQuantPrepareMode(self, slot) == .private_upload) return true;
    if (prepared_kind != .none and prepared_kind != kind) return false;

    const descriptor = slot_descriptor.descriptor;
    const private_started_at = monotonicNowNs();
    if (termite_metal_decode_runtime_prepare_quantized_linear_slot(
        slot_descriptor.runtime,
        @intFromEnum(layout.format),
        slot,
        descriptor.raw_bytes.ptr,
        descriptor.raw_bytes.len,
        in_dim,
        out_dim,
    ) != 0) return false;
    addRuntimeQuantPrivatePrepareNanos(self, monotonicNowNs() - private_started_at);
    self.raw_linear_slot_runtime_prepared_kind[slot] = kind;
    setRuntimeQuantPrepareMode(self, slot, .private_upload);
    return true;
}

fn applyRuntimeLinearBiasHost(self: anytype, slot: usize, output: []f32, rows: usize, out_dim: usize) bool {
    if (slot >= decoder_runtime_linear_slot_capacity or rows == 0 or out_dim == 0) return false;
    const bias_tensor = self.raw_linear_slot_dense_biases[slot] orelse return false;
    if (bias_tensor.len != out_dim) return false;
    if (output.len != rows * out_dim) return false;
    const bias = bias_tensor.data[0..out_dim];
    for (0..rows) |row| {
        const row_base = row * out_dim;
        for (0..out_dim) |col| output[row_base + col] += bias[col];
    }
    return true;
}

fn applyRuntimeLinearBiasDevice(self: anytype, slot: usize, output: *MetalTensor, rows: usize, out_dim: usize) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (slot >= decoder_runtime_linear_slot_capacity or rows == 0 or out_dim == 0) return false;
    if (self.raw_linear_slot_dense_biases[slot] == null) return false;
    if (!output.isDevice()) return false;
    if (output.elemCount() != rows * out_dim) return false;
    return termite_metal_decode_runtime_apply_linear_bias_device(
        runtime,
        slot,
        output.deviceHandle(),
        output.deviceByteOffset(),
        rows,
        out_dim,
    ) == 0;
}

fn quantizedRuntimeLinearKindHasSingleStageDeviceKernel(kind: RawQuantizedRuntimeLinearKind) bool {
    return switch (kind) {
        .q1_0,
        .i2_s,
        .q2_k,
        .q3_k,
        .q4_0,
        .q4_1,
        .q4_k,
        .q5_0,
        .q5_1,
        .q5_k,
        .q6_k,
        .q8_0,
        .q8_1,
        .q8_k,
        .i8_s,
        .iq4_nl,
        .iq4_xs,
        .mxfp4,
        .nvfp4,
        .iq2_xs,
        .tl1,
        .tl2,
        => true,
        else => false,
    };
}

fn quantizedRuntimeLinearKindHasPairDeviceKernel(kind: RawQuantizedRuntimeLinearKind) bool {
    return switch (kind) {
        .i2_s,
        .q4_0,
        .q4_k,
        .q6_k,
        .q8_0,
        => true,
        else => false,
    };
}

/// Formats the native Metal kernels (without MLX) can execute directly.
/// Dense float formats pass through to the CPU f32 fallback in native_compute,
/// so they are considered supported. Quantized formats must have a dedicated
/// Metal kernel (TL1/TL2 via bitnet path; I2_S, Q4_K, Q5_K via dispatcher).
pub fn isMetalNativeSupported(tensor_type: gguf_tensor_types.TensorType) bool {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .F32, .F16, .BF16 => true,
            .Q1_0, .Q4_0, .Q4_1, .Q5_0, .Q5_1, .Q8_0, .Q8_1, .Q2_K, .Q3_K, .Q4_K, .Q5_K, .Q6_K, .Q8_K => true,
            .I8_S => true,
            .IQ4_NL, .IQ4_XS => true,
            .MXFP4, .NVFP4, .IQ2_XS => true,
            .I2_S, .TL1 => true,
            else => false,
        },
        .bitnet_tl2 => true,
        .unknown => false,
    };
}

pub fn ensureQ10RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q1_0);
}

pub fn ensureI8SRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .i8_s);
}

pub fn ensureI2SRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .i2_s);
}

pub fn ensureQ4KRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q4_k);
}

pub fn ensureQ5KRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q5_k);
}

pub fn ensureQ6KRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q6_k);
}

pub fn ensureQ40RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q4_0);
}

pub fn ensureQ41RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q4_1);
}

pub fn ensureQ50RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q5_0);
}

pub fn ensureQ51RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q5_1);
}

fn ensureQ80RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q8_0);
}

pub fn ensureQ81RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q8_1);
}

pub fn ensureIq4NlRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .iq4_nl);
}

pub fn ensureQ8KRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q8_k);
}

pub fn ensureIq4XsRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .iq4_xs);
}

pub fn ensureQ2KRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q2_k);
}

pub fn ensureQ3KRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .q3_k);
}

pub fn ensureMxfp4RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .mxfp4);
}

pub fn ensureNvfp4RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .nvfp4);
}

pub fn ensureIq2XsRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    return ensureRuntimeQuantSlotPrepared(self, slot, in_dim, out_dim, .iq2_xs);
}

pub fn ensureTl1RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_linear_slots_prepared[slot]) return false;
    if (self.raw_linear_slot_kinds[slot] != .quantized) return false;
    if (self.raw_linear_slot_in_dims[slot] != in_dim or self.raw_linear_slot_out_dims[slot] != out_dim) return false;
    const storage = self.raw_linear_slot_quantized_storage[slot] orelse return false;
    if (quantizedRuntimeLinearKind(storage) != .tl1) return false;
    const source_bytes = storage.preparedBytes(.row_major_blocks) orelse storage.raw_bytes;
    const view = quant_codec.bitnetTL1View(storage.shape, source_bytes) catch return false;
    if (view.cols != in_dim or view.rows != out_dim) return false;
    const prepared_kind = self.raw_linear_slot_runtime_prepared_kind[slot];
    if (prepared_kind == .tl1) return true;
    if (prepared_kind != .none) return false;
    if (view.packed_bytes.len > std.math.maxInt(u32) or
        view.config.bm > std.math.maxInt(u32) or
        view.config.by > std.math.maxInt(u32) or
        view.config.bmm > std.math.maxInt(u32)) return false;
    if (termite_metal_decode_runtime_prepare_bitnet_tl1_linear_slot(
        runtime,
        slot,
        source_bytes.ptr,
        source_bytes.len,
        in_dim,
        out_dim,
        @intCast(view.packed_bytes.len),
        @intCast(view.config.bm),
        @intCast(view.config.by),
        @intCast(view.config.bmm),
    ) != 0) return false;
    self.raw_linear_slot_runtime_prepared_kind[slot] = .tl1;
    return true;
}

pub fn ensureTl2RuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_linear_slots_prepared[slot]) return false;
    if (self.raw_linear_slot_kinds[slot] != .quantized) return false;
    if (self.raw_linear_slot_in_dims[slot] != in_dim or self.raw_linear_slot_out_dims[slot] != out_dim) return false;
    const storage = self.raw_linear_slot_quantized_storage[slot] orelse return false;
    if (quantizedRuntimeLinearKind(storage) != .tl2) return false;
    const source_bytes = storage.preparedBytes(.row_major_blocks) orelse storage.raw_bytes;
    const view = quant_codec.bitnetTL2View(storage.shape, source_bytes) catch return false;
    if (view.cols != in_dim or view.rows != out_dim) return false;
    const prepared_kind = self.raw_linear_slot_runtime_prepared_kind[slot];
    if (prepared_kind == .tl2) return true;
    if (prepared_kind != .none) return false;
    const scale_off_u64 = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(view.cols), @intCast(view.rows) }) orelse return false) - 32;
    const scale_off: usize = @intCast(scale_off_u64);
    if (scale_off > std.math.maxInt(u32) or
        view.three_values.len > std.math.maxInt(u32) or
        view.three_signs.len > std.math.maxInt(u32) or
        view.config.bm > std.math.maxInt(u32) or
        view.config.by > std.math.maxInt(u32) or
        view.config.bmm > std.math.maxInt(u32) or
        view.three_cols > std.math.maxInt(u32) or
        view.two_cols > std.math.maxInt(u32)) return false;
    if (termite_metal_decode_runtime_prepare_bitnet_tl2_linear_slot(
        runtime,
        slot,
        source_bytes.ptr,
        source_bytes.len,
        in_dim,
        out_dim,
        @intCast(scale_off),
        @intCast(view.three_values.len),
        @intCast(view.three_signs.len),
        @intCast(view.config.bm),
        @intCast(view.config.by),
        @intCast(view.config.bmm),
        @intCast(view.three_cols),
        @intCast(view.two_cols),
    ) != 0) return false;
    self.raw_linear_slot_runtime_prepared_kind[slot] = .tl2;
    return true;
}

pub fn ensureQuantizedRuntimeLinearSlotPrepared(self: anytype, slot: usize, in_dim: usize, out_dim: usize) RawQuantizedRuntimeLinearKind {
    if (slot >= decoder_runtime_linear_slot_capacity) return .none;
    if (!self.raw_linear_slots_prepared[slot]) return .none;
    if (self.raw_linear_slot_kinds[slot] != .quantized) return .none;
    if (self.raw_linear_slot_in_dims[slot] != in_dim or self.raw_linear_slot_out_dims[slot] != out_dim) return .none;
    const storage = self.raw_linear_slot_quantized_storage[slot] orelse return .none;
    return switch (quantizedRuntimeLinearKind(storage)) {
        .q1_0 => if (ensureQ10RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q1_0 else .none,
        .i2_s => if (ensureI2SRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .i2_s else .none,
        .i8_s => if (ensureI8SRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .i8_s else .none,
        .q4_k => if (ensureQ4KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q4_k else .none,
        .q5_k => if (ensureQ5KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q5_k else .none,
        .q6_k => if (ensureQ6KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q6_k else .none,
        .q4_0 => if (ensureQ40RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q4_0 else .none,
        .q4_1 => if (ensureQ41RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q4_1 else .none,
        .q5_0 => if (ensureQ50RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q5_0 else .none,
        .q5_1 => if (ensureQ51RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q5_1 else .none,
        .q8_0 => if (ensureQ80RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q8_0 else .none,
        .q8_1 => if (ensureQ81RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q8_1 else .none,
        .q8_k => if (ensureQ8KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q8_k else .none,
        .iq4_nl => if (ensureIq4NlRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .iq4_nl else .none,
        .iq4_xs => if (ensureIq4XsRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .iq4_xs else .none,
        .q2_k => if (ensureQ2KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q2_k else .none,
        .q3_k => if (ensureQ3KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .q3_k else .none,
        .mxfp4 => if (ensureMxfp4RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .mxfp4 else .none,
        .nvfp4 => if (ensureNvfp4RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .nvfp4 else .none,
        .iq2_xs => if (ensureIq2XsRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .iq2_xs else .none,
        .tl1 => if (ensureTl1RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .tl1 else .none,
        .tl2 => if (ensureTl2RuntimeLinearSlotPrepared(self, slot, in_dim, out_dim)) .tl2 else .none,
        .none,
        .iq1_s,
        .iq1_m,
        .iq2_xxs,
        .iq2_s,
        .iq3_xxs,
        .iq3_s,
        .tq1_0,
        .tq2_0,
        => .none,
    };
}

fn ensureQuantizedRuntimeLinearSlotPreparedWritable(self: anytype, slot: usize, in_dim: usize, out_dim: usize) RawQuantizedRuntimeLinearKind {
    if (slot >= decoder_runtime_linear_slot_capacity) return .none;
    if (!self.raw_linear_slots_prepared[slot]) return .none;
    if (self.raw_linear_slot_kinds[slot] != .quantized) return .none;
    if (self.raw_linear_slot_in_dims[slot] != in_dim or self.raw_linear_slot_out_dims[slot] != out_dim) return .none;
    const storage = self.raw_linear_slot_quantized_storage[slot] orelse return .none;
    const kind = quantizedRuntimeLinearKind(storage);
    return switch (kind) {
        .q4_k, .q5_k, .q6_k, .q4_0, .q4_1, .q5_0, .q5_1, .q8_0, .q8_1 => if (ensureRuntimeQuantSlotPreparedPrivateWritable(self, slot, in_dim, out_dim, kind)) kind else .none,
        else => .none,
    };
}

pub fn decoderRuntimeCopyQuantLinearSlotToF32(
    self: anytype,
    slot: usize,
    row_offset: usize,
    rows: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (slot >= decoder_runtime_linear_slot_capacity or rows == 0 or dim == 0 or source_rows == 0) return null;
    if (row_offset > source_rows or rows > source_rows - row_offset) return null;
    const kind = ensureQuantizedRuntimeLinearSlotPrepared(self, slot, dim, source_rows);
    if (kind == .none) return null;
    const format = metalQuantFormatForKind(kind);
    if (format == .unsupported) return null;
    const layout = packedQuantBlockLayout(kind) orelse return null;
    if (dim % layout.values_per_block != 0) return null;

    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    var output = try MetalTensor.deviceAllocate(
        runtime,
        rows * dim * @sizeOf(f32),
        .private,
        &shape,
    );
    errdefer output.deinit();
    const rc = termite_metal_decode_runtime_quant_copy_linear_slot_to_f32_device(
        runtime,
        @intFromEnum(format),
        slot,
        row_offset,
        rows,
        dim,
        source_rows,
        scale,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output;
}

pub fn decoderRuntimeCopyF32ToQuantLinearSlot(
    self: anytype,
    slot: usize,
    row_offset: usize,
    rows: usize,
    dim: usize,
    source_rows: usize,
    scale: f32,
    input: MetalTensor,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (slot >= decoder_runtime_linear_slot_capacity or rows == 0 or dim == 0 or source_rows == 0) return false;
    if (row_offset > source_rows or rows > source_rows - row_offset) return false;
    if (!input.isDevice()) return false;
    const kind = ensureQuantizedRuntimeLinearSlotPreparedWritable(self, slot, dim, source_rows);
    const format = metalQuantFormatForKind(kind);
    if (format != .q8_0 and format != .q8_1 and format != .q4_0 and format != .q4_1 and format != .q5_0 and format != .q5_1 and format != .q4_k and format != .q5_k and format != .q6_k) return false;
    if (input.deviceByteLen() < rows * dim * @sizeOf(f32)) return false;
    const rc = termite_metal_decode_runtime_quant_copy_f32_to_linear_slot_device(
        runtime,
        @intFromEnum(format),
        slot,
        row_offset,
        rows,
        dim,
        source_rows,
        scale,
        input.deviceHandle(),
        input.deviceByteOffset(),
    );
    return rc == 0;
}

pub fn decoderRuntimeSetRowsQuantLinearSlot(
    self: anytype,
    slot: usize,
    ids: []const u32,
    dim: usize,
    source_rows: usize,
    scale: f32,
    input: MetalTensor,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (slot >= decoder_runtime_linear_slot_capacity or ids.len == 0 or dim == 0 or source_rows == 0) return false;
    if (!input.isDevice()) return false;
    const kind = ensureQuantizedRuntimeLinearSlotPreparedWritable(self, slot, dim, source_rows);
    const format = metalQuantFormatForKind(kind);
    if (format != .q8_0 and format != .q8_1 and format != .q4_0 and format != .q4_1 and format != .q5_0 and format != .q5_1 and format != .q4_k and format != .q5_k and format != .q6_k) return false;
    if (input.deviceByteLen() < ids.len * dim * @sizeOf(f32)) return false;
    const rc = termite_metal_decode_runtime_quant_set_rows_linear_slot_device(
        runtime,
        @intFromEnum(format),
        slot,
        ids.ptr,
        ids.len,
        dim,
        source_rows,
        scale,
        input.deviceHandle(),
        input.deviceByteOffset(),
    );
    return rc == 0;
}

pub fn decoderRuntimeGetRowsQuantLinearSlot(
    self: anytype,
    slot: usize,
    ids: []const u32,
    dim: usize,
    source_rows: usize,
    scale: f32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (slot >= decoder_runtime_linear_slot_capacity or ids.len == 0 or dim == 0 or source_rows == 0) return null;
    const kind = ensureQuantizedRuntimeLinearSlotPrepared(self, slot, dim, source_rows);
    if (kind == .none) return null;
    const format = metalQuantFormatForKind(kind);
    if (format == .unsupported) return null;
    const layout = packedQuantBlockLayout(kind) orelse return null;
    if (dim % layout.values_per_block != 0) return null;

    const shape = [_]i32{ @intCast(ids.len), @intCast(dim) };
    var output = try MetalTensor.deviceAllocate(
        runtime,
        ids.len * dim * @sizeOf(f32),
        .private,
        &shape,
    );
    errdefer output.deinit();
    const rc = termite_metal_decode_runtime_quant_get_rows_linear_slot_device(
        runtime,
        @intFromEnum(format),
        slot,
        ids.ptr,
        ids.len,
        dim,
        source_rows,
        scale,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output;
}

pub fn clearRawLinearSlot(self: anytype, slot: usize) void {
    if (self.raw_linear_slot_dense_weights[slot]) |*weight_tensor| {
        weight_tensor.deinit();
    }
    self.raw_linear_slot_dense_weights[slot] = null;
    if (self.raw_linear_slot_dense_biases[slot]) |*bias_tensor| {
        bias_tensor.deinit();
    }
    self.raw_linear_slot_dense_biases[slot] = null;
    if (self.raw_linear_slot_kinds[slot] == .quantized) {
        if (self.raw_linear_slot_quantized_storage[slot]) |storage| {
            storage.deinit();
            std.heap.c_allocator.destroy(storage);
        }
    }
    self.raw_linear_slot_quantized_storage[slot] = null;
    self.raw_linear_slot_kinds[slot] = .none;
    self.raw_linear_slot_in_dims[slot] = 0;
    self.raw_linear_slot_out_dims[slot] = 0;
    self.raw_linear_slot_runtime_prepared_kind[slot] = .none;
    setRuntimeQuantPrepareMode(self, slot, .none);
    self.raw_linear_slots_prepared[slot] = false;
}

pub fn makeQuantizedWeightArray(storage: *const QuantizedStorage) !c.mlx_array {
    const source_bytes = storage.preparedBytes(.row_major_blocks) orelse storage.raw_bytes;
    const weight_shape = [_]i32{@intCast(source_bytes.len)};
    return mlx.arrayFromBorrowedBytes(source_bytes, &weight_shape, c.MLX_UINT8);
}

pub fn dupQuantizedStorage(storage: *const QuantizedStorage) !*QuantizedStorage {
    const owned = try std.heap.c_allocator.create(QuantizedStorage);
    errdefer std.heap.c_allocator.destroy(owned);

    const raw_bytes = if (storage.raw_owned)
        try std.heap.c_allocator.dupe(u8, storage.raw_bytes)
    else
        storage.raw_bytes;
    errdefer if (storage.raw_owned) std.heap.c_allocator.free(@constCast(raw_bytes));
    const shape = try std.heap.c_allocator.dupe(i64, storage.shape);
    errdefer std.heap.c_allocator.free(shape);
    const source_name = if (storage.source_name) |name|
        try std.heap.c_allocator.dupe(u8, name)
    else
        null;
    errdefer if (source_name) |name| std.heap.c_allocator.free(name);
    owned.* = .{
        .tensor_type = storage.tensor_type,
        .raw_bytes = raw_bytes,
        .shape = shape,
        .source_name = source_name,
        .packed_expert = storage.packed_expert,
        .raw_owned = storage.raw_owned,
        .allocator = std.heap.c_allocator,
    };
    errdefer owned.deinit();
    for (storage.prepared.entries, 0..) |entry, idx| {
        const buffer = entry orelse continue;
        const bytes = try std.heap.c_allocator.dupe(u8, buffer.bytes);
        owned.setPreparedBytes(@enumFromInt(idx), bytes, buffer.panel_cols, buffer.row_blocks);
    }
    return owned;
}

pub fn tryApplyQuantizedRuntimeLinear(
    self: anytype,
    slot: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const kind = ensureQuantizedRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
    if (kind == .none) return null;

    if (referenceQuantizedRuntimeLinearDebug()) {
        const storage = self.raw_linear_slot_quantized_storage[slot] orelse return null;
        var input_host_tensor = input;
        const input_host = try tensorHostSlice(&input_host_tensor);
        const weight_host = try std.heap.c_allocator.alloc(f32, out_dim * in_dim);
        defer std.heap.c_allocator.free(weight_host);
        try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, weight_host);
        const output = try std.heap.c_allocator.alloc(f32, rows * out_dim);
        errdefer std.heap.c_allocator.free(output);
        native.sgemmTransBSync(rows, out_dim, in_dim, 1.0, input_host, weight_host, 0.0, output);
        if (!applyRuntimeLinearBiasHost(self, slot, output, rows, out_dim)) {
            std.heap.c_allocator.free(output);
            return null;
        }
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        return MetalTensor.owned(output, &shape);
    }

    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    if (input.isDevice() and quantizedRuntimeLinearKindHasSingleStageDeviceKernel(kind)) {
        const format = metalQuantFormatForKind(kind);
        if (format == .unsupported) return null;
        if (kind != .tl1 and kind != .tl2) {
            const storage = self.raw_linear_slot_quantized_storage[slot] orelse return null;
            const descriptor = packedWeightDescriptorForMatrix(storage, in_dim, out_dim, format) orelse return null;
            if (!descriptor.supported()) return null;
        }
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        var output_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
        errdefer output_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_quantized_linear_slot_device(
            runtime,
            @intFromEnum(format),
            slot,
            input.deviceHandle(),
            input.deviceByteOffset(),
            rows,
            in_dim,
            out_dim,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) {
            if (!applyRuntimeLinearBiasDevice(self, slot, &output_device, rows, out_dim)) {
                output_device.deinit();
                return null;
            }
            return output_device;
        }
        output_device.deinit();
    }

    if (frame_active) return null;

    var input_mut = input;
    const input_base = try tensorHostConstPtr(&input_mut);

    const output = try std.heap.c_allocator.alloc(f32, rows * out_dim);
    errdefer std.heap.c_allocator.free(output);

    const rc = switch (kind) {
        .i2_s => termite_metal_decode_runtime_apply_i2_s_linear_slot(
            runtime,
            slot,
            input_base,
            rows,
            in_dim,
            out_dim,
            output.ptr,
        ),
        .q1_0,
        .i8_s,
        .q2_k,
        .q3_k,
        .q4_0,
        .q4_1,
        .q4_k,
        .q5_0,
        .q5_1,
        .q5_k,
        .q6_k,
        .q8_0,
        .q8_1,
        .q8_k,
        .iq4_nl,
        .iq4_xs,
        .mxfp4,
        .nvfp4,
        .iq2_xs,
        .tl1,
        .tl2,
        => termite_metal_decode_runtime_apply_quantized_linear_slot_host(
            runtime,
            @intFromEnum(metalQuantFormatForKind(kind)),
            slot,
            input_base,
            rows,
            in_dim,
            out_dim,
            output.ptr,
        ),
        .none,
        .iq1_s,
        .iq1_m,
        .iq2_xxs,
        .iq2_s,
        .iq3_xxs,
        .iq3_s,
        .tq1_0,
        .tq2_0,
        => unreachable,
    };
    if (rc != 0) return null;
    if (!applyRuntimeLinearBiasHost(self, slot, output, rows, out_dim)) {
        std.heap.c_allocator.free(output);
        return null;
    }
    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    return MetalTensor.owned(output, &shape);
}

pub fn tryApplyQuantizedRuntimeLinearScratch(
    self: anytype,
    slot: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or rows == 0) return null;
    const kind = ensureQuantizedRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
    if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(kind)) return null;
    const format = metalQuantFormatForKind(kind);
    if (format == .unsupported) return null;
    if (kind != .tl1 and kind != .tl2) {
        const storage = self.raw_linear_slot_quantized_storage[slot] orelse return null;
        const descriptor = packedWeightDescriptorForMatrix(storage, in_dim, out_dim, format) orelse return null;
        if (!descriptor.supported()) return null;
    }
    var output_handle: ?*anyopaque = null;
    const rc = termite_metal_decode_runtime_apply_quantized_linear_slot_scratch_device(
        runtime,
        @intFromEnum(format),
        slot,
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        in_dim,
        out_dim,
        &output_handle,
    );
    if (rc != 0) return null;
    const handle = output_handle orelse return null;
    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    var output = MetalTensor.deviceBorrowed(@ptrCast(runtime), handle, 0, rows * out_dim * @sizeOf(f32), &shape);
    errdefer output.deinit();
    if (!applyRuntimeLinearBiasDevice(self, slot, &output, rows, out_dim)) return null;
    return output;
}

pub const PrefillSetupResult = struct {
    q: MetalTensor,
    k: ?MetalTensor = null,
    v: ?MetalTensor = null,

    pub fn deinit(self: *PrefillSetupResult) void {
        self.q.deinit();
        if (self.k) |*tensor| tensor.deinit();
        if (self.v) |*tensor| tensor.deinit();
        self.k = null;
        self.v = null;
    }
};

pub fn decoderRuntimeApplyPrefillSetupDevice(
    self: anytype,
    request: anytype,
) !?PrefillSetupResult {
    const trace = traceQuantBlockRequested();
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null reason=runtime-not-ready\n", .{});
        return null;
    }
    if (!hasActiveFrame(self.raw_decode_runtime)) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null reason=no-active-frame\n", .{});
        return null;
    }
    if (!request.input.isDevice()) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=input-not-device\n", .{ request.rows, request.shares_kv });
        return null;
    }
    if (request.input.ndim() != 2) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=input-ndim ndim={d}\n", .{ request.rows, request.shares_kv, request.input.ndim() });
        return null;
    }
    if (request.rows == 0 or request.hidden_size == 0 or request.attention_input_size == 0) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=empty-dims hidden={d} attn={d}\n", .{ request.rows, request.shares_kv, request.hidden_size, request.attention_input_size });
        return null;
    }
    if (request.num_heads == 0 or request.head_dim == 0 or request.rope_dim == 0) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=empty-head-dims heads={d} head_dim={d} rope_dim={d}\n", .{ request.rows, request.shares_kv, request.num_heads, request.head_dim, request.rope_dim });
        return null;
    }
    if (request.attention_input_size != request.num_heads * request.head_dim) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=attention-size attn={d} heads={d} head_dim={d}\n", .{ request.rows, request.shares_kv, request.attention_input_size, request.num_heads, request.head_dim });
        return null;
    }
    if (@as(usize, @intCast(request.input.dim(0))) != request.rows or
        @as(usize, @intCast(request.input.dim(1))) != request.hidden_size)
    {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=input-shape dim0={d} dim1={d} hidden={d}\n", .{ request.rows, request.shares_kv, @as(usize, @intCast(request.input.dim(0))), @as(usize, @intCast(request.input.dim(1))), request.hidden_size });
        return null;
    }
    const q_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.q_slot, request.hidden_size, request.attention_input_size);
    const q_format = metalQuantFormatForKind(q_kind);
    if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(q_kind) or q_format == .unsupported) {
        if (trace) {
            const slot = request.q_slot;
            const raw_prepared = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slots_prepared[slot] else false;
            const raw_kind = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_kinds[slot] else .none;
            const raw_in = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_in_dims[slot] else 0;
            const raw_out = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_out_dims[slot] else 0;
            const runtime_kind = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_runtime_prepared_kind[slot] else .none;
            const storage = if (slot < decoder_runtime_linear_slot_capacity) self.raw_linear_slot_quantized_storage[slot] else null;
            const storage_kind = if (storage) |quant| quantizedRuntimeLinearKind(quant) else .none;
            const descriptor = if (storage) |quant| packedWeightDescriptor(quant, request.hidden_size) else null;
            if (descriptor) |desc| {
                std.debug.print(
                    "metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=q-slot slot={d} hidden={d} attn={d} raw_prepared={} raw_kind={s} raw_in={d} raw_out={d} storage_kind={s} runtime_kind={s} desc_format={s} desc_vpb={d} desc_bpb={d} desc_stride={d} desc_bytes={d}\n",
                    .{ request.rows, request.shares_kv, slot, request.hidden_size, request.attention_input_size, raw_prepared, @tagName(raw_kind), raw_in, raw_out, @tagName(storage_kind), @tagName(runtime_kind), @tagName(desc.format), desc.values_per_block, desc.bytes_per_block, desc.row_stride_bytes, desc.raw_bytes.len },
                );
            } else {
                std.debug.print(
                    "metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=q-slot slot={d} hidden={d} attn={d} raw_prepared={} raw_kind={s} raw_in={d} raw_out={d} storage_kind={s} runtime_kind={s} desc=null\n",
                    .{ request.rows, request.shares_kv, slot, request.hidden_size, request.attention_input_size, raw_prepared, @tagName(raw_kind), raw_in, raw_out, @tagName(storage_kind), @tagName(runtime_kind) },
                );
            }
        }
        return null;
    }
    var k_format: MetalQuantFormat = .unsupported;
    var v_format: MetalQuantFormat = .unsupported;
    if (!request.shares_kv) {
        if (request.kv_dim == 0 or request.num_kv_heads == 0 or request.kv_dim != request.num_kv_heads * request.head_dim) {
            if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv=false reason=kv-dims kv_dim={d} kv_heads={d} head_dim={d}\n", .{ request.rows, request.kv_dim, request.num_kv_heads, request.head_dim });
            return null;
        }
        const k_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.k_slot, request.hidden_size, request.kv_dim);
        k_format = metalQuantFormatForKind(k_kind);
        if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(k_kind) or k_format == .unsupported) {
            if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv=false reason=k-slot slot={d} hidden={d} kv_dim={d}\n", .{ request.rows, request.k_slot, request.hidden_size, request.kv_dim });
            return null;
        }
        const v_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.v_slot, request.hidden_size, request.kv_dim);
        v_format = metalQuantFormatForKind(v_kind);
        if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(v_kind) or v_format == .unsupported) {
            if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv=false reason=v-slot slot={d} hidden={d} kv_dim={d}\n", .{ request.rows, request.v_slot, request.hidden_size, request.kv_dim });
            return null;
        }
    }
    if (request.q_norm_slot >= decoder_runtime_rms_norm_slot_capacity) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=q-norm-slot-range slot={d}\n", .{ request.rows, request.shares_kv, request.q_norm_slot });
        return null;
    }
    if (!self.raw_rms_norm_slots_prepared[request.q_norm_slot] or self.raw_rms_norm_slot_hidden_sizes[request.q_norm_slot] != request.head_dim) {
        if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv={} reason=q-norm-slot slot={d} prepared={} slot_hidden={d} head_dim={d}\n", .{ request.rows, request.shares_kv, request.q_norm_slot, self.raw_rms_norm_slots_prepared[request.q_norm_slot], self.raw_rms_norm_slot_hidden_sizes[request.q_norm_slot], request.head_dim });
        return null;
    }
    if (!request.shares_kv) {
        if (request.k_norm_slot >= decoder_runtime_rms_norm_slot_capacity) {
            if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv=false reason=k-norm-slot-range slot={d}\n", .{ request.rows, request.k_norm_slot });
            return null;
        }
        if (!self.raw_rms_norm_slots_prepared[request.k_norm_slot] or self.raw_rms_norm_slot_hidden_sizes[request.k_norm_slot] != request.head_dim) {
            if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv=false reason=k-norm-slot slot={d} prepared={} slot_hidden={d} head_dim={d}\n", .{ request.rows, request.k_norm_slot, self.raw_rms_norm_slots_prepared[request.k_norm_slot], self.raw_rms_norm_slot_hidden_sizes[request.k_norm_slot], request.head_dim });
            return null;
        }
    }
    const value_norm_present = request.value_norm_weight != null and !request.shares_kv;
    const value_norm_weight = request.value_norm_weight orelse undefined;
    if (value_norm_present) {
        if (!value_norm_weight.isDevice() or value_norm_weight.elemCount() != request.head_dim) {
            if (trace) std.debug.print("metal-prefill-quant-setup-zig-null rows={d} shares_kv=false reason=value-norm device={} elems={d} head_dim={d}\n", .{ request.rows, value_norm_weight.isDevice(), value_norm_weight.elemCount(), request.head_dim });
            return null;
        }
    }

    const planned_layer_contract: ops.PlannedLayerContract = if (@hasField(@TypeOf(request), "planned_layer_contract")) request.planned_layer_contract else .{};
    const raw_planned_layer_contract = RawPlannedLayerContract.fromContract(planned_layer_contract);
    const keep_scope_open_after = if (@hasField(@TypeOf(request), "keep_scope_open_after")) request.keep_scope_open_after else false;
    var q_handle: ?*anyopaque = null;
    var k_handle: ?*anyopaque = null;
    var v_handle: ?*anyopaque = null;
    const rc = termite_metal_decode_runtime_apply_prefill_quantized_setup_device(
        runtime,
        @intFromBool(request.shares_kv),
        @intFromEnum(q_format),
        if (request.shares_kv) 0 else @intFromEnum(k_format),
        if (request.shares_kv) 0 else @intFromEnum(v_format),
        request.q_slot,
        if (request.shares_kv) 0 else request.k_slot,
        if (request.shares_kv) 0 else request.v_slot,
        request.input.deviceHandle(),
        request.input.deviceByteOffset(),
        request.rows,
        request.hidden_size,
        request.attention_input_size,
        if (request.shares_kv) 0 else request.kv_dim,
        request.q_norm_slot,
        if (request.shares_kv) 0 else request.k_norm_slot,
        @intFromBool(value_norm_present),
        if (value_norm_present) value_norm_weight.deviceHandle() else null,
        if (value_norm_present) value_norm_weight.deviceByteOffset() else 0,
        request.num_heads,
        if (request.shares_kv) 0 else request.num_kv_heads,
        request.head_dim,
        request.rope_dim,
        request.position,
        request.theta,
        request.freq_scale,
        request.eps,
        request.query_value_scale,
        @intFromBool(request.consecutive_pairs),
        raw_planned_layer_contract,
        @intFromBool(keep_scope_open_after),
        &q_handle,
        &k_handle,
        &v_handle,
    );
    if (rc != 0) {
        if (trace) {
            std.debug.print(
                "metal-prefill-quant-setup-fail rows={d} shares_kv={} rc={d} q_slot={d} k_slot={d} v_slot={d} hidden={d} attn={d} kv_dim={d} heads={d} kv_heads={d} head_dim={d} rope_dim={d} plan_ops={d} plan_cmds={d} start={d}\n",
                .{
                    request.rows,
                    request.shares_kv,
                    rc,
                    request.q_slot,
                    if (request.shares_kv) 0 else request.k_slot,
                    if (request.shares_kv) 0 else request.v_slot,
                    request.hidden_size,
                    request.attention_input_size,
                    if (request.shares_kv) 0 else request.kv_dim,
                    request.num_heads,
                    if (request.shares_kv) 0 else request.num_kv_heads,
                    request.head_dim,
                    request.rope_dim,
                    planned_layer_contract.ops.len,
                    planned_layer_contract.command_ops.len,
                    planned_layer_contract.start_index,
                },
            );
        }
        return null;
    }

    const q_shape = [_]i32{ @intCast(request.rows), @intCast(request.attention_input_size) };
    const kv_shape = [_]i32{ @intCast(request.rows), @intCast(request.kv_dim) };
    return .{
        .q = MetalTensor.deviceBorrowed(@ptrCast(runtime), q_handle orelse return null, 0, request.rows * request.attention_input_size * @sizeOf(f32), &q_shape),
        .k = if (request.shares_kv) null else MetalTensor.deviceBorrowed(@ptrCast(runtime), k_handle orelse return null, 0, request.rows * request.kv_dim * @sizeOf(f32), &kv_shape),
        .v = if (request.shares_kv) null else MetalTensor.deviceBorrowed(@ptrCast(runtime), v_handle orelse return null, 0, request.rows * request.kv_dim * @sizeOf(f32), &kv_shape),
    };
}

pub fn runPrefillPagedGatedFrameLayerDevice(
    self: anytype,
    request: anytype,
    input_mt: MetalTensor,
    residual_mt: MetalTensor,
    value_norm_weight_opt: ?MetalTensor,
    ple_mt_opt: ?MetalTensor,
    paged_layer: anytype,
    block_token_offsets: []const u32,
    stats: anytype,
) !?MetalTensor {
    const trace = traceQuantBlockRequested();
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!hasActiveFrame(self.raw_decode_runtime)) return null;
    if (request.rows == 0 or request.kv_tokens == 0) return null;
    if (block_token_offsets.len == 0 or paged_layer.page_size_tokens == 0) return null;
    if (!input_mt.isDevice() or !residual_mt.isDevice()) return null;
    if (input_mt.ndim() != 2 or residual_mt.ndim() != 2) return null;
    if (@as(usize, @intCast(input_mt.dim(0))) != request.rows or
        @as(usize, @intCast(input_mt.dim(1))) != request.hidden_size or
        @as(usize, @intCast(residual_mt.dim(0))) != request.rows or
        @as(usize, @intCast(residual_mt.dim(1))) != request.hidden_size)
    {
        return null;
    }
    if (request.attention_input_size != request.num_heads * request.head_dim) return null;
    if (!request.shares_kv and request.kv_dim != request.num_kv_heads * request.head_dim) return null;

    const q_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.q_slot, request.hidden_size, request.attention_input_size);
    const q_format = metalQuantFormatForKind(q_kind);
    if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(q_kind) or q_format == .unsupported) return null;
    var k_format: MetalQuantFormat = .unsupported;
    var v_format: MetalQuantFormat = .unsupported;
    if (!request.shares_kv) {
        const k_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.k_slot, request.hidden_size, request.kv_dim);
        k_format = metalQuantFormatForKind(k_kind);
        if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(k_kind) or k_format == .unsupported) return null;
        const v_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.v_slot, request.hidden_size, request.kv_dim);
        v_format = metalQuantFormatForKind(v_kind);
        if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(v_kind) or v_format == .unsupported) return null;
    }

    const direct_block_format = directQuantizedBlockFormatForRequest(self, .{
        .num_heads = request.num_heads,
        .num_kv_heads = request.num_kv_heads,
        .head_dim = request.head_dim,
        .attention_linear_slot = request.attention_linear_slot,
        .hidden_size = request.hidden_size,
        .gate_ffn_linear_slot = request.gate_ffn_linear_slot,
        .up_ffn_linear_slot = request.up_ffn_linear_slot,
        .down_ffn_linear_slot = request.down_ffn_linear_slot,
        .intermediate_size = request.intermediate_size,
        .ple = ple_mt_opt,
        .ple_gate_linear_slot = request.ple_gate_linear_slot,
        .ple_proj_linear_slot = request.ple_proj_linear_slot,
        .ple_post_norm_slot = request.ple_post_norm_slot,
        .ple_hidden_size = request.ple_hidden_size,
    }) orelse return null;
    if (direct_block_format != .q8_0) return null;

    if (ple_mt_opt) |ple_mt| {
        const ple_post_norm_slot = request.ple_post_norm_slot orelse return null;
        if (request.ple_hidden_size == 0) return null;
        if (ple_mt.ndim() != 2 or
            @as(usize, @intCast(ple_mt.dim(0))) != request.rows or
            @as(usize, @intCast(ple_mt.dim(1))) != request.ple_hidden_size or
            !ple_mt.isDevice())
        {
            return null;
        }
        if (ple_post_norm_slot >= decoder_runtime_rms_norm_slot_capacity) return null;
        if (!self.raw_rms_norm_slots_prepared[ple_post_norm_slot]) return null;
        if (self.raw_rms_norm_slot_hidden_sizes[ple_post_norm_slot] != request.hidden_size) return null;
    }

    const value_norm_present = value_norm_weight_opt != null and !request.shares_kv;
    if (value_norm_present) {
        const value_norm_weight = value_norm_weight_opt.?;
        if (!value_norm_weight.isDevice() or value_norm_weight.elemCount() != request.head_dim) return null;
    }

    const out_shape = [_]i32{ @intCast(request.rows), @intCast(request.hidden_size) };
    var device_output = MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        request.rows * request.hidden_size * @sizeOf(f32),
        .private,
        &out_shape,
    ) catch return null;
    var output_owned = true;
    defer if (output_owned) device_output.deinit();

    const none = std.math.maxInt(usize);
    const has_frame_contract_fields = comptime @hasField(@TypeOf(request), "planned_frame_contract") and
        @hasField(@TypeOf(request), "planned_setup_start") and
        @hasField(@TypeOf(request), "planned_block_start") and
        @hasField(@TypeOf(request), "planned_layer_end");
    const setup_contract = if (has_frame_contract_fields) blk: {
        if (request.planned_frame_contract.command_ops.len != 0) {
            break :blk RawPlannedLayerContract.fromContractRange(
                request.planned_frame_contract,
                request.planned_setup_start,
                request.planned_block_start,
            );
        }
        break :blk RawPlannedLayerContract.fromContract(request.planned_setup_contract);
    } else RawPlannedLayerContract.fromContract(request.planned_setup_contract);
    const block_contract = if (has_frame_contract_fields) blk: {
        if (request.planned_frame_contract.command_ops.len != 0) {
            break :blk RawPlannedLayerContract.fromContractRange(
                request.planned_frame_contract,
                request.planned_block_start,
                request.planned_layer_end,
            );
        }
        break :blk RawPlannedLayerContract.fromContract(request.planned_block_contract);
    } else RawPlannedLayerContract.fromContract(request.planned_block_contract);
    const output_scale_value_opt: ?f32 = if (@hasField(@TypeOf(request), "output_scale_value")) request.output_scale_value else null;
    var timing: RawAttentionGatedBlockTiming = .{};
    try input_mt.retainForActiveFrame();
    try residual_mt.retainForActiveFrame();
    if (value_norm_weight_opt) |value_norm_weight| try value_norm_weight.retainForActiveFrame();
    if (ple_mt_opt) |ple_mt| try ple_mt.retainForActiveFrame();
    try device_output.retainForActiveFrame();

    const started_at = monotonicNowNs();
    const rc = termite_metal_decode_runtime_apply_prefill_gated_frame_layer_q8_0_device(
        runtime,
        @intFromBool(request.shares_kv),
        @intFromEnum(q_format),
        if (request.shares_kv) 0 else @intFromEnum(k_format),
        if (request.shares_kv) 0 else @intFromEnum(v_format),
        request.q_slot,
        if (request.shares_kv) 0 else request.k_slot,
        if (request.shares_kv) 0 else request.v_slot,
        input_mt.deviceHandle(),
        input_mt.deviceByteOffset(),
        request.rows,
        request.hidden_size,
        request.attention_input_size,
        if (request.shares_kv) 0 else request.kv_dim,
        request.q_norm_slot,
        if (request.shares_kv) 0 else request.k_norm_slot,
        @intFromBool(value_norm_present),
        if (value_norm_present) value_norm_weight_opt.?.deviceHandle() else null,
        if (value_norm_present) value_norm_weight_opt.?.deviceByteOffset() else 0,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.rope_dim,
        request.position,
        request.theta,
        request.freq_scale,
        request.eps,
        request.query_value_scale,
        @intFromBool(request.consecutive_pairs),
        setup_contract,
        request.kv_tokens,
        request.query_position_offset,
        request.kv_position_offset,
        request.sliding_window,
        request.total_sequence_len,
        request.attention_linear_slot,
        request.attention_pre_linear_rms_norm_slot orelse none,
        request.attention_post_linear_rms_norm_slot orelse none,
        residual_mt.deviceHandle(),
        residual_mt.deviceByteOffset(),
        request.ffn_layer_norm_slot orelse none,
        request.ffn_rms_norm_slot orelse none,
        request.ffn_post_gate_rms_norm_slot orelse none,
        request.ffn_post_down_rms_norm_slot orelse none,
        request.gate_ffn_linear_slot,
        request.up_ffn_linear_slot,
        request.down_ffn_linear_slot,
        request.intermediate_size,
        @intFromEnum(request.activation),
        request.layer_index,
        if (ple_mt_opt) |ple_mt| ple_mt.deviceHandle() else null,
        if (ple_mt_opt) |ple_mt| ple_mt.deviceByteOffset() else 0,
        request.ple_gate_linear_slot orelse none,
        request.ple_proj_linear_slot orelse none,
        request.ple_post_norm_slot orelse none,
        if (ple_mt_opt != null) request.ple_hidden_size else 0,
        if (output_scale_value_opt != null) 1 else 0,
        output_scale_value_opt orelse 1.0,
        device_output.deviceHandle(),
        device_output.deviceByteOffset(),
        block_contract,
        &timing,
        paged_layer.slot,
        paged_layer.format,
        block_token_offsets.ptr,
        block_token_offsets.len,
        @intCast(paged_layer.page_size_tokens),
        paged_layer.key_row_bytes,
        paged_layer.base_key_row_bytes,
        paged_layer.v_row_stride,
        if (request.shares_kv) 0 else request.rows,
    );
    stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - started_at);
    stats.compressed_block_attention_span_nanos += timing.attention_span_nanos;
    stats.compressed_block_attention_prefix_nanos += timing.attention_prefix_nanos;
    stats.compressed_block_gated_ffn_residual_nanos += timing.gated_ffn_residual_nanos;
    stats.compressed_block_command_wait_nanos += timing.command_wait_nanos;
    stats.compressed_block_gpu_nanos += timing.gpu_nanos;
    stats.active_decode_attention_f32_kernels += timing.attention_f32_kernels;
    stats.active_decode_q8_0_linear_kernels += timing.q8_0_linear_kernels;
    stats.active_decode_q8_0_attention_linear_kernels += timing.q8_0_attention_linear_kernels;
    stats.active_decode_q8_0_ffn_down_linear_kernels += timing.q8_0_ffn_down_linear_kernels;
    stats.active_decode_q8_0_ple_linear_kernels += timing.q8_0_ple_linear_kernels;
    stats.active_decode_q8_0_pair_activation_kernels += timing.q8_0_pair_activation_kernels;
    stats.active_decode_rms_norm_kernels += timing.rms_norm_kernels;
    stats.active_decode_rms_norm_add_kernels += timing.rms_norm_add_kernels;
    stats.active_decode_layer_norm_kernels += timing.layer_norm_kernels;
    stats.active_decode_add_kernels += timing.add_kernels;
    stats.active_decode_blit_copies += timing.blit_copies;
    if (rc != 0) {
        if (trace) {
            std.debug.print(
                "metal-prefill-frame-layer-direct-fail layer={d} rows={d} rc={d} stage={d} stage_rc={d}\n",
                .{ request.layer_index, request.rows, rc, timing.failure_stage, timing.failure_code },
            );
        }
        stats.f32_kv_quant_direct_block_failures += 1;
        return null;
    }
    stats.f32_kv_quant_direct_block_successes += 1;
    stats.quantized_gated_ffn_direct_successes += 1;
    output_owned = false;
    return device_output;
}

pub fn tryApplyQuantizedRuntimeLinearPair(
    self: anytype,
    slot_a: usize,
    slot_b: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !?RuntimeLinearPairResult {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    const kind_a = ensureQuantizedRuntimeLinearSlotPrepared(self, slot_a, in_dim, out_dim);
    const kind_b = ensureQuantizedRuntimeLinearSlotPrepared(self, slot_b, in_dim, out_dim);
    if (kind_a == .none or kind_b == .none or kind_a != kind_b) return null;
    const pair_format = metalQuantFormatForKind(kind_a);
    const has_direct_pair = quantizedRuntimeLinearKindHasPairDeviceKernel(kind_a) and pair_format != .unsupported;

    if (referenceQuantizedRuntimeLinearDebug()) {
        var first = (try tryApplyQuantizedRuntimeLinear(self, slot_a, input, rows, in_dim, out_dim)) orelse return null;
        errdefer first.deinit();
        const second = (try tryApplyQuantizedRuntimeLinear(self, slot_b, input, rows, in_dim, out_dim)) orelse {
            var first_mut = first;
            first_mut.deinit();
            return null;
        };
        return .{
            .first = first,
            .second = second,
        };
    }

    if (input.isDevice() and has_direct_pair) {
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        var first_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
        errdefer first_device.deinit();
        var second_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
        errdefer second_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_quantized_linear_pair_slots_device(
            runtime,
            @intFromEnum(pair_format),
            slot_a,
            slot_b,
            input.deviceHandle(),
            input.deviceByteOffset(),
            rows,
            in_dim,
            out_dim,
            first_device.deviceHandle(),
            first_device.deviceByteOffset(),
            second_device.deviceHandle(),
            second_device.deviceByteOffset(),
        );
        if (device_rc == 0) {
            if (!applyRuntimeLinearBiasDevice(self, slot_a, &first_device, rows, out_dim) or
                !applyRuntimeLinearBiasDevice(self, slot_b, &second_device, rows, out_dim))
            {
                second_device.deinit();
                first_device.deinit();
                return null;
            }
            return .{
                .first = first_device,
                .second = second_device,
            };
        }
        second_device.deinit();
        first_device.deinit();
    }

    if (input.isDevice() and quantizedRuntimeLinearKindHasSingleStageDeviceKernel(kind_a)) {
        var first = (try tryApplyQuantizedRuntimeLinear(self, slot_a, input, rows, in_dim, out_dim)) orelse return null;
        errdefer first.deinit();
        const second = (try tryApplyQuantizedRuntimeLinear(self, slot_b, input, rows, in_dim, out_dim)) orelse {
            var first_mut = first;
            first_mut.deinit();
            return null;
        };
        return .{
            .first = first,
            .second = second,
        };
    }

    if (!has_direct_pair) return null;
    if (frame_active) return null;

    var input_mut = input;
    const input_base = try tensorHostConstPtr(&input_mut);

    const first_out = try std.heap.c_allocator.alloc(f32, rows * out_dim);
    errdefer std.heap.c_allocator.free(first_out);
    const second_out = try std.heap.c_allocator.alloc(f32, rows * out_dim);
    errdefer std.heap.c_allocator.free(second_out);

    const rc = termite_metal_decode_runtime_apply_quantized_linear_pair_slots(
        runtime,
        @intFromEnum(pair_format),
        slot_a,
        slot_b,
        input_base,
        rows,
        in_dim,
        out_dim,
        first_out.ptr,
        second_out.ptr,
    );
    if (rc != 0) return null;
    if (!applyRuntimeLinearBiasHost(self, slot_a, first_out, rows, out_dim) or
        !applyRuntimeLinearBiasHost(self, slot_b, second_out, rows, out_dim))
    {
        std.heap.c_allocator.free(second_out);
        std.heap.c_allocator.free(first_out);
        return null;
    }
    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    return .{
        .first = MetalTensor.owned(first_out, &shape),
        .second = MetalTensor.owned(second_out, &shape),
    };
}

pub fn tryApplyDenseRuntimeLinear(
    self: anytype,
    slot: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    if (self.raw_linear_slot_kinds[slot] != .dense) return null;

    if (rows == 1 and input.isDevice()) {
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        var output_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
        errdefer output_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_linear_device(
            runtime,
            slot,
            input.deviceHandle(),
            input.deviceByteOffset(),
            in_dim,
            out_dim,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }
    if (rows != 1 and input.isDevice()) {
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        var output_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
        errdefer output_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_linear_multi_row_device(
            runtime,
            slot,
            input.deviceHandle(),
            input.deviceByteOffset(),
            rows,
            in_dim,
            out_dim,
            output_device.deviceHandle(),
            output_device.deviceByteOffset(),
        );
        if (device_rc == 0) return output_device;
        output_device.deinit();
    }

    if (frame_active) return null;

    const output = try std.heap.c_allocator.alloc(f32, rows * out_dim);
    errdefer std.heap.c_allocator.free(output);

    var input_mut = input;
    const rc = termite_metal_decode_runtime_apply_linear_multi_row(
        runtime,
        slot,
        try tensorHostConstPtr(&input_mut),
        rows,
        in_dim,
        out_dim,
        output.ptr,
    );
    if (rc != 0) return null;

    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    return MetalTensor.owned(output, &shape);
}

pub fn tryApplyDenseRuntimeMlp2(
    self: anytype,
    first_slot: usize,
    second_slot: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    hidden_dim: usize,
    out_dim: usize,
    activation: ops.DecoderRuntimeActivationKind,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice()) return null;
    if (first_slot >= decoder_runtime_linear_slot_capacity or second_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (self.raw_linear_slot_kinds[first_slot] != .dense or self.raw_linear_slot_kinds[second_slot] != .dense) return null;
    if (self.raw_linear_slot_in_dims[first_slot] != in_dim or self.raw_linear_slot_out_dims[first_slot] != hidden_dim) return null;
    if (self.raw_linear_slot_in_dims[second_slot] != hidden_dim or self.raw_linear_slot_out_dims[second_slot] != out_dim) return null;
    if (input.ndim() != 2) return null;
    if (@as(usize, @intCast(input.dim(0))) != rows or @as(usize, @intCast(input.dim(1))) != in_dim) return null;

    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    var output_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_dense_mlp2_device(
        runtime,
        first_slot,
        second_slot,
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        in_dim,
        hidden_dim,
        out_dim,
        @intFromEnum(activation),
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn tryApplyDenseRuntimeFfnLayerNorm(
    self: anytype,
    first_slot: usize,
    second_slot: usize,
    layer_norm_slot: usize,
    input: MetalTensor,
    residual: MetalTensor,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    eps: f32,
    activation: ops.DecoderRuntimeActivationKind,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or !residual.isDevice()) return null;
    if (first_slot >= decoder_runtime_linear_slot_capacity or
        second_slot >= decoder_runtime_linear_slot_capacity or
        layer_norm_slot >= decoder_runtime_layer_norm_slot_capacity) return null;
    if (self.raw_linear_slot_kinds[first_slot] != .dense or self.raw_linear_slot_kinds[second_slot] != .dense) return null;
    if (!self.raw_layer_norm_slots_prepared[layer_norm_slot]) return null;
    if (self.raw_linear_slot_in_dims[first_slot] != hidden_size or self.raw_linear_slot_out_dims[first_slot] != intermediate_size) return null;
    if (self.raw_linear_slot_in_dims[second_slot] != intermediate_size or self.raw_linear_slot_out_dims[second_slot] != hidden_size) return null;
    if (self.raw_layer_norm_slot_hidden_sizes[layer_norm_slot] != hidden_size) return null;
    if (input.ndim() != 2 or residual.ndim() != 2) return null;
    if (@as(usize, @intCast(input.dim(0))) != rows or @as(usize, @intCast(input.dim(1))) != hidden_size) return null;
    if (@as(usize, @intCast(residual.dim(0))) != rows or @as(usize, @intCast(residual.dim(1))) != hidden_size) return null;

    const shape = [_]i32{ @intCast(rows), @intCast(hidden_size) };
    var output_device = try MetalTensor.deviceAllocate(runtime, rows * hidden_size * @sizeOf(f32), .private, &shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_dense_ffn_layer_norm_device(
        runtime,
        first_slot,
        second_slot,
        layer_norm_slot,
        input.deviceHandle(),
        input.deviceByteOffset(),
        residual.deviceHandle(),
        residual.deviceByteOffset(),
        rows,
        hidden_size,
        intermediate_size,
        @intFromEnum(activation),
        eps,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn tryApplyDenseRuntimeLinearLayerNorm(
    self: anytype,
    linear_slot: usize,
    layer_norm_slot: usize,
    input: MetalTensor,
    residual: MetalTensor,
    rows: usize,
    in_dim: usize,
    hidden_size: usize,
    eps: f32,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or !residual.isDevice()) return null;
    if (linear_slot >= decoder_runtime_linear_slot_capacity or layer_norm_slot >= decoder_runtime_layer_norm_slot_capacity) return null;
    if (self.raw_linear_slot_kinds[linear_slot] != .dense) return null;
    if (!self.raw_layer_norm_slots_prepared[layer_norm_slot]) return null;
    if (self.raw_linear_slot_in_dims[linear_slot] != in_dim or self.raw_linear_slot_out_dims[linear_slot] != hidden_size) return null;
    if (self.raw_layer_norm_slot_hidden_sizes[layer_norm_slot] != hidden_size) return null;
    if (input.ndim() != 2 or residual.ndim() != 2) return null;
    if (@as(usize, @intCast(input.dim(0))) != rows or @as(usize, @intCast(input.dim(1))) != in_dim) return null;
    if (@as(usize, @intCast(residual.dim(0))) != rows or @as(usize, @intCast(residual.dim(1))) != hidden_size) return null;

    const shape = [_]i32{ @intCast(rows), @intCast(hidden_size) };
    var output_device = try MetalTensor.deviceAllocate(runtime, rows * hidden_size * @sizeOf(f32), .private, &shape);
    errdefer output_device.deinit();
    const rc = termite_metal_decode_runtime_apply_dense_linear_layer_norm_device(
        runtime,
        linear_slot,
        layer_norm_slot,
        input.deviceHandle(),
        input.deviceByteOffset(),
        residual.deviceHandle(),
        residual.deviceByteOffset(),
        rows,
        in_dim,
        hidden_size,
        eps,
        output_device.deviceHandle(),
        output_device.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return output_device;
}

pub fn tryApplyDenseRuntimeLinearPair(
    self: anytype,
    slot_a: usize,
    slot_b: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !?RuntimeLinearPairResult {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    if (self.raw_linear_slot_kinds[slot_a] != .dense or self.raw_linear_slot_kinds[slot_b] != .dense) return null;

    if (input.isDevice()) {
        if (frame_active and rows != 1) {
            var first_handle: ?*anyopaque = null;
            var second_handle: ?*anyopaque = null;
            const scratch_rc = termite_metal_decode_runtime_apply_dense_linear_pair_slots_scratch_device(
                runtime,
                slot_a,
                slot_b,
                input.deviceHandle(),
                input.deviceByteOffset(),
                rows,
                in_dim,
                out_dim,
                &first_handle,
                &second_handle,
            );
            if (scratch_rc == 0) {
                const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
                var first = MetalTensor.deviceBorrowed(@ptrCast(runtime), first_handle orelse return null, 0, rows * out_dim * @sizeOf(f32), &shape);
                errdefer first.deinit();
                var second = MetalTensor.deviceBorrowed(@ptrCast(runtime), second_handle orelse return null, 0, rows * out_dim * @sizeOf(f32), &shape);
                errdefer second.deinit();
                return .{
                    .first = first,
                    .second = second,
                };
            }
            return null;
        }
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        var first_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
        errdefer first_device.deinit();
        var second_device = try MetalTensor.deviceAllocate(runtime, rows * out_dim * @sizeOf(f32), .private, &shape);
        errdefer second_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_linear_pair_slots_device(
            runtime,
            slot_a,
            slot_b,
            input.deviceHandle(),
            input.deviceByteOffset(),
            rows,
            in_dim,
            out_dim,
            first_device.deviceHandle(),
            first_device.deviceByteOffset(),
            second_device.deviceHandle(),
            second_device.deviceByteOffset(),
        );
        if (device_rc == 0) {
            return .{
                .first = first_device,
                .second = second_device,
            };
        }
        second_device.deinit();
        first_device.deinit();
    }

    if (frame_active) return null;

    var input_mut = input;
    const input_base = try tensorHostConstPtr(&input_mut);

    const first_out = try std.heap.c_allocator.alloc(f32, rows * out_dim);
    errdefer std.heap.c_allocator.free(first_out);
    const second_out = try std.heap.c_allocator.alloc(f32, rows * out_dim);
    errdefer std.heap.c_allocator.free(second_out);

    const rc = termite_metal_decode_runtime_apply_linear_pair_slots(
        runtime,
        slot_a,
        slot_b,
        input_base,
        rows,
        in_dim,
        out_dim,
        first_out.ptr,
        second_out.ptr,
    );
    if (rc != 0) return null;

    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    return .{
        .first = MetalTensor.owned(first_out, &shape),
        .second = MetalTensor.owned(second_out, &shape),
    };
}

pub fn tryApplyDenseRuntimeLinearQkv(
    self: anytype,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
) !?RuntimeLinearTripleResult {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    if (self.raw_linear_slot_kinds[q_slot] != .dense or self.raw_linear_slot_kinds[k_slot] != .dense or self.raw_linear_slot_kinds[v_slot] != .dense) return null;

    if (rows == 1 and input.isDevice()) {
        const q_shape = [_]i32{ @intCast(rows), @intCast(q_out_dim) };
        const kv_shape = [_]i32{ @intCast(rows), @intCast(kv_out_dim) };
        var q_device = try MetalTensor.deviceAllocate(runtime, rows * q_out_dim * @sizeOf(f32), .private, &q_shape);
        errdefer q_device.deinit();
        var k_device = try MetalTensor.deviceAllocate(runtime, rows * kv_out_dim * @sizeOf(f32), .private, &kv_shape);
        errdefer k_device.deinit();
        var v_device = try MetalTensor.deviceAllocate(runtime, rows * kv_out_dim * @sizeOf(f32), .private, &kv_shape);
        errdefer v_device.deinit();
        const device_rc = termite_metal_decode_runtime_apply_linear_qkv_slots_device(
            runtime,
            q_slot,
            k_slot,
            v_slot,
            input.deviceHandle(),
            input.deviceByteOffset(),
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
            q_device.deviceHandle(),
            q_device.deviceByteOffset(),
            k_device.deviceHandle(),
            k_device.deviceByteOffset(),
            v_device.deviceHandle(),
            v_device.deviceByteOffset(),
        );
        if (device_rc == 0) {
            return .{
                .first = q_device,
                .second = k_device,
                .third = v_device,
            };
        }
        v_device.deinit();
        k_device.deinit();
        q_device.deinit();
    }
    if (rows != 1 and input.isDevice()) {
        if (frame_active and !getenvBool("TERMITE_METAL_DISABLE_DENSE_QKV_PACKED")) {
            var q_handle: ?*anyopaque = null;
            var k_handle: ?*anyopaque = null;
            var v_handle: ?*anyopaque = null;
            const scratch_rc = termite_metal_decode_runtime_apply_dense_linear_qkv_slots_scratch_device(
                runtime,
                q_slot,
                k_slot,
                v_slot,
                input.deviceHandle(),
                input.deviceByteOffset(),
                rows,
                in_dim,
                q_out_dim,
                kv_out_dim,
                &q_handle,
                &k_handle,
                &v_handle,
            );
            if (scratch_rc == 0) {
                const q_shape = [_]i32{ @intCast(rows), @intCast(q_out_dim) };
                const kv_shape = [_]i32{ @intCast(rows), @intCast(kv_out_dim) };
                var q = MetalTensor.deviceBorrowed(@ptrCast(runtime), q_handle orelse return null, 0, rows * q_out_dim * @sizeOf(f32), &q_shape);
                errdefer q.deinit();
                var k = MetalTensor.deviceBorrowed(@ptrCast(runtime), k_handle orelse return null, 0, rows * kv_out_dim * @sizeOf(f32), &kv_shape);
                errdefer k.deinit();
                var v = MetalTensor.deviceBorrowed(@ptrCast(runtime), v_handle orelse return null, 0, rows * kv_out_dim * @sizeOf(f32), &kv_shape);
                errdefer v.deinit();
                return .{
                    .first = q,
                    .second = k,
                    .third = v,
                };
            }
            return null;
        }
        var q_device = (try tryApplyDenseRuntimeLinear(self, q_slot, input, rows, in_dim, q_out_dim)) orelse return null;
        errdefer q_device.deinit();
        var k_device = (try tryApplyDenseRuntimeLinear(self, k_slot, input, rows, in_dim, kv_out_dim)) orelse {
            q_device.deinit();
            return null;
        };
        errdefer k_device.deinit();
        const v_device = (try tryApplyDenseRuntimeLinear(self, v_slot, input, rows, in_dim, kv_out_dim)) orelse {
            k_device.deinit();
            q_device.deinit();
            return null;
        };
        return .{
            .first = q_device,
            .second = k_device,
            .third = v_device,
        };
    }

    if (frame_active) return null;

    var input_mut = input;
    const input_base = try tensorHostConstPtr(&input_mut);
    const q_out = try std.heap.c_allocator.alloc(f32, rows * q_out_dim);
    errdefer std.heap.c_allocator.free(q_out);
    const k_out = try std.heap.c_allocator.alloc(f32, rows * kv_out_dim);
    errdefer std.heap.c_allocator.free(k_out);
    const v_out = try std.heap.c_allocator.alloc(f32, rows * kv_out_dim);
    errdefer std.heap.c_allocator.free(v_out);

    const rc = termite_metal_decode_runtime_apply_linear_qkv_slots(
        runtime,
        q_slot,
        k_slot,
        v_slot,
        input_base,
        rows,
        in_dim,
        q_out_dim,
        kv_out_dim,
        q_out.ptr,
        k_out.ptr,
        v_out.ptr,
    );
    if (rc != 0) return null;

    const q_shape = [_]i32{ @intCast(rows), @intCast(q_out_dim) };
    const kv_shape = [_]i32{ @intCast(rows), @intCast(kv_out_dim) };
    return .{
        .first = MetalTensor.owned(q_out, &q_shape),
        .second = MetalTensor.owned(k_out, &kv_shape),
        .third = MetalTensor.owned(v_out, &kv_shape),
    };
}

pub fn tryApplyQuantizedRuntimeLinearQkv(
    self: anytype,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
) !?RuntimeLinearTripleResult {
    const DirectCase = enum {
        q4_q4,
        q5_q4,
        q8_0,
    };
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);
    const q_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, q_slot, in_dim, q_out_dim);
    const k_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, k_slot, in_dim, kv_out_dim);
    const v_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, v_slot, in_dim, kv_out_dim);
    if (q_kind == .none or k_kind == .none or v_kind == .none) return null;
    const direct_case: ?DirectCase = if (q_kind == .q4_k and k_kind == .q4_k and v_kind == .q4_k)
        .q4_q4
    else if (q_kind == .q5_k and k_kind == .q4_k and v_kind == .q4_k)
        .q5_q4
    else if (q_kind == .q8_0 and k_kind == .q8_0 and v_kind == .q8_0)
        .q8_0
    else
        null;

    if (referenceQuantizedRuntimeLinearDebug()) {
        var q_out = (try tryApplyQuantizedRuntimeLinear(self, q_slot, input, rows, in_dim, q_out_dim)) orelse return null;
        errdefer q_out.deinit();
        const k_out = (try tryApplyQuantizedRuntimeLinear(self, k_slot, input, rows, in_dim, kv_out_dim)) orelse {
            var q_out_mut = q_out;
            q_out_mut.deinit();
            return null;
        };
        errdefer {
            var q_out_mut = q_out;
            q_out_mut.deinit();
            var k_out_mut = k_out;
            k_out_mut.deinit();
        }
        const v_out = (try tryApplyQuantizedRuntimeLinear(self, v_slot, input, rows, in_dim, kv_out_dim)) orelse {
            var q_out_mut = q_out;
            q_out_mut.deinit();
            var k_out_mut = k_out;
            k_out_mut.deinit();
            return null;
        };
        return .{
            .first = q_out,
            .second = k_out,
            .third = v_out,
        };
    }

    if (input.isDevice() and direct_case != null) {
        const q_shape = [_]i32{ @intCast(rows), @intCast(q_out_dim) };
        const kv_shape = [_]i32{ @intCast(rows), @intCast(kv_out_dim) };
        var q_device = try MetalTensor.deviceAllocate(runtime, rows * q_out_dim * @sizeOf(f32), .private, &q_shape);
        errdefer q_device.deinit();
        var k_device = try MetalTensor.deviceAllocate(runtime, rows * kv_out_dim * @sizeOf(f32), .private, &kv_shape);
        errdefer k_device.deinit();
        var v_device = try MetalTensor.deviceAllocate(runtime, rows * kv_out_dim * @sizeOf(f32), .private, &kv_shape);
        errdefer v_device.deinit();
        const device_rc = switch (direct_case orelse return null) {
            .q4_q4 => termite_metal_decode_runtime_apply_quantized_linear_qkv_slots_device(
                runtime,
                @intFromEnum(MetalQuantFormat.q4_k),
                @intFromEnum(MetalQuantFormat.q4_k),
                @intFromEnum(MetalQuantFormat.q4_k),
                q_slot,
                k_slot,
                v_slot,
                input.deviceHandle(),
                input.deviceByteOffset(),
                rows,
                in_dim,
                q_out_dim,
                kv_out_dim,
                q_device.deviceHandle(),
                q_device.deviceByteOffset(),
                k_device.deviceHandle(),
                k_device.deviceByteOffset(),
                v_device.deviceHandle(),
                v_device.deviceByteOffset(),
            ),
            .q5_q4 => termite_metal_decode_runtime_apply_quantized_linear_qkv_slots_device(
                runtime,
                @intFromEnum(MetalQuantFormat.q5_k),
                @intFromEnum(MetalQuantFormat.q4_k),
                @intFromEnum(MetalQuantFormat.q4_k),
                q_slot,
                k_slot,
                v_slot,
                input.deviceHandle(),
                input.deviceByteOffset(),
                rows,
                in_dim,
                q_out_dim,
                kv_out_dim,
                q_device.deviceHandle(),
                q_device.deviceByteOffset(),
                k_device.deviceHandle(),
                k_device.deviceByteOffset(),
                v_device.deviceHandle(),
                v_device.deviceByteOffset(),
            ),
            .q8_0 => termite_metal_decode_runtime_apply_quantized_linear_qkv_slots_device(
                runtime,
                @intFromEnum(MetalQuantFormat.q8_0),
                @intFromEnum(MetalQuantFormat.q8_0),
                @intFromEnum(MetalQuantFormat.q8_0),
                q_slot,
                k_slot,
                v_slot,
                input.deviceHandle(),
                input.deviceByteOffset(),
                rows,
                in_dim,
                q_out_dim,
                kv_out_dim,
                q_device.deviceHandle(),
                q_device.deviceByteOffset(),
                k_device.deviceHandle(),
                k_device.deviceByteOffset(),
                v_device.deviceHandle(),
                v_device.deviceByteOffset(),
            ),
        };
        if (device_rc == 0) {
            if (!applyRuntimeLinearBiasDevice(self, q_slot, &q_device, rows, q_out_dim) or
                !applyRuntimeLinearBiasDevice(self, k_slot, &k_device, rows, kv_out_dim) or
                !applyRuntimeLinearBiasDevice(self, v_slot, &v_device, rows, kv_out_dim))
            {
                v_device.deinit();
                k_device.deinit();
                q_device.deinit();
                return null;
            }
            return .{
                .first = q_device,
                .second = k_device,
                .third = v_device,
            };
        }
        v_device.deinit();
        k_device.deinit();
        q_device.deinit();
    }

    if (input.isDevice() and
        quantizedRuntimeLinearKindHasSingleStageDeviceKernel(q_kind) and
        quantizedRuntimeLinearKindHasSingleStageDeviceKernel(k_kind) and
        quantizedRuntimeLinearKindHasSingleStageDeviceKernel(v_kind))
    {
        var q_out = (try tryApplyQuantizedRuntimeLinear(self, q_slot, input, rows, in_dim, q_out_dim)) orelse return null;
        errdefer q_out.deinit();
        const k_out = (try tryApplyQuantizedRuntimeLinear(self, k_slot, input, rows, in_dim, kv_out_dim)) orelse {
            var q_out_mut = q_out;
            q_out_mut.deinit();
            return null;
        };
        errdefer {
            var q_out_mut = q_out;
            q_out_mut.deinit();
            var k_out_mut = k_out;
            k_out_mut.deinit();
        }
        const v_out = (try tryApplyQuantizedRuntimeLinear(self, v_slot, input, rows, in_dim, kv_out_dim)) orelse {
            var q_out_mut = q_out;
            q_out_mut.deinit();
            var k_out_mut = k_out;
            k_out_mut.deinit();
            return null;
        };
        return .{
            .first = q_out,
            .second = k_out,
            .third = v_out,
        };
    }

    if (direct_case == .q8_0) return null;
    if (frame_active) return null;

    var input_mut = input;
    const input_base = try tensorHostConstPtr(&input_mut);

    const q_out = try std.heap.c_allocator.alloc(f32, rows * q_out_dim);
    errdefer std.heap.c_allocator.free(q_out);
    const k_out = try std.heap.c_allocator.alloc(f32, rows * kv_out_dim);
    errdefer std.heap.c_allocator.free(k_out);
    const v_out = try std.heap.c_allocator.alloc(f32, rows * kv_out_dim);
    errdefer std.heap.c_allocator.free(v_out);

    const rc = switch (direct_case orelse return null) {
        .q4_q4 => termite_metal_decode_runtime_apply_quantized_q_kv_pair_linear_qkv_slots(
            runtime,
            @intFromEnum(MetalQuantFormat.q4_k),
            @intFromEnum(MetalQuantFormat.q4_k),
            q_slot,
            k_slot,
            v_slot,
            input_base,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
            q_out.ptr,
            k_out.ptr,
            v_out.ptr,
        ),
        .q5_q4 => termite_metal_decode_runtime_apply_quantized_q_kv_pair_linear_qkv_slots(
            runtime,
            @intFromEnum(MetalQuantFormat.q5_k),
            @intFromEnum(MetalQuantFormat.q4_k),
            q_slot,
            k_slot,
            v_slot,
            input_base,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
            q_out.ptr,
            k_out.ptr,
            v_out.ptr,
        ),
        .q8_0 => unreachable,
    };
    if (rc != 0) return null;
    if (!applyRuntimeLinearBiasHost(self, q_slot, q_out, rows, q_out_dim) or
        !applyRuntimeLinearBiasHost(self, k_slot, k_out, rows, kv_out_dim) or
        !applyRuntimeLinearBiasHost(self, v_slot, v_out, rows, kv_out_dim))
    {
        std.heap.c_allocator.free(v_out);
        std.heap.c_allocator.free(k_out);
        std.heap.c_allocator.free(q_out);
        return null;
    }

    const q_shape = [_]i32{ @intCast(rows), @intCast(q_out_dim) };
    const kv_shape = [_]i32{ @intCast(rows), @intCast(kv_out_dim) };
    return .{
        .first = MetalTensor.owned(q_out, &q_shape),
        .second = MetalTensor.owned(k_out, &kv_shape),
        .third = MetalTensor.owned(v_out, &kv_shape),
    };
}

pub fn tryApplyQuantizedRuntimeLinearQkvScratch(
    self: anytype,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input: MetalTensor,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
) !?RuntimeLinearTripleResult {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or rows == 0) return null;
    const q_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, q_slot, in_dim, q_out_dim);
    const k_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, k_slot, in_dim, kv_out_dim);
    const v_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, v_slot, in_dim, kv_out_dim);
    if (!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(q_kind) or
        !quantizedRuntimeLinearKindHasSingleStageDeviceKernel(k_kind) or
        !quantizedRuntimeLinearKindHasSingleStageDeviceKernel(v_kind)) return null;
    const q_format = metalQuantFormatForKind(q_kind);
    const k_format = metalQuantFormatForKind(k_kind);
    const v_format = metalQuantFormatForKind(v_kind);
    if (q_format == .unsupported or k_format == .unsupported or v_format == .unsupported) return null;
    var q_handle: ?*anyopaque = null;
    var k_handle: ?*anyopaque = null;
    var v_handle: ?*anyopaque = null;
    const rc = termite_metal_decode_runtime_apply_quantized_linear_qkv_slots_scratch_device(
        runtime,
        @intFromEnum(q_format),
        @intFromEnum(k_format),
        @intFromEnum(v_format),
        q_slot,
        k_slot,
        v_slot,
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        in_dim,
        q_out_dim,
        kv_out_dim,
        &q_handle,
        &k_handle,
        &v_handle,
    );
    if (rc != 0) return null;
    const q_shape = [_]i32{ @intCast(rows), @intCast(q_out_dim) };
    const kv_shape = [_]i32{ @intCast(rows), @intCast(kv_out_dim) };
    var q = MetalTensor.deviceBorrowed(@ptrCast(runtime), q_handle orelse return null, 0, rows * q_out_dim * @sizeOf(f32), &q_shape);
    errdefer q.deinit();
    var k = MetalTensor.deviceBorrowed(@ptrCast(runtime), k_handle orelse return null, 0, rows * kv_out_dim * @sizeOf(f32), &kv_shape);
    errdefer k.deinit();
    var v = MetalTensor.deviceBorrowed(@ptrCast(runtime), v_handle orelse return null, 0, rows * kv_out_dim * @sizeOf(f32), &kv_shape);
    errdefer v.deinit();
    if (!applyRuntimeLinearBiasDevice(self, q_slot, &q, rows, q_out_dim) or
        !applyRuntimeLinearBiasDevice(self, k_slot, &k, rows, kv_out_dim) or
        !applyRuntimeLinearBiasDevice(self, v_slot, &v, rows, kv_out_dim)) return null;
    return .{
        .first = q,
        .second = k,
        .third = v,
    };
}

pub fn tryRawPostGateI2SResidualHost(
    self: anytype,
    input: [*c]const f32,
    residual: [*c]const f32,
    intermediate_size: usize,
    hidden_size: usize,
    post_gate_rms_norm_slot: usize,
    down_linear_slot: usize,
    output: [*c]f32,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (!ensureI2SRuntimeLinearSlotPrepared(self, down_linear_slot, intermediate_size, hidden_size)) return false;
    return termite_metal_decode_runtime_apply_rms_norm_i2_s_linear_residual_slot(
        runtime,
        input,
        residual,
        intermediate_size,
        hidden_size,
        post_gate_rms_norm_slot,
        down_linear_slot,
        output,
    ) == 0;
}

pub fn tryRawQuantizedGatedFfnResidualHost(
    self: anytype,
    request: anytype,
    stats: anytype,
    logged_unsupported_type: *bool,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    const none = std.math.maxInt(usize);
    if (request.gate_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.up_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.down_linear_slot >= decoder_runtime_linear_slot_capacity)
    {
        return false;
    }
    if (!self.raw_linear_slots_prepared[request.gate_linear_slot] or
        !self.raw_linear_slots_prepared[request.up_linear_slot] or
        !self.raw_linear_slots_prepared[request.down_linear_slot])
    {
        return false;
    }
    if (self.raw_linear_slot_kinds[request.gate_linear_slot] != .quantized or
        self.raw_linear_slot_kinds[request.up_linear_slot] != .quantized or
        self.raw_linear_slot_kinds[request.down_linear_slot] != .quantized)
    {
        return false;
    }

    const gate_storage = self.raw_linear_slot_quantized_storage[request.gate_linear_slot] orelse return false;
    const up_storage = self.raw_linear_slot_quantized_storage[request.up_linear_slot] orelse return false;
    const down_storage = self.raw_linear_slot_quantized_storage[request.down_linear_slot] orelse return false;
    const gate_bytes = gate_storage.preparedBytes(.row_major_blocks) orelse gate_storage.raw_bytes;
    const up_bytes = up_storage.preparedBytes(.row_major_blocks) orelse up_storage.raw_bytes;
    const down_bytes = down_storage.preparedBytes(.row_major_blocks) orelse down_storage.raw_bytes;
    const request_eps: f32 = if (@hasField(@TypeOf(request), "eps")) request.eps else 0.0;

    if (std.meta.eql(gate_storage.tensor_type, up_storage.tensor_type) and
        std.meta.eql(gate_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q4_K }) and
        std.meta.eql(down_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q5_K }))
    {
        traceQuantizedGatedFfnPath("q4_k_pair_q5_k_down", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
        const post_gate_rms_norm_slot = request.post_gate_rms_norm_slot orelse return false;
        if (ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size) != .q4_k or
            ensureQuantizedRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size) != .q4_k or
            ensureQuantizedRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size) != .q5_k)
        {
            stats.quantized_gated_ffn_runtime_failures += 1;
            return false;
        }
        const mixed_rc = termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q5_k_down_slots(
            runtime,
            request.input,
            request.residual,
            request.rows,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.gate_linear_slot,
            request.up_linear_slot,
            post_gate_rms_norm_slot,
            request.post_down_rms_norm_slot orelse none,
            request.down_linear_slot,
            request.output,
        );
        if (mixed_rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
        return mixed_rc == 0;
    }

    if (std.meta.eql(gate_storage.tensor_type, up_storage.tensor_type) and
        std.meta.eql(gate_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q4_K }) and
        std.meta.eql(down_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q6_K }))
    {
        traceQuantizedGatedFfnPath("q4_k_pair_q6_k_down", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
        const post_gate_rms_norm_slot = request.post_gate_rms_norm_slot orelse return false;
        if (ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size) != .q4_k or
            ensureQuantizedRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size) != .q4_k or
            ensureQuantizedRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size) != .q6_k)
        {
            stats.quantized_gated_ffn_runtime_failures += 1;
            return false;
        }
        const mixed_rc = termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q6_k_down_slots(
            runtime,
            request.input,
            request.residual,
            request.rows,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.gate_linear_slot,
            request.up_linear_slot,
            post_gate_rms_norm_slot,
            request.post_down_rms_norm_slot orelse none,
            request.down_linear_slot,
            request.output,
        );
        if (mixed_rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
        return mixed_rc == 0;
    }

    if (std.meta.eql(gate_storage.tensor_type, up_storage.tensor_type) and
        std.meta.eql(gate_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q4_0 }) and
        std.meta.eql(down_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q8_0 }))
    {
        traceQuantizedGatedFfnPath("q4_0_pair_q8_0_down", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
        const post_gate_rms_norm_slot = request.post_gate_rms_norm_slot orelse return false;
        if (ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size) != .q4_0 or
            ensureQuantizedRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size) != .q4_0 or
            ensureQuantizedRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size) != .q8_0)
        {
            stats.quantized_gated_ffn_runtime_failures += 1;
            return false;
        }
        const mixed_rc = termite_metal_decode_runtime_apply_gated_ffn_residual_q4_0_pair_q8_0_down_slots(
            runtime,
            request.input,
            request.residual,
            request.rows,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.gate_linear_slot,
            request.up_linear_slot,
            post_gate_rms_norm_slot,
            request.post_down_rms_norm_slot orelse none,
            request.down_linear_slot,
            request.output,
        );
        if (mixed_rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
        return mixed_rc == 0;
    }

    if (!std.meta.eql(gate_storage.tensor_type, up_storage.tensor_type) or
        !std.meta.eql(gate_storage.tensor_type, down_storage.tensor_type))
    {
        stats.quantized_gated_ffn_type_mismatches += 1;
        return false;
    }

    return switch (gate_storage.tensor_type) {
        .known => |known| switch (known) {
            .I2_S => blk: {
                traceQuantizedGatedFfnPath("i2_s", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
                if (!ensureI2SRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size) or
                    !ensureI2SRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size) or
                    !ensureI2SRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size))
                {
                    stats.quantized_gated_ffn_runtime_failures += 1;
                    break :blk false;
                }
                const rc = termite_metal_decode_runtime_apply_gated_ffn_residual_i2_s_slots(
                    runtime,
                    request.input,
                    request.residual,
                    request.rows,
                    request.hidden_size,
                    request.intermediate_size,
                    @intFromEnum(request.activation),
                    request.gate_linear_slot,
                    request.up_linear_slot,
                    request.post_gate_rms_norm_slot orelse none,
                    request.down_linear_slot,
                    request.output,
                );
                if (rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
                break :blk rc == 0;
            },
            .Q8_0 => blk: {
                traceQuantizedGatedFfnPath("q8_0", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
                if (ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size) != .q8_0 or
                    ensureQuantizedRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size) != .q8_0 or
                    ensureQuantizedRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size) != .q8_0)
                {
                    stats.quantized_gated_ffn_runtime_failures += 1;
                    break :blk false;
                }
                const input_ptr: [*c]const f32 = @ptrCast(request.input);
                const residual_ptr: [*c]const f32 = @ptrCast(request.residual);
                const output_ptr: [*c]f32 = @ptrCast(request.output);
                const rc = if (request.rows > 1) rc: {
                    var row: usize = 0;
                    while (row < request.rows) : (row += 1) {
                        const hidden_offset = row * request.hidden_size;
                        const row_rc = termite_metal_decode_runtime_apply_gated_ffn_residual_q8_0_slots(
                            runtime,
                            input_ptr + hidden_offset,
                            residual_ptr + hidden_offset,
                            1,
                            request.hidden_size,
                            request.intermediate_size,
                            @intFromEnum(request.activation),
                            request_eps,
                            request.gate_linear_slot,
                            request.up_linear_slot,
                            request.post_gate_rms_norm_slot orelse none,
                            request.post_down_rms_norm_slot orelse none,
                            request.down_linear_slot,
                            output_ptr + hidden_offset,
                        );
                        if (row_rc != 0) break :rc row_rc;
                    }
                    break :rc 0;
                } else termite_metal_decode_runtime_apply_gated_ffn_residual_q8_0_slots(
                    runtime,
                    request.input,
                    request.residual,
                    1,
                    request.hidden_size,
                    request.intermediate_size,
                    @intFromEnum(request.activation),
                    request_eps,
                    request.gate_linear_slot,
                    request.up_linear_slot,
                    request.post_gate_rms_norm_slot orelse none,
                    request.post_down_rms_norm_slot orelse none,
                    request.down_linear_slot,
                    request.output,
                );
                if (rc != 0) {
                    stats.quantized_gated_ffn_runtime_failures += 1;
                    if (traceQuantizedGatedFfnRcRequested()) {
                        std.debug.print(
                            "metal-gated-ffn-rc path=q8_0 rc={d} rows={d} hidden={d} intermediate={d} post_gate_slot={d} post_down_slot={d}\n",
                            .{
                                rc,
                                request.rows,
                                request.hidden_size,
                                request.intermediate_size,
                                request.post_gate_rms_norm_slot orelse none,
                                request.post_down_rms_norm_slot orelse none,
                            },
                        );
                    }
                }
                break :blk rc == 0;
            },
            .Q4_K => blk: {
                traceQuantizedGatedFfnPath("q4_k", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
                if (ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size) != .q4_k or
                    ensureQuantizedRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size) != .q4_k or
                    ensureQuantizedRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size) != .q4_k)
                {
                    stats.quantized_gated_ffn_runtime_failures += 1;
                    break :blk false;
                }
                const rc = termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_slots(
                    runtime,
                    request.input,
                    request.residual,
                    request.rows,
                    request.hidden_size,
                    request.intermediate_size,
                    @intFromEnum(request.activation),
                    request.gate_linear_slot,
                    request.up_linear_slot,
                    request.post_gate_rms_norm_slot orelse none,
                    request.post_down_rms_norm_slot orelse none,
                    request.down_linear_slot,
                    request.output,
                );
                if (rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
                break :blk rc == 0;
            },
            .Q6_K => blk: {
                traceQuantizedGatedFfnPath("q6_k", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
                if (ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size) != .q6_k or
                    ensureQuantizedRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size) != .q6_k or
                    ensureQuantizedRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size) != .q6_k)
                {
                    stats.quantized_gated_ffn_runtime_failures += 1;
                    break :blk false;
                }
                const rc = termite_metal_decode_runtime_apply_gated_ffn_residual_q6_k_slots(
                    runtime,
                    request.input,
                    request.residual,
                    request.rows,
                    request.hidden_size,
                    request.intermediate_size,
                    @intFromEnum(request.activation),
                    request.gate_linear_slot,
                    request.up_linear_slot,
                    request.post_gate_rms_norm_slot orelse none,
                    request.post_down_rms_norm_slot orelse none,
                    request.down_linear_slot,
                    request.output,
                );
                if (rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
                break :blk rc == 0;
            },
            .TL1 => blk: {
                traceQuantizedGatedFfnPath("tl1", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
                const gate_view = try quant_codec.bitnetTL1View(gate_storage.shape, gate_bytes);
                const up_view = try quant_codec.bitnetTL1View(up_storage.shape, up_bytes);
                const down_view = try quant_codec.bitnetTL1View(down_storage.shape, down_bytes);
                if (gate_view.cols != request.hidden_size or gate_view.rows != request.intermediate_size) break :blk false;
                if (up_view.cols != request.hidden_size or up_view.rows != request.intermediate_size) break :blk false;
                if (down_view.cols != request.intermediate_size or down_view.rows != request.hidden_size) break :blk false;
                const rc = termite_metal_decode_runtime_apply_gated_ffn_residual_tl1(
                    runtime,
                    request.input,
                    request.residual,
                    request.hidden_size,
                    request.intermediate_size,
                    @intFromEnum(request.activation),
                    gate_bytes.ptr,
                    gate_bytes.len,
                    @intCast(gate_view.packed_bytes.len),
                    @intCast(gate_view.config.bm),
                    @intCast(gate_view.config.by),
                    @intCast(gate_view.config.bmm),
                    up_bytes.ptr,
                    up_bytes.len,
                    @intCast(up_view.packed_bytes.len),
                    @intCast(up_view.config.bm),
                    @intCast(up_view.config.by),
                    @intCast(up_view.config.bmm),
                    request.post_gate_rms_norm_slot orelse none,
                    down_bytes.ptr,
                    down_bytes.len,
                    @intCast(down_view.packed_bytes.len),
                    @intCast(down_view.config.bm),
                    @intCast(down_view.config.by),
                    @intCast(down_view.config.bmm),
                    request.output,
                );
                if (rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
                break :blk rc == 0;
            },
            else => blk: {
                stats.quantized_gated_ffn_unsupported_types += 1;
                if (!logged_unsupported_type.*) {
                    logged_unsupported_type.* = true;
                    std.log.info(
                        "quantized gated ffn direct runtime path unavailable for tensor type={s}; using quant-family backend fallback",
                        .{@tagName(known)},
                    );
                }
                break :blk false;
            },
        },
        .bitnet_tl2 => blk: {
            traceQuantizedGatedFfnPath("bitnet_tl2", gate_storage.tensor_type, up_storage.tensor_type, down_storage.tensor_type);
            const gate_view = try quant_codec.bitnetTL2View(gate_storage.shape, gate_bytes);
            const up_view = try quant_codec.bitnetTL2View(up_storage.shape, up_bytes);
            const down_view = try quant_codec.bitnetTL2View(down_storage.shape, down_bytes);
            if (gate_view.cols != request.hidden_size or gate_view.rows != request.intermediate_size) break :blk false;
            if (up_view.cols != request.hidden_size or up_view.rows != request.intermediate_size) break :blk false;
            if (down_view.cols != request.intermediate_size or down_view.rows != request.hidden_size) break :blk false;
            const gate_scale_off = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(gate_view.cols), @intCast(gate_view.rows) }) orelse return error.UnsupportedTensorShape) - 32;
            const up_scale_off = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(up_view.cols), @intCast(up_view.rows) }) orelse return error.UnsupportedTensorShape) - 32;
            const down_scale_off = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(down_view.cols), @intCast(down_view.rows) }) orelse return error.UnsupportedTensorShape) - 32;
            const rc = termite_metal_decode_runtime_apply_gated_ffn_residual_tl2(
                runtime,
                request.input,
                request.residual,
                request.hidden_size,
                request.intermediate_size,
                @intFromEnum(request.activation),
                gate_bytes.ptr,
                gate_bytes.len,
                @intCast(gate_scale_off),
                @intCast(gate_view.three_values.len),
                @intCast(gate_view.three_signs.len),
                @intCast(gate_view.config.bm),
                @intCast(gate_view.config.by),
                @intCast(gate_view.config.bmm),
                @intCast(gate_view.three_cols),
                @intCast(gate_view.two_cols),
                up_bytes.ptr,
                up_bytes.len,
                @intCast(up_scale_off),
                @intCast(up_view.three_values.len),
                @intCast(up_view.three_signs.len),
                @intCast(up_view.config.bm),
                @intCast(up_view.config.by),
                @intCast(up_view.config.bmm),
                @intCast(up_view.three_cols),
                @intCast(up_view.two_cols),
                request.post_gate_rms_norm_slot orelse none,
                down_bytes.ptr,
                down_bytes.len,
                @intCast(down_scale_off),
                @intCast(down_view.three_values.len),
                @intCast(down_view.three_signs.len),
                @intCast(down_view.config.bm),
                @intCast(down_view.config.by),
                @intCast(down_view.config.bmm),
                @intCast(down_view.three_cols),
                @intCast(down_view.two_cols),
                request.output,
            );
            if (rc != 0) stats.quantized_gated_ffn_runtime_failures += 1;
            break :blk rc == 0;
        },
        .unknown => blk: {
            stats.quantized_gated_ffn_unsupported_types += 1;
            if (!logged_unsupported_type.*) {
                logged_unsupported_type.* = true;
                std.log.info(
                    "quantized gated ffn direct runtime path unavailable for tensor type={s}; using quant-family backend fallback",
                    .{tensorTypeLabel(gate_storage.tensor_type)},
                );
            }
            break :blk false;
        },
    };
}

pub fn tryDeviceQuantizedGatedFfnResidual(
    self: anytype,
    request: anytype,
    input: MetalTensor,
    residual: MetalTensor,
    stats: anytype,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (!input.isDevice() or !residual.isDevice()) return null;
    // Batched Q8_0 gated FFN has diverged from rowwise execution for qLen>1.
    // Keep device residency by encoding one row at a time until the batched
    // kernel has parity coverage.
    if (request.gate_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.up_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.down_linear_slot >= decoder_runtime_linear_slot_capacity)
    {
        return null;
    }
    if (input.ndim() != 2 or residual.ndim() != 2) return null;
    if (@as(usize, @intCast(input.dim(0))) != request.rows or
        @as(usize, @intCast(input.dim(1))) != request.hidden_size or
        @as(usize, @intCast(residual.dim(0))) != request.rows or
        @as(usize, @intCast(residual.dim(1))) != request.hidden_size)
    {
        return null;
    }
    if (!self.raw_linear_slots_prepared[request.gate_linear_slot] or
        !self.raw_linear_slots_prepared[request.up_linear_slot] or
        !self.raw_linear_slots_prepared[request.down_linear_slot])
    {
        return null;
    }
    if (self.raw_linear_slot_kinds[request.gate_linear_slot] != .quantized or
        self.raw_linear_slot_kinds[request.up_linear_slot] != .quantized or
        self.raw_linear_slot_kinds[request.down_linear_slot] != .quantized)
    {
        return null;
    }

    const gate_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.gate_linear_slot, request.hidden_size, request.intermediate_size);
    const up_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.up_linear_slot, request.hidden_size, request.intermediate_size);
    const down_kind = ensureQuantizedRuntimeLinearSlotPrepared(self, request.down_linear_slot, request.intermediate_size, request.hidden_size);
    if (gate_kind == .none or up_kind == .none or down_kind == .none) {
        stats.quantized_gated_ffn_runtime_failures += 1;
        return null;
    }

    const all_q8_0 = gate_kind == .q8_0 and up_kind == .q8_0 and down_kind == .q8_0;
    if (!all_q8_0) {
        if (gate_kind != up_kind or
            !quantizedRuntimeLinearKindHasPairDeviceKernel(gate_kind) or
            !quantizedRuntimeLinearKindHasSingleStageDeviceKernel(down_kind))
        {
            return null;
        }

        const pair_started_at = monotonicNowNs();
        const gate_up = (try decoderRuntimeApplyLinearPair(self, .{
            .slot_a = request.gate_linear_slot,
            .slot_b = request.up_linear_slot,
            .input = input,
            .in_dim = request.hidden_size,
            .out_dim = request.intermediate_size,
        })) orelse return null;
        stats.quantized_gated_pair_nanos += @intCast(monotonicNowNs() - pair_started_at);
        var gate_first = gate_up.first;
        defer gate_first.deinit();
        var gate_second = gate_up.second;
        defer gate_second.deinit();

        const activation_started_at = monotonicNowNs();
        var activated = (try decoderRuntimeApplyActivation(self, .{
            .input = gate_first,
            .kind = request.activation,
            .dim = request.intermediate_size,
        }, stats)) orelse return null;
        defer activated.deinit();
        var gated = (try decoderRuntimeApplyMultiply(self, activated, gate_second, request.intermediate_size)) orelse return null;
        defer gated.deinit();
        stats.quantized_gated_activation_multiply_nanos += @intCast(monotonicNowNs() - activation_started_at);

        var post_gate_normed: ?MetalTensor = null;
        defer if (post_gate_normed) |*tensor| tensor.deinit();
        const down_input = if (request.post_gate_rms_norm_slot) |slot| blk: {
            const post_gate_norm_started_at = monotonicNowNs();
            post_gate_normed = (try decoderRuntimeApplyRmsNorm(self, .{
                .slot = slot,
                .input = gated,
                .hidden_size = request.intermediate_size,
                .eps = request.eps,
            }, stats)) orelse return null;
            stats.quantized_gated_post_gate_norm_nanos += @intCast(monotonicNowNs() - post_gate_norm_started_at);
            break :blk post_gate_normed.?;
        } else gated;

        const down_started_at = monotonicNowNs();
        var projected = (try tryApplyQuantizedRuntimeLinear(
            self,
            request.down_linear_slot,
            down_input,
            request.rows,
            request.intermediate_size,
            request.hidden_size,
        )) orelse return null;
        defer projected.deinit();
        stats.quantized_gated_down_nanos += @intCast(monotonicNowNs() - down_started_at);

        var post_down_normed: ?MetalTensor = null;
        defer if (post_down_normed) |*tensor| tensor.deinit();
        const add_lhs = if (request.post_down_rms_norm_slot) |slot| blk: {
            const post_down_norm_started_at = monotonicNowNs();
            post_down_normed = (try decoderRuntimeApplyRmsNorm(self, .{
                .slot = slot,
                .input = projected,
                .hidden_size = request.hidden_size,
                .eps = request.eps,
            }, stats)) orelse return null;
            stats.quantized_gated_post_gate_norm_nanos += @intCast(monotonicNowNs() - post_down_norm_started_at);
            break :blk post_down_normed.?;
        } else projected;

        const add_started_at = monotonicNowNs();
        const result = (try decoderRuntimeApplyAdd(self, .{
            .lhs = add_lhs,
            .rhs = residual,
            .dim = request.hidden_size,
        }, stats)) orelse return null;
        stats.quantized_gated_add_nanos += @intCast(monotonicNowNs() - add_started_at);
        return result;
    }

    const none = std.math.maxInt(usize);
    const out_shape = [_]i32{ @intCast(request.rows), @intCast(request.hidden_size) };
    var output = MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        request.rows * request.hidden_size * @sizeOf(f32),
        .private,
        &out_shape,
    ) catch return null;
    errdefer output.deinit();

    const input_handle = input.deviceHandle();
    const residual_handle = residual.deviceHandle();
    const output_handle = output.deviceHandle();
    const row_bytes = request.hidden_size * @sizeOf(f32);
    const rc = if (request.rows > 1) rc_blk: {
        var row: usize = 0;
        while (row < request.rows) : (row += 1) {
            const offset = row * row_bytes;
            const row_rc = termite_metal_decode_runtime_apply_gated_ffn_residual_q8_0_slots_device(
                runtime,
                input_handle,
                input.deviceByteOffset() + offset,
                residual_handle,
                residual.deviceByteOffset() + offset,
                1,
                request.hidden_size,
                request.intermediate_size,
                @intFromEnum(request.activation),
                request.gate_linear_slot,
                request.up_linear_slot,
                request.post_gate_rms_norm_slot orelse none,
                request.post_down_rms_norm_slot orelse none,
                request.eps,
                request.down_linear_slot,
                output_handle,
                output.deviceByteOffset() + offset,
            );
            if (row_rc != 0) break :rc_blk row_rc;
        }
        break :rc_blk 0;
    } else termite_metal_decode_runtime_apply_gated_ffn_residual_q8_0_slots_device(
        runtime,
        input_handle,
        input.deviceByteOffset(),
        residual_handle,
        residual.deviceByteOffset(),
        request.rows,
        request.hidden_size,
        request.intermediate_size,
        @intFromEnum(request.activation),
        request.gate_linear_slot,
        request.up_linear_slot,
        request.post_gate_rms_norm_slot orelse none,
        request.post_down_rms_norm_slot orelse none,
        request.eps,
        request.down_linear_slot,
        output_handle,
        output.deviceByteOffset(),
    );
    if (rc != 0) {
        stats.quantized_gated_ffn_runtime_failures += 1;
        return null;
    }
    return output;
}

pub fn shouldAttemptDirectQuantizedGatedFfn(
    self: anytype,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    down_linear_slot: usize,
    stats: anytype,
    logged_mixed_kind: *bool,
    logged_unsupported_kind: *bool,
) bool {
    const gate_storage = self.raw_linear_slot_quantized_storage[gate_linear_slot] orelse return false;
    const up_storage = self.raw_linear_slot_quantized_storage[up_linear_slot] orelse return false;
    const down_storage = self.raw_linear_slot_quantized_storage[down_linear_slot] orelse return false;
    if (std.meta.eql(gate_storage.tensor_type, up_storage.tensor_type) and
        std.meta.eql(gate_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q4_K }) and
        (std.meta.eql(down_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q5_K }) or
            std.meta.eql(down_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q6_K })))
    {
        return true;
    }
    if (std.meta.eql(gate_storage.tensor_type, up_storage.tensor_type) and
        std.meta.eql(gate_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q4_0 }) and
        std.meta.eql(down_storage.tensor_type, gguf_tensor_types.TensorType{ .known = .Q8_0 }))
    {
        return true;
    }
    if (!std.meta.eql(gate_storage.tensor_type, up_storage.tensor_type) or
        !std.meta.eql(gate_storage.tensor_type, down_storage.tensor_type))
    {
        stats.quantized_gated_ffn_backend_mixed_kind_fallbacks += 1;
        if (!logged_mixed_kind.*) {
            logged_mixed_kind.* = true;
            std.log.info(
                "quantized gated ffn direct runtime skipped due to mixed tensor types gate={s} up={s} down={s}",
                .{
                    tensorTypeLabel(gate_storage.tensor_type),
                    tensorTypeLabel(up_storage.tensor_type),
                    tensorTypeLabel(down_storage.tensor_type),
                },
            );
        }
        return false;
    }
    return switch (gate_storage.tensor_type) {
        .known => |known| switch (known) {
            .I2_S => true,
            .Q4_K => true,
            .Q6_K => true,
            .Q8_0 => true,
            .TL1 => true,
            else => blk: {
                stats.quantized_gated_ffn_backend_unsupported_kind_fallbacks += 1;
                if (!logged_unsupported_kind.*) {
                    logged_unsupported_kind.* = true;
                    std.log.info(
                        "quantized gated ffn direct runtime skipped for unsupported tensor type={s}",
                        .{@tagName(known)},
                    );
                }
                break :blk false;
            },
        },
        .bitnet_tl2 => true,
        .unknown => blk: {
            stats.quantized_gated_ffn_backend_unsupported_kind_fallbacks += 1;
            if (!logged_unsupported_kind.*) {
                logged_unsupported_kind.* = true;
                std.log.info(
                    "quantized gated ffn direct runtime skipped for unsupported tensor type=unknown",
                    .{},
                );
            }
            break :blk false;
        },
    };
}

pub fn runCompressedAttentionGatedPostGateQuantizedFfn(
    self: anytype,
    ctx: *anyopaque,
    request: anytype,
    attn_res: MetalTensor,
    ffn_normed: MetalTensor,
    stats: anytype,
    logged_unsupported_type: *bool,
    logged_backend_mixed_kind: *bool,
    logged_backend_unsupported_kind: *bool,
    apply_pair_fn: anytype,
    apply_linear_fn: anytype,
) !?MetalTensor {
    const post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot orelse return null;
    const frame_active = hasActiveFrame(self.raw_decode_runtime);

    const direct = if (ffn_normed.ndim() == 2 and attn_res.ndim() == 2) blk: {
        if (!frame_active and shouldAttemptDirectQuantizedGatedFfn(
            self,
            request.gate_ffn_linear_slot,
            request.up_ffn_linear_slot,
            request.down_ffn_linear_slot,
            stats,
            logged_backend_mixed_kind,
            logged_backend_unsupported_kind,
        )) {
            const out = try std.heap.c_allocator.alloc(f32, request.hidden_size);
            errdefer std.heap.c_allocator.free(out);
            var ffn_normed_mut = ffn_normed;
            var attn_res_mut = attn_res;
            if (try tryRawQuantizedGatedFfnResidualHost(self, .{
                .input = try tensorHostConstPtr(&ffn_normed_mut),
                .residual = try tensorHostConstPtr(&attn_res_mut),
                .rows = 1,
                .hidden_size = request.hidden_size,
                .intermediate_size = request.intermediate_size,
                .activation = request.activation,
                .gate_linear_slot = request.gate_ffn_linear_slot,
                .up_linear_slot = request.up_ffn_linear_slot,
                .down_linear_slot = request.down_ffn_linear_slot,
                .post_gate_rms_norm_slot = @as(?usize, post_gate_rms_norm_slot),
                .post_down_rms_norm_slot = null,
                .output = out.ptr,
            }, stats, logged_unsupported_type)) {
                stats.quantized_gated_ffn_direct_successes += 1;
                const shape = [_]i32{ 1, @intCast(request.hidden_size) };
                break :blk MetalTensor.owned(out, &shape);
            }
            stats.quantized_gated_ffn_direct_fallbacks += 1;
            std.heap.c_allocator.free(out);
        } else {
            stats.quantized_gated_ffn_backend_fallbacks += 1;
        }
        break :blk null;
    } else null;
    if (direct) |arr| return arr;

    if (comptime !build_options.enable_mlx) {
        const pair_started_at = monotonicNowNs();
        const gate_up = (try decoderRuntimeApplyLinearPair(self, .{
            .slot_a = request.gate_ffn_linear_slot,
            .slot_b = request.up_ffn_linear_slot,
            .input = ffn_normed,
            .in_dim = request.hidden_size,
            .out_dim = request.intermediate_size,
        })) orelse return null;
        stats.quantized_gated_pair_nanos += @intCast(monotonicNowNs() - pair_started_at);
        var gate_first = gate_up.first;
        defer gate_first.deinit();
        var gate_second = gate_up.second;
        defer gate_second.deinit();

        const activation_started_at = monotonicNowNs();
        var activated_tensor = (try decoderRuntimeApplyActivation(self, .{
            .input = gate_first,
            .kind = request.activation,
            .dim = request.intermediate_size,
        }, stats)) orelse return null;
        defer activated_tensor.deinit();
        var gated_tensor = (try decoderRuntimeApplyMultiply(
            self,
            activated_tensor,
            gate_second,
            request.intermediate_size,
        )) orelse return null;
        defer gated_tensor.deinit();
        stats.quantized_gated_activation_multiply_nanos += @intCast(monotonicNowNs() - activation_started_at);

        if (gated_tensor.ndim() == 2 and attn_res.ndim() == 2) {
            const out = try std.heap.c_allocator.alloc(f32, request.hidden_size);
            errdefer std.heap.c_allocator.free(out);
            const post_gate_direct_started_at = monotonicNowNs();
            var gated_tensor_mut = gated_tensor;
            var attn_res_mut = attn_res;
            if (!frame_active and try tryRawPostGateI2SResidualHost(
                self,
                try tensorHostConstPtr(&gated_tensor_mut),
                try tensorHostConstPtr(&attn_res_mut),
                request.intermediate_size,
                request.hidden_size,
                post_gate_rms_norm_slot,
                request.down_ffn_linear_slot,
                out.ptr,
            )) {
                stats.quantized_gated_post_gate_norm_nanos += @intCast(monotonicNowNs() - post_gate_direct_started_at);
                const shape = [_]i32{ 1, @intCast(request.hidden_size) };
                return MetalTensor.owned(out, &shape);
            }
            std.heap.c_allocator.free(out);
        }

        const post_gate_norm_started_at = monotonicNowNs();
        var normed_metal = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = post_gate_rms_norm_slot,
            .input = gated_tensor,
            .hidden_size = request.intermediate_size,
            .eps = request.eps,
        }, stats)) orelse return null;
        defer normed_metal.deinit();
        stats.quantized_gated_post_gate_norm_nanos += @intCast(monotonicNowNs() - post_gate_norm_started_at);

        const down_started_at = monotonicNowNs();
        var projected_tensor = (try decoderRuntimeApplyLinear(self, .{
            .slot = request.down_ffn_linear_slot,
            .input = normed_metal,
            .in_dim = request.intermediate_size,
            .out_dim = request.hidden_size,
        })) orelse return null;
        defer projected_tensor.deinit();
        stats.quantized_gated_down_nanos += @intCast(monotonicNowNs() - down_started_at);

        const add_started_at = monotonicNowNs();
        const result_tensor = (try decoderRuntimeApplyAdd(self, .{
            .lhs = projected_tensor,
            .rhs = attn_res,
            .dim = request.hidden_size,
        }, stats)) orelse return null;
        stats.quantized_gated_add_nanos += @intCast(monotonicNowNs() - add_started_at);
        return result_tensor;
    }

    if (comptime build_options.enable_mlx and
        backendUsesMlxArrayPair(apply_pair_fn) and
        backendUsesMlxArray(apply_linear_fn))
    {
        const ffn_normed_mlx = mlx_metal_bridge.borrowMetalTensorAsMlxArray(ffn_normed);
        defer _ = c.mlx_array_free(ffn_normed_mlx);

        const pair_started_at = monotonicNowNs();
        const gate_up = (try apply_pair_fn(ctx, &.{
            .slot_a = request.gate_ffn_linear_slot,
            .slot_b = request.up_ffn_linear_slot,
            .input = coerceBackendInput(apply_pair_fn, ffn_normed_mlx),
            .in_dim = request.hidden_size,
            .out_dim = request.intermediate_size,
        })) orelse return null;
        stats.quantized_gated_pair_nanos += @intCast(monotonicNowNs() - pair_started_at);
        defer _ = c.mlx_array_free(gate_up.first);
        defer _ = c.mlx_array_free(gate_up.second);

        const activation_started_at = monotonicNowNs();
        var gate_first_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const gate_first_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(gate_up.first, &gate_first_shape_buf);
        const activated_tensor = (try decoderRuntimeApplyActivation(self, .{
            .input = gate_first_tensor,
            .kind = request.activation,
            .dim = request.intermediate_size,
        }, stats)) orelse return null;
        var gate_second_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const gate_second_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(gate_up.second, &gate_second_shape_buf);
        var gated_tensor = (try decoderRuntimeApplyMultiply(
            self,
            activated_tensor,
            gate_second_tensor,
            request.intermediate_size,
        )) orelse {
            var activated_mut = activated_tensor;
            activated_mut.deinit();
            return null;
        };
        var activated_mut = activated_tensor;
        activated_mut.deinit();
        defer gated_tensor.deinit();
        stats.quantized_gated_activation_multiply_nanos += @intCast(monotonicNowNs() - activation_started_at);

        if (gated_tensor.ndim() == 2 and attn_res.ndim() == 2) {
            const out = try std.heap.c_allocator.alloc(f32, request.hidden_size);
            errdefer std.heap.c_allocator.free(out);
            const post_gate_direct_started_at = monotonicNowNs();
            var gated_tensor_mut = gated_tensor;
            var attn_res_mut = attn_res;
            if (!frame_active and try tryRawPostGateI2SResidualHost(
                self,
                try tensorHostConstPtr(&gated_tensor_mut),
                try tensorHostConstPtr(&attn_res_mut),
                request.intermediate_size,
                request.hidden_size,
                post_gate_rms_norm_slot,
                request.down_ffn_linear_slot,
                out.ptr,
            )) {
                stats.quantized_gated_post_gate_norm_nanos += @intCast(monotonicNowNs() - post_gate_direct_started_at);
                const shape = [_]i32{ 1, @intCast(request.hidden_size) };
                return MetalTensor.owned(out, &shape);
            }
            std.heap.c_allocator.free(out);
        }

        const post_gate_norm_started_at = monotonicNowNs();
        var normed_metal = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = post_gate_rms_norm_slot,
            .input = gated_tensor,
            .hidden_size = request.intermediate_size,
            .eps = request.eps,
        }, stats)) orelse return null;
        stats.quantized_gated_post_gate_norm_nanos += @intCast(monotonicNowNs() - post_gate_norm_started_at);
        defer normed_metal.deinit();
        const normed_gated = mlx_metal_bridge.borrowMetalTensorAsMlxArray(normed_metal);
        defer _ = c.mlx_array_free(normed_gated);

        const down_started_at = monotonicNowNs();
        const projected = (try apply_linear_fn(ctx, &.{
            .slot = request.down_ffn_linear_slot,
            .input = normed_gated,
            .in_dim = request.intermediate_size,
            .out_dim = request.hidden_size,
        })) orelse return null;
        stats.quantized_gated_down_nanos += @intCast(monotonicNowNs() - down_started_at);
        defer _ = c.mlx_array_free(projected);

        const add_started_at = monotonicNowNs();
        var projected_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const projected_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(projected, &projected_shape_buf);
        const result_tensor = (try decoderRuntimeApplyAdd(self, .{
            .lhs = projected_tensor,
            .rhs = attn_res,
            .dim = request.hidden_size,
        }, stats)) orelse return null;
        stats.quantized_gated_add_nanos += @intCast(monotonicNowNs() - add_started_at);
        return result_tensor;
    }
    return null;
}

fn runQuantizedGatedFfnResidualMetalTensor(
    self: anytype,
    request: anytype,
    input: MetalTensor,
    residual: MetalTensor,
    stats: anytype,
    logged_unsupported_type: *bool,
    logged_backend_mixed_kind: *bool,
    logged_backend_unsupported_kind: *bool,
) !?MetalTensor {
    if (input.ndim() != 2 or residual.ndim() != 2) return null;
    if (hasActiveFrame(self.raw_decode_runtime)) return null;
    if (!shouldAttemptDirectQuantizedGatedFfn(
        self,
        request.gate_ffn_linear_slot,
        request.up_ffn_linear_slot,
        request.down_ffn_linear_slot,
        stats,
        logged_backend_mixed_kind,
        logged_backend_unsupported_kind,
    )) return null;

    const rows: usize = @intCast(input.dim(0));
    if (@as(usize, @intCast(residual.dim(0))) != rows) return null;
    const output = try std.heap.c_allocator.alloc(f32, rows * request.hidden_size);
    errdefer std.heap.c_allocator.free(output);
    var input_mut = input;
    var residual_mut = residual;
    if (!(try tryRawQuantizedGatedFfnResidualHost(self, .{
        .input = try tensorHostConstPtr(&input_mut),
        .residual = try tensorHostConstPtr(&residual_mut),
        .rows = rows,
        .hidden_size = request.hidden_size,
        .intermediate_size = request.intermediate_size,
        .activation = request.activation,
        .gate_linear_slot = request.gate_ffn_linear_slot,
        .up_linear_slot = request.up_ffn_linear_slot,
        .down_linear_slot = request.down_ffn_linear_slot,
        .post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot,
        .post_down_rms_norm_slot = request.ffn_post_down_rms_norm_slot,
        .output = output.ptr,
    }, stats, logged_unsupported_type))) {
        return null;
    }
    stats.quantized_gated_ffn_direct_successes += 1;
    const shape = [_]i32{ @intCast(rows), @intCast(request.hidden_size) };
    return MetalTensor.owned(output, &shape);
}

const DirectQuantizedBlockFormat = enum {
    q8_0,
};

fn preparedQuantizedLinearSlotKind(self: anytype, slot: usize, in_dim: usize, out_dim: usize) RawQuantizedRuntimeLinearKind {
    if (slot >= decoder_runtime_linear_slot_capacity) return .none;
    if (!self.raw_linear_slots_prepared[slot]) return .none;
    if (self.raw_linear_slot_kinds[slot] != .quantized) return .none;
    return ensureQuantizedRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
}

fn directQuantizedBlockFormatForKind(kind: RawQuantizedRuntimeLinearKind) ?DirectQuantizedBlockFormat {
    return switch (kind) {
        .q8_0 => .q8_0,
        else => null,
    };
}

fn mergeDirectQuantizedBlockFormat(
    current: ?DirectQuantizedBlockFormat,
    next: RawQuantizedRuntimeLinearKind,
) ?DirectQuantizedBlockFormat {
    const next_format = directQuantizedBlockFormatForKind(next) orelse return null;
    if (current) |format| {
        if (format != next_format) return null;
        return format;
    }
    return next_format;
}

fn directQuantizedBlockFormatForRequest(self: anytype, request: anytype) ?DirectQuantizedBlockFormat {
    const attention_input_size = request.num_heads * request.head_dim;
    var format: ?DirectQuantizedBlockFormat = null;
    format = mergeDirectQuantizedBlockFormat(format, preparedQuantizedLinearSlotKind(self, request.attention_linear_slot, attention_input_size, request.hidden_size)) orelse return null;
    format = mergeDirectQuantizedBlockFormat(format, preparedQuantizedLinearSlotKind(self, request.gate_ffn_linear_slot, request.hidden_size, request.intermediate_size)) orelse return null;
    format = mergeDirectQuantizedBlockFormat(format, preparedQuantizedLinearSlotKind(self, request.up_ffn_linear_slot, request.hidden_size, request.intermediate_size)) orelse return null;
    format = mergeDirectQuantizedBlockFormat(format, preparedQuantizedLinearSlotKind(self, request.down_ffn_linear_slot, request.intermediate_size, request.hidden_size)) orelse return null;
    const ple_mt_opt: ?MetalTensor = if (@hasField(@TypeOf(request), "ple")) request.ple else null;
    if (ple_mt_opt != null) {
        const ple_gate_slot = request.ple_gate_linear_slot orelse return null;
        const ple_proj_slot = request.ple_proj_linear_slot orelse return null;
        const ple_post_norm_slot = request.ple_post_norm_slot orelse return null;
        if (request.ple_hidden_size == 0) return null;
        format = mergeDirectQuantizedBlockFormat(format, preparedQuantizedLinearSlotKind(self, ple_gate_slot, request.hidden_size, request.ple_hidden_size)) orelse return null;
        format = mergeDirectQuantizedBlockFormat(format, preparedQuantizedLinearSlotKind(self, ple_proj_slot, request.ple_hidden_size, request.hidden_size)) orelse return null;
        if (ple_post_norm_slot >= decoder_runtime_rms_norm_slot_capacity) return null;
        if (!self.raw_rms_norm_slots_prepared[ple_post_norm_slot]) return null;
        if (self.raw_rms_norm_slot_hidden_sizes[ple_post_norm_slot] != request.hidden_size) return null;
    }
    return format;
}

pub fn supportsDirectPagedGatedDecoderBlockDevice(self: anytype, request: anytype) bool {
    return directQuantizedBlockFormatForRequest(self, request) != null;
}

fn traceQuantBlockRequested() bool {
    return getenvBool("TERMITE_METAL_TRACE_QUANT_BLOCK") or
        getenvBool("TERMITE_METAL_TRACE_Q80_BLOCK");
}

fn runAttentionF32GatedDecoderBlockQuantizedDevice(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_full_mt: MetalTensor,
    v_full_mt: MetalTensor,
    residual_mt: MetalTensor,
    stats: anytype,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.query_sequence_len == 0) return null;
    const attention_input_size = request.num_heads * request.head_dim;
    const direct_block_format = directQuantizedBlockFormatForRequest(self, request) orelse return null;
    if (q_mt.ndim() != 2 or k_full_mt.ndim() != 2 or v_full_mt.ndim() != 2 or residual_mt.ndim() != 2) return null;
    const rows = request.query_sequence_len;
    if (@as(usize, @intCast(q_mt.dim(0))) != rows or
        @as(usize, @intCast(k_full_mt.dim(0))) != request.kv_tokens or
        @as(usize, @intCast(v_full_mt.dim(0))) != request.kv_tokens or
        @as(usize, @intCast(residual_mt.dim(0))) != rows)
    {
        return null;
    }
    if (@as(usize, @intCast(q_mt.dim(1))) != attention_input_size or
        @as(usize, @intCast(k_full_mt.dim(1))) != request.num_kv_heads * request.head_dim or
        @as(usize, @intCast(v_full_mt.dim(1))) != request.num_kv_heads * request.head_dim or
        @as(usize, @intCast(residual_mt.dim(1))) != request.hidden_size)
    {
        return null;
    }
    if (!q_mt.isDevice() or !k_full_mt.isDevice() or !v_full_mt.isDevice() or !residual_mt.isDevice()) return null;
    const ple_mt_opt: ?MetalTensor = request.ple;
    if (ple_mt_opt) |ple_mt| {
        const ple_post_norm_slot = request.ple_post_norm_slot orelse return null;
        if (request.ple_hidden_size == 0) return null;
        if (ple_mt.ndim() != 2 or @as(usize, @intCast(ple_mt.dim(0))) != rows or @as(usize, @intCast(ple_mt.dim(1))) != request.ple_hidden_size) return null;
        if (!ple_mt.isDevice()) return null;
        if (ple_post_norm_slot >= decoder_runtime_rms_norm_slot_capacity) return null;
        if (!self.raw_rms_norm_slots_prepared[ple_post_norm_slot]) return null;
        if (self.raw_rms_norm_slot_hidden_sizes[ple_post_norm_slot] != request.hidden_size) return null;
    }
    const output_scale_value_opt: ?f32 = if (@hasField(@TypeOf(request), "output_scale_value")) request.output_scale_value else null;

    const out_shape = [_]i32{ @intCast(rows), @intCast(request.hidden_size) };
    var device_output = MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        rows * request.hidden_size * @sizeOf(f32),
        .private,
        &out_shape,
    ) catch return null;
    errdefer device_output.deinit();

    const none = std.math.maxInt(usize);
    const planned_layer_contract: ops.PlannedLayerContract = if (@hasField(@TypeOf(request), "planned_layer_contract")) request.planned_layer_contract else .{};
    const raw_planned_layer_contract = RawPlannedLayerContract.fromContract(planned_layer_contract);
    var timing: RawAttentionGatedBlockTiming = .{};
    const started_at = monotonicNowNs();
    const rc = switch (direct_block_format) {
        .q8_0 => termite_metal_decode_runtime_apply_attention_f32_gated_block_q8_0_device_kv_device(
            runtime,
            q_mt.deviceHandle(),
            q_mt.deviceByteOffset(),
            k_full_mt.deviceHandle(),
            k_full_mt.deviceByteOffset(),
            v_full_mt.deviceHandle(),
            v_full_mt.deviceByteOffset(),
            rows,
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.query_position_offset,
            request.kv_position_offset,
            request.sliding_window,
            request.total_sequence_len,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot orelse none,
            request.attention_post_linear_rms_norm_slot orelse none,
            residual_mt.deviceHandle(),
            residual_mt.deviceByteOffset(),
            attention_input_size,
            request.hidden_size,
            request.eps,
            request.ffn_layer_norm_slot orelse none,
            request.ffn_rms_norm_slot orelse none,
            request.ffn_post_gate_rms_norm_slot orelse none,
            request.ffn_post_down_rms_norm_slot orelse none,
            request.gate_ffn_linear_slot,
            request.up_ffn_linear_slot,
            request.down_ffn_linear_slot,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.layer_index,
            if (ple_mt_opt) |ple_mt| ple_mt.deviceHandle() else null,
            if (ple_mt_opt) |ple_mt| ple_mt.deviceByteOffset() else 0,
            request.ple_gate_linear_slot orelse none,
            request.ple_proj_linear_slot orelse none,
            request.ple_post_norm_slot orelse none,
            if (ple_mt_opt != null) request.ple_hidden_size else 0,
            if (output_scale_value_opt != null) 1 else 0,
            output_scale_value_opt orelse 1.0,
            device_output.deviceHandle(),
            device_output.deviceByteOffset(),
            raw_planned_layer_contract,
            &timing,
            none,
            0,
            null,
            0,
            0,
            0,
            0,
            0,
            0,
        ),
    };
    stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - started_at);
    stats.compressed_block_attention_span_nanos += timing.attention_span_nanos;
    stats.compressed_block_attention_prefix_nanos += timing.attention_prefix_nanos;
    stats.compressed_block_gated_ffn_residual_nanos += timing.gated_ffn_residual_nanos;
    stats.compressed_block_command_wait_nanos += timing.command_wait_nanos;
    stats.compressed_block_gpu_nanos += timing.gpu_nanos;
    stats.active_decode_attention_f32_kernels += timing.attention_f32_kernels;
    stats.active_decode_q8_0_linear_kernels += timing.q8_0_linear_kernels;
    stats.active_decode_q8_0_attention_linear_kernels += timing.q8_0_attention_linear_kernels;
    stats.active_decode_q8_0_ffn_down_linear_kernels += timing.q8_0_ffn_down_linear_kernels;
    stats.active_decode_q8_0_ple_linear_kernels += timing.q8_0_ple_linear_kernels;
    stats.active_decode_q8_0_pair_activation_kernels += timing.q8_0_pair_activation_kernels;
    stats.active_decode_rms_norm_kernels += timing.rms_norm_kernels;
    stats.active_decode_rms_norm_add_kernels += timing.rms_norm_add_kernels;
    stats.active_decode_layer_norm_kernels += timing.layer_norm_kernels;
    stats.active_decode_add_kernels += timing.add_kernels;
    stats.active_decode_blit_copies += timing.blit_copies;
    if (rc != 0) {
        if (traceQuantBlockRequested()) {
            std.debug.print(
                "metal-f32-quant-block-fail layer={d} rows={d} rc={d} stage={d} stage_rc={d} graph_plan_count={d} graph_plan_slots={d}\n",
                .{
                    request.layer_index,
                    rows,
                    rc,
                    timing.failure_stage,
                    timing.failure_code,
                    runtimeMemorySnapshot(runtime).graph_plan_count,
                    runtimeMemorySnapshot(runtime).graph_plan_slots,
                },
            );
        }
        stats.f32_kv_quant_direct_block_failures += 1;
        return null;
    }
    stats.f32_kv_quant_direct_block_successes += 1;
    stats.quantized_gated_ffn_direct_successes += 1;
    return device_output;
}

pub fn runAttentionF32GatedDecoderBlockDevice(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_full_mt: MetalTensor,
    v_full_mt: MetalTensor,
    residual_mt: MetalTensor,
    stats: anytype,
) !?MetalTensor {
    return runAttentionF32GatedDecoderBlockQuantizedDevice(
        self,
        request,
        q_mt,
        k_full_mt,
        v_full_mt,
        residual_mt,
        stats,
    );
}

fn runAttentionPagedGatedDecoderBlockQuantizedDevice(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_suffix_mt: ?MetalTensor,
    v_suffix_mt: ?MetalTensor,
    residual_mt: MetalTensor,
    paged_layer: anytype,
    block_token_offsets: []const u32,
    stats: anytype,
) !?MetalTensor {
    const rows = request.query_sequence_len;
    const out_shape = [_]i32{ @intCast(rows), @intCast(request.hidden_size) };
    var device_output = MetalTensor.deviceAllocate(
        @ptrCast(self.raw_decode_runtime orelse return null),
        rows * request.hidden_size * @sizeOf(f32),
        .private,
        &out_shape,
    ) catch return null;
    errdefer device_output.deinit();
    if (!try runAttentionPagedGatedDecoderBlockQuantizedDeviceInto(
        self,
        request,
        q_mt,
        k_suffix_mt,
        v_suffix_mt,
        residual_mt,
        paged_layer,
        block_token_offsets,
        device_output,
        stats,
    )) return null;
    return device_output;
}

pub fn runAttentionPagedGatedDecoderBlockDevice(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_suffix_mt: ?MetalTensor,
    v_suffix_mt: ?MetalTensor,
    residual_mt: MetalTensor,
    paged_layer: anytype,
    block_token_offsets: []const u32,
    stats: anytype,
) !?MetalTensor {
    return runAttentionPagedGatedDecoderBlockQuantizedDevice(
        self,
        request,
        q_mt,
        k_suffix_mt,
        v_suffix_mt,
        residual_mt,
        paged_layer,
        block_token_offsets,
        stats,
    );
}

fn runAttentionPagedGatedDecoderBlockQuantizedDeviceInto(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_suffix_mt: ?MetalTensor,
    v_suffix_mt: ?MetalTensor,
    residual_mt: MetalTensor,
    paged_layer: anytype,
    block_token_offsets: []const u32,
    output_mt: MetalTensor,
    stats: anytype,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (request.query_sequence_len == 0 or request.kv_tokens == 0) return false;
    if (block_token_offsets.len == 0 or paged_layer.page_size_tokens == 0) return false;
    const attention_input_size = request.num_heads * request.head_dim;
    const direct_block_format = directQuantizedBlockFormatForRequest(self, request) orelse return false;
    if (q_mt.ndim() != 2 or residual_mt.ndim() != 2 or output_mt.ndim() != 2) return false;
    const rows = request.query_sequence_len;
    if (@as(usize, @intCast(q_mt.dim(0))) != rows or
        @as(usize, @intCast(residual_mt.dim(0))) != rows or
        @as(usize, @intCast(output_mt.dim(0))) != rows or
        @as(usize, @intCast(q_mt.dim(1))) != attention_input_size or
        @as(usize, @intCast(residual_mt.dim(1))) != request.hidden_size or
        @as(usize, @intCast(output_mt.dim(1))) != request.hidden_size)
    {
        return false;
    }
    if (!q_mt.isDevice() or !residual_mt.isDevice() or !output_mt.isDevice()) return false;
    const suffix_tokens: usize = if (k_suffix_mt != null or v_suffix_mt != null) rows else 0;
    if (suffix_tokens != 0) {
        const k = k_suffix_mt orelse return false;
        const v = v_suffix_mt orelse return false;
        const kv_dim = request.num_kv_heads * request.head_dim;
        if (k.ndim() != 2 or v.ndim() != 2 or
            @as(usize, @intCast(k.dim(0))) != suffix_tokens or
            @as(usize, @intCast(v.dim(0))) != suffix_tokens or
            @as(usize, @intCast(k.dim(1))) != kv_dim or
            @as(usize, @intCast(v.dim(1))) != kv_dim or
            !k.isDevice() or !v.isDevice())
        {
            return false;
        }
    }
    const ple_mt_opt: ?MetalTensor = request.ple;
    if (ple_mt_opt) |ple_mt| {
        const ple_post_norm_slot = request.ple_post_norm_slot orelse return false;
        if (request.ple_hidden_size == 0) return false;
        if (ple_mt.ndim() != 2 or @as(usize, @intCast(ple_mt.dim(0))) != rows or @as(usize, @intCast(ple_mt.dim(1))) != request.ple_hidden_size) return false;
        if (!ple_mt.isDevice()) return false;
        if (ple_post_norm_slot >= decoder_runtime_rms_norm_slot_capacity) return false;
        if (!self.raw_rms_norm_slots_prepared[ple_post_norm_slot]) return false;
        if (self.raw_rms_norm_slot_hidden_sizes[ple_post_norm_slot] != request.hidden_size) return false;
    }
    const output_scale_value_opt: ?f32 = if (@hasField(@TypeOf(request), "output_scale_value")) request.output_scale_value else null;

    const none = std.math.maxInt(usize);
    const planned_layer_contract: ops.PlannedLayerContract = if (@hasField(@TypeOf(request), "planned_layer_contract")) request.planned_layer_contract else .{};
    const raw_planned_layer_contract = RawPlannedLayerContract.fromContract(planned_layer_contract);
    var timing: RawAttentionGatedBlockTiming = .{};
    const started_at = monotonicNowNs();
    const k_handle = if (k_suffix_mt) |k| k.deviceHandle() else null;
    const k_offset = if (k_suffix_mt) |k| k.deviceByteOffset() else 0;
    const v_handle = if (v_suffix_mt) |v| v.deviceHandle() else null;
    const v_offset = if (v_suffix_mt) |v| v.deviceByteOffset() else 0;
    const rc = switch (direct_block_format) {
        .q8_0 => termite_metal_decode_runtime_apply_attention_f32_gated_block_q8_0_device_kv_device(
            runtime,
            q_mt.deviceHandle(),
            q_mt.deviceByteOffset(),
            k_handle,
            k_offset,
            v_handle,
            v_offset,
            rows,
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.query_position_offset,
            request.kv_position_offset,
            request.sliding_window,
            request.total_sequence_len,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot orelse none,
            request.attention_post_linear_rms_norm_slot orelse none,
            residual_mt.deviceHandle(),
            residual_mt.deviceByteOffset(),
            attention_input_size,
            request.hidden_size,
            request.eps,
            request.ffn_layer_norm_slot orelse none,
            request.ffn_rms_norm_slot orelse none,
            request.ffn_post_gate_rms_norm_slot orelse none,
            request.ffn_post_down_rms_norm_slot orelse none,
            request.gate_ffn_linear_slot,
            request.up_ffn_linear_slot,
            request.down_ffn_linear_slot,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.layer_index,
            if (ple_mt_opt) |ple_mt| ple_mt.deviceHandle() else null,
            if (ple_mt_opt) |ple_mt| ple_mt.deviceByteOffset() else 0,
            request.ple_gate_linear_slot orelse none,
            request.ple_proj_linear_slot orelse none,
            request.ple_post_norm_slot orelse none,
            if (ple_mt_opt != null) request.ple_hidden_size else 0,
            if (output_scale_value_opt != null) 1 else 0,
            output_scale_value_opt orelse 1.0,
            output_mt.deviceHandle(),
            output_mt.deviceByteOffset(),
            raw_planned_layer_contract,
            &timing,
            paged_layer.slot,
            paged_layer.format,
            block_token_offsets.ptr,
            block_token_offsets.len,
            @intCast(paged_layer.page_size_tokens),
            paged_layer.key_row_bytes,
            paged_layer.base_key_row_bytes,
            paged_layer.v_row_stride,
            suffix_tokens,
        ),
    };
    stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - started_at);
    stats.compressed_block_attention_span_nanos += timing.attention_span_nanos;
    stats.compressed_block_attention_prefix_nanos += timing.attention_prefix_nanos;
    stats.compressed_block_gated_ffn_residual_nanos += timing.gated_ffn_residual_nanos;
    stats.compressed_block_command_wait_nanos += timing.command_wait_nanos;
    stats.compressed_block_gpu_nanos += timing.gpu_nanos;
    stats.active_decode_attention_f32_kernels += timing.attention_f32_kernels;
    stats.active_decode_q8_0_linear_kernels += timing.q8_0_linear_kernels;
    stats.active_decode_q8_0_attention_linear_kernels += timing.q8_0_attention_linear_kernels;
    stats.active_decode_q8_0_ffn_down_linear_kernels += timing.q8_0_ffn_down_linear_kernels;
    stats.active_decode_q8_0_ple_linear_kernels += timing.q8_0_ple_linear_kernels;
    stats.active_decode_q8_0_pair_activation_kernels += timing.q8_0_pair_activation_kernels;
    stats.active_decode_rms_norm_kernels += timing.rms_norm_kernels;
    stats.active_decode_rms_norm_add_kernels += timing.rms_norm_add_kernels;
    stats.active_decode_layer_norm_kernels += timing.layer_norm_kernels;
    stats.active_decode_add_kernels += timing.add_kernels;
    stats.active_decode_blit_copies += timing.blit_copies;
    if (rc != 0) {
        if (traceQuantBlockRequested()) {
            std.debug.print(
                "metal-paged-quant-block-fail layer={d} rows={d} rc={d} stage={d} stage_rc={d}\n",
                .{ request.layer_index, rows, rc, timing.failure_stage, timing.failure_code },
            );
        }
        stats.f32_kv_quant_direct_block_failures += 1;
        return false;
    }
    stats.f32_kv_quant_direct_block_successes += 1;
    stats.quantized_gated_ffn_direct_successes += 1;
    return true;
}

pub fn runAttentionPagedGatedDecoderBlockDeviceInto(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_suffix_mt: ?MetalTensor,
    v_suffix_mt: ?MetalTensor,
    residual_mt: MetalTensor,
    paged_layer: anytype,
    block_token_offsets: []const u32,
    output_mt: MetalTensor,
    stats: anytype,
) !bool {
    return runAttentionPagedGatedDecoderBlockQuantizedDeviceInto(
        self,
        request,
        q_mt,
        k_suffix_mt,
        v_suffix_mt,
        residual_mt,
        paged_layer,
        block_token_offsets,
        output_mt,
        stats,
    );
}

fn runAttentionF32GatedDecoderBlockQuantizedDeviceInto(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_full_mt: MetalTensor,
    v_full_mt: MetalTensor,
    residual_mt: MetalTensor,
    output_mt: MetalTensor,
    stats: anytype,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (request.query_sequence_len == 0) return false;
    const attention_input_size = request.num_heads * request.head_dim;
    const direct_block_format = directQuantizedBlockFormatForRequest(self, request) orelse return false;
    if (q_mt.ndim() != 2 or k_full_mt.ndim() != 2 or v_full_mt.ndim() != 2 or residual_mt.ndim() != 2 or output_mt.ndim() != 2) return false;
    const rows = request.query_sequence_len;
    if (@as(usize, @intCast(q_mt.dim(0))) != rows or
        @as(usize, @intCast(k_full_mt.dim(0))) != request.kv_tokens or
        @as(usize, @intCast(v_full_mt.dim(0))) != request.kv_tokens or
        @as(usize, @intCast(residual_mt.dim(0))) != rows or
        @as(usize, @intCast(output_mt.dim(0))) != rows)
    {
        return false;
    }
    if (@as(usize, @intCast(q_mt.dim(1))) != attention_input_size or
        @as(usize, @intCast(k_full_mt.dim(1))) != request.num_kv_heads * request.head_dim or
        @as(usize, @intCast(v_full_mt.dim(1))) != request.num_kv_heads * request.head_dim or
        @as(usize, @intCast(residual_mt.dim(1))) != request.hidden_size or
        @as(usize, @intCast(output_mt.dim(1))) != request.hidden_size)
    {
        return false;
    }
    if (!q_mt.isDevice() or !k_full_mt.isDevice() or !v_full_mt.isDevice() or !residual_mt.isDevice() or !output_mt.isDevice()) return false;
    const ple_mt_opt: ?MetalTensor = request.ple;
    if (ple_mt_opt) |ple_mt| {
        const ple_post_norm_slot = request.ple_post_norm_slot orelse return false;
        if (request.ple_hidden_size == 0) return false;
        if (ple_mt.ndim() != 2 or @as(usize, @intCast(ple_mt.dim(0))) != rows or @as(usize, @intCast(ple_mt.dim(1))) != request.ple_hidden_size) return false;
        if (!ple_mt.isDevice()) return false;
        if (ple_post_norm_slot >= decoder_runtime_rms_norm_slot_capacity) return false;
        if (!self.raw_rms_norm_slots_prepared[ple_post_norm_slot]) return false;
        if (self.raw_rms_norm_slot_hidden_sizes[ple_post_norm_slot] != request.hidden_size) return false;
    }
    const output_scale_value_opt: ?f32 = if (@hasField(@TypeOf(request), "output_scale_value")) request.output_scale_value else null;

    const none = std.math.maxInt(usize);
    const planned_layer_contract: ops.PlannedLayerContract = if (@hasField(@TypeOf(request), "planned_layer_contract")) request.planned_layer_contract else .{};
    const raw_planned_layer_contract = RawPlannedLayerContract.fromContract(planned_layer_contract);
    var timing: RawAttentionGatedBlockTiming = .{};
    const started_at = monotonicNowNs();
    const rc = switch (direct_block_format) {
        .q8_0 => termite_metal_decode_runtime_apply_attention_f32_gated_block_q8_0_device_kv_device(
            runtime,
            q_mt.deviceHandle(),
            q_mt.deviceByteOffset(),
            k_full_mt.deviceHandle(),
            k_full_mt.deviceByteOffset(),
            v_full_mt.deviceHandle(),
            v_full_mt.deviceByteOffset(),
            rows,
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.query_position_offset,
            request.kv_position_offset,
            request.sliding_window,
            request.total_sequence_len,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot orelse none,
            request.attention_post_linear_rms_norm_slot orelse none,
            residual_mt.deviceHandle(),
            residual_mt.deviceByteOffset(),
            attention_input_size,
            request.hidden_size,
            request.eps,
            request.ffn_layer_norm_slot orelse none,
            request.ffn_rms_norm_slot orelse none,
            request.ffn_post_gate_rms_norm_slot orelse none,
            request.ffn_post_down_rms_norm_slot orelse none,
            request.gate_ffn_linear_slot,
            request.up_ffn_linear_slot,
            request.down_ffn_linear_slot,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.layer_index,
            if (ple_mt_opt) |ple_mt| ple_mt.deviceHandle() else null,
            if (ple_mt_opt) |ple_mt| ple_mt.deviceByteOffset() else 0,
            request.ple_gate_linear_slot orelse none,
            request.ple_proj_linear_slot orelse none,
            request.ple_post_norm_slot orelse none,
            if (ple_mt_opt != null) request.ple_hidden_size else 0,
            if (output_scale_value_opt != null) 1 else 0,
            output_scale_value_opt orelse 1.0,
            output_mt.deviceHandle(),
            output_mt.deviceByteOffset(),
            raw_planned_layer_contract,
            &timing,
            none,
            0,
            null,
            0,
            0,
            0,
            0,
            0,
            0,
        ),
    };
    stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - started_at);
    stats.compressed_block_attention_span_nanos += timing.attention_span_nanos;
    stats.compressed_block_attention_prefix_nanos += timing.attention_prefix_nanos;
    stats.compressed_block_gated_ffn_residual_nanos += timing.gated_ffn_residual_nanos;
    stats.compressed_block_command_wait_nanos += timing.command_wait_nanos;
    stats.compressed_block_gpu_nanos += timing.gpu_nanos;
    stats.active_decode_attention_f32_kernels += timing.attention_f32_kernels;
    stats.active_decode_q8_0_linear_kernels += timing.q8_0_linear_kernels;
    stats.active_decode_q8_0_attention_linear_kernels += timing.q8_0_attention_linear_kernels;
    stats.active_decode_q8_0_ffn_down_linear_kernels += timing.q8_0_ffn_down_linear_kernels;
    stats.active_decode_q8_0_ple_linear_kernels += timing.q8_0_ple_linear_kernels;
    stats.active_decode_q8_0_pair_activation_kernels += timing.q8_0_pair_activation_kernels;
    stats.active_decode_rms_norm_kernels += timing.rms_norm_kernels;
    stats.active_decode_rms_norm_add_kernels += timing.rms_norm_add_kernels;
    stats.active_decode_layer_norm_kernels += timing.layer_norm_kernels;
    stats.active_decode_add_kernels += timing.add_kernels;
    stats.active_decode_blit_copies += timing.blit_copies;
    if (rc != 0) {
        if (traceQuantBlockRequested()) {
            std.debug.print(
                "metal-f32-quant-block-into-fail layer={d} rows={d} rc={d} stage={d} stage_rc={d} graph_plan_count={d} graph_plan_slots={d}\n",
                .{
                    request.layer_index,
                    rows,
                    rc,
                    timing.failure_stage,
                    timing.failure_code,
                    runtimeMemorySnapshot(runtime).graph_plan_count,
                    runtimeMemorySnapshot(runtime).graph_plan_slots,
                },
            );
        }
        stats.f32_kv_quant_direct_block_failures += 1;
        return false;
    }
    stats.f32_kv_quant_direct_block_successes += 1;
    stats.quantized_gated_ffn_direct_successes += 1;
    return true;
}

pub fn runAttentionF32GatedDecoderBlockDeviceInto(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_full_mt: MetalTensor,
    v_full_mt: MetalTensor,
    residual_mt: MetalTensor,
    output_mt: MetalTensor,
    stats: anytype,
) !bool {
    return runAttentionF32GatedDecoderBlockQuantizedDeviceInto(
        self,
        request,
        q_mt,
        k_full_mt,
        v_full_mt,
        residual_mt,
        output_mt,
        stats,
    );
}

fn runCompressedAttentionGatedDecoderBlockQuantizedDevice(
    self: anytype,
    request: anytype,
    q_mt: MetalTensor,
    k_suffix_mt: MetalTensor,
    v_suffix_mt: MetalTensor,
    stats: anytype,
) !?MetalTensor {
    const trace = traceQuantBlockRequested();
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) {
        if (trace) std.debug.print("metal-quant-block-skip layer={d} reason=runtime-not-ready\n", .{request.layer_index});
        return null;
    }
    if (@hasField(@TypeOf(request), "skip_kv_write") and request.skip_kv_write) {
        if (trace) std.debug.print("metal-quant-block-skip layer={d} reason=shared-kv\n", .{request.layer_index});
        return null;
    }
    if (request.query_sequence_len != 1) {
        if (trace) std.debug.print("metal-quant-block-skip layer={d} reason=query-len q={d}\n", .{ request.layer_index, request.query_sequence_len });
        return null;
    }
    const attention_input_size = request.num_heads * request.head_dim;
    const direct_block_format = directQuantizedBlockFormatForRequest(self, request) orelse {
        if (trace) std.debug.print("metal-quant-block-skip layer={d} reason=slot attn={d} gate={d} up={d} down={d} attention_input={d} hidden={d} intermediate={d}\n", .{ request.layer_index, request.attention_linear_slot, request.gate_ffn_linear_slot, request.up_ffn_linear_slot, request.down_ffn_linear_slot, attention_input_size, request.hidden_size, request.intermediate_size });
        return null;
    };
    if (q_mt.ndim() != 2 or k_suffix_mt.ndim() != 2 or v_suffix_mt.ndim() != 2 or request.residual.ndim() != 2) {
        if (trace) std.debug.print("metal-quant-block-skip layer={d} reason=rank q={d} k={d} v={d} residual={d}\n", .{ request.layer_index, q_mt.ndim(), k_suffix_mt.ndim(), v_suffix_mt.ndim(), request.residual.ndim() });
        return null;
    }
    if (!q_mt.isDevice() or !k_suffix_mt.isDevice() or !v_suffix_mt.isDevice() or !request.residual.isDevice()) {
        if (trace) std.debug.print("metal-quant-block-skip layer={d} reason=device q={} k={} v={} residual={}\n", .{ request.layer_index, q_mt.isDevice(), k_suffix_mt.isDevice(), v_suffix_mt.isDevice(), request.residual.isDevice() });
        return null;
    }

    const base_key_row_bytes = switch (request.format) {
        .polar4 => (@as(usize, request.num_kv_heads) * request.head_dim + 1) / 2,
        .turbo3 => (@as(usize, request.num_kv_heads) * request.head_dim * 3 + 7) / 8,
    };
    try maybeWriteCompressedBlockKvSuffix(request, k_suffix_mt, v_suffix_mt);

    var gathered_full_owned: ?OwnedFullKv = null;
    defer if (gathered_full_owned) |*full| {
        full.k.deinit();
        full.v.deinit();
    };
    const full_k_for_span: ?MetalTensor = if (request.full_k) |full| full else blk: {
        gathered_full_owned = try maybeGatherCompressedBlockFullKv(request);
        break :blk if (gathered_full_owned) |full| full.k else null;
    };
    const full_v_for_span: ?MetalTensor = if (request.full_v) |full| full else if (gathered_full_owned) |full| full.v else null;

    const span_prep_started_at = monotonicNowNs();
    const gathered = (try updateGatheredSpan(
        self,
        .{
            .source_ptr_id = request.source_ptr_id,
            .sequence_id = request.sequence_id,
            .layer_index = request.layer_index,
        },
        k_suffix_mt,
        v_suffix_mt,
        request.bootstrap_k_blocks,
        request.bootstrap_v_blocks,
        request.bootstrap_block_token_counts,
        full_k_for_span,
        full_v_for_span,
        request.query_sequence_len,
        request.kv_tokens,
        request.kv_position_offset,
        stats,
    )) orelse return null;
    stats.compressed_block_span_prep_nanos += @intCast(monotonicNowNs() - span_prep_started_at);
    if (!gathered.k.isDevice() or !gathered.v.isDevice()) {
        if (trace) std.debug.print("metal-quant-block-skip layer={d} reason=gathered-device k={} v={}\n", .{ request.layer_index, gathered.k.isDevice(), gathered.v.isDevice() });
        return null;
    }

    const out_shape = [_]i32{ 1, @intCast(request.hidden_size) };
    var device_output = MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        request.hidden_size * @sizeOf(f32),
        .private,
        &out_shape,
    ) catch return null;
    errdefer device_output.deinit();

    const none = std.math.maxInt(usize);
    const v_row_stride: usize = @intCast(gathered.v.dim(1));
    var timing: RawAttentionGatedBlockTiming = .{};
    const apply_started_at = monotonicNowNs();
    if (trace) {
        std.debug.print(
            "metal-quant-block-apply format={s} kv_tokens={d} q_dim={d} k_rows={d} k_dim={d} v_rows={d} v_dim={d} key_row_bytes={d} base_key_row_bytes={d} v_stride={d} layer={d}\n",
            .{
                @tagName(direct_block_format),
                request.kv_tokens,
                q_mt.dim(1),
                gathered.k.dim(0),
                gathered.k.dim(1),
                gathered.v.dim(0),
                gathered.v.dim(1),
                request.key_row_bytes,
                base_key_row_bytes,
                v_row_stride,
                request.layer_index,
            },
        );
    }
    const rc = switch (direct_block_format) {
        .q8_0 => termite_metal_decode_runtime_apply_attention_gated_block_q8_0_device_kv_device(
            runtime,
            switch (request.format) {
                .polar4 => 0,
                .turbo3 => 1,
            },
            q_mt.deviceHandle(),
            q_mt.deviceByteOffset(),
            gathered.k.deviceHandle(),
            gathered.k.deviceByteOffset(),
            gathered.v.deviceHandle(),
            gathered.v.deviceByteOffset(),
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.key_row_bytes,
            v_row_stride,
            base_key_row_bytes,
            request.query_position,
            request.kv_position_offset,
            request.sliding_window,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot orelse none,
            request.attention_post_linear_rms_norm_slot orelse none,
            request.residual.deviceHandle(),
            request.residual.deviceByteOffset(),
            attention_input_size,
            request.hidden_size,
            request.eps,
            request.ffn_layer_norm_slot orelse none,
            request.ffn_rms_norm_slot orelse none,
            request.ffn_post_gate_rms_norm_slot orelse none,
            request.ffn_post_down_rms_norm_slot orelse none,
            request.gate_ffn_linear_slot,
            request.up_ffn_linear_slot,
            request.down_ffn_linear_slot,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.layer_index,
            device_output.deviceHandle(),
            device_output.deviceByteOffset(),
            &timing,
        ),
    };
    stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - apply_started_at);
    stats.compressed_block_replace_span_nanos += timing.replace_span_nanos;
    stats.compressed_block_attention_span_nanos += timing.attention_span_nanos;
    stats.compressed_block_attention_prefix_nanos += timing.attention_prefix_nanos;
    stats.compressed_block_gated_ffn_residual_nanos += timing.gated_ffn_residual_nanos;
    stats.compressed_block_command_wait_nanos += timing.command_wait_nanos;
    stats.compressed_block_gpu_nanos += timing.gpu_nanos;
    if (trace) std.debug.print("metal-quant-block-rc format={s} rc={d} stage={d} code={d}\n", .{ @tagName(direct_block_format), rc, timing.failure_stage, timing.failure_code });
    if (rc != 0) {
        stats.compressed_block_gated_direct_runtime_failures += 1;
        switch (timing.failure_stage) {
            1 => stats.compressed_block_gated_direct_fail_replace_span += 1,
            2 => stats.compressed_block_gated_direct_fail_attention_span += 1,
            3 => stats.compressed_block_gated_direct_fail_attention_prefix += 1,
            4 => stats.compressed_block_gated_direct_fail_gated_ffn += 1,
            else => {},
        }
        if (stats.compressed_block_gated_direct_first_failure_code == 0) {
            stats.compressed_block_gated_direct_first_failure_code = timing.failure_code;
        }
        return null;
    }
    stats.compressed_block_gated_direct_successes += 1;
    stats.quantized_gated_ffn_direct_successes += 1;
    return device_output;
}

pub fn runCompressedAttentionGatedDecoderBlockQuantized(
    self: anytype,
    ctx: *anyopaque,
    request: anytype,
    q_mt: MetalTensor,
    k_suffix_mt: MetalTensor,
    v_suffix_mt: MetalTensor,
    ffn_path: CompressedAttentionGatedFfnPath,
    stats: anytype,
    logged_unsupported_type: *bool,
    logged_backend_mixed_kind: *bool,
    logged_backend_unsupported_kind: *bool,
    run_gated_ffn_fn: anytype,
    apply_pair_fn: anytype,
    apply_linear_fn: anytype,
) !?MetalTensor {
    if (try runCompressedAttentionGatedDecoderBlockQuantizedDevice(
        self,
        request,
        q_mt,
        k_suffix_mt,
        v_suffix_mt,
        stats,
    )) |device_block| return device_block;
    if (hasActiveFrame(self.raw_decode_runtime)) return null;

    const attn_started_at = monotonicNowNs();
    var attn_res_mt = (try runCompressedAttentionResidual(self, .{
        .q = q_mt,
        .k_suffix = k_suffix_mt,
        .v_suffix = v_suffix_mt,
        .bootstrap_k_blocks = request.bootstrap_k_blocks,
        .bootstrap_v_blocks = request.bootstrap_v_blocks,
        .bootstrap_block_token_counts = request.bootstrap_block_token_counts,
        .full_k = request.full_k,
        .full_v = request.full_v,
        .source_ptr_id = request.source_ptr_id,
        .sequence_id = request.sequence_id,
        .layer_index = request.layer_index,
        .query_sequence_len = request.query_sequence_len,
        .kv_tokens = request.kv_tokens,
        .num_heads = request.num_heads,
        .num_kv_heads = request.num_kv_heads,
        .head_dim = request.head_dim,
        .key_row_bytes = request.key_row_bytes,
        .query_position = request.query_position,
        .kv_position_offset = request.kv_position_offset,
        .sliding_window = request.sliding_window,
        .format = request.format,
        .attention_linear_slot = request.attention_linear_slot,
        .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
        .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
        .residual = request.residual,
        .hidden_size = request.hidden_size,
        .attention_input_size = request.num_heads * request.head_dim,
        .eps = request.eps,
    }, stats)) orelse {
        stats.compressed_block_gated_quantized_attention_nulls += 1;
        if (request.query_sequence_len > 1)
            stats.compressed_block_gated_quantized_attention_prefill_nulls += 1
        else
            stats.compressed_block_gated_quantized_attention_decode_nulls += 1;
        return null;
    };
    stats.compressed_block_quantized_attention_calls += 1;
    stats.compressed_block_quantized_attention_nanos += @intCast(monotonicNowNs() - attn_started_at);
    debugRuntimeTensorFinite("block-attention-residual", request.layer_index, attn_res_mt);
    defer attn_res_mt.deinit();

    var ffn_normed_mt = (try decoderRuntimeApplyFfnNormInternal(
        self,
        attn_res_mt,
        request.ffn_layer_norm_slot,
        request.ffn_rms_norm_slot,
        request.hidden_size,
        request.eps,
    )) orelse {
        stats.compressed_block_gated_quantized_norm_nulls += 1;
        return null;
    };
    debugRuntimeTensorFinite("block-ffn-norm", request.layer_index, ffn_normed_mt);
    defer if (ffn_normed_mt.owned_by_c_allocator) ffn_normed_mt.deinit();

    const apply_started_at = monotonicNowNs();
    const hidden: ?MetalTensor = switch (ffn_path) {
        .quantized_post_gate => try runCompressedAttentionGatedPostGateQuantizedFfn(
            self,
            ctx,
            request,
            attn_res_mt,
            ffn_normed_mt,
            stats,
            logged_unsupported_type,
            logged_backend_mixed_kind,
            logged_backend_unsupported_kind,
            apply_pair_fn,
            apply_linear_fn,
        ),
        .quantized_runtime => blk: {
            if (comptime !build_options.enable_mlx) {
                break :blk try runQuantizedGatedFfnResidualMetalTensor(
                    self,
                    request,
                    ffn_normed_mt,
                    attn_res_mt,
                    stats,
                    logged_unsupported_type,
                    logged_backend_mixed_kind,
                    logged_backend_unsupported_kind,
                );
            }
            if (comptime !backendUsesMlxInputResidual(run_gated_ffn_fn)) {
                break :blk null;
            }
            const attn_res_mlx = mlx_metal_bridge.borrowMetalTensorAsMlxArray(attn_res_mt);
            defer _ = c.mlx_array_free(attn_res_mlx);
            const ffn_normed_mlx = mlx_metal_bridge.borrowMetalTensorAsMlxArray(ffn_normed_mt);
            defer _ = c.mlx_array_free(ffn_normed_mlx);
            const arr = (try run_gated_ffn_fn(ctx, &.{
                .input = coerceBackendInput(run_gated_ffn_fn, ffn_normed_mlx),
                .residual = coerceBackendField(run_gated_ffn_fn, "residual", attn_res_mlx),
                .gate_linear_slot = request.gate_ffn_linear_slot,
                .up_linear_slot = request.up_ffn_linear_slot,
                .down_linear_slot = request.down_ffn_linear_slot,
                .post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot,
                .post_down_rms_norm_slot = request.ffn_post_down_rms_norm_slot,
                .hidden_size = request.hidden_size,
                .intermediate_size = request.intermediate_size,
                .activation = request.activation,
            })) orelse break :blk null;
            const arr_mlx = coerceMlxArrayHandle(arr);
            defer _ = c.mlx_array_free(arr_mlx);
            var arr_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            const arr_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(arr_mlx, arr_shape_buf[0..]);
            var arr_mt_mut = arr_mt;
            break :blk try MetalTensor.ownedCloneFrom(try tensorHostSlice(&arr_mt_mut), arr_mt.shape());
        },
        .direct_dense => return null,
    };
    const apply_elapsed = monotonicNowNs() - apply_started_at;
    stats.compressed_block_apply_nanos += @intCast(apply_elapsed);
    stats.compressed_block_quantized_ffn_nanos += @intCast(apply_elapsed);
    if (hidden) |output_tensor| debugRuntimeTensorFinite("block-ffn-output", request.layer_index, output_tensor);
    return hidden;
}

pub fn runCompressedAttentionGatedDecoderBlockBackend(
    self: anytype,
    ctx: *anyopaque,
    request: anytype,
    stats: anytype,
    logged_unsupported_type: *bool,
    logged_backend_mixed_kind: *bool,
    logged_backend_unsupported_kind: *bool,
    run_gated_ffn_fn: anytype,
    apply_linear_fn: anytype,
    apply_pair_fn: anytype,
) !?MetalTensor {
    var prepared = (try prepareCompressedAttentionGatedDecoderBlockPlan(
        self,
        ctx,
        request,
        stats,
        apply_linear_fn,
        apply_pair_fn,
    )) orelse return null;
    defer prepared.deinit();

    return executeCompressedAttentionGatedDecoderBlockPlan(
        self,
        ctx,
        request,
        &prepared,
        stats,
        logged_unsupported_type,
        logged_backend_mixed_kind,
        logged_backend_unsupported_kind,
        run_gated_ffn_fn,
        apply_pair_fn,
        apply_linear_fn,
    );
}

pub fn prepareCompressedAttentionGatedDecoderBlockPlan(
    self: anytype,
    ctx: *anyopaque,
    request: anytype,
    stats: anytype,
    apply_linear_fn: anytype,
    apply_pair_fn: anytype,
) !?PreparedCompressedAttentionGatedDecoderBlockPlan {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.attention_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.gate_ffn_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.up_ffn_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.down_ffn_linear_slot >= decoder_runtime_linear_slot_capacity) return null;

    stats.compressed_block_gated_calls += 1;
    const can_use_direct_dense = self.raw_linear_slot_kinds[request.attention_linear_slot] == .dense and
        self.raw_linear_slot_kinds[request.gate_ffn_linear_slot] == .dense and
        self.raw_linear_slot_kinds[request.up_ffn_linear_slot] == .dense and
        self.raw_linear_slot_kinds[request.down_ffn_linear_slot] == .dense;
    const attention_path: CompressedAttentionGatedAttentionPath = if (!can_use_direct_dense) blk: {
        stats.compressed_block_gated_quantized_branch_calls += 1;
        break :blk .quantized_residual;
    } else .direct_dense;
    const selected_qkv = if (attention_path == .direct_dense and
        request.q == null and request.k_suffix != null and request.v_suffix != null and
        request.attention_input != null and request.q_linear_slot != null and
        request.q_linear_slot.? < decoder_runtime_linear_slot_capacity and
        self.raw_linear_slot_kinds[request.q_linear_slot.?] == .dense)
    blk: {
        var q = try retainedPlanTensor(request.attention_input.?);
        errdefer q.deinit();
        var k_suffix = try retainedPlanTensor(request.k_suffix.?);
        errdefer k_suffix.deinit();
        const v_suffix = try retainedPlanTensor(request.v_suffix.?);
        break :blk SelectedCompressedAttentionGatedQkv{
            .source = .projected_q_only_slot,
            .q = q,
            .k_suffix = k_suffix,
            .v_suffix = v_suffix,
        };
    } else (try selectCompressedAttentionGatedQkv(
        self,
        ctx,
        request,
        stats,
        apply_linear_fn,
        apply_pair_fn,
    )) orelse return null;
    const ffn_path: CompressedAttentionGatedFfnPath = switch (attention_path) {
        .direct_dense => .direct_dense,
        .quantized_residual => if (request.ffn_post_gate_rms_norm_slot != null and request.ffn_post_down_rms_norm_slot == null)
            .quantized_post_gate
        else
            .quantized_runtime,
    };

    return .{
        .qkv = selected_qkv,
        .attention_path = attention_path,
        .ffn_path = ffn_path,
    };
}

pub fn executeCompressedAttentionGatedDecoderBlockPlan(
    self: anytype,
    ctx: *anyopaque,
    request: anytype,
    prepared: *const PreparedCompressedAttentionGatedDecoderBlockPlan,
    stats: anytype,
    logged_unsupported_type: *bool,
    logged_backend_mixed_kind: *bool,
    logged_backend_unsupported_kind: *bool,
    run_gated_ffn_fn: anytype,
    apply_pair_fn: anytype,
    apply_linear_fn: anytype,
) !?MetalTensor {
    return switch (prepared.attention_path) {
        .quantized_residual => runCompressedAttentionGatedDecoderBlockQuantized(
            self,
            ctx,
            request,
            prepared.qkv.q,
            prepared.qkv.k_suffix,
            prepared.qkv.v_suffix,
            prepared.ffn_path,
            stats,
            logged_unsupported_type,
            logged_backend_mixed_kind,
            logged_backend_unsupported_kind,
            run_gated_ffn_fn,
            apply_pair_fn,
            apply_linear_fn,
        ),
        .direct_dense => runCompressedAttentionGatedDecoderBlockDirect(
            self,
            request,
            &prepared.qkv,
            stats,
        ),
    };
}

pub fn sliceRows(self: anytype, arr: MetalTensor, start_row: usize, row_count: usize) !MetalTensor {
    _ = self;
    if (arr.ndim() != 2) return error.InvalidPagedKvShape;
    const total_rows: usize = @intCast(arr.dim(0));
    const row_width: usize = @intCast(arr.dim(1));
    if (start_row + row_count > total_rows) return error.InvalidPagedKvSlice;

    const shape = [_]i32{ @intCast(row_count), @intCast(row_width) };
    if (arr.isDevice()) {
        const byte_offset = start_row * row_width * @sizeOf(f32);
        const byte_len = row_count * row_width * @sizeOf(f32);
        return arr.retainedView(byte_offset, byte_len, &shape);
    }

    const buf = try std.heap.c_allocator.alloc(f32, row_count * row_width);
    errdefer std.heap.c_allocator.free(buf);
    var arr_mut = arr;
    const src = try tensorHostSlice(&arr_mut);
    @memcpy(buf, src[start_row * row_width ..][0 .. row_count * row_width]);
    return MetalTensor.owned(buf, &shape);
}

pub fn concatenateRows(self: anytype, lhs: MetalTensor, rhs: MetalTensor) !MetalTensor {
    _ = self;
    if (lhs.ndim() != 2 or rhs.ndim() != 2) return error.InvalidPagedKvShape;
    const lhs_rows: usize = @intCast(lhs.dim(0));
    const rhs_rows: usize = @intCast(rhs.dim(0));
    const row_width: usize = @intCast(lhs.dim(1));
    if (@as(usize, @intCast(rhs.dim(1))) != row_width) return error.InvalidPagedKvShape;

    const total_rows = lhs_rows + rhs_rows;
    const shape = [_]i32{ @intCast(total_rows), @intCast(row_width) };
    if (lhs.isDevice() and rhs.isDevice()) {
        const lhs_dev = lhs.device.?;
        const rhs_dev = rhs.device.?;
        if (lhs_dev.ref.runtime == rhs_dev.ref.runtime) {
            var out = try MetalTensor.deviceAllocate(lhs_dev.ref.runtime, total_rows * row_width * @sizeOf(f32), .private, &shape);
            errdefer out.deinit();
            var lhs_dst = try out.retainedView(0, lhs_rows * row_width * @sizeOf(f32), &[_]i32{ @intCast(lhs_rows), @intCast(row_width) });
            defer lhs_dst.deinit();
            try lhs.copyInto(&lhs_dst);
            var rhs_dst = try out.retainedView(lhs_rows * row_width * @sizeOf(f32), rhs_rows * row_width * @sizeOf(f32), &[_]i32{ @intCast(rhs_rows), @intCast(row_width) });
            defer rhs_dst.deinit();
            try rhs.copyInto(&rhs_dst);
            return out;
        }
    }

    const buf = try std.heap.c_allocator.alloc(f32, total_rows * row_width);
    errdefer std.heap.c_allocator.free(buf);
    var lhs_host = lhs;
    var rhs_host = rhs;
    @memcpy(buf[0 .. lhs_rows * row_width], try tensorHostSlice(&lhs_host));
    @memcpy(buf[lhs_rows * row_width ..][0 .. rhs_rows * row_width], try tensorHostSlice(&rhs_host));
    return MetalTensor.owned(buf, &shape);
}

pub fn sliceLastDim2DDevice(self: anytype, tensor: MetalTensor, start: usize, stop: usize) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (!tensor.isDevice() or tensor.ndim() != 2) return null;
    const rows: usize = @intCast(tensor.dim(0));
    const cols: usize = @intCast(tensor.dim(1));
    if (rows == 0 or start > stop or stop > cols) return null;
    const out_cols = stop - start;
    if (out_cols == 0) return null;
    const shape = [_]i32{ @intCast(rows), @intCast(out_cols) };
    var out = try MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        rows * out_cols * @sizeOf(f32),
        .private,
        &shape,
    );
    errdefer out.deinit();
    const rc = termite_metal_buffer_slice_last_dim_2d(
        runtime,
        tensor.deviceHandle(),
        tensor.deviceByteOffset(),
        rows,
        cols,
        start,
        out_cols,
        out.deviceHandle(),
        out.deviceByteOffset(),
    );
    if (rc != 0) return null;
    return out;
}

fn cloneOrRetainTensor(tensor: MetalTensor) !MetalTensor {
    return tensor.retainedCopy();
}

fn cloneTensorToRuntimeDevice(self: anytype, tensor: MetalTensor) !?MetalTensor {
    if (tensor.isDevice()) return try tensor.retainedCopy();
    const runtime = self.raw_decode_runtime orelse return null;
    var out = try MetalTensor.deviceAllocate(runtime, tensor.deviceByteLen(), .private, tensor.shape());
    errdefer out.deinit();
    try tensor.copyInto(&out);
    return out;
}

pub fn cloneRowsFromBootstrapBlocks(self: anytype, blocks: []const MetalTensor, token_counts: []const usize) !?MetalTensor {
    if (blocks.len == 0) return null;
    if (blocks.len != token_counts.len) return error.InvalidPagedKvState;

    var row_width: usize = 0;
    var total_rows: usize = 0;
    for (blocks, token_counts) |block, token_count| {
        if (block.ndim() != 2) return error.InvalidPagedKvShape;
        const block_rows: usize = @intCast(block.dim(0));
        const block_width: usize = @intCast(block.dim(1));
        if (token_count > block_rows) return error.InvalidPagedKvState;
        if (token_count == 0) return error.InvalidPagedKvState;
        if (row_width == 0) {
            row_width = block_width;
        } else if (row_width != block_width) {
            return error.InvalidPagedKvShape;
        }
        total_rows += token_count;
    }
    if (row_width == 0) return null;

    var assembled: ?MetalTensor = null;
    errdefer if (assembled) |*tensor| tensor.deinit();
    for (blocks, token_counts) |block, token_count| {
        var chunk = if (token_count == @as(usize, @intCast(block.dim(0))))
            try cloneOrRetainTensor(block)
        else
            try sliceRows(self, block, 0, token_count);
        errdefer chunk.deinit();

        if (assembled) |current_value| {
            var current = current_value;
            const next = try concatenateRows(self, current, chunk);
            current.deinit();
            chunk.deinit();
            assembled = next;
        } else {
            assembled = chunk;
        }
    }
    return assembled;
}

pub fn replaceGatheredSpanEntry(
    self: anytype,
    entry: *GatheredSpanEntry,
    new_k: MetalTensor,
    new_v: MetalTensor,
    token_count: usize,
    position_offset: usize,
) !void {
    _ = self;
    entry.k.deinit();
    entry.v.deinit();
    if (entry.encoded_key) |encoded| std.heap.c_allocator.free(encoded);
    entry.* = .{
        .k = new_k,
        .v = new_v,
        .token_count = token_count,
        .position_offset = position_offset,
        .capacity_tokens = token_count,
        .encoded_key = null,
        .encoded_format = null,
        .encoded_key_row_bytes = 0,
    };
}

fn ensureGatheredSpanEntryDevice(
    self: anytype,
    entry: *GatheredSpanEntry,
    k_suffix: MetalTensor,
    v_suffix: MetalTensor,
) !void {
    if (!k_suffix.isDevice() or !v_suffix.isDevice()) return;
    if (entry.k.isDevice() and entry.v.isDevice()) return;

    var new_k = (try cloneTensorToRuntimeDevice(self, entry.k)) orelse return;
    errdefer new_k.deinit();
    var new_v = (try cloneTensorToRuntimeDevice(self, entry.v)) orelse return;
    errdefer new_v.deinit();
    try replaceGatheredSpanEntry(self, entry, new_k, new_v, entry.token_count, entry.position_offset);
}

pub fn resetGatheredSpans(self: anytype) void {
    var it = self.gathered_spans.iterator();
    while (it.next()) |entry| entry.value_ptr.deinit();
    self.gathered_spans.deinit(std.heap.c_allocator);
    self.gathered_spans = .empty;
}

pub fn hasGatheredSpanCache(self: anytype, source_ptr_id: usize, sequence_id: runtime_root.kv.manager.SequenceId, layer_index: usize) bool {
    return self.gathered_spans.contains(.{
        .source_ptr_id = source_ptr_id,
        .sequence_id = sequence_id,
        .layer_index = layer_index,
    });
}

pub fn hasGatheredSpanExact(
    self: anytype,
    source_ptr_id: usize,
    sequence_id: runtime_root.kv.manager.SequenceId,
    layer_index: usize,
    token_count: usize,
    position_offset: usize,
) bool {
    const entry = self.gathered_spans.getPtr(.{
        .source_ptr_id = source_ptr_id,
        .sequence_id = sequence_id,
        .layer_index = layer_index,
    }) orelse return false;
    return entry.token_count == token_count and entry.position_offset == position_offset;
}

pub fn cloneGatheredSpanRows(
    self: anytype,
    allocator: std.mem.Allocator,
    key: GatheredSpanKey,
    token_count: usize,
    position_offset: usize,
) !?struct { k: []f32, v: []f32 } {
    const entry = self.gathered_spans.getPtr(key) orelse return null;
    if (entry.token_count != token_count or entry.position_offset != position_offset) return null;
    var k_tensor = entry.k;
    const k = try allocator.dupe(f32, try tensorHostSlice(&k_tensor));
    errdefer allocator.free(k);
    var v_tensor = entry.v;
    const v = try allocator.dupe(f32, try tensorHostSlice(&v_tensor));
    return .{ .k = k, .v = v };
}

pub fn retainGatheredSpanDevice(
    self: anytype,
    key: GatheredSpanKey,
    token_count: usize,
    position_offset: usize,
) !?struct { k: MetalTensor, v: MetalTensor } {
    const entry = self.gathered_spans.getPtr(key) orelse return null;
    if (entry.position_offset != position_offset or entry.token_count < token_count) return null;
    if (!entry.k.isDevice() or !entry.v.isDevice()) return null;
    if (entry.k.ndim() != 2 or entry.v.ndim() != 2) return error.InvalidPagedKvShape;
    if (entry.token_count == token_count) {
        var k_copy = try entry.k.retainedCopy();
        errdefer k_copy.deinit();
        const v_copy = try entry.v.retainedCopy();
        return .{
            .k = k_copy,
            .v = v_copy,
        };
    }
    const row_width: usize = @intCast(entry.k.dim(1));
    if (@as(usize, @intCast(entry.v.dim(1))) != row_width) return error.InvalidPagedKvShape;
    const shape = [_]i32{ @intCast(token_count), @intCast(row_width) };
    const byte_len = token_count * row_width * @sizeOf(f32);
    var k_view = try entry.k.retainedView(0, byte_len, &shape);
    errdefer k_view.deinit();
    const v_view = try entry.v.retainedView(0, byte_len, &shape);
    return .{
        .k = k_view,
        .v = v_view,
    };
}

fn compactGatheredKvRows(
    allocator: std.mem.Allocator,
    rows: []const f32,
    token_count: usize,
    dst_width: usize,
) ![]f32 {
    if (token_count == 0) return allocator.dupe(f32, rows);
    if (rows.len % token_count != 0) return error.InvalidPagedKvShape;
    const src_width = rows.len / token_count;
    if (src_width < dst_width) return error.InvalidPagedKvShape;
    if (src_width == dst_width) return allocator.dupe(f32, rows);
    const out = try allocator.alloc(f32, token_count * dst_width);
    errdefer allocator.free(out);
    for (0..token_count) |tok| {
        @memcpy(
            out[tok * dst_width ..][0..dst_width],
            rows[tok * src_width ..][0..dst_width],
        );
    }
    return out;
}

fn maybeWriteCompressedBlockKvSuffix(request: anytype, k_suffix: MetalTensor, v_suffix: MetalTensor) !void {
    if (!@hasField(@TypeOf(request), "skip_kv_write")) return;
    if (request.skip_kv_write or request.query_sequence_len == 0) return;

    if (@hasField(@TypeOf(request), "kv_storage")) {
        if (request.kv_storage) |storage| {
            if (!storage.storage.config.store_cpu_bytes) return;
        }
    }
    if (@hasField(@TypeOf(request), "kv_manager") and @hasField(@TypeOf(request), "kv_pool_id")) {
        if (request.kv_manager) |manager| {
            const pool = manager.getPool(request.kv_pool_id) orelse return;
            if (!pool.config.store_cpu_bytes) return;
        }
    }

    var k_suffix_mut = k_suffix;
    var v_suffix_mut = v_suffix;
    const k_rows = try tensorHostSlice(&k_suffix_mut);
    const v_rows = try tensorHostSlice(&v_suffix_mut);

    if (@hasField(@TypeOf(request), "kv_storage")) {
        if (request.kv_storage) |storage| {
            try storage.writeLayerKvSuffix(
                request.sequence_id,
                request.layer_index,
                request.kv_tokens,
                request.query_sequence_len,
                k_rows,
                v_rows,
            );
            return;
        }
    }
    if (@hasField(@TypeOf(request), "kv_manager")) {
        if (request.kv_manager) |manager| {
            try manager.writeLayerKvSuffix(
                request.sequence_id,
                request.layer_index,
                request.kv_tokens,
                request.query_sequence_len,
                k_rows,
                v_rows,
            );
        }
    }
}

fn maybeGatherCompressedBlockFullKv(request: anytype) !?OwnedFullKv {
    if (request.query_sequence_len >= request.kv_tokens) return null;
    const dst_width = request.num_kv_heads * request.head_dim;

    if (@hasField(@TypeOf(request), "kv_storage")) {
        if (request.kv_storage) |storage| {
            if (!storage.storage.config.store_cpu_bytes) return null;
            const rows = try storage.gatherLayerKv(std.heap.c_allocator, request.sequence_id, request.layer_index, request.kv_tokens);
            defer {
                std.heap.c_allocator.free(rows.k);
                std.heap.c_allocator.free(rows.v);
            }
            const compact_k = try compactGatheredKvRows(std.heap.c_allocator, rows.k, request.kv_tokens, dst_width);
            errdefer std.heap.c_allocator.free(compact_k);
            const compact_v = try compactGatheredKvRows(std.heap.c_allocator, rows.v, request.kv_tokens, dst_width);
            errdefer std.heap.c_allocator.free(compact_v);
            const shape = [_]i32{ @intCast(request.kv_tokens), @intCast(dst_width) };
            return .{
                .k = MetalTensor.owned(compact_k, &shape),
                .v = MetalTensor.owned(compact_v, &shape),
            };
        }
    }
    if (@hasField(@TypeOf(request), "kv_manager")) {
        if (request.kv_manager) |manager| {
            if (@hasField(@TypeOf(request), "kv_pool_id")) {
                const pool = manager.getPool(request.kv_pool_id) orelse return null;
                if (!pool.config.store_cpu_bytes) return null;
            }
            const rows = try manager.gatherLayerKv(std.heap.c_allocator, request.sequence_id, request.layer_index, request.kv_tokens);
            defer {
                std.heap.c_allocator.free(rows.k);
                std.heap.c_allocator.free(rows.v);
            }
            const compact_k = try compactGatheredKvRows(std.heap.c_allocator, rows.k, request.kv_tokens, dst_width);
            errdefer std.heap.c_allocator.free(compact_k);
            const compact_v = try compactGatheredKvRows(std.heap.c_allocator, rows.v, request.kv_tokens, dst_width);
            errdefer std.heap.c_allocator.free(compact_v);
            const shape = [_]i32{ @intCast(request.kv_tokens), @intCast(dst_width) };
            return .{
                .k = MetalTensor.owned(compact_k, &shape),
                .v = MetalTensor.owned(compact_v, &shape),
            };
        }
    }
    return null;
}

pub fn encodeCompressedKeyRowsForRuntime(
    self: anytype,
    k: MetalTensor,
    kv_tokens: usize,
    num_kv_heads: usize,
    head_dim: usize,
    format: CompressedKeyFormat,
) ![]u8 {
    _ = self;
    if (k.ndim() != 2) return error.UnexpectedOutputShape;
    if (@as(usize, @intCast(k.dim(0))) != kv_tokens) return error.UnexpectedOutputShape;
    const row_width: usize = @intCast(k.dim(1));
    const expected_width = num_kv_heads * head_dim;
    if (row_width != expected_width) return error.UnexpectedOutputShape;

    const num_kv_heads_u32: u32 = @intCast(num_kv_heads);
    const head_dim_u32: u32 = @intCast(head_dim);
    const key_row_bytes = switch (format) {
        .polar4 => turboquant.polar4KeyBytes(num_kv_heads_u32, head_dim_u32),
        .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads_u32, head_dim_u32) + turboquant.turbo3ResidualBytes(num_kv_heads_u32, head_dim_u32),
    };
    if (key_row_bytes == 0) return error.UnsupportedKvHeadDim;

    var k_mut = k;
    const k_ptr = (try tensorHostSlice(&k_mut)).ptr;
    const encoded = try std.heap.c_allocator.alloc(u8, kv_tokens * key_row_bytes);
    errdefer std.heap.c_allocator.free(encoded);

    for (0..kv_tokens) |row| {
        const src = k_ptr[row * row_width ..][0..row_width];
        const dst = encoded[row * key_row_bytes ..][0..key_row_bytes];
        switch (format) {
            .polar4 => try turboquant.encodePolar4Key(src, dst, num_kv_heads_u32, head_dim_u32),
            .turbo3 => {
                const base_bytes = turboquant.turbo3KeyBytes(num_kv_heads_u32, head_dim_u32);
                const residual_bytes = turboquant.turbo3ResidualBytes(num_kv_heads_u32, head_dim_u32);
                try turboquant.encodeTurbo3Key(src, dst[0..base_bytes], num_kv_heads_u32, head_dim_u32);
                try turboquant.encodeTurbo3ResidualSketch(src, dst[0..base_bytes], dst[base_bytes..][0..residual_bytes], num_kv_heads_u32, head_dim_u32);
            },
        }
    }
    return encoded;
}

const GatheredSpanSeed = struct {
    k: MetalTensor,
    v: MetalTensor,
};

fn tryBuildInitialGatheredSpanSeed(
    self: anytype,
    k_suffix: MetalTensor,
    v_suffix: MetalTensor,
    bootstrap_k_blocks: []const MetalTensor,
    bootstrap_v_blocks: []const MetalTensor,
    bootstrap_block_token_counts: []const usize,
    full_k: ?MetalTensor,
    full_v: ?MetalTensor,
    query_sequence_len: usize,
    kv_tokens: usize,
) !?GatheredSpanSeed {
    var bootstrap_k = try cloneRowsFromBootstrapBlocks(self, bootstrap_k_blocks, bootstrap_block_token_counts);
    defer if (bootstrap_k) |*t| t.deinit();
    var bootstrap_v = try cloneRowsFromBootstrapBlocks(self, bootstrap_v_blocks, bootstrap_block_token_counts);
    defer if (bootstrap_v) |*t| t.deinit();

    const can_use_suffix = query_sequence_len == kv_tokens;
    if (bootstrap_k == null and full_k == null and !can_use_suffix) return null;
    if (bootstrap_v == null and full_v == null and !can_use_suffix) return null;

    var entry_k: MetalTensor = undefined;
    if (bootstrap_k) |owned| {
        entry_k = owned;
        bootstrap_k = null;
    } else if (full_k) |src| {
        entry_k = try cloneOrRetainTensor(src);
    } else {
        entry_k = try cloneOrRetainTensor(k_suffix);
    }
    errdefer entry_k.deinit();

    var entry_v: MetalTensor = undefined;
    if (bootstrap_v) |owned| {
        entry_v = owned;
        bootstrap_v = null;
    } else if (full_v) |src| {
        entry_v = try cloneOrRetainTensor(src);
    } else {
        entry_v = try cloneOrRetainTensor(v_suffix);
    }
    errdefer entry_v.deinit();

    return .{ .k = entry_k, .v = entry_v };
}

fn tryBuildResetGatheredSpanSeed(
    self: anytype,
    bootstrap_k_blocks: []const MetalTensor,
    bootstrap_v_blocks: []const MetalTensor,
    bootstrap_block_token_counts: []const usize,
    full_k: ?MetalTensor,
    full_v: ?MetalTensor,
) !?GatheredSpanSeed {
    var bootstrap_k = try cloneRowsFromBootstrapBlocks(self, bootstrap_k_blocks, bootstrap_block_token_counts);
    defer if (bootstrap_k) |*t| t.deinit();
    var bootstrap_v = try cloneRowsFromBootstrapBlocks(self, bootstrap_v_blocks, bootstrap_block_token_counts);
    defer if (bootstrap_v) |*t| t.deinit();
    if (bootstrap_k == null and full_k == null) return null;
    if (bootstrap_v == null and full_v == null) return null;

    var new_k: MetalTensor = undefined;
    if (bootstrap_k) |owned| {
        new_k = owned;
        bootstrap_k = null;
    } else {
        new_k = try cloneOrRetainTensor(full_k.?);
    }
    errdefer new_k.deinit();

    var new_v: MetalTensor = undefined;
    if (bootstrap_v) |owned| {
        new_v = owned;
        bootstrap_v = null;
    } else {
        new_v = try cloneOrRetainTensor(full_v.?);
    }
    errdefer new_v.deinit();

    return .{ .k = new_k, .v = new_v };
}

fn nextGatheredSpanCapacity(current: usize, required: usize) usize {
    var capacity = @max(current, @as(usize, 16));
    while (capacity < required) capacity *= 2;
    return capacity;
}

fn sameDeviceByteRange(a: MetalTensor, b: MetalTensor, byte_len: usize) bool {
    if (!a.isDevice() or !b.isDevice()) return false;
    return a.deviceHandle() == b.deviceHandle() and
        a.deviceByteOffset() == b.deviceByteOffset() and
        a.deviceByteLen() >= byte_len and
        b.deviceByteLen() >= byte_len;
}

pub fn reserveGatheredSpanSuffixViews(
    self: anytype,
    key: GatheredSpanKey,
    expected_prefix_tokens: usize,
    kv_tokens: usize,
    kv_position_offset: usize,
    k_width: usize,
    v_width: usize,
    suffix_tokens: usize,
) !?GatheredSpanSuffixViews {
    if (suffix_tokens == 0 or expected_prefix_tokens + suffix_tokens != kv_tokens) return null;
    const entry = self.gathered_spans.getPtr(key) orelse return null;
    if (entry.position_offset != kv_position_offset or entry.token_count != expected_prefix_tokens) return null;
    if (!entry.k.isDevice() or !entry.v.isDevice()) return null;
    if (entry.k.ndim() != 2 or entry.v.ndim() != 2) return null;
    if (@as(usize, @intCast(entry.k.dim(1))) != k_width or @as(usize, @intCast(entry.v.dim(1))) != v_width) return null;

    const runtime = entry.k.device.?.ref.runtime;
    if (entry.v.device.?.ref.runtime != runtime) return null;
    var capacity = if (entry.capacity_tokens != 0) entry.capacity_tokens else entry.token_count;
    const needed_capacity = kv_tokens;

    var k_storage: MetalTensor = undefined;
    var v_storage: MetalTensor = undefined;
    var owns_storage = false;
    if (capacity < needed_capacity) {
        capacity = nextGatheredSpanCapacity(capacity, needed_capacity);
        const k_storage_shape = [_]i32{ @intCast(capacity), @intCast(k_width) };
        const v_storage_shape = [_]i32{ @intCast(capacity), @intCast(v_width) };
        k_storage = try MetalTensor.deviceAllocate(runtime, capacity * k_width * @sizeOf(f32), .private, &k_storage_shape);
        errdefer k_storage.deinit();
        v_storage = try MetalTensor.deviceAllocate(runtime, capacity * v_width * @sizeOf(f32), .private, &v_storage_shape);
        errdefer v_storage.deinit();
        owns_storage = true;

        const k_prefix_bytes = entry.token_count * k_width * @sizeOf(f32);
        const v_prefix_bytes = entry.token_count * v_width * @sizeOf(f32);
        const k_prefix_shape = [_]i32{ @intCast(entry.token_count), @intCast(k_width) };
        var k_prefix_dst = try k_storage.retainedView(0, k_prefix_bytes, &k_prefix_shape);
        defer k_prefix_dst.deinit();
        const v_prefix_shape = [_]i32{ @intCast(entry.token_count), @intCast(v_width) };
        var v_prefix_dst = try v_storage.retainedView(0, v_prefix_bytes, &v_prefix_shape);
        defer v_prefix_dst.deinit();
        if (k_prefix_bytes != 0 and v_prefix_bytes != 0 and entry.k.deviceHandle() != null and entry.v.deviceHandle() != null) {
            const rc = termite_metal_buffer_copy_pair(
                @ptrCast(runtime),
                entry.k.deviceHandle(),
                entry.k.deviceByteOffset(),
                k_prefix_dst.deviceHandle(),
                k_prefix_dst.deviceByteOffset(),
                k_prefix_bytes,
                entry.v.deviceHandle(),
                entry.v.deviceByteOffset(),
                v_prefix_dst.deviceHandle(),
                v_prefix_dst.deviceByteOffset(),
                v_prefix_bytes,
            );
            if (rc != 0) return error.MetalBufferCopyFailed;
        } else {
            try entry.k.copyInto(&k_prefix_dst);
            try entry.v.copyInto(&v_prefix_dst);
        }

        var new_k = try k_storage.retainedView(0, k_prefix_bytes, &k_prefix_shape);
        errdefer new_k.deinit();
        var new_v = try v_storage.retainedView(0, v_prefix_bytes, &v_prefix_shape);
        errdefer new_v.deinit();
        entry.k.deinit();
        entry.v.deinit();
        if (entry.encoded_key) |encoded| std.heap.c_allocator.free(encoded);
        entry.k = new_k;
        entry.v = new_v;
        entry.capacity_tokens = capacity;
        entry.encoded_key = null;
    } else {
        k_storage = try entry.k.retainedStorageView(0, capacity * k_width * @sizeOf(f32), &[_]i32{ @intCast(capacity), @intCast(k_width) });
        errdefer k_storage.deinit();
        v_storage = try entry.v.retainedStorageView(0, capacity * v_width * @sizeOf(f32), &[_]i32{ @intCast(capacity), @intCast(v_width) });
        errdefer v_storage.deinit();
        owns_storage = true;
    }

    const k_suffix_offset = entry.token_count * k_width * @sizeOf(f32);
    const k_suffix_bytes = suffix_tokens * k_width * @sizeOf(f32);
    const v_suffix_offset = entry.token_count * v_width * @sizeOf(f32);
    const v_suffix_bytes = suffix_tokens * v_width * @sizeOf(f32);
    var k_view = try k_storage.retainedView(k_suffix_offset, k_suffix_bytes, &[_]i32{ @intCast(suffix_tokens), @intCast(k_width) });
    errdefer k_view.deinit();
    const v_view = try v_storage.retainedView(v_suffix_offset, v_suffix_bytes, &[_]i32{ @intCast(suffix_tokens), @intCast(v_width) });
    if (owns_storage) {
        k_storage.deinit();
        v_storage.deinit();
    }
    return .{ .k = k_view, .v = v_view };
}

fn appendGatheredSpanSuffixInPlace(
    self: anytype,
    entry: *GatheredSpanEntry,
    k_suffix: MetalTensor,
    v_suffix: MetalTensor,
    expected_prefix_tokens: usize,
    dropped_tokens: usize,
    kv_tokens: usize,
    kv_position_offset: usize,
) !bool {
    _ = self;
    if (dropped_tokens != 0) return false;
    if (expected_prefix_tokens != entry.token_count) return false;
    if (!entry.k.isDevice() or !entry.v.isDevice() or !k_suffix.isDevice() or !v_suffix.isDevice()) return false;
    if (entry.k.ndim() != 2 or entry.v.ndim() != 2 or k_suffix.ndim() != 2 or v_suffix.ndim() != 2) return false;

    const suffix_tokens: usize = @intCast(k_suffix.dim(0));
    if (suffix_tokens == 0 or @as(usize, @intCast(v_suffix.dim(0))) != suffix_tokens) return false;
    if (entry.token_count + suffix_tokens != kv_tokens) return false;
    const k_width: usize = @intCast(entry.k.dim(1));
    const v_width: usize = @intCast(entry.v.dim(1));
    if (@as(usize, @intCast(k_suffix.dim(1))) != k_width) return false;
    if (@as(usize, @intCast(v_suffix.dim(1))) != v_width) return false;

    const k_dev = entry.k.device.?;
    const v_dev = entry.v.device.?;
    const k_suffix_dev = k_suffix.device.?;
    const v_suffix_dev = v_suffix.device.?;
    const runtime = k_dev.ref.runtime;
    if (v_dev.ref.runtime != runtime or k_suffix_dev.ref.runtime != runtime or v_suffix_dev.ref.runtime != runtime) return false;

    const needed_capacity = kv_tokens;
    var capacity = if (entry.capacity_tokens != 0) entry.capacity_tokens else entry.token_count;
    var k_storage: MetalTensor = undefined;
    var v_storage: MetalTensor = undefined;
    var owns_storage = false;
    if (capacity < needed_capacity) {
        capacity = nextGatheredSpanCapacity(capacity, needed_capacity);
        const k_storage_shape = [_]i32{ @intCast(capacity), @intCast(k_width) };
        const v_storage_shape = [_]i32{ @intCast(capacity), @intCast(v_width) };
        k_storage = try MetalTensor.deviceAllocate(runtime, capacity * k_width * @sizeOf(f32), .private, &k_storage_shape);
        errdefer k_storage.deinit();
        v_storage = try MetalTensor.deviceAllocate(runtime, capacity * v_width * @sizeOf(f32), .private, &v_storage_shape);
        errdefer v_storage.deinit();
        owns_storage = true;

        const prefix_shape = [_]i32{ @intCast(entry.token_count), @intCast(k_width) };
        var k_prefix_dst = try k_storage.retainedView(0, entry.token_count * k_width * @sizeOf(f32), &prefix_shape);
        defer k_prefix_dst.deinit();
        try entry.k.copyInto(&k_prefix_dst);
        const v_prefix_shape = [_]i32{ @intCast(entry.token_count), @intCast(v_width) };
        var v_prefix_dst = try v_storage.retainedView(0, entry.token_count * v_width * @sizeOf(f32), &v_prefix_shape);
        defer v_prefix_dst.deinit();
        try entry.v.copyInto(&v_prefix_dst);
    } else {
        k_storage = try entry.k.retainedStorageView(0, capacity * k_width * @sizeOf(f32), &[_]i32{ @intCast(capacity), @intCast(k_width) });
        errdefer k_storage.deinit();
        v_storage = try entry.v.retainedStorageView(0, capacity * v_width * @sizeOf(f32), &[_]i32{ @intCast(capacity), @intCast(v_width) });
        errdefer v_storage.deinit();
        owns_storage = true;
    }

    const k_suffix_offset = entry.token_count * k_width * @sizeOf(f32);
    const k_suffix_bytes = suffix_tokens * k_width * @sizeOf(f32);
    var k_suffix_dst = try k_storage.retainedView(k_suffix_offset, k_suffix_bytes, &[_]i32{ @intCast(suffix_tokens), @intCast(k_width) });
    defer k_suffix_dst.deinit();

    const v_suffix_offset = entry.token_count * v_width * @sizeOf(f32);
    const v_suffix_bytes = suffix_tokens * v_width * @sizeOf(f32);
    var v_suffix_dst = try v_storage.retainedView(v_suffix_offset, v_suffix_bytes, &[_]i32{ @intCast(suffix_tokens), @intCast(v_width) });
    defer v_suffix_dst.deinit();
    if (sameDeviceByteRange(k_suffix, k_suffix_dst, k_suffix_bytes) and
        sameDeviceByteRange(v_suffix, v_suffix_dst, v_suffix_bytes))
    {
        // The caller already wrote the transformed suffix rows into the planned
        // gathered-KV destination.
    } else if (k_suffix.deviceHandle()) |k_src_handle| {
        if (k_suffix_dst.deviceHandle()) |k_dst_handle| {
            if (v_suffix.deviceHandle()) |v_src_handle| {
                if (v_suffix_dst.deviceHandle()) |v_dst_handle| {
                    const rc = termite_metal_buffer_copy_pair(
                        @ptrCast(runtime),
                        k_src_handle,
                        k_suffix.deviceByteOffset(),
                        k_dst_handle,
                        k_suffix_dst.deviceByteOffset(),
                        k_suffix_bytes,
                        v_src_handle,
                        v_suffix.deviceByteOffset(),
                        v_dst_handle,
                        v_suffix_dst.deviceByteOffset(),
                        v_suffix_bytes,
                    );
                    if (rc != 0) return error.MetalBufferCopyFailed;
                } else {
                    try k_suffix.copyInto(&k_suffix_dst);
                    try v_suffix.copyInto(&v_suffix_dst);
                }
            } else {
                try k_suffix.copyInto(&k_suffix_dst);
                try v_suffix.copyInto(&v_suffix_dst);
            }
        } else {
            try k_suffix.copyInto(&k_suffix_dst);
            try v_suffix.copyInto(&v_suffix_dst);
        }
    } else {
        try k_suffix.copyInto(&k_suffix_dst);
        try v_suffix.copyInto(&v_suffix_dst);
    }

    var new_k = try k_storage.retainedView(0, kv_tokens * k_width * @sizeOf(f32), &[_]i32{ @intCast(kv_tokens), @intCast(k_width) });
    errdefer new_k.deinit();
    var new_v = try v_storage.retainedView(0, kv_tokens * v_width * @sizeOf(f32), &[_]i32{ @intCast(kv_tokens), @intCast(v_width) });
    errdefer new_v.deinit();
    if (owns_storage) {
        k_storage.deinit();
        v_storage.deinit();
    }

    entry.k.deinit();
    entry.v.deinit();
    if (entry.encoded_key) |encoded| std.heap.c_allocator.free(encoded);
    entry.* = .{
        .k = new_k,
        .v = new_v,
        .token_count = kv_tokens,
        .position_offset = kv_position_offset,
        .capacity_tokens = capacity,
        .encoded_key = null,
        .encoded_format = null,
        .encoded_key_row_bytes = 0,
    };
    return true;
}

fn appendGatheredSpanSuffix(
    self: anytype,
    entry: *GatheredSpanEntry,
    k_suffix: MetalTensor,
    v_suffix: MetalTensor,
    expected_prefix_tokens: usize,
    dropped_tokens: usize,
    kv_tokens: usize,
    kv_position_offset: usize,
) !void {
    if (try appendGatheredSpanSuffixInPlace(
        self,
        entry,
        k_suffix,
        v_suffix,
        expected_prefix_tokens,
        dropped_tokens,
        kv_tokens,
        kv_position_offset,
    )) return;

    var retained_k = try sliceRows(self, entry.k, dropped_tokens, expected_prefix_tokens);
    defer retained_k.deinit();
    var retained_v = try sliceRows(self, entry.v, dropped_tokens, expected_prefix_tokens);
    defer retained_v.deinit();

    var new_k = if (expected_prefix_tokens > 0)
        try concatenateRows(self, retained_k, k_suffix)
    else
        try cloneOrRetainTensor(k_suffix);
    errdefer new_k.deinit();

    var new_v = if (expected_prefix_tokens > 0)
        try concatenateRows(self, retained_v, v_suffix)
    else
        try cloneOrRetainTensor(v_suffix);
    errdefer new_v.deinit();

    try replaceGatheredSpanEntry(self, entry, new_k, new_v, kv_tokens, kv_position_offset);
}

const GatheredSpanTransition = union(enum) {
    same_span,
    append_suffix: struct {
        dropped_tokens: usize,
        expected_prefix_tokens: usize,
    },
    reset_rebuild,
};

fn decideGatheredSpanTransition(
    entry: *const GatheredSpanEntry,
    query_sequence_len: usize,
    kv_tokens: usize,
    kv_position_offset: usize,
    stats: anytype,
) GatheredSpanTransition {
    if (entry.position_offset == kv_position_offset and entry.token_count == kv_tokens) {
        stats.gathered_span_same_span_hits += 1;
        return .same_span;
    }

    if (kv_position_offset >= entry.position_offset) {
        const dropped_tokens = kv_position_offset - entry.position_offset;
        const expected_prefix_tokens = kv_tokens - query_sequence_len;
        if (entry.token_count >= dropped_tokens and entry.token_count - dropped_tokens == expected_prefix_tokens) {
            stats.gathered_span_append_hits += 1;
            return .{ .append_suffix = .{
                .dropped_tokens = dropped_tokens,
                .expected_prefix_tokens = expected_prefix_tokens,
            } };
        }
        stats.gathered_span_prefix_token_mismatches += 1;
        stats.gathered_span_prefix_mismatch_resets += 1;
        return .reset_rebuild;
    }

    stats.gathered_span_offset_regressions += 1;
    stats.gathered_span_prefix_mismatch_resets += 1;
    return .reset_rebuild;
}

pub fn updateGatheredSpan(
    self: anytype,
    key: GatheredSpanKey,
    k_suffix: MetalTensor,
    v_suffix: MetalTensor,
    bootstrap_k_blocks: []const MetalTensor,
    bootstrap_v_blocks: []const MetalTensor,
    bootstrap_block_token_counts: []const usize,
    full_k: ?MetalTensor,
    full_v: ?MetalTensor,
    query_sequence_len: usize,
    kv_tokens: usize,
    kv_position_offset: usize,
    stats: anytype,
) !?*GatheredSpanEntry {
    if (query_sequence_len == kv_tokens) {
        if (stats.gathered_span_first_prefill_source_ptr == 0) {
            stats.gathered_span_first_prefill_source_ptr = key.source_ptr_id;
        }
    } else if (stats.gathered_span_first_decode_source_ptr == 0) {
        stats.gathered_span_first_decode_source_ptr = key.source_ptr_id;
    }

    const gop = try self.gathered_spans.getOrPut(std.heap.c_allocator, key);
    if (!gop.found_existing) {
        errdefer _ = self.gathered_spans.remove(key);
        const seed = try tryBuildInitialGatheredSpanSeed(
            self,
            k_suffix,
            v_suffix,
            bootstrap_k_blocks,
            bootstrap_v_blocks,
            bootstrap_block_token_counts,
            full_k,
            full_v,
            query_sequence_len,
            kv_tokens,
        ) orelse {
            stats.gathered_span_cold_miss_nulls += 1;
            _ = self.gathered_spans.remove(key);
            return null;
        };
        gop.value_ptr.* = .{
            .k = seed.k,
            .v = seed.v,
            .token_count = kv_tokens,
            .position_offset = kv_position_offset,
            .capacity_tokens = kv_tokens,
        };
        try ensureGatheredSpanEntryDevice(self, gop.value_ptr, k_suffix, v_suffix);
        return gop.value_ptr;
    }

    const entry = gop.value_ptr;
    if (query_sequence_len == kv_tokens) {
        const init_k = full_k orelse k_suffix;
        const init_v = full_v orelse v_suffix;
        var new_k = try cloneOrRetainTensor(init_k);
        errdefer new_k.deinit();
        var new_v = try cloneOrRetainTensor(init_v);
        errdefer new_v.deinit();
        try replaceGatheredSpanEntry(self, entry, new_k, new_v, kv_tokens, kv_position_offset);
        try ensureGatheredSpanEntryDevice(self, entry, k_suffix, v_suffix);
        return entry;
    }

    switch (decideGatheredSpanTransition(entry, query_sequence_len, kv_tokens, kv_position_offset, stats)) {
        .same_span => {
            try ensureGatheredSpanEntryDevice(self, entry, k_suffix, v_suffix);
            return entry;
        },
        .append_suffix => |append| {
            try appendGatheredSpanSuffix(
                self,
                entry,
                k_suffix,
                v_suffix,
                append.expected_prefix_tokens,
                append.dropped_tokens,
                kv_tokens,
                kv_position_offset,
            );
            try ensureGatheredSpanEntryDevice(self, entry, k_suffix, v_suffix);
            return entry;
        },
        .reset_rebuild => {},
    }

    const reset_seed = try tryBuildResetGatheredSpanSeed(
        self,
        bootstrap_k_blocks,
        bootstrap_v_blocks,
        bootstrap_block_token_counts,
        full_k,
        full_v,
    ) orelse return null;
    stats.gathered_span_reset_rebuilds += 1;
    try replaceGatheredSpanEntry(self, entry, reset_seed.k, reset_seed.v, kv_tokens, kv_position_offset);
    try ensureGatheredSpanEntryDevice(self, entry, k_suffix, v_suffix);
    return entry;
}

fn ensureGatheredSpanHostEncoding(
    self: anytype,
    entry: *GatheredSpanEntry,
    kv_tokens: usize,
    num_kv_heads: usize,
    head_dim: usize,
    format: CompressedKeyFormat,
    key_row_bytes: usize,
    stats: anytype,
) !void {
    if (entry.encoded_key != null and entry.encoded_format == format and entry.encoded_key_row_bytes == key_row_bytes) return;
    const encode_started_at = monotonicNowNs();
    const encoded = try encodeCompressedKeyRowsForRuntime(self, entry.k, kv_tokens, num_kv_heads, head_dim, format);
    stats.compressed_block_encode_nanos += @intCast(monotonicNowNs() - encode_started_at);
    if (entry.encoded_key) |prior| std.heap.c_allocator.free(prior);
    entry.encoded_key = encoded;
    entry.encoded_format = format;
    entry.encoded_key_row_bytes = key_row_bytes;
}

/// Apply optional layer-norm or RMS-norm to `input`. When neither slot is set,
/// returns a borrowed MetalTensor view of the input (caller's deinit is a no-op
/// because `owned_by_c_allocator = false`). When a norm fires, returns an owned
/// MetalTensor backed by c_allocator that the caller must deinit.
pub fn decoderRuntimeApplyFfnNormInternal(
    self: anytype,
    input: MetalTensor,
    layer_norm_slot: ?usize,
    rms_norm_slot: ?usize,
    hidden_size: usize,
    eps: f32,
) !?MetalTensor {
    var norm_stats = struct {
        decoder_runtime_apply_layer_norm_calls: u64 = 0,
    }{};
    if (layer_norm_slot) |slot| {
        return try decoderRuntimeApplyLayerNorm(self, .{
            .input = input,
            .slot = slot,
            .hidden_size = hidden_size,
            .eps = eps,
        }, &norm_stats);
    }
    if (rms_norm_slot) |slot| {
        return try decoderRuntimeApplyRmsNorm(self, .{
            .input = input,
            .slot = slot,
            .hidden_size = hidden_size,
            .eps = eps,
        }, &norm_stats);
    }
    return input;
}

pub fn runCompressedAttentionResidual(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const attention_input_size = request.attention_input_size;
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.attention_linear_slot >= decoder_runtime_linear_slot_capacity) return null;
    if (!self.raw_linear_slots_prepared[request.attention_linear_slot]) return null;
    if (self.raw_linear_slot_in_dims[request.attention_linear_slot] != attention_input_size or
        self.raw_linear_slot_out_dims[request.attention_linear_slot] != request.hidden_size) return null;
    if (request.q.ndim() != 2 or
        request.k_suffix.ndim() != 2 or
        request.v_suffix.ndim() != 2 or
        request.residual.ndim() != 2) return null;
    const q_rows: usize = @intCast(request.q.dim(0));
    if (q_rows == 0 or @as(usize, @intCast(request.residual.dim(0))) != q_rows) return null;
    if (q_rows > 1) {
        stats.compressed_attention_residual_multi_row_calls += 1;
        if (request.query_sequence_len != q_rows or request.kv_tokens < request.query_sequence_len) return null;
        const prefix_tokens = request.kv_tokens - request.query_sequence_len;
        const pieces = try std.heap.c_allocator.alloc(MetalTensor, q_rows);
        defer std.heap.c_allocator.free(pieces);
        var piece_count: usize = 0;
        defer {
            for (pieces[0..piece_count]) |*piece| piece.deinit();
        }
        for (0..q_rows) |row| {
            var row_q_mt = try sliceRows(self, request.q, row, 1);
            defer row_q_mt.deinit();
            var row_residual_mt = try sliceRows(self, request.residual, row, 1);
            defer row_residual_mt.deinit();
            const suffix_count = row + 1;
            var row_k_suffix_mt = try sliceRows(self, request.k_suffix, 0, suffix_count);
            defer row_k_suffix_mt.deinit();
            var row_v_suffix_mt = try sliceRows(self, request.v_suffix, 0, suffix_count);
            defer row_v_suffix_mt.deinit();
            pieces[piece_count] = (try runCompressedAttentionResidual(self, .{
                .q = row_q_mt,
                .k_suffix = row_k_suffix_mt,
                .v_suffix = row_v_suffix_mt,
                .bootstrap_k_blocks = request.bootstrap_k_blocks,
                .bootstrap_v_blocks = request.bootstrap_v_blocks,
                .bootstrap_block_token_counts = request.bootstrap_block_token_counts,
                .full_k = request.full_k,
                .full_v = request.full_v,
                .source_ptr_id = request.source_ptr_id,
                .sequence_id = request.sequence_id,
                .layer_index = request.layer_index,
                .query_sequence_len = suffix_count,
                .kv_tokens = prefix_tokens + suffix_count,
                .num_heads = request.num_heads,
                .num_kv_heads = request.num_kv_heads,
                .head_dim = request.head_dim,
                .key_row_bytes = request.key_row_bytes,
                .query_position = request.kv_position_offset + prefix_tokens + row,
                .kv_position_offset = request.kv_position_offset,
                .sliding_window = request.sliding_window,
                .format = request.format,
                .attention_linear_slot = request.attention_linear_slot,
                .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
                .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
                .residual = row_residual_mt,
                .hidden_size = request.hidden_size,
                .attention_input_size = request.attention_input_size,
                .eps = request.eps,
            }, stats)) orelse return null;
            piece_count += 1;
        }
        var total_rows: usize = 0;
        const row_width: usize = @intCast(pieces[0].dim(1));
        for (pieces[0..piece_count]) |piece| {
            if (piece.ndim() != 2 or @as(usize, @intCast(piece.dim(1))) != row_width) return error.InvalidPagedKvShape;
            total_rows += @intCast(piece.dim(0));
        }
        const concat_buf = try std.heap.c_allocator.alloc(f32, total_rows * row_width);
        errdefer std.heap.c_allocator.free(concat_buf);
        var cursor: usize = 0;
        for (pieces[0..piece_count]) |piece| {
            const piece_len: usize = @intCast(piece.dim(0));
            var piece_mut = piece;
            @memcpy(concat_buf[cursor..][0 .. piece_len * row_width], try tensorHostSlice(&piece_mut));
            cursor += piece_len * row_width;
        }
        const concat_shape = [_]i32{ @intCast(total_rows), @intCast(row_width) };
        stats.compressed_attention_residual_multi_row_successes += 1;
        return MetalTensor.owned(concat_buf, &concat_shape);
    }

    const base_key_row_bytes = switch (request.format) {
        .polar4 => (@as(usize, request.num_kv_heads) * request.head_dim + 1) / 2,
        .turbo3 => (@as(usize, request.num_kv_heads) * request.head_dim * 3 + 7) / 8,
    };
    const span_prep_started_at = monotonicNowNs();
    const gathered = (try updateGatheredSpan(
        self,
        .{
            .source_ptr_id = request.source_ptr_id,
            .sequence_id = request.sequence_id,
            .layer_index = request.layer_index,
        },
        request.k_suffix,
        request.v_suffix,
        request.bootstrap_k_blocks,
        request.bootstrap_v_blocks,
        request.bootstrap_block_token_counts,
        request.full_k,
        request.full_v,
        request.query_sequence_len,
        request.kv_tokens,
        request.kv_position_offset,
        stats,
    )) orelse return null;
    stats.compressed_block_span_prep_nanos += @intCast(monotonicNowNs() - span_prep_started_at);

    var q_tensor = request.q;
    var residual_tensor = request.residual;
    const v_row_stride: usize = @intCast(gathered.v.dim(1));
    if (q_tensor.isDevice() and residual_tensor.isDevice() and gathered.k.isDevice() and gathered.v.isDevice()) {
        if (try decoderRuntimeApplyQuantizedKvAttention(self, .{
            .q = q_tensor,
            .k = gathered.k,
            .v = gathered.v,
            .kv_tokens = request.kv_tokens,
            .num_heads = request.num_heads,
            .num_kv_heads = request.num_kv_heads,
            .head_dim = request.head_dim,
            .key_row_bytes = request.key_row_bytes,
            .base_key_row_bytes = base_key_row_bytes,
            .query_position = request.query_position,
            .kv_position_offset = request.kv_position_offset,
            .sliding_window = request.sliding_window,
            .format = request.format,
        })) |attention_output_value| {
            var attention_output = attention_output_value;
            errdefer attention_output.deinit();
            debugRuntimeTensorFinite("attention-output", request.layer_index, attention_output);
            if (self.raw_linear_slot_kinds[request.attention_linear_slot] == .quantized) {
                defer attention_output.deinit();
                var projected_tensor = (try decoderRuntimeApplyLinear(self, .{
                    .slot = request.attention_linear_slot,
                    .input = attention_output,
                    .in_dim = attention_input_size,
                    .out_dim = request.hidden_size,
                })) orelse return null;
                debugRuntimeTensorFinite("attention-projected", request.layer_index, projected_tensor);
                defer projected_tensor.deinit();
                var post_linear = projected_tensor;
                var post_norm_owned: ?MetalTensor = null;
                defer if (post_norm_owned) |*tensor| tensor.deinit();
                if (request.attention_post_linear_rms_norm_slot) |slot| {
                    post_norm_owned = (try decoderRuntimeApplyRmsNorm(self, .{
                        .slot = slot,
                        .input = projected_tensor,
                        .hidden_size = request.hidden_size,
                        .eps = request.eps,
                    }, stats)) orelse return null;
                    post_linear = post_norm_owned.?;
                    debugRuntimeTensorFinite("attention-post-norm", request.layer_index, post_linear);
                }
                const result_tensor = (try decoderRuntimeApplyAdd(self, .{
                    .lhs = post_linear,
                    .rhs = request.residual,
                    .dim = request.hidden_size,
                }, stats)) orelse return null;
                debugRuntimeTensorFinite("attention-residual", request.layer_index, result_tensor);
                stats.compressed_attention_residual_post_linear_successes += 1;
                return result_tensor;
            }

            var current = attention_output;
            defer current.deinit();

            if (request.attention_pre_linear_rms_norm_slot) |slot| {
                const normed = (try decoderRuntimeApplyRmsNorm(self, .{
                    .slot = slot,
                    .input = current,
                    .hidden_size = attention_input_size,
                    .eps = request.eps,
                }, stats)) orelse return null;
                current.deinit();
                current = normed;
            }

            const projected = (try decoderRuntimeApplyLinear(self, .{
                .slot = request.attention_linear_slot,
                .input = current,
                .in_dim = attention_input_size,
                .out_dim = request.hidden_size,
            })) orelse return null;
            current.deinit();
            current = projected;

            if (request.attention_post_linear_rms_norm_slot) |slot| {
                const normed = (try decoderRuntimeApplyRmsNorm(self, .{
                    .slot = slot,
                    .input = current,
                    .hidden_size = request.hidden_size,
                    .eps = request.eps,
                }, stats)) orelse return null;
                current.deinit();
                current = normed;
            }

            const fallback_result = (try decoderRuntimeApplyAdd(self, .{
                .lhs = current,
                .rhs = request.residual,
                .dim = request.hidden_size,
            }, stats)) orelse return null;
            stats.compressed_attention_residual_post_linear_successes += 1;
            return fallback_result;
        }
    }

    ensureGatheredSpanHostEncoding(
        self,
        gathered,
        request.kv_tokens,
        request.num_kv_heads,
        request.head_dim,
        request.format,
        request.key_row_bytes,
        stats,
    ) catch |err| switch (err) {
        error.UnsupportedKvHeadDim => return null,
        else => return err,
    };
    const encoded_ptr: [*c]const u8 = if (gathered.encoded_key) |encoded| encoded.ptr else null;
    const q_ptr = try tensorHostConstPtr(&q_tensor);
    var gathered_v = gathered.v;
    const v_ptr = try tensorHostConstPtr(&gathered_v);
    const residual_ptr = try tensorHostConstPtr(&residual_tensor);

    const result = try std.heap.c_allocator.alloc(f32, request.hidden_size);
    errdefer std.heap.c_allocator.free(result);
    if (try tryRawCompressedAttentionResidualHost(
        self,
        request.format,
        q_ptr,
        try tensorHostConstPtr(&gathered.k),
        encoded_ptr,
        v_ptr,
        request.kv_tokens,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.key_row_bytes,
        v_row_stride,
        base_key_row_bytes,
        request.query_position,
        request.kv_position_offset,
        request.sliding_window,
        residual_ptr,
        attention_input_size,
        request.hidden_size,
        request.eps,
        request.attention_linear_slot,
        request.attention_pre_linear_rms_norm_slot,
        request.attention_post_linear_rms_norm_slot,
        request.layer_index,
        result.ptr,
    )) {
        debugRuntimeSliceFinite("attention-residual-raw-host", request.layer_index, result[0..request.hidden_size]);
        stats.compressed_attention_residual_fused_successes += 1;
        const out_shape = [_]i32{ 1, @intCast(request.hidden_size) };
        return MetalTensor.owned(result, &out_shape);
    }
    std.heap.c_allocator.free(result);

    const encoded = gathered.encoded_key orelse return error.MlxDataNull;

    if (!decoderRuntimeReserveAttentionSpanScratch(
        self,
        request.kv_tokens,
        request.key_row_bytes,
        v_row_stride,
    )) {
        stats.compressed_attention_residual_update_span_failures += 1;
        return null;
    }
    if (termite_metal_decode_runtime_update_attention_span(
        runtime,
        encoded.ptr,
        v_ptr,
        request.kv_tokens,
        request.key_row_bytes,
        v_row_stride,
        request.kv_position_offset,
    ) != 0) {
        stats.compressed_attention_residual_update_span_failures += 1;
        return null;
    }

    const attention_output = try std.heap.c_allocator.alloc(f32, attention_input_size);
    var attention_output_owned = true;
    errdefer if (attention_output_owned) std.heap.c_allocator.free(attention_output);
    stats.decoder_runtime_attention_span_calls += 1;
    if (termite_metal_decode_runtime_attention_span(
        runtime,
        switch (request.format) {
            .polar4 => 0,
            .turbo3 => 1,
        },
        q_ptr,
        request.kv_tokens,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.key_row_bytes,
        base_key_row_bytes,
        request.query_position,
        request.kv_position_offset,
        request.sliding_window,
        attention_output.ptr,
    ) != 0) {
        stats.compressed_attention_residual_attention_span_failures += 1;
        return null;
    }
    debugRuntimeSliceFinite("attention-output-host", request.layer_index, attention_output[0..attention_input_size]);

    const post_span_result = try std.heap.c_allocator.alloc(f32, request.hidden_size);
    errdefer std.heap.c_allocator.free(post_span_result);
    if (try tryRawAttentionResidualHost(
        self,
        attention_output.ptr,
        residual_ptr,
        attention_input_size,
        request.hidden_size,
        request.eps,
        request.attention_linear_slot,
        request.attention_pre_linear_rms_norm_slot,
        request.attention_post_linear_rms_norm_slot,
        post_span_result.ptr,
    )) {
        debugRuntimeSliceFinite("attention-residual-host", request.layer_index, post_span_result[0..request.hidden_size]);
        stats.compressed_attention_residual_post_linear_successes += 1;
        std.heap.c_allocator.free(attention_output);
        attention_output_owned = false;
        const out_shape = [_]i32{ 1, @intCast(request.hidden_size) };
        return MetalTensor.owned(post_span_result, &out_shape);
    }
    stats.compressed_attention_residual_post_linear_failures += 1;
    std.heap.c_allocator.free(post_span_result);

    if (self.raw_linear_slot_kinds[request.attention_linear_slot] == .quantized) {
        const attention_shape = [_]i32{ 1, @intCast(attention_input_size) };
        var current = MetalTensor.owned(attention_output, &attention_shape);
        defer current.deinit();
        attention_output_owned = false;

        var projected_tensor = (try decoderRuntimeApplyLinear(self, .{
            .slot = request.attention_linear_slot,
            .input = current,
            .in_dim = attention_input_size,
            .out_dim = request.hidden_size,
        })) orelse return null;
        debugRuntimeTensorFinite("attention-projected-host", request.layer_index, projected_tensor);
        defer projected_tensor.deinit();
        var post_linear = projected_tensor;
        var post_norm_owned: ?MetalTensor = null;
        defer if (post_norm_owned) |*tensor| tensor.deinit();
        if (request.attention_post_linear_rms_norm_slot) |slot| {
            post_norm_owned = (try decoderRuntimeApplyRmsNorm(self, .{
                .slot = slot,
                .input = projected_tensor,
                .hidden_size = request.hidden_size,
                .eps = request.eps,
            }, stats)) orelse return null;
            post_linear = post_norm_owned.?;
            debugRuntimeTensorFinite("attention-post-norm-host", request.layer_index, post_linear);
        }
        const result_tensor = (try decoderRuntimeApplyAdd(self, .{
            .lhs = post_linear,
            .rhs = request.residual,
            .dim = request.hidden_size,
        }, stats)) orelse return null;
        debugRuntimeTensorFinite("attention-residual-host", request.layer_index, result_tensor);
        stats.compressed_attention_residual_post_linear_successes += 1;
        return result_tensor;
    }

    const attention_shape = [_]i32{ 1, @intCast(attention_input_size) };
    var current = MetalTensor.owned(attention_output, &attention_shape);
    defer current.deinit();
    attention_output_owned = false;

    if (request.attention_pre_linear_rms_norm_slot) |slot| {
        const normed = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = slot,
            .input = current,
            .hidden_size = attention_input_size,
            .eps = request.eps,
        }, stats)) orelse return null;
        current.deinit();
        current = normed;
    }

    const projected = (try decoderRuntimeApplyLinear(self, .{
        .slot = request.attention_linear_slot,
        .input = current,
        .in_dim = attention_input_size,
        .out_dim = request.hidden_size,
    })) orelse return null;
    current.deinit();
    current = projected;

    if (request.attention_post_linear_rms_norm_slot) |slot| {
        const normed = (try decoderRuntimeApplyRmsNorm(self, .{
            .slot = slot,
            .input = current,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, stats)) orelse return null;
        current.deinit();
        current = normed;
    }

    const fallback_result = (try decoderRuntimeApplyAdd(self, .{
        .lhs = current,
        .rhs = request.residual,
        .dim = request.hidden_size,
    }, stats)) orelse return null;
    stats.compressed_attention_residual_post_linear_successes += 1;
    return fallback_result;
}

pub fn runCompressedAttentionDenseDecoderBlockDirect(self: anytype, request: anytype, stats: anytype) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.attention_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.first_ffn_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.second_ffn_linear_slot >= decoder_runtime_linear_slot_capacity)
    {
        return null;
    }
    var projected_fused_qkv: ?MetalTensor = null;
    defer if (projected_fused_qkv) |*tensor| tensor.deinit();
    var projected_q: ?MetalTensor = null;
    defer if (projected_q) |*tensor| tensor.deinit();
    var projected_k: ?MetalTensor = null;
    defer if (projected_k) |*tensor| tensor.deinit();
    var projected_v: ?MetalTensor = null;
    defer if (projected_v) |*tensor| tensor.deinit();
    const maybe_q: ?MetalTensor = if (comptime @typeInfo(@TypeOf(request.q)) == .optional) request.q else request.q;
    const maybe_k_suffix: ?MetalTensor = if (comptime @typeInfo(@TypeOf(request.k_suffix)) == .optional) request.k_suffix else request.k_suffix;
    const maybe_v_suffix: ?MetalTensor = if (comptime @typeInfo(@TypeOf(request.v_suffix)) == .optional) request.v_suffix else request.v_suffix;
    const maybe_attention_input: ?MetalTensor = if (comptime @typeInfo(@TypeOf(request.attention_input)) == .optional) request.attention_input else request.attention_input;
    const maybe_fused_qkv_linear_slot: ?usize = if (comptime @typeInfo(@TypeOf(request.fused_qkv_linear_slot)) == .optional) request.fused_qkv_linear_slot else request.fused_qkv_linear_slot;
    const maybe_kv_manager = if (comptime @hasField(@TypeOf(request), "kv_manager")) request.kv_manager else null;
    const maybe_kv_storage = if (comptime @hasField(@TypeOf(request), "kv_storage")) request.kv_storage else null;
    const kv_pool_id = if (comptime @hasField(@TypeOf(request), "kv_pool_id")) request.kv_pool_id else 0;
    const skip_kv_write = if (comptime @hasField(@TypeOf(request), "skip_kv_write")) request.skip_kv_write else false;
    const input_rows: usize = blk: {
        if (maybe_q) |tensor| break :blk @intCast(tensor.dim(0));
        if (maybe_attention_input) |tensor| break :blk @intCast(tensor.dim(0));
        break :blk 0;
    };
    if (input_rows == 0) return null;
    if (@as(usize, @intCast(request.residual.dim(0))) != input_rows) return null;
    if (input_rows > 1) {
        if (request.query_sequence_len != input_rows or request.kv_tokens < request.query_sequence_len) return null;
        const prefix_tokens = request.kv_tokens - request.query_sequence_len;
        var combined: ?MetalTensor = null;
        errdefer if (combined) |*tensor| tensor.deinit();
        for (0..input_rows) |row| {
            var row_residual = try sliceRows(self, request.residual, row, 1);
            defer row_residual.deinit();
            const row_kv_tokens = prefix_tokens + row + 1;
            const row_query_position = request.kv_position_offset + prefix_tokens + row;
            var row_output = if (maybe_q != null and maybe_k_suffix != null and maybe_v_suffix != null) blk: {
                var row_q = try sliceRows(self, maybe_q.?, row, 1);
                defer row_q.deinit();
                var row_k = try sliceRows(self, maybe_k_suffix.?, row, 1);
                defer row_k.deinit();
                var row_v = try sliceRows(self, maybe_v_suffix.?, row, 1);
                defer row_v.deinit();
                break :blk (try runCompressedAttentionDenseDecoderBlockDirect(self, .{
                    .q = row_q,
                    .k_suffix = row_k,
                    .v_suffix = row_v,
                    .attention_input = null,
                    .fused_qkv_linear_slot = null,
                    .kv_manager = maybe_kv_manager,
                    .kv_storage = maybe_kv_storage,
                    .kv_pool_id = kv_pool_id,
                    .skip_kv_write = skip_kv_write,
                    .bootstrap_k_blocks = request.bootstrap_k_blocks,
                    .bootstrap_v_blocks = request.bootstrap_v_blocks,
                    .bootstrap_block_token_counts = request.bootstrap_block_token_counts,
                    .full_k = request.full_k,
                    .full_v = request.full_v,
                    .source_ptr_id = request.source_ptr_id,
                    .sequence_id = request.sequence_id,
                    .layer_index = request.layer_index,
                    .query_sequence_len = 1,
                    .kv_tokens = row_kv_tokens,
                    .num_heads = request.num_heads,
                    .num_kv_heads = request.num_kv_heads,
                    .head_dim = request.head_dim,
                    .key_row_bytes = request.key_row_bytes,
                    .query_position = row_query_position,
                    .kv_position_offset = request.kv_position_offset,
                    .sliding_window = request.sliding_window,
                    .format = request.format,
                    .attention_linear_slot = request.attention_linear_slot,
                    .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
                    .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
                    .residual = row_residual,
                    .hidden_size = request.hidden_size,
                    .eps = request.eps,
                    .ffn_layer_norm_slot = request.ffn_layer_norm_slot,
                    .ffn_rms_norm_slot = request.ffn_rms_norm_slot,
                    .first_ffn_linear_slot = request.first_ffn_linear_slot,
                    .second_ffn_linear_slot = request.second_ffn_linear_slot,
                    .intermediate_size = request.intermediate_size,
                    .activation = request.activation,
                }, stats)) orelse return null;
            } else blk: {
                const attention_input = maybe_attention_input orelse return null;
                const fused_qkv_linear_slot = maybe_fused_qkv_linear_slot orelse return null;
                var row_attention_input = try sliceRows(self, attention_input, row, 1);
                defer row_attention_input.deinit();
                break :blk (try runCompressedAttentionDenseDecoderBlockDirect(self, .{
                    .q = null,
                    .k_suffix = null,
                    .v_suffix = null,
                    .attention_input = row_attention_input,
                    .fused_qkv_linear_slot = fused_qkv_linear_slot,
                    .kv_manager = maybe_kv_manager,
                    .kv_storage = maybe_kv_storage,
                    .kv_pool_id = kv_pool_id,
                    .skip_kv_write = skip_kv_write,
                    .bootstrap_k_blocks = request.bootstrap_k_blocks,
                    .bootstrap_v_blocks = request.bootstrap_v_blocks,
                    .bootstrap_block_token_counts = request.bootstrap_block_token_counts,
                    .full_k = request.full_k,
                    .full_v = request.full_v,
                    .source_ptr_id = request.source_ptr_id,
                    .sequence_id = request.sequence_id,
                    .layer_index = request.layer_index,
                    .query_sequence_len = 1,
                    .kv_tokens = row_kv_tokens,
                    .num_heads = request.num_heads,
                    .num_kv_heads = request.num_kv_heads,
                    .head_dim = request.head_dim,
                    .key_row_bytes = request.key_row_bytes,
                    .query_position = row_query_position,
                    .kv_position_offset = request.kv_position_offset,
                    .sliding_window = request.sliding_window,
                    .format = request.format,
                    .attention_linear_slot = request.attention_linear_slot,
                    .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
                    .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
                    .residual = row_residual,
                    .hidden_size = request.hidden_size,
                    .eps = request.eps,
                    .ffn_layer_norm_slot = request.ffn_layer_norm_slot,
                    .ffn_rms_norm_slot = request.ffn_rms_norm_slot,
                    .first_ffn_linear_slot = request.first_ffn_linear_slot,
                    .second_ffn_linear_slot = request.second_ffn_linear_slot,
                    .intermediate_size = request.intermediate_size,
                    .activation = request.activation,
                }, stats)) orelse return null;
            };
            if (combined) |current| {
                const merged = try concatenateRows(self, current, row_output);
                var current_owned = current;
                current_owned.deinit();
                row_output.deinit();
                combined = merged;
            } else {
                combined = row_output;
            }
        }
        return combined;
    }

    var q_tensor = maybe_q orelse blk: {
        const attention_input = maybe_attention_input orelse return null;
        const fused_qkv_linear_slot = maybe_fused_qkv_linear_slot orelse return null;
        if (attention_input.ndim() != 2 or @as(usize, @intCast(attention_input.dim(0))) != 1) return null;
        const q_dim = request.num_heads * request.head_dim;
        const kv_dim = request.num_kv_heads * request.head_dim;
        const q_shape = [_]i32{ 1, @intCast(q_dim) };
        const kv_shape = [_]i32{ 1, @intCast(kv_dim) };
        const fused_qkv = (try decoderRuntimeApplyLinear(self, .{
            .slot = fused_qkv_linear_slot,
            .input = attention_input,
            .in_dim = request.hidden_size,
            .out_dim = q_dim + kv_dim * 2,
        })) orelse return null;
        projected_fused_qkv = fused_qkv;
        projected_q = try fused_qkv.retainedView(0, q_dim * @sizeOf(f32), &q_shape);
        projected_k = try fused_qkv.retainedView(q_dim * @sizeOf(f32), kv_dim * @sizeOf(f32), &kv_shape);
        projected_v = try fused_qkv.retainedView((q_dim + kv_dim) * @sizeOf(f32), kv_dim * @sizeOf(f32), &kv_shape);
        break :blk projected_q.?;
    };
    var k_suffix_tensor = maybe_k_suffix orelse projected_k orelse return null;
    var v_suffix_tensor = maybe_v_suffix orelse projected_v orelse return null;
    if (q_tensor.ndim() != 2 or
        k_suffix_tensor.ndim() != 2 or
        v_suffix_tensor.ndim() != 2 or
        request.residual.ndim() != 2)
    {
        return null;
    }
    try maybeWriteCompressedBlockKvSuffix(request, k_suffix_tensor, v_suffix_tensor);

    var gathered_full_owned: ?OwnedFullKv = null;
    defer if (gathered_full_owned) |*full| {
        full.k.deinit();
        full.v.deinit();
    };
    const full_k_for_span: ?MetalTensor = if (request.full_k) |full| full else blk: {
        gathered_full_owned = try maybeGatherCompressedBlockFullKv(request);
        break :blk if (gathered_full_owned) |full| full.k else null;
    };
    const full_v_for_span: ?MetalTensor = if (request.full_v) |full| full else if (gathered_full_owned) |full| full.v else null;

    const base_key_row_bytes = switch (request.format) {
        .polar4 => (@as(usize, request.num_kv_heads) * request.head_dim + 1) / 2,
        .turbo3 => (@as(usize, request.num_kv_heads) * request.head_dim * 3 + 7) / 8,
    };
    const span_prep_started_at = monotonicNowNs();
    const gathered = (try updateGatheredSpan(
        self,
        .{
            .source_ptr_id = request.source_ptr_id,
            .sequence_id = request.sequence_id,
            .layer_index = request.layer_index,
        },
        k_suffix_tensor,
        v_suffix_tensor,
        request.bootstrap_k_blocks,
        request.bootstrap_v_blocks,
        request.bootstrap_block_token_counts,
        full_k_for_span,
        full_v_for_span,
        request.query_sequence_len,
        request.kv_tokens,
        request.kv_position_offset,
        stats,
    )) orelse return null;
    stats.compressed_block_span_prep_nanos += @intCast(monotonicNowNs() - span_prep_started_at);

    var residual_tensor = request.residual;
    const none = std.math.maxInt(usize);
    const v_row_stride: usize = @intCast(gathered.v.dim(1));

    if (q_tensor.isDevice() and residual_tensor.isDevice() and gathered.k.isDevice() and gathered.v.isDevice()) {
        const q_handle = q_tensor.deviceHandle().?;
        const q_offset = q_tensor.deviceByteOffset();
        const k_handle = gathered.k.deviceHandle().?;
        const k_offset = gathered.k.deviceByteOffset();
        const v_handle = gathered.v.deviceHandle().?;
        const v_offset = gathered.v.deviceByteOffset();
        const residual_handle = residual_tensor.deviceHandle().?;
        const residual_offset = residual_tensor.deviceByteOffset();
        const hidden_bytes = request.hidden_size * @sizeOf(f32);
        const out_shape = [_]i32{ 1, @intCast(request.hidden_size) };
        var device_output = MetalTensor.deviceAllocate(
            @ptrCast(runtime),
            hidden_bytes,
            .private,
            &out_shape,
        ) catch return null;
        errdefer device_output.deinit();
        const out_handle = device_output.deviceHandle().?;
        const out_offset = device_output.deviceByteOffset();
        const apply_started_at = monotonicNowNs();
        const rc_device = termite_metal_decode_runtime_apply_attention_dense_block_device_kv_device(
            runtime,
            switch (request.format) {
                .polar4 => 0,
                .turbo3 => 1,
            },
            q_handle,
            q_offset,
            k_handle,
            k_offset,
            v_handle,
            v_offset,
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.key_row_bytes,
            v_row_stride,
            base_key_row_bytes,
            request.query_position,
            request.kv_position_offset,
            request.sliding_window,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot orelse none,
            request.attention_post_linear_rms_norm_slot orelse none,
            residual_handle,
            residual_offset,
            request.num_heads * request.head_dim,
            request.hidden_size,
            request.eps,
            request.ffn_layer_norm_slot orelse none,
            request.ffn_rms_norm_slot orelse none,
            request.first_ffn_linear_slot,
            request.second_ffn_linear_slot,
            request.intermediate_size,
            @intFromEnum(request.activation),
            out_handle,
            out_offset,
        );
        stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - apply_started_at);
        if (rc_device != 0) {
            device_output.deinit();
            return null;
        }
        return device_output;
    }

    var gathered_v = gathered.v;
    const v_ptr = try tensorHostConstPtr(&gathered_v);
    ensureGatheredSpanHostEncoding(
        self,
        gathered,
        request.kv_tokens,
        request.num_kv_heads,
        request.head_dim,
        request.format,
        request.key_row_bytes,
        stats,
    ) catch |err| switch (err) {
        error.UnsupportedKvHeadDim => return null,
        else => return err,
    };
    const encoded = gathered.encoded_key orelse return error.MlxDataNull;
    const q_ptr = try tensorHostConstPtr(&q_tensor);
    const residual_ptr = try tensorHostConstPtr(&residual_tensor);

    const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
    errdefer std.heap.c_allocator.free(output);
    const apply_started_at = monotonicNowNs();
    const rc = termite_metal_decode_runtime_apply_attention_dense_block(
        runtime,
        switch (request.format) {
            .polar4 => 0,
            .turbo3 => 1,
        },
        q_ptr,
        encoded.ptr,
        v_ptr,
        request.kv_tokens,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
        request.key_row_bytes,
        v_row_stride,
        base_key_row_bytes,
        request.query_position,
        request.kv_position_offset,
        request.sliding_window,
        request.attention_linear_slot,
        request.attention_pre_linear_rms_norm_slot orelse none,
        request.attention_post_linear_rms_norm_slot orelse none,
        residual_ptr,
        request.num_heads * request.head_dim,
        request.hidden_size,
        request.eps,
        request.ffn_layer_norm_slot orelse none,
        request.ffn_rms_norm_slot orelse none,
        request.first_ffn_linear_slot,
        request.second_ffn_linear_slot,
        request.intermediate_size,
        @intFromEnum(request.activation),
        output.ptr,
    );
    stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - apply_started_at);
    if (rc != 0) return null;
    const shape = [_]i32{ 1, @intCast(request.hidden_size) };
    return MetalTensor.owned(output, &shape);
}

pub fn runCompressedAttentionGatedDecoderBlockDirect(
    self: anytype,
    request: anytype,
    selected_qkv: *const SelectedCompressedAttentionGatedQkv,
    stats: anytype,
) !?MetalTensor {
    const runtime = self.raw_decode_runtime orelse return null;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
    if (request.attention_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.gate_ffn_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.up_ffn_linear_slot >= decoder_runtime_linear_slot_capacity or
        request.down_ffn_linear_slot >= decoder_runtime_linear_slot_capacity)
    {
        return null;
    }
    if ((selected_qkv.source != .projected_q_only_slot and selected_qkv.q.ndim() != 2) or
        selected_qkv.k_suffix.ndim() != 2 or
        selected_qkv.v_suffix.ndim() != 2 or
        request.residual.ndim() != 2)
    {
        return null;
    }

    const base_key_row_bytes = switch (request.format) {
        .polar4 => (@as(usize, request.num_kv_heads) * request.head_dim + 1) / 2,
        .turbo3 => (@as(usize, request.num_kv_heads) * request.head_dim * 3 + 7) / 8,
    };
    try maybeWriteCompressedBlockKvSuffix(request, selected_qkv.k_suffix, selected_qkv.v_suffix);

    var gathered_full_owned: ?OwnedFullKv = null;
    defer if (gathered_full_owned) |*full| {
        full.k.deinit();
        full.v.deinit();
    };
    const full_k_for_span: ?MetalTensor = if (request.full_k) |full| full else blk: {
        gathered_full_owned = try maybeGatherCompressedBlockFullKv(request);
        break :blk if (gathered_full_owned) |full| full.k else null;
    };
    const full_v_for_span: ?MetalTensor = if (request.full_v) |full| full else if (gathered_full_owned) |full| full.v else null;

    const span_prep_started_at = monotonicNowNs();
    const gathered = (try updateGatheredSpan(
        self,
        .{
            .source_ptr_id = request.source_ptr_id,
            .sequence_id = request.sequence_id,
            .layer_index = request.layer_index,
        },
        selected_qkv.k_suffix,
        selected_qkv.v_suffix,
        request.bootstrap_k_blocks,
        request.bootstrap_v_blocks,
        request.bootstrap_block_token_counts,
        full_k_for_span,
        full_v_for_span,
        request.query_sequence_len,
        request.kv_tokens,
        request.kv_position_offset,
        stats,
    )) orelse return null;
    stats.compressed_block_span_prep_nanos += @intCast(monotonicNowNs() - span_prep_started_at);

    var q_tensor = selected_qkv.q;
    var residual_tensor = request.residual;

    const none = std.math.maxInt(usize);
    const v_row_stride: usize = @intCast(gathered.v.dim(1));

    // Device fast-path: if both q (or attention_input for q_slot source) and
    // residual are already device-resident, invoke the *_device variant so the
    // three host round-trips (q upload, residual upload, output download) all
    // collapse into on-GPU buffer reads.
    if (q_tensor.isDevice() and residual_tensor.isDevice() and gathered.k.isDevice() and gathered.v.isDevice()) {
        const q_handle = q_tensor.deviceHandle().?;
        const q_offset = q_tensor.deviceByteOffset();
        const k_handle = gathered.k.deviceHandle().?;
        const k_offset = gathered.k.deviceByteOffset();
        const v_handle = gathered.v.deviceHandle().?;
        const v_offset = gathered.v.deviceByteOffset();
        const residual_handle = residual_tensor.deviceHandle().?;
        const residual_offset = residual_tensor.deviceByteOffset();
        const hidden_bytes = request.hidden_size * @sizeOf(f32);
        const out_shape = [_]i32{ 1, @intCast(request.hidden_size) };
        var device_output = MetalTensor.deviceAllocate(
            @ptrCast(runtime),
            hidden_bytes,
            .private,
            &out_shape,
        ) catch return null;
        errdefer device_output.deinit();
        const out_handle = device_output.deviceHandle().?;
        const out_offset = device_output.deviceByteOffset();
        var timing: RawAttentionGatedBlockTiming = .{};
        const apply_started_at = monotonicNowNs();
        const rc_device = switch (selected_qkv.source) {
            .projected_q_only_slot => termite_metal_decode_runtime_apply_attention_gated_block_q_slot_device_kv_device(
                runtime,
                switch (request.format) {
                    .polar4 => 0,
                    .turbo3 => 1,
                },
                q_handle,
                q_offset,
                request.q_linear_slot orelse return null,
                k_handle,
                k_offset,
                v_handle,
                v_offset,
                request.kv_tokens,
                request.num_heads,
                request.num_kv_heads,
                request.head_dim,
                request.key_row_bytes,
                v_row_stride,
                base_key_row_bytes,
                request.query_position,
                request.kv_position_offset,
                request.sliding_window,
                request.attention_linear_slot,
                request.attention_pre_linear_rms_norm_slot orelse none,
                request.attention_post_linear_rms_norm_slot orelse none,
                residual_handle,
                residual_offset,
                request.hidden_size,
                request.num_heads * request.head_dim,
                request.eps,
                request.ffn_layer_norm_slot orelse none,
                request.ffn_rms_norm_slot orelse none,
                request.ffn_post_gate_rms_norm_slot orelse none,
                request.gate_ffn_linear_slot,
                request.up_ffn_linear_slot,
                request.down_ffn_linear_slot,
                request.intermediate_size,
                @intFromEnum(request.activation),
                request.layer_index,
                out_handle,
                out_offset,
                &timing,
            ),
            else => termite_metal_decode_runtime_apply_attention_gated_block_device_kv_device(
                runtime,
                switch (request.format) {
                    .polar4 => 0,
                    .turbo3 => 1,
                },
                q_handle,
                q_offset,
                k_handle,
                k_offset,
                v_handle,
                v_offset,
                request.kv_tokens,
                request.num_heads,
                request.num_kv_heads,
                request.head_dim,
                request.key_row_bytes,
                v_row_stride,
                base_key_row_bytes,
                request.query_position,
                request.kv_position_offset,
                request.sliding_window,
                request.attention_linear_slot,
                request.attention_pre_linear_rms_norm_slot orelse none,
                request.attention_post_linear_rms_norm_slot orelse none,
                residual_handle,
                residual_offset,
                request.num_heads * request.head_dim,
                request.hidden_size,
                request.eps,
                request.ffn_layer_norm_slot orelse none,
                request.ffn_rms_norm_slot orelse none,
                request.ffn_post_gate_rms_norm_slot orelse none,
                request.gate_ffn_linear_slot,
                request.up_ffn_linear_slot,
                request.down_ffn_linear_slot,
                request.intermediate_size,
                @intFromEnum(request.activation),
                request.layer_index,
                out_handle,
                out_offset,
                &timing,
            ),
        };
        stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - apply_started_at);
        stats.compressed_block_replace_span_nanos += timing.replace_span_nanos;
        stats.compressed_block_attention_span_nanos += timing.attention_span_nanos;
        stats.compressed_block_attention_prefix_nanos += timing.attention_prefix_nanos;
        stats.compressed_block_gated_ffn_residual_nanos += timing.gated_ffn_residual_nanos;
        stats.compressed_block_command_wait_nanos += timing.command_wait_nanos;
        stats.compressed_block_gpu_nanos += timing.gpu_nanos;
        if (rc_device != 0) {
            stats.compressed_block_gated_direct_runtime_failures += 1;
            switch (timing.failure_stage) {
                1 => stats.compressed_block_gated_direct_fail_replace_span += 1,
                2 => stats.compressed_block_gated_direct_fail_attention_span += 1,
                3 => stats.compressed_block_gated_direct_fail_attention_prefix += 1,
                4 => stats.compressed_block_gated_direct_fail_gated_ffn += 1,
                else => {},
            }
            if (stats.compressed_block_gated_direct_first_failure_code == 0) {
                stats.compressed_block_gated_direct_first_failure_code = timing.failure_code;
            }
            device_output.deinit();
            return null;
        }
        stats.compressed_block_gated_direct_successes += 1;
        return device_output;
    }

    var gathered_v = gathered.v;
    const v_ptr = try tensorHostConstPtr(&gathered_v);
    var gathered_k = gathered.k;
    const k_ptr = try tensorHostConstPtr(&gathered_k);
    const q_ptr = try tensorHostConstPtr(&q_tensor);
    const residual_ptr = try tensorHostConstPtr(&residual_tensor);

    const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
    errdefer std.heap.c_allocator.free(output);
    var timing: RawAttentionGatedBlockTiming = .{};
    const apply_started_at = monotonicNowNs();
    const rc = switch (selected_qkv.source) {
        .projected_q_only_slot => termite_metal_decode_runtime_apply_attention_gated_block_q_slot(
            runtime,
            switch (request.format) {
                .polar4 => 0,
                .turbo3 => 1,
            },
            q_ptr,
            request.q_linear_slot orelse return null,
            k_ptr,
            v_ptr,
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.key_row_bytes,
            v_row_stride,
            base_key_row_bytes,
            request.query_position,
            request.kv_position_offset,
            request.sliding_window,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot orelse none,
            request.attention_post_linear_rms_norm_slot orelse none,
            residual_ptr,
            request.hidden_size,
            request.num_heads * request.head_dim,
            request.eps,
            request.ffn_layer_norm_slot orelse none,
            request.ffn_rms_norm_slot orelse none,
            request.ffn_post_gate_rms_norm_slot orelse none,
            request.gate_ffn_linear_slot,
            request.up_ffn_linear_slot,
            request.down_ffn_linear_slot,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.layer_index,
            output.ptr,
            &timing,
        ),
        else => termite_metal_decode_runtime_apply_attention_gated_block(
            runtime,
            switch (request.format) {
                .polar4 => 0,
                .turbo3 => 1,
            },
            q_ptr,
            k_ptr,
            v_ptr,
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.key_row_bytes,
            @intCast(gathered.v.dim(1)),
            base_key_row_bytes,
            request.query_position,
            request.kv_position_offset,
            request.sliding_window,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot orelse none,
            request.attention_post_linear_rms_norm_slot orelse none,
            residual_ptr,
            request.num_heads * request.head_dim,
            request.hidden_size,
            request.eps,
            request.ffn_layer_norm_slot orelse none,
            request.ffn_rms_norm_slot orelse none,
            request.ffn_post_gate_rms_norm_slot orelse none,
            request.gate_ffn_linear_slot,
            request.up_ffn_linear_slot,
            request.down_ffn_linear_slot,
            request.intermediate_size,
            @intFromEnum(request.activation),
            request.layer_index,
            output.ptr,
            &timing,
        ),
    };
    stats.compressed_block_apply_nanos += @intCast(monotonicNowNs() - apply_started_at);
    stats.compressed_block_replace_span_nanos += timing.replace_span_nanos;
    stats.compressed_block_attention_span_nanos += timing.attention_span_nanos;
    stats.compressed_block_attention_prefix_nanos += timing.attention_prefix_nanos;
    stats.compressed_block_gated_ffn_residual_nanos += timing.gated_ffn_residual_nanos;
    stats.compressed_block_command_wait_nanos += timing.command_wait_nanos;
    stats.compressed_block_gpu_nanos += timing.gpu_nanos;
    if (rc != 0) {
        stats.compressed_block_gated_direct_runtime_failures += 1;
        switch (timing.failure_stage) {
            1 => stats.compressed_block_gated_direct_fail_replace_span += 1,
            2 => stats.compressed_block_gated_direct_fail_attention_span += 1,
            3 => stats.compressed_block_gated_direct_fail_attention_prefix += 1,
            4 => stats.compressed_block_gated_direct_fail_gated_ffn += 1,
            else => {},
        }
        if (stats.compressed_block_gated_direct_first_failure_code == 0) {
            stats.compressed_block_gated_direct_first_failure_code = timing.failure_code;
        }
        return null;
    }
    stats.compressed_block_gated_direct_successes += 1;
    const shape = [_]i32{ 1, @intCast(request.hidden_size) };
    return MetalTensor.owned(output, &shape);
}

pub fn selectCompressedAttentionGatedQkv(
    self: anytype,
    ctx: *anyopaque,
    request: anytype,
    stats: anytype,
    apply_linear_fn: anytype,
    apply_pair_fn: anytype,
) !?SelectedCompressedAttentionGatedQkv {
    if (request.q != null and request.k_suffix != null and request.v_suffix != null) {
        var q = try retainedPlanTensor(request.q.?);
        errdefer q.deinit();
        var k_suffix = try retainedPlanTensor(request.k_suffix.?);
        errdefer k_suffix.deinit();
        const v_suffix = try retainedPlanTensor(request.v_suffix.?);
        return .{
            .source = .provided_qkv,
            .q = q,
            .k_suffix = k_suffix,
            .v_suffix = v_suffix,
        };
    }

    if (request.q == null and request.k_suffix != null and request.v_suffix != null and
        request.attention_input != null and request.q_linear_slot != null)
    {
        if (comptime !build_options.enable_mlx) {
            const project_started_at = monotonicNowNs();
            const q_projected = (try decoderRuntimeApplyLinear(self, .{
                .slot = request.q_linear_slot.?,
                .input = request.attention_input.?,
                .in_dim = request.hidden_size,
                .out_dim = request.num_heads * request.head_dim,
            })) orelse return null;
            errdefer {
                var q_mut = q_projected;
                q_mut.deinit();
            }
            var k_suffix = try retainedPlanTensor(request.k_suffix.?);
            errdefer k_suffix.deinit();
            const v_suffix = try retainedPlanTensor(request.v_suffix.?);
            stats.compressed_block_project_nanos += @intCast(monotonicNowNs() - project_started_at);
            return .{
                .source = .projected_q_only,
                .q = q_projected,
                .k_suffix = k_suffix,
                .v_suffix = v_suffix,
            };
        }
        if (comptime build_options.enable_mlx and backendUsesMlxArray(apply_linear_fn)) {
            const ai_mlx = mlx_metal_bridge.borrowMetalTensorAsMlxArray(request.attention_input.?);
            defer _ = c.mlx_array_free(ai_mlx);
            const project_started_at = monotonicNowNs();
            const q_projected = (try apply_linear_fn(ctx, &.{
                .slot = request.q_linear_slot.?,
                .input = coerceBackendInput(apply_linear_fn, ai_mlx),
                .in_dim = request.hidden_size,
                .out_dim = request.num_heads * request.head_dim,
            })) orelse return null;
            defer _ = c.mlx_array_free(q_projected);
            stats.compressed_block_project_nanos += @intCast(monotonicNowNs() - project_started_at);
            var q_scratch: [metal_tensor.max_dims]i32 = undefined;
            const q_borrowed = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(q_projected, q_scratch[0..]);
            var q_borrowed_mut = q_borrowed;
            const q_owned = try MetalTensor.ownedCloneFrom(try tensorHostSlice(&q_borrowed_mut), q_borrowed.shape());
            errdefer {
                var q_mut = q_owned;
                q_mut.deinit();
            }
            var k_suffix = try retainedPlanTensor(request.k_suffix.?);
            errdefer k_suffix.deinit();
            const v_suffix = try retainedPlanTensor(request.v_suffix.?);
            return .{
                .source = .projected_q_only,
                .q = q_owned,
                .k_suffix = k_suffix,
                .v_suffix = v_suffix,
            };
        }
        return null;
    }

    const attention_input_mt = request.attention_input orelse return null;
    const q_linear_slot = request.q_linear_slot orelse return null;
    const k_linear_slot = request.k_linear_slot orelse return null;
    const v_linear_slot = request.v_linear_slot orelse return null;
    if (comptime !build_options.enable_mlx) {
        const project_started_at = monotonicNowNs();
        const q_projected = (try decoderRuntimeApplyLinear(self, .{
            .slot = q_linear_slot,
            .input = attention_input_mt,
            .in_dim = request.hidden_size,
            .out_dim = request.num_heads * request.head_dim,
        })) orelse return null;
        errdefer {
            var q_mut = q_projected;
            q_mut.deinit();
        }
        const kv_projected = (try decoderRuntimeApplyLinearPair(self, .{
            .slot_a = k_linear_slot,
            .slot_b = v_linear_slot,
            .input = attention_input_mt,
            .in_dim = request.hidden_size,
            .out_dim = request.num_kv_heads * request.head_dim,
        })) orelse {
            var q_mut = q_projected;
            q_mut.deinit();
            return null;
        };
        stats.compressed_block_project_nanos += @intCast(monotonicNowNs() - project_started_at);
        return .{
            .source = .projected_qkv,
            .q = q_projected,
            .k_suffix = kv_projected.first,
            .v_suffix = kv_projected.second,
        };
    }
    if (comptime build_options.enable_mlx and
        backendUsesMlxArray(apply_linear_fn) and
        backendUsesMlxArrayPair(apply_pair_fn))
    {
        const ai_mlx = mlx_metal_bridge.borrowMetalTensorAsMlxArray(attention_input_mt);
        defer _ = c.mlx_array_free(ai_mlx);
        const project_started_at = monotonicNowNs();
        const q_projected = (try apply_linear_fn(ctx, &.{
            .slot = q_linear_slot,
            .input = coerceBackendInput(apply_linear_fn, ai_mlx),
            .in_dim = request.hidden_size,
            .out_dim = request.num_heads * request.head_dim,
        })) orelse return null;
        defer _ = c.mlx_array_free(q_projected);
        const kv_projected = (try apply_pair_fn(ctx, &.{
            .slot_a = k_linear_slot,
            .slot_b = v_linear_slot,
            .input = coerceBackendInput(apply_pair_fn, ai_mlx),
            .in_dim = request.hidden_size,
            .out_dim = request.num_kv_heads * request.head_dim,
        })) orelse return null;
        defer _ = c.mlx_array_free(kv_projected.first);
        defer _ = c.mlx_array_free(kv_projected.second);
        stats.compressed_block_project_nanos += @intCast(monotonicNowNs() - project_started_at);
        var q_scratch: [metal_tensor.max_dims]i32 = undefined;
        var ks_scratch: [metal_tensor.max_dims]i32 = undefined;
        var vs_scratch: [metal_tensor.max_dims]i32 = undefined;
        const q_borrowed = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(q_projected, q_scratch[0..]);
        const k_borrowed = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(kv_projected.first, ks_scratch[0..]);
        const v_borrowed = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(kv_projected.second, vs_scratch[0..]);
        var q_borrowed_mut = q_borrowed;
        const q_owned = try MetalTensor.ownedCloneFrom(try tensorHostSlice(&q_borrowed_mut), q_borrowed.shape());
        errdefer {
            var q_mut = q_owned;
            q_mut.deinit();
        }
        var k_borrowed_mut = k_borrowed;
        const k_owned = try MetalTensor.ownedCloneFrom(try tensorHostSlice(&k_borrowed_mut), k_borrowed.shape());
        errdefer {
            var k_mut = k_owned;
            k_mut.deinit();
        }
        var v_borrowed_mut = v_borrowed;
        const v_owned = try MetalTensor.ownedCloneFrom(try tensorHostSlice(&v_borrowed_mut), v_borrowed.shape());
        return .{
            .source = .projected_qkv,
            .q = q_owned,
            .k_suffix = k_owned,
            .v_suffix = v_owned,
        };
    }
    return null;
}

pub fn tryRawAttentionResidualHost(
    self: anytype,
    attention_input: [*c]const f32,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize,
    attention_post_linear_rms_norm_slot: ?usize,
    output: [*c]f32,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (attention_linear_slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_linear_slots_prepared[attention_linear_slot]) return false;
    if (self.raw_linear_slot_in_dims[attention_linear_slot] != attention_input_size or self.raw_linear_slot_out_dims[attention_linear_slot] != hidden_size) return false;
    const kind = self.raw_linear_slot_kinds[attention_linear_slot];
    if (kind == .none) return false;
    const storage = if (kind == .quantized) self.raw_linear_slot_quantized_storage[attention_linear_slot] else null;
    const none = std.math.maxInt(usize);
    if (storage) |quantized_storage| {
        const source_bytes = quantized_storage.preparedBytes(.row_major_blocks) orelse quantized_storage.raw_bytes;
        const specialized_ok = switch (quantized_storage.tensor_type) {
            .known => |known| switch (known) {
                .TL1 => blk: {
                    const view = try quant_codec.bitnetTL1View(quantized_storage.shape, source_bytes);
                    if (view.cols != attention_input_size or view.rows != hidden_size) break :blk false;
                    break :blk termite_metal_decode_runtime_apply_attention_residual_tl1(
                        runtime,
                        attention_input,
                        attention_pre_linear_rms_norm_slot orelse none,
                        source_bytes.ptr,
                        source_bytes.len,
                        @intCast(view.packed_bytes.len),
                        @intCast(view.config.bm),
                        @intCast(view.config.by),
                        @intCast(view.config.bmm),
                        attention_post_linear_rms_norm_slot orelse none,
                        residual,
                        attention_input_size,
                        hidden_size,
                        eps,
                        output,
                    ) == 0;
                },
                .I2_S => blk: {
                    if (!ensureI2SRuntimeLinearSlotPrepared(self, attention_linear_slot, attention_input_size, hidden_size)) break :blk false;
                    break :blk termite_metal_decode_runtime_apply_attention_residual_i2_s_slot(
                        runtime,
                        attention_input,
                        attention_pre_linear_rms_norm_slot orelse none,
                        attention_linear_slot,
                        attention_post_linear_rms_norm_slot orelse none,
                        residual,
                        attention_input_size,
                        hidden_size,
                        eps,
                        output,
                    ) == 0;
                },
                .Q4_K => blk: {
                    if (attention_input_size % 256 != 0) break :blk false;
                    if (ensureQuantizedRuntimeLinearSlotPrepared(self, attention_linear_slot, attention_input_size, hidden_size) != .q4_k) break :blk false;
                    break :blk termite_metal_decode_runtime_apply_attention_residual_q4_k_slot(
                        runtime,
                        attention_input,
                        attention_pre_linear_rms_norm_slot orelse none,
                        attention_linear_slot,
                        attention_post_linear_rms_norm_slot orelse none,
                        residual,
                        attention_input_size,
                        hidden_size,
                        eps,
                        output,
                    ) == 0;
                },
                .Q5_K => blk: {
                    if (attention_input_size % 256 != 0) break :blk false;
                    if (ensureQuantizedRuntimeLinearSlotPrepared(self, attention_linear_slot, attention_input_size, hidden_size) != .q5_k) break :blk false;
                    break :blk termite_metal_decode_runtime_apply_attention_residual_q5_k_slot(
                        runtime,
                        attention_input,
                        attention_pre_linear_rms_norm_slot orelse none,
                        attention_linear_slot,
                        attention_post_linear_rms_norm_slot orelse none,
                        residual,
                        attention_input_size,
                        hidden_size,
                        eps,
                        output,
                    ) == 0;
                },
                .Q8_0 => blk: {
                    if (comptime !build_options.enable_mlx) break :blk false;
                    if (attention_input_size % 32 != 0) break :blk false;
                    if (ensureQuantizedRuntimeLinearSlotPrepared(self, attention_linear_slot, attention_input_size, hidden_size) != .q8_0) break :blk false;
                    break :blk termite_metal_decode_runtime_apply_attention_residual_q8_0_slot(
                        runtime,
                        attention_input,
                        attention_pre_linear_rms_norm_slot orelse none,
                        attention_linear_slot,
                        attention_post_linear_rms_norm_slot orelse none,
                        residual,
                        attention_input_size,
                        hidden_size,
                        eps,
                        output,
                    ) == 0;
                },
                else => false,
            },
            .bitnet_tl2 => blk: {
                const view = try quant_codec.bitnetTL2View(quantized_storage.shape, source_bytes);
                if (view.cols != attention_input_size or view.rows != hidden_size) break :blk false;
                const scale_off_u64 = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(view.cols), @intCast(view.rows) }) orelse return error.UnsupportedTensorShape) - 32;
                break :blk termite_metal_decode_runtime_apply_attention_residual_tl2(
                    runtime,
                    attention_input,
                    attention_pre_linear_rms_norm_slot orelse none,
                    source_bytes.ptr,
                    source_bytes.len,
                    @intCast(scale_off_u64),
                    @intCast(view.three_values.len),
                    @intCast(view.three_signs.len),
                    @intCast(view.config.bm),
                    @intCast(view.config.by),
                    @intCast(view.config.bmm),
                    @intCast(view.three_cols),
                    @intCast(view.two_cols),
                    attention_post_linear_rms_norm_slot orelse none,
                    residual,
                    attention_input_size,
                    hidden_size,
                    eps,
                    output,
                ) == 0;
            },
            .unknown => false,
        };
        if (specialized_ok) return true;
    }

    var pre_normed_storage: ?[]f32 = null;
    defer if (pre_normed_storage) |buf| std.heap.c_allocator.free(buf);
    const linear_input: [*c]const f32 = if (attention_pre_linear_rms_norm_slot) |norm_slot| blk: {
        if (!self.raw_rms_norm_slots_prepared[norm_slot] or self.raw_rms_norm_slot_hidden_sizes[norm_slot] != attention_input_size) return false;
        var weight_tensor = self.raw_rms_norm_slot_weights[norm_slot] orelse return false;
        const normed = try std.heap.c_allocator.alloc(f32, attention_input_size);
        pre_normed_storage = normed;
        @memcpy(normed, attention_input[0..attention_input_size]);
        activations.rmsNorm(normed, (try tensorHostSlice(&weight_tensor))[0..attention_input_size], attention_input_size, eps);
        break :blk normed.ptr;
    } else attention_input;

    const projected = try std.heap.c_allocator.alloc(f32, hidden_size);
    defer std.heap.c_allocator.free(projected);
    const linear_ok = switch (kind) {
        .dense => try tryRawLinearHost(
            self,
            attention_linear_slot,
            linear_input,
            attention_input_size,
            hidden_size,
            projected.ptr,
        ),
        .quantized => try tryRawProviderQuantizedLinearHost(
            self,
            storage.?,
            linear_input,
            1,
            attention_input_size,
            hidden_size,
            projected.ptr,
        ),
        else => false,
    };
    if (!linear_ok) return false;

    if (attention_post_linear_rms_norm_slot) |norm_slot| {
        if (!self.raw_rms_norm_slots_prepared[norm_slot] or self.raw_rms_norm_slot_hidden_sizes[norm_slot] != hidden_size) return false;
        const weight_tensor = self.raw_rms_norm_slot_weights[norm_slot] orelse return false;
        activations.rmsNorm(projected, (try tensorHostSlice(&weight_tensor))[0..hidden_size], hidden_size, eps);
    }

    for (0..hidden_size) |i| {
        output[i] = projected[i] + residual[i];
    }
    return true;
}

pub fn canTryRawAttentionResidualHost(
    self: anytype,
    attention_linear_slot: usize,
    attention_input_size: usize,
    hidden_size: usize,
) bool {
    if (attention_linear_slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_linear_slots_prepared[attention_linear_slot]) return false;
    if (self.raw_linear_slot_in_dims[attention_linear_slot] != attention_input_size or
        self.raw_linear_slot_out_dims[attention_linear_slot] != hidden_size)
    {
        return false;
    }
    return self.raw_linear_slot_kinds[attention_linear_slot] != .none;
}

pub fn tryRawCompressedAttentionResidualHost(
    self: anytype,
    format: CompressedKeyFormat,
    q: [*c]const f32,
    k: [*c]const f32,
    encoded_key: [*c]const u8,
    v: [*c]const f32,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    v_row_stride: usize,
    base_key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    residual: [*c]const f32,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize,
    attention_post_linear_rms_norm_slot: ?usize,
    layer_index: usize,
    output: [*c]f32,
) !bool {
    const runtime = self.raw_decode_runtime orelse return false;
    if (termite_metal_decode_runtime_ready(runtime) == 0) return false;
    if (attention_linear_slot >= decoder_runtime_linear_slot_capacity) return false;
    if (!self.raw_linear_slots_prepared[attention_linear_slot]) return false;
    if (self.raw_linear_slot_in_dims[attention_linear_slot] != attention_input_size or self.raw_linear_slot_out_dims[attention_linear_slot] != hidden_size) return false;
    if (self.raw_linear_slot_kinds[attention_linear_slot] != .quantized) return false;
    const storage = self.raw_linear_slot_quantized_storage[attention_linear_slot] orelse return false;
    const none = std.math.maxInt(usize);
    const source_bytes = storage.preparedBytes(.row_major_blocks) orelse storage.raw_bytes;
    const format_int: u32 = switch (format) {
        .polar4 => 0,
        .turbo3 => 1,
    };
    return switch (storage.tensor_type) {
        .known => |known| switch (known) {
            .TL1 => blk: {
                const view = try quant_codec.bitnetTL1View(storage.shape, source_bytes);
                if (view.cols != attention_input_size or view.rows != hidden_size) break :blk false;
                break :blk termite_metal_decode_runtime_apply_attention_span_residual_tl1(
                    runtime,
                    format_int,
                    q,
                    encoded_key,
                    v,
                    kv_tokens,
                    num_heads,
                    num_kv_heads,
                    head_dim,
                    key_row_bytes,
                    v_row_stride,
                    base_key_row_bytes,
                    query_position,
                    kv_position_offset,
                    sliding_window,
                    attention_pre_linear_rms_norm_slot orelse none,
                    source_bytes.ptr,
                    source_bytes.len,
                    @intCast(view.packed_bytes.len),
                    @intCast(view.config.bm),
                    @intCast(view.config.by),
                    @intCast(view.config.bmm),
                    attention_post_linear_rms_norm_slot orelse none,
                    residual,
                    attention_input_size,
                    hidden_size,
                    eps,
                    output,
                ) == 0;
            },
            .I2_S => blk: {
                if (!ensureI2SRuntimeLinearSlotPrepared(self, attention_linear_slot, attention_input_size, hidden_size)) break :blk false;
                break :blk termite_metal_decode_runtime_apply_attention_span_residual_i2_s_slot(
                    runtime,
                    format_int,
                    q,
                    encoded_key,
                    v,
                    kv_tokens,
                    num_heads,
                    num_kv_heads,
                    head_dim,
                    key_row_bytes,
                    v_row_stride,
                    base_key_row_bytes,
                    query_position,
                    kv_position_offset,
                    sliding_window,
                    attention_pre_linear_rms_norm_slot orelse none,
                    attention_linear_slot,
                    attention_post_linear_rms_norm_slot orelse none,
                    residual,
                    attention_input_size,
                    hidden_size,
                    eps,
                    output,
                ) == 0;
            },
            .Q8_0 => blk: {
                if (comptime !build_options.enable_mlx) break :blk false;
                if (attention_input_size % 32 != 0) break :blk false;
                if (ensureQuantizedRuntimeLinearSlotPrepared(self, attention_linear_slot, attention_input_size, hidden_size) != .q8_0) break :blk false;
                break :blk termite_metal_decode_runtime_apply_attention_f32_span_residual_q8_0_slot(
                    runtime,
                    format_int,
                    q,
                    k,
                    v,
                    kv_tokens,
                    num_heads,
                    num_kv_heads,
                    head_dim,
                    key_row_bytes,
                    v_row_stride,
                    base_key_row_bytes,
                    query_position,
                    kv_position_offset,
                    sliding_window,
                    attention_pre_linear_rms_norm_slot orelse none,
                    attention_linear_slot,
                    attention_post_linear_rms_norm_slot orelse none,
                    residual,
                    attention_input_size,
                    hidden_size,
                    eps,
                    layer_index,
                    output,
                ) == 0;
            },
            else => false,
        },
        .bitnet_tl2 => blk: {
            const view = try quant_codec.bitnetTL2View(storage.shape, source_bytes);
            if (view.cols != attention_input_size or view.rows != hidden_size) break :blk false;
            const scale_off_u64 = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(view.cols), @intCast(view.rows) }) orelse return error.UnsupportedTensorShape) - 32;
            break :blk termite_metal_decode_runtime_apply_attention_span_residual_tl2(
                runtime,
                format_int,
                q,
                encoded_key,
                v,
                kv_tokens,
                num_heads,
                num_kv_heads,
                head_dim,
                key_row_bytes,
                v_row_stride,
                base_key_row_bytes,
                query_position,
                kv_position_offset,
                sliding_window,
                attention_pre_linear_rms_norm_slot orelse none,
                source_bytes.ptr,
                source_bytes.len,
                @intCast(scale_off_u64),
                @intCast(view.three_values.len),
                @intCast(view.three_signs.len),
                @intCast(view.config.bm),
                @intCast(view.config.by),
                @intCast(view.config.bmm),
                @intCast(view.three_cols),
                @intCast(view.two_cols),
                attention_post_linear_rms_norm_slot orelse none,
                residual,
                attention_input_size,
                hidden_size,
                eps,
                output,
            ) == 0;
        },
        .unknown => false,
    };
}

test "metal native quant support map matches direct runtime slot coverage" {
    const direct_slot_kinds = [_]RawQuantizedRuntimeLinearKind{
        .q1_0,
        .i2_s,
        .i8_s,
        .q2_k,
        .q3_k,
        .q4_0,
        .q4_1,
        .q4_k,
        .q5_0,
        .q5_1,
        .q5_k,
        .q6_k,
        .q8_0,
        .q8_1,
        .q8_k,
        .iq4_nl,
        .iq4_xs,
        .mxfp4,
    };
    for (direct_slot_kinds) |kind| {
        try std.testing.expect(quantizedRuntimeLinearKindHasSingleStageDeviceKernel(kind));
        try std.testing.expect(metalQuantFormatForKind(kind) != .unsupported);
    }

    const non_generic_direct_kinds = [_]RawQuantizedRuntimeLinearKind{
        .none,
        .iq1_s,
        .iq1_m,
        .iq2_xxs,
        .iq2_s,
        .iq3_xxs,
        .iq3_s,
        .tq1_0,
        .tq2_0,
    };
    for (non_generic_direct_kinds) |kind| {
        try std.testing.expect(!quantizedRuntimeLinearKindHasSingleStageDeviceKernel(kind));
    }

    const native_supported = [_]gguf_tensor_types.TensorType{
        .{ .known = .F32 },
        .{ .known = .F16 },
        .{ .known = .BF16 },
        .{ .known = .Q1_0 },
        .{ .known = .I2_S },
        .{ .known = .I8_S },
        .{ .known = .Q2_K },
        .{ .known = .Q3_K },
        .{ .known = .Q4_0 },
        .{ .known = .Q4_1 },
        .{ .known = .Q4_K },
        .{ .known = .Q5_0 },
        .{ .known = .Q5_1 },
        .{ .known = .Q5_K },
        .{ .known = .Q6_K },
        .{ .known = .Q8_0 },
        .{ .known = .Q8_1 },
        .{ .known = .Q8_K },
        .{ .known = .IQ4_NL },
        .{ .known = .IQ4_XS },
        .{ .known = .MXFP4 },
        .{ .known = .NVFP4 },
        .{ .known = .IQ2_XS },
        .{ .known = .TL1 },
        .bitnet_tl2,
    };
    for (native_supported) |tensor_type| {
        try std.testing.expect(isMetalNativeSupported(tensor_type));
    }

    const unsupported = [_]gguf_tensor_types.TensorType{
        .{ .known = .TQ1_0 },
        .{ .unknown = 0xffff },
    };
    for (unsupported) |tensor_type| {
        try std.testing.expect(!isMetalNativeSupported(tensor_type));
    }
}

test "metal native decoderRuntimeApplyLinear q8_0 matches trivial reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const in_dim: usize = 32;
    const out_dim: usize = 2;
    var weight_raw: [68]u8 = [_]u8{0} ** 68;

    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C;
    for (0..32) |i| weight_raw[2 + i] = @bitCast(@as(i8, 1));

    weight_raw[34] = 0x00;
    weight_raw[35] = 0x3C;
    for (0..32) |i| weight_raw[36 + i] = @bitCast(@as(i8, 2));

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{ 0.0, 0.0 };
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    const input_data = [_]f32{1.0} ** in_dim;
    var input = try MetalTensor.ownedCloneFrom(&input_data, &[_]i32{ 1, @intCast(in_dim) });
    defer input.deinit();

    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(@as(usize, out_dim), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, 32.0), actual[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 64.0), actual[1], 1e-4);
}

test "metal native decoderRuntimeApplyLinear nvfp4 matches trivial reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const in_dim: usize = 64;
    const out_dim: usize = 1;
    var weight_raw: [36]u8 = [_]u8{0} ** 36;
    for (0..4) |i| weight_raw[i] = 0x40;
    for (4..36) |i| weight_raw[i] = 0x11;

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .NVFP4 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0};
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    const input_data = [_]f32{1.0} ** in_dim;
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ 1, @intCast(in_dim) });
    defer input.deinit();
    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(@as(usize, out_dim), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, 64.0), actual[0], 1e-4);
}

test "metal native decoderRuntimeApplyLinear iq2_xs matches trivial reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const in_dim: usize = 256;
    const out_dim: usize = 1;
    var weight_raw: [74]u8 = [_]u8{0} ** 74;
    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C;

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .IQ2_XS },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0};
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    const input_data = [_]f32{1.0} ** in_dim;
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ 1, @intCast(in_dim) });
    defer input.deinit();
    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(@as(usize, out_dim), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, 32.0), actual[0], 1e-4);
}

test "metal native decoderRuntimeApplyLinear tl1 matches trivial reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const in_dim: usize = 1536;
    const out_dim: usize = 1536;
    const packed_len = out_dim * in_dim / 4;
    const weight_raw = try std.testing.allocator.alloc(u8, packed_len + 32);
    defer std.testing.allocator.free(weight_raw);
    @memset(weight_raw, 0x88);
    std.mem.writeInt(u32, weight_raw[packed_len..][0..4], @bitCast(@as(f32, 1.0)), .little);

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .TL1 },
        .raw_bytes = weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    var bias_data = [_]f32{0.0} ** out_dim;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    var input_data = [_]f32{1.0} ** in_dim;
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ 1, @intCast(in_dim) });
    defer input.deinit();
    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(@as(usize, out_dim), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(in_dim)), actual[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(in_dim)), actual[out_dim - 1], 1e-3);
}

test "metal native decoderRuntimeApplyLinear tl2 matches trivial reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const in_dim: usize = 1536;
    const out_dim: usize = 1536;
    const raw_len: usize = @intCast(gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(in_dim), @intCast(out_dim) }) orelse return error.SkipZigTest);
    const weight_raw = try std.testing.allocator.alloc(u8, raw_len);
    defer std.testing.allocator.free(weight_raw);
    @memset(weight_raw, 0);
    const view = try quant_codec.bitnetTL2View(&.{ @intCast(out_dim), @intCast(in_dim) }, weight_raw);
    @memset(weight_raw[0..view.three_values.len], 0xdd);
    const scale_off = raw_len - 32;
    std.mem.writeInt(u32, weight_raw[scale_off..][0..4], @bitCast(@as(f32, 1.0)), .little);

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage = QuantizedStorage{
        .tensor_type = .bitnet_tl2,
        .raw_bytes = weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    var bias_data = [_]f32{0.0} ** out_dim;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    var input_data = [_]f32{1.0} ** in_dim;
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ 1, @intCast(in_dim) });
    defer input.deinit();
    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(@as(usize, out_dim), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(in_dim)), actual[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(in_dim)), actual[out_dim - 1], 1e-3);
}

test "metal native quant row ops q8_0 linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 32;
    const source_rows: usize = 3;
    const row_bytes: usize = 34;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    for (0..source_rows) |row| {
        const base = row * row_bytes;
        weight_raw[base + 0] = 0x00;
        weight_raw[base + 1] = 0x3C;
        for (0..dim) |col| {
            const signed = @as(i16, @intCast(row * 5 + col % 7)) - 3;
            weight_raw[base + 2 + col] = @bitCast(@as(i8, @intCast(signed)));
        }
    }

    const shape = [_]i64{ 1, @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    const ids = [_]u32{ 2, 0 };
    var gathered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        0,
        &ids,
        dim,
        source_rows,
        0.5,
    )) orelse return error.UnexpectedNull;
    defer gathered.deinit();

    var gathered_mut = gathered;
    const gathered_host = try tensorHostSlice(&gathered_mut);
    try std.testing.expectEqual(@as(usize, ids.len * dim), gathered_host.len);
    for (ids, 0..) |row_id, out_row| {
        for (0..dim) |col| {
            const raw: i8 = @bitCast(weight_raw[@as(usize, row_id) * row_bytes + 2 + col]);
            const expected = @as(f32, @floatFromInt(raw)) * 0.5;
            try std.testing.expectApproxEqAbs(expected, gathered_host[out_row * dim + col], 1e-5);
        }
    }
    const mode_after_read = provider.raw_linear_slot_runtime_prepared_modes[0];

    var copied = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        0,
        1,
        2,
        dim,
        source_rows,
        2.0,
    )) orelse return error.UnexpectedNull;
    defer copied.deinit();

    var copied_mut = copied;
    const copied_host = try tensorHostSlice(&copied_mut);
    try std.testing.expectEqual(@as(usize, 2 * dim), copied_host.len);
    for (0..2) |out_row| {
        const row_id = out_row + 1;
        for (0..dim) |col| {
            const raw: i8 = @bitCast(weight_raw[row_id * row_bytes + 2 + col]);
            const expected = @as(f32, @floatFromInt(raw)) * 2.0;
            try std.testing.expectApproxEqAbs(expected, copied_host[out_row * dim + col], 1e-5);
        }
    }

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 5) % 31)) - 15;
        value.* = @as(f32, @floatFromInt(signed)) / 16.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        0,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));
    if (mode_after_read == .mapped_shared) {
        try std.testing.expectEqual(@as(RawQuantizedRuntimeLinearStorageMode, .private_upload), provider.raw_linear_slot_runtime_prepared_modes[0]);
    }
    try std.testing.expectEqual(@as(RawQuantizedRuntimeLinearStorageMode, .private_upload), provider.raw_linear_slot_runtime_prepared_modes[0]);

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        0,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    try std.testing.expectEqual(@as(usize, rewrite_data.len), rewritten_host.len);
    for (rewrite_data, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.008);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 9) % 37)) - 18;
        value.* = @as(f32, @floatFromInt(signed)) / 20.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        0,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        0,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    try std.testing.expectEqual(@as(usize, scatter_data.len), scattered_host.len);
    for (scatter_data, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.008);
    }
}

test "metal native quant row ops q4_0 writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 32;
    const source_rows: usize = 3;
    const row_bytes: usize = 18;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 11 + col * 5) % 29)) - 14;
            value.* = @as(f32, @floatFromInt(signed)) / 17.0;
        }
        quant_codec.quantizeQ4_0Block(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 1,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 7) % 41)) - 20;
        value.* = @as(f32, @floatFromInt(signed)) / 23.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        1,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        1,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    var rewrite_expected: [2 * dim]f32 = undefined;
    for (0..2) |row| {
        var row_quant: [row_bytes]u8 = undefined;
        const start = row * dim;
        quant_codec.quantizeQ4_0Block(rewrite_data[start..][0..dim], &row_quant);
        try quant_codec.dequantizeToFloat32(.{ .known = .Q4_0 }, &row_quant, rewrite_expected[start..][0..dim]);
    }
    for (rewrite_expected, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.008);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 13) % 43)) - 21;
        value.* = @as(f32, @floatFromInt(signed)) / 25.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        1,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        1,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    var scatter_expected: [2 * dim]f32 = undefined;
    for (0..2) |row| {
        var row_quant: [row_bytes]u8 = undefined;
        const start = row * dim;
        quant_codec.quantizeQ4_0Block(scatter_data[start..][0..dim], &row_quant);
        try quant_codec.dequantizeToFloat32(.{ .known = .Q4_0 }, &row_quant, scatter_expected[start..][0..dim]);
    }
    for (scatter_expected, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.008);
    }
}

test "metal native quant row ops q5_0 writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 32;
    const source_rows: usize = 3;
    const row_bytes: usize = 22;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 13 + col * 7) % 37)) - 18;
            value.* = @as(f32, @floatFromInt(signed)) / 19.0;
        }
        quant_codec.quantizeQ5_0Block(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q5_0 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 2,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 11) % 47)) - 23;
        value.* = @as(f32, @floatFromInt(signed)) / 29.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        2,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        2,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    var rewrite_expected: [2 * dim]f32 = undefined;
    for (0..2) |row| {
        var row_quant: [row_bytes]u8 = undefined;
        const start = row * dim;
        quant_codec.quantizeQ5_0Block(rewrite_data[start..][0..dim], &row_quant);
        try quant_codec.dequantizeToFloat32(.{ .known = .Q5_0 }, &row_quant, rewrite_expected[start..][0..dim]);
    }
    for (rewrite_expected, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.008);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 17) % 53)) - 26;
        value.* = @as(f32, @floatFromInt(signed)) / 31.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        2,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        2,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    var scatter_expected: [2 * dim]f32 = undefined;
    for (0..2) |row| {
        var row_quant: [row_bytes]u8 = undefined;
        const start = row * dim;
        quant_codec.quantizeQ5_0Block(scatter_data[start..][0..dim], &row_quant);
        try quant_codec.dequantizeToFloat32(.{ .known = .Q5_0 }, &row_quant, scatter_expected[start..][0..dim]);
    }
    for (scatter_expected, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.008);
    }
}

test "metal native quant row ops q4_1 writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 32;
    const source_rows: usize = 3;
    const row_bytes: usize = 20;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 17 + col * 7) % 41)) - 13;
            value.* = @as(f32, @floatFromInt(signed)) / 21.0;
        }
        quant_codec.quantizeQ4_1Block(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_1 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 3,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 13) % 47)) - 19;
        value.* = @as(f32, @floatFromInt(signed)) / 27.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        3,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        3,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    for (rewrite_data, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.07);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 19) % 53)) - 11;
        value.* = @as(f32, @floatFromInt(signed)) / 25.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        3,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        3,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    for (scatter_data, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.07);
    }
}

test "metal native quant row ops q5_1 writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 32;
    const source_rows: usize = 3;
    const row_bytes: usize = 24;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 19 + col * 11) % 59)) - 17;
            value.* = @as(f32, @floatFromInt(signed)) / 31.0;
        }
        quant_codec.quantizeQ5_1Block(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q5_1 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 4,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 17) % 61)) - 23;
        value.* = @as(f32, @floatFromInt(signed)) / 37.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        4,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        4,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    for (rewrite_data, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.04);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 23) % 67)) - 29;
        value.* = @as(f32, @floatFromInt(signed)) / 41.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        4,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        4,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    for (scatter_data, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.04);
    }
}

test "metal native quant row ops q8_1 writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 32;
    const source_rows: usize = 3;
    const row_bytes: usize = 36;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 23 + col * 13) % 71)) - 35;
            value.* = @as(f32, @floatFromInt(signed)) / 43.0;
        }
        quant_codec.quantizeQ8_1Block(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_1 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 5,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 29) % 73)) - 31;
        value.* = @as(f32, @floatFromInt(signed)) / 47.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        5,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        5,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    for (rewrite_data, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.01);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 31) % 79)) - 37;
        value.* = @as(f32, @floatFromInt(signed)) / 53.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        5,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        5,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    for (scatter_data, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.01);
    }
}

test "metal native quant row ops q6_k writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 256;
    const source_rows: usize = 3;
    const row_bytes: usize = 210;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 29 + col * 17) % 97)) - 48;
            value.* = @as(f32, @floatFromInt(signed)) / 61.0;
        }
        quant_codec.quantizeQ6_KBlock(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q6_K },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 6,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 31) % 101)) - 50;
        value.* = @as(f32, @floatFromInt(signed)) / 67.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        6,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        6,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    for (rewrite_data, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.04);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 37) % 103)) - 51;
        value.* = @as(f32, @floatFromInt(signed)) / 71.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        6,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        6,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    for (scatter_data, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.04);
    }
}

test "metal native quant row ops q4_k writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 256;
    const source_rows: usize = 3;
    const row_bytes: usize = 144;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 31 + col * 19) % 109)) - 54;
            value.* = @as(f32, @floatFromInt(signed)) / 73.0;
        }
        quant_codec.quantizeQ4_KBlock(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_K },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 7,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 41) % 113)) - 56;
        value.* = @as(f32, @floatFromInt(signed)) / 79.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        7,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        7,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    for (rewrite_data, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.09);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 43) % 127)) - 63;
        value.* = @as(f32, @floatFromInt(signed)) / 83.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        7,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        7,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    for (scatter_data, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.09);
    }
}

test "metal native quant row ops q5_k writeback linear slot match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 256;
    const source_rows: usize = 3;
    const row_bytes: usize = 176;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    var seed_dense: [dim]f32 = undefined;
    for (0..source_rows) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 37 + col * 23) % 131)) - 65;
            value.* = @as(f32, @floatFromInt(signed)) / 89.0;
        }
        quant_codec.quantizeQ5_KBlock(&seed_dense, weight_raw[row * row_bytes ..][0..row_bytes]);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q5_K },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** source_rows;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(source_rows)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 8,
        .in_dim = dim,
        .out_dim = source_rows,
        .retain_dense_fallback = false,
    }, &stats));

    var rewrite_data: [2 * dim]f32 = undefined;
    for (&rewrite_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 47) % 137)) - 68;
        value.* = @as(f32, @floatFromInt(signed)) / 97.0;
    }
    var rewrite = try testDeviceTensorFromSlice(runtime, &rewrite_data, &[_]i32{ 2, @intCast(dim) });
    defer rewrite.deinit();
    try std.testing.expect(try decoderRuntimeCopyF32ToQuantLinearSlot(
        &provider,
        8,
        1,
        2,
        dim,
        source_rows,
        1.0,
        rewrite,
    ));

    var rewritten = (try decoderRuntimeCopyQuantLinearSlotToF32(
        &provider,
        8,
        1,
        2,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer rewritten.deinit();
    var rewritten_mut = rewritten;
    const rewritten_host = try tensorHostSlice(&rewritten_mut);
    for (rewrite_data, rewritten_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.05);
    }

    var scatter_data: [2 * dim]f32 = undefined;
    for (&scatter_data, 0..) |*value, idx| {
        const signed = @as(i32, @intCast((idx * 53) % 139)) - 69;
        value.* = @as(f32, @floatFromInt(signed)) / 101.0;
    }
    var scatter = try testDeviceTensorFromSlice(runtime, &scatter_data, &[_]i32{ 2, @intCast(dim) });
    defer scatter.deinit();
    const scatter_ids = [_]u32{ 2, 0 };
    try std.testing.expect(try decoderRuntimeSetRowsQuantLinearSlot(
        &provider,
        8,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
        scatter,
    ));

    var scattered = (try decoderRuntimeGetRowsQuantLinearSlot(
        &provider,
        8,
        &scatter_ids,
        dim,
        source_rows,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer scattered.deinit();
    var scattered_mut = scattered;
    const scattered_host = try tensorHostSlice(&scattered_mut);
    for (scatter_data, scattered_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.05);
    }
}

test "metal native quant embedding lookup q4_0 uses generic row kernel" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 32;
    const source_rows: usize = 2;
    const row_bytes: usize = 18;
    var weight_raw: [source_rows * row_bytes]u8 = [_]u8{0} ** (source_rows * row_bytes);
    for (0..source_rows) |row| {
        const base = row * row_bytes;
        weight_raw[base + 0] = 0x00;
        weight_raw[base + 1] = 0x3C;
        const lo: u8 = @intCast(8 + 1 + row * 2);
        const hi: u8 = @intCast(8 + 2 + row * 2);
        for (0..16) |i| weight_raw[base + 2 + i] = lo | (hi << 4);
    }

    const shape = [_]i64{ @intCast(source_rows), @intCast(dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    var looked_up = (try decoderRuntimeQuantEmbeddingLookup(
        &provider,
        &storage,
        &[_]i64{ 1, 0 },
        2,
        dim,
        1.0,
    )) orelse return error.UnexpectedNull;
    defer looked_up.deinit();

    var looked_up_mut = looked_up;
    const out = try tensorHostSlice(&looked_up_mut);
    try std.testing.expectEqual(@as(usize, 2 * dim), out.len);
    for (0..dim) |col| {
        const expected_row_1: f32 = if (col < 16) 3.0 else 4.0;
        const expected_row_0: f32 = if (col < 16) 1.0 else 2.0;
        try std.testing.expectApproxEqAbs(expected_row_1, out[col], 1e-5);
        try std.testing.expectApproxEqAbs(expected_row_0, out[dim + col], 1e-5);
    }

    const ids_u32 = [_]u32{ 1, 0 };
    const out_shape = [_]i32{ @intCast(ids_u32.len), @intCast(dim) };
    var frame_out = try MetalTensor.deviceAllocate(runtime, ids_u32.len * dim * @sizeOf(f32), .private, &out_shape);
    defer frame_out.deinit();

    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_prepare_quant_embedding_table(
        runtime,
        @intFromEnum(MetalQuantFormat.q4_0),
        &weight_raw,
        weight_raw.len,
        source_rows,
        dim,
    ));
    try beginFrame(runtime);
    errdefer if (hasActiveFrame(runtime)) cancelFrame(runtime) catch {};
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_quant_embedding_lookup_prepared_device(
        runtime,
        @intFromEnum(MetalQuantFormat.q4_0),
        &ids_u32,
        ids_u32.len,
        dim,
        1.0,
        frame_out.deviceHandle(),
        frame_out.deviceByteOffset(),
    ));
    try submitFrame(runtime);
    try waitFrame(runtime);

    var frame_out_mut = frame_out;
    const frame_host = try tensorHostSlice(&frame_out_mut);
    try std.testing.expectEqual(out.len, frame_host.len);
    for (out, frame_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
    }
}

test "metal native decoderRuntimeApplyLinear q8_0 device rows match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 10;
    const in_dim: usize = 2048;
    const out_dim: usize = 257;
    const row_blocks = in_dim / 32;
    const block_bytes = 34;
    var weight_raw: [out_dim * row_blocks * block_bytes]u8 = [_]u8{0} ** (out_dim * row_blocks * block_bytes);

    for (0..out_dim) |o| {
        for (0..row_blocks) |b| {
            const base = (o * row_blocks + b) * block_bytes;
            weight_raw[base + 0] = 0x00; // f16 1.0
            weight_raw[base + 1] = 0x3C;
            for (0..32) |i| {
                const q = @as(i8, @intCast(@as(i16, @intCast(((o * 17 + b * 7 + i * 3) % 23))) - 11));
                weight_raw[base + 2 + i] = @bitCast(q);
            }
        }
    }

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    var bias_data: [out_dim]f32 = undefined;
    for (&bias_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 7) % 17)) - 8;
        value.* = @as(f32, @floatFromInt(signed)) / 31.0;
    }
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    var input_data: [rows * in_dim]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 29)) - 14;
        value.* = @as(f32, @floatFromInt(signed)) * 0.03125;
    }
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer input.deinit();

    const before_dispatch = runtimeMemorySnapshot(runtime);
    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();
    const after_dispatch = runtimeMemorySnapshot(runtime);
    // C enum mirror: TERMITE_METAL_Q8_0_LINEAR_FAMILY_NONE. The selected
    // dispatch can fall back when the preferred MM pipeline is unavailable.
    try std.testing.expect(after_dispatch.q8_0_linear_family_dispatch_counts[0][0] +
        after_dispatch.q8_0_linear_family_dispatch_counts[0][1] +
        after_dispatch.q8_0_linear_family_dispatch_counts[0][2] +
        after_dispatch.q8_0_linear_family_dispatch_counts[0][3] >
        before_dispatch.q8_0_linear_family_dispatch_counts[0][0] +
            before_dispatch.q8_0_linear_family_dispatch_counts[0][1] +
            before_dispatch.q8_0_linear_family_dispatch_counts[0][2] +
            before_dispatch.q8_0_linear_family_dispatch_counts[0][3]);
    try std.testing.expect(after_dispatch.q8_0_linear_rows_9_64 > before_dispatch.q8_0_linear_rows_9_64);
    try std.testing.expect(output.isDevice());

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(rows * out_dim, actual.len);

    for (0..rows) |r| {
        for (0..out_dim) |o| {
            var expected: f32 = 0.0;
            for (0..row_blocks) |b| {
                const base = (o * row_blocks + b) * block_bytes;
                for (0..32) |i| {
                    const q: i8 = @bitCast(weight_raw[base + 2 + i]);
                    expected += input_data[r * in_dim + b * 32 + i] * @as(f32, @floatFromInt(q));
                }
            }
            expected += bias_data[o];
            try std.testing.expectApproxEqAbs(expected, actual[r * out_dim + o], 1e-4);
        }
    }
}

test "metal native decoderRuntimeApplyLinear q4_k device rows match reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 9;
    const in_dim: usize = 2048;
    const out_dim: usize = 23;
    const q4k_values_per_block: usize = 256;
    const q4k_bytes_per_block: usize = 144;
    const row_bytes: usize = (in_dim / q4k_values_per_block) * q4k_bytes_per_block;
    var weight_raw: [out_dim * row_bytes]u8 = [_]u8{0} ** (out_dim * row_bytes);
    var prepared_poison: [out_dim * row_bytes]u8 = [_]u8{0xA5} ** (out_dim * row_bytes);
    var seed_dense: [in_dim]f32 = undefined;
    for (0..out_dim) |row| {
        for (&seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 31 + col * 19) % 109)) - 54;
            value.* = @as(f32, @floatFromInt(signed)) / 73.0;
        }
        for (0..in_dim / q4k_values_per_block) |block| {
            quant_codec.quantizeQ4_KBlock(
                seed_dense[block * q4k_values_per_block ..][0..q4k_values_per_block],
                weight_raw[row * row_bytes + block * q4k_bytes_per_block ..][0..q4k_bytes_per_block],
            );
        }
    }

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    var storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_K },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    const prepared_poison_owned = try std.testing.allocator.dupe(u8, &prepared_poison);
    storage.setPreparedBytes(.row_major_blocks, prepared_poison_owned, 0, 0);
    defer storage.prepared.deinit(std.testing.allocator);

    const bias_data = [_]f32{0.0} ** out_dim;
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    var input_data: [rows * in_dim]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 13) % 127)) - 63;
        value.* = @as(f32, @floatFromInt(signed)) / 97.0;
    }
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer input.deinit();

    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var weight_host: [out_dim * in_dim]f32 = undefined;
    try quant_codec.dequantizeToFloat32(.{ .known = .Q4_K }, &weight_raw, &weight_host);
    var expected: [rows * out_dim]f32 = undefined;
    native.sgemmTransBSync(rows, out_dim, in_dim, 1.0, &input_data, &weight_host, 0.0, &expected);
    for (0..rows) |row| {
        for (0..out_dim) |col| expected[row * out_dim + col] += bias_data[col];
    }

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |exp, got, i| {
        if (!std.math.approxEqAbs(f32, exp, got, 2e-3)) {
            std.debug.print("q4_k device linear mismatch idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
    }
}

test "metal native quick gelu feeding q4_k linear matches reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 77;
    const in_dim: usize = 2048;
    const out_dim: usize = 512;
    const q4k_values_per_block: usize = 256;
    const q4k_bytes_per_block: usize = 144;
    const row_bytes: usize = (in_dim / q4k_values_per_block) * q4k_bytes_per_block;
    var weight_raw = try std.testing.allocator.alloc(u8, out_dim * row_bytes);
    defer std.testing.allocator.free(weight_raw);
    var seed_dense = try std.testing.allocator.alloc(f32, in_dim);
    defer std.testing.allocator.free(seed_dense);
    for (0..out_dim) |row| {
        for (seed_dense, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 17 + col * 29) % 127)) - 63;
            value.* = @as(f32, @floatFromInt(signed)) / 211.0;
        }
        for (0..in_dim / q4k_values_per_block) |block| {
            quant_codec.quantizeQ4_KBlock(
                seed_dense[block * q4k_values_per_block ..][0..q4k_values_per_block],
                weight_raw[row * row_bytes + block * q4k_bytes_per_block ..][0..q4k_bytes_per_block],
            );
        }
    }

    const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_K },
        .raw_bytes = weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = try std.testing.allocator.alloc(f32, out_dim);
    defer std.testing.allocator.free(bias_data);
    for (bias_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 11) % 23)) - 11;
        value.* = @as(f32, @floatFromInt(signed)) / 47.0;
    }
    var bias = try MetalTensor.ownedCloneFrom(bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    const input_data = try std.testing.allocator.alloc(f32, rows * in_dim);
    defer std.testing.allocator.free(input_data);
    for (input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 23) % 257)) - 128;
        value.* = @as(f32, @floatFromInt(signed)) / 19.0;
    }
    var input = try testDeviceTensorFromSlice(runtime, input_data, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer input.deinit();

    var activated = (try decoderRuntimeApplyActivation(&provider, .{
        .input = input,
        .kind = @as(ops.DecoderRuntimeActivationKind, .quick_gelu),
        .dim = in_dim,
    }, &stats)) orelse return error.UnexpectedNull;
    defer activated.deinit();
    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = activated,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    const expected_input = try std.testing.allocator.dupe(f32, input_data);
    defer std.testing.allocator.free(expected_input);
    applyActivationHost(expected_input, .quick_gelu);
    const weight_host = try std.testing.allocator.alloc(f32, out_dim * in_dim);
    defer std.testing.allocator.free(weight_host);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q4_K }, weight_raw, weight_host);
    const expected = try std.testing.allocator.alloc(f32, rows * out_dim);
    defer std.testing.allocator.free(expected);
    native.sgemmTransBSync(rows, out_dim, in_dim, 1.0, expected_input, weight_host, 0.0, expected);
    for (0..rows) |row| {
        for (0..out_dim) |col| expected[row * out_dim + col] += bias_data[col];
    }

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |exp, got, i| {
        if (!std.math.approxEqAbs(f32, exp, got, 5e-3)) {
            std.debug.print("quick_gelu q4_k chain mismatch idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
    }
}

test "metal native real clip q4_k ffn linear matches reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const model_path = std.c.getenv("TERMITE_TEST_CLIP_GGUF") orelse "/Users/ajroetker/.termite/models/antflydb/clipclap/clipclap-clip.Q4_K.gguf";

    const allocator = std.testing.allocator;
    const store_impl = tensor_store_mod.GgufStore.initAbsolute(allocator, std.mem.span(model_path)) catch return error.SkipZigTest;
    var store = store_impl.tensorStore();
    defer store.deinit();

    var tensor_ref = try store.describeTensor(allocator, "text_model.encoder.layers.0.mlp.fc2.weight");
    defer tensor_ref.deinit(allocator);
    var loaded = try store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    const storage = &(loaded.quantized_storage orelse return error.SkipZigTest);
    if (storage.tensor_type != .known or storage.tensor_type.known != .Q4_K) return error.SkipZigTest;
    if (storage.shape.len != 2) return error.SkipZigTest;
    const out_dim: usize = @intCast(storage.shape[0]);
    const in_dim: usize = @intCast(storage.shape[1]);
    const rows: usize = 77;
    if (in_dim == 0 or out_dim == 0) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const bias_data = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_data);
    @memset(bias_data, 0.0);
    var bias = try MetalTensor.ownedCloneFrom(bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, storage),
        .slot = 0,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    const input_data = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input_data);
    for (input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 23) % 257)) - 128;
        value.* = @as(f32, @floatFromInt(signed)) / 19.0;
    }
    var input = try testDeviceTensorFromSlice(runtime, input_data, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer input.deinit();
    var activated = (try decoderRuntimeApplyActivation(&provider, .{
        .input = input,
        .kind = @as(ops.DecoderRuntimeActivationKind, .quick_gelu),
        .dim = in_dim,
    }, &stats)) orelse return error.UnexpectedNull;
    defer activated.deinit();
    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = activated,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    const expected_input = try allocator.dupe(f32, input_data);
    defer allocator.free(expected_input);
    applyActivationHost(expected_input, .quick_gelu);
    const weight_host = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight_host);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q4_K }, storage.raw_bytes, weight_host);
    const expected = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(expected);
    native.sgemmTransBSync(rows, out_dim, in_dim, 1.0, expected_input, weight_host, 0.0, expected);

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |exp, got, i| {
        if (!std.math.approxEqAbs(f32, exp, got, 5e-3)) {
            std.debug.print("real clip q4_k chain mismatch idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
    }
}

test "metal native decoderRuntimeApplyLinearPair q8_0 device rows use mm dispatch" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 10;
    const in_dim: usize = 64;
    const out_dim: usize = 17;
    const row_blocks = in_dim / 32;
    const block_bytes = 34;
    var weight_a_raw: [out_dim * row_blocks * block_bytes]u8 = [_]u8{0} ** (out_dim * row_blocks * block_bytes);
    var weight_b_raw: [out_dim * row_blocks * block_bytes]u8 = [_]u8{0} ** (out_dim * row_blocks * block_bytes);

    const fillQ80 = struct {
        fn run(bytes: []u8, seed: usize, out: usize, blocks: usize) void {
            for (0..out) |o| {
                for (0..blocks) |b| {
                    const base = (o * blocks + b) * 34;
                    bytes[base + 0] = 0x00;
                    bytes[base + 1] = 0x3C;
                    for (0..32) |i| {
                        const raw = @as(i16, @intCast((o * 11 + b * 7 + i * 5 + seed) % 19)) - 9;
                        bytes[base + 2 + i] = @bitCast(@as(i8, @intCast(raw)));
                    }
                }
            }
        }
    }.run;
    fillQ80(&weight_a_raw, 3, out_dim, row_blocks);
    fillQ80(&weight_b_raw, 13, out_dim, row_blocks);

    const shape_a = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage_a = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = &weight_a_raw,
        .shape = &shape_a,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    const shape_b = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const storage_b = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = &weight_b_raw,
        .shape = &shape_b,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const bias_data = [_]f32{0.0} ** out_dim;
    var bias_a = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias_a.deinit();
    var bias_b = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias_b.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias_a,
        .quantized_storage = @as(?*const QuantizedStorage, &storage_a),
        .slot = 20,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias_b,
        .quantized_storage = @as(?*const QuantizedStorage, &storage_b),
        .slot = 21,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    var input_data: [rows * in_dim]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 7) % 37)) - 18;
        value.* = @as(f32, @floatFromInt(signed)) * 0.03125;
    }
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer input.deinit();

    const before_pair_dispatch = runtimeMemorySnapshot(runtime);
    var pair = (try decoderRuntimeApplyLinearPair(&provider, .{
        .slot_a = 20,
        .slot_b = 21,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer pair.first.deinit();
    defer pair.second.deinit();
    const after_pair_dispatch = runtimeMemorySnapshot(runtime);
    // C enum mirror: TERMITE_METAL_Q8_0_LINEAR_FAMILY_PAIR. The selected
    // dispatch can fall back when the preferred MM pipeline is unavailable.
    try std.testing.expect(after_pair_dispatch.q8_0_linear_family_dispatch_counts[4][0] +
        after_pair_dispatch.q8_0_linear_family_dispatch_counts[4][1] +
        after_pair_dispatch.q8_0_linear_family_dispatch_counts[4][2] +
        after_pair_dispatch.q8_0_linear_family_dispatch_counts[4][3] >
        before_pair_dispatch.q8_0_linear_family_dispatch_counts[4][0] +
            before_pair_dispatch.q8_0_linear_family_dispatch_counts[4][1] +
            before_pair_dispatch.q8_0_linear_family_dispatch_counts[4][2] +
            before_pair_dispatch.q8_0_linear_family_dispatch_counts[4][3]);
    try std.testing.expect(after_pair_dispatch.q8_0_linear_rows_9_64 > before_pair_dispatch.q8_0_linear_rows_9_64);
    try std.testing.expect(pair.first.isDevice());
    try std.testing.expect(pair.second.isDevice());

    var first_expected = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 20,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer first_expected.deinit();
    var second_expected = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 21,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer second_expected.deinit();

    const pairs = [_]struct { expected: *MetalTensor, actual: *MetalTensor }{
        .{ .expected = &first_expected, .actual = &pair.first },
        .{ .expected = &second_expected, .actual = &pair.second },
    };
    for (pairs) |entry| {
        const expected = try tensorHostSlice(entry.expected);
        const actual = try tensorHostSlice(entry.actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |exp, got| {
            try std.testing.expectApproxEqAbs(exp, got, 1e-4);
        }
    }
}

test "metal native activation device rows match host" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 7;
    const dim: usize = 37;
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    const kinds = [_]ops.DecoderRuntimeActivationKind{ .gelu, .gelu_new, .silu, .relu, .quick_gelu, .relu_squared };

    var input_data: [rows * dim]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const pattern = [_]f32{ -200.0, -150.0, -50.0, -12.5, -1.0, -0.125, 0.0, 0.125, 1.0, 12.5, 50.0, 150.0, 200.0 };
        value.* = pattern[i % pattern.len] + @as(f32, @floatFromInt(i % 5)) * 0.03125;
    }

    for (kinds) |kind| {
        var input = try testDeviceTensorFromSlice(runtime, &input_data, &shape);
        defer input.deinit();
        var output = try MetalTensor.deviceAllocate(runtime, input_data.len * @sizeOf(f32), .private, &shape);
        defer output.deinit();

        const rc = termite_metal_decode_runtime_apply_activation_device(
            runtime,
            @intFromEnum(kind),
            input.deviceHandle(),
            input.deviceByteOffset(),
            rows,
            dim,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (rc != 0) {
            std.debug.print("activation rows device rc={d} kind={s}\n", .{ rc, @tagName(kind) });
            return error.TestUnexpectedResult;
        }

        var expected = input_data;
        applyActivationHost(&expected, kind);
        var output_mut = output;
        const actual = try tensorHostSlice(&output_mut);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual, 0..) |exp, got, i| {
            if (!std.math.isFinite(got)) {
                std.debug.print("activation rows nonfinite kind={s} idx={d} expected={d} got={d}\n", .{ @tagName(kind), i, exp, got });
                return error.TestUnexpectedResult;
            }
            if (!std.math.approxEqAbs(f32, exp, got, 2e-4)) {
                std.debug.print("activation rows mismatch kind={s} idx={d} expected={d} got={d}\n", .{ @tagName(kind), i, exp, got });
                return error.TestUnexpectedResult;
            }
        }
    }
}

test "metal native layer norm device rows match host" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 77;
    const hidden: usize = 512;
    const shape = [_]i32{ @intCast(rows), @intCast(hidden) };

    var input_data: [rows * hidden]f32 = undefined;
    var weight: [hidden]f32 = undefined;
    var bias: [hidden]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 17) % 97)) - 48;
        value.* = @as(f32, @floatFromInt(signed)) * 0.03125;
    }
    for (&weight, 0..) |*value, i| value.* = 0.75 + @as(f32, @floatFromInt(i % 11)) * 0.015625;
    for (&bias, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 13)) - 6)) * 0.0078125;

    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_prepare_layer_norm(runtime, 0, &weight, &bias, hidden));
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &shape);
    defer input.deinit();
    var output = try MetalTensor.deviceAllocate(runtime, input_data.len * @sizeOf(f32), .private, &shape);
    defer output.deinit();

    const rc = termite_metal_decode_runtime_apply_layer_norm_device(
        runtime,
        0,
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        hidden,
        1e-5,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    try std.testing.expectEqual(@as(c_int, 0), rc);

    var expected = input_data;
    activations.layerNorm(&expected, &weight, &bias, hidden, 1e-5);
    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |exp, got, i| {
        if (!std.math.approxEqAbs(f32, exp, got, 2e-4)) {
            std.debug.print("layer norm device mismatch idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
    }
}

test "metal native activation device handles Gemma FFN prefill shape" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 10;
    const dim: usize = 6144;
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    var input_data = try std.testing.allocator.alloc(f32, rows * dim);
    defer std.testing.allocator.free(input_data);
    for (input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 4096)) - 2048;
        value.* = @as(f32, @floatFromInt(signed)) * 0.005 + @as(f32, @floatFromInt(i % 7)) * 0.0009765625;
    }
    input_data[2880] = 11.367443;

    var input = try testDeviceTensorFromSlice(runtime, input_data, &shape);
    defer input.deinit();
    var output = try MetalTensor.deviceAllocate(runtime, input_data.len * @sizeOf(f32), .private, &shape);
    defer output.deinit();

    const rc = termite_metal_decode_runtime_apply_activation_device(
        runtime,
        @intFromEnum(@as(ops.DecoderRuntimeActivationKind, .silu)),
        input.deviceHandle(),
        input.deviceByteOffset(),
        rows,
        dim,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    try std.testing.expectEqual(@as(c_int, 0), rc);

    const expected = try std.testing.allocator.dupe(f32, input_data);
    defer std.testing.allocator.free(expected);
    applyActivationHost(expected, .silu);
    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |exp, got, i| {
        if (!std.math.isFinite(got)) {
            std.debug.print("gemma activation nonfinite idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
        if (!std.math.approxEqAbs(f32, exp, got, 2e-4)) {
            std.debug.print("gemma activation mismatch idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
    }
}

test "metal native activation device active frame keeps inline params stable" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 5;
    const dim: usize = 73;
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    var gate_data: [rows * dim]f32 = undefined;
    var up_data: [rows * dim]f32 = undefined;
    for (&gate_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 97)) - 48;
        value.* = @as(f32, @floatFromInt(signed)) * 0.125;
    }
    for (&up_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 53)) - 26;
        value.* = @as(f32, @floatFromInt(signed)) * 0.03125;
    }

    var gate = try testDeviceTensorFromSlice(runtime, &gate_data, &shape);
    defer gate.deinit();
    var up = try testDeviceTensorFromSlice(runtime, &up_data, &shape);
    defer up.deinit();
    var activated = try MetalTensor.deviceAllocate(runtime, gate_data.len * @sizeOf(f32), .private, &shape);
    defer activated.deinit();
    var multiplied = try MetalTensor.deviceAllocate(runtime, gate_data.len * @sizeOf(f32), .private, &shape);
    defer multiplied.deinit();

    try beginFrame(runtime);
    errdefer if (hasActiveFrame(runtime)) cancelFrame(runtime) catch {};

    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_apply_activation_device(
            runtime,
            @intFromEnum(@as(ops.DecoderRuntimeActivationKind, .silu)),
            gate.deviceHandle(),
            gate.deviceByteOffset(),
            rows,
            dim,
            activated.deviceHandle(),
            activated.deviceByteOffset(),
        ),
    );
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_apply_activation_multiply_device(
            runtime,
            @intFromEnum(@as(ops.DecoderRuntimeActivationKind, .quick_gelu)),
            gate.deviceHandle(),
            gate.deviceByteOffset(),
            up.deviceHandle(),
            up.deviceByteOffset(),
            rows,
            dim,
            multiplied.deviceHandle(),
            multiplied.deviceByteOffset(),
        ),
    );
    try submitFrame(runtime);
    try waitFrame(runtime);

    var expected_activated = gate_data;
    applyActivationHost(&expected_activated, .silu);
    var expected_multiplied = gate_data;
    applyActivationHost(&expected_multiplied, .quick_gelu);
    for (&expected_multiplied, up_data) |*lhs, rhs| lhs.* *= rhs;

    var activated_mut = activated;
    const activated_actual = try tensorHostSlice(&activated_mut);
    var multiplied_mut = multiplied;
    const multiplied_actual = try tensorHostSlice(&multiplied_mut);
    for (expected_activated, activated_actual, 0..) |exp, got, i| {
        if (!std.math.isFinite(got)) {
            std.debug.print("active-frame activation nonfinite idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
        if (!std.math.approxEqAbs(f32, exp, got, 2e-4)) {
            std.debug.print("active-frame activation mismatch idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
    }
    for (expected_multiplied, multiplied_actual, 0..) |exp, got, i| {
        if (!std.math.isFinite(got)) {
            std.debug.print("active-frame activation multiply nonfinite idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
        if (!std.math.approxEqAbs(f32, exp, got, 2e-4)) {
            std.debug.print("active-frame activation multiply mismatch idx={d} expected={d} got={d}\n", .{ i, exp, got });
            return error.TestUnexpectedResult;
        }
    }
}

test "metal native activation host ABI copies fallback output buffer" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const dim: usize = 37;
    const kinds = [_]ops.DecoderRuntimeActivationKind{ .gelu, .gelu_new, .silu, .relu, .quick_gelu, .relu_squared };

    var input_data: [dim]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const pattern = [_]f32{ -50.0, -12.5, -1.0, -0.125, 0.0, 0.125, 1.0, 12.5, 50.0 };
        value.* = pattern[i % pattern.len] + @as(f32, @floatFromInt(i % 3)) * 0.03125;
    }

    for (kinds) |kind| {
        var output_backing: [dim + 1]f32 = [_]f32{std.math.nan(f32)} ** (dim + 1);
        const output = output_backing[1..][0..dim];
        const rc = termite_metal_decode_runtime_apply_activation(
            runtime,
            @intFromEnum(kind),
            &input_data,
            dim,
            output.ptr,
        );
        if (rc != 0) {
            std.debug.print("activation host rc={d} kind={s}\n", .{ rc, @tagName(kind) });
            return error.TestUnexpectedResult;
        }

        var expected = input_data;
        applyActivationHost(&expected, kind);
        for (expected, output, 0..) |exp, got, i| {
            if (!std.math.isFinite(got)) {
                std.debug.print("activation host nonfinite kind={s} idx={d} expected={d} got={d}\n", .{ @tagName(kind), i, exp, got });
                return error.TestUnexpectedResult;
            }
            if (!std.math.approxEqAbs(f32, exp, got, 2e-4)) {
                std.debug.print("activation host mismatch kind={s} idx={d} expected={d} got={d}\n", .{ @tagName(kind), i, exp, got });
                return error.TestUnexpectedResult;
            }
        }
    }
}

test "metal native activation multiply device rows match host" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 9;
    const dim: usize = 64;
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    const kinds = [_]ops.DecoderRuntimeActivationKind{ .gelu, .gelu_new, .silu, .relu, .quick_gelu, .relu_squared };

    var gate_data: [rows * dim]f32 = undefined;
    var up_data: [rows * dim]f32 = undefined;
    for (&gate_data, &up_data, 0..) |*gate, *up, i| {
        const gate_pattern = [_]f32{ -200.0, -80.0, -20.0, -2.0, -0.25, 0.0, 0.25, 2.0, 20.0, 80.0, 200.0 };
        gate.* = gate_pattern[i % gate_pattern.len] + @as(f32, @floatFromInt(i % 7)) * 0.015625;
        const signed = @as(i32, @intCast(i % 17)) - 8;
        up.* = @as(f32, @floatFromInt(signed)) * 0.0625;
    }

    for (kinds) |kind| {
        var gate = try testDeviceTensorFromSlice(runtime, &gate_data, &shape);
        defer gate.deinit();
        var up = try testDeviceTensorFromSlice(runtime, &up_data, &shape);
        defer up.deinit();
        var output = try MetalTensor.deviceAllocate(runtime, gate_data.len * @sizeOf(f32), .private, &shape);
        defer output.deinit();

        const rc = termite_metal_decode_runtime_apply_activation_multiply_device(
            runtime,
            @intFromEnum(kind),
            gate.deviceHandle(),
            gate.deviceByteOffset(),
            up.deviceHandle(),
            up.deviceByteOffset(),
            rows,
            dim,
            output.deviceHandle(),
            output.deviceByteOffset(),
        );
        if (rc != 0) {
            std.debug.print("activation multiply rows device rc={d} kind={s}\n", .{ rc, @tagName(kind) });
            return error.TestUnexpectedResult;
        }

        var activated = gate_data;
        applyActivationHost(&activated, kind);
        for (&activated, up_data) |*value, up_value| value.* *= up_value;

        var output_mut = output;
        const actual = try tensorHostSlice(&output_mut);
        try std.testing.expectEqual(activated.len, actual.len);
        for (activated, actual, 0..) |exp, got, i| {
            if (!std.math.isFinite(got)) {
                std.debug.print("activation multiply rows nonfinite kind={s} idx={d} expected={d} got={d}\n", .{ @tagName(kind), i, exp, got });
                return error.TestUnexpectedResult;
            }
            if (!std.math.approxEqAbs(f32, exp, got, 2e-4)) {
                std.debug.print("activation multiply rows mismatch kind={s} idx={d} expected={d} got={d}\n", .{ @tagName(kind), i, exp, got });
                return error.TestUnexpectedResult;
            }
        }
    }
}

test "metal native PLE residual q8_0 single row matches decomposed device path" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 1;
    const hidden_size: usize = 128;
    const ple_hidden_size: usize = 64;
    const block_bytes: usize = 34;
    const gate_blocks = hidden_size / 32;
    const proj_blocks = ple_hidden_size / 32;

    var gate_raw = try std.testing.allocator.alloc(u8, ple_hidden_size * gate_blocks * block_bytes);
    defer std.testing.allocator.free(gate_raw);
    var proj_raw = try std.testing.allocator.alloc(u8, hidden_size * proj_blocks * block_bytes);
    defer std.testing.allocator.free(proj_raw);

    for (0..ple_hidden_size) |o| {
        for (0..gate_blocks) |b| {
            const base = (o * gate_blocks + b) * block_bytes;
            gate_raw[base + 0] = 0x00;
            gate_raw[base + 1] = 0x3C;
            for (0..32) |i| {
                const q = @as(i8, @intCast(@as(i16, @intCast((o * 13 + b * 5 + i * 7) % 19)) - 9));
                gate_raw[base + 2 + i] = @bitCast(q);
            }
        }
    }
    for (0..hidden_size) |o| {
        for (0..proj_blocks) |b| {
            const base = (o * proj_blocks + b) * block_bytes;
            proj_raw[base + 0] = 0x00;
            proj_raw[base + 1] = 0x3C;
            for (0..32) |i| {
                const q = @as(i8, @intCast(@as(i16, @intCast((o * 11 + b * 3 + i * 5) % 17)) - 8));
                proj_raw[base + 2 + i] = @bitCast(q);
            }
        }
    }

    const gate_shape = [_]i64{ @intCast(ple_hidden_size), @intCast(hidden_size) };
    const proj_shape = [_]i64{ @intCast(hidden_size), @intCast(ple_hidden_size) };
    const gate_storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = gate_raw,
        .shape = &gate_shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    const proj_storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = proj_raw,
        .shape = &proj_shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    var gate_bias_data = [_]f32{0.0} ** ple_hidden_size;
    var proj_bias_data = [_]f32{0.0} ** hidden_size;
    var gate_bias = try MetalTensor.ownedCloneFrom(&gate_bias_data, &[_]i32{@intCast(ple_hidden_size)});
    defer gate_bias.deinit();
    var proj_bias = try MetalTensor.ownedCloneFrom(&proj_bias_data, &[_]i32{@intCast(hidden_size)});
    defer proj_bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = gate_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &gate_storage),
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = ple_hidden_size,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = proj_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &proj_storage),
        .slot = 1,
        .in_dim = ple_hidden_size,
        .out_dim = hidden_size,
        .retain_dense_fallback = false,
    }, &prep_stats));

    var norm_weight_data = [_]f32{1.0} ** hidden_size;
    for (&norm_weight_data, 0..) |*value, i| {
        value.* = 0.75 + @as(f32, @floatFromInt(i % 7)) * 0.05;
    }
    var norm_weight = try MetalTensor.ownedCloneFrom(&norm_weight_data, &[_]i32{@intCast(hidden_size)});
    defer norm_weight.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .slot = 0,
        .weight = norm_weight,
        .hidden_size = hidden_size,
    }));

    var hidden_data: [rows * hidden_size]f32 = undefined;
    for (&hidden_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 31)) - 15;
        value.* = @as(f32, @floatFromInt(signed)) * 0.015625;
    }
    var ple_data: [rows * ple_hidden_size]f32 = undefined;
    for (&ple_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 23)) - 11;
        value.* = @as(f32, @floatFromInt(signed)) * 0.03125;
    }
    var hidden = try testDeviceTensorFromSlice(runtime, &hidden_data, &[_]i32{ @intCast(rows), @intCast(hidden_size) });
    defer hidden.deinit();
    var ple = try testDeviceTensorFromSlice(runtime, &ple_data, &[_]i32{ @intCast(rows), @intCast(ple_hidden_size) });
    defer ple.deinit();
    try std.testing.expect(ensureQuantizedRuntimeLinearSlotPrepared(&provider, 0, hidden_size, ple_hidden_size) == .q8_0);
    try std.testing.expect(ensureQuantizedRuntimeLinearSlotPrepared(&provider, 1, ple_hidden_size, hidden_size) == .q8_0);

    const hidden_shape = [_]i32{ @intCast(rows), @intCast(hidden_size) };
    var combined = try MetalTensor.deviceAllocate(runtime, rows * hidden_size * @sizeOf(f32), .private, &hidden_shape);
    defer combined.deinit();
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_ple_residual_q8_0_device(
        runtime,
        hidden.deviceHandle(),
        hidden.deviceByteOffset(),
        ple.deviceHandle(),
        ple.deviceByteOffset(),
        0,
        1,
        0,
        rows,
        hidden_size,
        ple_hidden_size,
        1e-5,
        @intFromEnum(@as(ops.DecoderRuntimeActivationKind, .gelu)),
        combined.deviceHandle(),
        combined.deviceByteOffset(),
    ));

    var stats: ops.NativeQuantTimingStats = .{};
    var gate = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = hidden,
        .in_dim = hidden_size,
        .out_dim = ple_hidden_size,
    })) orelse return error.UnexpectedNull;
    defer gate.deinit();
    var activated = (try decoderRuntimeApplyActivation(&provider, .{
        .input = gate,
        .kind = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .dim = ple_hidden_size,
    }, &stats)) orelse return error.UnexpectedNull;
    defer activated.deinit();
    var gated = (try decoderRuntimeApplyMultiply(&provider, activated, ple, ple_hidden_size)) orelse return error.UnexpectedNull;
    defer gated.deinit();
    var projected = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 1,
        .input = gated,
        .in_dim = ple_hidden_size,
        .out_dim = hidden_size,
    })) orelse return error.UnexpectedNull;
    defer projected.deinit();
    var normed = (try decoderRuntimeApplyRmsNorm(&provider, .{
        .slot = 0,
        .input = projected,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    }, &stats)) orelse return error.UnexpectedNull;
    defer normed.deinit();
    var decomposed = (try decoderRuntimeApplyAdd(&provider, .{
        .lhs = hidden,
        .rhs = normed,
        .dim = hidden_size,
    }, &stats)) orelse return error.UnexpectedNull;
    defer decomposed.deinit();

    var combined_mut = combined;
    var decomposed_mut = decomposed;
    const actual = try tensorHostSlice(&combined_mut);
    const expected = try tensorHostSlice(&decomposed_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-3);
    }
}

test "metal native PLE residual q4_0 uses generic device descriptor path" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 2;
    const hidden_size: usize = 64;
    const ple_hidden_size: usize = 32;
    const gate_blocks = hidden_size / 32;
    const proj_blocks = ple_hidden_size / 32;
    const q4_block_bytes: usize = 18;

    var gate_raw = try std.testing.allocator.alloc(u8, ple_hidden_size * gate_blocks * q4_block_bytes);
    defer std.testing.allocator.free(gate_raw);
    var proj_raw = try std.testing.allocator.alloc(u8, hidden_size * proj_blocks * q4_block_bytes);
    defer std.testing.allocator.free(proj_raw);

    var seed_gate: [hidden_size]f32 = undefined;
    for (0..ple_hidden_size) |row| {
        for (0..gate_blocks) |block| {
            for (&seed_gate, 0..) |*value, col| {
                const signed = @as(i32, @intCast((row * 17 + block * 3 + col * 5) % 37)) - 18;
                value.* = @as(f32, @floatFromInt(signed)) / 31.0;
            }
            quant_codec.quantizeQ4_0Block(
                seed_gate[block * 32 ..][0..32],
                gate_raw[(row * gate_blocks + block) * q4_block_bytes ..][0..q4_block_bytes],
            );
        }
    }
    var seed_proj: [ple_hidden_size]f32 = undefined;
    for (0..hidden_size) |row| {
        for (0..proj_blocks) |block| {
            for (&seed_proj, 0..) |*value, col| {
                const signed = @as(i32, @intCast((row * 13 + block * 7 + col * 11) % 41)) - 20;
                value.* = @as(f32, @floatFromInt(signed)) / 29.0;
            }
            quant_codec.quantizeQ4_0Block(
                seed_proj[block * 32 ..][0..32],
                proj_raw[(row * proj_blocks + block) * q4_block_bytes ..][0..q4_block_bytes],
            );
        }
    }

    const gate_shape = [_]i64{ @intCast(ple_hidden_size), @intCast(hidden_size) };
    const proj_shape = [_]i64{ @intCast(hidden_size), @intCast(ple_hidden_size) };
    const gate_storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = gate_raw,
        .shape = &gate_shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    const proj_storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = proj_raw,
        .shape = &proj_shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    var gate_bias_data = [_]f32{0.0} ** ple_hidden_size;
    var proj_bias_data = [_]f32{0.0} ** hidden_size;
    var gate_bias = try MetalTensor.ownedCloneFrom(&gate_bias_data, &[_]i32{@intCast(ple_hidden_size)});
    defer gate_bias.deinit();
    var proj_bias = try MetalTensor.ownedCloneFrom(&proj_bias_data, &[_]i32{@intCast(hidden_size)});
    defer proj_bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = gate_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &gate_storage),
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = ple_hidden_size,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = proj_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &proj_storage),
        .slot = 1,
        .in_dim = ple_hidden_size,
        .out_dim = hidden_size,
        .retain_dense_fallback = false,
    }, &prep_stats));

    var norm_weight_data = [_]f32{1.0} ** hidden_size;
    for (&norm_weight_data, 0..) |*value, i| value.* = 0.8 + @as(f32, @floatFromInt(i % 5)) * 0.03;
    var norm_weight = try MetalTensor.ownedCloneFrom(&norm_weight_data, &[_]i32{@intCast(hidden_size)});
    defer norm_weight.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .slot = 0,
        .weight = norm_weight,
        .hidden_size = hidden_size,
    }));

    var hidden_data: [rows * hidden_size]f32 = undefined;
    for (&hidden_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 47)) - 23;
        value.* = @as(f32, @floatFromInt(signed)) / 37.0;
    }
    var ple_data: [rows * ple_hidden_size]f32 = undefined;
    for (&ple_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 31)) - 15;
        value.* = @as(f32, @floatFromInt(signed)) / 19.0;
    }
    var hidden = try testDeviceTensorFromSlice(runtime, &hidden_data, &[_]i32{ @intCast(rows), @intCast(hidden_size) });
    defer hidden.deinit();
    var ple = try testDeviceTensorFromSlice(runtime, &ple_data, &[_]i32{ @intCast(rows), @intCast(ple_hidden_size) });
    defer ple.deinit();

    try std.testing.expect(ensureQuantizedRuntimeLinearSlotPrepared(&provider, 0, hidden_size, ple_hidden_size) == .q4_0);
    try std.testing.expect(ensureQuantizedRuntimeLinearSlotPrepared(&provider, 1, ple_hidden_size, hidden_size) == .q4_0);

    var fused = (try decoderRuntimeApplyPleResidualDevice(&provider, .{
        .hidden = hidden,
        .ple = ple,
        .gate_linear_slot = 0,
        .proj_linear_slot = 1,
        .post_norm_slot = 0,
        .hidden_size = hidden_size,
        .ple_hidden_size = ple_hidden_size,
        .eps = 1e-5,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu),
    })) orelse return error.UnexpectedNull;
    defer fused.deinit();
    try std.testing.expect(fused.isDevice());

    var stats: ops.NativeQuantTimingStats = .{};
    var gate = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = hidden,
        .in_dim = hidden_size,
        .out_dim = ple_hidden_size,
    })) orelse return error.UnexpectedNull;
    defer gate.deinit();
    var activated = (try decoderRuntimeApplyActivation(&provider, .{
        .input = gate,
        .kind = @as(ops.DecoderRuntimeActivationKind, .gelu),
        .dim = ple_hidden_size,
    }, &stats)) orelse return error.UnexpectedNull;
    defer activated.deinit();
    var gated = (try decoderRuntimeApplyMultiply(&provider, activated, ple, ple_hidden_size)) orelse return error.UnexpectedNull;
    defer gated.deinit();
    var projected = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 1,
        .input = gated,
        .in_dim = ple_hidden_size,
        .out_dim = hidden_size,
    })) orelse return error.UnexpectedNull;
    defer projected.deinit();
    var normed = (try decoderRuntimeApplyRmsNorm(&provider, .{
        .slot = 0,
        .input = projected,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    }, &stats)) orelse return error.UnexpectedNull;
    defer normed.deinit();
    var expected_tensor = (try decoderRuntimeApplyAdd(&provider, .{
        .lhs = hidden,
        .rhs = normed,
        .dim = hidden_size,
    }, &stats)) orelse return error.UnexpectedNull;
    defer expected_tensor.deinit();

    var fused_mut = fused;
    var expected_mut = expected_tensor;
    const actual = try tensorHostSlice(&fused_mut);
    const expected = try tensorHostSlice(&expected_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-4);
    }
}

test "metal native decoderRuntimeApplyRmsNorm plus q8_0 linear matches reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const hidden_size: usize = 32;
    const out_dim: usize = 2;
    var weight_raw: [68]u8 = [_]u8{0} ** 68;

    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C;
    for (0..32) |i| weight_raw[2 + i] = @bitCast(@as(i8, 1));

    weight_raw[34] = 0x00;
    weight_raw[35] = 0x3C;
    for (0..32) |i| weight_raw[36 + i] = @bitCast(@as(i8, -2));

    const shape = [_]i64{ @intCast(out_dim), @intCast(hidden_size) };
    const storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = &weight_raw,
        .shape = &shape,
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const rms_weight_data = [_]f32{1.0} ** hidden_size;
    var rms_weight = try MetalTensor.ownedCloneFrom(&rms_weight_data, &[_]i32{@intCast(hidden_size)});
    defer rms_weight.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .weight = rms_weight,
        .slot = 0,
        .hidden_size = hidden_size,
    }));

    const bias_data = [_]f32{ 0.0, 0.0 };
    var bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, &storage),
        .slot = 1,
        .in_dim = hidden_size,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, &stats));

    var input_data: [hidden_size]f32 = undefined;
    for (&input_data, 0..) |*value, idx| value.* = @floatFromInt(@as(i32, @intCast(idx + 1)));
    var input = try MetalTensor.ownedCloneFrom(&input_data, &[_]i32{ 1, @intCast(hidden_size) });
    defer input.deinit();

    var normed = (try decoderRuntimeApplyRmsNorm(&provider, .{
        .slot = 0,
        .input = input,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    }, &stats)) orelse return error.UnexpectedNull;
    defer normed.deinit();

    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 1,
        .input = normed,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    try std.testing.expectEqual(@as(RawQuantizedRuntimeLinearKind, .q8_0), provider.raw_linear_slot_runtime_prepared_kind[1]);
    if (provider.raw_linear_slot_runtime_prepared_modes[1] == .mapped_shared) {
        try std.testing.expectEqual(@as(RawQuantizedRuntimeLinearStorageMode, .mapped_shared), provider.raw_linear_slot_runtime_prepared_modes[1]);
        try std.testing.expectEqual(@as(u64, 1), provider.raw_quant_runtime_mapped_attempts);
        try std.testing.expectEqual(@as(u64, 0), provider.raw_quant_runtime_mapped_fallbacks);
        try std.testing.expectEqual(@as(u64, 0), provider.raw_quant_runtime_mapped_failures);
    } else {
        try std.testing.expectEqual(@as(RawQuantizedRuntimeLinearStorageMode, .private_upload), provider.raw_linear_slot_runtime_prepared_modes[1]);
        try std.testing.expectEqual(@as(u64, 0), provider.raw_quant_runtime_mapped_attempts);
    }

    var sum_sq: f32 = 0.0;
    for (input_data) |value| sum_sq += value * value;
    const inv_rms: f32 = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(hidden_size)) + 1e-5);
    var expected_sum: f32 = 0.0;
    for (input_data) |value| expected_sum += value * inv_rms;

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(@as(usize, out_dim), actual.len);
    try std.testing.expectApproxEqAbs(expected_sum, actual[0], 1e-3);
    try std.testing.expectApproxEqAbs(-2.0 * expected_sum, actual[1], 1e-3);
}

test "metal native q8_0 qkv scratch supports batched rows" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const rows: usize = 10;
    const hidden_size: usize = 64;
    const q_out_dim: usize = 16;
    const kv_out_dim: usize = 8;

    const TestQ80Weight = struct {
        bytes: []u8,
        shape: [2]i64,
        storage: QuantizedStorage,
    };
    const makePatternQ80Weight = struct {
        fn build(allocator: std.mem.Allocator, out_dim: usize, in_dim: usize, seed: usize) !TestQ80Weight {
            const blocks_per_row = try std.math.divExact(usize, in_dim, 32);
            const bytes = try allocator.alloc(u8, out_dim * blocks_per_row * 34);
            errdefer allocator.free(bytes);
            for (0..out_dim) |row| {
                for (0..blocks_per_row) |block| {
                    const base = (row * blocks_per_row + block) * 34;
                    bytes[base + 0] = 0x00;
                    bytes[base + 1] = 0x38;
                    for (0..32) |i| {
                        const raw = @as(i16, @intCast((row * 7 + block * 5 + i * 3 + seed) % 17)) - 8;
                        bytes[base + 2 + i] = @bitCast(@as(i8, @intCast(raw)));
                    }
                }
            }
            const shape = [2]i64{ @intCast(out_dim), @intCast(in_dim) };
            return .{
                .bytes = bytes,
                .shape = shape,
                .storage = .{
                    .tensor_type = .{ .known = .Q8_0 },
                    .raw_bytes = bytes,
                    .shape = &shape,
                    .raw_owned = false,
                    .allocator = allocator,
                },
            };
        }
    }.build;

    var q_weight = try makePatternQ80Weight(std.testing.allocator, q_out_dim, hidden_size, 1);
    defer std.testing.allocator.free(q_weight.bytes);
    var k_weight = try makePatternQ80Weight(std.testing.allocator, kv_out_dim, hidden_size, 5);
    defer std.testing.allocator.free(k_weight.bytes);
    var v_weight = try makePatternQ80Weight(std.testing.allocator, kv_out_dim, hidden_size, 11);
    defer std.testing.allocator.free(v_weight.bytes);

    const zero_q_bias_data = [_]f32{0.0} ** q_out_dim;
    var q_bias = try MetalTensor.ownedCloneFrom(&zero_q_bias_data, &[_]i32{@intCast(q_out_dim)});
    defer q_bias.deinit();
    const zero_kv_bias_data = [_]f32{0.0} ** kv_out_dim;
    var k_bias = try MetalTensor.ownedCloneFrom(&zero_kv_bias_data, &[_]i32{@intCast(kv_out_dim)});
    defer k_bias.deinit();
    var v_bias = try MetalTensor.ownedCloneFrom(&zero_kv_bias_data, &[_]i32{@intCast(kv_out_dim)});
    defer v_bias.deinit();
    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = q_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &q_weight.storage),
        .slot = 10,
        .in_dim = hidden_size,
        .out_dim = q_out_dim,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = k_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &k_weight.storage),
        .slot = 11,
        .in_dim = hidden_size,
        .out_dim = kv_out_dim,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = v_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &v_weight.storage),
        .slot = 12,
        .in_dim = hidden_size,
        .out_dim = kv_out_dim,
        .retain_dense_fallback = false,
    }, &prep_stats));

    var input_data: [rows * hidden_size]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 5) % 31)) - 15;
        value.* = @as(f32, @floatFromInt(signed)) * 0.0625;
    }
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ @intCast(rows), @intCast(hidden_size) });
    defer input.deinit();

    var owned = (try tryApplyQuantizedRuntimeLinearQkv(
        &provider,
        10,
        11,
        12,
        input,
        rows,
        hidden_size,
        q_out_dim,
        kv_out_dim,
    )) orelse return error.UnexpectedNull;
    defer owned.first.deinit();
    defer owned.second.deinit();
    defer owned.third.deinit();

    var scratch = (try tryApplyQuantizedRuntimeLinearQkvScratch(
        &provider,
        10,
        11,
        12,
        input,
        rows,
        hidden_size,
        q_out_dim,
        kv_out_dim,
    )) orelse return error.UnexpectedNull;
    defer scratch.first.deinit();
    defer scratch.second.deinit();
    defer scratch.third.deinit();

    const triples = [_]struct { expected: *MetalTensor, actual: *MetalTensor }{
        .{ .expected = &owned.first, .actual = &scratch.first },
        .{ .expected = &owned.second, .actual = &scratch.second },
        .{ .expected = &owned.third, .actual = &scratch.third },
    };
    for (triples) |triple| {
        const expected = try tensorHostSlice(triple.expected);
        const actual = try tensorHostSlice(triple.actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |exp, got| {
            try std.testing.expectApproxEqAbs(exp, got, 1e-4);
        }
    }
}

test "metal native q8_0 gated ffn batched matches rowwise" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const hidden_size: usize = 32;
    const intermediate_size: usize = 32;
    const rows: usize = 2;

    const TestQ80Weight = struct {
        bytes: []u8,
        shape: [2]i64,
        storage: QuantizedStorage,
    };

    const makeUniformQ80Weight = struct {
        fn build(allocator: std.mem.Allocator, out_dim: usize, in_dim: usize, value: i8) !TestQ80Weight {
            const bytes = try allocator.alloc(u8, out_dim * 34);
            errdefer allocator.free(bytes);
            for (0..out_dim) |row| {
                const base = row * 34;
                bytes[base + 0] = 0x00;
                bytes[base + 1] = 0x3C;
                for (0..32) |i| bytes[base + 2 + i] = @bitCast(value);
            }
            const shape = [2]i64{ @intCast(out_dim), @intCast(in_dim) };
            return .{
                .bytes = bytes,
                .shape = shape,
                .storage = .{
                    .tensor_type = .{ .known = .Q8_0 },
                    .raw_bytes = bytes,
                    .shape = &shape,
                    .raw_owned = false,
                    .allocator = allocator,
                },
            };
        }
    }.build;

    var gate_weight = try makeUniformQ80Weight(std.testing.allocator, intermediate_size, hidden_size, 1);
    defer std.testing.allocator.free(gate_weight.bytes);
    var up_weight = try makeUniformQ80Weight(std.testing.allocator, intermediate_size, hidden_size, 2);
    defer std.testing.allocator.free(up_weight.bytes);
    var down_weight = try makeUniformQ80Weight(std.testing.allocator, hidden_size, intermediate_size, 1);
    defer std.testing.allocator.free(down_weight.bytes);

    const zero_intermediate_bias_data = [_]f32{0.0} ** intermediate_size;
    var gate_bias = try MetalTensor.ownedCloneFrom(&zero_intermediate_bias_data, &[_]i32{@intCast(intermediate_size)});
    defer gate_bias.deinit();
    var up_bias = try MetalTensor.ownedCloneFrom(&zero_intermediate_bias_data, &[_]i32{@intCast(intermediate_size)});
    defer up_bias.deinit();
    const zero_hidden_bias_data = [_]f32{0.0} ** hidden_size;
    var down_bias = try MetalTensor.ownedCloneFrom(&zero_hidden_bias_data, &[_]i32{@intCast(hidden_size)});
    defer down_bias.deinit();

    const rms_weight_data = [_]f32{1.0} ** hidden_size;
    var post_down_rms_weight = try MetalTensor.ownedCloneFrom(&rms_weight_data, &[_]i32{@intCast(hidden_size)});
    defer post_down_rms_weight.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .weight = post_down_rms_weight,
        .slot = 0,
        .hidden_size = hidden_size,
    }));
    var post_gate_rms_weight_data: [intermediate_size]f32 = undefined;
    for (&post_gate_rms_weight_data, 0..) |*value, i| {
        value.* = 0.75 + @as(f32, @floatFromInt(@as(u32, @intCast(i % 5)))) * 0.125;
    }
    var post_gate_rms_weight = try MetalTensor.ownedCloneFrom(&post_gate_rms_weight_data, &[_]i32{@intCast(intermediate_size)});
    defer post_gate_rms_weight.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .weight = post_gate_rms_weight,
        .slot = 1,
        .hidden_size = intermediate_size,
    }));

    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = gate_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &gate_weight.storage),
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = up_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &up_weight.storage),
        .slot = 1,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = down_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &down_weight.storage),
        .slot = 2,
        .in_dim = intermediate_size,
        .out_dim = hidden_size,
        .retain_dense_fallback = false,
    }, &prep_stats));

    var input_data: [rows * hidden_size]f32 = undefined;
    var residual_data: [rows * hidden_size]f32 = undefined;
    for (&input_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i % hidden_size) + 1)))) * 0.125;
    for (&residual_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i % hidden_size) + 3)))) * 0.0625;

    var batched_output = [_]f32{0.0} ** (rows * hidden_size);
    var stats: ops.NativeQuantTimingStats = .{};
    var logged_unsupported_type = false;
    try std.testing.expect(try tryRawQuantizedGatedFfnResidualHost(&provider, .{
        .input = &input_data,
        .residual = &residual_data,
        .rows = rows,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .relu),
        .gate_linear_slot = 0,
        .up_linear_slot = 1,
        .down_linear_slot = 2,
        .post_gate_rms_norm_slot = @as(?usize, 1),
        .post_down_rms_norm_slot = @as(?usize, 0),
        .output = &batched_output,
    }, &stats, &logged_unsupported_type));

    var rowwise_output = [_]f32{0.0} ** (rows * hidden_size);
    for (0..rows) |row| {
        const row_offset = row * hidden_size;
        try std.testing.expect(try tryRawQuantizedGatedFfnResidualHost(&provider, .{
            .input = input_data[row_offset..].ptr,
            .residual = residual_data[row_offset..].ptr,
            .rows = 1,
            .hidden_size = hidden_size,
            .intermediate_size = intermediate_size,
            .activation = @as(ops.DecoderRuntimeActivationKind, .relu),
            .gate_linear_slot = 0,
            .up_linear_slot = 1,
            .down_linear_slot = 2,
            .post_gate_rms_norm_slot = @as(?usize, 1),
            .post_down_rms_norm_slot = @as(?usize, 0),
            .output = rowwise_output[row_offset..].ptr,
        }, &stats, &logged_unsupported_type));
    }

    for (batched_output, rowwise_output) |actual, expected| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
    }
}

test "metal native q8_0 gated ffn device frame matches decomposed" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const hidden_size: usize = 1536;
    const intermediate_size: usize = 8960;
    const rows: usize = 10;

    const TestQ80Weight = struct {
        bytes: []u8,
        shape: [2]i64,
        storage: QuantizedStorage,
    };
    const makePatternQ80Weight = struct {
        fn build(allocator: std.mem.Allocator, out_dim: usize, in_dim: usize, seed: usize) !TestQ80Weight {
            const blocks_per_row = try std.math.divExact(usize, in_dim, 32);
            const bytes = try allocator.alloc(u8, out_dim * blocks_per_row * 34);
            errdefer allocator.free(bytes);
            for (0..out_dim) |row| {
                for (0..blocks_per_row) |block| {
                    const base = (row * blocks_per_row + block) * 34;
                    bytes[base + 0] = 0x00;
                    bytes[base + 1] = 0x38;
                    for (0..32) |i| {
                        const raw = @as(i16, @intCast((row * 17 + block * 11 + i * 5 + seed) % 23)) - 11;
                        bytes[base + 2 + i] = @bitCast(@as(i8, @intCast(raw)));
                    }
                }
            }
            const shape = [2]i64{ @intCast(out_dim), @intCast(in_dim) };
            return .{
                .bytes = bytes,
                .shape = shape,
                .storage = .{
                    .tensor_type = .{ .known = .Q8_0 },
                    .raw_bytes = bytes,
                    .shape = &shape,
                    .raw_owned = false,
                    .allocator = allocator,
                },
            };
        }
    }.build;

    var gate_weight = try makePatternQ80Weight(std.testing.allocator, intermediate_size, hidden_size, 1);
    defer std.testing.allocator.free(gate_weight.bytes);
    var up_weight = try makePatternQ80Weight(std.testing.allocator, intermediate_size, hidden_size, 7);
    defer std.testing.allocator.free(up_weight.bytes);
    var down_weight = try makePatternQ80Weight(std.testing.allocator, hidden_size, intermediate_size, 13);
    defer std.testing.allocator.free(down_weight.bytes);

    const zero_intermediate_bias_data = [_]f32{0.0} ** intermediate_size;
    var gate_bias = try MetalTensor.ownedCloneFrom(&zero_intermediate_bias_data, &[_]i32{@intCast(intermediate_size)});
    defer gate_bias.deinit();
    var up_bias = try MetalTensor.ownedCloneFrom(&zero_intermediate_bias_data, &[_]i32{@intCast(intermediate_size)});
    defer up_bias.deinit();
    const zero_hidden_bias_data = [_]f32{0.0} ** hidden_size;
    var down_bias = try MetalTensor.ownedCloneFrom(&zero_hidden_bias_data, &[_]i32{@intCast(hidden_size)});
    defer down_bias.deinit();

    var post_down_rms_data: [hidden_size]f32 = undefined;
    for (&post_down_rms_data, 0..) |*value, i| value.* = 0.75 + @as(f32, @floatFromInt(i % 5)) * 0.0625;
    var post_down_rms_weight = try MetalTensor.ownedCloneFrom(&post_down_rms_data, &[_]i32{@intCast(hidden_size)});
    defer post_down_rms_weight.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .weight = post_down_rms_weight,
        .slot = 0,
        .hidden_size = hidden_size,
    }));

    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = gate_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &gate_weight.storage),
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = up_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &up_weight.storage),
        .slot = 1,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
        .retain_dense_fallback = false,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .weight = dummy_weight,
        .bias = down_bias,
        .quantized_storage = @as(?*const QuantizedStorage, &down_weight.storage),
        .slot = 2,
        .in_dim = intermediate_size,
        .out_dim = hidden_size,
        .retain_dense_fallback = false,
    }, &prep_stats));

    var input_data: [rows * hidden_size]f32 = undefined;
    var residual_data: [rows * hidden_size]f32 = undefined;
    for (&input_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast(i % 37)) - 18;
        value.* = @as(f32, @floatFromInt(signed)) * 0.03125;
    }
    for (&residual_data, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 3) % 29)) - 14;
        value.* = @as(f32, @floatFromInt(signed)) * 0.015625;
    }
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ @intCast(rows), @intCast(hidden_size) });
    defer input.deinit();
    var residual = try testDeviceTensorFromSlice(runtime, &residual_data, &[_]i32{ @intCast(rows), @intCast(hidden_size) });
    defer residual.deinit();

    var stats: ops.NativeQuantTimingStats = .{};
    var gate = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
    })) orelse return error.UnexpectedNull;
    defer gate.deinit();
    var up = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 1,
        .input = input,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
    })) orelse return error.UnexpectedNull;
    defer up.deinit();
    var activated = (try decoderRuntimeApplyActivation(&provider, .{
        .input = gate,
        .kind = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .dim = intermediate_size,
    }, &stats)) orelse return error.UnexpectedNull;
    defer activated.deinit();
    var gated = (try decoderRuntimeApplyMultiply(&provider, activated, up, intermediate_size)) orelse return error.UnexpectedNull;
    defer gated.deinit();
    var projected = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 2,
        .input = gated,
        .in_dim = intermediate_size,
        .out_dim = hidden_size,
    })) orelse return error.UnexpectedNull;
    defer projected.deinit();
    var normed = (try decoderRuntimeApplyRmsNorm(&provider, .{
        .slot = 0,
        .input = projected,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    }, &stats)) orelse return error.UnexpectedNull;
    defer normed.deinit();
    var decomposed = (try decoderRuntimeApplyAdd(&provider, .{
        .lhs = normed,
        .rhs = residual,
        .dim = hidden_size,
    }, &stats)) orelse return error.UnexpectedNull;
    defer decomposed.deinit();

    const before_direct_dispatch = runtimeMemorySnapshot(runtime);
    var direct = (try tryDeviceQuantizedGatedFfnResidual(&provider, .{
        .gate_linear_slot = 0,
        .up_linear_slot = 1,
        .down_linear_slot = 2,
        .rows = rows,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .eps = 1e-5,
        .post_gate_rms_norm_slot = null,
        .post_down_rms_norm_slot = @as(?usize, 0),
    }, input, residual, &stats)) orelse return error.UnexpectedNull;
    defer direct.deinit();
    const after_direct_dispatch = runtimeMemorySnapshot(runtime);
    // C enum mirror: TERMITE_METAL_Q8_0_LINEAR_FAMILY_PAIR_ACTIVATION / DISPATCH_MMV or DISPATCH_MM.
    try std.testing.expect(
        after_direct_dispatch.q8_0_linear_family_dispatch_counts[1][1] > before_direct_dispatch.q8_0_linear_family_dispatch_counts[1][1] or
            after_direct_dispatch.q8_0_linear_family_dispatch_counts[1][3] > before_direct_dispatch.q8_0_linear_family_dispatch_counts[1][3],
    );

    try std.testing.expect(decoderRuntimeReserveGatedFfnScratch(&provider, rows, hidden_size, intermediate_size));
    try beginFrame(runtime);
    var framed = (try tryDeviceQuantizedGatedFfnResidual(&provider, .{
        .gate_linear_slot = 0,
        .up_linear_slot = 1,
        .down_linear_slot = 2,
        .rows = rows,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .eps = 1e-5,
        .post_gate_rms_norm_slot = null,
        .post_down_rms_norm_slot = @as(?usize, 0),
    }, input, residual, &stats)) orelse return error.UnexpectedNull;
    defer framed.deinit();
    try flushActiveFrame(runtime);
    try cancelFrame(runtime);

    var decomposed_mut = decomposed;
    var direct_mut = direct;
    var framed_mut = framed;
    const expected = try tensorHostSlice(&decomposed_mut);
    const direct_actual = try tensorHostSlice(&direct_mut);
    const framed_actual = try tensorHostSlice(&framed_mut);
    try std.testing.expectEqual(expected.len, direct_actual.len);
    try std.testing.expectEqual(expected.len, framed_actual.len);
    for (expected, direct_actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-3);
    }
    for (expected, framed_actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-3);
    }

    var row_input = try testDeviceTensorFromSlice(runtime, input_data[0..hidden_size], &[_]i32{ 1, @intCast(hidden_size) });
    defer row_input.deinit();
    var row_residual = try testDeviceTensorFromSlice(runtime, residual_data[0..hidden_size], &[_]i32{ 1, @intCast(hidden_size) });
    defer row_residual.deinit();
    try std.testing.expect(decoderRuntimeReserveGatedFfnScratch(&provider, 1, hidden_size, intermediate_size));
    try beginFrame(runtime);
    var framed_row = (try tryDeviceQuantizedGatedFfnResidual(&provider, .{
        .gate_linear_slot = 0,
        .up_linear_slot = 1,
        .down_linear_slot = 2,
        .rows = 1,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .eps = 1e-5,
        .post_gate_rms_norm_slot = null,
        .post_down_rms_norm_slot = @as(?usize, 0),
    }, row_input, row_residual, &stats)) orelse return error.UnexpectedNull;
    defer framed_row.deinit();
    try submitFrame(runtime);
    try waitFrame(runtime);

    var framed_row_mut = framed_row;
    const framed_row_actual = try tensorHostSlice(&framed_row_mut);
    try std.testing.expectEqual(@as(usize, hidden_size), framed_row_actual.len);
    for (expected[0..hidden_size], framed_row_actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-3);
    }
    const planned_snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expect(planned_snapshot.last_frame_planned_compute_scope_count >= 1);
    if (getenvBool("TERMITE_METAL_DISABLE_PLANNED_COMPUTE_BARRIERS")) {
        try std.testing.expectEqual(@as(u64, 0), planned_snapshot.last_frame_planned_barrier_count);
    } else {
        try std.testing.expect(planned_snapshot.last_frame_planned_barrier_count >= 1);
    }
}

test "metal native planned q8_0 attention ffn ple block matches decomposed" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const hidden_size: usize = 32;
    const attention_input_size: usize = 32;
    const intermediate_size: usize = 64;
    const ple_hidden_size: usize = 32;
    const kv_tokens: usize = 3;
    const num_heads: usize = 2;
    const num_kv_heads: usize = 1;
    const head_dim: usize = 16;
    const eps: f32 = 1e-5;

    const TestQ80Weight = struct { bytes: []u8, shape: [2]i64, storage: QuantizedStorage };
    const makePatternQ80Weight = struct {
        fn build(allocator: std.mem.Allocator, out_dim: usize, in_dim: usize, seed: usize) !TestQ80Weight {
            const blocks_per_row = try std.math.divExact(usize, in_dim, 32);
            const bytes = try allocator.alloc(u8, out_dim * blocks_per_row * 34);
            errdefer allocator.free(bytes);
            for (0..out_dim) |row| {
                for (0..blocks_per_row) |block| {
                    const base = (row * blocks_per_row + block) * 34;
                    bytes[base + 0] = 0x00;
                    bytes[base + 1] = 0x38;
                    for (0..32) |i| {
                        const raw = @as(i16, @intCast((row * 13 + block * 7 + i * 3 + seed) % 19)) - 9;
                        bytes[base + 2 + i] = @bitCast(@as(i8, @intCast(raw)));
                    }
                }
            }
            const shape = [2]i64{ @intCast(out_dim), @intCast(in_dim) };
            return .{
                .bytes = bytes,
                .shape = shape,
                .storage = .{
                    .tensor_type = .{ .known = .Q8_0 },
                    .raw_bytes = bytes,
                    .shape = &shape,
                    .raw_owned = false,
                    .allocator = allocator,
                },
            };
        }
    }.build;

    var attention_weight = try makePatternQ80Weight(std.testing.allocator, hidden_size, attention_input_size, 1);
    defer std.testing.allocator.free(attention_weight.bytes);
    var gate_weight = try makePatternQ80Weight(std.testing.allocator, intermediate_size, hidden_size, 3);
    defer std.testing.allocator.free(gate_weight.bytes);
    var up_weight = try makePatternQ80Weight(std.testing.allocator, intermediate_size, hidden_size, 5);
    defer std.testing.allocator.free(up_weight.bytes);
    var down_weight = try makePatternQ80Weight(std.testing.allocator, hidden_size, intermediate_size, 7);
    defer std.testing.allocator.free(down_weight.bytes);
    var ple_gate_weight = try makePatternQ80Weight(std.testing.allocator, ple_hidden_size, hidden_size, 11);
    defer std.testing.allocator.free(ple_gate_weight.bytes);
    var ple_proj_weight = try makePatternQ80Weight(std.testing.allocator, hidden_size, ple_hidden_size, 13);
    defer std.testing.allocator.free(ple_proj_weight.bytes);

    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    const SlotPrep = struct {
        fn linear(p: anytype, weight: MetalTensor, storage: *const QuantizedStorage, slot: usize, in_dim: usize, out_dim: usize, stats: *ops.NativeQuantTimingStats) !void {
            const bias_data = try std.testing.allocator.alloc(f32, out_dim);
            defer std.testing.allocator.free(bias_data);
            @memset(bias_data, 0.0);
            var bias = try MetalTensor.ownedCloneFrom(bias_data, &[_]i32{@intCast(out_dim)});
            defer bias.deinit();
            try std.testing.expect(try decoderRuntimePrepareLinear(p, .{
                .weight = weight,
                .bias = bias,
                .quantized_storage = @as(?*const QuantizedStorage, storage),
                .slot = slot,
                .in_dim = in_dim,
                .out_dim = out_dim,
                .retain_dense_fallback = false,
            }, stats));
        }
    };
    try SlotPrep.linear(&provider, dummy_weight, &attention_weight.storage, 0, attention_input_size, hidden_size, &prep_stats);
    try SlotPrep.linear(&provider, dummy_weight, &gate_weight.storage, 1, hidden_size, intermediate_size, &prep_stats);
    try SlotPrep.linear(&provider, dummy_weight, &up_weight.storage, 2, hidden_size, intermediate_size, &prep_stats);
    try SlotPrep.linear(&provider, dummy_weight, &down_weight.storage, 3, intermediate_size, hidden_size, &prep_stats);
    try SlotPrep.linear(&provider, dummy_weight, &ple_gate_weight.storage, 4, hidden_size, ple_hidden_size, &prep_stats);
    try SlotPrep.linear(&provider, dummy_weight, &ple_proj_weight.storage, 5, ple_hidden_size, hidden_size, &prep_stats);

    var hidden_norm_weight_data: [hidden_size]f32 = undefined;
    for (&hidden_norm_weight_data, 0..) |*value, i| value.* = 0.75 + @as(f32, @floatFromInt(i % 7)) * 0.03125;
    var attention_post_norm = try MetalTensor.ownedCloneFrom(&hidden_norm_weight_data, &[_]i32{@intCast(hidden_size)});
    defer attention_post_norm.deinit();
    var ffn_pre_norm = try MetalTensor.ownedCloneFrom(&hidden_norm_weight_data, &[_]i32{@intCast(hidden_size)});
    defer ffn_pre_norm.deinit();
    var ffn_post_norm = try MetalTensor.ownedCloneFrom(&hidden_norm_weight_data, &[_]i32{@intCast(hidden_size)});
    defer ffn_post_norm.deinit();
    var ple_post_norm = try MetalTensor.ownedCloneFrom(&hidden_norm_weight_data, &[_]i32{@intCast(hidden_size)});
    defer ple_post_norm.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{ .weight = attention_post_norm, .slot = 0, .hidden_size = hidden_size }));
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{ .weight = ffn_pre_norm, .slot = 1, .hidden_size = hidden_size }));
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{ .weight = ffn_post_norm, .slot = 2, .hidden_size = hidden_size }));
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{ .weight = ple_post_norm, .slot = 3, .hidden_size = hidden_size }));

    var q_data: [attention_input_size]f32 = undefined;
    var k_data: [kv_tokens * num_kv_heads * head_dim]f32 = undefined;
    var v_data: [kv_tokens * num_kv_heads * head_dim]f32 = undefined;
    var residual_data: [hidden_size]f32 = undefined;
    var ple_data: [ple_hidden_size]f32 = undefined;
    for (&q_data, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(@as(i32, @intCast(i % 11)) - 5))) * 0.0625;
    for (&k_data, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(@as(i32, @intCast((i * 3) % 13)) - 6))) * 0.05;
    for (&v_data, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 17)) - 8))) * 0.04;
    for (&residual_data, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 19)) - 9))) * 0.03125;
    for (&ple_data, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(@as(i32, @intCast((i * 2) % 9)) - 4))) * 0.025;
    var q = try testDeviceTensorFromSlice(runtime, &q_data, &[_]i32{ 1, @intCast(attention_input_size) });
    defer q.deinit();
    var k = try testDeviceTensorFromSlice(runtime, &k_data, &[_]i32{ @intCast(kv_tokens), @intCast(num_kv_heads * head_dim) });
    defer k.deinit();
    var v = try testDeviceTensorFromSlice(runtime, &v_data, &[_]i32{ @intCast(kv_tokens), @intCast(num_kv_heads * head_dim) });
    defer v.deinit();
    var residual = try testDeviceTensorFromSlice(runtime, &residual_data, &[_]i32{ 1, @intCast(hidden_size) });
    defer residual.deinit();
    var ple = try testDeviceTensorFromSlice(runtime, &ple_data, &[_]i32{ 1, @intCast(ple_hidden_size) });
    defer ple.deinit();

    var stats: ops.NativeQuantTimingStats = .{};
    var attn = (try decoderRuntimeApplyAttentionF32(&provider, .{
        .q = q,
        .k = k,
        .v = v,
        .bias = @as(?MetalTensor, null),
        .attn_or_mask = @as(?[]const u8, null),
        .q_len = 1,
        .kv_len = kv_tokens,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .query_position_offset = kv_tokens - 1,
        .kv_position_offset = 0,
        .sliding_window = 0,
        .total_sequence_len = kv_tokens,
    })) orelse return error.UnexpectedNull;
    defer attn.deinit();
    var attn_projected = (try decoderRuntimeApplyLinear(&provider, .{ .slot = 0, .input = attn, .in_dim = attention_input_size, .out_dim = hidden_size })) orelse return error.UnexpectedNull;
    defer attn_projected.deinit();
    var attn_post = (try decoderRuntimeApplyRmsNorm(&provider, .{ .slot = 0, .input = attn_projected, .hidden_size = hidden_size, .eps = eps }, &stats)) orelse return error.UnexpectedNull;
    defer attn_post.deinit();
    var attn_added = (try decoderRuntimeApplyAdd(&provider, .{ .lhs = attn_post, .rhs = residual, .dim = hidden_size }, &stats)) orelse return error.UnexpectedNull;
    defer attn_added.deinit();
    var ffn_normed = (try decoderRuntimeApplyFfnNormInternal(&provider, attn_added, null, 1, hidden_size, eps)) orelse return error.UnexpectedNull;
    defer ffn_normed.deinit();
    var ffn_output = (try tryDeviceQuantizedGatedFfnResidual(&provider, .{
        .gate_linear_slot = 1,
        .up_linear_slot = 2,
        .down_linear_slot = 3,
        .rows = 1,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .eps = eps,
        .post_gate_rms_norm_slot = null,
        .post_down_rms_norm_slot = @as(?usize, 2),
    }, ffn_normed, attn_added, &stats)) orelse return error.UnexpectedNull;
    defer ffn_output.deinit();
    var decomposed = (try decoderRuntimeApplyPleResidualDevice(&provider, .{
        .hidden = ffn_output,
        .ple = ple,
        .gate_linear_slot = 4,
        .proj_linear_slot = 5,
        .post_norm_slot = 3,
        .hidden_size = hidden_size,
        .ple_hidden_size = ple_hidden_size,
        .eps = eps,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
    })) orelse return error.UnexpectedNull;
    defer decomposed.deinit();

    var layer_plan = metal_command_planner.GatedLayerCommandLowerer{};
    try layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = false,
        .attention_pre_norm_slot = 0,
        .q_linear_slot = 100,
        .k_linear_slot = 101,
        .v_linear_slot = 102,
        .q_head_norm_slot = 0,
        .k_head_norm_slot = 0,
        .attention_layer_index = 0,
        .value_norm = false,
        .kv_seed = false,
        .attention_linear_slot = 0,
        .attention_post_norm_slot = 0,
        .ffn_pre_norm_slot = 1,
        .gate_linear_slot = 1,
        .up_linear_slot = 2,
        .down_linear_slot = 3,
        .ffn_post_norm_slot = 2,
        .ple_gate_linear_slot = 4,
        .ple_proj_linear_slot = 5,
        .ple_post_norm_slot = 3,
        .source = @intFromEnum(ComputeSource.layer),
        .region = @intFromEnum(ComputeRegion.layer),
        .rows = 1,
        .kv_len = kv_tokens,
        .hidden_size = hidden_size,
        .attention_input_size = attention_input_size,
        .kv_dim = num_kv_heads * head_dim,
        .head_dim = head_dim,
        .attention_kv_format = .f32,
        .attention_storage = .dense,
        .intermediate_size = intermediate_size,
        .ple_hidden_size = ple_hidden_size,
    });
    var planned_ops = [_]u16{0} ** 16;
    var planned_barriers = [_]u8{0} ** 16;
    var planned_dispatches = [_]u8{255} ** 16;
    var planned_command_ops = [_]ops.PlannedCommandOp{.{}} ** 16;
    const planned_contract = plannedContractFromCommandPlan(layer_plan.commandView(), &planned_ops, &planned_barriers, &planned_dispatches, &planned_command_ops, 3);

    try std.testing.expect(decoderRuntimeReservePrefillLayerScratch(
        &provider,
        1,
        num_heads,
        num_kv_heads,
        head_dim,
        hidden_size,
        @max(intermediate_size, ple_hidden_size),
        0,
    ));
    try beginFrame(runtime);
    var planned = (try runAttentionF32GatedDecoderBlockQuantizedDevice(&provider, .{
        .query_sequence_len = 1,
        .kv_tokens = kv_tokens,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .query_position_offset = kv_tokens - 1,
        .kv_position_offset = 0,
        .sliding_window = 0,
        .total_sequence_len = kv_tokens,
        .attention_linear_slot = 0,
        .attention_pre_linear_rms_norm_slot = @as(?usize, null),
        .attention_post_linear_rms_norm_slot = @as(?usize, 0),
        .hidden_size = hidden_size,
        .eps = eps,
        .ffn_layer_norm_slot = @as(?usize, null),
        .ffn_rms_norm_slot = @as(?usize, 1),
        .ffn_post_gate_rms_norm_slot = @as(?usize, null),
        .ffn_post_down_rms_norm_slot = @as(?usize, 2),
        .gate_ffn_linear_slot = 1,
        .up_ffn_linear_slot = 2,
        .down_ffn_linear_slot = 3,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .layer_index = 0,
        .ple = ple,
        .ple_gate_linear_slot = @as(?usize, 4),
        .ple_proj_linear_slot = @as(?usize, 5),
        .ple_post_norm_slot = @as(?usize, 3),
        .ple_hidden_size = ple_hidden_size,
        .planned_layer_contract = planned_contract,
    }, q, k, v, residual, &stats)) orelse return error.UnexpectedNull;
    defer planned.deinit();
    try submitFrame(runtime);
    try waitFrame(runtime);

    var decomposed_mut = decomposed;
    var planned_mut = planned;
    const expected = try tensorHostSlice(&decomposed_mut);
    const actual = try tensorHostSlice(&planned_mut);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-3);
    }

    const kv_dim = num_kv_heads * head_dim;
    const prior_tokens: usize = kv_tokens - 1;
    var k_prior = try testDeviceTensorFromSlice(runtime, k_data[0 .. prior_tokens * kv_dim], &[_]i32{ @intCast(prior_tokens), @intCast(kv_dim) });
    defer k_prior.deinit();
    var v_prior = try testDeviceTensorFromSlice(runtime, v_data[0 .. prior_tokens * kv_dim], &[_]i32{ @intCast(prior_tokens), @intCast(kv_dim) });
    defer v_prior.deinit();
    var k_suffix = try testDeviceTensorFromSlice(runtime, k_data[prior_tokens * kv_dim .. kv_tokens * kv_dim], &[_]i32{ 1, @intCast(kv_dim) });
    defer k_suffix.deinit();
    var v_suffix = try testDeviceTensorFromSlice(runtime, v_data[prior_tokens * kv_dim .. kv_tokens * kv_dim], &[_]i32{ 1, @intCast(kv_dim) });
    defer v_suffix.deinit();

    const raw_f32_kv_format: u32 = 2;
    const page_size: usize = 16;
    const key_row_bytes = kv_dim * @sizeOf(f32);
    const block_offsets = [_]u32{0};
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_reset_attention_span_slot(runtime, 0));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_update_attention_paged_from_f32_key_device_slot(
        runtime,
        0,
        raw_f32_kv_format,
        k_prior.deviceHandle(),
        k_prior.deviceByteOffset(),
        v_prior.deviceHandle(),
        v_prior.deviceByteOffset(),
        prior_tokens,
        prior_tokens,
        num_kv_heads,
        head_dim,
        key_row_bytes,
        key_row_bytes,
        kv_dim,
        0,
        &block_offsets,
        block_offsets.len,
        page_size,
    ));

    var paged_layer_plan = metal_command_planner.GatedLayerCommandLowerer{};
    try paged_layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = false,
        .attention_pre_norm_slot = 0,
        .q_linear_slot = 100,
        .k_linear_slot = 101,
        .v_linear_slot = 102,
        .q_head_norm_slot = 0,
        .k_head_norm_slot = 0,
        .attention_layer_index = 0,
        .value_norm = false,
        .kv_seed = true,
        .attention_linear_slot = 0,
        .attention_post_norm_slot = 0,
        .ffn_pre_norm_slot = 1,
        .gate_linear_slot = 1,
        .up_linear_slot = 2,
        .down_linear_slot = 3,
        .ffn_post_norm_slot = 2,
        .ple_gate_linear_slot = 4,
        .ple_proj_linear_slot = 5,
        .ple_post_norm_slot = 3,
        .source = @intFromEnum(ComputeSource.layer),
        .region = @intFromEnum(ComputeRegion.layer),
        .rows = 1,
        .kv_len = kv_tokens,
        .hidden_size = hidden_size,
        .attention_input_size = attention_input_size,
        .kv_dim = kv_dim,
        .head_dim = head_dim,
        .attention_kv_format = .f32,
        .attention_storage = .paged,
        .intermediate_size = intermediate_size,
        .ple_hidden_size = ple_hidden_size,
    });
    var paged_planned_ops = [_]u16{0} ** 16;
    var paged_planned_barriers = [_]u8{0} ** 16;
    var paged_planned_dispatches = [_]u8{255} ** 16;
    var paged_planned_command_ops = [_]ops.PlannedCommandOp{.{}} ** 16;
    const paged_planned_contract = plannedContractFromCommandPlan(
        paged_layer_plan.commandView(),
        &paged_planned_ops,
        &paged_planned_barriers,
        &paged_planned_dispatches,
        &paged_planned_command_ops,
        3,
    );

    const paged_layer = .{
        .slot = @as(usize, 0),
        .format = raw_f32_kv_format,
        .page_size_tokens = @as(u16, @intCast(page_size)),
        .key_row_bytes = key_row_bytes,
        .base_key_row_bytes = key_row_bytes,
        .v_row_stride = kv_dim,
    };
    try beginFrame(runtime);
    var planned_paged = (try runAttentionPagedGatedDecoderBlockQuantizedDevice(&provider, .{
        .query_sequence_len = 1,
        .kv_tokens = kv_tokens,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .query_position_offset = kv_tokens - 1,
        .kv_position_offset = 0,
        .sliding_window = 0,
        .total_sequence_len = kv_tokens,
        .attention_linear_slot = 0,
        .attention_pre_linear_rms_norm_slot = @as(?usize, null),
        .attention_post_linear_rms_norm_slot = @as(?usize, 0),
        .hidden_size = hidden_size,
        .eps = eps,
        .ffn_layer_norm_slot = @as(?usize, null),
        .ffn_rms_norm_slot = @as(?usize, 1),
        .ffn_post_gate_rms_norm_slot = @as(?usize, null),
        .ffn_post_down_rms_norm_slot = @as(?usize, 2),
        .gate_ffn_linear_slot = 1,
        .up_ffn_linear_slot = 2,
        .down_ffn_linear_slot = 3,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .layer_index = 0,
        .ple = ple,
        .ple_gate_linear_slot = @as(?usize, 4),
        .ple_proj_linear_slot = @as(?usize, 5),
        .ple_post_norm_slot = @as(?usize, 3),
        .ple_hidden_size = ple_hidden_size,
        .planned_layer_contract = paged_planned_contract,
    }, q, k_suffix, v_suffix, residual, paged_layer, block_offsets[0..], &stats)) orelse return error.UnexpectedNull;
    defer planned_paged.deinit();
    try submitFrame(runtime);
    try waitFrame(runtime);

    var planned_paged_mut = planned_paged;
    const paged_actual = try tensorHostSlice(&planned_paged_mut);
    try std.testing.expectEqual(expected.len, paged_actual.len);
    for (expected, paged_actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-3);
    }

    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_reset_attention_span_slot(runtime, 0));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_update_attention_paged_from_f32_key_device_slot(
        runtime,
        0,
        raw_f32_kv_format,
        k_prior.deviceHandle(),
        k_prior.deviceByteOffset(),
        v_prior.deviceHandle(),
        v_prior.deviceByteOffset(),
        prior_tokens,
        prior_tokens,
        num_kv_heads,
        head_dim,
        key_row_bytes,
        key_row_bytes,
        kv_dim,
        0,
        &block_offsets,
        block_offsets.len,
        page_size,
    ));
    var planned_paged_output = MetalTensor.deviceAllocate(
        @ptrCast(runtime),
        hidden_size * @sizeOf(f32),
        .private,
        &[_]i32{ 1, @intCast(hidden_size) },
    ) catch return error.UnexpectedNull;
    defer planned_paged_output.deinit();
    try beginFrame(runtime);
    try std.testing.expect(try runAttentionPagedGatedDecoderBlockQuantizedDeviceInto(&provider, .{
        .query_sequence_len = 1,
        .kv_tokens = kv_tokens,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .query_position_offset = kv_tokens - 1,
        .kv_position_offset = 0,
        .sliding_window = 0,
        .total_sequence_len = kv_tokens,
        .attention_linear_slot = 0,
        .attention_pre_linear_rms_norm_slot = @as(?usize, null),
        .attention_post_linear_rms_norm_slot = @as(?usize, 0),
        .hidden_size = hidden_size,
        .eps = eps,
        .ffn_layer_norm_slot = @as(?usize, null),
        .ffn_rms_norm_slot = @as(?usize, 1),
        .ffn_post_gate_rms_norm_slot = @as(?usize, null),
        .ffn_post_down_rms_norm_slot = @as(?usize, 2),
        .gate_ffn_linear_slot = 1,
        .up_ffn_linear_slot = 2,
        .down_ffn_linear_slot = 3,
        .intermediate_size = intermediate_size,
        .activation = @as(ops.DecoderRuntimeActivationKind, .gelu_new),
        .layer_index = 0,
        .ple = ple,
        .ple_gate_linear_slot = @as(?usize, 4),
        .ple_proj_linear_slot = @as(?usize, 5),
        .ple_post_norm_slot = @as(?usize, 3),
        .ple_hidden_size = ple_hidden_size,
        .planned_layer_contract = paged_planned_contract,
    }, q, k_suffix, v_suffix, residual, paged_layer, block_offsets[0..], planned_paged_output, &stats));
    try submitFrame(runtime);
    try waitFrame(runtime);

    var planned_paged_output_mut = planned_paged_output;
    const paged_into_actual = try tensorHostSlice(&planned_paged_output_mut);
    try std.testing.expectEqual(expected.len, paged_into_actual.len);
    for (expected, paged_into_actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-3);
    }
}

test "metal native decoder runtime absolute embeddings" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const tok_data = [_]f32{
        1.0,  2.0,  3.0,
        10.0, 20.0, 30.0,
    };
    const pos_data = [_]f32{
        0.5, 0.25, 0.125,
        1.0, 1.5,  2.0,
    };
    var tok = try MetalTensor.ownedCloneFrom(&tok_data, &[_]i32{ 2, 3 });
    defer tok.deinit();
    var pos = try MetalTensor.ownedCloneFrom(&pos_data, &[_]i32{ 2, 3 });
    defer pos.deinit();

    try std.testing.expect(try decoderRuntimePrepareAbsoluteEmbeddings(&provider, .{
        .token_embedding = tok,
        .position_embedding = pos,
        .vocab_size = 2,
        .max_position_embeddings = 2,
        .hidden_size = 3,
    }));

    var stats: ops.NativeQuantTimingStats = .{};
    var embedded = (try decoderRuntimeEmbedAbsolutePosition(&provider, .{
        .token_id = 1,
        .position_id = 1,
        .hidden_size = 3,
    }, &stats)) orelse return error.UnexpectedNull;
    defer embedded.deinit();

    var embedded_mut = embedded;
    const out = try tensorHostSlice(&embedded_mut);
    try std.testing.expectEqual(@as(usize, 3), out.len);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 11.0, 21.5, 32.0 }, out);
}

test "metal native decoder runtime embedding lookup" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const weight_data = [_]f32{
        1.0, 2.0,  3.0,  4.0,
        5.0, 6.0,  7.0,  8.0,
        9.0, 10.0, 11.0, 12.0,
    };
    var weight = try MetalTensor.ownedCloneFrom(&weight_data, &[_]i32{ 3, 4 });
    defer weight.deinit();

    var looked_up = (try decoderRuntimeEmbeddingLookup(&provider, .{
        .weight = weight,
        .ids = &[_]i64{ 2, 0 },
        .total = 2,
        .dim = 4,
    })) orelse return error.UnexpectedNull;
    defer looked_up.deinit();

    var looked_up_mut = looked_up;
    const out = try tensorHostSlice(&looked_up_mut);
    try std.testing.expectEqual(@as(usize, 8), out.len);
    try std.testing.expectEqualSlices(f32, &[_]f32{
        9.0, 10.0, 11.0, 12.0,
        1.0, 2.0,  3.0,  4.0,
    }, out);
}

test "metal native decoder runtime rope matches reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const input_data = [_]f32{
        1.0, 0.0, 0.0, 1.0,
        1.0, 0.0, 0.0, 1.0,
    };
    var input = try MetalTensor.ownedCloneFrom(&input_data, &[_]i32{ 2, 4 });
    defer input.deinit();

    var rotated = (try decoderRuntimeApplyRope(&provider, .{
        .input = input,
        .positions = &[_]usize{ 0, 1 },
        .head_dim = 4,
        .rope_dim = 4,
        .theta = 10000.0,
        .freq_scale = 1.0,
        .consecutive_pairs = false,
    })) orelse return error.UnexpectedNull;
    defer rotated.deinit();

    var rotated_mut = rotated;
    const out = try tensorHostSlice(&rotated_mut);
    const freq1 = 1.0 / std.math.pow(f32, 10000.0, 2.0 / 4.0);
    const expected = [_]f32{
        1.0,                 0.0,          0.0,                 1.0,
        @cos(@as(f32, 1.0)), -@sin(freq1), @sin(@as(f32, 1.0)), @cos(freq1),
    };
    try std.testing.expectEqual(expected.len, out.len);
    for (expected, out) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-5);
    }
}

test "metal native decoder runtime rope matches partial-rope reference" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const input_data = [_]f32{
        1.0, 0.0, 0.0, 1.0, 10.0, 11.0, 12.0, 13.0,
        1.0, 0.0, 0.0, 1.0, 20.0, 21.0, 22.0, 23.0,
    };
    var input = try MetalTensor.ownedCloneFrom(&input_data, &[_]i32{ 2, 8 });
    defer input.deinit();

    var rotated = (try decoderRuntimeApplyRope(&provider, .{
        .input = input,
        .positions = &[_]usize{ 0, 1 },
        .head_dim = 8,
        .rope_dim = 4,
        .theta = 10000.0,
        .freq_scale = 1.0,
        .consecutive_pairs = false,
    })) orelse return error.UnexpectedNull;
    defer rotated.deinit();

    var rotated_mut = rotated;
    const out = try tensorHostSlice(&rotated_mut);
    const freq1 = 1.0 / std.math.pow(f32, 10000.0, 2.0 / 4.0);
    const expected = [_]f32{
        1.0,                                              0.0,                 0.0, 1.0, 10.0,                                             11.0,               12.0, 13.0,
        @cos(@as(f32, 1.0)) - 20.0 * @sin(@as(f32, 1.0)), -21.0 * @sin(freq1), 0.0, 1.0, @sin(@as(f32, 1.0)) + 20.0 * @cos(@as(f32, 1.0)), 21.0 * @cos(freq1), 22.0, 23.0,
    };
    try std.testing.expectEqual(expected.len, out.len);
    for (expected, out) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-5);
    }
}

fn testDeviceTensorFromSlice(runtime: *RawMetalDecodeRuntime, data: []const f32, dims: []const i32) !MetalTensor {
    var tensor = try MetalTensor.deviceAllocate(runtime, data.len * @sizeOf(f32), .private, dims);
    errdefer tensor.deinit();
    const rc = termite_metal_buffer_upload(
        runtime,
        tensor.deviceHandle(),
        tensor.deviceByteOffset(),
        @ptrCast(data.ptr),
        data.len * @sizeOf(f32),
    );
    if (rc != 0) return error.MetalBufferUploadFailed;
    return tensor;
}

test "metal native decoder runtime prepares rms norm from device weight without host download" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const hidden_size: usize = 32;
    const weight_data = [_]f32{1.0} ** hidden_size;
    var weight = try testDeviceTensorFromSlice(runtime, &weight_data, &[_]i32{@intCast(hidden_size)});
    defer weight.deinit();

    metal_tensor.resetMemoryStats();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .slot = 0,
        .weight = weight,
        .hidden_size = hidden_size,
    }));
    const stats = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.to_host_device_calls);
    try std.testing.expectEqual(@as(u64, 0), stats.host_mirror_allocations);
    try std.testing.expect(decoderRuntimeRmsNormSlotPrepared(&provider, 0, hidden_size));
}

test "metal native decoder runtime embedding lookup from device weight" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const weight_data = [_]f32{
        1.0, 2.0,  3.0,  4.0,
        5.0, 6.0,  7.0,  8.0,
        9.0, 10.0, 11.0, 12.0,
    };
    var weight = try testDeviceTensorFromSlice(runtime, &weight_data, &[_]i32{ 3, 4 });
    defer weight.deinit();

    var looked_up = (try decoderRuntimeEmbeddingLookup(&provider, .{
        .weight = weight,
        .ids = &[_]i64{ 1, 2 },
        .total = 2,
        .dim = 4,
    })) orelse return error.UnexpectedNull;
    defer looked_up.deinit();

    try std.testing.expect(looked_up.isDevice());
    var looked_up_mut = looked_up;
    const out = try tensorHostSlice(&looked_up_mut);
    try std.testing.expectEqualSlices(f32, &[_]f32{
        5.0, 6.0,  7.0,  8.0,
        9.0, 10.0, 11.0, 12.0,
    }, out);
}

test "metal native decoder runtime attention device bias mask matches host" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const q_data = [_]f32{
        0.25, -0.5, 0.75, 1.0,
        -0.1, 0.4,  -0.3, 0.8,
    };
    const k_data = [_]f32{
        0.2,  0.1,
        -0.4, 0.6,
    };
    const v_data = [_]f32{
        0.3,  0.7,
        -0.2, 0.5,
    };
    const bias_data = [_]f32{
        0.0,  -0.25,
        0.15, 0.0,
        0.05, 0.1,
        -0.2, 0.0,
    };
    const mask = [_]u8{
        1, 1,
        1, 1,
    };

    var q_host = try MetalTensor.ownedCloneFrom(&q_data, &[_]i32{ 2, 4 });
    defer q_host.deinit();
    var k_host = try MetalTensor.ownedCloneFrom(&k_data, &[_]i32{ 2, 2 });
    defer k_host.deinit();
    var v_host = try MetalTensor.ownedCloneFrom(&v_data, &[_]i32{ 2, 2 });
    defer v_host.deinit();
    var bias_host = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{ 2, 2, 2 });
    defer bias_host.deinit();

    var host_out = (try decoderRuntimeApplyAttentionF32(&provider, .{
        .q = q_host,
        .k = k_host,
        .v = v_host,
        .bias = bias_host,
        .attn_or_mask = &mask,
        .q_len = 2,
        .kv_len = 2,
        .num_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 2,
        .query_position_offset = 0,
        .kv_position_offset = 0,
        .sliding_window = 0,
        .total_sequence_len = 2,
    })) orelse return error.UnexpectedNull;
    defer host_out.deinit();

    var q_device = try testDeviceTensorFromSlice(runtime, &q_data, &[_]i32{ 2, 4 });
    defer q_device.deinit();
    var k_device = try testDeviceTensorFromSlice(runtime, &k_data, &[_]i32{ 2, 2 });
    defer k_device.deinit();
    var v_device = try testDeviceTensorFromSlice(runtime, &v_data, &[_]i32{ 2, 2 });
    defer v_device.deinit();
    var bias_device = try testDeviceTensorFromSlice(runtime, &bias_data, &[_]i32{ 2, 2, 2 });
    defer bias_device.deinit();

    var device_out = (try decoderRuntimeApplyAttentionF32(&provider, .{
        .q = q_device,
        .k = k_device,
        .v = v_device,
        .bias = bias_device,
        .attn_or_mask = &mask,
        .q_len = 2,
        .kv_len = 2,
        .num_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 2,
        .query_position_offset = 0,
        .kv_position_offset = 0,
        .sliding_window = 0,
        .total_sequence_len = 2,
    })) orelse return error.UnexpectedNull;
    defer device_out.deinit();

    try std.testing.expect(device_out.isDevice());
    var host_out_mut = host_out;
    var device_out_mut = device_out;
    const host_values = try tensorHostSlice(&host_out_mut);
    const device_values = try tensorHostSlice(&device_out_mut);
    try std.testing.expectEqual(host_values.len, device_values.len);
    for (host_values, device_values) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
    }
}

test "metal native decoder runtime dense linear and rms-linear preserve device tensors" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const hidden_size: usize = 4;
    const out_dim: usize = 3;

    const rms_weight_data = [_]f32{ 1.0, 1.0, 1.0, 1.0 };
    var rms_weight = try MetalTensor.ownedCloneFrom(&rms_weight_data, &[_]i32{@intCast(hidden_size)});
    defer rms_weight.deinit();
    try std.testing.expect(try decoderRuntimePrepareRmsNorm(&provider, .{
        .slot = 0,
        .weight = rms_weight,
        .hidden_size = hidden_size,
    }));

    const linear_weight_data = [_]f32{
        1.0, 0.0, 0.0, 0.0,
        0.0, 1.0, 0.0, 0.0,
        0.5, 0.5, 0.5, 0.5,
    };
    const linear_bias_data = [_]f32{ 0.0, 0.0, 0.0 };
    var linear_weight = try MetalTensor.ownedCloneFrom(&linear_weight_data, &[_]i32{ @intCast(out_dim), @intCast(hidden_size) });
    defer linear_weight.deinit();
    var linear_bias = try MetalTensor.ownedCloneFrom(&linear_bias_data, &[_]i32{@intCast(out_dim)});
    defer linear_bias.deinit();
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .slot = 0,
        .weight = linear_weight,
        .bias = linear_bias,
        .quantized_storage = null,
        .in_dim = hidden_size,
        .out_dim = out_dim,
        .retain_dense_fallback = true,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .slot = 1,
        .weight = linear_weight,
        .bias = linear_bias,
        .quantized_storage = null,
        .in_dim = hidden_size,
        .out_dim = out_dim,
        .retain_dense_fallback = true,
    }, &prep_stats));
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .slot = 2,
        .weight = linear_weight,
        .bias = linear_bias,
        .quantized_storage = null,
        .in_dim = hidden_size,
        .out_dim = out_dim,
        .retain_dense_fallback = true,
    }, &prep_stats));

    const input_data = [_]f32{ 1.0, -2.0, 0.5, 3.0 };
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ 1, @intCast(hidden_size) });
    defer input.deinit();

    var linear_out = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer linear_out.deinit();
    try std.testing.expect(linear_out.isDevice());

    var rms_linear_out = (try decoderRuntimeApplyRmsNormLinear(&provider, .{
        .norm_slot = 0,
        .linear_slot = 0,
        .input = input,
        .hidden_size = hidden_size,
        .eps = 1e-5,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer rms_linear_out.deinit();
    try std.testing.expect(rms_linear_out.isDevice());

    var activation_stats: ops.NativeQuantTimingStats = .{};
    var activated = (try decoderRuntimeApplyActivation(&provider, .{
        .kind = @as(ops.DecoderRuntimeActivationKind, .relu),
        .input = rms_linear_out,
        .dim = out_dim,
    }, &activation_stats)) orelse return error.UnexpectedNull;
    defer activated.deinit();
    try std.testing.expect(activated.isDevice());

    var added = (try decoderRuntimeApplyAdd(&provider, .{
        .lhs = rms_linear_out,
        .rhs = linear_out,
        .dim = out_dim,
    }, &activation_stats)) orelse return error.UnexpectedNull;
    defer added.deinit();
    try std.testing.expect(added.isDevice());

    var rms_added = (try decoderRuntimeApplyRmsNormAdd(&provider, .{
        .input = input,
        .norm_slot = 0,
        .residual = input,
        .rows = 1,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    }, &activation_stats)) orelse return error.UnexpectedNull;
    defer rms_added.deinit();
    try std.testing.expect(rms_added.isDevice());
    var rms_added_host: [hidden_size]f32 = undefined;
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_buffer_download(runtime, rms_added.deviceHandle(), rms_added.deviceByteOffset(), @ptrCast(&rms_added_host), hidden_size * @sizeOf(f32)));
    var sum_sq: f32 = 0.0;
    for (input_data) |value| sum_sq += value * value;
    const inv_rms = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(hidden_size)) + 1e-5);
    for (input_data, rms_added_host) |value, actual| {
        try std.testing.expectApproxEqAbs(value + value * inv_rms, actual, 1e-4);
    }

    var add_scaled = (try decoderRuntimeApplyAddScale(&provider, .{
        .lhs = rms_linear_out,
        .rhs = linear_out,
        .dim = out_dim,
        .scale = 0.25,
    }, &activation_stats)) orelse return error.UnexpectedNull;
    defer add_scaled.deinit();
    try std.testing.expect(add_scaled.isDevice());
    var added_host: [out_dim]f32 = undefined;
    var add_scaled_host: [out_dim]f32 = undefined;
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_buffer_download(runtime, added.deviceHandle(), added.deviceByteOffset(), @ptrCast(&added_host), out_dim * @sizeOf(f32)));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_buffer_download(runtime, add_scaled.deviceHandle(), add_scaled.deviceByteOffset(), @ptrCast(&add_scaled_host), out_dim * @sizeOf(f32)));
    for (added_host, add_scaled_host) |base, scaled| {
        try std.testing.expectApproxEqAbs(base * 0.25, scaled, 1e-4);
    }

    var scaled_add_scaled = (try decoderRuntimeApplyScaledAddScale(&provider, .{
        .lhs = rms_linear_out,
        .rhs = linear_out,
        .dim = out_dim,
        .lhs_scale = 0.5,
        .output_scale = 0.25,
    }, &activation_stats)) orelse return error.UnexpectedNull;
    defer scaled_add_scaled.deinit();
    try std.testing.expect(scaled_add_scaled.isDevice());
    var rms_host: [out_dim]f32 = undefined;
    var linear_host: [out_dim]f32 = undefined;
    var scaled_add_scaled_host: [out_dim]f32 = undefined;
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_buffer_download(runtime, rms_linear_out.deviceHandle(), rms_linear_out.deviceByteOffset(), @ptrCast(&rms_host), out_dim * @sizeOf(f32)));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_buffer_download(runtime, linear_out.deviceHandle(), linear_out.deviceByteOffset(), @ptrCast(&linear_host), out_dim * @sizeOf(f32)));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_buffer_download(runtime, scaled_add_scaled.deviceHandle(), scaled_add_scaled.deviceByteOffset(), @ptrCast(&scaled_add_scaled_host), out_dim * @sizeOf(f32)));
    for (rms_host, linear_host, scaled_add_scaled_host) |lhs, rhs, scaled| {
        try std.testing.expectApproxEqAbs((lhs * 0.5 + rhs) * 0.25, scaled, 1e-4);
    }

    var multiplied = (try decoderRuntimeApplyMultiply(&provider, activated, linear_out, out_dim)) orelse return error.UnexpectedNull;
    defer multiplied.deinit();
    try std.testing.expect(multiplied.isDevice());

    var pair = (try tryApplyDenseRuntimeLinearPair(&provider, 0, 1, input, 1, hidden_size, out_dim)) orelse return error.UnexpectedNull;
    defer pair.first.deinit();
    defer pair.second.deinit();
    try std.testing.expect(pair.first.isDevice());
    try std.testing.expect(pair.second.isDevice());

    var qkv = (try tryApplyDenseRuntimeLinearQkv(&provider, 0, 1, 2, input, 1, hidden_size, out_dim, out_dim)) orelse return error.UnexpectedNull;
    defer qkv.first.deinit();
    defer qkv.second.deinit();
    defer qkv.third.deinit();
    try std.testing.expect(qkv.first.isDevice());
    try std.testing.expect(qkv.second.isDevice());
    try std.testing.expect(qkv.third.isDevice());

    const input_rows_data = [_]f32{
        1.0,  -2.0, 0.5, 3.0,
        -0.5, 0.25, 2.0, 1.0,
    };
    var input_rows = try testDeviceTensorFromSlice(runtime, &input_rows_data, &[_]i32{ 2, @intCast(hidden_size) });
    defer input_rows.deinit();
    var rms_added_rows = (try decoderRuntimeApplyRmsNormAdd(&provider, .{
        .input = input_rows,
        .norm_slot = 0,
        .residual = input_rows,
        .rows = 2,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    }, &activation_stats)) orelse return error.UnexpectedNull;
    defer rms_added_rows.deinit();
    try std.testing.expect(rms_added_rows.isDevice());
    var rms_added_rows_mut = rms_added_rows;
    const rms_added_rows_host = try tensorHostSlice(&rms_added_rows_mut);
    try std.testing.expectEqual(input_rows_data.len, rms_added_rows_host.len);
    for (0..2) |row| {
        var row_sum_sq: f32 = 0.0;
        for (0..hidden_size) |col| {
            const value = input_rows_data[row * hidden_size + col];
            row_sum_sq += value * value;
        }
        const row_inv_rms = 1.0 / @sqrt(row_sum_sq / @as(f32, @floatFromInt(hidden_size)) + 1e-5);
        for (0..hidden_size) |col| {
            const value = input_rows_data[row * hidden_size + col];
            try std.testing.expectApproxEqAbs(value + value * row_inv_rms, rms_added_rows_host[row * hidden_size + col], 1e-4);
        }
    }
    var qkv_rows = (try tryApplyDenseRuntimeLinearQkv(&provider, 0, 1, 2, input_rows, 2, hidden_size, out_dim, out_dim)) orelse return error.UnexpectedNull;
    defer qkv_rows.first.deinit();
    defer qkv_rows.second.deinit();
    defer qkv_rows.third.deinit();
    try std.testing.expect(qkv_rows.first.isDevice());
    try std.testing.expect(qkv_rows.second.isDevice());
    try std.testing.expect(qkv_rows.third.isDevice());

    const reduce_hidden_size: usize = 128;
    const reduce_out_dim: usize = 128;
    const reduce_rows: usize = 2;
    const reduce_weight_data = try std.testing.allocator.alloc(f32, reduce_hidden_size * reduce_out_dim);
    defer std.testing.allocator.free(reduce_weight_data);
    @memset(reduce_weight_data, 0);
    for (0..reduce_out_dim) |idx| reduce_weight_data[idx * reduce_hidden_size + idx] = 1.0;
    const reduce_bias_data = try std.testing.allocator.alloc(f32, reduce_out_dim);
    defer std.testing.allocator.free(reduce_bias_data);
    @memset(reduce_bias_data, 0);
    var reduce_weight = try MetalTensor.ownedCloneFrom(reduce_weight_data, &[_]i32{ @intCast(reduce_out_dim), @intCast(reduce_hidden_size) });
    defer reduce_weight.deinit();
    var reduce_bias = try MetalTensor.ownedCloneFrom(reduce_bias_data, &[_]i32{@intCast(reduce_out_dim)});
    defer reduce_bias.deinit();
    for (3..6) |slot| {
        try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
            .slot = slot,
            .weight = reduce_weight,
            .bias = reduce_bias,
            .quantized_storage = null,
            .in_dim = reduce_hidden_size,
            .out_dim = reduce_out_dim,
            .retain_dense_fallback = true,
        }, &prep_stats));
    }
    const reduce_input_data = try std.testing.allocator.alloc(f32, reduce_rows * reduce_hidden_size);
    defer std.testing.allocator.free(reduce_input_data);
    for (reduce_input_data, 0..) |*value, idx| value.* = @floatFromInt(@as(i32, @intCast(idx % 17)) - 8);
    var reduce_input = try testDeviceTensorFromSlice(runtime, reduce_input_data, &[_]i32{ @intCast(reduce_rows), @intCast(reduce_hidden_size) });
    defer reduce_input.deinit();
    var reduce_qkv = (try tryApplyDenseRuntimeLinearQkv(&provider, 3, 4, 5, reduce_input, reduce_rows, reduce_hidden_size, reduce_out_dim, reduce_out_dim)) orelse return error.UnexpectedNull;
    defer reduce_qkv.first.deinit();
    defer reduce_qkv.second.deinit();
    defer reduce_qkv.third.deinit();
    try std.testing.expect(reduce_qkv.first.isDevice());
    try std.testing.expect(reduce_qkv.second.isDevice());
    try std.testing.expect(reduce_qkv.third.isDevice());
    var reduce_q_mut = reduce_qkv.first;
    const reduce_q_host = try tensorHostSlice(&reduce_q_mut);
    try std.testing.expectEqual(reduce_input_data.len, reduce_q_host.len);
    for (reduce_input_data, reduce_q_host) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
    }

    try beginFrame(runtime);
    try beginPlannedComputeScope(runtime, @intFromEnum(ComputeSource.dense_linear), .ffn);
    var scoped_pair = (try tryApplyDenseRuntimeLinearPair(&provider, 0, 1, input, 1, hidden_size, out_dim)) orelse return error.UnexpectedNull;
    defer scoped_pair.first.deinit();
    defer scoped_pair.second.deinit();
    var scoped_qkv = (try tryApplyDenseRuntimeLinearQkv(&provider, 0, 1, 2, input, 1, hidden_size, out_dim, out_dim)) orelse return error.UnexpectedNull;
    defer scoped_qkv.first.deinit();
    defer scoped_qkv.second.deinit();
    defer scoped_qkv.third.deinit();
    try plannedComputeBarrier(runtime);
    var scoped_activation_stats: ops.NativeQuantTimingStats = .{};
    var scoped_activated = (try decoderRuntimeApplyActivation(&provider, .{
        .kind = @as(ops.DecoderRuntimeActivationKind, .relu),
        .input = scoped_pair.first,
        .dim = out_dim,
    }, &scoped_activation_stats)) orelse return error.UnexpectedNull;
    defer scoped_activated.deinit();
    try plannedComputeBarrier(runtime);
    var scoped_multiplied = (try decoderRuntimeApplyMultiply(&provider, scoped_activated, scoped_pair.second, out_dim)) orelse return error.UnexpectedNull;
    defer scoped_multiplied.deinit();
    try plannedComputeBarrier(runtime);
    var scoped_added = (try decoderRuntimeApplyAdd(&provider, .{
        .lhs = scoped_multiplied,
        .rhs = scoped_qkv.first,
        .dim = out_dim,
    }, &scoped_activation_stats)) orelse return error.UnexpectedNull;
    defer scoped_added.deinit();
    try endPlannedComputeScope(runtime);
    try submitFrame(runtime);
    try waitFrame(runtime);
    const planned_snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(@as(u64, 1), planned_snapshot.last_frame_compute_encoder_count);
    try std.testing.expectEqual(@as(u64, 1), planned_snapshot.last_frame_planned_compute_scope_count);
    try std.testing.expectEqual(@as(u64, 1), planned_snapshot.last_frame_compute_dense_linear_count);
    try std.testing.expectEqual(@as(u64, 1), planned_snapshot.last_frame_compute_region_ffn_count);

    const linear_argmax = (try decoderRuntimeApplyLinearArgmax(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    try std.testing.expect(linear_argmax < out_dim);

    const logits_argmax = (try argmaxLogitsDevice(&provider, linear_out, out_dim)) orelse return error.UnexpectedNull;
    try std.testing.expect(logits_argmax < out_dim);

    const token_id = (try decoderRuntimeApplyRmsNormLinearArgmax(&provider, .{
        .norm_slot = 0,
        .linear_slot = 0,
        .input = input,
        .hidden_size = hidden_size,
        .eps = 1e-5,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    try std.testing.expect(token_id < out_dim);
}

test "metal native decoder runtime can prepare bf16 linear without copying model-owned bytes" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const hidden_size: usize = 4;
    const out_dim: usize = 3;
    const bf16_weight_words = [_]u16{
        0x3f80, 0x0000, 0x0000, 0x0000,
        0x0000, 0x3f80, 0x0000, 0x0000,
        0x3f00, 0x3f00, 0x3f00, 0x3f00,
    };
    const bf16_weight_bytes = std.mem.sliceAsBytes(&bf16_weight_words);
    const bias_data = [_]f32{ 0.0, 0.0, 0.0 };
    var linear_bias = try MetalTensor.ownedCloneFrom(&bias_data, &[_]i32{@intCast(out_dim)});
    defer linear_bias.deinit();

    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .slot = 0,
        .weight = dummy_weight,
        .bias = linear_bias,
        .quantized_storage = null,
        .in_dim = hidden_size,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
        .dense_bf16_bytes = bf16_weight_bytes,
        .dense_bf16_no_copy_safe = true,
    }, &prep_stats));

    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const input_data = [_]f32{ 1.0, -2.0, 0.5, 3.0 };
    var input = try testDeviceTensorFromSlice(runtime, &input_data, &[_]i32{ 1, @intCast(hidden_size) });
    defer input.deinit();

    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(@as(usize, out_dim), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), actual[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, -2.0), actual[1], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 1.25), actual[2], 1e-3);
}

test "metal native decoder runtime bf16 multi-row linear matches identity projection" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const rows: usize = 3;
    const hidden_size: usize = 128;
    const out_dim: usize = 128;
    const bf16_one: u16 = 0x3f80;

    var bf16_weight_words = try allocator.alloc(u16, hidden_size * out_dim);
    defer allocator.free(bf16_weight_words);
    @memset(bf16_weight_words, 0);
    for (0..out_dim) |out| {
        bf16_weight_words[out * hidden_size + out] = bf16_one;
    }
    const bf16_weight_bytes = std.mem.sliceAsBytes(bf16_weight_words);

    const bias_data = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_data);
    @memset(bias_data, 0.0);
    var linear_bias = try MetalTensor.ownedCloneFrom(bias_data, &[_]i32{@intCast(out_dim)});
    defer linear_bias.deinit();

    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    var prep_stats: ops.NativeQuantTimingStats = .{};
    try std.testing.expect(try decoderRuntimePrepareLinear(&provider, .{
        .slot = 0,
        .weight = dummy_weight,
        .bias = linear_bias,
        .quantized_storage = null,
        .in_dim = hidden_size,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
        .dense_bf16_bytes = bf16_weight_bytes,
        .dense_bf16_no_copy_safe = true,
    }, &prep_stats));

    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const input_data = try allocator.alloc(f32, rows * hidden_size);
    defer allocator.free(input_data);
    for (input_data, 0..) |*value, index| {
        const row = index / hidden_size;
        const col = index % hidden_size;
        value.* = @as(f32, @floatFromInt(row * 1000 + col));
    }
    var input = try testDeviceTensorFromSlice(runtime, input_data, &[_]i32{ @intCast(rows), @intCast(hidden_size) });
    defer input.deinit();

    var output = (try decoderRuntimeApplyLinear(&provider, .{
        .slot = 0,
        .input = input,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    })) orelse return error.UnexpectedNull;
    defer output.deinit();

    var output_mut = output;
    const actual = try tensorHostSlice(&output_mut);
    try std.testing.expectEqual(rows * out_dim, actual.len);
    for (0..rows) |row| {
        for (0..out_dim) |col| {
            try std.testing.expectApproxEqAbs(input_data[row * hidden_size + col], actual[row * out_dim + col], 1e-3);
        }
    }
}

test "metal native decoder runtime activation scratch pool and hidden state" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    const runtime = provider.raw_decode_runtime;

    // Hidden-state ping-pong: front/back must be distinct live buffers, and
    // capacity/max_rows accessors must report what we reserved.
    const hidden_size: usize = 128;
    const max_prefill_rows: usize = 8;
    try reserveHiddenState(runtime, max_prefill_rows, hidden_size);
    try std.testing.expectEqual(max_prefill_rows, hiddenStateMaxRows(runtime));
    try std.testing.expectEqual(max_prefill_rows * hidden_size * @sizeOf(f32), hiddenStateCapacity(runtime));

    const front = hiddenStateBuffer(runtime, .front) orelse return error.UnexpectedNull;
    defer termite_metal_buffer_release(front);
    const back = hiddenStateBuffer(runtime, .back) orelse return error.UnexpectedNull;
    defer termite_metal_buffer_release(back);
    try std.testing.expect(front != back);

    const hidden_seed = try std.testing.allocator.alloc(f32, hidden_size * 2);
    defer std.testing.allocator.free(hidden_seed);
    for (hidden_seed, 0..) |*value, i| value.* = @floatFromInt(i);
    const hidden_shape = [_]i32{ 2, @intCast(hidden_size) };
    var seed_tensor = try testDeviceTensorFromSlice(runtime.?, hidden_seed, &hidden_shape);
    defer seed_tensor.deinit();
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_buffer_copy(
            runtime,
            seed_tensor.deviceHandle(),
            seed_tensor.deviceByteOffset(),
            front,
            0,
            seed_tensor.deviceByteLen(),
        ),
    );
    const hidden_seed_copy = try std.testing.allocator.alloc(f32, hidden_seed.len);
    defer std.testing.allocator.free(hidden_seed_copy);
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_buffer_download(
            runtime,
            front,
            0,
            @ptrCast(hidden_seed_copy.ptr),
            seed_tensor.deviceByteLen(),
        ),
    );
    try std.testing.expectEqualSlices(f32, hidden_seed, hidden_seed_copy);

    // Growing the reservation must keep max_rows current and not shrink capacity.
    const prev_capacity = hiddenStateCapacity(runtime);
    try reserveHiddenState(runtime, max_prefill_rows * 2, hidden_size);
    try std.testing.expectEqual(max_prefill_rows * 2, hiddenStateMaxRows(runtime));
    try std.testing.expect(hiddenStateCapacity(runtime) >= prev_capacity);

    // Scratch pool acquisition returns distinct handles until exhausted.
    const capacity = 16; // TERMITE_METAL_SCRATCH_POOL_CAPACITY
    var handles: [capacity]?*anyopaque = [_]?*anyopaque{null} ** capacity;
    var acquired: usize = 0;
    errdefer for (handles[0..acquired]) |h| {
        if (h) |ptr| releaseScratch(runtime, ptr);
    };
    for (0..capacity) |i| {
        handles[i] = try acquireScratch(runtime, 4096);
        acquired += 1;
        for (0..i) |j| {
            try std.testing.expect(handles[i] != handles[j]);
        }
    }
    try std.testing.expectError(ScratchAcquireError.ScratchPoolExhausted, acquireScratch(runtime, 4096));

    // Release then re-acquire reuses a previously-returned slot. The pool
    // scans linearly for the first idle slot with enough capacity, so
    // releasing slot 3 and re-acquiring must return the same buffer handle —
    // `__bridge_retained` yields the same pointer value for the same
    // underlying MTLBuffer across acquires.
    const released_handle: *anyopaque = handles[3].?;
    releaseScratch(runtime, released_handle);
    handles[3] = null;
    const reused = try acquireScratch(runtime, 4096);
    try std.testing.expectEqual(released_handle, reused);
    handles[3] = reused;
    releaseScratch(runtime, handles[3].?);
    handles[3] = null;

    // reset_state marks every scratch slot idle, so we can re-acquire the full pool.
    _ = termite_metal_decode_runtime_reset_state(runtime);
    for (handles[0..capacity]) |h| {
        if (h) |ptr| releaseScratch(runtime, ptr);
    }
    handles = [_]?*anyopaque{null} ** capacity;
    acquired = 0;

    // Inside an active/submitted frame, release retires the slot until the
    // frame is waited. Re-acquire must not hand out the same buffer while
    // queued GPU work may still reference it.
    const frame_scratch = try acquireScratch(runtime, 4096);
    try beginFrame(runtime);
    releaseScratch(runtime, frame_scratch);
    const during_frame_scratch = try acquireScratch(runtime, 4096);
    try std.testing.expect(during_frame_scratch != frame_scratch);
    try submitFrame(runtime);
    releaseScratch(runtime, during_frame_scratch);
    try waitFrame(runtime);
    const after_frame_scratch = try acquireScratch(runtime, 4096);
    try std.testing.expectEqual(frame_scratch, after_frame_scratch);
    releaseScratch(runtime, after_frame_scratch);

    for (0..capacity) |i| {
        handles[i] = try acquireScratch(runtime, 4096);
        acquired += 1;
    }
    for (handles[0..capacity]) |h| {
        if (h) |ptr| releaseScratch(runtime, ptr);
    }
}

test "metal native decoder runtime frame API batches device ops into one command buffer" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    // No frame active yet: submit/wait fail, begin succeeds, double-begin rejected.
    try std.testing.expect(!hasActiveFrame(runtime));
    try std.testing.expectError(FrameError.FrameNotActive, submitFrame(runtime));
    try std.testing.expectError(FrameError.FrameNotActive, waitFrame(runtime));
    try beginFrame(runtime);
    try std.testing.expect(hasActiveFrame(runtime));
    try std.testing.expectError(FrameError.FrameAlreadyActive, beginFrame(runtime));

    // Prepare two RMS-norm slots and encode both into the active frame. The
    // entrypoint returns 0 without committing, so the CB counter is still 0
    // after both encodes.
    const hidden_size: usize = 32;
    const weight_a = try std.testing.allocator.alloc(f32, hidden_size);
    defer std.testing.allocator.free(weight_a);
    @memset(weight_a, 1.0);
    const weight_b = try std.testing.allocator.alloc(f32, hidden_size);
    defer std.testing.allocator.free(weight_b);
    @memset(weight_b, 2.0);
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_prepare_rms_norm(runtime, 0, weight_a.ptr, hidden_size),
    );
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_prepare_rms_norm(runtime, 1, weight_b.ptr, hidden_size),
    );

    const input = try std.testing.allocator.alloc(f32, hidden_size);
    defer std.testing.allocator.free(input);
    for (input, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i + 1));
    const shape = [_]i32{ 1, @intCast(hidden_size) };
    var input_tensor = try testDeviceTensorFromSlice(runtime, input, &shape);
    defer input_tensor.deinit();
    var out_a = try MetalTensor.deviceAllocate(runtime, hidden_size * @sizeOf(f32), .private, &shape);
    defer out_a.deinit();
    var out_b = try MetalTensor.deviceAllocate(runtime, hidden_size * @sizeOf(f32), .private, &shape);
    defer out_b.deinit();

    const cb_count_before = frameCommandBufferCount(runtime);
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_apply_rms_norm_device(
            runtime,
            0,
            input_tensor.deviceHandle(),
            input_tensor.deviceByteOffset(),
            hidden_size,
            1e-5,
            out_a.deviceHandle(),
            out_a.deviceByteOffset(),
        ),
    );
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_apply_rms_norm_device(
            runtime,
            1,
            input_tensor.deviceHandle(),
            input_tensor.deviceByteOffset(),
            hidden_size,
            1e-5,
            out_b.deviceHandle(),
            out_b.deviceByteOffset(),
        ),
    );
    // No commit yet — counter unchanged.
    try std.testing.expectEqual(cb_count_before, frameCommandBufferCount(runtime));

    try submitFrame(runtime);
    try std.testing.expect(!hasActiveFrame(runtime));
    try std.testing.expectEqual(cb_count_before + 1, frameCommandBufferCount(runtime));
    try waitFrame(runtime);
    try std.testing.expectError(FrameError.FrameNotActive, waitFrame(runtime));

    // Verify the two RMS-norm results are sensible (weight_b = 2 * weight_a → out_b == 2 * out_a).
    const out_a_host = try std.testing.allocator.alloc(f32, hidden_size);
    defer std.testing.allocator.free(out_a_host);
    const out_b_host = try std.testing.allocator.alloc(f32, hidden_size);
    defer std.testing.allocator.free(out_b_host);
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_buffer_download(runtime, out_a.deviceHandle(), out_a.deviceByteOffset(), @ptrCast(out_a_host.ptr), hidden_size * @sizeOf(f32)),
    );
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_buffer_download(runtime, out_b.deviceHandle(), out_b.deviceByteOffset(), @ptrCast(out_b_host.ptr), hidden_size * @sizeOf(f32)),
    );
    for (out_a_host, out_b_host) |a, b| {
        try std.testing.expectApproxEqAbs(b, 2.0 * a, 1e-4);
    }

    // Second frame: single op, verify CB count bumps by exactly one.
    try beginFrame(runtime);
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_apply_rms_norm_device(
            runtime,
            0,
            input_tensor.deviceHandle(),
            input_tensor.deviceByteOffset(),
            hidden_size,
            1e-5,
            out_a.deviceHandle(),
            out_a.deviceByteOffset(),
        ),
    );
    try submitFrame(runtime);
    try waitFrame(runtime);
    try std.testing.expectEqual(cb_count_before + 2, frameCommandBufferCount(runtime));

    // reset_state clears an abandoned frame so subsequent callers can begin cleanly.
    try beginFrame(runtime);
    try std.testing.expect(hasActiveFrame(runtime));
    _ = termite_metal_decode_runtime_reset_state(runtime);
    try std.testing.expect(!hasActiveFrame(runtime));
    try beginFrame(runtime);
    try submitFrame(runtime);
    try waitFrame(runtime);
}

test "metal native decoder runtime active frame keeps common op params stable" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const metal_native_provider = @import("metal_native_provider.zig");
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const Compare = struct {
        fn close(label: []const u8, expected: []const f32, actual: []const f32) !void {
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual, 0..) |exp, got, i| {
                if (!std.math.isFinite(got) or !std.math.approxEqAbs(f32, exp, got, 2e-4)) {
                    std.debug.print("{s} mismatch idx={d} expected={d} got={d}\n", .{ label, i, exp, got });
                    return error.TestUnexpectedResult;
                }
            }
        }
    };

    const elem_count: usize = 64;
    var lhs_data: [elem_count]f32 = undefined;
    var rhs_data: [elem_count]f32 = undefined;
    for (&lhs_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 31)) * 0.125;
    for (&rhs_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 17)) - 8)) * 0.25;
    const elem_shape = [_]i32{ 1, @intCast(elem_count) };
    var lhs = try testDeviceTensorFromSlice(runtime, &lhs_data, &elem_shape);
    defer lhs.deinit();
    var rhs = try testDeviceTensorFromSlice(runtime, &rhs_data, &elem_shape);
    defer rhs.deinit();
    var add_ref = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, &elem_shape);
    defer add_ref.deinit();
    var add_frame = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, &elem_shape);
    defer add_frame.deinit();
    var mul_ref = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, &elem_shape);
    defer mul_ref.deinit();
    var mul_frame = try MetalTensor.deviceAllocate(runtime, elem_count * @sizeOf(f32), .private, &elem_shape);
    defer mul_frame.deinit();

    const table_rows: usize = 4;
    const table_dim: usize = 5;
    var table: [table_rows * table_dim]f32 = undefined;
    for (&table, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i)) * 0.0625;
    var ids = [_]u32{ 3, 1, 2 };
    const embed_shape = [_]i32{ @intCast(ids.len), @intCast(table_dim) };
    var embed_ref = try MetalTensor.deviceAllocate(runtime, ids.len * table_dim * @sizeOf(f32), .private, embed_shape[0..]);
    defer embed_ref.deinit();
    var embed_frame = try MetalTensor.deviceAllocate(runtime, ids.len * table_dim * @sizeOf(f32), .private, embed_shape[0..]);
    defer embed_frame.deinit();

    const rope_chunks: usize = 3;
    const head_dim: usize = 8;
    var rope_data: [rope_chunks * head_dim]f32 = undefined;
    for (&rope_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 11));
    const rope_shape = [_]i32{ @intCast(rope_chunks), @intCast(head_dim) };
    var positions = [_]u32{ 0, 2, 5 };
    var rope_input = try testDeviceTensorFromSlice(runtime, &rope_data, &rope_shape);
    defer rope_input.deinit();
    var rope_ref = try MetalTensor.deviceAllocate(runtime, rope_data.len * @sizeOf(f32), .private, &rope_shape);
    defer rope_ref.deinit();
    var rope_frame = try MetalTensor.deviceAllocate(runtime, rope_data.len * @sizeOf(f32), .private, &rope_shape);
    defer rope_frame.deinit();
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_rope_device(runtime, rope_input.deviceHandle(), rope_input.deviceByteOffset(), positions[0..].ptr, rope_chunks, head_dim, 4, 10000.0, 1.0, 1, rope_ref.deviceHandle(), rope_ref.deviceByteOffset()));

    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_add_device(runtime, lhs.deviceHandle(), lhs.deviceByteOffset(), rhs.deviceHandle(), rhs.deviceByteOffset(), elem_count, add_ref.deviceHandle(), add_ref.deviceByteOffset()));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_multiply_device(runtime, lhs.deviceHandle(), lhs.deviceByteOffset(), rhs.deviceHandle(), rhs.deviceByteOffset(), elem_count, mul_ref.deviceHandle(), mul_ref.deviceByteOffset()));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_prepare_embedding_table(runtime, table[0..].ptr, table_rows, table_dim));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_embedding_lookup_prepared_device(runtime, ids[0..].ptr, ids.len, table_dim, embed_ref.deviceHandle(), embed_ref.deviceByteOffset()));

    const norm_rows: usize = 2;
    const norm_hidden: usize = 8;
    var norm_input_data: [norm_rows * norm_hidden]f32 = undefined;
    var norm_weight: [norm_hidden]f32 = undefined;
    var norm_bias: [norm_hidden]f32 = undefined;
    for (&norm_input_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % norm_hidden)) - 3)) * 0.5;
    for (&norm_weight, 0..) |*value, i| value.* = 0.75 + @as(f32, @floatFromInt(i)) * 0.03125;
    for (&norm_bias, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 4)) * 0.015625;
    const norm_shape = [_]i32{ @intCast(norm_rows), @intCast(norm_hidden) };
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_prepare_layer_norm(runtime, 0, norm_weight[0..].ptr, norm_bias[0..].ptr, norm_hidden));
    var norm_input = try testDeviceTensorFromSlice(runtime, &norm_input_data, &norm_shape);
    defer norm_input.deinit();
    var norm_ref = try MetalTensor.deviceAllocate(runtime, norm_input_data.len * @sizeOf(f32), .private, &norm_shape);
    defer norm_ref.deinit();
    var norm_frame = try MetalTensor.deviceAllocate(runtime, norm_input_data.len * @sizeOf(f32), .private, &norm_shape);
    defer norm_frame.deinit();
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_layer_norm_device(runtime, 0, norm_input.deviceHandle(), norm_input.deviceByteOffset(), norm_rows, norm_hidden, 1e-5, norm_ref.deviceHandle(), norm_ref.deviceByteOffset()));

    try beginFrame(runtime);
    errdefer if (hasActiveFrame(runtime)) cancelFrame(runtime) catch {};
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_add_device(runtime, lhs.deviceHandle(), lhs.deviceByteOffset(), rhs.deviceHandle(), rhs.deviceByteOffset(), elem_count, add_frame.deviceHandle(), add_frame.deviceByteOffset()));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_multiply_device(runtime, lhs.deviceHandle(), lhs.deviceByteOffset(), rhs.deviceHandle(), rhs.deviceByteOffset(), elem_count, mul_frame.deviceHandle(), mul_frame.deviceByteOffset()));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_rope_device(runtime, rope_input.deviceHandle(), rope_input.deviceByteOffset(), positions[0..].ptr, rope_chunks, head_dim, 4, 10000.0, 1.0, 1, rope_frame.deviceHandle(), rope_frame.deviceByteOffset()));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_embedding_lookup_prepared_device(runtime, ids[0..].ptr, ids.len, table_dim, embed_frame.deviceHandle(), embed_frame.deviceByteOffset()));
    try std.testing.expectEqual(@as(c_int, 0), termite_metal_decode_runtime_apply_layer_norm_device(runtime, 0, norm_input.deviceHandle(), norm_input.deviceByteOffset(), norm_rows, norm_hidden, 1e-5, norm_frame.deviceHandle(), norm_frame.deviceByteOffset()));
    try submitFrame(runtime);
    try waitFrame(runtime);

    var add_ref_mut = add_ref;
    var add_frame_mut = add_frame;
    try Compare.close("add", try tensorHostSlice(&add_ref_mut), try tensorHostSlice(&add_frame_mut));
    var mul_ref_mut = mul_ref;
    var mul_frame_mut = mul_frame;
    try Compare.close("multiply", try tensorHostSlice(&mul_ref_mut), try tensorHostSlice(&mul_frame_mut));
    var rope_ref_mut = rope_ref;
    var rope_frame_mut = rope_frame;
    try Compare.close("rope", try tensorHostSlice(&rope_ref_mut), try tensorHostSlice(&rope_frame_mut));
    var embed_ref_mut = embed_ref;
    var embed_frame_mut = embed_frame;
    try Compare.close("embedding", try tensorHostSlice(&embed_ref_mut), try tensorHostSlice(&embed_frame_mut));
    var norm_ref_mut = norm_ref;
    var norm_frame_mut = norm_frame;
    try Compare.close("layer_norm", try tensorHostSlice(&norm_ref_mut), try tensorHostSlice(&norm_frame_mut));
}

test "metal native planned compute scope records scopes and barriers" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const runtime = termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.SkipZigTest;

    try std.testing.expectError(FrameError.FrameNotActive, beginPlannedComputeScope(runtime, 4, .ffn));
    try beginFrame(runtime);
    try beginPlannedComputeScope(runtime, 4, .ffn);
    // Adjacent planned scopes coalesce into a single encoder so frame-level
    // command plans can move across logical regions without ending encoding.
    try beginPlannedComputeScope(runtime, 4, .ffn);
    try plannedComputeBarrier(runtime);
    try plannedComputeBarrier(runtime);
    try submitFrame(runtime);
    try waitFrame(runtime);

    const snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(@as(u64, 1), snapshot.last_frame_compute_encoder_count);
    try std.testing.expectEqual(@as(u64, 2), snapshot.last_frame_planned_compute_scope_count);
    try std.testing.expectEqual(@as(u64, 2), snapshot.last_frame_planned_barrier_count);
    try std.testing.expectEqual(@as(u64, 2), snapshot.last_frame_compute_rms_norm_count);
    try std.testing.expectEqual(@as(u64, 2), snapshot.last_frame_compute_region_ffn_count);
}

test "planned compute sequence exports active typed contract" {
    var planned_ops = [_]metal_command_planner.PlannedOp{
        .{
            .kind = .qkv_linear,
            .op_index = 0,
            .scope_index = 0,
            .barrier_before = false,
        },
        .{
            .kind = .q_head_norm_rope,
            .op_index = 1,
            .scope_index = 0,
            .barrier_before = true,
        },
        .{
            .kind = .attention,
            .op_index = 2,
            .scope_index = 0,
            .barrier_before = true,
        },
    };
    var scopes = [_]metal_command_planner.EncoderScope{
        .{
            .first_op = 0,
            .op_count = planned_ops.len,
            .source = 11,
            .region = @intFromEnum(ComputeRegion.layer),
            .barrier_count = 2,
        },
    };
    var sequence = PlannedComputeSequence{
        .runtime = null,
        .plan = .{
            .planned_ops = &planned_ops,
            .scopes = &scopes,
            .barrier_count = 2,
        },
        .next_planned_op = 1,
        .active_scope_index = 0,
    };
    var op_storage = [_]u16{0} ** 3;
    var barrier_storage = [_]u8{0} ** 3;
    const contract = sequence.exportActiveContract(&op_storage, &barrier_storage);

    try std.testing.expectEqual(@as(usize, 1), contract.start_index);
    try std.testing.expectEqual(@as(usize, 3), contract.ops.len);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.qkv_linear), contract.ops[0]);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.q_head_norm_rope), contract.ops[1]);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.attention), contract.ops[2]);
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 1 }, contract.barriers);

    sequence.active_scope_index = null;
    const inactive = sequence.exportActiveContract(&op_storage, &barrier_storage);
    try std.testing.expectEqual(@as(usize, 0), inactive.ops.len);
    try std.testing.expectEqual(@as(usize, 0), inactive.barriers.len);
}

test "planned compute sequence exports active command contract" {
    var tail_plan = metal_command_planner.TailCommandLowerer{};
    try tail_plan.build(.{
        .final_norm_slot = 3,
        .lm_head_slot = 9,
        .source = @intFromEnum(ComputeSource.tail),
        .region = @intFromEnum(ComputeRegion.tail),
        .hidden_size = 2304,
        .vocab_size = 262144,
    });
    const command_view = tail_plan.commandView();
    var sequence = PlannedComputeSequence{
        .runtime = null,
        .plan = command_view.planView(),
        .command_plan = command_view,
        .next_planned_op = 1,
        .active_scope_index = 0,
    };
    var op_storage = [_]u16{0} ** 3;
    var barrier_storage = [_]u8{0} ** 3;
    var quant_dispatch_storage = [_]u8{0} ** 3;
    var command_op_storage = [_]ops.PlannedCommandOp{.{}} ** 3;
    const contract = sequence.exportActiveCommandContract(
        &op_storage,
        &barrier_storage,
        &quant_dispatch_storage,
        &command_op_storage,
    );

    try std.testing.expectEqual(@as(usize, 1), contract.start_index);
    try std.testing.expectEqual(@as(usize, 3), contract.ops.len);
    try std.testing.expectEqual(@as(usize, 3), contract.command_ops.len);
    try std.testing.expectEqual(@as(u8, 255), contract.command_ops[0].quant_dispatch);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.QuantMatmulDispatchKind.mmv), contract.command_ops[1].quant_dispatch);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.Operator.mul_mv), contract.command_ops[1].operator);
    try std.testing.expectEqual(@as(u8, @intCast(@intFromEnum(metal_command_planner.QuantMatmulFormat.q8_0))), contract.command_ops[1].format);
    try std.testing.expectEqual(@as(u8, 1), contract.command_ops[1].barrier_before);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.tail_lm_head), contract.command_ops[1].kind);

    sequence.active_scope_index = null;
    const inactive = sequence.exportActiveCommandContract(
        &op_storage,
        &barrier_storage,
        &quant_dispatch_storage,
        &command_op_storage,
    );
    try std.testing.expectEqual(@as(usize, 0), inactive.command_ops.len);
}

test "planned contract exports whole plan without active sequence state" {
    var planned_ops = [_]metal_command_planner.PlannedOp{
        .{
            .kind = .tail_final_norm,
            .op_index = 0,
            .scope_index = 0,
            .barrier_before = false,
        },
        .{
            .kind = .tail_lm_head,
            .op_index = 1,
            .scope_index = 0,
            .barrier_before = true,
        },
        .{
            .kind = .tail_argmax,
            .op_index = 2,
            .scope_index = 0,
            .barrier_before = true,
        },
    };
    var scopes = [_]metal_command_planner.EncoderScope{
        .{
            .first_op = 0,
            .op_count = planned_ops.len,
            .source = @intFromEnum(ComputeSource.tail),
            .region = @intFromEnum(ComputeRegion.tail),
            .barrier_count = 2,
        },
    };
    var op_storage = [_]u16{0} ** 3;
    var barrier_storage = [_]u8{0} ** 3;
    const contract = plannedContractFromPlan(
        .{
            .planned_ops = &planned_ops,
            .scopes = &scopes,
            .barrier_count = 2,
        },
        &op_storage,
        &barrier_storage,
        0,
    );

    try std.testing.expectEqual(@as(usize, 0), contract.start_index);
    try std.testing.expectEqual(@as(usize, 3), contract.ops.len);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.tail_final_norm), contract.ops[0]);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.tail_lm_head), contract.ops[1]);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.tail_argmax), contract.ops[2]);
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 1 }, contract.barriers);
}

test "planned command contract exports quant matmul dispatches" {
    var tail_plan = metal_command_planner.TailCommandLowerer{};
    try tail_plan.build(.{
        .final_norm_slot = 3,
        .lm_head_slot = 9,
        .source = @intFromEnum(ComputeSource.tail),
        .region = @intFromEnum(ComputeRegion.tail),
        .hidden_size = 2304,
        .vocab_size = 262144,
    });

    var op_storage = [_]u16{0} ** 3;
    var barrier_storage = [_]u8{0} ** 3;
    var quant_dispatch_storage = [_]u8{0} ** 3;
    var command_op_storage = [_]ops.PlannedCommandOp{.{}} ** 3;
    const contract = plannedContractFromCommandPlan(
        tail_plan.commandView(),
        &op_storage,
        &barrier_storage,
        &quant_dispatch_storage,
        &command_op_storage,
        0,
    );

    try std.testing.expectEqual(@as(usize, 3), contract.ops.len);
    try std.testing.expectEqual(@as(usize, 3), contract.quant_dispatches.len);
    try std.testing.expectEqual(@as(usize, 3), contract.command_ops.len);
    try std.testing.expectEqual(@as(u8, 255), contract.quant_dispatches[0]);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.QuantMatmulDispatchKind.mmv), contract.quant_dispatches[1]);
    try std.testing.expectEqual(@as(u8, 255), contract.quant_dispatches[2]);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.tail_lm_head), contract.command_ops[1].kind);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.QuantMatmulDispatchKind.mmv), contract.command_ops[1].quant_dispatch);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.Operator.mul_mv), contract.command_ops[1].operator);
    try std.testing.expectEqual(@as(u8, @intCast(@intFromEnum(metal_command_planner.QuantMatmulFormat.q8_0))), contract.command_ops[1].format);
    try std.testing.expectEqual(@as(u8, 1), contract.command_ops[1].barrier_before);
    try std.testing.expectEqual(@intFromEnum(ComputeSource.tail), contract.command_ops[1].source);
}

test "planned command contract exports activation dtypes" {
    var layer_plan = metal_command_planner.PrefillGatedLayerCommandLowerer{};
    try layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = false,
        .attention_pre_norm_slot = 0,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .activation_dtype = .f16,
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = @intFromEnum(ComputeSource.layer),
        .region = @intFromEnum(ComputeRegion.layer),
        .rows = 10,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
    });

    var op_storage = [_]u16{0} ** 32;
    var barrier_storage = [_]u8{0} ** 32;
    var quant_dispatch_storage = [_]u8{0} ** 32;
    var command_op_storage = [_]ops.PlannedCommandOp{.{}} ** 32;
    const contract = plannedContractFromCommandPlan(
        layer_plan.commandView(),
        &op_storage,
        &barrier_storage,
        &quant_dispatch_storage,
        &command_op_storage,
        0,
    );

    var found_gate_up = false;
    var found_down = false;
    for (contract.command_ops) |command| {
        if (command.kind == @intFromEnum(metal_command_planner.OpKind.ffn_gate_up_activation)) {
            found_gate_up = true;
            try std.testing.expectEqual(@intFromEnum(metal_command_planner.ActivationDType.f32), command.input_dtype);
            try std.testing.expectEqual(@intFromEnum(metal_command_planner.ActivationDType.f16), command.output_dtype);
        }
        if (command.kind == @intFromEnum(metal_command_planner.OpKind.ffn_down_linear)) {
            found_down = true;
            try std.testing.expectEqual(@intFromEnum(metal_command_planner.ActivationDType.f16), command.input_dtype);
            try std.testing.expectEqual(@intFromEnum(metal_command_planner.ActivationDType.f32), command.output_dtype);
        }
    }
    try std.testing.expect(found_gate_up);
    try std.testing.expect(found_down);
}

test "planned command contract storage exports global windows" {
    var tail_plan = metal_command_planner.TailCommandLowerer{};
    try tail_plan.build(.{
        .final_norm_slot = 3,
        .lm_head_slot = 9,
        .source = @intFromEnum(ComputeSource.tail),
        .region = @intFromEnum(ComputeRegion.tail),
        .hidden_size = 2304,
        .vocab_size = 262144,
    });

    var op_storage = [_]u16{0} ** 3;
    var barrier_storage = [_]u8{0} ** 3;
    var quant_dispatch_storage = [_]u8{0} ** 3;
    var command_op_storage = [_]ops.PlannedCommandOp{.{}} ** 3;
    const ok = populatePlannedCommandContractStorage(tail_plan.commandView(), .{
        .ops = &op_storage,
        .barriers = &barrier_storage,
        .quant_dispatches = &quant_dispatch_storage,
        .command_ops = &command_op_storage,
    });
    try std.testing.expect(ok);

    const contract = plannedContractWindowFromStorage(.{
        .ops = &op_storage,
        .barriers = &barrier_storage,
        .quant_dispatches = &quant_dispatch_storage,
        .command_ops = &command_op_storage,
    }, 1, 3);
    try std.testing.expectEqual(@as(usize, 1), contract.start_index);
    try std.testing.expectEqual(@as(usize, 3), contract.ops.len);
    try std.testing.expectEqual(@as(usize, 3), contract.command_ops.len);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.tail_lm_head), contract.command_ops[1].kind);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.OpKind.tail_argmax), contract.command_ops[2].kind);
    try std.testing.expectEqual(@intFromEnum(metal_command_planner.QuantMatmulDispatchKind.mmv), contract.command_ops[1].quant_dispatch);

    const empty = plannedContractWindowFromStorage(.{
        .ops = &op_storage,
        .barriers = &barrier_storage,
        .quant_dispatches = &quant_dispatch_storage,
        .command_ops = &command_op_storage,
    }, 3, 3);
    try std.testing.expectEqual(@as(usize, 0), empty.command_ops.len);
}

test "planned attention helpers reject stale command records" {
    const flash_f32 = ops.PlannedLayerContract{
        .command_ops = &.{
            .{
                .kind = @intFromEnum(metal_command_planner.OpKind.attention),
                .operator = @intFromEnum(metal_command_planner.Operator.attention_flash),
                .format = @intFromEnum(metal_command_planner.AttentionKvFormat.f32),
            },
        },
    };
    try std.testing.expect(plannedContractAllowsF32Attention(flash_f32));
    try std.testing.expect(!plannedContractAllowsPagedAttention(flash_f32));

    const paged_f32 = ops.PlannedLayerContract{
        .command_ops = &.{
            .{
                .kind = @intFromEnum(metal_command_planner.OpKind.attention),
                .operator = @intFromEnum(metal_command_planner.Operator.attention_paged),
                .format = @intFromEnum(metal_command_planner.AttentionKvFormat.f32),
            },
        },
    };
    try std.testing.expect(plannedContractAllowsPagedAttention(paged_f32));
    try std.testing.expect(!plannedContractAllowsF32Attention(paged_f32));

    const stale_linear = ops.PlannedLayerContract{
        .command_ops = &.{
            .{
                .kind = @intFromEnum(metal_command_planner.OpKind.attention_output_linear),
                .operator = @intFromEnum(metal_command_planner.Operator.mul_mv),
                .format = @intFromEnum(metal_command_planner.QuantMatmulFormat.q8_0),
            },
        },
    };
    try std.testing.expect(!plannedContractAllowsF32Attention(stale_linear));
    try std.testing.expect(!plannedContractAllowsPagedAttention(stale_linear));
}

test "metal native graph planner reserves sampled tail scratch up front" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const runtime = termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.SkipZigTest;

    const vocab_size: usize = 1024;
    const top_k: usize = 40;
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_sample_tail_scratch(runtime, vocab_size, top_k),
    );
    const snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_active);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_count);
    try std.testing.expectEqual(@as(u64, 4), snapshot.graph_plan_slots);
    try std.testing.expectEqual(@as(u64, 4), snapshot.graph_plan_allocations);
    try std.testing.expect(snapshot.graph_plan_bytes >= vocab_size * @sizeOf(f32));

    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_sample_tail_scratch(runtime, vocab_size / 2, top_k),
    );
    const reused_snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(snapshot.graph_plan_count, reused_snapshot.graph_plan_count);
    try std.testing.expectEqual(snapshot.graph_plan_slots, reused_snapshot.graph_plan_slots);
    try std.testing.expectEqual(snapshot.graph_plan_allocations, reused_snapshot.graph_plan_allocations);
}

test "metal native graph planner reserves compressed attention span scratch up front" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const runtime = termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.SkipZigTest;

    const kv_tokens: usize = 8;
    const key_row_bytes: usize = 96;
    const v_row_stride: usize = 128;
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_attention_span_scratch(
            runtime,
            kv_tokens,
            key_row_bytes,
            v_row_stride,
        ),
    );
    const snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_active);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_count);
    try std.testing.expectEqual(@as(u64, 2), snapshot.graph_plan_slots);
    try std.testing.expectEqual(@as(u64, 2), snapshot.graph_plan_allocations);
    try std.testing.expectEqual(@as(u64, 0), snapshot.attention_span_bytes);
    try std.testing.expect(snapshot.graph_plan_bytes >= kv_tokens * key_row_bytes);
    try std.testing.expect(snapshot.graph_plan_bytes >= kv_tokens * v_row_stride * @sizeOf(f32));

    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_attention_span_scratch(
            runtime,
            kv_tokens / 2,
            key_row_bytes,
            v_row_stride,
        ),
    );
    const reused_snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(snapshot.graph_plan_count, reused_snapshot.graph_plan_count);
    try std.testing.expectEqual(snapshot.graph_plan_slots, reused_snapshot.graph_plan_slots);
    try std.testing.expectEqual(snapshot.graph_plan_allocations, reused_snapshot.graph_plan_allocations);
}

test "metal native graph planner reserves prefill layer scratch up front" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const runtime = termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.SkipZigTest;

    const rows: usize = 10;
    const num_heads: usize = 8;
    const num_kv_heads: usize = 4;
    const head_dim: usize = 256;
    const hidden_size: usize = 2304;
    const intermediate_size: usize = 9216;
    const tail_vocab_size: usize = 1024;
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_prefill_layer_scratch(
            runtime,
            rows,
            num_heads,
            num_kv_heads,
            head_dim,
            hidden_size,
            intermediate_size,
            tail_vocab_size,
        ),
    );
    const snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_active);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_count);
    try std.testing.expectEqual(@as(u64, 21), snapshot.graph_plan_slots);
    try std.testing.expectEqual(@as(u64, 21), snapshot.graph_plan_allocations);
    try std.testing.expect(snapshot.graph_plan_bytes >= rows * intermediate_size * @sizeOf(f32));

    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_prefill_layer_scratch(
            runtime,
            rows * 2,
            num_heads,
            num_kv_heads,
            head_dim,
            hidden_size,
            intermediate_size,
            tail_vocab_size,
        ),
    );
    const reused_snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(snapshot.graph_plan_count, reused_snapshot.graph_plan_count);
    try std.testing.expectEqual(snapshot.graph_plan_slots, reused_snapshot.graph_plan_slots);
    try std.testing.expectEqual(snapshot.graph_plan_allocations, reused_snapshot.graph_plan_allocations);
}

test "metal native graph planner reserves gated ffn scratch up front" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metalDeviceAvailable()) return error.SkipZigTest;

    const runtime = termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer termite_metal_decode_runtime_destroy(runtime);
    if (termite_metal_decode_runtime_ready(runtime) == 0) return error.SkipZigTest;

    const rows: usize = 4;
    const hidden_size: usize = 256;
    const intermediate_size: usize = 1024;
    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_gated_ffn_scratch(
            runtime,
            rows,
            hidden_size,
            intermediate_size,
        ),
    );
    const snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_active);
    try std.testing.expectEqual(@as(u64, 1), snapshot.graph_plan_count);
    try std.testing.expectEqual(@as(u64, 9), snapshot.graph_plan_slots);
    try std.testing.expectEqual(@as(u64, 9), snapshot.graph_plan_allocations);

    try std.testing.expectEqual(
        @as(c_int, 0),
        termite_metal_decode_runtime_reserve_gated_ffn_scratch(
            runtime,
            rows / 2,
            hidden_size,
            intermediate_size,
        ),
    );
    const reused_snapshot = runtimeMemorySnapshot(runtime);
    try std.testing.expectEqual(snapshot.graph_plan_count, reused_snapshot.graph_plan_count);
    try std.testing.expectEqual(snapshot.graph_plan_slots, reused_snapshot.graph_plan_slots);
    try std.testing.expectEqual(snapshot.graph_plan_allocations, reused_snapshot.graph_plan_allocations);
}
