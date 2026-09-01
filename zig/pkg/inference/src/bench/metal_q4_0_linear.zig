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
const inference_internal = @import("inference_internal");
const metal_runtime = inference_internal.metal_runtime;
const metal_native_provider = inference_internal.metal_native_provider;
const MetalTensor = metal_runtime.MetalTensor;
const QuantizedStorage = metal_runtime.QuantizedStorage;
const quant_codec = inference_internal.gguf.quant_codec;
const ops = inference_internal.ops;

fn q4RowBytes(in_dim: usize) u64 {
    return @intCast(((in_dim + 31) / 32) * 18);
}

fn q6RowBytes(in_dim: usize) u64 {
    return @intCast(((in_dim + 255) / 256) * 210);
}

const Mode = enum {
    linear,
    q6_linear,
    q6_argmax,
    head_rope,
    pair,
    qkv,
    split_qkv,
    ffn,
    ple,

    fn parse(value: []const u8) !Mode {
        if (std.mem.eql(u8, value, "linear")) return .linear;
        if (std.mem.eql(u8, value, "q6-linear")) return .q6_linear;
        if (std.mem.eql(u8, value, "q6-argmax")) return .q6_argmax;
        if (std.mem.eql(u8, value, "head-rope")) return .head_rope;
        if (std.mem.eql(u8, value, "pair")) return .pair;
        if (std.mem.eql(u8, value, "qkv")) return .qkv;
        if (std.mem.eql(u8, value, "split-qkv")) return .split_qkv;
        if (std.mem.eql(u8, value, "ffn")) return .ffn;
        if (std.mem.eql(u8, value, "ple")) return .ple;
        return error.InvalidArgument;
    }

    fn name(self: Mode) []const u8 {
        return switch (self) {
            .linear => "linear",
            .q6_linear => "q6-linear",
            .q6_argmax => "q6-argmax",
            .head_rope => "head-rope",
            .pair => "pair",
            .qkv => "qkv",
            .split_qkv => "split-qkv",
            .ffn => "ffn",
            .ple => "ple",
        };
    }
};

const Q4MmRoute = enum {
    aligned,
    aligned_tail,
    unrolled,

    fn parse(value: []const u8) !Q4MmRoute {
        if (std.mem.eql(u8, value, "aligned")) return .aligned;
        if (std.mem.eql(u8, value, "aligned-tail")) return .aligned_tail;
        if (std.mem.eql(u8, value, "unrolled")) return .unrolled;
        return error.InvalidArgument;
    }
};

const Q4MmvVariant = enum {
    nr4_nsg2,
    nr8_nsg2,
    nr4_nsg4,
    nr8_nsg4,

    fn parse(value: []const u8) !Q4MmvVariant {
        if (std.mem.eql(u8, value, "nr4-nsg2")) return .nr4_nsg2;
        if (std.mem.eql(u8, value, "nr8-nsg2")) return .nr8_nsg2;
        if (std.mem.eql(u8, value, "nr4-nsg4")) return .nr4_nsg4;
        if (std.mem.eql(u8, value, "nr8-nsg4")) return .nr8_nsg4;
        return error.InvalidArgument;
    }

    fn name(self: Q4MmvVariant) []const u8 {
        return switch (self) {
            .nr4_nsg2 => "nr4-nsg2",
            .nr8_nsg2 => "nr8-nsg2",
            .nr4_nsg4 => "nr4-nsg4",
            .nr8_nsg4 => "nr8-nsg4",
        };
    }
};

const Q4PairMmRoute = enum {
    m32_n64_aligned,
    m32_n64_tail,
    m32_n32_aligned,
    m32_n32_tail,

    fn parse(value: []const u8) !Q4PairMmRoute {
        if (std.mem.eql(u8, value, "m32-n64-aligned")) return .m32_n64_aligned;
        if (std.mem.eql(u8, value, "m32-n64-tail")) return .m32_n64_tail;
        if (std.mem.eql(u8, value, "m32-n32-aligned")) return .m32_n32_aligned;
        if (std.mem.eql(u8, value, "m32-n32-tail")) return .m32_n32_tail;
        return error.InvalidArgument;
    }
};

const Config = struct {
    mode: Mode = .linear,
    rows: usize = 1,
    weight_slots: usize = 1,
    in_dim: usize = 2048,
    out_dim: usize = 2048,
    kv_out_dim: usize = 512,
    warmup_iters: usize = 20,
    measure_iters: usize = 200,
    ops_per_frame: usize = 1,
    expect_q4_route: ?Q4MmRoute = null,
    expect_q4_mmv_variant: ?Q4MmvVariant = null,
    expect_q4_mmv_auto: bool = false,
    expect_q4_mmv_fallbacks: ?usize = null,
    expect_q4_pair_mmv_variant: ?Q4MmvVariant = null,
    expect_q4_pair_mmv_fallbacks: ?usize = null,
    expect_q4_pair_mm_route: ?Q4PairMmRoute = null,
    expect_q4_pair_mm_fallbacks: ?usize = null,
    expect_output_hash: ?u64 = null,
    skip_unless_apple_m4: bool = false,
};

fn usage() void {
    std.debug.print(
        \\usage: zig build inference-metal-bench -- [--mode linear|q6-linear|q6-argmax|head-rope|pair|qkv|split-qkv|ffn|ple] [--rows N] [--in N] [--out N] [--kv-out N] [--warmup N] [--iters N] [--ops-per-frame N] [--expect-q4-route aligned|aligned-tail|unrolled] [--expect-q4-mmv-variant nr4-nsg2|nr8-nsg2|nr4-nsg4|nr8-nsg4] [--expect-q4-mmv-auto] [--expect-q4-mmv-fallbacks N] [--expect-q4-pair-mmv-variant nr4-nsg2|nr8-nsg2|nr4-nsg4|nr8-nsg4] [--expect-q4-pair-mmv-fallbacks N] [--expect-q4-pair-mm-route m32-n64-aligned|m32-n64-tail|m32-n32-aligned|m32-n32-tail] [--expect-q4-pair-mm-fallbacks N] [--expect-output-hash HEX] [--skip-unless-apple-m4]
        \\
    , .{});
}

fn parsePositiveUsize(value: []const u8) !usize {
    const parsed = try std.fmt.parseUnsigned(usize, value, 10);
    if (parsed == 0) return error.InvalidArgument;
    return parsed;
}

fn parseArgs(args: []const [:0]const u8) !Config {
    var cfg: Config = .{};

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            usage();
            std.process.exit(0);
        }
        if (std.mem.eql(u8, arg, "--expect-q4-mmv-auto")) {
            cfg.expect_q4_mmv_auto = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--skip-unless-apple-m4")) {
            cfg.skip_unless_apple_m4 = true;
            continue;
        }
        if (i + 1 >= args.len) return error.InvalidArgument;
        const value = args[i + 1];
        i += 1;
        if (std.mem.eql(u8, arg, "--mode")) {
            cfg.mode = try Mode.parse(value);
        } else if (std.mem.eql(u8, arg, "--rows")) {
            cfg.rows = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--weight-slots")) {
            cfg.weight_slots = try parsePositiveUsize(value);
            if (cfg.weight_slots > 64) return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--in")) {
            cfg.in_dim = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--out")) {
            cfg.out_dim = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--kv-out")) {
            cfg.kv_out_dim = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            cfg.warmup_iters = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--iters")) {
            cfg.measure_iters = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--ops-per-frame")) {
            cfg.ops_per_frame = try parsePositiveUsize(value);
        } else if (std.mem.eql(u8, arg, "--expect-q4-route")) {
            cfg.expect_q4_route = try Q4MmRoute.parse(value);
        } else if (std.mem.eql(u8, arg, "--expect-q4-mmv-variant")) {
            cfg.expect_q4_mmv_variant = try Q4MmvVariant.parse(value);
        } else if (std.mem.eql(u8, arg, "--expect-q4-mmv-fallbacks")) {
            cfg.expect_q4_mmv_fallbacks = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--expect-q4-pair-mmv-variant")) {
            cfg.expect_q4_pair_mmv_variant = try Q4MmvVariant.parse(value);
        } else if (std.mem.eql(u8, arg, "--expect-q4-pair-mmv-fallbacks")) {
            cfg.expect_q4_pair_mmv_fallbacks = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--expect-q4-pair-mm-route")) {
            cfg.expect_q4_pair_mm_route = try Q4PairMmRoute.parse(value);
        } else if (std.mem.eql(u8, arg, "--expect-q4-pair-mm-fallbacks")) {
            cfg.expect_q4_pair_mm_fallbacks = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--expect-output-hash")) {
            const digits = if (std.mem.startsWith(u8, value, "0x")) value[2..] else value;
            if (digits.len == 0) return error.InvalidArgument;
            cfg.expect_output_hash = try std.fmt.parseUnsigned(u64, digits, 16);
        } else {
            return error.InvalidArgument;
        }
    }

    if (cfg.in_dim % 32 != 0) return error.InvalidArgument;
    if ((cfg.mode == .q6_linear or cfg.mode == .q6_argmax) and cfg.in_dim % 256 != 0) return error.InvalidArgument;
    if (cfg.mode == .head_rope and (cfg.out_dim == 0 or cfg.in_dim % cfg.out_dim != 0 or cfg.kv_out_dim > cfg.out_dim)) return error.InvalidArgument;
    if (cfg.mode != .linear and cfg.kv_out_dim == 0) return error.InvalidArgument;
    if (cfg.rows != 1 and cfg.mode != .linear and cfg.mode != .q6_linear and cfg.mode != .ffn) return error.InvalidArgument;
    if (cfg.expect_q4_route != null and cfg.mode != .linear) return error.InvalidArgument;
    if (cfg.expect_q4_mmv_variant != null and
        ((cfg.mode != .linear and cfg.mode != .ffn) or cfg.rows != 1)) return error.InvalidArgument;
    if (cfg.expect_q4_mmv_auto and (cfg.mode != .linear or cfg.rows != 1 or cfg.expect_q4_mmv_variant != null)) return error.InvalidArgument;
    if (cfg.expect_q4_mmv_fallbacks != null and cfg.expect_q4_mmv_variant == null and !cfg.expect_q4_mmv_auto) return error.InvalidArgument;
    if ((cfg.expect_q4_pair_mmv_variant != null or cfg.expect_q4_pair_mmv_fallbacks != null or cfg.expect_q4_pair_mm_route != null or cfg.expect_q4_pair_mm_fallbacks != null) and cfg.mode != .ffn) return error.InvalidArgument;
    if (cfg.expect_q4_pair_mmv_variant != null and cfg.rows != 1) return error.InvalidArgument;
    if (cfg.expect_q4_pair_mm_route != null and cfg.rows < 9) return error.InvalidArgument;
    return cfg;
}

fn isQualifiedAppleM4() !bool {
    var device: metal_runtime.MetalDeviceInfo = .{};
    if (metal_runtime.termite_metal_device_info_get(&device) != 0) return error.MetalDeviceInfoUnavailable;
    var device_name_buffer: [256]u8 = undefined;
    const device_name_len = metal_runtime.termite_metal_copy_device_name(null, 0);
    if (device_name_len == 0 or device_name_len > device_name_buffer.len or
        metal_runtime.termite_metal_copy_device_name(device_name_buffer[0..].ptr, device_name_buffer.len) != device_name_len)
    {
        return error.MetalDeviceInfoUnavailable;
    }
    const device_name = device_name_buffer[0..device_name_len];
    return device.apple_gpu_family == 9 and std.mem.startsWith(u8, device_name, "Apple M4");
}

fn expectedAutoQ4MmvVariant(cfg: Config) !Q4MmvVariant {
    const exact_m4 = try isQualifiedAppleM4();
    const gemma4_ffn = (cfg.in_dim == 2560 and cfg.out_dim == 10240) or
        (cfg.in_dim == 10240 and cfg.out_dim == 2560);
    if (exact_m4 and gemma4_ffn) return .nr4_nsg2;
    return if (cfg.in_dim <= 4096 and cfg.out_dim <= 4096) .nr4_nsg2 else .nr8_nsg2;
}

fn fillInput(input: []f32) void {
    for (input, 0..) |*value, i| {
        const signed = @as(i32, @intCast((i * 23 + 3) % 151)) - 75;
        value.* = @as(f32, @floatFromInt(signed)) / 113.0;
    }
}

fn fillQ4_0Weights(raw: []u8, dense_row: []f32, in_dim: usize, out_dim: usize, seed: usize) void {
    const values_per_block: usize = 32;
    const bytes_per_block: usize = 18;
    const row_blocks = in_dim / values_per_block;
    const row_bytes = row_blocks * bytes_per_block;

    for (0..out_dim) |row| {
        for (dense_row, 0..) |*value, col| {
            const signed = @as(i32, @intCast((row * 31 + col * 13 + seed * 17 + 7) % 139)) - 69;
            value.* = @as(f32, @floatFromInt(signed)) / 97.0;
        }
        for (0..row_blocks) |block| {
            quant_codec.quantizeQ4_0Block(
                dense_row[block * values_per_block ..][0..values_per_block],
                raw[row * row_bytes + block * bytes_per_block ..][0..bytes_per_block],
            );
        }
    }
}

fn fillQ6_KWeights(raw: []u8, dense_block: []f32, in_dim: usize, out_dim: usize) void {
    const values_per_block: usize = 256;
    const bytes_per_block: usize = 210;
    const row_blocks = in_dim / values_per_block;
    const row_bytes = row_blocks * bytes_per_block;

    for (0..out_dim) |row| {
        for (0..row_blocks) |block| {
            for (dense_block[0..values_per_block], 0..) |*value, offset| {
                const col = block * values_per_block + offset;
                const signed = @as(i32, @intCast((row * 29 + col * 17 + 5) % 127)) - 63;
                value.* = @as(f32, @floatFromInt(signed)) / 89.0;
            }
            quant_codec.quantizeQ6_KBlock(
                dense_block[0..values_per_block],
                raw[row * row_bytes + block * bytes_per_block ..][0..bytes_per_block],
            );
        }
    }
}

fn deviceTensorFromSlice(runtime: *metal_runtime.RawMetalDecodeRuntime, data: []const f32, dims: []const i32) !MetalTensor {
    var host = try MetalTensor.ownedCloneFrom(data, dims);
    defer host.deinit();
    var device = try MetalTensor.deviceAllocate(runtime, data.len * @sizeOf(f32), .private, dims);
    errdefer device.deinit();
    try host.copyInto(&device);
    return device;
}

const PreparedQuantSlot = struct {
    raw: []u8,
    shape: []i64,
    storage: *QuantizedStorage,
    bias: MetalTensor,

    fn deinit(self: *PreparedQuantSlot, allocator: std.mem.Allocator) void {
        self.bias.deinit();
        allocator.destroy(self.storage);
        allocator.free(self.shape);
        allocator.free(self.raw);
    }
};

fn prepareQ4_0LinearSlot(
    allocator: std.mem.Allocator,
    provider: *metal_native_provider.MetalNativeProvider,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
    stats: *ops.NativeQuantTimingStats,
) !PreparedQuantSlot {
    if (in_dim % 32 != 0) return error.InvalidArgument;
    const row_blocks = in_dim / 32;
    const row_bytes = row_blocks * 18;
    const raw = try allocator.alloc(u8, out_dim * row_bytes);
    errdefer allocator.free(raw);
    const dense_row = try allocator.alloc(f32, in_dim);
    defer allocator.free(dense_row);
    fillQ4_0Weights(raw, dense_row, in_dim, out_dim, slot);

    const bias_data = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_data);
    @memset(bias_data, 0.0);
    var bias = try MetalTensor.ownedCloneFrom(bias_data, &[_]i32{@intCast(out_dim)});
    errdefer bias.deinit();

    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    const shape = try allocator.alloc(i64, 2);
    errdefer allocator.free(shape);
    shape[0] = @intCast(out_dim);
    shape[1] = @intCast(in_dim);
    const storage = try allocator.create(QuantizedStorage);
    errdefer allocator.destroy(storage);
    storage.* = QuantizedStorage{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = raw,
        .shape = shape,
        .raw_owned = false,
        .allocator = allocator,
    };

    if (!(try metal_runtime.decoderRuntimePrepareLinear(provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, storage),
        .slot = slot,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, stats))) return error.LinearPrepareFailed;
    if (provider.raw_linear_slot_dense_biases[slot]) |*prepared_bias| {
        prepared_bias.deinit();
        provider.raw_linear_slot_dense_biases[slot] = null;
    }

    return .{ .raw = raw, .shape = shape, .storage = storage, .bias = bias };
}

fn prepareQ6_KLinearSlot(
    allocator: std.mem.Allocator,
    provider: *metal_native_provider.MetalNativeProvider,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
    stats: *ops.NativeQuantTimingStats,
) !PreparedQuantSlot {
    if (in_dim % 256 != 0) return error.InvalidArgument;
    const row_blocks = in_dim / 256;
    const row_bytes = row_blocks * 210;
    const raw = try allocator.alloc(u8, out_dim * row_bytes);
    errdefer allocator.free(raw);
    const dense_block = try allocator.alloc(f32, 256);
    defer allocator.free(dense_block);
    fillQ6_KWeights(raw, dense_block, in_dim, out_dim);

    const bias_data = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_data);
    @memset(bias_data, 0.0);
    var bias = try MetalTensor.ownedCloneFrom(bias_data, &[_]i32{@intCast(out_dim)});
    errdefer bias.deinit();

    var dummy_weight_value = [_]f32{0.0};
    const dummy_weight = MetalTensor.borrowed(dummy_weight_value[0..].ptr, 1, &[_]i32{0});
    const shape = try allocator.alloc(i64, 2);
    errdefer allocator.free(shape);
    shape[0] = @intCast(out_dim);
    shape[1] = @intCast(in_dim);
    const storage = try allocator.create(QuantizedStorage);
    errdefer allocator.destroy(storage);
    storage.* = QuantizedStorage{
        .tensor_type = .{ .known = .Q6_K },
        .raw_bytes = raw,
        .shape = shape,
        .raw_owned = false,
        .allocator = allocator,
    };

    if (!(try metal_runtime.decoderRuntimePrepareLinear(provider, .{
        .weight = dummy_weight,
        .bias = bias,
        .quantized_storage = @as(?*const QuantizedStorage, storage),
        .slot = slot,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = false,
    }, stats))) return error.LinearPrepareFailed;
    if (provider.raw_linear_slot_dense_biases[slot]) |*prepared_bias| {
        prepared_bias.deinit();
        provider.raw_linear_slot_dense_biases[slot] = null;
    }

    return .{ .raw = raw, .shape = shape, .storage = storage, .bias = bias };
}

fn nowNanos() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn applyOnce(
    provider: *metal_native_provider.MetalNativeProvider,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    in_dim: usize,
    out_dim: usize,
    weight_slots: usize,
    outputs: []MetalTensor,
) !u64 {
    const start = nowNanos();
    try metal_runtime.beginFrame(runtime);
    var produced: usize = 0;
    errdefer for (outputs[0..produced]) |*output| output.deinit();
    while (produced < outputs.len) : (produced += 1) {
        outputs[produced] = (try metal_runtime.decoderRuntimeApplyLinear(provider, .{
            .slot = produced % @max(weight_slots, 1),
            .input = input,
            .in_dim = in_dim,
            .out_dim = out_dim,
        })) orelse return error.LinearDispatchFailed;
    }
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    for (outputs) |*output| output.deinit();
    return nowNanos() - start;
}

fn applyLinearArgmaxOnce(
    provider: *metal_native_provider.MetalNativeProvider,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    in_dim: usize,
    out_dim: usize,
    outputs: []MetalTensor,
) !u64 {
    const start = nowNanos();
    try metal_runtime.beginFrame(runtime);
    var produced: usize = 0;
    errdefer for (outputs[0..produced]) |*output| output.deinit();
    while (produced < outputs.len) : (produced += 1) {
        var logits = (try metal_runtime.decoderRuntimeApplyLinear(provider, .{
            .slot = 0,
            .input = input,
            .in_dim = in_dim,
            .out_dim = out_dim,
        })) orelse return error.LinearDispatchFailed;
        errdefer logits.deinit();
        if (!(try metal_runtime.encodeArgmaxLogitsDevice(provider, logits, out_dim))) return error.LinearDispatchFailed;
        outputs[produced] = logits;
    }
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    for (outputs) |*output| output.deinit();
    return nowNanos() - start;
}

fn applyHeadRopeOnce(
    provider: *metal_native_provider.MetalNativeProvider,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    total_values: usize,
    head_dim: usize,
    rope_dim: usize,
    outputs: []MetalTensor,
) !u64 {
    const heads = total_values / head_dim;
    const start = nowNanos();
    try metal_runtime.beginFrame(runtime);
    var produced: usize = 0;
    errdefer for (outputs[0..produced]) |*output| output.deinit();
    while (produced < outputs.len) : (produced += 1) {
        outputs[produced] = (try metal_runtime.decoderRuntimeApplyHeadRmsNormRope(provider, .{
            .slot = 0,
            .input = input,
            .total_heads = heads,
            .head_dim = head_dim,
            .rope_dim = rope_dim,
            .position = 1024,
            .theta = 1_000_000.0,
            .freq_scale = 1.0,
            .eps = 0.0,
            .value_scale = 1.0,
            .consecutive_pairs = false,
        })) orelse return error.LinearDispatchFailed;
    }
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    for (outputs) |*output| output.deinit();
    return nowNanos() - start;
}

const QkvOutput = struct {
    q: MetalTensor,
    k: MetalTensor,
    v: MetalTensor,

    fn deinit(self: *QkvOutput) void {
        self.v.deinit();
        self.k.deinit();
        self.q.deinit();
    }
};

const PairOutput = struct {
    first: MetalTensor,
    second: MetalTensor,

    fn deinit(self: *PairOutput) void {
        self.second.deinit();
        self.first.deinit();
    }
};

fn applyPairOnce(
    provider: *metal_native_provider.MetalNativeProvider,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    in_dim: usize,
    out_dim: usize,
    outputs: []PairOutput,
) !u64 {
    const start = nowNanos();
    try metal_runtime.beginFrame(runtime);
    var produced: usize = 0;
    errdefer for (outputs[0..produced]) |*output| output.deinit();
    while (produced < outputs.len) : (produced += 1) {
        const pair = (try metal_runtime.decoderRuntimeApplyLinearPair(provider, .{
            .slot_a = 0,
            .slot_b = 1,
            .input = input,
            .in_dim = in_dim,
            .out_dim = out_dim,
        })) orelse return error.LinearDispatchFailed;
        outputs[produced] = .{ .first = pair.first, .second = pair.second };
    }
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    for (outputs) |*output| output.deinit();
    return nowNanos() - start;
}

fn applyQkvOnce(
    provider: *metal_native_provider.MetalNativeProvider,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    cfg: Config,
    outputs: []QkvOutput,
) !u64 {
    const start = nowNanos();
    try metal_runtime.beginFrame(runtime);
    var produced: usize = 0;
    errdefer for (outputs[0..produced]) |*output| output.deinit();
    while (produced < outputs.len) : (produced += 1) {
        outputs[produced] = switch (cfg.mode) {
            .linear, .q6_linear, .q6_argmax, .head_rope, .pair, .ple => unreachable,
            .qkv => qkv: {
                const qkv = (try metal_runtime.tryApplyQuantizedRuntimeLinearQkv(
                    provider,
                    0,
                    1,
                    2,
                    input,
                    1,
                    cfg.in_dim,
                    cfg.out_dim,
                    cfg.kv_out_dim,
                )) orelse return error.LinearDispatchFailed;
                break :qkv .{ .q = qkv.first, .k = qkv.second, .v = qkv.third };
            },
            .split_qkv => split: {
                var q = (try metal_runtime.decoderRuntimeApplyLinear(provider, .{
                    .slot = 0,
                    .input = input,
                    .in_dim = cfg.in_dim,
                    .out_dim = cfg.out_dim,
                })) orelse return error.LinearDispatchFailed;
                errdefer q.deinit();
                var k = (try metal_runtime.decoderRuntimeApplyLinear(provider, .{
                    .slot = 1,
                    .input = input,
                    .in_dim = cfg.in_dim,
                    .out_dim = cfg.kv_out_dim,
                })) orelse return error.LinearDispatchFailed;
                errdefer k.deinit();
                const v = (try metal_runtime.decoderRuntimeApplyLinear(provider, .{
                    .slot = 2,
                    .input = input,
                    .in_dim = cfg.in_dim,
                    .out_dim = cfg.kv_out_dim,
                })) orelse return error.LinearDispatchFailed;
                break :split .{ .q = q, .k = k, .v = v };
            },
            .ffn => unreachable,
        };
    }
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    for (outputs) |*output| output.deinit();
    return nowNanos() - start;
}

fn encodeFfnOutput(
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    cfg: Config,
) !MetalTensor {
    const shape = [_]i32{ @intCast(cfg.rows), @intCast(cfg.in_dim) };
    var output = try MetalTensor.deviceAllocate(
        runtime,
        cfg.rows * cfg.in_dim * @sizeOf(f32),
        .private,
        &shape,
    );
    errdefer output.deinit();
    const none = std.math.maxInt(usize);
    const rc = metal_runtime.termite_metal_decode_runtime_apply_gated_ffn_residual_q4_0_slots_device(
        runtime,
        input.deviceHandle(),
        input.deviceByteOffset(),
        input.deviceHandle(),
        input.deviceByteOffset(),
        cfg.rows,
        cfg.in_dim,
        cfg.out_dim,
        @backingInt(ops.DecoderRuntimeActivationKind.gelu_new),
        0,
        1,
        none,
        0,
        1e-5,
        2,
        output.deviceHandle(),
        output.deviceByteOffset(),
    );
    if (rc != 0) {
        std.debug.print("ffn_dispatch_rc={d}\n", .{rc});
        return error.LinearDispatchFailed;
    }
    return output;
}

fn applyFfnOnce(
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    cfg: Config,
    outputs: []MetalTensor,
) !u64 {
    const start = nowNanos();
    try metal_runtime.beginFrame(runtime);
    try metal_runtime.beginPlannedComputeScope(runtime, @backingInt(metal_runtime.ComputeSource.ffn), .ffn);
    var produced: usize = 0;
    errdefer {
        _ = metal_runtime.endPlannedComputeScope(runtime) catch {};
        for (outputs[0..produced]) |*output| output.deinit();
    }
    while (produced < outputs.len) : (produced += 1) {
        outputs[produced] = try encodeFfnOutput(runtime, input, cfg);
    }
    try metal_runtime.endPlannedComputeScope(runtime);
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    for (outputs) |*output| output.deinit();
    return nowNanos() - start;
}

fn applyFfnOutput(
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    input: MetalTensor,
    cfg: Config,
) !MetalTensor {
    try metal_runtime.beginFrame(runtime);
    try metal_runtime.beginPlannedComputeScope(runtime, @backingInt(metal_runtime.ComputeSource.ffn), .ffn);
    errdefer _ = metal_runtime.endPlannedComputeScope(runtime) catch {};
    var output = try encodeFfnOutput(runtime, input, cfg);
    errdefer output.deinit();
    try metal_runtime.endPlannedComputeScope(runtime);
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    return output;
}

fn applyPleOnce(
    provider: *metal_native_provider.MetalNativeProvider,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    hidden: MetalTensor,
    ple: MetalTensor,
    cfg: Config,
    outputs: []MetalTensor,
) !u64 {
    const start = nowNanos();
    try metal_runtime.beginFrame(runtime);
    var produced: usize = 0;
    errdefer for (outputs[0..produced]) |*output| output.deinit();
    while (produced < outputs.len) : (produced += 1) {
        outputs[produced] = (try metal_runtime.decoderRuntimeApplyPleResidualDevice(provider, .{
            .hidden = hidden,
            .ple = ple,
            .gate_linear_slot = 0,
            .proj_linear_slot = 1,
            .post_norm_slot = 0,
            .hidden_size = cfg.in_dim,
            .ple_hidden_size = cfg.out_dim,
            .eps = 1e-5,
            .activation = @as(ops.DecoderRuntimeActivationKind, .gelu),
        })) orelse return error.LinearDispatchFailed;
    }
    try metal_runtime.submitFrame(runtime);
    try metal_runtime.waitFrame(runtime);
    for (outputs) |*output| output.deinit();
    return nowNanos() - start;
}

fn median(sorted: []const u64) u64 {
    return sorted[sorted.len / 2];
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const args = try init.minimal.args.toSlice(arena.allocator());
    const cfg = parseArgs(args) catch |err| {
        usage();
        return err;
    };

    if (!metal_runtime.metalDeviceAvailable()) return error.MetalDeviceUnavailable;
    if (cfg.skip_unless_apple_m4 and !try isQualifiedAppleM4()) {
        std.debug.print("metal_q4_0_bench: skipped (requires qualified Apple M4 device)\n", .{});
        return;
    }
    var provider = try metal_native_provider.MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.MetalDeviceUnavailable;

    var stats: ops.NativeQuantTimingStats = .{};
    var slot0 = switch (cfg.mode) {
        .q6_linear, .q6_argmax => try prepareQ6_KLinearSlot(allocator, &provider, 0, cfg.in_dim, cfg.out_dim, &stats),
        else => try prepareQ4_0LinearSlot(allocator, &provider, 0, cfg.in_dim, cfg.out_dim, &stats),
    };
    defer slot0.deinit(allocator);
    var extra_weight_slots: [64]?PreparedQuantSlot = @splat(null);
    defer for (&extra_weight_slots) |*maybe_slot| {
        if (maybe_slot.*) |*slot| slot.deinit(allocator);
    };
    if (cfg.weight_slots > 1 and (cfg.mode == .linear or cfg.mode == .q6_linear)) {
        var slot_index: usize = 1;
        while (slot_index < cfg.weight_slots) : (slot_index += 1) {
            extra_weight_slots[slot_index] = switch (cfg.mode) {
                .q6_linear => try prepareQ6_KLinearSlot(allocator, &provider, slot_index, cfg.in_dim, cfg.out_dim, &stats),
                else => try prepareQ4_0LinearSlot(allocator, &provider, slot_index, cfg.in_dim, cfg.out_dim, &stats),
            };
        }
    }
    var slot1: ?PreparedQuantSlot = null;
    defer if (slot1) |*slot| slot.deinit(allocator);
    var slot2: ?PreparedQuantSlot = null;
    defer if (slot2) |*slot| slot.deinit(allocator);
    switch (cfg.mode) {
        .linear, .q6_linear, .q6_argmax => {},
        .head_rope => {
            const norm_weight_data = try allocator.alloc(f32, cfg.out_dim);
            defer allocator.free(norm_weight_data);
            @memset(norm_weight_data, 1.0);
            var norm_weight = try MetalTensor.ownedCloneFrom(norm_weight_data, &[_]i32{@intCast(cfg.out_dim)});
            defer norm_weight.deinit();
            if (!(try metal_runtime.decoderRuntimePrepareRmsNorm(&provider, .{
                .weight = norm_weight,
                .slot = 0,
                .hidden_size = cfg.out_dim,
            }))) return error.LinearPrepareFailed;
        },
        .pair => {
            slot1 = try prepareQ4_0LinearSlot(allocator, &provider, 1, cfg.in_dim, cfg.out_dim, &stats);
        },
        .qkv, .split_qkv => {
            slot1 = try prepareQ4_0LinearSlot(allocator, &provider, 1, cfg.in_dim, cfg.kv_out_dim, &stats);
            slot2 = try prepareQ4_0LinearSlot(allocator, &provider, 2, cfg.in_dim, cfg.kv_out_dim, &stats);
        },
        .ffn => {
            slot1 = try prepareQ4_0LinearSlot(allocator, &provider, 1, cfg.in_dim, cfg.out_dim, &stats);
            slot2 = try prepareQ4_0LinearSlot(allocator, &provider, 2, cfg.out_dim, cfg.in_dim, &stats);
            if (metal_runtime.ensureQuantizedRuntimeLinearSlotPrepared(&provider, 0, cfg.in_dim, cfg.out_dim) != .q4_0 or
                metal_runtime.ensureQuantizedRuntimeLinearSlotPrepared(&provider, 1, cfg.in_dim, cfg.out_dim) != .q4_0 or
                metal_runtime.ensureQuantizedRuntimeLinearSlotPrepared(&provider, 2, cfg.out_dim, cfg.in_dim) != .q4_0)
            {
                return error.LinearPrepareFailed;
            }
            if (!metal_runtime.decoderRuntimeReserveGatedFfnScratch(&provider, cfg.rows, cfg.in_dim, cfg.out_dim)) return error.LinearPrepareFailed;
            const norm_weight_data = try allocator.alloc(f32, cfg.in_dim);
            defer allocator.free(norm_weight_data);
            @memset(norm_weight_data, 1.0);
            var norm_weight = try MetalTensor.ownedCloneFrom(norm_weight_data, &[_]i32{@intCast(cfg.in_dim)});
            defer norm_weight.deinit();
            if (!(try metal_runtime.decoderRuntimePrepareRmsNorm(&provider, .{
                .weight = norm_weight,
                .slot = 0,
                .hidden_size = cfg.in_dim,
            }))) return error.LinearPrepareFailed;
        },
        .ple => {
            slot1 = try prepareQ4_0LinearSlot(allocator, &provider, 1, cfg.out_dim, cfg.in_dim, &stats);
            if (!metal_runtime.decoderRuntimeReserveGatedFfnScratch(&provider, 1, cfg.in_dim, cfg.out_dim)) return error.LinearPrepareFailed;
            const norm_weight_data = try allocator.alloc(f32, cfg.in_dim);
            defer allocator.free(norm_weight_data);
            @memset(norm_weight_data, 1.0);
            var norm_weight = try MetalTensor.ownedCloneFrom(norm_weight_data, &[_]i32{@intCast(cfg.in_dim)});
            defer norm_weight.deinit();
            if (!(try metal_runtime.decoderRuntimePrepareRmsNorm(&provider, .{
                .weight = norm_weight,
                .slot = 0,
                .hidden_size = cfg.in_dim,
            }))) return error.LinearPrepareFailed;
        },
    }

    const input_data = try allocator.alloc(f32, cfg.rows * cfg.in_dim);
    defer allocator.free(input_data);
    fillInput(input_data);
    var input = try deviceTensorFromSlice(runtime, input_data, &[_]i32{ @intCast(cfg.rows), @intCast(cfg.in_dim) });
    defer input.deinit();
    var ple_input: ?MetalTensor = null;
    defer if (ple_input) |*tensor| tensor.deinit();
    if (cfg.mode == .ple) {
        const ple_data = try allocator.alloc(f32, cfg.out_dim);
        defer allocator.free(ple_data);
        fillInput(ple_data);
        ple_input = try deviceTensorFromSlice(runtime, ple_data, &[_]i32{ 1, @intCast(cfg.out_dim) });
    }

    const frame_outputs = try allocator.alloc(MetalTensor, cfg.ops_per_frame);
    defer allocator.free(frame_outputs);
    const pair_outputs = try allocator.alloc(PairOutput, cfg.ops_per_frame);
    defer allocator.free(pair_outputs);
    const qkv_outputs = try allocator.alloc(QkvOutput, cfg.ops_per_frame);
    defer allocator.free(qkv_outputs);

    var warmup: usize = 0;
    while (warmup < cfg.warmup_iters) : (warmup += 1) {
        _ = switch (cfg.mode) {
            .linear, .q6_linear => try applyOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, cfg.weight_slots, frame_outputs),
            .q6_argmax => try applyLinearArgmaxOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, frame_outputs),
            .head_rope => try applyHeadRopeOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, cfg.kv_out_dim, frame_outputs),
            .pair => try applyPairOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, pair_outputs),
            .qkv, .split_qkv => try applyQkvOnce(&provider, runtime, input, cfg, qkv_outputs),
            .ffn => try applyFfnOnce(runtime, input, cfg, frame_outputs),
            .ple => try applyPleOnce(&provider, runtime, input, ple_input.?, cfg, frame_outputs),
        };
    }

    const before = metal_runtime.runtimeMemorySnapshot(runtime);
    const samples = try allocator.alloc(u64, cfg.measure_iters);
    defer allocator.free(samples);
    const gpu_samples = try allocator.alloc(u64, cfg.measure_iters);
    defer allocator.free(gpu_samples);
    for (samples, gpu_samples) |*sample, *gpu_sample| {
        sample.* = switch (cfg.mode) {
            .linear, .q6_linear => try applyOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, cfg.weight_slots, frame_outputs),
            .q6_argmax => try applyLinearArgmaxOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, frame_outputs),
            .head_rope => try applyHeadRopeOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, cfg.kv_out_dim, frame_outputs),
            .pair => try applyPairOnce(&provider, runtime, input, cfg.in_dim, cfg.out_dim, pair_outputs),
            .qkv, .split_qkv => try applyQkvOnce(&provider, runtime, input, cfg, qkv_outputs),
            .ffn => try applyFfnOnce(runtime, input, cfg, frame_outputs),
            .ple => try applyPleOnce(&provider, runtime, input, ple_input.?, cfg, frame_outputs),
        };
        gpu_sample.* = metal_runtime.termite_metal_decode_runtime_last_frame_gpu_nanos(runtime);
    }
    const after = metal_runtime.runtimeMemorySnapshot(runtime);
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    std.mem.sort(u64, gpu_samples, {}, std.sort.asc(u64));

    var total: u128 = 0;
    for (samples) |sample| total += sample;
    const mean_ns: u64 = @intCast(total / samples.len);
    const median_ns = median(samples);
    const median_gpu_ns = median(gpu_samples);
    const min_ns = samples[0];
    const max_ns = samples[samples.len - 1];
    const q4_calls = after.q4_0_linear_reduce - before.q4_0_linear_reduce;
    const q4_f16_in = after.q4_0_linear_reduce_f16_input - before.q4_0_linear_reduce_f16_input;
    const q4_f16_out = after.q4_0_linear_reduce_f16_output - before.q4_0_linear_reduce_f16_output;
    const q4_f16_in_out = after.q4_0_linear_reduce_f16_input_f16_output - before.q4_0_linear_reduce_f16_input_f16_output;
    const q4_sumsq = after.q4_0_linear_reduce_sumsq - before.q4_0_linear_reduce_sumsq;
    const q4_mmv_nr4_nsg2 = after.q4_0_mmv_nr4_nsg2_dispatches - before.q4_0_mmv_nr4_nsg2_dispatches;
    const q4_mmv_nr8_nsg2 = after.q4_0_mmv_nr8_nsg2_dispatches - before.q4_0_mmv_nr8_nsg2_dispatches;
    const q4_mmv_nr4_nsg4 = after.q4_0_mmv_nr4_nsg4_dispatches - before.q4_0_mmv_nr4_nsg4_dispatches;
    const q4_mmv_nr8_nsg4 = after.q4_0_mmv_nr8_nsg4_dispatches - before.q4_0_mmv_nr8_nsg4_dispatches;
    const q4_mmv_fallbacks = after.q4_0_mmv_variant_fallbacks - before.q4_0_mmv_variant_fallbacks;
    const q4_pair_mmv_nr4_nsg2 = after.q4_0_pair_activation_mmv_nr4_nsg2_dispatches - before.q4_0_pair_activation_mmv_nr4_nsg2_dispatches;
    const q4_pair_mmv_nr8_nsg2 = after.q4_0_pair_activation_mmv_nr8_nsg2_dispatches - before.q4_0_pair_activation_mmv_nr8_nsg2_dispatches;
    const q4_pair_mmv_nr4_nsg4 = after.q4_0_pair_activation_mmv_nr4_nsg4_dispatches - before.q4_0_pair_activation_mmv_nr4_nsg4_dispatches;
    const q4_pair_mmv_nr8_nsg4 = after.q4_0_pair_activation_mmv_nr8_nsg4_dispatches - before.q4_0_pair_activation_mmv_nr8_nsg4_dispatches;
    const q4_pair_mmv_fallbacks = after.q4_0_pair_activation_mmv_variant_fallbacks - before.q4_0_pair_activation_mmv_variant_fallbacks;
    const q4_mm_aligned = after.q4_0_mm_sg_aligned_dispatches - before.q4_0_mm_sg_aligned_dispatches;
    const q4_mm_aligned_tail = after.q4_0_mm_sg_aligned_tail_dispatches - before.q4_0_mm_sg_aligned_tail_dispatches;
    const q4_mm_unrolled = after.q4_0_mm_sg_unrolled_dispatches - before.q4_0_mm_sg_unrolled_dispatches;
    const q4_pair_mm_m32_n64_aligned = after.q4_0_pair_activation_mm_m32_n64_aligned_dispatches - before.q4_0_pair_activation_mm_m32_n64_aligned_dispatches;
    const q4_pair_mm_m32_n64_tail = after.q4_0_pair_activation_mm_m32_n64_tail_dispatches - before.q4_0_pair_activation_mm_m32_n64_tail_dispatches;
    const q4_pair_mm_m32_n32_aligned = after.q4_0_pair_activation_mm_m32_n32_aligned_dispatches - before.q4_0_pair_activation_mm_m32_n32_aligned_dispatches;
    const q4_pair_mm_m32_n32_tail = after.q4_0_pair_activation_mm_m32_n32_tail_dispatches - before.q4_0_pair_activation_mm_m32_n32_tail_dispatches;
    const q4_pair_mm_fallbacks = after.q4_0_pair_activation_mm_variant_fallbacks - before.q4_0_pair_activation_mm_variant_fallbacks;
    const q4_pair_reduce = after.q4_0_pair_reduce - before.q4_0_pair_reduce;
    const q4_pair = after.q4_0_pair - before.q4_0_pair;
    const q4_pair_activation = after.q4_0_pair_activation_reduce - before.q4_0_pair_activation_reduce;
    const q4_pair_activation_f16 = after.q4_0_pair_activation_reduce_f16_output - before.q4_0_pair_activation_reduce_f16_output;
    const q4_activation_rhs = after.q4_0_activation_rhs_reduce - before.q4_0_activation_rhs_reduce;
    const q4_activation_rhs_f16 = after.q4_0_activation_rhs_reduce_f16_output - before.q4_0_activation_rhs_reduce_f16_output;
    const q4_ple_activation_rhs_f16 = after.q4_0_ple_activation_rhs_reduce_f16_output - before.q4_0_ple_activation_rhs_reduce_f16_output;
    const q4_ple_linear_f16_input = after.q4_0_ple_linear_reduce_f16_input - before.q4_0_ple_linear_reduce_f16_input;
    const q6_calls = after.q6_k_linear_reduce - before.q6_k_linear_reduce;
    const q6_f16_in = after.q6_k_linear_reduce_f16_input - before.q6_k_linear_reduce_f16_input;
    const rms_norm_add_sumsq = after.rms_norm_add_sumsq - before.rms_norm_add_sumsq;
    const total_ops = cfg.measure_iters * cfg.ops_per_frame;
    if (cfg.expect_q4_route) |expected_route| {
        const expected_ops: u64 = @intCast(total_ops);
        const expected_aligned: u64 = switch (expected_route) {
            .aligned => expected_ops,
            .aligned_tail, .unrolled => 0,
        };
        const expected_tail: u64 = switch (expected_route) {
            .aligned_tail => expected_ops,
            .aligned, .unrolled => 0,
        };
        const expected_unrolled: u64 = if (expected_route == .unrolled) expected_ops else 0;
        if (q4_mm_aligned != expected_aligned or
            q4_mm_aligned_tail != expected_tail or
            q4_mm_unrolled != expected_unrolled)
        {
            std.debug.print(
                "expected exclusive Q4 MM route {s} for {d} operations, observed aligned={d} aligned_tail={d} unrolled={d}\n",
                .{ @tagName(expected_route), total_ops, q4_mm_aligned, q4_mm_aligned_tail, q4_mm_unrolled },
            );
            return error.ExpectedQ4MmRouteNotUsed;
        }
    }
    const expected_q4_mmv_variant = if (cfg.expect_q4_mmv_auto)
        try expectedAutoQ4MmvVariant(cfg)
    else
        cfg.expect_q4_mmv_variant;
    if (expected_q4_mmv_variant) |expected_variant| {
        const dispatches_per_op: usize = if (cfg.mode == .ffn) 3 else 1;
        const expected_ops: u64 = @intCast(total_ops * dispatches_per_op);
        const observed = switch (expected_variant) {
            .nr4_nsg2 => q4_mmv_nr4_nsg2,
            .nr8_nsg2 => q4_mmv_nr8_nsg2,
            .nr4_nsg4 => q4_mmv_nr4_nsg4,
            .nr8_nsg4 => q4_mmv_nr8_nsg4,
        };
        if (observed != expected_ops or
            q4_mmv_nr4_nsg2 + q4_mmv_nr8_nsg2 + q4_mmv_nr4_nsg4 + q4_mmv_nr8_nsg4 != expected_ops)
        {
            std.debug.print(
                "expected Q4_0 MMV variant {s} for {d} dispatches, observed {d}\n",
                .{ expected_variant.name(), expected_ops, observed },
            );
            return error.ExpectedQ4MmvVariantNotUsed;
        }
        const expected_fallbacks: u64 = @intCast(cfg.expect_q4_mmv_fallbacks orelse 0);
        if (q4_mmv_fallbacks != expected_fallbacks) {
            std.debug.print(
                "expected {d} Q4_0 MMV fallbacks, observed {d}\n",
                .{ expected_fallbacks, q4_mmv_fallbacks },
            );
            return error.UnexpectedQ4MmvFallbackCount;
        }
    }
    if (cfg.expect_q4_pair_mmv_variant) |expected_variant| {
        const expected_ops: u64 = @intCast(total_ops);
        const observed = switch (expected_variant) {
            .nr4_nsg2 => q4_pair_mmv_nr4_nsg2,
            .nr8_nsg2 => q4_pair_mmv_nr8_nsg2,
            .nr4_nsg4 => q4_pair_mmv_nr4_nsg4,
            .nr8_nsg4 => q4_pair_mmv_nr8_nsg4,
        };
        if (observed != expected_ops or q4_pair_mmv_nr4_nsg2 + q4_pair_mmv_nr8_nsg2 + q4_pair_mmv_nr4_nsg4 + q4_pair_mmv_nr8_nsg4 != expected_ops) return error.ExpectedQ4MmvVariantNotUsed;
        if (q4_pair_mmv_fallbacks != @as(u64, @intCast(cfg.expect_q4_pair_mmv_fallbacks orelse 0))) return error.UnexpectedQ4MmvFallbackCount;
    }
    if (cfg.expect_q4_pair_mm_route) |expected_route| {
        const expected_ops: u64 = @intCast(total_ops);
        const observed = switch (expected_route) {
            .m32_n64_aligned => q4_pair_mm_m32_n64_aligned,
            .m32_n64_tail => q4_pair_mm_m32_n64_tail,
            .m32_n32_aligned => q4_pair_mm_m32_n32_aligned,
            .m32_n32_tail => q4_pair_mm_m32_n32_tail,
        };
        if (observed != expected_ops or q4_pair_mm_m32_n64_aligned + q4_pair_mm_m32_n64_tail + q4_pair_mm_m32_n32_aligned + q4_pair_mm_m32_n32_tail != expected_ops) return error.ExpectedQ4MmRouteNotUsed;
        if (q4_pair_mm_fallbacks != @as(u64, @intCast(cfg.expect_q4_pair_mm_fallbacks orelse 0))) return error.UnexpectedQ4MmFallbackCount;
    }

    // Streamed bytes per op for roofline attribution: quant weight rows plus
    // f32 activation traffic. Modes with mixed op sequences report null and
    // omit the GB/s column rather than print a misleading figure.
    const approx_op_bytes: ?u64 = switch (cfg.mode) {
        .linear => q4RowBytes(cfg.in_dim) * cfg.out_dim +
            4 * cfg.rows * (cfg.in_dim + cfg.out_dim),
        .q6_linear, .q6_argmax => q6RowBytes(cfg.in_dim) * cfg.out_dim +
            4 * cfg.rows * (cfg.in_dim + cfg.out_dim),
        .pair => 2 * q4RowBytes(cfg.in_dim) * cfg.out_dim +
            4 * cfg.rows * (cfg.in_dim + 2 * cfg.out_dim),
        .ffn => 2 * q4RowBytes(cfg.in_dim) * cfg.out_dim +
            q4RowBytes(cfg.out_dim) * cfg.in_dim +
            4 * cfg.rows * (2 * cfg.in_dim + 3 * cfg.out_dim),
        else => null,
    };
    const median_op_ns = @as(f64, @floatFromInt(median_ns)) / @as(f64, @floatFromInt(cfg.ops_per_frame));
    const approx_gb_s: f64 = if (approx_op_bytes) |bytes|
        (@as(f64, @floatFromInt(bytes)) / (median_op_ns / 1_000_000_000.0)) / 1_000_000_000.0
    else
        0.0;
    std.debug.print(
        "metal_q4_0_linear mode={s} rows={d} in={d} out={d} kv_out={d} warmup={d} iters={d} ops_per_frame={d} median_frame_ms={d:.3} median_op_ms={d:.3} mean_frame_ms={d:.3} mean_op_ms={d:.3} min_frame_ms={d:.3} max_frame_ms={d:.3} total_ops={d} approx_op_bytes={d} approx_gb_s={d:.1}",
        .{
            cfg.mode.name(),
            cfg.rows,
            cfg.in_dim,
            cfg.out_dim,
            cfg.kv_out_dim,
            cfg.warmup_iters,
            cfg.measure_iters,
            cfg.ops_per_frame,
            @as(f64, @floatFromInt(median_ns)) / 1_000_000.0,
            median_op_ns / 1_000_000.0,
            @as(f64, @floatFromInt(mean_ns)) / 1_000_000.0,
            @as(f64, @floatFromInt(mean_ns)) / @as(f64, @floatFromInt(cfg.ops_per_frame)) / 1_000_000.0,
            @as(f64, @floatFromInt(min_ns)) / 1_000_000.0,
            @as(f64, @floatFromInt(max_ns)) / 1_000_000.0,
            total_ops,
            approx_op_bytes orelse 0,
            approx_gb_s,
        },
    );
    std.debug.print(
        " q4_0_linear_reduce={d} q4_0_linear_reduce_f16_input={d} q4_0_linear_reduce_f16_output={d} q4_0_linear_reduce_f16_input_f16_output={d} q4_0_linear_reduce_sumsq={d} q4_0_mmv_nr4_nsg2={d} q4_0_mmv_nr8_nsg2={d} q4_0_mmv_nr4_nsg4={d} q4_0_mmv_nr8_nsg4={d} q4_0_mmv_fallbacks={d} q4_0_mm_sg_aligned={d} q4_0_mm_sg_aligned_tail={d} q4_0_mm_sg_unrolled={d} q4_0_pair_reduce={d} q4_0_pair={d} q4_0_pair_activation_reduce={d} q4_0_pair_activation_reduce_f16_output={d} q4_0_activation_rhs_reduce={d} q4_0_activation_rhs_reduce_f16_output={d} q4_0_ple_activation_rhs_reduce_f16_output={d} q4_0_ple_linear_reduce_f16_input={d} q6_k_linear_reduce={d} q6_k_linear_reduce_f16_input={d} rms_norm_add_sumsq={d} median_gpu_ms={d:.3} last_gpu_ms={d:.3} last_compute_encoders={d} last_blit_encoders={d} last_ops={d}\n",
        .{
            q4_calls,
            q4_f16_in,
            q4_f16_out,
            q4_f16_in_out,
            q4_sumsq,
            q4_mmv_nr4_nsg2,
            q4_mmv_nr8_nsg2,
            q4_mmv_nr4_nsg4,
            q4_mmv_nr8_nsg4,
            q4_mmv_fallbacks,
            q4_mm_aligned,
            q4_mm_aligned_tail,
            q4_mm_unrolled,
            q4_pair_reduce,
            q4_pair,
            q4_pair_activation,
            q4_pair_activation_f16,
            q4_activation_rhs,
            q4_activation_rhs_f16,
            q4_ple_activation_rhs_f16,
            q4_ple_linear_f16_input,
            q6_calls,
            q6_f16_in,
            rms_norm_add_sumsq,
            @as(f64, @floatFromInt(median_gpu_ns)) / 1_000_000.0,
            @as(f64, @floatFromInt(metal_runtime.termite_metal_decode_runtime_last_frame_gpu_nanos(runtime))) / 1_000_000.0,
            after.last_frame_compute_encoder_count,
            after.last_frame_blit_encoder_count,
            after.last_frame_planned_command_op_count,
        },
    );
    std.debug.print(
        "metal_q4_0_pair_activation_policy: mmv_nr4_nsg2={d} mmv_nr8_nsg2={d} mmv_nr4_nsg4={d} mmv_nr8_nsg4={d} mmv_variant_fallbacks={d} mm_m32_n64_aligned={d} mm_m32_n64_tail={d} mm_m32_n32_aligned={d} mm_m32_n32_tail={d} mm_variant_fallbacks={d}\n",
        .{ q4_pair_mmv_nr4_nsg2, q4_pair_mmv_nr8_nsg2, q4_pair_mmv_nr4_nsg4, q4_pair_mmv_nr8_nsg4, q4_pair_mmv_fallbacks, q4_pair_mm_m32_n64_aligned, q4_pair_mm_m32_n64_tail, q4_pair_mm_m32_n32_aligned, q4_pair_mm_m32_n32_tail, q4_pair_mm_fallbacks },
    );

    // Keep the timed loop readback-free, then fingerprint one deterministic
    // result so candidate kernels can prove bitwise parity.
    if (cfg.mode == .linear or cfg.mode == .ffn) {
        var output = if (cfg.mode == .linear) linear: {
            try metal_runtime.beginFrame(runtime);
            const linear_output = (try metal_runtime.decoderRuntimeApplyLinear(&provider, .{
                .slot = 0,
                .input = input,
                .in_dim = cfg.in_dim,
                .out_dim = cfg.out_dim,
            })) orelse return error.LinearDispatchFailed;
            try metal_runtime.submitFrame(runtime);
            try metal_runtime.waitFrame(runtime);
            break :linear linear_output;
        } else try applyFfnOutput(runtime, input, cfg);
        defer output.deinit();
        const values = try output.toHostSlice();
        const output_hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(values));
        if (cfg.expect_output_hash) |expected_hash| {
            if (output_hash != expected_hash) {
                std.debug.print("expected output_hash={x}, observed {x}\n", .{ expected_hash, output_hash });
                return error.UnexpectedOutputHash;
            }
        }
        std.debug.print("output_hash={x}\n", .{output_hash});
    }
}
