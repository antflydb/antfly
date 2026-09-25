// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Model-owned Laya constants and strict Metal execution. The caller holds the
//! provider lease. No request allocator or cancellation callback is retained.
const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("ops.zig");
const device = @import("gliner_boundary_device_ops.zig");
const runtime = @import("../backends/metal_runtime.zig");
const MT = @import("../backends/metal_tensor.zig").MetalTensor;
const Provider = @import("../backends/metal_native_provider.zig").MetalNativeProvider;
const Config = @import("../architectures/modern_bert.zig").Config;
const Tensor = @import("../backends/tensor.zig").Tensor;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

extern fn termite_metal_decode_runtime_release_linear_views(rt: ?*runtime.RawMetalDecodeRuntime, count: usize) c_int;
extern fn termite_metal_decode_runtime_linear_view_bytes(rt: ?*runtime.RawMetalDecodeRuntime, count: usize) u64;

pub fn enabled() bool {
    return platform.env.getenvBool("ANTFLY_LAYA_METAL_RESIDENT") and !platform.env.getenvBool("TERMITE_METAL_DISABLE_LAYA_RESIDENT");
}

/// The fused path reads dense weights. A checkpoint served with quantized
/// linears (`laya.weight_quantization`) runs the generic encoder instead.
pub fn enabledFor(laya: @import("../models/laya.zig").Config) bool {
    return enabled() and (laya.effectiveWeightQuantization() catch return false) == .none;
}

/// Conservative two-layer frame bound, including dense-attention scratch.
/// Shape checks and checked products happen before any device allocation.
pub fn workspaceBound(cfg: Config, batch: usize, seq: usize, count: usize) !usize {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    if (batch == 0 or batch > 512 or seq == 0 or seq > laya.max_len or count < 2 or count > 20) return error.InvalidLayaInputs;
    const mul = std.math.mul;
    const add = std.math.add;
    const rows = try mul(usize, batch, seq);
    const width = try add(usize, try mul(usize, cfg.hidden_size, 64), try mul(usize, cfg.intermediate_size, 12));
    const activations = try mul(usize, try mul(usize, rows, width), 4);
    const scores = try mul(usize, try mul(usize, try mul(usize, rows, seq), @max(cfg.num_attention_heads, cfg.hidden_size / 64)), 24);
    const scoring = try mul(usize, try mul(usize, try mul(usize, batch, count), cfg.hidden_size), 16);
    const bias = try mul(usize, try mul(usize, try mul(usize, seq, seq), cfg.num_attention_heads), 4);
    return add(usize, try add(usize, activations, scores), try add(usize, scoring, try add(usize, bias, 16 * 1024 * 1024)));
}

pub const Stats = struct {
    prepared: bool = false,
    weight_upload_bytes: u64 = 0,
    weight_upload_calls: u64 = 0,
    model_bytes: usize = 0,
    requests: u64 = 0,
    input_upload_bytes: u64 = 0,
    output_readback_bytes: u64 = 0,
    intermediate_readbacks: u64 = 0,
    host_fallbacks: u64 = 0,
    submissions: u64 = 0,
    activation_host_accesses: u64 = 0,
    physical_upload_bytes: u64 = 0,
    physical_download_bytes: u64 = 0,
    physical_download_calls: u64 = 0,
    request_workspace_bound_bytes: usize = 0,
    cached_activation_bytes: u64 = 0,
    workspace_pool_peak_bytes: usize = 0,
    workspace_pool_hits: u64 = 0,
    workspace_pool_allocations: u64 = 0,
};
const Linear = struct { slot: usize, input: usize, output: usize };
pub const Owner = struct {
    allocator: std.mem.Allocator,
    provider: *Provider,
    runtime_identity: *runtime.RawMetalDecodeRuntime,
    constants: std.StringHashMapUnmanaged(MT) = .empty,
    linears: std.StringHashMapUnmanaged(Linear) = .empty,
    next_slot: usize = 0,
    stats: Stats = .{},

    pub fn snapshot(self: *Owner) Stats {
        var result = self.stats;
        result.cached_activation_bytes = termite_metal_decode_runtime_linear_view_bytes(self.runtime_identity, self.next_slot);
        return result;
    }

    fn releaseActivationViews(self: *Owner) !void {
        if (termite_metal_decode_runtime_release_linear_views(self.runtime_identity, self.next_slot) != 0)
            return error.LayaResidentFrameAlreadyActive;
    }

    pub fn destroy(self: *Owner) void {
        var it = self.constants.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.constants.deinit(self.allocator);
        var lit = self.linears.iterator();
        while (lit.next()) |entry| {
            runtime.clearRawLinearSlot(self.provider, entry.value_ptr.slot);
            self.allocator.free(entry.key_ptr.*);
        }
        self.linears.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    fn put(self: *Owner, name: []const u8, tensor: MT) !void {
        const owned = tensor;
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.constants.putNoClobber(self.allocator, key, owned);
    }

    fn constant(self: *Owner, compute: anytype, name: []const u8, control: ?Control) !void {
        if (control) |active| try active.check();
        if (self.constants.contains(name)) return;
        const reader = (compute.data.tensor_store orelse return error.UnsupportedLayaArtifact).singleSafetensorsReader() orelse return error.UnsupportedLayaArtifact;
        var tensor = try reader.readTensor(name);
        defer tensor.deinit();
        const count = tensor.elementCount();
        var dims: [8]i32 = undefined;
        if (tensor.shape.len > dims.len) return error.InvalidLayaWeights;
        for (tensor.shape, 0..) |dim, i| dims[i] = std.math.cast(i32, dim) orelse return error.InvalidLayaWeights;
        var gpu = try MT.deviceAllocateWithAllocator(self.allocator, self.provider.raw_decode_runtime.?, count * 4, .private, dims[0..tensor.shape.len]);
        errdefer gpu.deinit();
        // Lossless F16/BF16 -> F32 for lookup/norm constants, bounded staging.
        const values = try self.allocator.alloc(f32, @min(count, 1024 * 1024));
        defer self.allocator.free(values);
        var offset: usize = 0;
        while (offset < count) {
            if (control) |active| try active.check();
            const n = @min(values.len, count - offset);
            for (values[0..n], 0..) |*value, j| {
                const i = offset + j;
                value.* = switch (tensor.dtype) {
                    .f32 => @bitCast(std.mem.readInt(u32, tensor.data[i * 4 ..][0..4], .little)),
                    .f16 => @floatCast(@as(f16, @bitCast(std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little)))),
                    .bf16 => @bitCast(@as(u32, std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little)) << 16),
                    else => return error.UnsupportedLayaArtifact,
                };
                if (!std.math.isFinite(value.*)) return error.InvalidLayaWeights;
            }
            var view = try gpu.retainedView(offset * 4, n * 4, &.{@intCast(n)});
            defer view.deinit();
            try view.uploadBytes(std.mem.sliceAsBytes(values[0..n]));
            self.stats.weight_upload_bytes += n * 4;
            self.stats.weight_upload_calls += 1;
            offset += n;
        }
        self.stats.model_bytes += count * 4;
        return self.put(name, gpu);
    }

    fn constantValues(self: *Owner, name: []const u8, data: []const f32) !void {
        var gpu = try MT.deviceAllocateWithAllocator(self.allocator, self.provider.raw_decode_runtime.?, data.len * 4, .private, &.{@intCast(data.len)});
        errdefer gpu.deinit();
        try gpu.uploadBytes(std.mem.sliceAsBytes(data));
        self.stats.weight_upload_bytes += data.len * 4;
        self.stats.weight_upload_calls += 1;
        self.stats.model_bytes += data.len * 4;
        return self.put(name, gpu);
    }

    fn validateWeight(compute: anytype, name: []const u8, expected: usize, control: ?Control) !void {
        const reader = (compute.data.tensor_store orelse return error.UnsupportedLayaArtifact).singleSafetensorsReader() orelse return error.UnsupportedLayaArtifact;
        var tensor = try reader.readTensor(name);
        defer tensor.deinit();
        if (tensor.elementCount() != expected) return error.InvalidLayaWeights;
        for (0..expected) |i| {
            if (i % (1024 * 1024) == 0) if (control) |c| try c.check();
            const finite = switch (tensor.dtype) {
                .f32 => std.mem.readInt(u32, tensor.data[i * 4 ..][0..4], .little) & 0x7f800000 != 0x7f800000,
                .f16 => std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little) & 0x7c00 != 0x7c00,
                .bf16 => std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little) & 0x7f80 != 0x7f80,
                else => return error.UnsupportedLayaArtifact,
            };
            if (!finite) return error.InvalidLayaWeights;
        }
    }

    fn linear(self: *Owner, compute: anytype, name: []const u8, wname: []const u8, bname: ?[]const u8, input: usize, output: usize, control: ?Control) !void {
        try validateWeight(compute, wname, try std.math.mul(usize, input, output), control);
        if (bname) |b| try validateWeight(compute, b, output, control);
        defer compute.releaseLayaPreparationWeightCaches();
        var cb = compute.computeBackend();
        var buf: [256]u8 = undefined;
        const canonical = if (std.mem.startsWith(u8, wname, "encoder.")) wname[8..] else wname;
        const weight = try cb.getWeight(try std.fmt.bufPrint(&buf, "model.{s}", .{canonical}));
        defer cb.free(weight);
        const bias = if (bname) |b| blk: {
            const key = if (std.mem.startsWith(u8, b, "encoder.")) b[8..] else b;
            break :blk try cb.getWeight(try std.fmt.bufPrint(&buf, "model.{s}", .{key}));
        } else blk: {
            const zeros = try self.allocator.alloc(f32, output);
            defer self.allocator.free(zeros);
            @memset(zeros, 0);
            break :blk try cb.fromFloat32Shape(zeros, &.{@intCast(output)});
        };
        defer cb.free(bias);
        const slot = self.next_slot;
        if (slot >= runtime.decoder_runtime_linear_slot_capacity) return error.ResourceLimitExceeded;
        self.next_slot += 1;
        const already = cb.decoderRuntimeLinearSlotPrepared(slot, input, output);
        errdefer if (!already) runtime.clearRawLinearSlot(self.provider, slot);
        if (!already and !try cb.decoderRuntimePrepareLinear(&.{ .slot = slot, .weight = weight, .bias = bias, .in_dim = input, .out_dim = output, .retain_dense_fallback = false })) {
            std.log.err("Laya resident preparation failed: {s} ({d} x {d})", .{ name, input, output });
            return error.LayaResidentLinearPreparationFailed;
        }
        const reader = compute.data.tensor_store.?.singleSafetensorsReader().?;
        const meta = reader.header.tensors.get(wname) orelse return error.InvalidLayaWeights;
        const bytes = input * output * meta.dtype.byteSize() + output * 4;
        self.stats.model_bytes += bytes;
        if (!already) {
            self.stats.weight_upload_bytes += bytes;
            self.stats.weight_upload_calls += 1;
        }
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.linears.putNoClobber(self.allocator, key, .{ .slot = slot, .input = input, .output = output });
    }

    pub fn prepare(compute: anytype, cfg: Config, control: ?Control) !*Owner {
        if (compute.data.laya_resident) |owner| {
            if (owner.provider != compute.provider_impl or owner.runtime_identity != compute.provider_impl.raw_decode_runtime.? or !owner.stats.prepared) return error.StaleLayaResidentState;
            return owner;
        }
        const a = compute.data.allocator;
        const owner = try a.create(Owner);
        owner.* = .{ .allocator = a, .provider = compute.provider_impl, .runtime_identity = compute.provider_impl.raw_decode_runtime.? };
        errdefer owner.destroy();
        const h: usize = cfg.hidden_size;
        const f: usize = cfg.intermediate_size;
        const laya = cfg.laya orelse return error.InvalidLayaConfig;
        const zeros = try a.alloc(f32, h);
        defer a.free(zeros);
        @memset(zeros, 0);
        try owner.constantValues("zero", zeros);
        var scales: [12]f32 = undefined;
        for (0..3) |kind| for ([_]usize{ 2, 3, 6, 11 }, 0..) |count, bucket| {
            scales[kind * 4 + bucket] = laya.scale(@enumFromInt(kind), count);
        };
        try owner.constantValues("scales", &scales);
        for ([_][]const u8{ "encoder.embeddings.tok_embeddings.weight", "encoder.embeddings.norm.weight", "encoder.final_norm.weight", "type_emb.weight" }) |name| try owner.constant(compute, name, control);
        for (0..cfg.num_hidden_layers) |layer| {
            if (control) |c| try c.check();
            var name: [128]u8 = undefined;
            if (layer != 0) try owner.constant(compute, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn_norm.weight", .{layer}), control);
            try owner.constant(compute, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp_norm.weight", .{layer}), control);
            for ([_][]const u8{ "attn.Wqkv", "attn.Wo", "mlp.Wi", "mlp.Wo" }, 0..) |suffix, i| {
                const ins = [_]usize{ h, h, h, f };
                const outs = [_]usize{ h * 3, h, f * 2, h };
                const prefix = try std.fmt.bufPrint(&name, "encoder.layers.{d}.{s}", .{ layer, suffix });
                var w: [160]u8 = undefined;
                try owner.linear(compute, prefix, try std.fmt.bufPrint(&w, "{s}.weight", .{prefix}), null, ins[i], outs[i], control);
            }
        }
        for (0..laya.head_layers) |layer| {
            if (control) |c| try c.check();
            var name: [128]u8 = undefined;
            for ([_][]const u8{ "norm1.weight", "norm1.bias", "norm2.weight", "norm2.bias" }) |suffix| try owner.constant(compute, try std.fmt.bufPrint(&name, "head.layers.{d}.{s}", .{ layer, suffix }), control);
            for ([_][]const u8{ "self_attn.in_proj", "self_attn.out_proj", "linear1", "linear2" }, 0..) |suffix, i| {
                const ins = [_]usize{ h, h, h, h * 4 };
                const outs = [_]usize{ h * 3, h, h * 4, h };
                const prefix = try std.fmt.bufPrint(&name, "head.layers.{d}.{s}", .{ layer, suffix });
                var w: [160]u8 = undefined;
                var b: [160]u8 = undefined;
                const sep = if (i == 0) "_" else ".";
                try owner.linear(compute, prefix, try std.fmt.bufPrint(&w, "{s}{s}weight", .{ prefix, sep }), try std.fmt.bufPrint(&b, "{s}{s}bias", .{ prefix, sep }), ins[i], outs[i], control);
            }
        }
        try owner.constant(compute, "scorer.0.weight", control);
        try owner.constant(compute, "scorer.0.bias", control);
        for ([_][]const u8{ "scorer.1", "scorer.3", "act_head.0", "act_head.2" }, 0..) |prefix, i| {
            const ins = [_]usize{ h, h, h + 4, 256 };
            const outs = [_]usize{ h, 1, 256, laya.n_act };
            var w: [128]u8 = undefined;
            var b: [128]u8 = undefined;
            try owner.linear(compute, prefix, try std.fmt.bufPrint(&w, "{s}.weight", .{prefix}), try std.fmt.bufPrint(&b, "{s}.bias", .{prefix}), ins[i], outs[i], control);
        }
        if (control) |c| try c.check();
        owner.stats.prepared = true;
        compute.data.laya_resident = owner;
        return owner;
    }
};

const Execution = struct {
    a: std.mem.Allocator,
    owner: *Owner,
    tensors: std.ArrayList(MT) = .empty,
    control: ?Control,
    reuse_buffers: bool = false,
    pool: std.ArrayList(struct { tensor: MT, in_use: bool }) = .empty,
    pool_bytes: usize = 0,
    pool_limit: usize = 0,
    pinned_bias: ?*anyopaque = null,

    fn allocate(self: *Execution, bytes: usize, dims: []const i32) !MT {
        const rt = self.owner.provider.raw_decode_runtime.?;
        if (!self.reuse_buffers) return MT.deviceAllocateWithAllocator(self.a, rt, bytes, .private, dims);
        var best: ?usize = null;
        for (self.pool.items, 0..) |entry, i| {
            if (!entry.in_use and entry.tensor.deviceByteLen() >= bytes and
                (best == null or entry.tensor.deviceByteLen() < self.pool.items[best.?].tensor.deviceByteLen())) best = i;
        }
        const index = best orelse blk: {
            const next_bytes = try std.math.add(usize, self.pool_bytes, bytes);
            if (next_bytes > self.pool_limit) return error.ResourceLimitExceeded;
            var tensor = try MT.deviceAllocateWithAllocator(self.a, rt, bytes, .private, dims);
            errdefer tensor.deinit();
            try self.pool.append(self.a, .{ .tensor = tensor, .in_use = false });
            self.pool_bytes = next_bytes;
            self.owner.stats.workspace_pool_peak_bytes = @max(self.owner.stats.workspace_pool_peak_bytes, next_bytes);
            self.owner.stats.workspace_pool_allocations += 1;
            break :blk self.pool.items.len - 1;
        };
        if (best != null) self.owner.stats.workspace_pool_hits += 1;
        self.pool.items[index].in_use = true;
        return self.pool.items[index].tensor.retainedView(0, bytes, dims);
    }

    fn deinitPool(self: *Execution) void {
        for (self.pool.items) |*entry| entry.tensor.deinit();
        self.pool.deinit(self.a);
    }

    fn keep(self: *Execution, tensor: MT) !MT {
        var t = tensor;
        errdefer t.deinit();
        try self.tensors.append(self.a, t);
        return t;
    }
    fn retire(self: *Execution) void {
        for (self.tensors.items) |*t| t.deinit();
        self.tensors.clearRetainingCapacity();
        for (self.pool.items) |*entry| entry.in_use = entry.tensor.deviceHandle() == self.pinned_bias;
    }
    fn drain(self: *Execution, keep_tensor: MT) !MT {
        var retained = try keep_tensor.retainedCopy();
        errdefer retained.deinit();
        try runtime.submitFrame(self.owner.provider.raw_decode_runtime);
        try runtime.waitFrame(self.owner.provider.raw_decode_runtime);
        try self.owner.releaseActivationViews();
        self.owner.stats.submissions += 1;
        self.retire();
        // Both the carried hidden state and local-attention bias outlive this
        // frame. Their backing buffers cannot become an output in the next one.
        for (self.pool.items) |*entry| if (entry.tensor.deviceHandle() == retained.deviceHandle()) {
            entry.in_use = true;
        };
        if (self.control) |c| try c.check();
        try runtime.beginFrame(self.owner.provider.raw_decode_runtime);
        try self.tensors.append(self.a, retained);
        return retained;
    }
    fn kernel(self: *Execution, kind: device.Kind, inputs: []const MT, dims: []const usize, scalars: []const f32) !MT {
        var k = device.Kernel{ .kind = kind };
        for (dims, 0..) |d, i| k.dims[i] = std.math.cast(u32, d) orelse return error.ResourceLimitExceeded;
        @memcpy(k.scalars[0..scalars.len], scalars);
        var values: [device.max_inputs]?MT = @splat(null);
        for (inputs, 0..) |t, i| values[i] = t;
        try runtime.requireGlinerBoundaryReady(self.owner.provider.raw_decode_runtime);
        const layout = try k.layout();
        var output = try self.allocate(layout.output_elements * 4, &.{@intCast(layout.output_elements)});
        errdefer output.deinit();
        if (!try runtime.decoderRuntimeGlinerBoundaryIntoDevice(self.owner.provider, k, values, output)) {
            std.log.err("Laya resident kernel failed: {s} dims={any}", .{ @tagName(kind), dims });
            return error.LayaResidentKernelFailed;
        }
        try self.tensors.append(self.a, output);
        return output;
    }
    fn constantTensor(self: *Execution, name: []const u8) !MT {
        return self.owner.constants.get(name) orelse error.MissingLayaResidentWeight;
    }
    fn norm(self: *Execution, x: MT, name: []const u8, rows: usize, dim: usize, eps: f32, has_bias: bool) !MT {
        var w: [160]u8 = undefined;
        var b: [160]u8 = undefined;
        return self.kernel(.norm, &.{ x, try self.constantTensor(try std.fmt.bufPrint(&w, "{s}.weight", .{name})), try self.constantTensor(if (has_bias) try std.fmt.bufPrint(&b, "{s}.bias", .{name}) else "zero") }, &.{ rows, dim }, &.{eps});
    }
    fn linear(self: *Execution, x: MT, name: []const u8, rows: usize) !MT {
        const l = self.owner.linears.get(name) orelse return error.MissingLayaResidentWeight;
        var view = try x.retainedView(0, rows * l.input * 4, &.{ @intCast(rows), @intCast(l.input) });
        defer view.deinit();
        var output = try self.allocate(rows * l.output * 4, &.{ @intCast(rows), @intCast(l.output) });
        errdefer output.deinit();
        if (!try runtime.tryApplyDenseRuntimeLinearInto(self.owner.provider, l.slot, view, rows, l.input, l.output, output)) {
            std.log.err("Laya resident projection failed: {s} rows={d} input={d} output={d}", .{ name, rows, l.input, l.output });
            return error.LayaResidentLinearDispatchFailed;
        }
        try self.tensors.append(self.a, output);
        return output;
    }
    fn attention(self: *Execution, qkv: MT, mask: MT, bias: ?MT, batch: usize, seq: usize, heads: usize, h: usize, theta: ?f32) !MT {
        var parts: [3]MT = undefined;
        for (&parts, 0..) |*part, i| {
            const flat = try self.kernel(.laya_qkv, &.{qkv}, &.{ batch * seq, h, heads, seq, i, @intFromBool(theta != null and i < 2) }, &.{theta orelse 1});
            part.* = try self.keep(try flat.retainedView(0, batch * seq * h * 4, &.{ @intCast(batch * seq), @intCast(h) }));
        }
        return self.keep((try runtime.decoderRuntimeSdpaF32Device(self.owner.provider, .{ .q = parts[0], .k = parts[1], .v = parts[2], .mask = @as(?MT, mask), .bias = bias, .bias_mode = @as(u32, if (bias != null) 1 else 0), .batch = batch, .seq_len = seq, .num_heads = heads, .head_dim = h / heads })) orelse return error.LayaResidentAttentionFailed);
    }
};

fn upload(a: std.mem.Allocator, owner: *Owner, bytes: []const u8, elements: usize) !MT {
    var t = try MT.deviceAllocateWithAllocator(a, owner.provider.raw_decode_runtime.?, bytes.len, .private, &.{@intCast(elements)});
    errdefer t.deinit();
    try t.uploadBytes(bytes);
    owner.stats.input_upload_bytes += bytes.len;
    return t;
}

pub fn run(compute: anytype, a: std.mem.Allocator, cfg: Config, ids: []const i64, mask: []const i64, kinds: []const i64, markers: []const i64, batch: usize, seq: usize, count: usize, control: ?Control, decisions: bool) ![]Tensor {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    const workspace = try workspaceBound(cfg, batch, seq, count);
    if (cfg.checkpoint_layout != .huggingface_fused_qkv_no_bias or cfg.rope_interleaved or cfg.global_attn_every_n_layers == 0 or cfg.hidden_size == 0 or cfg.num_attention_heads == 0 or cfg.hidden_size % cfg.num_attention_heads != 0) return error.UnsupportedLayaResidentConfiguration;
    const h: usize = cfg.hidden_size;
    const f: usize = cfg.intermediate_size;
    if (batch == 0 or batch > 512 or seq == 0 or seq > laya.max_len or count < 2 or count > 20 or ids.len != batch * seq or mask.len != ids.len or kinds.len != batch or markers.len != batch * count) return error.InvalidLayaInputs;
    for (ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidLayaInputs;
    for (kinds) |kind| if (kind < 0 or kind > 2) return error.InvalidLayaInputs;
    for (0..batch) |b| {
        var valid: usize = 0;
        for (markers[b * count ..][0..count]) |pos| {
            if (pos == -1) continue;
            if (pos < 0 or pos >= seq or mask[b * seq + @as(usize, @intCast(pos))] != 1) return error.InvalidLayaInputs;
            valid += 1;
        }
        if (valid < 2) return error.InvalidLayaInputs;
    }
    if (control) |c| try c.check();
    const owner = try Owner.prepare(compute, cfg, control);
    owner.stats.request_workspace_bound_bytes = @max(owner.stats.request_workspace_bound_bytes, workspace);
    if (compute.data.tier_cache) |cache| if (cache.hard_budget) |budget| {
        if (budget.backend_limit_bytes != 0 and try std.math.add(usize, owner.stats.model_bytes, workspace) > budget.backend_limit_bytes) return error.ResourceLimitExceeded;
    };
    const rt = owner.provider.raw_decode_runtime;
    const tensor_mod = @import("../backends/metal_tensor.zig");
    var audit = tensor_mod.TransferAudit{};
    try tensor_mod.beginTransferAudit(&audit);
    defer {
        tensor_mod.endTransferAudit();
        owner.stats.activation_host_accesses += audit.host_accesses;
        owner.stats.physical_upload_bytes += audit.upload_bytes;
        owner.stats.physical_download_bytes += audit.download_bytes;
        owner.stats.physical_download_calls += audit.download_calls;
    }
    if (runtime.hasActiveFrame(rt) or runtime.hasSubmittedFrame(rt)) return error.LayaResidentFrameAlreadyActive;
    var exec = Execution{ .a = a, .owner = owner, .control = control, .reuse_buffers = !platform.env.getenvBool("TERMITE_METAL_DISABLE_LAYA_WORKSPACE_REUSE"), .pool_limit = workspace / 2 };
    defer exec.tensors.deinit(a);
    defer exec.deinitPool();
    defer exec.retire();
    const int_values = try a.alloc(i32, ids.len + batch + markers.len * 2);
    defer a.free(int_values);
    for (ids, 0..) |id, i| int_values[i] = @intCast(id);
    for (kinds, 0..) |kind, i| int_values[ids.len + i] = @intCast(kind);
    for (markers, 0..) |pos, i| {
        int_values[ids.len + batch + i] = @intCast(pos);
        int_values[ids.len + batch + markers.len + i] = @intCast((i / count) * seq + @as(usize, @intCast(@max(pos, 0))));
    }
    var input = try upload(a, owner, std.mem.sliceAsBytes(int_values), int_values.len);
    defer input.deinit();
    var tokens = try input.retainedView(0, ids.len * 4, &.{@intCast(ids.len)});
    defer tokens.deinit();
    var types = try input.retainedView(ids.len * 4, batch * 4, &.{@intCast(batch)});
    defer types.deinit();
    var positions = try input.retainedView((ids.len + batch) * 4, markers.len * 4, &.{@intCast(markers.len)});
    defer positions.deinit();
    var gather = try input.retainedView((ids.len + batch + markers.len) * 4, markers.len * 4, &.{@intCast(markers.len)});
    defer gather.deinit();
    const mask_values = try a.alloc(f32, mask.len);
    defer a.free(mask_values);
    for (mask, mask_values) |v, *dest| dest.* = if (v == 0) 0 else 1;
    var gpu_mask = try upload(a, owner, std.mem.sliceAsBytes(mask_values), mask_values.len);
    defer gpu_mask.deinit();
    var retained_bias: ?MT = null;
    defer if (retained_bias) |*t| t.deinit();
    // Always drain/cancel before releasing any encoded tensor, including errors.
    defer {
        if (runtime.hasActiveFrame(rt)) runtime.cancelFrame(rt) catch {};
        if (runtime.hasSubmittedFrame(rt)) runtime.waitFrame(rt) catch {};
        owner.releaseActivationViews() catch {};
    }
    try runtime.beginFrame(rt);
    const local_bias = try exec.kernel(.laya_window, &.{}, &.{ cfg.num_attention_heads, seq, cfg.local_attention_window / 2 }, &.{});
    retained_bias = try local_bias.retainedCopy();
    exec.pinned_bias = local_bias.deviceHandle();
    const embedded = try exec.kernel(.gather_i32, &.{ try exec.constantTensor("encoder.embeddings.tok_embeddings.weight"), tokens }, &.{ cfg.vocab_size, h, batch * seq }, &.{});
    var hidden = try exec.norm(embedded, "encoder.embeddings.norm", batch * seq, h, cfg.layer_norm_eps, false);
    for (0..cfg.num_hidden_layers) |layer| {
        if (control) |c| try c.check();
        var name: [128]u8 = undefined;
        const normed = if (layer == 0) hidden else try exec.norm(hidden, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn_norm", .{layer}), batch * seq, h, cfg.layer_norm_eps, false);
        const qkv = try exec.linear(normed, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wqkv", .{layer}), batch * seq);
        const global = layer % cfg.global_attn_every_n_layers == 0;
        const attn = try exec.attention(qkv, gpu_mask, if (global) null else retained_bias.?, batch, seq, cfg.num_attention_heads, h, if (global) cfg.global_rope_theta else cfg.local_rope_theta);
        const proj = try exec.linear(attn, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wo", .{layer}), batch * seq);
        const residual = try exec.kernel(.add, &.{ hidden, proj }, &.{batch * seq * h}, &.{});
        const n = try exec.norm(residual, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp_norm", .{layer}), batch * seq, h, cfg.layer_norm_eps, false);
        const up = try exec.linear(n, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wi", .{layer}), batch * seq);
        const act = try exec.kernel(.laya_geglu, &.{up}, &.{ batch * seq, f }, &.{});
        const down = try exec.linear(act, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wo", .{layer}), batch * seq);
        hidden = try exec.kernel(.add, &.{ residual, down }, &.{batch * seq * h}, &.{});
        if ((layer + 1) % 2 == 0) hidden = try exec.drain(hidden);
    }
    hidden = try exec.norm(hidden, "encoder.final_norm", batch * seq, h, cfg.layer_norm_eps, false);
    hidden = try exec.kernel(.laya_type_add, &.{ hidden, try exec.constantTensor("type_emb.weight"), types }, &.{ batch, seq, h }, &.{});
    for (0..laya.head_layers) |layer| {
        var name: [128]u8 = undefined;
        const n1 = try exec.norm(hidden, try std.fmt.bufPrint(&name, "head.layers.{d}.norm1", .{layer}), batch * seq, h, 1e-5, true);
        const qkv = try exec.linear(n1, try std.fmt.bufPrint(&name, "head.layers.{d}.self_attn.in_proj", .{layer}), batch * seq);
        const attn = try exec.attention(qkv, gpu_mask, null, batch, seq, h / 64, h, null);
        const proj = try exec.linear(attn, try std.fmt.bufPrint(&name, "head.layers.{d}.self_attn.out_proj", .{layer}), batch * seq);
        const res = try exec.kernel(.add, &.{ hidden, proj }, &.{batch * seq * h}, &.{});
        const n2 = try exec.norm(res, try std.fmt.bufPrint(&name, "head.layers.{d}.norm2", .{layer}), batch * seq, h, 1e-5, true);
        const up = try exec.linear(n2, try std.fmt.bufPrint(&name, "head.layers.{d}.linear1", .{layer}), batch * seq);
        const act = try exec.kernel(.relu, &.{up}, &.{batch * seq * h * 4}, &.{});
        const down = try exec.linear(act, try std.fmt.bufPrint(&name, "head.layers.{d}.linear2", .{layer}), batch * seq);
        hidden = try exec.kernel(.add, &.{ res, down }, &.{batch * seq * h}, &.{});
        hidden = try exec.drain(hidden);
    }
    const gathered = try exec.kernel(.gather_i32, &.{ hidden, gather }, &.{ batch * seq, h, batch * count }, &.{});
    const n = try exec.norm(gathered, "scorer.0", batch * count, h, 1e-5, true);
    const s1 = try exec.linear(n, "scorer.1", batch * count);
    const sg = try exec.kernel(.laya_exact_gelu, &.{s1}, &.{batch * count * h}, &.{});
    const logits = try exec.linear(sg, "scorer.3", batch * count);
    const features = try exec.kernel(.laya_action_features, &.{ hidden, logits, positions }, &.{ batch, seq, h, count }, &.{});
    const a1 = try exec.linear(features, "act_head.0", batch);
    const ag = try exec.kernel(.laya_exact_gelu, &.{a1}, &.{batch * 256}, &.{});
    const acts = try exec.linear(ag, "act_head.2", batch);
    const result_values = if (decisions)
        try exec.kernel(.laya_decisions, &.{ logits, acts, positions, types, try exec.constantTensor("scales") }, &.{ batch, count, laya.n_act }, &.{})
    else
        try exec.kernel(.laya_logits, &.{ logits, acts, positions }, &.{ batch, count, laya.n_act }, &.{});
    try runtime.submitFrame(rt);
    try runtime.waitFrame(rt);
    try owner.releaseActivationViews();
    owner.stats.submissions += 1;
    const width = count + if (decisions) @as(usize, 6) else laya.n_act;
    const host = try a.alloc(f32, batch * width);
    defer a.free(host);
    if (audit.host_accesses != 0 or audit.download_bytes != 0) return error.LayaResidentIntermediateReadback;
    try result_values.downloadF32Into(host);
    if (audit.download_calls != 1 or audit.download_bytes != host.len * 4) return error.LayaResidentTransferMismatch;
    owner.stats.output_readback_bytes += host.len * 4;
    if (control) |c| try c.check();
    owner.stats.requests += 1;
    if (decisions) {
        for (0..batch) |b| if (host[b * width + count + 5] != 0) return error.InvalidLayaOutput;
        const out = try a.alloc(Tensor, 1);
        errdefer a.free(out);
        out[0] = try Tensor.initFloat32(a, "laya_decisions", &.{ @intCast(batch), @intCast(width) }, host);
        return out;
    }
    const out = try a.alloc(Tensor, 2);
    errdefer a.free(out);
    const zs = try a.alloc(f32, batch * count);
    defer a.free(zs);
    const actions = try a.alloc(f32, batch * laya.n_act);
    defer a.free(actions);
    for (0..batch) |b| {
        @memcpy(zs[b * count ..][0..count], host[b * width ..][0..count]);
        @memcpy(actions[b * laya.n_act ..][0..laya.n_act], host[b * width + count ..][0..laya.n_act]);
    }
    out[0] = try Tensor.initFloat32(a, "logits", &.{ @intCast(batch), @intCast(count) }, zs);
    errdefer out[0].deinit();
    out[1] = try Tensor.initFloat32(a, "action_logits", &.{ @intCast(batch), @intCast(laya.n_act) }, actions);
    return out;
}

test "laya resident workspace admission validates geometry before allocation" {
    const cfg = Config{ .laya = .{} };
    try std.testing.expectError(error.InvalidLayaInputs, workspaceBound(cfg, 0, 61, 4));
    try std.testing.expectError(error.InvalidLayaInputs, workspaceBound(cfg, 513, 61, 4));
    try std.testing.expectError(error.InvalidLayaInputs, workspaceBound(cfg, 1, 513, 4));
    try std.testing.expectError(error.InvalidLayaInputs, workspaceBound(cfg, 1, 61, 21));
    try std.testing.expect(try workspaceBound(cfg, 8, 512, 4) > try workspaceBound(cfg, 1, 61, 4));
}

test "laya resident decision kernel calibration padding ties and nonfinite rejection" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const pipeline = @import("../pipelines/laya.zig");
    const a = std.testing.allocator;
    var provider = try Provider.create();
    defer provider.deinitOwned();
    const rt = provider.raw_decode_runtime orelse return error.SkipZigTest;
    var owner = Owner{ .allocator = a, .provider = &provider, .runtime_identity = rt };
    var exec = Execution{ .a = a, .owner = &owner, .control = null };
    defer exec.tensors.deinit(a);
    defer exec.retire();
    const cfg = @import("../models/laya.zig").Config{
        .temperature = .{ 1.4, 0.7, 2.0 },
        .buckets = .{ .{ 0.5, 1.5, 2.5, 3.5 }, .{ 1.2, 0.8, 0.4, 2.2 }, .{ 0.6, null, null, null } },
    };
    var scales: [12]f32 = undefined;
    for (0..3) |kind| for ([_]usize{ 2, 3, 6, 11 }, 0..) |count, bucket| {
        scales[kind * 4 + bucket] = cfg.scale(@enumFromInt(kind), count);
    };
    var gs = try upload(a, &owner, std.mem.sliceAsBytes(&scales), scales.len);
    defer gs.deinit();
    const labels = [_][]const u8{ "false", "true", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19" };
    const descriptions = [_][]const u8{""} ** 20;
    for ([_]usize{ 2, 3, 5, 6, 10, 11, 20 }) |count| {
        for (0..3) |kind| {
            if (kind == 2 and count != 2) continue;
            for (0..3) |variant| {
                var logits: [20]f32 = @splat(1234); // Padded logits must be ignored.
                var positions: [20]i32 = @splat(-1);
                for (0..count) |i| {
                    logits[i] = if (variant == 0) 0 else if (variant == 1) @as(f32, @floatFromInt(i)) * 0.3 - 1 else if (i == 0) 10000 else -10000;
                    positions[i] = @intCast(i);
                }
                const acts = [_]f32{ -2, 3 };
                const types = [_]i32{@intCast(kind)};
                var gz = try upload(a, &owner, std.mem.sliceAsBytes(&logits), 20);
                defer gz.deinit();
                var ga = try upload(a, &owner, std.mem.sliceAsBytes(&acts), 2);
                defer ga.deinit();
                var gp = try upload(a, &owner, std.mem.sliceAsBytes(&positions), 20);
                defer gp.deinit();
                var gt = try upload(a, &owner, std.mem.sliceAsBytes(&types), 1);
                defer gt.deinit();
                const result = try exec.kernel(.laya_decisions, &.{ gz, ga, gp, gt, gs }, &.{ 1, 20, 2 }, &.{});
                var host: [26]f32 = undefined;
                try result.downloadF32Into(&host);
                const question = pipeline.Question{ .name = "test", .kind = @enumFromInt(kind), .instruction = "test", .labels = labels[0..count], .descriptions = descriptions[0..count] };
                const want = try pipeline.decode(a, cfg, question, logits[0..count], &acts);
                defer a.free(want.probabilities);
                for (want.probabilities, host[0..count]) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-6);
                for (host[count..20]) |v| try std.testing.expectEqual(@as(f32, 0), v);
                try std.testing.expectEqual(@as(f32, 0), host[25]);
                try std.testing.expectApproxEqAbs(want.confidence, host[21], 2e-6);
                try std.testing.expectApproxEqAbs(want.act_probability, host[24], 2e-6);
                if (want.expected_value) |v| try std.testing.expectApproxEqAbs(v, host[22], 2e-6);
                if (want.true_probability) |v| try std.testing.expectApproxEqAbs(v, host[23], 2e-6);
                if (variant == 0) try std.testing.expectEqual(@as(f32, 0), host[20]);
                exec.retire();
            }
        }
    }
    for ([_]f32{ std.math.nan(f32), std.math.inf(f32), -std.math.inf(f32) }) |invalid| {
        const logits = [_]f32{ 0, invalid };
        const acts = [_]f32{ 0, 1 };
        const positions = [_]i32{ 0, 1 };
        const types = [_]i32{0};
        var gz = try upload(a, &owner, std.mem.sliceAsBytes(&logits), 2);
        defer gz.deinit();
        var ga = try upload(a, &owner, std.mem.sliceAsBytes(&acts), 2);
        defer ga.deinit();
        var gp = try upload(a, &owner, std.mem.sliceAsBytes(&positions), 2);
        defer gp.deinit();
        var gt = try upload(a, &owner, std.mem.sliceAsBytes(&types), 1);
        defer gt.deinit();
        const result = try exec.kernel(.laya_decisions, &.{ gz, ga, gp, gt, gs }, &.{ 1, 2, 2 }, &.{});
        var host: [8]f32 = undefined;
        try result.downloadF32Into(&host);
        try std.testing.expectEqual(@as(f32, 1), host[7]);
        exec.retire();
    }
}
