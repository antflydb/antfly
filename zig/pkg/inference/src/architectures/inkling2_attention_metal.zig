// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Resident Metal attention weights in the shared Inkling checkpoint layout.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const layout = @import("inkling2_attention_weights.zig");
const metal = @import("../backends/metal_runtime.zig");
const Tensor = @import("../backends/metal_tensor.zig").MetalTensor;
const Weight = layout.Weight;

pub const Layer = struct {
    local: bool,
    projections: Tensor,
    weights: [@typeInfo(Weight).@"enum".field_names.len]?Tensor = @splat(null),

    pub fn load(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: *const checkpoint.Checkpoint, layer: usize) !Layer {
        return loadTensors(a, runtime, source, source.config.value.text_config, layer);
    }

    pub fn loadTensors(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: anytype, text: checkpoint.TextConfig, layer: usize) !Layer {
        const local = try layout.isLocal(text, layer);
        const hidden: usize = text.hidden_size;
        const elements = try std.math.mul(usize, hidden, layout.projectionSize(text, &layout.projection_weights));
        var result: Layer = .{ .local = local, .projections = try allocate(a, runtime, elements) };
        errdefer result.deinit();
        var offset: usize = 0;
        for (layout.projection_weights) |key| {
            const count = hidden * layout.projectionWidth(text, key);
            const bytes = count * 2;
            var destination = try result.projections.retainedView(offset, bytes, &.{@intCast(count)});
            errdefer destination.deinit();
            try destination.uploadBytes(try source.layerTensor(layer, layout.tensorName(key, local)));
            result.weights[@backingInt(key)] = destination;
            offset += bytes;
        }
        for ([_]Weight{ .out, .q_scale, .r_scale, .k_taps, .v_taps, .k_dynamic, .v_dynamic }) |key| {
            if (local and (key == .k_dynamic or key == .v_dynamic)) continue;
            result.weights[@backingInt(key)] = try upload(a, runtime, try source.layerTensor(layer, layout.tensorName(key, local)));
        }
        const basis = try source.layerTensor(layer, layout.tensorName(.relative_basis, local));
        const transposed = try layout.transposeRelative(a, basis, text.d_rel, text.rel_extent);
        defer a.free(transposed);
        result.weights[@backingInt(Weight.relative_basis)] = try upload(a, runtime, transposed);
        return result;
    }

    pub const Inputs = struct { query: *const Tensor, key: *const Tensor, value: *const Tensor };
    pub const Projections = struct {
        query: *Tensor,
        relative: *Tensor,
        gate: *Tensor,
        sink: *Tensor,
        key: *Tensor,
        value: *Tensor,
        key_taps: ?*Tensor = null,
        value_taps: ?*Tensor = null,
    };

    /// Project normalized visit inputs into the operands for paged attention.
    pub fn projectInputs(self: *const Layer, runtime: *metal.RawMetalDecodeRuntime, text: checkpoint.TextConfig, rows: u32, inputs: Inputs, outputs: Projections) !void {
        if (self.local != (outputs.key_taps == null) or self.local != (outputs.value_taps == null))
            return error.InvalidShape;
        const destinations = [_]*Tensor{ outputs.query, outputs.relative, outputs.gate, outputs.sink, outputs.key, outputs.value };
        const sources = [_]*const Tensor{ inputs.query, inputs.query, inputs.query, inputs.query, inputs.key, inputs.value };
        for (layout.projection_weights, sources, destinations) |key, input, output| {
            try self.project(runtime, key, rows, text.hidden_size, layout.projectionWidth(text, key), input, output);
        }
        if (!self.local) {
            const width = @as(u32, text.num_key_value_heads) * text.sconv_kernel_size;
            try self.project(runtime, .k_dynamic, rows, text.hidden_size, width, inputs.key, outputs.key_taps.?);
            try self.project(runtime, .v_dynamic, rows, text.hidden_size, width, inputs.value, outputs.value_taps.?);
        }
    }

    pub fn normalizeHeads(self: *const Layer, runtime: *metal.RawMetalDecodeRuntime, text: checkpoint.TextConfig, rows: u32, position: u32, query: *const Tensor, relative: *const Tensor, output_query: *Tensor, output_relative: *Tensor) !void {
        var norm: metal.HeadNormBf16 = .{
            .rows = rows,
            .heads = text.num_attention_heads,
            .dim = text.head_dim,
            .position = position,
            .log_floor = if (self.local) 0 else text.log_scaling_n_floor,
            .clip = text.qk_l2_clip_threshold,
            .epsilon = text.rms_norm_eps,
            .log_alpha = text.log_scaling_alpha,
        };
        try norm.encode(runtime, query, self.get(.q_scale) orelse return error.MissingWeight, output_query);
        norm.dim = text.d_rel;
        norm.clip = text.rel_l2_clip_threshold;
        try norm.encode(runtime, relative, self.get(.r_scale) orelse return error.MissingWeight, output_relative);
    }

    pub fn projectRelative(self: *const Layer, runtime: *metal.RawMetalDecodeRuntime, text: checkpoint.TextConfig, rows: u32, input: *const Tensor, output: *Tensor) !void {
        const head_rows = try std.math.mul(u32, rows, text.num_attention_heads);
        try self.project(runtime, .relative_basis, head_rows, text.d_rel, text.rel_extent, input, output);
    }

    pub fn projectOutput(self: *const Layer, runtime: *metal.RawMetalDecodeRuntime, text: checkpoint.TextConfig, rows: u32, input: *const Tensor, output: *Tensor) !void {
        try self.project(runtime, .out, rows, layout.projectionWidth(text, .q), text.hidden_size, input, output);
    }

    const kv = @import("../runtime/kv/hybrid_paged.zig");

    pub const History = struct {
        key: ?*const Tensor,
        value: ?*const Tensor,
        next_key: *Tensor,
        next_value: *Tensor,
    };

    pub const Workspace = struct {
        projected: Projections,
        normalized_query: *Tensor,
        normalized_relative: *Tensor,
        relative_logits: *Tensor,
        staged_key: *Tensor,
        staged_value: *Tensor,
        attention_scratch: *Tensor,
        heads: *Tensor,
    };

    /// Encodes a complete attention visit from already normalized hidden rows.
    /// The caller owns admission, the frame fence and history publication.
    pub fn encode(
        self: *const Layer,
        runtime: *metal.RawMetalDecodeRuntime,
        text: checkpoint.TextConfig,
        append: *kv.Append,
        inputs: Inputs,
        page_ids: *const Tensor,
        planes: metal.PagedRelativeBf16Attention.Planes,
        history: History,
        workspace: Workspace,
        output: *Tensor,
    ) !void {
        if (text.head_dim != 128 or text.sconv_kernel_size != 4) return error.InvalidShape;
        const group: kv.Group = if (self.local) .sliding else .full;
        const range = try append.stagedRange(group);
        const start = std.math.cast(u32, append.sequence.committed_tokens) orelse return error.InvalidShape;
        const rows = std.math.cast(u32, range.visible_end - start) orelse return error.InvalidShape;
        const q = workspace.projected;
        try self.projectInputs(runtime, text, rows, inputs, q);
        try self.normalizeHeads(runtime, text, rows, start, q.query, q.relative, workspace.normalized_query, workspace.normalized_relative);
        try self.projectRelative(runtime, text, rows, workspace.normalized_relative, workspace.relative_logits);
        try metal.CausalPagedRelativeBf16.encode(runtime, append, group, .{
            .rows = rows,
            .start = start,
            .q_heads = text.num_attention_heads,
            .kv_heads = text.num_key_value_heads,
            .window = if (self.local) text.sliding_window_size else 0,
            .relative_extent = text.rel_extent,
            .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(text.head_dim))),
        }, .{
            .query = workspace.normalized_query,
            .relative = workspace.relative_logits,
            .page_ids = page_ids,
            .sinks = q.sink,
            .gates = q.gate,
        }, planes, .{
            .input = .{ .values = q.key, .weight = self.get(.k_taps) orelse return error.MissingWeight, .history = history.key, .dynamic_taps = q.key_taps },
            .staged = workspace.staged_key,
            .next_history = history.next_key,
        }, .{
            .input = .{ .values = q.value, .weight = self.get(.v_taps) orelse return error.MissingWeight, .history = history.value, .dynamic_taps = q.value_taps },
            .staged = workspace.staged_value,
            .next_history = history.next_value,
        }, .{
            .channels = @as(u32, text.num_key_value_heads) * text.head_dim,
            .rows = rows,
            .valid_history = @min(3, start),
            .output_scale = if (self.local) 1.0 / @sqrt(@as(f32, 5.0)) else 1.0 / 3.0,
            .clip = text.qk_l2_clip_threshold,
            .epsilon = text.rms_norm_eps,
        }, workspace.attention_scratch, workspace.heads);
        try self.projectOutput(runtime, text, rows, workspace.heads, output);
    }

    fn project(self: *const Layer, runtime: *metal.RawMetalDecodeRuntime, key: Weight, rows: u32, in: u32, out: u32, input: *const Tensor, output: *Tensor) !void {
        if (output.dtype != .i16) return error.UnsupportedTensorType;
        const projection: metal.DenseBf16Projection = .{ .rows = rows, .input_width = in, .output_width = out };
        try projection.encode(runtime, input, self.get(key) orelse return error.MissingWeight, output);
    }

    pub fn deinit(self: *Layer) void {
        for (&self.weights) |*entry| {
            if (entry.*) |*weight| weight.deinit();
        }
        self.projections.deinit();
    }

    pub fn get(self: *const Layer, key: Weight) ?*const Tensor {
        return if (self.weights[@backingInt(key)]) |*weight| weight else null;
    }
};

/// One BF16 arena and one reduction plane, reusable across all visits.
pub const RequestWorkspace = struct {
    const Plane = enum {
        query,
        relative,
        gate,
        sink,
        key,
        value,
        normalized_query,
        normalized_relative,
        relative_logits,
        staged_key,
        staged_value,
        heads,
        key_taps,
        value_taps,
    };
    const plane_count = @typeInfo(Plane).@"enum".field_names.len;
    backing: Tensor,
    views: [plane_count]Tensor,
    reduction: Tensor,

    pub const Plan = struct {
        offsets: [plane_count]usize,
        elements: [plane_count]i32,
        bytes: usize,
        reduction_elements: i32,

        pub fn init(text: checkpoint.TextConfig, rows: u32, max_tokens: u32) !Plan {
            if (rows == 0 or rows > max_tokens or rows > 256) return error.InvalidShape;
            const query = layout.projectionWidth(text, .q);
            const key = layout.projectionWidth(text, .k);
            const relative = layout.projectionWidth(text, .r);
            const tap_width = @as(u32, text.num_key_value_heads) * text.sconv_kernel_size;
            const widths: [plane_count]u32 = .{
                query,     relative,  query,                                                text.num_attention_heads, key, key,
                query,     relative,  @as(u32, text.num_attention_heads) * text.rel_extent, key,                      key, query,
                tap_width, tap_width,
            };
            const attention: metal.PagedRelativeBf16Attention = .{
                .query = .{
                    .rows = rows,
                    .start = max_tokens - rows,
                    .q_heads = text.num_attention_heads,
                    .kv_heads = text.num_key_value_heads,
                    .relative_extent = text.rel_extent,
                    .score_scale = 1,
                },
                .range = .{
                    .first_page = 0,
                    .page_count = max_tokens,
                    .physical_pages = max_tokens,
                    .page_tokens = 1,
                    .visible_start = 0,
                    .visible_end = max_tokens,
                },
            };
            var result: Plan = .{ .offsets = undefined, .elements = undefined, .bytes = 0, .reduction_elements = try attention.scratchElements() };
            for (widths, &result.offsets, &result.elements) |width, *offset, *elements| {
                if (width == 0) return error.InvalidShape;
                const count = @as(usize, rows) * width;
                elements.* = std.math.cast(i32, count) orelse return error.InvalidShape;
                offset.* = std.mem.alignForward(usize, result.bytes, 256);
                result.bytes = offset.* + count * 2;
            }
            return result;
        }
    };

    pub fn init(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, plan: Plan) !RequestWorkspace {
        const size = std.math.cast(i32, plan.bytes / 2) orelse return error.InvalidShape;
        var backing = try Tensor.deviceAllocateTyped(a, runtime, .i16, .private, &.{size});
        errdefer backing.deinit();
        var reduction = try Tensor.deviceAllocateTyped(a, runtime, .f32, .private, &.{plan.reduction_elements});
        errdefer reduction.deinit();
        var views: [plane_count]Tensor = undefined;
        var count: usize = 0;
        errdefer for (views[0..count]) |*view| view.deinit();
        for (&views, plan.offsets, plan.elements) |*view, offset, elements| {
            view.* = try backing.retainedView(offset, @as(usize, @intCast(elements)) * 2, &.{elements});
            count += 1;
        }
        return .{ .backing = backing, .views = views, .reduction = reduction };
    }

    pub fn deinit(self: *RequestWorkspace) void {
        for (&self.views) |*view| view.deinit();
        self.reduction.deinit();
        self.backing.deinit();
    }

    fn plane(self: *RequestWorkspace, key: Plane) *Tensor {
        return &self.views[@backingInt(key)];
    }

    pub fn bind(self: *RequestWorkspace, local: bool) Layer.Workspace {
        return .{
            .projected = .{
                .query = self.plane(.query),
                .relative = self.plane(.relative),
                .gate = self.plane(.gate),
                .sink = self.plane(.sink),
                .key = self.plane(.key),
                .value = self.plane(.value),
                .key_taps = if (local) null else self.plane(.key_taps),
                .value_taps = if (local) null else self.plane(.value_taps),
            },
            .normalized_query = self.plane(.normalized_query),
            .normalized_relative = self.plane(.normalized_relative),
            .relative_logits = self.plane(.relative_logits),
            .staged_key = self.plane(.staged_key),
            .staged_value = self.plane(.staged_value),
            .heads = self.plane(.heads),
            .attention_scratch = &self.reduction,
        };
    }
};

fn allocate(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, elements: usize) !Tensor {
    if (elements == 0) return error.InvalidShape;
    const extent = std.math.cast(i32, elements) orelse return error.InvalidShape;
    return Tensor.deviceAllocateTyped(a, runtime, .i16, .shared, &.{extent});
}

fn upload(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, bytes: []const u8) !Tensor {
    if (bytes.len % 2 != 0) return error.InvalidShape;
    var result = try allocate(a, runtime, bytes.len / 2);
    errdefer result.deinit();
    try result.uploadBytes(bytes);
    return result;
}

test "Inkling Metal attention weights preserve packed offsets and local/global layout" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const Fixture = struct {
        const data_a = [_]u8{ 0x80, 0x3f };
        const data_b = [_]u8{ 0x00, 0x40 };

        fn layerTensor(_: @This(), layer: usize, name: []const u8) ![]const u8 {
            if (std.mem.eql(u8, name, "attn.k_sconv.weight_proj")) {
                if (layer == 0) return error.UnexpectedDynamicWeight;
                return &data_b;
            }
            return if (std.mem.eql(u8, name, "attn.wq_du.weight")) &data_b else &data_a;
        }
    };
    var text: checkpoint.TextConfig = undefined;
    text.hidden_size = 1;
    text.num_attention_heads = 1;
    text.num_key_value_heads = 1;
    text.head_dim = 1;
    text.d_rel = 1;
    text.rel_extent = 1;
    text.sconv_kernel_size = 1;
    text.log_scaling_n_floor = 1;
    text.log_scaling_alpha = 1;
    text.qk_l2_clip_threshold = 1;
    text.rel_l2_clip_threshold = 1;
    text.rms_norm_eps = 0.000001;
    @memset(&text.local_layer_ids, 0);
    for ([_]usize{ 0, 1 }) |index| {
        var layer = try Layer.loadTensors(std.testing.allocator, runtime, Fixture{}, text, index);
        defer layer.deinit();
        const q = layer.get(.q) orelse return error.MissingWeight;
        const k = layer.get(.k) orelse return error.MissingWeight;
        try std.testing.expectEqual(q.deviceHandle(), k.deviceHandle());
        try std.testing.expectEqual(@as(usize, 8), k.deviceByteOffset() - q.deviceByteOffset());
        var bytes: [2]u8 = undefined;
        try q.downloadBytesInto(&bytes);
        try std.testing.expectEqual(Fixture.data_b, bytes);
        try k.downloadBytesInto(&bytes);
        try std.testing.expectEqual(Fixture.data_a, bytes);
        try std.testing.expectEqual(index == 0, layer.local);
        try std.testing.expectEqual(index == 1, layer.get(.k_dynamic) != null);

        var source = try upload(std.testing.allocator, runtime, &Fixture.data_b);
        defer source.deinit();
        var projected: [12]Tensor = undefined;
        var count: usize = 0;
        defer for (projected[0..count]) |*output| output.deinit();
        for (&projected) |*output| {
            output.* = try allocate(std.testing.allocator, runtime, 1);
            count += 1;
        }
        try metal.beginFrame(runtime);
        errdefer metal.cancelFrame(runtime) catch {};
        try layer.projectInputs(runtime, text, 1, .{ .query = &source, .key = &source, .value = &source }, .{
            .query = &projected[0],
            .relative = &projected[1],
            .gate = &projected[2],
            .sink = &projected[3],
            .key = &projected[4],
            .value = &projected[5],
            .key_taps = if (layer.local) null else &projected[6],
            .value_taps = if (layer.local) null else &projected[7],
        });
        try layer.normalizeHeads(runtime, text, 1, 1, &projected[0], &projected[1], &projected[10], &projected[11]);
        try layer.projectRelative(runtime, text, 1, &projected[11], &projected[8]);
        try layer.projectOutput(runtime, text, 1, &projected[0], &projected[9]);
        try metal.submitFrame(runtime);
        try metal.waitFrame(runtime);
        const normalized: u16 = if (layer.local) 0x3f80 else 0x3fd9;
        const expected = [_]u16{ 0x4080, 0x4000, 0x4000, 0x4000, 0x4000, 0x4000, 0x4080, 0x4000, normalized, 0x4080, normalized, normalized };
        for (&projected, expected, 0..) |*output, bits, slot| {
            if (layer.local and (slot == 6 or slot == 7)) continue;
            var actual: u16 = undefined;
            try output.downloadBytesInto(std.mem.asBytes(&actual));
            try std.testing.expectEqual(bits, actual);
        }
    }
}

test "Inkling Metal attention weights drive local and global visits across committed pages" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const kv = @import("../runtime/kv/hybrid_paged.zig");
    const State = @import("inkling2_state_metal.zig").State;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const Fixture = struct {
        const zeros: [512]u16 = @splat(0);
        const ones: [128]u16 = @splat(0x3f80);
        const dynamic = [4]u16{ 0x3f00, 0, 0, 0 };

        fn layerTensor(_: @This(), _: usize, name: []const u8) ![]const u8 {
            if (std.mem.eql(u8, name, "attn.wv_dv.weight") or std.mem.eql(u8, name, "attn.wo_ud.weight"))
                return std.mem.asBytes(&ones);
            if (std.mem.endsWith(u8, name, "sconv.weight") or std.mem.endsWith(u8, name, "sconv.static_w"))
                return std.mem.asBytes(&zeros);
            if (std.mem.endsWith(u8, name, "sconv.weight_proj")) return std.mem.asBytes(&dynamic);
            if (std.mem.eql(u8, name, "attn.wq_du.weight") or std.mem.eql(u8, name, "attn.wk_dv.weight") or
                std.mem.eql(u8, name, "attn.gate_attn.weight") or std.mem.eql(u8, name, "attn.q_l2_clip.scale"))
                return std.mem.sliceAsBytes(zeros[0..128]);
            return std.mem.sliceAsBytes(zeros[0..1]);
        }
    };
    var text: checkpoint.TextConfig = undefined;
    text.hidden_size = 1;
    text.num_attention_heads = 1;
    text.num_key_value_heads = 1;
    text.head_dim = 128;
    text.d_rel = 1;
    text.rel_extent = 1;
    text.sconv_kernel_size = 4;
    text.sliding_window_size = 1;
    text.log_scaling_alpha = 1;
    text.log_scaling_n_floor = 1;
    text.qk_l2_clip_threshold = 1;
    text.rel_l2_clip_threshold = 1;
    text.rms_norm_eps = 0.000001;
    @memset(&text.local_layer_ids, 0);
    for ([_]usize{ 0, 1 }) |index| {
        var layer = try Layer.loadTensors(a, runtime, Fixture{}, text, index);
        defer layer.deinit();
        const group: kv.Group = if (layer.local) .sliding else .full;
        var state: State = undefined;
        try state.init(a, runtime, .{
            .layout = .{ .page_tokens = 1, .max_tokens = 2, .window_tokens = 1, .max_append_tokens = 1 },
            .full_slots = 1,
            .sliding_slots = 1,
            .kv_heads = 1,
            .head_dim = 128,
        }, 128);
        defer state.deinit();

        var source = try allocate(a, runtime, 1);
        defer source.deinit();
        var workspace = try RequestWorkspace.init(a, runtime, try .init(text, 1, 2));
        defer workspace.deinit();
        var output = try allocate(a, runtime, 1);
        defer output.deinit();
        var residual_input = try upload(a, runtime, std.mem.asBytes(&Fixture.ones));
        defer residual_input.deinit();
        var residual_weight = try upload(a, runtime, std.mem.asBytes(&Fixture.zeros));
        defer residual_weight.deinit();
        var residual_output = try allocate(a, runtime, 128);
        defer residual_output.deinit();
        for ([_]u16{ 0x3f80, 0xbf80 }, 0..) |activation, position| {
            var append = try state.prepare(1);
            defer append.abort();
            const binding = try state.binding(&append, .{ .group = group, .slot = 0 });
            if (position == 0) {
                try std.testing.expectEqual(null, binding.attention_residual.committed);
                try std.testing.expectEqual(null, binding.expert_residual.committed);
            } else {
                try std.testing.expect(binding.attention_residual.committed.? != binding.attention_residual.next);
                try std.testing.expect(binding.expert_residual.committed.? != binding.expert_residual.next);
            }
            try source.uploadBytes(std.mem.asBytes(&activation));
            try metal.beginFrame(runtime);
            errdefer metal.cancelFrame(runtime) catch {};
            try layer.encode(runtime, text, &append, .{ .query = &source, .key = &source, .value = &source }, binding.page_ids, binding.planes, binding.history, workspace.bind(layer.local), &output);
            try state.convolveResidual(&append, 128, &residual_input, &residual_weight, binding.attention_residual, &residual_output);
            try state.convolveResidual(&append, 128, &residual_input, &residual_weight, binding.expert_residual, &residual_output);
            try metal.submitFrame(runtime);
            try state.publish(&append);
            var next_history: [384]u16 = undefined;
            try binding.expert_residual.next.downloadBytesInto(std.mem.asBytes(&next_history));
            for (next_history[0 .. (2 - position) * 128]) |element| try std.testing.expectEqual(@as(u16, 0), element);
            for (next_history[(2 - position) * 128 ..]) |element| try std.testing.expectEqual(@as(u16, 0x3f80), element);
            try std.testing.expectEqual(position + 1, state.sequence.committed_tokens);
            var result: u16 = undefined;
            try output.downloadBytesInto(std.mem.asBytes(&result));
            // Global retains +1 and -1 values, while the one-token window sees only -1.
            const expected: u16 = if (position == 0) 0x4200 else if (layer.local) 0xc200 else 0;
            try std.testing.expectEqual(expected, result);
        }
    }
}
