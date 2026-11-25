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

//! Resident Inkling text decoder using the shared checkpoint visit schedule.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const attention = @import("inkling2_attention_metal.zig");
const moe = @import("inkling2_moe_metal.zig");
const Controller = @import("inkling2_controller_metal.zig").Controller;
const Endpoints = @import("inkling2_endpoints_metal.zig").Endpoints;
const State = @import("inkling2_state_metal.zig").State;
const Stack = @import("inkling2_residual_metal.zig").Stack;
const kv = @import("../runtime/kv/hybrid_paged.zig");
const metal = @import("../backends/metal_runtime.zig");
const tensors = @import("../backends/metal_tensor.zig");
const Tensor = tensors.MetalTensor;
const Runtime = metal.RawMetalDecodeRuntime;

fn allocate(a: std.mem.Allocator, runtime: *Runtime, dtype: tensors.DType, elements: usize, mode: tensors.StorageMode) !Tensor {
    return Tensor.deviceAllocateTyped(a, runtime, dtype, mode, &.{std.math.cast(i32, elements) orelse return error.InvalidShape});
}

fn upload(a: std.mem.Allocator, runtime: *Runtime, dtype: tensors.DType, bytes: []const u8) !Tensor {
    if (bytes.len == 0 or bytes.len % dtype.byteSize() != 0) return error.InvalidShape;
    var tensor = try allocate(a, runtime, dtype, bytes.len / dtype.byteSize(), .shared);
    errdefer tensor.deinit();
    try tensor.uploadBytes(bytes);
    return tensor;
}

fn row(tensor: *const Tensor, index: usize, count: usize) !Tensor {
    const bytes = count * tensor.dtype.byteSize();
    return tensor.retainedView(index * bytes, bytes, &.{@intCast(count)});
}

const Layer = struct {
    attention: attention.Layer,
    experts: moe.Layer,
    weights: [8]?Tensor = @splat(null),
    controllers: [4]?Controller = @splat(null),

    const Weight = enum { q_norm, k_norm, v_norm, expert_norm, attention_taps, expert_taps, attention_query, expert_query };

    fn weight(self: *const Layer, key: Weight) *const Tensor {
        return if (self.weights[@backingInt(key)]) |*value| value else unreachable;
    }

    fn load(a: std.mem.Allocator, runtime: *Runtime, source: *const checkpoint.Checkpoint, layer: usize) !Layer {
        var at = try attention.Layer.load(a, runtime, source, layer);
        const experts = moe.Layer.load(a, runtime, source, layer) catch |err| {
            at.deinit();
            return err;
        };
        var result: Layer = .{ .attention = at, .experts = experts };
        errdefer result.deinit();
        const names = [_][]const u8{
            if (at.local) "attn_norm.weight" else "q_input_norm.weight",
            "k_input_norm.weight",
            "v_input_norm.weight",
            "mlp_norm.weight",
            "attn_sconv.weight",
            "mlp_sconv.weight",
        };
        for (names, 0..) |name, index| {
            if (at.local and (index == 1 or index == 2)) continue;
            result.weights[index] = try upload(a, runtime, .i16, try source.layerTensor(layer, name));
        }
        var name_buf: [128]u8 = undefined;
        if (layer != 0) {
            const name = try std.fmt.bufPrint(&name_buf, "model.llm.attn_res.attn_queries.{d}", .{layer - 1});
            result.weights[@backingInt(Weight.attention_query)] = try upload(a, runtime, .f32, try source.reader.rawTensorBytes(name));
        }
        const name = try std.fmt.bufPrint(&name_buf, "model.llm.attn_res.mlp_queries.{d}", .{layer});
        result.weights[@backingInt(Weight.expert_query)] = try upload(a, runtime, .f32, try source.reader.rawTensorBytes(name));
        const loop = source.config.value.text_config.loop_config;
        if (layer >= loop.core_start and layer < loop.core_end) {
            for (std.enums.values(checkpoint.ControllerStream), &result.controllers) |stream, *controller|
                controller.* = try Controller.load(a, runtime, source, layer, stream);
        }
        return result;
    }

    fn deinit(self: *Layer) void {
        for (&self.controllers) |*entry| if (entry.*) |*controller| controller.deinit();
        for (&self.weights) |*entry| if (entry.*) |*weight_tensor| weight_tensor.deinit();
        self.experts.deinit();
        self.attention.deinit();
    }
};

pub const Decoder = struct {
    runtime: *Runtime,
    text: checkpoint.TextConfig,
    endpoints: Endpoints,
    layers: [checkpoint.layer_count]Layer,
    schedule: [checkpoint.visit_count]checkpoint.Visit,
    controller_queries: [2]Tensor,

    pub fn load(a: std.mem.Allocator, runtime: *Runtime, source: *const checkpoint.Checkpoint) !Decoder {
        var text = source.config.value.text_config;
        text.model_type = "inkling2_loop_text";
        var result: Decoder = .{
            .runtime = runtime,
            .text = text,
            .schedule = source.schedule,
            .endpoints = try Endpoints.load(a, runtime, source),
            .layers = undefined,
            .controller_queries = undefined,
        };
        errdefer result.endpoints.deinit();
        result.controller_queries[0] = try upload(a, runtime, .f32, try source.reader.rawTensorBytes("model.llm.middle_cycle_cross_pass_queries"));
        errdefer result.controller_queries[0].deinit();
        result.controller_queries[1] = try upload(a, runtime, .f32, try source.reader.rawTensorBytes("model.llm.middle_cycle_cross_pass_kv_queries"));
        errdefer result.controller_queries[1].deinit();
        var count: usize = 0;
        errdefer for (result.layers[0..count]) |*layer| layer.deinit();
        for (&result.layers, 0..) |*layer, index| {
            layer.* = try Layer.load(a, runtime, source, index);
            count += 1;
        }
        return result;
    }

    pub fn deinit(self: *Decoder) void {
        for (&self.layers) |*layer| layer.deinit();
        for (&self.controller_queries) |*weight| weight.deinit();
        self.endpoints.deinit();
    }

    fn query(self: *const Decoder, layer: usize, stream: checkpoint.ControllerStream) !Tensor {
        const bank: usize = @intFromBool(stream == .k or stream == .v);
        const lane: usize = @intFromBool(stream == .v or stream == .mlp);
        return row(&self.controller_queries[bank], 2 * (layer - self.text.loop_config.core_start) + lane, self.text.hidden_size);
    }
};

const Temp = enum {
    query,
    key,
    value,
    raw,
    convolved_attention,
    convolved_expert,
    normalized,
    mix,
    shared_mix,
    message,
    pair,
    rank,
    gates,
    router,
    selected,
    route_weights,
    gate_up,
    activated,
    projected,
    logits,
};

pub const Request = struct {
    model: *const Decoder,
    state: State,
    attention_workspace: attention.RequestWorkspace,
    stack: Stack,
    temps: [@typeInfo(Temp).@"enum".field_names.len]Tensor,
    host_logits: []f32,
    allocator: std.mem.Allocator,

    /// Initialize the request in its final storage so that KV's metadata pools
    /// retain stable addresses across submissions.
    pub fn init(self: *Request, a: std.mem.Allocator, model: *const Decoder, max_tokens: u32) !void {
        const text = model.text;
        if (max_tokens == 0 or max_tokens > text.model_max_length) return error.InvalidInkling2Context;
        const runtime = model.runtime;
        const width = text.hidden_size;
        const shape = try model.layers[0].experts.geometry(text, 1).scratchShape();
        self.* = .{ .model = model, .state = undefined, .stack = undefined, .attention_workspace = undefined, .temps = undefined, .host_logits = undefined, .allocator = a };
        self.host_logits = try a.alloc(f32, text.unpadded_vocab_size);
        errdefer a.free(self.host_logits);
        self.stack = try Stack.init(a, runtime, width, 1);
        errdefer self.stack.deinit();
        self.attention_workspace = try .init(a, runtime, try .init(text, 1, max_tokens));
        errdefer self.attention_workspace.deinit();
        const counts = [_]usize{
            width,                 width,                      width,                 width,                 width,                    width,                   width,                   width,                     width,                     width,
            @as(usize, width) * 2, text.loop_config.gate_rank, @as(usize, width) * 2, text.n_routed_experts, @intCast(shape.selected), @intCast(shape.weights), @intCast(shape.gate_up), @intCast(shape.activated), @intCast(shape.projected), text.unpadded_vocab_size,
        };
        var initialized: usize = 0;
        errdefer for (self.temps[0..initialized]) |*tensor| tensor.deinit();
        for (&self.temps, std.enums.values(Temp), counts) |*tensor, kind, elements| {
            const dtype: tensors.DType = switch (kind) {
                .mix, .shared_mix, .message, .gates, .router, .route_weights, .logits => .f32,
                .selected => .i32,
                else => .i16,
            };
            tensor.* = try allocate(a, runtime, dtype, elements, if (kind == .logits) .shared else .private);
            initialized += 1;
        }
        try self.state.init(a, runtime, .{
            .layout = .{ .page_tokens = 16, .window_tokens = text.sliding_window_size, .max_tokens = max_tokens, .max_append_tokens = 1 },
            .full_slots = checkpoint.full_slot_count,
            .sliding_slots = checkpoint.sliding_slot_count,
            .kv_heads = text.num_key_value_heads,
            .head_dim = text.head_dim,
        }, width);
    }

    pub fn deinit(self: *Request) void {
        self.state.deinit();
        for (&self.temps) |*tensor| tensor.deinit();
        self.attention_workspace.deinit();
        self.stack.deinit();
        self.allocator.free(self.host_logits);
    }

    fn get(self: *Request, key: Temp) *Tensor {
        return &self.temps[@backingInt(key)];
    }

    fn control(self: *Request, layer: *const Layer, layer_index: usize, stream: checkpoint.ControllerStream, dest: *const Tensor, gamma: *const Tensor, output: *Tensor) !void {
        const runtime = self.model.runtime;
        const eps = self.model.text.rms_norm_eps;
        var query = try self.model.query(layer_index, stream);
        defer query.deinit();
        try self.stack.mixBank(runtime, eps, &query, self.get(.message));
        const ctrl = if (layer.controllers[@backingInt(stream)]) |*value| value else return error.InvalidInkling2Schedule;
        try ctrl.encode(runtime, 1, eps, dest, self.get(.message), gamma, .{
            .pair = self.get(.pair),
            .rank = self.get(.rank),
            .gates = self.get(.gates),
        }, output);
    }

    fn visit(self: *Request, pending: *kv.Append, op: checkpoint.Visit) !void {
        const text = self.model.text;
        const runtime = self.model.runtime;
        const eps = text.rms_norm_eps;
        const layer = &self.model.layers[op.weight_layer];
        const local = layer.attention.local;
        const looped = op.pass != 0;
        try self.stack.advance(op, text.loop_config.core_start);

        const destinations = [_]*Tensor{ self.get(.query), self.get(.key), self.get(.value) };
        for ([_]checkpoint.ControllerStream{ .attn, .k, .v }, [_]Layer.Weight{ .q_norm, .k_norm, .v_norm }, destinations, 0..) |stream, norm, dest, index| {
            if (local and !looped and index != 0) break;
            const gamma = layer.weight(if (local) .q_norm else norm);
            if (op.weight_layer == 0) {
                try metal.rowRmsBf16(runtime, 1, text.hidden_size, eps, self.stack.current(), gamma, dest);
                continue;
            }
            if (!looped or !local or index == 0) {
                var query = try row(layer.weight(.attention_query), index, text.hidden_size);
                defer query.deinit();
                if (looped) {
                    try self.stack.mix(runtime, eps, &query, null, self.get(.shared_mix));
                } else try self.stack.mix(runtime, eps, &query, gamma, dest);
            }
            if (looped) try self.control(layer, op.weight_layer, stream, self.get(.shared_mix), gamma, dest);
        }
        const binding = try self.state.binding(pending, .{ .group = op.group, .slot = op.slot });
        try layer.attention.encode(runtime, text, pending, .{
            .query = destinations[0],
            .key = if (local and !looped) destinations[0] else destinations[1],
            .value = if (local and !looped) destinations[0] else destinations[2],
        }, binding.page_ids, binding.planes, binding.history, self.attention_workspace.bind(local), self.get(.raw));
        try self.state.convolveResidual(pending, text.hidden_size, self.get(.raw), layer.weight(.attention_taps), binding.attention_residual, self.get(.convolved_attention));

        const boundary = op.weight_layer % text.n_layers_per_attn_res_block == 0;
        const completed = self.stack.depth + @intFromBool(boundary);
        const previous = if (boundary) null else self.stack.current();
        const gamma = layer.weight(.expert_norm);
        try self.stack.mixPartial(runtime, eps, completed, self.get(.convolved_attention), previous, layer.weight(.expert_query), if (looped) null else gamma, self.get(if (looped) .mix else .normalized));
        if (looped) try self.control(layer, op.weight_layer, .mlp, self.get(.mix), gamma, self.get(.normalized));
        try layer.experts.encode(runtime, text, 1, self.get(.normalized), self.get(.router), .{
            .selected = self.get(.selected),
            .weights = self.get(.route_weights),
            .gate_up = self.get(.gate_up),
            .activated = self.get(.activated),
            .projected = self.get(.projected),
        }, self.get(.raw));
        try self.state.convolveResidual(pending, text.hidden_size, self.get(.raw), layer.weight(.expert_taps), binding.expert_residual, self.get(.convolved_expert));
        try self.stack.finish(runtime, boundary, self.get(.convolved_attention), self.get(.convolved_expert));
    }

    fn run(self: *Request, token: u32, produce_logits: bool) !void {
        const runtime = self.model.runtime;
        var pending = try self.state.prepare(1);
        defer pending.abort();
        try metal.beginFrame(runtime);
        errdefer metal.cancelFrame(runtime) catch {};
        try self.model.endpoints.embed(runtime, token, self.stack.reset());
        for (self.model.schedule) |op| try self.visit(&pending, op);
        if (produce_logits) try self.model.endpoints.logits(runtime, &self.stack.backing, self.stack.current(), @intCast(self.stack.depth + 1), self.get(.normalized), self.get(.logits));
        try metal.submitFrame(runtime);
        try self.state.publish(&pending);
    }

    pub fn append(self: *Request, token: u32) !void {
        try self.run(token, false);
    }

    pub fn step(self: *Request, token: u32) ![]const f32 {
        try self.run(token, true);
        try self.get(.logits).downloadF32Into(self.host_logits);
        return self.host_logits;
    }
};

test "Inkling Metal resident decoder runs the released text checkpoint" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const dir = @import("antfly_platform").env.getenv("ANTFLY_INKLING2_METAL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    var source = try checkpoint.Checkpoint.open(a, dir);
    defer source.deinit();
    var decoder = try Decoder.load(a, runtime, &source);
    defer decoder.deinit();
    var request: Request = undefined;
    try request.init(a, &decoder, 8);
    defer request.deinit();
    try request.append(1);
    const logits = try request.step(2);
    try std.testing.expectEqual(@as(usize, decoder.text.unpadded_vocab_size), logits.len);
    for (logits) |score| try std.testing.expect(std.math.isFinite(score));
    try std.testing.expectEqual(@as(usize, 2), request.state.sequence.committed_tokens);
}
