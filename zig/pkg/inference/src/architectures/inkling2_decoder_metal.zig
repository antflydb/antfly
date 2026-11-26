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

    const Weight = checkpoint.ResidualWeight;

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
        for (std.enums.values(Weight), &result.weights) |weight_key, *destination| {
            if (try source.residualWeight(layer, at.local, weight_key)) |bytes|
                destination.* = try upload(a, runtime, if (weight_key.isF32()) .f32 else .i16, bytes);
        }
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

    pub fn residentBytes(self: *const Decoder) usize {
        var bytes: usize = 0;
        for (&self.endpoints.weights) |*value| bytes += value.allocatedDeviceBytes() orelse value.deviceByteLen();
        for (&self.controller_queries) |*value| bytes += value.allocatedDeviceBytes() orelse value.deviceByteLen();
        for (&self.layers) |*layer| {
            bytes += layer.attention.residentBytes() + layer.experts.residentBytes();
            for (&layer.weights) |*entry| if (entry.*) |*value| {
                bytes += value.allocatedDeviceBytes() orelse value.deviceByteLen();
            };
            for (&layer.controllers) |*entry| if (entry.*) |*controller| {
                for ([_]*const Tensor{ &controller.down, &controller.gates }) |value|
                    bytes += value.allocatedDeviceBytes() orelse value.deviceByteLen();
            };
        }
        return bytes;
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

fn tempDtype(kind: Temp) tensors.DType {
    return switch (kind) {
        .mix, .shared_mix, .message, .gates, .router, .route_weights, .logits => .f32,
        .selected => .i32,
        else => .i16,
    };
}

pub const prefill_chunk_rows = 16;
const grouped_prefill_chunk_rows = 256;

fn groupedPrefillEnabled() bool {
    return @import("antfly_platform").env.getenvBoolDefault("ANTFLY_METAL_GROUPED_ROUTED_PREFILL", true);
}

pub fn preferredPrefillRows() u32 {
    return if (groupedPrefillEnabled()) grouped_prefill_chunk_rows else prefill_chunk_rows;
}

const WorkPlan = struct {
    rows: u32,
    reduction_rows: u32,
    elements: [@typeInfo(Temp).@"enum".field_names.len]usize,
    attention: attention.RequestWorkspace.Plan,
    group_elements: usize,

    fn init(model: *const Decoder, rows: u32, max_tokens: u32, reduction_rows: u32) !WorkPlan {
        const text = model.text;
        const width: usize = text.hidden_size;
        const count = @as(usize, rows) * width;
        const experts = model.layers[0].experts.geometry(text, rows);
        const shape = try experts.scratchShape();
        const elements = std.EnumArray(Temp, usize).init(.{
            .query = count,
            .key = count,
            .value = count,
            .raw = count,
            .convolved_attention = count,
            .convolved_expert = count,
            .normalized = count,
            .mix = count,
            .shared_mix = count,
            .message = count,
            .pair = count * 2,
            .rank = @as(usize, rows) * text.loop_config.gate_rank,
            .gates = count * 2,
            .router = @as(usize, rows) * text.n_routed_experts,
            .selected = @intCast(shape.selected),
            .route_weights = @intCast(shape.weights),
            .gate_up = @intCast(shape.gate_up),
            .activated = @intCast(shape.activated),
            .projected = @intCast(shape.projected),
            .logits = if (rows == 1) text.unpadded_vocab_size else 0,
        });
        return .{
            .rows = rows,
            .reduction_rows = reduction_rows,
            .elements = elements.values,
            .attention = try .init(text, rows, max_tokens, reduction_rows),
            .group_elements = if (rows > 1 and groupedPrefillEnabled())
                try experts.groupElements()
            else
                0,
        };
    }

    fn deviceBytes(self: WorkPlan, width: u32) !usize {
        var bytes = try Stack.deviceBytes(width, self.rows);
        bytes = try std.math.add(usize, bytes, self.attention.bytes);
        bytes = try std.math.add(usize, bytes, @as(usize, @intCast(self.attention.reduction_elements)) * @sizeOf(f32));
        for (std.enums.values(Temp), self.elements) |kind, count|
            bytes = try std.math.add(usize, bytes, try std.math.mul(usize, count, tempDtype(kind).byteSize()));
        return std.math.add(usize, bytes, try std.math.mul(usize, self.group_elements, @sizeOf(u32)));
    }
};

/// Exact requested device payloads. Metal allocation alignment and opaque
/// command storage are accounted for separately by the allocator.
pub const RequestPlan = struct {
    width: u32,
    bank: kv.BankConfig,
    decode: WorkPlan,
    prefill: ?WorkPlan,
    prefill_tail: ?WorkPlan = null,

    pub fn init(model: *const Decoder, max_tokens: u32) !RequestPlan {
        return initPrefill(model, max_tokens, 1);
    }

    pub fn initPrefill(model: *const Decoder, max_tokens: u32, prefill_rows: u32) !RequestPlan {
        return initWithFormat(model, max_tokens, prefill_rows, .bf16);
    }

    pub fn initWithFormat(model: *const Decoder, max_tokens: u32, prefill_rows: u32, format: kv.PlaneFormat) !RequestPlan {
        return initWithReductionRows(model, max_tokens, prefill_rows, prefill_rows, format);
    }

    pub fn initWithReductionRows(model: *const Decoder, max_tokens: u32, prefill_rows: u32, reduction_rows: u32, format: kv.PlaneFormat) !RequestPlan {
        const text = model.text;
        if (max_tokens == 0 or max_tokens > text.model_max_length or prefill_rows == 0 or
            reduction_rows == 0 or reduction_rows > prefill_rows or
            prefill_rows > @min(grouped_prefill_chunk_rows, max_tokens)) return error.InvalidInkling2Context;
        return .{
            .width = text.hidden_size,
            .bank = .{
                .layout = .{ .page_tokens = 16, .window_tokens = text.sliding_window_size, .max_tokens = max_tokens, .max_append_tokens = prefill_rows },
                .full_slots = checkpoint.full_slot_count,
                .sliding_slots = checkpoint.sliding_slot_count,
                .kv_heads = text.num_key_value_heads,
                .head_dim = text.head_dim,
                .full_format = format,
            },
            .decode = try .init(model, 1, max_tokens, 1),
            .prefill = if (prefill_rows == 1) null else try .init(model, prefill_rows, max_tokens, reduction_rows),
        };
    }

    pub fn withPrefillTail(self: RequestPlan, model: *const Decoder, tail_rows: u32) !RequestPlan {
        if (tail_rows < 2) return self;
        const main = self.prefill orelse return error.InvalidShape;
        if (tail_rows >= main.rows) return error.InvalidShape;
        var result = self;
        result.prefill_tail = try .init(model, tail_rows, @intCast(self.bank.layout.max_tokens), @min(tail_rows, main.reduction_rows));
        return result;
    }

    pub fn deviceBytes(self: RequestPlan) !usize {
        const bytes = try std.math.add(usize, try State.deviceBytes(self.bank, self.width), try self.decode.deviceBytes(self.width));
        const main_bytes = if (self.prefill) |plan| try plan.deviceBytes(self.width) else 0;
        const tail_bytes = if (self.prefill_tail) |plan| try plan.deviceBytes(self.width) else 0;
        return std.math.add(usize, bytes, @max(main_bytes, tail_bytes));
    }
};

const Work = struct {
    stack: Stack,
    attention: attention.RequestWorkspace,
    groups: ?Tensor,
    temps: [@typeInfo(Temp).@"enum".field_names.len]Tensor,

    fn init(a: std.mem.Allocator, runtime: *Runtime, width: u32, plan: WorkPlan) !Work {
        var result: Work = undefined;
        result.stack = try Stack.init(a, runtime, width, plan.rows);
        errdefer result.stack.deinit();
        result.attention = try .init(a, runtime, plan.attention);
        errdefer result.attention.deinit();
        var initialized: usize = 0;
        errdefer for (result.temps[0..initialized]) |*tensor| tensor.deinit();
        for (&result.temps, std.enums.values(Temp), plan.elements) |*tensor, kind, elements| {
            if (elements == 0) break;
            tensor.* = try allocate(a, runtime, tempDtype(kind), elements, if (kind == .logits) .shared else .private);
            initialized += 1;
        }
        result.groups = if (plan.group_elements > 0) try allocate(a, runtime, .i32, plan.group_elements, .private) else null;
        return result;
    }

    fn deinit(self: *Work) void {
        if (self.groups) |*groups| groups.deinit();
        const count = if (self.stack.rows == 1) self.temps.len else @backingInt(Temp.logits);
        for (self.temps[0..count]) |*tensor| tensor.deinit();
        self.attention.deinit();
        self.stack.deinit();
    }

    fn get(self: *Work, key: Temp) *Tensor {
        return &self.temps[@backingInt(key)];
    }
};

pub const PrefixPart = union(enum) {
    tokens: []const i32,
    embeddings: struct { tensor: *const Tensor, start: usize, count: usize },

    pub fn rows(self: PrefixPart) usize {
        return switch (self) {
            .tokens => |tokens| tokens.len,
            .embeddings => |values| values.count,
        };
    }

    pub fn slice(self: PrefixPart, start: usize, count: usize) PrefixPart {
        std.debug.assert(start <= self.rows() and count <= self.rows() - start);
        return switch (self) {
            .tokens => |tokens| .{ .tokens = tokens[start..][0..count] },
            .embeddings => |values| .{ .embeddings = .{
                .tensor = values.tensor,
                .start = values.start + start,
                .count = count,
            } },
        };
    }
};

pub const Request = struct {
    model: *const Decoder,
    state: State,
    decode: Work,
    prefill: ?Work,
    prefill_tail: ?WorkPlan,
    host_logits: []f32,
    allocator: std.mem.Allocator,

    /// Initialize the request in its final storage so that KV's metadata pools
    /// retain stable addresses across submissions.
    pub fn init(self: *Request, a: std.mem.Allocator, model: *const Decoder, max_tokens: u32) !void {
        try self.initWithPlan(a, model, try RequestPlan.init(model, max_tokens));
    }

    pub fn initWithPlan(self: *Request, a: std.mem.Allocator, model: *const Decoder, plan: RequestPlan) !void {
        const runtime = model.runtime;
        self.* = .{ .model = model, .state = undefined, .decode = undefined, .prefill = null, .prefill_tail = plan.prefill_tail, .host_logits = undefined, .allocator = a };
        self.host_logits = try a.alloc(f32, model.text.unpadded_vocab_size);
        errdefer a.free(self.host_logits);
        self.decode = try .init(a, runtime, plan.width, plan.decode);
        errdefer self.decode.deinit();
        if (plan.prefill) |batch| self.prefill = try .init(a, runtime, plan.width, batch);
        errdefer if (self.prefill) |*batch| batch.deinit();
        try self.state.init(a, runtime, plan.bank, plan.width);
    }

    pub fn deinit(self: *Request) void {
        self.state.deinit();
        if (self.prefill) |*batch| batch.deinit();
        self.decode.deinit();
        self.allocator.free(self.host_logits);
    }

    fn control(self: *Request, work: *Work, layer: *const Layer, layer_index: usize, stream: checkpoint.ControllerStream, dest: *const Tensor, gamma: *const Tensor, output: *Tensor) !void {
        const runtime = self.model.runtime;
        const eps = self.model.text.rms_norm_eps;
        var query = try self.model.query(layer_index, stream);
        defer query.deinit();
        try work.stack.mixBank(runtime, eps, &query, work.get(.message));
        const ctrl = if (layer.controllers[@backingInt(stream)]) |*value| value else return error.InvalidInkling2Schedule;
        try ctrl.encode(runtime, work.stack.rows, eps, dest, work.get(.message), gamma, .{
            .pair = work.get(.pair),
            .rank = work.get(.rank),
            .gates = work.get(.gates),
        }, output);
    }

    fn visit(self: *Request, work: *Work, pending: *kv.Append, op: checkpoint.Visit) !void {
        const text = self.model.text;
        const runtime = self.model.runtime;
        const eps = text.rms_norm_eps;
        const layer = &self.model.layers[op.weight_layer];
        const local = layer.attention.local;
        const looped = op.pass != 0;
        try work.stack.advance(op, text.loop_config.core_start);

        const destinations = [_]*Tensor{ work.get(.query), work.get(.key), work.get(.value) };
        for ([_]checkpoint.ControllerStream{ .attn, .k, .v }, [_]Layer.Weight{ .q_norm, .k_norm, .v_norm }, destinations, 0..) |stream, norm, dest, index| {
            if (local and !looped and index != 0) break;
            const gamma = layer.weight(if (local) .q_norm else norm);
            if (op.weight_layer == 0) {
                try metal.rowRmsBf16(runtime, work.stack.rows, text.hidden_size, eps, work.stack.current(), gamma, dest);
                continue;
            }
            if (!looped or !local or index == 0) {
                var query = try row(layer.weight(.attn_query), index, text.hidden_size);
                defer query.deinit();
                if (looped) {
                    try work.stack.mix(runtime, eps, &query, null, work.get(.shared_mix));
                } else try work.stack.mix(runtime, eps, &query, gamma, dest);
            }
            if (looped) try self.control(work, layer, op.weight_layer, stream, work.get(.shared_mix), gamma, dest);
        }
        const binding = try self.state.binding(pending, .{ .group = op.group, .slot = op.slot });
        try layer.attention.encode(runtime, text, pending, .{
            .query = destinations[0],
            .key = if (local and !looped) destinations[0] else destinations[1],
            .value = if (local and !looped) destinations[0] else destinations[2],
        }, binding.page_ids, binding.planes, binding.history, work.attention.bind(local), work.get(.raw));
        try self.state.convolveResidual(pending, text.hidden_size, work.get(.raw), layer.weight(.attn_taps), binding.attention_residual, work.get(.convolved_attention));

        const boundary = op.weight_layer % text.n_layers_per_attn_res_block == 0;
        const completed = work.stack.depth + @intFromBool(boundary);
        const previous = if (boundary) null else work.stack.current();
        const gamma = layer.weight(.mlp_norm);
        try work.stack.mixPartial(runtime, eps, completed, work.get(.convolved_attention), previous, layer.weight(.mlp_query), if (looped) null else gamma, work.get(if (looped) .mix else .normalized));
        if (looped) try self.control(work, layer, op.weight_layer, .mlp, work.get(.mix), gamma, work.get(.normalized));
        try layer.experts.encode(runtime, text, work.stack.rows, work.get(.normalized), work.get(.router), .{
            .selected = work.get(.selected),
            .weights = work.get(.route_weights),
            .gate_up = work.get(.gate_up),
            .activated = work.get(.activated),
            .projected = work.get(.projected),
            .groups = if (work.groups) |*groups| groups else null,
        }, work.get(.raw));
        try self.state.convolveResidual(pending, text.hidden_size, work.get(.raw), layer.weight(.mlp_taps), binding.expert_residual, work.get(.convolved_expert));
        try work.stack.finish(runtime, boundary, work.get(.convolved_attention), work.get(.convolved_expert));
    }

    const Input = union(enum) {
        token: u32,
        prefix: []const PrefixPart,
    };

    const Output = enum { none, logits, greedy };

    fn run(self: *Request, work: *Work, input: Input, output: Output) !void {
        const runtime = self.model.runtime;
        var pending = try self.state.prepare(work.stack.rows);
        defer pending.abort();
        try metal.beginFrame(runtime);
        errdefer metal.cancelFrame(runtime) catch {};
        const hidden = work.stack.reset();
        switch (input) {
            .token => |token| try self.model.endpoints.embed(runtime, token, hidden),
            .prefix => |parts| try self.encodePrefix(parts, hidden),
        }
        for (self.model.schedule) |op| try self.visit(work, &pending, op);
        if (output != .none) {
            const logits = work.get(.logits);
            try self.model.endpoints.logits(runtime, &work.stack.backing, work.stack.current(), @intCast(work.stack.depth + 1), work.get(.normalized), logits);
            if (output == .greedy and metal.termite_metal_decode_runtime_encode_argmax_from_logits_device(
                runtime,
                logits.deviceHandle(),
                logits.deviceByteOffset(),
                self.model.text.unpadded_vocab_size,
            ) != 0) return error.MetalKernelDispatchFailed;
        }
        try metal.submitFrame(runtime);
        try self.state.publish(&pending);
    }

    pub fn append(self: *Request, token: u32) !void {
        try self.run(&self.decode, .{ .token = token }, .none);
    }

    fn encodePrefix(self: *Request, parts: []const PrefixPart, hidden: *Tensor) !void {
        const width = self.model.text.hidden_size;
        const row_bytes = @as(usize, width) * @sizeOf(u16);
        var offset: usize = 0;
        for (parts) |part| {
            switch (part) {
                .tokens => |tokens| {
                    if (tokens.len > 0) {
                        const bytes = tokens.len * row_bytes;
                        const elements: i32 = @intCast(bytes / @sizeOf(u16));
                        var destination = try hidden.retainedView(offset * row_bytes, bytes, &.{elements});
                        defer destination.deinit();
                        try self.model.endpoints.embedRows(self.model.runtime, tokens, &destination);
                    }
                },
                .embeddings => |values| {
                    const source = values.tensor;
                    if (!source.isDevice() or source.dtype != .i16 or
                        values.start > source.deviceByteLen() / row_bytes or
                        values.count > source.deviceByteLen() / row_bytes - values.start)
                        return error.InvalidShape;
                    if (values.count > 0) {
                        const bytes = values.count * row_bytes;
                        const elements: i32 = @intCast(bytes / @sizeOf(u16));
                        var image_rows = try source.retainedView(values.start * row_bytes, bytes, &.{elements});
                        defer image_rows.deinit();
                        var destination = try hidden.retainedView(offset * row_bytes, bytes, &.{elements});
                        defer destination.deinit();
                        try image_rows.copyInto(&destination);
                    }
                },
            }
            offset += part.rows();
        }
    }

    pub fn appendPrefix(self: *Request, parts: []const PrefixPart) !void {
        var rows: usize = 0;
        for (parts) |part| rows = try std.math.add(usize, rows, part.rows());
        if (rows == 1) return self.run(&self.decode, .{ .prefix = parts }, .none);
        const current = if (self.prefill) |*batch| batch else return error.InvalidShape;
        if (rows != current.stack.rows) {
            const tail = self.prefill_tail orelse return error.InvalidShape;
            if (rows != tail.rows) return error.InvalidShape;
            // Completed frames have released their bindings. Replace the
            // main workspace before allocating its shorter final shape.
            current.deinit();
            self.prefill = null;
            self.prefill = try .init(self.allocator, self.model.runtime, self.model.text.hidden_size, tail);
            self.prefill_tail = null;
        }
        try self.run(&self.prefill.?, .{ .prefix = parts }, .none);
    }

    pub fn appendTokens(self: *Request, tokens: []const i32) !void {
        return self.appendPrefix(&.{.{ .tokens = tokens }});
    }

    pub fn appendEmbeddings(self: *Request, image_rows: *const Tensor, rows: u32) !void {
        if (image_rows.deviceByteLen() != @as(usize, rows) * self.model.text.hidden_size * @sizeOf(u16))
            return error.InvalidShape;
        return self.appendPrefix(&.{.{ .embeddings = .{ .tensor = image_rows, .start = 0, .count = rows } }});
    }

    pub fn step(self: *Request, token: u32) ![]const f32 {
        try self.run(&self.decode, .{ .token = token }, .logits);
        try self.decode.get(.logits).downloadF32Into(self.host_logits);
        return self.host_logits;
    }

    pub fn stepGreedy(self: *Request, token: u32) !u32 {
        try self.run(&self.decode, .{ .token = token }, .greedy);
        var next: u32 = 0;
        if (metal.termite_metal_decode_runtime_read_token_id(self.model.runtime, &next) != 0)
            return error.MetalKernelDispatchFailed;
        return next;
    }
};

test "Inkling Metal resident decoder runs the released text checkpoint" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const dir = @import("antfly_platform").env.getenv("ANTFLY_INKLING2_METAL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const platform = @import("antfly_platform");
    const time = platform.time;
    const load_start = time.monotonicNs();
    const MetalSession = @import("inkling2_session_metal.zig").Runtime;
    const session = load: {
        var source = try checkpoint.Checkpoint.open(a, dir);
        defer source.deinit();
        break :load try MetalSession.loadCheckpoint(a, &source);
    };
    defer session.close();
    const resident = MetalSession.fromSession(session) orelse return error.Inkling2FixtureRequired;
    const decoder = &resident.model;
    try std.testing.expect(resident.resident_bytes > 0);
    std.debug.print("Inkling Metal resident text model loaded in {d:.3}s, {d} visits, {d} resident bytes\n", .{
        @as(f64, @floatFromInt(time.monotonicNs() - load_start)) / 1e9, decoder.schedule.len, resident.resident_bytes,
    });
    var request: Request = undefined;
    try request.init(a, decoder, 8);
    defer request.deinit();
    // Start from the same token as the released CUDA decoder test.
    var token: u32 = 42;
    for (1..5) |position| {
        const step_start = time.monotonicNs();
        const logits = try request.step(token);
        try std.testing.expectEqual(@as(usize, decoder.text.unpadded_vocab_size), logits.len);
        var next: usize = 0;
        for (logits, 0..) |score, index| {
            try std.testing.expect(std.math.isFinite(score));
            if (score > logits[next]) next = index;
        }
        try std.testing.expect(logits[next] > 0);
        try std.testing.expectEqual(position, request.state.sequence.committed_tokens);
        std.debug.print("Inkling Metal text position={d} token={d} next={d} peak={d:.5} latency_ms={d:.3} gpu_ms={d:.3}\n", .{
            position,                                                                token, next, logits[next], @as(f64, @floatFromInt(time.monotonicNs() - step_start)) / 1e6,
            @as(f64, @floatFromInt(metal.lastFrameGpuNanos(decoder.runtime))) / 1e6,
        });
        token = @intCast(next);
    }

    const prefix = [_]i32{ 42, 34851, 220, 110104, 42, 34851, 220, 110104, 42, 34851, 220, 110104, 42, 34851, 220, 110104 };
    const last: u32 = 1541;
    var serial: Request = undefined;
    try serial.init(a, decoder, prefix.len + 1);
    defer serial.deinit();
    const serial_start = time.monotonicNs();
    for (prefix) |id| try serial.append(@intCast(id));
    const serial_ns = time.monotonicNs() - serial_start;
    const reference = try serial.step(last);

    var batched: Request = undefined;
    try batched.initWithPlan(a, decoder, try RequestPlan.initPrefill(decoder, prefix.len + 1, prefix.len));
    defer batched.deinit();
    const batch_start = time.monotonicNs();
    try batched.appendTokens(&prefix);
    const batch_ns = time.monotonicNs() - batch_start;
    if (batched.prefill.?.groups) |*table| {
        const words = try a.alloc(u32, table.deviceByteLen() / @sizeOf(u32));
        defer a.free(words);
        try table.downloadBytesInto(std.mem.sliceAsBytes(words));
        var grouped_rows: usize = 0;
        var offset: usize = 0;
        while (offset < words.len) : (offset += 9) {
            if (words[offset] >= 4 and words[offset] <= 8) grouped_rows += words[offset];
        }
        std.debug.print("Inkling Metal grouped expert rows {d}/{d} in the last prefill visit\n", .{ grouped_rows, words.len / 9 });
    }
    const candidate = try batched.step(last);
    try std.testing.expectEqual(prefix.len + 1, batched.state.sequence.committed_tokens);
    const argmax = @import("../backends/activations.zig").argmax;
    try std.testing.expectEqual(argmax(reference), argmax(candidate));
    var max_delta: f32 = 0;
    for (reference, candidate) |expected, actual| {
        try std.testing.expect(std.math.isFinite(actual));
        max_delta = @max(max_delta, @abs(expected - actual));
    }
    try std.testing.expectEqualSlices(f32, reference, candidate);
    std.debug.print("Inkling Metal prefill rows={d} serial_ms={d:.3} batched_ms={d:.3} peak_id={d} max_logit_delta={d:.5}\n", .{
        prefix.len, @as(f64, @floatFromInt(serial_ns)) / 1e6, @as(f64, @floatFromInt(batch_ns)) / 1e6, argmax(candidate), max_delta,
    });

    if (groupedPrefillEnabled()) {
        const reference_rows = 64;
        const tail_rows = 17;
        var tokens: [grouped_prefill_chunk_rows + tail_rows]i32 = undefined;
        for (&tokens, 0..) |*id, i| id.* = prefix[i % prefix.len];
        var baseline: Request = undefined;
        try baseline.initWithPlan(a, decoder, try RequestPlan.initPrefill(decoder, tokens.len + 1, reference_rows));
        defer baseline.deinit();
        const base_start = time.monotonicNs();
        var offset: usize = 0;
        while (offset + reference_rows <= tokens.len) : (offset += reference_rows)
            try baseline.appendTokens(tokens[offset..][0..reference_rows]);
        for (tokens[offset..]) |id| try baseline.append(@intCast(id));
        const baseline_ns = time.monotonicNs() - base_start;
        const baseline_logits = try baseline.step(last);

        var large: Request = undefined;
        const wide = try RequestPlan.initPrefill(decoder, tokens.len + 1, grouped_prefill_chunk_rows);
        const tiled = try RequestPlan.initWithReductionRows(decoder, tokens.len + 1, grouped_prefill_chunk_rows, 16, .bf16);
        try std.testing.expect(try tiled.deviceBytes() < try wide.deviceBytes());
        const wide_tail = try tiled.withPrefillTail(decoder, tail_rows);
        try std.testing.expectEqual(try tiled.deviceBytes(), try wide_tail.deviceBytes());
        try large.initWithPlan(a, decoder, wide_tail);
        defer large.deinit();
        const started = time.monotonicNs();
        try large.appendTokens(tokens[0..grouped_prefill_chunk_rows]);
        try large.appendTokens(tokens[grouped_prefill_chunk_rows..]);
        const elapsed = time.monotonicNs() - started;
        const grouped = try large.step(last);
        try std.testing.expectEqual(argmax(baseline_logits), argmax(grouped));
        try std.testing.expectEqualSlices(f32, baseline_logits, grouped);
        try std.testing.expectEqual(tokens.len + 1, large.state.sequence.committed_tokens);
        std.debug.print("Inkling Metal grouped prefill rows={d} default_ms={d:.3} grouped_ms={d:.3} next={d}\n", .{
            tokens.len, @as(f64, @floatFromInt(baseline_ns)) / 1e6, @as(f64, @floatFromInt(elapsed)) / 1e6, argmax(grouped),
        });
    }

    const full_context = decoder.text.model_max_length;
    const full_plan = try RequestPlan.initWithFormat(decoder, full_context, preferredPrefillRows(), .q4_head128);
    const full_exact = try RequestPlan.initPrefill(decoder, full_context, preferredPrefillRows());
    try std.testing.expectEqual(@as(usize, 8_858_370_048), try full_plan.bank.deviceBytes(.full));
    try std.testing.expectEqual(@as(usize, 34_359_738_368), try full_exact.bank.deviceBytes(.full));
    try std.testing.expect(resident.resident_bytes + try full_plan.deviceBytes() < 29_000_000_000);
    const retained_at_full = try State.deviceBytes(full_plan.bank, full_plan.width);
    const default_kv_budget = 3 * 1024 * 1024 * 1024;
    const widened = resident.widenCacheBudget(.{ .kv_limit_bytes = default_kv_budget, .combined_limit_bytes = 29_000_000_000 });
    try std.testing.expectEqual(retained_at_full, widened.kv_limit_bytes);
    const capped = resident.widenCacheBudget(.{ .kv_limit_bytes = default_kv_budget, .combined_limit_bytes = 25_000_000_000 });
    try std.testing.expectEqual(@as(usize, default_kv_budget), capped.kv_limit_bytes);
    const thin_rows = @max(1, preferredPrefillRows() / 4);
    const thin = try RequestPlan.initWithFormat(decoder, full_context, thin_rows, .q4_head128);
    const thin_bytes = resident.resident_bytes + try thin.deviceBytes() + @as(usize, decoder.text.unpadded_vocab_size) * @sizeOf(f32);
    const widened_under_tight_limit = resident.widenCacheBudget(.{ .kv_limit_bytes = default_kv_budget, .combined_limit_bytes = thin_bytes });
    try std.testing.expect(widened_under_tight_limit.kv_limit_bytes >= try State.deviceBytes(thin.bank, thin.width));
    try std.testing.expect(widened_under_tight_limit.kv_limit_bytes <= retained_at_full);
    if (platform.env.getenvBool("ANTFLY_INKLING2_METAL_FULL_LAYOUT")) {
        const allocation_start = time.monotonicNs();
        var maximum: Request = undefined;
        try maximum.initWithPlan(a, decoder, full_plan);
        defer maximum.deinit();
        const allocation_ns = time.monotonicNs() - allocation_start;
        try std.testing.expectEqual(@as(u32, 34851), try maximum.stepGreedy(42));
        std.debug.print("Inkling Metal allocated {d} positions with {d} KV bytes in {d:.3}ms and completed the first decode\n", .{
            full_context, retained_at_full, @as(f64, @floatFromInt(allocation_ns)) / 1e6,
        });
    }
    const compressed_plan = try RequestPlan.initWithFormat(decoder, prefix.len + 1, prefix.len, .q4_head128);
    var compressed: Request = undefined;
    try compressed.initWithPlan(a, decoder, compressed_plan);
    defer compressed.deinit();
    const compressed_start = time.monotonicNs();
    try compressed.appendTokens(&prefix);
    const compressed_ns = time.monotonicNs() - compressed_start;
    const compressed_logits = try compressed.step(last);
    try std.testing.expectEqual(argmax(reference), argmax(compressed_logits));
    var compact_error: f32 = 0;
    for (reference, compressed_logits) |expected, actual| {
        try std.testing.expect(std.math.isFinite(actual));
        compact_error = @max(compact_error, @abs(expected - actual));
    }
    std.debug.print("Inkling Metal Q4 prefill_ms={d:.3} peak_id={d} max_logit_delta={d:.5} full_kv_bytes={d} exact_kv_bytes={d}\n", .{
        @as(f64, @floatFromInt(compressed_ns)) / 1e6,           argmax(compressed_logits),                                compact_error,
        try State.deviceBytes(full_plan.bank, full_plan.width), try State.deviceBytes(full_exact.bank, full_exact.width),
    });

    // The same patterned RGB fixture runs in the released CUDA decoder suite.
    const image_width = 57;
    const image_height = 62;
    var pixels: [image_width * image_height * 3]u8 = undefined;
    for (&pixels, 0..) |*value, i| value.* = @truncate(i * 17);
    const vision_start = time.monotonicNs();
    var image = try resident.vision.encodeRgb(a, decoder.runtime, &pixels, image_width, image_height);
    defer image.tensor.deinit();
    const vision_ns = time.monotonicNs() - vision_start;
    try std.testing.expectEqual(@as(u32, 4), image.rows);
    var multimodal: Request = undefined;
    try multimodal.initWithPlan(a, decoder, try RequestPlan.initPrefill(decoder, image.rows + 1, image.rows));
    defer multimodal.deinit();
    try multimodal.appendEmbeddings(&image.tensor, image.rows);
    const image_logits = try multimodal.step(42);
    const image_next = argmax(image_logits);
    try std.testing.expectEqual(@as(usize, 5), multimodal.state.sequence.committed_tokens);
    try std.testing.expectEqual(@as(usize, 12), image_next);
    try std.testing.expectApproxEqAbs(@as(f32, 19.87989), image_logits[image_next], 0.25);
    std.debug.print("Inkling Metal image={d}x{d} projected_rows={d} next={d} peak={d:.5} vision_ms={d:.3}\n", .{
        image_width, image_height, image.rows, image_next, image_logits[image_next], @as(f64, @floatFromInt(vision_ns)) / 1e6,
    });

    const model_dir = try std.Io.Dir.openDirAbsolute(std.testing.io, dir, .{});
    defer model_dir.close(std.testing.io);
    var tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromDir(a, model_dir, std.testing.io, "tokenizer.json");
    defer tokenizer.deinitSelf();
    const gen = @import("../pipelines/generation.zig");
    const config: gen.GenerationConfig = .{ .max_tokens = 4, .enable_thinking = false };
    const image_mod = @import("antfly_image");
    const dimensions = 80;
    var rgba: [dimensions * dimensions * 4]u8 = undefined;
    for (0..dimensions * dimensions) |index| {
        rgba[index * 4 ..][0..4].* = .{ 255, 0, 0, 255 };
    }
    const png = try image_mod.png.encodeRgba(a, dimensions, dimensions, &rgba);
    defer a.free(png);
    const messages: []const gen.Message = &.{.{
        .role = "user",
        .content = "",
        .image_bytes = &.{png},
        .content_parts = &.{ .{ .text = "What color is this image? Answer with one word." }, .{ .image = 0 } },
    }};
    const Pipeline = @import("inkling2_session_metal.zig").TextPipeline;
    var pipeline = try Pipeline.init(a, tokenizer.tokenizer(), .{}, messages, config, null, .{});
    defer pipeline.deinit();
    var result = try resident.generate(session, &pipeline, config, .{});
    defer result.deinit();
    try std.testing.expect(std.ascii.eqlIgnoreCase("red", std.mem.trim(u8, result.text, " .\n\t")));
    std.debug.print("Inkling Metal image chat prompt_tokens={d} answer={s}\n", .{ result.prompt_tokens, result.text });
}

test "Inkling Metal resident decoder executes the full visit schedule on a synthetic checkpoint" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    var text: checkpoint.TextConfig = .{
        .model_type = "inkling2_loop_text",
        .model_max_length = 1048576,
        .hidden_size = 2304,
        .num_hidden_layers = 24,
        .vocab_size = 201024,
        .unpadded_vocab_size = 200058,
        .num_attention_heads = 24,
        .num_key_value_heads = 8,
        .head_dim = 128,
        .d_rel = 16,
        .rel_extent = 1024,
        .q_bias = false,
        .o_bias = false,
        .log_scaling_n_floor = 8192,
        .log_scaling_alpha = 1,
        .rms_norm_eps = 0.000001,
        .use_value_norm_no_gamma = true,
        .qk_l2_clip_threshold = 1,
        .rel_l2_clip_threshold = 1,
        .local_layer_ids = .{ 0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22 },
        .sconv_kernel_size = 4,
        .final_logit_softcapping = 1,
        .swa_head_dim = 128,
        .swa_num_attention_heads = 24,
        .swa_num_key_value_heads = 8,
        .sliding_window_size = 1024,
        .n_layers_per_attn_res_block = 6,
        .attn_res_normalize_values = true,
        .n_routed_experts = 128,
        .num_experts_per_tok = 8,
        .n_shared_experts = 0,
        .intermediate_size = 1152,
        .mlp_gate_softcap = 3,
        .mlp_up_softcap = 2,
        .route_scale = 1,
        .use_gate_bias = true,
        .loop_config = .{ .is_varloop = false, .core_start = 6, .core_end = 18, .num_passes = 3, .gate_rank = 64 },
    };
    const config: checkpoint.Config = .{
        .model_type = "inkling2_loop",
        .eos_token_id = 0,
        .text_config = text,
        .quantization_config = .{ .quant_method = "modelopt", .quant_algo = "NVFP4" },
    };
    const schedule = try config.visits();
    const width = 128;
    const intermediate = 32;
    text.hidden_size = width;
    text.num_attention_heads = 1;
    text.num_key_value_heads = 1;
    text.d_rel = 1;
    text.rel_extent = 1;
    text.n_routed_experts = 2;
    text.num_experts_per_tok = 1;
    text.intermediate_size = intermediate;
    text.loop_config.gate_rank = 1;
    text.vocab_size = 2;
    text.unpadded_vocab_size = 2;
    text.sliding_window_size = 2;

    const Fixture = struct {
        const zero: [width * width * 2]u8 = @splat(0);
        const norm: [width]u16 = @splat(0x3f80);

        pub fn layerTensor(_: @This(), _: usize, name: []const u8) ![]const u8 {
            const layout = @import("inkling2_attention_weights.zig");
            for ([_]layout.Weight{ .q, .k, .v, .gate, .out }) |key|
                if (std.mem.eql(u8, name, layout.tensorName(key, false))) return zero[0 .. width * width * 2];
            for ([_]layout.Weight{ .r, .sink, .q_scale }) |key|
                if (std.mem.eql(u8, name, layout.tensorName(key, false))) return zero[0 .. width * 2];
            if (std.mem.endsWith(u8, name, "sconv.weight_proj") or std.mem.endsWith(u8, name, "sconv.static_w") or
                std.mem.endsWith(u8, name, "sconv.weight")) return zero[0 .. 4 * width * 2];
            return zero[0..2];
        }

        fn load(runtime_arg: *Runtime, text_arg: checkpoint.TextConfig, index: usize) !Layer {
            var attn = try attention.Layer.loadTensors(std.testing.allocator, runtime_arg, @This(){}, text_arg, index);
            const bias = [2]f32{ 0, 0 };
            const scales: [2048]u8 = @splat(0x38);
            const outer = [2]f32{ 1, 1 };
            const divisor = [1]f32{1};
            const format: checkpoint.ExpertFormat = if (index < checkpoint.nvfp4_layers) .nvfp4 else .mxfp8;
            const gate: checkpoint.ExpertWeights = .{
                .format = format,
                .weights = &zero,
                .block_scales = &scales,
                .outer_amax = if (format == .nvfp4) std.mem.asBytes(&outer) else null,
                .outer_denominator = if (format == .nvfp4) std.mem.asBytes(&divisor) else null,
            };
            const experts = moe.Layer.fromWeights(std.testing.allocator, runtime_arg, .{
                .weight = zero[0 .. 2 * width * 2],
                .bias = std.mem.asBytes(&bias),
            }, gate, gate) catch |err| {
                attn.deinit();
                return err;
            };
            var result: Layer = .{ .attention = attn, .experts = experts };
            errdefer result.deinit();
            for (&result.weights, 0..) |*weight, slot| {
                weight.* = switch (slot) {
                    0...3 => try upload(std.testing.allocator, runtime_arg, .i16, std.mem.asBytes(&norm)),
                    4, 5 => try upload(std.testing.allocator, runtime_arg, .i16, zero[0 .. 4 * width * 2]),
                    6 => try upload(std.testing.allocator, runtime_arg, .f32, zero[0 .. 3 * width * 4]),
                    7 => try upload(std.testing.allocator, runtime_arg, .f32, zero[0 .. width * 4]),
                    else => unreachable,
                };
            }
            if (index >= 6 and index < 18) for (&result.controllers) |*controller| {
                controller.* = try Controller.fromWeights(std.testing.allocator, runtime_arg, width, 1, .{
                    .down = zero[0 .. width * 4],
                    .gain = zero[0 .. width * 2],
                    .forget = zero[0 .. width * 2],
                });
            };
            return result;
        }
    };

    var embeddings: [2 * width]u16 = @splat(0x3f80);
    @memset(embeddings[0..width], 0xbf80);
    var vocabulary: [2 * width]u16 = @splat(0);
    vocabulary[0] = 0x3f80;
    vocabulary[width] = 0xbf80;

    var model: Decoder = .{
        .runtime = runtime,
        .text = text,
        .schedule = schedule,
        .layers = undefined,
        .controller_queries = undefined,
        .endpoints = try Endpoints.fromBytes(a, runtime, width, 2, text.rms_norm_eps, 1, .{
            std.mem.asBytes(&embeddings),   std.mem.asBytes(&Fixture.norm), Fixture.zero[0 .. width * 4],
            std.mem.asBytes(&Fixture.norm), std.mem.asBytes(&vocabulary),
        }),
    };
    defer model.endpoints.deinit();
    var queries_loaded: usize = 0;
    defer for (model.controller_queries[0..queries_loaded]) |*weight| weight.deinit();
    for (&model.controller_queries) |*weight| {
        weight.* = try upload(a, runtime, .f32, Fixture.zero[0 .. 24 * width * 4]);
        queries_loaded += 1;
    }
    var loaded: usize = 0;
    defer for (model.layers[0..loaded]) |*layer| layer.deinit();
    for (&model.layers, 0..) |*layer, index| {
        layer.* = try Fixture.load(runtime, text, index);
        loaded += 1;
    }
    const weights_before = model.residentBytes();
    try std.testing.expect(weights_before > 0);
    const short_plan = try RequestPlan.init(&model, 3);
    const expanded_plan = try RequestPlan.init(&model, 17);
    const new_page_bytes = checkpoint.visit_count * 16 * width * 2 * @sizeOf(u16) + 2 * @sizeOf(u32);
    try std.testing.expectEqual(@as(usize, new_page_bytes), (try expanded_plan.deviceBytes()) - (try short_plan.deviceBytes()));
    var request: Request = undefined;
    try request.init(a, &model, 3);
    defer request.deinit();
    var image_row = try upload(a, runtime, .i16, std.mem.sliceAsBytes(embeddings[0..width]));
    defer image_row.deinit();
    try std.testing.expectError(error.InvalidShape, request.appendEmbeddings(&model.controller_queries[0], 1));
    try std.testing.expectEqual(@as(usize, 0), request.state.sequence.committed_tokens);
    try request.appendEmbeddings(&image_row, 1);
    try std.testing.expectError(error.InvalidInkling2Token, request.append(2));
    try std.testing.expectEqual(@as(usize, 1), request.state.sequence.committed_tokens);
    const logits = try request.step(1);
    try std.testing.expectApproxEqAbs(@as(f32, 0.76159416), logits[0], 0.00001);
    try std.testing.expectApproxEqAbs(@as(f32, -0.76159416), logits[1], 0.00001);
    try std.testing.expectEqual(@as(usize, 2), request.state.sequence.committed_tokens);
    var batched: Request = undefined;
    try batched.initWithPlan(a, &model, try RequestPlan.initPrefill(&model, 3, 2));
    defer batched.deinit();
    try std.testing.expectError(error.InvalidInkling2Token, batched.appendTokens(&.{ 0, -1 }));
    try std.testing.expectEqual(@as(usize, 0), batched.state.sequence.committed_tokens);
    try batched.appendTokens(&.{ 0, 1 });
    try std.testing.expectEqual(@as(usize, 2), batched.state.sequence.committed_tokens);
    const expected_after_prefix = try request.stepGreedy(0);
    try std.testing.expectEqual(expected_after_prefix, try batched.stepGreedy(0));
    const hf_tokenizer = @import("inference_hf_tokenizer");
    var hf = try hf_tokenizer.HfTokenizer.loadFromBytes(a,
        \\{"version":"1.0","added_tokens":[{"id":14,"content":"<|end_message|>"}],"model":{"type":"BPE","unk_token":"<unk>","vocab":{"x":0,"y":1,"<unk>":2,"<|end_message|>":14},"merges":[]}}
    );
    defer hf.deinitSelf();
    var pipeline = @import("inkling2_session_metal.zig").TextPipeline{
        .allocator = a,
        .tokenizer = hf.tokenizer(),
        .control = .{},
        .prompt = .{ .media = &.{}, .suffix = try a.dupe(i32, &.{1}), .initial_channel = null },
        .options = .{},
    };
    defer pipeline.deinit();
    var diagnostics = @import("../execution/diagnostics.zig").Diagnostics{};
    diagnostics.configure(true);
    var generated = try pipeline.generate(&model, 1, .{ .max_tokens = 2, .ignore_eos = true }, .{}, .{ .diagnostics = &diagnostics });
    defer generated.deinit();
    try std.testing.expectEqualSlices(i32, &.{ 0, 1 }, generated.token_ids.?);
    try std.testing.expectEqualStrings("length", generated.finish_reason);
    try std.testing.expectEqual(@as(usize, 1), generated.prompt_tokens);
    const measured = diagnostics.snapshot().?;
    try std.testing.expectEqual(.decode_step, measured.operation);
    try std.testing.expectEqual(.success, measured.outcome);
    try std.testing.expect(measured.runtime.submission.?.metal_encoded_dispatches.? > 0);
    try std.testing.expectEqual(.unavailable, measured.context_runtime_scope);
    try std.testing.expectEqual(weights_before, model.residentBytes());
}
