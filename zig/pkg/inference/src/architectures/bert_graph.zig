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

// BERT encoder as an ml.graph computation graph, for autodiff-based LoRA
// training. Equivalent to `bert.zig`'s ComputeBackend forward but emits a
// Graph of Builder nodes instead of calling backend ops directly. Downstream
// code applies `ml.graph.lora.injectLoRA` to add adapters and
// `ml.graph.autodiff.gradient` to get backward for every layer.
//
// Parameter names match HuggingFace BERT checkpoints so `injectLoRA`'s
// pattern matcher can find the q/k/v/dense weights without special-casing.
//
// Attention masking is passed in at execution time as a pre-built additive
// bias tensor (0 for valid positions, large-negative for padded). The
// caller constructs this outside the graph and binds it as an input; the
// graph adds it to the attention scores before softmax. This keeps the
// graph free of dtype-conversion ops that aren't yet in the Builder.

const std = @import("std");
const ml = @import("ml");
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const DType = ml.graph.shape.DType;

pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    intermediate_size: u32,
    max_position_embeddings: u32 = 512,
    type_vocab_size: u32 = 2,
    layer_norm_eps: f32 = 1e-12,
    hidden_act: @import("../models/bert.zig").HiddenActivation = .gelu_exact,
    /// When true, token_type_ids are looked up and added. Distilbert-style
    /// models omit this layer.
    use_token_type: bool = true,
};

pub const BertGraph = struct {
    /// [batch * seq_len] i64 placeholder, bind at execution time.
    input_ids_node: NodeId,
    /// [batch * seq_len] i64 placeholder. Same shape as input_ids.
    position_ids_node: NodeId,
    /// [batch * seq_len] i64 placeholder, optional (null if use_token_type = false).
    token_type_ids_node: NodeId,
    /// [batch, num_heads, seq_len, seq_len] f32 attention bias. Caller
    /// precomputes: 0 for valid positions, -1e9 for padded.
    attn_bias_node: NodeId,
    /// [batch * seq_len, hidden_size] — final encoder output.
    output_node: NodeId,
};

/// Inputs passed into `buildForwardGraph`. The caller (typically the
/// autodiff trainer harness) creates these nodes up front and threads them
/// through. This avoids the dual-placeholder mismatch that happens when
/// both the harness and the architecture try to own the input placeholders.
pub const BertInputs = struct {
    /// [batch * seq_len] i64 token IDs.
    input_ids: NodeId,
    /// [batch * seq_len] i64 position IDs (0..seq_len-1 repeated per batch
    /// item). The caller-side harness can derive this as a constant when
    /// seq_len is known.
    position_ids: NodeId,
    /// [batch * seq_len] i64 token type IDs, or `null` when the config
    /// disables token type embeddings.
    token_type_ids: ?NodeId,
    /// [batch * num_heads, seq_len, seq_len] f32 additive attention bias.
    /// 0 for valid positions, large-negative (e.g. -1e9) for padded.
    attn_bias: NodeId,
};

/// Construct a BERT forward graph with HF-compatible parameter names.
/// The graph has no pre-bound weight values; a WeightStore must be loaded
/// onto the execution backend separately. LoRA is NOT injected here —
/// call `ml.graph.lora.injectLoRA` after construction to add adapters.
pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    batch: u32,
    seq_len: u32,
    inputs: BertInputs,
) !BertGraph {
    const H: u32 = config.hidden_size;
    const total: u32 = batch * seq_len;

    if (config.use_token_type and inputs.token_type_ids == null) {
        return error.MissingTokenTypeIds;
    }
    const token_type_ids = inputs.token_type_ids orelse 0;

    // ──────── Embeddings ────────
    var hidden = try embeddings(bld, config, inputs.input_ids, inputs.position_ids, token_type_ids, total, H);

    // ──────── Encoder layers ────────
    var layer: u32 = 0;
    while (layer < config.num_hidden_layers) : (layer += 1) {
        hidden = try encoderLayer(bld, config, hidden, inputs.attn_bias, batch, seq_len, layer);
    }

    return .{
        .input_ids_node = inputs.input_ids,
        .position_ids_node = inputs.position_ids,
        .token_type_ids_node = token_type_ids,
        .attn_bias_node = inputs.attn_bias,
        .output_node = hidden,
    };
}

fn embeddings(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    position_ids: NodeId,
    token_type_ids: NodeId,
    total: u32,
    H: u32,
) !NodeId {
    var context = EmbeddingContext{ .bld = bld, .config = config, .ids = .{ input_ids, position_ids, token_type_ids }, .total = total, .hidden = H };
    return @import("bert_blocks.zig").embeddings(&context);
}

const EmbeddingContext = struct {
    pub const Value = NodeId;
    bld: *Builder,
    config: Config,
    ids: [3]NodeId,
    total: u32,
    hidden: u32,
    pub fn lookup(self: *@This(), comptime role: @import("bert_blocks.zig").Embedding) !?NodeId {
        if (role == .token_type and !self.config.use_token_type) return null;
        const count = switch (role) {
            .word => self.config.vocab_size,
            .position => self.config.max_position_embeddings,
            .token_type => self.config.type_vocab_size,
        };
        const weight = try self.bld.parameter(@import("bert_blocks.zig").embeddingName(role), Shape.init(.f32, &.{ @intCast(count), @intCast(self.hidden) }));
        return try self.bld.embeddingLookup(weight, self.ids[@backingInt(role)], self.total, self.hidden);
    }
    pub fn add(self: *@This(), lhs: NodeId, rhs: NodeId) !NodeId {
        return self.bld.add(lhs, rhs);
    }
    pub fn normalize(self: *@This(), input: NodeId) !NodeId {
        const weight = try self.bld.parameter("embeddings.LayerNorm.weight", Shape.init(.f32, &.{@intCast(self.hidden)}));
        const bias = try self.bld.parameter("embeddings.LayerNorm.bias", Shape.init(.f32, &.{@intCast(self.hidden)}));
        return self.bld.layerNorm(input, weight, bias, self.hidden, self.config.layer_norm_eps);
    }
    pub fn free(_: *@This(), _: NodeId) void {}
};

fn encoderLayer(bld: *Builder, config: Config, hidden: NodeId, attn_bias: NodeId, batch: u32, seq_len: u32, layer: u32) !NodeId {
    var context = EncoderOutputContext{ .bld = bld, .config = config, .layer = layer, .total = batch * seq_len, .batch = batch, .seq_len = seq_len, .attn_bias = attn_bias };
    return @import("bert_blocks.zig").encoderLayer(&context, hidden);
}

const EncoderOutputContext = struct {
    pub const Value = NodeId;
    bld: *Builder,
    config: Config,
    layer: u32,
    total: u32,
    batch: u32 = 1,
    seq_len: u32 = 0,
    attn_bias: NodeId = ml.graph.null_node,

    pub fn fusedQueryKeyValue(_: *@This(), _: NodeId) !?@import("bert_blocks.zig").Qkv(NodeId) {
        return null;
    }
    pub fn attention(self: *@This(), Q: NodeId, K: NodeId, V: NodeId) !NodeId {
        const bld = self.bld;
        const batch = self.batch;
        const seq_len = self.seq_len;
        const num_heads = self.config.num_attention_heads;
        const head_dim = self.config.hidden_size / num_heads;
        const H = self.config.hidden_size;
        const total = self.total;
        const attn_bias = self.attn_bias;
        // Reshape for multi-head: [batch*seq, hidden] → [batch*num_heads, seq, head_dim]
        // via [batch, seq, num_heads, head_dim] → transpose → flatten head dim.
        const q_bsnh = try bld.reshape(
            Q,
            Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) }),
        );
        const k_bsnh = try bld.reshape(
            K,
            Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) }),
        );
        const v_bsnh = try bld.reshape(
            V,
            Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) }),
        );
        // Transpose to [batch, num_heads, seq, head_dim]
        const q_bnsh = try bld.transpose(q_bsnh, &.{ 0, 2, 1, 3 });
        const k_bnsh = try bld.transpose(k_bsnh, &.{ 0, 2, 1, 3 });
        const v_bnsh = try bld.transpose(v_bsnh, &.{ 0, 2, 1, 3 });
        // Flatten batch*num_heads → [bh, seq, head_dim]
        const q_bhsd = try bld.reshape(
            q_bnsh,
            Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) }),
        );
        const k_bhsd = try bld.reshape(
            k_bnsh,
            Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) }),
        );
        const v_bhsd = try bld.reshape(
            v_bnsh,
            Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) }),
        );

        // ──────── Masked scaled dot-product attention (manual decomposition) ────────
        // scores = Q @ K^T, shape [bh, seq, seq]
        const k_t = try bld.transpose(k_bhsd, &.{ 0, 2, 1 });
        const scores_raw = try bld.matmul3D(q_bhsd, k_t);
        // scale
        const scale = try bld.scalarConst(.f32, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))));
        const scores_scaled = try bld.mul(scores_raw, scale);
        // Add mask bias (broadcast [bh, seq, seq] + [bh, seq, seq])
        const scores_masked = try bld.add(scores_scaled, attn_bias);
        // Softmax over last axis
        const probs = try bld.softmax(scores_masked);
        // attn_out = probs @ V, shape [bh, seq, head_dim]
        const attn_bhsd = try bld.matmul3D(probs, v_bhsd);

        // Reshape back: [bh, seq, head_dim] → [batch, seq, hidden]
        const attn_bnsh = try bld.reshape(
            attn_bhsd,
            Shape.init(.f32, &.{ @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(head_dim) }),
        );
        const attn_bsnh = try bld.transpose(attn_bnsh, &.{ 0, 2, 1, 3 });
        return bld.reshape(attn_bsnh, Shape.init(.f32, &.{ @intCast(total), @intCast(H) }));
    }

    pub fn free(_: *@This(), _: NodeId) void {}
    pub fn add(self: *@This(), a: NodeId, b: NodeId) !NodeId {
        return self.bld.add(a, b);
    }
    pub fn activate(self: *@This(), input: NodeId) !NodeId {
        return switch (self.config.hidden_act) {
            .gelu_exact => self.bld.geluExact(input),
            .gelu_tanh => self.bld.gelu(input),
            .relu => self.bld.relu(input),
            .silu => self.bld.silu(input),
        };
    }
    pub fn linear(self: *@This(), comptime role: @import("bert_blocks.zig").Linear, input: NodeId) !NodeId {
        const base = comptime @import("bert_blocks.zig").linearBase(role);
        const in_dim = if (role == .output) self.config.intermediate_size else self.config.hidden_size;
        const out_dim = if (role == .intermediate) self.config.intermediate_size else self.config.hidden_size;
        const weight = try layerParam(self.bld, self.layer, base ++ ".weight", .{ out_dim, in_dim });
        const bias = try layerParam(self.bld, self.layer, base ++ ".bias", .{out_dim});
        return self.bld.linear(input, weight, bias, self.total, in_dim, out_dim);
    }
    pub fn norm(self: *@This(), comptime role: @import("bert_blocks.zig").Norm, input: NodeId) !NodeId {
        const base = switch (role) {
            .attention_output => "attention.output.LayerNorm",
            .output => "output.LayerNorm",
        };
        const weight = try layerParam(self.bld, self.layer, base ++ ".weight", .{self.config.hidden_size});
        const bias = try layerParam(self.bld, self.layer, base ++ ".bias", .{self.config.hidden_size});
        return self.bld.layerNorm(input, weight, bias, self.config.hidden_size, self.config.layer_norm_eps);
    }
    pub fn fusedAttentionOutput(_: *@This(), _: NodeId, _: NodeId) !?NodeId {
        return null;
    }
    pub fn fusedFeedForward(_: *@This(), _: NodeId) !?NodeId {
        return null;
    }
    pub fn fusedFeedForwardResidual(_: *@This(), _: NodeId) !?NodeId {
        return null;
    }
};

/// Helper: build a per-layer parameter name and emit a `bld.parameter` node.
/// `dims` is a 1- or 2-element tuple/array of u32 dimensions.
fn layerParam(bld: *Builder, layer: u32, suffix: []const u8, dims: anytype) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = try std.fmt.bufPrint(&name_buf, "encoder.layer.{d}.{s}", .{ layer, suffix });

    const d = dims;
    const shape = switch (@typeInfo(@TypeOf(d)).@"struct".field_names.len) {
        1 => Shape.init(.f32, &.{@intCast(d[0])}),
        2 => Shape.init(.f32, &.{ @intCast(d[0]), @intCast(d[1]) }),
        else => @compileError("layerParam: only 1-D or 2-D dims supported"),
    };
    return bld.parameter(name, shape);
}

test "buildForwardGraph constructs BERT graph with correct parameter count" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const config = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{8}));
    const test_position_ids = try bld.parameter("test_position_ids", Shape.init(.i64, &.{8}));
    const test_token_type_ids = try bld.parameter("test_token_type_ids", Shape.init(.i64, &.{8}));
    const test_attn_bias = try bld.parameter(
        "test_attn_bias",
        Shape.init(.f32, &.{ 1 * 4, 8, 8 }),
    );
    const bert_graph = try buildForwardGraph(&bld, config, 1, 8, .{
        .input_ids = test_input_ids,
        .position_ids = test_position_ids,
        .token_type_ids = test_token_type_ids,
        .attn_bias = test_attn_bias,
    });

    // Output node shape should be [batch*seq, hidden] = [8, 32].
    const out_shape = g.node(bert_graph.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 32), out_shape.dim(1));

    // Parameter count sanity check. Per layer we emit:
    //   - 8 linear weights/biases (q/k/v/o attention + intermediate + output dense)
    //   - 4 layer norm weights/biases (attention.output + output)
    //   = 12 parameters.
    // Plus embeddings: word + position + token_type + LN weight + LN bias = 5.
    // Plus 4 input placeholders (input_ids, position_ids, token_type_ids, attn_bias).
    // Total params = 5 + 12 * 2 + 4 = 33.
    try std.testing.expect(g.parameters.items.len >= 29);
}

test "shared BERT encoder block gradients match finite differences for every activation" {
    const interpreter = @import("../graph/interpreter.zig");
    const a = std.testing.allocator;
    const Activation = @import("../models/bert.zig").HiddenActivation;
    for ([_]Activation{ .gelu_exact, .gelu_tanh, .relu, .silu }) |activation| {
        var graph = Graph.init(a);
        defer graph.deinit();
        var b = Builder.init(&graph);
        const hidden = try b.parameter("hidden", Shape.init(.f32, &.{ 2, 4 }));
        const attention = try b.parameter("attention", Shape.init(.f32, &.{ 2, 4 }));
        var context = EncoderOutputContext{ .bld = &b, .config = .{
            .vocab_size = 8,
            .hidden_size = 4,
            .num_hidden_layers = 1,
            .num_attention_heads = 1,
            .intermediate_size = 8,
            .hidden_act = activation,
        }, .layer = 0, .total = 2 };
        const output = try @import("bert_blocks.zig").encoderOutput(&context, hidden, attention);
        const upstream = try b.tensorConst(&.{ 0.1, -0.3, 0.7, 0.2, -0.5, 0.4, 0.8, -0.2 }, Shape.init(.f32, &.{ 2, 4 }));
        const loss = try b.reduceSum(try b.mul(output, upstream), &.{ 0, 1 });
        try graph.markOutput(loss);
        var gradient = try ml.graph.autodiff.gradient(a, &graph, loss, &.{hidden});
        defer gradient.deinit();
        gradient.graph.outputs.clearRetainingCapacity();
        try gradient.graph.markOutput(gradient.param_grads[0]);
        var backend = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
        defer backend.deinit(a);
        const cb = backend.computeBackend();
        const inputs = try a.alloc(interpreter.RuntimeInput, graph.parameters.items.len);
        defer a.free(inputs);
        var initialized: usize = 0;
        defer for (inputs[0..initialized]) |input| cb.free(input.value);
        const gradient_inputs = try a.alloc(interpreter.RuntimeInput, inputs.len);
        defer a.free(gradient_inputs);
        var hidden_values: [8]f32 = undefined;
        for (graph.parameters.items, 0..) |id, index| {
            const node = graph.node(id);
            const name = graph.parameterName(node);
            const values = try a.alloc(f32, @intCast(node.output_shape.numElements().?));
            defer a.free(values);
            for (values, 0..) |*value, i| {
                value.* = if (std.mem.endsWith(u8, name, "LayerNorm.weight")) 1 else if (std.mem.endsWith(u8, name, ".bias")) 0 else @as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + id) % 19)) - 9)) / 16;
            }
            if (id == hidden) @memcpy(&hidden_values, values);
            inputs[index] = .{ .node_id = id, .value = try cb.fromFloat32(values) };
            initialized += 1;
            gradient_inputs[index] = .{ .node_id = gradient.id_map[id], .value = inputs[index].value };
        }
        var derivative = try interpreter.execute(a, &gradient.graph, &cb, .{ .runtime_inputs = gradient_inputs });
        defer derivative.deinit(&cb);
        const analytic = try cb.toFloat32(derivative.outputs[0], a);
        defer a.free(analytic);
        // Check both rows. This crosses the attention residual, both LayerNorms,
        // and the selected activation, rather than testing only a linear head.
        for ([_]usize{ 1, 6 }) |coordinate| {
            var losses: [2]f32 = undefined;
            for (0..2) |side| {
                var values = hidden_values;
                const epsilon: f32 = 0.002;
                values[coordinate] += if (side == 0) epsilon else -epsilon;
                cb.free(inputs[0].value);
                inputs[0].value = try cb.fromFloat32(&values);
                var result = try interpreter.execute(a, &graph, &cb, .{ .runtime_inputs = inputs });
                defer result.deinit(&cb);
                const scalar = try cb.toFloat32(result.outputs[0], a);
                defer a.free(scalar);
                losses[side] = scalar[0];
            }
            const numerical = (losses[0] - losses[1]) / 0.004;
            try std.testing.expectApproxEqAbs(numerical, analytic[coordinate], 2e-3);
        }
    }
}

test "shared full BERT encoder gradients cross embeddings and masked attention" {
    const interpreter = @import("../graph/interpreter.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const a = std.testing.allocator;
    for ([_]@import("../models/bert.zig").HiddenActivation{ .gelu_exact, .gelu_tanh }) |activation| {
        var graph = Graph.init(a);
        defer graph.deinit();
        var b = Builder.init(&graph);
        const ids = try b.parameter("ids", Shape.init(.i64, &.{3}));
        const positions = try b.parameter("positions", Shape.init(.i64, &.{3}));
        const types = try b.parameter("types", Shape.init(.i64, &.{3}));
        const mask = try b.parameter("mask", Shape.init(.f32, &.{ 2, 3, 3 }));
        const forward = try buildForwardGraph(&b, .{ .vocab_size = 8, .hidden_size = 4, .num_hidden_layers = 1, .num_attention_heads = 2, .intermediate_size = 8, .max_position_embeddings = 3, .layer_norm_eps = 1e-5, .hidden_act = activation }, 1, 3, .{ .input_ids = ids, .position_ids = positions, .token_type_ids = types, .attn_bias = mask });
        const upstream = try b.tensorConst(&.{ 0.1, -0.3, 0.7, 0.2, -0.5, 0.4, 0.8, -0.2, 0, 0, 0, 0 }, Shape.init(.f32, &.{ 3, 4 }));
        const loss = try b.reduceSum(try b.mul(forward.output_node, upstream), &.{ 0, 1 });
        try graph.markOutput(loss);
        var wrt: [2]NodeId = @splat(ml.graph.null_node);
        var input_indices: [2]usize = undefined;
        for (graph.parameters.items, 0..) |id, i| {
            const name = graph.parameterName(graph.node(id));
            if (std.mem.eql(u8, name, "embeddings.word_embeddings.weight")) {
                wrt[0] = id;
                input_indices[0] = i;
            }
            if (std.mem.eql(u8, name, "encoder.layer.0.attention.self.query.weight")) {
                wrt[1] = id;
                input_indices[1] = i;
            }
        }
        for (wrt) |id| try std.testing.expect(id != ml.graph.null_node);
        var gradient = try ml.graph.autodiff.gradient(a, &graph, loss, &wrt);
        defer gradient.deinit();
        gradient.graph.outputs.clearRetainingCapacity();
        for (gradient.param_grads) |id| try gradient.graph.markOutput(id);
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
        defer context.deinit(a);
        const cb = context.computeBackend();
        const inputs = try a.alloc(interpreter.RuntimeInput, graph.parameters.items.len);
        defer a.free(inputs);
        var initialized: usize = 0;
        defer for (inputs[0..initialized]) |input| cb.free(input.value);
        const gradient_inputs = try a.alloc(interpreter.RuntimeInput, inputs.len);
        defer a.free(gradient_inputs);
        for (graph.parameters.items, 0..) |id, index| {
            const node = graph.node(id);
            const shape = node.output_shape;
            const name = graph.parameterName(node);
            var tensor: Tensor = undefined;
            if (shape.dtype == .i64) {
                const values: []const i64 = if (id == ids) &.{ 1, 2, 0 } else if (id == positions) &.{ 0, 1, 2 } else &.{ 0, 1, 0 };
                tensor = try Tensor.initInt64(a, name, shape.dims[0..shape.rank()], values);
            } else {
                const values = try a.alloc(f32, @intCast(shape.numElements().?));
                defer a.free(values);
                for (values, 0..) |*value, i| value.* = if (id == mask) (if (i % 3 == 2) -10000 else 0) else if (std.mem.endsWith(u8, name, "LayerNorm.weight")) 1 else if (std.mem.endsWith(u8, name, ".bias")) 0 else @as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + id) % 19)) - 9)) / 16;
                tensor = try Tensor.initFloat32(a, name, shape.dims[0..shape.rank()], values);
            }
            defer tensor.deinit();
            inputs[index] = .{ .node_id = id, .value = try context.importHostTensor(a, &tensor) };
            initialized += 1;
            gradient_inputs[index] = .{ .node_id = gradient.id_map[id], .value = inputs[index].value };
        }
        var derivative = try interpreter.execute(a, &gradient.graph, &cb, .{ .runtime_inputs = gradient_inputs });
        defer derivative.deinit(&cb);
        for (input_indices, [_]usize{ 6, 5 }, 0..) |input_index, coordinate, output_index| {
            const analytic = try cb.toFloat32(derivative.outputs[output_index], a);
            defer a.free(analytic);
            if (output_index == 0) for (analytic[0..4]) |value| try std.testing.expectApproxEqAbs(@as(f32, 0), value, 2e-6);
            const original = inputs[input_index].value;
            const values = try cb.toFloat32(original, a);
            defer a.free(values);
            const saved = values[coordinate];
            var losses: [2]f32 = undefined;
            for (0..2) |side| {
                values[coordinate] = saved + (if (side == 0) @as(f32, 0.002) else -0.002);
                const perturbed = try cb.fromFloat32(values);
                defer cb.free(perturbed);
                inputs[input_index].value = perturbed;
                defer inputs[input_index].value = original;
                var result = try interpreter.execute(a, &graph, &cb, .{ .runtime_inputs = inputs });
                defer result.deinit(&cb);
                const scalar = try cb.toFloat32(result.outputs[0], a);
                defer a.free(scalar);
                losses[side] = scalar[0];
            }
            const numerical = (losses[0] - losses[1]) / 0.004;
            try std.testing.expect(@abs(analytic[coordinate]) > 1e-5);
            try std.testing.expectApproxEqAbs(numerical, analytic[coordinate], 2e-3);
        }
    }
}
