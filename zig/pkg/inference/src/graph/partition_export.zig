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
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;

const ops = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const ComputeBackend = ops.ComputeBackend;
const partition_mod = @import("partition.zig");

pub const ExportedSubgraph = struct {
    graph: Graph,
    input_node_ids: []NodeId,
    runtime_input_parameter_node_ids: []NodeId,
    output_node_ids: []NodeId,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ExportedSubgraph) void {
        self.graph.deinit();
        self.allocator.free(self.input_node_ids);
        self.allocator.free(self.runtime_input_parameter_node_ids);
        self.allocator.free(self.output_node_ids);
    }
};

pub fn computeOutputs(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    part_set: *const std.AutoHashMapUnmanaged(NodeId, void),
) ![]NodeId {
    const count = graph.nodeCount();

    var is_output = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer is_output.deinit(allocator);

    for (graph.outputs.items) |out_id| {
        if (part_set.contains(out_id)) {
            try is_output.put(allocator, out_id, {});
        }
    }

    for (0..count) |i| {
        const nid: NodeId = @intCast(i);
        if (part_set.contains(nid)) continue;
        const n = graph.node(nid);
        for (n.getInputs()) |inp| {
            if (inp == null_node or inp >= count) continue;
            if (part_set.contains(inp)) {
                try is_output.put(allocator, inp, {});
            }
        }
    }

    var result = std.ArrayListUnmanaged(NodeId).empty;
    errdefer result.deinit(allocator);
    for (0..count) |i| {
        const nid: NodeId = @intCast(i);
        if (is_output.contains(nid)) {
            try result.append(allocator, nid);
        }
    }
    return result.toOwnedSlice(allocator);
}

pub fn buildExportableSubgraph(
    allocator: std.mem.Allocator,
    src: *const Graph,
    part: *const partition_mod.Partition,
    cb: *const ComputeBackend,
    preserve_weight_parameters: bool,
    extra_output_node_ids: []const NodeId,
) !ExportedSubgraph {
    var part_set = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer part_set.deinit(allocator);
    for (part.node_ids) |nid| {
        try part_set.put(allocator, nid, {});
    }

    var required_nodes = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer required_nodes.deinit(allocator);
    for (part.node_ids) |nid| {
        try markRequiredForLowering(allocator, src, &required_nodes, nid);
    }

    const base_output_node_ids = try computeOutputs(allocator, src, &part_set);
    defer allocator.free(base_output_node_ids);

    var output_node_id_list = std.ArrayListUnmanaged(NodeId).empty;
    errdefer output_node_id_list.deinit(allocator);
    try output_node_id_list.appendSlice(allocator, base_output_node_ids);
    for (extra_output_node_ids) |extra_id| {
        if (!part_set.contains(extra_id)) return error.InvalidArtifactOutput;
        var found = false;
        for (output_node_id_list.items) |existing| {
            if (existing == extra_id) {
                found = true;
                break;
            }
        }
        if (!found) try output_node_id_list.append(allocator, extra_id);
    }

    const output_node_ids = try output_node_id_list.toOwnedSlice(allocator);
    errdefer allocator.free(output_node_ids);

    var graph = Graph.init(allocator);
    errdefer graph.deinit();

    const old_to_new = try allocator.alloc(?NodeId, src.nodeCount());
    defer allocator.free(old_to_new);
    @memset(old_to_new, null);

    var input_node_ids = std.ArrayListUnmanaged(NodeId).empty;
    errdefer input_node_ids.deinit(allocator);
    var runtime_input_parameter_node_ids = std.ArrayListUnmanaged(NodeId).empty;
    errdefer runtime_input_parameter_node_ids.deinit(allocator);

    for (part.external_inputs) |ext_in| {
        const src_node = src.node(ext_in.node_id);
        old_to_new[@intCast(ext_in.node_id)] = switch (src_node.op) {
            .constant => try copyConstantNode(&graph, src, ext_in.node_id, src_node),
            .parameter => if (isSkipKvRuntimeInput(src, part, ext_in.node_id)) blk: {
                const new_id = try copyRuntimeInputParameter(&graph, src_node, input_node_ids.items.len);
                try input_node_ids.append(allocator, ext_in.node_id);
                try runtime_input_parameter_node_ids.append(allocator, new_id);
                break :blk new_id;
            } else if (preserve_weight_parameters and canPreserveWeightParameter(src, src_node, cb))
                try copyParameterNode(&graph, src, src_node)
            else
                try copyWeightParameterAsConstant(&graph, src, ext_in.node_id, src_node, cb),
            else => blk: {
                const new_id = try copyRuntimeInputParameter(&graph, src_node, input_node_ids.items.len);
                try input_node_ids.append(allocator, ext_in.node_id);
                try runtime_input_parameter_node_ids.append(allocator, new_id);
                break :blk new_id;
            },
        };
    }

    for (0..src.nodeCount()) |i| {
        const old_id: NodeId = @intCast(i);
        if (!required_nodes.contains(old_id)) continue;
        if (old_to_new[@intCast(old_id)] != null) continue;
        const src_node = src.node(old_id);
        old_to_new[@intCast(old_id)] = switch (src_node.op) {
            .fused_from_float32 => blk: {
                const new_id = try copyRuntimeInputParameter(&graph, src_node, input_node_ids.items.len);
                try input_node_ids.append(allocator, old_id);
                try runtime_input_parameter_node_ids.append(allocator, new_id);
                break :blk new_id;
            },
            .fused_to_float32 => blk: {
                if (requiresExternalPairSecondRuntimeInput(src, &part_set, src_node)) {
                    const new_id = try copyRuntimeInputParameter(&graph, src_node, input_node_ids.items.len);
                    try input_node_ids.append(allocator, old_id);
                    try runtime_input_parameter_node_ids.append(allocator, new_id);
                    break :blk new_id;
                }
                break :blk try copyPartitionNode(&graph, src_node, old_to_new);
            },
            .parameter => if (preserve_weight_parameters and canPreserveWeightParameter(src, src_node, cb))
                try copyParameterNode(&graph, src, src_node)
            else
                try copyWeightParameterAsConstant(&graph, src, old_id, src_node, cb),
            .constant => try copyConstantNode(&graph, src, old_id, src_node),
            else => try copyPartitionNode(&graph, src_node, old_to_new),
        };
    }

    for (output_node_ids) |old_id| {
        const new_id = old_to_new[@intCast(old_id)] orelse return error.MissingOutputMapping;
        try graph.markOutput(new_id);
    }

    return .{
        .graph = graph,
        .input_node_ids = try input_node_ids.toOwnedSlice(allocator),
        .runtime_input_parameter_node_ids = try runtime_input_parameter_node_ids.toOwnedSlice(allocator),
        .output_node_ids = output_node_ids,
        .allocator = allocator,
    };
}

fn isSkipKvRuntimeInput(
    graph: *const Graph,
    part: *const partition_mod.Partition,
    node_id: NodeId,
) bool {
    for (part.node_ids) |part_node_id| {
        const node = graph.node(part_node_id);
        switch (node.op) {
            .fused_gqa_causal_attention => |attrs| {
                if (!attrs.skip_kv_write) continue;
                const inputs = node.getInputs();
                if (inputs.len < 3) continue;
                if (inputs[1] == node_id or inputs[2] == node_id) return true;
            },
            else => {},
        }
    }
    return false;
}

fn requiresExternalPairSecondRuntimeInput(
    src: *const Graph,
    part_set: *const std.AutoHashMapUnmanaged(NodeId, void),
    src_node: *const Node,
) bool {
    const inputs = src_node.getInputs();
    if (inputs.len != 1) return false;
    const pair_id = inputs[0];
    if (pair_id == null_node or part_set.contains(pair_id)) return false;
    return std.meta.activeTag(src.node(pair_id).op) == .fused_linear_no_bias_pair;
}

fn copyParameterNode(
    dst: *Graph,
    src: *const Graph,
    src_node: *const Node,
) !NodeId {
    const name = src.parameterName(src_node);
    const interned = try dst.internString(name);
    const node_id = try dst.addNode(.{
        .op = .{ .parameter = .{ .name_offset = interned.offset, .name_len = interned.len } },
        .output_shape = src_node.output_shape,
    });
    try dst.parameters.append(dst.allocator, node_id);
    return node_id;
}

fn canPreserveWeightParameter(
    src: *const Graph,
    src_node: *const Node,
    cb: *const ComputeBackend,
) bool {
    const name = src.parameterName(src_node);
    const ct = cb.getWeight(name) catch return false;
    cb.free(ct);
    return true;
}

fn markRequiredForLowering(
    allocator: std.mem.Allocator,
    src: *const Graph,
    required_nodes: *std.AutoHashMapUnmanaged(NodeId, void),
    node_id: NodeId,
) !void {
    if (required_nodes.contains(node_id)) return;
    try required_nodes.put(allocator, node_id, {});

    const node = src.node(node_id);
    if (node.vjp_alternate != null_node) {
        try markRequiredSubgraph(allocator, src, required_nodes, node.vjp_alternate);
    }
}

fn markRequiredSubgraph(
    allocator: std.mem.Allocator,
    src: *const Graph,
    required_nodes: *std.AutoHashMapUnmanaged(NodeId, void),
    node_id: NodeId,
) !void {
    if (node_id == null_node or required_nodes.contains(node_id)) return;
    try required_nodes.put(allocator, node_id, {});

    const node = src.node(node_id);
    for (node.getInputs()) |input_id| {
        if (input_id == null_node) continue;
        try markRequiredSubgraph(allocator, src, required_nodes, input_id);
    }
}

fn copyRuntimeInputParameter(
    dst: *Graph,
    src_node: *const Node,
    input_idx: usize,
) !NodeId {
    const name = try std.fmt.allocPrint(dst.allocator, "input_{d}", .{input_idx});
    defer dst.allocator.free(name);
    const interned = try dst.internString(name);
    const new_id = try dst.addNode(.{
        .op = .{ .parameter = .{ .name_offset = interned.offset, .name_len = interned.len } },
        .output_shape = src_node.output_shape,
    });
    try dst.parameters.append(dst.allocator, new_id);
    return new_id;
}

fn copyWeightParameterAsConstant(
    dst: *Graph,
    src: *const Graph,
    src_node_id: NodeId,
    src_node: *const Node,
    cb: *const ComputeBackend,
) !NodeId {
    _ = src_node_id;
    const weight_name = src.parameterName(src_node);
    const ct = try getExportWeight(dst.allocator, cb, weight_name);
    defer cb.free(ct);
    var constant_shape = src_node.output_shape;
    if (cb.tensorShape(ct, dst.allocator)) |actual_shape| {
        defer dst.allocator.free(actual_shape);
        constant_shape = ml.graph.Shape.init(src_node.output_shape.dtype, actual_shape);
    } else |err| switch (err) {
        error.UnsupportedShape => {},
        else => return err,
    }
    const data = try cb.toFloat32(ct, dst.allocator);
    defer dst.allocator.free(data);
    constant_shape = try resolveDynamicShapeFromElementCount(dst.allocator, constant_shape, data.len);
    try ensureConstantPoolCapacity(dst, try f32ByteLen(data.len));
    const loc = try dst.internConstant(data);
    return dst.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = constant_shape,
    });
}

fn copyConstantNode(
    dst: *Graph,
    src: *const Graph,
    old_id: NodeId,
    src_node: *const Node,
) !NodeId {
    const attrs = src_node.op.constant;
    const byte_len = try constantByteLen(src_node.output_shape.dtype, attrs.data_len);
    const data = src.constantBytes(attrs.data_offset, byte_len);
    try ensureConstantPoolCapacity(dst, data.len);
    const loc = try dst.internConstantBytes(data, src_node.output_shape.dtype);
    _ = old_id;
    return dst.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = src_node.output_shape,
    });
}

fn resolveDynamicShapeFromElementCount(
    allocator: std.mem.Allocator,
    shape: ml.graph.Shape,
    element_count: usize,
) !ml.graph.Shape {
    _ = allocator;
    var resolved = shape;
    var unknown_axis: ?u8 = null;
    var known_product: usize = 1;
    for (0..shape.rank()) |i| {
        const dim = shape.dim(@intCast(i));
        if (dim <= 0) {
            if (unknown_axis != null) return shape;
            unknown_axis = @intCast(i);
            continue;
        }
        known_product *= @as(usize, @intCast(dim));
    }
    if (unknown_axis == null or known_product == 0 or element_count % known_product != 0) return shape;
    resolved.dims[unknown_axis.?] = @intCast(@divExact(element_count, known_product));
    return resolved;
}

fn ensureConstantPoolCapacity(dst: *const Graph, additional_len: usize) !void {
    const current_len = dst.constant_pool.items.len;
    const max_u32 = std.math.maxInt(u32);
    if (current_len > max_u32) return error.ConstantPoolTooLarge;
    if (additional_len > max_u32) return error.ConstantPoolTooLarge;
    if (current_len + additional_len > max_u32) return error.ConstantPoolTooLarge;
}

fn constantByteLen(dtype: ml.graph.DType, elem_count: u32) !u32 {
    const bytes = std.math.mul(usize, @intCast(elem_count), dtype.byteSize()) catch return error.ConstantPoolTooLarge;
    if (bytes > std.math.maxInt(u32)) return error.ConstantPoolTooLarge;
    return @intCast(bytes);
}

fn f32ByteLen(elem_count: usize) !usize {
    return std.math.mul(usize, elem_count, @sizeOf(f32)) catch return error.ConstantPoolTooLarge;
}

fn getExportWeight(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    name: []const u8,
) !contracts.CT {
    return cb.getWeight(name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => {
            if (std.mem.eql(u8, name, "lm_head.weight")) {
                return cb.getWeight("wte.weight") catch cb.getWeight("model.embed_tokens.weight");
            }
            if (try buildGpt2SplitProjectionWeight(allocator, cb, name)) |ct| {
                return ct;
            }
            var fallback_buf: [128]u8 = undefined;
            if (omittedVProjFallback(name, &fallback_buf)) |fallback_name| {
                return cb.getWeight(fallback_name);
            }
            std.log.err("export missing weight name={s}", .{name});
            return err;
        },
        else => return err,
    };
}

fn omittedVProjFallback(name: []const u8, buf: *[128]u8) ?[]const u8 {
    const prefix = "model.layers.";
    const suffix = ".self_attn.v_proj.weight";
    if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, suffix)) return null;
    const layer_text = name[prefix.len .. name.len - suffix.len];
    const layer = std.fmt.parseInt(usize, layer_text, 10) catch return null;
    return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer}) catch null;
}

fn buildGpt2SplitProjectionWeight(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    name: []const u8,
) !?contracts.CT {
    const suffix_weight = "_proj.weight";
    const suffix_bias = "_proj.bias";
    const prefix = "h.";
    const mid = ".attn.";
    if (!std.mem.startsWith(u8, name, prefix)) return null;
    const attn_idx = std.mem.indexOf(u8, name, mid) orelse return null;
    const layer_text = name[prefix.len..attn_idx];
    const layer = std.fmt.parseInt(usize, layer_text, 10) catch return null;
    const tail = name[attn_idx + mid.len ..];
    const proj_and_suffix = if (std.mem.endsWith(u8, tail, suffix_weight))
        tail[0 .. tail.len - suffix_weight.len]
    else if (std.mem.endsWith(u8, tail, suffix_bias))
        tail[0 .. tail.len - suffix_bias.len]
    else
        return null;
    const chunk_idx: usize = if (std.mem.eql(u8, proj_and_suffix, "q"))
        0
    else if (std.mem.eql(u8, proj_and_suffix, "k"))
        1
    else if (std.mem.eql(u8, proj_and_suffix, "v"))
        2
    else
        return null;

    var buf: [128]u8 = undefined;
    const is_bias = std.mem.endsWith(u8, name, suffix_bias);
    const fused_name = try std.fmt.bufPrint(&buf, "h.{d}.attn.c_attn.{s}", .{ layer, if (is_bias) "bias" else "weight" });
    const fused = try cb.getWeight(fused_name);
    defer cb.free(fused);
    const fused_data = try cb.toFloat32(fused, allocator);
    defer allocator.free(fused_data);
    const fused_bias_name = try std.fmt.bufPrint(&buf, "h.{d}.attn.c_attn.bias", .{layer});
    const fused_bias = try cb.getWeight(fused_bias_name);
    defer cb.free(fused_bias);
    const fused_bias_data = try cb.toFloat32(fused_bias, allocator);
    defer allocator.free(fused_bias_data);

    if (is_bias) {
        if (fused_bias_data.len == 0 or fused_bias_data.len % 3 != 0) return error.UnsupportedShape;
        const out_dim: usize = @divExact(fused_bias_data.len, 3);
        const row_start = chunk_idx * out_dim;
        const slice = try allocator.alloc(f32, out_dim);
        defer allocator.free(slice);
        @memcpy(slice, fused_bias_data[row_start..][0..out_dim]);
        const shape_buf = [_]i32{@intCast(out_dim)};
        return cb.fromFloat32Shape(slice, &shape_buf);
    }

    if (fused_bias_data.len == 0 or fused_bias_data.len % 3 != 0) return error.UnsupportedShape;
    const out_dim: usize = @divExact(fused_bias_data.len, 3);
    if (out_dim == 0 or fused_data.len % (3 * out_dim) != 0) return error.UnsupportedShape;
    const in_dim: usize = @divExact(fused_data.len, 3 * out_dim);
    const row_start = chunk_idx * out_dim;
    const slice = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(slice);
    for (0..out_dim) |r| {
        @memcpy(slice[r * in_dim ..][0..in_dim], fused_data[(row_start + r) * in_dim ..][0..in_dim]);
    }
    const shape_buf = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
    return cb.fromFloat32Shape(slice, &shape_buf);
}

fn copyPartitionNode(
    dst: *Graph,
    src_node: *const Node,
    old_to_new: []const ?NodeId,
) !NodeId {
    var new_node = src_node.*;
    for (0..new_node.num_inputs) |i| {
        const old_input = new_node.inputs[i];
        if (old_input == null_node) continue;
        new_node.inputs[i] = old_to_new[@intCast(old_input)] orelse return error.MissingInputMapping;
    }
    if (new_node.vjp_alternate != null_node) {
        new_node.vjp_alternate = old_to_new[@intCast(new_node.vjp_alternate)] orelse return error.MissingVjpAlternateMapping;
    }
    return dst.addNode(new_node);
}

const Shape = ml.graph.Shape;
const Builder = ml.graph.Builder;
const tracing_compute = @import("tracing_compute.zig");

test "computeOutputs finds graph outputs and cross-partition edges" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.gelu(x);
    const z = try b.relu(y);
    try g.markOutput(z);

    var part_set = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer part_set.deinit(allocator);
    try part_set.put(allocator, y, {});

    const outputs = try computeOutputs(allocator, &g, &part_set);
    defer allocator.free(outputs);

    try std.testing.expectEqual(@as(usize, 1), outputs.len);
    try std.testing.expectEqual(y, outputs[0]);
}

test "buildExportableSubgraph preserves fused decomposition closure for lowering" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const y = try b.linearNoBias(x, w, 2, 4, 3);
    try g.markOutput(y);

    const ext_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = x, .source_partition = 0 },
        .{ .node_id = w, .source_partition = 0 },
    };
    const part_nodes = [_]NodeId{y};
    const part = partition_mod.Partition{
        .backend = .onnx,
        .node_ids = &part_nodes,
        .external_inputs = &ext_inputs,
    };

    var tracer = try tracing_compute.TracingCompute.initWithWeights(allocator, &.{});
    defer tracer.deinit();
    var cb = tracer.backend();

    var subgraph = try buildExportableSubgraph(allocator, &g, &part, &cb, false, &.{});
    defer subgraph.deinit();

    var lowered = try ml.graph.lower.lower(allocator, &subgraph.graph);
    defer lowered.deinit();

    for (0..lowered.graph.nodeCount()) |i| {
        try std.testing.expect(lowered.graph.node(@intCast(i)).op.isPrimitive());
    }
}

test "buildExportableSubgraph materializes external pair second output as runtime input" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4 }));
    const w1 = try b.parameter("w1", Shape.init(.f32, &.{ 3, 4 }));
    const w2 = try b.parameter("w2", Shape.init(.f32, &.{ 3, 4 }));

    const pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{
            .rows = 1,
            .in_dim = 4,
            .out_dim = 3,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 3 }),
        .inputs = .{ x, w1, w2, null_node },
        .num_inputs = 3,
    });
    const second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = Shape.init(.f32, &.{ 1, 3 }),
        .inputs = .{ pair, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const out = try b.relu(second);
    try g.markOutput(out);

    const ext_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = pair, .source_partition = 0 },
    };
    const part_nodes = [_]NodeId{ second, out };
    const part = partition_mod.Partition{
        .backend = .onnx,
        .node_ids = &part_nodes,
        .external_inputs = &ext_inputs,
    };

    var tracer = try tracing_compute.TracingCompute.initWithWeights(allocator, &.{});
    defer tracer.deinit();
    var cb = tracer.backend();

    var subgraph = try buildExportableSubgraph(allocator, &g, &part, &cb, false, &.{});
    defer subgraph.deinit();

    try std.testing.expectEqual(@as(usize, 2), subgraph.input_node_ids.len);
    try std.testing.expectEqual(pair, subgraph.input_node_ids[0]);
    try std.testing.expectEqual(second, subgraph.input_node_ids[1]);

    const second_param_id = subgraph.runtime_input_parameter_node_ids[1];
    const second_param_node = subgraph.graph.node(second_param_id);
    try std.testing.expectEqual(.parameter, std.meta.activeTag(second_param_node.op));
    try std.testing.expectEqual(Shape.init(.f32, &.{ 1, 3 }), second_param_node.output_shape);
}

test "buildExportableSubgraph preserves typed constant bytes" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();

    const values = [_]i64{ 7, 11, 13 };
    const loc = try g.internConstantBytes(std.mem.sliceAsBytes(&values), .i64);
    const c = try g.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = Shape.init(.i64, &.{3}),
    });
    try g.markOutput(c);

    const part_nodes = [_]NodeId{c};
    const part = partition_mod.Partition{
        .backend = .onnx,
        .node_ids = &part_nodes,
        .external_inputs = &.{},
    };

    var tracer = try tracing_compute.TracingCompute.initWithWeights(allocator, &.{});
    defer tracer.deinit();
    var cb = tracer.backend();

    var subgraph = try buildExportableSubgraph(allocator, &g, &part, &cb, false, &.{});
    defer subgraph.deinit();

    const copied = subgraph.graph.node(subgraph.graph.outputs.items[0]);
    try std.testing.expectEqual(Shape.init(.i64, &.{3}), copied.output_shape);
    const attrs = copied.op.constant;
    try std.testing.expectEqualSlices(i64, &values, subgraph.graph.constantDataAs(i64, attrs.data_offset, attrs.data_len));
}

test "buildExportableSubgraph keeps skip-kv attention cache inputs runtime" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("q", Shape.init(.f32, &.{ 1, 4 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 1, 2 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 1, 2 }));
    const attn = try g.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 2,
            .num_kv_heads = 1,
            .head_dim = 2,
            .layer_index = 3,
            .skip_kv_write = true,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try g.markOutput(attn);

    const ext_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = q, .source_partition = 0 },
        .{ .node_id = k, .source_partition = 0 },
        .{ .node_id = v, .source_partition = 0 },
    };
    const part_nodes = [_]NodeId{attn};
    const part = partition_mod.Partition{
        .backend = .onnx,
        .node_ids = &part_nodes,
        .external_inputs = &ext_inputs,
    };

    var tracer = try tracing_compute.TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "q", .shape = Shape.init(.f32, &.{ 1, 4 }) },
        .{ .name = "k", .shape = Shape.init(.f32, &.{ 1, 2 }) },
        .{ .name = "v", .shape = Shape.init(.f32, &.{ 1, 2 }) },
    });
    defer tracer.deinit();
    var cb = tracer.backend();

    var subgraph = try buildExportableSubgraph(allocator, &g, &part, &cb, true, &.{});
    defer subgraph.deinit();

    try std.testing.expectEqual(@as(usize, 2), subgraph.input_node_ids.len);
    try std.testing.expectEqual(k, subgraph.input_node_ids[0]);
    try std.testing.expectEqual(v, subgraph.input_node_ids[1]);
}
