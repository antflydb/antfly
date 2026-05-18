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

//! WebGPU partition executor.
//!
//! WebGPU kernels are promoted incrementally. This executor validates the graph
//! buffer-plan contract, directly dispatches the claimed WebGPU command surface,
//! and delegates only partitions that are not fully covered by that surface.

const std = @import("std");
const ml = @import("ml");

const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const partition_mod = @import("partition.zig");
const buffer_plan_mod = @import("buffer_plan.zig");
const device_mesh_mod = @import("device_mesh.zig");
const native_partition_executor = @import("native_partition_executor.zig");
const interpreter = @import("interpreter.zig");
const operator_plan_mod = @import("operator_plan.zig");
const webgpu_capabilities = @import("webgpu_capabilities.zig");

const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;

const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const PartitionExecutor = partition_mod.PartitionExecutor;
const DeviceId = device_mesh_mod.DeviceId;
const GraphPlanSlot = ops_mod.GraphPlanSlot;
const OperatorPlan = operator_plan_mod.OperatorPlan;

const max_graph_plan_slots = 64;

pub const WebGpuGraphPlanAllocation = struct {
    allocation: buffer_plan_mod.AllocationId,
    graph_slot: usize,
    bytes: usize,
};

pub const WebGpuPartitionGraphPlan = struct {
    slots: []const GraphPlanSlot,
    allocations: []const WebGpuGraphPlanAllocation,

    pub fn deinit(self: *WebGpuPartitionGraphPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        allocator.free(self.allocations);
    }
};

pub const WebGpuShaderCommand = enum {
    elementwise_binary,
    where_select,
    unary,
    reshape_view,
    slice_copy,
    concat_copy,
    broadcast,
    reduction,
    softmax,
    log_softmax,
    matmul_transb,
    matmul_transb_bias,
    pair_second,
    rms_norm,
    layer_norm,
    gelu,
};

pub const WebGpuEncodedCommand = struct {
    node_id: NodeId,
    shader: WebGpuShaderCommand,
    output_elements: usize,
};

pub const WebGpuPartitionExecutor = struct {
    allocator: std.mem.Allocator,
    graph: *const Graph,
    backend: *const ComputeBackend,
    pe: PartitionExecutor = undefined,
    owned: bool = false,

    const vtable = PartitionExecutor.VTable{
        .execute = &executeFn,
        .deinit = &deinitFn,
    };

    pub fn initBorrowed(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        backend: *const ComputeBackend,
    ) WebGpuPartitionExecutor {
        return .{
            .allocator = allocator,
            .graph = graph,
            .backend = backend,
        };
    }

    pub fn create(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        backend: *const ComputeBackend,
    ) !*WebGpuPartitionExecutor {
        const exec = try allocator.create(WebGpuPartitionExecutor);
        exec.* = .{
            .allocator = allocator,
            .graph = graph,
            .backend = backend,
            .owned = true,
        };
        exec.pe = .{ .ptr = exec, .vtable = &vtable };
        return exec;
    }

    pub fn partitionExecutor(self: *WebGpuPartitionExecutor) *const PartitionExecutor {
        self.pe = .{ .ptr = self, .vtable = &vtable };
        return &self.pe;
    }

    fn executeFn(
        ctx: *anyopaque,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        device_id: DeviceId,
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) anyerror!void {
        const self: *WebGpuPartitionExecutor = @ptrCast(@alignCast(ctx));
        return self.execute(values, value_device, node_ids, device_id, exec_ctx);
    }

    fn deinitFn(ctx: *anyopaque) void {
        const self: *WebGpuPartitionExecutor = @ptrCast(@alignCast(ctx));
        if (self.owned) self.allocator.destroy(self);
    }

    fn execute(
        self: *WebGpuPartitionExecutor,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        device_id: DeviceId,
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) !void {
        const allocator = exec_ctx.allocator orelse self.allocator;
        const buffer_plan = exec_ctx.buffer_plan orelse return error.MissingPartitionExecutionContext;
        const partition_plan = exec_ctx.partition_plan orelse return error.MissingPartitionExecutionContext;
        const partition_index = try partitionIndexForNodes(buffer_plan, node_ids);

        var partition_view = try buffer_plan.partitionView(allocator, partition_plan, partition_index);
        defer partition_view.deinit(allocator);
        try validatePartitionView(partition_view, node_ids);

        var webgpu_graph_plan = try buildWebGpuGraphPlan(allocator, buffer_plan, partition_view);
        defer webgpu_graph_plan.deinit(allocator);
        const graph_plan_reserved = try (exec_ctx.backend orelse self.backend).reserveGraphPlanSlots(webgpu_graph_plan.slots);
        if (graph_plan_reserved) {
            if (exec_ctx.stats) |stats| {
                stats.graph_plan_slots_reserved += webgpu_graph_plan.slots.len;
                for (webgpu_graph_plan.slots) |slot| stats.graph_plan_bytes_reserved += slot.bytes;
            }
        }

        if (try self.executeCommands(values, value_device, node_ids, device_id, exec_ctx)) {
            return;
        }

        var native_exec = native_partition_executor.NativePartitionExecutor.initBorrowed(
            allocator,
            exec_ctx.graph orelse self.graph,
            exec_ctx.backend orelse self.backend,
        );
        try native_exec.partitionExecutor().execute(values, value_device, node_ids, device_id, exec_ctx);
    }

    fn executeCommands(
        self: *WebGpuPartitionExecutor,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        device_id: DeviceId,
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) !bool {
        const allocator = exec_ctx.allocator orelse self.allocator;
        const graph = exec_ctx.graph orelse self.graph;
        const cb = exec_ctx.backend orelse self.backend;
        const reachable = exec_ctx.reachable orelse return error.MissingPartitionExecutionContext;
        const last_use = exec_ctx.last_use orelse return error.MissingPartitionExecutionContext;
        const partition_plan = exec_ctx.partition_plan orelse return error.MissingPartitionExecutionContext;
        const options = exec_ctx.options orelse interpreter.ExecuteOptions{};

        if (!supportsCommandPartition(graph, node_ids, reachable, options.runtime_inputs)) {
            return false;
        }

        var rt_map = std.AutoHashMapUnmanaged(NodeId, CT).empty;
        defer rt_map.deinit(allocator);
        var donated = std.AutoHashMapUnmanaged(NodeId, void).empty;
        defer donated.deinit(allocator);
        var pair_second: ?CT = null;
        defer if (pair_second) |ct| cb.free(ct);
        if (options.runtime_inputs) |inputs| {
            for (inputs, 0..) |ri, idx| {
                try rt_map.put(allocator, ri.node_id, ri.value);
                if (options.donate) |donate| {
                    if (idx < donate.len and donate[idx]) try donated.put(allocator, ri.node_id, {});
                }
            }
        }

        for (node_ids) |node_id| {
            const i: usize = @intCast(node_id);
            if (i >= reachable.len or !reachable[i]) continue;

            if (rt_map.get(node_id)) |rt_val| {
                values[i] = rt_val;
                value_device[i] = device_id;
                continue;
            }

            const n = graph.node(node_id);
            if (n.op == .fused_from_float32) continue;
            _ = encodeWebGpuCommandNode(graph, node_id, n) orelse return error.UnsupportedPrimitiveOp;

            const op_plan = partition_plan.operatorPlanForNode(node_id);
            values[i] = try executeCommandNode(cb, graph, values, n, op_plan, &pair_second);
            value_device[i] = device_id;
            if (exec_ctx.stats) |stats| {
                stats.backend_command_dispatches += 1;
                if (op_plan != null) stats.planned_operator_dispatches += 1;
            }

            try interpreter.cloneOutputIfAliasedInputWouldBeFreed(
                allocator,
                graph,
                cb,
                values,
                node_id,
                last_use,
                rt_map,
                donated,
            );

            for (n.getInputs()) |input_id| {
                if (input_id == null_node or input_id >= values.len) continue;
                const input_index: usize = @intCast(input_id);
                if (last_use[input_index] != i) continue;
                if (rt_map.contains(input_id) and !donated.contains(input_id)) continue;
                if (values[input_index]) |ct| {
                    if (values[i]) |out_ct| {
                        if (ct == out_ct and interpreter.canKeepAliasedOutput(n.op)) {
                            values[input_index] = null;
                            continue;
                        }
                    }
                    cb.free(ct);
                    values[input_index] = null;
                }
            }
        }

        return true;
    }
};

fn supportsCommandPartition(
    graph: *const Graph,
    node_ids: []const NodeId,
    reachable: []const bool,
    runtime_inputs: ?[]const interpreter.RuntimeInput,
) bool {
    for (node_ids) |node_id| {
        const i: usize = @intCast(node_id);
        if (i >= reachable.len or !reachable[i]) continue;
        if (runtimeInputContains(runtime_inputs, node_id)) continue;
        const node = graph.node(node_id);
        const op = node.op;
        switch (op) {
            .fused_from_float32,
            .add,
            .fused_elem_add,
            .mul,
            .fused_elem_multiply,
            .sub,
            .div,
            .less_than,
            .where_select,
            .neg,
            .sqrt,
            .rsqrt,
            .exp,
            .log,
            .sin,
            .cos,
            .tanh,
            .erf,
            .abs,
            .reshape,
            .slice,
            .concat_prim,
            .broadcast_in_dim,
            .reduce_sum,
            .reduce_max,
            .reduce_mean,
            .fused_softmax,
            .fused_log_softmax,
            .fused_linear,
            .fused_to_float32,
            => {},
            .fused_linear_no_bias, .fused_linear_no_bias_pair => |attrs| {
                if (!webGpuLinearAttrsHaveSupportedShape(attrs)) return false;
            },
            .fused_layer_norm,
            .fused_rms_norm,
            => |attrs| {
                const output_elems = tensorElementCount(node.output_shape) orelse return false;
                if (attrs.dim == 0 or output_elems % attrs.dim != 0) return false;
            },
            .fused_gelu,
            => {},
            else => return false,
        }
    }
    return true;
}

fn runtimeInputContains(runtime_inputs: ?[]const interpreter.RuntimeInput, node_id: NodeId) bool {
    const inputs = runtime_inputs orelse return false;
    for (inputs) |input| {
        if (input.node_id == node_id) return true;
    }
    return false;
}

fn encodeWebGpuCommandNode(
    graph: *const Graph,
    node_id: NodeId,
    node: *const ml.graph.Node,
) ?WebGpuEncodedCommand {
    const output_elements = tensorElementCount(node.output_shape) orelse return null;
    const shader: WebGpuShaderCommand = switch (node.op) {
        .add,
        .fused_elem_add,
        .mul,
        .fused_elem_multiply,
        .sub,
        .div,
        .less_than,
        => .elementwise_binary,
        .where_select => .where_select,
        .neg,
        .sqrt,
        .rsqrt,
        .exp,
        .log,
        .sin,
        .cos,
        .tanh,
        .erf,
        .abs,
        => .unary,
        .reshape => .reshape_view,
        .slice => .slice_copy,
        .concat_prim => .concat_copy,
        .broadcast_in_dim => .broadcast,
        .reduce_sum,
        .reduce_max,
        .reduce_mean,
        => .reduction,
        .fused_softmax => .softmax,
        .fused_log_softmax => .log_softmax,
        .fused_linear => .matmul_transb_bias,
        .fused_linear_no_bias => |attrs| blk: {
            if (!webGpuLinearAttrsHaveSupportedShape(attrs)) return null;
            break :blk .matmul_transb;
        },
        .fused_linear_no_bias_pair => |attrs| blk: {
            if (!webGpuLinearAttrsHaveSupportedShape(attrs)) return null;
            break :blk .matmul_transb;
        },
        .fused_to_float32 => .pair_second,
        .fused_layer_norm => |attrs| blk: {
            if (attrs.dim == 0 or output_elements % attrs.dim != 0) return null;
            break :blk .layer_norm;
        },
        .fused_rms_norm => |attrs| blk: {
            if (attrs.dim == 0 or output_elements % attrs.dim != 0) return null;
            break :blk .rms_norm;
        },
        .fused_gelu => .gelu,
        else => return null,
    };
    _ = graph;
    return .{
        .node_id = node_id,
        .shader = shader,
        .output_elements = output_elements,
    };
}

fn expectWebGpuEncodedCommand(graph: *const Graph, node_id: NodeId, expected: WebGpuShaderCommand) !void {
    const node = graph.node(node_id);
    try std.testing.expect(webgpu_capabilities.supportsWebGpuGraph(node.op));
    try std.testing.expectEqual(expected, encodeWebGpuCommandNode(graph, node_id, node).?.shader);
}

fn executeCommandNode(
    cb: *const ComputeBackend,
    graph: *const Graph,
    values: []?CT,
    node: *const ml.graph.Node,
    op_plan: ?OperatorPlan,
    pair_second: *?CT,
) !CT {
    const inputs = node.getInputs();
    return switch (node.op) {
        .add, .fused_elem_add => blk: {
            const lhs = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const rhs = valueFor(values, inputs[1]) orelse return error.MissingValue;
            break :blk try cb.add(lhs, rhs);
        },
        .mul, .fused_elem_multiply => blk: {
            const lhs = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const rhs = valueFor(values, inputs[1]) orelse return error.MissingValue;
            break :blk try cb.multiply(lhs, rhs);
        },
        .sub => blk: {
            const lhs = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const rhs = valueFor(values, inputs[1]) orelse return error.MissingValue;
            break :blk try cb.primSubtract(lhs, rhs);
        },
        .div => blk: {
            const lhs = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const rhs = valueFor(values, inputs[1]) orelse return error.MissingValue;
            break :blk try cb.primDivide(lhs, rhs);
        },
        .less_than => blk: {
            const lhs = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const rhs = valueFor(values, inputs[1]) orelse return error.MissingValue;
            break :blk try cb.primLessThan(lhs, rhs);
        },
        .where_select => blk: {
            const cond = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const on_true = valueFor(values, inputs[1]) orelse return error.MissingValue;
            const on_false = valueFor(values, inputs[2]) orelse return error.MissingValue;
            break :blk try cb.primWhereSelect(cond, on_true, on_false);
        },
        .neg => try cb.primNegate(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .sqrt => try cb.primSqrt(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .rsqrt => try cb.primRsqrt(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .exp => try cb.primExp(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .log => try cb.primLog(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .sin => try cb.primSin(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .cos => try cb.primCos(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .tanh => try cb.primTanh(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .erf => try cb.primErf(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .abs => try cb.primAbs(valueFor(values, inputs[0]) orelse return error.MissingValue),
        .reshape => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            var dims_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const dims = try fillShapeDims(attrs.new_shape, &dims_buf);
            break :blk try cb.primReshape(input, dims);
        },
        .slice => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
            const rank = @as(usize, attrs.num_axes);
            if (rank > ml.graph.shape.max_rank) return error.UnsupportedShape;
            var starts: [ml.graph.shape.max_rank]i64 = undefined;
            var limits: [ml.graph.shape.max_rank]i64 = undefined;
            var strides: [ml.graph.shape.max_rank]i64 = undefined;
            for (0..rank) |axis| {
                starts[axis] = attrs.starts[axis];
                limits[axis] = attrs.limits[axis];
                strides[axis] = attrs.strides[axis];
            }
            break :blk try cb.primSlice(input, starts[0..rank], limits[0..rank], strides[0..rank], in_shape);
        },
        .concat_prim => |attrs| blk: {
            const lhs = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const rhs = valueFor(values, inputs[1]) orelse return error.MissingValue;
            var lhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            var rhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const lhs_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &lhs_shape_buf);
            const rhs_shape = try fillShapeDims(graph.node(inputs[1]).output_shape, &rhs_shape_buf);
            break :blk try cb.primConcatPrim(lhs, rhs, attrs.axis, lhs_shape, rhs_shape);
        },
        .broadcast_in_dim => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
            var target_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const target_shape = try fillShapeDims(attrs.target_shape, &target_shape_buf);
            break :blk try cb.primBroadcastInDim(input, target_shape, attrs.broadcast_axes[0..attrs.num_axes], in_shape);
        },
        .reduce_sum => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
            break :blk try cb.primReduceSum(input, attrs.axes[0..attrs.num_axes], in_shape);
        },
        .reduce_max => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
            break :blk try cb.primReduceMax(input, attrs.axes[0..attrs.num_axes], in_shape);
        },
        .reduce_mean => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
            break :blk try cb.primReduceMean(input, attrs.axes[0..attrs.num_axes], in_shape);
        },
        .fused_softmax => |attrs| try cb.primSoftmax(valueFor(values, inputs[0]) orelse return error.MissingValue, attrs.dim),
        .fused_log_softmax => |attrs| try cb.primLogSoftmax(valueFor(values, inputs[0]) orelse return error.MissingValue, attrs.dim),
        .fused_linear => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const weight = valueFor(values, inputs[1]) orelse return error.MissingValue;
            const bias = valueFor(values, inputs[2]) orelse return error.MissingValue;
            break :blk try cb.linearWithPlan(input, weight, bias, attrs.rows, attrs.in_dim, attrs.out_dim, op_plan);
        },
        .fused_linear_no_bias => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const weight = valueFor(values, inputs[1]) orelse return error.MissingValue;
            if (attrs.num_projections != 0 and op_plan == null) {
                break :blk try cb.linearNoBiasGrouped(
                    input,
                    weight,
                    attrs.rows,
                    attrs.in_dim,
                    attrs.out_dim,
                    attrs.projection_out_dims[0..attrs.num_projections],
                    attrs.num_projections,
                );
            }
            break :blk try cb.linearNoBiasWithPlan(input, weight, attrs.rows, attrs.in_dim, attrs.out_dim, op_plan);
        },
        .fused_linear_no_bias_pair => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const weight_a = valueFor(values, inputs[1]) orelse return error.MissingValue;
            const weight_b = valueFor(values, inputs[2]) orelse return error.MissingValue;
            if (pair_second.* != null) return error.InvalidPartitionPlan;
            if (op_plan) |plan| {
                const first = try cb.linearNoBiasWithPlan(input, weight_a, attrs.rows, attrs.in_dim, attrs.out_dim, plan);
                errdefer cb.free(first);
                const second = try cb.linearNoBiasWithPlan(input, weight_b, attrs.rows, attrs.in_dim, attrs.out_dim, plan);
                pair_second.* = second;
                break :blk first;
            }
            const result = try cb.linearNoBiasPair(input, weight_a, weight_b, attrs.rows, attrs.in_dim, attrs.out_dim);
            pair_second.* = result.second;
            break :blk result.first;
        },
        .fused_to_float32 => blk: {
            if (pair_second.*) |second| {
                pair_second.* = null;
                break :blk second;
            }
            break :blk valueFor(values, inputs[0]) orelse return error.MissingValue;
        },
        .fused_layer_norm => |attrs| blk: {
            const output_elems = tensorElementCount(node.output_shape) orelse return error.UnsupportedShape;
            if (attrs.dim == 0 or output_elems % attrs.dim != 0) return error.UnsupportedShape;
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const weight = valueFor(values, inputs[1]) orelse return error.MissingValue;
            const bias = valueFor(values, inputs[2]) orelse return error.MissingValue;
            break :blk try cb.layerNorm(input, weight, bias, attrs.dim, attrs.eps);
        },
        .fused_rms_norm => |attrs| blk: {
            const output_elems = tensorElementCount(node.output_shape) orelse return error.UnsupportedShape;
            if (attrs.dim == 0 or output_elems % attrs.dim != 0) return error.UnsupportedShape;
            const input = valueFor(values, inputs[0]) orelse return error.MissingValue;
            const weight = valueFor(values, inputs[1]) orelse return error.MissingValue;
            break :blk try cb.rmsNorm(input, weight, attrs.dim, attrs.eps);
        },
        .fused_gelu => try cb.gelu(valueFor(values, inputs[0]) orelse return error.MissingValue),
        else => error.UnsupportedPrimitiveOp,
    };
}

fn tensorElementCount(shape: ml.graph.Shape) ?usize {
    const rank = shape.rank();
    var total: usize = 1;
    for (0..rank) |axis| {
        const dim = shape.dim(@intCast(axis));
        if (dim < 0) return null;
        total *= @intCast(dim);
    }
    return total;
}

fn webGpuLinearAttrsHaveSupportedShape(attrs: ml.graph.node.LinearAttrs) bool {
    if (attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0) return false;
    if (attrs.num_projections == 0) return true;
    if (attrs.num_projections > attrs.projection_out_dims.len) return false;
    var total: u64 = 0;
    for (attrs.projection_out_dims[0..attrs.num_projections]) |dim| {
        if (dim == 0) return false;
        total = std.math.add(u64, total, dim) catch return false;
    }
    return total == attrs.out_dim;
}

fn fillShapeDims(shape: ml.graph.Shape, buf: *[ml.graph.shape.max_rank]i64) ![]const i64 {
    const rank = shape.rank();
    if (rank > buf.len) return error.UnsupportedShape;
    for (0..rank) |axis| buf[axis] = shape.dim(@intCast(axis));
    return buf[0..rank];
}

fn valueFor(values: []?CT, node_id: NodeId) ?CT {
    if (node_id == null_node or node_id >= values.len) return null;
    return values[@intCast(node_id)];
}

fn partitionIndexForNodes(
    buffer_plan: *const buffer_plan_mod.BufferPlan,
    node_ids: []const NodeId,
) !u32 {
    if (node_ids.len == 0) return error.InvalidPartitionPlan;
    const first = buffer_plan.slotForNode(node_ids[0]) orelse return error.InvalidBufferPlan;
    const partition_index = first.partition_index;
    for (node_ids) |node_id| {
        const slot = buffer_plan.slotForNode(node_id) orelse return error.InvalidBufferPlan;
        if (slot.partition_index != partition_index) return error.InvalidPartitionPlan;
    }
    return partition_index;
}

fn validatePartitionView(
    view: buffer_plan_mod.PartitionBufferView,
    node_ids: []const NodeId,
) !void {
    if (view.backend != .webgpu) return error.InvalidPartitionPlan;
    for (node_ids) |node_id| {
        var found = false;
        for (view.slots) |slot_view| {
            if (slot_view.slot.node_id == node_id and slot_view.roles.local) {
                found = true;
                break;
            }
        }
        if (!found) return error.InvalidBufferPlan;
    }
}

fn buildWebGpuGraphPlan(
    allocator: std.mem.Allocator,
    buffer_plan: *const buffer_plan_mod.BufferPlan,
    view: buffer_plan_mod.PartitionBufferView,
) !WebGpuPartitionGraphPlan {
    var mappings = std.ArrayListUnmanaged(WebGpuGraphPlanAllocation).empty;
    errdefer mappings.deinit(allocator);

    for (view.slots) |slot_view| {
        if (!slot_view.roles.local) continue;
        const allocation_id = slot_view.slot.allocation;
        if (allocation_id == buffer_plan_mod.invalid_allocation) continue;
        const allocation = buffer_plan.allocations[@intCast(allocation_id)];
        if (allocation.kind != .tensor) continue;
        try addGraphPlanAllocation(allocator, &mappings, allocation_id, allocation.byte_size);
    }

    const slots = try allocator.alloc(GraphPlanSlot, mappings.items.len);
    errdefer allocator.free(slots);
    for (mappings.items, slots) |mapping, *slot| {
        slot.* = .{ .slot = mapping.graph_slot, .bytes = mapping.bytes };
    }

    return .{
        .slots = slots,
        .allocations = try mappings.toOwnedSlice(allocator),
    };
}

fn addGraphPlanAllocation(
    allocator: std.mem.Allocator,
    mappings: *std.ArrayListUnmanaged(WebGpuGraphPlanAllocation),
    allocation_id: buffer_plan_mod.AllocationId,
    bytes_u64: u64,
) !void {
    const bytes: usize = std.math.cast(usize, bytes_u64) orelse return error.OutOfMemory;
    if (bytes == 0) return;
    for (mappings.items) |*mapping| {
        if (mapping.allocation != allocation_id) continue;
        mapping.bytes = @max(mapping.bytes, bytes);
        return;
    }
    if (mappings.items.len >= max_graph_plan_slots) {
        var smallest_idx: usize = 0;
        for (mappings.items[1..], 1..) |mapping, idx| {
            if (mapping.bytes < mappings.items[smallest_idx].bytes) smallest_idx = idx;
        }
        if (bytes <= mappings.items[smallest_idx].bytes) return;
        mappings.items[smallest_idx] = .{
            .allocation = allocation_id,
            .graph_slot = smallest_idx,
            .bytes = bytes,
        };
        return;
    }
    const graph_slot = mappings.items.len;
    try mappings.append(allocator, .{
        .allocation = allocation_id,
        .graph_slot = graph_slot,
        .bytes = bytes,
    });
}

test "webgpu graph plan reserves unique local tensor allocations" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    const bias = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    const sum = try b.add(x, bias);
    const out = try b.gelu(sum);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();
    var partition_view = try buffer_plan.partitionView(allocator, &partition_plan, 0);
    defer partition_view.deinit(allocator);
    var graph_plan = try buildWebGpuGraphPlan(allocator, &buffer_plan, partition_view);
    defer graph_plan.deinit(allocator);

    try std.testing.expect(graph_plan.slots.len > 0);
    try std.testing.expect(graph_plan.slots.len <= max_graph_plan_slots);
    for (graph_plan.slots) |slot| {
        try std.testing.expect(slot.slot < max_graph_plan_slots);
        try std.testing.expect(slot.bytes >= 4 * 4 * @sizeOf(f32));
    }
}

test "webgpu command encoder classifies claimed shader families" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w = try b.parameter("w", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const scale = try b.parameter("scale", ml.graph.Shape.init(.f32, &.{hidden}));
    const bias = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{hidden}));
    const y = try b.parameter("y", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const scalar = try b.scalarConst(.f32, 0.0);

    const add = try b.add(x, y);
    const fused_add = try b.elemAdd(x, y);
    const mul = try b.mul(x, y);
    const fused_mul = try b.elemMultiply(x, y);
    const sub = try b.sub(x, y);
    const div = try b.div(x, y);
    const less_than = try g.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ x, scalar, null_node, null_node },
        .num_inputs = 2,
    });
    const where_select = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ less_than, scalar, x, null_node },
        .num_inputs = 3,
    });
    const neg = try b.neg(x);
    const sqrt = try b.sqrt(x);
    const rsqrt = try b.rsqrt(x);
    const exp = try b.expOp(x);
    const log = try b.logOp(x);
    const sin = try b.sinOp(x);
    const cos = try b.cosOp(x);
    const tanh = try b.tanhOp(x);
    const erf = try b.erfOp(x);
    const abs = try b.absOp(x);
    const reshape = try b.reshape(x, ml.graph.Shape.init(.f32, &.{ hidden, rows }));
    const slice = try b.sliceLastDim(x, 1, 3);
    const concat = try b.concat(x, y, 1);
    var broadcast_attrs = ml.graph.node.BroadcastAttrs{
        .target_shape = ml.graph.Shape.init(.f32, &.{ rows, hidden }),
        .num_axes = 1,
    };
    broadcast_attrs.broadcast_axes[0] = 1;
    const broadcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = broadcast_attrs },
        .output_shape = broadcast_attrs.target_shape,
        .inputs = .{ bias, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const reduce_sum = try b.reduceSum(x, &.{1});
    const reduce_max = try b.reduceMax(x, &.{1});
    const reduce_mean = try b.reduceMean(x, &.{1});
    const softmax = try b.softmax(x);
    const log_softmax = try b.logSoftmax(x);
    const linear_bias = try b.linear(x, w, bias, rows, hidden, hidden);
    const lin = try b.linearNoBias(x, w, rows, hidden, hidden);
    const lin_pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{ .rows = @intCast(rows), .in_dim = @intCast(hidden), .out_dim = @intCast(hidden) } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(hidden) }),
        .inputs = .{ x, w, w, null_node },
        .num_inputs = 3,
    });
    const pair_second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = g.node(lin_pair).output_shape,
        .inputs = .{ lin_pair, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const rms = try b.rmsNorm(lin, scale, hidden, 1e-5);
    const layer = try b.layerNorm(lin, scale, bias, hidden, 1e-5);
    const gelu = try b.gelu(rms);
    const out = try b.reduceSum(gelu, &.{1});
    try g.markOutput(out);

    inline for (.{
        add,
        fused_add,
        mul,
        fused_mul,
        sub,
        div,
        less_than,
    }) |node_id| try expectWebGpuEncodedCommand(&g, node_id, .elementwise_binary);
    try expectWebGpuEncodedCommand(&g, where_select, .where_select);
    inline for (.{
        neg,
        sqrt,
        rsqrt,
        exp,
        log,
        sin,
        cos,
        tanh,
        erf,
        abs,
    }) |node_id| try expectWebGpuEncodedCommand(&g, node_id, .unary);
    try expectWebGpuEncodedCommand(&g, reshape, .reshape_view);
    try expectWebGpuEncodedCommand(&g, slice, .slice_copy);
    try expectWebGpuEncodedCommand(&g, concat, .concat_copy);
    try expectWebGpuEncodedCommand(&g, broadcast, .broadcast);
    inline for (.{
        reduce_sum,
        reduce_max,
        reduce_mean,
        out,
    }) |node_id| try expectWebGpuEncodedCommand(&g, node_id, .reduction);
    try expectWebGpuEncodedCommand(&g, softmax, .softmax);
    try expectWebGpuEncodedCommand(&g, log_softmax, .log_softmax);
    try expectWebGpuEncodedCommand(&g, linear_bias, .matmul_transb_bias);
    try expectWebGpuEncodedCommand(&g, lin, .matmul_transb);
    try expectWebGpuEncodedCommand(&g, lin_pair, .matmul_transb);
    try expectWebGpuEncodedCommand(&g, pair_second, .pair_second);
    try expectWebGpuEncodedCommand(&g, rms, .rms_norm);
    try expectWebGpuEncodedCommand(&g, layer, .layer_norm);
    try expectWebGpuEncodedCommand(&g, gelu, .gelu);

    const from_float = try g.addNode(.{
        .op = .{ .fused_from_float32 = {} },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ null_node, null_node, null_node, null_node },
        .num_inputs = 0,
    });
    try std.testing.expect(webgpu_capabilities.supportsWebGpuGraph(g.node(from_float).op));
    try std.testing.expect(encodeWebGpuCommandNode(&g, from_float, g.node(from_float)) == null);
}

test "webgpu partition executor delegates through partition executor path" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const bias = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const out = try b.add(x, bias);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(&.{ 1.0, 2.0, 3.0, 4.0 }, &.{ 2, 2 });
    defer if (values[@intCast(x)]) |ct| cb.free(ct);
    values[@intCast(bias)] = try cb.fromFloat32Shape(&.{ 0.5, 1.5, -1.0, 2.0 }, &.{ 2, 2 });
    defer if (values[@intCast(bias)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = values[@intCast(x)].? },
                .{ .node_id = bias, .value = values[@intCast(bias)].? },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .stats = &exec_stats,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqualSlices(f32, &.{ 1.5, 3.5, 2.0, 6.0 }, raw);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
}

test "webgpu partition executor direct softmax reduction commands" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const probs = try b.softmax(x);
    const out = try b.reduceSum(probs, &.{1});
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(&.{ 0.0, 1.0, 2.0, 3.0, 1.0, 3.0, 5.0, 7.0 }, &.{ 2, 4 });
    defer if (values[@intCast(x)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{.{ .node_id = x, .value = values[@intCast(x)].? }},
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .stats = &exec_stats,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), raw[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), raw[1], 1e-5);
    try std.testing.expectEqual(@as(u64, 2), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
}

test "webgpu partition executor direct mask select commands" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{6}));
    const threshold = try b.scalarConst(.f32, 0.0);
    const neg_one = try b.scalarConst(.f32, -1.0);
    const pos_one = try b.scalarConst(.f32, 1.0);
    const cond = try g.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ x, threshold, null_node, null_node },
        .num_inputs = 2,
    });
    const out = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ cond, neg_one, pos_one, null_node },
        .num_inputs = 3,
    });
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(&.{ -3.0, -0.5, 0.0, 0.25, 1.0, -2.0 }, &.{6});
    defer if (values[@intCast(x)]) |ct| cb.free(ct);
    values[@intCast(threshold)] = try cb.fromFloat32Shape(&.{0.0}, &.{});
    defer if (values[@intCast(threshold)]) |ct| cb.free(ct);
    values[@intCast(neg_one)] = try cb.fromFloat32Shape(&.{-1.0}, &.{});
    defer if (values[@intCast(neg_one)]) |ct| cb.free(ct);
    values[@intCast(pos_one)] = try cb.fromFloat32Shape(&.{1.0}, &.{});
    defer if (values[@intCast(pos_one)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = values[@intCast(x)].? },
                .{ .node_id = threshold, .value = values[@intCast(threshold)].? },
                .{ .node_id = neg_one, .value = values[@intCast(neg_one)].? },
                .{ .node_id = pos_one, .value = values[@intCast(pos_one)].? },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .stats = &exec_stats,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqualSlices(f32, &.{ -1.0, -1.0, 1.0, 1.0, 1.0, -1.0 }, raw);
    try std.testing.expectEqual(@as(u64, 2), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
}

test "webgpu partition executor direct view movement commands" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 6 }));
    const left = try b.sliceLastDim(x, 0, 3);
    const right = try b.sliceLastDim(x, 3, 6);
    const joined = try b.concat(left, right, 1);
    const reshaped = try b.reshape(joined, ml.graph.Shape.init(.f32, &.{ 3, 4 }));
    const out = try b.reduceSum(reshaped, &.{1});
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(&.{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0 }, &.{ 2, 6 });
    defer if (values[@intCast(x)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{.{ .node_id = x, .value = values[@intCast(x)].? }},
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .stats = &exec_stats,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqualSlices(f32, &.{ 10.0, 26.0, 42.0 }, raw);
    try std.testing.expectEqual(@as(u64, 5), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
}

test "webgpu partition executor direct transformer dense commands" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 4;

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w1 = try b.parameter("w1", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const rms_weight = try b.parameter("rms_weight", ml.graph.Shape.init(.f32, &.{hidden}));
    const w2 = try b.parameter("w2", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const bias = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{hidden}));
    const gamma = try b.parameter("gamma", ml.graph.Shape.init(.f32, &.{hidden}));
    const beta = try b.parameter("beta", ml.graph.Shape.init(.f32, &.{hidden}));
    const lin1 = try b.linearNoBias(x, w1, rows, hidden, hidden);
    const rms = try b.rmsNorm(lin1, rms_weight, hidden, 1e-5);
    const gelu = try b.gelu(rms);
    const lin2 = try b.linear(gelu, w2, bias, rows, hidden, hidden);
    const out = try b.layerNorm(lin2, gamma, beta, hidden, 1e-5);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const x_data = [_]f32{ 0.5, -1.0, 2.0, 3.0, -2.0, 0.25, 1.5, -0.75 };
    const w1_data = [_]f32{
        1.0,  0.0,   0.5,  -0.25,
        -0.5, 1.0,   0.0,  0.25,
        0.25, -0.75, 1.0,  0.0,
        0.0,  0.5,   -0.5, 1.0,
    };
    const rms_weight_data = [_]f32{ 1.0, 0.9, 1.1, 0.8 };
    const w2_data = [_]f32{
        0.75,  -0.25, 0.5,  0.0,
        0.0,   0.5,   -0.5, 1.0,
        -0.25, 0.75,  0.25, -0.5,
        1.0,   0.25,  0.0,  0.5,
    };
    const bias_data = [_]f32{ 0.1, -0.2, 0.3, -0.4 };
    const gamma_data = [_]f32{ 1.0, 0.75, 1.25, 0.5 };
    const beta_data = [_]f32{ 0.0, 0.1, -0.1, 0.2 };

    const e_x = try cb.fromFloat32Shape(&x_data, &.{ rows, hidden });
    defer cb.free(e_x);
    const e_w1 = try cb.fromFloat32Shape(&w1_data, &.{ hidden, hidden });
    defer cb.free(e_w1);
    const e_rms_weight = try cb.fromFloat32Shape(&rms_weight_data, &.{hidden});
    defer cb.free(e_rms_weight);
    const e_w2 = try cb.fromFloat32Shape(&w2_data, &.{ hidden, hidden });
    defer cb.free(e_w2);
    const e_bias = try cb.fromFloat32Shape(&bias_data, &.{hidden});
    defer cb.free(e_bias);
    const e_gamma = try cb.fromFloat32Shape(&gamma_data, &.{hidden});
    defer cb.free(e_gamma);
    const e_beta = try cb.fromFloat32Shape(&beta_data, &.{hidden});
    defer cb.free(e_beta);

    const e_lin1 = try cb.linearNoBias(e_x, e_w1, rows, hidden, hidden);
    defer cb.free(e_lin1);
    const e_rms = try cb.rmsNorm(e_lin1, e_rms_weight, hidden, 1e-5);
    defer cb.free(e_rms);
    const e_gelu = try cb.gelu(e_rms);
    defer cb.free(e_gelu);
    const e_lin2 = try cb.linear(e_gelu, e_w2, e_bias, rows, hidden, hidden);
    defer cb.free(e_lin2);
    const e_out = try cb.layerNorm(e_lin2, e_gamma, e_beta, hidden, 1e-5);
    defer cb.free(e_out);
    const expected = try cb.toFloat32(e_out, allocator);
    defer allocator.free(expected);

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(&x_data, &.{ rows, hidden });
    defer if (values[@intCast(x)]) |ct| cb.free(ct);
    values[@intCast(w1)] = try cb.fromFloat32Shape(&w1_data, &.{ hidden, hidden });
    defer if (values[@intCast(w1)]) |ct| cb.free(ct);
    values[@intCast(rms_weight)] = try cb.fromFloat32Shape(&rms_weight_data, &.{hidden});
    defer if (values[@intCast(rms_weight)]) |ct| cb.free(ct);
    values[@intCast(w2)] = try cb.fromFloat32Shape(&w2_data, &.{ hidden, hidden });
    defer if (values[@intCast(w2)]) |ct| cb.free(ct);
    values[@intCast(bias)] = try cb.fromFloat32Shape(&bias_data, &.{hidden});
    defer if (values[@intCast(bias)]) |ct| cb.free(ct);
    values[@intCast(gamma)] = try cb.fromFloat32Shape(&gamma_data, &.{hidden});
    defer if (values[@intCast(gamma)]) |ct| cb.free(ct);
    values[@intCast(beta)] = try cb.fromFloat32Shape(&beta_data, &.{hidden});
    defer if (values[@intCast(beta)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = values[@intCast(x)].? },
                .{ .node_id = w1, .value = values[@intCast(w1)].? },
                .{ .node_id = rms_weight, .value = values[@intCast(rms_weight)].? },
                .{ .node_id = w2, .value = values[@intCast(w2)].? },
                .{ .node_id = bias, .value = values[@intCast(bias)].? },
                .{ .node_id = gamma, .value = values[@intCast(gamma)].? },
                .{ .node_id = beta, .value = values[@intCast(beta)].? },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .stats = &exec_stats,
    });

    const actual = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(actual);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-5);
    }
    try std.testing.expectEqual(@as(u64, 5), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
}

test "webgpu partition executor direct planned grouped transformer commands" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 9;
    const hidden: usize = 32;
    const qkv_out: usize = 64;

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const qkv_w = try b.parameter("qkv_w", ml.graph.Shape.init(.f32, &.{ qkv_out, hidden }));
    const out_w = try b.parameter("out_w", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const qkv = try g.addNode(.{
        .op = .{ .fused_linear_no_bias = .{
            .rows = rows,
            .in_dim = hidden,
            .out_dim = qkv_out,
            .projection_out_dims = .{ 32, 16, 16, 0 },
            .num_projections = 3,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, qkv_out }),
        .inputs = .{ x, qkv_w, null_node, null_node },
        .num_inputs = 2,
    });
    const q = try b.sliceLastDim(qkv, 0, hidden);
    const probs = try b.softmax(q);
    const out = try b.linearNoBias(probs, out_w, rows, hidden, hidden);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &webgpu_capabilities.supportsWebGpuGraph, .decide = &webgpu_capabilities.decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll, .decide = &partition_mod.decideNative },
    };
    const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(descriptor_seeds);
    try partition_mod.seedAllUploadableResidency(descriptor_seeds, &g, .webgpu, 0);
    descriptor_seeds[@intCast(qkv_w)] = contracts.TensorDesc.packedQuant(g.node(qkv_w).output_shape, .q4_0);
    descriptor_seeds[@intCast(out_w)] = contracts.TensorDesc.packedQuant(g.node(out_w).output_shape, .q4_0);
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{ .tensor_descs = descriptor_seeds });
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mm, partition_plan.operatorPlanForNode(qkv).?.operator());
    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mm, partition_plan.operatorPlanForNode(out).?.operator());

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const x_data = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(x_data);
    for (x_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i % 17) + 1)) / 17.0 - 0.5;
    const qkv_w_data = try allocator.alloc(f32, qkv_out * hidden);
    defer allocator.free(qkv_w_data);
    for (qkv_w_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i % 23) + 1)) / 23.0 - 0.5;
    const out_w_data = try allocator.alloc(f32, hidden * hidden);
    defer allocator.free(out_w_data);
    for (out_w_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i % 19) + 1)) / 19.0 - 0.5;

    const e_x = try cb.fromFloat32Shape(x_data, &.{ rows, hidden });
    defer cb.free(e_x);
    const e_qkv_w = try cb.fromFloat32Shape(qkv_w_data, &.{ qkv_out, hidden });
    defer cb.free(e_qkv_w);
    const e_out_w = try cb.fromFloat32Shape(out_w_data, &.{ hidden, hidden });
    defer cb.free(e_out_w);
    const e_qkv = try cb.linearNoBiasGrouped(e_x, e_qkv_w, rows, hidden, qkv_out, &.{ 32, 16, 16 }, 3);
    defer cb.free(e_qkv);
    const e_q = try cb.primSlice(e_qkv, &.{ 0, 0 }, &.{ rows, hidden }, &.{ 1, 1 }, &.{ rows, qkv_out });
    defer cb.free(e_q);
    const e_probs = try cb.primSoftmax(e_q, hidden);
    defer cb.free(e_probs);
    const e_out = try cb.linearNoBias(e_probs, e_out_w, rows, hidden, hidden);
    defer cb.free(e_out);
    const expected = try cb.toFloat32(e_out, allocator);
    defer allocator.free(expected);

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(x_data, &.{ rows, hidden });
    defer if (values[@intCast(x)]) |ct| cb.free(ct);
    values[@intCast(qkv_w)] = try cb.fromFloat32Shape(qkv_w_data, &.{ qkv_out, hidden });
    defer if (values[@intCast(qkv_w)]) |ct| cb.free(ct);
    values[@intCast(out_w)] = try cb.fromFloat32Shape(out_w_data, &.{ hidden, hidden });
    defer if (values[@intCast(out_w)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    for (partition_plan.partitions) |part| {
        if (part.backend != .webgpu) continue;
        try exec.partitionExecutor().execute(values, value_device, part.node_ids, 0, .{
            .allocator = allocator,
            .graph = &g,
            .backend = &cb,
            .options = .{
                .runtime_inputs = &.{
                    .{ .node_id = x, .value = values[@intCast(x)].? },
                    .{ .node_id = qkv_w, .value = values[@intCast(qkv_w)].? },
                    .{ .node_id = out_w, .value = values[@intCast(out_w)].? },
                },
            },
            .reachable = reachable,
            .last_use = last_use,
            .partition_plan = &partition_plan,
            .buffer_plan = &buffer_plan,
            .stats = &exec_stats,
        });
    }

    const actual = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(actual);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-5);
    }
    try std.testing.expectEqual(@as(u64, 4), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 2), exec_stats.planned_operator_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(DeviceId, 0), value_device[@intCast(out)]);
}

test "webgpu partition executor direct planned pair projection commands" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 9;
    const hidden: usize = 32;
    const out_dim: usize = 16;

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w_a = try b.parameter("w_a", ml.graph.Shape.init(.f32, &.{ out_dim, hidden }));
    const w_b = try b.parameter("w_b", ml.graph.Shape.init(.f32, &.{ out_dim, hidden }));
    const pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{
            .rows = rows,
            .in_dim = hidden,
            .out_dim = out_dim,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, w_a, w_b, null_node },
        .num_inputs = 3,
    });
    const second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ pair, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const out = try b.add(pair, second);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &webgpu_capabilities.supportsWebGpuGraph, .decide = &webgpu_capabilities.decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll, .decide = &partition_mod.decideNative },
    };
    const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(descriptor_seeds);
    try partition_mod.seedAllUploadableResidency(descriptor_seeds, &g, .webgpu, 0);
    descriptor_seeds[@intCast(w_a)] = contracts.TensorDesc.packedQuant(g.node(w_a).output_shape, .q4_0);
    descriptor_seeds[@intCast(w_b)] = contracts.TensorDesc.packedQuant(g.node(w_b).output_shape, .q4_0);
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{ .tensor_descs = descriptor_seeds });
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mm, partition_plan.operatorPlanForNode(pair).?.operator());

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const x_data = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(x_data);
    for (x_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i % 13) + 1)) / 13.0 - 0.5;
    const w_a_data = try allocator.alloc(f32, out_dim * hidden);
    defer allocator.free(w_a_data);
    for (w_a_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i % 17) + 1)) / 17.0 - 0.5;
    const w_b_data = try allocator.alloc(f32, out_dim * hidden);
    defer allocator.free(w_b_data);
    for (w_b_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i % 19) + 1)) / 19.0 - 0.5;

    const e_x = try cb.fromFloat32Shape(x_data, &.{ rows, hidden });
    defer cb.free(e_x);
    const e_w_a = try cb.fromFloat32Shape(w_a_data, &.{ out_dim, hidden });
    defer cb.free(e_w_a);
    const e_w_b = try cb.fromFloat32Shape(w_b_data, &.{ out_dim, hidden });
    defer cb.free(e_w_b);
    const e_pair = try cb.linearNoBiasPair(e_x, e_w_a, e_w_b, rows, hidden, out_dim);
    defer cb.free(e_pair.first);
    defer cb.free(e_pair.second);
    const e_out = try cb.add(e_pair.first, e_pair.second);
    defer cb.free(e_out);
    const expected = try cb.toFloat32(e_out, allocator);
    defer allocator.free(expected);

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(x_data, &.{ rows, hidden });
    defer if (values[@intCast(x)]) |ct| cb.free(ct);
    values[@intCast(w_a)] = try cb.fromFloat32Shape(w_a_data, &.{ out_dim, hidden });
    defer if (values[@intCast(w_a)]) |ct| cb.free(ct);
    values[@intCast(w_b)] = try cb.fromFloat32Shape(w_b_data, &.{ out_dim, hidden });
    defer if (values[@intCast(w_b)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    for (partition_plan.partitions) |part| {
        if (part.backend != .webgpu) continue;
        try exec.partitionExecutor().execute(values, value_device, part.node_ids, 0, .{
            .allocator = allocator,
            .graph = &g,
            .backend = &cb,
            .options = .{
                .runtime_inputs = &.{
                    .{ .node_id = x, .value = values[@intCast(x)].? },
                    .{ .node_id = w_a, .value = values[@intCast(w_a)].? },
                    .{ .node_id = w_b, .value = values[@intCast(w_b)].? },
                },
            },
            .reachable = reachable,
            .last_use = last_use,
            .partition_plan = &partition_plan,
            .buffer_plan = &buffer_plan,
            .stats = &exec_stats,
        });
    }

    const actual = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(actual);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-5);
    }
    try std.testing.expectEqual(@as(u64, 3), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.planned_operator_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(DeviceId, 0), value_device[@intCast(out)]);
}

test "webgpu partition executor resident transformer block parity with planned quant projections" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 16;
    const hidden: usize = 32;

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const qkv_w = try b.parameter("qkv_w", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const rms_weight = try b.parameter("rms_weight", ml.graph.Shape.init(.f32, &.{hidden}));
    const proj_w = try b.parameter("proj_w", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const out_w = try b.parameter("out_w", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const qkv = try g.addNode(.{
        .op = .{ .fused_linear_no_bias = .{
            .rows = rows,
            .in_dim = hidden,
            .out_dim = hidden,
            .projection_out_dims = .{ 16, 8, 8, 0 },
            .num_projections = 3,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, hidden }),
        .inputs = .{ x, qkv_w, null_node, null_node },
        .num_inputs = 2,
    });
    const rms = try b.rmsNorm(qkv, rms_weight, hidden, 1e-5);
    const gelu = try b.gelu(rms);
    const proj = try b.linearNoBias(gelu, proj_w, rows, hidden, hidden);
    const resid = try b.add(proj, x);
    const probs = try b.softmax(resid);
    const out = try b.linearNoBias(probs, out_w, rows, hidden, hidden);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &webgpu_capabilities.supportsWebGpuGraph, .decide = &webgpu_capabilities.decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll, .decide = &partition_mod.decideNative },
    };
    const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(descriptor_seeds);
    try partition_mod.seedAllUploadableResidency(descriptor_seeds, &g, .webgpu, 0);
    descriptor_seeds[@intCast(qkv_w)] = contracts.TensorDesc.packedQuant(g.node(qkv_w).output_shape, .q4_0);
    descriptor_seeds[@intCast(proj_w)] = contracts.TensorDesc.packedQuant(g.node(proj_w).output_shape, .q4_0);
    descriptor_seeds[@intCast(out_w)] = contracts.TensorDesc.packedQuant(g.node(out_w).output_shape, .q4_0);
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{ .tensor_descs = descriptor_seeds });
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mm, partition_plan.operatorPlanForNode(qkv).?.operator());
    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mm, partition_plan.operatorPlanForNode(proj).?.operator());
    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mm, partition_plan.operatorPlanForNode(out).?.operator());
    try std.testing.expectEqual(contracts.BackendKind.webgpu, partition_plan.partitions[partition_plan.node_assignment[out]].backend);

    const native_compute = @import("../ops/native_compute.zig");
    var weight_store = native_compute.WeightStore{
        .resident_weights = .empty,
        .lazy_weights = .empty,
        .allocator = allocator,
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const x_data = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(x_data);
    for (x_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i * 17) % 37)) / 37.0 - 0.5;
    const qkv_w_data = try allocator.alloc(f32, hidden * hidden);
    defer allocator.free(qkv_w_data);
    for (qkv_w_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i * 13) % 41)) / 41.0 - 0.5;
    const rms_weight_data = try allocator.alloc(f32, hidden);
    defer allocator.free(rms_weight_data);
    for (rms_weight_data, 0..) |*v, i| v.* = 0.75 + @as(f32, @floatFromInt((i * 5) % 17)) / 32.0;
    const proj_w_data = try allocator.alloc(f32, hidden * hidden);
    defer allocator.free(proj_w_data);
    for (proj_w_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i * 19) % 43)) / 43.0 - 0.5;
    const out_w_data = try allocator.alloc(f32, hidden * hidden);
    defer allocator.free(out_w_data);
    for (out_w_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i * 23) % 47)) / 47.0 - 0.5;

    const e_x = try cb.fromFloat32Shape(x_data, &.{ rows, hidden });
    defer cb.free(e_x);
    const e_qkv_w = try cb.fromFloat32Shape(qkv_w_data, &.{ hidden, hidden });
    defer cb.free(e_qkv_w);
    const e_rms_weight = try cb.fromFloat32Shape(rms_weight_data, &.{hidden});
    defer cb.free(e_rms_weight);
    const e_proj_w = try cb.fromFloat32Shape(proj_w_data, &.{ hidden, hidden });
    defer cb.free(e_proj_w);
    const e_out_w = try cb.fromFloat32Shape(out_w_data, &.{ hidden, hidden });
    defer cb.free(e_out_w);
    const e_qkv = try cb.linearNoBiasGrouped(e_x, e_qkv_w, rows, hidden, hidden, &.{ 16, 8, 8 }, 3);
    defer cb.free(e_qkv);
    const e_rms = try cb.rmsNorm(e_qkv, e_rms_weight, hidden, 1e-5);
    defer cb.free(e_rms);
    const e_gelu = try cb.gelu(e_rms);
    defer cb.free(e_gelu);
    const e_proj = try cb.linearNoBias(e_gelu, e_proj_w, rows, hidden, hidden);
    defer cb.free(e_proj);
    const e_resid = try cb.add(e_proj, e_x);
    defer cb.free(e_resid);
    const e_probs = try cb.primSoftmax(e_resid, hidden);
    defer cb.free(e_probs);
    const e_out = try cb.linearNoBias(e_probs, e_out_w, rows, hidden, hidden);
    defer cb.free(e_out);
    const expected = try cb.toFloat32(e_out, allocator);
    defer allocator.free(expected);

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    values[@intCast(x)] = try cb.fromFloat32Shape(x_data, &.{ rows, hidden });
    defer if (values[@intCast(x)]) |ct| cb.free(ct);
    values[@intCast(qkv_w)] = try cb.fromFloat32Shape(qkv_w_data, &.{ hidden, hidden });
    defer if (values[@intCast(qkv_w)]) |ct| cb.free(ct);
    values[@intCast(rms_weight)] = try cb.fromFloat32Shape(rms_weight_data, &.{hidden});
    defer if (values[@intCast(rms_weight)]) |ct| cb.free(ct);
    values[@intCast(proj_w)] = try cb.fromFloat32Shape(proj_w_data, &.{ hidden, hidden });
    defer if (values[@intCast(proj_w)]) |ct| cb.free(ct);
    values[@intCast(out_w)] = try cb.fromFloat32Shape(out_w_data, &.{ hidden, hidden });
    defer if (values[@intCast(out_w)]) |ct| cb.free(ct);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = WebGpuPartitionExecutor.initBorrowed(allocator, &g, &cb);
    for (partition_plan.partitions) |part| {
        if (part.backend != .webgpu) continue;
        try exec.partitionExecutor().execute(values, value_device, part.node_ids, 0, .{
            .allocator = allocator,
            .graph = &g,
            .backend = &cb,
            .options = .{
                .runtime_inputs = &.{
                    .{ .node_id = x, .value = values[@intCast(x)].? },
                    .{ .node_id = qkv_w, .value = values[@intCast(qkv_w)].? },
                    .{ .node_id = rms_weight, .value = values[@intCast(rms_weight)].? },
                    .{ .node_id = proj_w, .value = values[@intCast(proj_w)].? },
                    .{ .node_id = out_w, .value = values[@intCast(out_w)].? },
                },
            },
            .reachable = reachable,
            .last_use = last_use,
            .partition_plan = &partition_plan,
            .buffer_plan = &buffer_plan,
            .stats = &exec_stats,
        });
    }

    const actual = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(actual);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-5);
    }
    try std.testing.expectEqual(@as(u64, 7), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 3), exec_stats.planned_operator_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(DeviceId, 0), value_device[@intCast(out)]);
}
