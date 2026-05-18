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
const platform = @import("antfly_platform");

const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;

const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const interpreter = @import("interpreter.zig");
const partition_mod = @import("partition.zig");
const device_mesh_mod = @import("device_mesh.zig");
const transpose_utils = @import("transpose_utils.zig");

const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const PartitionExecutor = partition_mod.PartitionExecutor;
const DeviceId = device_mesh_mod.DeviceId;
const native_compute = @import("../ops/native_compute.zig");

fn deinitEmptyNativeWeightStore(weight_store: *native_compute.WeightStore, allocator: std.mem.Allocator) void {
    native_compute.deinitPrefetchQueue(weight_store);
    weight_store.resident_weights.deinit(allocator);
    weight_store.lazy_weights.deinit(allocator);
}

pub const NativePartitionExecutor = struct {
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
    ) NativePartitionExecutor {
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
    ) !*NativePartitionExecutor {
        const exec = try allocator.create(NativePartitionExecutor);
        exec.* = .{
            .allocator = allocator,
            .graph = graph,
            .backend = backend,
            .owned = true,
        };
        exec.pe = .{ .ptr = exec, .vtable = &vtable };
        return exec;
    }

    pub fn partitionExecutor(self: *NativePartitionExecutor) *const PartitionExecutor {
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
        const self: *NativePartitionExecutor = @ptrCast(@alignCast(ctx));
        return self.execute(values, value_device, node_ids, device_id, exec_ctx);
    }

    fn deinitFn(ctx: *anyopaque) void {
        const self: *NativePartitionExecutor = @ptrCast(@alignCast(ctx));
        if (self.owned) self.allocator.destroy(self);
    }

    fn execute(
        self: *NativePartitionExecutor,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        device_id: DeviceId,
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) !void {
        const allocator = exec_ctx.allocator orelse self.allocator;
        const graph = exec_ctx.graph orelse self.graph;
        const cb = exec_ctx.backend orelse self.backend;
        const options = exec_ctx.options orelse interpreter.ExecuteOptions{
            .attention = if (exec_ctx.attention) |attention| attention.* else null,
            .embedding_ids = exec_ctx.embedding_ids,
        };
        const reachable = exec_ctx.reachable orelse return error.MissingPartitionExecutionContext;
        const last_use = exec_ctx.last_use orelse return error.MissingPartitionExecutionContext;

        var rt_map = std.AutoHashMapUnmanaged(NodeId, CT).empty;
        defer rt_map.deinit(allocator);
        var donated = std.AutoHashMapUnmanaged(NodeId, void).empty;
        defer donated.deinit(allocator);
        if (options.runtime_inputs) |inputs| {
            for (inputs, 0..) |ri, idx| {
                try rt_map.put(allocator, ri.node_id, ri.value);
                if (options.donate) |donate| {
                    if (idx < donate.len and donate[idx]) {
                        try donated.put(allocator, ri.node_id, {});
                    }
                }
            }
        }

        var exec_state = interpreter.ExecState{
            .attention_layer = if (exec_ctx.attention_layer) |layer| layer.* else 0,
            .options = options,
            .last_use = last_use,
            .pair_second = if (exec_ctx.pair_second) |pair| pair.* else null,
        };
        defer exec_state.freeMoeState();

        for (node_ids) |node_id| {
            const i: usize = @intCast(node_id);
            if (i >= reachable.len or !reachable[i]) continue;

            if (rt_map.get(node_id)) |rt_val| {
                const current_dev = value_device[i];
                if (current_dev != device_id) {
                    const mesh = exec_ctx.mesh orelse return error.DeviceNotFound;
                    const src_entry = mesh.device(current_dev) orelse return error.DeviceNotFound;
                    const transferred = try transferTensor(allocator, rt_val, src_entry.backend, cb);
                    values[i] = transferred;
                    if (exec_ctx.owned_runtime_transfers) |owned| {
                        try owned.put(allocator, node_id, {});
                    }
                    if (exec_ctx.stats) |stats| stats.runtime_input_transfers += 1;
                } else {
                    values[i] = rt_val;
                }
                value_device[i] = device_id;
                continue;
            }

            if (graph.node(node_id).op == .fused_from_float32) continue;

            if (try executeNativePlannedNode(allocator, graph, cb, values, node_id, exec_ctx.partition_plan, &exec_state)) |planned| {
                values[i] = planned;
                if (exec_ctx.stats) |stats| stats.planned_operator_dispatches += 1;
            } else {
                if (nativeGraphFallbackTraceEnabled()) {
                    std.debug.print(
                        "native_graph_fallback node_id={} op={s}\n",
                        .{ node_id, @tagName(std.meta.activeTag(graph.node(node_id).op)) },
                    );
                }
                if (nativeGraphRequireNoInterpreterFallbacks() and nativeGraphNodeRequiresPlannedExecution(graph, values, node_id)) {
                    return error.NativeGraphInterpreterFallback;
                }
                values[i] = try interpreter.executeNode(graph, cb, values, node_id, &exec_state);
                if (exec_ctx.stats) |stats| stats.interpreter_fallbacks += 1;
            }
            value_device[i] = device_id;

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

            const n = graph.node(node_id);
            for (n.getInputs()) |input_id| {
                if (input_id == null_node or input_id >= values.len) continue;
                const input_index: usize = @intCast(input_id);
                if (last_use[input_index] != i) continue;
                if (rt_map.contains(input_id) and
                    !donated.contains(input_id) and
                    !ownedRuntimeTransferContains(exec_ctx, input_id)) continue;
                if (values[input_index]) |ct| {
                    if (values[i]) |out_ct| {
                        if (ct == out_ct and interpreter.canKeepAliasedOutput(n.op)) {
                            values[input_index] = null;
                            continue;
                        }
                    }
                    const inp_dev = value_device[input_index];
                    if (exec_ctx.mesh) |mesh| {
                        if (mesh.device(inp_dev)) |entry| {
                            entry.backend.free(ct);
                        } else {
                            cb.free(ct);
                        }
                    } else {
                        cb.free(ct);
                    }
                    values[input_index] = null;
                }
            }
        }

        if (exec_ctx.attention_layer) |layer| layer.* = exec_state.attention_layer;
        if (exec_ctx.pair_second) |pair| pair.* = exec_state.pair_second;
    }
};

fn executeNativePlannedNode(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    partition_plan: ?*const partition_mod.PartitionPlan,
    exec_state: *interpreter.ExecState,
) !?CT {
    if (cb.kind() != .native) return null;
    const n = graph.node(node_id);
    const inputs = n.getInputs();
    return switch (n.op) {
        .parameter => blk: {
            const name = graph.parameterName(n);
            break :blk try cb.getWeight(name);
        },
        .constant => |attrs| blk: {
            const constant = try graph.constantDataAsF32(
                graph.allocator,
                n.output_shape.dtype,
                attrs.data_offset,
                attrs.data_len,
            );
            defer constant.deinit(graph.allocator);
            if (n.output_shape.rank() > 1) {
                var shape_buf: [8]i32 = undefined;
                const rank = n.output_shape.rank();
                for (0..rank) |ax| shape_buf[ax] = @intCast(n.output_shape.dim(@intCast(ax)));
                break :blk try cb.fromFloat32Shape(constant.data, shape_buf[0..rank]);
            }
            break :blk try cb.fromFloat32(constant.data);
        },
        .fused_linear => |attrs| blk: {
            const plan = nativeOperatorPlanForLinear(graph, values, node_id, partition_plan, attrs.rows, attrs.in_dim, attrs.out_dim) orelse {
                if (native_compute.nativeTensorHasQuantizedStorage(valueAt(values, inputs[1]))) return null;
                break :blk try cb.linear(
                    valueAt(values, inputs[0]),
                    valueAt(values, inputs[1]),
                    valueAt(values, inputs[2]),
                    attrs.rows,
                    attrs.in_dim,
                    attrs.out_dim,
                );
            };
            break :blk try cb.linearWithPlan(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                attrs.rows,
                attrs.in_dim,
                attrs.out_dim,
                plan,
            );
        },
        .fused_linear_no_bias => |attrs| blk: {
            const plan = nativeOperatorPlanForLinear(graph, values, node_id, partition_plan, attrs.rows, attrs.in_dim, attrs.out_dim) orelse {
                if (native_compute.nativeTensorHasQuantizedStorage(valueAt(values, inputs[1]))) return null;
                break :blk try cb.linearNoBias(valueAt(values, inputs[0]), valueAt(values, inputs[1]), attrs.rows, attrs.in_dim, attrs.out_dim);
            };
            if (attrs.num_projections > 0) {
                break :blk try cb.linearNoBiasGrouped(
                    valueAt(values, inputs[0]),
                    valueAt(values, inputs[1]),
                    attrs.rows,
                    attrs.in_dim,
                    attrs.out_dim,
                    attrs.projection_out_dims[0..attrs.num_projections],
                    attrs.num_projections,
                );
            }
            break :blk try cb.linearNoBiasWithPlan(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                attrs.rows,
                attrs.in_dim,
                attrs.out_dim,
                plan,
            );
        },
        .fused_linear_no_bias_pair => |attrs| blk: {
            _ = nativeOperatorPlanForLinear(graph, values, node_id, partition_plan, attrs.rows, attrs.in_dim, attrs.out_dim) orelse return null;
            const result = try cb.linearNoBiasPair(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                attrs.rows,
                attrs.in_dim,
                attrs.out_dim,
            );
            exec_state.pair_second = result.second;
            break :blk result.first;
        },
        .fused_embedding_lookup => |attrs| blk: {
            var owned_ids: ?[]i64 = null;
            defer if (owned_ids) |buf| allocator.free(buf);
            const ids = ids_blk: {
                if (graph.node(inputs[1]).op == .fused_from_float32) {
                    break :ids_blk exec_state.options.embedding_ids orelse return error.MissingRuntimeInput;
                }
                const raw = try cb.toFloat32(valueAt(values, inputs[1]), allocator);
                defer allocator.free(raw);
                const converted = try allocator.alloc(i64, raw.len);
                for (converted, raw) |*dst, value| dst.* = @intFromFloat(@round(value));
                owned_ids = converted;
                break :ids_blk converted;
            };
            break :blk try cb.embeddingLookup(valueAt(values, inputs[0]), ids, attrs.total, attrs.dim);
        },
        .fused_layer_norm => |attrs| blk: {
            break :blk try cb.layerNorm(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                attrs.dim,
                attrs.eps,
            );
        },
        .fused_gelu => try cb.gelu(valueAt(values, inputs[0])),
        .fused_relu => try cb.relu(valueAt(values, inputs[0])),
        .fused_quick_gelu => try cb.quickGelu(valueAt(values, inputs[0])),
        .fused_softmax => |attrs| try cb.primSoftmax(valueAt(values, inputs[0]), attrs.dim),
        .fused_conv2d => |attrs| blk: {
            const input_actual = cb.tensorShape(valueAt(values, inputs[0]), allocator) catch null;
            defer if (input_actual) |dims| allocator.free(dims);
            const input_declared = graph.node(inputs[0]).output_shape;
            break :blk try cb.conv2d(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                try positiveResolvedDim(input_actual, input_declared, 0),
                try positiveResolvedDim(input_actual, input_declared, 1),
                attrs.out_channels,
                try positiveResolvedDim(input_actual, input_declared, 2),
                try positiveResolvedDim(input_actual, input_declared, 3),
                attrs.kernel_h,
                attrs.kernel_w,
                attrs.stride_h,
                attrs.stride_w,
                attrs.padding_h,
                attrs.padding_w,
                attrs.groups,
            );
        },
        .fused_windowed_self_attention => |attrs| blk: {
            break :blk try executeNativeClapWindowAttention(
                allocator,
                cb,
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                valueAt(values, inputs[3]),
                attrs.batch,
                attrs.height,
                attrs.dim,
                attrs.num_heads,
                attrs.window_size,
            );
        },
        .fused_causal_self_attention => |attrs| blk: {
            break :blk try cb.causalSelfAttention(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                optionalValueAt(values, if (n.num_inputs > 3) inputs[3] else null_node),
                attrs.batch,
                attrs.seq_len,
                attrs.num_heads,
                attrs.head_dim,
            );
        },
        .fused_sdpa => |attrs| blk: {
            var batch = attrs.batch;
            var seq_len = attrs.seq_len;
            var num_heads = attrs.num_heads;
            var head_dim = attrs.head_dim;
            if (batch == 0 or seq_len == 0 or num_heads == 0 or head_dim == 0) {
                const actual = try cb.tensorShape(valueAt(values, inputs[0]), allocator);
                defer allocator.free(actual);
                if (actual.len == 4) {
                    if (batch == 0 and actual[0] > 0) batch = @intCast(actual[0]);
                    if (num_heads == 0 and actual[1] > 0) num_heads = @intCast(actual[1]);
                    if (seq_len == 0 and actual[2] > 0) seq_len = @intCast(actual[2]);
                    if (head_dim == 0 and actual[3] > 0) head_dim = @intCast(actual[3]);
                } else if (actual.len == 3) {
                    if (seq_len == 0 and actual[1] > 0) seq_len = @intCast(actual[1]);
                    if (head_dim == 0 and actual[2] > 0) head_dim = @intCast(actual[2]);
                    if (batch == 0 and num_heads > 0 and actual[0] > 0) batch = @intCast(@divFloor(actual[0], @as(i64, @intCast(num_heads))));
                }
            }
            var synthesized_mask: ?[]i64 = null;
            defer if (synthesized_mask) |mask| allocator.free(mask);
            const mask = mask_blk: {
                if (exec_state.options.sdpa_mask) |runtime_mask| {
                    if (attrs.seq_len == 0 and batch > 0 and runtime_mask.len % batch == 0) {
                        seq_len = @intCast(runtime_mask.len / batch);
                    }
                    break :mask_blk runtime_mask;
                }
                if (batch == 0) batch = 1;
                if (seq_len == 0) return error.MissingRuntimeInput;
                const full_mask = try allocator.alloc(i64, batch * seq_len);
                @memset(full_mask, 1);
                synthesized_mask = full_mask;
                break :mask_blk full_mask;
            };
            break :blk try cb.scaledDotProductAttention(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                mask,
                optionalValueAt(values, if (n.num_inputs > 3) inputs[3] else null_node),
                batch,
                seq_len,
                num_heads,
                head_dim,
            );
        },
        .reshape => |attrs| blk: {
            const rank = attrs.new_shape.rank();
            var dims: [8]i64 = undefined;
            for (0..rank) |d| dims[d] = attrs.new_shape.dim(@intCast(d));
            break :blk try cb.primReshape(valueAt(values, inputs[0]), dims[0..rank]);
        },
        .transpose => |attrs| blk: {
            var input_shape_buf: [8]i64 = undefined;
            const input_shape = fillShapeDims(graph, inputs[0], &input_shape_buf);
            var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
            const perm = transpose_utils.effectivePerm(attrs, graph.node(inputs[0]).output_shape.rank(), &perm_buf);
            break :blk try cb.primTranspose(valueAt(values, inputs[0]), perm, input_shape);
        },
        .broadcast_in_dim => |attrs| blk: {
            var input_shape_buf: [8]i64 = undefined;
            const input_shape = fillShapeDims(graph, inputs[0], &input_shape_buf);
            const rank = attrs.target_shape.rank();
            var target_dims: [8]i64 = undefined;
            for (0..rank) |d| target_dims[d] = attrs.target_shape.dim(@intCast(d));
            break :blk try cb.primBroadcastInDim(
                valueAt(values, inputs[0]),
                target_dims[0..rank],
                attrs.broadcast_axes[0..attrs.num_axes],
                input_shape,
            );
        },
        .convert_dtype => |attrs| blk: {
            const input = valueAt(values, inputs[0]);
            const in_dtype = graph.node(inputs[0]).output_shape.dtype;
            if (in_dtype == attrs.target) break :blk input;
            if (try cb.tryConvertDType(input, attrs.target)) |converted| break :blk converted;
            switch (attrs.target) {
                .i64, .i32, .u8, .bool_ => {
                    const data = try cb.toFloat32(input, allocator);
                    defer allocator.free(data);
                    for (data) |*v| v.* = @round(v.*);
                    if (cb.tensorShape(input, allocator)) |actual_shape| {
                        defer allocator.free(actual_shape);
                        if (actual_shape.len > 1) {
                            var dims: [8]i32 = undefined;
                            for (0..actual_shape.len) |d| dims[d] = @intCast(actual_shape[d]);
                            break :blk try cb.fromFloat32Shape(data, dims[0..actual_shape.len]);
                        }
                    } else |_| {}
                    break :blk try cb.fromFloat32(data);
                },
                else => break :blk input,
            }
        },
        .neg => try cb.primNegate(valueAt(values, inputs[0])),
        .sqrt => try cb.primSqrt(valueAt(values, inputs[0])),
        .rsqrt => try cb.primRsqrt(valueAt(values, inputs[0])),
        .exp => try cb.primExp(valueAt(values, inputs[0])),
        .log => try cb.primLog(valueAt(values, inputs[0])),
        .sin => try cb.primSin(valueAt(values, inputs[0])),
        .cos => try cb.primCos(valueAt(values, inputs[0])),
        .tanh => try cb.primTanh(valueAt(values, inputs[0])),
        .erf => try cb.primErf(valueAt(values, inputs[0])),
        .abs => try cb.primAbs(valueAt(values, inputs[0])),
        .add => try cb.add(valueAt(values, inputs[0]), valueAt(values, inputs[1])),
        .mul => try cb.multiply(valueAt(values, inputs[0]), valueAt(values, inputs[1])),
        .sub => try cb.primSubtract(valueAt(values, inputs[0]), valueAt(values, inputs[1])),
        .div => try cb.primDivide(valueAt(values, inputs[0]), valueAt(values, inputs[1])),
        .less_than => try cb.primLessThan(valueAt(values, inputs[0]), valueAt(values, inputs[1])),
        .reduce_sum => |attrs| blk: {
            var input_shape_buf: [8]i64 = undefined;
            const input_shape = fillShapeDims(graph, inputs[0], &input_shape_buf);
            break :blk try cb.primReduceSum(valueAt(values, inputs[0]), attrs.axes[0..attrs.num_axes], input_shape);
        },
        .reduce_mean => |attrs| blk: {
            var input_shape_buf: [8]i64 = undefined;
            const input_shape = fillShapeDims(graph, inputs[0], &input_shape_buf);
            break :blk try cb.primReduceMean(valueAt(values, inputs[0]), attrs.axes[0..attrs.num_axes], input_shape);
        },
        .reduce_max => |attrs| blk: {
            var input_shape_buf: [8]i64 = undefined;
            const input_shape = fillShapeDims(graph, inputs[0], &input_shape_buf);
            break :blk try cb.primReduceMax(valueAt(values, inputs[0]), attrs.axes[0..attrs.num_axes], input_shape);
        },
        .dot_general => |attrs| blk: {
            var lhs_shape_buf: [8]i64 = undefined;
            var rhs_shape_buf: [8]i64 = undefined;
            const lhs_shape = fillShapeDims(graph, inputs[0], &lhs_shape_buf);
            const rhs_shape = fillShapeDims(graph, inputs[1], &rhs_shape_buf);
            break :blk try cb.primDotGeneral(
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                lhs_shape,
                rhs_shape,
                attrs.lhs_contracting[0..attrs.num_contracting],
                attrs.rhs_contracting[0..attrs.num_contracting],
                attrs.lhs_batch[0..attrs.num_batch],
                attrs.rhs_batch[0..attrs.num_batch],
            );
        },
        .slice => |attrs| blk: {
            var input_shape_buf: [8]i64 = undefined;
            const input_shape = fillShapeDims(graph, inputs[0], &input_shape_buf);
            const rank = @as(usize, attrs.num_axes);
            var starts: [8]i64 = undefined;
            var limits: [8]i64 = undefined;
            var strides: [8]i64 = undefined;
            for (0..rank) |d| {
                starts[d] = attrs.starts[d];
                limits[d] = attrs.limits[d];
                strides[d] = attrs.strides[d];
            }
            break :blk try cb.primSlice(valueAt(values, inputs[0]), starts[0..rank], limits[0..rank], strides[0..rank], input_shape);
        },
        .concat_prim => |attrs| blk: {
            var a_shape_buf: [8]i64 = undefined;
            var b_shape_buf: [8]i64 = undefined;
            const a_shape = fillShapeDims(graph, inputs[0], &a_shape_buf);
            const b_shape = fillShapeDims(graph, inputs[1], &b_shape_buf);
            break :blk try cb.primConcatPrim(valueAt(values, inputs[0]), valueAt(values, inputs[1]), attrs.axis, a_shape, b_shape);
        },
        .scatter_add => |attrs| blk: {
            var dest_shape_buf: [8]i64 = undefined;
            var values_shape_buf: [8]i64 = undefined;
            var indices_shape_buf: [8]i64 = undefined;
            const dest_shape = fillShapeDims(graph, inputs[0], &dest_shape_buf);
            const values_shape = fillShapeDims(graph, inputs[1], &values_shape_buf);
            const indices_shape = fillShapeDims(graph, inputs[2], &indices_shape_buf);
            break :blk try executeNativeScatterAdd(
                allocator,
                cb,
                valueAt(values, inputs[0]),
                valueAt(values, inputs[1]),
                valueAt(values, inputs[2]),
                dest_shape,
                values_shape,
                indices_shape,
                attrs.axis,
            );
        },
        else => null,
    };
}

fn fillShapeDims(graph: *const Graph, node_id: NodeId, buf: *[8]i64) []const i64 {
    const shape = graph.node(node_id).output_shape;
    const rank = shape.rank();
    for (0..rank) |d| buf[d] = shape.dim(@intCast(d));
    return buf[0..rank];
}

fn positiveResolvedDim(actual: ?[]const i64, shape: ml.graph.Shape, axis: usize) !usize {
    if (actual) |dims| {
        if (axis < dims.len and dims[axis] > 0) {
            return std.math.cast(usize, dims[axis]) orelse return error.UnsupportedShape;
        }
    }
    return positiveShapeDim(shape, axis);
}

fn positiveShapeDim(shape: ml.graph.Shape, axis: usize) !usize {
    if (axis >= shape.rank()) return error.UnsupportedShape;
    const dim = shape.dim(@intCast(axis));
    if (dim <= 0) return error.UnsupportedShape;
    return std.math.cast(usize, dim) orelse return error.UnsupportedShape;
}

fn nativeOperatorPlanForLinear(
    graph: *const Graph,
    values: []?CT,
    node_id: NodeId,
    partition_plan: ?*const partition_mod.PartitionPlan,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) ?ops_mod.OperatorPlan {
    if (partition_plan) |plan| {
        if (plan.operatorPlanForNode(node_id)) |op_plan| return op_plan;
    }
    const n = graph.node(node_id);
    const inputs = n.getInputs();
    if (inputs.len < 2) return null;
    return native_compute.nativeQuantMatmulOperatorPlanForTensor(
        valueAt(values, inputs[1]),
        rows,
        in_dim,
        out_dim,
    );
}

fn valueAt(values: []?CT, node_id: NodeId) CT {
    return values[@intCast(node_id)].?;
}

fn optionalValueAt(values: []?CT, node_id: NodeId) ?CT {
    if (node_id == null_node) return null;
    return values[@intCast(node_id)];
}

fn executeNativeScatterAdd(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    dest: CT,
    update_values: CT,
    indices: CT,
    dest_shape: []const i64,
    values_shape: []const i64,
    indices_shape: []const i64,
    axis: u8,
) !CT {
    if (axis != 0) return error.UnsupportedPrimitiveOp;
    if (dest_shape.len != 2 or values_shape.len != 2) return error.UnsupportedPrimitiveOp;
    if (dest_shape[0] < 0 or dest_shape[1] <= 0 or values_shape[0] < 0 or values_shape[1] != dest_shape[1]) return error.UnsupportedShape;
    if (indices_shape.len == 0) return error.UnsupportedShape;

    const out_rows: usize = @intCast(dest_shape[0]);
    const value_rows: usize = @intCast(values_shape[0]);
    const dim: usize = @intCast(dest_shape[1]);

    const dest_data = try cb.toFloat32(dest, allocator);
    defer allocator.free(dest_data);
    const values_data = try cb.toFloat32(update_values, allocator);
    defer allocator.free(values_data);
    const index_data = try cb.toFloat32(indices, allocator);
    defer allocator.free(index_data);

    if (dest_data.len != out_rows * dim or values_data.len != value_rows * dim or index_data.len < value_rows) return error.ShapeMismatch;

    const output = try allocator.dupe(f32, dest_data);
    errdefer allocator.free(output);
    defer allocator.free(output);
    for (0..value_rows) |row_idx| {
        const out_row_f = @round(index_data[row_idx]);
        if (out_row_f < 0) return error.IndexOutOfBounds;
        const out_row: usize = @intFromFloat(out_row_f);
        if (out_row >= out_rows) return error.IndexOutOfBounds;
        const src = values_data[row_idx * dim ..][0..dim];
        const dst = output[out_row * dim ..][0..dim];
        for (src, dst) |v, *d| d.* += v;
    }

    const dims = [_]i32{ @intCast(out_rows), @intCast(dim) };
    return cb.fromFloat32Shape(output, &dims);
}

fn executeNativeClapWindowAttention(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    rel_bias_ct: CT,
    num_windows_u32: u32,
    window_area_u32: u32,
    dim_u32: u32,
    num_heads_u32: u32,
    window_size_u32: u32,
) !CT {
    const num_windows: usize = @intCast(num_windows_u32);
    const window_area: usize = @intCast(window_area_u32);
    const dim: usize = @intCast(dim_u32);
    const num_heads: usize = @intCast(num_heads_u32);
    const window_size: usize = @intCast(window_size_u32);
    const q = try cb.toFloat32(q_ct, allocator);
    defer allocator.free(q);
    const k = try cb.toFloat32(k_ct, allocator);
    defer allocator.free(k);
    const v = try cb.toFloat32(v_ct, allocator);
    defer allocator.free(v);
    const rel_bias = try cb.toFloat32(rel_bias_ct, allocator);
    defer allocator.free(rel_bias);
    const out = try clapWindowAttention(allocator, q, k, v, rel_bias, num_windows, window_area, dim, num_heads, window_size);
    defer allocator.free(out);
    return cb.fromFloat32Shape(out, &.{ @intCast(num_windows * window_area), @intCast(dim) });
}

fn clapWindowAttention(
    allocator: std.mem.Allocator,
    q: []const f32,
    k: []const f32,
    v: []const f32,
    rel_bias: []const f32,
    num_windows: usize,
    window_area: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
) ![]f32 {
    if (num_heads == 0 or dim % num_heads != 0) return error.InvalidInputShape;
    if (q.len != num_windows * window_area * dim or k.len != q.len or v.len != q.len) return error.InvalidInputShape;
    if (window_size == 0 or window_area != window_size * window_size) return error.InvalidInputShape;
    const out = try allocator.alloc(f32, q.len);
    errdefer allocator.free(out);
    const head_dim = dim / num_heads;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const scores = try allocator.alloc(f32, window_area);
    defer allocator.free(scores);
    const linalg_prim = @import("termite_linalg").primitives;
    for (0..num_windows) |win| {
        for (0..num_heads) |head| {
            for (0..window_area) |i| {
                const q_base = ((win * window_area + i) * dim) + head * head_dim;
                for (0..window_area) |j| {
                    const k_base = ((win * window_area + j) * dim) + head * head_dim;
                    const dot = linalg_prim.dotPtrs(q[q_base..].ptr, k[k_base..].ptr, head_dim);
                    scores[j] = dot * scale + clapRelativeBias(rel_bias, window_size, num_heads, head, i, j);
                }
                linalg_prim.softmaxRow(scores[0..window_area]);
                const out_base = ((win * window_area + i) * dim) + head * head_dim;
                @memset(out[out_base..][0..head_dim], 0);
                for (0..window_area) |j| {
                    const weight = scores[j];
                    if (weight == 0.0) continue;
                    const v_base = ((win * window_area + j) * dim) + head * head_dim;
                    linalg_prim.axpyPtrs(weight, v[v_base..].ptr, out[out_base..].ptr, head_dim);
                }
            }
        }
    }
    return out;
}

fn clapRelativeBias(table: []const f32, window_size: usize, num_heads: usize, head: usize, token_i: usize, token_j: usize) f32 {
    if (table.len == 0 or num_heads == 0) return 0.0;
    const yi = token_i / window_size;
    const xi = token_i % window_size;
    const yj = token_j / window_size;
    const xj = token_j % window_size;
    const rel_y: usize = @intCast(@as(isize, @intCast(yi)) - @as(isize, @intCast(yj)) + @as(isize, @intCast(window_size - 1)));
    const rel_x: usize = @intCast(@as(isize, @intCast(xi)) - @as(isize, @intCast(xj)) + @as(isize, @intCast(window_size - 1)));
    const side = 2 * window_size - 1;
    const idx = (rel_y * side + rel_x) * num_heads + head;
    return if (idx < table.len) table[idx] else 0.0;
}

fn nativeGraphRequireNoInterpreterFallbacks() bool {
    return platform.env.getenvBoolDefault("TERMITE_NATIVE_GRAPH_REQUIRE_NO_INTERPRETER_FALLBACK", false);
}

fn nativeGraphFallbackTraceEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_NATIVE_GRAPH_FALLBACK_TRACE", false);
}

fn nativeGraphNodeRequiresPlannedExecution(graph: *const Graph, values: []?CT, node_id: NodeId) bool {
    const n = graph.node(node_id);
    const inputs = n.getInputs();
    return switch (n.op) {
        .fused_linear,
        .fused_linear_no_bias,
        => inputs.len > 1 and native_compute.nativeTensorHasQuantizedStorage(valueAt(values, inputs[1])),
        .fused_linear_no_bias_pair => inputs.len > 2 and
            (native_compute.nativeTensorHasQuantizedStorage(valueAt(values, inputs[1])) or
                native_compute.nativeTensorHasQuantizedStorage(valueAt(values, inputs[2]))),
        else => false,
    };
}

fn ownedRuntimeTransferContains(
    exec_ctx: PartitionExecutor.ExecutionContext,
    node_id: NodeId,
) bool {
    const owned = exec_ctx.owned_runtime_transfers orelse return false;
    return owned.contains(node_id);
}

fn transferTensor(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
) !CT {
    const shape_i64 = try from.tensorShape(value, allocator);
    defer allocator.free(shape_i64);
    const shape_i32 = try tensorShapeI32(allocator, shape_i64);
    defer allocator.free(shape_i32);
    const f32_data = try from.toFloat32(value, allocator);
    defer allocator.free(f32_data);
    return to.fromFloat32Shape(f32_data, shape_i32);
}

fn tensorShapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(out);
    for (shape, 0..) |dim, i| {
        out[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return out;
}

test "native partition executor evaluates a partition through backend ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const input_data = [_]f32{ -1.0, 0.0, 1.0, 2.0 };
    const input_ct = try cb.fromFloat32(&input_data);
    defer cb.free(input_ct);
    values[@intCast(x)] = input_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec = NativePartitionExecutor.initBorrowed(allocator, &g, &cb);
    const pe = exec.partitionExecutor();
    try pe.execute(values, value_device, &.{ x, out }, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{.{ .node_id = x, .value = input_ct }},
        },
        .reachable = reachable,
        .last_use = last_use,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expect(raw[0] < 0.0);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), raw[1], 1e-6);
    try std.testing.expect(raw[2] > 0.8 and raw[2] < 0.9);
    try std.testing.expect(raw[3] > 1.9 and raw[3] < 2.0);
}

test "native partition executor executes linear through native cblas path" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const w = try b.parameter("linear.weight", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const out = try b.linearNoBias(x, w, 2, 3, 2);
    try g.markOutput(out);

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const w_data = [_]f32{ 1, 0, 0, 0, 1, 1 };
    const x_ct = try cb.fromFloat32Shape(&x_data, &.{ 2, 3 });
    defer cb.free(x_ct);
    const w_ct = try cb.fromFloat32Shape(&w_data, &.{ 2, 3 });
    defer cb.free(w_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(w)] = w_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec = NativePartitionExecutor.initBorrowed(allocator, &g, &cb);
    const pe = exec.partitionExecutor();
    try pe.execute(values, value_device, &.{ x, w, out }, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = w, .value = w_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqualSlices(f32, &.{ 1, 5, 4, 11 }, raw);
}

test "native partition executor owned lifecycle deinitializes cleanly" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const exec = try NativePartitionExecutor.create(allocator, &g, &cb);
    const pe = exec.partitionExecutor();
    pe.deinitExecutor();
}
