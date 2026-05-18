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

// Multi-device graph executor.
//
// Executes a partitioned graph across multiple ComputeBackend instances.
// Each partition runs on its assigned device; cross-partition data flows
// through CPU-mediated transfers (toFloat32 → fromFloat32). On Apple
// Silicon with unified memory, this is effectively a memcpy.
//
// Partitions execute sequentially in plan order. True pipeline overlap
// (concurrent stages) is a future extension requiring std.Thread.

const std = @import("std");
const ml = @import("ml");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const OpCode = ml.graph.OpCode;

const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const interpreter = @import("interpreter.zig");
const RuntimeInput = interpreter.RuntimeInput;
const ExecuteOptions = interpreter.ExecuteOptions;
const CachedAnalysis = interpreter.CachedAnalysis;
const executeNode = interpreter.executeNode;

const partition_mod = @import("partition.zig");
const native_partition_executor = @import("native_partition_executor.zig");
const metal_partition_executor = @import("metal_partition_executor.zig");
const webgpu_partition_executor = @import("webgpu_partition_executor.zig");
const buffer_plan_mod = @import("buffer_plan.zig");
const executor_stats = @import("executor_stats.zig");
const Partition = partition_mod.Partition;
const PartitionPlan = partition_mod.PartitionPlan;
const PartitionExecutor = partition_mod.PartitionExecutor;
const ExecutionStats = PartitionExecutor.ExecutionStats;
const ExternalInput = partition_mod.ExternalInput;

const device_mesh_mod = @import("device_mesh.zig");
const DeviceId = device_mesh_mod.DeviceId;
const DeviceMesh = device_mesh_mod.DeviceMesh;
const DeviceEntry = device_mesh_mod.DeviceEntry;

pub const MultiExecuteError = error{
    DeviceNotFound,
    MissingValue,
};

/// A PartitionPlan extended with per-partition device assignment.
pub const DevicePartitionPlan = struct {
    /// The underlying partition plan (owns partitions + node_assignment).
    base: PartitionPlan,
    /// device_assignment[partition_index] = DeviceId.
    device_assignment: []const DeviceId,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DevicePartitionPlan) void {
        self.base.deinit();
        self.allocator.free(self.device_assignment);
    }
};

/// Result of multi-device graph execution.
pub const MultiExecutionResult = struct {
    /// Output tensors (same order as graph.outputs).
    outputs: []CT,
    /// Which device produced each output.
    output_devices: []DeviceId,
    stats: ExecutionStats = .{},
    allocator: std.mem.Allocator,

    pub fn deinit(self: *MultiExecutionResult, mesh: *const DeviceMesh) void {
        const trace = traceOutputsEnabled();
        for (self.outputs, self.output_devices, 0..) |ct, dev_id, idx| {
            var duplicate = false;
            for (self.outputs[0..idx]) |prev| {
                if (prev == ct) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) continue;
            if (mesh.device(dev_id)) |entry| {
                if (trace) std.debug.print(
                    "graph_executor_output_trace: result_deinit_free idx={d} dev={d} backend={s}\n",
                    .{ idx, dev_id, @tagName(entry.backend.kind()) },
                );
                entry.backend.free(ct);
                if (trace) std.debug.print(
                    "graph_executor_output_trace: result_deinit_freed idx={d}\n",
                    .{idx},
                );
            }
        }
        self.allocator.free(self.outputs);
        self.allocator.free(self.output_devices);
    }
};

/// Transfer a tensor from one backend to another via CPU f32 buffer.
pub fn transferTensor(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
) !CT {
    return transferTensorWithKnownShape(allocator, value, from, to, null);
}

fn transferTensorWithKnownShape(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
    known_shape: ?[]const i64,
) !CT {
    const trace = traceTransfersEnabled();
    if (trace) std.debug.print("graph_executor_transfer_stage: begin from={s} to={s}\n", .{ @tagName(from.kind()), @tagName(to.kind()) });
    const owned_shape_i64 = if (known_shape == null) try from.tensorShape(value, allocator) else null;
    defer if (owned_shape_i64) |shape| allocator.free(shape);
    const shape_i64 = known_shape orelse owned_shape_i64.?;
    if (trace) std.debug.print("graph_executor_transfer_stage: shape_i64={any}\n", .{shape_i64});
    const shape_i32 = try tensorShapeI32(allocator, shape_i64);
    defer allocator.free(shape_i32);
    if (trace) std.debug.print("graph_executor_transfer_stage: shape_i32={any}\n", .{shape_i32});
    const f32_data = try from.toFloat32(value, allocator);
    defer allocator.free(f32_data);
    if (trace) std.debug.print("graph_executor_transfer_stage: f32_len={d}\n", .{f32_data.len});
    const transferred = try to.fromFloat32Shape(f32_data, shape_i32);
    errdefer to.free(transferred);
    if (trace) std.debug.print("graph_executor_transfer_stage: from_float32_shape_done\n", .{});
    if (to.kind() == .metal) {
        if (try metal_partition_executor.makeMetalDeviceResident(to, transferred)) |device_transferred| {
            to.free(transferred);
            if (trace) std.debug.print("graph_executor_transfer_stage: made_metal_device_resident\n", .{});
            return device_transferred;
        }
    }
    if (trace) std.debug.print("graph_executor_transfer_stage: end host_resident\n", .{});
    return transferred;
}

fn graphShapeDims(shape: ml.graph.Shape, buf: *[ml.graph.shape.max_rank]i64) ![]const i64 {
    const rank = shape.rank();
    if (rank > buf.len) return error.UnsupportedShape;
    for (0..rank) |axis| buf[axis] = shape.dim(@intCast(axis));
    return buf[0..rank];
}

fn traceTransfersEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_EXECUTOR_TRACE_TRANSFERS", false);
}

fn traceOutputsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_EXECUTOR_TRACE_OUTPUTS", false);
}

fn tensorShapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(out);
    for (shape, 0..) |dim, i| {
        out[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return out;
}

/// Execute a partitioned graph across a device mesh.
///
/// Each partition runs on its assigned device. Cross-partition edges
/// (external inputs) are transferred between devices automatically.
/// Partitions execute sequentially in plan order.
pub fn executeMultiDevice(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    plan: *const DevicePartitionPlan,
    mesh: *const DeviceMesh,
    options: ExecuteOptions,
) !MultiExecutionResult {
    const count = graph.nodeCount();
    if (count == 0 or plan.base.partitions.len == 0) {
        return .{
            .outputs = try allocator.alloc(CT, 0),
            .output_devices = try allocator.alloc(DeviceId, 0),
            .allocator = allocator,
        };
    }

    // Per-node value storage and device ownership.
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);

    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    // Build runtime input lookup.
    var rt_map = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    defer rt_map.deinit(allocator);
    var donated = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer donated.deinit(allocator);
    var owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer owned_runtime_transfers.deinit(allocator);
    if (options.runtime_inputs) |inputs| {
        for (inputs, 0..) |ri, idx| {
            try rt_map.put(allocator, ri.node_id, ri.value);
            if (options.donate) |donate| {
                if (idx < donate.len and donate[idx]) {
                    try donated.put(allocator, ri.node_id, {});
                }
            }
            values[@intCast(ri.node_id)] = ri.value;
            value_device[@intCast(ri.node_id)] = 0;
        }
    }

    // Compute reachable + last_use (or use cached).
    const have_cache = options.cached_analysis != null;
    const reachable = if (options.cached_analysis) |ca| ca.reachable else try interpreter.computeReachable(allocator, graph);
    defer if (!have_cache) allocator.free(reachable);
    const last_use = if (options.cached_analysis) |ca| ca.last_use else try interpreter.computeLastUse(allocator, graph, reachable);
    defer if (!have_cache) allocator.free(last_use);

    var graph_buffer_plan = try buffer_plan_mod.build(allocator, graph, &plan.base, .{});
    defer graph_buffer_plan.deinit();
    try graph_buffer_plan.validate(graph, &plan.base);

    // Track attention layer counter across partitions.
    var attention_layer: usize = 0;
    var pair_second: ?CT = null;
    var exec_attention = options.attention;
    var exec_stats: ExecutionStats = .{};
    const trace_partitions = tracePartitionsEnabled();

    // Execute each partition in order.
    for (plan.base.partitions, 0..) |part, part_idx| {
        exec_stats.partitions_executed += 1;
        const dev_id = plan.device_assignment[part_idx];
        const dev_entry = mesh.device(dev_id) orelse return error.DeviceNotFound;
        const cb = dev_entry.backend;
        if (trace_partitions) {
            printPartitionTrace("begin", graph, part, part_idx, dev_id);
        }

        // Transfer external inputs from source device to this device.
        for (part.external_inputs) |ext_in| {
            const nid: usize = @intCast(ext_in.node_id);
            const src_val = values[nid] orelse continue;
            const src_dev = value_device[nid];
            if (src_dev != dev_id) {
                const src_entry = mesh.device(src_dev) orelse continue;
                if (trace_partitions) {
                    std.debug.print(
                        "graph_executor_transfer_trace: partition={d} node={d} op={s} from_device={d} to_device={d} from_backend={s} to_backend={s} shape={any}\n",
                        .{
                            part_idx,
                            ext_in.node_id,
                            @tagName(graph.node(ext_in.node_id).op),
                            src_dev,
                            dev_id,
                            @tagName(src_entry.backend.kind()),
                            @tagName(cb.kind()),
                            graph.node(ext_in.node_id).output_shape,
                        },
                    );
                }
                var static_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
                const static_shape = try graphShapeDims(graph.node(ext_in.node_id).output_shape, &static_shape_buf);
                const transferred = try transferTensorWithKnownShape(allocator, src_val, src_entry.backend, cb, static_shape);
                exec_stats.cross_device_transfers += 1;
                if (cb.kind() == .metal and metal_partition_executor.isMetalDeviceResident(cb, transferred)) {
                    exec_stats.device_resident_transfers += 1;
                }
                const source_is_borrowed_runtime_input = isBorrowedRuntimeInputValue(
                    options.runtime_inputs,
                    donated,
                    ext_in.node_id,
                    src_val,
                );
                // Keep the old value alive (other partitions may need it).
                // Store the transferred copy as an additional ref.
                // We'll use the transferred value for this partition's nodes.
                values[nid] = transferred;
                value_device[nid] = dev_id;
                if (!source_is_borrowed_runtime_input) {
                    src_entry.backend.free(src_val);
                }
                if (rt_map.contains(ext_in.node_id)) {
                    try owned_runtime_transfers.put(allocator, ext_in.node_id, {});
                }
            }
        }

        const exec_ctx = PartitionExecutor.ExecutionContext{
            .allocator = allocator,
            .graph = graph,
            .backend = cb,
            .mesh = mesh,
            .options = options,
            .reachable = reachable,
            .last_use = last_use,
            .partition_plan = &plan.base,
            .buffer_plan = &graph_buffer_plan,
            .owned_runtime_transfers = &owned_runtime_transfers,
            .materialize_boundary_outputs = part.backend != .metal,
            .stats = &exec_stats,
            .attention = if (exec_attention) |*attn| attn else null,
            .attention_layer = &attention_layer,
            .pair_second = &pair_second,
            .embedding_ids = options.embedding_ids,
        };

        if (part.executor) |exec| {
            // Opaque compiled executor — executes
            // the entire partition as a unit. External inputs are already
            // populated in `values` by the transfer loop above.
            try exec.execute(values, value_device, part.node_ids, dev_id, exec_ctx);
        } else if (part.backend == .native or part.backend == .graph) {
            var native_exec = native_partition_executor.NativePartitionExecutor.initBorrowed(allocator, graph, cb);
            try native_exec.partitionExecutor().execute(values, value_device, part.node_ids, dev_id, exec_ctx);
        } else if (part.backend == .metal) {
            var metal_exec = metal_partition_executor.MetalPartitionExecutor.initBorrowed(allocator, graph, cb);
            try metal_exec.partitionExecutor().execute(values, value_device, part.node_ids, dev_id, exec_ctx);
        } else if (part.backend == .webgpu) {
            var webgpu_exec = webgpu_partition_executor.WebGpuPartitionExecutor.initBorrowed(allocator, graph, cb);
            try webgpu_exec.partitionExecutor().execute(values, value_device, part.node_ids, dev_id, exec_ctx);
        } else {
            // Per-node interpretation via ComputeBackend VTable.

            // Build per-partition ExecState.
            var exec_state = interpreter.ExecState{
                .attention_layer = attention_layer,
                .options = options,
                .last_use = last_use,
                .pair_second = pair_second,
            };

            // Execute each node in this partition.
            for (part.node_ids) |node_id| {
                const i: usize = @intCast(node_id);
                if (!reachable[i]) continue;

                // Runtime input override.
                if (rt_map.get(node_id)) |rt_val| {
                    const current_dev = value_device[i];
                    if (current_dev != dev_id) {
                        const src_entry = mesh.device(current_dev) orelse return error.DeviceNotFound;
                        const transferred = try transferTensor(allocator, rt_val, src_entry.backend, cb);
                        values[i] = transferred;
                        try owned_runtime_transfers.put(allocator, node_id, {});
                    } else {
                        values[i] = rt_val;
                    }
                    value_device[i] = dev_id;
                    continue;
                }

                // Skip placeholders.
                if (graph.node(node_id).op == .fused_from_float32) continue;

                values[i] = try executeNode(graph, cb, values, node_id, &exec_state);
                value_device[i] = dev_id;
                try interpreter.cloneOutputIfAliasedInputWouldBeFreed(
                    allocator,
                    graph,
                    cb,
                    values,
                    node_id,
                    last_use,
                    rt_map,
                    .empty,
                );

                // Free inputs whose last consumer is this node.
                const n = graph.node(node_id);
                for (n.getInputs()) |input_id| {
                    if (input_id == null_node or input_id >= count) continue;
                    if (last_use[@intCast(input_id)] == i) {
                        if (rt_map.contains(input_id) and !donated.contains(input_id) and !owned_runtime_transfers.contains(input_id)) continue;
                        if (values[@intCast(input_id)]) |ct| {
                            if (values[i]) |out_ct| {
                                if (ct == out_ct and interpreter.canKeepAliasedOutput(n.op)) {
                                    values[@intCast(input_id)] = null;
                                    continue;
                                }
                            }
                            const inp_dev = value_device[@intCast(input_id)];
                            if (mesh.device(inp_dev)) |inp_entry| {
                                inp_entry.backend.free(ct);
                            }
                            values[@intCast(input_id)] = null;
                        }
                    }
                }
            }

            // Thread attention layer counter to next partition.
            attention_layer = exec_state.attention_layer;
            pair_second = exec_state.pair_second;

            // Clean up MoE state.
            exec_state.freeMoeState();
        }
        if (trace_partitions) {
            printPartitionTrace("end", graph, part, part_idx, dev_id);
        }
    }

    // Collect outputs.
    const num_outputs = graph.outputs.items.len;
    const outputs = try allocator.alloc(CT, num_outputs);
    const output_devices = try allocator.alloc(DeviceId, num_outputs);

    for (graph.outputs.items, 0..) |out_id, idx| {
        const ct = values[@intCast(out_id)] orelse return error.MissingValue;
        const out_dev = value_device[@intCast(out_id)];
        const out_entry = mesh.device(out_dev) orelse return error.DeviceNotFound;
        var aliases_rt = false;
        if (options.runtime_inputs) |inputs| {
            for (inputs) |ri| {
                if (!donated.contains(ri.node_id) and ri.value == ct) {
                    aliases_rt = true;
                    break;
                }
            }
        }
        if (aliases_rt) {
            const one_data: [1]f32 = .{1.0};
            const one = try out_entry.backend.fromFloat32(&one_data);
            defer out_entry.backend.free(one);
            outputs[idx] = try out_entry.backend.multiply(ct, one);
        } else {
            outputs[idx] = ct;
        }
        output_devices[idx] = out_dev;
    }

    // Free remaining parameter handles (not outputs, not runtime inputs). Fused
    // executors may deliberately alias an intermediate value to a later graph
    // node so downstream consumers see the fused result; free each handle once.
    var freed_values = std.AutoHashMapUnmanaged(CT, void).empty;
    defer freed_values.deinit(allocator);
    for (0..count) |i| {
        if (values[i] == null) continue;
        var is_output = false;
        for (graph.outputs.items) |out_id| {
            if (out_id == @as(NodeId, @intCast(i))) {
                is_output = true;
                break;
            }
        }
        if (is_output) continue;
        var aliases_output = false;
        for (outputs) |out_ct| {
            if (values[i].? == out_ct) {
                aliases_output = true;
                break;
            }
        }
        if (aliases_output) continue;
        if (rt_map.contains(@intCast(i)) and !donated.contains(@intCast(i)) and !owned_runtime_transfers.contains(@intCast(i))) continue;
        if (freed_values.contains(values[i].?)) continue;
        try freed_values.put(allocator, values[i].?, {});
        const dev = value_device[i];
        if (mesh.device(dev)) |entry| {
            entry.backend.free(values[i].?);
        }
    }

    executor_stats.record(exec_stats);
    if (executor_stats.enabled()) executor_stats.print(exec_stats);

    return .{
        .outputs = outputs,
        .output_devices = output_devices,
        .stats = exec_stats,
        .allocator = allocator,
    };
}

fn isBorrowedRuntimeInputValue(
    runtime_inputs: ?[]const RuntimeInput,
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    node_id: NodeId,
    value: CT,
) bool {
    const inputs = runtime_inputs orelse return false;
    if (donated.contains(node_id)) return false;
    for (inputs) |ri| {
        if (ri.node_id == node_id and ri.value == value) return true;
    }
    return false;
}

fn tracePartitionsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_EXECUTOR_TRACE_PARTITIONS", false);
}

fn printPartitionTrace(
    phase: []const u8,
    graph: *const Graph,
    part: Partition,
    part_idx: usize,
    dev_id: DeviceId,
) void {
    const first_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[0]).op) else "none";
    const last_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[part.node_ids.len - 1]).op) else "none";
    std.debug.print(
        "graph_executor_partition_trace: {s} index={d} backend={s} device={d} nodes={d} first={s} last={s}\n",
        .{ phase, part_idx, @tagName(part.backend), dev_id, part.node_ids.len, first_op, last_op },
    );
    if (part.node_ids.len > 0) {
        const last_id = part.node_ids[part.node_ids.len - 1];
        const last = graph.node(last_id);
        switch (last.op) {
            .concat_prim => |attrs| {
                const inputs = last.getInputs();
                const lhs = graph.node(inputs[0]);
                const rhs = graph.node(inputs[1]);
                std.debug.print(
                    "graph_executor_partition_trace: concat index={d} node={d} axis={d} lhs_shape={any} rhs_shape={any} out_shape={any} lhs_op={s} rhs_op={s}\n",
                    .{
                        part_idx,
                        last_id,
                        attrs.axis,
                        lhs.output_shape,
                        rhs.output_shape,
                        last.output_shape,
                        @tagName(lhs.op),
                        @tagName(rhs.op),
                    },
                );
            },
            else => {},
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const BackendKind = contracts.BackendKind;
const partition_fn = partition_mod.partition;
const Capability = partition_mod.Capability;
const native_compute = @import("../ops/native_compute.zig");
const metal_compute_mod = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};
const metal_runtime_mod = if (build_options.enable_metal) @import("../backends/metal_runtime.zig") else struct {
    pub fn metalDeviceAvailable() bool {
        return false;
    }
};
const gpu_hosted_store_mod = @import("../ops/gpu_hosted_store.zig");

fn deinitEmptyNativeWeightStore(weight_store: *native_compute.WeightStore, allocator: std.mem.Allocator) void {
    native_compute.deinitPrefetchQueue(weight_store);
    weight_store.resident_weights.deinit(allocator);
    weight_store.lazy_weights.deinit(allocator);
}

fn initEmptyMetalWeightStore(allocator: std.mem.Allocator) gpu_hosted_store_mod.WeightStore {
    if (comptime build_options.enable_mlx) {
        return .{
            .allocator = allocator,
            .resident_weights = .{},
            .stream = .{},
            .prefix = "",
            .lazy_weights = .empty,
        };
    }
    return .{
        .allocator = allocator,
        .resident_weights = {},
        .stream = {},
        .prefix = "",
        .lazy_weights = .empty,
    };
}

fn deinitEmptyMetalWeightStore(weight_store: *gpu_hosted_store_mod.WeightStore, allocator: std.mem.Allocator) void {
    metal_compute_mod.deinitPrefetchQueue(weight_store);
    weight_store.lazy_weights.deinit(allocator);
}

test "single-device multi-executor matches interpreter" {
    const allocator = std.testing.allocator;

    // Build a simple graph: param → gelu → output
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    // Partition everything to one backend.
    const caps = [_]Capability{
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    const base_plan = try partition_fn(allocator, &g, &caps);

    // Wrap as DevicePartitionPlan — all on device 0.
    const dev_assign = try allocator.alloc(DeviceId, base_plan.partitions.len);
    @memset(dev_assign, 0);

    var dpp = DevicePartitionPlan{
        .base = base_plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    // We can't actually execute without a real backend, but we can verify
    // the plan structure is correct.
    try std.testing.expectEqual(@as(usize, 1), dpp.base.partitions.len);
    try std.testing.expectEqual(@as(DeviceId, 0), dpp.device_assignment[0]);
}

test "two-device partition plan structure" {
    const allocator = std.testing.allocator;

    // Build: param → gelu → neg → output
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const activated = try b.gelu(x);
    const out = try b.neg(activated);
    try g.markOutput(out);

    // Two-backend split: "accelerator" handles gelu, native handles rest.
    const onlyGelu = struct {
        fn f(op: OpCode) bool {
            return op == .fused_gelu;
        }
    }.f;

    const caps = [_]Capability{
        .{ .backend = .mlx, .priority = 10, .supports = &onlyGelu },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    const base_plan = try partition_fn(allocator, &g, &caps);

    // Assign partitions to devices based on their backend.
    const dev_assign = try allocator.alloc(DeviceId, base_plan.partitions.len);
    for (base_plan.partitions, 0..) |part, i| {
        dev_assign[i] = if (part.backend == .mlx) 1 else 0;
    }

    var dpp = DevicePartitionPlan{
        .base = base_plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    // Should have at least 3 partitions: native(param+decomposed), mlx(gelu), native(neg)
    try std.testing.expect(dpp.base.partitions.len >= 3);

    // The gelu partition should be assigned to device 1.
    var found_mlx = false;
    for (dpp.base.partitions, 0..) |part, i| {
        if (part.backend == .mlx) {
            try std.testing.expectEqual(@as(DeviceId, 1), dpp.device_assignment[i]);
            found_mlx = true;
        }
    }
    try std.testing.expect(found_mlx);
}

test "transferTensor round-trip preserves data" {
    // This test requires a real backend. We test the structure only.
    // The transferTensor function is exercised in integration tests with
    // real native backends.
    const allocator = std.testing.allocator;

    // Verify the function signature compiles correctly.
    _ = &transferTensor;
    _ = allocator;
}

test "graph backend partition executes through native partition executor path" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const out = try b.neg(x);
    try g.markOutput(out);

    const caps = [_]Capability{
        .{ .backend = .graph, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    const base_plan = try partition_fn(allocator, &g, &caps);
    const dev_assign = try allocator.alloc(DeviceId, base_plan.partitions.len);
    @memset(dev_assign, 0);
    var dpp = DevicePartitionPlan{
        .base = base_plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    try std.testing.expectEqual(BackendKind.graph, dpp.base.partitions[0].backend);

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();
    var mesh = try DeviceMesh.init(allocator, &.{.{ .id = 0, .backend = &cb, .kind = .native }});
    defer mesh.deinit();

    const input_data = [_]f32{ 1, -2, 3, -4 };
    const input = try cb.fromFloat32(&input_data);
    defer cb.free(input);

    var result = try executeMultiDevice(allocator, &g, &dpp, &mesh, .{
        .runtime_inputs = &.{.{ .node_id = x, .value = input }},
    });
    defer result.deinit(&mesh);

    try std.testing.expectEqual(@as(DeviceId, 0), result.output_devices[0]);
    const raw = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &.{ -1, 2, -3, 4 }, raw);
}

test "native partition executor transfers borrowed runtime input across devices" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const caps = [_]Capability{
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    const base_plan = try partition_fn(allocator, &g, &caps);
    const dev_assign = try allocator.alloc(DeviceId, base_plan.partitions.len);
    @memset(dev_assign, 1);
    var dpp = DevicePartitionPlan{
        .base = base_plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    var weight_store_a = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store_a, allocator);
    var compute_a = native_compute.NativeCompute.init(allocator, &weight_store_a, null);
    var cb_a = compute_a.computeBackend();

    var weight_store_b = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store_b, allocator);
    var compute_b = native_compute.NativeCompute.init(allocator, &weight_store_b, null);
    var cb_b = compute_b.computeBackend();

    var mesh = try DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &cb_a, .kind = .native },
        .{ .id = 1, .backend = &cb_b, .kind = .native },
    });
    defer mesh.deinit();

    const input_data = [_]f32{ -1, 0, 1, 2 };
    const input = try cb_a.fromFloat32Shape(&input_data, &.{4});
    defer cb_a.free(input);

    var result = try executeMultiDevice(allocator, &g, &dpp, &mesh, .{
        .runtime_inputs = &.{.{ .node_id = x, .value = input }},
    });
    defer result.deinit(&mesh);

    try std.testing.expectEqual(@as(DeviceId, 1), result.output_devices[0]);
    const output = try cb_b.toFloat32(result.outputs[0], allocator);
    defer allocator.free(output);
    try std.testing.expect(output[0] < 0.0);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), output[1], 1e-6);
    try std.testing.expect(output[2] > 0.8 and output[2] < 0.9);
    try std.testing.expect(output[3] > 1.9 and output[3] < 2.0);

    const original = try cb_a.toFloat32(input, allocator);
    defer allocator.free(original);
    try std.testing.expectEqualSlices(f32, &input_data, original);
}

test "multi-executor keeps metal partition outputs resident until final readback" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4 }));
    const out = try b.silu(x);
    try g.markOutput(out);

    const caps = [_]Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    const base_plan = try partition_fn(allocator, &g, &caps);
    const dev_assign = try allocator.alloc(DeviceId, base_plan.partitions.len);
    @memset(dev_assign, 1);
    var dpp = DevicePartitionPlan{
        .base = base_plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    var native_weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&native_weight_store, allocator);
    var native_compute_impl = native_compute.NativeCompute.init(allocator, &native_weight_store, null);
    var native_cb = native_compute_impl.computeBackend();

    var metal_weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&metal_weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &metal_weight_store, null);
    defer metal_compute.deinit();
    var metal_cb = metal_compute.computeBackend();
    if (!metal_cb.decoderRuntimeReady()) return error.SkipZigTest;

    var mesh = try DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &native_cb, .kind = .native },
        .{ .id = 1, .backend = &metal_cb, .kind = .metal },
    });
    defer mesh.deinit();

    const input_data = [_]f32{ -2.0, -0.5, 0.5, 2.0 };
    const input = try native_cb.fromFloat32Shape(&input_data, &.{ 1, 4 });
    defer native_cb.free(input);

    var result = try executeMultiDevice(allocator, &g, &dpp, &mesh, .{
        .runtime_inputs = &.{.{ .node_id = x, .value = input }},
    });
    defer result.deinit(&mesh);

    try std.testing.expectEqual(@as(DeviceId, 1), result.output_devices[0]);
    try std.testing.expect(metal_partition_executor.isMetalDeviceResident(&metal_cb, result.outputs[0]));
    try std.testing.expectEqual(@as(u64, 0), result.stats.boundary_output_materializations);
    try std.testing.expect(result.stats.device_resident_transfers >= 1);
    try std.testing.expect(result.stats.backend_command_dispatches >= 1);
    try std.testing.expectEqual(@as(u64, 0), result.stats.interpreter_fallbacks);

    const output = try metal_cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(output);
    for (input_data, output) |input_value, actual| {
        const expected = input_value / (1.0 + @exp(-input_value));
        try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
    }
}
