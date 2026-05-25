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

//! End-to-end tests for the PJRT backend.
//!
//! Tests the full pipeline: graph construction → partitioning → HLO
//! compilation → PJRT execution → output verification.
//!
//! All tests require the PJRT CPU plugin and gracefully skip if it's
//! not available. Run with: zig build test -Dpjrt=true

const std = @import("std");
const ml = @import("ml");
const pjrt_lib = @import("pjrt");
const build_options = @import("build_options");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const NodeId = ml.graph.NodeId;

const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const native_mod = @import("../ops/native_compute.zig");
const NativeCompute = native_mod.NativeCompute;
const WeightStore = native_mod.WeightStore;

const partition_mod = @import("partition.zig");
const Partition = partition_mod.Partition;
const Capability = partition_mod.Capability;

const compiler = @import("pjrt_compiler.zig");
const pjrt_executor = @import("pjrt_executor.zig");
const pjrt_mesh = @import("pjrt_mesh.zig");
const device_mesh_mod = @import("device_mesh.zig");
const model_runtime_mod = @import("model_runtime.zig");

const Tensor = @import("../backends/tensor.zig").Tensor;
const LoadedWeight = @import("../models/weight_source.zig").LoadedWeight;

// ── Helpers ────────────────────────────────────────────────────────

const plugin_path: [:0]const u8 = "/Users/ajroetker/go/src/github.com/antflydb/antfly/termite/pjrt/darwin-arm64/lib/pjrt_c_api_cpu_v0.83.4_plugin.so";

fn initClient() ?pjrt_lib.pjrt.Client {
    return pjrt_lib.pjrt.Client.init(plugin_path) catch |err| {
        std.debug.print("Skipping PJRT test (plugin load failed: {s})\n", .{@errorName(err)});
        return null;
    };
}

fn pjrtHloSerializationUnavailable(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
) !bool {
    var compile_result = try compiler.compilePartition(allocator, graph, part, cb);
    defer compile_result.deinit();
    if (compile_result.hlo_bytes.len > 0) return false;
    std.debug.print("Skipping PJRT execution test (xla_proto serializer stub produced empty HLO)\n", .{});
    return true;
}

/// Create a native WeightStore populated with named f32 weights.
const WeightEntry = struct {
    name: []const u8,
    shape: []const i64,
    data: []const f32,
};

fn setupWeights(
    allocator: std.mem.Allocator,
    entries: []const WeightEntry,
) !struct { ws: WeightStore, tensors: []Tensor } {
    var ws = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    var tensors = try allocator.alloc(Tensor, entries.len);
    for (entries, 0..) |entry, i| {
        tensors[i] = try Tensor.initFloat32(allocator, entry.name, entry.shape, entry.data);
        const owned_key = try allocator.dupe(u8, entry.name);
        try ws.resident_weights.put(allocator, owned_key, .{
            .tensor = tensors[i],
            .quantized = false,
            .quantized_storage = null,
        });
        // Mark tensor as not owned — we'll clean up via the weight store.
        tensors[i].owns_data = false;
        tensors[i].owns_shape = false;
    }
    return .{ .ws = ws, .tensors = tensors };
}

fn cleanupWeights(allocator: std.mem.Allocator, ws: *WeightStore, tensors: []Tensor) void {
    var it = ws.resident_weights.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.tensor.deinit();
        allocator.free(entry.key_ptr.*);
    }
    ws.resident_weights.deinit(allocator);
    allocator.free(tensors);
}

fn approxEq(a: f32, b: f32, tol: f32) bool {
    return @abs(a - b) <= tol;
}

/// Populate external input values for a PJRT partition.
/// Weight parameters are loaded from the compute backend weight store;
/// the runtime input (not found in the store) gets `input_data`.
fn populateExternalInputs(
    values: []?CT,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
    input_data: []const f32,
    allocator: std.mem.Allocator,
) !void {
    for (part.external_inputs) |ext_in| {
        // Skip if already populated (e.g. output from a prior partition).
        if (values[@intCast(ext_in.node_id)] != null) continue;

        const n = graph.node(ext_in.node_id);
        // Only parameter nodes can have stored weights.
        if (n.op != .parameter) {
            values[@intCast(ext_in.node_id)] = try cb.fromFloat32(input_data);
            continue;
        }
        const name = graph.parameterName(n);
        // Try loading from weight store (model parameters).
        const weight_ct = cb.getWeight(name) catch {
            // Not a stored weight — it's the runtime input.
            values[@intCast(ext_in.node_id)] = try cb.fromFloat32(input_data);
            continue;
        };
        defer cb.free(weight_ct); // getWeight creates a new buffer
        // Copy weight through f32 so values[nid] has independent ownership.
        const f32_data = try cb.toFloat32(weight_ct, allocator);
        defer allocator.free(f32_data);
        values[@intCast(ext_in.node_id)] = try cb.fromFloat32(f32_data);
    }
}

// ── Test: compiler produces valid HLO accepted by PJRT ─────────────

test "pjrt_compiler: linear partition compiles to valid HLO" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    // Build graph: Y = X @ W^T + bias (fused_linear)
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("weight", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{3}));
    const result = try b.linear(x, w, bias, 2, 4, 3);
    try g.markOutput(result);

    // Set up weights
    const weight_data = [_]f32{
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
    };
    const bias_data = [_]f32{ 0.1, 0.2, 0.3 };
    var weight_setup = try setupWeights(allocator, &.{
        .{ .name = "weight", .shape = &.{ 3, 4 }, .data = &weight_data },
        .{ .name = "bias", .shape = &.{3}, .data = &bias_data },
    });
    defer cleanupWeights(allocator, &weight_setup.ws, weight_setup.tensors);

    var compute = NativeCompute.init(allocator, &weight_setup.ws, null);
    const cb = compute.computeBackend();

    // Partition: everything goes to PJRT
    const caps = [_]Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_mod.partition(allocator, &g, &caps);
    defer plan.deinit();

    // Find the PJRT partition
    var pjrt_part_idx: ?usize = null;
    for (plan.partitions, 0..) |part, i| {
        if (part.backend == .pjrt) {
            pjrt_part_idx = i;
            break;
        }
    }
    const part_idx = pjrt_part_idx orelse return error.NoPjrtPartition;
    const part = &plan.partitions[part_idx];

    // Compile to HLO
    var compile_result = try compiler.compilePartition(allocator, &g, part, &cb);
    defer compile_result.deinit();

    // Verify HLO bytes are non-empty when the real xla_proto serializer is present.
    if (compile_result.hlo_bytes.len == 0) {
        std.debug.print("Skipping PJRT compile validation (xla_proto serializer stub produced empty HLO)\n", .{});
        return;
    }
    try std.testing.expect(compile_result.input_node_ids.len > 0);
    try std.testing.expect(compile_result.output_node_ids.len > 0);

    // Verify PJRT accepts the HLO (this is the real validation)
    var executable = try client.compile(compile_result.hlo_bytes, compile_result.output_node_ids.len);
    executable.deinit();
}

// ── Test: executor produces correct output for linear layer ────────

test "pjrt_executor: linear layer end-to-end" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    // Build graph: Y = X @ W^T + bias
    // W is identity-like (first 3 cols of I_4), so Y[i] = X[i][0..3] + bias
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("weight", Shape.init(.f32, &.{ 3, 4 }));
    const bias_node = try b.parameter("bias", Shape.init(.f32, &.{3}));
    const result = try b.linear(x, w, bias_node, 2, 4, 3);
    try g.markOutput(result);

    // Weight: W = [[1,0,0,0],[0,1,0,0],[0,0,1,0]]
    // So X @ W^T = X[:, 0:3]
    const weight_data = [_]f32{
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
    };
    const bias_data = [_]f32{ 0.1, 0.2, 0.3 };
    var weight_setup = try setupWeights(allocator, &.{
        .{ .name = "weight", .shape = &.{ 3, 4 }, .data = &weight_data },
        .{ .name = "bias", .shape = &.{3}, .data = &bias_data },
    });
    defer cleanupWeights(allocator, &weight_setup.ws, weight_setup.tensors);

    var compute = NativeCompute.init(allocator, &weight_setup.ws, null);
    const cb = compute.computeBackend();

    // Partition
    const caps = [_]Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_mod.partition(allocator, &g, &caps);
    defer plan.deinit();

    // Find PJRT partition
    var pjrt_part_idx: ?usize = null;
    for (plan.partitions, 0..) |part, i| {
        if (part.backend == .pjrt) {
            pjrt_part_idx = i;
            break;
        }
    }
    const part_idx = pjrt_part_idx orelse return error.NoPjrtPartition;
    if (try pjrtHloSerializationUnavailable(allocator, &g, &plan.partitions[part_idx], &cb)) return;

    // Create executor
    var exec = try pjrt_executor.createExecutor(
        allocator,
        &g,
        &plan.partitions[part_idx],
        &cb,
        &cb,
        &client,
    );
    defer exec.partitionExecutor().deinitExecutor();

    // Prepare value array
    const num_nodes = g.nodeCount();
    const values = try allocator.alloc(?CT, num_nodes);
    defer {
        for (values) |maybe_ct| {
            if (maybe_ct) |ct| cb.free(ct);
        }
        allocator.free(values);
    }
    @memset(values, null);

    const value_devices = try allocator.alloc(device_mesh_mod.DeviceId, num_nodes);
    defer allocator.free(value_devices);
    @memset(value_devices, 0);

    // Populate external inputs (weights from store, runtime input = X).
    const x_data = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try populateExternalInputs(values, &g, &plan.partitions[part_idx], &cb, &x_data, allocator);

    // Execute
    try exec.partitionExecutor().execute(
        values,
        value_devices,
        plan.partitions[part_idx].node_ids,
        0,
        .{},
    );

    // Check output.
    // Expected: X @ W^T + bias
    //   Row 0: [1,2,3] + [0.1,0.2,0.3] = [1.1, 2.2, 3.3]
    //   Row 1: [5,6,7] + [0.1,0.2,0.3] = [5.1, 6.2, 7.3]
    const output_nid = result;
    const output_ct = values[@intCast(output_nid)] orelse return error.MissingOutput;
    const output_f32 = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(output_f32);

    try std.testing.expectEqual(@as(usize, 6), output_f32.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.1), output_f32[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 2.2), output_f32[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 3.3), output_f32[2], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 5.1), output_f32[3], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 6.2), output_f32[4], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 7.3), output_f32[5], 1e-4);
}

test "pjrt_executor: embedding lookup uses runtime embedding ids" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const weight = try b.parameter("embed.weight", Shape.init(.f32, &.{ 4, 3 }));
    const indices = try g.addNode(.{
        .op = .{ .fused_from_float32 = {} },
        .output_shape = Shape.init(.i64, &.{2}),
    });
    const embedded = try b.embeddingLookup(weight, indices, 2, 3);
    try g.markOutput(embedded);

    const weight_data = [_]f32{
        0.0, 1.0,  2.0,
        3.0, 4.0,  5.0,
        6.0, 7.0,  8.0,
        9.0, 10.0, 11.0,
    };
    var weight_setup = try setupWeights(allocator, &.{
        .{ .name = "embed.weight", .shape = &.{ 4, 3 }, .data = &weight_data },
    });
    defer cleanupWeights(allocator, &weight_setup.ws, weight_setup.tensors);

    var compute = NativeCompute.init(allocator, &weight_setup.ws, null);
    const cb = compute.computeBackend();

    const supportsEmbedding = struct {
        fn f(op: ml.graph.OpCode) bool {
            return switch (op) {
                .fused_from_float32, .fused_embedding_lookup => true,
                else => false,
            };
        }
    }.f;
    const caps = [_]Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &supportsEmbedding },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_mod.partition(allocator, &g, &caps);
    defer plan.deinit();

    var pjrt_part_idx: ?usize = null;
    for (plan.partitions, 0..) |part, i| {
        if (part.backend != .pjrt) continue;
        for (part.node_ids) |nid| {
            if (g.node(nid).op == .fused_embedding_lookup) {
                pjrt_part_idx = i;
                break;
            }
        }
        if (pjrt_part_idx != null) break;
    }
    const part_idx = pjrt_part_idx orelse return error.NoPjrtPartition;

    var compile_result = try compiler.compilePartition(allocator, &g, &plan.partitions[part_idx], &cb);
    defer compile_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), compile_result.input_node_ids.len);
    try std.testing.expectEqual(weight, compile_result.input_node_ids[0]);
    try std.testing.expectEqual(@as(usize, 2), compile_result.input_bindings.len);
    var saw_embedding_ids = false;
    for (compile_result.input_bindings) |binding| {
        switch (binding) {
            .embedding_ids => |node_id| {
                try std.testing.expectEqual(indices, node_id);
                saw_embedding_ids = true;
            },
            .graph_node => {},
            .semantic_past_graph_node => {},
        }
    }
    try std.testing.expect(saw_embedding_ids);

    var exec = try pjrt_executor.createExecutor(
        allocator,
        &g,
        &plan.partitions[part_idx],
        &cb,
        &cb,
        &client,
    );
    defer exec.partitionExecutor().deinitExecutor();

    const num_nodes = g.nodeCount();
    const values = try allocator.alloc(?CT, num_nodes);
    defer {
        for (values) |maybe_ct| {
            if (maybe_ct) |ct| cb.free(ct);
        }
        allocator.free(values);
    }
    @memset(values, null);

    const value_devices = try allocator.alloc(device_mesh_mod.DeviceId, num_nodes);
    defer allocator.free(value_devices);
    @memset(value_devices, 0);

    try populateExternalInputs(values, &g, &plan.partitions[part_idx], &cb, &.{}, allocator);

    const ids = [_]i64{ 2, 0 };
    try exec.partitionExecutor().execute(
        values,
        value_devices,
        plan.partitions[part_idx].node_ids,
        0,
        .{ .embedding_ids = &ids },
    );

    const output_ct = values[@intCast(embedded)] orelse return error.MissingOutput;
    const output_f32 = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(output_f32);

    try std.testing.expectEqual(@as(usize, 6), output_f32.len);
    try std.testing.expectEqualSlices(f32, &.{ 6, 7, 8, 0, 1, 2 }, output_f32);

    var model_executor_ctx = try pjrt_executor.createModelExecutor(
        allocator,
        &g,
        &plan.partitions[part_idx],
        &cb,
        &cb,
        &client,
    );
    var model_executor = model_executor_ctx.modelExecutor();
    defer model_executor.deinit();

    var model_runtime = try model_executor.createRuntime(allocator);
    defer model_runtime.deinit();
    const runtime_caps = model_runtime.capabilities();
    try std.testing.expect(runtime_caps.supports_decode);
    try std.testing.expectEqual(model_runtime_mod.RuntimeStateOwnership.host_assisted_inputs, runtime_caps.state_ownership);

    var model_output = try model_runtime.prefill(allocator, .{
        .input_ids = &ids,
        .seq_len = ids.len,
        .query_seq_len = ids.len,
        .attention_mode = .paged_prefill,
    });
    defer model_output.deinit(allocator);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 2 }, try model_output.hostLogits(allocator));
}

test "pjrt_model_runtime: decode uses single token id" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const weight = try b.parameter("embed.weight", Shape.init(.f32, &.{ 4, 3 }));
    const indices = try g.addNode(.{
        .op = .{ .fused_from_float32 = {} },
        .output_shape = Shape.init(.i64, &.{1}),
    });
    const embedded = try b.embeddingLookup(weight, indices, 1, 3);
    try g.markOutput(embedded);

    const weight_data = [_]f32{
        0.0, 1.0,  2.0,
        3.0, 4.0,  5.0,
        6.0, 7.0,  8.0,
        9.0, 10.0, 11.0,
    };
    var weight_setup = try setupWeights(allocator, &.{
        .{ .name = "embed.weight", .shape = &.{ 4, 3 }, .data = &weight_data },
    });
    defer cleanupWeights(allocator, &weight_setup.ws, weight_setup.tensors);

    var compute = NativeCompute.init(allocator, &weight_setup.ws, null);
    const cb = compute.computeBackend();

    const supportsEmbedding = struct {
        fn f(op: ml.graph.OpCode) bool {
            return switch (op) {
                .fused_from_float32, .fused_embedding_lookup => true,
                else => false,
            };
        }
    }.f;
    const caps = [_]Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &supportsEmbedding },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_mod.partition(allocator, &g, &caps);
    defer plan.deinit();

    var pjrt_part_idx: ?usize = null;
    for (plan.partitions, 0..) |part, i| {
        if (part.backend != .pjrt) continue;
        for (part.node_ids) |nid| {
            if (g.node(nid).op == .fused_embedding_lookup) {
                pjrt_part_idx = i;
                break;
            }
        }
        if (pjrt_part_idx != null) break;
    }
    const part_idx = pjrt_part_idx orelse return error.NoPjrtPartition;
    if (try pjrtHloSerializationUnavailable(allocator, &g, &plan.partitions[part_idx], &cb)) return;

    var model_executor_ctx = try pjrt_executor.createModelExecutor(
        allocator,
        &g,
        &plan.partitions[part_idx],
        &cb,
        &cb,
        &client,
    );
    var model_executor = model_executor_ctx.modelExecutor();
    defer model_executor.deinit();

    var model_runtime = try model_executor.createRuntime(allocator);
    defer model_runtime.deinit();

    var decoded = try model_runtime.decode(allocator, .{
        .token_id = 3,
        .position = 0,
    });
    defer decoded.deinit(allocator);
    try std.testing.expectEqualSlices(f32, &.{ 9, 10, 11 }, try decoded.hostLogits(allocator));
}

// ── Test: gelu activation through PJRT ─────────────────────────────

test "pjrt_executor: gelu activation end-to-end" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    // Build graph: Y = gelu(X)
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{4}));
    const result = try b.gelu(x);
    try g.markOutput(result);

    // No weights needed — gelu is elementwise
    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };

    var compute = NativeCompute.init(allocator, &ws, null);
    const cb = compute.computeBackend();

    // Partition
    const caps = [_]Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_mod.partition(allocator, &g, &caps);
    defer plan.deinit();

    var pjrt_part_idx: ?usize = null;
    for (plan.partitions, 0..) |part, i| {
        if (part.backend == .pjrt) {
            pjrt_part_idx = i;
            break;
        }
    }
    const part_idx = pjrt_part_idx orelse return error.NoPjrtPartition;
    if (try pjrtHloSerializationUnavailable(allocator, &g, &plan.partitions[part_idx], &cb)) return;

    var exec = try pjrt_executor.createExecutor(
        allocator,
        &g,
        &plan.partitions[part_idx],
        &cb,
        &cb,
        &client,
    );
    defer exec.partitionExecutor().deinitExecutor();

    const num_nodes = g.nodeCount();
    const values = try allocator.alloc(?CT, num_nodes);
    defer {
        for (values) |maybe_ct| if (maybe_ct) |ct| cb.free(ct);
        allocator.free(values);
    }
    @memset(values, null);

    const value_devices = try allocator.alloc(device_mesh_mod.DeviceId, num_nodes);
    defer allocator.free(value_devices);
    @memset(value_devices, 0);

    // Input: [-1, 0, 1, 2]
    const x_data = [_]f32{ -1.0, 0.0, 1.0, 2.0 };
    for (plan.partitions[part_idx].external_inputs) |ext_in| {
        values[@intCast(ext_in.node_id)] = try cb.fromFloat32(&x_data);
    }

    try exec.partitionExecutor().execute(values, value_devices, plan.partitions[part_idx].node_ids, 0, .{});

    const output_ct = values[@intCast(result)] orelse return error.MissingOutput;
    const output_f32 = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(output_f32);

    // GELU reference values (from scipy.stats.norm.cdf approximation):
    //   gelu(-1) ≈ -0.1588, gelu(0) = 0, gelu(1) ≈ 0.8412, gelu(2) ≈ 1.9545
    try std.testing.expectEqual(@as(usize, 4), output_f32.len);
    try std.testing.expectApproxEqAbs(@as(f32, -0.1588), output_f32[0], 2e-2);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), output_f32[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8412), output_f32[2], 2e-2);
    try std.testing.expectApproxEqAbs(@as(f32, 1.9545), output_f32[3], 2e-2);
}

// ── Test: linear + rms_norm + gelu pipeline ────────────────────────

test "pjrt_executor: linear + rms_norm + gelu pipeline" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    // Build graph: gelu(rms_norm(X @ W^T, gamma))
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 1, 4 }));
    const w = try b.parameter("proj.weight", Shape.init(.f32, &.{ 4, 4 }));
    const gamma = try b.parameter("norm.weight", Shape.init(.f32, &.{4}));

    const lin = try b.linearNoBias(x, w, 1, 4, 4);
    const normed = try b.rmsNorm(lin, gamma, 4, 1e-5);
    const result = try b.gelu(normed);
    try g.markOutput(result);

    // Identity weight → linear is passthrough. Gamma = 1 → rms_norm normalizes.
    const w_data = [_]f32{
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
        0, 0, 0, 1,
    };
    const gamma_data = [_]f32{ 1, 1, 1, 1 };
    var weight_setup = try setupWeights(allocator, &.{
        .{ .name = "proj.weight", .shape = &.{ 4, 4 }, .data = &w_data },
        .{ .name = "norm.weight", .shape = &.{4}, .data = &gamma_data },
    });
    defer cleanupWeights(allocator, &weight_setup.ws, weight_setup.tensors);

    var compute = NativeCompute.init(allocator, &weight_setup.ws, null);
    const cb = compute.computeBackend();

    const caps = [_]Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_mod.partition(allocator, &g, &caps);
    defer plan.deinit();

    // Execute ALL PJRT partitions in order (pipeline may span multiple partitions
    // due to decomposed subgraph nodes creating partition boundaries).
    const num_nodes = g.nodeCount();
    const values = try allocator.alloc(?CT, num_nodes);
    defer {
        for (values) |maybe_ct| if (maybe_ct) |ct| cb.free(ct);
        allocator.free(values);
    }
    @memset(values, null);

    const value_devices = try allocator.alloc(device_mesh_mod.DeviceId, num_nodes);
    defer allocator.free(value_devices);
    @memset(value_devices, 0);

    const x_data = [_]f32{ 1, 2, 3, 4 };

    // Track executors for cleanup.
    var executors: [8]*pjrt_executor.PjrtExecutor = undefined;
    var num_executors: usize = 0;
    defer for (executors[0..num_executors]) |e| e.partitionExecutor().deinitExecutor();

    for (plan.partitions) |*part| {
        if (part.backend != .pjrt) continue;
        if (try pjrtHloSerializationUnavailable(allocator, &g, part, &cb)) return;

        try populateExternalInputs(values, &g, part, &cb, &x_data, allocator);

        var exec = try pjrt_executor.createExecutor(
            allocator,
            &g,
            part,
            &cb,
            &cb,
            &client,
        );
        executors[num_executors] = exec;
        num_executors += 1;

        try exec.partitionExecutor().execute(values, value_devices, part.node_ids, 0, .{});
    }

    if (num_executors == 0) return error.NoPjrtPartition;

    const output_ct = values[@intCast(result)] orelse return error.MissingOutput;
    const output_f32 = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(output_f32);

    // After identity linear: [1,2,3,4]
    // RMS = sqrt(mean([1,4,9,16])) = sqrt(7.5) ≈ 2.7386
    // rms_norm: x / RMS * gamma = [0.3651, 0.7303, 1.0954, 1.4606]
    // gelu(0.3651) ≈ 0.214, gelu(0.7303) ≈ 0.518, gelu(1.0954) ≈ 0.912, gelu(1.4606) ≈ 1.335
    try std.testing.expectEqual(@as(usize, 4), output_f32.len);

    // Verify all outputs are finite and in reasonable range
    for (output_f32) |v| {
        try std.testing.expect(!std.math.isNan(v));
        try std.testing.expect(!std.math.isInf(v));
    }

    // Verify ordering: gelu is monotonic, and input is sorted, so output should be sorted
    for (1..output_f32.len) |i| {
        try std.testing.expect(output_f32[i] >= output_f32[i - 1]);
    }

    // Approximate value checks
    try std.testing.expectApproxEqAbs(@as(f32, 0.214), output_f32[0], 0.05);
    try std.testing.expectApproxEqAbs(@as(f32, 0.518), output_f32[1], 0.05);
    try std.testing.expectApproxEqAbs(@as(f32, 0.912), output_f32[2], 0.05);
    try std.testing.expectApproxEqAbs(@as(f32, 1.335), output_f32[3], 0.05);
}

// ── Test: PJRT mesh creation ───────────────────────────────────────

test "pjrt_mesh: createPjrtMesh from CPU plugin" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    // Use fake backends — we only test mesh structure, not execution.
    const fake_pjrt_cb = @as(*const ComputeBackend, @ptrFromInt(0x1000));
    const fake_native_cb = @as(*const ComputeBackend, @ptrFromInt(0x2000));

    var mesh = try pjrt_mesh.createPjrtMesh(allocator, &client, fake_pjrt_cb, fake_native_cb);
    defer mesh.deinit();

    // CPU plugin has at least 1 device; mesh should have that + 1 native fallback
    try std.testing.expect(mesh.deviceCount() >= 2);

    // First device(s) should be PJRT
    const first = mesh.device(0).?;
    try std.testing.expectEqual(contracts.BackendKind.pjrt, first.kind);
    try std.testing.expectEqual(fake_pjrt_cb, first.backend);

    // Last device should be native fallback
    const last_id: device_mesh_mod.DeviceId = @intCast(mesh.deviceCount() - 1);
    const last = mesh.device(last_id).?;
    try std.testing.expectEqual(contracts.BackendKind.native, last.kind);
    try std.testing.expectEqual(fake_native_cb, last.backend);

    // pjrtDeviceCount should be total - 1
    const pjrt_count = pjrt_mesh.pjrtDeviceCount(&mesh);
    try std.testing.expectEqual(mesh.deviceCount() - 1, pjrt_count);

    // pjrtDeviceIds should return only PJRT device IDs
    const pjrt_ids = try pjrt_mesh.pjrtDeviceIds(allocator, &mesh);
    defer allocator.free(pjrt_ids);
    try std.testing.expectEqual(pjrt_count, pjrt_ids.len);
    for (pjrt_ids) |id| {
        try std.testing.expectEqual(contracts.BackendKind.pjrt, mesh.device(id).?.kind);
    }
}

// ── Test: elem_add through PJRT ────────────────────────────────────

test "pjrt_executor: element-wise add end-to-end" {
    if (!build_options.enable_pjrt) return;
    const allocator = std.testing.allocator;

    var client = initClient() orelse return;
    defer client.deinit();

    // Build graph: Z = X + Y (fused_elem_add)
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    const result = try b.elemAdd(x, y);
    try g.markOutput(result);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    const cb = compute.computeBackend();

    const caps = [_]Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_mod.partition(allocator, &g, &caps);
    defer plan.deinit();

    var pjrt_part_idx: ?usize = null;
    for (plan.partitions, 0..) |part, i| {
        if (part.backend == .pjrt) {
            pjrt_part_idx = i;
            break;
        }
    }
    const part_idx = pjrt_part_idx orelse return error.NoPjrtPartition;
    if (try pjrtHloSerializationUnavailable(allocator, &g, &plan.partitions[part_idx], &cb)) return;

    var exec = try pjrt_executor.createExecutor(
        allocator,
        &g,
        &plan.partitions[part_idx],
        &cb,
        &cb,
        &client,
    );
    defer exec.partitionExecutor().deinitExecutor();

    const num_nodes = g.nodeCount();
    var values = try allocator.alloc(?CT, num_nodes);
    defer {
        for (values) |maybe_ct| if (maybe_ct) |ct| cb.free(ct);
        allocator.free(values);
    }
    @memset(values, null);

    const value_devices = try allocator.alloc(device_mesh_mod.DeviceId, num_nodes);
    defer allocator.free(value_devices);
    @memset(value_devices, 0);

    // Both inputs are external (parameters without weights = runtime inputs)
    const x_data = [_]f32{ 1, 2, 3, 4 };
    const y_data = [_]f32{ 10, 20, 30, 40 };
    // Populate both external inputs
    const ext_inputs = plan.partitions[part_idx].external_inputs;
    try std.testing.expectEqual(@as(usize, 2), ext_inputs.len);
    values[@intCast(ext_inputs[0].node_id)] = try cb.fromFloat32(&x_data);
    values[@intCast(ext_inputs[1].node_id)] = try cb.fromFloat32(&y_data);

    try exec.partitionExecutor().execute(values, value_devices, plan.partitions[part_idx].node_ids, 0, .{});

    const output_ct = values[@intCast(result)] orelse return error.MissingOutput;
    const output_f32 = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(output_f32);

    try std.testing.expectEqual(@as(usize, 4), output_f32.len);
    try std.testing.expectApproxEqAbs(@as(f32, 11), output_f32[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 22), output_f32[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 33), output_f32[2], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 44), output_f32[3], 1e-4);
}
