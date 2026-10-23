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

//! Prepare once; repeated submission reuses optimization, graph analysis,
//! partition implementations, physical planning and backend command caches.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const ModelProgram = @import("program.zig").ModelProgram;
pub const runtime_module = if (@import("build_options").enable_wasm) @import("wasm_runtime.zig") else @import("../graph/runtime.zig");

/// Borrows immutable program and backend. Both must outlive this executable,
/// while the backend context must outlive resident results. Result storage and
/// representation owners survive executable/plan eviction.
pub const Executable = struct {
    diagnostics: @import("diagnostics.zig").Diagnostics = .{},
    graph: *const @import("ml").graph.Graph,
    analysis: interpreter.CachedAnalysis,
    runtime: runtime_module.Runtime,

    pub fn run(self: *Executable, allocator: std.mem.Allocator, options: interpreter.ExecuteOptions) !runtime_module.Result {
        var prepared_options = options;
        prepared_options.cached_analysis = self.analysis;
        if (self.runtime.default_backend.kind() == .metal) prepared_options.planned_device_execution = true;
        if (self.diagnostics.isEnabled()) prepared_options.collect_partition_stats = true;
        const result = try self.runtime.execute(allocator, self.graph, prepared_options);
        self.diagnostics.record(result.stats, self.runtime.default_backend);
        return result;
    }

    pub fn deinit(self: *Executable) void {
        self.runtime.deinit();
    }
};

pub const PrepareOptions = struct {
    input_bindings: ?[]const interpreter.RuntimeInput = null,
    tensor_descs: ?[]const ?@import("../graph/backend_contracts.zig").TensorDesc = null,
    region_costs: ?@import("placement.zig").RegionCosts = .{},
};

pub fn prepare(allocator: std.mem.Allocator, program: *const ModelProgram, backend: *const ops.ComputeBackend, strategy: runtime_module.Strategy) !Executable {
    return prepareWithOptions(allocator, program, backend, strategy, .{});
}

pub fn prepareWithOptions(allocator: std.mem.Allocator, program: *const ModelProgram, backend: *const ops.ComputeBackend, strategy: runtime_module.Strategy, options: PrepareOptions) !Executable {
    return prepareAnalyzedGraph(allocator, &program.graph, program.analysis, backend, strategy, options);
}

/// Borrow canonical IR and its cached analysis from an existing semantic owner.
/// This adapts traced caches without optimizing again or cloning graph storage.
pub fn prepareAnalyzedGraph(allocator: std.mem.Allocator, graph: *const @import("ml").graph.Graph, analysis: interpreter.CachedAnalysis, backend: *const ops.ComputeBackend, strategy: runtime_module.Strategy, options: PrepareOptions) !Executable {
    const partition = @import("../graph/partition.zig");
    var owned_seeds: ?[]?@import("../graph/backend_contracts.zig").TensorDesc = null;
    defer if (owned_seeds) |seeds| allocator.free(seeds);
    var tensor_descs = options.tensor_descs;
    if (options.input_bindings) |inputs| {
        if (inputs.len != 0) {
            const seeds = try partition.allocTensorDescriptorSeeds(allocator, graph);
            owned_seeds = seeds;
            try partition.seedAllUploadableResidency(seeds, graph, backend.kind(), 0);
            if (tensor_descs) |provided| {
                if (provided.len != seeds.len) return error.InvalidTensorDescriptorTable;
                for (provided, seeds) |descriptor, *seed| if (descriptor) |value| {
                    seed.* = value;
                };
            }
            for (inputs) |input| {
                if (input.node_id >= graph.nodeCount()) return error.InvalidRuntimeInputNode;
                if (graph.node(input.node_id).op != .parameter) continue;
                if (seeds[input.node_id] == null) seeds[input.node_id] = @import("../graph/backend_contracts.zig").TensorDesc.init(graph.node(input.node_id).output_shape, .unknown);
                seeds[input.node_id].?.current_backend = (input.source_backend orelse backend).kind();
            }
            tensor_descs = seeds;
        }
    }
    return .{ .graph = graph, .analysis = analysis, .runtime = if (comptime @import("build_options").enable_wasm)
        try runtime_module.Runtime.init(allocator, graph, backend, strategy)
    else
        try runtime_module.Runtime.initWithOptions(allocator, graph, backend, strategy, .{ .tensor_descs = tensor_descs, .analysis = analysis, .region_costs = options.region_costs }) };
}

test "prepared executable reuses CPU and Metal plans with resident outputs" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const a = std.testing.allocator;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    _ = try b.parameter("dead", ml.graph.Shape.init(.f32, &.{ 1, 8 }));
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 8 }));
    const out = try b.relu(x);
    try graph.markOutput(out);
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .node_id = x, .kind = .input }, .{ .name = "out", .node_id = out, .kind = .output } });
    defer program.deinit();
    const BackendType = @import("../backends/backends.zig").BackendType;
    for ([_]BackendType{ .native, .metal }) |kind| {
        if (kind == .metal) {
            if (comptime !@import("build_options").enable_metal) continue;
            if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) continue;
        }
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, kind, null);
        defer context.deinit(a);
        const cb = context.computeBackend();
        var executable = try prepare(a, &program, &cb, .compiled_preferred);
        defer executable.deinit();
        try std.testing.expect(executable.diagnostics.snapshot() == null);
        executable.diagnostics.configure(true);
        var tensor = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{ 1, 8 }, &.{ -4, -3, -2, -1, 0, 1, 2, 3 });
        defer tensor.deinit();
        const input = try context.importHostTensor(a, &tensor);
        defer cb.free(input);
        var raw_inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input }};
        const inputs = try program.remapInputs(&raw_inputs);
        for (0..4) |iteration| {
            var result = try executable.run(a, .{ .runtime_inputs = inputs, .output_placement = .producer, .collect_partition_stats = true, .activation_readbacks = .forbidden });
            defer result.deinit(&executable.runtime);
            const owner = result.backendFor(&executable.runtime, 0);
            if (kind == .metal) {
                try std.testing.expectEqual(ops.BackendKind.metal, owner.kind());
                try std.testing.expect(@import("../graph/metal_partition_executor.zig").isMetalDeviceResident(owner, result.outputs[0]));
                try std.testing.expectEqual(@as(u64, 0), result.stats.boundary_output_materializations);
                try std.testing.expect(result.stats.backend_command_dispatches > 0);
                try std.testing.expect(result.stats.assigned_kernel_dispatches > 0);
            }
            if (kind == .native) {
                const diagnostics = executable.diagnostics.snapshot().?;
                try std.testing.expectEqual(result.stats.assigned_kernel_dispatches, diagnostics.execution.assigned_kernel_dispatches);
                try std.testing.expect(result.stats.assigned_kernel_dispatches > 0);
                if (iteration > 0) {
                    try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
                    try std.testing.expect(result.stats.physical_storage_reuses > 0);
                }
            }
            const values = try owner.toFloat32(result.outputs[0], a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 1, 2, 3 }, values);
        }
    }
}

test "prepared BERT program matches CPU reference on CPU and Metal" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const bert_graph = @import("../architectures/bert_graph.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const BackendType = @import("../backends/backends.zig").BackendType;
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var graph = ml.graph.Graph.init(arena.allocator());
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const ids = try b.parameter("ids", ml.graph.Shape.init(.i64, &.{4}));
    const positions = try b.parameter("positions", ml.graph.Shape.init(.i64, &.{4}));
    const types = try b.parameter("types", ml.graph.Shape.init(.i64, &.{4}));
    const mask = try b.parameter("mask", ml.graph.Shape.init(.f32, &.{ 2, 4, 4 }));
    const built = try bert_graph.buildForwardGraph(&b, .{ .vocab_size = 8, .hidden_size = 8, .num_hidden_layers = 1, .num_attention_heads = 2, .intermediate_size = 16, .max_position_embeddings = 4 }, 1, 4, .{ .input_ids = ids, .position_ids = positions, .token_type_ids = types, .attn_bias = mask });
    try graph.markOutput(built.output_node);
    var program = try ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var expected: ?[]f32 = null;
    defer if (expected) |values| a.free(values);
    for ([_]BackendType{ .native, .metal }) |kind| {
        if (kind == .metal) {
            if (comptime !@import("build_options").enable_metal) continue;
            if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) continue;
        }
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, kind, null);
        defer context.deinit(a);
        const cb = context.computeBackend();
        var inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
        defer {
            for (inputs.items) |input| cb.free(input.value);
            inputs.deinit(a);
        }
        for (graph.parameters.items) |id| {
            const node = graph.node(id);
            const name = graph.parameterName(node);
            const shape = node.output_shape;
            const count: usize = @intCast(shape.numElements().?);
            var tensor: Tensor = undefined;
            if (shape.dtype == .i64) {
                const values = [_]i64{ 0, 1, 2, 3 };
                const zeros = [_]i64{ 0, 0, 0, 0 };
                tensor = try Tensor.initInt64(a, name, shape.dims[0..shape.rank()], if (id == types) &zeros else &values);
            } else {
                const values = try a.alloc(f32, count);
                defer a.free(values);
                for (values, 0..) |*value, i| {
                    value.* = if (id == mask) (if (i % 4 == 3) -10000 else 0) else if (std.mem.endsWith(u8, name, "LayerNorm.weight")) 1 else if (std.mem.endsWith(u8, name, ".bias")) 0 else @as(f32, @floatFromInt(@as(i32, @intCast(i % 17)) - 8)) / 64;
                }
                tensor = try Tensor.initFloat32(a, name, shape.dims[0..shape.rank()], values);
                if (kind == .native and id != mask) {
                    const owned_name = try a.dupe(u8, name);
                    errdefer a.free(owned_name);
                    var weight = try Tensor.initFloat32(a, name, shape.dims[0..shape.rank()], values);
                    errdefer weight.deinit();
                    try context.native.weight_store.resident_weights.put(a, owned_name, .{ .tensor = weight });
                }
            }
            defer tensor.deinit();
            const value = try context.importHostTensor(a, &tensor);
            errdefer cb.free(value);
            try inputs.append(a, .{ .node_id = id, .value = value });
        }
        if (kind == .native) {
            var reference = try runtime_module.Runtime.init(a, &graph, &cb, .interpreter);
            defer reference.deinit();
            var result = try reference.execute(a, &graph, .{ .runtime_inputs = inputs.items });
            defer result.deinit(&reference);
            expected = try cb.toFloat32(result.outputs[0], a);
            const eager = try @import("../architectures/bert.zig").forward(&cb, a, .{
                .vocab_size = 8,
                .hidden_size = 8,
                .num_hidden_layers = 1,
                .num_attention_heads = 2,
                .intermediate_size = 16,
                .max_position_embeddings = 4,
            }, &.{ 0, 1, 2, 3 }, &.{ 1, 1, 1, 0 }, &.{ 0, 0, 0, 0 }, 1, 4);
            defer a.free(eager);
            try std.testing.expectEqual(expected.?.len, eager.len);
            for (expected.?, eager) |graph_value, eager_value| try std.testing.expectApproxEqAbs(eager_value, graph_value, 2e-4);
        }
        const input_bindings = try a.dupe(interpreter.RuntimeInput, inputs.items);
        defer a.free(input_bindings);
        const bound = try program.remapInputs(input_bindings);
        var executable = try prepare(a, &program, &cb, .compiled_preferred);
        defer executable.deinit();
        var result = try executable.run(a, .{ .runtime_inputs = bound, .output_placement = .producer });
        defer result.deinit(&executable.runtime);
        const values = try result.backendFor(&executable.runtime, 0).toFloat32(result.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqual(expected.?.len, values.len);
        for (expected.?, values) |reference, value| try std.testing.expectApproxEqAbs(reference, value, 2e-4);
        if (@import("antfly_platform").env.getenvBool("ANTFLY_PREPARED_BENCH")) {
            var reference = try runtime_module.Runtime.init(a, &graph, &cb, .compiled_preferred);
            defer reference.deinit();
            var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
            defer analysis.deinit(a);
            var elapsed = [_]u64{ 0, 0 };
            var commands = [_]u64{ 0, 0 };
            for (0..2) |path| {
                // Warm each path before timing explicit final materialization.
                // This tiny synthetic fixture is not a checkpoint benchmark.
                for (0..24) |iteration| {
                    const start = @import("antfly_platform").time.awakeNs(std.testing.io);
                    var measured = if (path == 0)
                        try reference.execute(a, &graph, .{ .runtime_inputs = inputs.items, .cached_analysis = analysis, .output_placement = .producer, .planned_device_execution = kind == .metal })
                    else
                        try executable.run(a, .{ .runtime_inputs = bound, .output_placement = .producer });
                    const owner = if (path == 0) &reference else &executable.runtime;
                    defer measured.deinit(owner);
                    const output = try measured.backendFor(owner, 0).toFloat32(measured.outputs[0], a);
                    defer a.free(output);
                    if (iteration >= 4) {
                        elapsed[path] += @import("antfly_platform").time.awakeNs(std.testing.io) - start;
                        commands[path] += measured.stats.backend_command_dispatches;
                    }
                }
            }
            std.debug.print("prepared_bert_bench backend={s} raw_nodes={d} prepared_nodes={d} reference_ns={d} prepared_ns={d} reference_commands={d} prepared_commands={d}\n", .{ @tagName(kind), graph.nodeCount(), program.graph.nodeCount(), elapsed[0] / 20, elapsed[1] / 20, commands[0] / 20, commands[1] / 20 });
        }
    }
}

test "prepared physical frames pin published outputs survive eviction and bound retained storage" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const ResidentOutputs = @import("../backends/session.zig").ResidentOutputs;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const input_node = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try b.relu(input_node));
    var program = try ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .compiled_preferred);
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    var published: [6]ResidentOutputs = undefined;
    var initialized: usize = 0;
    defer for (published[0..initialized]) |*value| value.deinit();
    for (&published, 0..) |*held, iteration| {
        const v: f32 = @floatFromInt(iteration + 1);
        const input = try cb.fromFloat32Shape(&.{ -v, v, v + 1, -v - 1 }, &.{4});
        defer cb.free(input);
        const inputs = [_]interpreter.RuntimeInput{.{ .node_id = program.id_map[input_node], .value = input }};
        var result = try executable.run(a, .{ .runtime_inputs = &inputs, .collect_partition_stats = true });
        defer result.deinit(&executable.runtime);
        if (iteration < 4) {
            try std.testing.expect(result.stats.assigned_kernel_dispatches > 0);
        } else {
            try std.testing.expectEqual(@as(u64, 1), result.stats.physical_frame_busy_fallbacks);
            try std.testing.expectEqual(@as(u64, 0), result.stats.assigned_kernel_dispatches);
        }
        held.* = .{ .outputs = result.outputs, .allocator = a, .backend = &cb };
        result.outputs = &.{};
        initialized += 1;
    }
    // One released output makes exactly that planned frame reusable.
    published[0].deinit();
    const input = try cb.fromFloat32Shape(&.{ -9, 9, 10, -10 }, &.{4});
    defer cb.free(input);
    const inputs = [_]interpreter.RuntimeInput{.{ .node_id = program.id_map[input_node], .value = input }};
    {
        var result = try executable.run(a, .{ .runtime_inputs = &inputs, .collect_partition_stats = true });
        defer result.deinit(&executable.runtime);
        try std.testing.expect(result.stats.assigned_kernel_dispatches > 0);
        try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
        try std.testing.expect(result.stats.physical_storage_reuses > 0);
    }
    executable.deinit();
    executable_owned = false;
    // Storage leases, not the plan's lifetime, own these published values.
    for (published[1..], 1..) |held, iteration| {
        const values = try cb.toFloat32(held.outputs[0], a);
        defer a.free(values);
        const v: f32 = @floatFromInt(iteration + 1);
        try std.testing.expectEqualSlices(f32, &.{ 0, v, v + 1, 0 }, values);
    }
}

test "prepared physical workspace is declared once reused and owned by its frame" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const Native = @import("../ops/native_compute.zig").NativeCompute;
    const a = std.testing.allocator;
    const ScratchKernel = struct {
        fn requirement(_: *anyopaque, descriptor: *const ops.kernel_descriptor.Descriptor) !ops.kernel_descriptor.WorkspaceRequirement {
            return .{ .byte_size = descriptor.elements() * @sizeOf(f32) };
        }
        fn run(raw: *anyopaque, request: ops.kernel_descriptor.Invocation) !bool {
            const compute: *Native = @ptrCast(@alignCast(raw));
            const original = compute.computeBackend();
            const invoke = original.vtable.kernels.?.runAssignedKernel.?;
            if (request.output == null) return invoke(raw, request);
            const scratch = request.workspace orelse return error.MissingAssignedWorkspace;
            // An idempotent two-pass kernel exercises actual scratch writes
            // and reads while retaining the graph's ReLU semantics.
            var stage = request;
            stage.output = scratch;
            stage.workspace = null;
            if (!try invoke(raw, stage)) return error.UnexpectedKernelDecline;
            stage.inputs = &.{scratch};
            stage.output = request.output;
            return invoke(raw, stage);
        }
    };
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    var cb = context.computeBackend();
    var vtable = cb.vtable.*;
    var functions = vtable.kernels.?.*;
    functions.assignedKernelWorkspace = ScratchKernel.requirement;
    functions.runAssignedKernel = ScratchKernel.run;
    vtable.kernels = &functions;
    cb.vtable = &vtable;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    try graph.markOutput(try b.relu(x));
    var program = try ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .compiled_preferred);
    defer executable.deinit();
    const pool = executable.runtime.physical_pool.?;
    const scratch_id = pool.workspace_allocations[0];
    const allocation = executable.runtime.buffer_plan.?.allocations[scratch_id];
    try std.testing.expectEqual(@import("../graph/buffer_plan.zig").AllocationKind.workspace, allocation.kind);
    try std.testing.expectEqual(@as(u64, 32), allocation.byte_size);
    try std.testing.expect(allocation.first_use == 0 and allocation.last_use >= program.graph.nodeCount());
    const input = try cb.fromFloat32Shape(&.{ -4, -3, -2, -1, 0, 1, 2, 3 }, &.{ 2, 4 });
    defer cb.free(input);
    const bindings = [_]interpreter.RuntimeInput{.{ .node_id = program.id_map[x], .value = input }};
    var scratch_handle: ?ops.CT = null;
    for (0..4) |iteration| {
        var result = try executable.run(a, .{ .runtime_inputs = &bindings, .collect_partition_stats = true });
        defer result.deinit(&executable.runtime);
        const values = try cb.toFloat32(result.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 1, 2, 3 }, values);
        const lease = pool.frames.items[0].storages[scratch_id].?;
        if (iteration == 0) {
            scratch_handle = lease.tensor;
            try std.testing.expectEqual(@as(u64, 2), result.stats.physical_storage_allocations);
        } else {
            try std.testing.expectEqual(scratch_handle.?, lease.tensor);
            try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
        }
        const scratch_values = try cb.toFloat32(lease.tensor, a);
        defer a.free(scratch_values);
        try std.testing.expectEqualSlices(f32, values, scratch_values);
    }
}

test "prepared physical views keep their source live through publication and competing writes" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{8}));
    const positive = try b.relu(x);
    const view = try b.reshape(positive, ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const competing = try b.neg(x);
    const second = try b.expOp(competing);
    try graph.markOutput(view);
    try graph.markOutput(second);
    var program = try ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .compiled_preferred);
    defer executable.deinit();
    const plan = &executable.runtime.buffer_plan.?;
    const source_slot = plan.slotForNode(program.id_map[positive]).?;
    const view_slot = plan.slotForNode(program.id_map[view]).?;
    const competing_slot = plan.slotForNode(program.id_map[competing]).?;
    try std.testing.expectEqual(source_slot.allocation, view_slot.allocation);
    try std.testing.expect(source_slot.last_use >= view_slot.last_use);
    try std.testing.expect(source_slot.allocation != competing_slot.allocation);
    const input = try cb.fromFloat32Shape(&.{ -4, -3, -2, -1, 0, 1, 2, 3 }, &.{8});
    defer cb.free(input);
    const bindings = [_]interpreter.RuntimeInput{.{ .node_id = program.id_map[x], .value = input }};
    for (0..3) |_| {
        var result = try executable.run(a, .{ .runtime_inputs = &bindings });
        defer result.deinit(&executable.runtime);
        const first = try cb.toFloat32(result.outputs[0], a);
        defer a.free(first);
        try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 1, 2, 3 }, first);
        const other = try cb.toFloat32(result.outputs[1], a);
        defer a.free(other);
        for (other, [_]f32{ 4, 3, 2, 1, 0, -1, -2, -3 }) |value, exponent| try std.testing.expectApproxEqAbs(@exp(exponent), value, 1e-6);
    }
}

test "prepared physical transfer protocol binds download and upload storage without CUDA hardware" {
    const ml = @import("ml");
    const native = @import("../ops/native_compute.zig");
    const Probe = struct {
        fn kind(_: *anyopaque) ops.BackendKind {
            return .cuda;
        }
        fn upload(ctx: *anyopaque, output: ops.CT, input: []const f32) !void {
            // Use native storage to verify the CUDA planner protocol without
            // submitting a device kernel. Actual CUDA upload uses its driver.
            const target = (try native.vtable_impl.hostFloat32Read.?(ctx, output)).?;
            if (target.len != input.len) return error.InvalidPhysicalTransferSize;
            @memcpy(@constCast(target), input);
        }
    };
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    var table = native.vtable_impl;
    table.backendKind = Probe.kind;
    var functions = table.kernels.?.*;
    functions.writePhysicalFloat32 = Probe.upload;
    table.kernels = &functions;
    var cb = context.computeBackend();
    cb.vtable = &table;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const indices = try b.parameter("indices", ml.graph.Shape.init(.i32, &.{2}));
    const gathered = try b.gather(x, indices, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    graph.nodeMut(gathered).op.gather.axis = 1;
    const out = try b.relu(gathered);
    try graph.markOutput(out);
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .kind = .input, .node_id = x }, .{ .name = "indices", .kind = .input, .node_id = indices }, .{ .name = "out", .kind = .output, .node_id = out } });
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const input = try cb.fromFloat32Shape(&.{ -1, 2, -3, 4, -5, 6 }, &.{ 2, 3 });
    defer cb.free(input);
    const index_values = (try cb.fromInt32Shape(&.{ 1, 0 }, &.{2})).?;
    defer cb.free(index_values);
    const bindings = [_]interpreter.RuntimeInput{ .{ .node_id = program.id_map[x], .value = input }, .{ .node_id = program.id_map[indices], .value = index_values } };
    for (0..3) |iteration| {
        var result = try executable.run(a, .{ .runtime_inputs = &bindings, .output_placement = .producer, .collect_partition_stats = true });
        defer result.deinit(&executable.runtime);
        const actual = try result.backendFor(&executable.runtime, 0).toFloat32(result.outputs[0], a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, &.{ 2, 0, 0, 4 }, actual);
        try std.testing.expectEqual(@as(u64, 2), result.stats.physical_transfer_dispatches);
        try std.testing.expectEqual(@as(u64, 40), result.stats.physical_transfer_bytes);
        if (iteration != 0) try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
    }
}

test "prepared Metal fanout placement preserves outputs and removes two boundary transfers" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const ml = @import("ml");
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const indices = try b.parameter("indices", ml.graph.Shape.init(.i32, &.{2}));
    const first = try b.gather(x, indices, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    graph.nodeMut(first).op.gather.axis = 1;
    const activation = try b.relu(first);
    const host = try b.gather(activation, indices, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    graph.nodeMut(host).op.gather.axis = 1;
    const resident = try b.softmax(activation);
    try graph.markOutput(host);
    try graph.markOutput(resident);
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .kind = .input, .node_id = x }, .{ .name = "indices", .kind = .input, .node_id = indices }, .{ .name = "host", .kind = .output, .node_id = host }, .{ .name = "resident", .kind = .output, .node_id = resident } });
    defer program.deinit();
    const input = try cb.fromFloat32Shape(&.{ -1, 2, -3, 4, -5, 6 }, &.{ 2, 3 });
    defer cb.free(input);
    const index_values = (try cb.fromInt32Shape(&.{ 1, 0 }, &.{2})).?;
    defer cb.free(index_values);
    const bindings = [_]interpreter.RuntimeInput{ .{ .node_id = program.id_map[x], .value = input }, .{ .node_id = program.id_map[indices], .value = index_values } };
    for ([_]bool{ false, true }) |refine| {
        var executable = try prepareAnalyzedGraph(a, &program.graph, program.analysis, &cb, .partitioned, .{ .region_costs = if (refine) .{} else null });
        defer executable.deinit();
        const placement = &executable.runtime.plan.?.base;
        try std.testing.expectEqual(if (refine) ops.BackendKind.native else .metal, placement.partitions[placement.node_assignment[program.id_map[activation]]].backend);
        try std.testing.expectError(error.DeviceReadbackForbidden, executable.run(a, .{ .runtime_inputs = &bindings, .output_placement = .producer, .activation_readbacks = .forbidden }));
        for (0..3) |iteration| {
            var result = try executable.run(a, .{ .runtime_inputs = &bindings, .output_placement = .producer, .collect_partition_stats = true });
            defer result.deinit(&executable.runtime);
            const host_backend = result.backendFor(&executable.runtime, 0);
            try std.testing.expectEqual(ops.BackendKind.native, host_backend.kind());
            const values = try host_backend.toFloat32(result.outputs[0], a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 0, 2, 4, 0 }, values);
            const device_backend = result.backendFor(&executable.runtime, 1);
            try std.testing.expectEqual(ops.BackendKind.metal, device_backend.kind());
            try std.testing.expect(@import("../graph/metal_partition_executor.zig").isMetalDeviceResident(device_backend, result.outputs[1]));
            const probabilities = try device_backend.toFloat32(result.outputs[1], a);
            defer a.free(probabilities);
            for (probabilities, [_]f32{ 0.8807971, 0.11920292, 0.01798621, 0.98201376 }) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
            try std.testing.expectEqual(@as(u64, if (refine) 3 else 5), result.stats.cross_device_transfers);
            // Both placements now dispatch two shared kernels: CPU ReLU plus Metal
            // softmax after refinement, or Metal ReLU plus Metal softmax before it.
            try std.testing.expectEqual(@as(u64, 2), result.stats.assigned_kernel_dispatches);
            try std.testing.expectEqual(@as(u64, if (refine) 24 else 40), result.stats.physical_transfer_bytes);
            try std.testing.expectEqual(@as(u64, if (refine) 32 else 48), result.stats.actual_download_bytes);
            if (iteration != 0) try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
        }
    }
}

test "prepared auxiliary planes reuse primary lifetimes and survive frame retirement" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const Native = @import("../ops/native_compute.zig").NativeCompute;
    const a = std.testing.allocator;
    const SavedKernel = struct {
        fn requirement(_: *anyopaque, descriptor: *const ops.kernel_descriptor.Descriptor) !ops.kernel_descriptor.AuxiliaryRequirements {
            var result = ops.kernel_descriptor.AuxiliaryRequirements{};
            result.entries[0] = .{ .role = .inverse_std, .shape = descriptor.output_shape };
            result.count = 1;
            return result;
        }
        fn run(raw: *anyopaque, request: ops.kernel_descriptor.Invocation) !bool {
            const compute: *Native = @ptrCast(@alignCast(raw));
            const original = compute.computeBackend();
            const invoke = original.vtable.kernels.?.runAssignedKernel.?;
            if (request.output == null) return invoke(raw, request);
            if (request.auxiliaries.len != 1 or request.auxiliaries[0].role != .inverse_std) return error.MissingAuxiliary;
            var stage = request;
            stage.output = request.auxiliaries[0].tensor;
            stage.auxiliaries = &.{};
            if (!try invoke(raw, stage)) return error.UnexpectedKernelDecline;
            stage.inputs = &.{stage.output.?};
            stage.output = request.output;
            return invoke(raw, stage);
        }
    };
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    var cb = context.computeBackend();
    var vtable = cb.vtable.*;
    var functions = vtable.kernels.?.*;
    functions.assignedKernelAuxiliaries = SavedKernel.requirement;
    functions.runAssignedKernel = SavedKernel.run;
    vtable.kernels = &functions;
    cb.vtable = &vtable;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    try graph.markOutput(try b.relu(x));
    var program = try ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .compiled_preferred);
    var executable_live = true;
    defer if (executable_live) executable.deinit();
    const pool = executable.runtime.physical_pool.?;
    var plane_id: @import("../graph/buffer_plan.zig").AllocationId = undefined;
    for (pool.kernels) |maybe_kernel| if (maybe_kernel) |kernel| {
        const auxiliary = kernel.auxiliaries[0].?;
        plane_id = auxiliary.allocation;
        const primary = pool.plan.allocations[kernel.output_allocation];
        const plane = pool.plan.allocations[plane_id];
        try std.testing.expectEqual(primary.first_use, plane.first_use);
        try std.testing.expectEqual(primary.last_use, plane.last_use);
        try std.testing.expectEqual(.auxiliary, plane.kind);
    };
    const input = try cb.fromFloat32Shape(&.{ -4, -3, -2, -1, 0, 1, 2, 3 }, &.{ 2, 4 });
    defer cb.free(input);
    const bindings = [_]interpreter.RuntimeInput{.{ .node_id = program.id_map[x], .value = input }};
    for (0..4) |iteration| {
        var result = try executable.run(a, .{ .runtime_inputs = &bindings, .collect_partition_stats = true });
        defer result.deinit(&executable.runtime);
        const values = try cb.toFloat32(result.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 1, 2, 3 }, values);
        try std.testing.expectEqual(@as(u64, if (iteration == 0) 2 else 0), result.stats.physical_storage_allocations);
    }
    const retained_plane = pool.frames.items[0].storages[plane_id].?;
    retained_plane.retain();
    defer retained_plane.release();
    executable.deinit();
    executable_live = false;
    const saved_values = try cb.toFloat32(retained_plane.tensor, a);
    defer a.free(saved_values);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 1, 2, 3 }, saved_values);
}
