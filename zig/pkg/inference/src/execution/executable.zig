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
    mutable_parameters: ?[]const bool = null,
    runtime: runtime_module.Runtime,

    pub fn run(self: *Executable, allocator: std.mem.Allocator, options: interpreter.ExecuteOptions) !runtime_module.Result {
        var result: runtime_module.Result = undefined;
        try self.runInto(allocator, &options, &result);
        return result;
    }

    /// Borrow invocation options synchronously and write a fresh caller-owned
    /// result directly. The value API remains available to existing callers.
    pub fn runInto(self: *Executable, allocator: std.mem.Allocator, options: *const interpreter.ExecuteOptions, result: *runtime_module.Result) !void {
        if (!self.diagnostics.isEnabled()) return self.runBodyInto(false, allocator, options, result, {});
        return self.runObservedInto(allocator, options, result);
    }
    // Rich scope arrays live only on the enabled stack. Both paths share the
    // actual option preparation, execution and error/publication behavior.
    noinline fn runObservedInto(self: *Executable, allocator: std.mem.Allocator, options: *const interpreter.ExecuteOptions, result: *runtime_module.Result) !void {
        var device_owners: @import("device_owner_registry.zig").Registry = undefined;
        const observing = true;
        if (observing) device_owners.begin(@import("allocation_registry.zig").metadataAllocator());
        defer if (observing) {
            _ = device_owners.end();
        };
        var before: ?@import("diagnostics.zig").Start = if (observing) self.diagnostics.beginEnabled(self.runtime.default_backend) else null;
        var device_roots: @import("device_allocation_registry.zig").Registry = undefined;
        var kernels: @import("runtime_observation.zig").KernelScope = .{};
        var metal_buffers: @import("../backends/metal_telemetry.zig").BufferInterval = .{};
        var video_resources: @import("antfly_video").preparation.OwnedResourceInterval = .{};
        if (observing) {
            device_roots.begin(@import("allocation_registry.zig").metadataAllocator());
            kernels.begin();
            metal_buffers.begin(@import("build_options").enable_metal);
            video_resources.begin(@import("build_options").enable_metal);
            // beginEnabled registered the retained owner census before the
            // roots attached. Bind the actual context root at this boundary.
            self.runtime.default_backend.registerRuntimeObservation();
        }
        defer if (observing) {
            _ = video_resources.finish(@import("build_options").enable_metal);
            _ = metal_buffers.finish(@import("build_options").enable_metal);
            _ = kernels.finish();
            _ = device_roots.end();
        };
        if (before) |*start| {
            start.device_owners = &device_owners;
            start.device_roots = &device_roots;
            start.kernels = &kernels;
            start.metal_buffers = &metal_buffers;
            start.video_resources = &video_resources;
        }
        if (observing) if (options.runtime_inputs) |inputs| for (inputs) |input| {
            (input.source_backend orelse self.runtime.default_backend).registerTensorOwnershipObservation(input.value);
        };
        defer if (observing) @import("diagnostics.zig").Diagnostics.end(before);
        return self.runBodyInto(true, allocator, options, result, before);
    }
    fn runBodyInto(self: *Executable, comptime observing: bool, allocator: std.mem.Allocator, options: *const interpreter.ExecuteOptions, result: *runtime_module.Result, before: if (observing) ?@import("diagnostics.zig").Start else void) !void {
        var prepared_options = options.*;
        prepared_options.cached_analysis = self.analysis;
        prepared_options.mutable_parameters = self.mutable_parameters;
        if (self.runtime.default_backend.kind() == .metal) prepared_options.planned_device_execution = true;
        if (self.diagnostics.isEnabled()) prepared_options.collect_partition_stats = true;
        if (comptime @import("build_options").enable_wasm) {
            result.* = self.runtime.execute(allocator, self.graph, prepared_options) catch |err| {
                if (observing) self.diagnostics.recordFailure(self.runtime.default_backend, before, err);
                return err;
            };
        } else self.runtime.executeInto(allocator, self.graph, &prepared_options, result) catch |err| {
            if (observing) self.diagnostics.recordFailure(self.runtime.default_backend, before, err);
            return err;
        };
        if (observing) self.diagnostics.record(result.stats, self.runtime.default_backend, before);
    }

    /// Cold mutable provider configuration; direct executable users quiesce
    /// concurrent backend users before enabling, including metadata readers.
    pub fn configureExecutionDiagnostics(self: *Executable, enabled: bool) void {
        self.diagnostics.configureBackend(enabled, self.runtime.default_backend);
    }
    pub fn deinit(self: *Executable) void {
        self.deinitWithCleanup(.{});
    }
    pub fn deinitWithCleanup(self: *Executable, cleanup: @import("cleanup_context.zig").Context) void {
        self.runtime.deinitWithCleanup(cleanup);
    }
};

pub const PrepareOptions = struct {
    physical_replay: bool = true,
    immutable_input_bindings: ?[]const @import("kernel_route.zig").InputBinding = null,
    input_bindings: ?[]const interpreter.RuntimeInput = null,
    tensor_descs: ?[]const ?@import("../graph/backend_contracts.zig").TensorDesc = null,
    region_costs: ?@import("placement.zig").RegionCosts = .{},
};

pub fn prepare(allocator: std.mem.Allocator, program: *const ModelProgram, backend: *const ops.ComputeBackend, strategy: runtime_module.Strategy) !Executable {
    return prepareWithOptions(allocator, program, backend, strategy, .{});
}

pub fn prepareWithOptions(allocator: std.mem.Allocator, program: *const ModelProgram, backend: *const ops.ComputeBackend, strategy: runtime_module.Strategy, options: PrepareOptions) !Executable {
    var prepared = try prepareAnalyzedGraph(allocator, &program.graph, program.analysis, backend, strategy, options);
    errdefer prepared.deinit();
    if (comptime !@import("build_options").enable_wasm) {
        if (options.physical_replay) if (prepared.runtime.physical_pool) |pool| try pool.prepareReplay(&program.graph, &prepared.runtime.buffer_plan.?, program.bindings, options.region_costs orelse .{});
    }
    prepared.mutable_parameters = program.mutable_parameters;
    if (comptime !@import("build_options").enable_wasm) {
        if (prepared.runtime.physical_pool) |pool| for (program.bindings) |binding| {
            if (binding.kind == .state_input or binding.kind == .state_output) {
                pool.cuda_frame_dispatch = false;
                pool.metal_frame_dispatch = false;
            }
        };
    }
    return prepared;
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
    var result: Executable = .{ .graph = graph, .analysis = analysis, .runtime = if (comptime @import("build_options").enable_wasm)
        try runtime_module.Runtime.init(allocator, graph, backend, strategy)
    else
        try runtime_module.Runtime.initWithOptions(allocator, graph, backend, strategy, .{ .tensor_descs = tensor_descs, .analysis = analysis, .region_costs = options.region_costs }) };
    errdefer result.deinit();
    try configureOwnedCoverage(&result, graph, analysis);
    if (comptime !@import("build_options").enable_wasm) if (result.runtime.physical_pool) |pool| try pool.configureRoutes(options.immutable_input_bindings orelse &.{});
    return result;
}

test "prepared executable reuses CPU Metal and CUDA plans with resident outputs" {
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
    for ([_]BackendType{ .native, .metal, .cuda }) |kind| {
        if (kind == .cuda) {
            if (comptime !@import("build_options").enable_cuda) continue;
        }
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
        executable.configureExecutionDiagnostics(true);
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
                try std.testing.expectEqual(result.stats.assigned_kernel_dispatches, diagnostics.execution.?.assigned_kernel_dispatches);
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

test "prepared BERT program matches CPU reference on CPU Metal and CUDA" {
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
    for ([_]BackendType{ .native, .metal, .cuda }) |kind| {
        if (kind == .cuda) {
            if (comptime !@import("build_options").enable_cuda) continue;
        }
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
        // This fixture is admitted on each device; a CPU-only fallback must
        // not masquerade as cross-backend prepared execution parity.
        try std.testing.expectEqual(cb.kind(), result.backendFor(&executable.runtime, 0).kind());
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
    try testPhysicalFrameLifetime(.native);
}

test "CUDA prepared physical frames pin outputs across pool pressure and executable eviction" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    try testPhysicalFrameLifetime(.cuda);
}

fn testPhysicalFrameLifetime(comptime backend_kind: @import("../backends/backends.zig").BackendType) !void {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const ResidentOutputs = @import("../backends/session.zig").ResidentOutputs;
    const a = std.testing.allocator;
    var context = @import("../backends/compute_context.zig").BackendContext.init(a, backend_kind, null) catch |err| switch (@as(anyerror, err)) {
        error.CudaUnavailable, error.NoCudaDevices => if (backend_kind == .cuda) return error.SkipZigTest else return err,
        else => return err,
    };
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
        var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        try @import("transfer_audit.zig").begin(&audit);
        const executed = executable.run(a, .{ .runtime_inputs = &inputs, .collect_partition_stats = true });
        @import("transfer_audit.zig").end();
        var result = try executed;
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
        var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        try @import("transfer_audit.zig").begin(&audit);
        const executed = executable.run(a, .{ .runtime_inputs = &inputs, .collect_partition_stats = true });
        @import("transfer_audit.zig").end();
        var result = try executed;
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
    functions.implementation_table = &vtable; // Explicit fixture physical-program opt-in.
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
        // Compare the vector F32 exponential against an independent F64
        // oracle. One F32 ULP at exp(4) already exceeds 1e-6 absolute.
        for (other, [_]f64{ 4, 3, 2, 1, 0, -1, -2, -3 }) |value, exponent| {
            try std.testing.expectApproxEqRel(@exp(exponent), @as(f64, value), 2 * std.math.floatEps(f32));
        }
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
    functions.implementation_table = &table; // Explicit fixture physical-program opt-in.
    table.kernels = &functions;
    var cb = context.computeBackend();
    cb.vtable = &table;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const indices = try b.parameter("indices", ml.graph.Shape.init(.i32, &.{ 2, 2 }));
    const gathered = try b.gather(x, indices, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    graph.nodeMut(gathered).op.gather.axis = 1;
    // GatherElements deliberately remains a CPU boundary. Ordinary Gather is
    // now a CUDA kernel and cannot exercise the transfer protocol here.
    graph.nodeMut(gathered).op.gather.elements = true;
    const out = try b.relu(gathered);
    try graph.markOutput(out);
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .kind = .input, .node_id = x }, .{ .name = "indices", .kind = .input, .node_id = indices }, .{ .name = "out", .kind = .output, .node_id = out } });
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const input = try cb.fromFloat32Shape(&.{ -1, 2, -3, 4, -5, 6 }, &.{ 2, 3 });
    defer cb.free(input);
    const index_values = (try cb.fromInt32Shape(&.{ 1, 0, 1, 0 }, &.{ 2, 2 })).?;
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
    functions.implementation_table = &vtable; // Explicit fixture physical-program opt-in.
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

test "prepared CPU RoPE descriptors use invocation attention offsets and retain earlier outputs" {
    try testPreparedRopePositions(.native);
}

test "prepared CUDA RoPE descriptors use invocation attention offsets and retain earlier outputs" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    try testPreparedRopePositions(.cuda);
}

test "prepared Metal RoPE descriptors use invocation attention offsets and retain earlier outputs" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    try testPreparedRopePositions(.metal);
}

fn testPreparedRopePositions(comptime kind: @import("../backends/backends.zig").BackendType) !void {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, kind, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 4, 8 });
    const input_node = try builder.parameter("x", shape);
    const rotated = try graph.addNode(.{ .op = .{ .fused_rope = .{ .seq_len = 2, .head_dim = 4, .rope_dim = 4, .theta = 10000, .freq_scale = 1, .position_offset = 3 } }, .output_shape = shape, .inputs = .{ input_node, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node }, .num_inputs = 1 });
    try graph.markOutput(rotated);
    var program = try ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .compiled_preferred);
    defer executable.deinit();
    var input_data: [32]f32 = undefined;
    for (&input_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 11)) - 5)) / 16;
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&input_data, &.{ 4, 8 }));
    defer cb.free(input);
    const bindings = [_]interpreter.RuntimeInput{.{ .node_id = program.id_map[input_node], .value = input }};
    var held: ?runtime_module.Result = null;
    defer if (held) |*value| value.deinit(&executable.runtime);
    var first_values: [32]f32 = undefined;
    for ([_]?usize{ null, 5, null, 8 }, 0..) |override, iteration| {
        const offset = override orelse 3;
        var result = try executable.run(a, .{ .runtime_inputs = &bindings, .collect_partition_stats = true, .attention = if (override != null) .{ .total_sequence_len = offset + 2, .query_sequence_len = 2, .kv_sequence_len = offset + 2 } else null });
        var result_owned = true;
        defer if (result_owned) result.deinit(&executable.runtime);
        try std.testing.expect(result.stats.assigned_kernel_dispatches >= 1);
        const values = try cb.toFloat32(result.outputs[0], a);
        defer a.free(values);
        for (0..8) |chunk| for (0..2) |pair| {
            const position = offset + (chunk / 2) % 2;
            const angle = @as(f64, @floatFromInt(position)) / (if (pair == 0) @as(f64, 1) else 100);
            const lo = chunk * 4 + pair;
            const hi = lo + 2;
            try std.testing.expectApproxEqAbs(@as(f32, @floatCast(@as(f64, input_data[lo]) * @cos(angle) - @as(f64, input_data[hi]) * @sin(angle))), values[lo], 3e-5);
            try std.testing.expectApproxEqAbs(@as(f32, @floatCast(@as(f64, input_data[lo]) * @sin(angle) + @as(f64, input_data[hi]) * @cos(angle))), values[hi], 3e-5);
        };
        if (iteration == 0) {
            @memcpy(&first_values, values);
            held = result;
            result_owned = false;
        } else {
            const retained = try cb.toFloat32(held.?.outputs[0], a);
            defer a.free(retained);
            try std.testing.expectEqualSlices(f32, &first_values, retained);
        }
        const retained_input = try cb.toFloat32(input, a);
        defer a.free(retained_input);
        try std.testing.expectEqualSlices(f32, &input_data, retained_input);
    }
    try std.testing.expectError(error.InvalidRoPEInput, executable.run(a, .{ .runtime_inputs = &bindings, .attention = .{ .total_sequence_len = 1, .query_sequence_len = 2, .kv_sequence_len = 1 } }));
    const after_rejection = try cb.toFloat32(held.?.outputs[0], a);
    defer a.free(after_rejection);
    try std.testing.expectEqualSlices(f32, &first_values, after_rejection);
    if (kind != .metal) {
        const frozen = executable.runtime.physical_pool.?.kernels[program.id_map[rotated]].?.descriptor.operation.rope;
        try std.testing.expectEqual(@as(usize, 3), frozen.position_offset);
    }
}

test "equivalent BERT ONNX safetensors GGUF and Q8 snapshots share CPU preparation" {
    try @import("testing/model_formats.zig").run(.native);
}
test "equivalent BERT ONNX safetensors GGUF and Q8 snapshots share Metal preparation" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    try @import("testing/model_formats.zig").run(.metal);
}
test "equivalent BERT ONNX safetensors GGUF and Q8 snapshots share CUDA preparation" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    try @import("testing/model_formats.zig").run(.cuda);
}

test "equivalent stateful decoder ONNX safetensors GGUF and Q8 snapshots share CPU preparation" {
    try @import("testing/model_formats.zig").runDecoder(.native);
}
test "equivalent stateful decoder ONNX safetensors GGUF and Q8 snapshots share Metal preparation" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    try @import("testing/model_formats.zig").runDecoder(.metal);
}
test "equivalent stateful decoder ONNX safetensors GGUF and Q8 snapshots share CUDA preparation" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    try @import("testing/model_formats.zig").runDecoder(.cuda);
}

test "prepared state matrix bindings stay live without immutable weight packing on Metal" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const state = try b.parameter("state", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    try graph.markOutput(try b.matmul(x, try b.transpose(state, &.{ 1, 0 })));
    var program = try ModelProgram.init(a, &graph, &.{
        .{ .name = "x", .node_id = x, .kind = .input },
        .{ .name = "state", .node_id = state, .kind = .state_input },
    });
    defer program.deinit();
    var prepared = try prepare(a, &program, &cb, .partitioned);
    defer prepared.deinit();
    const input_values = [_]f32{ -4, -3, -2, -1, 0, 1, 2, 3 };
    var tensor = try Tensor.initFloat32(a, "x", &.{ 2, 4 }, &input_values);
    defer tensor.deinit();
    const input = try context.importHostTensor(a, &tensor);
    defer cb.free(input);
    var held: [3]runtime_module.Result = undefined;
    var initialized: usize = 0;
    defer for (held[0..initialized]) |*result| result.deinit(&prepared.runtime);
    for (0..held.len) |iteration| {
        var weights: [16]f32 = @splat(0);
        for (0..4) |i| weights[i * 4 + i] = @floatFromInt(iteration + 1);
        var rhs = try Tensor.initFloat32(a, "state", &.{ 4, 4 }, &weights);
        defer rhs.deinit();
        const value = try context.importHostTensor(a, &rhs);
        defer cb.free(value);
        var inputs = [_]interpreter.RuntimeInput{ .{ .node_id = x, .value = input }, .{ .node_id = state, .value = value } };
        held[iteration] = try prepared.run(a, .{ .runtime_inputs = try program.remapInputs(&inputs), .output_placement = .producer, .activation_readbacks = .forbidden, .collect_partition_stats = true });
        initialized += 1;
        try std.testing.expectEqual(@as(u64, 0), held[iteration].stats.device_host_accesses);
    }
    for (&held, 0..) |*result, iteration| {
        const owner = result.backendFor(&prepared.runtime, 0);
        try std.testing.expectEqual(ops.BackendKind.metal, owner.kind());
        const values = try owner.toFloat32(result.outputs[0], a);
        defer a.free(values);
        for (values, input_values) |value, expected| try std.testing.expectApproxEqAbs(expected * @as(f32, @floatFromInt(iteration + 1)), value, 1e-5);
    }
}

test "prepared physical CUDA replay owns address banks and preserves held outputs across eviction" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .cuda, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 4, 4 });
    const x = try b.parameter("x", shape);
    const state = try b.parameter("state", shape);
    const weight_node = try b.parameter("weight", shape);
    const projected = try b.matmul(x, weight_node);
    var out = try b.relu(try b.add(try b.mul(projected, state), x));
    for (0..8) |_| out = try b.relu(try b.add(out, x));
    const published = try b.reshape(out, ml.graph.Shape.init(.f32, &.{ 2, 8 }));
    try graph.markOutput(published);
    var program = try ModelProgram.init(a, &graph, &.{
        .{ .name = "x", .node_id = x, .kind = .input },
        .{ .name = "state", .node_id = state, .kind = .state_input },
        .{ .name = "weight", .node_id = weight_node, .kind = .weight },
        .{ .name = "out", .node_id = published, .kind = .output },
    });
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    const pool = executable.runtime.physical_pool.?;
    try std.testing.expect(pool.replay_backend != null);
    pool.max_frames = 2;
    const values = [_]f32{ -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7 };
    const input = try cb.fromFloat32Shape(&values, &.{ 4, 4 });
    defer cb.free(input);
    const weight = try cb.fromFloat32Shape(&.{ 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1 }, &.{ 4, 4 });
    var weight_owned = true;
    defer if (weight_owned) cb.free(weight);
    var held: [2]runtime_module.Result = undefined;
    var held_count: usize = 0;
    defer for (held[0..held_count]) |*result| result.deinit(&executable.runtime);
    const Run = struct {
        fn run(allocator: std.mem.Allocator, exec: *Executable, p: *const ModelProgram, backend: *const ops.ComputeBackend, input_value: ops.CT, weight_value: ops.CT, x_id: ml.graph.NodeId, state_id: ml.graph.NodeId, weight_id: ml.graph.NodeId, iteration: usize) !runtime_module.Result {
            const scale: [16]f32 = @splat(@floatFromInt(iteration + 1));
            const state_value = try backend.fromFloat32Shape(&scale, &.{ 4, 4 });
            defer backend.free(state_value);
            var inputs = [_]interpreter.RuntimeInput{ .{ .node_id = x_id, .value = input_value }, .{ .node_id = state_id, .value = state_value }, .{ .node_id = weight_id, .value = weight_value } };
            return exec.run(allocator, .{ .runtime_inputs = try p.remapInputs(&inputs), .output_placement = .producer, .activation_readbacks = .forbidden });
        }
        fn check(allocator: std.mem.Allocator, exec: *Executable, result: *const runtime_module.Result, original: []const f32, iteration: usize) !void {
            const actual = try result.backendFor(&exec.runtime, 0).toFloat32(result.outputs[0], allocator);
            defer allocator.free(actual);
            for (actual, original) |value, source| try std.testing.expectEqual(@max(@as(f32, 0), source * @as(f32, @floatFromInt(iteration + 10))), value);
        }
    };
    for (0..7) |iteration| {
        var result = try Run.run(a, &executable, &program, &cb, input, weight, x, state, weight_node, iteration);
        var owned = true;
        defer if (owned) result.deinit(&executable.runtime);
        try Run.check(a, &executable, &result, &values, iteration);
        try std.testing.expectEqual(@as(u64, 0), result.stats.device_host_accesses);
        if (iteration == 1 or iteration == 4) try std.testing.expectEqual(@as(u64, 1), result.stats.physical_graph_captures);
        if (iteration == 2 or iteration == 5) {
            try std.testing.expectEqual(@as(u64, 1), result.stats.physical_graph_replays);
            try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
            try std.testing.expectEqual(@as(u64, 2), result.stats.physical_input_copies);
            held[held_count] = result;
            held_count += 1;
            owned = false;
        }
        if (iteration == 6) try std.testing.expectEqual(@as(u64, 1), result.stats.physical_frame_busy_fallbacks);
    }
    try Run.check(a, &executable, &held[0], &values, 2);
    try Run.check(a, &executable, &held[1], &values, 5);
    held[0].deinit(&executable.runtime);
    held[0] = held[1];
    held_count = 1;
    var foreign = ops.physical_storage.CapturedLeases{};
    defer foreign.deinit(a);
    for (pool.frames.items[0].storages, 0..) |lease, id| if (lease) |value| {
        if (pool.plan.allocations[id].kind == .constant_binding) continue;
        try foreign.retain(a, value);
        break;
    };
    var blocked = try Run.run(a, &executable, &program, &cb, input, weight, x, state, weight_node, 7);
    defer blocked.deinit(&executable.runtime);
    try std.testing.expectEqual(@as(u64, 1), blocked.stats.physical_frame_busy_fallbacks);
    foreign.deinit(a);
    // Captured frames retain immutable weight handles independently of the
    // caller's original reference; replay must still read valid storage.
    cb.free(weight);
    weight_owned = false;
    var resumed = try Run.run(a, &executable, &program, &cb, input, weight, x, state, weight_node, 8);
    defer resumed.deinit(&executable.runtime);
    try std.testing.expectEqual(@as(u64, 1), resumed.stats.physical_graph_replays);
    try Run.check(a, &executable, &resumed, &values, 8);
    const published_shape = try cb.tensorShape(resumed.outputs[0], a);
    defer a.free(published_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 8 }, published_shape);
    executable.deinit();
    executable_owned = false;
    // Published views and their captured storage outlive the prepared owner.
    try Run.check(a, &executable, &held[0], &values, 5);
}

test "prepared physical CUDA capture failure falls back before submission and restores the context" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const Replay = ops.physical_storage.Replay;
    const Failure = struct {
        inner: Replay,
        var original_factory: @FieldType(ops.KernelBackend, "createPhysicalReplay") = null;
        const vtable = Replay.VTable{ .begin = begin, .commit = commit, .abort = abort, .launch = launch, .retains = retains, .deinit = deinit };
        fn create(ctx: *anyopaque) !?Replay {
            const inner = (try original_factory.?(ctx)) orelse return null;
            errdefer inner.deinit();
            const owner = try std.testing.allocator.create(@This());
            owner.* = .{ .inner = inner };
            return .{ .ptr = owner, .vtable = &vtable };
        }
        fn begin(raw: *anyopaque, leases: []const ?*ops.physical_storage.Lease) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.inner.begin(leases);
        }
        fn commit(_: *anyopaque) !void {
            // Simulate optional graph-executable memory pressure after real
            // CUDA recording, before any graph submission.
            return error.OutOfMemory;
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inner.abort();
        }
        fn launch(_: *anyopaque) !void {
            return error.TestUnexpectedResult;
        }
        fn retains(raw: *anyopaque, lease: *const ops.physical_storage.Lease) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.inner.retains(lease);
        }
        fn deinit(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inner.deinit();
            std.testing.allocator.destroy(self);
        }
    };
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .cuda, null);
    defer context.deinit(a);
    const original = context.computeBackend();
    var functions = original.vtable.kernels.?.*;
    Failure.original_factory = functions.createPhysicalReplay;
    functions.createPhysicalReplay = Failure.create;
    var failure_vtable = original.vtable.*;
    functions.implementation_table = &failure_vtable; // Explicit fixture physical-program opt-in.
    failure_vtable.kernels = &functions;
    var failing_backend = original;
    failing_backend.vtable = &failure_vtable;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 8 }));
    const positive = try b.absOp(x);
    var out = positive;
    for (0..8) |_| out = try b.relu(try b.add(out, positive));
    try graph.markOutput(out);
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var failed = try prepare(a, &program, &failing_backend, .partitioned);
    defer failed.deinit();
    var recovered = try prepare(a, &program, &original, .partitioned);
    defer recovered.deinit();
    failed.configureExecutionDiagnostics(true);
    recovered.configureExecutionDiagnostics(true);
    var retained_failure: ?@import("invocation_report.zig").Report = null;
    for ([_]*Executable{ &failed, &recovered }, 0..) |executable, mode| for (0..3) |iteration| {
        const values: [8]f32 = @splat(@floatFromInt(iteration + 1));
        const input = try original.fromFloat32Shape(&values, &.{ 1, 8 });
        defer original.free(input);
        var inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input }};
        var result = try executable.run(a, .{ .runtime_inputs = try program.remapInputs(&inputs), .activation_readbacks = .forbidden, .output_placement = .producer });
        defer result.deinit(&executable.runtime);
        const actual = try original.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual) |value| try std.testing.expectEqual(@as(f32, @floatFromInt(iteration + 1)) * 9, value);
        if (mode == 0 and iteration >= 1) {
            try std.testing.expectEqual(@import("stats.zig").PhysicalReplayDecision.capture_failed, result.stats.physical_replay);
            try std.testing.expectEqualStrings("OutOfMemory", result.stats.physical_replay_error.?);
            const report = executable.diagnostics.snapshot().?.report.?;
            try std.testing.expectEqualStrings("success", report.outcome);
            try std.testing.expectEqual(@as(u8, 0), report.failure.len);
            try std.testing.expectEqualStrings("capture_failed", report.program_route_decision.?);
            const reason = report.program_failure.?;
            try std.testing.expectEqualStrings("OutOfMemory", reason.bytes[0..reason.len]);
            try std.testing.expect(!reason.truncated);
            retained_failure = report;
            try std.testing.expectEqual(@as(u64, if (iteration == 1) 1 else 0), result.stats.physical_capture_failures);
            try std.testing.expectEqual(@as(u64, 0), result.stats.physical_graph_replays);
        }
        if (mode == 1) try std.testing.expect(executable.diagnostics.snapshot().?.report.?.program_failure == null);
        if (mode == 1 and iteration == 2) try std.testing.expectEqual(@as(u64, 1), result.stats.physical_graph_replays);
    };
    failed.configureExecutionDiagnostics(false);
    const retained_reason = retained_failure.?.program_failure.?;
    try std.testing.expectEqualStrings("OutOfMemory", retained_reason.bytes[0..retained_reason.len]);
}

test "assigned single-axis reductions use the Metal plan output and preserve held frames" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const ml = @import("ml");
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const values = [_]f32{ 1, -2, 4, -1, 3, -7, -5, 8, -9, 2, -3, 6 };
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&values, &.{ 2, 3, 2 }));
    defer cb.free(input);
    for ([_]ops.kernel_descriptor.ReductionMode{ .sum, .max, .mean }) |mode| {
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var b = ml.graph.Builder.init(&graph);
        const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3, 2 }));
        const output = switch (mode) {
            .sum => try b.reduceSum(x, &.{1}),
            .max => try b.reduceMax(x, &.{1}),
            .mean => try b.reduceMean(x, &.{1}),
        };
        try graph.markOutput(output);
        var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
        defer program.deinit();
        var raw_inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input, .source_backend = &cb }};
        const inputs = try program.remapInputs(&raw_inputs);
        var executable = try prepareWithOptions(a, &program, &cb, .compiled_preferred, .{ .input_bindings = inputs });
        defer executable.deinit();
        var held: ?@import("../backends/session.zig").ResidentOutputs = null;
        defer if (held) |*owner| owner.deinit();
        const expected: [4]f32 = switch (mode) {
            .sum => .{ 8, -10, -17, 16 },
            .max => .{ 4, -1, -3, 8 },
            .mean => .{ 8.0 / 3.0, -10.0 / 3.0, -17.0 / 3.0, 16.0 / 3.0 },
        };
        for (0..4) |iteration| {
            var result = try executable.run(a, .{ .runtime_inputs = inputs, .output_placement = .producer, .collect_partition_stats = true, .activation_readbacks = .forbidden });
            defer result.deinit(&executable.runtime);
            try std.testing.expectEqual(@as(u64, 1), result.stats.assigned_kernel_dispatches);
            try std.testing.expectEqual(@as(u64, 0), result.stats.device_host_accesses);
            try std.testing.expectEqual(@as(u64, 0), result.stats.boundary_output_materializations);
            if (iteration > 1) try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
            const actual = try cb.toFloat32(result.outputs[0], a);
            defer a.free(actual);
            for (expected, actual) |wanted, value| try std.testing.expectApproxEqAbs(wanted, value, 1e-6);
            if (iteration == 0) {
                held = .{ .outputs = result.outputs, .allocator = a, .backend = &cb };
                result.outputs = &.{};
            }
        }
        const retained = try cb.toFloat32(held.?.outputs[0], a);
        defer a.free(retained);
        for (expected, retained) |wanted, value| try std.testing.expectApproxEqAbs(wanted, value, 1e-6);
    }
}
test "prepared grouped projections retain resident assigned outputs across reuse and held frames" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    const a = std.testing.allocator;
    const input_values = [_]f32{ 1, 2, 3, 4, -4, -3, -2, -1, 0.5, -0.5, 1, -1 };
    const weights = [_]f32{ 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1, 1, -1, 1 };
    const expected = [_]f32{ 1, 2, 3, 4, 10, 2, -4, -3, -2, -1, -10, 2, 0.5, -0.5, 1, -1, 0, -3 };
    inline for (.{ .native, .metal, .cuda }) |kind| {
        if (comptime (kind == .native and !@import("build_options").enable_native) or
            (kind == .metal and !@import("build_options").enable_metal) or
            (kind == .cuda and !@import("build_options").enable_cuda)) continue;
        backend_arm: {
            var context = @import("../backends/compute_context.zig").BackendContext.init(a, kind, std.testing.io) catch |err| switch (@as(anyerror, err)) {
                error.CudaUnavailable, error.NoCudaDevices => if (kind == .cuda) break :backend_arm else return err,
                error.MetalUnavailable, error.MetalRuntimeUnavailable => if (kind == .metal) break :backend_arm else return err,
                else => return err,
            };
            defer context.deinit(a);
            const cb = context.computeBackend();
            const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&input_values, &.{ 3, 4 }));
            defer cb.free(input);
            const weight = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&weights, &.{ 6, 4 }));
            defer cb.free(weight);
            // Immutable matrix mirrors/packing are preparation work. Warm the
            // existing grouped route before enforcing activation-readback policy.
            const warmed = try cb.linearNoBiasGrouped(input, weight, 3, 4, 6, &.{ 2, 1, 3 }, 3);
            cb.free(warmed);
            var graph = ml.graph.Graph.init(a);
            defer graph.deinit();
            var b = ml.graph.Builder.init(&graph);
            const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 3, 4 }));
            const w = try b.parameter("w", ml.graph.Shape.init(.f32, &.{ 6, 4 }));
            const output = try graph.addNode(.{
                .op = .{ .fused_linear_no_bias = .{ .rows = 3, .in_dim = 4, .out_dim = 6, .projection_out_dims = .{ 2, 1, 3, 0 }, .num_projections = 3 } },
                .output_shape = ml.graph.Shape.init(.f32, &.{ 3, 6 }),
                .inputs = .{ x, w, ml.graph.null_node, ml.graph.null_node },
                .num_inputs = 2,
            });
            try graph.markOutput(output);
            var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .node_id = x, .kind = .input }, .{ .name = "w", .node_id = w, .kind = .weight } });
            defer program.deinit();
            var raw_inputs = [_]interpreter.RuntimeInput{ .{ .node_id = x, .value = input, .source_backend = &cb }, .{ .node_id = w, .value = weight, .source_backend = &cb } };
            const inputs = try program.remapInputs(&raw_inputs);
            var executable = try prepareWithOptions(a, &program, &cb, .partitioned, .{ .input_bindings = inputs, .physical_replay = false });
            defer executable.deinit();
            var held: ?@import("../backends/session.zig").ResidentOutputs = null;
            defer if (held) |*owner| owner.deinit();
            for (0..5) |iteration| {
                var result = try executable.run(a, .{ .runtime_inputs = inputs, .output_placement = .producer, .collect_partition_stats = true, .activation_readbacks = .forbidden });
                defer result.deinit(&executable.runtime);
                const owner = result.backendFor(&executable.runtime, 0);
                try std.testing.expectEqual(cb.kind(), owner.kind());
                try std.testing.expectEqual(@as(u64, 1), result.stats.assigned_kernel_dispatches);
                try std.testing.expectEqual(@as(u64, 0), result.stats.device_host_accesses);
                try std.testing.expectEqual(@as(u64, 0), result.stats.boundary_output_materializations);
                if (iteration > 1) try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
                const actual = try owner.toFloat32(result.outputs[0], a);
                defer a.free(actual);
                for (expected, actual) |wanted, value| try std.testing.expectApproxEqAbs(wanted, value, 1e-6);
                if (iteration == 0) {
                    held = .{ .outputs = result.outputs, .allocator = a, .backend = owner };
                    result.outputs = &.{};
                }
            }
            const retained = try held.?.backend.toFloat32(held.?.outputs[0], a);
            defer a.free(retained);
            for (expected, retained) |wanted, value| try std.testing.expectApproxEqAbs(wanted, value, 1e-6);
        }
    }
}

test "prepared CUDA gather uses assigned control scratch and validates current exact indices" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const ml = @import("ml");
    const tensor_mod = @import("../backends/tensor.zig");
    const Context = @import("../backends/compute_context.zig").BackendContext;
    const a = std.testing.allocator;
    var context = Context.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const cb = context.computeBackend();
    var cpu = try Context.init(a, .native, null);
    defer cpu.deinit(a);
    const reference_backend = cpu.computeBackend();
    var table = try tensor_mod.Tensor.initFloat32(a, "table", &.{ 4, 2 }, &.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    defer table.deinit();
    const device_table = try context.importHostTensor(a, &table);
    defer cb.free(device_table);
    const host_table = try cpu.importHostTensor(a, &table);
    defer reference_backend.free(host_table);
    inline for (.{ tensor_mod.DType.f32, tensor_mod.DType.i32, tensor_mod.DType.i64 }) |dtype| {
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const values_node = try builder.parameter("table", ml.graph.Shape.init(.f32, &.{ 4, 2 }));
        const index_node = try builder.parameter("indices", ml.graph.Shape.init(@field(ops.GraphDType, @tagName(dtype)), &.{3}));
        const gathered = try builder.gather(values_node, index_node, ml.graph.Shape.init(.f32, &.{ 3, 2 }));
        try graph.markOutput(gathered);
        var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "table", .node_id = values_node, .kind = .weight }, .{ .name = "indices", .node_id = index_node, .kind = .input }, .{ .name = "output", .node_id = gathered, .kind = .output } });
        defer program.deinit();
        var indices = switch (dtype) {
            .f32 => try tensor_mod.Tensor.initFloat32(a, "indices", &.{3}, &.{ 2.9, -1.9, 0.9 }),
            .i32 => try tensor_mod.Tensor.initInt32(a, "indices", &.{3}, &.{ 2, -1, 0 }),
            .i64 => try tensor_mod.Tensor.initInt64(a, "indices", &.{3}, &.{ 2, -1, 0 }),
            else => unreachable,
        };
        defer indices.deinit();
        const device_indices = try context.importHostTensor(a, &indices);
        defer cb.free(device_indices);
        const host_indices = try cpu.importHostTensor(a, &indices);
        defer reference_backend.free(host_indices);
        const reference = try reference_backend.primGather(host_table, host_indices, 0, &.{ 4, 2 });
        defer reference_backend.free(reference);
        const expected = try reference_backend.toFloat32(reference, a);
        defer a.free(expected);
        try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8, 1, 2 }, expected);
        var raw_inputs = [_]interpreter.RuntimeInput{ .{ .node_id = values_node, .value = device_table }, .{ .node_id = index_node, .value = device_indices } };
        const inputs = try program.remapInputs(&raw_inputs);
        var executable = try prepare(a, &program, &cb, .compiled_preferred);
        defer executable.deinit();
        const options = interpreter.ExecuteOptions{ .runtime_inputs = inputs, .output_placement = .producer, .collect_partition_stats = true, .activation_readbacks = .forbidden };
        for (0..3) |iteration| {
            var result = try executable.run(a, options);
            defer result.deinit(&executable.runtime);
            try std.testing.expectEqual(@as(u64, 1), result.stats.assigned_kernel_dispatches);
            try std.testing.expectEqual(@as(u64, 0), result.stats.interpreter_fallbacks);
            try std.testing.expectEqual(@as(u64, 0), result.stats.actual_upload_calls);
            try std.testing.expectEqual(@as(u64, 1), result.stats.actual_download_calls);
            try std.testing.expectEqual(@as(u64, indices.data.len), result.stats.actual_download_bytes);
            try std.testing.expectEqual(result.stats.device_host_access_bytes, result.stats.index_metadata_host_access_bytes);
            try std.testing.expectEqual(@as(u64, indices.data.len), result.stats.index_metadata_host_access_bytes);
            try std.testing.expectEqual(@as(u64, 0), result.stats.physical_graph_captures);
            const usage = result.stats.planned_storage.?;
            try std.testing.expectEqual(@as(?u64, indices.data.len), usage.host_workspace.backing_live_bytes);
            if (iteration > 0) {
                try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
                try std.testing.expect(result.stats.physical_storage_reuses > 0);
            }
            const actual = try result.backendFor(&executable.runtime, 0).toFloat32(result.outputs[0], a);
            defer a.free(actual);
            try std.testing.expectEqualSlices(f32, expected, actual);
        }
        var held = try executable.run(a, options);
        defer held.deinit(&executable.runtime);
        // Mutate the exact same device address. No cached host summary may
        // authorize these different indices or stale capture preconditions.
        var changed = switch (dtype) {
            .f32 => try tensor_mod.Tensor.initFloat32(a, "indices", &.{3}, &.{ 0, 1, 2 }),
            .i32 => try tensor_mod.Tensor.initInt32(a, "indices", &.{3}, &.{ 0, 1, 2 }),
            .i64 => try tensor_mod.Tensor.initInt64(a, "indices", &.{3}, &.{ 0, 1, 2 }),
            else => unreachable,
        };
        defer changed.deinit();
        const raw: *@import("../ops/cuda/cuda_compute.zig").CudaTensor = @ptrCast(@alignCast(device_indices));
        try raw.buffer.copyFromHost(&context.cuda.ctx, changed.data);
        var next = try executable.run(a, options);
        defer next.deinit(&executable.runtime);
        const actual_next = try next.backendFor(&executable.runtime, 0).toFloat32(next.outputs[0], a);
        defer a.free(actual_next);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6 }, actual_next);
        const retained = try held.backendFor(&executable.runtime, 0).toFloat32(held.outputs[0], a);
        defer a.free(retained);
        try std.testing.expectEqualSlices(f32, expected, retained);
        switch (dtype) {
            .f32 => std.mem.bytesAsSlice(f32, changed.data)[0] = 4,
            .i32 => std.mem.bytesAsSlice(i32, changed.data)[0] = 4,
            .i64 => std.mem.bytesAsSlice(i64, changed.data)[0] = 4,
            else => unreachable,
        }
        try raw.buffer.copyFromHost(&context.cuda.ctx, changed.data);
        try std.testing.expectError(error.IndexOutOfBounds, executable.run(a, options));
        if (comptime dtype == .f32) {
            std.mem.bytesAsSlice(f32, changed.data)[0] = std.math.nan(f32);
            try raw.buffer.copyFromHost(&context.cuda.ctx, changed.data);
            try std.testing.expectError(error.InvalidIndex, executable.run(a, options));
        }
        var all_reads = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        try @import("transfer_audit.zig").begin(&all_reads);
        const forbidden = executable.run(a, options);
        @import("transfer_audit.zig").end();
        try std.testing.expectError(error.DeviceReadbackForbidden, forbidden);
        try std.testing.expectEqual(@as(u64, 0), all_reads.download_calls);
    }
}

fn qualifyCudaVendorReplay(with_bias: bool, evict_before_capture: bool) !void {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    if (!@import("antfly_platform").env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CUBLASLT", true)) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = @import("../backends/compute_context.zig").BackendContext.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const compute = context.cuda;
    if (compute.ctx.info.compute_major < 8 or compute.cublaslt == null) return error.SkipZigTest;
    const blas = &compute.cublaslt.?;
    const cb = context.computeBackend();
    const rows = 128;
    const width = 64;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, width }));
    const weight_node = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{ width, width }));
    const bias_node = if (with_bias) try builder.parameter("bias", ml.graph.Shape.init(.f32, &.{width})) else ml.graph.null_node;
    var output = if (with_bias)
        try builder.linear(x, weight_node, bias_node, rows, width, width)
    else
        try builder.linearNoBias(x, weight_node, rows, width, width);
    // Retain enough real work to make replay profitable under the ordinary
    // launch/copy cost policy. No qualification-only cost override is used.
    for (0..16) |_| output = try builder.add(output, x);
    try graph.markOutput(output);
    const bindings = [_]@import("program.zig").Binding{
        .{ .name = "x", .kind = .input, .node_id = x },
        .{ .name = "weight", .kind = .weight, .node_id = weight_node },
        .{ .name = "bias", .kind = .weight, .node_id = bias_node },
        .{ .name = "output", .kind = .output, .node_id = output },
    };
    var program = try ModelProgram.init(a, &graph, if (with_bias) &bindings else &.{ bindings[0], bindings[1], bindings[3] });
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    executable.configureExecutionDiagnostics(true);
    var retained_failure: ?@import("invocation_report.zig").Report = null;
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    try std.testing.expect(pool.replay_backend != null);
    var identity: [width * width]f32 = @splat(0);
    for (0..width) |index| identity[index * width + index] = 1;
    const weight = try cb.fromFloat32Shape(&identity, &.{ width, width });
    defer cb.free(weight);
    const ones: [width]f32 = @splat(1);
    const bias = if (with_bias) try cb.fromFloat32Shape(&ones, &.{width}) else null;
    defer if (bias) |value| cb.free(value);
    var held: ?runtime_module.Result = null;
    defer if (held) |*result| result.deinit(&executable.runtime);
    var held_first: f32 = 0;
    for (0..4) |iteration| {
        var values: [rows * width]f32 = undefined;
        for (&values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(index % 17)) - 8 + @as(i32, @intCast(iteration)))) / 8;
        const input = try cb.fromFloat32Shape(&values, &.{ rows, width });
        defer cb.free(input);
        var inputs = [_]interpreter.RuntimeInput{
            .{ .node_id = x, .value = input },
            .{ .node_id = weight_node, .value = weight },
            .{ .node_id = bias_node, .value = bias orelse weight },
        };
        var result = try executable.run(a, .{
            .runtime_inputs = try program.remapInputs(if (with_bias) &inputs else inputs[0..2]),
            .output_placement = .producer,
            .activation_readbacks = .forbidden,
            .collect_partition_stats = true,
        });
        var result_owned = true;
        defer if (result_owned) result.deinit(&executable.runtime);
        const actual = try cb.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual, values) |found, original| try std.testing.expectEqual(original * 17 + @as(f32, if (with_bias) 1 else 0), found);
        try std.testing.expectEqual(@as(u64, 0), result.stats.device_host_accesses);
        if (iteration == 0) {
            // Prove that the real warm invocation used cuBLASLt, with no
            // scratch borrowed from the backend's growable vendor arena.
            try std.testing.expect(blas.tensor_core_plans.count() > 0);
            try std.testing.expectEqual(@as(usize, 0), compute.cublaslt_workspace_scratch.buffer.len);
            if (evict_before_capture) try std.testing.expect(try blas.reserveTensorCorePlanSlots(&compute.ctx, blas.plan_cache_capacity));
        }
        if (iteration == 1) {
            if (evict_before_capture) {
                try std.testing.expectEqual(@as(u64, 1), result.stats.physical_capture_failures);
                try std.testing.expectEqualStrings("CublasLtUnsupported", result.stats.physical_replay_error.?);
                const report = executable.diagnostics.snapshot().?.report.?;
                try std.testing.expectEqualStrings("success", report.outcome);
                try std.testing.expectEqual(@as(u8, 0), report.failure.len);
                try std.testing.expectEqualStrings("capture_failed", report.program_route_decision.?);
                const reason = report.program_failure.?;
                try std.testing.expectEqualStrings("CublasLtUnsupported", reason.bytes[0..reason.len]);
                try std.testing.expect(!reason.truncated);
                retained_failure = report;
                try std.testing.expectEqual(@as(u64, 0), result.stats.physical_graph_captures);
                // Ordinary execution rebuilt the same strict-F32 vendor plan.
                try std.testing.expect(blas.tensor_core_plans.count() > 0);
            } else {
                try std.testing.expectEqual(@as(u64, 1), result.stats.physical_graph_captures);
                try std.testing.expect(blas.plan_cache_hits > 0);
            }
        }
        if (iteration == 2 and !evict_before_capture) {
            try std.testing.expectEqual(@as(u64, 1), result.stats.physical_graph_replays);
            try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
            held_first = actual[0];
            held = result;
            result_owned = false;
        }
    }
    // Published values retain their storage after graph/plan retirement.
    executable.deinit();
    executable_owned = false;
    if (evict_before_capture) {
        const reason = retained_failure.?.program_failure.?;
        try std.testing.expectEqualStrings("CublasLtUnsupported", reason.bytes[0..reason.len]);
    }
    if (held) |*result| {
        const preserved = try cb.toFloat32(result.outputs[0], a);
        defer a.free(preserved);
        try std.testing.expectEqual(held_first, preserved[0]);
    }
}

test "prepared physical CUDA strict F32 vendor GEMMs capture without hidden workspace" {
    try qualifyCudaVendorReplay(false, false);
    try qualifyCudaVendorReplay(true, false);
}

test "prepared physical CUDA vendor cache invalidation aborts recording before schedule fallback" {
    try qualifyCudaVendorReplay(false, true);
    try qualifyCudaVendorReplay(true, true);
}

test "prepared physical Metal output banks pin metadata aliases across reuse and retirement" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    const negated = try builder.neg(x);
    const output = try builder.reshape(negated, ml.graph.Shape.init(.f32, &.{ 2, 8 }));
    try graph.markOutput(output);
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    pool.max_frames = 2;
    const ledger = pool.accounting.?;
    ledger.retain();
    defer ledger.release();
    var alias: ?ops.CT = null;
    defer if (alias) |value| cb.free(value);
    var expected: [16]f32 = undefined;
    for (0..4) |iteration| {
        var values: [16]f32 = undefined;
        for (&values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index + iteration * 16)) / 4;
        const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&values, &.{ 4, 4 }));
        defer cb.free(input);
        var inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input, .source_backend = &cb }};
        var result = try executable.run(a, .{
            .runtime_inputs = try program.remapInputs(&inputs),
            .output_placement = .producer,
            .collect_partition_stats = true,
            .activation_readbacks = .forbidden,
        });
        defer result.deinit(&executable.runtime);
        try std.testing.expectEqual(@as(u64, 1), result.stats.assigned_kernel_dispatches);
        try std.testing.expectEqual(@as(u64, 0), result.stats.graph_output_owned_copies);
        try std.testing.expectEqual(@as(u64, 0), result.stats.actual_download_bytes);
        if (iteration > 1) try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
        const actual = try cb.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual, values) |found, original| try std.testing.expectEqual(-original, found);
        if (iteration == 0) {
            @memcpy(&expected, actual);
            alias = (try cb.cloneTensorShape(result.outputs[0], &.{ 1, 16 })) orelse return error.MissingRetainedView;
        } else {
            const retained = try cb.toFloat32(alias.?, a);
            defer a.free(retained);
            try std.testing.expectEqualSlices(f32, &expected, retained);
        }
    }
    try std.testing.expectEqual(@as(usize, 2), pool.frames.items.len);
    executable.deinit();
    executable_owned = false;
    // Only the published backing span survives, without the model/executable
    // retaining the unrelated warm frame or a host mirror.
    try std.testing.expectEqual(@as(u64, 64), ledger.snapshot().activation.requested_live_bytes);
    const retained = try cb.toFloat32(alias.?, a);
    defer a.free(retained);
    try std.testing.expectEqualSlices(f32, &expected, retained);
    cb.free(alias.?);
    alias = null;
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().activation.requested_live_bytes);
}

test "prepared physical Metal preserves deferred multiply-add command fusion" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 4, 4 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const negated = try b.neg(x);
    const half = try b.scalarConst(.f32, 0.5);
    const scaled = try b.mul(negated, half);
    const sum = try b.add(scaled, y);
    try graph.markOutput(sum);
    var program = try ModelProgram.init(a, &graph, &.{
        .{ .name = "x", .node_id = x, .kind = .input },
        .{ .name = "y", .node_id = y, .kind = .input },
    });
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    _ = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    for (0..3) |iteration| {
        var host: [16]f32 = undefined;
        for (&host, 0..) |*value, index| value.* = @floatFromInt(index + iteration);
        const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&host, &.{ 4, 4 }));
        defer cb.free(input);
        var inputs = [_]interpreter.RuntimeInput{
            .{ .node_id = x, .value = input, .source_backend = &cb },
            .{ .node_id = y, .value = input, .source_backend = &cb },
        };
        var result = try executable.run(a, .{
            .runtime_inputs = try program.remapInputs(&inputs),
            .output_placement = .producer,
            .collect_partition_stats = true,
            .activation_readbacks = .forbidden,
        });
        defer result.deinit(&executable.runtime);
        const actual = try cb.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual, host) |found, original| try std.testing.expectEqual(original * 0.5, found);
        try std.testing.expectEqual(@as(u64, 0), result.stats.actual_download_bytes);
    }
}

test "prepared physical Metal declines banks in caller-owned unsubmitted frames" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const metal = @import("../backends/metal_runtime.zig");
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    try graph.markOutput(try b.neg(x));
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const host = [_]f32{ -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7 };
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&host, &.{ 4, 4 }));
    defer cb.free(input);
    for (0..2) |_| {
        try std.testing.expect(try cb.decoderRuntimeBeginFrame());
        defer cb.decoderRuntimeCancelFrame() catch {};
        var inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input, .source_backend = &cb }};
        var result = try executable.run(a, .{
            .runtime_inputs = try program.remapInputs(&inputs),
            .output_placement = .producer,
            .collect_partition_stats = true,
        });
        defer result.deinit(&executable.runtime);
        try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
        // The bank remains completely unallocated even if the caller defers
        // submission across more than one invocation.
        for (pool.frames.items) |frame| for (frame.storages) |allocation| try std.testing.expect(allocation == null);
        try cb.decoderRuntimeSubmitAndWaitFrame();
        const actual = try cb.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual, host) |found, original| try std.testing.expectEqual(-original, found);
    }
}

test "prepared physical Metal lease construction and retained views unwind every allocation failure" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&.{ 1, -2, 3, -4 }, &.{ 2, 2 }));
    defer cb.free(input);
    const Case = struct {
        fn run(allocator: std.mem.Allocator, backend: ops.ComputeBackend, source: ops.CT) !void {
            const compute: *@import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(backend.ptr));
            const previous = compute.allocator;
            compute.allocator = allocator;
            defer compute.allocator = previous;
            const ledger = try @import("storage_accounting.zig").Ledger.create(allocator);
            defer ledger.release();
            const lease = (try ops.physical_storage.Lease.createTracked(allocator, backend, 16, ledger, .activation)).?;
            var owner = true;
            defer if (owner) lease.release();
            const functions = backend.vtable.kernels.?;
            const output = (try functions.physicalTensorView.?(backend.ptr, .{ .storage = lease, .shape = &.{ 2, 2 } })).?;
            var output_owned = true;
            defer if (output_owned) backend.free(output);
            var descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .unary = .negate }, .input_count = 1, .input_shapes = undefined, .output_shape = @import("ml").graph.Shape.init(.f32, &.{ 2, 2 }) };
            descriptor.input_shapes[0] = descriptor.output_shape;
            try std.testing.expect(try functions.runAssignedKernel.?(backend.ptr, .{ .descriptor = &descriptor, .inputs = &.{source}, .output = output }));
            const retained = (try backend.cloneTensorShape(output, &.{4})).?;
            defer backend.free(retained);
            backend.free(output);
            output_owned = false;
            lease.release();
            owner = false;
            try std.testing.expectEqual(@as(u64, 16), ledger.snapshot().activation.requested_live_bytes);
            var values: [4]f32 = undefined;
            try std.testing.expect(try backend.readFloat32Into(retained, &values));
            try std.testing.expectEqualSlices(f32, &.{ -1, 2, -3, 4 }, &values);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{ cb, input });
}
test "prepared physical Metal executor allocation failures cancel owned frames before bank reuse" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const metal = @import("../backends/metal_runtime.zig");
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    const neg = try b.neg(x);
    const sum = try b.add(neg, x);
    try graph.markOutput(neg);
    try graph.markOutput(sum);
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&.{ -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7 }, &.{ 4, 4 }));
    defer cb.free(input);
    const Case = struct {
        fn run(allocator: std.mem.Allocator, prepared: *Executable, semantic: *const ModelProgram, backend: *const ops.ComputeBackend, input_node: ml.graph.NodeId, input_value: ops.CT) !void {
            var inputs = [_]interpreter.RuntimeInput{.{ .node_id = input_node, .value = input_value, .source_backend = backend }};
            defer {
                std.debug.assert(!backend.decoderRuntimeHasActiveFrame());
                if (prepared.runtime.physical_pool) |pool| for (pool.frames.items) |frame| std.debug.assert(!frame.in_use);
            }
            var result = try prepared.run(allocator, .{
                .runtime_inputs = try semantic.remapInputs(&inputs),
                .output_placement = .producer,
                .collect_partition_stats = true,
                .activation_readbacks = .forbidden,
            });
            defer result.deinit(&prepared.runtime);
            const zero = try backend.toFloat32(result.outputs[1], allocator);
            defer allocator.free(zero);
            for (zero) |value| try std.testing.expectEqual(@as(f32, 0), value);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{ &executable, &program, &cb, x, input });
}
test "prepared physical warm claims lend exact bank quota without a release or capacity gap" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const memory = @import("../runtime/tier/memory.zig");
    const admission = @import("storage_admission.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        current: memory.AdmissionLease = undefined,
        credit: memory.AdmissionAmounts = .{},
        refs: usize = 1,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: @import("storage_accounting.zig").Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.current, .{
                .controller = &self.controller,
                .backend_class = .cpu,
                .limits = .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 },
                .check_live_memory = false,
                .credit = &self.credit,
                .lifetime = .{ .context = self, .retain = retain, .release = release },
            }, kind, class, bytes);
        }
    };
    const Order = struct {
        admitted: bool = false,
        before_reuse: usize = 0,
        records: usize = 0,
        fn before(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.admitted) return error.DeviceFenceBeforeAdmission;
            self.before_reuse += 1;
        }
        fn record(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.records += 1;
        }
        fn deinit(_: *anyopaque) void {}
        const vtable = ops.physical_storage.Completion.VTable{ .record = record, .before_reuse = before, .deinit = deinit };
    };
    var order = Order{};
    var owner = Owner{};
    defer owner.controller.deinit();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    try graph.markOutput(try b.neg(x));
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    const pool = executable.runtime.physical_pool.?;
    pool.max_frames = 1;
    var held: ?ops.CT = null;
    defer if (held) |value| cb.free(value);
    const cap = admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
    for (0..3) |iteration| {
        order.admitted = false;
        var claim = try pool.claimWarm(cap.identity);
        defer if (claim) |*value| value.release();
        if (iteration == 0) {
            try std.testing.expect(claim == null);
            try std.testing.expectEqual(@as(usize, 0), pool.frames.items.len);
        } else {
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, claim.?.credit);
            try std.testing.expect((try pool.claimWarm(cap.identity)) == null);
            try std.testing.expect((try pool.acquire()) == null);
            if (iteration == 1) {
                var foreign = memory.AdmissionController{};
                defer foreign.deinit();
                var wrong_cap = cap;
                wrong_cap.identity.controller = &foreign;
                if (claim) |*value| {
                    try std.testing.expectError(error.PhysicalStorageAdmissionMismatch, pool.acquirePrepared(wrong_cap, value));
                    try std.testing.expect(value.frame != null);
                    const previous_nonce = value.nonce;
                    value.release();
                    claim = try pool.claimWarm(cap.identity);
                    try std.testing.expect(claim != null);
                    try std.testing.expect(claim.?.nonce != previous_nonce);
                }
            }
        }
        const incremental: usize = if (claim) |value| 64 - value.credit.host_scratch_bytes else 64;
        owner.current = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, .{ .host_scratch_bytes = incremental }, false);
        defer owner.current.release();
        order.admitted = true;
        owner.credit = .{ .host_scratch_bytes = incremental };
        var host: [16]f32 = undefined;
        for (&host, 0..) |*value, index| value.* = @floatFromInt(index + iteration);
        const input = try cb.fromFloat32Shape(&host, &.{ 4, 4 });
        defer cb.free(input);
        var inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input, .source_backend = &cb }};
        var result = try executable.run(a, .{
            .runtime_inputs = try program.remapInputs(&inputs),
            .storage_admission = cap,
            .physical_frame_claim = if (claim) |*value| value else null,
            .output_placement = .producer,
            .collect_partition_stats = true,
        });
        defer result.deinit(&executable.runtime);
        if (iteration == 0) pool.frames.items[0].completions[0] = .{ .ptr = &order, .vtable = &Order.vtable };
        try std.testing.expectEqual(iteration, order.before_reuse);
        try std.testing.expectEqual(iteration, order.records);
        try std.testing.expectEqual(@as(u64, if (iteration == 0) 1 else 0), result.stats.physical_storage_allocations);
        const actual = try cb.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual, host) |found, original| try std.testing.expectEqual(-original, found);
        if (iteration == 2) held = try cb.primReshape(result.outputs[0], &.{ 1, 16 });
        if (claim) |value| try std.testing.expect(value.frame == null);
        owner.current.release();
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, owner.controller.snapshot());
        try std.testing.expectEqual(@as(usize, 2), owner.refs);
    }
    // Published aliases block the loan while retaining the original quota.
    try std.testing.expect((try pool.claimWarm(cap.identity)) == null);
    executable.deinit();
    executable_owned = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, owner.controller.snapshot());
    cb.free(held.?);
    held = null;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), owner.refs);
}
test "prepared physical warm capacity excludes measured allocator padding" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const memory = @import("../runtime/tier/memory.zig");
    const admission = @import("storage_admission.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        current: memory.AdmissionLease = undefined,
        credit: memory.AdmissionAmounts = .{},
        refs: usize = 1,
        limit: usize = 192,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: @import("storage_accounting.zig").Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.current, .{
                .controller = &self.controller,
                .backend_class = .cpu,
                .limits = .{ .host_limit_bytes = self.limit, .combined_limit_bytes = self.limit },
                .check_live_memory = false,
                .credit = &self.credit,
                .lifetime = .{ .context = self, .retain = retain, .release = release },
            }, kind, class, bytes);
        }
    };
    const Order = struct {
        admitted: bool = false,
        before_reuse: usize = 0,
        records: usize = 0,
        fn before(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.admitted) return error.DeviceFenceBeforeAdmission;
            self.before_reuse += 1;
        }
        fn record(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.records += 1;
        }
        fn deinit(_: *anyopaque) void {}
        const vtable = ops.physical_storage.Completion.VTable{ .record = record, .before_reuse = before, .deinit = deinit };
    };
    var order = Order{};
    var owner = Owner{};
    defer owner.controller.deinit();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const original_backend = context.computeBackend();
    const Padded = struct {
        fn bound(_: *anyopaque, request: ops.physical_storage.Request) !usize {
            return std.math.add(usize, request.byte_size, 128);
        }
    };
    var kernels = original_backend.vtable.kernels.?.*;
    var physical_policy = kernels.physical_storage_policy.?.*;
    physical_policy.reservation_bytes = Padded.bound;
    kernels.physical_storage_policy = &physical_policy;
    var table = original_backend.vtable.*;
    kernels.implementation_table = &table; // Explicit fixture physical-program opt-in.
    table.kernels = &kernels;
    const cb = ops.ComputeBackend{ .ptr = original_backend.ptr, .vtable = &table };

    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 4, 4 }));
    try graph.markOutput(try b.neg(x));
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    const pool = executable.runtime.physical_pool.?;
    pool.max_frames = 1;
    try std.testing.expectEqual(@as(usize, 192), (try pool.storageRequirement()).host_scratch_bytes);
    var held: ?ops.CT = null;
    defer if (held) |value| cb.free(value);
    const cap = admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
    for (0..3) |iteration| {
        order.admitted = false;
        var claim = try pool.claimWarm(cap.identity);
        defer if (claim) |*value| value.release();
        if (iteration == 0) {
            try std.testing.expect(claim == null);
            try std.testing.expectEqual(@as(usize, 0), pool.frames.items.len);
        } else {
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, claim.?.credit);
            try std.testing.expect((try pool.claimWarm(cap.identity)) == null);
            try std.testing.expect((try pool.acquire()) == null);
            if (iteration == 1) {
                var foreign = memory.AdmissionController{};
                defer foreign.deinit();
                var wrong_cap = cap;
                wrong_cap.identity.controller = &foreign;
                if (claim) |*value| {
                    try std.testing.expectError(error.PhysicalStorageAdmissionMismatch, pool.acquirePrepared(wrong_cap, value));
                    try std.testing.expect(value.frame != null);
                    const previous_nonce = value.nonce;
                    value.release();
                    claim = try pool.claimWarm(cap.identity);
                    try std.testing.expect(claim != null);
                    try std.testing.expect(claim.?.nonce != previous_nonce);
                }
            }
        }
        const requirement = if (claim) |value|
            value.requirement orelse (try pool.storageRequirement())
        else
            try pool.storageRequirement();
        if (iteration != 0) try std.testing.expectEqual(@as(usize, 64), requirement.host_scratch_bytes);
        const incremental: usize = requirement.host_scratch_bytes - (if (claim) |value| @min(requirement.host_scratch_bytes, value.credit.host_scratch_bytes) else 0);
        owner.limit = if (iteration == 0) 192 else 64;
        owner.current = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = owner.limit, .combined_limit_bytes = owner.limit }, .{ .host_scratch_bytes = incremental }, false);
        defer owner.current.release();
        order.admitted = true;
        owner.credit = .{ .host_scratch_bytes = incremental };
        var host: [16]f32 = undefined;
        for (&host, 0..) |*value, index| value.* = @floatFromInt(index + iteration);
        const input = try cb.fromFloat32Shape(&host, &.{ 4, 4 });
        defer cb.free(input);
        var inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input, .source_backend = &cb }};
        var result = try executable.run(a, .{
            .runtime_inputs = try program.remapInputs(&inputs),
            .storage_admission = cap,
            .physical_frame_claim = if (claim) |*value| value else null,
            .output_placement = .producer,
            .collect_partition_stats = true,
        });
        defer result.deinit(&executable.runtime);
        if (iteration == 0) pool.frames.items[0].completions[0] = .{ .ptr = &order, .vtable = &Order.vtable };
        try std.testing.expectEqual(iteration, order.before_reuse);
        try std.testing.expectEqual(iteration, order.records);
        try std.testing.expectEqual(@as(u64, if (iteration == 0) 1 else 0), result.stats.physical_storage_allocations);
        const actual = try cb.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual, host) |found, original| try std.testing.expectEqual(-original, found);
        if (iteration == 2) held = try cb.primReshape(result.outputs[0], &.{ 1, 16 });
        if (claim) |value| try std.testing.expect(value.frame == null);
        owner.current.release();
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, owner.controller.snapshot());
        try std.testing.expectEqual(@as(usize, 2), owner.refs);
    }
    // Published aliases block the loan while retaining the original quota.
    try std.testing.expect((try pool.claimWarm(cap.identity)) == null);
    executable.deinit();
    executable_owned = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, owner.controller.snapshot());
    cb.free(held.?);
    held = null;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), owner.refs);
}

test "prepared immutable literal banks reuse once and publish independent held outputs" {
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const constant = try builder.tensorConst(&.{ 1, -2, 3, -4 }, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const input = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    try graph.markOutput(constant);
    try graph.markOutput(try builder.neg(input));
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = input, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .compiled_preferred);
    var owned = true;
    defer if (owned) executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    pool.max_frames = 2;
    const ledger = pool.accounting.?;
    ledger.retain();
    defer ledger.release();
    var held: ?ops.CT = null;
    defer if (held) |value| cb.free(value);
    const input_value = try cb.fromFloat32Shape(&.{ 8, 7, 6, 5 }, &.{ 2, 2 });
    defer cb.free(input_value);
    for (0..5) |iteration| {
        var bindings = [_]interpreter.RuntimeInput{.{ .node_id = input, .value = input_value, .source_backend = &cb }};
        var result = try executable.run(a, .{ .runtime_inputs = try program.remapInputs(&bindings), .output_placement = .producer });
        defer result.deinit(&executable.runtime);
        const output = try cb.toFloat32(result.outputs[0], a);
        defer a.free(output);
        try std.testing.expectEqualSlices(f32, &.{ 1, -2, 3, -4 }, output);
        try std.testing.expectEqual(@as(u64, 1), result.stats.graph_output_owned_copies);
        if (iteration > 1) try std.testing.expectEqual(@as(u64, 0), result.stats.physical_storage_allocations);
        if (iteration == 0) {
            held = try cb.primReshape(result.outputs[0], &.{ 1, 4 });
            // A public result is independently writable. This must never alter
            // the immutable literal that later invocations will borrow.
            try cb.vtable.kernels.?.copyPhysicalInput.?(cb.ptr, result.outputs[0], input_value);
        }
    }
    const changed = try cb.toFloat32(held.?, a);
    defer a.free(changed);
    try std.testing.expectEqualSlices(f32, &.{ 8, 7, 6, 5 }, changed);
    executable.deinit();
    owned = false;
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().constant.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 16), ledger.snapshot().activation.requested_live_bytes);
    cb.free(held.?);
    held = null;
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().activation.requested_live_bytes);
}

test "prepared immutable literal construction and independent publication unwind every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const ml = @import("ml");
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            const cb = context.computeBackend();
            var graph = ml.graph.Graph.init(a);
            defer graph.deinit();
            var b = ml.graph.Builder.init(&graph);
            const literal = try b.tensorConst(&.{ 1, 2, 3, 4 }, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
            try graph.markOutput(literal);
            var program = try ModelProgram.init(a, &graph, &.{});
            defer program.deinit();
            var executable = try prepare(a, &program, &cb, .compiled_preferred);
            defer executable.deinit();
            var result = try executable.run(a, .{ .output_placement = .producer });
            defer result.deinit(&executable.runtime);
            const values = try cb.toFloat32(result.outputs[0], a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prepared immutable literal selections lend only their own pinned bank credit" {
    const a = std.testing.allocator;
    const ml = @import("ml");
    const memory = @import("../runtime/tier/memory.zig");
    const admission = @import("storage_admission.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        lease: memory.AdmissionLease = undefined,
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: @import("storage_accounting.zig").Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.lease, .{ .controller = &self.controller, .backend_class = .cpu, .limits = .{}, .check_live_memory = false }, kind, class, bytes);
        }
    };
    var owner = Owner{};
    defer owner.controller.deinit();
    owner.lease = try owner.controller.tryAcquire(.cpu, .{}, .{}, false);
    defer owner.lease.release();
    const cap = admission.Capability{ .context = &owner, .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .services = &.{ .reserve = Owner.reserve } };
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    try graph.markOutput(try b.tensorConst(&.{ 1, 2, 3, 4 }, ml.graph.Shape.init(.f32, &.{ 2, 2 })));
    try graph.markOutput(try b.tensorConst(&.{ 5, 6, 7, 8, 9, 10, 11, 12 }, ml.graph.Shape.init(.f32, &.{ 2, 4 })));
    var program = try ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .compiled_preferred);
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    const store = executable.runtime.physical_pool.?.literal_store.?;
    try std.testing.expectEqual(@as(usize, 2), store.literals.len);
    for (store.literals, 0..) |_, index| _ = try store.value(index, cap, null);
    const Selection = @import("immutable_literals.zig").SharedLiteralStore.LiteralSelection;
    var mask = [_]bool{ true, false };
    const first = try Selection.create(a, store, &mask);
    defer first.release();
    mask = .{ false, true }; // Reusing the caller slice cannot alter first's mask.
    const second = try Selection.create(a, store, &mask);
    defer second.release();
    var first_claim = try store.claimImmutableFor(cap.identity, first);
    defer first_claim.release();
    var second_claim = try store.claimImmutableFor(cap.identity, second);
    defer second_claim.release();
    try std.testing.expectEqual(store.literals[0].data.len * 4, first_claim.totals.credit.host_scratch_bytes);
    try std.testing.expectEqual(store.literals[1].data.len * 4, second_claim.totals.credit.host_scratch_bytes);
    var all_claim = try store.claimImmutable(cap.identity);
    defer all_claim.release();
    try std.testing.expectEqual(@as(usize, 48), all_claim.totals.credit.host_scratch_bytes);
    executable.deinit();
    executable_owned = false;
    // Selection and claims own their bank independently of the evicted pool.
    try first_claim.validate(store, cap);
    try second_claim.validate(store, cap);
    try std.testing.expectEqual(@as(usize, 48), owner.controller.snapshot().host_scratch_bytes);
}

fn exercisePreparedRouteEviction(allocator: std.mem.Allocator) !void {
    const ml = @import("ml");
    const route = @import("kernel_route.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 1, 4 });
    const input_node = try builder.parameter("x", shape);
    const output_node = try builder.relu(input_node);
    try graph.markOutput(output_node);
    var program = try ModelProgram.init(allocator, &graph, &.{ .{ .name = "x", .node_id = input_node, .kind = .input }, .{ .name = "out", .node_id = output_node, .kind = .output } });
    defer program.deinit();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(allocator, .native, null);
    defer context.deinit(allocator);
    const backend = context.computeBackend();
    var executable = try prepareWithOptions(allocator, &program, &backend, .compiled_preferred, .{ .physical_replay = false });
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    pool.variant_cache.?.limit = 1;
    const node = program.id_map[input_node];
    const facts = [_]route.InputBinding{.{ .node_id = node, .value = .{ .imported_host = .{ .logical = shape, .target = .native } } }};
    const selected = (try pool.prepareRoutes(&facts)) orelse return error.MissingPreparedRoutes;
    var selected_owned = true;
    defer if (selected_owned) selected.release();
    const again = (try pool.prepareRoutes(&facts)) orelse return error.MissingPreparedRoutes;
    try std.testing.expect(selected == again);
    again.release();
    try selected.validate(&facts);
    try std.testing.expect(selected.pool.preparedKernelRoute(program.graph.outputs.items[0]) != null);
    var tensor = try Tensor.initFloat32(allocator, "x", &.{ 1, 4 }, &.{ -2, -1, 1, 2 });
    defer tensor.deinit();
    const input = try context.importHostTensor(allocator, &tensor);
    defer backend.free(input);
    const values = [_]interpreter.RuntimeInput{.{ .node_id = node, .value = input, .source_backend = &backend }};
    var result = try executable.run(allocator, .{ .runtime_inputs = &values, .prepared_routes = selected, .output_placement = .producer, .activation_readbacks = .forbidden });
    defer result.deinit(&executable.runtime);
    try std.testing.expect(result.stats.assigned_kernel_dispatches > 0);
    selected_owned = false;
    selected.release();
    // Eviction retires the entire old variant/frame while this published
    // output retains only its physical Lease and stable slot authority.
    const unknown = [_]route.InputBinding{.{ .node_id = node, .value = .unknown }};
    const second = (try pool.prepareRoutes(&unknown)) orelse return error.MissingPreparedRoutes;
    defer second.release();
    try std.testing.expectEqual(@as(u64, 1), pool.variant_cache.?.snapshot().evictions);
    const actual = try result.backendFor(&executable.runtime, 0).toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 1, 2 }, actual);
}

test "prepared exact Native routes intern metadata and published outputs survive variant eviction" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try exercisePreparedRouteEviction(std.testing.allocator);
}

test "prepared exact Native route owners unwind every metadata allocation failure" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exercisePreparedRouteEviction, .{});
}

test "prepared model parameter bindings cannot be overridden as runtime inputs" {
    const ml = @import("ml");
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const route = @import("kernel_route.zig");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 1, 4 });
    const x = try builder.parameter("x", shape);
    const weight = try builder.parameter("weight", shape);
    const output = try builder.add(x, weight);
    try graph.markOutput(output);
    var program = try ModelProgram.init(a, &graph, &.{
        .{ .name = "x", .node_id = x, .kind = .input },
        .{ .name = "weight", .node_id = weight, .kind = .weight },
        .{ .name = "output", .node_id = output, .kind = .output },
    });
    defer program.deinit();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    const bound = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 4 });
    defer backend.free(bound);
    const replacement = try backend.fromFloat32Shape(&.{ 10, 20, 30, 40 }, &.{ 1, 4 });
    defer backend.free(replacement);
    const weight_node = program.id_map[weight];
    const x_node = program.id_map[x];
    const immutable = [_]route.InputBinding{.{ .node_id = weight_node, .value = .{ .tensor = .{
        .value = bound,
        .implementation = .{ .context = backend.ptr, .table = backend.vtable },
    } } }};
    var executable = try prepareWithOptions(a, &program, &backend, .partitioned, .{
        .physical_replay = false,
        .immutable_input_bindings = &immutable,
    });
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const facts = [_]route.InputBinding{.{ .node_id = x_node, .value = .{ .imported_host = .{ .logical = shape, .target = .native } } }};
    const selected = (try pool.prepareRoutes(&facts)) orelse return error.MissingPreparedRoutes;
    defer selected.release();
    try std.testing.expectError(error.PreparedImmutableBindingChanged, selected.validateRuntimeInputs(&.{.{ .node_id = weight_node, .value = replacement }}, &backend));
    try std.testing.expectError(error.InvalidRuntimeKernelFact, pool.prepareRoutes(&.{.{ .node_id = weight_node, .value = .{ .imported_host = .{ .logical = shape, .target = .native } } }}));
    try selected.validateRuntimeInputs(&.{.{ .node_id = weight_node, .value = bound }}, &backend);
    try std.testing.expectEqual(@as(usize, 0), selected.pool.frames.items.len);
}

test "prepared dense Native reshape chains preserve exact downstream routes" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input_shape = ml.graph.Shape.init(.f32, &.{ 1, 4 });
    const output_shape = ml.graph.Shape.init(.f32, &.{ 2, 2 });
    const input_node = try builder.parameter("x", input_shape);
    const positive = try builder.relu(input_node);
    const alias = try builder.reshape(positive, output_shape);
    const output_node = try builder.neg(alias);
    try graph.markOutput(output_node);
    var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
    defer analysis.deinit(a);
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .compiled_preferred, .{ .physical_replay = false });
    defer executable.deinit();
    const facts = [_]@import("kernel_route.zig").InputBinding{.{ .node_id = input_node, .value = .{ .imported_host = .{ .logical = input_shape, .target = .native } } }};
    const selected = (try executable.runtime.physical_pool.?.prepareRoutes(&facts)).?;
    defer selected.release();
    try std.testing.expect(selected.pool.preparedKernelRoute(positive) != null);
    try std.testing.expect(selected.pool.preparedKernelRoute(output_node) != null);
    try std.testing.expectEqual(@as(u8, 1), selected.pool.selection.?.plane_counts[alias]);
    try std.testing.expect(selected.pool.selection.?.planes[alias][0].descriptor.shape.eq(output_shape));
    var tensor = try Tensor.initFloat32(a, "x", &.{ 1, 4 }, &.{ -2, -1, 1, 2 });
    defer tensor.deinit();
    const input = try context.importHostTensor(a, &tensor);
    defer backend.free(input);
    var result = try executable.run(a, .{ .runtime_inputs = &.{.{ .node_id = input_node, .value = input, .source_backend = &backend }}, .prepared_routes = selected, .output_placement = .producer, .activation_readbacks = .forbidden });
    defer result.deinit(&executable.runtime);
    const values = try result.backendFor(&executable.runtime, 0).toFloat32(result.outputs[0], a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, -1, -2 }, values);
    try std.testing.expectEqual(@as(u64, 2), result.stats.assigned_kernel_dispatches);
}

fn exercisePreparedTransientFrame(a: std.mem.Allocator) !void {
    const ml = @import("ml");
    const memory = @import("../runtime/tier/memory.zig");
    const admission = @import("storage_admission.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        current: memory.AdmissionLease = undefined,
        credit: memory.AdmissionAmounts = .{},
        refs: usize = 1,
        supplementary: bool = false,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: ops.BackendKind, class: @import("storage_accounting.zig").Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const reservation = try admission.fromRunLease(allocator, &self.current, .{ .controller = &self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 80, .combined_limit_bytes = 80 }, .check_live_memory = false, .credit = &self.credit, .lifetime = .{ .context = self, .retain = retain, .release = release } }, kind, class, bytes);
            self.supplementary = self.supplementary or reservation.leases[1] != null;
            return reservation;
        }
    };
    var owner = Owner{};
    defer owner.controller.deinit();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const input_node = try builder.parameter("x", shape);
    try graph.markOutput(try builder.neg(input_node));
    var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
    defer analysis.deinit(a);
    var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .compiled_preferred, .{ .physical_replay = false });
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    const pool = executable.runtime.physical_pool.?;
    const facts = [_]@import("kernel_route.zig").InputBinding{.{ .node_id = input_node, .value = .{ .imported_host = .{ .logical = shape, .target = .native } } }};
    const selected = (try pool.prepareRoutes(&facts)).?;
    var selected_owned = true;
    defer if (selected_owned) selected.release();
    const cold = try selected.storageRequirement();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 16 }, cold);
    const input = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
    defer backend.free(input);
    var results: [5]?runtime_module.Result = @splat(null);
    defer for (&results) |*maybe| if (maybe.*) |*result| result.deinit(&executable.runtime);
    const capability = admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
    for (&results, 0..) |*result, index| {
        // Every call has full cold admission and no warm claim; the fifth
        // request must partition this exact token, never admit a new deficit.
        owner.current = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 80, .combined_limit_bytes = 80 }, cold, false);
        defer owner.current.release();
        owner.credit = cold;
        result.* = try executable.run(a, .{ .runtime_inputs = &.{.{ .node_id = input_node, .value = input, .source_backend = &backend }}, .prepared_routes = selected, .storage_admission = capability, .output_placement = .producer, .collect_partition_stats = true, .activation_readbacks = .forbidden });
        try std.testing.expectEqual(@as(u64, 0), result.*.?.stats.physical_frame_busy_fallbacks);
        try std.testing.expectEqual(@as(u64, 0), result.*.?.stats.physical_graph_replays);
        try std.testing.expectEqual(@as(u64, 0), result.*.?.stats.physical_graph_captures);
        try std.testing.expectEqual(@as(usize, @min(index + 1, 4)), selected.pool.frames.items.len);
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = (index + 1) * 16 }, owner.controller.snapshot());
    }
    try std.testing.expect(!owner.supplementary);
    const actual = try results[4].?.backendFor(&executable.runtime, 0).toFloat32(results[4].?.outputs[0], a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4 }, actual);
    results[4].?.deinit(&executable.runtime);
    results[4] = null;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, owner.controller.snapshot());
    for (results[0..4]) |*maybe| {
        maybe.*.?.deinit(&executable.runtime);
        maybe.* = null;
    }
    selected_owned = false;
    selected.release();
    executable_owned = false;
    executable.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), owner.refs);
}

test "prepared fifth held Native output uses an admitted transient exact frame" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try exercisePreparedTransientFrame(std.testing.allocator);
}

test "prepared transient exact frame unwinds every allocation failure without replay" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exercisePreparedTransientFrame, .{});
}

test "prepared CUDA host ingress uploads once without replay and preserves held output banks" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = @import("../backends/compute_context.zig").BackendContext.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = @import("ml").graph.Graph.init(a);
    defer graph.deinit();
    var b = @import("ml").graph.Builder.init(&graph);
    const x = try b.parameter("x", @import("ml").graph.Shape.init(.f32, &.{ 1, 8 }));
    try graph.markOutput(try b.neg(x));
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepareWithOptions(a, &program, &cb, .partitioned, .{ .physical_replay = false });
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const routes = (try pool.prepareRoutes(&.{.{ .node_id = x, .value = .{ .imported_host = .{ .logical = graph.node(x).output_shape, .target = .cuda, .owned_ingress = true } } }})) orelse return error.MissingPreparedKernelRoute;
    defer routes.release();
    const legacy = (try pool.prepareRoutes(&.{.{ .node_id = x, .value = .{ .imported_host = .{ .logical = graph.node(x).output_shape, .target = .cuda } } }})) orelse return error.MissingPreparedKernelRoute;
    defer legacy.release();
    try std.testing.expect(legacy != routes);
    try std.testing.expectEqual(@as(usize, 0), legacy.pool.host_input_plans.len);
    try std.testing.expect(!routes.pool.hostIngressBackingKnown());
    const direct_requirement = try routes.storageRequirement();
    const legacy_requirement = try legacy.storageRequirement();
    try std.testing.expectEqual(legacy_requirement.host_scratch_bytes + 32, direct_requirement.host_scratch_bytes);
    try std.testing.expectEqual(legacy_requirement.backend_scratch_bytes + 32, direct_requirement.backend_scratch_bytes);
    routes.pool.max_frames = 1;
    var input = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{ 1, 8 }, &.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    defer input.deinit();
    const ingress = [_]@import("host_ingress.zig").Input{.{ .node_id = x, .value = &input }};
    try std.testing.expect(routes.pool.supportsHostInputs(&ingress));
    const before = context.cuda.stats.d2d_bytes;
    var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
    try @import("transfer_audit.zig").begin(&audit);
    var first = executable.run(a, .{ .host_inputs = &ingress, .prepared_routes = routes, .collect_partition_stats = true, .activation_readbacks = .forbidden }) catch |err| {
        @import("transfer_audit.zig").end();
        return err;
    };
    @import("transfer_audit.zig").end();
    defer first.deinit(&executable.runtime);
    try std.testing.expectEqual(@as(u64, 1), audit.upload_calls);
    try std.testing.expectEqual(@as(u64, 32), audit.upload_bytes);
    try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
    try std.testing.expectEqual(before, context.cuda.stats.d2d_bytes);
    try std.testing.expectEqual(@as(u64, 0), first.stats.physical_input_copies);
    // Held first output prevents bank reuse; the second exact plan uses its
    // admitted transient frame, leaving the first result unchanged.
    input.asFloat32Mut()[0] = 9;
    var second = try executable.run(a, .{ .host_inputs = &ingress, .prepared_routes = routes, .collect_partition_stats = true });
    defer second.deinit(&executable.runtime);
    const first_values = try cb.toFloat32(first.outputs[0], a);
    defer a.free(first_values);
    const second_values = try cb.toFloat32(second.outputs[0], a);
    defer a.free(second_values);
    try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4, -5, -6, -7, -8 }, first_values);
    try std.testing.expectEqualSlices(f32, &.{ -9, -2, -3, -4, -5, -6, -7, -8 }, second_values);
}

test "prepared CUDA host ingress keeps one upload through graph capture and replay" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = @import("../backends/compute_context.zig").BackendContext.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = @import("ml").graph.Graph.init(a);
    defer graph.deinit();
    var b = @import("ml").graph.Builder.init(&graph);
    const x = try b.parameter("x", @import("ml").graph.Shape.init(.f32, &.{ 1, 8 }));
    const positive = try b.absOp(x);
    var out = positive;
    for (0..8) |_| out = try b.relu(try b.add(out, positive));
    try graph.markOutput(out);
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const routes = (try pool.prepareRoutes(&.{.{ .node_id = x, .value = .{ .imported_host = .{ .logical = graph.node(x).output_shape, .target = .cuda, .owned_ingress = true } } }})) orelse return error.MissingPreparedKernelRoute;
    defer routes.release();
    try std.testing.expect(routes.pool.replay_backend != null);
    var input = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{ 1, 8 }, &.{ 1, 1, 1, 1, 1, 1, 1, 1 });
    defer input.deinit();
    const ingress = [_]@import("host_ingress.zig").Input{.{ .node_id = x, .value = &input }};
    // Cold immutable initialization is measured separately from mutable ingress.
    // This optimized graph legitimately retains one shared scalar ReLU literal.
    for (program.graph.nodes.items, 0..) |node, id| {
        const live = routes.pool.coverage_reachable.?[id];
        const replay_index = routes.pool.replay_input_indices[id];
        const literal = if (replay_index != @import("ml").graph.null_node) routes.pool.replay_inputs[replay_index].constant != null else false;
        std.debug.print("CUDA_REPLAY_IR id={d} op={s} inputs={any} vjp={d} reachable={} route={} literal={}\n", .{ id, @tagName(node.op), node.getInputs(), node.vjp_alternate, live, routes.pool.preparedKernelRoute(@intCast(id)) != null, literal });
    }
    {
        const d2d = context.cuda.stats.d2d_bytes;
        var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        try @import("transfer_audit.zig").begin(&audit);
        var cold = executable.run(a, .{ .host_inputs = &ingress, .prepared_routes = routes, .collect_partition_stats = true, .activation_readbacks = .forbidden }) catch |err| {
            @import("transfer_audit.zig").end();
            return err;
        };
        @import("transfer_audit.zig").end();
        defer cold.deinit(&executable.runtime);
        std.debug.print("CUDA_INGRESS_COLD uploads={d} bytes={d} capture={d} replay={d} decision={s}\n", .{ audit.upload_calls, audit.upload_bytes, cold.stats.physical_graph_captures, cold.stats.physical_graph_replays, @tagName(cold.stats.physical_replay) });
        try std.testing.expectEqual(@as(u64, 2), audit.upload_calls);
        try std.testing.expectEqual(@as(u64, 36), audit.upload_bytes);
        try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
        try std.testing.expectEqual(d2d, context.cuda.stats.d2d_bytes);
        try std.testing.expectEqual(@as(u64, 0), cold.stats.physical_graph_captures);
        try std.testing.expectEqual(@as(u64, 0), cold.stats.physical_graph_replays);
    }
    var captures: u64 = 0;
    var replays: u64 = 0;
    for (0..4) |iteration| {
        @memset(input.asFloat32Mut(), @floatFromInt(iteration + 1));
        const before = context.cuda.stats.d2d_bytes;
        var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        try @import("transfer_audit.zig").begin(&audit);
        var result = executable.run(a, .{ .host_inputs = &ingress, .prepared_routes = routes, .collect_partition_stats = true, .activation_readbacks = .forbidden }) catch |err| {
            @import("transfer_audit.zig").end();
            return err;
        };
        @import("transfer_audit.zig").end();
        defer result.deinit(&executable.runtime);
        std.debug.print("CUDA_INGRESS_CAPTURE iteration={d} uploads={d} upload_bytes={d} downloads={d} d2d_delta={d} copies={d} captures={d} replays={d}\n", .{ iteration, audit.upload_calls, audit.upload_bytes, audit.download_calls, context.cuda.stats.d2d_bytes - before, result.stats.physical_input_copies, result.stats.physical_graph_captures, result.stats.physical_graph_replays });
        if (audit.upload_calls != 1) return error.CudaHostIngressUploadMultiplicity;
        try std.testing.expectEqual(@as(u64, 32), audit.upload_bytes);
        std.debug.print("CUDA_INGRESS_DECISION {s}\n", .{@tagName(result.stats.physical_replay)});
        try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
        try std.testing.expectEqual(before, context.cuda.stats.d2d_bytes);
        try std.testing.expectEqual(@as(u64, 0), result.stats.physical_input_copies);
        captures += result.stats.physical_graph_captures;
        replays += result.stats.physical_graph_replays;
        const values = try cb.toFloat32(result.outputs[0], a);
        defer a.free(values);
        for (values) |value| try std.testing.expectEqual(@as(f32, @floatFromInt(9 * (iteration + 1))), value);
    }
    std.debug.print("CUDA_INGRESS_CAPTURE_TOTAL captures={d} replays={d}\n", .{ captures, replays });
    if (captures != 1) return error.CudaHostIngressCaptureMultiplicity;
    if (replays < 2) return error.CudaHostIngressInsufficientReplay;
}

test "direct executable diagnostics replace success with actual failed invocation telemetry" {
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const y = try builder.relu(x);
    try graph.markOutput(y);
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .node_id = x, .kind = .input }, .{ .name = "y", .node_id = y, .kind = .output } });
    defer program.deinit();
    var executable = try prepare(a, &program, &backend, .partitioned);
    defer executable.deinit();
    executable.configureExecutionDiagnostics(true);
    const input = try backend.fromFloat32Shape(&.{ -1, 2, -3, 4 }, &.{4});
    defer backend.free(input);
    var bound = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input }};
    var result = try executable.run(a, .{ .runtime_inputs = try program.remapInputs(&bound) });
    result.deinit(&executable.runtime);
    const successful = executable.diagnostics.snapshot().?;
    try std.testing.expectEqual(.success, successful.outcome);
    try std.testing.expect(successful.report.?.joined_native_workers != null);
    try std.testing.expect(successful.report.?.device_allocator_roots != null);
    try std.testing.expect(successful.report.?.owned_program_fallbacks != null);
    const invalid = [_]interpreter.RuntimeInput{.{ .node_id = std.math.maxInt(ml.graph.NodeId), .value = input }};
    if (executable.run(a, .{ .runtime_inputs = &invalid })) |unexpected| {
        var owned = unexpected;
        owned.deinit(&executable.runtime);
        return error.ExpectedRuntimeInputFailure;
    } else |failure| {
        const measured = executable.diagnostics.snapshot().?;
        try std.testing.expectEqual(.failed, measured.outcome);
        try std.testing.expectEqualStrings(@errorName(failure), measured.error_name.?.view());
        try std.testing.expect(measured.execution == null);
        try std.testing.expect(measured.runtime.device_owners != null);
    }
    try std.testing.expect(!@import("device_owner_registry.zig").enabled());
    try std.testing.expect(!@import("device_allocation_registry.zig").enabled());
    try std.testing.expect(!@import("runtime_observation.zig").kernelEnabled());
}

fn configureOwnedCoverage(executable: *Executable, graph: *const @import("ml").graph.Graph, analysis: interpreter.CachedAnalysis) !void {
    if (comptime @import("build_options").enable_wasm) return;
    const pool = executable.runtime.physical_pool orelse return;
    pool.coverage_graph = graph;
    pool.coverage_reachable = analysis.reachable;
    pool.native_frame_dispatch = false;
    pool.cuda_frame_dispatch = false;
    pool.metal_frame_dispatch = false;
    pool.coverage_backend = .{ .ptr = executable.runtime.default_backend.ptr, .vtable = executable.runtime.default_backend.vtable };
    if (comptime @import("build_options").enable_metal) {
        if (executable.runtime.default_backend.kind() == .metal and @import("device_owned_coverage.zig").services(executable.runtime.default_backend.*) != null) {
            const plan = executable.runtime.plan orelse return;
            const mesh = executable.runtime.mesh orelse return;
            const entry = executable.runtime.runtime_cache_entry orelse return;
            const compiled = entry.compiled_partitions orelse return;
            if (plan.device_assignment.len != plan.base.partitions.len or plan.base.node_assignment.len != graph.nodeCount() or analysis.reachable.len != graph.nodeCount() or pool.plan.transfers.len != 0) return;
            var targets: usize = 0;
            for (mesh.devices, 0..) |device, index| {
                for (mesh.devices[0..index]) |previous| if (previous.id == device.id) return;
                if (device.backend.ptr == executable.runtime.default_backend.ptr and device.backend.vtable == executable.runtime.default_backend.vtable) {
                    if (device.id != 0 or device.kind != .metal) return;
                    targets += 1;
                }
            }
            if (targets != 1) return;
            // Nonblocking compatibility decline if this cold executable was built
            // beneath an external owner. Never wait while an unknown outer lane is
            // held, and never reinterpret its legacy scope as managed authority.
            var metadata = @import("backend_operation.zig").enter(executable.runtime.default_backend.*, graph, null, .metadata) catch |err| switch (err) {
                error.QueueFull => return,
                else => return err,
            };
            defer metadata.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            if (metadata.token() == null) return;
            for (plan.base.partitions, 0..) |part, index| {
                var active = false;
                for (part.node_ids) |id| {
                    if (id >= analysis.reachable.len) return;
                    if (!analysis.reachable[id]) continue;
                    if (plan.base.node_assignment[id] != index or graph.node(id).op == .fused_from_float32) return;
                    active = true;
                }
                if (!active) continue;
                if (part.backend != .metal or plan.device_assignment[index] != 0) return;
                const actual = part.executor orelse return;
                var owned = false;
                for (compiled) |cached| if (cached.partition_idx == index and actual.ptr == cached.executor.ptr and actual.vtable == cached.executor.vtable) {
                    owned = true;
                };
                if (!owned or !try @import("../graph/metal_partition_executor.zig").MetalPartitionExecutor.attestOwnedFrameDispatch(actual, graph, executable.runtime.default_backend, part.node_ids, analysis.reachable, analysis.last_use)) return;
            }
            for (analysis.reachable, 0..) |active, id| {
                if (!active) continue;
                const assigned = plan.base.node_assignment[id];
                if (assigned >= plan.base.partitions.len) return;
                var count: usize = 0;
                for (plan.base.partitions[assigned].node_ids) |node| if (node == id) {
                    count += 1;
                };
                if (count != 1) return;
            }
            pool.metal_frame_dispatch = true;
            return;
        }
    }
    // CUDA's ordinary per-node branch calls interpreter.executeNode and its
    // Frame first. No compiled executor/fusion may bypass the selected route.
    if (@import("cuda_owned_coverage.zig").services(executable.runtime.default_backend.*) != null) {
        const plan = executable.runtime.plan orelse return;
        const mesh = executable.runtime.mesh orelse return;
        if (plan.device_assignment.len != plan.base.partitions.len or plan.base.node_assignment.len != graph.nodeCount() or analysis.reachable.len != graph.nodeCount()) return;
        // The runtime always catalogs an independently owned Native fallback
        // when available. Its mere presence does not execute or transfer data.
        // However runtimeInputDevice(null source_backend) resolves to literal
        // DeviceId0, and explicit inputs resolve through the first exact owner
        // match. Require one unambiguous target at0 and EVERY active part at0.
        // Unused catalog entries keep their existing fallback behavior.
        var target_entries: usize = 0;
        for (mesh.devices, 0..) |entry, entry_index| {
            for (mesh.devices[0..entry_index]) |previous| if (previous.id == entry.id) return;
            if (entry.backend.ptr == executable.runtime.default_backend.ptr and entry.backend.vtable == executable.runtime.default_backend.vtable) {
                if (entry.id != 0 or entry.kind != .cuda) return;
                target_entries += 1;
            }
        }
        if (target_entries != 1) return;
        for (plan.base.partitions, 0..) |part, index| {
            var active = false;
            for (part.node_ids) |id| {
                if (id >= analysis.reachable.len) return;
                if (!analysis.reachable[id]) continue;
                if (plan.base.node_assignment[id] != index or graph.node(id).op == .fused_from_float32) return;
                active = true;
            }
            if (!active) continue;
            if (part.backend != .cuda or part.executor != null or plan.device_assignment[index] != 0) return;
            const device = mesh.device(plan.device_assignment[index]) orelse return;
            if (device.kind != .cuda or device.backend.ptr != executable.runtime.default_backend.ptr or device.backend.vtable != executable.runtime.default_backend.vtable) return;
        }
        for (analysis.reachable, 0..) |active, id| {
            if (!active) continue;
            const assigned = plan.base.node_assignment[id];
            if (assigned >= plan.base.partitions.len) return;
            var count: usize = 0;
            for (plan.base.partitions[assigned].node_ids) |node| if (node == id) {
                count += 1;
            };
            if (count != 1) return;
        }
        pool.cuda_frame_dispatch = true;
        return;
    }
    if (executable.runtime.default_backend.vtable != &@import("../ops/native_compute.zig").vtable_impl) return;
    const plan = executable.runtime.plan orelse return;
    if (analysis.reachable.len != graph.nodeCount()) return;
    for (plan.base.partitions) |part| {
        var executed = false;
        for (part.node_ids) |id| {
            if (id >= analysis.reachable.len) return;
            executed = executed or analysis.reachable[id];
        }
        if (!executed) continue;
        if (part.backend != .native) return;
        const actual = part.executor orelse return;
        var found = false;
        for (executable.runtime.native_partition_executors.items) |owner| {
            if (actual.ptr == @as(*anyopaque, @ptrCast(owner)) and actual.vtable == owner.pe.vtable and owner.graph == graph and owner.backend.ptr == executable.runtime.default_backend.ptr and owner.backend.vtable == &@import("../ops/native_compute.zig").vtable_impl) {
                for (pool.backends) |backend| if (backend.ptr == owner.backend.ptr and backend.vtable == owner.backend.vtable) {
                    found = true;
                };
            }
        }
        if (!found) return;
    }
    pool.native_frame_dispatch = true;
}

test "owned payload coverage follows exact Native routes publication aliases and actual executor identity" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const coverage = @import("owned_coverage.zig");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const negative = try builder.neg(input);
    const alias = try builder.reshape(negative, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const literal = try builder.tensorConst(&.{ 1, 2, 3, 4 }, ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(alias);
    try graph.markOutput(literal);
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = input, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const x = program.id_map[input];
    const constant = program.id_map[literal];
    const bindings = [_]@import("kernel_route.zig").InputBinding{.{ .node_id = x, .value = .{ .imported_host = .{ .logical = program.graph.node(x).output_shape, .target = .native, .owned_ingress = true } } }};
    const routes = (try pool.prepareRoutes(&bindings)) orelse return error.MissingPreparedKernelRoute;
    defer routes.release();
    const complete = routes.pool.completeOwnedPayload() orelse return error.MissingOwnedPayloadWitness;
    try std.testing.expectEqual(routes.pool.selection.?.epoch, complete.epoch);
    // Allocation sizes and descriptor counts stay unchanged when the actual
    // selected operation or publication owner is absent.
    var operation_id: ?usize = null;
    for (routes.pool.selection.?.routes, 0..) |owned, id| if (owned != null) {
        operation_id = id;
        break;
    };
    const selected_operation = operation_id orelse return error.MissingSelectedNativeOperation;
    {
        const operation = routes.pool.selection.?.routes[selected_operation];
        routes.pool.selection.?.routes[selected_operation] = null;
        defer routes.pool.selection.?.routes[selected_operation] = operation;
        try std.testing.expect(coverage.assess(routes.pool) == .compatibility);
    }
    const store = routes.pool.literal_store orelse return error.MissingLiteralStore;
    if (constant >= store.publications.len) return error.MissingLiteralPublication;
    {
        const publication = store.publications[constant];
        store.publications[constant] = null;
        defer store.publications[constant] = publication;
        try std.testing.expectEqual(coverage.Reason.output_publication, coverage.assess(routes.pool).compatibility);
    }
    try std.testing.expect(coverage.assess(routes.pool) == .complete);
    const unknown = (try pool.prepareRoutes(&.{.{ .node_id = x, .value = .unknown }})) orelse return error.MissingPreparedKernelRoute;
    defer unknown.release();
    try std.testing.expect(unknown.pool.completeOwnedPayload() == null);
    const Policy = struct {
        fn deny(_: *anyopaque, _: ops.CT) !ops.CT {
            return error.CustomNumericsMustNotBeBypassed;
        }
    };
    var table = cb.vtable.*;
    table.negate = Policy.deny;
    var custom = cb;
    custom.vtable = &table;
    var custom_executable = try prepare(a, &program, &custom, .partitioned);
    defer custom_executable.deinit();
    if (custom_executable.runtime.physical_pool) |custom_pool| {
        const selected = (try custom_pool.prepareRoutes(&bindings)) orelse return error.MissingPreparedKernelRoute;
        defer selected.release();
        try std.testing.expect(selected.pool.completeOwnedPayload() == null);
    }
    var unsupported_graph = ml.graph.Graph.init(a);
    defer unsupported_graph.deinit();
    var b = ml.graph.Builder.init(&unsupported_graph);
    const source = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    try unsupported_graph.markOutput(try b.absOp(try b.transpose(try b.neg(source), &.{ 1, 0 })));
    var unsupported_program = try ModelProgram.init(a, &unsupported_graph, &.{.{ .name = "x", .node_id = source, .kind = .input }});
    defer unsupported_program.deinit();
    var unsupported = try prepare(a, &unsupported_program, &cb, .partitioned);
    defer unsupported.deinit();
    const unsupported_pool = unsupported.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const source_id = unsupported_program.id_map[source];
    const partial = (try unsupported_pool.prepareRoutes(&.{.{ .node_id = source_id, .value = .{ .imported_host = .{ .logical = unsupported_program.graph.node(source_id).output_shape, .target = .native, .owned_ingress = true } } }})) orelse return error.MissingPreparedKernelRoute;
    defer partial.release();
    try std.testing.expect(partial.pool.completeOwnedPayload() == null);
}

test "Native planned concat slice gather execute only declared banks across warm and held-output OOM" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const Gate = struct {
        backing: std.mem.Allocator,
        deny_payloads: bool = false,
        denied: usize = 0,
        fn alloc(raw: *anyopaque, n: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.deny_payloads and (n == 24 or n == 48)) {
                self.denied += 1;
                return null;
            }
            return self.backing.rawAlloc(n, alignment, ret);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(bytes, alignment, ret);
        }
        const table = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &table };
        }
    };
    var gate = Gate{ .backing = a };
    const payload_allocator = gate.allocator();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(payload_allocator, .native, null);
    defer context.deinit(payload_allocator);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const joined = try b.concat(x, x, 1);
    const sliced = try b.sliceLastDim(joined, 1, 4);
    const indices = try b.tensorConst(&.{ -1, 0 }, ml.graph.Shape.init(.f32, &.{2}));
    const gathered = try b.gather(sliced, indices, ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    try graph.markOutput(gathered);
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const node = program.id_map[x];
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const routes = (try pool.prepareRoutes(&.{.{ .node_id = node, .value = .{ .imported_host = .{ .logical = program.graph.node(node).output_shape, .target = .native, .owned_ingress = true } } }})) orelse return error.MissingPreparedKernelRoute;
    defer routes.release();
    if (routes.pool.completeOwnedPayload() == null) {
        const selected_pool = routes.pool;
        std.debug.print("NATIVE_LAYOUT_COLD coverage={s} reason={s} frame_dispatch={} regions={d} epoch={d}\n", .{ @tagName(std.meta.activeTag(selected_pool.payload_coverage)), if (selected_pool.payload_coverage == .compatibility) @tagName(selected_pool.payload_coverage.compatibility) else "none", selected_pool.native_frame_dispatch, selected_pool.regions.len, selected_pool.selection.?.epoch });
        for (selected_pool.plan.slots) |slot| {
            std.debug.print("NATIVE_LAYOUT_NODE node={d} op={s} kind={s} shape={any} dtype={s} output={} route={} allocation={d}\n", .{ slot.node_id, @tagName(std.meta.activeTag(program.graph.node(slot.node_id).op)), @tagName(slot.kind), slot.desc.shape.dims[0..slot.desc.shape.rank()], @tagName(slot.desc.shape.dtype), slot.roles.graph_output, selected_pool.preparedKernelRoute(slot.node_id) != null, slot.allocation });
            if (selected_pool.preparedKernelRoute(slot.node_id)) |owned| std.debug.print("NATIVE_LAYOUT_RESOURCE node={d} primary={d} host={d} probe_host={d} alignment={d}\n", .{ slot.node_id, owned.requirements.primary_bytes, owned.requirements.workspace.host_byte_size, owned.requirements.probe_workspace.host_byte_size, owned.requirements.host_alignment });
        }
        return error.NativeLayoutMissingCompletePayloadWitness;
    }
    for ([_]ml.graph.NodeId{ joined, sliced, gathered }) |original| {
        const id = program.id_map[original];
        if (routes.pool.preparedKernelRoute(id) == null) {
            std.debug.print("NATIVE_LAYOUT_MISSING_ROUTE original={d} optimized={d}\n", .{ original, id });
            return error.NativeLayoutMissingSelectedOperation;
        }
    }
    var input = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
    defer input.deinit();
    const options = interpreter.ExecuteOptions{ .host_inputs = &.{.{ .node_id = node, .value = &input }}, .prepared_routes = routes };
    {
        var result = try executable.run(a, options);
        defer result.deinit(&executable.runtime);
        const output = try cb.toFloat32(result.outputs[0], a);
        defer a.free(output);
        try std.testing.expectEqualSlices(f32, &.{ 5, 6, 4, 2, 3, 1 }, output);
    }
    // Every allocating compatibility implementation needs a 24/48-byte float
    // payload. Warm assigned execution may allocate metadata, never these data.
    gate.deny_payloads = true;
    var held = try executable.run(a, options);
    defer held.deinit(&executable.runtime);
    if (gate.denied != 0) {
        std.debug.print("NATIVE_LAYOUT_WARM_DENIAL denied={d}\n", .{gate.denied});
        return error.NativeLayoutUnexpectedWarmPayloadAllocation;
    }
    const before = try cb.toFloat32(held.outputs[0], a);
    defer a.free(before);
    try std.testing.expectError(error.OutOfMemory, executable.run(a, options));
    if (gate.denied == 0) return error.NativeLayoutHeldOutputDidNotExerciseAllocatorFailure;
    const after = try cb.toFloat32(held.outputs[0], a);
    defer a.free(after);
    try std.testing.expectEqualSlices(f32, before, after);
    gate.deny_payloads = false;
    _ = try routes.pool.reclaimRetiredFrames();
    var retry = try executable.run(a, options);
    defer retry.deinit(&executable.runtime);
    const values = try cb.toFloat32(retry.outputs[0], a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6, 4, 2, 3, 1 }, values);
}

test "Native declared typed ingress executes exact gather and retains held outputs across index failure" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const idx = try b.parameter("indices", ml.graph.Shape.init(.i64, &.{2}));
    const gather = try b.gather(x, idx, ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    try graph.markOutput(gather);
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .node_id = x, .kind = .input }, .{ .name = "indices", .node_id = idx, .kind = .input } });
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const x_id = program.id_map[x];
    const index_id = program.id_map[idx];
    const facts = [_]@import("kernel_route.zig").InputBinding{ .{ .node_id = x_id, .value = .{ .imported_host = .{ .logical = program.graph.node(x_id).output_shape, .target = .native, .owned_ingress = true } } }, .{ .node_id = index_id, .value = .{ .imported_host = .{ .logical = program.graph.node(index_id).output_shape, .target = .native, .owned_ingress = true } } } };
    const routes = (try pool.prepareRoutes(&facts)) orelse return error.MissingPreparedKernelRoute;
    defer routes.release();
    try std.testing.expect(routes.pool.completeOwnedPayload() != null);
    try std.testing.expectEqual(@as(usize, 2), routes.pool.host_input_plans.len);
    var input = try Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
    defer input.deinit();
    var storage: [17]u8 = undefined;
    const offset: usize = if (@intFromPtr(&storage) % 8 == 0) 1 else 0;
    const raw = storage[offset..][0..16];
    const valid = [_]i64{ 1, -2 };
    @memcpy(raw, std.mem.sliceAsBytes(&valid));
    try std.testing.expect(@intFromPtr(raw.ptr) % 8 != 0);
    const indices = Tensor{ .data = raw, .dtype = .i64, .shape = &.{2}, .name = "indices", .allocator = a, .owns_data = false, .owns_shape = false };
    const options = interpreter.ExecuteOptions{ .host_inputs = &.{ .{ .node_id = x_id, .value = &input }, .{ .node_id = index_id, .value = &indices } }, .prepared_routes = routes };
    var held = try executable.run(a, options);
    defer held.deinit(&executable.runtime);
    const before = try cb.toFloat32(held.outputs[0], a);
    defer a.free(before);
    try std.testing.expectEqualSlices(f32, &.{ 4, 5, 6, 1, 2, 3 }, before);
    // This value would round through F32. Exact indices must reject it, leave
    // the first published frame untouched, and retire the failed frame safely.
    const invalid = [_]i64{ 0, 9_007_199_254_740_993 };
    @memcpy(raw, std.mem.sliceAsBytes(&invalid));
    try std.testing.expectError(error.IndexOutOfBounds, executable.run(a, options));
    const after = try cb.toFloat32(held.outputs[0], a);
    defer a.free(after);
    try std.testing.expectEqualSlices(f32, before, after);
    _ = try routes.pool.reclaimRetiredFrames();
    @memcpy(raw, std.mem.sliceAsBytes(&valid));
    var retry = try executable.run(a, options);
    defer retry.deinit(&executable.runtime);
    const actual = try cb.toFloat32(retry.outputs[0], a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, before, actual);
}

test "raw cached CUDA reachability excludes actual dead gather literals from upload and capture legality" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = @import("../backends/compute_context.zig").BackendContext.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const backend = context.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input_node = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 8 }));
    const payload = try a.alloc(f32, 16384);
    defer a.free(payload);
    @memset(payload, 7);
    const dead_table = try builder.tensorConst(payload, ml.graph.Shape.init(.f32, &.{ 8192, 2 }));
    const dead_indices = try builder.tensorConst(&.{1}, ml.graph.Shape.init(.f32, &.{1}));
    const dead_gather = try builder.gather(dead_table, dead_indices, ml.graph.Shape.init(.f32, &.{ 1, 2 }));
    var output = try builder.absOp(input_node);
    for (0..24) |_| output = try builder.neg(output);
    try graph.markOutput(output);
    var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
    defer analysis.deinit(a);
    try std.testing.expect(!analysis.reachable[dead_table] and !analysis.reachable[dead_indices] and !analysis.reachable[dead_gather]);
    var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .partitioned, .{});
    defer executable.deinit();
    const root = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    try root.prepareReplay(&graph, &executable.runtime.buffer_plan.?, &.{.{ .name = "x", .node_id = input_node, .kind = .input }}, .{});
    try std.testing.expect(root.replay_backend != null);
    try std.testing.expectEqual(@as(usize, 25), root.replay_kernel_count);
    const routes = (try root.prepareRoutes(&.{.{ .node_id = input_node, .value = .{ .imported_host = .{ .logical = graph.node(input_node).output_shape, .target = .cuda, .owned_ingress = true } } }})) orelse return error.MissingPreparedRoutes;
    defer routes.release();
    try std.testing.expectEqual(@as(usize, 1), routes.pool.replay_inputs.len);
    try std.testing.expectEqual(ml.graph.null_node, routes.pool.replay_input_indices[dead_table]);
    try std.testing.expectEqual(ml.graph.null_node, routes.pool.replay_input_indices[dead_indices]);
    var input = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{ 1, 8 }, &.{ 1, 1, 1, 1, 1, 1, 1, 1 });
    defer input.deinit();
    const inputs = [_]@import("host_ingress.zig").Input{.{ .node_id = input_node, .value = &input }};
    var captures: u64 = 0;
    var replays: u64 = 0;
    for (0..5) |iteration| {
        @memset(input.asFloat32Mut(), @floatFromInt(iteration + 1));
        const d2d = context.cuda.stats.d2d_bytes;
        var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        try @import("transfer_audit.zig").begin(&audit);
        var result = executable.run(a, .{ .host_inputs = &inputs, .prepared_routes = routes, .collect_partition_stats = true, .activation_readbacks = .forbidden }) catch |err| {
            @import("transfer_audit.zig").end();
            return err;
        };
        @import("transfer_audit.zig").end();
        defer result.deinit(&executable.runtime);
        std.debug.print("RAW_REPLAY iteration={d} uploads={d} bytes={d} capture={d} replay={d} decision={s}\n", .{ iteration, audit.upload_calls, audit.upload_bytes, result.stats.physical_graph_captures, result.stats.physical_graph_replays, @tagName(result.stats.physical_replay) });
        try std.testing.expectEqual(@as(u64, 1), audit.upload_calls);
        try std.testing.expectEqual(@as(u64, 32), audit.upload_bytes);
        try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
        try std.testing.expectEqual(d2d, context.cuda.stats.d2d_bytes);
        captures += result.stats.physical_graph_captures;
        replays += result.stats.physical_graph_replays;
        const actual = try backend.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        for (actual) |value| try std.testing.expectEqual(@as(f32, @floatFromInt(iteration + 1)), value);
    }
    try std.testing.expectEqual(@as(u64, 1), captures);
    try std.testing.expect(replays >= 3);
}

test "owned host generation exhaustion retires the frame before scope and permits clean retry" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try b.neg(x));
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const node = program.id_map[x];
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const routes = (try pool.prepareRoutes(&.{.{ .node_id = node, .value = .{ .imported_host = .{ .logical = program.graph.node(node).output_shape, .target = .native, .owned_ingress = true } } }})).?;
    defer routes.release();
    const frame = (try routes.pool.acquire()).?;
    frame.host_input_generation = std.math.maxInt(u64);
    try std.testing.expectError(error.HostInputGenerationExhausted, frame.begin());
    try std.testing.expect(frame.retired);
    _ = try frame.finish();
    try std.testing.expect(try routes.pool.reclaimRetiredFrames());
    var input = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{4}, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    var result = try executable.run(a, .{ .host_inputs = &.{.{ .node_id = node, .value = &input }}, .prepared_routes = routes });
    defer result.deinit(&executable.runtime);
    const values = try cb.toFloat32(result.outputs[0], a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4 }, values);
}

fn exerciseReachableNativeProjection(a: std.mem.Allocator) !void {
    const ml = @import("ml");
    const memory = @import("../runtime/tier/memory.zig");
    const storage_admission = @import("storage_admission.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        current: memory.AdmissionLease = undefined,
        credit: memory.AdmissionAmounts = .{},
        refs: usize = 1,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: ops.BackendKind, class: @import("storage_accounting.zig").Class, bytes: usize) !storage_admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return storage_admission.fromRunLease(allocator, &self.current, .{ .controller = &self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 48, .combined_limit_bytes = 48 }, .check_live_memory = false, .credit = &self.credit, .lifetime = .{ .context = self, .retain = retain, .release = release } }, kind, class, bytes);
        }
    };
    var owner = Owner{};
    defer owner.controller.deinit();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const input_node = try builder.parameter("x", shape);
    const large = try a.alloc(f32, 16384);
    defer a.free(large);
    @memset(large, 7);
    const dead_literal = try builder.tensorConst(large, ml.graph.Shape.init(.f32, &.{16384}));
    const dead_kernel = try builder.neg(dead_literal);
    const middle = try builder.neg(input_node);
    const alias = try builder.reshape(middle, ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const output = try builder.neg(alias);
    try graph.markOutput(output);
    var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
    defer analysis.deinit(a);
    var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .compiled_preferred, .{ .physical_replay = false });
    var executable_owned = true;
    defer if (executable_owned) executable.deinit();
    const root = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const original_bank = root.plan.slotForNode(dead_kernel).?.allocation;
    try std.testing.expectEqual(original_bank, root.plan.slotForNode(middle).?.allocation);
    try std.testing.expectEqual(@as(u64, 65536), root.plan.allocations[original_bank].byte_size);
    const routes = (try root.prepareRoutes(&.{.{ .node_id = input_node, .value = .{ .imported_host = .{ .logical = shape, .target = .native, .owned_ingress = true } } }})) orelse return error.MissingPreparedRoutes;
    var routes_owned = true;
    defer if (routes_owned) routes.release();
    try std.testing.expect(routes.pool.completeOwnedPayload() != null);
    try std.testing.expect(routes.pool.plan.slotForNode(dead_kernel) == null);
    try std.testing.expect(routes.pool.plan.slotForNode(dead_literal) == null);
    try std.testing.expect(routes.pool.preparedKernelRoute(dead_kernel) == null);
    try std.testing.expectEqual(@import("../graph/buffer_plan.zig").invalid_allocation, routes.pool.outputSlot(dead_kernel, 65536).allocation);
    try std.testing.expect(root.plan.slotForNode(dead_kernel) != null);
    try std.testing.expectEqual(@as(u64, 65536), root.plan.allocations[original_bank].byte_size);
    try std.testing.expectEqual(@as(u64, 16), routes.pool.plan.allocations[original_bank].byte_size);
    const store = routes.pool.literal_store orelse return error.MissingLiteralStore;
    const literal_index = store.node_indices[dead_literal];
    try std.testing.expect(!routes.pool.literalEnabled(literal_index));
    try std.testing.expect(routes.pool.plan.allocationById(store.literals[literal_index].allocation) == null);
    try routes.pool.plan.validate(&graph, &executable.runtime.plan.?.base);
    // Inactivity is not a fabricated alias: a dead slot may never recover a
    // writable allocation merely because that bank is live for another node.
    routes.pool.plan.slots[dead_kernel].allocation = original_bank;
    const malformed = routes.pool.plan.validate(&graph, &executable.runtime.plan.?.base);
    routes.pool.plan.slots[dead_kernel].allocation = @import("../graph/buffer_plan.zig").invalid_allocation;
    try std.testing.expectError(error.InvalidBufferPlan, malformed);
    const bad_reachable = try a.dupe(bool, analysis.reachable);
    defer a.free(bad_reachable);
    bad_reachable[input_node] = false;
    try std.testing.expectError(error.InvalidReachabilityTable, routes.pool.plan.projectReachable(&graph, bad_reachable));
    try std.testing.expect(routes.pool.plan.slotForNode(input_node) != null);
    try std.testing.expectEqual(@as(u64, 48), routes.pool.plan.totalAllocationBytes(null, null));
    var partition = try routes.pool.plan.partitionView(a, &executable.runtime.plan.?.base, 0);
    defer partition.deinit(a);
    for (partition.slots) |slot| try std.testing.expect(slot.slot.node_id != dead_kernel and slot.slot.node_id != dead_literal);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 48 }, try routes.storageRequirement());
    {
        const saved = root.coverage_reachable;
        root.coverage_reachable = null;
        defer root.coverage_reachable = saved;
        const compatibility = (try root.prepareRoutes(&.{.{ .node_id = input_node, .value = .unknown }})) orelse return error.MissingCompatibilityRoutes;
        defer compatibility.release();
        try std.testing.expect(compatibility.pool.plan.execution_reachable == null);
        try std.testing.expect(compatibility.pool.completeOwnedPayload() == null);
        try std.testing.expect((try compatibility.storageRequirement()).host_scratch_bytes >= 131072);
    }
    owner.current = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 48, .combined_limit_bytes = 48 }, .{}, false);
    defer owner.current.release();
    const cap = storage_admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
    var input = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "x", &.{4}, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    const inputs = [_]@import("host_ingress.zig").Input{.{ .node_id = input_node, .value = &input }};
    var result: ?runtime_module.Result = null;
    defer if (result) |*value| value.deinit(&executable.runtime);
    var held: ?ops.CT = null;
    defer if (held) |value| backend.free(value);
    result = try executable.run(a, .{ .host_inputs = &inputs, .prepared_routes = routes, .storage_admission = cap, .output_placement = .producer, .activation_readbacks = .forbidden, .collect_partition_stats = true });
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 48 }, owner.controller.snapshot());
    try std.testing.expectEqual(@as(u64, 48), routes.pool.accounting.?.snapshot().total.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), routes.pool.accounting.?.snapshot().constant.requested_live_bytes);
    try std.testing.expectError(error.InactivePhysicalNode, routes.pool.frames.items[0].execute(&backend, &.{}, dead_kernel, null));
    if (executable.run(a, .{ .host_inputs = &inputs, .prepared_routes = routes, .storage_admission = cap, .output_placement = .producer })) |unexpected| {
        var cleanup = unexpected;
        cleanup.deinit(&executable.runtime);
        return error.UnexpectedPhysicalQuotaGap;
    } else |err| {
        if (err != error.ResourceTemporarilyUnavailable) return err;
    }
    const first = try backend.toFloat32(result.?.outputs[0], a);
    defer a.free(first);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, first);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 48 }, owner.controller.snapshot());
    result.?.deinit(&executable.runtime);
    result = null;
    var claim = (try routes.pool.claimWarm(cap.identity)) orelse return error.MissingWarmProjectedFrame;
    defer claim.release();
    @memset(input.asFloat32Mut(), 9);
    result = try executable.run(a, .{ .host_inputs = &inputs, .prepared_routes = routes, .physical_frame_claim = &claim, .storage_admission = cap, .output_placement = .producer, .collect_partition_stats = true });
    try std.testing.expectEqual(@as(u64, 0), result.?.stats.physical_storage_allocations);
    held = try backend.primReshape(result.?.outputs[0], &.{4});
    result.?.deinit(&executable.runtime);
    result = null;
    routes.release();
    routes_owned = false;
    executable.deinit();
    executable_owned = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 16 }, owner.controller.snapshot());
    const final = try backend.toFloat32(held.?, a);
    defer a.free(final);
    try std.testing.expectEqualSlices(f32, &.{ 9, 9, 9, 9 }, final);
    backend.free(held.?);
    held = null;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), owner.refs);
}

test "cached reachability removes real dead literal payload and shrinks reused Native banks under exact quota" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try exerciseReachableNativeProjection(std.testing.allocator);
}
test "reachable physical projection unwinds every allocation failure with held output and hard quota" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exerciseReachableNativeProjection, .{});
}

fn borrowedResultHandoffCase(allocator: std.mem.Allocator) !void {
    if (comptime @import("build_options").enable_wasm or !@import("build_options").enable_native) return error.SkipZigTest;
    const ml = @import("ml");
    const partition = @import("../graph/partition.zig");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(allocator, .native, null);
    defer context.deinit(allocator);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input_node = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    try graph.markOutput(try builder.neg(input_node));
    var analysis = try interpreter.CachedAnalysis.compute(allocator, &graph);
    defer analysis.deinit(allocator);
    var executable = try prepareAnalyzedGraph(allocator, &graph, analysis, &backend, .partitioned, .{});
    var executable_live = true;
    defer if (executable_live) executable.deinit();
    const input = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 4 });
    defer backend.free(input);
    const inputs = [_]interpreter.RuntimeInput{.{ .node_id = input_node, .value = input, .source_backend = &backend }};
    const options = interpreter.ExecuteOptions{ .runtime_inputs = &inputs, .collect_partition_stats = true };
    var direct: runtime_module.Result = undefined;
    const Probe = struct {
        inner: ?*const partition.PartitionExecutor,
        expected: *@import("stats.zig").ExecutionStats,
        observed: bool = false,
        fn execute(raw: *anyopaque, values: []?ops.CT, devices: []@import("../graph/device_mesh.zig").DeviceId, nodes: []const ml.graph.NodeId, device: @import("../graph/device_mesh.zig").DeviceId, exec_ctx: partition.PartitionExecutor.ExecutionContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const actual = exec_ctx.stats orelse return error.MissingBorrowedStatsSink;
            if (actual != self.expected) return error.CopiedBorrowedStatsSink;
            self.observed = true;
            if (self.inner) |inner| return inner.execute(values, devices, nodes, device, exec_ctx);
            var native = @import("../graph/native_partition_executor.zig").NativePartitionExecutor.initBorrowed(exec_ctx.allocator.?, exec_ctx.graph.?, exec_ctx.backend.?);
            try native.partitionExecutor().execute(values, devices, nodes, device, exec_ctx);
        }
        fn deinit(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.inner) |inner| inner.deinitExecutor();
        }
        const table = partition.PartitionExecutor.VTable{ .execute = execute, .deinit = deinit };
    };
    const plan = &executable.runtime.plan.?;
    var probe = Probe{ .inner = plan.base.partitions[0].executor, .expected = &direct.stats };
    const probe_executor = partition.PartitionExecutor{ .ptr = &probe, .vtable = &Probe.table };
    plan.base.partitions[0].executor = &probe_executor;
    try executable.runInto(allocator, &options, &direct);
    defer direct.deinit(&executable.runtime);
    try std.testing.expect(probe.observed);
    try std.testing.expect(options.cached_analysis == null and options.physical_frame == null);
    try std.testing.expect(@intFromPtr(options.runtime_inputs.?.ptr) == @intFromPtr(&inputs));
    const first = try backend.toFloat32(direct.outputs[0], allocator);
    defer allocator.free(first);
    try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4 }, first);
    // Distinct caller-owned sinks/results may remain live together. Restore the
    // normal executor before comparing the unchanged compatibility value API.
    plan.base.partitions[0].executor = probe.inner;
    var legacy = try executable.run(allocator, options);
    defer legacy.deinit(&executable.runtime);
    const second = try backend.toFloat32(legacy.outputs[0], allocator);
    defer allocator.free(second);
    try std.testing.expectEqualSlices(f32, first, second);
    try std.testing.expectEqual(direct.stats.assigned_kernel_dispatches, legacy.stats.assigned_kernel_dispatches);
    try std.testing.expectEqual(direct.stats.partitions_executed, legacy.stats.partitions_executed);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var cancelled = options;
    cancelled.execution_control = .{ .check_fn = Cancel.check, .native_call_policy = .in_process };
    var failed: runtime_module.Result = undefined;
    if (executable.runInto(allocator, &cancelled, &failed)) |_| {
        failed.deinit(&executable.runtime);
        return error.ExpectedCancelledBorrowedSubmission;
    } else |err| {
        // The all-allocation-failure harness may deny frame metadata before
        // the existing downstream cancellation checkpoint is reached.
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.Cancelled, err);
    }
    // Debugging disabled still uses invocation-local storage and real counters;
    // no diagnostic singleton or borrowed pointer escapes request completion.
    var quiet = options;
    quiet.collect_partition_stats = false;
    var third: runtime_module.Result = undefined;
    try executable.runInto(allocator, &quiet, &third);
    defer third.deinit(&executable.runtime);
    const final = try backend.toFloat32(third.outputs[0], allocator);
    defer allocator.free(final);
    try std.testing.expectEqualSlices(f32, first, final);
    executable.deinit();
    executable_live = false;
    const held = try backend.toFloat32(direct.outputs[0], allocator);
    defer allocator.free(held);
    try std.testing.expectEqualSlices(f32, first, held);
}

test "borrowed options and caller result preserve real stats address parity held outputs cancellation and every allocation failure" {
    try borrowedResultHandoffCase(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, borrowedResultHandoffCase, .{});
}

fn nativeAffineGraphParity(a: std.mem.Allocator) !void {
    const ml = @import("ml");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const memory = @import("../runtime/tier/memory.zig");
    const admission = @import("storage_admission.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        current: memory.AdmissionLease = undefined,
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: ops.BackendKind, class: @import("storage_accounting.zig").Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.current, .{ .controller = &self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 96, .combined_limit_bytes = 96 }, .check_live_memory = false }, kind, class, bytes);
        }
    };
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    for (0..4) |mode| {
        var owner = Owner{};
        defer owner.controller.deinit();
        owner.current = try owner.controller.tryAcquire(.cpu, .{}, .{}, false);
        defer owner.current.release();
        const cap = admission.Capability{ .context = &owner, .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .services = &.{ .reserve = Owner.reserve } };
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
        const x = try builder.parameter("x", shape);
        const negative = try builder.neg(x);
        const alias = switch (mode) {
            0 => try builder.reshape(negative, ml.graph.Shape.init(.f32, &.{ 3, 2 })),
            1 => try builder.transpose(negative, &.{ 1, 0 }),
            2 => block: {
                var axes: [ml.graph.shape.max_rank]u8 = @splat(0);
                axes[0] = 1;
                axes[1] = 2;
                break :block try graph.addNode(.{ .op = .{ .broadcast_in_dim = .{ .target_shape = ml.graph.Shape.init(.f32, &.{ 2, 2, 3 }), .broadcast_axes = axes, .num_axes = 2 } }, .output_shape = ml.graph.Shape.init(.f32, &.{ 2, 2, 3 }), .inputs = .{ negative, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node }, .num_inputs = 1 });
            },
            else => try builder.sliceLastDim(negative, 0, 3),
        };
        try graph.markOutput(alias);
        var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
        defer analysis.deinit(a);
        var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .partitioned, .{ .physical_replay = false });
        defer executable.deinit();
        const root = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
        const routes = (try root.prepareRoutes(&.{.{ .node_id = x, .value = .{ .imported_host = .{ .logical = shape, .target = .native, .owned_ingress = true } } }})) orelse return error.MissingPreparedRoutes;
        defer routes.release();
        if (routes.pool.completeOwnedPayload() == null) return error.MissingAffineCoverage;
        try std.testing.expectEqual(routes.pool.plan.slotForNode(negative).?.allocation, routes.pool.plan.slotForNode(alias).?.allocation);
        const requirement = try routes.pool.storageRequirement();
        try std.testing.expectEqual(@as(usize, 48), requirement.host_scratch_bytes);
        try std.testing.expectError(error.ResourceLimitExceeded, owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 47, .combined_limit_bytes = 47 }, requirement, false));
        var input = try Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
        defer input.deinit();
        const options = interpreter.ExecuteOptions{ .host_inputs = &.{.{ .node_id = x, .value = &input }}, .prepared_routes = routes, .storage_admission = cap, .output_placement = .producer, .activation_readbacks = .forbidden };
        var held = try executable.run(a, options);
        defer held.deinit(&executable.runtime);
        var second = try executable.run(a, options);
        defer second.deinit(&executable.runtime);
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 96 }, owner.controller.snapshot());
        if (executable.run(a, options)) |unexpected| {
            var cleanup = unexpected;
            cleanup.deinit(&executable.runtime);
            return error.AffineHeldOutputQuotaGap;
        } else |err| {
            if (err != error.ResourceTemporarilyUnavailable) return err;
        }
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 96 }, owner.controller.snapshot());
        const expected: []const f32 = switch (mode) {
            1 => &.{ -1, -4, -2, -5, -3, -6 },
            2 => &.{ -1, -2, -3, -4, -5, -6, -1, -2, -3, -4, -5, -6 },
            else => &.{ -1, -2, -3, -4, -5, -6 },
        };
        const exported = (try backend.exportTensorData(held.outputs[0], a)) orelse return error.MissingAffineExport;
        defer a.free(exported.payload.bytes);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected), exported.payload.bytes);
        const exported_again = (try backend.exportTensorData(second.outputs[0], a)) orelse return error.MissingAffineExport;
        defer a.free(exported_again.payload.bytes);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected), exported_again.payload.bytes);
    }
}

test "Native affine aliases use exact retained banks across warm and held outputs" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try nativeAffineGraphParity(std.testing.allocator);
}
test "Native affine graph owners unwind every allocation failure" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    // Enumerate every allocate/copy/free fallback independently of the
    // backing allocator's layout-dependent in-place remap successes.
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, nativeAffineGraphParity, .{});
}

test "Native opaque affine consumer preserves source liveness across intervening same-size allocation" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try builder.parameter("x", shape);
    const original = try builder.neg(x);
    const alias = try builder.transpose(original, &.{ 1, 0 });
    const intervening = try builder.absOp(x);
    const reduced = try builder.reduceSum(alias, &.{1});
    try graph.markOutput(intervening);
    try graph.markOutput(reduced);
    var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
    defer analysis.deinit(a);
    var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .partitioned, .{ .physical_replay = false });
    defer executable.deinit();
    const root = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const routes = (try root.prepareRoutes(&.{.{ .node_id = x, .value = .{ .imported_host = .{ .logical = shape, .target = .native, .owned_ingress = true } } }})) orelse return error.MissingPreparedRoutes;
    defer routes.release();
    try std.testing.expect(routes.pool.completeOwnedPayload() == null);
    try std.testing.expect(routes.pool.preparedKernelRoute(reduced) == null);
    try std.testing.expectEqual(@as(u8, 0), routes.pool.selection.?.plane_counts[alias]);
    const original_slot = routes.pool.plan.slotForNode(original).?;
    const intervening_slot = routes.pool.plan.slotForNode(intervening).?;
    try std.testing.expect(original_slot.allocation != intervening_slot.allocation);
    try std.testing.expect(original_slot.last_use >= routes.pool.plan.positionForNode(reduced));
    var input = try Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
    defer input.deinit();
    var result = try executable.run(a, .{ .host_inputs = &.{.{ .node_id = x, .value = &input }}, .prepared_routes = routes });
    defer result.deinit(&executable.runtime);
    const first = try backend.toFloat32(result.outputs[0], a);
    defer a.free(first);
    const second = try backend.toFloat32(result.outputs[1], a);
    defer a.free(second);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6 }, first);
    try std.testing.expectEqualSlices(f32, &.{ -5, -7, -9 }, second);
}

test "Native imported parameter affine aliases keep opaque routes and legacy numerical parity" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    for (0..2) |mode| {
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var b = ml.graph.Builder.init(&graph);
        const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
        const x = try b.parameter("x", shape);
        const alias = if (mode == 0) try b.transpose(x, &.{ 1, 0 }) else block: {
            var axes: [ml.graph.shape.max_rank]u8 = @splat(0);
            axes[0] = 1;
            axes[1] = 2;
            const output_shape = ml.graph.Shape.init(.f32, &.{ 2, 2, 3 });
            break :block try graph.addNode(.{ .op = .{ .broadcast_in_dim = .{ .target_shape = output_shape, .broadcast_axes = axes, .num_axes = 2 } }, .output_shape = output_shape, .inputs = .{ x, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node }, .num_inputs = 1 });
        };
        const output = try b.neg(alias);
        try graph.markOutput(output);
        var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
        defer analysis.deinit(a);
        var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .partitioned, .{ .physical_replay = false });
        defer executable.deinit();
        const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
        const routes = (try pool.prepareRoutes(&.{.{ .node_id = x, .value = .{ .imported_host = .{ .logical = shape, .target = .native, .owned_ingress = true } } }})) orelse return error.MissingPreparedRoutes;
        defer routes.release();
        try std.testing.expect(routes.pool.completeOwnedPayload() == null);
        try std.testing.expectEqual(@as(u8, 0), routes.pool.selection.?.plane_counts[alias]);
        try std.testing.expect(routes.pool.preparedKernelRoute(output) == null);
        var input = try Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
        defer input.deinit();
        var result = try executable.run(a, .{ .host_inputs = &.{.{ .node_id = x, .value = &input }}, .prepared_routes = routes });
        defer result.deinit(&executable.runtime);
        const actual = try backend.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        const expected: []const f32 = if (mode == 0) &.{ -1, -4, -2, -5, -3, -6 } else &.{ -1, -2, -3, -4, -5, -6, -1, -2, -3, -4, -5, -6 };
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
}

fn affineResidentSourceConsumerParity(a: std.mem.Allocator) !void {
    if (comptime @import("build_options").enable_wasm or !@import("build_options").enable_native) return error.SkipZigTest;
    const ml = @import("ml");
    const ownership = @import("storage_ownership.zig");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    const original = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    defer backend.free(original);
    const input = try backend.primTranspose(original, &.{ 1, 0 }, &.{ 3, 2 });
    defer backend.free(input);
    const Collector = struct {
        plane: ?ownership.Plane = null,
        fn emit(raw: *anyopaque, report: ownership.Report) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (report == .plane and report.plane.role == .primary) {
                if (self.plane != null) return error.AmbiguousAffineInput;
                self.plane = report.plane;
            }
        }
    };
    var source = Collector{};
    const query = backend.vtable.kernels.?.storageOwnership.?;
    try std.testing.expect(try query(backend.ptr, input, .{ .context = &source, .visit = Collector.emit }) == .complete);
    const plane = source.plane orelse return error.MissingAffineResidentPlane;
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, plane.descriptor.strides.asSlice());
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try builder.parameter("x", shape);
    // Both branches remain observable, with legacy materialization scheduled
    // before the second branch tries to construct its frozen alias.
    const early = try builder.neg(x);
    const alias = try builder.transpose(x, &.{ 1, 0 });
    const late = try builder.neg(alias);
    try graph.markOutput(early);
    try graph.markOutput(late);
    var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
    defer analysis.deinit(a);
    var executable = try prepareAnalyzedGraph(a, &graph, analysis, &backend, .partitioned, .{ .physical_replay = false });
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingAffinePool;
    const planes = [_]ownership.Plane{plane};
    const facts = [_]@import("kernel_route.zig").InputBinding{.{ .node_id = x, .value = .{ .resident = .{ .logical = shape, .planes = &planes, .implementation = .{ .context = backend.ptr, .table = backend.vtable } } } }};
    const selected = (try pool.prepareRoutes(&facts)) orelse return error.MissingAffineRoutes;
    defer selected.release();
    const inputs = [_]interpreter.RuntimeInput{.{ .node_id = x, .value = input, .source_backend = &backend }};
    var result = try executable.run(a, .{ .runtime_inputs = &inputs, .prepared_routes = selected, .collect_partition_stats = true });
    defer result.deinit(&executable.runtime);
    const first = (try backend.exportTensorData(result.outputs[0], a)) orelse return error.MissingAffineOutput;
    defer a.free(first.payload.bytes);
    const second = (try backend.exportTensorData(result.outputs[1], a)) orelse return error.MissingAffineOutput;
    defer a.free(second.payload.bytes);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(@as([]const f32, &.{ -1, -3, -5, -2, -4, -6 })), first.payload.bytes);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(@as([]const f32, &.{ -1, -2, -3, -4, -5, -6 })), second.payload.bytes);
    // These assertions follow execution so the old implementation reproduces
    // its real PreparedMetadataAliasChanged failure before coverage checks.
    try std.testing.expectEqual(@as(u8, 0), selected.pool.selection.?.plane_counts[alias]);
    try std.testing.expect(selected.pool.preparedKernelRoute(late) == null);
    try std.testing.expect(selected.pool.completeOwnedPayload() == null);
}

test "Native affine resident source with earlier opaque materialization preserves both branch outputs" {
    try affineResidentSourceConsumerParity(std.testing.allocator);
}

test "CUDA owned dispatch permits unused Native catalog and declines active fallback aliases and state" {
    const build = @import("build_options");
    if (comptime build.enable_wasm or !build.enable_cuda or !build.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const shared = @import("../backends/compute_context.zig").SharedBackendContext.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer shared.release();
    const backend = shared.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{16}));
    const output = try builder.absOp(input);
    try graph.markOutput(output);
    var program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .node_id = input, .kind = .input }, .{ .name = "out", .node_id = output, .kind = .output } });
    defer program.deinit();
    var executable = try prepare(a, &program, &backend, .partitioned);
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const original_mesh = executable.runtime.mesh orelse return error.MissingDeviceMesh;
    const fallback = original_mesh.device(1) orelse return error.MissingNativeFallback;
    try std.testing.expectEqual(.native, fallback.kind);
    try std.testing.expectEqual(@as(usize, 2), original_mesh.deviceCount());
    try std.testing.expect(pool.cuda_frame_dispatch);
    const device_plan = &executable.runtime.plan.?;
    // Production assignments are immutable. Fault cases use an owned mutable
    // test copy, restored before the actual plan destroys its original owner.
    const original_assignments = device_plan.device_assignment;
    const mutable_assignments = try a.dupe(@TypeOf(original_assignments[0]), original_assignments);
    defer a.free(mutable_assignments);
    device_plan.device_assignment = mutable_assignments;
    defer device_plan.device_assignment = original_assignments;
    const active_part = device_plan.base.node_assignment[program.id_map[output]];
    {
        const old_kind = device_plan.base.partitions[active_part].backend;
        const old_device = mutable_assignments[active_part];
        defer {
            device_plan.base.partitions[active_part].backend = old_kind;
            mutable_assignments[active_part] = old_device;
        }
        // Actual initialized fallback provider/catalog, made active in the
        // concrete execution plan: it must never inherit CUDA physical credit.
        device_plan.base.partitions[active_part].backend = .native;
        mutable_assignments[active_part] = 1;
        try configureOwnedCoverage(&executable, executable.graph, executable.analysis);
        try std.testing.expect(!pool.cuda_frame_dispatch);
    }
    try configureOwnedCoverage(&executable, executable.graph, executable.analysis);
    try std.testing.expect(pool.cuda_frame_dispatch);
    {
        const mesh_mod = @import("../graph/device_mesh.zig");
        var aliased = try mesh_mod.DeviceMesh.init(a, &.{ original_mesh.devices[0], original_mesh.devices[1], .{ .id = 77, .kind = .cuda, .backend = &backend } });
        defer aliased.deinit();
        executable.runtime.mesh = aliased;
        defer executable.runtime.mesh = original_mesh;
        try configureOwnedCoverage(&executable, executable.graph, executable.analysis);
        try std.testing.expect(!pool.cuda_frame_dispatch);
    }
    {
        const mesh_mod = @import("../graph/device_mesh.zig");
        var nonzero = try mesh_mod.DeviceMesh.init(a, &.{ .{ .id = 77, .kind = .cuda, .backend = &backend }, original_mesh.devices[1] });
        defer nonzero.deinit();
        executable.runtime.mesh = nonzero;
        defer executable.runtime.mesh = original_mesh;
        const old_device = mutable_assignments[active_part];
        defer mutable_assignments[active_part] = old_device;
        mutable_assignments[active_part] = 77;
        // Null source_backend runtime inputs resolve to DeviceId0 in the real
        // executor; a lone target at77 cannot prove absence of transfers.
        try configureOwnedCoverage(&executable, executable.graph, executable.analysis);
        try std.testing.expect(!pool.cuda_frame_dispatch);
    }
    try configureOwnedCoverage(&executable, executable.graph, executable.analysis);
    try std.testing.expect(pool.cuda_frame_dispatch);
    var state_program = try ModelProgram.init(a, &graph, &.{ .{ .name = "x", .node_id = input, .kind = .state_input }, .{ .name = "out", .node_id = output, .kind = .output } });
    defer state_program.deinit();
    var state_executable = try prepare(a, &state_program, &backend, .partitioned);
    defer state_executable.deinit();
    try std.testing.expect(!state_executable.runtime.physical_pool.?.cuda_frame_dispatch);
}

test "Metal Frame coverage refuses input rooted aliases and keeps tuned linear executor ownership" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 128, 64 }));
    const alias = try builder.reshape(x, ml.graph.Shape.init(.f32, &.{8192}));
    try graph.markOutput(try builder.absOp(alias));
    var program = try ModelProgram.init(a, &graph, &.{.{ .name = "x", .node_id = x, .kind = .input }});
    defer program.deinit();
    var executable = try prepare(a, &program, &cb, .partitioned);
    defer executable.deinit();
    const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
    const before = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
    const bindings = [_]@import("kernel_route.zig").InputBinding{.{ .node_id = program.id_map[x], .value = .{ .imported_host = .{ .target = .metal, .logical = ml.graph.Shape.init(.f32, &.{ 128, 64 }), .owned_ingress = true } } }};
    if (@import("../backends/metal_runtime.zig").antfly_metal_host_input_shared_supported(context.metal_hosted.compute.provider_impl.raw_decode_runtime)) {
        const selected = (try pool.prepareRoutes(&bindings)) orelse return error.MissingPreparedRoute;
        defer selected.release();
        try std.testing.expect(selected.pool.completeOwnedPayload() == null);
        try std.testing.expectEqual(.alias, selected.pool.payload_coverage.compatibility);
        try std.testing.expectEqual(before.device_owned_buffers_created, @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_buffers_created);
    }
    var projection_graph = ml.graph.Graph.init(a);
    defer projection_graph.deinit();
    var projection = ml.graph.Builder.init(&projection_graph);
    const px = try projection.parameter("x", ml.graph.Shape.init(.f32, &.{ 128, 64 }));
    const pw = try projection.parameter("w", ml.graph.Shape.init(.f32, &.{ 64, 64 }));
    try projection_graph.markOutput(try projection.linearNoBias(px, pw, 128, 64, 64));
    var projection_program = try ModelProgram.init(a, &projection_graph, &.{ .{ .name = "x", .node_id = px, .kind = .input }, .{ .name = "w", .node_id = pw, .kind = .input } });
    defer projection_program.deinit();
    var tuned = try prepare(a, &projection_program, &cb, .partitioned);
    defer tuned.deinit();
    try std.testing.expect(!tuned.runtime.physical_pool.?.metal_frame_dispatch);
    // Compilation keeps the actual canonical Metal owner attached. Coverage
    // refusal cannot switch the partition to a per-node/interpreter schedule.
    const plan = tuned.runtime.plan orelse return error.MissingGraphRuntimePlan;
    var owned: usize = 0;
    for (plan.base.partitions) |part| if (part.backend == .metal and part.node_ids.len != 0) {
        try std.testing.expect(part.executor != null);
        owned += 1;
    };
    try std.testing.expect(owned != 0);
}

test "Metal Frame cold metadata publication unwinds every allocation failure without SDK payload" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 128, 64 }));
    const root = try builder.neg(x);
    const alias = try builder.reshape(root, ml.graph.Shape.init(.f32, &.{8192}));
    try graph.markOutput(try builder.absOp(alias));
    var analysis = try interpreter.CachedAnalysis.compute(a, &graph);
    defer analysis.deinit(a);
    const Case = struct {
        fn run(allocator: std.mem.Allocator, ir: *const ml.graph.Graph, cached: interpreter.CachedAnalysis, cb: *const ops.ComputeBackend) !void {
            var executable = try prepareAnalyzedGraph(allocator, ir, cached, cb, .partitioned, .{});
            defer executable.deinit();
            try std.testing.expect(executable.runtime.physical_pool.?.metal_frame_dispatch);
        }
    };
    const before = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Case.run, .{ &graph, analysis, &backend });
    try std.testing.expectEqual(before.device_owned_buffers_created, @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_buffers_created);
}
