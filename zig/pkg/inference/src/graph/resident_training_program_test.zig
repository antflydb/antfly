// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const options = @import("build_options");
const ml = @import("ml").graph;
const program_mod = @import("resident_training_program.zig");
const seeded = @import("seeded_training.zig");
const interpreter = @import("interpreter.zig");
const ops = @import("../ops/ops.zig");
const metal = @import("../ops/metal_compute.zig");
const gpu_store = @import("../ops/gpu_hosted_store.zig");
const native = @import("../ops/native_compute.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const Allocator = std.mem.Allocator;

const Device = struct {
    allocator: Allocator,
    store: *gpu_store.WeightStore,
    backend: *metal.MetalCompute,
    fn init(a: Allocator) !Device {
        const store = try a.create(gpu_store.WeightStore);
        errdefer a.destroy(store);
        store.* = .{ .allocator = a, .prefix = "", .lazy_weights = .empty, .prefer_f32_dense_tensors = true };
        errdefer store.lazy_weights.deinit(a);
        metal.initPrefetchQueue(store, a);
        errdefer metal.deinitPrefetchQueue(store);
        errdefer metal.deinitSharedNativeProvider(store);
        const backend = try a.create(metal.MetalCompute);
        errdefer a.destroy(backend);
        backend.* = try metal.MetalCompute.init(a, store, null);
        return .{ .allocator = a, .store = store, .backend = backend };
    }
    fn deinit(self: *Device) void {
        self.backend.deinit();
        self.allocator.destroy(self.backend);
        metal.deinitSharedNativeProvider(self.store);
        metal.deinitPrefetchQueue(self.store);
        self.store.lazy_weights.deinit(self.allocator);
        self.allocator.destroy(self.store);
        self.* = undefined;
    }
};
const Input = struct { id: ml.NodeId, values: []const f32 = &.{}, indices: []const i32 = &.{} };
const Fixture = struct {
    graph: ml.Graph,
    inputs: [7]Input,
    parameters: [5]ml.NodeId,
    seed: ml.autodiff.Seed,
    fn init(a: Allocator) !Fixture {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var b = ml.Builder.init(&graph);
        const x = try b.parameter("x", ml.Shape.init(.f32, &.{ 2, 3 }));
        const weight = try b.parameter("weight", ml.Shape.init(.f32, &.{ 4, 3 }));
        const bias = try b.parameter("bias", ml.Shape.init(.f32, &.{4}));
        const gamma = try b.parameter("gamma", ml.Shape.init(.f32, &.{4}));
        const beta = try b.parameter("beta", ml.Shape.init(.f32, &.{4}));
        const dropout = try b.parameter("dropout", ml.Shape.init(.f32, &.{ 2, 4 }));
        const routes = try b.parameter("routes", ml.Shape.init(.i32, &.{3}));
        const projected = try b.linear(x, weight, bias, 2, 3, 4);
        const activated = try b.mul(try b.geluExact(projected), dropout);
        const normalized = try b.layerNorm(activated, gamma, beta, 4, 1e-5);
        const probabilities = try b.softmax(normalized);
        const selected = try b.gather(probabilities, routes, ml.Shape.init(.f32, &.{ 3, 4 }));
        const transposed = try b.transpose(selected, &.{ 1, 0 });
        const sliced = try b.sliceLastDim(transposed, 0, 2);
        const duplicated = try b.concat(sliced, sliced, 1);
        const mean = try b.reduceMean(duplicated, &.{0});
        const maximum = try b.reduceMax(mean, &.{1});
        const row_norm = try b.sqrt(try b.add(try b.reduceSum(try b.mul(x, x), &.{1}), try b.scalarConst(.f32, 1e-4)));
        const detached = try b.stopGradient(row_norm);
        const direction = try b.reduceSum(try b.absOp(try b.div(x, detached)), &.{ 0, 1 });
        const output = try b.add(try b.add(mean, maximum), direction);
        const seed = try b.parameter("cotangent", graph.node(output).output_shape);
        try graph.markOutput(output);
        return .{ .graph = graph, .parameters = .{ x, weight, bias, gamma, beta }, .seed = .{ .output = output, .cotangent = seed }, .inputs = .{
            .{ .id = x, .values = &.{ 0.2, -0.6, 0.9, 0.7, 0.4, -0.3 } },
            .{ .id = weight, .values = &.{ 0.3, -0.4, 0.6, 0.7, 0.2, -0.1, -0.6, 0.5, 0.4, 0.1, 0.8, -0.3 } },
            .{ .id = bias, .values = &.{ 0.1, -0.2, 0.05, 0.3 } },
            .{ .id = gamma, .values = &.{ 1.1, 0.9, 0.8, 1.2 } },
            .{ .id = beta, .values = &.{ 0.02, -0.05, 0.1, 0.04 } },
            .{ .id = dropout, .values = &.{ 1.25, 0, 1.25, 1.25, 1.25, 1.25, 0, 1.25 } },
            .{ .id = routes, .indices = &.{ 1, 0, 1 } },
        } };
    }
};

fn shape32(shape: ml.Shape, buffer: *[8]i32) []const i32 {
    for (shape.dims[0..shape.rank_], 0..) |dim, axis| buffer[axis] = @intCast(dim);
    return buffer[0..shape.rank_];
}
fn upload(cb: *const ops.ComputeBackend, values: []const f32, shape: []const i32) !ops.CT {
    return cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = shape } }, .{});
}
fn compare(cb: *const ops.ComputeBackend, actual: ops.CT, expected: []const f32, label: []const u8, index: usize) !void {
    const a = std.testing.allocator;
    const values = try a.alloc(f32, expected.len);
    defer a.free(values);
    try cb.glinerBoundaryDownload(actual, values);
    for (values, expected, 0..) |got, want, ordinal| {
        if (!std.math.isFinite(got) or @abs(got - want) > 2e-5 + 5e-5 * @abs(want)) {
            std.debug.print("resident program {s}[{d}] element{d}: expected={d} actual={d}\n", .{ label, index, ordinal, want, got });
            return error.TestExpectedApproxEqAbs;
        }
    }
}

test "resident program Metal compiled forward and retained cut VJP match native without activation downloads" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a);
    defer fixture.graph.deinit();
    var session = try seeded.Session.init(a, &fixture.graph, &.{fixture.seed}, &fixture.parameters, .{});
    defer session.deinit();
    var forward = try program_mod.Program.init(a, &session.differentiated.graph, session.captures, .{});
    defer forward.deinit();
    var backward = try program_mod.Program.init(a, &session.backward.graph, session.backward.graph.outputs.items, .{});
    defer backward.deinit();
    var device = try Device.init(a);
    defer device.deinit();
    const gpu = device.backend.computeBackend();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var native_backend = native.NativeCompute.init(a, &store, null);
    defer native_backend.deinit();
    const cpu = native_backend.computeBackend();
    var cpu_inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer cpu_inputs.deinit(a);
    defer for (cpu_inputs.items) |input| cpu.free(input.value);
    var gpu_inputs = std.ArrayListUnmanaged(program_mod.Binding).empty;
    defer gpu_inputs.deinit(a);
    defer for (gpu_inputs.items) |input| gpu.free(input.value);
    for (fixture.inputs) |input| {
        const id = session.differentiated.id_map[input.id];
        var dims: [8]i32 = undefined;
        const shape = shape32(fixture.graph.node(input.id).output_shape, &dims);
        const cpu_value = if (input.indices.len == 0) try cpu.fromFloat32Shape(input.values, shape) else (try cpu.fromInt32Shape(input.indices, shape)).?;
        cpu_inputs.append(a, .{ .node_id = id, .value = cpu_value }) catch |err| {
            cpu.free(cpu_value);
            return err;
        };
        const gpu_value = if (input.indices.len == 0) try upload(&gpu, input.values, shape) else (try gpu.fromInt32Shape(input.indices, shape)).?;
        gpu_inputs.append(a, .{ .node_id = id, .value = gpu_value }) catch |err| {
            gpu.free(gpu_value);
            return err;
        };
    }
    var reference_captures = try interpreter.captureNodeValues(a, &session.differentiated.graph, &cpu, .{ .runtime_inputs = cpu_inputs.items, .cached_analysis = session.forward_analysis, .strict_integer_constants = true }, session.captures);
    defer reference_captures.deinit(&cpu);
    const before = metal_tensor.memoryStatsSnapshot();
    var captures = try forward.execute(a, &gpu, gpu_inputs.items, null);
    defer captures.deinit(&gpu);
    var cpu_backward = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer cpu_backward.deinit(a);
    var gpu_backward = std.ArrayListUnmanaged(program_mod.Binding).empty;
    defer gpu_backward.deinit(a);
    for (cpu_inputs.items, gpu_inputs.items) |host, resident| {
        const id = session.backward.id_map[host.node_id];
        if (id != ml.null_node and backward.usesParameter(id)) {
            try cpu_backward.append(a, .{ .node_id = id, .value = host.value });
            try gpu_backward.append(a, .{ .node_id = id, .value = resident.value });
        }
    }
    for (session.captures, reference_captures.values, captures.outputs) |original, host, resident| {
        const id = session.backward.id_map[original];
        if (id != ml.null_node and backward.usesParameter(id)) {
            try cpu_backward.append(a, .{ .node_id = id, .value = host });
            try gpu_backward.append(a, .{ .node_id = id, .value = resident });
        }
    }
    const cotangent = [_]f32{ 1, -0.3, 0.4, 0.2 };
    const cpu_seed = try cpu.fromFloat32Shape(&cotangent, &.{ 1, 4 });
    defer cpu.free(cpu_seed);
    const gpu_seed = try upload(&gpu, &cotangent, &.{ 1, 4 });
    defer gpu.free(gpu_seed);
    const seed_id = session.backward.id_map[session.differentiated.id_map[fixture.seed.cotangent]];
    try cpu_backward.append(a, .{ .node_id = seed_id, .value = cpu_seed });
    try gpu_backward.append(a, .{ .node_id = seed_id, .value = gpu_seed });
    var reference = try interpreter.execute(a, &session.backward.graph, &cpu, .{ .runtime_inputs = cpu_backward.items, .strict_integer_constants = true });
    defer reference.deinit(&cpu);
    var gradients = try backward.execute(a, &gpu, gpu_backward.items, null);
    defer gradients.deinit(&gpu);
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    for (reference_captures.values, captures.outputs, 0..) |host, resident, i| {
        const values = try cpu.toFloat32(host, a);
        defer a.free(values);
        try compare(&gpu, resident, values, "capture", i);
    }
    try std.testing.expectEqual(@as(usize, 5), gradients.outputs.len);
    for (reference.outputs, gradients.outputs, 0..) |host, resident, i| {
        const values = try cpu.toFloat32(host, a);
        defer a.free(values);
        try compare(&gpu, resident, values, "gradient", i);
    }
}

fn executionAllocationCheck(a: Allocator, backend: *metal.MetalCompute, program: *const program_mod.Program, input: program_mod.Binding) !void {
    const original = backend.allocator;
    backend.allocator = a;
    defer backend.allocator = original;
    const cb = backend.computeBackend();
    var result = try program.execute(a, &cb, &.{input}, null);
    defer result.deinit(&cb);
}

test "resident program Metal allocation cancellation and external frame failures preserve bindings" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const input = try b.parameter("x", ml.Shape.init(.f32, &.{3}));
    const output = try b.add(try b.mul(input, input), try b.scalarConst(.f32, 0.5));
    var program = try program_mod.Program.init(a, &graph, &.{output}, .{});
    defer program.deinit();
    const source = try upload(&cb, &.{ 1, 2, 3 }, &.{3});
    defer cb.free(source);
    const binding = program_mod.Binding{ .node_id = input, .value = source };
    const before = metal_tensor.memoryStatsSnapshot();
    try std.testing.checkAllAllocationFailures(a, executionAllocationCheck, .{ device.backend, &program, binding });
    const Cancel = struct {
        calls: usize = 0,
        at: usize,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.at) return error.Cancelled;
        }
    };
    for (1..13) |at| {
        var cancel = Cancel{ .at = at };
        try std.testing.expectError(error.Cancelled, program.execute(a, &cb, &.{binding}, .{ .ptr = &cancel, .check_fn = Cancel.check }));
    }
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_live_bytes, after.device_owned_live_bytes);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try compare(&cb, source, &.{ 1, 2, 3 }, "unchanged", 0);
    const wrong_shape = try upload(&cb, &.{ 1, 2, 3 }, &.{ 1, 3 });
    defer cb.free(wrong_shape);
    try std.testing.expectError(error.InvalidResidentProgramShape, program.execute(a, &cb, &.{.{ .node_id = input, .value = wrong_shape }}, null));
    try metal_runtime.beginFrame(device.backend.provider_impl.raw_decode_runtime);
    defer metal_runtime.cancelFrame(device.backend.provider_impl.raw_decode_runtime) catch {};
    try std.testing.expectError(error.ResidentTrainingExternalFrame, program.execute(a, &cb, &.{binding}, null));
}
