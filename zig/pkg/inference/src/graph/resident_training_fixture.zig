// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test-only bridge from the pinned native fixture bindings to strict resident
//! programs. This never enables the production seeded-session Metal guard.
//! All readbacks happen after both programs finish and are diagnostic only.
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
const metal = @import("../ops/metal_compute.zig");
const gpu_store = @import("../ops/gpu_hosted_store.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const program = @import("resident_training_program.zig");
const seeded = @import("seeded_training.zig");
const interpreter = @import("interpreter.zig");
const Allocator = std.mem.Allocator;

pub const Device = struct {
    allocator: Allocator,
    store: *gpu_store.WeightStore,
    backend: *metal.MetalCompute,

    pub fn init(a: Allocator) !Device {
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

    pub fn deinit(self: *Device) void {
        self.backend.deinit();
        self.allocator.destroy(self.backend);
        metal.deinitSharedNativeProvider(self.store);
        metal.deinitPrefetchQueue(self.store);
        self.store.lazy_weights.deinit(self.allocator);
        self.allocator.destroy(self.store);
        self.* = undefined;
    }
};

fn uploadNative(a: Allocator, device: *const ops.ComputeBackend, source: *const ops.ComputeBackend, value: ops.CT, shape: ml.Shape) !ops.CT {
    if (source.kind() != .native) return error.InvalidResidentFixtureSource;
    try seeded.validateTensor(a, source, value, shape);
    var dimensions: [8]i32 = undefined;
    if (shape.rank_ > dimensions.len) return error.InvalidResidentFixtureShape;
    for (shape.dims[0..shape.rank_], dimensions[0..shape.rank_]) |dim, *out| out.* = std.math.cast(i32, dim) orelse return error.InvalidResidentFixtureShape;
    const dims = dimensions[0..shape.rank_];
    switch (shape.dtype) {
        .f32 => {
            const values = try source.toFloat32(value, a);
            defer a.free(values);
            return device.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = dims } }, .{});
        },
        .i32 => {
            const exported = (try source.exportTensorData(value, a)) orelse return error.InvalidResidentFixtureIndices;
            defer switch (exported.payload) {
                .bytes => |bytes| a.free(bytes),
                .quantized_f32 => |quantized| {
                    a.free(quantized.raw_bytes);
                    a.free(quantized.shape);
                },
            };
            if (exported.dtype != .i32 or exported.payload != .bytes) return error.InvalidResidentFixtureIndices;
            const raw = exported.payload.bytes;
            if (raw.len % @sizeOf(i32) != 0) return error.InvalidResidentFixtureIndices;
            const values = try a.alloc(i32, raw.len / @sizeOf(i32));
            defer a.free(values);
            // SafeTensor and native export bytes need not have i32 alignment.
            for (std.mem.bytesAsSlice(i32, raw), values) |item, *out| out.* = item;
            return device.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = values, .shape = dims } }, .{});
        },
        else => return error.InvalidResidentFixtureDType,
    }
}

pub const Result = struct {
    session: *const seeded.Session,
    captures: program.Result,
    gradients: ?program.Result,

    pub fn logits(self: *const Result, seed_index: usize) !ops.CT {
        if (seed_index >= self.session.seeds.len) return error.InvalidResidentFixtureSeed;
        const output = self.session.differentiated.id_map[self.session.seeds[seed_index].output];
        const index = std.mem.indexOfScalar(ml.NodeId, self.session.captures, output) orelse return error.InvalidResidentFixtureSeed;
        return self.captures.outputs[index];
    }

    pub fn deinit(self: *Result, cb: *const ops.ComputeBackend) void {
        if (self.gradients) |*gradients| gradients.deinit(cb);
        self.captures.deinit(cb);
        self.* = undefined;
    }
};

/// Validate every reachable forward and actual-cut backward instruction using
/// the same descriptors as Metal, even when the test runs on a CPU-only host.
pub fn validate(a: Allocator, session: *const seeded.Session) !void {
    var forward = try program.Program.init(a, &session.differentiated.graph, session.captures, .{});
    defer forward.deinit();
    var backward = try program.Program.init(a, &session.backward.graph, session.backward.graph.outputs.items, .{});
    defer backward.deinit();
}

/// Native tensors are only input fixtures. The resident execution uses fresh
/// explicit uploads, physical i32 routing and independent device captures.
/// Cotangents are in declared seed order; null runs the forward fixtures only.
pub fn run(a: Allocator, device: *const ops.ComputeBackend, source: *const ops.ComputeBackend, session: *const seeded.Session, inputs: []const interpreter.RuntimeInput, cotangents: ?[]const ops.CT) !Result {
    if (device.kind() != .metal or source.kind() != .native) return error.InvalidResidentFixtureBackend;
    if (cotangents) |values| if (values.len != session.seeds.len) return error.InvalidResidentFixtureSeed;
    var forward = try program.Program.init(a, &session.differentiated.graph, session.captures, .{});
    defer forward.deinit();
    var backward: ?program.Program = if (cotangents != null) try program.Program.init(a, &session.backward.graph, session.backward.graph.outputs.items, .{}) else null;
    defer if (backward) |*compiled| compiled.deinit();
    var owned = std.ArrayListUnmanaged(ops.CT).empty;
    defer {
        for (owned.items) |value| device.free(value);
        owned.deinit(a);
    }
    var forward_bindings = std.ArrayListUnmanaged(program.Binding).empty;
    defer forward_bindings.deinit(a);
    var backward_bindings = std.ArrayListUnmanaged(program.Binding).empty;
    defer backward_bindings.deinit(a);
    for (inputs) |input| {
        if (input.node_id >= session.original_parameters.len or !session.original_parameters[input.node_id]) return error.InvalidResidentFixtureBinding;
        const id = session.differentiated.id_map[input.node_id];
        if (id == ml.null_node) continue;
        const backward_id = session.backward.id_map[id];
        const forward_used = forward.usesParameter(id);
        const backward_used = if (backward) |*compiled| backward_id != ml.null_node and compiled.usesParameter(backward_id) else false;
        if (!forward_used and !backward_used) continue;
        const value = try uploadNative(a, device, source, input.value, session.differentiated.graph.node(id).output_shape);
        owned.append(a, value) catch |err| {
            device.free(value);
            return err;
        };
        if (forward_used) try forward_bindings.append(a, .{ .node_id = id, .value = value });
        if (backward_used) try backward_bindings.append(a, .{ .node_id = backward_id, .value = value });
    }
    if (cotangents) |values| for (session.seeds, values) |seed, tensor| {
        const id = session.differentiated.id_map[seed.cotangent];
        const backward_id = session.backward.id_map[id];
        if (backward_id == ml.null_node or !backward.?.usesParameter(backward_id)) continue;
        const value = try uploadNative(a, device, source, tensor, session.differentiated.graph.node(id).output_shape);
        owned.append(a, value) catch |err| {
            device.free(value);
            return err;
        };
        try backward_bindings.append(a, .{ .node_id = backward_id, .value = value });
    };
    const before = metal_tensor.memoryStatsSnapshot();
    var captures = try forward.execute(a, device, forward_bindings.items, null);
    errdefer captures.deinit(device);
    var gradients: ?program.Result = null;
    errdefer if (gradients) |*values| values.deinit(device);
    if (backward) |*compiled| {
        for (session.captures, captures.outputs) |original, value| {
            const id = session.backward.id_map[original];
            if (id != ml.null_node and compiled.usesParameter(id)) try backward_bindings.append(a, .{ .node_id = id, .value = value });
        }
        gradients = try compiled.execute(a, device, backward_bindings.items, null);
    }
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    return .{ .session = session, .captures = captures, .gradients = gradients };
}

pub fn expectValues(a: Allocator, cb: *const ops.ComputeBackend, value: ops.CT, expected: []const f32, absolute: f32, relative: f32, label: []const u8) !void {
    const actual = try a.alloc(f32, expected.len);
    defer a.free(actual);
    try cb.glinerBoundaryDownload(value, actual);
    for (expected, actual, 0..) |want, got, index| {
        if (!std.math.isFinite(got) or @abs(got - want) > absolute + relative * @abs(want)) {
            std.debug.print("resident fixture {s} element{d}: expected={d} actual={d} tolerance={d}\n", .{ label, index, want, got, absolute + relative * @abs(want) });
            return error.TestExpectedApproxEqAbs;
        }
    }
}
