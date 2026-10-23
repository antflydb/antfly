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

//! Shared ownership for physical allocations and tensor views. The execution
//! plan owns one reference; outstanding values pin storage across plan eviction.
const std = @import("std");
const ops = @import("ops.zig");

pub const Lease = struct {
    allocator: std.mem.Allocator,
    backend: ops.ComputeBackend,
    tensor: ops.CT,
    byte_size: usize,
    references: std.atomic.Value(usize) = .init(1),

    pub fn create(allocator: std.mem.Allocator, backend: ops.ComputeBackend, byte_size: usize) !?*Lease {
        if (byte_size == 0 or byte_size % @sizeOf(f32) != 0 or byte_size / @sizeOf(f32) > std.math.maxInt(i32)) return null;
        const self = try allocator.create(Lease);
        errdefer allocator.destroy(self);
        const allocate_storage = if (backend.vtable.kernels) |functions| functions.allocatePhysicalStorage else null;
        const tensor = (if (allocate_storage) |allocate| try allocate(backend.ptr, byte_size) else try backend.allocUninitF32Shape(&.{@intCast(byte_size / @sizeOf(f32))})) orelse {
            allocator.destroy(self);
            return null;
        };
        self.* = .{ .allocator = allocator, .backend = backend, .tensor = tensor, .byte_size = byte_size };
        return self;
    }
    pub fn retain(self: *Lease) void {
        const before = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(before > 0);
    }
    pub fn unique(self: *const Lease) bool {
        return self.references.load(.acquire) == 1;
    }
    pub fn release(self: *Lease) void {
        const before = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(before > 0);
        if (before != 1) return;
        self.backend.free(self.tensor);
        self.allocator.destroy(self);
    }
};

/// A captured executable owns every planned address it references. This is
/// separate from an invocation's tensor handles: graph replay omits the host
/// calls which would otherwise keep those handles alive. Release only after
/// destroying the executable and completing its last submission.
pub const CapturedLeases = struct {
    values: std.ArrayListUnmanaged(*Lease) = .empty,

    pub fn retain(self: *CapturedLeases, allocator: std.mem.Allocator, lease: *Lease) !void {
        for (self.values.items) |existing| if (existing == lease) return;
        try self.values.append(allocator, lease);
        lease.retain();
    }
    pub fn deinit(self: *CapturedLeases, allocator: std.mem.Allocator) void {
        for (self.values.items) |lease| lease.release();
        self.values.deinit(allocator);
        self.* = .{};
    }
};

pub const View = struct { storage: *Lease, shape: []const i32 };

/// A recorded submission fence. Waiting may enqueue a device dependency; it
/// need not block the host. The owner records before publishing a frame as idle.
pub const Completion = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        record: *const fn (*anyopaque) anyerror!void,
        before_reuse: *const fn (*anyopaque) anyerror!void,
        wait: ?*const fn (*anyopaque) anyerror!void = null,
        deinit: *const fn (*anyopaque) void,
    };
    pub fn record(self: Completion) !void {
        return self.vtable.record(self.ptr);
    }
    pub fn beforeReuse(self: Completion) !void {
        return self.vtable.before_reuse(self.ptr);
    }
    pub fn wait(self: Completion) !void {
        if (self.vtable.wait) |synchronize| try synchronize(self.ptr);
    }
    pub fn deinit(self: Completion) void {
        self.vtable.deinit(self.ptr);
    }
};

test "physical storage pins views across ownership release and assigned writes preserve inputs" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const storage = (try Lease.create(a, cb, 16 * @sizeOf(f32))).?;
    var storage_owned = true;
    defer if (storage_owned) storage.release();
    try std.testing.expect(storage.unique());
    const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{ 2, 4 } })).?;
    defer cb.free(view);
    try std.testing.expect(!storage.unique());
    const input = try cb.fromFloat32Shape(&.{ -4, -3, -2, -1, 0, 1, 2, 3 }, &.{ 2, 4 });
    defer cb.free(input);
    const ml = @import("ml");
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 4 });
    const descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .unary = .relu }, .output_shape = shape, .input_shapes = .{ shape, undefined, undefined }, .input_count = 1 };
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{input} }));
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{input}, .output = view }));
    const values = try cb.toFloat32(view, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 1, 2, 3 }, values);
    const unchanged = try cb.toFloat32(input, a);
    defer a.free(unchanged);
    try std.testing.expectEqualSlices(f32, &.{ -4, -3, -2, -1, 0, 1, 2, 3 }, unchanged);
    // Drop the plan's ownership. The returned view still owns its storage.
    storage.release();
    storage_owned = false;
    const retained = try cb.toFloat32(view, a);
    defer a.free(retained);
    try std.testing.expectEqualSlices(f32, values, retained);
}

test "physical storage and view metadata release on every allocation failure" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            const cb = context.computeBackend();
            const lease = (try Lease.create(a, cb, 64)).?;
            defer lease.release();
            const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = &.{ 2, 4 } })).?;
            defer cb.free(view);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "assigned normalization and softmax match allocating kernels on stable storage" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const gamma = try b.parameter("gamma", ml.graph.Shape.init(.f32, &.{4}));
    const beta = try b.parameter("beta", ml.graph.Shape.init(.f32, &.{4}));
    const nodes = [_]ml.graph.NodeId{ try b.layerNorm(x, gamma, beta, 4, 1e-5), try b.rmsNorm(x, gamma, 4, 1e-5), try b.softmax(x), try b.logSoftmax(x) };
    const input = try cb.fromFloat32Shape(&.{ -100, 0, 100, 101, 1, 1, 1, 1 }, &.{ 2, 4 });
    defer cb.free(input);
    const weight = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
    defer cb.free(weight);
    const bias = try cb.fromFloat32Shape(&.{ -1, 1, -2, 2 }, &.{4});
    defer cb.free(bias);
    const storage = (try Lease.create(a, cb, 8 * @sizeOf(f32))).?;
    defer storage.release();
    const output = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{ 2, 4 } })).?;
    defer cb.free(output);
    for (nodes, 0..) |node, index| {
        const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
        const operands = [_]@import("../graph/backend_contracts.zig").CT{ input, weight, bias };
        const inputs = operands[0..descriptor.input_count];
        try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = inputs }));
        try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = inputs, .output = output }));
        const reference = switch (index) {
            0 => try cb.layerNorm(input, weight, bias, 4, 1e-5),
            1 => try cb.rmsNorm(input, weight, 4, 1e-5),
            2 => try cb.primSoftmax(input, 4),
            else => try cb.primLogSoftmax(input, 4),
        };
        defer cb.free(reference);
        const expected = try cb.toFloat32(reference, a);
        defer a.free(expected);
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    // A malformed learned width must decline before any target submission.
    graph.nodeMut(nodes[0]).op.fused_layer_norm.dim = 3;
    try std.testing.expect(ops.kernel_descriptor.lower(&graph, nodes[0]) == null);
}

test "assigned reductions share axis geometry preserve traversal and reject invalid axes" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3, 2 }));
    const nodes = [_]ml.graph.NodeId{ try b.reduceSum(x, &.{ 0, 2 }), try b.reduceMean(x, &.{ 0, 2 }), try b.reduceMax(x, &.{ 0, 2 }), try b.reduceSum(x, &.{}) };
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, &.{ 2, 3, 2 });
    defer cb.free(input);
    const storage = (try Lease.create(a, cb, 12 * @sizeOf(f32))).?;
    defer storage.release();
    for (nodes, 0..) |node, index| {
        const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
        var dims: [8]i32 = undefined;
        for (descriptor.output_shape.dims[0..descriptor.output_shape.rank()], 0..) |dim, i| dims[i] = @intCast(dim);
        const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = dims[0..descriptor.output_shape.rank()] })).?;
        defer cb.free(view);
        try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{input}, .output = view }));
        const actual = try cb.toFloat32(view, a);
        defer a.free(actual);
        const expected: []const f32 = switch (index) {
            0 => &.{ 18, 26, 34 },
            1 => &.{ 4.5, 6.5, 8.5 },
            2 => &.{ 8, 10, 12 },
            else => &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 },
        };
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    const geometry = try ops.kernel_descriptor.ReductionGeometry.init(&.{ 2, 3, 2 }, &.{ 0, 2, 0 });
    try std.testing.expectEqual(@as(usize, 4), geometry.reduce_count);
    try std.testing.expectEqual(@as(usize, 3), geometry.output_count);
    try std.testing.expectError(error.InvalidShape, ops.kernel_descriptor.ReductionGeometry.init(&.{ 2, 3, 2 }, &.{3}));
    graph.nodeMut(nodes[0]).op.reduce_sum.axes[0] = 8;
    try std.testing.expect(ops.kernel_descriptor.lower(&graph, nodes[0]) == null);
}

test "assigned dense contractions preserve all matrix layouts and batched accumulation" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    for ([_]bool{ false, true }) |batched| {
        for ([_]bool{ false, true }) |left_transposed| for ([_]bool{ false, true }) |right_transposed| {
            const left_dims = [_]i64{ 2, if (left_transposed) 4 else 2, if (left_transposed) 2 else 4 };
            const right_dims = [_]i64{ 2, if (right_transposed) 3 else 4, if (right_transposed) 4 else 3 };
            const left_shape = left_dims[if (batched) @as(usize, 0) else 1..];
            const right_shape = right_dims[if (batched) @as(usize, 0) else 1..];
            var graph = ml.graph.Graph.init(a);
            defer graph.deinit();
            var b = ml.graph.Builder.init(&graph);
            const lhs = try b.parameter("lhs", ml.graph.Shape.init(.f32, left_shape));
            const rhs = try b.parameter("rhs", ml.graph.Shape.init(.f32, right_shape));
            const node = if (batched) try b.matmul3DLayout(lhs, rhs, left_transposed, right_transposed) else try b.matmul2DLayout(lhs, rhs, left_transposed, right_transposed);
            const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
            var left_values: [16]f32 = undefined;
            var right_values: [24]f32 = undefined;
            for (&left_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 7)) / 8;
            for (&right_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 11)) / 8;
            const left_count = if (batched) @as(usize, 16) else 8;
            const right_count = if (batched) @as(usize, 24) else 12;
            var ld: [3]i32 = undefined;
            var rd: [3]i32 = undefined;
            for (left_shape, 0..) |dim, i| ld[i] = @intCast(dim);
            for (right_shape, 0..) |dim, i| rd[i] = @intCast(dim);
            const left = try cb.fromFloat32Shape(left_values[0..left_count], ld[0..left_shape.len]);
            defer cb.free(left);
            const right = try cb.fromFloat32Shape(right_values[0..right_count], rd[0..right_shape.len]);
            defer cb.free(right);
            const attrs = graph.node(node).op.dot_general;
            const reference = try cb.primDotGeneral(left, right, left_shape, right_shape, attrs.lhs_contracting[0..1], attrs.rhs_contracting[0..1], attrs.lhs_batch[0..attrs.num_batch], attrs.rhs_batch[0..attrs.num_batch]);
            defer cb.free(reference);
            const storage = (try Lease.create(a, cb, descriptor.elements() * @sizeOf(f32))).?;
            defer storage.release();
            var output_shape: [8]i32 = undefined;
            for (descriptor.output_shape.dims[0..descriptor.output_shape.rank()], 0..) |dim, i| output_shape[i] = @intCast(dim);
            const output = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = output_shape[0..descriptor.output_shape.rank()] })).?;
            defer cb.free(output);
            try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{ left, right }, .output = output }));
            const expected = try cb.toFloat32(reference, a);
            defer a.free(expected);
            const actual = try cb.toFloat32(output, a);
            defer a.free(actual);
            try std.testing.expectEqualSlices(f32, expected, actual);
        };
    }
}

test "assigned linear keeps source-backed weight scheduling without materializing its input handle" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("weight", ml.graph.Shape.init(.f32, &.{ 3, 4 }));
    const bias_node = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{3}));
    const node = try b.linear(x, w, bias_node, 2, 4, 3);
    const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 2, 4 });
    defer cb.free(input);
    const bias = try cb.fromFloat32Shape(&.{ 0.5, -0.5, 1 }, &.{3});
    defer cb.free(bias);
    const tensor = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "weight", &.{ 3, 4 }, &.{ 1, 0, -1, 0, 0, 1, 0, -1, 1, 1, 1, 1 });
    const weight = try context.native.compute.importOwnedStaticTensor(tensor);
    defer cb.free(weight);
    const storage = (try Lease.create(a, cb, 6 * @sizeOf(f32))).?;
    defer storage.release();
    const output = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{ 2, 3 } })).?;
    defer cb.free(output);
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{ input, weight, bias } }));
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{ input, weight, bias }, .output = output }));
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ -1.5, -2.5, 11, -1.5, -2.5, 27 }, values);
}

test "captured physical addresses deduplicate and survive executable plan eviction" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            const cb = context.computeBackend();
            const lease = (try Lease.create(a, cb, 32)).?;
            var owner = true;
            defer if (owner) lease.release();
            var captured = CapturedLeases{};
            defer captured.deinit(a);
            try captured.retain(a, lease);
            try captured.retain(a, lease);
            try std.testing.expectEqual(@as(usize, 1), captured.values.items.len);
            try std.testing.expectEqual(@as(usize, 2), lease.references.load(.acquire));
            lease.release();
            owner = false;
            // The executable's owner is gone; replay still owns valid storage.
            try std.testing.expect(lease.unique());
            const data = (try cb.hostFloat32Write(lease.tensor)).?;
            data[0] = 42;
            try std.testing.expectEqual(@as(f32, 42), (try cb.hostFloat32Read(lease.tensor)).?[0]);
            captured.deinit(a);
            // Aborted/failed capture cleanup is idempotent.
            captured.deinit(a);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "row snapshots keep their own values after source storage changes" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
    defer cb.free(input);
    const published = try cb.snapshotRows2D(a, input, 1, 1, 2);
    defer cb.free(published);
    @memset((try cb.hostFloat32Write(input)).?, 9);
    const values = try cb.toFloat32(published, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, values);
}
