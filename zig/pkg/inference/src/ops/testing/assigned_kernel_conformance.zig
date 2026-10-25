// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One device-independent fixture for assigned CPU/Metal/CUDA kernels. Inputs,
//! output and workspace use the public physical-storage protocol; only explicit
//! setup/verification copies are outside the strict resident dispatch scope.
const std = @import("std");
const ops = @import("../ops.zig");
const activations_mod = @import("../../backends/activations.zig");
const CT = ops.CT;

pub fn physicalTensor(cb: *ops.ComputeBackend, values: []const f32, shape: []const i32) !CT {
    const lease = (try ops.physical_storage.Lease.create(std.testing.allocator, cb.*, values.len * @sizeOf(f32))) orelse return error.PhysicalStorageUnsupported;
    defer lease.release();
    const kernels = cb.vtable.kernels.?;
    if (kernels.writePhysicalFloat32 == null) {
        const host = (try cb.hostFloat32Write(lease.tensor)) orelse return error.HostStorageWriteUnsupported;
        @memcpy(host[0..values.len], values);
    }
    const view = (try kernels.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = shape })) orelse return error.PhysicalViewUnsupported;
    errdefer cb.free(view);
    if (kernels.writePhysicalFloat32) |write| try write(cb.ptr, view, values);
    return view;
}

pub fn run(backend: *ops.ComputeBackend, comptime supports_layouts: bool) !void {
    try runWithFactory(backend, supports_layouts, physicalTensor);
}

pub fn runWithFactory(backend: *ops.ComputeBackend, comptime supports_layouts: bool, comptime makeTensor: anytype) !void {
    const K = ops.kernel_descriptor;
    const Shape = @import("ml").graph.Shape;
    const Case = struct {
        fn run(cb: *ops.ComputeBackend, descriptor: K.Descriptor, data: []const []const f32, expected: []const f32) !void {
            const a = std.testing.allocator;
            var inputs: [3]CT = undefined;
            var count: usize = 0;
            defer for (inputs[0..count]) |input| cb.free(input);
            for (data, 0..) |values, i| {
                const shape = descriptor.input_shapes[i];
                var dims: [8]i32 = undefined;
                for (0..shape.rank()) |axis| dims[axis] = @intCast(shape.dims[axis]);
                inputs[i] = try makeTensor(cb, values, dims[0..shape.rank()]);
                count += 1;
            }
            var output_dims: [8]i32 = undefined;
            for (0..descriptor.output_shape.rank()) |axis| output_dims[axis] = @intCast(descriptor.output_shape.dims[axis]);
            const sentinel = try a.alloc(f32, expected.len);
            defer a.free(sentinel);
            @memset(sentinel, -99);
            const output = try makeTensor(cb, sentinel, output_dims[0..descriptor.output_shape.rank()]);
            defer cb.free(output);
            const dispatch = cb.vtable.kernels.?.runAssignedKernel.?;
            const requirements = if (cb.vtable.kernels.?.assignedKernelWorkspace) |query| try query(cb.ptr, &descriptor) else K.WorkspaceRequirement{};
            var workspace: ?CT = null;
            defer if (workspace) |value| cb.free(value);
            if (requirements.byte_size != 0) {
                const zeros = try a.alloc(f32, requirements.byte_size / @sizeOf(f32));
                defer a.free(zeros);
                @memset(zeros, 0);
                workspace = try makeTensor(cb, zeros, &.{@intCast(zeros.len)});
            }
            var request = K.Invocation{ .descriptor = &descriptor, .inputs = inputs[0..count], .workspace = workspace };
            const audit_mod = @import("../../execution/transfer_audit.zig");
            if (supports_layouts and descriptor.operation == .gather and data[0].len == expected.len) {
                var alias = request;
                alias.output = inputs[0];
                var audit = audit_mod.Audit{ .forbid_device_host_access = true };
                try audit_mod.begin(&audit);
                const rejected = dispatch(cb.ptr, alias);
                audit_mod.end();
                try std.testing.expectError(error.InvalidAssignedAlias, rejected);
                const retained = try cb.toFloat32(inputs[0], a);
                defer a.free(retained);
                try std.testing.expectEqualSlices(f32, data[0], retained);
            }
            for (0..3) |_| {
                var audit = audit_mod.Audit{ .forbid_device_host_access = true };
                try audit_mod.begin(&audit);
                const admitted = dispatch(cb.ptr, request);
                audit_mod.end();
                try std.testing.expect(try admitted);
                // Legality queries leave an already-bound output untouched.
                if (request.output == null) {
                    const untouched = try cb.toFloat32(output, a);
                    defer a.free(untouched);
                    try std.testing.expectEqualSlices(f32, sentinel, untouched);
                }
                request.output = output;
                try audit_mod.begin(&audit);
                const executed = dispatch(cb.ptr, request);
                audit_mod.end();
                try std.testing.expect(try executed);
                const actual = try cb.toFloat32(output, a);
                defer a.free(actual);
                try std.testing.expectEqual(expected.len, actual.len);
                for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.00003);
            }
            for (data, inputs[0..count]) |values, input| {
                const actual = try cb.toFloat32(input, a);
                defer a.free(actual);
                try std.testing.expectEqualSlices(f32, values, actual);
            }
        }
    };
    const shape = Shape.init(.f32, &.{ 2, 17 });
    var x: [34]f32 = undefined;
    for (&x, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i + 1)) / 32;
    inline for (std.meta.tags(K.Unary)) |unary| {
        var expected = x;
        switch (unary) {
            .gelu => activations_mod.gelu(&expected),
            .gelu_exact => activations_mod.geluExact(&expected),
            .silu => activations_mod.silu(&expected),
            else => for (&expected) |*v| {
                v.* = switch (unary) {
                    .negate => -v.*,
                    .sqrt => @sqrt(v.*),
                    .rsqrt => 1 / @sqrt(v.*),
                    .exp => @exp(v.*),
                    .log => @log(v.*),
                    .sin => @sin(v.*),
                    .cos => @cos(v.*),
                    .tanh => std.math.tanh(v.*),
                    .erf => activations_mod.erfApproxF32(v.*),
                    .abs => @abs(v.*),
                    .relu => @max(0, v.*),
                    else => unreachable,
                };
            },
        }
        try Case.run(backend, .{ .operation = .{ .unary = unary }, .output_shape = shape, .input_shapes = .{ shape, undefined, undefined }, .input_count = 1 }, &.{&x}, &expected);
    }
    inline for (std.meta.tags(K.Binary)) |binary| {
        var expected: [34]f32 = undefined;
        for (&expected, x) |*v, value| v.* = switch (binary) {
            .add => value + value,
            .subtract => 0,
            .multiply => value * value,
            .divide => 1,
        };
        try Case.run(backend, .{ .operation = .{ .binary = binary }, .output_shape = shape, .input_shapes = .{ shape, shape, undefined }, .input_count = 2 }, &.{ &x, &x }, &expected);
        if (binary == .subtract or binary == .divide) {
            for ([_]bool{ false, true }) |left| {
                for (&expected, x) |*v, value| v.* = if (binary == .subtract) (if (left) 2 - value else value - 2) else (if (left) 2 / value else value / 2);
                try Case.run(backend, .{ .operation = .{ .binary = binary }, .output_shape = shape, .input_shapes = if (left) .{ Shape.init(.f32, &.{1}), shape, undefined } else .{ shape, Shape.init(.f32, &.{1}), undefined }, .input_count = 2 }, if (left) &.{ &.{2}, &x } else &.{ &x, &.{2} }, &expected);
            }
        }
    }
    for ([_]bool{ false, true }) |logarithmic| {
        var expected: [34]f32 = undefined;
        for (0..2) |row| {
            var sum: f32 = 0;
            for (x[row * 17 ..][0..17]) |value| sum += @exp(value);
            for (0..17) |column| {
                const i = row * 17 + column;
                expected[i] = if (logarithmic) x[i] - @log(sum) else @exp(x[i]) / sum;
            }
        }
        try Case.run(backend, .{ .operation = .{ .softmax = .{ .width = 17, .logarithmic = logarithmic } }, .output_shape = shape, .input_shapes = .{ shape, undefined, undefined }, .input_count = 1 }, &.{&x}, &expected);
    }
    inline for (std.meta.tags(K.ReductionMode)) |mode| {
        var expected: [2]f32 = undefined;
        for (0..2) |row| {
            var result: f32 = if (mode == .max) -std.math.inf(f32) else 0;
            for (x[row * 17 ..][0..17]) |value| result = if (mode == .max) @max(result, value) else result + value;
            expected[row] = if (mode == .mean) result / 17 else result;
        }
        const geometry = try K.ReductionGeometry.init(&.{ 2, 17 }, &.{1});
        try Case.run(backend, .{ .operation = .{ .reduction = .{ .mode = mode, .geometry = geometry } }, .output_shape = Shape.init(.f32, &.{ 2, 1 }), .input_shapes = .{ shape, undefined, undefined }, .input_count = 1 }, &.{&x}, &expected);
    }
    const small = Shape.init(.f32, &.{ 2, 3 });
    if (supports_layouts) {
        try Case.run(backend, .{ .operation = .{ .transpose = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .input_rank = 2, .output_rank = 2 } }, .output_shape = Shape.init(.f32, &.{ 3, 2 }), .input_shapes = .{ small, undefined, undefined }, .input_count = 1 }, &.{&.{ 1, 2, 3, 4, 5, 6 }}, &.{ 1, 4, 2, 5, 3, 6 });
        try Case.run(backend, .{ .operation = .{ .broadcast = .{ .axes = .{ 0, 1, 0, 0, 0, 0, 0, 0 }, .input_rank = 2, .output_rank = 2 } }, .output_shape = small, .input_shapes = .{ Shape.init(.f32, &.{ 2, 1 }), undefined, undefined }, .input_count = 1 }, &.{&.{ 2, 7 }}, &.{ 2, 2, 2, 7, 7, 7 });
    }
    if (supports_layouts) {
        var shape_scratch: [8]i64 = undefined;
        const last = try K.kernel_geometry.ConcatAxis.init(&.{ 2, 1 }, &.{ 2, 2 }, 1, &shape_scratch);
        try Case.run(backend, .{ .operation = .{ .concat = last }, .output_shape = small, .input_shapes = .{ Shape.init(.f32, &.{ 2, 1 }), Shape.init(.f32, &.{ 2, 2 }), undefined }, .input_count = 2 }, &.{ &.{ 1, 2 }, &.{ 3, 4, 5, 6 } }, &.{ 1, 3, 4, 2, 5, 6 });
        const first = try K.kernel_geometry.ConcatAxis.init(&.{ 1, 2 }, &.{ 2, 2 }, 0, &shape_scratch);
        try Case.run(backend, .{ .operation = .{ .concat = first }, .output_shape = Shape.init(.f32, &.{ 3, 2 }), .input_shapes = .{ Shape.init(.f32, &.{ 1, 2 }), Shape.init(.f32, &.{ 2, 2 }), undefined }, .input_count = 2 }, &.{ &.{ 1, 2 }, &.{ 3, 4, 5, 6 } }, &.{ 1, 2, 3, 4, 5, 6 });
        const middle = try K.kernel_geometry.ConcatAxis.init(&.{ 2, 1, 2 }, &.{ 2, 2, 2 }, 1, &shape_scratch);
        try Case.run(backend, .{ .operation = .{ .concat = middle }, .output_shape = Shape.init(.f32, &.{ 2, 3, 2 }), .input_shapes = .{ Shape.init(.f32, &.{ 2, 1, 2 }), Shape.init(.f32, &.{ 2, 2, 2 }), undefined }, .input_count = 2 }, &.{ &.{ 1, 2, 3, 4 }, &.{ 5, 6, 7, 8, 9, 10, 11, 12 } }, &.{ 1, 2, 5, 6, 7, 8, 3, 4, 9, 10, 11, 12 });
        const sliced = try K.Slice.init(&.{ 2, 5 }, &.{ 2, 2 }, &.{ 0, 1 }, &.{ 2, 5 }, &.{ 1, 2 });
        try Case.run(backend, .{ .operation = .{ .slice = sliced }, .output_shape = Shape.init(.f32, &.{ 2, 2 }), .input_shapes = .{ Shape.init(.f32, &.{ 2, 5 }), undefined, undefined }, .input_count = 1 }, &.{&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }}, &.{ 2, 4, 7, 9 });
        const gather = K.Gather{ .axis = 0, .geometry = try K.kernel_geometry.GatherAxis.init(std.math.maxInt(u32), &.{ 3, 2 }, 0) };
        try Case.run(backend, .{ .operation = .{ .gather = gather }, .output_shape = Shape.init(.f32, &.{ 3, 2 }), .input_shapes = .{ Shape.init(.f32, &.{ 3, 2 }), Shape.init(.f32, &.{3}), undefined }, .input_count = 2 }, &.{ &.{ 1, 2, 3, 4, 5, 6 }, &.{ -1, 0, 0 } }, &.{ 5, 6, 1, 2, 1, 2 });
    }
    inline for ([_]bool{ false, true }) |transpose_rhs| {
        const rhs = if (transpose_rhs) Shape.init(.f32, &.{ 2, 3 }) else Shape.init(.f32, &.{ 3, 2 });
        const geometry = try K.kernel_geometry.planDenseDotGeneral(&.{ 2, 3 }, rhs.dims[0..rhs.rank()], &.{1}, if (transpose_rhs) &.{1} else &.{0}, &.{}, &.{});
        try Case.run(backend, .{ .operation = .{ .dot = geometry }, .output_shape = Shape.init(.f32, &.{ 2, 2 }), .input_shapes = .{ small, rhs, undefined }, .input_count = 2 }, &.{ &.{ 1, 2, 3, 4, 5, 6 }, if (transpose_rhs) &.{ 1, 3, 5, 2, 4, 6 } else &.{ 1, 2, 3, 4, 5, 6 } }, &.{ 22, 28, 49, 64 });
    }
}
