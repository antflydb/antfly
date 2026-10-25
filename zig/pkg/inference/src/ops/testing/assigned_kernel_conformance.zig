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
            const ordinary_norm: ?[]f32 = if (descriptor.operation == .norm) blk: {
                const norm = descriptor.operation.norm;
                const ordinary = if (norm.kind == .layer) try cb.layerNorm(inputs[0], inputs[1], inputs[2], norm.dim, norm.eps) else try cb.rmsNorm(inputs[0], inputs[1], norm.dim, norm.eps);
                defer cb.free(ordinary);
                break :blk try cb.toFloat32(ordinary, a);
            } else null;
            defer if (ordinary_norm) |values| a.free(values);
            if (ordinary_norm) |values| for (expected, values) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.00003);
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
            if ((descriptor.operation == .rope or (supports_layouts and descriptor.operation == .gather)) and data[0].len == expected.len) {
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
            if (descriptor.operation == .rope) {
                var invalid = descriptor;
                invalid.operation.rope.total_chunks += 1;
                var declined = request;
                declined.descriptor = &invalid;
                try std.testing.expect(!try dispatch(cb.ptr, declined));
                declined = request;
                declined.auxiliaries = &.{.{ .role = .mean, .tensor = inputs[0] }};
                try std.testing.expect(!try dispatch(cb.ptr, declined));
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
                if (ordinary_norm) |values| {
                    if (cb.kind() == .metal) try std.testing.expectEqualSlices(f32, values, actual);
                }
            }
            for (data, inputs[0..count]) |values, input| {
                const actual = try cb.toFloat32(input, a);
                defer a.free(actual);
                try std.testing.expectEqualSlices(f32, values, actual);
            }
        }
    };
    // Independent F64 references cover both normalization families and native
    // vector/row tails. Case.run also verifies query purity, three replays,
    // strict device residency, and every retained input after execution.
    for ([_]usize{ 1, 3, 4, 33, 128, 768 }) |width| {
        const a = std.testing.allocator;
        const values = try a.alloc(f32, 3 * width);
        defer a.free(values);
        const gamma = try a.alloc(f32, width);
        defer a.free(gamma);
        const beta = try a.alloc(f32, width);
        defer a.free(beta);
        const expected = try a.alloc(f32, values.len);
        defer a.free(expected);
        for (values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 13) % 31)) - 15)) / 16;
        for (gamma, beta, 0..) |*g, *b, i| {
            g.* = 0.5 + @as(f32, @floatFromInt(i % 7)) / 16;
            b.* = @as(f32, @floatFromInt(i % 5)) / 32;
        }
        inline for (.{ K.Norm{ .kind = .layer, .dim = 0, .eps = 1e-5 }, K.Norm{ .kind = .rms, .dim = 0, .eps = 1e-5 } }) |policy| {
            var norm = policy;
            norm.dim = @intCast(width);
            for (0..3) |row| {
                var mean: f64 = 0;
                if (norm.kind == .layer) {
                    for (values[row * width ..][0..width]) |value| mean += value;
                    mean /= @as(f64, @floatFromInt(width));
                }
                var square: f64 = 0;
                for (values[row * width ..][0..width]) |value| square += (@as(f64, value) - mean) * (@as(f64, value) - mean);
                const inverse = 1 / @sqrt(square / @as(f64, @floatFromInt(width)) + @as(f64, norm.eps));
                for (0..width) |column| expected[row * width + column] = @floatCast((@as(f64, values[row * width + column]) - mean) * inverse * gamma[column] + (if (norm.kind == .layer) @as(f64, beta[column]) else 0));
            }
            const tensor_shape = Shape.init(.f32, &.{ 3, @intCast(width) });
            const vector_shape = Shape.init(.f32, &.{@intCast(width)});
            try Case.run(backend, .{ .operation = .{ .norm = norm }, .output_shape = tensor_shape, .input_shapes = .{ tensor_shape, vector_shape, vector_shape }, .input_count = if (norm.kind == .layer) 3 else 2 }, if (norm.kind == .layer) &.{ values, gamma, beta } else &.{ values, gamma }, expected);
        }
    }
    // Ordinary generated RoPE: batch two, sequence three, two heads. Explicit
    // tables and vision/MRoPE indexing retain their own execution contracts.
    const rotary_shape = Shape.init(.f32, &.{ 6, 16 });
    var rotary_input: [96]f32 = undefined;
    for (&rotary_input, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 23)) - 11)) / 16;
    for ([_]usize{ 0, 7 }) |offset| for ([_]usize{ 4, 8 }) |rotary_width| for ([_]bool{ false, true }) |consecutive| for ([_]f32{ 0.5, 1 }) |frequency_scale| {
        const rope = try K.Rope.init(&.{ 6, 16 }, rotary_input.len, 3, 8, rotary_width, 10000, frequency_scale, offset, consecutive);
        var expected = rotary_input;
        for (0..12) |chunk| {
            const position = offset + ((chunk / 2) % 3);
            for (0..rotary_width / 2) |pair| {
                const angle = @as(f64, @floatFromInt(position)) * frequency_scale / std.math.pow(f64, 10000, @as(f64, @floatFromInt(2 * pair)) / @as(f64, @floatFromInt(rotary_width)));
                const lo = chunk * 8 + (if (consecutive) 2 * pair else pair);
                const hi = chunk * 8 + (if (consecutive) 2 * pair + 1 else pair + 4);
                expected[lo] = @floatCast(@as(f64, rotary_input[lo]) * @cos(angle) - @as(f64, rotary_input[hi]) * @sin(angle));
                expected[hi] = @floatCast(@as(f64, rotary_input[lo]) * @sin(angle) + @as(f64, rotary_input[hi]) * @cos(angle));
            }
        }
        try Case.run(backend, .{ .operation = .{ .rope = rope }, .output_shape = rotary_shape, .input_shapes = .{ rotary_shape, undefined, undefined }, .input_count = 1 }, &.{&rotary_input}, &expected);
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
