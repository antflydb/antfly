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

//! Conservative metadata-only propagation for canonical dense F32 reshapes.
//! Identity, squeeze and unsqueeze adapters lower to this graph operation.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const route = @import("kernel_route.zig");
const ownership = @import("storage_ownership.zig");
const contracts = @import("../graph/backend_contracts.zig");

pub fn reshapePlane(source: ownership.Plane, logical: ml.graph.Shape, output: ml.graph.Shape, kind: ops.BackendKind, source_node: ml.graph.NodeId) ?ownership.Plane {
    if (source.role != .primary or logical.dtype != .f32 or output.dtype != .f32 or !source.descriptor.shape.eq(logical) or source.descriptor.current_backend != kind or source.descriptor.quant_format != null or source.descriptor.attention_storage != null or source.descriptor.attention_kv_format != null) return null;
    const count = logical.numElements() orelse return null;
    if (count <= 0 or output.numElements() != count) return null;
    const bytes = std.math.mul(usize, @intCast(count), @sizeOf(f32)) catch return null;
    if (source.logical_bytes != bytes or source.readable_bytes < bytes) return null;
    // Native host_dense includes lazy SourceTensor payloads; metadata_view may
    // trigger getData materialization even when its exposed strides are dense.
    const storage: contracts.TensorStorageClass = switch (kind) {
        .native => .host_f32,
        .metal => .metal_buffer,
        .cuda => .cuda_buffer,
        else => return null,
    };
    if (source.descriptor.storage != storage) return null;
    const dense = contracts.TensorStrides.dense(logical);
    if (!std.mem.eql(i64, dense.asSlice(), source.descriptor.strides.asSlice())) return null;
    var result = source;
    result.descriptor.shape = output;
    result.descriptor.strides = contracts.TensorStrides.dense(output);
    result.descriptor.view_source = source_node;
    result.logical_bytes = bytes;
    result.readable_bytes = bytes;
    result.layout_key = null; // opaque shape-dependent packing is not inherited
    result.writable = null; // aliases cannot inherit a producer's padded writes
    return result;
}

const Primary = struct {
    value: ?ownership.Plane = null,
    fn emit(raw: *anyopaque, report: ownership.Report) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        switch (report) {
            .backing => {},
            .plane => |plane| if (plane.role == .primary) {
                if (self.value != null) return error.AmbiguousPhysicalPrimary;
                self.value = plane;
            },
        }
    }
};

/// No allocations, tensor retention, transfer, materialization or submission.
/// The caller pins the actual resident/model input for the invocation lifetime.
pub fn primary(binding: route.Binding, target: ops.ComputeBackend, expected: ml.graph.Shape) !?ownership.Plane {
    const functions = target.vtable.kernels orelse return null;
    if (!functions.accepts(target.vtable) or !functions.physicalReplayReshapeViews) return null;
    return switch (binding) {
        .planned => |known| planes(known.logical, known.planes, expected),
        .resident => |known| blk: {
            // CUDA's legacy borrowed runtime slots copy on reshape. Plane-only
            // facts do not prove that their wrapper supports retained aliases.
            if (target.kind() == .cuda) break :blk null;
            if (!functions.accepts(known.implementation.table) or (target.kind() != .native and known.implementation.context != target.ptr)) break :blk null;
            break :blk planes(known.logical, known.planes, expected);
        },
        .tensor => |known| blk: {
            const backend = ops.ComputeBackend{ .ptr = known.implementation.context, .vtable = @ptrCast(@alignCast(known.implementation.table)) };
            const source_functions = backend.vtable.kernels orelse break :blk null;
            if (backend.kind() != target.kind() or !source_functions.accepts(backend.vtable) or (target.kind() != .native and backend.ptr != target.ptr)) break :blk null;
            const query = source_functions.storageOwnership orelse break :blk null;
            var result = Primary{};
            if (try query(backend.ptr, known.value, .{ .context = &result, .visit = Primary.emit }) != .complete) break :blk null;
            const plane = result.value orelse break :blk null;
            break :blk if (plane.descriptor.shape.eq(expected)) plane else null;
        },
        .imported_host, .unknown => null,
    };
}
fn planes(logical: ml.graph.Shape, input: []const ownership.Plane, expected: ml.graph.Shape) ?ownership.Plane {
    if (!logical.eq(expected)) return null;
    var result: ?ownership.Plane = null;
    for (input) |plane| if (plane.role == .primary) {
        if (result != null) return null;
        result = plane;
    };
    return result;
}

test "dense reshape facts preserve exact primary span without padding mirror or strided claims" {
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const target = ml.graph.Shape.init(.f32, &.{ 1, 2, 3, 1 });
    var descriptor = contracts.TensorDesc.init(shape, .host_f32);
    descriptor.current_backend = .native;
    var source = ownership.Plane{ .descriptor = descriptor, .role = .primary, .storage_epoch = 11, .backing = .{ .kind = .host, .base = 4096 }, .byte_offset = 16, .logical_bytes = 24, .readable_bytes = 24, .layout_key = 99 };
    const alias = reshapePlane(source, shape, target, .native, 7) orelse return error.MissingDenseAlias;
    try std.testing.expect(alias.descriptor.shape.eq(target));
    try std.testing.expectEqual(@as(usize, 16), alias.byte_offset);
    try std.testing.expectEqual(@as(usize, 24), alias.readable_bytes);
    try std.testing.expect(alias.layout_key == null and alias.writable == null);
    source.descriptor.strides = contracts.TensorStrides.init(&.{ 1, 2 });
    try std.testing.expect(reshapePlane(source, shape, target, .native, 7) == null);
    source.descriptor = descriptor;
    source.descriptor.storage = .host_dense;
    try std.testing.expect(reshapePlane(source, shape, target, .native, 7) == null);
    source.descriptor = descriptor;
    source.readable_bytes = 20;
    try std.testing.expect(reshapePlane(source, shape, target, .native, 7) == null);
    source.readable_bytes = 24;
    source.role = .bf16_mirror;
    try std.testing.expect(reshapePlane(source, shape, target, .native, 7) == null);
}
