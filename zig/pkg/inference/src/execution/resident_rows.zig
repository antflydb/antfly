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

//! Synchronous borrowed dense row inputs. The source keeps its tensor and
//! provider alive; no raw tensor is read through another provider namespace.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;

pub const Rows = struct {
    source: *const ComputeBackend,
    tensor: ops.CT,
    rows: usize,
    width: usize,

    pub fn validateGeometry(self: Rows, rows: usize, width: usize) !void {
        if (self.rows == 0 or self.width == 0 or self.rows != rows or self.width != width) return error.InvalidPreparedRows;
        _ = try std.math.mul(usize, self.rows, self.width);
    }
    /// Called under the target operation, before KV or numerical mutation.
    pub fn validateFor(self: Rows, target: *const ComputeBackend, rows: usize, width: usize, operation: ?@import("backend_operation.zig").Borrowed) !void {
        try self.validateGeometry(rows, width);
        if (self.source.ptr != target.ptr or self.source.vtable != target.vtable) {
            // Separate request/runtime wrappers may share a provider. Only an
            // explicit live operation can attest both canonical implementations
            // before the target inspects the source tensor's opaque header.
            const held = operation orelse return error.ForeignPreparedRows;
            try held.validate(.{ .implementation = .{ .context = self.source.ptr, .table = self.source.vtable }, .program = null, .mode = .legacy_unknown_frame });
            try held.validate(.{ .implementation = .{ .context = target.ptr, .table = target.vtable }, .program = null, .mode = .legacy_unknown_frame });
        }
        const backing = (try namespace(target, self.tensor, rows, width)) orelse return error.UnsupportedPreparedRows;
        if (backing.kind == .host) return error.UnsupportedPreparedRows;
    }
};

pub fn namespace(backend: *const ComputeBackend, source: ops.CT, rows: usize, width: usize) !?@import("../execution/storage_ownership.zig").Identity {
    const functions = backend.vtable.kernels orelse return null;
    if (!functions.accepts(backend.vtable)) return null;
    const inspect = functions.storageOwnership orelse return null;
    const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), @sizeOf(f32));
    const Proof = struct {
        bytes: usize,
        primary: usize = 0,
        namespace: ?@import("../execution/storage_ownership.zig").Identity = null,
        valid: bool = true,
        fn visit(raw: *anyopaque, report: @import("../execution/storage_ownership.zig").Report) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const plane = switch (report) {
                .plane => |plane| plane,
                .backing => return,
            };
            if (plane.role != .primary) return;
            self.primary += 1;
            self.namespace = plane.backing;
            const descriptor = plane.descriptor;
            if (descriptor.shape.dtype != .f32 or descriptor.quant_format != null or descriptor.attention_kv_format != null or descriptor.attention_storage != null or plane.backing == null or plane.logical_bytes != self.bytes or plane.readable_bytes < self.bytes) {
                self.valid = false;
                return;
            }
            switch (descriptor.storage) {
                .host_f32, .host_dense, .metal_buffer, .cuda_buffer, .metadata_view => {},
                else => {
                    self.valid = false;
                    return;
                },
            }
            const elements = descriptor.shape.numElements() orelse {
                self.valid = false;
                return;
            };
            if (elements < 0 or std.math.cast(usize, elements) != self.bytes / @sizeOf(f32)) {
                self.valid = false;
                return;
            }
            const dense = @import("../graph/backend_contracts.zig").TensorStrides.dense(descriptor.shape);
            if (!std.mem.eql(i64, dense.asSlice(), descriptor.strides.asSlice())) self.valid = false;
        }
    };
    var proof = Proof{ .bytes = bytes };
    const coverage = inspect(backend.ptr, source, .{ .context = &proof, .visit = Proof.visit }) catch |err| switch (err) {
        error.UnsupportedShape, error.UnsupportedTensorType => return null,
        else => return err,
    };
    return if (coverage == .complete and proof.valid and proof.primary == 1) proof.namespace else null;
}

test "borrowed prepared rows reject foreign wrappers before inspecting tensor storage" {
    var source_marker: u8 = 0;
    var target_marker: u8 = 0;
    var table: ComputeBackend.VTable = undefined;
    const source = ComputeBackend{ .ptr = &source_marker, .vtable = &table };
    const target = ComputeBackend{ .ptr = &target_marker, .vtable = &table };
    const rows = Rows{ .source = &source, .tensor = @ptrFromInt(16), .rows = 2, .width = 8 };
    try std.testing.expectError(error.ForeignPreparedRows, rows.validateFor(&target, 2, 8, null));
    try std.testing.expectError(error.InvalidPreparedRows, rows.validateFor(&target, 3, 8, null));
}
