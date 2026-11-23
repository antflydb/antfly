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

//! Admitted host ingress into an invocation's exact stable input allocation.
//! Staging resources belong to the frame, not a process-global upload cache.
const std = @import("std");
const ml = @import("ml");
const CT = @import("../graph/backend_contracts.zig").CT;
const Tensor = @import("../backends/tensor.zig").Tensor;
const route = @import("kernel_route.zig");

pub const Input = struct { node_id: ml.graph.NodeId, value: *const Tensor };
pub const Requirements = struct {
    bytes: usize,
    /// Only providers whose requirements depend exclusively on this frozen
    /// logical descriptor may omit repeated invocation-time requirement queries.
    validation_scope: enum { invocation, prepared_program } = .invocation,
    /// Admitted SDK-owned staging span. CUDA reports the exact requested
    /// pinned span; driver-private OS backing granularity remains unknown.
    host_bytes: usize,
    host_backing_bytes: ?usize = null,
    alignment: u32 = @alignOf(f32),
    profile: u32,
    /// Direct CPU writes to a unified device bank require the actual managed
    /// provider scope, whose prior command buffers are terminal on entry.
    requires_managed_operation: bool = false,

    pub fn eql(self: Requirements, other: Requirements) bool {
        return self.validation_scope == other.validation_scope and self.bytes == other.bytes and self.host_bytes == other.host_bytes and self.host_backing_bytes == other.host_backing_bytes and self.alignment == other.alignment and self.profile == other.profile and self.requires_managed_operation == other.requires_managed_operation;
    }
};
pub const Upload = struct {
    context: *anyopaque,
    implementation: route.Implementation,
    requirements: Requirements,
    vtable: *const VTable,
    slot: route.OutputSlot,
    borrowed: bool = false,
    index_bounds: ?IndexBounds = null,
    pub const VTable = struct {
        bind_borrowed: ?*const fn (*anyopaque, []const u8) anyerror!CT = null,
        unbind_borrowed: ?*const fn (*anyopaque) anyerror!void = null,
        write: *const fn (*anyopaque, CT, route.AssignedOutput, []const u8) anyerror!void,
        deinit: *const fn (*anyopaque) void,
    };
    pub fn validateOperation(self: Upload, operation: ?*const @import("backend_operation.zig").Borrowed) !void {
        if (!self.requirements.requires_managed_operation) return;
        const token = operation orelse return error.HostIngressRequiresManagedOperation;
        try token.validate(.{ .implementation = self.implementation, .program = null, .mode = .managed });
    }
    pub fn write(self: Upload, output: CT, assigned: route.AssignedOutput, values: []const u8) !void {
        return self.writeWithOperation(output, assigned, values, null);
    }
    pub fn writeWithOperation(self: Upload, output: CT, assigned: route.AssignedOutput, values: []const u8, operation: ?*const @import("backend_operation.zig").Borrowed) !void {
        try self.validateOperation(operation);
        if (values.len != self.requirements.bytes or !self.matchesSlot(assigned.slot)) return error.InvalidHostIngressAuthority;
        if (self.index_bounds) |bounds| try bounds.validate(self.slot.logical.dtype, values);
        try self.vtable.write(self.context, output, assigned, values);
    }
    pub fn matchesSlot(self: Upload, slot: route.OutputSlot) bool {
        return self.slot.node == slot.node and self.slot.allocation == slot.allocation and self.slot.epoch == slot.epoch and self.slot.logical.eq(slot.logical) and self.slot.writable_bytes == slot.writable_bytes;
    }
    pub fn deinit(self: Upload) void {
        self.vtable.deinit(self.context);
    }
};
pub const Create = struct {
    borrowed: bool = false,
    index_bounds: ?IndexBounds = null,
    allocator: std.mem.Allocator,
    implementation: route.Implementation,
    requirements: Requirements,
    slot: route.OutputSlot,
};

/// Dense raw ingress has an intentionally narrow wire contract. Provider
/// qualification and exact shape/byte checks remain mandatory independently.
pub fn rawDType(dtype: anytype) bool {
    return switch (dtype) {
        .f32, .i32, .i64 => true,
        else => false,
    };
}

/// Each consumer normalizes negative indices against its own extent. Validity
/// ranges intersect at the minimum extent without changing that normalization.
pub const IndexBounds = struct {
    extent: usize,
    pub fn intersect(self: @This(), other: @This()) @This() {
        return .{ .extent = @min(self.extent, other.extent) };
    }
    pub fn validate(self: @This(), dtype: anytype, bytes: []const u8) !void {
        if (self.extent == 0) return error.InvalidHostIndexBounds;
        const converted: @import("../backends/tensor.zig").DType = switch (dtype) {
            .f32 => .f32,
            .i32 => .i32,
            .i64 => .i64,
            else => return error.UnsupportedTensorType,
        };
        try (try @import("../ops/index_metadata.zig").Reader.init(converted, bytes)).validate(self.extent);
    }
};

test "host index bound intersection preserves exact wide negative and float semantics" {
    const bounds = IndexBounds{ .extent = 7 };
    const intersection = bounds.intersect(.{ .extent = 3 });
    try std.testing.expectEqual(@as(usize, 3), intersection.extent);
    const exact = [_]i64{ -3, -1, 0, 2 };
    var raw: [33]u8 = undefined;
    @memcpy(raw[1..], std.mem.sliceAsBytes(&exact));
    try intersection.validate(.i64, raw[1..]);
    try std.testing.expectError(error.IndexOutOfBounds, intersection.validate(.i64, std.mem.sliceAsBytes(&[_]i64{-4})));
    try std.testing.expectError(error.IndexOutOfBounds, intersection.validate(.i64, std.mem.sliceAsBytes(&[_]i64{9_007_199_254_740_993})));
    try intersection.validate(.f32, std.mem.sliceAsBytes(&[_]f32{ 1.9, -1.9 }));
    try std.testing.expectError(error.InvalidIndex, intersection.validate(.f32, std.mem.sliceAsBytes(&[_]f32{std.math.nan(f32)})));
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&exact), raw[1..]);
}
