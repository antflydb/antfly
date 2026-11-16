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

//! Publication control values; no physical index builders.
//! Creator-issued optional allocator capacity; no foreign allocator inspection.
const std = @import("std");
const native = @import("antfly_runtime_abi").native_abi;
const A = std.mem.Allocator;
pub const Value = extern struct {
    available_bytes: usize = 0,
    known: u8 = 0,
    reserved: [7]u8 = @splat(0),
    pub fn fromOptional(value: ?usize) Value {
        return if (value) |bytes| .{ .available_bytes = bytes, .known = 1 } else .{};
    }
    pub fn optional(self: Value) ?usize {
        if (!std.mem.allEqual(u8, &self.reserved, 0) or self.known != 1) return null;
        return self.available_bytes;
    }
};
const Query = *const fn (*const anyopaque) callconv(.c) Value;
fn signature() native.CallContract {
    return .of("antfly_lake_allocator_headroom_v1", Query, *const anyopaque, Value);
}
pub const Capability = extern struct {
    version: u32 = 1,
    reserved: u32 = 0,
    value_layout: native.TypeContract = .of(Value),
    contract: native.CallContract = signature(),
    context: ?*const anyopaque = null,
    query: ?Query = null,
    pub fn validate(self: Capability) !void {
        if (self.version != 1 or self.reserved != 0 or self.context == null or self.query == null or !self.value_layout.matches(.of(Value)) or !self.contract.matches(signature())) return error.InvalidArgument;
    }
    pub fn available(self: Capability) ?usize {
        self.validate() catch return null;
        return self.query.?(self.context.?).optional();
    }
};
/// Local allocator identity advertises only checked creator capacity. Raw
/// allocations still run through the original allocator and every hard limit.
/// The capability and adapter remain on the synchronous operation's stack.
pub const BorrowedAllocator = struct {
    backing: A,
    capacity: ?Capability,
    /// Owner-wide provenance includes allocations outside private map/transport
    /// budgets. Only failed raw allocations set it; resize refusal is normal.
    backing_allocation_failed: std.atomic.Value(bool) = .init(false),
    pub fn init(backing: A, capacity: ?Capability) !BorrowedAllocator {
        if (capacity) |value| try value.validate();
        return .{ .backing = backing, .capacity = capacity };
    }
    pub fn allocator(self: *BorrowedAllocator) A {
        return .{ .ptr = self, .vtable = &vtable };
    }
    pub fn headroom(a: A) ?struct { available: ?usize } {
        if (a.vtable != &vtable) return null;
        const self: *BorrowedAllocator = @ptrCast(@alignCast(a.ptr));
        return .{ .available = if (self.capacity) |value| value.available() else null };
    }
    const vtable: A.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *BorrowedAllocator = @ptrCast(@alignCast(raw));
        return self.backing.rawAlloc(len, alignment, ra) orelse {
            self.backing_allocation_failed.store(true, .monotonic);
            return null;
        };
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *BorrowedAllocator = @ptrCast(@alignCast(raw));
        return self.backing.rawResize(bytes, alignment, len, ra);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *BorrowedAllocator = @ptrCast(@alignCast(raw));
        return self.backing.rawRemap(bytes, alignment, len, ra);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *BorrowedAllocator = @ptrCast(@alignCast(raw));
        self.backing.rawFree(bytes, alignment, ra);
    }
};

test "external lake execution backing OOM provenance survives successful allocations" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var borrowed = try BorrowedAllocator.init(failing.allocator(), null);
    const bytes = try borrowed.allocator().alloc(u8, 1);
    borrowed.allocator().free(bytes);
    try std.testing.expect(!borrowed.backing_allocation_failed.load(.monotonic));
    try std.testing.expectError(error.OutOfMemory, borrowed.allocator().alloc(u8, 1));
    try std.testing.expect(borrowed.backing_allocation_failed.load(.monotonic));
}
