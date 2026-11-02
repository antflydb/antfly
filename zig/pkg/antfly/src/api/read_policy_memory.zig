// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

/// A move-only foreground reservation retained across prepared CPU work and
/// policy reentry. Validation may use only this storage; exhaustion never falls
/// back to another allocator or consumes the protected control heap.
pub const Reservation = struct {
    pub const max_bytes = 1 << 20;
    parent: std.mem.Allocator,
    buffer: []u8,
    fixed: std.heap.FixedBufferAllocator,
    active: bool = false,

    pub fn init(parent: std.mem.Allocator, bytes: usize) !Reservation {
        if (bytes == 0 or bytes > max_bytes) return error.InvalidRunnableDemand;
        const buffer = try parent.alloc(u8, bytes);
        return .{ .parent = parent, .buffer = buffer, .fixed = .init(buffer) };
    }
    /// All nested arenas and lookup results must retire before end(). The
    /// owner must stay at a stable address while this allocator is borrowed.
    pub fn begin(self: *Reservation) !std.mem.Allocator {
        if (self.active or self.buffer.len == 0) return error.InvalidReadFenceTransition;
        self.fixed.reset();
        self.active = true;
        return self.fixed.allocator();
    }
    pub fn end(self: *Reservation) void {
        std.debug.assert(self.active);
        self.fixed.reset();
        self.active = false;
    }
    pub fn deinit(self: *Reservation) void {
        std.debug.assert(!self.active);
        self.parent.free(self.buffer);
        self.buffer = &.{};
        self.fixed = .init(&.{});
    }
};

test "read policy memory remains reserved while validation reuses bounded storage without fallback" {
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var memory = try Reservation.init(backing.allocator(), 128);
    defer memory.deinit();
    backing.fail_index = backing.alloc_index;
    const first = try memory.begin();
    const data = try first.alloc(u8, 128);
    @memset(data, 0xa5);
    try std.testing.expectError(error.OutOfMemory, first.alloc(u8, 1));
    try std.testing.expectError(error.InvalidReadFenceTransition, memory.begin());
    memory.end();
    try std.testing.expectEqual(@as(usize, 128), backing.allocated_bytes - backing.freed_bytes);
    const again = try memory.begin();
    const reused = try again.alloc(u8, 128);
    try std.testing.expect(data.ptr == reused.ptr);
    try std.testing.expect(!backing.has_induced_failure);
    memory.end();
    memory.deinit();
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    try std.testing.expectError(error.InvalidReadFenceTransition, memory.begin());
}

test "read policy memory rejects impossible and unavailable reservations before publication" {
    var unavailable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.InvalidRunnableDemand, Reservation.init(unavailable.allocator(), 0));
    try std.testing.expectError(error.InvalidRunnableDemand, Reservation.init(unavailable.allocator(), Reservation.max_bytes + 1));
    try std.testing.expectEqual(@as(usize, 0), unavailable.alloc_index);
    try std.testing.expectError(error.OutOfMemory, Reservation.init(unavailable.allocator(), 128));
    try std.testing.expectEqual(@as(usize, 0), unavailable.allocated_bytes);
}
