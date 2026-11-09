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

//! Process-local scheduling hints. Durable replay markers remain authoritative.
//! Tickets own identities, never pointers into catalog/runtime storage.
const std = @import("std");

pub const Key = struct { table_id: u64, incarnation: u128 };
pub const Queue = struct {
    mutex: std.Io.Mutex = .init,
    tickets: std.Deque(Key) = .empty,
    members: std.AutoHashMapUnmanaged(Key, void) = .empty,
    burst: u8 = 0,
    limit: usize = 0,

    pub fn deinit(self: *Queue, a: std.mem.Allocator) void {
        self.tickets.deinit(a);
        self.members.deinit(a);
        self.* = .{};
    }

    /// Registration may allocate before exposing the observer. Committed
    /// mutation notification never allocates or changes the operation's result.
    pub fn reserve(self: *Queue, a: std.mem.Allocator, io: std.Io, namespaces: usize) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const capacity = try std.math.add(usize, namespaces, self.tickets.len + 1);
        try self.tickets.ensureTotalCapacity(a, capacity);
        try self.members.ensureTotalCapacity(a, @intCast(capacity));
        self.limit = @max(self.limit, capacity);
    }

    pub fn notify(self: *Queue, io: std.Io, key: Key) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.members.contains(key)) return;
        // A stale incarnation can consume a reserved slot. Losing a hint is
        // safe: fair catalog audits still inspect durable work and recovery.
        if (self.tickets.len >= self.limit) return;
        self.members.putAssumeCapacity(key, {});
        self.tickets.pushBackAssumeCapacity(key);
    }

    /// Three runnable slices, then one mandatory audit. An empty queue also
    /// selects an audit, covering foreign writers, retries, TTL and cleanup.
    pub fn choose(self: *Queue, io: std.Io) ?Key {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.burst == 3 or self.tickets.len == 0) {
            self.burst = 0;
            return null;
        }
        const key = self.tickets.popFront().?;
        std.debug.assert(self.members.remove(key));
        self.burst += 1;
        return key;
    }

    pub fn pending(self: *Queue, io: std.Io) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.tickets.len != 0;
    }
};

test "capi native runnable namespace queue deduplicates and preserves incarnation identity" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{};
    defer queue.deinit(a);
    try queue.reserve(a, io, 3);
    const old: Key = .{ .table_id = 7, .incarnation = 11 };
    const replacement: Key = .{ .table_id = 7, .incarnation = 12 };
    for (0..100) |_| queue.notify(io, old);
    queue.notify(io, replacement);
    try std.testing.expectEqual(old, queue.choose(io).?);
    try std.testing.expectEqual(replacement, queue.choose(io).?);
    try std.testing.expect(queue.choose(io) == null);
    try std.testing.expect(!queue.pending(io));
}

test "capi native runnable namespace queue reserves audit service under continuous writes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{};
    defer queue.deinit(a);
    try queue.reserve(a, io, 1);
    const key: Key = .{ .table_id = 1, .incarnation = 1 };
    queue.notify(io, key);
    for (0..12) |i| {
        const selected = queue.choose(io);
        if (i % 4 == 3) {
            try std.testing.expect(selected == null);
        } else {
            try std.testing.expectEqual(key, selected.?);
            queue.notify(io, key);
        }
    }
}

test "capi native runnable namespace queue tolerates missing reservation and allocation failure" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{};
    defer queue.deinit(a);
    const key: Key = .{ .table_id = 2, .incarnation = 2 };
    queue.notify(io, key);
    try std.testing.expect(!queue.pending(io));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, queue.reserve(failing.allocator(), io, 1));
    queue.notify(io, key);
    try std.testing.expect(!queue.pending(io));
    try queue.reserve(a, io, 1);
    queue.notify(io, key);
    try std.testing.expectEqual(key, queue.choose(io).?);
}

test "capi native runnable namespace queue overflow loses only scheduling hints" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{};
    defer queue.deinit(a);
    try queue.reserve(a, io, 1);
    for (0..100) |i| queue.notify(io, .{ .table_id = i, .incarnation = 1 });
    try std.testing.expectEqual(queue.limit, queue.tickets.len);
    var selected: usize = 0;
    while (queue.pending(io)) if (queue.choose(io) != null) {
        selected += 1;
    };
    try std.testing.expectEqual(queue.limit, selected);
    // Audits remain eligible after an overflow and the queue can be reused.
    try std.testing.expect(queue.choose(io) == null);
    const next: Key = .{ .table_id = 999, .incarnation = 2 };
    queue.notify(io, next);
    try std.testing.expectEqual(next, queue.choose(io).?);
}
