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

//! Bounded process-owned sequence leases. Durable session snapshots carry
//! currval/lastval only; eviction or restart burns every unused cached value.
const std = @import("std");
const session = @import("antfly_local_sources").sql_sequence_session;
const A = std.mem.Allocator;
pub const capacity = 256;
pub const Key = struct { kind: enum { connection, transaction }, id: [32]u8 };
const Entry = struct {
    key: Key,
    principal: [32]u8,
    generation: u64,
    owner: u64,
    state: session.State,
    active: bool = true,
    touched: u64,
};
pub const Lease = struct {
    pool: *Pool,
    entry: *Entry,

    pub fn state(self: Lease) *session.State {
        return &self.entry.state;
    }
    pub fn release(self: Lease, io: std.Io) void {
        self.pool.mutex.lockUncancelable(io);
        defer self.pool.mutex.unlock(io);
        std.debug.assert(self.entry.active);
        self.entry.active = false;
    }
};
pub const Pool = struct {
    mutex: std.Io.Mutex = .init,
    entries: std.AutoHashMapUnmanaged(Key, *Entry) = .empty,
    clock: u64 = 0,

    pub fn borrow(self: *Pool, a: A, io: std.Io, key: Key, principal: []const u8, generation: u64, owner: u64, snapshot: session.Snapshot) !Lease {
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(principal, &digest, .{});
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        self.clock +|= 1;
        if (self.entries.get(key)) |entry| {
            if (!std.mem.eql(u8, &entry.principal, &digest) or entry.owner != owner) return error.SqlConnectionNotFound;
            if (entry.active) return error.SqlConnectionBusy;
            if (entry.generation != generation) {
                var replacement = try session.State.init(a, snapshot);
                errdefer replacement.deinit(a);
                entry.state.deinit(a);
                entry.state = replacement;
                entry.generation = generation;
            }
            entry.active = true;
            entry.touched = self.clock;
            return .{ .pool = self, .entry = entry };
        }
        var state = try session.State.init(a, snapshot);
        errdefer state.deinit(a);
        const entry = try a.create(Entry);
        errdefer a.destroy(entry);
        // Capacity is acquired before evicting an idle owner, so OOM cannot
        // invalidate a live lease or lose a previously usable cache.
        try self.entries.ensureUnusedCapacity(a, 1);
        if (self.entries.count() == capacity) {
            var oldest: ?*Entry = null;
            var it = self.entries.valueIterator();
            while (it.next()) |item| if (!item.*.active and (oldest == null or item.*.touched < oldest.?.touched)) {
                oldest = item.*;
            };
            const retired = oldest orelse return error.SqlProgramLimitExceeded;
            _ = self.entries.remove(retired.key);
            retired.state.deinit(a);
            a.destroy(retired);
        }
        entry.* = .{ .key = key, .principal = digest, .generation = generation, .owner = owner, .state = state, .touched = self.clock };
        self.entries.putAssumeCapacity(key, entry);
        return .{ .pool = self, .entry = entry };
    }
    pub fn deinit(self: *Pool, a: A) void {
        var it = self.entries.valueIterator();
        while (it.next()) |entry| {
            std.debug.assert(!entry.*.active);
            entry.*.state.deinit(a);
            a.destroy(entry.*);
        }
        self.entries.deinit(a);
        self.* = .{};
    }
};

test "SQL sequence cache serializes connection effects and burns evicted generations" {
    const a = std.testing.allocator;
    var pool: Pool = .{};
    defer pool.deinit(a);
    const key: Key = .{ .kind = .connection, .id = @splat(1) };
    const first = try pool.borrow(a, std.testing.io, key, "alice", 1, 7, .{});
    const id: session.Identity = .{ .incarnation = @splat(2), .id = 3 };
    const slot = try first.state().prepare(a, id);
    try slot.install(.{ .generation = 1, .counter_revision = 2, .first = 100, .increment = 10, .count = 4 });
    first.state().publishNext(id, (try slot.take(1)).?);
    try std.testing.expectError(error.SqlConnectionBusy, pool.borrow(a, std.testing.io, key, "alice", 1, 7, .{}));
    first.release(std.testing.io);
    const continued = try pool.borrow(a, std.testing.io, key, "alice", 1, 7, .{});
    try std.testing.expectEqual(@as(?i64, 110), try continued.state().entries.getPtr(id).?.take(1));
    continued.release(std.testing.io);
    try std.testing.expectError(error.SqlConnectionNotFound, pool.borrow(a, std.testing.io, key, "bob", 1, 7, .{}));
    const discarded = try pool.borrow(a, std.testing.io, key, "alice", 2, 7, .{});
    defer discarded.release(std.testing.io);
    try std.testing.expectError(error.SqlSequenceValueNotDefined, discarded.state().current(id));
}
