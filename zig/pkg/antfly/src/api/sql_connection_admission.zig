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

//! One in-flight command per authenticated HTTP logical connection. Pgwire
//! already owns its command loop. This local admission lease does not confer
//! durable connection, transaction or native lock authority: the admitted
//! request must reload and validate those owners before doing any work.
const std = @import("std");
const A = std.mem.Allocator;
pub const capacity = 128;
pub const Key = struct {
    connection: [32]u8,
    principal: [32]u8,
    pub fn init(connection: [32]u8, principal: []const u8) Key {
        var result: Key = .{ .connection = connection, .principal = undefined };
        std.crypto.hash.Blake3.hash(principal, &result.principal, .{});
        return result;
    }
};
pub const Lease = struct {
    pool: ?*Pool,
    key: Key,
    ticket: u64,
    pub fn release(self: *Lease, io: std.Io) void {
        const pool = self.pool orelse return;
        pool.mutex.lockUncancelable(io);
        defer pool.mutex.unlock(io);
        // An old copied lease cannot release a later request on this key.
        if (pool.active.get(self.key)) |ticket| if (ticket == self.ticket) {
            std.debug.assert(pool.active.remove(self.key));
        };
        self.pool = null;
    }
};
pub const Pool = struct {
    mutex: std.Io.Mutex = .init,
    active: std.AutoHashMapUnmanaged(Key, u64) = .empty,
    next_ticket: u64 = 1,
    pub fn acquire(self: *Pool, a: A, io: std.Io, key: Key) !Lease {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.active.contains(key)) return error.SqlConnectionExecutionBusy;
        if (self.active.count() == capacity or self.next_ticket == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
        // Allocate before publishing admission. No allocator or mutex remains
        // held while the caller compiles, waits, performs RPCs or encodes rows.
        try self.active.ensureUnusedCapacity(a, 1);
        const ticket = self.next_ticket;
        self.active.putAssumeCapacity(key, ticket);
        self.next_ticket += 1;
        return .{ .pool = self, .key = key, .ticket = ticket };
    }
    pub fn deinit(self: *Pool, a: A) void {
        std.debug.assert(self.active.count() == 0);
        self.active.deinit(a);
        self.* = .{};
    }
};

test "SQL connection admission serializes commands independently of effect kind and prevents stale lease releases" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var pool: Pool = .{};
    defer pool.deinit(a);
    const key = Key.init(@splat('1'), "alice");
    var first = try pool.acquire(a, io, key);
    var stale = first;
    try std.testing.expectError(error.SqlConnectionExecutionBusy, pool.acquire(a, io, key));
    first.release(io);
    first.release(io);
    var second = try pool.acquire(a, io, key);
    defer second.release(io);
    stale.release(io);
    try std.testing.expectError(error.SqlConnectionExecutionBusy, pool.acquire(a, io, key));
    // Authentication is checked independently after admission. A caller with
    // another identity cannot occupy the actual owner's execution slot.
    var unrelated = try pool.acquire(a, io, Key.init(key.connection, "bob"));
    unrelated.release(io);
}

test "SQL connection admission is bounded and reuses released capacity" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var pool: Pool = .{};
    defer pool.deinit(a);
    var leases: [capacity]Lease = undefined;
    var owned: usize = 0;
    defer for (leases[0..owned]) |*lease| lease.release(io);
    for (&leases, 0..) |*lease, number| {
        var id: [32]u8 = @splat(0);
        std.mem.writeInt(u64, id[24..32], @intCast(number), .little);
        lease.* = try pool.acquire(a, io, Key.init(id, "alice"));
        owned += 1;
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, pool.acquire(a, io, Key.init(@splat('f'), "alice")));
    leases[0].release(io);
    var available = try pool.acquire(a, io, Key.init(@splat('f'), "alice"));
    available.release(io);
}
