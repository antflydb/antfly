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
const Command = @import("antfly_local_sources").storage_pessimistic_lock_authority.Command;
const Entry = struct { ticket: u64, command: ?Command = null };
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
    /// Liveness proof only: this never grants native/durable authority.
    pub fn bindCommand(self: *Lease, io: std.Io, command: Command) !void {
        try command.validate();
        const pool = self.pool orelse return error.SqlConnectionExecutionBusy;
        try pool.mutex.lock(io);
        defer pool.mutex.unlock(io);
        const entry = pool.active.getPtr(self.key) orelse return error.SqlConnectionExecutionBusy;
        if (entry.ticket != self.ticket or (entry.command != null and !std.meta.eql(entry.command.?, command))) return error.SqlConnectionExecutionBusy;
        entry.command = command;
    }
    pub fn release(self: *Lease, io: std.Io) void {
        const pool = self.pool orelse return;
        pool.mutex.lockUncancelable(io);
        defer pool.mutex.unlock(io);
        // An old copied lease cannot release a later request on this key.
        if (pool.active.get(self.key)) |entry| if (entry.ticket == self.ticket) {
            std.debug.assert(pool.active.remove(self.key));
        };
        self.pool = null;
    }
};
pub const Pool = struct {
    mutex: std.Io.Mutex = .init,
    active: std.AutoHashMapUnmanaged(Key, Entry) = .empty,
    next_ticket: u64 = 1,
    instance: ?[16]u8 = null,
    /// One secure source identity for this process-owned connection pool.
    /// Durable command records retain it; a new pool cannot resume old SQL.
    pub fn executionInstance(self: *Pool, io: std.Io) ![16]u8 {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.instance) |value| return value;
        var value: [16]u8 = undefined;
        try io.randomSecure(&value);
        if (std.mem.allEqual(u8, &value, 0)) return error.InvalidSqlConnection;
        self.instance = value;
        return value;
    }
    pub fn executing(self: *Pool, io: std.Io, key: Key, command: Command) !bool {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        const entry = self.active.get(key) orelse return false;
        return std.meta.eql(entry.command, @as(?Command, command));
    }
    pub fn acquire(self: *Pool, a: A, io: std.Io, key: Key) !Lease {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.active.contains(key)) return error.SqlConnectionExecutionBusy;
        if (self.active.count() == capacity or self.next_ticket == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
        // Allocate before publishing admission. No allocator or mutex remains
        // held while the caller compiles, waits, performs RPCs or encodes rows.
        try self.active.ensureUnusedCapacity(a, 1);
        const ticket = self.next_ticket;
        self.active.putAssumeCapacity(key, .{ .ticket = ticket });
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

test "SQL connection admission liveness binds the exact command and cannot be renewed by a newer request" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var pool: Pool = .{};
    defer pool.deinit(a);
    const key = Key.init(@splat('1'), "alice");
    const command: Command = .{ .sequence = 1, .nonce = @splat(1), .executor = @splat(2) };
    var old = try pool.acquire(a, io, key);
    var stale = old;
    try old.bindCommand(io, command);
    try old.bindCommand(io, command);
    try std.testing.expect(try pool.executing(io, key, command));
    old.release(io);
    var fresh = try pool.acquire(a, io, key);
    defer fresh.release(io);
    try std.testing.expect(!try pool.executing(io, key, command));
    try std.testing.expectError(error.SqlConnectionExecutionBusy, stale.bindCommand(io, command));
    var next = command;
    next.sequence = 2;
    next.executor = @splat(3);
    try fresh.bindCommand(io, next);
    try std.testing.expect(try pool.executing(io, key, next));
    try std.testing.expect(!try pool.executing(io, key, command));
    try std.testing.expectError(error.SqlConnectionExecutionBusy, fresh.bindCommand(io, command));
    stale.release(io);
    try std.testing.expect(try pool.executing(io, key, next));
}
