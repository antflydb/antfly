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

//! Session storage adapter: immutable metadata reads and atomic Raft CAS writes.
//! The host owns linearizable barriers, term fencing, and proposal durability.
const std = @import("std");
const local = @import("antfly_local_sources");
const erased = local.storage_backend_erased;
const types = local.storage_backend_types;
const record = @import("transaction_session_record.zig");
const A = std.mem.Allocator;
pub const Host = struct {
    ptr: *anyopaque,
    read: *const fn (*anyopaque, A) anyerror!erased.ReadTxn,
    term: *const fn (*anyopaque) ?u64,
    propose: *const fn (*anyopaque, []const u8, u64) anyerror!void,
};
pub const Store = struct {
    host: Host,
    group: u64,
    gate: std.atomic.Mutex = .unlocked,
    pub fn runtime(self: *Store, a: A) erased.Store {
        return .{ .allocator = a, .ptr = self, .write_gate = &self.gate, .vtable = &.{ .deinit = deinit, .capabilities = capabilities, .begin_read = beginRead, .begin_write = beginWrite, .begin_batch = beginBatch } };
    }
    fn cast(ptr: *anyopaque) *Store {
        return @ptrCast(@alignCast(ptr));
    }
    fn deinit(_: A, _: *anyopaque) void {}
    fn capabilities(_: *anyopaque) types.Capabilities {
        return .{ .reverse_ranges = true };
    }
    fn beginRead(a: A, ptr: *anyopaque) !erased.ReadTxn {
        const read = try Read.create(a, cast(ptr));
        return .{ .allocator = a, .ptr = read, .vtable = &.{ .abort = Read.abort, .get = Read.get, .open_cursor = Read.cursor } };
    }
    fn beginWrite(a: A, ptr: *anyopaque) !erased.WriteTxn {
        const self = cast(ptr);
        const term = self.host.term(self.host.ptr) orelse return error.NotLeader;
        const write = try a.create(Write);
        errdefer a.destroy(write);
        const read = try Read.create(a, self);
        errdefer read.destroy(a);
        if (self.host.term(self.host.ptr) != term) return error.NotLeader;
        const raw = read.physical.get(try record.keyAlloc(read.arena.allocator(), self.group, record.epoch_key)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const epoch = if (raw) |value| blk: {
            if (value.len != 8) return error.InvalidTransactionSessionRecord;
            break :blk std.mem.readInt(u64, value[0..8], .little);
        } else 0;
        write.* = .{ .read = read, .term = term, .epoch = epoch };
        return .{ .allocator = a, .ptr = write, .vtable = &.{ .abort = Write.abort, .commit = Write.commit, .get = Write.get, .put = Write.put, .delete = Write.delete, .open_cursor = Write.cursor } };
    }
    fn beginBatch(a: A, ptr: *anyopaque) !erased.Batch {
        const write = try beginWrite(a, ptr);
        return .{ .allocator = a, .ptr = write.ptr, .vtable = &.{ .abort = Write.abort, .commit = Write.commit, .get = Write.get, .put = Write.put, .delete = Write.delete, .open_cursor = Write.cursor } };
    }
};
const Read = struct {
    store: *Store,
    physical: erased.ReadTxn,
    arena: std.heap.ArenaAllocator,
    prefix: []const u8,
    fn cast(ptr: *anyopaque) *Read {
        return @ptrCast(@alignCast(ptr));
    }
    fn create(a: A, store: *Store) !*Read {
        const self = try a.create(Read);
        errdefer a.destroy(self);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        var buf: [96]u8 = undefined;
        const prefix = try arena.allocator().dupe(u8, try record.prefixForGroup(&buf, store.group));
        self.* = .{ .store = store, .physical = try store.host.read(store.host.ptr, a), .arena = arena, .prefix = prefix };
        return self;
    }
    fn destroy(self: *Read, a: A) void {
        self.physical.abort();
        self.arena.deinit();
        a.destroy(self);
    }
    fn abort(a: A, ptr: *anyopaque) void {
        cast(ptr).destroy(a);
    }
    fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
        const self = cast(ptr);
        if (std.mem.eql(u8, key, record.epoch_key)) return error.NotFound;
        return self.physical.get(try record.keyAlloc(self.arena.allocator(), self.store.group, key));
    }
    fn cursor(a: A, ptr: *anyopaque) !erased.Cursor {
        return Cursor.create(a, cast(ptr), null);
    }
};
const Write = struct {
    read: *Read,
    term: u64,
    epoch: u64,
    scanned: bool = false,
    owned_bytes: usize = 0,
    changes: std.ArrayList(record.Change) = .empty,
    fn cast(ptr: *anyopaque) *Write {
        return @ptrCast(@alignCast(ptr));
    }
    fn abort(a: A, ptr: *anyopaque) void {
        const self = cast(ptr);
        self.read.destroy(a);
        a.destroy(self);
    }
    fn find(self: *Write, key: []const u8) ?*record.Change {
        for (self.changes.items) |*change| if (std.mem.eql(u8, change.key, key)) return change;
        return null;
    }
    fn observe(self: *Write, key: []const u8) !*record.Change {
        if (self.find(key)) |change| return change;
        if (self.changes.items.len >= record.max_operations) return error.TransactionTooLarge;
        if (key.len == 0 or key.len > 512 or std.mem.eql(u8, key, record.epoch_key)) return error.InvalidTransactionSessionRecord;
        const a = self.read.arena.allocator();
        const actual = Read.get(self.read, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const expected = if (actual) |value| try self.own(value) else null;
        try self.changes.append(a, .{ .key = try self.own(key), .expected = expected, .value = expected });
        return &self.changes.items[self.changes.items.len - 1];
    }
    fn own(self: *Write, bytes: []const u8) ![]const u8 {
        if (bytes.len > record.max_bytes -| self.owned_bytes) return error.TransactionTooLarge;
        const copy = try self.read.arena.allocator().dupe(u8, bytes);
        self.owned_bytes += bytes.len;
        return copy;
    }
    fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
        return (try cast(ptr).observe(key)).value orelse error.NotFound;
    }
    fn put(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
        const self = cast(ptr);
        const change = try self.observe(key);
        change.value = try self.own(value);
    }
    fn delete(ptr: *anyopaque, key: []const u8) !void {
        (try cast(ptr).observe(key)).value = null;
    }
    fn cursor(a: A, ptr: *anyopaque) !erased.Cursor {
        const self = cast(ptr);
        self.scanned = true;
        return Cursor.create(a, self.read, self);
    }
    fn commit(a: A, ptr: *anyopaque) !void {
        const self = cast(ptr);
        const store = self.read.store;
        if (store.host.term(store.host.ptr) != self.term) return error.NotLeader;
        if (self.changes.items.len != 0) {
            const bytes = try record.encode(self.read.arena.allocator(), .{ .changes = self.changes.items, .scan_epoch = if (self.scanned) self.epoch else null });
            try store.host.propose(store.host.ptr, bytes, self.term);
            // The write gate remains held through this confirmation. A term
            // change instead returns an uncertain result for receipt recovery.
            if (store.host.term(store.host.ptr) != self.term) return error.NotLeader;
            const current = try Read.create(a, store);
            defer current.destroy(a);
            for (self.changes.items) |change| {
                const actual = Read.get(current, change.key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (change.value) |value| {
                    if (actual == null or !std.mem.eql(u8, value, actual.?)) return error.SessionLeaseLost;
                } else if (actual != null) return error.SessionLeaseLost;
            }
        }
        abort(a, ptr);
    }
};
const Cursor = struct {
    a: A,
    read: *Read,
    write: ?*Write,
    physical: erased.Cursor,
    key: std.ArrayList(u8) = .empty,
    seek: std.ArrayList(u8) = .empty,
    fn cast(ptr: *anyopaque) *Cursor {
        return @ptrCast(@alignCast(ptr));
    }
    fn create(a: A, read: *Read, write: ?*Write) !erased.Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .read = read, .write = write, .physical = try read.physical.openCursor() };
        return .{ .allocator = a, .ptr = self, .vtable = &.{ .close = close, .first = first, .last = last, .next = next, .prev = prev, .seek_at_or_after = after, .seek_at_or_before = before } };
    }
    fn close(a: A, ptr: *anyopaque) void {
        const self = cast(ptr);
        self.physical.close();
        self.key.deinit(a);
        self.seek.deinit(a);
        a.destroy(self);
    }
    fn eligible(key: []const u8, bound: ?[]const u8, forward: bool, inclusive: bool) bool {
        const requested = bound orelse return true;
        const order = std.mem.order(u8, key, requested);
        return (inclusive and order == .eq) or order == @as(std.math.Order, if (forward) .gt else .lt);
    }
    fn position(self: *Cursor, bound: ?[]const u8, forward: bool, inclusive: bool) !?erased.Entry {
        self.seek.clearRetainingCapacity();
        try self.seek.appendSlice(self.a, self.read.prefix);
        if (bound) |key| try self.seek.appendSlice(self.a, key) else if (!forward) self.seek.items[self.seek.items.len - 1] += 1;
        var base = if (forward) try self.physical.seekAtOrAfter(self.seek.items) else try self.physical.seekAtOrBefore(self.seek.items);
        while (base) |entry| {
            if (!std.mem.startsWith(u8, entry.key, self.read.prefix)) {
                // An inclusive reverse seek may initially land on the first
                // key after this namespace. Step back into its own range.
                if (!forward and std.mem.order(u8, entry.key, self.read.prefix) == .gt) {
                    base = try self.physical.prev();
                    continue;
                }
                base = null;
                break;
            }
            const key = entry.key[self.read.prefix.len..];
            if (!std.mem.eql(u8, key, record.epoch_key) and eligible(key, bound, forward, inclusive) and (self.write == null or self.write.?.find(key) == null)) break;
            base = if (forward) try self.physical.next() else try self.physical.prev();
        }
        var selected: ?erased.Entry = if (base) |entry| .{ .key = entry.key[self.read.prefix.len..], .value = entry.value } else null;
        if (self.write) |write| for (write.changes.items) |change| {
            const value = change.value orelse continue;
            if (!eligible(change.key, bound, forward, inclusive)) continue;
            if (selected == null or std.mem.order(u8, change.key, selected.?.key) == @as(std.math.Order, if (forward) .lt else .gt)) selected = .{ .key = change.key, .value = value };
        };
        const entry = selected orelse return null;
        self.key.clearRetainingCapacity();
        try self.key.appendSlice(self.a, entry.key);
        return .{ .key = self.key.items, .value = entry.value };
    }
    fn first(ptr: *anyopaque) !?erased.Entry {
        return cast(ptr).position(null, true, true);
    }
    fn last(ptr: *anyopaque) !?erased.Entry {
        return cast(ptr).position(null, false, true);
    }
    fn next(ptr: *anyopaque) !?erased.Entry {
        const self = cast(ptr);
        return self.position(self.key.items, true, false);
    }
    fn prev(ptr: *anyopaque) !?erased.Entry {
        const self = cast(ptr);
        return self.position(self.key.items, false, false);
    }
    fn after(ptr: *anyopaque, key: []const u8) !?erased.Entry {
        return cast(ptr).position(key, true, true);
    }
    fn before(ptr: *anyopaque, key: []const u8) !?erased.Entry {
        return cast(ptr).position(key, false, true);
    }
};
test "replicated session adapter retains cuts, merges cursors, fences terms and handles lost acknowledgements" {
    const Fixture = struct {
        physical: *erased.Store,
        current_term: u64 = 1,
        lose_ack: bool = false,
        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn read(ptr: *anyopaque, _: A) !erased.ReadTxn {
            return cast(ptr).physical.beginRead();
        }
        fn term(ptr: *anyopaque) ?u64 {
            return cast(ptr).current_term;
        }
        fn propose(ptr: *anyopaque, bytes: []const u8, expected: u64) !void {
            const self = cast(ptr);
            if (self.current_term != expected) return error.NotLeader;
            var txn = try self.physical.beginWrite();
            var active = true;
            defer if (active) txn.abort();
            _ = try record.apply(&txn, std.testing.allocator, 7, bytes);
            try txn.commit();
            active = false;
            if (self.lose_ack) return error.Unavailable;
        }
    };
    const a = std.testing.allocator;
    var backend = local.storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var physical = try backend.runtimeStore(a, .{ .name = "metadata" });
    defer physical.deinit();
    var fixture: Fixture = .{ .physical = &physical };
    var owner: Store = .{ .host = .{ .ptr = &fixture, .read = Fixture.read, .term = Fixture.term, .propose = Fixture.propose }, .group = 7 };
    var logical = owner.runtime(a);
    defer logical.deinit();
    {
        var txn = try logical.beginWrite();
        errdefer txn.abort();
        try txn.put("receipt", "pending");
        try txn.put("lease", "owner:1");
        try txn.commit();
    }
    var retained = try logical.beginRead();
    defer retained.abort();
    {
        var txn = try logical.beginWrite();
        errdefer txn.abort();
        try txn.delete("lease");
        try txn.put("receipt", "committed");
        try txn.put("z", "last");
        var cursor = try txn.openCursor();
        {
            defer cursor.close();
            try std.testing.expectEqualStrings("receipt", (try cursor.first()).?.key);
            try std.testing.expectEqualStrings("committed", (try cursor.seekAtOrAfter("receipt")).?.value);
            try std.testing.expectEqualStrings("z", (try cursor.next()).?.key);
            try std.testing.expect((try cursor.next()) == null);
            try std.testing.expectEqualStrings("z", (try cursor.last()).?.key);
            try std.testing.expectEqualStrings("receipt", (try cursor.prev()).?.key);
            try std.testing.expect((try cursor.prev()) == null);
        }
        try txn.commit();
    }
    try std.testing.expectEqualStrings("pending", try retained.get("receipt"));
    {
        var txn = try logical.beginWrite();
        try txn.put("receipt", "must not commit");
        fixture.current_term = 2;
        try std.testing.expectError(error.NotLeader, txn.commit());
        txn.abort();
    }
    {
        var txn = try logical.beginWrite();
        try txn.put("receipt", "acknowledgement lost");
        fixture.lose_ack = true;
        try std.testing.expectError(error.Unavailable, txn.commit());
        txn.abort();
        fixture.lose_ack = false;
    }
    var current = try logical.beginRead();
    defer current.abort();
    try std.testing.expectEqualStrings("acknowledgement lost", try current.get("receipt"));
    try std.testing.expectEqualStrings("pending", try retained.get("receipt"));
    {
        var txn = try logical.beginWrite();
        try txn.put("receipt", "stale owner");
        var remote = try physical.beginWrite();
        const key = try record.keyAlloc(a, 7, "receipt");
        defer a.free(key);
        try remote.put(key, "new owner");
        try remote.commit();
        try std.testing.expectError(error.SessionLeaseLost, txn.commit());
        txn.abort();
    }
}
