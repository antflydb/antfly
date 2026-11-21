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
    propose: *const fn (*anyopaque, []const u8, u64, *bool) anyerror!bool,
};
pub const Store = struct {
    host: Host,
    group: u64,
    gate: std.atomic.Mutex = .unlocked,
    pub fn runtime(self: *Store, a: A) erased.Store {
        return .{ .allocator = a, .ptr = self, .write_gate = &self.gate, .vtable = &.{ .deinit = deinit, .capabilities = capabilities, .begin_read = beginRead, .begin_probe = beginProbe, .begin_write = beginWrite, .begin_batch = beginBatch } };
    }
    fn cast(ptr: *anyopaque) *Store {
        return @ptrCast(@alignCast(ptr));
    }
    fn deinit(_: A, _: *anyopaque) void {}
    fn capabilities(_: *anyopaque) types.Capabilities {
        return .{ .reverse_ranges = true, .max_atomic_compare_write_bytes = record.max_bytes };
    }
    fn beginRead(a: A, ptr: *anyopaque) !erased.ReadTxn {
        const read = try Read.create(a, cast(ptr));
        return .{ .allocator = a, .ptr = read, .vtable = &.{ .abort = Read.abort, .get = Read.get, .open_cursor = Read.cursor, .open_read_scope = Read.scope, .count_prefix = Read.countPrefix, .read_prefix_page = Read.prefixPage } };
    }
    fn beginProbe(a: A, ptr: *anyopaque) !erased.ProbeTxn {
        const read = try Read.create(a, cast(ptr));
        return .{ .allocator = a, .ptr = read, .vtable = &.{ .abort = Read.abort, .get = Read.get, .contains = Read.contains } };
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
        return .{ .allocator = a, .ptr = write, .vtable = &.{ .abort = Write.abort, .commit = Write.commit, .get = Write.get, .put = Write.put, .delete = Write.delete, .open_cursor = Write.cursor, .count_prefix = Write.countPrefix } };
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
    refs: std.atomic.Value(usize) = .init(1),
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
    fn destroy(self: *Read, _: A) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.arena.child_allocator;
        self.physical.abort();
        self.arena.deinit();
        a.destroy(self);
    }
    fn abort(a: A, ptr: *anyopaque) void {
        cast(ptr).destroy(a);
    }
    fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
        const self = cast(ptr);
        if (record.internalKey(key)) return error.NotFound;
        return self.physical.get(try record.keyAlloc(self.arena.allocator(), self.store.group, key));
    }
    fn contains(ptr: *anyopaque, key: []const u8) !bool {
        if (record.internalKey(key)) return false;
        const self = cast(ptr);
        const a = self.arena.child_allocator;
        const physical = try record.keyAlloc(a, self.store.group, key);
        defer a.free(physical);
        var probe = try self.physical.openCursor();
        defer probe.close();
        probe.setKeysOnly();
        const entry = (try probe.seekAtOrAfter(physical)) orelse return false;
        return std.mem.eql(u8, entry.key, physical);
    }

    fn scope(a: A, ptr: *anyopaque) !erased.ReadScope {
        const self = cast(ptr);
        var physical = try self.physical.openReadScope(a);
        errdefer physical.close();
        return erased.readScopeFrom(a, Scope{ .alloc = a, .group = self.store.group, .physical = physical });
    }
    const Scope = struct {
        alloc: A,
        group: u64,
        physical: erased.ReadScope,
        fn physicalKey(self: *@This(), key: []const u8) ![]const u8 {
            if (record.internalKey(key)) return error.NotFound;
            return record.keyAlloc(self.alloc, self.group, key);
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            const physical = try self.physicalKey(key);
            defer self.alloc.free(physical);
            return self.physical.get(physical);
        }
        pub fn getRange(self: *@This(), key: []const u8, offset: u64, length: usize) ![]const u8 {
            const physical = try self.physicalKey(key);
            defer self.alloc.free(physical);
            return self.physical.getRange(physical, offset, length) catch |err| switch (err) {
                error.Unsupported => (try record.cachedRange(&self.physical, self.alloc, self.group, key, offset, length)) orelse error.Unsupported,
                else => err,
            };
        }
        pub fn close(self: *@This()) void {
            self.physical.close();
        }
    };
    fn prefixPage(ptr: *anyopaque, alloc: A, prefix: []const u8, after: ?[]const u8, length: usize) ![]erased.Entry {
        const self = cast(ptr);
        if (!std.mem.startsWith(u8, prefix, record.session_prefix)) return error.Unsupported;
        const physical_prefix = try record.keyAlloc(alloc, self.store.group, prefix);
        defer alloc.free(physical_prefix);
        const physical_after = if (after) |key| try record.keyAlloc(alloc, self.store.group, key) else null;
        defer if (physical_after) |key| alloc.free(key);
        const rows = try self.physical.readPrefixPage(alloc, physical_prefix, physical_after, length);
        for (rows) |*row| {
            if (!std.mem.startsWith(u8, row.key, physical_prefix)) return error.InvalidTransactionSessionRecord;
            row.key = row.key[self.prefix.len..];
        }
        return rows;
    }

    fn countPrefix(ptr: *anyopaque, prefix: []const u8) !u64 {
        const self = cast(ptr);
        const a = self.arena.child_allocator;
        if (!std.mem.startsWith(u8, prefix, record.session_prefix)) {
            var logical = try cursor(a, ptr);
            defer logical.close();
            return logical.countPrefix(prefix);
        }
        // A session-record prefix cannot include any hidden metadata indexes.
        const physical = try record.keyAlloc(a, self.store.group, prefix);
        defer a.free(physical);
        return self.physical.countPrefix(physical);
    }
    fn cursor(a: A, ptr: *anyopaque) !erased.Cursor {
        return Cursor.create(a, cast(ptr), null);
    }
};
const Write = struct {
    read: *Read,
    term: u64,
    epoch: u64,
    refs: std.atomic.Value(usize) = .init(1),
    scanned: bool = false,
    owned_bytes: usize = 0,
    owned_limit: usize = record.max_bytes,
    changes: std.ArrayList(record.Change) = .empty,
    fn cast(ptr: *anyopaque) *Write {
        return @ptrCast(@alignCast(ptr));
    }
    fn abort(a: A, ptr: *anyopaque) void {
        const self = cast(ptr);
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
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
        if (key.len == 0 or key.len > 512 or record.internalKey(key)) return error.InvalidTransactionSessionRecord;
        const a = self.read.arena.allocator();
        const actual = Read.get(self.read, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        // The physical read owns observed values until the write closes.
        const expected = actual;
        var value = expected;
        if (std.mem.eql(u8, key, record.count_key)) {
            // A legacy writer can change membership without touching the
            // counter. Fence the epoch and rebuild a stale count in this cut.
            self.scanned = true;
            if (expected != null and !try record.countIsCurrent(&self.read.physical, a, self.read.store.group, self.epoch)) value = null;
        }
        try self.changes.append(a, .{ .key = try self.own(key), .expected = expected, .value = value, .observation_only = true });
        return &self.changes.items[self.changes.items.len - 1];
    }
    fn own(self: *Write, bytes: []const u8) ![]const u8 {
        if (bytes.len > self.owned_limit -| self.owned_bytes) return error.TransactionTooLarge;
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
        if (std.mem.startsWith(u8, key, record.session_prefix) and change.expected != null and
            value.len <= record.max_bytes + record.max_receipt_growth and value.len <= change.expected.?.len +| record.max_receipt_growth and
            value.len > record.max_bytes -| self.owned_bytes)
        {
            self.owned_limit = record.max_bytes + record.max_receipt_growth;
        }
        change.value = try self.own(value);
        change.observation_only = false;
    }
    fn delete(ptr: *anyopaque, key: []const u8) !void {
        const change = try cast(ptr).observe(key);
        change.value = null;
        change.observation_only = false;
    }
    fn countPrefix(ptr: *anyopaque, prefix: []const u8) !u64 {
        const self = cast(ptr);
        self.scanned = true;
        // The session namespace cannot overlap the hidden physical indexes.
        if (!std.mem.startsWith(u8, prefix, record.session_prefix)) {
            var merged = try cursor(self.read.arena.child_allocator, ptr);
            defer merged.close();
            return merged.countPrefix(prefix);
        }
        var total = try Read.countPrefix(self.read, prefix);
        for (self.changes.items) |change| {
            if (!std.mem.startsWith(u8, change.key, prefix)) continue;
            if (change.expected == null and change.value != null) total += 1;
            if (change.expected != null and change.value == null) total -= 1;
        }
        return total;
    }
    fn cursor(a: A, ptr: *anyopaque) !erased.Cursor {
        const self = cast(ptr);
        self.scanned = true;
        return Cursor.create(a, self.read, self);
    }
    fn encodeChanges(self: *Write) ![]const u8 {
        const a = self.read.arena.allocator();
        const epoch = if (self.scanned) self.epoch else null;
        const changes = if (self.scanned) filtered: {
            var writes: std.ArrayList(record.Change) = .empty;
            for (self.changes.items) |change| {
                if (!change.observation_only) try writes.append(a, change);
            }
            break :filtered writes.items;
        } else self.changes.items;
        return record.encode(a, .{ .changes = changes, .scan_epoch = epoch }) catch |err| {
            if (err != error.TransactionTooLarge) return err;
            const compact = try a.dupe(record.Change, changes);
            const Digest = std.crypto.hash.Blake3;
            for (compact) |*change| {
                const previous = change.expected orelse continue;
                if (!std.mem.startsWith(u8, change.key, record.session_prefix) or previous.len <= Digest.digest_length) continue;
                const digest = try a.alloc(u8, Digest.digest_length);
                Digest.hash(previous, digest[0..Digest.digest_length], .{});
                change.expected = digest;
                change.expected_is_digest = true;
            }
            return record.encode(a, .{ .changes = compact, .scan_epoch = epoch }) catch |compact_err| {
                if (compact_err != error.TransactionTooLarge) return compact_err;
                for (compact, changes) |*change, original| {
                    if (!change.expected_is_digest) continue;
                    const replacement = original.value orelse continue;
                    if (try record.receiptSplice(a, original.expected.?, replacement)) |patch| {
                        change.value = patch;
                        change.value_is_splice = true;
                    }
                }
                return record.encode(a, .{ .changes = compact, .scan_epoch = epoch });
            };
        };
    }

    fn commit(a: A, ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (self.refs.load(.acquire) != 1) return error.TransactionCursorActive;
        const store = self.read.store;
        if (store.host.term(store.host.ptr) != self.term) return error.SessionWriteNotLeaderBeforeProposal;
        if (self.changes.items.len != 0) {
            const bytes = self.encodeChanges() catch |err| return switch (err) {
                error.OutOfMemory => error.SessionWriteOutOfMemoryBeforeProposal,
                else => err,
            };
            var submitted = false;
            const applied = store.host.propose(store.host.ptr, bytes, self.term, &submitted) catch |err| {
                if (submitted) return err;
                return switch (err) {
                    error.NotLeader => error.SessionWriteNotLeaderBeforeProposal,
                    error.OutOfMemory => error.SessionWriteOutOfMemoryBeforeProposal,
                    error.TransactionTooLarge => err,
                    else => error.SessionWriteUnavailableBeforeProposal,
                };
            };
            if (!applied) return error.SessionLeaseLost;
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
    at_row: bool = false,
    fn cast(ptr: *anyopaque) *Cursor {
        return @ptrCast(@alignCast(ptr));
    }
    fn create(a: A, read: *Read, write: ?*Write) !erased.Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .read = read, .write = write, .physical = try read.physical.openCursor() };
        if (write) |w| {
            _ = w.refs.fetchAdd(1, .monotonic);
        } else {
            _ = read.refs.fetchAdd(1, .monotonic);
        }
        return .{ .allocator = a, .ptr = self, .vtable = &.{ .close = close, .first = first, .last = last, .next = next, .prev = prev, .seek_at_or_after = after, .seek_at_or_before = before, .set_keys_only = setKeysOnly } };
    }
    fn setKeysOnly(ptr: *anyopaque) void {
        cast(ptr).physical.setKeysOnly();
    }
    fn close(a: A, ptr: *anyopaque) void {
        const self = cast(ptr);
        self.physical.close();
        if (self.write) |w| Write.abort(self.read.arena.child_allocator, w) else self.read.destroy(a);
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
        const base = if (forward) try self.physical.seekAtOrAfter(self.seek.items) else try self.physical.seekAtOrBefore(self.seek.items);
        return self.project(base, bound, forward, inclusive);
    }

    fn project(self: *Cursor, initial: ?erased.Entry, bound: ?[]const u8, forward: bool, inclusive: bool) !?erased.Entry {
        var base = initial;
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
            if (std.mem.startsWith(u8, key, record.cache_namespace)) {
                self.seek.clearRetainingCapacity();
                try self.seek.appendSlice(self.a, self.read.prefix);
                try self.seek.appendSlice(self.a, record.cache_namespace);
                // The reserved prefix ends in '/'. Its predecessor/successor
                // bound skips all derived entries in a single seek.
                if (forward) self.seek.items[self.seek.items.len - 1] += 1;
                base = if (forward) try self.physical.seekAtOrAfter(self.seek.items) else try self.physical.seekAtOrBefore(self.seek.items);
                if (!forward and base != null and std.mem.eql(u8, base.?.key, self.seek.items)) base = try self.physical.prev();
                continue;
            }
            if (!record.internalKey(key) and eligible(key, bound, forward, inclusive) and (self.write == null or self.write.?.find(key) == null)) break;
            base = if (forward) try self.physical.next() else try self.physical.prev();
        }
        var selected: ?erased.Entry = if (base) |entry| .{ .key = entry.key[self.read.prefix.len..], .value = entry.value } else null;
        if (self.write) |write| for (write.changes.items) |change| {
            const value = change.value orelse continue;
            if (!eligible(change.key, bound, forward, inclusive)) continue;
            if (selected == null or std.mem.order(u8, change.key, selected.?.key) == @as(std.math.Order, if (forward) .lt else .gt)) selected = .{ .key = change.key, .value = value };
        };
        self.at_row = selected != null;
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
        if (self.write == null and self.at_row) return self.project(try self.physical.next(), null, true, true);
        return self.position(self.key.items, true, false);
    }
    fn prev(ptr: *anyopaque) !?erased.Entry {
        const self = cast(ptr);
        if (self.write == null and self.at_row) return self.project(try self.physical.prev(), null, false, true);
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
        last_frame: u8 = 0,
        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn read(ptr: *anyopaque, _: A) !erased.ReadTxn {
            return cast(ptr).physical.beginRead();
        }
        fn term(ptr: *anyopaque) ?u64 {
            return cast(ptr).current_term;
        }
        fn propose(ptr: *anyopaque, bytes: []const u8, expected: u64, submitted: *bool) !bool {
            const self = cast(ptr);
            if (self.current_term != expected) return error.NotLeader;
            self.last_frame = bytes[0];
            submitted.* = true;
            var txn = try self.physical.beginWrite();
            var active = true;
            defer if (active) txn.abort();
            const applied = try record.apply(&txn, std.testing.allocator, 7, bytes);
            try txn.commit();
            active = false;
            if (self.lose_ack) return error.Unavailable;
            return applied;
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
        try std.testing.expectError(error.SessionWriteNotLeaderBeforeProposal, txn.commit());
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
    {
        var read = try logical.beginRead();
        var cursor = read.openCursor() catch |err| {
            read.abort();
            return err;
        };
        read.abort();
        defer cursor.close();
        try std.testing.expectEqualStrings("acknowledgement lost", (try cursor.seekAtOrAfter("receipt")).?.value);
    }
    {
        var txn = try logical.beginWrite();
        var cursor = txn.openCursor() catch |err| {
            txn.abort();
            return err;
        };
        defer cursor.close();
        try std.testing.expectError(error.TransactionCursorActive, txn.commit());
        txn.abort();
        try std.testing.expectEqualStrings("receipt", (try cursor.first()).?.key);
    }
    {
        const one = std.mem.toBytes(std.mem.nativeToLittle(u64, 1));
        var seed = try logical.beginWrite();
        errdefer seed.abort();
        try seed.put(record.count_key, &one);
        try seed.commit();
    }
    {
        var legacy = try logical.beginWrite();
        errdefer legacy.abort();
        try legacy.put(record.session_prefix ++ "legacy", "session");
        try std.testing.expectEqual(@as(u64, 1), try legacy.countPrefix(record.session_prefix));
        try legacy.commit();
    }
    {
        var read = try logical.beginRead();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), try read.countPrefix(record.session_prefix));
        try std.testing.expectEqual(@as(u64, 0), try retained.countPrefix(record.session_prefix));
        var overlay = try logical.beginWrite();
        defer overlay.abort();
        try overlay.put(record.session_prefix ++ "legacy", "updated");
        try std.testing.expectEqual(@as(u64, 1), try overlay.countPrefix(record.session_prefix));
        try overlay.delete(record.session_prefix ++ "legacy");
        try std.testing.expectEqual(@as(u64, 0), try overlay.countPrefix(record.session_prefix));
        try overlay.put(record.session_prefix ++ "new", "created");
        try std.testing.expectEqual(@as(u64, 1), try overlay.countPrefix(record.session_prefix));
    }
    {
        var repair = try logical.beginWrite();
        errdefer repair.abort();
        try std.testing.expectError(error.NotFound, repair.get(record.count_key));
        const two = std.mem.toBytes(std.mem.nativeToLittle(u64, 2));
        try repair.put(record.count_key, &two);
        try repair.commit();
    }
    {
        var txn = try logical.beginWrite();
        defer txn.abort();
        _ = try txn.countPrefix("receipt");
        // The desired value already matches, but the epoch no longer does.
        try txn.put("receipt", "acknowledgement lost");
        var remote = try physical.beginWrite();
        var remote_open = true;
        defer if (remote_open) remote.abort();
        const epoch_key = try record.keyAlloc(a, 7, record.epoch_key);
        defer a.free(epoch_key);
        const epoch = std.mem.readInt(u64, (try remote.get(epoch_key))[0..8], .little);
        var next_epoch: [8]u8 = undefined;
        std.mem.writeInt(u64, &next_epoch, epoch + 1, .little);
        try remote.put(epoch_key, &next_epoch);
        try remote.commit();
        remote_open = false;
        try std.testing.expectError(error.SessionLeaseLost, txn.commit());
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
    {
        const key = record.session_prefix ++ "legacy-large";
        const payload = try a.alloc(u8, record.max_bytes / 2 + 1);
        defer a.free(payload);
        @memset(payload, 'x');
        {
            var txn = try logical.beginWrite();
            errdefer txn.abort();
            try txn.put(key, payload);
            try txn.commit();
            try std.testing.expectEqual(@as(u8, 1), fixture.last_frame);
        }
        {
            var probe = try logical.beginProbe();
            defer probe.abort();
            try std.testing.expect(try probe.contains(key));
            try std.testing.expect(!try probe.contains(key ++ "-absent"));
        }
        {
            var txn = try logical.beginWrite();
            errdefer txn.abort();
            try std.testing.expectEqual(payload.len, (try txn.get(key)).len);
            _ = try txn.countPrefix("recovery-marker");
            try txn.put("recovery-marker", &.{});
            try txn.commit();
            // Epoch-fenced reads do not carry receipt bytes in the frame.
            try std.testing.expectEqual(@as(u8, 1), fixture.last_frame);
        }
        {
            var txn = try logical.beginWrite();
            errdefer txn.abort();
            payload[payload.len - 1] = 'y';
            try txn.put(key, payload);
            try txn.commit();
            try std.testing.expectEqual(@as(u8, 3), fixture.last_frame);
        }
    }
    {
        const key = record.session_prefix ++ "legacy-near-limit";
        const prefix = "{\"id\":1,\"execution_plan\":\"";
        const end = "\"}";
        const payload = try a.alloc(u8, record.max_bytes - 512);
        defer a.free(payload);
        @memset(payload, 'x');
        @memcpy(payload[0..prefix.len], prefix);
        @memcpy(payload[payload.len - end.len ..], end);
        {
            var txn = try logical.beginWrite();
            errdefer txn.abort();
            try txn.put(key, payload);
            try txn.commit();
        }
        const replacement = try a.alloc(u8, record.max_bytes + 1);
        defer a.free(replacement);
        @memcpy(replacement[0 .. payload.len - end.len], payload[0 .. payload.len - end.len]);
        @memset(replacement[payload.len - end.len ..], 'y');
        @memcpy(replacement[replacement.len - end.len ..], end);
        {
            var txn = try logical.beginWrite();
            errdefer txn.abort();
            try txn.put(key, replacement);
            try txn.put("lease-update", "owner:2");
            try txn.commit();
            try std.testing.expectEqual(@as(u8, 4), fixture.last_frame);
        }
        var read = try logical.beginRead();
        defer read.abort();
        try std.testing.expectEqualSlices(u8, replacement, try read.get(key));
    }
}
