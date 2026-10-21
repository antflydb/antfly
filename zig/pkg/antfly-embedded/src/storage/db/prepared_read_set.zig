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

//! Owned point observations and cursor transcripts. Replaying a transcript
//! detects changed results and scan phantoms without retaining source buffers.
//! This is optimistic validation, not durable exclusion across a decision.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const DocStore = @import("../docstore.zig").DocStore;
const Entry = @import("../backend_erased.zig").Entry;
const A = std.mem.Allocator;

pub const OwnedCursor = struct {
    allocator: A,
    read: *DocStore.Txn,
    cursor: DocStore.Txn.CursorAdapter,
    captured: ?*Cursor = null,
    owns_read: bool = true,
    pub fn seekAtOrAfter(self: *@This(), selected: []const u8) !?@import("../backend_erased.zig").Entry {
        if (self.captured) |trace| try trace.check();
        const entry = try self.cursor.seekAtOrAfter(selected);
        if (self.captured) |trace| try trace.record(.seek_after, selected, entry);
        return entry;
    }
    pub fn next(self: *@This()) !?@import("../backend_erased.zig").Entry {
        if (self.captured) |trace| try trace.check();
        const entry = try self.cursor.next();
        if (self.captured) |trace| try trace.record(.next, "", entry);
        return entry;
    }
    pub fn prev(self: *@This()) !?@import("../backend_erased.zig").Entry {
        if (self.captured) |trace| try trace.check();
        const entry = try self.cursor.prev();
        if (self.captured) |trace| try trace.record(.prev, "", entry);
        return entry;
    }
    pub fn seekAtOrBefore(self: *@This(), selected: []const u8) !?@import("../backend_erased.zig").Entry {
        if (self.captured) |trace| try trace.check();
        const entry = try self.cursor.seekAtOrBefore(selected);
        if (self.captured) |trace| try trace.record(.seek_before, selected, entry);
        return entry;
    }
    pub fn setUpperBound(self: *@This(), bound: ?[]const u8) !void {
        if (self.captured) |trace| {
            try trace.check();
            try trace.record(if (bound != null) .upper_bound else .clear_upper_bound, bound orelse "", null);
        }
        self.cursor.setUpperBound(bound);
    }
    pub fn first(self: *@This()) !?Entry {
        if (self.captured) |trace| try trace.check();
        const entry = try self.cursor.first();
        if (self.captured) |trace| try trace.record(.first, "", entry);
        return entry;
    }
    pub fn last(self: *@This()) !?Entry {
        if (self.captured) |trace| try trace.check();
        const entry = try self.cursor.last();
        if (self.captured) |trace| try trace.record(.last, "", entry);
        return entry;
    }
    pub fn close(self: *@This()) void {
        self.cursor.close();
        if (self.owns_read) {
            self.read.abort();
            self.allocator.destroy(self.read);
        }
        if (self.captured) |trace| trace.close();
    }
};
/// Borrows one existing transaction. Returned values remain transaction-owned;
/// closing a traced cursor never aborts or destroys the caller's transaction.
pub fn observeTxn(txn: *DocStore.Txn, observations: ?*Owned) TxnReader {
    return .{ .txn = txn, .observations = observations };
}
pub const TxnReader = struct {
    txn: *DocStore.Txn,
    observations: ?*Owned,
    pub fn get(self: TxnReader, key: []const u8) ![]const u8 {
        if (self.observations) |captured| try captured.check();
        const value = self.txn.get(key) catch |err| {
            if (err == error.NotFound) if (self.observations) |captured| try captured.recordPoint(key, null);
            return err;
        };
        if (self.observations) |captured| try captured.recordPoint(key, value);
        return value;
    }
    pub fn getManySorted(self: TxnReader, keys: []const []const u8, values: []?[]const u8) !void {
        self.getManySortedInner(keys, values) catch |err| {
            if (self.observations) |captured| if (captured.failure == null) {
                captured.failure = err;
            };
            return err;
        };
    }
    fn getManySortedInner(self: TxnReader, keys: []const []const u8, values: []?[]const u8) !void {
        if (self.observations) |captured| try captured.check();
        try self.txn.getManySorted(keys, values);
        if (self.observations) |captured| for (keys, values) |key, value| try captured.recordPoint(key, value);
    }
    pub fn openCursor(self: TxnReader) !OwnedCursor {
        return self.open(false);
    }
    pub fn openPhysicalCursorAdapter(self: TxnReader) !OwnedCursor {
        return self.open(true);
    }
    fn open(self: TxnReader, physical: bool) !OwnedCursor {
        if (self.observations) |captured| try captured.check();
        var cursor = if (physical) try self.txn.openPhysicalCursorAdapter() else try self.txn.openCursor();
        errdefer cursor.close();
        return .{ .allocator = self.txn.alloc, .read = self.txn, .cursor = cursor, .owns_read = false, .captured = if (self.observations) |captured| try captured.openCursorWithMode(physical) else null };
    }
    pub fn hasPrefix(self: TxnReader, prefix: []const u8) !bool {
        var cursor = try self.openPhysicalCursorAdapter();
        defer cursor.close();
        const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        return std.mem.startsWith(u8, row.key, prefix);
    }
};
/// Owns the snapshot returned to a preparation helper. The scope is movable
/// before reads/cursors start; callers close every borrowed cursor before abort.
/// No writer or raw-transaction accessor is part of this read interface.
pub const Transaction = struct {
    txn: DocStore.Txn,
    observations: ?*Owned,
    pub fn abort(self: *Transaction) void {
        self.txn.abort();
    }
    pub fn get(self: *Transaction, key: []const u8) ![]const u8 {
        return observeTxn(&self.txn, self.observations).get(key);
    }
    pub fn getManySorted(self: *Transaction, keys: []const []const u8, values: []?[]const u8) !void {
        return observeTxn(&self.txn, self.observations).getManySorted(keys, values);
    }
    pub fn openCursor(self: *Transaction) !OwnedCursor {
        return observeTxn(&self.txn, self.observations).openCursor();
    }
    pub fn openPhysicalCursorAdapter(self: *Transaction) !OwnedCursor {
        return observeTxn(&self.txn, self.observations).openPhysicalCursorAdapter();
    }
    pub fn hasPrefix(self: *Transaction, prefix: []const u8) !bool {
        return observeTxn(&self.txn, self.observations).hasPrefix(prefix);
    }
    fn scan(self: *Transaction, lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
        self.scanInner(lower, upper, options, ctx, callback) catch |err| {
            if (self.observations) |captured| if (captured.failure == null) {
                captured.failure = err;
            };
            return err;
        };
    }
    fn scanInner(self: *Transaction, lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
        var cursor = try observeTxn(&self.txn, self.observations).open(options.physical_payloads);
        defer cursor.close();
        try scanCursor(&cursor, lower, upper, options, ctx, callback);
    }
};
fn scanCursor(cursor: *OwnedCursor, lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
    if (!options.reverse) {
        try cursor.setUpperBound(if (upper.len != 0) upper else null);
        var next = if (lower.len == 0) try cursor.first() else try cursor.seekAtOrAfter(lower);
        while (next) |row| : (next = try cursor.next()) {
            if (upper.len != 0 and std.mem.order(u8, row.key, upper) != .lt) break;
            if (options.lower_exclusive and lower.len != 0 and std.mem.eql(u8, row.key, lower)) continue;
            if (options.skip_fn) |skip| if (skip(row.key)) continue;
            if (try callback(ctx, row.key, row.value) == .stop) break;
        }
    } else {
        // Match DocStore's reverse scan contract: upper is exclusive;
        // lower_exclusive only applies to forward scans.
        var next = if (upper.len == 0) try cursor.last() else try cursor.seekAtOrBefore(upper);
        while (next) |row| : (next = try cursor.prev()) {
            if (upper.len != 0 and std.mem.order(u8, row.key, upper) != .lt) continue;
            if (lower.len != 0 and std.mem.order(u8, row.key, lower) == .lt) break;
            if (options.skip_fn) |skip| if (skip(row.key)) continue;
            if (try callback(ctx, row.key, row.value) == .stop) break;
        }
    }
}
/// A read-only observed surface for preparation helpers. It returns caller-owned
/// bytes just like the physical store; the witness owns only keys/fingerprints.
/// It deliberately exposes no writes or unobserved transaction/cursor escape.
pub fn observe(store: anytype, observations: ?*Owned) ObservedReader(@TypeOf(store)) {
    return .{ .store = store, .observations = observations };
}
pub fn ObservedReader(comptime Store: type) type {
    return struct {
        store: Store,
        observations: ?*Owned,
        pub fn beginProbeTxn(self: @This()) !Transaction {
            if (self.observations) |captured| try captured.check();
            return .{ .txn = try self.store.beginProbeTxn(), .observations = self.observations };
        }
        pub fn beginProbeTxnWithBlockCacheAdmission(self: @This(), mode: @import("../backend_types.zig").Namespace.BlockCacheAdmission) !Transaction {
            if (self.observations) |captured| try captured.check();
            return .{ .txn = try self.store.beginProbeTxnWithBlockCacheAdmission(mode), .observations = self.observations };
        }
        pub fn beginReadTxn(self: @This()) !Transaction {
            if (self.observations) |captured| try captured.check();
            return .{ .txn = try self.store.beginReadTxn(), .observations = self.observations };
        }
        pub fn beginReadTxnWithBlockCacheAdmission(self: @This(), mode: @import("../backend_types.zig").Namespace.BlockCacheAdmission) !Transaction {
            if (self.observations) |captured| try captured.check();
            return .{ .txn = try self.store.beginReadTxnWithBlockCacheAdmission(mode), .observations = self.observations };
        }
        pub fn scanReadTxnWithContext(self: @This(), txn: *Transaction, lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
            if (self.observations == null) return self.store.scanReadTxnWithContext(&txn.txn, lower, upper, options, ctx, callback);
            return txn.scan(lower, upper, options, ctx, callback);
        }
        pub fn mayHaveGraphRetirements(self: @This()) !bool {
            if (self.observations == null) return self.store.mayHaveGraphRetirements();
            var read = try self.beginReadTxn();
            defer read.abort();
            return DocStore.mayHaveGraphRetirementsFromTxn(&read);
        }
        pub fn scanPrefixKeysPage(self: @This(), a: A, prefix: []const u8, after_key: ?[]const u8, limit: usize) ![][]u8 {
            if (self.observations == null) return self.store.scanPrefixKeysPage(a, prefix, after_key, limit);
            return self.scanPrefixKeysInner(a, prefix, after_key, limit) catch |err| {
                if (self.observations.?.failure == null) self.observations.?.failure = err;
                return err;
            };
        }
        fn scanPrefixKeysInner(self: @This(), a: A, prefix: []const u8, after_key: ?[]const u8, limit: usize) ![][]u8 {
            if (limit == 0) return a.alloc([]u8, 0);
            var cursor = try self.openCursor(a, true);
            defer cursor.close();
            var keys: std.ArrayListUnmanaged([]u8) = .empty;
            errdefer {
                for (keys.items) |key| a.free(key);
                keys.deinit(a);
            }
            const after = if (after_key) |key| if (std.mem.startsWith(u8, key, prefix)) key else null else null;
            var next = try cursor.seekAtOrAfter(after orelse prefix);
            while (next) |row| : (next = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                if (after) |key| if (std.mem.order(u8, row.key, key) != .gt) continue;
                const captured = self.observations.?;
                const key = try copy(a, row.key, captured.control, &captured.failure);
                errdefer a.free(key);
                try keys.append(a, @constCast(key));
                if (keys.items.len >= limit) break;
            }
            return keys.toOwnedSlice(a);
        }
        pub fn getOptional(self: @This(), a: A, key: []const u8) !?[]u8 {
            const raw = self.get(a, key) catch |err| return if (err == error.NotFound) null else err;
            return @constCast(raw);
        }
        pub fn openCursor(self: @This(), a: A, physical_payloads: bool) !OwnedCursor {
            if (self.observations) |captured| try captured.check();
            const read = try a.create(DocStore.Txn);
            errdefer a.destroy(read);
            read.* = try self.store.beginReadTxn();
            errdefer read.abort();
            var cursor = if (physical_payloads) try read.openPhysicalCursorAdapter() else try read.openCursor();
            errdefer cursor.close();
            return .{ .allocator = a, .read = read, .cursor = cursor, .captured = if (self.observations) |captured| try captured.openCursorWithMode(physical_payloads) else null };
        }
        pub fn scanPrefix(self: @This(), a: A, prefix: []const u8) ![]@import("../docstore.zig").OwnedKVPair {
            if (self.observations == null) return self.store.scanPrefix(a, prefix);
            return self.scanPrefixInner(a, prefix) catch |err| {
                if (self.observations.?.failure == null) self.observations.?.failure = err;
                return err;
            };
        }
        fn scanPrefixInner(self: @This(), a: A, prefix: []const u8) ![]@import("../docstore.zig").OwnedKVPair {
            var cursor = try self.openCursor(a, false);
            defer cursor.close();
            var results: std.ArrayListUnmanaged(@import("../docstore.zig").OwnedKVPair) = .empty;
            errdefer {
                for (results.items) |row| {
                    a.free(row.key);
                    a.free(row.value);
                }
                results.deinit(a);
            }
            var next = try cursor.seekAtOrAfter(prefix);
            while (next) |row| : (next = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                const captured = self.observations.?;
                const key = try copy(a, row.key, captured.control, &captured.failure);
                errdefer a.free(key);
                const value = try copy(a, row.value, captured.control, &captured.failure);
                errdefer a.free(value);
                try results.append(a, .{ .key = @constCast(key), .value = @constCast(value) });
            }
            return results.toOwnedSlice(a);
        }
        pub fn scanWithContext(self: @This(), lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
            if (self.observations == null) return self.store.scanWithContext(lower, upper, options, ctx, callback);
            self.scanInner(lower, upper, options, ctx, callback) catch |err| {
                if (self.observations.?.failure == null) self.observations.?.failure = err;
                return err;
            };
        }
        fn scanInner(self: @This(), lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
            var cursor = try self.openCursor(self.observations.?.allocator, options.physical_payloads);
            defer cursor.close();
            try scanCursor(&cursor, lower, upper, options, ctx, callback);
        }

        pub fn get(self: @This(), a: A, key: []const u8) ![]const u8 {
            if (self.observations) |captured| try captured.check();
            const value = self.store.get(a, key) catch |err| {
                if (err == error.NotFound) if (self.observations) |captured| try captured.recordPoint(key, null);
                return err;
            };
            errdefer a.free(value);
            if (self.observations) |captured| try captured.recordPoint(key, value);
            return value;
        }
    };
}

fn checkpoint(control: ?admission.Port, failure: ?*?anyerror) !void {
    if (failure) |slot| if (slot.* != null) return error.ReadAdmissionRejected;
    if (control) |original| original.checkpoint() catch |err| {
        if (failure) |slot| {
            if (slot.* == null) slot.* = err;
            return error.ReadAdmissionRejected;
        }
        return err;
    };
}
fn copy(a: A, bytes: []const u8, control: ?admission.Port, failure: ?*?anyerror) ![]const u8 {
    try checkpoint(control, failure);
    const result = try a.alloc(u8, bytes.len);
    errdefer a.free(result);
    var offset: usize = 0;
    while (offset < bytes.len) {
        try checkpoint(control, failure);
        const end = offset + @min(@as(usize, 4096), bytes.len - offset);
        @memcpy(result[offset..end], bytes[offset..end]);
        offset = end;
    }
    return result;
}
const Fingerprint = struct {
    len: usize,
    digest: [32]u8,
    fn of(bytes: []const u8, control: ?admission.Port, failure: ?*?anyerror) !Fingerprint {
        try checkpoint(control, failure);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var offset: usize = 0;
        while (offset < bytes.len) {
            try checkpoint(control, failure);
            const end = offset + @min(@as(usize, 4096), bytes.len - offset);
            hash.update(bytes[offset..end]);
            offset = end;
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return .{ .len = bytes.len, .digest = digest };
    }
};
const Row = struct {
    key: Fingerprint,
    value: Fingerprint,
    fn of(entry: Entry, control: ?admission.Port, failure: ?*?anyerror) !Row {
        return .{ .key = try Fingerprint.of(entry.key, control, failure), .value = try Fingerprint.of(entry.value, control, failure) };
    }
};
pub const Kind = enum { seek_after, seek_before, next, prev, first, last, upper_bound, clear_upper_bound };
const Operation = struct { kind: Kind, argument: []const u8, expected: ?Row };
const Point = struct { key: []const u8, expected: ?Fingerprint };

pub const Cursor = struct {
    allocator: A,
    control: ?admission.Port,
    failure: ?*?anyerror,
    active: bool = true,
    physical_payloads: bool = true,
    operations: std.ArrayListUnmanaged(Operation) = .empty,

    pub fn record(self: *Cursor, kind: Kind, argument: []const u8, entry: ?Entry) !void {
        self.recordInner(kind, argument, entry) catch |err| {
            if (self.failure) |slot| if (slot.* == null) {
                slot.* = err;
            };
            return err;
        };
    }
    pub fn check(self: *Cursor) !void {
        if (!self.active) return error.TransactionClosed;
        try checkpoint(self.control, self.failure);
    }
    fn recordInner(self: *Cursor, kind: Kind, argument: []const u8, entry: ?Entry) !void {
        if (!self.active) return error.TransactionClosed;
        try checkpoint(self.control, self.failure);
        const expected = if (entry) |row| try Row.of(row, self.control, self.failure) else null;
        const owned = try copy(self.allocator, argument, self.control, self.failure);
        errdefer self.allocator.free(owned);
        try self.operations.append(self.allocator, .{ .kind = kind, .argument = owned, .expected = expected });
    }
    pub fn close(self: *Cursor) void {
        self.active = false;
    }
    fn deinit(self: *Cursor) void {
        std.debug.assert(!self.active);
        for (self.operations.items) |operation| self.allocator.free(operation.argument);
        self.operations.deinit(self.allocator);
    }
};

pub const Owned = struct {
    allocator: A,
    control: ?admission.Port,
    failure: ?anyerror = null,
    points: std.ArrayListUnmanaged(Point) = .empty,
    positions: std.StringHashMapUnmanaged(usize) = .empty,
    cursors: std.ArrayListUnmanaged(*Cursor) = .empty,
    sealed: bool = false,

    pub fn init(a: A, control: ?admission.Port) Owned {
        return .{ .allocator = a, .control = control };
    }
    pub fn recordPoint(self: *Owned, key: []const u8, value: ?[]const u8) !void {
        self.recordPointInner(key, value) catch |err| {
            if (self.failure == null) self.failure = err;
            return err;
        };
    }
    pub fn check(self: *Owned) !void {
        try checkpoint(self.control, &self.failure);
    }
    fn recordPointInner(self: *Owned, key: []const u8, value: ?[]const u8) !void {
        if (self.sealed) return error.TransactionClosed;
        try checkpoint(self.control, &self.failure);
        const expected = if (value) |bytes| try Fingerprint.of(bytes, self.control, &self.failure) else null;
        if (self.positions.get(key)) |index| {
            if (!std.meta.eql(expected, self.points.items[index].expected)) return error.PreparedReadSetChanged;
            return;
        }
        const owned = try copy(self.allocator, key, self.control, &self.failure);
        errdefer self.allocator.free(owned);
        try self.points.ensureUnusedCapacity(self.allocator, 1);
        try self.positions.put(self.allocator, owned, self.points.items.len);
        self.points.appendAssumeCapacity(.{ .key = owned, .expected = expected });
    }
    pub fn openCursor(self: *Owned) !*Cursor {
        return self.openCursorWithMode(true);
    }
    pub fn openCursorWithMode(self: *Owned, physical_payloads: bool) !*Cursor {
        return self.openCursorInner(physical_payloads) catch |err| {
            if (self.failure == null) self.failure = err;
            return err;
        };
    }
    fn openCursorInner(self: *Owned, physical_payloads: bool) !*Cursor {
        if (self.sealed) return error.TransactionClosed;
        try checkpoint(self.control, &self.failure);
        const cursor = try self.allocator.create(Cursor);
        errdefer self.allocator.destroy(cursor);
        cursor.* = .{ .allocator = self.allocator, .control = self.control, .failure = &self.failure, .physical_payloads = physical_payloads };
        try self.cursors.append(self.allocator, cursor);
        return cursor;
    }
    /// No source cursor or borrowed control/failure address may escape capture.
    pub fn finish(self: *Owned) !void {
        try checkpoint(self.control, &self.failure);
        for (self.cursors.items) |cursor| if (cursor.active) return error.InvalidArgument;
        self.control = null;
        for (self.cursors.items) |cursor| {
            cursor.control = null;
            cursor.failure = null;
        }
        self.sealed = true;
    }
    pub fn take(self: *Owned) !Owned {
        if (!self.sealed or self.failure != null) return error.InvalidArgument;
        const result = self.*;
        self.* = Owned.init(self.allocator, null);
        return result;
    }
    pub fn deinit(self: *Owned) void {
        self.positions.deinit(self.allocator);
        for (self.points.items) |point| self.allocator.free(point.key);
        self.points.deinit(self.allocator);
        for (self.cursors.items) |cursor| {
            cursor.deinit();
            self.allocator.destroy(cursor);
        }
        self.cursors.deinit(self.allocator);
        self.* = undefined;
    }
    /// Caller retains publication/apply exclusion. All observations are compared
    /// with one fresh immutable primary read, including each cursor's stopping row.
    pub fn validate(self: *const Owned, store: *DocStore, original: ?admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        self.validateInner(store, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
    }
    fn validateInner(self: *const Owned, store: *DocStore, control: ?admission.Port) !void {
        try checkpoint(control, null);
        if (!self.sealed or self.failure != null) return error.InvalidArgument;
        if (self.points.items.len == 0 and self.cursors.items.len == 0) return;
        var read = if (control) |original| try store.beginReadTxnWithAdmission(.retain, original) else try store.beginReadTxn();
        defer read.abort();
        for (self.points.items) |point| {
            try checkpoint(control, null);
            const value = read.get(point.key) catch |err| if (err == error.NotFound) null else return err;
            const actual = if (value) |bytes| try Fingerprint.of(bytes, control, null) else null;
            if (!std.meta.eql(actual, point.expected)) return error.PreparedReadSetChanged;
        }
        for (self.cursors.items) |captured| {
            if (captured.active) return error.InvalidArgument;
            var cursor = if (captured.physical_payloads) try read.openPhysicalCursorAdapter() else try read.openCursor();
            defer cursor.close();
            for (captured.operations.items) |operation| {
                try checkpoint(control, null);
                const entry = switch (operation.kind) {
                    .seek_after => try cursor.seekAtOrAfter(operation.argument),
                    .seek_before => try cursor.seekAtOrBefore(operation.argument),
                    .next => try cursor.next(),
                    .prev => try cursor.prev(),
                    .first => try cursor.first(),
                    .last => try cursor.last(),
                    .upper_bound => blk: {
                        cursor.setUpperBound(operation.argument);
                        break :blk null;
                    },
                    .clear_upper_bound => blk: {
                        cursor.setUpperBound(null);
                        break :blk null;
                    },
                };
                const actual = if (entry) |row| try Row.of(row, control, null) else null;
                if (!std.meta.eql(actual, operation.expected)) return error.PreparedReadSetChanged;
            }
        }
        try checkpoint(control, null);
    }
};

/// Durable observations for an exact accepted effect/participant contract.
/// Encoding does not install a lease or grant a PREPARE vote. A future owner
/// must atomically bind these bytes to the decision and enforce them on writers.
pub const Durable = struct {
    const effects = @import("prepared_transaction_effects.zig");
    pub const Binding = struct {
        contract: effects.Contract,
        participant: effects.Binding,
        namespace: [24]u8,
        frame_digest: [32]u8,
    };
    pub const max_frame_bytes = 64 * 1024 * 1024;
    pub const max_observations = 65_536;
    const header_bytes = 250;
    const checksum_bytes = 32;
    const version: u16 = 1;

    const Writer = struct {
        data: ?[]u8 = null,
        offset: usize = 0,
        control: ?admission.Port,
        fn bytes(self: *@This(), value: []const u8) !void {
            try checkpoint(self.control, null);
            const end = std.math.add(usize, self.offset, value.len) catch return error.TransactionTooLarge;
            if (end > max_frame_bytes - checksum_bytes) return error.TransactionTooLarge;
            if (self.data) |buffer| {
                if (end > buffer.len) return error.InvalidTxnRecord;
                var start: usize = 0;
                while (start < value.len) {
                    try checkpoint(self.control, null);
                    const next = start + @min(@as(usize, 4096), value.len - start);
                    @memcpy(buffer[self.offset + start .. self.offset + next], value[start..next]);
                    start = next;
                }
            }
            self.offset = end;
        }
        fn int(self: *@This(), comptime T: type, value: T) !void {
            var buffer: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, &buffer, value, .little);
            try self.bytes(&buffer);
        }
        fn slice(self: *@This(), value: []const u8) !void {
            try self.int(u32, std.math.cast(u32, value.len) orelse return error.TransactionTooLarge);
            try self.bytes(value);
        }
        fn fingerprint(self: *@This(), value: Fingerprint) !void {
            try self.int(u64, value.len);
            try self.bytes(&value.digest);
        }
    };
    fn write(writer: *Writer, owned: *const Owned, binding: Binding, size: u32) !void {
        if (!owned.sealed or owned.failure != null or owned.control != null) return error.InvalidArgument;
        if (binding.contract.intent_revision == 0) return error.InvalidPreparationRequirement;
        const participant = try binding.participant.encode();
        if (owned.points.items.len > max_observations or owned.cursors.items.len > max_observations) return error.TransactionTooLarge;
        var observations: usize = owned.points.items.len + owned.cursors.items.len;
        if (observations > max_observations) return error.TransactionTooLarge;
        try writer.bytes("ATRO");
        try writer.int(u16, version);
        try writer.int(u16, 0);
        try writer.int(u32, size);
        try writer.bytes(&binding.contract.txn_id);
        try writer.int(u64, binding.contract.intent_revision);
        try writer.bytes(&binding.contract.intent_digest);
        try writer.bytes(&binding.contract.catalog_digest);
        try writer.int(u8, @intFromBool(binding.contract.schema_version != null));
        try writer.int(u32, binding.contract.schema_version orelse 0);
        try writer.int(u8, @backingInt(binding.contract.mode));
        try writer.bytes(&participant);
        try writer.bytes(&binding.frame_digest);
        try writer.bytes(&binding.namespace);
        try writer.int(u32, @intCast(owned.points.items.len));
        try writer.int(u32, @intCast(owned.cursors.items.len));
        for (owned.points.items) |point| {
            try writer.slice(point.key);
            try writer.int(u8, @intFromBool(point.expected != null));
            if (point.expected) |expected| try writer.fingerprint(expected);
        }
        for (owned.cursors.items) |cursor| {
            if (cursor.active or cursor.control != null or cursor.failure != null) return error.InvalidArgument;
            observations = std.math.add(usize, observations, cursor.operations.items.len) catch return error.TransactionTooLarge;
            if (observations > max_observations) return error.TransactionTooLarge;
            try writer.int(u8, @intFromBool(cursor.physical_payloads));
            try writer.int(u32, @intCast(cursor.operations.items.len));
            for (cursor.operations.items) |operation| {
                if ((operation.kind != .seek_after and operation.kind != .seek_before and operation.kind != .upper_bound and operation.argument.len != 0) or ((operation.kind == .upper_bound or operation.kind == .clear_upper_bound) and operation.expected != null)) return error.InvalidArgument;
                // Wire tags are explicit and independent of compiler enum tags.
                try writer.int(u8, switch (operation.kind) {
                    .seek_after => 0,
                    .seek_before => 1,
                    .next => 2,
                    .prev => 3,
                    .first => 4,
                    .last => 5,
                    .upper_bound => 6,
                    .clear_upper_bound => 7,
                });
                try writer.slice(operation.argument);
                try writer.int(u8, @intFromBool(operation.expected != null));
                if (operation.expected) |expected| {
                    try writer.fingerprint(expected.key);
                    try writer.fingerprint(expected.value);
                }
            }
        }
    }
    pub fn encode(alloc: A, owned: *const Owned, binding: Binding, original: ?admission.Port) ![]u8 {
        var trace: admission.RejectionTrace = .{};
        return encodeInner(alloc, owned, binding, trace.bind(original)) catch |err| return trace.failure orelse err;
    }
    fn encodeInner(alloc: A, owned: *const Owned, binding: Binding, control: ?admission.Port) ![]u8 {
        var size: Writer = .{ .control = control };
        try write(&size, owned, binding, 0);
        const frame = try alloc.alloc(u8, size.offset + checksum_bytes);
        errdefer alloc.free(frame);
        var writer: Writer = .{ .data = frame, .control = control };
        try write(&writer, owned, binding, @intCast(frame.len));
        if (writer.offset != size.offset) return error.InvalidTxnRecord;
        const hash = try Fingerprint.of(frame[0..writer.offset], control, null);
        @memcpy(frame[writer.offset..], &hash.digest);
        try checkpoint(control, null);
        return frame;
    }

    const Reader = struct {
        data: []const u8,
        offset: usize = 0,
        control: ?admission.Port,
        fn bytes(self: *@This(), length: usize) ![]const u8 {
            try checkpoint(self.control, null);
            if (length > self.data.len - self.offset) return error.InvalidTxnRecord;
            const value = self.data[self.offset..][0..length];
            self.offset += length;
            return value;
        }
        fn int(self: *@This(), comptime T: type) !T {
            return std.mem.readInt(T, (try self.bytes(@sizeOf(T)))[0..@sizeOf(T)], .little);
        }
        fn boolean(self: *@This()) !bool {
            return switch (try self.int(u8)) {
                0 => false,
                1 => true,
                else => error.InvalidTxnRecord,
            };
        }
        fn slice(self: *@This(), alloc: A) ![]const u8 {
            const length = try self.int(u32);
            return copy(alloc, try self.bytes(length), self.control, null);
        }
        fn fingerprint(self: *@This()) !Fingerprint {
            const length = std.math.cast(usize, try self.int(u64)) orelse return error.InvalidTxnRecord;
            return .{ .len = length, .digest = (try self.bytes(32))[0..32].* };
        }
    };
    pub fn decode(alloc: A, frame: []const u8, expected: Binding, original: ?admission.Port) !Owned {
        var trace: admission.RejectionTrace = .{};
        return decodeInner(alloc, frame, expected, trace.bind(original)) catch |err| return trace.failure orelse err;
    }
    fn decodeInner(alloc: A, frame: []const u8, expected: Binding, control: ?admission.Port) !Owned {
        try checkpoint(control, null);
        if (frame.len < header_bytes + checksum_bytes or frame.len > max_frame_bytes) return error.InvalidTxnRecord;
        const end = frame.len - checksum_bytes;
        const actual = try Fingerprint.of(frame[0..end], control, null);
        if (!std.mem.eql(u8, &actual.digest, frame[end..])) return error.InvalidTxnRecord;
        var reader: Reader = .{ .data = frame[0..end], .control = control };
        if (!std.mem.eql(u8, try reader.bytes(4), "ATRO")) return error.InvalidTxnRecord;
        if (try reader.int(u16) != version) return error.UnsupportedVersion;
        if (try reader.int(u16) != 0 or try reader.int(u32) != frame.len) return error.InvalidTxnRecord;
        var binding: Binding = undefined;
        binding.contract.txn_id = (try reader.bytes(16))[0..16].*;
        binding.contract.intent_revision = try reader.int(u64);
        binding.contract.intent_digest = (try reader.bytes(32))[0..32].*;
        binding.contract.catalog_digest = (try reader.bytes(32))[0..32].*;
        const has_schema = try reader.boolean();
        const schema_version = try reader.int(u32);
        if (!has_schema and schema_version != 0) return error.InvalidTxnRecord;
        binding.contract.schema_version = if (has_schema) schema_version else null;
        binding.contract.mode = std.enums.fromInt(effects.Mode, try reader.int(u8)) orelse return error.InvalidTxnRecord;
        binding.participant = try effects.Binding.decode(try reader.bytes(effects.Binding.encoded_bytes));
        binding.frame_digest = (try reader.bytes(32))[0..32].*;
        binding.namespace = (try reader.bytes(24))[0..24].*;
        if (binding.contract.intent_revision == 0) return error.InvalidTxnRecord;
        if (!std.meta.eql(binding, expected)) return error.PreparedEffectsMismatch;
        const points = try reader.int(u32);
        const cursors = try reader.int(u32);
        if (points > max_observations or cursors > max_observations) return error.InvalidTxnRecord;
        var observations = @as(usize, points) + cursors;
        if (observations > max_observations or @as(u64, points) * 5 + @as(u64, cursors) * 5 > reader.data.len - reader.offset) return error.InvalidTxnRecord;
        var result = Owned.init(alloc, null);
        errdefer result.deinit();
        try result.points.ensureTotalCapacityPrecise(alloc, points);
        for (0..points) |_| {
            const key = try reader.slice(alloc);
            errdefer alloc.free(key);
            const fingerprint = if (try reader.boolean()) try reader.fingerprint() else null;
            result.points.appendAssumeCapacity(.{ .key = key, .expected = fingerprint });
        }
        try result.cursors.ensureTotalCapacityPrecise(alloc, cursors);
        for (0..cursors) |_| {
            const physical = try reader.boolean();
            const operations = try reader.int(u32);
            if (operations > max_observations) return error.InvalidTxnRecord;
            observations += operations;
            if (observations > max_observations or @as(u64, operations) * 6 > reader.data.len - reader.offset) return error.InvalidTxnRecord;
            const cursor = try alloc.create(Cursor);
            cursor.* = .{ .allocator = alloc, .control = null, .failure = null, .active = false, .physical_payloads = physical };
            result.cursors.appendAssumeCapacity(cursor);
            try cursor.operations.ensureTotalCapacityPrecise(alloc, operations);
            for (0..operations) |_| {
                const kind: Kind = switch (try reader.int(u8)) {
                    0 => .seek_after,
                    1 => .seek_before,
                    2 => .next,
                    3 => .prev,
                    4 => .first,
                    5 => .last,
                    6 => .upper_bound,
                    7 => .clear_upper_bound,
                    else => return error.InvalidTxnRecord,
                };
                const argument = try reader.slice(alloc);
                errdefer alloc.free(argument);
                const row: ?Row = if (try reader.boolean()) .{ .key = try reader.fingerprint(), .value = try reader.fingerprint() } else null;
                if ((kind != .seek_after and kind != .seek_before and kind != .upper_bound and argument.len != 0) or ((kind == .upper_bound or kind == .clear_upper_bound) and row != null)) return error.InvalidTxnRecord;
                cursor.operations.appendAssumeCapacity(.{ .kind = kind, .argument = argument, .expected = row });
            }
        }
        if (reader.offset != reader.data.len) return error.InvalidTxnRecord;
        try checkpoint(control, null);
        result.sealed = true;
        return result;
    }
};
