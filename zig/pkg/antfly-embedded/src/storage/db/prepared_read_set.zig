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

//! Owned point, semantic range observations and raw cursor transcripts. Replaying a transcript
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
        if (self.observations) |captured| return self.hasPrefixCaptured(captured, prefix) catch |err| {
            if (captured.failure == null) captured.failure = err;
            return err;
        };
        var cursor = try self.openPhysicalCursorAdapter();
        defer cursor.close();
        const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        return std.mem.startsWith(u8, row.key, prefix);
    }
    fn hasPrefixCaptured(self: TxnReader, captured: *Owned, prefix: []const u8) !bool {
        const Callback = struct {
            fn visit(raw: ?*anyopaque, _: []const u8, _: []const u8) !DocStore.ScanAction {
                const found: *bool = @ptrCast(@alignCast(raw.?));
                found.* = true;
                return .stop;
            }
        };
        const end = try prefixEnd(captured.allocator, prefix, captured);
        defer if (end) |key| captured.allocator.free(key);
        var found = false;
        try captureScan(self.txn, captured, prefix, end, .{ .physical_payloads = true }, true, &found, Callback.visit);
        return found;
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
        if (self.observations) |captured| return captureScan(&self.txn, captured, lower, if (upper.len == 0) null else upper, options, false, ctx, callback);
        var cursor = try observeTxn(&self.txn, null).open(options.physical_payloads);
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
            const captured = self.observations.?;
            const Context = struct {
                allocator: A,
                captured: *Owned,
                keys: std.ArrayListUnmanaged([]u8) = .empty,
                limit: usize,
                fn visit(raw: ?*anyopaque, key: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
                    const context: *@This() = @ptrCast(@alignCast(raw.?));
                    const owned = try copy(context.allocator, key, context.captured.control, &context.captured.failure);
                    errdefer context.allocator.free(owned);
                    try context.keys.append(context.allocator, @constCast(owned));
                    return if (context.keys.items.len == context.limit) .stop else .@"continue";
                }
            };
            var context: Context = .{ .allocator = a, .captured = captured, .limit = limit };
            errdefer {
                for (context.keys.items) |key| a.free(key);
                context.keys.deinit(a);
            }
            const after = if (after_key) |key| if (std.mem.startsWith(u8, key, prefix)) key else null else null;
            const end = try prefixEnd(a, prefix, captured);
            defer if (end) |key| a.free(key);
            var read = try self.store.beginReadTxn();
            defer read.abort();
            try captureScan(&read, captured, after orelse prefix, end, .{ .lower_exclusive = after != null, .physical_payloads = true }, true, &context, Context.visit);
            return context.keys.toOwnedSlice(a);
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
            const captured = self.observations.?;
            const Context = struct {
                allocator: A,
                captured: *Owned,
                results: std.ArrayListUnmanaged(@import("../docstore.zig").OwnedKVPair) = .empty,
                fn visit(raw: ?*anyopaque, key: []const u8, value: []const u8) anyerror!DocStore.ScanAction {
                    const context: *@This() = @ptrCast(@alignCast(raw.?));
                    const owned_key = try copy(context.allocator, key, context.captured.control, &context.captured.failure);
                    errdefer context.allocator.free(owned_key);
                    const owned_value = try copy(context.allocator, value, context.captured.control, &context.captured.failure);
                    errdefer context.allocator.free(owned_value);
                    try context.results.append(context.allocator, .{ .key = @constCast(owned_key), .value = @constCast(owned_value) });
                    return .@"continue";
                }
            };
            var context: Context = .{ .allocator = a, .captured = captured };
            errdefer {
                for (context.results.items) |row| {
                    a.free(row.key);
                    a.free(row.value);
                }
                context.results.deinit(a);
            }
            const end = try prefixEnd(a, prefix, captured);
            defer if (end) |key| a.free(key);
            var read = try self.store.beginReadTxn();
            defer read.abort();
            try captureScan(&read, captured, prefix, end, .{}, false, &context, Context.visit);
            return context.results.toOwnedSlice(a);
        }
        pub fn scanWithContext(self: @This(), lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
            if (self.observations == null) return self.store.scanWithContext(lower, upper, options, ctx, callback);
            self.scanInner(lower, upper, options, ctx, callback) catch |err| {
                if (self.observations.?.failure == null) self.observations.?.failure = err;
                return err;
            };
        }
        fn scanInner(self: @This(), lower: []const u8, upper: []const u8, options: DocStore.ScanOptions, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
            var read = try self.store.beginReadTxn();
            defer read.abort();
            try captureScan(&read, self.observations.?, lower, if (upper.len == 0) null else upper, options, false, ctx, callback);
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
fn prefixEnd(a: A, prefix: []const u8, captured: *Owned) !?[]const u8 {
    const result = try copy(a, prefix, captured.control, &captured.failure);
    errdefer a.free(result);
    var length = result.len;
    while (length != 0) {
        try captured.check();
        length -= 1;
        if (result[length] == 0xff) continue;
        const mutable = @constCast(result);
        mutable[length] += 1;
        return try a.realloc(mutable, length + 1);
    }
    a.free(result);
    return null;
}
/// Semantic scan bounds exclude rows fetched only to detect exhaustion. A page
/// that stops early closes its interval at its last observed key. Keys-only
/// scans depend on membership, not the payloads of existing members.
const Range = struct {
    lower: []const u8,
    upper: ?[]const u8,
    lower_inclusive: bool = true,
    upper_inclusive: bool = false,
    reverse: bool = false,
    physical_payloads: bool = false,
    keys_only: bool = false,
    count: u64 = 0,
    digest: [32]u8 = undefined,
    fn valid(self: Range) bool {
        return !(self.upper_inclusive and self.upper == null) and !(self.keys_only and !self.physical_payloads);
    }
    fn deinit(self: *Range, a: A) void {
        a.free(self.lower);
        if (self.upper) |end| a.free(end);
    }
};
const RangeHash = struct {
    hash: std.crypto.hash.sha2.Sha256 = std.crypto.hash.sha2.Sha256.init(.{}),
    count: u64 = 0,
    fn add(self: *@This(), key: []const u8, value: []const u8, control: ?admission.Port, failure: ?*?anyerror) !void {
        const row = try Row.of(.{ .key = key, .value = value }, control, failure);
        var size: [8]u8 = undefined;
        for ([_]Fingerprint{ row.key, row.value }) |part| {
            std.mem.writeInt(u64, &size, part.len, .little);
            self.hash.update(&size);
            self.hash.update(&part.digest);
        }
        self.count = std.math.add(u64, self.count, 1) catch return error.TransactionTooLarge;
    }
    fn finish(self: *@This()) [32]u8 {
        var digest: [32]u8 = undefined;
        self.hash.final(&digest);
        return digest;
    }
};
/// Bounds are checked on physical keys before resolving any logical payload.
/// This also works on adapters whose upper bound is only an optimization hint.
const RangeCursor = struct {
    cursor: DocStore.Txn.CursorAdapter,
    spec: *const Range,
    started: bool = false,
    fn next(self: *@This(), control: ?admission.Port, failure: ?*?anyerror) !?Entry {
        while (true) {
            try checkpoint(control, failure);
            const entry = if (!self.started) blk: {
                self.started = true;
                break :blk if (self.spec.reverse)
                    if (self.spec.upper) |end| try self.cursor.seekAtOrBefore(end) else try self.cursor.last()
                else
                    try self.cursor.seekAtOrAfter(self.spec.lower);
            } else if (self.spec.reverse) try self.cursor.prev() else try self.cursor.next();
            const row = entry orelse return null;
            const low = try Footprint.compare(row.key, self.spec.lower, control);
            if (low == .lt or (low == .eq and !self.spec.lower_inclusive)) {
                if (self.spec.reverse) return null;
                continue;
            }
            if (self.spec.upper) |end| {
                const high = try Footprint.compare(row.key, end, control);
                if (high == .gt or (high == .eq and !self.spec.upper_inclusive)) {
                    if (!self.spec.reverse) return null;
                    continue;
                }
            }
            return row;
        }
    }
};
fn captureScan(read: *DocStore.Txn, captured: *Owned, lower: []const u8, upper: ?[]const u8, options: DocStore.ScanOptions, keys_only: bool, ctx: ?*anyopaque, callback: DocStore.ScanWithContextCallback) !void {
    if (captured.sealed) return error.TransactionClosed;
    try captured.check();
    captured.active_scans += 1;
    defer captured.active_scans -= 1;
    // Generic scan lower_exclusive ignores an empty lower. Page continuations
    // instead treat an explicitly supplied empty key as a real exclusive key.
    var spec: Range = .{ .lower = try copy(captured.allocator, lower, captured.control, &captured.failure), .upper = null, .lower_inclusive = options.reverse or !options.lower_exclusive or (lower.len == 0 and !keys_only), .reverse = options.reverse, .physical_payloads = options.physical_payloads, .keys_only = keys_only };
    errdefer spec.deinit(captured.allocator);
    if (upper) |end| spec.upper = try copy(captured.allocator, end, captured.control, &captured.failure);
    var cursor: RangeCursor = .{ .cursor = try read.openPhysicalCursorAdapter(), .spec = &spec };
    defer cursor.cursor.close();
    var hash: RangeHash = .{};
    while (try cursor.next(captured.control, &captured.failure)) |row| {
        // Point resolution and callbacks may invalidate borrowed buffers.
        const key = try copy(captured.allocator, row.key, captured.control, &captured.failure);
        defer captured.allocator.free(key);
        const value = if (keys_only) "" else if (options.physical_payloads) row.value else try read.get(key);
        try hash.add(key, value, captured.control, &captured.failure);
        if (options.skip_fn) |skip| if (skip(key)) continue;
        if (try callback(ctx, key, value) == .stop) {
            const endpoint = try copy(captured.allocator, key, captured.control, &captured.failure);
            if (options.reverse) {
                captured.allocator.free(spec.lower);
                spec.lower = endpoint;
                spec.lower_inclusive = true;
            } else {
                if (spec.upper) |end| captured.allocator.free(end);
                spec.upper = endpoint;
                spec.upper_inclusive = true;
            }
            break;
        }
    }
    spec.count = hash.count;
    spec.digest = hash.finish();
    try captured.ranges.append(captured.allocator, spec);
}
fn validateRange(spec: *const Range, read: *DocStore.Txn, a: A, control: ?admission.Port) !void {
    if (!spec.valid()) return error.InvalidArgument;
    var cursor: RangeCursor = .{ .cursor = try read.openPhysicalCursorAdapter(), .spec = spec };
    defer cursor.cursor.close();
    var hash: RangeHash = .{};
    while (try cursor.next(control, null)) |row| {
        const key = try copy(a, row.key, control, null);
        defer a.free(key);
        const value = if (spec.keys_only) "" else if (spec.physical_payloads) row.value else try read.get(key);
        try hash.add(key, value, control, null);
    }
    if (hash.count != spec.count or !std.mem.eql(u8, &hash.finish(), &spec.digest)) return error.PreparedReadSetChanged;
}
pub const Kind = enum { seek_after, seek_before, next, prev, first, last, upper_bound, clear_upper_bound };
const Operation = struct { kind: Kind, argument: []const u8, expected: ?Row };
const Point = struct { key: []const u8, expected: ?Fingerprint };
fn validOperationShape(operation: Operation) bool {
    return !((operation.kind != .seek_after and operation.kind != .seek_before and operation.kind != .upper_bound and operation.argument.len != 0) or
        ((operation.kind == .upper_bound or operation.kind == .clear_upper_bound) and operation.expected != null));
}

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
    ranges: std.ArrayListUnmanaged(Range) = .empty,
    active_scans: usize = 0,

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
        if (self.active_scans != 0) return error.InvalidArgument;
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
    /// Release one point, cursor operation/cursor, or range per turn. No
    /// borrowed source cursor/control survives a finished capture. Partial
    /// destruction invalidates every capture/validation/serialization entry.
    pub fn deinitStep(self: *Owned) bool {
        self.control = null;
        self.failure = error.TransactionClosed;
        self.sealed = false;
        self.positions.deinit(self.allocator);
        self.positions = .empty;
        if (self.points.pop()) |point| {
            self.allocator.free(point.key);
            return false;
        }
        if (self.cursors.items.len != 0) {
            const cursor = self.cursors.items[self.cursors.items.len - 1];
            std.debug.assert(!cursor.active);
            if (cursor.operations.pop()) |operation| {
                self.allocator.free(operation.argument);
                return false;
            }
            cursor.operations.deinit(self.allocator);
            self.allocator.destroy(cursor);
            _ = self.cursors.pop();
            return false;
        }
        if (self.ranges.pop()) |range| {
            var owned = range;
            owned.deinit(self.allocator);
            return false;
        }
        self.points.deinit(self.allocator);
        self.cursors.deinit(self.allocator);
        self.ranges.deinit(self.allocator);
        self.* = undefined;
        return true;
    }
    pub fn deinit(self: *Owned) void {
        while (!self.deinitStep()) {}
    }
    /// Caller retains publication/apply exclusion. All observations are compared
    /// with one fresh immutable primary read. Raw cursor traces include stopping
    /// rows; semantic scan records include only their examined interval.
    pub fn validate(self: *const Owned, store: *DocStore, original: ?admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        self.validateInner(store, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
    }
    fn validateInner(self: *const Owned, store: *DocStore, control: ?admission.Port) !void {
        try checkpoint(control, null);
        if (!self.sealed or self.failure != null) return error.InvalidArgument;
        if (self.points.items.len == 0 and self.cursors.items.len == 0 and self.ranges.items.len == 0) return;
        var read = if (control) |original| try store.beginReadTxnWithAdmission(.retain, original) else try store.beginReadTxn();
        defer read.abort();
        for (self.points.items) |point| {
            try checkpoint(control, null);
            const value = read.get(point.key) catch |err| if (err == error.NotFound) null else return err;
            const actual = if (value) |bytes| try Fingerprint.of(bytes, control, null) else null;
            if (!std.meta.eql(actual, point.expected)) return error.PreparedReadSetChanged;
        }
        for (self.ranges.items) |*range| try validateRange(range, &read, self.allocator, control);
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
    /// Structural peek only; decode still verifies checksum and exact binding.
    pub fn requiredReaderVersion(frame: []const u8) !u32 {
        if (frame.len < header_bytes + checksum_bytes or frame.len > max_frame_bytes or !std.mem.eql(u8, frame[0..4], "ATRO")) return error.InvalidTxnRecord;
        return switch (std.mem.readInt(u16, frame[4..6], .little)) {
            1 => @import("catalog/reader_versions.zig").read_observations_reader_version,
            2 => @import("catalog/reader_versions.zig").read_ranges_reader_version,
            else => error.UnsupportedVersion,
        };
    }

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
        if (!owned.sealed or owned.failure != null or owned.control != null or owned.active_scans != 0) return error.InvalidArgument;
        if (binding.contract.intent_revision == 0) return error.InvalidPreparationRequirement;
        const participant = try binding.participant.encode();
        if (owned.points.items.len > max_observations or owned.cursors.items.len > max_observations or owned.ranges.items.len > max_observations) return error.TransactionTooLarge;
        var observations: usize = owned.points.items.len + owned.cursors.items.len + owned.ranges.items.len;
        if (observations > max_observations) return error.TransactionTooLarge;
        try writer.bytes("ATRO");
        try writer.int(u16, if (owned.ranges.items.len == 0) 1 else 2);
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
        if (owned.ranges.items.len != 0) try writer.int(u32, @intCast(owned.ranges.items.len));
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
                if (!validOperationShape(operation)) return error.InvalidArgument;
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
        for (owned.ranges.items) |range| {
            if (!range.valid()) return error.InvalidArgument;
            const flags: u8 = @as(u8, @intFromBool(range.lower_inclusive)) |
                (@as(u8, @intFromBool(range.upper_inclusive)) << 1) |
                (@as(u8, @intFromBool(range.reverse)) << 2) |
                (@as(u8, @intFromBool(range.physical_payloads)) << 3) |
                (@as(u8, @intFromBool(range.upper != null)) << 4) |
                (@as(u8, @intFromBool(range.keys_only)) << 5);
            try writer.int(u8, flags);
            try writer.slice(range.lower);
            if (range.upper) |end| try writer.slice(end);
            try writer.int(u64, range.count);
            try writer.bytes(&range.digest);
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
        const version = try reader.int(u16);
        if (version != 1 and version != 2) return error.UnsupportedVersion;
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
        const ranges = if (version == 2) try reader.int(u32) else 0;
        if (points > max_observations or cursors > max_observations or ranges > max_observations) return error.InvalidTxnRecord;
        var observations = @as(usize, points) + cursors + ranges;
        if (observations > max_observations or @as(u64, points) * 5 + @as(u64, cursors) * 5 + @as(u64, ranges) * 45 > reader.data.len - reader.offset) return error.InvalidTxnRecord;
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
                if (!validOperationShape(.{ .kind = kind, .argument = argument, .expected = row })) return error.InvalidTxnRecord;
                cursor.operations.appendAssumeCapacity(.{ .kind = kind, .argument = argument, .expected = row });
            }
        }
        try result.ranges.ensureTotalCapacityPrecise(alloc, ranges);
        for (0..ranges) |_| {
            const flags = try reader.int(u8);
            if (flags & 0xc0 != 0 or (flags & 2 != 0 and flags & 16 == 0) or (flags & 32 != 0 and flags & 8 == 0)) return error.InvalidTxnRecord;
            var range: Range = .{ .lower = try reader.slice(alloc), .upper = null, .lower_inclusive = flags & 1 != 0, .upper_inclusive = flags & 2 != 0, .reverse = flags & 4 != 0, .physical_payloads = flags & 8 != 0, .keys_only = flags & 32 != 0 };
            errdefer range.deinit(alloc);
            if (flags & 16 != 0) range.upper = try reader.slice(alloc);
            range.count = try reader.int(u64);
            range.digest = (try reader.bytes(32))[0..32].*;
            result.ranges.appendAssumeCapacity(range);
        }
        if (reader.offset != reader.data.len) return error.InvalidTxnRecord;
        try checkpoint(control, null);
        result.sealed = true;
        return result;
    }
};

/// Owned over-approximation of writes that can change the captured results.
/// This is not a lease. The caller must retain its apply/publication authority
/// through compilation and eventual atomic admission. Raw cursor footprints can
/// include stopping rows and control metadata; admission must reject or refine
/// unsafe control dependencies rather than fencing mandatory progress.
pub const Footprint = struct {
    arena: std.heap.ArenaAllocator,
    spans: []const Span,
    membership_spans: []const Span = &.{},
    pub const max_predicates = 65_536;
    pub const max_key_bytes = 64 * 1024 * 1024;
    pub const Span = struct {
        lower: []const u8,
        lower_inclusive: bool,
        upper: ?[]const u8,
        upper_inclusive: bool,
    };
    pub fn deinit(self: *Footprint) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn conflicts(self: *const Footprint, key: []const u8, original: ?admission.Port) !bool {
        var trace: admission.RejectionTrace = .{};
        const control = trace.bind(original);
        return (contains(self.spans, key, control) catch |err| return trace.failure orelse err) or
            (contains(self.membership_spans, key, control) catch |err| return trace.failure orelse err);
    }
    /// Caller supplies existence before/after the proposed write in its atomic
    /// writer view. Replacing an existing payload cannot change a key-only page.
    pub fn conflictsMutation(self: *const Footprint, key: []const u8, existed: bool, present: bool, original: ?admission.Port) !bool {
        if (!existed and !present) {
            try checkpoint(original, null);
            return false;
        }
        var trace: admission.RejectionTrace = .{};
        const control = trace.bind(original);
        return (contains(self.spans, key, control) catch |err| return trace.failure orelse err) or
            (existed != present and (contains(self.membership_spans, key, control) catch |err| return trace.failure orelse err));
    }
    fn contains(spans: []const Span, key: []const u8, control: ?admission.Port) !bool {
        try checkpoint(control, null);
        var lower: usize = 0;
        var upper = spans.len;
        while (lower < upper) {
            const mid = lower + (upper - lower) / 2;
            const span = spans[mid];
            const order = try compare(span.lower, key, control);
            if (order == .gt or (order == .eq and !span.lower_inclusive)) upper = mid else lower = mid + 1;
        }
        if (lower == 0) return false;
        const span = spans[lower - 1];
        const end = span.upper orelse return true;
        const order = try compare(key, end, control);
        return order == .lt or (order == .eq and span.upper_inclusive);
    }
    fn compare(left: []const u8, right: []const u8, control: ?admission.Port) !std.math.Order {
        var offset: usize = 0;
        const length = @min(left.len, right.len);
        while (offset < length) {
            try checkpoint(control, null);
            const end = offset + @min(@as(usize, 4096), length - offset);
            const order = std.mem.order(u8, left[offset..end], right[offset..end]);
            if (order != .eq) return order;
            offset = end;
        }
        try checkpoint(control, null);
        return std.math.order(left.len, right.len);
    }
    const Builder = struct {
        arena: std.heap.ArenaAllocator,
        spans: std.ArrayListUnmanaged(Span) = .empty,
        key_bytes: usize = 0,
        control: ?admission.Port,
        fn key(self: *@This(), bytes: []const u8) ![]const u8 {
            self.key_bytes = std.math.add(usize, self.key_bytes, bytes.len) catch return error.TransactionTooLarge;
            if (self.key_bytes > max_key_bytes) return error.TransactionTooLarge;
            return copy(self.arena.allocator(), bytes, self.control, null);
        }
        fn add(self: *@This(), lower: []const u8, lower_inclusive: bool, upper: ?[]const u8, upper_inclusive: bool) !void {
            try checkpoint(self.control, null);
            if (upper) |end| {
                const order = try compare(lower, end, self.control);
                if (order == .gt or (order == .eq and (!lower_inclusive or !upper_inclusive))) return;
            }
            if (self.spans.items.len == max_predicates) return error.TransactionTooLarge;
            try self.spans.append(self.arena.allocator(), .{ .lower = try self.key(lower), .lower_inclusive = lower_inclusive, .upper = if (upper) |end| try self.key(end) else null, .upper_inclusive = upper_inclusive });
        }
        fn less(self: *@This(), left: Span, right: Span) !bool {
            const order = try compare(left.lower, right.lower, self.control);
            return order == .lt or (order == .eq and left.lower_inclusive and !right.lower_inclusive);
        }
        fn normalize(self: *@This()) ![]const Span {
            const count = self.spans.items.len;
            if (count == 0) return &.{};
            // Bottom-up merge sort can stop at every comparison on cancellation.
            // No comparator swallows a rejection while completing an opaque sort.
            var source = self.spans.items;
            var target = try self.arena.allocator().alloc(Span, count);
            var width: usize = 1;
            while (width < count) : (width *= 2) {
                var start: usize = 0;
                while (start < count) : (start += 2 * width) {
                    const middle = @min(start + width, count);
                    const end = @min(start + 2 * width, count);
                    var left = start;
                    var right = middle;
                    for (target[start..end]) |*out| {
                        try checkpoint(self.control, null);
                        if (left < middle and (right == end or !try self.less(source[right], source[left]))) {
                            out.* = source[left];
                            left += 1;
                        } else {
                            out.* = source[right];
                            right += 1;
                        }
                    }
                }
                std.mem.swap([]Span, &source, &target);
            }
            var used: usize = 0;
            for (source) |span| {
                try checkpoint(self.control, null);
                if (used != 0) {
                    const previous = &source[used - 1];
                    const overlap = if (previous.upper) |end| blk: {
                        const order = try compare(end, span.lower, self.control);
                        break :blk order == .gt or (order == .eq and (previous.upper_inclusive or span.lower_inclusive));
                    } else true;
                    if (overlap) {
                        if (span.upper == null) {
                            previous.upper = null;
                            previous.upper_inclusive = false;
                        } else if (previous.upper) |end| {
                            const order = try compare(end, span.upper.?, self.control);
                            if (order == .lt) {
                                previous.upper = span.upper;
                                previous.upper_inclusive = span.upper_inclusive;
                            } else if (order == .eq) previous.upper_inclusive = previous.upper_inclusive or span.upper_inclusive;
                        }
                        continue;
                    }
                }
                source[used] = span;
                used += 1;
            }
            return source[0..used];
        }
    };

    /// Replay on one immutable snapshot. Logical payload observations use point
    /// resolution from that same snapshot; physical keys always come from its
    /// raw cursor so a payload wrapper cannot hide the underlying cursor bounds.
    pub fn compile(alloc: A, captured: *const Owned, store: *DocStore, original: ?admission.Port) !Footprint {
        var trace: admission.RejectionTrace = .{};
        return compileInner(alloc, captured, store, trace.bind(original)) catch |err| return trace.failure orelse err;
    }
    fn compileInner(alloc: A, captured: *const Owned, store: *DocStore, control: ?admission.Port) !Footprint {
        try checkpoint(control, null);
        if (!captured.sealed or captured.failure != null or captured.control != null or captured.active_scans != 0) return error.InvalidArgument;
        if (captured.points.items.len == 0 and captured.cursors.items.len == 0 and captured.ranges.items.len == 0) return .{ .arena = std.heap.ArenaAllocator.init(alloc), .spans = &.{} };
        var observations = std.math.add(usize, captured.points.items.len, captured.cursors.items.len) catch return error.TransactionTooLarge;
        observations = std.math.add(usize, observations, captured.ranges.items.len) catch return error.TransactionTooLarge;
        if (observations > max_predicates) return error.TransactionTooLarge;
        for (captured.cursors.items) |program| {
            try checkpoint(control, null);
            if (program.active or program.control != null or program.failure != null) return error.InvalidArgument;
            observations = std.math.add(usize, observations, program.operations.items.len) catch return error.TransactionTooLarge;
            if (observations > max_predicates) return error.TransactionTooLarge;
        }
        const capabilities = store.runtime_store.capabilities();
        if (!capabilities.ordered_ranges or capabilities.read_snapshots != .snapshot) return error.Unsupported;
        var builder: Builder = .{ .arena = std.heap.ArenaAllocator.init(alloc), .control = control };
        errdefer builder.arena.deinit();
        var read = if (control) |owner| try store.beginReadTxnWithAdmission(.retain, owner) else try store.beginReadTxn();
        defer read.abort();
        for (captured.points.items) |point| {
            try checkpoint(control, null);
            const value = read.get(point.key) catch |err| if (err == error.NotFound) null else return err;
            const actual = if (value) |bytes| try Fingerprint.of(bytes, control, null) else null;
            if (!std.meta.eql(actual, point.expected)) return error.PreparedReadSetChanged;
            try builder.add(point.key, true, point.key, true);
        }
        for (captured.cursors.items) |program| {
            if (program.active) return error.InvalidArgument;
            var cursor = try read.openPhysicalCursorAdapter();
            defer cursor.close();
            var position: ?[]const u8 = null;
            var bound: ?[]const u8 = null;
            for (program.operations.items) |operation| {
                try checkpoint(control, null);
                if (!validOperationShape(operation)) return error.InvalidArgument;
                if (operation.kind == .upper_bound or operation.kind == .clear_upper_bound) {
                    cursor.setUpperBound(if (operation.kind == .upper_bound) operation.argument else null);
                    bound = if (capabilities.semantic_cursor_bounds and operation.kind == .upper_bound) operation.argument else null;
                    if (!capabilities.semantic_cursor_bounds) position = null;
                    continue;
                }
                const entry = switch (operation.kind) {
                    .seek_after => try cursor.seekAtOrAfter(operation.argument),
                    .seek_before => try cursor.seekAtOrBefore(operation.argument),
                    .next => try cursor.next(),
                    .prev => try cursor.prev(),
                    .first => try cursor.first(),
                    .last => try cursor.last(),
                    .upper_bound, .clear_upper_bound => unreachable,
                };
                // Stabilize the returned key before point payload resolution or
                // the next cursor move can retire a borrowed source buffer.
                const selected = if (entry) |row| try builder.key(row.key) else null;
                const actual: ?Row = if (entry) |row| .{ .key = try Fingerprint.of(selected.?, control, null), .value = try Fingerprint.of(if (program.physical_payloads) row.value else try read.get(selected.?), control, null) } else null;
                if (!std.meta.eql(actual, operation.expected)) return error.PreparedReadSetChanged;
                if (selected) |key| {
                    if (bound) |end| if (try compare(key, end, control) != .lt) return error.InvalidTxnRecord;
                }
                switch (operation.kind) {
                    .seek_after, .first => {
                        const lower = if (operation.kind == .first) "" else operation.argument;
                        if (selected) |key| if (try compare(key, lower, control) == .lt) return error.InvalidTxnRecord;
                        try builder.add(lower, true, selected orelse bound, selected != null);
                    },
                    .seek_before, .last => {
                        var upper = if (operation.kind == .last) bound else @as(?[]const u8, operation.argument);
                        var inclusive = operation.kind == .seek_before;
                        if (bound) |end| if (upper == null or try compare(upper.?, end, control) != .lt) {
                            upper = end;
                            inclusive = false;
                        };
                        if (selected) |key| if (upper) |end| {
                            const order = try compare(key, end, control);
                            if (order == .gt or (order == .eq and !inclusive)) return error.InvalidTxnRecord;
                        };
                        try builder.add(selected orelse "", true, upper, inclusive);
                    },
                    .next => {
                        if (position) |lower| {
                            if (selected) |key| if (try compare(key, lower, control) != .gt) return error.InvalidTxnRecord;
                            try builder.add(lower, false, selected orelse bound, selected != null);
                        } else if (!capabilities.semantic_cursor_bounds) {
                            try builder.add("", true, null, false);
                        } else if (selected != null) return error.InvalidTxnRecord;
                    },
                    .prev => {
                        if (position) |upper_| {
                            if (selected) |key| if (try compare(key, upper_, control) != .lt) return error.InvalidTxnRecord;
                            var upper: ?[]const u8 = upper_;
                            if (bound) |end| if (try compare(end, upper_, control) == .lt) {
                                upper = end;
                            };
                            try builder.add(selected orelse "", true, upper, false);
                        } else if (!capabilities.semantic_cursor_bounds) {
                            try builder.add("", true, null, false);
                        } else if (selected != null) return error.InvalidTxnRecord;
                    },
                    .upper_bound, .clear_upper_bound => unreachable,
                }
                position = selected;
            }
        }
        for (captured.ranges.items) |*range| {
            try validateRange(range, &read, alloc, control);
            if (!range.keys_only) try builder.add(range.lower, range.lower_inclusive, range.upper, range.upper_inclusive);
        }
        const spans = try builder.normalize();
        // Reuse the arena and the copied-key budget, but sort membership spans
        // independently so value-only writes retain their narrower conflicts.
        builder.spans = .empty;
        for (captured.ranges.items) |range| {
            if (range.keys_only) try builder.add(range.lower, range.lower_inclusive, range.upper, range.upper_inclusive);
        }
        const membership_spans = try builder.normalize();
        try checkpoint(control, null);
        return .{ .arena = builder.arena, .spans = spans, .membership_spans = membership_spans };
    }
};

test "prepared lifetime read observations retire point and cursor metadata one item per turn" {
    const a = std.testing.allocator;
    var owned = Owned.init(a, null);
    var complete = false;
    defer if (!complete) owned.deinit();
    for (0..9) |index| {
        const key = try std.fmt.allocPrint(a, "point-{d}", .{index});
        defer a.free(key);
        try owned.recordPoint(key, "value");
    }
    const cursor = try owned.openCursor();
    for (0..17) |_| try cursor.record(.next, "cursor-argument", null);
    cursor.close();
    try owned.finish();
    var turns: usize = 0;
    var previous: usize = 9 + 17 + 1;
    while (true) {
        const done = owned.deinitStep();
        turns += 1;
        if (done) {
            complete = true;
            break;
        }
        var remaining = owned.points.items.len + owned.cursors.items.len;
        for (owned.cursors.items) |entry| remaining += entry.operations.items.len;
        try std.testing.expectEqual(@as(usize, 1), previous - remaining);
        previous = remaining;
        try std.testing.expectError(error.ReadAdmissionRejected, owned.check());
        try std.testing.expectError(error.InvalidArgument, owned.take());
    }
    try std.testing.expectEqual(@as(usize, 28), turns);
}
