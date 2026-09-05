// Copyright 2026 Antfly, Inc.
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
const backend_erased = @import("backend_erased.zig");

pub const OwnedKVPair = struct {
    key: []u8,
    value: []u8,
};

pub const RangePage = struct {
    items: []OwnedKVPair,
    /// False when an item/byte budget stopped the cursor. A following page may
    /// be empty when the bound landed exactly on the range end.
    reached_end: bool,

    pub fn deinit(self: *RangePage, alloc: std.mem.Allocator) void {
        freeResults(alloc, self.items);
        self.* = undefined;
    }
};

pub const ScanOptions = struct {
    skip_fn: ?*const fn (key: []const u8) bool = null,
    reverse: bool = false,
};

pub const ScanAction = enum { @"continue", stop };

pub const ScanWithContextCallback = *const fn (
    ctx: ?*anyopaque,
    key: []const u8,
    value: []const u8,
) anyerror!ScanAction;

pub fn freeResults(alloc: std.mem.Allocator, results: []OwnedKVPair) void {
    for (results) |item| {
        alloc.free(item.key);
        alloc.free(item.value);
    }
    alloc.free(results);
}

fn appendOwnedPair(
    alloc: std.mem.Allocator,
    results: *std.ArrayListUnmanaged(OwnedKVPair),
    key: []const u8,
    value: []const u8,
) !void {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try results.append(alloc, .{ .key = owned_key, .value = owned_value });
}

pub fn scan(
    store: *backend_erased.Store,
    lower: []const u8,
    upper: []const u8,
    options: ScanOptions,
    callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,
) !void {
    const Adapter = struct {
        callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,

        fn run(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!ScanAction {
            const adapter: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try adapter.callback(key, value);
        }
    };
    var adapter = Adapter{ .callback = callback };
    return try scanWithContext(store, lower, upper, options, &adapter, Adapter.run);
}

pub fn scanCurrent(
    store: *backend_erased.Store,
    lower: []const u8,
    upper: []const u8,
    options: ScanOptions,
    callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,
) !void {
    const Adapter = struct {
        callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,

        fn run(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!ScanAction {
            const adapter: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try adapter.callback(key, value);
        }
    };
    var adapter = Adapter{ .callback = callback };
    return try scanCurrentWithContext(store, lower, upper, options, &adapter, Adapter.run);
}

pub fn scanWithContext(
    store: *backend_erased.Store,
    lower: []const u8,
    upper: []const u8,
    options: ScanOptions,
    ctx: ?*anyopaque,
    callback: ScanWithContextCallback,
) !void {
    var txn = try store.beginRead();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();
    try scanCursorWithContext(&cur, lower, upper, options, ctx, callback);
}

pub fn scanCurrentWithContext(
    store: *backend_erased.Store,
    lower: []const u8,
    upper: []const u8,
    options: ScanOptions,
    ctx: ?*anyopaque,
    callback: ScanWithContextCallback,
) !void {
    var txn = try store.beginCurrentScan();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();
    try scanCursorWithContext(&cur, lower, upper, options, ctx, callback);
}

fn scanCursor(
    cur: *backend_erased.Cursor,
    lower: []const u8,
    upper: []const u8,
    options: ScanOptions,
    callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,
) !void {
    const Adapter = struct {
        callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,

        fn run(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!ScanAction {
            const adapter: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try adapter.callback(key, value);
        }
    };
    var adapter = Adapter{ .callback = callback };
    try scanCursorWithContext(cur, lower, upper, options, &adapter, Adapter.run);
}

fn scanCursorWithContext(
    cur: *backend_erased.Cursor,
    lower: []const u8,
    upper: []const u8,
    options: ScanOptions,
    ctx: ?*anyopaque,
    callback: ScanWithContextCallback,
) !void {
    if (!options.reverse) {
        cur.setUpperBound(if (upper.len > 0) upper else null);

        const first = if (lower.len == 0)
            (try cur.first()) orelse return
        else
            (try cur.seekAtOrAfter(lower)) orelse return;

        if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) return;

        if (options.skip_fn == null or !options.skip_fn.?(first.key)) {
            if (try callback(ctx, first.key, first.value) == .stop) return;
        }

        var entry = try cur.next();
        while (entry) |kv| : (entry = try cur.next()) {
            if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
            if (options.skip_fn) |skip| {
                if (skip(kv.key)) continue;
            }
            if (try callback(ctx, kv.key, kv.value) == .stop) return;
        }
        return;
    }

    var entry = if (upper.len == 0)
        try cur.last()
    else
        try cur.seekAtOrBefore(upper);
    while (entry) |kv| {
        if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) {
            entry = try cur.prev();
            continue;
        }
        if (lower.len > 0 and std.mem.order(u8, kv.key, lower) == .lt) break;
        if (options.skip_fn) |skip| {
            if (skip(kv.key)) {
                entry = try cur.prev();
                continue;
            }
        }
        if (try callback(ctx, kv.key, kv.value) == .stop) return;
        entry = try cur.prev();
    }
}

pub fn scanPrefixCurrent(alloc: std.mem.Allocator, store: *backend_erased.Store, prefix: []const u8) ![]OwnedKVPair {
    var txn = try store.beginCurrentScan();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();

    return try scanPrefixCursor(alloc, &cur, prefix);
}

pub fn scanPrefix(alloc: std.mem.Allocator, store: *backend_erased.Store, prefix: []const u8) ![]OwnedKVPair {
    var txn = try store.beginRead();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();

    return try scanPrefixCursor(alloc, &cur, prefix);
}

fn scanPrefixCursor(alloc: std.mem.Allocator, cur: *backend_erased.Cursor, prefix: []const u8) ![]OwnedKVPair {
    var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
    errdefer {
        for (results.items) |item| {
            alloc.free(item.key);
            alloc.free(item.value);
        }
        results.deinit(alloc);
    }

    var entry = try cur.seekAtOrAfter(prefix);
    while (entry) |kv| {
        if (!std.mem.startsWith(u8, kv.key, prefix)) break;
        try results.append(alloc, .{
            .key = try alloc.dupe(u8, kv.key),
            .value = try alloc.dupe(u8, kv.value),
        });
        entry = try cur.next();
    }

    return try results.toOwnedSlice(alloc);
}

pub fn scanRangeCurrent(
    alloc: std.mem.Allocator,
    store: *backend_erased.Store,
    lower: []const u8,
    upper: []const u8,
) ![]OwnedKVPair {
    var txn = try store.beginCurrentScan();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();

    return try scanRangeCursor(alloc, &cur, lower, upper);
}

pub fn scanRange(
    alloc: std.mem.Allocator,
    store: *backend_erased.Store,
    lower: []const u8,
    upper: []const u8,
) ![]OwnedKVPair {
    var txn = try store.beginRead();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();

    return try scanRangeCursor(alloc, &cur, lower, upper);
}

/// Copy one bounded range page from a fresh snapshot. `lower_exclusive` makes
/// the last key of the previous page a stable resume cursor without inventing
/// a byte successor. At least one entry is admitted even when it exceeds the
/// byte target, ensuring forward progress for unusually large rows.
pub fn scanRangePage(
    alloc: std.mem.Allocator,
    store: *backend_erased.Store,
    lower: []const u8,
    lower_exclusive: bool,
    upper: []const u8,
    max_items: usize,
    max_bytes: usize,
) !RangePage {
    if (max_items == 0 or max_bytes == 0) return error.InvalidArgument;
    var txn = try store.beginRead();
    defer txn.abort();
    var cur = try txn.openCursor();
    defer cur.close();
    cur.setUpperBound(if (upper.len > 0) upper else null);

    var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
    errdefer {
        for (results.items) |item| {
            alloc.free(item.key);
            alloc.free(item.value);
        }
        results.deinit(alloc);
    }
    var copied_bytes: usize = 0;
    var entry = if (lower.len == 0) try cur.first() else try cur.seekAtOrAfter(lower);
    if (lower_exclusive) if (entry) |kv| {
        if (std.mem.eql(u8, kv.key, lower)) entry = try cur.next();
    };
    while (entry) |kv| {
        if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt)
            return .{ .items = try results.toOwnedSlice(alloc), .reached_end = true };
        const item_bytes = std.math.add(usize, kv.key.len, kv.value.len) catch return error.OutOfMemory;
        if (results.items.len > 0 and
            (results.items.len >= max_items or copied_bytes > max_bytes -| item_bytes))
            return .{ .items = try results.toOwnedSlice(alloc), .reached_end = false };
        try appendOwnedPair(alloc, &results, kv.key, kv.value);
        copied_bytes = std.math.add(usize, copied_bytes, item_bytes) catch std.math.maxInt(usize);
        if (results.items.len >= max_items or copied_bytes >= max_bytes)
            return .{ .items = try results.toOwnedSlice(alloc), .reached_end = false };
        entry = try cur.next();
    }
    return .{ .items = try results.toOwnedSlice(alloc), .reached_end = true };
}

fn scanRangeCursor(
    alloc: std.mem.Allocator,
    cur: *backend_erased.Cursor,
    lower: []const u8,
    upper: []const u8,
) ![]OwnedKVPair {
    cur.setUpperBound(if (upper.len > 0) upper else null);

    var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
    errdefer {
        for (results.items) |item| {
            alloc.free(item.key);
            alloc.free(item.value);
        }
        results.deinit(alloc);
    }

    var entry = if (lower.len == 0) try cur.first() else try cur.seekAtOrAfter(lower);
    while (entry) |kv| {
        if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
        try results.append(alloc, .{
            .key = try alloc.dupe(u8, kv.key),
            .value = try alloc.dupe(u8, kv.value),
        });
        entry = try cur.next();
    }

    return try results.toOwnedSlice(alloc);
}
