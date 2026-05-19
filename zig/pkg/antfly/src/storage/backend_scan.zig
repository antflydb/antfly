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

pub const ScanOptions = struct {
    skip_fn: ?*const fn (key: []const u8) bool = null,
};

pub const ScanAction = enum { @"continue", stop };

pub fn freeResults(alloc: std.mem.Allocator, results: []OwnedKVPair) void {
    for (results) |item| {
        alloc.free(item.key);
        alloc.free(item.value);
    }
    alloc.free(results);
}

pub fn scan(
    store: *backend_erased.Store,
    lower: []const u8,
    upper: []const u8,
    options: ScanOptions,
    callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,
) !void {
    var txn = try store.beginRead();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();
    cur.setUpperBound(if (upper.len > 0) upper else null);

    const first = if (lower.len == 0)
        (try cur.first()) orelse return
    else
        (try cur.seekAtOrAfter(lower)) orelse return;

    if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) return;

    if (options.skip_fn == null or !options.skip_fn.?(first.key)) {
        if (try callback(first.key, first.value) == .stop) return;
    }

    var entry = try cur.next();
    while (entry) |kv| : (entry = try cur.next()) {
        if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
        if (options.skip_fn) |skip| {
            if (skip(kv.key)) continue;
        }
        if (try callback(kv.key, kv.value) == .stop) return;
    }
}

pub fn scanPrefix(alloc: std.mem.Allocator, store: *backend_erased.Store, prefix: []const u8) ![]OwnedKVPair {
    var txn = try store.beginRead();
    defer txn.abort();

    var cur = try txn.openCursor();
    defer cur.close();

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
