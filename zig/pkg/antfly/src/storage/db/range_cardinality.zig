// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license

const std = @import("std");
const Allocator = std.mem.Allocator;
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const types = @import("types.zig");

pub fn decode(raw: []const u8) !u64 {
    if (raw.len != 8) return error.InvalidRangeDocumentCount;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn load(alloc: Allocator, store: *docstore_mod.DocStore) !?u64 {
    const raw = store.get(alloc, &internal_keys.range_document_count_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return try decode(raw);
}

pub fn countPrimaryDocuments(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    byte_range: types.ByteRange,
) !u64 {
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, byte_range.start);
    defer alloc.free(lower);
    const upper = if (byte_range.end.len == 0)
        null
    else
        try internal_keys.documentRangeUpperAlloc(alloc, byte_range.end);
    defer if (upper) |key| alloc.free(key);

    const CountState = struct {
        count: u64 = 0,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            if (internal_keys.isPrimaryDocumentKey(key)) state.count = std.math.add(u64, state.count, 1) catch
                return error.RangeDocumentCountOverflow;
            return .@"continue";
        }
    };

    var state = CountState{};
    try store.scanWithContext(lower, if (upper) |key| key else "", .{}, &state, CountState.scanEntry);
    return state.count;
}

pub fn loadOrCount(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    byte_range: types.ByteRange,
) !u64 {
    return (try load(alloc, store)) orelse try countPrimaryDocuments(alloc, store, byte_range);
}

/// Appends the range-local cardinality transition to the caller's atomic
/// primary batch. Identity visibility is namespace-wide by design, but its
/// live-count delta is local because range ownership is validated before the
/// primary mutation is admitted.
pub fn appendIdentityTransitionAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    byte_range: types.ByteRange,
    before_live: u64,
    after_live: u64,
    out: *std.ArrayListUnmanaged(docstore_mod.KVPair),
) !void {
    const persisted = try load(alloc, store);
    if (before_live == after_live and persisted != null) return;
    const current = persisted orelse try countPrimaryDocuments(alloc, store, byte_range);
    const next = if (after_live > before_live)
        std.math.add(u64, current, after_live - before_live) catch return error.RangeDocumentCountOverflow
    else blk: {
        const removed = before_live - after_live;
        if (removed > current) return error.InvalidRangeDocumentCount;
        break :blk current - removed;
    };
    const key = try alloc.dupe(u8, &internal_keys.range_document_count_key);
    errdefer alloc.free(key);
    const value = try alloc.alloc(u8, 8);
    errdefer alloc.free(value);
    std.mem.writeInt(u64, value[0..8], next, .little);
    try out.append(alloc, .{
        .key = key,
        .value = value,
    });
}
