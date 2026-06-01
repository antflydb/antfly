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
const Allocator = std.mem.Allocator;
const backend_erased = @import("../backend_erased.zig");
const docstore_mod = @import("../docstore.zig");
const lsm_backend = @import("../lsm_backend.zig");
const mem_backend = @import("../mem_backend.zig");

const range_key = "\x00\x00__metadata__:range";
const split_delta_final_seq_key = "\x00\x00__metadata__:split_delta_final_seq";

pub fn loadRange(alloc: Allocator, store: anytype) !docstore_mod.ByteRange {
    return try loadRangeAtKey(alloc, store, range_key);
}

pub fn loadRangeAtKey(alloc: Allocator, store: anytype, key: []const u8) !docstore_mod.ByteRange {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{ .start = "", .end = "" },
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    return try decodeRangeAlloc(alloc, raw);
}

pub fn decodeRangeAlloc(alloc: Allocator, raw: []const u8) !docstore_mod.ByteRange {
    if (raw.len < 8) return error.InvalidRangeState;
    var pos: usize = 0;
    const start_len = std.mem.readInt(u32, raw[pos..][0..4], .little);
    pos += 4;
    if (pos + start_len + 4 > raw.len) return error.InvalidRangeState;
    const start = try alloc.dupe(u8, raw[pos .. pos + start_len]);
    pos += start_len;
    errdefer alloc.free(start);

    const end_len = std.mem.readInt(u32, raw[pos..][0..4], .little);
    pos += 4;
    if (pos + end_len != raw.len) return error.InvalidRangeState;
    const end = try alloc.dupe(u8, raw[pos .. pos + end_len]);

    return .{
        .start = start,
        .end = end,
    };
}

pub fn freeRange(alloc: Allocator, byte_range: docstore_mod.ByteRange) void {
    if (byte_range.start.len > 0) alloc.free(@constCast(byte_range.start));
    if (byte_range.end.len > 0) alloc.free(@constCast(byte_range.end));
}

pub fn saveRange(store: anytype, byte_range: docstore_mod.ByteRange) !void {
    try saveRangeAtKey(store, range_key, byte_range);
}

pub fn saveRangeAtKey(store: anytype, key: []const u8, byte_range: docstore_mod.ByteRange) !void {
    var stack_buf: [1024]u8 = undefined;
    const buf = try encodeRange(byte_range, &stack_buf);
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, buf);
    try txn.commit();
}

pub fn encodeRange(byte_range: docstore_mod.ByteRange, stack_buf: []u8) ![]const u8 {
    const total_len = 8 + byte_range.start.len + byte_range.end.len;
    if (total_len > stack_buf.len) return error.RangeTooLarge;
    const buf = stack_buf[0..total_len];

    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(byte_range.start.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..byte_range.start.len], byte_range.start);
    pos += byte_range.start.len;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(byte_range.end.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..byte_range.end.len], byte_range.end);
    return buf;
}

pub fn loadSplitDeltaFinalSeq(alloc: Allocator, store: anytype) !u64 {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(split_delta_final_seq_key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    if (raw.len != 8) return error.InvalidSplitDeltaFinalSeq;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn saveSplitDeltaFinalSeq(store: anytype, seq: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, seq, .little);
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(split_delta_final_seq_key, &buf);
    try txn.commit();
}

pub fn clearSplitDeltaFinalSeq(store: anytype) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    txn.delete(split_delta_final_seq_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    try txn.commit();
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

test "range state saves and loads namespaced ranges" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/range-state", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z.ptr, .{});
    defer store.close();

    try saveRangeAtKey(&store, "group-range:7", .{
        .start = "doc:b",
        .end = "doc:m",
    });

    const loaded = try loadRangeAtKey(std.testing.allocator, &store, "group-range:7");
    defer freeRange(std.testing.allocator, loaded);

    try std.testing.expectEqualStrings("doc:b", loaded.start);
    try std.testing.expectEqualStrings("doc:m", loaded.end);
}

test "range state returns empty range for missing namespaced key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/range-state-empty", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z.ptr, .{});
    defer store.close();

    const loaded = try loadRangeAtKey(std.testing.allocator, &store, "group-range:missing");
    defer freeRange(std.testing.allocator, loaded);

    try std.testing.expectEqualStrings("", loaded.start);
    try std.testing.expectEqualStrings("", loaded.end);
}

test "range state saves and loads via memory backend store" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveRangeAtKey(runtime, "group-range:9", .{
        .start = "doc:c",
        .end = "doc:q",
    });

    const loaded = try loadRangeAtKey(std.testing.allocator, runtime, "group-range:9");
    defer freeRange(std.testing.allocator, loaded);

    try std.testing.expectEqualStrings("doc:c", loaded.start);
    try std.testing.expectEqualStrings("doc:q", loaded.end);

    try saveSplitDeltaFinalSeq(runtime, 17);
    try std.testing.expectEqual(@as(u64, 17), try loadSplitDeltaFinalSeq(std.testing.allocator, runtime));
    try clearSplitDeltaFinalSeq(runtime);
    try std.testing.expectEqual(@as(u64, 0), try loadSplitDeltaFinalSeq(std.testing.allocator, runtime));
}

test "range state saves and loads via lsm backend store" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveRangeAtKey(runtime, "group-range:10", .{
        .start = "doc:d",
        .end = "doc:r",
    });

    const loaded = try loadRangeAtKey(std.testing.allocator, runtime, "group-range:10");
    defer freeRange(std.testing.allocator, loaded);

    try std.testing.expectEqualStrings("doc:d", loaded.start);
    try std.testing.expectEqualStrings("doc:r", loaded.end);

    try saveSplitDeltaFinalSeq(runtime, 21);
    try std.testing.expectEqual(@as(u64, 21), try loadSplitDeltaFinalSeq(std.testing.allocator, runtime));
    try clearSplitDeltaFinalSeq(runtime);
    try std.testing.expectEqual(@as(u64, 0), try loadSplitDeltaFinalSeq(std.testing.allocator, runtime));
}

test "range state lsm point loads do not clone mutable snapshot" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveRangeAtKey(runtime, "group-range:11", .{
        .start = "doc:e",
        .end = "doc:s",
    });
    try saveSplitDeltaFinalSeq(runtime, 34);

    const before = backend.snapshotMaintenanceStats();
    const loaded = try loadRangeAtKey(std.testing.allocator, runtime, "group-range:11");
    defer freeRange(std.testing.allocator, loaded);
    try std.testing.expectEqualStrings("doc:e", loaded.start);
    try std.testing.expectEqualStrings("doc:s", loaded.end);
    try std.testing.expectEqual(@as(u64, 34), try loadSplitDeltaFinalSeq(std.testing.allocator, runtime));
    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}
