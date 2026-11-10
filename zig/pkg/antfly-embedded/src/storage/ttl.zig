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

//! Document-level TTL with query-time filtering and background cleanup.
//!
//! Matches Go antfly's TTL system:
//!   - Timestamp metadata stores a u64 write version plus an optional signed i128 TTL source
//!   - Query-time filtering: isExpired() for immediate skip
//!   - Background cleanup: cleanupExpired() with grace period for replication safety
//!
//! Go reference:
//!   src/store/db/ttl.go — TTLCleaner
//!   src/store/db/db.go — isDocumentExpiredByTimestamp

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const docstore = @import("docstore.zig");
const DocStore = docstore.DocStore;
const internal_keys = @import("internal_keys.zig");
const platform_time = @import("antfly_platform").time;

pub const TtlConfig = struct {
    /// TTL duration in nanoseconds.
    duration_ns: u64,
    /// Wall-clock grace before cleanup eligibility; not a replication fence.
    /// Default: 5 seconds. Query-time filtering does NOT use the grace period.
    grace_period_ns: u64 = 5_000_000_000,
};

// ============================================================================
// Timestamp read/write
// ============================================================================

/// Legacy records contain an unsigned write version. Extended records preserve
/// that version in the first eight bytes and append the signed TTL source instant.
/// Keeping the two separate avoids turning historical event time into version zero.
pub const TimestampMetadata = struct { version: u64, source: i128 };
pub const document_version_watermark_key = "\x00\x00__metadata__:document_version_watermark_v1";

/// Called under the primary apply fence. One sorted metadata read seeds a
/// durable batch revision from both the allocator and legacy rows being changed.
/// Deletions advance the watermark too, preventing delete/reinsert ABA.
pub fn nextDocumentVersion(store: anytype, a: Allocator, wall: u64, writes: anytype, deletes: []const []const u8) !?u64 {
    var keys = std.ArrayList([]const u8).empty;
    defer {
        for (keys.items) |key| if (!std.mem.eql(u8, key, document_version_watermark_key)) a.free(key);
        keys.deinit(a);
    }
    try keys.append(a, document_version_watermark_key);
    for (writes) |write| if (!internal_keys.isInternalUserKey(write.key) and !std.mem.startsWith(u8, write.key, "\x00\x00__metadata__:") and !std.mem.startsWith(u8, write.key, "splitstate:") and !std.mem.startsWith(u8, write.key, "splitdelta:")) {
        const key = try internal_keys.ttlKeyAlloc(a, write.key);
        errdefer a.free(key);
        try keys.append(a, key);
    };
    for (deletes) |id| if (!internal_keys.isInternalUserKey(id) and !std.mem.startsWith(u8, id, "\x00\x00__metadata__:") and !std.mem.startsWith(u8, id, "splitstate:") and !std.mem.startsWith(u8, id, "splitdelta:")) {
        const key = try internal_keys.ttlKeyAlloc(a, id);
        errdefer a.free(key);
        try keys.append(a, key);
    };
    if (keys.items.len == 1) return null;
    std.mem.sort([]const u8, keys.items, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.order(u8, left, right) == .lt;
        }
    }.less);
    var unique = std.ArrayList([]const u8).empty;
    defer unique.deinit(a);
    for (keys.items) |key| if (unique.items.len == 0 or !std.mem.eql(u8, unique.items[unique.items.len - 1], key)) try unique.append(a, key);
    const values = try a.alloc(?[]const u8, unique.items.len);
    defer a.free(values);
    var txn = if (comptime @TypeOf(store) == *DocStore) try store.beginProbeTxn() else blk: {
        var backend = store;
        break :blk try backend.beginProbe();
    };
    defer txn.abort();
    try txn.getManySorted(unique.items, values);
    var previous: u64 = 0;
    for (unique.items, values) |key, value| if (value) |bytes| {
        const version = if (std.mem.eql(u8, key, document_version_watermark_key)) blk: {
            if (bytes.len != 8) return error.InvalidTimestamp;
            break :blk std.mem.readInt(u64, bytes[0..8], .little);
        } else writeVersion(bytes) orelse return error.InvalidTimestamp;
        previous = @max(previous, version);
    };
    return @max(wall, std.math.add(u64, previous, 1) catch return error.InvalidTimestamp);
}

/// The only decoder for primary timestamp sidecars. Version fences must use
/// `version`; expiration must use `source`, including epoch and negative instants.
pub fn decodeTimestamp(value: []const u8) ?TimestampMetadata {
    if (value.len != 8 and value.len != 24) return null;
    const version = std.mem.readInt(u64, value[0..8], .little);
    return .{ .version = version, .source = if (value.len == 24) std.mem.readInt(i128, value[8..24], .little) else @as(i128, version) };
}

pub fn writeVersion(value: []const u8) ?u64 {
    return (decodeTimestamp(value) orelse return null).version;
}

pub fn sourceTimestamp(value: []const u8) ?i128 {
    return (decodeTimestamp(value) orelse return null).source;
}

pub fn encodeTimestampAlloc(alloc: Allocator, version: u64, source: i128) ![]u8 {
    const extended = source != @as(i128, version);
    const bytes = try alloc.alloc(u8, if (extended) 24 else 8);
    std.mem.writeInt(u64, bytes[0..8], version, .little);
    if (extended) std.mem.writeInt(i128, bytes[8..24], source, .little);
    return bytes;
}

pub fn readSourceTimestamp(store: *DocStore, alloc: Allocator, key: []const u8) !?i128 {
    const ts_key = try internal_keys.ttlKeyAlloc(alloc, key);
    defer alloc.free(ts_key);
    const val = store.get(alloc, ts_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(val);
    return sourceTimestamp(val);
}

/// The shared admission and persistence parser retains the full signed instant.
pub fn parseSourceTimestamp(value: std.json.Value) !i128 {
    return switch (value) {
        .integer => |v| v,
        .number_string => |text| std.fmt.parseInt(i128, std.mem.trim(u8, text, " \t\r\n"), 10) catch return error.InvalidTimestamp,
        .string => |text| blk: {
            const trimmed = std.mem.trim(u8, text, " \t\r\n");
            if (std.fmt.parseInt(i128, trimmed, 10)) |v| break :blk v else |_| {}
            break :blk @import("../datetime.zig").parseRfc3339ToSignedNs(trimmed) orelse return error.InvalidTimestamp;
        },
        else => error.InvalidTimestamp,
    };
}

/// Write a timestamp for a document key using the structured internal TTL key.
pub fn writeTimestamp(store: *DocStore, key: []const u8, timestamp_ns: u64) !void {
    const ts_key = try internal_keys.ttlKeyAlloc(store.alloc, key);
    defer store.alloc.free(ts_key);
    var val_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &val_buf, timestamp_ns, .little);
    try store.put(ts_key, &val_buf);
}

/// Read a timestamp for a document key. Returns null if no timestamp exists.
pub fn readTimestamp(store: *DocStore, alloc: Allocator, key: []const u8) !?u64 {
    const ts_key = try internal_keys.ttlKeyAlloc(alloc, key);
    defer alloc.free(ts_key);
    const val = store.get(alloc, ts_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(val);
    return writeVersion(val);
}

// ============================================================================
// Expiration checks
// ============================================================================

/// Check if a document is expired (for query-time filtering, no grace period).
pub fn isExpired(timestamp_ns: i128, duration_ns: u64, now_ns: u64) bool {
    return now_ns > timestamp_ns +| duration_ns;
}

/// Check if a document is expired with grace period (for background cleanup).
pub fn isExpiredWithGrace(timestamp_ns: i128, duration_ns: u64, grace_ns: u64, now_ns: u64) bool {
    return now_ns > (timestamp_ns +| duration_ns) +| grace_ns;
}

/// Convenience: check if a document key is expired by reading its timestamp.
/// Returns true if expired, false if not expired or no timestamp exists.
pub fn isDocExpired(store: *DocStore, alloc: Allocator, key: []const u8, config: TtlConfig, now_ns: u64) !bool {
    const ts = (try readSourceTimestamp(store, alloc, key)) orelse return false;
    return isExpired(ts, config.duration_ns, now_ns);
}

// ============================================================================
// Background cleanup
// ============================================================================

/// Scan structured internal TTL keys in the store, collect expired ones, delete in batches.
/// Returns the number of documents deleted.
pub fn cleanupExpired(
    alloc: Allocator,
    store: *DocStore,
    config: TtlConfig,
    now_ns: u64,
    batch_size: u32,
) !u32 {
    // Scan all keys to find timestamp keys
    var expired_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (expired_keys.items) |k| alloc.free(k);
        expired_keys.deinit(alloc);
    }

    // Use a cleanup scan state with threadlocal for the callback
    const CleanupState = struct {
        expired: *std.ArrayListUnmanaged([]u8),
        the_alloc: Allocator,
        the_config: TtlConfig,
        the_now_ns: u64,

        threadlocal var active: ?*@This() = null;

        fn cb(key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
            const self = active.?;
            if (!internal_keys.isTtlKey(key)) return .@"continue";
            const ts = sourceTimestamp(value) orelse return .@"continue";
            if (isExpiredWithGrace(ts, self.the_config.duration_ns, self.the_config.grace_period_ns, self.the_now_ns)) {
                const base_key = (try internal_keys.decodeDocumentComponentAlloc(self.the_alloc, key)) orelse return .@"continue";
                errdefer self.the_alloc.free(base_key);
                try self.expired.append(self.the_alloc, base_key);
            }
            return .@"continue";
        }
    };

    var state = CleanupState{
        .expired = &expired_keys,
        .the_alloc = alloc,
        .the_config = config,
        .the_now_ns = now_ns,
    };
    CleanupState.active = &state;
    defer CleanupState.active = null;

    var runtime = try backend_erased.storeFrom(alloc, store.backendStore());
    defer runtime.deinit();
    try backend_scan.scan(&runtime, "", "", .{}, &CleanupState.cb);

    // Delete in batches
    var deleted: u32 = 0;
    var i: usize = 0;
    while (i < expired_keys.items.len) {
        const end = @min(i + batch_size, expired_keys.items.len);
        for (expired_keys.items[i..end]) |base_key| {
            const primary_key = try internal_keys.documentKeyAlloc(alloc, base_key);
            defer alloc.free(primary_key);
            const ts_key = try internal_keys.ttlKeyAlloc(alloc, base_key);
            defer alloc.free(ts_key);

            store.delete(primary_key) catch {};
            store.delete(ts_key) catch {};
            deleted += 1;
        }
        i = end;
    }

    return deleted;
}

// ============================================================================
// Tests
// ============================================================================

var tmp_path_nonce: u64 = 0;

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &tmp_path_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-ttl-{s}-{d}-{d}\x00", .{ label, ns, nonce }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "writeTimestamp and readTimestamp round-trip" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "ts1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    const ts: u64 = 1_700_000_000_000_000_000; // ~2023 in nanoseconds
    try writeTimestamp(&store, "doc1", ts);

    const read_ts = try readTimestamp(&store, alloc, "doc1");
    try std.testing.expect(read_ts != null);
    try std.testing.expectEqual(ts, read_ts.?);

    // Non-existent key returns null
    const missing = try readTimestamp(&store, alloc, "doc_missing");
    try std.testing.expect(missing == null);
}

test "isExpired returns true after duration" {
    const ts: u64 = 1_000_000_000; // 1 second in ns
    const duration: u64 = 5_000_000_000; // 5 seconds
    const now_expired: u64 = 7_000_000_000; // 7 seconds — past expiration

    try std.testing.expect(isExpired(ts, duration, now_expired));
}

test "isExpired returns false before duration" {
    const ts: u64 = 1_000_000_000;
    const duration: u64 = 5_000_000_000;
    const now_valid: u64 = 4_000_000_000; // 4 seconds — within TTL

    try std.testing.expect(!isExpired(ts, duration, now_valid));
}

test "cleanupExpired deletes expired, preserves unexpired" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "cl1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // doc1: expired (ts=1s, TTL=5s, now=20s, grace=5s → expired at 11s)
    const doc1_key = try internal_keys.documentKeyAlloc(alloc, "doc1");
    defer alloc.free(doc1_key);
    try store.put(doc1_key, "content1");
    try writeTimestamp(&store, "doc1", 1_000_000_000);

    // doc2: not expired (ts=15s, TTL=5s, now=20s, grace=5s → expires at 25s)
    const doc2_key = try internal_keys.documentKeyAlloc(alloc, "doc2");
    defer alloc.free(doc2_key);
    try store.put(doc2_key, "content2");
    try writeTimestamp(&store, "doc2", 15_000_000_000);

    const config = TtlConfig{
        .duration_ns = 5_000_000_000,
        .grace_period_ns = 5_000_000_000,
    };

    const deleted = try cleanupExpired(alloc, &store, config, 20_000_000_000, 100);
    try std.testing.expectEqual(@as(u32, 1), deleted);

    // doc1 should be gone
    const doc1 = store.get(alloc, doc1_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    try std.testing.expect(doc1 == null);

    // doc2 should still exist
    const doc2 = try store.get(alloc, doc2_key);
    defer alloc.free(doc2);
    try std.testing.expectEqualStrings("content2", doc2);
}

test "ttl timestamp keys support arbitrary document ids" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "binary-id");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    const raw = "doc\x00:t\xff";
    const doc_key = try internal_keys.documentKeyAlloc(alloc, raw);
    defer alloc.free(doc_key);
    try store.put(doc_key, "content");
    try writeTimestamp(&store, raw, 1_000_000_000);

    try std.testing.expectEqual(@as(?u64, 1_000_000_000), try readTimestamp(&store, alloc, raw));
    try std.testing.expect(try isDocExpired(&store, alloc, raw, .{
        .duration_ns = 5_000_000_000,
        .grace_period_ns = 0,
    }, 7_000_000_000));

    const deleted = try cleanupExpired(alloc, &store, .{
        .duration_ns = 5_000_000_000,
        .grace_period_ns = 0,
    }, 7_000_000_000, 100);
    try std.testing.expectEqual(@as(u32, 1), deleted);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_key));
}

test "grace period prevents premature cleanup" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "gp1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // doc: ts=1s, TTL=5s → expires at 6s, but grace=5s → cleanup at 11s
    const doc1_key = try internal_keys.documentKeyAlloc(alloc, "doc1");
    defer alloc.free(doc1_key);
    try store.put(doc1_key, "content");
    try writeTimestamp(&store, "doc1", 1_000_000_000);

    const config = TtlConfig{
        .duration_ns = 5_000_000_000,
        .grace_period_ns = 5_000_000_000,
    };

    // At now=8s: expired by TTL (6s) but within grace period (11s)
    // isExpired says true (no grace), cleanupExpired should NOT delete (uses grace)
    try std.testing.expect(isExpired(1_000_000_000, 5_000_000_000, 8_000_000_000));

    const deleted = try cleanupExpired(alloc, &store, config, 8_000_000_000, 100);
    try std.testing.expectEqual(@as(u32, 0), deleted);

    // Doc should still exist
    const doc = try store.get(alloc, doc1_key);
    defer alloc.free(doc);
    try std.testing.expectEqualStrings("content", doc);
}

test "time series TTL signed source records retain versions and legacy compatibility" {
    const a = std.testing.allocator;
    const legacy = try encodeTimestampAlloc(a, 123, 123);
    defer a.free(legacy);
    try std.testing.expectEqual(@as(usize, 8), legacy.len);
    try std.testing.expectEqual(@as(?i128, 123), sourceTimestamp(legacy));
    const historical = try encodeTimestampAlloc(a, 456, -1_000_000_000);
    defer a.free(historical);
    try std.testing.expectEqual(@as(usize, 24), historical.len);
    try std.testing.expectEqual(@as(u64, 456), std.mem.readInt(u64, historical[0..8], .little));
    try std.testing.expectEqual(@as(?i128, -1_000_000_000), sourceTimestamp(historical));
    try std.testing.expectEqual(@as(?u64, 456), writeVersion(historical));
    for ([_]usize{ 0, 7, 9, 16, 23 }) |length| try std.testing.expect(decodeTimestamp(historical[0..length]) == null);
    try std.testing.expect(!isExpired(-1_000_000_000, 2_000_000_000, 1_000_000_000));
    try std.testing.expect(isExpired(-1_000_000_000, 2_000_000_000, 1_000_000_001));
    try std.testing.expect(!isExpiredWithGrace(-1_000_000_000, 2_000_000_000, 5, 1_000_000_005));
    try std.testing.expect(isExpiredWithGrace(-1_000_000_000, 2_000_000_000, 5, 1_000_000_006));
    try std.testing.expect(!isExpired(std.math.maxInt(i128), std.math.maxInt(u64), std.math.maxInt(u64)));
    try std.testing.expect(isExpired(std.math.minInt(i128), std.math.maxInt(u64), 0));
    try std.testing.expectEqual(@as(?i128, null), sourceTimestamp(historical[0..16]));
}
