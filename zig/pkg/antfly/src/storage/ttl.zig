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

//! Document-level TTL with query-time filtering and background cleanup.
//!
//! Matches Go antfly's TTL system:
//!   - Timestamp stored as a structured internal TTL key (8-byte u64 LE, unix nanoseconds)
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
const lmdb = @import("lmdb.zig");
const platform_time = @import("../platform/time.zig");

pub const TtlConfig = struct {
    /// TTL duration in nanoseconds.
    duration_ns: u64,
    /// Grace period for cleanup (prevents deleting docs not yet replicated).
    /// Default: 5 seconds. Query-time filtering does NOT use the grace period.
    grace_period_ns: u64 = 5_000_000_000,
};

// ============================================================================
// Timestamp read/write
// ============================================================================

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
        lmdb.Error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(val);
    if (val.len < 8) return null;
    return std.mem.readInt(u64, val[0..8], .little);
}

// ============================================================================
// Expiration checks
// ============================================================================

/// Check if a document is expired (for query-time filtering, no grace period).
pub fn isExpired(timestamp_ns: u64, duration_ns: u64, now_ns: u64) bool {
    return now_ns > timestamp_ns +| duration_ns;
}

/// Check if a document is expired with grace period (for background cleanup).
pub fn isExpiredWithGrace(timestamp_ns: u64, duration_ns: u64, grace_ns: u64, now_ns: u64) bool {
    return now_ns > (timestamp_ns +| duration_ns) +| grace_ns;
}

/// Convenience: check if a document key is expired by reading its timestamp.
/// Returns true if expired, false if not expired or no timestamp exists.
pub fn isDocExpired(store: *DocStore, alloc: Allocator, key: []const u8, config: TtlConfig, now_ns: u64) !bool {
    const ts = (try readTimestamp(store, alloc, key)) orelse return false;
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
            if (value.len < 8) return .@"continue";

            const ts = std.mem.readInt(u64, value[0..8], .little);
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
        lmdb.Error.NotFound => null,
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
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, doc_key));
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
