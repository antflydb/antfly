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

//! Lake transactions over the shared indexed object WAL. This adapter owns CDC
//! admission policy, not a separate append/receipt/retention storage protocol.
const std = @import("std");
const objectstore = @import("objectstore");
const catalog = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog;
const ObjectWal = @import("wal/object_store.zig").ObjectStore;
const IndexedWal = @import("wal/indexed_object_store.zig").IndexedObjectStore;
const A = std.mem.Allocator;
const namespace = "lake-transactions";
pub const max_pending = 64;
pub const Record = struct { lsn: u64, payload: []const u8, operation_id: []const u8, previous: ?[]const u8 = null };
pub const Head = struct { lsn: u64, key: []const u8, captured: IndexedWal.CapturedHead };
pub const Range = struct { lsn: u64, records: []const Record };
pub const Store = struct {
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context = .{},

    pub fn open(self: *const Store, a: A, initialize: bool) !ObjectWal {
        try self.context.ensureActive();
        var client = self.client;
        client.allocator = a;
        const bucket = try a.dupe(u8, self.bucket);
        errdefer a.free(bucket);
        var wal: ObjectWal = .{ .alloc = a, .client = client, .owns_client = false, .bucket = bucket, .prefix = try a.dupe(u8, self.prefix), .cancellation = catalog.types.contextCancellation(&self.context) };
        errdefer wal.deinit();
        // Unmarked, nonempty prefixes fail closed. Pre-release lake state must
        // be exported/rebuilt into a fresh incarnation, never silently hidden.
        try wal.enableIndexed(.{ .read_state = true, .packed_pages = true, .managed_collection = true, .initialize = initialize });
        try self.context.ensureActive();
        return wal;
    }
    pub fn collectionNamespace(_: Store) []const u8 {
        return namespace;
    }
    pub fn watermark(self: Store, a: A) !u64 {
        var wal = try self.open(a, false);
        defer wal.deinit();
        return wal.indexed.?.latestLsn(namespace);
    }
    pub fn latest(self: Store, a: A) !?Record {
        var wal = try self.open(a, false);
        defer wal.deinit();
        const cut = try wal.indexed.?.captureRead(namespace);
        if (cut.snapshot.latest_lsn < cut.snapshot.first_lsn) return null;
        const rows = try wal.indexed.?.readCapturedRangeAlloc(a, namespace, cut, .{ .start_lsn = cut.snapshot.latest_lsn, .end_lsn = cut.snapshot.latest_lsn, .max_records = 1, .max_bytes = 32 * 1024 * 1024, .cancellation = catalog.types.contextCancellation(&self.context) });
        const records = try adaptRows(a, rows);
        defer a.free(records);
        return records[0];
    }
    fn adapt(row: @import("wal/types.zig").Record) !Record {
        return .{ .lsn = row.lsn, .payload = row.payload, .operation_id = row.operation_id orelse return error.InvalidWal };
    }
    fn adaptRows(a: A, rows: []@import("wal/types.zig").Record) ![]Record {
        defer a.free(rows);
        errdefer for (rows) |row| {
            a.free(row.payload);
            if (row.operation_id) |id| a.free(id);
        };
        const records = try a.alloc(Record, rows.len);
        errdefer a.free(records);
        for (rows, records) |row, *out| out.* = try adapt(row);
        return records;
    }
    pub fn find(self: Store, a: A, id: []const u8, payload: []const u8) !?u64 {
        var wal = try self.open(a, false);
        defer wal.deinit();
        return wal.indexed.?.findIdempotent(namespace, 0, payload, id);
    }
    pub fn append(self: Store, a: A, id: []const u8, payload: []const u8, expected_lsn: u64, covered_lsn: u64) !u64 {
        var wal = try self.open(a, true);
        defer wal.deinit();
        const indexed = wal.indexed.?;
        if (try indexed.findIdempotent(namespace, 0, payload, id)) |accepted| return accepted;
        const latest_lsn = try indexed.latestLsn(namespace);
        if (latest_lsn != expected_lsn) return error.LakeCheckpointConflict;
        if (covered_lsn > latest_lsn) return error.LakeWalCoverageGap;
        if (latest_lsn - covered_lsn >= max_pending) return error.LakeIngestionBackpressure;
        indexed.max_new_lsn = std.math.add(u64, covered_lsn, max_pending) catch return error.InvalidWal;
        return (indexed.appendIdempotentIfLatest(namespace, 0, payload, id, expected_lsn) catch |err| {
            // A failed response can follow the atomic HEAD commit. A durable
            // receipt resolves it; replay never creates another transaction.
            if (try indexed.findIdempotent(namespace, 0, payload, id)) |accepted| return accepted;
            return switch (err) {
                error.PublicationLagExceeded => error.LakeIngestionBackpressure,
                else => err,
            };
        }) orelse error.LakeCheckpointConflict;
    }
    pub fn pin(self: Store, a: A, cut: u64) !?Head {
        var wal = try self.open(a, false);
        defer wal.deinit();
        const captured = try wal.indexed.?.captureRead(namespace);
        const snapshot = captured.snapshot;
        if (cut < snapshot.first_lsn - 1) return error.LakeOverlayCoverageUnavailable;
        if (snapshot.latest_lsn < cut) return error.LakeWalCoverageGap;
        if (snapshot.latest_lsn - cut > max_pending) return error.LakeOverlayCoverageUnavailable;
        if (snapshot.latest_lsn == 0) return null;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&captured.scope);
        hash.update(&snapshot.identity);
        const identity = hash.finalResult();
        return .{ .lsn = snapshot.latest_lsn, .key = try a.dupe(u8, &std.fmt.bytesToHex(identity, .lower)), .captured = captured };
    }
    pub fn range(self: Store, a: A, cut: u64) !Range {
        return self.rangePinned(a, cut, try self.pin(a, cut));
    }
    pub fn rangePinned(self: Store, a: A, cut: u64, head: ?Head) !Range {
        const pinned = head orelse return .{ .lsn = cut, .records = &.{} };
        if (pinned.lsn < cut or pinned.lsn - cut > max_pending) return error.LakeOverlayCoverageUnavailable;
        if (cut == pinned.lsn) return .{ .lsn = cut, .records = &.{} };
        var wal = try self.open(a, false);
        defer wal.deinit();
        const rows = wal.indexed.?.readCapturedRangeAlloc(a, namespace, pinned.captured, .{ .start_lsn = cut + 1, .end_lsn = pinned.lsn, .max_records = max_pending, .max_bytes = 32 * 1024 * 1024, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.WalSnapshotExpired => return error.LakeOverlayCoverageUnavailable,
            error.WalReadBudgetExceeded => return error.LakeOverlayTooLarge,
            else => return err,
        };
        return .{ .lsn = pinned.lsn, .records = try adaptRows(a, rows) };
    }
    pub fn rangeAfterHead(self: Store, a: A, previous: Head, head: Head) !?Range {
        if (!std.mem.eql(u8, &previous.captured.scope, &head.captured.scope)) return null;
        if (previous.lsn == head.lsn) return if (std.mem.eql(u8, previous.key, head.key)) .{ .lsn = head.lsn, .records = &.{} } else null;
        const snapshot = head.captured.snapshot;
        // One shared authenticated parent proves incremental replay. Larger
        // jumps use a bounded full suffix rather than trusting equal LSNs.
        if (snapshot.parent_lsn != previous.lsn or snapshot.parent_identity == null or !std.mem.eql(u8, &snapshot.parent_identity.?, &previous.captured.snapshot.identity)) return null;
        return try self.rangePinned(a, previous.lsn, head);
    }
    pub fn gcFloor(self: Store, a: A) !u64 {
        var wal = try self.open(a, false);
        defer wal.deinit();
        return (try wal.indexed.?.readSnapshot(namespace)).first_lsn - 1;
    }
    pub const GcResult = struct { records_removed: usize = 0, floor: u64 = 0, complete: bool = false, collection: ?@import("wal/collector.zig").Collector.Result = null };
    /// Catalog/serving leases choose the safe logical cut. The shared collector
    /// subsequently reclaims pages under its persisted epoch/root-use contract.
    pub fn prune(self: Store, a: A, cut: u64, maximum: usize, dry_run: bool) !GcResult {
        if (maximum == 0 or maximum > 4096) return error.InvalidLakeMaintenanceLimits;
        var wal = try self.open(a, !dry_run);
        defer wal.deinit();
        const indexed = wal.indexed.?;
        const before = try indexed.captureRead(namespace);
        if (cut > before.snapshot.latest_lsn) return error.LakeWalCoverageGap;
        const floor = before.snapshot.first_lsn - 1;
        if (dry_run) return .{ .floor = floor, .records_removed = @intCast(@min(maximum, cut -| floor)), .complete = cut <= floor };
        if (cut > floor) _ = try indexed.truncatePrefix(namespace, cut + 1);
        const more = try indexed.compactPrefixStepWithCancellation(namespace, @min(maximum, 256), catalog.types.contextCancellation(&self.context));
        const after = try indexed.captureRead(namespace);
        return .{ .floor = after.snapshot.first_lsn - 1, .records_removed = @intCast(after.head.physical_first_lsn - before.head.physical_first_lsn), .complete = !more };
    }
    pub fn next(self: Store, a: A, cut: u64) !?Record {
        const head = try self.pin(a, cut) orelse return null;
        if (head.lsn == cut) return null;
        var wal = try self.open(a, false);
        defer wal.deinit();
        const rows = try wal.indexed.?.readCapturedRangeAlloc(a, namespace, head.captured, .{ .start_lsn = cut + 1, .end_lsn = cut + 1, .max_records = 1, .max_bytes = 32 * 1024 * 1024, .cancellation = catalog.types.contextCancellation(&self.context) });
        const records = try adaptRows(a, rows);
        defer a.free(records);
        return records[0];
    }
};

test "external lake segmented WAL resolves lost tail response and fences concurrent admission" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var store: Store = .{ .client = faults.client(), .bucket = "archive", .prefix = "wal" };
    faults.put_filter = .{ .ptr = &faults, .matches = struct {
        fn call(_: *anyopaque, _: []const u8, key_name: []const u8, _: []const u8) bool {
            return std.mem.endsWith(u8, key_name, "/HEAD");
        }
    }.call };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectEqual(@as(u64, 1), try store.append(a, "a", "one", 0, 0));
    try std.testing.expectEqual(@as(u64, 1), try store.append(a, "a", "one", 0, 0));
    try std.testing.expectError(error.WalIdempotencyConflict, store.append(a, "a", "changed", 1, 0));
    try std.testing.expectError(error.LakeCheckpointConflict, store.append(a, "b", "two", 0, 0));
    try std.testing.expectEqual(@as(u64, 2), try store.append(a, "b", "two", 1, 0));
    try std.testing.expectEqualStrings("one", (try store.next(a, 0)).?.payload);
    try std.testing.expectEqualStrings("two", (try store.next(a, 1)).?.payload);
    try std.testing.expect(try store.next(a, 2) == null);
    try std.testing.expectError(error.LakeWalCoverageGap, store.next(a, 3));
}

test "external lake segmented WAL bounds backlog retains old replay receipts and commits receipts atomically" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    const store: Store = .{ .client = faults.client(), .bucket = "archive", .prefix = "wal" };
    faults.put_filter = .{ .ptr = &faults, .matches = struct {
        fn call(_: *anyopaque, _: []const u8, key_name: []const u8, _: []const u8) bool {
            return std.mem.endsWith(u8, key_name, "/HEAD");
        }
    }.call };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectEqual(@as(u64, 1), try store.append(a, "first", "one", 0, 0));
    try std.testing.expectEqual(@as(?u64, 1), try store.find(a, "first", "one"));
    for (1..max_pending) |index| {
        const id = try std.fmt.allocPrint(a, "batch-{d}", .{index});
        try std.testing.expectEqual(@as(u64, index + 1), try store.append(a, id, "data", index, 0));
    }
    try std.testing.expectError(error.LakeIngestionBackpressure, store.append(a, "full", "data", max_pending, 0));
    try std.testing.expectEqual(@as(u64, max_pending + 1), try store.append(a, "full", "data", max_pending, 1));
    try std.testing.expectEqual(@as(u64, 1), try store.append(a, "first", "one", 0, 0));
    try std.testing.expectEqual(@as(u64, 2), (try store.next(a, 1)).?.lsn);
    const canceled: std.atomic.Value(bool) = .init(true);
    var stopped = store;
    stopped.context.cancellation = objectstore.CancellationToken.fromAtomic(&canceled);
    try std.testing.expectError(error.Canceled, stopped.latest(a));
}

test "external lake WAL collection resumes bounded jobs and preserves old idempotency proofs" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    const store: Store = .{ .client = memory.client(), .bucket = "archive", .prefix = "wal" };
    for (0..6) |i| {
        const id = try std.fmt.allocPrint(a, "op-{d}", .{i});
        _ = try store.append(a, id, "same payload across different IDs", i, i);
    }
    const planned = try store.prune(a, 5, 1, true);
    try std.testing.expectEqual(@as(u64, 0), try store.gcFloor(a));
    try std.testing.expectEqual(@as(usize, 1), planned.records_removed);
    var result = try store.prune(a, 5, 1, false);
    try std.testing.expect(!result.complete);
    try std.testing.expectEqual(@as(u64, 5), result.floor);
    while (!result.complete) result = try store.prune(a, 5, 1, false);
    try std.testing.expectEqual(@as(u64, 1), (try store.find(a, "op-0", "same payload across different IDs")).?);
    try std.testing.expectError(error.WalIdempotencyConflict, store.find(a, "op-0", "different"));
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, store.range(a, 4));
    try std.testing.expectEqualStrings("same payload across different IDs", (try store.next(a, 5)).?.payload);
    const after = try store.prune(a, 6, 4096, false);
    try std.testing.expect(after.complete);
    try std.testing.expectEqual(@as(u64, 6), try store.gcFloor(a));
    try std.testing.expectEqual(@as(u64, 7), try store.append(a, "op-6", "new payload", 6, 6));
    const pending = try store.range(a, 6);
    try std.testing.expectEqual(@as(usize, 1), pending.records.len);
    try std.testing.expectEqualStrings("new payload", pending.records[0].payload);
}

test "external lake unified WAL shares indexed records and rejects an unmarked legacy prefix" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    const store: Store = .{ .client = client, .bucket = "archive", .prefix = "new" };
    _ = try store.append(ca, "batch", "transaction body", 0, 0);
    var shared = try store.open(a, false);
    defer shared.deinit();
    const rows = try shared.indexed.?.readRangeAlloc(a, namespace, .{ .start_lsn = 1, .end_lsn = 1 });
    defer @import("wal/types.zig").freeRecords(a, rows);
    try std.testing.expectEqualStrings("transaction body", rows[0].payload);
    try std.testing.expectEqualStrings("batch", rows[0].operation_id.?);
    var old = try client.putObject("archive", "old/tail.json", "{\"lsn\":7}", .{});
    defer old.deinit(a);
    const legacy: Store = .{ .client = client, .bucket = "archive", .prefix = "old" };
    try std.testing.expectError(error.ManagedWalMigrationRequired, legacy.watermark(ca));
    var listed = try client.listObjects("archive", .{ .prefix = "old/" });
    defer listed.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), listed.entries.len);
}
