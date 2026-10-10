// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Segmented lake-ingestion WAL: immutable transaction records and request
//! intents, one conditional tail pointer, durable acceptance receipts. Normal
//! append work is independent of archive age. Bounded pending depth provides
//! backpressure while preserving every acknowledged transaction across crashes.
const std = @import("std");
const objectstore = @import("objectstore");
const catalog = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog;
const A = std.mem.Allocator;
pub const max_pending = 64;
pub const Record = struct { lsn: u64, payload: []const u8, operation_id: []const u8, previous: ?[]const u8 };
const StoredRecord = struct { lsn: u64, payload_ref: []const u8, operation_id: []const u8, previous: ?[]const u8 };
const Receipt = struct { version: u8 = 1, lsn: u64, operation_id: []const u8, payload_hash: []const u8 };
const Head = struct { lsn: u64, key: []const u8 };
const Object = struct { bytes: []const u8, etag: ?[]const u8 };
pub const Store = struct {
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context = .{},
    fn key(self: Store, a: A, relative: []const u8) ![]const u8 {
        return std.fmt.allocPrint(a, "{s}/{s}", .{ self.prefix, relative });
    }
    fn get(self: Store, a: A, key_name: []const u8) !?Object {
        try self.context.ensureActive();
        var client = self.client;
        var response = client.getObject(self.bucket, key_name, .{ .cancellation = catalog.types.contextCancellation(&self.context), .max_response_bytes = 12 * 1024 * 1024 }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => return null,
            else => return err,
        };
        defer response.deinit(client.allocator);
        return .{ .bytes = try a.dupe(u8, response.body), .etag = if (response.metadata.etag) |etag| try a.dupe(u8, etag) else null };
    }
    fn put(self: Store, a: A, key_name: []const u8, bytes: []const u8) !void {
        try self.context.ensureActive();
        var client = self.client;
        var result = client.putObject(self.bucket, key_name, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => {
                const old = (try self.get(a, key_name)) orelse return error.LakeWalOutcomeUnknown;
                if (!std.mem.eql(u8, old.bytes, bytes)) return error.WalIdempotencyConflict;
                return;
            },
            else => return err,
        };
        defer result.deinit(client.allocator);
    }
    fn loadRecord(self: Store, a: A, ref: []const u8) !StoredRecord {
        if (!std.mem.startsWith(u8, ref, "records/") or ref.len != "records/".len + 64) return error.InvalidWal;
        const data = (try self.get(a, try self.key(a, ref))) orelse return error.LakeWalCoverageGap;
        if (!std.mem.eql(u8, &catalog.types.digestHex(data.bytes), ref["records/".len..])) return error.InvalidWal;
        return std.json.parseFromSliceLeaky(StoredRecord, a, data.bytes, .{});
    }
    fn tail(self: Store, a: A) !struct { head: ?Head, etag: ?[]const u8 } {
        const data = (try self.get(a, try self.key(a, "tail.json"))) orelse return .{ .head = null, .etag = null };
        return .{ .head = try std.json.parseFromSliceLeaky(Head, a, data.bytes, .{}), .etag = data.etag orelse return error.MissingObjectEtag };
    }
    fn readPayload(self: Store, a: A, record: StoredRecord) ![]const u8 {
        const ref = record.payload_ref;
        if (!std.mem.startsWith(u8, ref, "payloads/") or (ref.len != "payloads/".len + 64 and ref.len != "payloads/".len + 129)) return error.InvalidWal;
        const data = (try self.get(a, try self.key(a, ref))) orelse return error.LakeWalCoverageGap;
        if (!std.mem.eql(u8, &catalog.types.digestHex(data.bytes), ref[ref.len - 64 ..])) return error.InvalidWal;
        return data.bytes;
    }
    fn expand(self: Store, a: A, record: StoredRecord) !Record {
        return .{ .lsn = record.lsn, .payload = try self.readPayload(a, record), .operation_id = record.operation_id, .previous = record.previous };
    }
    /// Monotone admission watermark; validation must not enumerate WAL rows.
    pub fn watermark(self: Store, a: A) !u64 {
        return if ((try self.tail(a)).head) |head| head.lsn else 0;
    }
    pub fn latest(self: Store, a: A) !?Record {
        const current = try self.tail(a);
        return if (current.head) |head| try self.expand(a, try self.loadRecord(a, head.key)) else null;
    }
    fn receipt(self: Store, a: A, record_key: []const u8, record: StoredRecord) !void {
        const path = try self.key(a, try std.fmt.allocPrint(a, "receipts/{s}", .{catalog.types.digestHex(record.operation_id)}));
        const proof: Receipt = .{ .lsn = record.lsn, .operation_id = record.operation_id, .payload_hash = record.payload_ref[record.payload_ref.len - 64 ..] };
        const bytes = try std.json.Stringify.valueAlloc(a, proof, .{});
        if (try self.get(a, path)) |old| {
            if (std.mem.eql(u8, old.bytes, bytes)) return;
            if (!std.mem.eql(u8, old.bytes, record_key)) return error.WalIdempotencyConflict;
            var client = self.client;
            var upgraded = try client.putObject(self.bucket, path, bytes, .{ .if_match_etag = old.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&self.context) });
            upgraded.deinit(client.allocator);
            return;
        }
        try self.put(a, path, bytes);
    }
    /// Resolve a retry from its receipt or walk only the unreceipted recent
    /// chain. The drain persists receipts before committing coverage, so old
    /// acknowledged requests never require traversing archived history.
    pub fn find(self: Store, a: A, id: []const u8, payload: []const u8) !?u64 {
        const receipt_key = try self.key(a, try std.fmt.allocPrint(a, "receipts/{s}", .{catalog.types.digestHex(id)}));
        if (try self.get(a, receipt_key)) |proof| {
            if (proof.bytes.len != 0 and proof.bytes[0] == '{') {
                const receipt_value = try std.json.parseFromSliceLeaky(Receipt, a, proof.bytes, .{});
                if (receipt_value.version != 1 or receipt_value.lsn == 0 or !std.mem.eql(u8, receipt_value.operation_id, id) or !std.mem.eql(u8, receipt_value.payload_hash, &catalog.types.digestHex(payload))) return error.WalIdempotencyConflict;
                return receipt_value.lsn;
            }
            const record = try self.loadRecord(a, proof.bytes);
            if (!std.mem.eql(u8, record.operation_id, id) or !std.mem.eql(u8, try self.readPayload(a, record), payload)) return error.WalIdempotencyConflict;
            return record.lsn;
        }
        const intent_key = try self.key(a, try std.fmt.allocPrint(a, "requests/{s}", .{catalog.types.digestHex(id)}));
        const intent = (try self.get(a, intent_key)) orelse return null;
        const candidate = try self.loadRecord(a, intent.bytes);
        if (!std.mem.eql(u8, candidate.operation_id, id) or !std.mem.eql(u8, try self.readPayload(a, candidate), payload)) return error.WalIdempotencyConflict;
        const current = try self.tail(a);
        var ref = if (current.head) |head| @as(?[]const u8, head.key) else null;
        var checked: usize = 0;
        while (ref) |record_key| : (checked += 1) {
            if (checked >= max_pending) return error.LakeWalOutcomeUnknown;
            const record = try self.loadRecord(a, record_key);
            if (record.lsn < candidate.lsn) return null;
            if (record.lsn == candidate.lsn) {
                if (!std.mem.eql(u8, record_key, intent.bytes)) return error.LakeCheckpointConflict;
                try self.receipt(a, record_key, record);
                return record.lsn;
            }
            ref = record.previous;
        }
        return null;
    }
    pub fn append(self: Store, a: A, id: []const u8, payload: []const u8, expected_lsn: u64, covered_lsn: u64) !u64 {
        if (try self.find(a, id, payload)) |lsn| return lsn;
        const current = try self.tail(a);
        const latest_lsn = if (current.head) |head| head.lsn else 0;
        if (latest_lsn != expected_lsn) return error.LakeCheckpointConflict;
        if (covered_lsn > latest_lsn) return error.LakeWalCoverageGap;
        if (latest_lsn - covered_lsn >= max_pending) return error.LakeIngestionBackpressure;
        if (latest_lsn == std.math.maxInt(u64)) return error.InvalidWal;
        const payload_ref = try std.fmt.allocPrint(a, "payloads/{s}-{s}", .{ catalog.types.digestHex(id), catalog.types.digestHex(payload) });
        try self.put(a, try self.key(a, payload_ref), payload);
        const record: StoredRecord = .{ .lsn = latest_lsn + 1, .payload_ref = payload_ref, .operation_id = id, .previous = if (current.head) |head| head.key else null };
        const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
        const record_key = try std.fmt.allocPrint(a, "records/{s}", .{catalog.types.digestHex(bytes)});
        try self.put(a, try self.key(a, record_key), bytes);
        const intent_key = try self.key(a, try std.fmt.allocPrint(a, "requests/{s}", .{catalog.types.digestHex(id)}));
        try self.put(a, intent_key, record_key);
        const head = try std.json.Stringify.valueAlloc(a, Head{ .lsn = record.lsn, .key = record_key }, .{});
        try self.context.ensureActive();
        var client = self.client;
        var result = client.putObject(self.bucket, try self.key(a, "tail.json"), head, .{ .if_none_match = current.head == null, .if_match_etag = current.etag, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| {
            // A transport error may follow a successful conditional write.
            // Resolve immutable chain evidence before telling a caller to retry.
            if (try self.find(a, id, payload)) |accepted| return accepted;
            return switch (err) {
                error.PreconditionFailed => error.LakeCheckpointConflict,
                else => error.LakeWalOutcomeUnknown,
            };
        };
        defer result.deinit(client.allocator);
        try self.receipt(a, record_key, record);
        return record.lsn;
    }
    /// Pin one immutable accepted tail and return its complete bounded suffix.
    /// Changes accepted after this call belong to a subsequent read snapshot.
    pub fn range(self: Store, a: A, cut: u64) !struct { lsn: u64, records: []const Record } {
        if (cut < try self.gcFloor(a)) return error.LakeOverlayCoverageUnavailable;
        const current = try self.tail(a);
        const head = current.head orelse {
            if (cut != 0) return error.LakeWalCoverageGap;
            return .{ .lsn = 0, .records = &.{} };
        };
        if (head.lsn < cut or head.lsn - cut > max_pending) return error.LakeOverlayCoverageUnavailable;
        var records: std.ArrayList(Record) = .empty;
        var ref = head.key;
        var expected = head.lsn;
        var bytes: usize = 0;
        while (expected > cut) : (expected -= 1) {
            const record = try self.loadRecord(a, ref);
            if (record.lsn != expected) return error.LakeWalCoverageGap;
            const expanded = try self.expand(a, record);
            bytes = std.math.add(usize, bytes, expanded.payload.len) catch return error.LakeOverlayTooLarge;
            if (bytes > 32 * 1024 * 1024) return error.LakeOverlayTooLarge;
            try records.append(a, expanded);
            if (expected > cut + 1) ref = record.previous orelse return error.LakeWalCoverageGap;
        }
        std.mem.reverse(Record, records.items);
        return .{ .lsn = head.lsn, .records = records.items };
    }
    pub fn gcFloor(self: Store, a: A) !u64 {
        const value = (try self.get(a, try self.key(a, "gc-floor"))) orelse return 0;
        return std.fmt.parseInt(u64, value.bytes, 10);
    }
    const PendingGc = struct { ref: []const u8, record: StoredRecord };
    const GcCursor = struct { cut: u64, stop: u64, next: ?[]const u8, pending: ?PendingGc = null };
    pub const GcResult = struct { records_removed: usize = 0, floor: u64 = 0, complete: bool = false };
    /// cut must be covered by the catalog AND every retained serving generation.
    /// Receipts retain compact payload hashes, so pruning bytes never permits an
    /// old batch identity to be replayed as a new transaction.
    pub fn prune(self: Store, a: A, cut: u64, maximum: usize, dry_run: bool) !GcResult {
        if (maximum == 0 or maximum > 4096) return error.InvalidLakeMaintenanceLimits;
        const floor = try self.gcFloor(a);
        const cursor_key = try self.key(a, "gc-cursor.json");
        var saved = try self.get(a, cursor_key);
        if (saved == null and (cut <= floor or cut == 0)) return .{ .floor = floor, .complete = true };
        var cursor: GcCursor = undefined;
        if (saved) |value| cursor = try std.json.parseFromSliceLeaky(GcCursor, a, value.bytes, .{}) else {
            const current = try self.tail(a);
            const head = current.head orelse return error.LakeWalCoverageGap;
            if (head.lsn < cut or head.lsn - cut > max_pending) return error.LakeOverlayCoverageUnavailable;
            var ref = head.key;
            while (true) {
                const record = try self.loadRecord(a, ref);
                if (record.lsn == cut) {
                    cursor = .{ .cut = cut, .stop = @max(floor, 1), .next = record.previous };
                    break;
                }
                if (record.lsn < cut) return error.LakeWalCoverageGap;
                ref = record.previous orelse return error.LakeWalCoverageGap;
            }
            if (!dry_run) {
                const cursor_bytes = try std.json.Stringify.valueAlloc(a, cursor, .{});
                try self.put(a, cursor_key, cursor_bytes);
                saved = try self.get(a, cursor_key);
                if (saved == null or !std.mem.eql(u8, saved.?.bytes, cursor_bytes)) return error.LakeCheckpointConflict;
            }
        }
        if (cursor.cut > cut) return error.LakeOverlayCoverageUnavailable;
        var result: GcResult = .{ .floor = if (dry_run) floor else cursor.cut };
        if (!dry_run) {
            const floor_key = try self.key(a, "gc-floor");
            const previous = try self.get(a, floor_key);
            const current_floor = if (previous) |value| try std.fmt.parseInt(u64, value.bytes, 10) else 0;
            if (current_floor > cursor.cut) return error.LakeWalCoverageGap;
            if (current_floor < cursor.cut) {
                var client = self.client;
                var written = try client.putObject(self.bucket, floor_key, try std.fmt.allocPrint(a, "{d}", .{cursor.cut}), .{ .if_none_match = previous == null, .if_match_etag = if (previous) |value| value.etag orelse return error.MissingObjectEtag else null, .cancellation = catalog.types.contextCancellation(&self.context) });
                written.deinit(client.allocator);
            }
        }
        while (result.records_removed < maximum) {
            const ref = if (cursor.pending) |pending| pending.ref else cursor.next orelse break;
            const record = if (cursor.pending) |pending| pending.record else try self.loadRecord(a, ref);
            if (record.lsn >= cursor.cut or record.lsn < cursor.stop) return error.InvalidWal;
            const next_ref = if (record.lsn == cursor.stop) null else record.previous;
            if (!dry_run) {
                if (cursor.pending == null) {
                    cursor.pending = .{ .ref = ref, .record = record };
                    cursor.next = next_ref;
                    saved = try self.saveGcCursor(a, cursor_key, saved.?, cursor);
                }
                // Upgrade legacy receipts before deleting their referenced bytes.
                const receipt_key = try self.key(a, try std.fmt.allocPrint(a, "receipts/{s}", .{catalog.types.digestHex(record.operation_id)}));
                const old = (try self.get(a, receipt_key)) orelse return error.LakeWalCoverageGap;
                const proof: Receipt = .{ .lsn = record.lsn, .operation_id = record.operation_id, .payload_hash = record.payload_ref[record.payload_ref.len - 64 ..] };
                const bytes = try std.json.Stringify.valueAlloc(a, proof, .{});
                if (!std.mem.eql(u8, old.bytes, bytes)) {
                    if (!std.mem.eql(u8, old.bytes, ref)) return error.InvalidWal;
                    var client = self.client;
                    var upgraded = try client.putObject(self.bucket, receipt_key, bytes, .{ .if_match_etag = old.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&self.context) });
                    upgraded.deinit(client.allocator);
                }
                // Legacy content-addressed payloads may be shared by callers
                // of the generic store. Retain them until a separate mark pass;
                // new payload keys also include stable operation identity.
                if (record.payload_ref.len == "payloads/".len + 129) try self.deleteImmutable(a, record.payload_ref);
                try self.deleteImmutable(a, try std.fmt.allocPrint(a, "requests/{s}", .{catalog.types.digestHex(record.operation_id)}));
                // The cursor owns a copy of the header while deletion is
                // pending, so restart can finish even after its bytes disappear.
                try self.deleteImmutable(a, ref);
                cursor.pending = null;
                saved = try self.saveGcCursor(a, cursor_key, saved.?, cursor);
            } else cursor.next = next_ref;
            result.records_removed += 1;
        }
        result.complete = cursor.next == null and cursor.pending == null;
        if (result.complete and !dry_run) {
            var client = self.client;
            try client.deleteObject(self.bucket, cursor_key, .{ .if_match_etag = saved.?.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&self.context) });
        }
        return result;
    }
    fn saveGcCursor(self: Store, a: A, path: []const u8, previous: Object, value: GcCursor) !Object {
        var client = self.client;
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        var written = try client.putObject(self.bucket, path, bytes, .{ .if_match_etag = previous.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&self.context) });
        defer written.deinit(client.allocator);
        const fresh = (try self.get(a, path)) orelse return error.LakeWalCoverageGap;
        if (!std.mem.eql(u8, fresh.bytes, bytes)) return error.LakeCheckpointConflict;
        return fresh;
    }
    fn deleteImmutable(self: Store, a: A, relative: []const u8) !void {
        const path = try self.key(a, relative);
        const value = (try self.get(a, path)) orelse return;
        var client = self.client;
        try client.deleteObject(self.bucket, path, .{ .if_match_etag = value.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&self.context) });
    }
    pub fn next(self: Store, a: A, cut: u64) !?Record {
        const current = try self.tail(a);
        if (current.head == null) {
            if (cut != 0) return error.LakeWalCoverageGap;
            return null;
        }
        const head = current.head.?;
        if (head.lsn < cut) return error.LakeWalCoverageGap;
        if (head.lsn == cut) return null;
        if (head.lsn - cut > max_pending) return error.LakeWalCoverageGap;
        var ref = head.key;
        while (true) {
            const record = try self.loadRecord(a, ref);
            if (record.lsn == cut + 1) {
                try self.receipt(a, ref, record);
                return try self.expand(a, record);
            }
            if (record.lsn <= cut + 1) return error.LakeWalCoverageGap;
            ref = record.previous orelse return error.LakeWalCoverageGap;
        }
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
            return std.mem.endsWith(u8, key_name, "/tail.json");
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

test "external lake segmented WAL bounds backlog retains old replay receipts and recovers receipt loss" {
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
            return std.mem.indexOf(u8, key_name, "/receipts/") != null;
        }
    }.call };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, store.append(a, "first", "one", 0, 0));
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
