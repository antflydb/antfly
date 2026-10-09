// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
        if (!std.mem.startsWith(u8, ref, "payloads/") or ref.len != "payloads/".len + 64) return error.InvalidWal;
        const data = (try self.get(a, try self.key(a, ref))) orelse return error.LakeWalCoverageGap;
        if (!std.mem.eql(u8, &catalog.types.digestHex(data.bytes), ref["payloads/".len..])) return error.InvalidWal;
        return data.bytes;
    }
    fn expand(self: Store, a: A, record: StoredRecord) !Record {
        return .{ .lsn = record.lsn, .payload = try self.readPayload(a, record), .operation_id = record.operation_id, .previous = record.previous };
    }
    pub fn latest(self: Store, a: A) !?Record {
        const current = try self.tail(a);
        return if (current.head) |head| try self.expand(a, try self.loadRecord(a, head.key)) else null;
    }
    fn receipt(self: Store, a: A, record_key: []const u8, record: StoredRecord) !void {
        const path = try self.key(a, try std.fmt.allocPrint(a, "receipts/{s}", .{catalog.types.digestHex(record.operation_id)}));
        try self.put(a, path, record_key);
    }
    /// Resolve a retry from its receipt or walk only the unreceipted recent
    /// chain. The drain persists receipts before committing coverage, so old
    /// acknowledged requests never require traversing archived history.
    pub fn find(self: Store, a: A, id: []const u8, payload: []const u8) !?u64 {
        const receipt_key = try self.key(a, try std.fmt.allocPrint(a, "receipts/{s}", .{catalog.types.digestHex(id)}));
        if (try self.get(a, receipt_key)) |proof| {
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
        const payload_ref = try std.fmt.allocPrint(a, "payloads/{s}", .{catalog.types.digestHex(payload)});
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
