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

//! Bounded, copy-on-write object WAL. Two authenticated ordered trees are
//! committed by one small HEAD CAS: LSN -> record and operation ID -> receipt.
//! A failed CAS publishes neither tree. Old roots are immutable, so a captured
//! read never changes underneath its cursor. No LIST or full-log rewrite is
//! required for append, idempotency lookup, latest LSN, or a bounded tail read.
const std = @import("std");
const objectstore = @import("objectstore");
const tree = @import("../graph_segment/page_tree.zig");
const codec = @import("record_codec.zig");
const types = @import("types.zig");
const WalStore = @import("store.zig").WalStore;
const A = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const IndexedObjectStore = struct {
    alloc: A,
    client: objectstore.Client,
    bucket: []u8,
    prefix: []u8,
    max_attempts: usize = 8,

    pub const Head = struct {
        format: u32 = 1,
        latest_lsn: u64 = 0,
        first_lsn: u64 = 1,
        records: ?tree.Ref = null,
        operations: ?tree.Ref = null,
    };

    pub fn init(alloc: A, client: objectstore.Client, bucket: []const u8, prefix: []const u8) !@This() {
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        var view = client;
        view.allocator = alloc;
        return .{ .alloc = alloc, .client = view, .bucket = owned_bucket, .prefix = try alloc.dupe(u8, prefix) };
    }

    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }

    pub fn walStore(self: *@This()) WalStore {
        return .{ .allocator = self.alloc, .ptr = self, .vtable = &vtable };
    }

    pub fn append(self: *@This(), namespace: []const u8, timestamp: u64, payload: []const u8) !u64 {
        return (try self.commit(namespace, timestamp, payload, null, null)).?;
    }

    pub fn appendIdempotent(self: *@This(), namespace: []const u8, timestamp: u64, payload: []const u8, operation_id: []const u8) !u64 {
        return (try self.commit(namespace, timestamp, payload, operation_id, null)).?;
    }

    pub fn appendIdempotentIfLatest(self: *@This(), namespace: []const u8, timestamp: u64, payload: []const u8, operation_id: []const u8, latest: u64) !?u64 {
        return self.commit(namespace, timestamp, payload, operation_id, latest);
    }

    fn commit(self: *@This(), namespace: []const u8, timestamp: u64, payload: []const u8, operation_id: ?[]const u8, expected_latest: ?u64) !?u64 {
        // Admission before any object write; records must fit an ordinary tree
        // value. Large application blobs belong in immutable object storage.
        if (payload.len > tree.max_record_bytes - 128) return error.WalRecordTooLarge;
        if (operation_id) |id| if (id.len == 0 or id.len > 4096) return error.InvalidWalOperationId;
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        var cache = tree.Cache{ .alloc = self.alloc, .underlying = ctx.store() };
        defer cache.deinit();
        const pages = cache.store();
        for (0..self.max_attempts) |_| {
            var current = try ctx.readHead();
            defer current.deinit(self.alloc);
            var receipt: [48]u8 = undefined;
            std.mem.writeInt(u64, receipt[8..16], timestamp, .little);
            Sha256.hash(payload, receipt[16..48], .{});
            if (operation_id) |id| {
                const found = try tree.lookupMany(self.alloc, pages, current.value.operations, &.{id});
                defer self.alloc.free(found);
                defer if (found[0]) |value| value.deinit(self.alloc);
                if (found[0]) |value| {
                    if (value.value.len != receipt.len) return error.WalIdempotencyConflict;
                    if (!std.mem.eql(u8, value.value[8..], receipt[8..])) return error.WalIdempotencyConflict;
                    const committed = std.mem.readInt(u64, value.value[0..8], .little);
                    if (committed == 0 or committed > current.value.latest_lsn) return error.InvalidWal;
                    return committed;
                }
            }
            if (expected_latest) |latest| if (current.value.latest_lsn != latest) return null;
            const next = std.math.add(u64, current.value.latest_lsn, 1) catch return error.WalLsnExhausted;
            // Reserve maxInt for the exclusive upper bound and retained floor.
            if (next == std.math.maxInt(u64)) return error.WalLsnExhausted;
            const encoded = if (operation_id) |id|
                try codec.encodeIdempotentRecordAlloc(self.alloc, next, timestamp, payload, id)
            else
                try codec.encodeRecordAlloc(self.alloc, next, timestamp, payload);
            defer self.alloc.free(encoded);
            if (encoded.len + 8 > tree.max_record_bytes) return error.WalRecordTooLarge;
            var lsn_key: [8]u8 = undefined;
            std.mem.writeInt(u64, &lsn_key, next, .big);
            var updated = current.value;
            updated.records = try tree.apply(self.alloc, pages, updated.records, &.{.{ .key = &lsn_key, .value = encoded }});
            if (operation_id) |id| {
                std.mem.writeInt(u64, receipt[0..8], next, .little);
                updated.operations = try tree.apply(self.alloc, pages, updated.operations, &.{.{ .key = id, .value = &receipt }});
            }
            updated.latest_lsn = next;
            ctx.writeHead(updated, current.etag) catch |err| switch (err) {
                error.PreconditionFailed => continue,
                else => return err,
            };
            return next;
        }
        return error.WalCommitContended;
    }

    pub const BatchResult = types.BatchResult;

    /// One atomic visibility boundary for a bounded batch. Tree path copies
    /// are shared within the sorted update, and HEAD is written once rather
    /// than once per document. The operation ID identifies the entire batch.
    pub fn appendBatchIdempotent(self: *@This(), namespace: []const u8, timestamp: u64, payloads: []const []const u8, id: []const u8) !BatchResult {
        if (payloads.len == 0 or payloads.len > 1024) return error.InvalidWalBatch;
        if (id.len == 0 or id.len > 4096) return error.InvalidWalOperationId;
        var hash = Sha256.init(.{});
        var total: usize = 0;
        for (payloads) |payload| {
            if (payload.len > tree.max_record_bytes - 128) return error.WalRecordTooLarge;
            total = std.math.add(usize, total, payload.len + 32) catch return error.WalRecordTooLarge;
            if (total > 8 * 1024 * 1024) return error.WalRecordTooLarge;
            var len: [8]u8 = undefined;
            std.mem.writeInt(u64, &len, payload.len, .big);
            hash.update(&len);
            hash.update(payload);
        }
        var receipt: [56]u8 = undefined;
        std.mem.writeInt(u64, receipt[16..24], timestamp, .little);
        @memcpy(receipt[24..56], &hash.finalResult());
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        var cache = tree.Cache{ .alloc = self.alloc, .underlying = ctx.store() };
        defer cache.deinit();
        const pages = cache.store();
        for (0..self.max_attempts) |_| {
            var current = try ctx.readHead();
            defer current.deinit(self.alloc);
            const found = try tree.lookupMany(self.alloc, pages, current.value.operations, &.{id});
            defer self.alloc.free(found);
            defer if (found[0]) |value| value.deinit(self.alloc);
            if (found[0]) |value| {
                if (value.value.len != receipt.len or !std.mem.eql(u8, value.value[16..], receipt[16..])) return error.WalIdempotencyConflict;
                const start = std.mem.readInt(u64, value.value[0..8], .little);
                const end = std.mem.readInt(u64, value.value[8..16], .little);
                if (start == 0 or end < start or end > current.value.latest_lsn or end - start + 1 != payloads.len) return error.InvalidWal;
                return .{ .start_lsn = start, .end_lsn = end };
            }
            const last = std.math.add(u64, current.value.latest_lsn, payloads.len) catch return error.WalLsnExhausted;
            if (last == std.math.maxInt(u64)) return error.WalLsnExhausted;
            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            const temporary = arena.allocator();
            const keys = try temporary.alloc([8]u8, payloads.len);
            const changes = try temporary.alloc(tree.Mutation, payloads.len);
            for (payloads, keys, changes, 0..) |payload, *key, *change, i| {
                const lsn = current.value.latest_lsn + i + 1;
                std.mem.writeInt(u64, key, lsn, .big);
                change.* = .{ .key = key, .value = try codec.encodeRecordAlloc(temporary, lsn, timestamp, payload) };
            }
            var updated = current.value;
            updated.records = try tree.apply(self.alloc, pages, updated.records, changes);
            std.mem.writeInt(u64, receipt[0..8], current.value.latest_lsn + 1, .little);
            std.mem.writeInt(u64, receipt[8..16], last, .little);
            updated.operations = try tree.apply(self.alloc, pages, updated.operations, &.{.{ .key = id, .value = &receipt }});
            updated.latest_lsn = last;
            ctx.writeHead(updated, current.etag) catch |err| switch (err) {
                error.PreconditionFailed => continue,
                else => return err,
            };
            return .{ .start_lsn = current.value.latest_lsn + 1, .end_lsn = last };
        }
        return error.WalCommitContended;
    }

    pub fn latestLsn(self: *@This(), namespace: []const u8) !u64 {
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        var head = try ctx.readHead();
        defer head.deinit(self.alloc);
        return head.value.latest_lsn;
    }

    pub fn readRangeAlloc(self: *@This(), alloc: A, namespace: []const u8, range: types.ReadRange) ![]types.Record {
        try range.check();
        if (range.start_lsn == 0 or range.end_lsn == std.math.maxInt(u64)) return error.InvalidWal;
        var ctx = try Context.init(self, namespace, range.cancellation);
        defer ctx.deinit();
        var head = try ctx.readHead();
        defer head.deinit(self.alloc);
        if (range.start_lsn > range.end_lsn) return alloc.alloc(types.Record, 0);
        if (range.start_lsn < head.value.first_lsn) return error.WalSnapshotExpired;
        if (range.end_lsn > head.value.latest_lsn) return error.WalVisibilityUnavailable;
        const count = range.end_lsn - range.start_lsn + 1;
        if (count > range.max_records) return error.WalReadBudgetExceeded;
        var lower: [8]u8 = undefined;
        var upper: [8]u8 = undefined;
        std.mem.writeInt(u64, &lower, range.start_lsn, .big);
        std.mem.writeInt(u64, &upper, range.end_lsn + 1, .big);
        var cursor = try tree.Cursor.init(alloc, ctx.store(), head.value.records, &lower, &upper);
        defer cursor.deinit();
        var out: std.ArrayListUnmanaged(types.Record) = .empty;
        errdefer {
            for (out.items) |record| {
                alloc.free(record.payload);
                if (record.operation_id) |id| alloc.free(id);
            }
            out.deinit(alloc);
        }
        var bytes: usize = 0;
        var expected = range.start_lsn;
        while (try cursor.next()) |entry| {
            try range.check();
            const record = try codec.parseRecord(entry.value, 0);
            if (entry.key.len != 8 or record.next_cursor != entry.value.len or record.lsn != expected or
                std.mem.readInt(u64, entry.key[0..8], .big) != expected) return error.InvalidWal;
            bytes = std.math.add(usize, bytes, entry.value.len + @sizeOf(types.Record)) catch return error.WalReadBudgetExceeded;
            if (bytes > range.max_bytes) return error.WalReadBudgetExceeded;
            const payload = try alloc.dupe(u8, record.payload);
            errdefer alloc.free(payload);
            const id = if (record.operation_id) |value| try alloc.dupe(u8, value) else null;
            errdefer if (id) |value| alloc.free(value);
            try out.append(alloc, .{ .lsn = record.lsn, .timestamp_ns = record.timestamp_ns, .payload = payload, .operation_id = id });
            expected += 1;
        }
        if (expected != range.end_lsn + 1) return error.InvalidWal;
        try range.check();
        return out.toOwnedSlice(alloc);
    }

    /// Logical retention does not reset the high watermark or idempotency
    /// receipts. Immutable page reclamation must be separately lease-fenced.
    pub fn truncatePrefix(self: *@This(), namespace: []const u8, keep_from: u64) !u64 {
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        for (0..self.max_attempts) |_| {
            var head = try ctx.readHead();
            defer head.deinit(self.alloc);
            const next_floor = @min(keep_from, head.value.latest_lsn + 1);
            if (next_floor <= head.value.first_lsn) return 0;
            var updated = head.value;
            updated.first_lsn = next_floor;
            ctx.writeHead(updated, head.etag) catch |err| switch (err) {
                error.PreconditionFailed => continue,
                else => return err,
            };
            return next_floor - head.value.first_lsn;
        }
        return error.WalCommitContended;
    }

    const Snapshot = struct {
        value: Head = .{},
        etag: ?[]u8 = null,
        fn deinit(self: *@This(), alloc: A) void {
            if (self.etag) |value| alloc.free(value);
        }
    };

    const Context = struct {
        owner: *IndexedObjectStore,
        root: []u8,
        domain: [32]u8,
        cancellation: ?objectstore.CancellationToken,

        fn init(owner: *IndexedObjectStore, namespace: []const u8, cancellation: ?objectstore.CancellationToken) !@This() {
            // Hash the namespace so untrusted names cannot escape the prefix.
            var digest: [32]u8 = undefined;
            Sha256.hash(namespace, &digest, .{});
            const root = try std.fmt.allocPrint(owner.alloc, "{s}/wal-v1/{s}", .{ owner.prefix, std.fmt.bytesToHex(digest, .lower) });
            var domain: [32]u8 = undefined;
            Sha256.hash(root, &domain, .{});
            return .{ .owner = owner, .root = root, .domain = domain, .cancellation = cancellation };
        }
        fn deinit(self: *@This()) void {
            self.owner.alloc.free(self.root);
        }
        fn store(self: *@This()) tree.Store {
            return .{ .ptr = self, .domain = self.domain, .get = getPage, .put = putPage, .check = check, .prefix_keys = true };
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cancellation) |token| try token.check();
        }
        fn readHead(self: *@This()) !Snapshot {
            const a = self.owner.alloc;
            const key = try std.fmt.allocPrint(a, "{s}/HEAD", .{self.root});
            defer a.free(key);
            var result = self.owner.client.getObject(self.owner.bucket, key, .{ .max_response_bytes = 4096, .cancellation = self.cancellation }) catch |err| switch (err) {
                error.FileNotFound => return .{},
                else => return err,
            };
            defer result.deinit(a);
            const parsed = try std.json.parseFromSlice(Head, a, result.body, .{});
            defer parsed.deinit();
            const head = parsed.value;
            if (head.format != 1 or head.first_lsn == 0 or head.latest_lsn == std.math.maxInt(u64) or
                head.first_lsn > head.latest_lsn + 1 or (head.latest_lsn != 0 and head.records == null)) return error.InvalidWal;
            if (head.records) |ref| {
                try ref.validate();
                if (ref.records != head.latest_lsn) return error.InvalidWal;
            }
            if (head.operations) |ref| {
                try ref.validate();
                if (ref.records > head.latest_lsn) return error.InvalidWal;
            }
            const etag = result.metadata.etag orelse return error.MissingObjectEtag;
            return .{ .value = head, .etag = try a.dupe(u8, etag) };
        }
        fn writeHead(self: *@This(), head: Head, etag: ?[]const u8) !void {
            const a = self.owner.alloc;
            const key = try std.fmt.allocPrint(a, "{s}/HEAD", .{self.root});
            defer a.free(key);
            const bytes = try std.json.Stringify.valueAlloc(a, head, .{});
            defer a.free(bytes);
            var result = try self.owner.client.putObject(self.owner.bucket, key, bytes, .{
                .if_none_match = etag == null,
                .if_match_etag = etag,
                .content_type = "application/json",
                .cancellation = self.cancellation,
            });
            defer result.deinit(a);
        }
        fn pageKey(self: *@This(), a: A, ref: tree.Ref) ![]u8 {
            return std.fmt.allocPrint(a, "{s}/pages/{s}", .{ self.root, std.fmt.bytesToHex(ref.digest, .lower) });
        }
        fn getPage(raw: *anyopaque, a: A, ref: tree.Ref) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const key = try self.pageKey(a, ref);
            defer a.free(key);
            var client = self.owner.client;
            client.allocator = a;
            var result = try client.getObject(self.owner.bucket, key, .{ .max_response_bytes = ref.bytes, .cancellation = self.cancellation });
            defer result.metadata.deinit(a);
            return result.body;
        }
        fn putPage(raw: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = self.owner.alloc;
            const key = try self.pageKey(a, ref);
            defer a.free(key);
            var result = self.owner.client.putObject(self.owner.bucket, key, bytes, .{ .if_none_match = true, .cancellation = self.cancellation }) catch |err| switch (err) {
                error.PreconditionFailed => {
                    // Existing content-addressed bytes are still authenticated
                    // before accepting a successful immutable upload.
                    const existing = try getPage(raw, a, ref);
                    defer a.free(existing);
                    if (!std.mem.eql(u8, existing, bytes)) return error.ArtifactIntegrityMismatch;
                    return;
                },
                else => return err,
            };
            defer result.deinit(a);
        }
    };

    const vtable: WalStore.VTable = .{
        .deinit = erasedDeinit,
        .append = erasedAppend,
        .append_idempotent = erasedIdempotent,
        .append_batch_idempotent = erasedBatch,
        .append_idempotent_if_latest = erasedConditional,
        .latest_lsn = erasedLatest,
        .read_from_alloc = erasedRead,
        .read_range_alloc = erasedRange,
        .truncate_prefix = erasedTruncate,
    };
    fn erasedDeinit(_: A, raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.deinit();
    }
    fn erasedAppend(raw: *anyopaque, ns: []const u8, t: u64, p: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.append(ns, t, p);
    }
    fn erasedIdempotent(raw: *anyopaque, ns: []const u8, t: u64, p: []const u8, id: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.appendIdempotent(ns, t, p, id);
    }
    fn erasedConditional(raw: *anyopaque, ns: []const u8, t: u64, p: []const u8, id: []const u8, latest: u64) !?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.appendIdempotentIfLatest(ns, t, p, id, latest);
    }
    fn erasedBatch(raw: *anyopaque, ns: []const u8, t: u64, payloads: []const []const u8, id: []const u8) !BatchResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.appendBatchIdempotent(ns, t, payloads, id);
    }
    fn erasedLatest(raw: *anyopaque, ns: []const u8) !u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.latestLsn(ns);
    }
    fn erasedTruncate(raw: *anyopaque, ns: []const u8, keep: u64) !u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.truncatePrefix(ns, keep);
    }
    fn erasedRange(raw: *anyopaque, a: A, ns: []const u8, range: types.ReadRange) ![]types.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.readRangeAlloc(a, ns, range);
    }
    fn erasedRead(raw: *anyopaque, a: A, ns: []const u8, start: u64) ![]types.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const latest = try self.latestLsn(ns);
        return self.readRangeAlloc(a, ns, .{ .start_lsn = start, .end_lsn = latest, .max_records = std.math.maxInt(usize), .max_bytes = std.math.maxInt(usize) });
    }
};

test "serverless indexed WAL survives reopen, retains idempotency and never resets LSN on truncation" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("test");
    var first = try IndexedObjectStore.init(a, client, "test", "scope");
    defer first.deinit();
    try std.testing.expectEqual(@as(u64, 1), try first.appendIdempotent("docs", 10, "hello", "op-1"));
    try std.testing.expectEqual(@as(u64, 2), try first.append("docs", 20, "world"));
    var second = try IndexedObjectStore.init(a, client, "test", "scope");
    defer second.deinit();
    try std.testing.expectEqual(@as(u64, 2), try second.latestLsn("docs"));
    try std.testing.expectEqual(@as(u64, 1), try second.appendIdempotent("docs", 10, "hello", "op-1"));
    try std.testing.expectError(error.WalIdempotencyConflict, second.appendIdempotent("docs", 10, "other", "op-1"));
    try std.testing.expectEqual(@as(?u64, null), try second.appendIdempotentIfLatest("docs", 30, "later", "op-3", 1));
    const records = try second.readRangeAlloc(a, "docs", .{ .start_lsn = 2, .end_lsn = 2 });
    defer types.freeRecords(a, records);
    try std.testing.expectEqualStrings("world", records[0].payload);
    try std.testing.expectError(error.WalReadBudgetExceeded, second.readRangeAlloc(a, "docs", .{ .start_lsn = 1, .end_lsn = 2, .max_records = 1 }));
    try std.testing.expectError(error.WalReadBudgetExceeded, second.readRangeAlloc(a, "docs", .{ .start_lsn = 1, .end_lsn = 2, .max_bytes = 1 }));
    try std.testing.expectError(error.WalVisibilityUnavailable, second.readRangeAlloc(a, "docs", .{ .start_lsn = 1, .end_lsn = 3 }));
    try std.testing.expectEqual(@as(u64, 2), try second.truncatePrefix("docs", 100));
    try std.testing.expectEqual(@as(u64, 2), try second.latestLsn("docs"));
    try std.testing.expectError(error.WalSnapshotExpired, second.readRangeAlloc(a, "docs", .{ .start_lsn = 1, .end_lsn = 2 }));
    try std.testing.expectEqual(@as(u64, 1), try second.appendIdempotent("docs", 10, "hello", "op-1"));
    try std.testing.expectEqual(@as(u64, 3), try second.append("docs", 30, "later"));
    try std.testing.expectEqual(@as(u64, 0), try second.latestLsn("other-table"));
}
