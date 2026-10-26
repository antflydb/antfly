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
    /// Optional host admission ceiling. Existing receipts bypass this limit.
    max_new_lsn: ?u64 = null,
    /// Enable only after every WAL reader supports HEAD format 3.
    inline_tail_enabled: bool = false,

    /// A bounded copy of the last record is committed by the same HEAD CAS as
    /// both trees. A one-record tail read needs no dependent page fetches.
    pub const InlineTail = struct {
        bytes: [1536]u8 = @splat(0),
        len: u16 = 0,
        fn fromRecord(bytes: []const u8) ?InlineTail {
            if (bytes.len == 0 or bytes.len > 1536) return null;
            var out: InlineTail = .{ .len = @intCast(bytes.len) };
            @memcpy(out.bytes[0..bytes.len], bytes);
            return out;
        }
        pub fn jsonStringify(self: InlineTail, writer: anytype) !void {
            var encoded: [2048]u8 = undefined;
            return writer.write(std.base64.standard.Encoder.encode(&encoded, self.bytes[0..self.len]));
        }
        pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) !InlineTail {
            const text = try std.json.innerParse([]const u8, a, source, options);
            const len = std.base64.standard.Decoder.calcSizeForSlice(text) catch return error.UnexpectedToken;
            if (len == 0 or len > 1536) return error.UnexpectedToken;
            var out: InlineTail = .{ .len = @intCast(len) };
            std.base64.standard.Decoder.decode(out.bytes[0..len], text) catch return error.UnexpectedToken;
            return out;
        }
    };

    pub const Head = struct {
        format: u32 = 2,
        write_epoch: u64 = 1,
        physical_first_lsn: u64 = 1,
        gc_attempt: [16]u8 = @splat(0),
        gc_records: ?tree.Ref = null,
        gc_operations: ?tree.Ref = null,
        gc_retired_before: u64 = 0,
        latest_lsn: u64 = 0,
        first_lsn: u64 = 1,
        records: ?tree.Ref = null,
        operations: ?tree.Ref = null,
        inline_tail: ?InlineTail = null,
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
        for (0..self.max_attempts) |_| {
            var current = try ctx.readHead();
            defer current.deinit(self.alloc);
            ctx.setEpoch(current.value.write_epoch);
            var uploads = tree.BufferedWrites{ .alloc = self.alloc, .underlying = ctx.store() };
            defer uploads.deinit();
            var cache = tree.Cache{ .alloc = self.alloc, .underlying = uploads.store() };
            defer cache.deinit();
            const pages = cache.store();
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
            if (self.max_new_lsn) |maximum| if (next > maximum) return error.PublicationLagExceeded;
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
            updated.inline_tail = if (self.inline_tail_enabled) InlineTail.fromRecord(encoded) else null;
            try uploads.flush();
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
        const results = try self.appendGroupsIdempotent(namespace, &.{.{ .timestamp = timestamp, .payloads = payloads, .id = id }});
        defer self.alloc.free(results);
        return results[0];
    }

    pub const Group = struct { timestamp: u64, payloads: []const []const u8, id: []const u8 };

    /// Share path copies and one HEAD CAS across independent caller receipts.
    /// Existing receipts keep their original interval, including duplicates in
    /// this group. No receipt or record becomes visible before the shared CAS.
    pub fn appendGroupsIdempotent(self: *@This(), namespace: []const u8, groups: []const Group) ![]BatchResult {
        if (groups.len == 0 or groups.len > 16) return error.InvalidWalBatch;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const temporary = arena.allocator();
        const receipts = try temporary.alloc([56]u8, groups.len);
        const ids = try temporary.alloc([]const u8, groups.len);
        var count: usize = 0;
        var total: usize = 0;
        for (groups, receipts, ids) |group, *receipt, *id| {
            if (group.payloads.len == 0 or group.payloads.len > 1024) return error.InvalidWalBatch;
            if (group.id.len == 0 or group.id.len > 4096) return error.InvalidWalOperationId;
            id.* = group.id;
            count = std.math.add(usize, count, group.payloads.len) catch return error.InvalidWalBatch;
            if (count > 1024) return error.InvalidWalBatch;
            var hash = Sha256.init(.{});
            for (group.payloads) |payload| {
                if (payload.len > tree.max_record_bytes - 128) return error.WalRecordTooLarge;
                total = std.math.add(usize, total, payload.len + 32) catch return error.WalRecordTooLarge;
                if (total > 8 * 1024 * 1024) return error.WalRecordTooLarge;
                var len: [8]u8 = undefined;
                std.mem.writeInt(u64, &len, payload.len, .big);
                hash.update(&len);
                hash.update(payload);
            }
            std.mem.writeInt(u64, receipt[16..24], group.timestamp, .little);
            @memcpy(receipt[24..56], &hash.finalResult());
        }
        const results = try self.alloc.alloc(BatchResult, groups.len);
        errdefer self.alloc.free(results);
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        for (0..self.max_attempts) |_| {
            var attempt = std.heap.ArenaAllocator.init(self.alloc);
            defer attempt.deinit();
            const scratch = attempt.allocator();
            var current = try ctx.readHead();
            defer current.deinit(self.alloc);
            ctx.setEpoch(current.value.write_epoch);
            var uploads = tree.BufferedWrites{ .alloc = self.alloc, .underlying = ctx.store() };
            defer uploads.deinit();
            var cache = tree.Cache{ .alloc = self.alloc, .underlying = uploads.store() };
            defer cache.deinit();
            const pages = cache.store();
            const found = try tree.lookupMany(scratch, pages, current.value.operations, ids);
            var changes = std.ArrayList(tree.Mutation).empty;
            var operations = std.ArrayList(tree.Mutation).empty;
            var last = current.value.latest_lsn;
            for (groups, receipts, found, 0..) |group, *receipt, existing, index| {
                var duplicate: ?usize = null;
                for (groups[0..index], 0..) |prior, i| {
                    if (std.mem.eql(u8, prior.id, group.id)) {
                        duplicate = i;
                        break;
                    }
                }
                if (duplicate) |i| {
                    if (!std.mem.eql(u8, receipts[i][16..], receipt[16..])) return error.WalIdempotencyConflict;
                    results[index] = results[i];
                    continue;
                }
                if (existing) |value| {
                    if (value.value.len != receipt.len or !std.mem.eql(u8, value.value[16..], receipt[16..])) return error.WalIdempotencyConflict;
                    const first = std.mem.readInt(u64, value.value[0..8], .little);
                    const end = std.mem.readInt(u64, value.value[8..16], .little);
                    if (first == 0 or end < first or end > current.value.latest_lsn or end - first + 1 != group.payloads.len) return error.InvalidWal;
                    results[index] = .{ .start_lsn = first, .end_lsn = end };
                    continue;
                }
                const first = last + 1;
                last = std.math.add(u64, last, group.payloads.len) catch return error.WalLsnExhausted;
                if (self.max_new_lsn) |maximum| if (last > maximum) return error.PublicationLagExceeded;
                if (last == std.math.maxInt(u64)) return error.WalLsnExhausted;
                for (group.payloads, 0..) |payload, i| {
                    const key = try scratch.create([8]u8);
                    std.mem.writeInt(u64, key, first + i, .big);
                    try changes.append(scratch, .{ .key = key, .value = try codec.encodeRecordAlloc(scratch, first + i, group.timestamp, payload) });
                }
                std.mem.writeInt(u64, receipt[0..8], first, .little);
                std.mem.writeInt(u64, receipt[8..16], last, .little);
                try operations.append(scratch, .{ .key = group.id, .value = receipt });
                results[index] = .{ .start_lsn = first, .end_lsn = last };
            }
            if (changes.items.len == 0) return results;
            var updated = current.value;
            updated.records = try tree.apply(self.alloc, pages, updated.records, changes.items);
            std.mem.sort(tree.Mutation, operations.items, {}, struct {
                fn less(_: void, left: tree.Mutation, right: tree.Mutation) bool {
                    return std.mem.lessThan(u8, left.key, right.key);
                }
            }.less);
            updated.operations = try tree.apply(self.alloc, pages, updated.operations, operations.items);
            updated.latest_lsn = last;
            updated.inline_tail = if (self.inline_tail_enabled) InlineTail.fromRecord(changes.items[changes.items.len - 1].value.?) else null;
            try uploads.flush();
            ctx.writeHead(updated, current.etag) catch |err| switch (err) {
                error.PreconditionFailed => continue,
                else => return err,
            };
            return results;
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
        if (range.start_lsn == head.value.latest_lsn and count == 1) if (head.value.inline_tail) |tail| {
            if (@as(usize, tail.len) + @sizeOf(types.Record) > range.max_bytes) return error.WalReadBudgetExceeded;
            const record = try codec.parseRecord(tail.bytes[0..tail.len], 0);
            const out = try alloc.alloc(types.Record, 1);
            errdefer alloc.free(out);
            const payload = try alloc.dupe(u8, record.payload);
            errdefer alloc.free(payload);
            const id = if (record.operation_id) |value| try alloc.dupe(u8, value) else null;
            errdefer if (id) |value| alloc.free(value);
            try range.check();
            out[0] = .{ .lsn = record.lsn, .timestamp_ns = record.timestamp_ns, .payload = payload, .operation_id = id };
            return out;
        };
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

    /// Remove only a bounded prefix of logically expired records. The receipt
    /// index and high watermark survive compaction. A later epoch barrier and
    /// reader grace period are required before deleting unreachable pages.
    pub fn compactPrefixStep(self: *@This(), namespace: []const u8, max_records: usize) !bool {
        if (max_records == 0 or max_records > 256) return error.InvalidWalBatch;
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        for (0..self.max_attempts) |_| {
            var head = try ctx.readHead();
            defer head.deinit(self.alloc);
            if (head.value.physical_first_lsn == head.value.first_lsn) return false;
            ctx.setEpoch(head.value.write_epoch);
            var uploads = tree.BufferedWrites{ .alloc = self.alloc, .underlying = ctx.store() };
            defer uploads.deinit();
            var cache = tree.Cache{ .alloc = self.alloc, .underlying = uploads.store() };
            defer cache.deinit();
            const pages = cache.store();
            const end = @min(head.value.first_lsn, head.value.physical_first_lsn +| max_records);
            var keys: [256][8]u8 = undefined;
            var changes: [256]tree.Mutation = undefined;
            const count: usize = @intCast(end - head.value.physical_first_lsn);
            for (keys[0..count], changes[0..count], 0..) |*key, *change, i| {
                std.mem.writeInt(u64, key, head.value.physical_first_lsn + i, .big);
                change.* = .{ .key = key, .value = null };
            }
            var updated = head.value;
            updated.records = try tree.apply(self.alloc, pages, head.value.records, changes[0..count]);
            updated.physical_first_lsn = end;
            try uploads.flush();
            ctx.writeHead(updated, head.etag) catch |err| switch (err) {
                error.PreconditionFailed => continue,
                else => return err,
            };
            return end != updated.first_lsn;
        }
        return error.WalCommitContended;
    }

    pub const Collection = struct { records: ?tree.Ref, operations: ?tree.Ref, retired_before: u64, domain: [32]u8 };
    /// Atomically fences every writer that could publish an older upload
    /// attempt. Caller must retain these roots and honor bounded reader leases
    /// before sweeping attempts older than retired_before. Idempotent by job ID.
    pub fn beginCollection(self: *@This(), namespace: []const u8, id: [16]u8) !Collection {
        if (std.mem.allEqual(u8, &id, 0)) return error.InvalidCollectionId;
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        for (0..self.max_attempts) |_| {
            var head = try ctx.readHead();
            defer head.deinit(self.alloc);
            var next = head.value;
            if (!std.mem.eql(u8, &next.gc_attempt, &id)) {
                next.write_epoch = std.math.add(u64, next.write_epoch, 1) catch return error.WalLsnExhausted;
                next.gc_attempt = id;
                next.gc_records = next.records;
                next.gc_operations = next.operations;
                next.gc_retired_before = next.write_epoch;
                ctx.writeHead(next, head.etag) catch |err| switch (err) {
                    error.PreconditionFailed => continue,
                    else => return err,
                };
            }
            return .{ .records = next.gc_records, .operations = next.gc_operations, .retired_before = next.gc_retired_before, .domain = ctx.domain };
        }
        return error.WalCommitContended;
    }

    pub fn tracePage(self: *@This(), alloc: A, namespace: []const u8, ref: tree.Ref) !tree.Page {
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        return tree.readPage(alloc, ctx.store(), ref);
    }
    pub fn pageKeyAlloc(self: *@This(), alloc: A, namespace: []const u8, ref: tree.Ref) ![]u8 {
        var ctx = try Context.init(self, namespace, null);
        defer ctx.deinit();
        return ctx.pageKey(alloc, ref);
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
        attempt: [16]u8 = @splat(0),

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
        fn setEpoch(self: *@This(), epoch: u64) void {
            std.mem.writeInt(u64, self.attempt[0..8], epoch, .big);
        }
        fn store(self: *@This()) tree.Store {
            return .{ .ptr = self, .domain = self.domain, .get = getPage, .put = putPage, .put_many = putPages, .check = check, .prefix_keys = true, .attempt = self.attempt };
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
            if ((head.format != 1 and head.format != 2 and head.format != 3) or head.write_epoch == 0 or head.physical_first_lsn == 0 or head.physical_first_lsn > head.first_lsn or head.first_lsn == 0 or head.latest_lsn == std.math.maxInt(u64) or
                head.first_lsn > head.latest_lsn + 1 or (head.latest_lsn >= head.physical_first_lsn and head.records == null)) return error.InvalidWal;
            if ((head.format == 3) != (head.inline_tail != null)) return error.InvalidWal;
            if (head.inline_tail) |tail| {
                const record = codec.parseRecord(tail.bytes[0..tail.len], 0) catch return error.InvalidWal;
                if (record.lsn != head.latest_lsn or record.next_cursor != tail.len or head.first_lsn > head.latest_lsn) return error.InvalidWal;
            }
            if (head.records) |ref| {
                try ref.validate();
                if (ref.records != head.latest_lsn + 1 - head.physical_first_lsn) return error.InvalidWal;
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
            var upgraded = head;
            if (upgraded.first_lsn > upgraded.latest_lsn) upgraded.inline_tail = null;
            upgraded.format = if (upgraded.inline_tail != null) 3 else 2;
            var bytes = try std.json.Stringify.valueAlloc(a, upgraded, .{ .emit_null_optional_fields = false });
            if (bytes.len > 4096 and upgraded.inline_tail != null) {
                a.free(bytes);
                upgraded.inline_tail = null;
                upgraded.format = 2;
                bytes = try std.json.Stringify.valueAlloc(a, upgraded, .{ .emit_null_optional_fields = false });
            }
            defer a.free(bytes);
            if (bytes.len > 4096) return error.WalHeadTooLarge;
            var result = try self.owner.client.putObject(self.owner.bucket, key, bytes, .{
                .if_none_match = etag == null,
                .if_match_etag = etag,
                .content_type = "application/json",
                .cancellation = self.cancellation,
            });
            defer result.deinit(a);
        }
        fn pageKey(self: *@This(), a: A, ref: tree.Ref) ![]u8 {
            if (std.mem.allEqual(u8, &ref.attempt, 0)) return std.fmt.allocPrint(a, "{s}/pages/{s}", .{ self.root, std.fmt.bytesToHex(ref.digest, .lower) });
            return std.fmt.allocPrint(a, "{s}/pages/{s}/{s}", .{ self.root, std.fmt.bytesToHex(ref.attempt, .lower), std.fmt.bytesToHex(ref.digest, .lower) });
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
        fn putPages(raw: *anyopaque, writes: []const tree.Write) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (writes.len == 0 or writes.len > tree.max_write_batch) return error.InvalidObjectBatch;
            const a = self.owner.alloc;
            var requests: [tree.max_write_batch]objectstore.types.PutObjectRequest = undefined;
            var initialized: usize = 0;
            defer for (requests[0..initialized]) |item| a.free(item.key);
            for (writes, requests[0..writes.len]) |write, *request| {
                try check(raw);
                request.* = .{ .key = try self.pageKey(a, write.ref), .body = write.bytes, .options = .{ .if_none_match = true, .cancellation = self.cancellation } };
                initialized += 1;
            }
            const results = self.owner.client.putObjects(self.owner.bucket, requests[0..writes.len]) catch |err| switch (err) {
                error.PreconditionFailed => {
                    for (writes) |write| try putPage(raw, write.ref, write.bytes);
                    return;
                },
                else => return err,
            };
            defer {
                for (results) |*result| result.deinit(a);
                a.free(results);
            }
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

test "serverless indexed WAL serves a validated bounded last record without page reads" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var wal = try IndexedObjectStore.init(a, client, "db", "tail");
    defer wal.deinit();
    wal.inline_tail_enabled = true;
    _ = try wal.appendIdempotent("docs", 10, "first", "op-1");
    _ = try wal.appendIdempotent("docs", 20, "second", "op-2");
    memory.resetOperationCount();
    const records = try wal.readRangeAlloc(a, "docs", .{ .start_lsn = 2, .end_lsn = 2 });
    defer types.freeRecords(a, records);
    try std.testing.expectEqual(@as(u64, 1), memory.operationCount());
    try std.testing.expectEqualStrings("second", records[0].payload);
    try std.testing.expectEqualStrings("op-2", records[0].operation_id.?);
    try std.testing.expectEqual(@as(u64, 20), records[0].timestamp_ns);
    try std.testing.expectError(error.WalReadBudgetExceeded, wal.readRangeAlloc(a, "docs", .{ .start_lsn = 2, .end_lsn = 2, .max_bytes = 1 }));
    try std.testing.expectError(error.WalReadBudgetExceeded, wal.readRangeAlloc(a, "docs", .{ .start_lsn = 2, .end_lsn = 2, .max_records = 0 }));
    const large: [2048]u8 = @splat('x');
    _ = try wal.append("docs", 30, &large);
    memory.resetOperationCount();
    const fallback = try wal.readRangeAlloc(a, "docs", .{ .start_lsn = 3, .end_lsn = 3 });
    defer types.freeRecords(a, fallback);
    try std.testing.expect(memory.operationCount() > 1);
    try std.testing.expectEqualStrings(&large, fallback[0].payload);
    _ = try wal.append("docs", 40, "small again");
    var ctx = try IndexedObjectStore.Context.init(&wal, "docs", null);
    defer ctx.deinit();
    var snapshot = try ctx.readHead();
    defer snapshot.deinit(a);
    try std.testing.expectEqual(@as(u32, 3), snapshot.value.format);
    snapshot.value.inline_tail.?.bytes[0] ^= 1;
    const bytes = try std.json.Stringify.valueAlloc(a, snapshot.value, .{ .emit_null_optional_fields = false });
    defer a.free(bytes);
    try std.testing.expect(bytes.len <= 4096);
    const key = try std.fmt.allocPrint(a, "{s}/HEAD", .{ctx.root});
    defer a.free(key);
    var receipt = try client.putObject("db", key, bytes, .{});
    defer receipt.deinit(a);
    try std.testing.expectError(error.InvalidWal, wal.latestLsn("docs"));
}

test "serverless indexed WAL inline tail read allocation failures release ownership" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var wal = try IndexedObjectStore.init(a, client, "db", "tail-allocation");
    defer wal.deinit();
    wal.inline_tail_enabled = true;
    _ = try wal.appendIdempotent("docs", 1, "body", "op");
    const Check = struct {
        fn run(alloc: A, storage: objectstore.Client) !void {
            var reader = try IndexedObjectStore.init(alloc, storage, "db", "tail-allocation");
            defer reader.deinit();
            const result = try reader.readRangeAlloc(alloc, "docs", .{ .start_lsn = 1, .end_lsn = 1 });
            defer types.freeRecords(alloc, result);
            try std.testing.expectEqualStrings("body", result[0].payload);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{client});
}

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

test "serverless indexed WAL compaction preserves receipts and collection barriers are retryable" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var wal = try IndexedObjectStore.init(a, client, "db", "prefix");
    defer wal.deinit();
    for (0..10) |i| {
        var id: [20]u8 = undefined;
        _ = try wal.appendIdempotent("table", 0, "body", try std.fmt.bufPrint(&id, "op-{d}", .{i}));
    }
    _ = try wal.truncatePrefix("table", 9);
    try std.testing.expect(try wal.compactPrefixStep("table", 3));
    try std.testing.expect(try wal.compactPrefixStep("table", 3));
    try std.testing.expect(!try wal.compactPrefixStep("table", 3));
    const snapshot = try wal.beginCollection("table", @splat(1));
    try std.testing.expectEqual(@as(u64, 2), snapshot.records.?.records);
    try std.testing.expectEqual(@as(u64, 10), snapshot.operations.?.records);
    const same = try wal.beginCollection("table", @splat(1));
    try std.testing.expectEqual(snapshot.retired_before, same.retired_before);
    try std.testing.expectEqual(@as(u64, 1), try wal.appendIdempotent("table", 0, "body", "op-0"));
    try std.testing.expectEqual(@as(u64, 11), try wal.append("table", 0, "next"));
    const next = try wal.beginCollection("table", @splat(2));
    try std.testing.expect(next.retired_before > same.retired_before);
    var page = try wal.tracePage(a, "table", next.records.?);
    defer page.deinit(a);
    _ = try wal.truncatePrefix("table", 100);
    try std.testing.expect(!try wal.compactPrefixStep("table", 256));
    try std.testing.expectEqual(@as(u64, 12), try wal.append("table", 0, "after-empty"));
}

test "serverless grouped WAL preserves independent receipts and rejects an entire conflicting group" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("test");
    var wal = try IndexedObjectStore.init(a, client, "test", "scope");
    defer wal.deinit();
    const groups = [_]IndexedObjectStore.Group{
        .{ .id = "z", .timestamp = 1, .payloads = &.{ "first", "second" } },
        .{ .id = "b", .timestamp = 2, .payloads = &.{"third"} },
        .{ .id = "z", .timestamp = 1, .payloads = &.{ "first", "second" } },
    };
    const results = try wal.appendGroupsIdempotent("docs", &groups);
    defer a.free(results);
    try std.testing.expectEqual(@as(u64, 2), results[0].end_lsn);
    try std.testing.expectEqual(@as(u64, 3), results[1].end_lsn);
    try std.testing.expectEqual(results[0], results[2]);
    wal.max_new_lsn = 0;
    const replay = try wal.appendGroupsIdempotent("docs", &groups);
    defer a.free(replay);
    try std.testing.expectEqualSlices(IndexedObjectStore.BatchResult, results, replay);
    wal.max_new_lsn = null;
    try std.testing.expectError(error.WalIdempotencyConflict, wal.appendGroupsIdempotent("docs", &.{
        .{ .id = "new", .timestamp = 3, .payloads = &.{"invisible"} },
        .{ .id = "b", .timestamp = 2, .payloads = &.{"conflict"} },
    }));
    try std.testing.expectEqual(@as(u64, 3), try wal.latestLsn("docs"));
    const next = try wal.appendBatchIdempotent("docs", 3, &.{"invisible"}, "new");
    try std.testing.expectEqual(@as(u64, 4), next.end_lsn);
}
