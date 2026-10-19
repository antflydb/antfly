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

//! Durable HA replication record log.
//!
//! This layer stores versioned HA replication records inside the generic storage
//! WAL and enforces that the record envelope LSN matches the durable WAL LSN.
//! Later streaming, slot, base-backup catch-up, and standby receive/apply paths
//! should use this API instead of appending raw bytes directly.

const std = @import("std");
const Allocator = std.mem.Allocator;
const replication_record = @import("antfly_local_sources").storage_db_replication_record;
const wal_mod = @import("../wal_runtime.zig");

var test_path_counter: u64 = 0;

pub const OpenOptions = struct {
    wal_options: wal_mod.WalOptions = .{},
};

pub const Entry = struct {
    wal_lsn: u64,
    encoded: []const u8,
    record: replication_record.RecordView,

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.encoded);
        self.* = undefined;
    }
};

/// A synchronous borrow, never an owned entry. Encoded and payload slices
/// expire when the visiting callback returns.
pub const EntryView = struct {
    wal_lsn: u64,
    encoded: []const u8,
    record: replication_record.RecordView,
};

fn decodeEntry(wal_entry: wal_mod.WalEntry) !replication_record.RecordView {
    return decodeEntryWithAdmission(wal_entry, null);
}

fn decodeEntryWithAdmission(wal_entry: wal_mod.WalEntry, control: ?@import("antfly_local_sources").common_read_admission.Port) !replication_record.RecordView {
    const decoded = try replication_record.decodeWithAdmission(wal_entry.data, control);
    if (decoded.lsn != wal_entry.lsn) return error.RecordWalLsnMismatch;
    const next = std.math.add(u64, decoded.previous_lsn, 1) catch return error.RecordPreviousLsnMismatch;
    if (next != decoded.lsn) return error.RecordPreviousLsnMismatch;
    return decoded;
}

pub const ReplicationLog = struct {
    wal: wal_mod.WAL,

    pub fn open(path: [*:0]const u8, options: OpenOptions) !ReplicationLog {
        return openWithAllocator(std.heap.page_allocator, path, options);
    }

    pub fn openWithAllocator(allocator: Allocator, path: [*:0]const u8, options: OpenOptions) !ReplicationLog {
        return .{
            .wal = try wal_mod.WAL.openWithAllocator(allocator, path, options.wal_options),
        };
    }

    pub fn close(self: *ReplicationLog) void {
        self.wal.close();
        self.* = undefined;
    }

    pub fn lastLsn(self: *const ReplicationLog) u64 {
        return self.wal.lastLsn();
    }

    pub fn nextLsn(self: *const ReplicationLog) u64 {
        return self.lastLsn() + 1;
    }

    pub fn append(self: *ReplicationLog, alloc: Allocator, record: replication_record.Record) !u64 {
        const expected_lsn = self.nextLsn();
        if (record.lsn != expected_lsn) return error.UnexpectedRecordLsn;
        if (record.previous_lsn != expected_lsn - 1) return error.UnexpectedPreviousLsn;

        const encoded = try replication_record.encodeAlloc(alloc, record);
        defer alloc.free(encoded);

        const wal_lsn = try self.wal.append(encoded);
        if (wal_lsn != record.lsn) return error.WalLsnMismatch;
        return wal_lsn;
    }

    /// Both encoding buffers and tail CRC work were funded before commit.
    /// Backend working memory and I/O admission remain independent resources.
    pub fn appendPrepared(self: *ReplicationLog, prepared: *replication_record.Prepared, wal_entry: *wal_mod.WAL.PreparedAppend, metadata: replication_record.Record) !u64 {
        const expected_lsn = self.nextLsn();
        if (metadata.lsn != expected_lsn) return error.UnexpectedRecordLsn;
        if (metadata.previous_lsn != expected_lsn - 1) return error.UnexpectedPreviousLsn;
        const encoded = try prepared.seal(metadata);
        const wal_lsn = try wal_entry.publish(encoded[0..replication_record.header_size]);
        if (wal_lsn != metadata.lsn) return error.WalLsnMismatch;
        return wal_lsn;
    }

    pub fn appendIfRecoveryPosition(self: *ReplicationLog, alloc: Allocator, position: wal_mod.RecoveryPosition, record: replication_record.Record, original: @import("antfly_local_sources").common_read_admission.Port) !wal_mod.RecoveryAppendResult {
        var retained: @import("antfly_local_sources").common_read_admission.RetainedDependency = .{};
        const control = retained.bind(original).?;
        try control.checkpoint();
        if (position.next_lsn == 0 or record.lsn != position.next_lsn) return error.UnexpectedRecordLsn;
        if (record.previous_lsn != position.next_lsn - 1) return error.UnexpectedPreviousLsn;
        const encoded = try replication_record.encodeAlloc(alloc, record);
        defer alloc.free(encoded);
        try control.checkpoint();
        return self.wal.appendIfRecoveryPosition(position, encoded, control);
    }

    pub fn bootstrapAt(self: *ReplicationLog, alloc: Allocator, record: replication_record.Record) !u64 {
        if (self.lastLsn() != 0) return error.ReplicationLogNotEmpty;
        if (record.lsn == 0) return error.InvalidBootstrapLsn;
        if (record.previous_lsn != record.lsn - 1) return error.UnexpectedPreviousLsn;

        const encoded = try replication_record.encodeAlloc(alloc, record);
        defer alloc.free(encoded);
        return wal_mod.appendAt(&self.wal, record.lsn, encoded);
    }

    pub fn iterateFrom(self: *ReplicationLog, alloc: Allocator, from_lsn: u64) ![]Entry {
        const wal_entries = try self.wal.iterateFrom(alloc, from_lsn);
        defer {
            for (wal_entries) |entry| alloc.free(entry.data);
            alloc.free(wal_entries);
        }

        var results = std.ArrayListUnmanaged(Entry).empty;
        errdefer {
            for (results.items) |*entry| entry.deinit(alloc);
            results.deinit(alloc);
        }

        for (wal_entries) |wal_entry| {
            _ = try decodeEntry(wal_entry);

            const owned = try alloc.dupe(u8, wal_entry.data);
            errdefer alloc.free(owned);
            const owned_decoded = try replication_record.decode(owned);
            try results.append(alloc, .{
                .wal_lsn = wal_entry.lsn,
                .encoded = owned,
                .record = owned_decoded,
            });
        }

        return try results.toOwnedSlice(alloc);
    }

    pub fn entryAt(self: *ReplicationLog, alloc: Allocator, lsn: u64) !?Entry {
        const wal_entry = (try self.wal.readAt(alloc, lsn)) orelse return null;
        errdefer alloc.free(wal_entry.data);
        const decoded = try decodeEntry(wal_entry);
        return .{
            .wal_lsn = wal_entry.lsn,
            .encoded = wal_entry.data,
            .record = decoded,
        };
    }

    /// Decode and inspect an exact retained record while its WAL bytes remain
    /// borrowed. This preserves entryAt's envelope/continuity validation while
    /// removing the caller's payload-sized allocation from read-only matching.
    pub fn withEntryAt(self: *ReplicationLog, lsn: u64, context: anytype, comptime callback: fn (@TypeOf(context), EntryView) anyerror!void) !bool {
        return self.withEntryAtWithAdmission(lsn, null, context, callback);
    }

    pub fn withEntryAtWithAdmission(self: *ReplicationLog, lsn: u64, original: ?@import("antfly_local_sources").common_read_admission.Port, context: anytype, comptime callback: fn (@TypeOf(context), EntryView) anyerror!void) !bool {
        var retained: @import("antfly_local_sources").common_read_admission.RetainedDependency = .{};
        const control = retained.bind(original);
        const Adapter = struct {
            user: @TypeOf(context),
            control: ?@import("antfly_local_sources").common_read_admission.Port,
            fn visit(self_adapter: *@This(), entry: wal_mod.WalEntry) !void {
                const decoded = try decodeEntryWithAdmission(entry, self_adapter.control);
                try callback(self_adapter.user, .{ .wal_lsn = entry.lsn, .encoded = entry.data, .record = decoded });
            }
        };
        var adapter: Adapter = .{ .user = context, .control = control };
        return self.wal.withEntryAtWithAdmission(lsn, control, &adapter, Adapter.visit);
    }

    pub fn encodedByteCount(self: *ReplicationLog, from_lsn: u64, to_lsn: u64) !u64 {
        if (from_lsn == 0 or to_lsn < from_lsn) return 0;
        var context = EncodedByteCountContext{ .to_lsn = to_lsn };
        try self.wal.iterateFromStreamingWithContext(from_lsn, &context, countEncodedBytes);
        return context.total;
    }

    pub fn retainedAgeNs(self: *ReplicationLog, from_lsn: u64, to_lsn: u64) !u64 {
        if (from_lsn == 0 or to_lsn < from_lsn) return 0;
        var context = RetainedAgeContext{ .to_lsn = to_lsn };
        try self.wal.iterateFromStreamingWithContext(from_lsn, &context, scanRetainedAge);
        if (!context.has_timestamp) return 0;
        return @intCast(context.max_timestamp_ns - context.min_timestamp_ns);
    }

    pub fn truncate(self: *ReplicationLog, up_to_lsn: u64) !void {
        try self.wal.truncate(up_to_lsn);
    }

    pub fn truncateAfter(self: *ReplicationLog, keep_lsn: u64) !void {
        try self.wal.truncateAfter(keep_lsn);
    }
};

const EncodedByteCountContext = struct {
    to_lsn: u64,
    total: u64 = 0,
};

const RetainedAgeContext = struct {
    to_lsn: u64,
    min_timestamp_ns: i64 = std.math.maxInt(i64),
    max_timestamp_ns: i64 = std.math.minInt(i64),
    has_timestamp: bool = false,
};

fn countEncodedBytes(context: *EncodedByteCountContext, entry: wal_mod.WalEntry) !wal_mod.WAL.ScanAction {
    if (entry.lsn > context.to_lsn) return .stop;
    _ = try decodeEntry(entry);
    context.total += @intCast(entry.data.len);
    return .@"continue";
}

fn scanRetainedAge(context: *RetainedAgeContext, entry: wal_mod.WalEntry) !wal_mod.WAL.ScanAction {
    if (entry.lsn > context.to_lsn) return .stop;
    const decoded = try decodeEntry(entry);
    if (decoded.commit_timestamp_ns > 0) {
        context.has_timestamp = true;
        context.min_timestamp_ns = @min(context.min_timestamp_ns, decoded.commit_timestamp_ns);
        context.max_timestamp_ns = @max(context.max_timestamp_ns, decoded.commit_timestamp_ns);
    }
    return .@"continue";
}

pub fn freeEntries(alloc: Allocator, entries: []Entry) void {
    for (entries) |*entry| entry.deinit(alloc);
    alloc.free(entries);
}

fn testPath(alloc: Allocator, comptime name: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-replication-log-" ++ name ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(raw);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), raw) catch {};
    return try alloc.dupeSentinel(u8, raw, 0);
}

fn baseRecord(lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .cluster_id = 100,
        .table_id = 10,
        .shard_id = 20,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

test "storage.hot_standby replication log appends and iterates records in wal order" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "append-iterate");
    defer alloc.free(path);

    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();

    try std.testing.expectEqual(@as(u64, 1), log.nextLsn());
    try std.testing.expectEqual(@as(u64, 1), try log.append(alloc, baseRecord(1, "one")));
    try std.testing.expectEqual(@as(u64, 2), try log.append(alloc, baseRecord(2, "two")));

    const entries = try log.iterateFrom(alloc, 1);
    defer freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].wal_lsn);
    try std.testing.expectEqual(@as(u64, 1), entries[0].record.lsn);
    try std.testing.expectEqualStrings("one", entries[0].record.payload);
    try std.testing.expectEqual(@as(u64, 2), entries[1].wal_lsn);
    try std.testing.expectEqual(@as(u64, 2), entries[1].record.lsn);
    try std.testing.expectEqualStrings("two", entries[1].record.payload);
}

test "storage.hot_standby replication log reads retained entries by exact lsn" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "entry-at");
    defer alloc.free(path);

    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();

    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));

    var entry = (try log.entryAt(alloc, 2)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), entry.wal_lsn);
    try std.testing.expectEqual(@as(u64, 2), entry.record.lsn);
    try std.testing.expectEqualStrings("two", entry.record.payload);

    try log.truncate(1);
    try std.testing.expect((try log.entryAt(alloc, 1)) == null);
}

test "storage.hot_standby replication log survives reopen and keeps next lsn" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "reopen");
    defer alloc.free(path);

    {
        var log = try ReplicationLog.open(path.ptr, .{});
        defer log.close();
        _ = try log.append(alloc, baseRecord(1, "before"));
    }

    {
        var reopened = try ReplicationLog.open(path.ptr, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 1), reopened.lastLsn());
        try std.testing.expectEqual(@as(u64, 2), reopened.nextLsn());
        _ = try reopened.append(alloc, baseRecord(2, "after"));

        const entries = try reopened.iterateFrom(alloc, 2);
        defer freeEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqualStrings("after", entries[0].record.payload);
    }
}

test "storage.hot_standby replication log rejects non-contiguous records" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "contiguous");
    defer alloc.free(path);

    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();

    try std.testing.expectError(error.UnexpectedRecordLsn, log.append(alloc, baseRecord(2, "gap")));
    var bad_previous = baseRecord(1, "bad-prev");
    bad_previous.previous_lsn = 9;
    try std.testing.expectError(error.UnexpectedPreviousLsn, log.append(alloc, bad_previous));
}

test "storage.hot_standby replication log detects corrupt durable entries" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "corrupt");
    defer alloc.free(path);

    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();

    var corrupt: [replication_record.header_size]u8 = undefined;
    @memset(&corrupt, 'x');
    _ = try log.wal.append(&corrupt);
    try std.testing.expectError(error.InvalidMagic, log.iterateFrom(alloc, 1));
}

test "storage.hot_standby replication log truncates acknowledged entries" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "truncate");
    defer alloc.free(path);

    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();

    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));
    _ = try log.append(alloc, baseRecord(3, "three"));
    try log.truncate(2);

    const entries = try log.iterateFrom(alloc, 1);
    defer freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 3), entries[0].record.lsn);
    try std.testing.expectEqualStrings("three", entries[0].record.payload);
}

test "storage.hot_standby replication log counts encoded retained bytes without materializing entries" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "encoded-byte-count");
    defer alloc.free(path);

    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();

    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));
    _ = try log.append(alloc, baseRecord(3, "three"));

    const entries = try log.iterateFrom(alloc, 2);
    defer freeEntries(alloc, entries);
    var expected: u64 = 0;
    for (entries) |entry| {
        if (entry.wal_lsn <= 3) expected += @intCast(entry.encoded.len);
    }

    try std.testing.expectEqual(expected, try log.encodedByteCount(2, 3));
    try std.testing.expectEqual(@as(u64, 0), try log.encodedByteCount(4, 3));
    try std.testing.expectEqual(@as(u64, 0), try log.encodedByteCount(0, 3));
}

test "storage.hot_standby replication log computes retained age from record timestamps" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "retained-age");
    defer alloc.free(path);

    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();

    var first = baseRecord(1, "one");
    first.commit_timestamp_ns = 100;
    var second = baseRecord(2, "two");
    second.commit_timestamp_ns = 250;
    var third = baseRecord(3, "three");
    third.commit_timestamp_ns = 500;
    _ = try log.append(alloc, first);
    _ = try log.append(alloc, second);
    _ = try log.append(alloc, third);

    try std.testing.expectEqual(@as(u64, 400), try log.retainedAgeNs(1, 3));
    try std.testing.expectEqual(@as(u64, 250), try log.retainedAgeNs(2, 3));
    try std.testing.expectEqual(@as(u64, 0), try log.retainedAgeNs(4, 3));
}

test "storage.hot_standby replication log truncates divergent suffix and reuses next lsn" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "truncate-suffix");
    defer alloc.free(path);

    {
        var log = try ReplicationLog.open(path.ptr, .{});
        defer log.close();

        _ = try log.append(alloc, baseRecord(1, "one"));
        _ = try log.append(alloc, baseRecord(2, "two"));
        _ = try log.append(alloc, baseRecord(3, "divergent-three"));
        _ = try log.append(alloc, baseRecord(4, "divergent-four"));
        try std.testing.expectEqual(@as(u64, 4), log.lastLsn());

        try log.truncateAfter(2);
        try std.testing.expectEqual(@as(u64, 2), log.lastLsn());
        try std.testing.expect((try log.entryAt(alloc, 3)) == null);
        try std.testing.expectEqual(@as(u64, 3), try log.append(alloc, baseRecord(3, "new-three")));
    }

    {
        var reopened = try ReplicationLog.open(path.ptr, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 3), reopened.lastLsn());
        try std.testing.expectEqual(@as(u64, 4), reopened.nextLsn());

        const entries = try reopened.iterateFrom(alloc, 1);
        defer freeEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 3), entries.len);
        try std.testing.expectEqualStrings("one", entries[0].record.payload);
        try std.testing.expectEqualStrings("two", entries[1].record.payload);
        try std.testing.expectEqualStrings("new-three", entries[2].record.payload);
    }
}

test "storage.hot_standby replication log bootstraps an empty log at a checkpoint lsn" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "bootstrap");
    defer alloc.free(path);

    {
        var log = try ReplicationLog.open(path.ptr, .{});
        defer log.close();
        var record = baseRecord(10, "checkpoint");
        record.kind = .checkpoint;
        try std.testing.expectEqual(@as(u64, 10), try log.bootstrapAt(alloc, record));
        try std.testing.expectEqual(@as(u64, 11), log.nextLsn());
        try std.testing.expectError(error.ReplicationLogNotEmpty, log.bootstrapAt(alloc, record));
    }

    {
        var reopened = try ReplicationLog.open(path.ptr, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 10), reopened.lastLsn());
        try std.testing.expectEqual(@as(u64, 11), reopened.nextLsn());
        const entries = try reopened.iterateFrom(alloc, 1);
        defer freeEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqual(@as(u64, 10), entries[0].record.lsn);
        try std.testing.expectEqual(replication_record.RecordKind.checkpoint, entries[0].record.kind);
    }
}

test "WAL borrowed replication entry preserves envelope checks and callback failures" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "borrowed-entry");
    defer alloc.free(path);
    var log = try ReplicationLog.open(path.ptr, .{});
    defer log.close();
    _ = try log.append(alloc, baseRecord(1, "one"));
    const Visit = struct {
        calls: usize = 0,
        reject: bool = false,
        fn run(self: *@This(), entry: EntryView) !void {
            try std.testing.expectEqual(@as(u64, 1), entry.wal_lsn);
            try std.testing.expectEqualStrings("one", entry.record.payload);
            try std.testing.expectEqualSlices(u8, entry.encoded[replication_record.header_size..], entry.record.payload);
            self.calls += 1;
            if (self.reject) return error.InjectedWalVisitRejection;
        }
    };
    var visit: Visit = .{};
    try std.testing.expect(try log.withEntryAt(1, &visit, Visit.run));
    try std.testing.expect(!try log.withEntryAt(99, &visit, Visit.run));
    visit.reject = true;
    try std.testing.expectError(error.InjectedWalVisitRejection, log.withEntryAt(1, &visit, Visit.run));
    try std.testing.expectEqual(@as(usize, 2), visit.calls);
    // A valid outer WAL checksum cannot excuse a corrupt logical envelope.
    const bad = try replication_record.encodeAlloc(alloc, baseRecord(2, "bad"));
    defer alloc.free(bad);
    bad[replication_record.header_size] ^= 1;
    _ = try log.wal.append(bad);
    try std.testing.expectError(error.PayloadCrcMismatch, log.withEntryAt(2, &visit, Visit.run));
    // Overflowing predecessor metadata is a typed corruption error, not a trap.
    var invalid = baseRecord(3, "invalid predecessor");
    invalid.previous_lsn = std.math.maxInt(u64);
    const overflow = try replication_record.encodeAlloc(alloc, invalid);
    defer alloc.free(overflow);
    _ = try log.wal.append(overflow);
    try std.testing.expectError(error.RecordPreviousLsnMismatch, log.withEntryAt(3, &visit, Visit.run));
    try std.testing.expectError(error.RecordPreviousLsnMismatch, log.entryAt(alloc, 3));
    try std.testing.expectError(error.RecordPreviousLsnMismatch, log.iterateFrom(alloc, 3));
    try std.testing.expectError(error.RecordPreviousLsnMismatch, log.encodedByteCount(3, 3));
    try std.testing.expectError(error.RecordPreviousLsnMismatch, log.retainedAgeNs(3, 3));
    try std.testing.expectEqual(@as(usize, 2), visit.calls);
}
