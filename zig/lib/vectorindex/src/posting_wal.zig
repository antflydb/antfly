// Copyright 2026 Antfly, Inc.
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

//! Framed write-ahead log for an immutable posting-segment store.
//!
//! A batch becomes query-visible only at its commit frame. Replay ignores a
//! complete or partial uncommitted tail, but rejects corruption in every
//! complete frame. The checksum covers record metadata as well as payload so
//! a damaged posting id, sequence, or commit marker cannot be accepted.

const std = @import("std");
const Allocator = std.mem.Allocator;
const posting_segment = @import("posting_segment.zig");

pub const PostingId = posting_segment.PostingId;

const magic: u32 = 0x41465057; // AFPW
const version: u16 = 2;
const min_supported_version: u16 = 1;
const frame_header_len: usize = 48;
const checksum_offset: usize = 12;
const checksum_body_offset: usize = 16;
const max_frame_len: usize = 256 * 1024 * 1024;

pub const RecordKind = enum(u8) {
    base = 1,
    mutation = 2,
    centroid_directory = 3,
    quantized_checkpoint = 4,
    tombstone = 5,
    posting_state = 6,
    commit = 255,
};

pub const Record = struct {
    kind: RecordKind,
    batch_id: u64,
    posting_id: PostingId,
    source_sequence: u64,
    payload: []const u8,
};

pub const Writer = struct {
    alloc: Allocator,
    out: std.ArrayListUnmanaged(u8) = .empty,
    open_batch: ?u64 = null,
    open_batch_records: usize = 0,
    open_batch_max_sequence: u64 = 0,
    last_committed_batch: ?u64 = null,
    covered_source_sequence: u64 = 0,

    pub fn init(alloc: Allocator) Writer {
        return .{ .alloc = alloc };
    }

    /// Starts a frame encoder after an already durable committed prefix. The
    /// returned writer emits only new frames while preserving global batch and
    /// source-sequence ordering checks.
    pub fn initAfterCommitted(alloc: Allocator, last_committed_batch: ?u64, covered_source_sequence: u64) Writer {
        return .{
            .alloc = alloc,
            .last_committed_batch = last_committed_batch,
            .covered_source_sequence = covered_source_sequence,
        };
    }

    pub fn deinit(self: *Writer) void {
        self.out.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn bytes(self: *const Writer) []const u8 {
        return self.out.items;
    }

    pub fn append(
        self: *Writer,
        kind: RecordKind,
        batch_id: u64,
        posting_id: PostingId,
        source_sequence: u64,
        payload: []const u8,
    ) !void {
        if (kind == .commit) return error.InvalidPostingWalRecord;
        const starts_batch = self.open_batch == null;
        if (self.open_batch) |open_batch| {
            if (batch_id != open_batch) return error.InterleavedPostingWalBatch;
            if (source_sequence < self.open_batch_max_sequence) return error.OutOfOrderPostingWalSequence;
        } else {
            if (self.last_committed_batch) |last| {
                if (batch_id <= last) return error.OutOfOrderPostingWalBatch;
            }
            if (source_sequence < self.covered_source_sequence) return error.OutOfOrderPostingWalSequence;
        }
        try appendFrame(self.alloc, &self.out, .{
            .kind = kind,
            .batch_id = batch_id,
            .posting_id = posting_id,
            .source_sequence = source_sequence,
            .payload = payload,
        });
        if (starts_batch) self.open_batch = batch_id;
        self.open_batch_records += 1;
        self.open_batch_max_sequence = @max(self.open_batch_max_sequence, source_sequence);
    }

    pub fn commit(self: *Writer, batch_id: u64, covered_source_sequence: u64) !void {
        if (self.open_batch == null or self.open_batch.? != batch_id or self.open_batch_records == 0) {
            return error.InvalidPostingWalCommit;
        }
        if (covered_source_sequence < self.open_batch_max_sequence) return error.InvalidPostingWalCommit;
        try appendFrame(self.alloc, &self.out, .{
            .kind = .commit,
            .batch_id = batch_id,
            .posting_id = 0,
            .source_sequence = covered_source_sequence,
            .payload = &.{},
        });
        self.last_committed_batch = batch_id;
        self.covered_source_sequence = covered_source_sequence;
        self.open_batch = null;
        self.open_batch_records = 0;
        self.open_batch_max_sequence = 0;
    }
};

pub const Replay = struct {
    pub const Resolution = union(enum) {
        missing,
        tombstone,
        value: Record,
    };

    const Latest = union(enum) {
        tombstone,
        record_index: usize,
    };

    alloc: Allocator,
    records: std.ArrayListUnmanaged(Record) = .empty,
    posting_order: std.ArrayListUnmanaged(usize) = .empty,
    latest_by_kind: std.AutoHashMapUnmanaged(u128, Latest) = .empty,
    valid_bytes: usize = 0,
    committed_bytes: usize = 0,
    last_committed_batch: ?u64 = null,
    covered_source_sequence: u64 = 0,

    pub fn parse(alloc: Allocator, bytes: []const u8) !Replay {
        var replay: Replay = .{ .alloc = alloc };
        errdefer replay.deinit();

        var offset: usize = 0;
        var open_batch: ?u64 = null;
        var open_batch_start: usize = 0;
        var open_batch_records: usize = 0;
        var open_batch_max_sequence: u64 = 0;

        while (offset < bytes.len) {
            const decoded = (try decodeFrame(bytes[offset..])) orelse break;
            const record = decoded.record;
            const next_offset = std.math.add(usize, offset, decoded.frame_len) catch return error.CorruptedPostingWal;

            if (record.kind == .commit) {
                if (record.posting_id != 0 or record.payload.len != 0) return error.InvalidPostingWalCommit;
                if (open_batch == null or open_batch.? != record.batch_id or open_batch_records == 0) {
                    return error.InvalidPostingWalCommit;
                }
                if (record.source_sequence < open_batch_max_sequence) return error.InvalidPostingWalCommit;
                replay.last_committed_batch = record.batch_id;
                replay.covered_source_sequence = record.source_sequence;
                replay.committed_bytes = next_offset;
                open_batch = null;
                open_batch_records = 0;
                open_batch_max_sequence = 0;
            } else {
                if (open_batch) |batch_id| {
                    if (record.batch_id != batch_id) return error.InterleavedPostingWalBatch;
                    if (record.source_sequence < open_batch_max_sequence) return error.OutOfOrderPostingWalSequence;
                } else {
                    if (replay.last_committed_batch) |last| {
                        if (record.batch_id <= last) return error.OutOfOrderPostingWalBatch;
                    }
                    if (record.source_sequence < replay.covered_source_sequence) return error.OutOfOrderPostingWalSequence;
                    open_batch = record.batch_id;
                    open_batch_start = replay.records.items.len;
                }
                try replay.records.append(alloc, record);
                open_batch_records += 1;
                open_batch_max_sequence = @max(open_batch_max_sequence, record.source_sequence);
            }

            offset = next_offset;
            replay.valid_bytes = offset;
        }

        if (open_batch != null) replay.records.shrinkRetainingCapacity(open_batch_start);
        try replay.buildPostingIndex();
        return replay;
    }

    pub fn deinit(self: *Replay) void {
        self.latest_by_kind.deinit(self.alloc);
        self.posting_order.deinit(self.alloc);
        self.records.deinit(self.alloc);
        self.* = undefined;
    }

    /// Returns committed records for one posting in source/WAL order without
    /// scanning records belonging to other postings.
    pub fn recordsFor(self: *const Replay, posting_id: PostingId) PostingIterator {
        const start = self.postingLowerBound(posting_id);
        var end = start;
        while (end < self.posting_order.items.len and self.records.items[self.posting_order.items[end]].posting_id == posting_id) : (end += 1) {}
        return .{ .replay = self, .index = start, .end = end };
    }

    /// Returns the newest committed value for a posting and kind. A newer
    /// posting tombstone masks an older value. Replay builds a direct latest
    /// value table so query work does not grow with WAL-tail depth.
    pub fn latest(self: *const Replay, posting_id: PostingId, kind: RecordKind) ?Record {
        return switch (self.resolve(posting_id, kind)) {
            .value => |record| record,
            .missing, .tombstone => null,
        };
    }

    /// Distinguishes an absent WAL override from a tombstone, allowing callers
    /// to fall back to the immutable segment only in the former case.
    pub fn resolve(self: *const Replay, posting_id: PostingId, kind: RecordKind) Resolution {
        const resolved = self.latest_by_kind.get(postingKindKey(posting_id, kind)) orelse return .missing;
        return switch (resolved) {
            .tombstone => .tombstone,
            .record_index => |index| .{ .value = self.records.items[index] },
        };
    }

    fn buildPostingIndex(self: *Replay) !void {
        try self.posting_order.ensureTotalCapacity(self.alloc, self.records.items.len);
        for (self.records.items, 0..) |record, index| {
            self.posting_order.appendAssumeCapacity(index);
            if (record.kind == .tombstone) {
                inline for (lookup_record_kinds) |kind| {
                    try self.latest_by_kind.put(self.alloc, postingKindKey(record.posting_id, kind), .tombstone);
                }
            } else {
                try self.latest_by_kind.put(
                    self.alloc,
                    postingKindKey(record.posting_id, record.kind),
                    .{ .record_index = index },
                );
            }
        }
        std.mem.sort(usize, self.posting_order.items, self, replayPostingLessThan);
    }

    fn postingLowerBound(self: *const Replay, posting_id: PostingId) usize {
        var lo: usize = 0;
        var hi = self.posting_order.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const record = self.records.items[self.posting_order.items[mid]];
            if (record.posting_id < posting_id) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
};

const lookup_record_kinds = [_]RecordKind{
    .base,
    .mutation,
    .centroid_directory,
    .quantized_checkpoint,
    .posting_state,
};

fn postingKindKey(posting_id: PostingId, kind: RecordKind) u128 {
    return (@as(u128, posting_id) << 8) | @intFromEnum(kind);
}

pub const PostingIterator = struct {
    replay: *const Replay,
    index: usize,
    end: usize,

    pub fn next(self: *PostingIterator) ?Record {
        if (self.index >= self.end) return null;
        const record = self.replay.records.items[self.replay.posting_order.items[self.index]];
        self.index += 1;
        return record;
    }
};

fn replayPostingLessThan(replay: *const Replay, lhs_index: usize, rhs_index: usize) bool {
    const lhs = replay.records.items[lhs_index];
    const rhs = replay.records.items[rhs_index];
    if (lhs.posting_id != rhs.posting_id) return lhs.posting_id < rhs.posting_id;
    if (lhs.source_sequence != rhs.source_sequence) return lhs.source_sequence < rhs.source_sequence;
    return lhs_index < rhs_index;
}

const DecodedFrame = struct {
    record: Record,
    frame_len: usize,
};

fn appendFrame(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), record: Record) !void {
    const total_len = std.math.add(usize, frame_header_len, record.payload.len) catch return error.PostingWalRecordTooLarge;
    if (total_len > max_frame_len or total_len > std.math.maxInt(u32)) return error.PostingWalRecordTooLarge;
    const start = out.items.len;
    try out.resize(alloc, start + total_len);
    const frame = out.items[start .. start + total_len];
    std.mem.writeInt(u32, frame[0..4], magic, .big);
    std.mem.writeInt(u16, frame[4..6], version, .big);
    std.mem.writeInt(u16, frame[6..8], frame_header_len, .big);
    std.mem.writeInt(u32, frame[8..12], @intCast(total_len), .big);
    @memset(frame[checksum_offset..checksum_body_offset], 0);
    frame[16] = @intFromEnum(record.kind);
    @memset(frame[17..24], 0);
    std.mem.writeInt(u64, frame[24..32], record.batch_id, .big);
    std.mem.writeInt(u64, frame[32..40], record.posting_id, .big);
    std.mem.writeInt(u64, frame[40..48], record.source_sequence, .big);
    @memcpy(frame[frame_header_len..], record.payload);
    std.mem.writeInt(u32, frame[checksum_offset..checksum_body_offset], std.hash.Crc32.hash(frame[checksum_body_offset..]), .big);
}

fn decodeFrame(bytes: []const u8) !?DecodedFrame {
    if (bytes.len < checksum_body_offset) return null;
    if (std.mem.readInt(u32, bytes[0..4], .big) != magic) return error.BadPostingWalMagic;
    const frame_version = std.mem.readInt(u16, bytes[4..6], .big);
    if (frame_version < min_supported_version or frame_version > version) return error.UnsupportedPostingWalVersion;
    if (std.mem.readInt(u16, bytes[6..8], .big) != frame_header_len) return error.UnsupportedPostingWalHeader;
    const total_len: usize = @intCast(std.mem.readInt(u32, bytes[8..12], .big));
    if (total_len < frame_header_len) return error.CorruptedPostingWal;
    if (total_len > max_frame_len) return error.PostingWalRecordTooLarge;
    if (bytes.len < total_len) return null;
    const frame = bytes[0..total_len];
    if (std.mem.readInt(u32, frame[checksum_offset..checksum_body_offset], .big) != std.hash.Crc32.hash(frame[checksum_body_offset..])) {
        return error.PostingWalChecksumMismatch;
    }
    for (frame[17..24]) |reserved| if (reserved != 0) return error.UnsupportedPostingWalFlags;
    const kind: RecordKind = switch (frame[16]) {
        @intFromEnum(RecordKind.base) => .base,
        @intFromEnum(RecordKind.mutation) => .mutation,
        @intFromEnum(RecordKind.centroid_directory) => .centroid_directory,
        @intFromEnum(RecordKind.quantized_checkpoint) => .quantized_checkpoint,
        @intFromEnum(RecordKind.tombstone) => .tombstone,
        @intFromEnum(RecordKind.posting_state) => .posting_state,
        @intFromEnum(RecordKind.commit) => .commit,
        else => return error.InvalidPostingWalRecord,
    };
    return .{
        .record = .{
            .kind = kind,
            .batch_id = std.mem.readInt(u64, frame[24..32], .big),
            .posting_id = std.mem.readInt(u64, frame[32..40], .big),
            .source_sequence = std.mem.readInt(u64, frame[40..48], .big),
            .payload = frame[frame_header_len..],
        },
        .frame_len = total_len,
    };
}

pub const Checkpoint = struct {
    pub const encoded_len: usize = 52;
    const checkpoint_magic: [4]u8 = "AFPC".*;
    const checkpoint_version: u16 = 1;

    segment_generation: u64,
    segment_checksum: u32,
    wal_generation: u64,
    wal_committed_bytes: u64,
    covered_source_sequence: u64,

    pub fn encode(self: Checkpoint) [encoded_len]u8 {
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..4], &checkpoint_magic);
        std.mem.writeInt(u16, out[4..6], checkpoint_version, .big);
        std.mem.writeInt(u16, out[6..8], 0, .big);
        std.mem.writeInt(u64, out[8..16], self.segment_generation, .big);
        std.mem.writeInt(u32, out[16..20], self.segment_checksum, .big);
        std.mem.writeInt(u32, out[20..24], 0, .big);
        std.mem.writeInt(u64, out[24..32], self.wal_generation, .big);
        std.mem.writeInt(u64, out[32..40], self.wal_committed_bytes, .big);
        std.mem.writeInt(u64, out[40..48], self.covered_source_sequence, .big);
        std.mem.writeInt(u32, out[48..52], std.hash.Crc32.hash(out[0..48]), .big);
        return out;
    }

    pub fn decode(bytes: []const u8) !Checkpoint {
        if (bytes.len != encoded_len) return error.InvalidPostingCheckpoint;
        if (!std.mem.eql(u8, bytes[0..4], &checkpoint_magic)) return error.BadPostingCheckpointMagic;
        if (std.mem.readInt(u16, bytes[4..6], .big) != checkpoint_version) return error.UnsupportedPostingCheckpointVersion;
        if (std.mem.readInt(u16, bytes[6..8], .big) != 0 or std.mem.readInt(u32, bytes[20..24], .big) != 0) {
            return error.UnsupportedPostingCheckpointFlags;
        }
        if (std.mem.readInt(u32, bytes[48..52], .big) != std.hash.Crc32.hash(bytes[0..48])) {
            return error.PostingCheckpointChecksumMismatch;
        }
        return .{
            .segment_generation = std.mem.readInt(u64, bytes[8..16], .big),
            .segment_checksum = std.mem.readInt(u32, bytes[16..20], .big),
            .wal_generation = std.mem.readInt(u64, bytes[24..32], .big),
            .wal_committed_bytes = std.mem.readInt(u64, bytes[32..40], .big),
            .covered_source_sequence = std.mem.readInt(u64, bytes[40..48], .big),
        };
    }
};

pub fn testCommittedBatchesReplayInOrder() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.mutation, 10, 7, 100, "insert:1");
    try writer.append(.quantized_checkpoint, 10, 7, 100, "quant-v1");
    try writer.commit(10, 100);
    try writer.append(.mutation, 11, 7, 101, "delete:2");
    try writer.commit(11, 101);

    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqual(@as(usize, 3), replay.records.items.len);
    try std.testing.expectEqual(@as(?u64, 11), replay.last_committed_batch);
    try std.testing.expectEqual(@as(u64, 101), replay.covered_source_sequence);
    try std.testing.expectEqual(writer.bytes().len, replay.committed_bytes);
    try std.testing.expectEqualSlices(u8, "delete:2", replay.latest(7, .mutation).?.payload);
    var posting_records = replay.recordsFor(7);
    try std.testing.expectEqualSlices(u8, "insert:1", posting_records.next().?.payload);
    try std.testing.expectEqualSlices(u8, "quant-v1", posting_records.next().?.payload);
    try std.testing.expectEqualSlices(u8, "delete:2", posting_records.next().?.payload);
    try std.testing.expect(posting_records.next() == null);
}

test "posting WAL writer can continue after a durable prefix" {
    const alloc = std.testing.allocator;
    var writer = Writer.initAfterCommitted(alloc, 9, 99);
    defer writer.deinit();

    try writer.append(.base, 10, 7, 100, "base-v2");
    try writer.commit(10, 100);
    try std.testing.expectError(error.OutOfOrderPostingWalBatch, writer.append(.base, 10, 7, 101, "bad"));

    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqual(@as(?u64, 10), replay.last_committed_batch);
    try std.testing.expectEqual(@as(u64, 100), replay.covered_source_sequence);
}

pub fn testIgnoresUncommittedAndPartialTails() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.base, 1, 3, 10, "base-v1");
    try writer.commit(1, 10);
    const committed_len = writer.bytes().len;
    try writer.append(.mutation, 2, 3, 11, "uncommitted");

    var complete_tail = try Replay.parse(alloc, writer.bytes());
    defer complete_tail.deinit();
    try std.testing.expectEqual(@as(usize, 1), complete_tail.records.items.len);
    try std.testing.expectEqual(@as(usize, 1), complete_tail.posting_order.items.len);
    try std.testing.expectEqual(committed_len, complete_tail.committed_bytes);
    try std.testing.expect(complete_tail.valid_bytes > complete_tail.committed_bytes);

    const truncated = writer.bytes()[0 .. writer.bytes().len - 3];
    var partial_tail = try Replay.parse(alloc, truncated);
    defer partial_tail.deinit();
    try std.testing.expectEqual(@as(usize, 1), partial_tail.records.items.len);
    try std.testing.expectEqual(committed_len, partial_tail.committed_bytes);
    try std.testing.expectEqual(committed_len, partial_tail.valid_bytes);
}

pub fn testRejectsChecksumAndOrderingErrors() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.base, 1, 3, 10, "base-v1");
    try writer.commit(1, 10);

    var corrupt = try alloc.dupe(u8, writer.bytes());
    defer alloc.free(corrupt);
    corrupt[frame_header_len] ^= 1;
    try std.testing.expectError(error.PostingWalChecksumMismatch, Replay.parse(alloc, corrupt));

    var unordered = Writer.init(alloc);
    defer unordered.deinit();
    try unordered.append(.mutation, 2, 3, 11, "newer");
    try std.testing.expectError(error.OutOfOrderPostingWalSequence, unordered.append(.mutation, 2, 3, 10, "older"));
}

pub fn testCheckpointRoundTripAndChecksum() !void {
    const expected: Checkpoint = .{
        .segment_generation = 8,
        .segment_checksum = 0x12345678,
        .wal_generation = 9,
        .wal_committed_bytes = 4096,
        .covered_source_sequence = 1234,
    };
    var encoded = expected.encode();
    const decoded = try Checkpoint.decode(&encoded);
    try std.testing.expectEqualDeep(expected, decoded);
    encoded[10] ^= 1;
    try std.testing.expectError(error.PostingCheckpointChecksumMismatch, Checkpoint.decode(&encoded));
}

pub fn testSegmentCheckpointAndWalTailCompose() !void {
    const alloc = std.testing.allocator;
    var segment_writer = posting_segment.Writer.init(alloc);
    defer segment_writer.deinit();
    try segment_writer.appendBase(7, "base-v1");
    try segment_writer.appendQuantizedCheckpoint(7, "quant-v1");
    const segment_bytes = try segment_writer.build();
    defer alloc.free(segment_bytes);
    const segment = try posting_segment.Reader.init(segment_bytes);

    var wal_writer = Writer.init(alloc);
    defer wal_writer.deinit();
    try wal_writer.append(.base, 1, 7, 101, "base-v2");
    try wal_writer.append(.quantized_checkpoint, 1, 7, 101, "quant-v2");
    try wal_writer.commit(1, 101);
    var replay = try Replay.parse(alloc, wal_writer.bytes());
    defer replay.deinit();

    try std.testing.expectEqualSlices(u8, "base-v1", (try segment.getBase(7)).?);
    try std.testing.expectEqualSlices(u8, "base-v2", replay.latest(7, .base).?.payload);
    try std.testing.expectEqualSlices(u8, "quant-v2", replay.latest(7, .quantized_checkpoint).?.payload);
}

test "posting wal replays committed batches in order" {
    try testCommittedBatchesReplayInOrder();
}

test "posting wal ignores uncommitted and partial tails" {
    try testIgnoresUncommittedAndPartialTails();
}

test "posting wal rejects checksum and ordering errors" {
    try testRejectsChecksumAndOrderingErrors();
}

test "posting checkpoint round trips with checksum" {
    try testCheckpointRoundTripAndChecksum();
}

test "posting segment checkpoint and wal tail compose" {
    try testSegmentCheckpointAndWalTailCompose();
}

test "posting WAL distinguishes tombstones from missing overrides" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.posting_state, 1, 7, 100, "state-v1");
    try writer.commit(1, 100);
    try writer.append(.tombstone, 2, 8, 101, &.{});
    try writer.commit(2, 101);

    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqualStrings("state-v1", replay.resolve(7, .posting_state).value.payload);
    try std.testing.expect(replay.resolve(8, .posting_state) == .tombstone);
    try std.testing.expect(replay.resolve(9, .posting_state) == .missing);
}
