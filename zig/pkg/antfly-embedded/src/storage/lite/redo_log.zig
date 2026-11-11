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

//! Sequence-numbered, checksummed redo-log records for Lite's native
//! single-file store (docs/design/lite-performance/
//! lite-single-commit-write-path.md §9.2).
//!
//! This module is intentionally standalone: it knows nothing about
//! `NativeFile`, `PageAllocator`, `allocator_v4`, or `docstore.Store`. It
//! operates over any `std.Io.File` and a caller-chosen `[base_offset,
//! base_offset + capacity)` byte range -- in the eventual integration
//! (build step (c)) that range is a page range inside `.aflite` (§9.1), but
//! for this module's own unit tests it is just a region of a temp file, so
//! the format, the writer, and the reader can be reviewed and tested
//! (`zig test redo_log.zig`) before any commit-path code changes.
//!
//! Record layout (§9.2), 24-byte header plus payload:
//!
//! ```
//! offset  bytes  field
//! 0       4      magic ("ARDO")
//! 4       2      format_version
//! 6       2      header_len (always `header_len`; present for forward
//!                compatibility the same way lsm_backend/wal.zig's own
//!                record header carries one)
//! 8       4      payload_len
//! 12      8      commit_sequence
//! 20      4      crc32 (antfly_hash.Crc32.hash(payload); payload only)
//! 24      -      payload
//! ```
//!
//! Recovery (§9.5) distinguishes two failure modes while replaying a
//! region, matching `lsm_backend/wal.zig`'s own torn-tail-vs-corrupt
//! distinction:
//!
//! - **torn tail / end of region**: fewer bytes remain than the next
//!   record claims, or the next header position is still the untouched,
//!   zero-filled tail of a preallocated extent. Benign; this is where the
//!   valid prefix ends.
//! - **corrupt record**: a complete-length record whose checksum doesn't
//!   match its payload, or whose `commit_sequence` is not exactly one
//!   greater than the previous valid record's (a gap means a record
//!   silently vanished). A hard stop one record earlier than the bad one;
//!   unlike a torn tail, a caller must never advance past this point.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const File = std.Io.File;
const Crc32 = @import("antfly_hash").Crc32;

/// "ARDO" read as a little-endian u32, i.e. bytes 'A','R','D','O' in file
/// order.
pub const magic: u32 = 0x4f445241;

/// Independent of native.zig's own `format_version`/`indexed_format_version`
/// (§9.5) -- this is the redo-record wire format version, not the page
/// format version.
pub const format_version: u16 = 1;

pub const header_len: usize = 24;

pub const RecordHeader = struct {
    magic: u32 = magic,
    format_version: u16 = format_version,
    header_len: u16 = header_len,
    payload_len: u32,
    commit_sequence: u64,
    crc32: u32,

    pub fn encode(self: RecordHeader, out: *[header_len]u8) void {
        std.mem.writeInt(u32, out[0..4], self.magic, .little);
        std.mem.writeInt(u16, out[4..6], self.format_version, .little);
        std.mem.writeInt(u16, out[6..8], @intCast(self.header_len), .little);
        std.mem.writeInt(u32, out[8..12], self.payload_len, .little);
        std.mem.writeInt(u64, out[12..20], self.commit_sequence, .little);
        std.mem.writeInt(u32, out[20..24], self.crc32, .little);
    }

    /// Decodes a header from exactly `header_len` bytes. Does not validate
    /// magic/version/length against policy -- that is `Reader.replay`'s job,
    /// since a plain decode needs to be callable on untrusted bytes without
    /// deciding what to do about them.
    pub fn decode(bytes: *const [header_len]u8) RecordHeader {
        return .{
            .magic = std.mem.readInt(u32, bytes[0..4], .little),
            .format_version = std.mem.readInt(u16, bytes[4..6], .little),
            .header_len = std.mem.readInt(u16, bytes[6..8], .little),
            .payload_len = std.mem.readInt(u32, bytes[8..12], .little),
            .commit_sequence = std.mem.readInt(u64, bytes[12..20], .little),
            .crc32 = std.mem.readInt(u32, bytes[20..24], .little),
        };
    }
};

pub const AppendError = error{
    /// The record (header + payload) would not fit in
    /// `[base_offset, base_offset + capacity)`.
    LogFull,
    /// `commit_sequence` was not exactly one greater than the last
    /// appended record's sequence (or, for the first append after
    /// `initAt`, not exactly one greater than `start_after_sequence`).
    SequenceOutOfOrder,
    /// `payload.len` does not fit in the header's `u32` field.
    PayloadTooLarge,
} || File.WritePositionalError || File.SyncError;

/// Appends redo records to `[base_offset, base_offset + capacity)` in one
/// `std.Io.File`. One `Writer` per log region; not safe for concurrent use
/// from more than one caller at a time (the same single-leader-at-a-time
/// rule docstore.zig's group commit already follows, §3.2a).
pub const Writer = struct {
    file: File,
    io: Io,
    base_offset: u64,
    capacity: u64,
    /// Offset of the next write, relative to `base_offset`.
    tail: u64 = 0,
    last_sequence: ?u64 = null,

    /// Starts a fresh (empty) log region.
    pub fn init(file: File, io: Io, base_offset: u64, capacity: u64) Writer {
        return initAt(file, io, base_offset, capacity, 0, null);
    }

    /// Resumes a log region whose first `tail` bytes are already valid
    /// records ending at `last_sequence` (as determined by a prior
    /// `Reader.replay`), so recovery can keep appending after the valid
    /// prefix instead of starting over.
    pub fn initAt(file: File, io: Io, base_offset: u64, capacity: u64, tail: u64, last_sequence: ?u64) Writer {
        return .{
            .file = file,
            .io = io,
            .base_offset = base_offset,
            .capacity = capacity,
            .tail = tail,
            .last_sequence = last_sequence,
        };
    }

    /// Bytes remaining before the region is full.
    pub fn remaining(self: *const Writer) u64 {
        return self.capacity - self.tail;
    }

    /// Appends one record. Does not fsync -- callers batch many appends
    /// behind one `sync()` call, matching §9.3's "one append, one fsync"
    /// per group.
    pub fn append(self: *Writer, commit_sequence: u64, payload: []const u8) AppendError!void {
        if (payload.len > std.math.maxInt(u32)) return error.PayloadTooLarge;
        if (self.last_sequence) |last| {
            if (commit_sequence != last + 1) return error.SequenceOutOfOrder;
        }
        const record_len: u64 = header_len + payload.len;
        if (record_len > self.remaining()) return error.LogFull;

        var header_buf: [header_len]u8 = undefined;
        const header: RecordHeader = .{
            .payload_len = @intCast(payload.len),
            .commit_sequence = commit_sequence,
            .crc32 = Crc32.hash(payload),
        };
        header.encode(&header_buf);

        const offset = self.base_offset + self.tail;
        try self.file.writePositionalAll(self.io, &header_buf, offset);
        if (payload.len != 0) {
            try self.file.writePositionalAll(self.io, payload, offset + header_len);
        }

        self.tail += record_len;
        self.last_sequence = commit_sequence;
    }

    /// The durability barrier (§9.3 step 3). Callers are expected to have
    /// already batched every ride-along member's `append` before calling
    /// this once per group, same as `commitTransactionWithDurability`'s
    /// existing `syncIfRequired` boundary.
    pub fn sync(self: *Writer) File.SyncError!void {
        try self.file.sync(self.io);
    }
};

/// One decoded, checksum-and-sequence-validated record, handed to a
/// `Reader.replay` callback. `payload` aliases the caller-supplied scratch
/// buffer and is only valid for the duration of the callback.
pub const RecordView = struct {
    commit_sequence: u64,
    payload: []const u8,
};

pub const StopReason = enum {
    /// The region's remaining bytes are the zero-filled tail of a
    /// preallocated extent (never written) -- a clean end, not a crash
    /// artifact.
    end_of_region,
    /// Fewer bytes remain than a record claims, or a short, non-zero
    /// fragment sits where a header should be. Expected after a crash
    /// mid-append; the valid prefix ends here, benignly.
    torn_tail,
    /// A complete-length record failed checksum or sequence-continuity
    /// validation. Not expected; callers must stop here and must not
    /// trust anything from this record onward.
    corrupt_record,
};

pub const ReplayOutcome = struct {
    /// Number of records accepted into the valid prefix.
    valid_count: u64,
    /// Bytes consumed by the valid prefix, relative to `base_offset` --
    /// where a `Writer` should resume appending.
    valid_bytes: u64,
    /// The last valid record's sequence, or the baseline passed in if no
    /// record was replayed.
    last_sequence: ?u64,
    stop_reason: StopReason,
};

/// `replay`'s own I/O and allocation failures. Its actual return type is
/// wider than this (`anyerror!ReplayOutcome`) because an `on_record`
/// callback's own error set is unconstrained (`Callback` returns
/// `anyerror!void`) and propagates through unchanged.
pub const ReplayError = File.ReadPositionalError || Allocator.Error;

pub const Callback = *const fn (ctx: ?*anyopaque, record: RecordView) anyerror!void;

/// Replays the valid prefix of `[base_offset, base_offset + capacity)`,
/// calling `on_record` (if given) for each valid record in order, and
/// stopping at the first torn or invalid record (§9.5).
///
/// `start_after_sequence`, if given, is the last sequence already covered
/// by a durable checkpoint (or `null` for a fresh/empty region): the first
/// replayed record must have exactly `start_after_sequence + 1`, enforcing
/// continuity against the checkpoint baseline the same way continuity is
/// enforced between records.
pub fn replay(
    allocator: Allocator,
    file: File,
    io: Io,
    base_offset: u64,
    capacity: u64,
    start_after_sequence: ?u64,
    on_record: ?Callback,
    ctx: ?*anyopaque,
) anyerror!ReplayOutcome {
    var pos: u64 = 0;
    var count: u64 = 0;
    var last_sequence = start_after_sequence;
    var payload_buf = std.ArrayList(u8).empty;
    defer payload_buf.deinit(allocator);

    while (true) {
        const remaining = capacity - pos;
        if (remaining < header_len) {
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .end_of_region };
        }

        var header_buf: [header_len]u8 = undefined;
        // `readPositionalAll` never raises an error for a short read at
        // end-of-file -- it just returns fewer bytes than requested
        // (std.Io.File.zig's own `readPositionalAll`: `if (amt == 0) break;`
        // then returns the accumulated count). A genuine I/O error still
        // propagates as `err` from the `try` below.
        const got = try file.readPositionalAll(io, &header_buf, base_offset + pos);
        if (got < header_len) {
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .torn_tail };
        }

        if (isAllZero(&header_buf)) {
            // Untouched, preallocated space: a clean end, never a torn
            // write (a real torn write leaves whatever bytes landed, not
            // all-zero bytes, since the region is zero-filled ahead of any
            // append).
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .end_of_region };
        }

        const header = RecordHeader.decode(&header_buf);
        if (header.magic != magic or header.format_version != format_version or header.header_len != header_len) {
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .corrupt_record };
        }

        const payload_len: u64 = header.payload_len;
        if (header_len + payload_len > remaining) {
            // The header is well-formed but claims more payload than the
            // region has left. A real write can never produce this (the
            // writer checks `remaining()` before appending), so this is
            // either a torn write that landed a plausible-looking header
            // with garbage length bits, or genuine corruption. Either way
            // the record cannot be validated (there aren't enough bytes to
            // checksum), so treat it as a torn tail: there is no complete
            // record here to call corrupt.
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .torn_tail };
        }

        payload_buf.clearRetainingCapacity();
        try payload_buf.resize(allocator, payload_len);
        if (payload_len != 0) {
            const payload_got = try file.readPositionalAll(io, payload_buf.items, base_offset + pos + header_len);
            if (payload_got < payload_len) {
                return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .torn_tail };
            }
        }

        if (Crc32.hash(payload_buf.items) != header.crc32) {
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .corrupt_record };
        }

        if (last_sequence) |last| {
            if (header.commit_sequence != last + 1) {
                return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .corrupt_record };
            }
        }

        if (on_record) |cb| {
            try cb(ctx, .{ .commit_sequence = header.commit_sequence, .payload = payload_buf.items });
        }

        pos += header_len + payload_len;
        count += 1;
        last_sequence = header.commit_sequence;
    }
}

fn isAllZero(bytes: []const u8) bool {
    for (bytes) |b| {
        if (b != 0) return false;
    }
    return true;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

var temp_region_counter: u64 = 0;

const TempRegion = struct { file: File, path: [64]u8, path_len: usize };

fn makeTempRegion(io: Io, capacity: u64) !TempRegion {
    var name_buf: [64]u8 = undefined;
    temp_region_counter += 1;
    const name = try std.fmt.bufPrint(&name_buf, "redo_log_test_{d}.bin", .{temp_region_counter});
    var file = try std.Io.Dir.cwd().createFile(io, name, .{ .read = true, .truncate = true });
    // Zero-fill the capacity up front, mirroring the "preallocated extent"
    // assumption `replay`'s end-of-region detection relies on.
    const zeros = try testing.allocator.alloc(u8, @intCast(capacity));
    defer testing.allocator.free(zeros);
    @memset(zeros, 0);
    try file.writePositionalAll(io, zeros, 0);
    var result: TempRegion = undefined;
    result.file = file;
    @memcpy(result.path[0..name.len], name);
    result.path_len = name.len;
    return result;
}

fn cleanupTempRegion(io: Io, file: *File, path: []const u8) void {
    file.close(io);
    std.Io.Dir.cwd().deleteFile(io, path) catch {};
}

test "RecordHeader encode/decode round trip" {
    const header: RecordHeader = .{ .payload_len = 42, .commit_sequence = 7, .crc32 = 0xdeadbeef };
    var buf: [header_len]u8 = undefined;
    header.encode(&buf);
    const decoded = RecordHeader.decode(&buf);
    try testing.expectEqual(magic, decoded.magic);
    try testing.expectEqual(format_version, decoded.format_version);
    try testing.expectEqual(@as(u16, header_len), decoded.header_len);
    try testing.expectEqual(@as(u32, 42), decoded.payload_len);
    try testing.expectEqual(@as(u64, 7), decoded.commit_sequence);
    try testing.expectEqual(@as(u32, 0xdeadbeef), decoded.crc32);
}

test "append then replay round trip" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "alpha");
    try writer.append(2, "beta-beta");
    try writer.append(3, "");
    try writer.append(4, "gamma-gamma-gamma");
    try writer.sync();

    const Collected = struct {
        allocator: Allocator,
        sequences: std.ArrayList(u64) = .empty,
        payloads: std.ArrayList([]u8) = .empty,

        fn onRecord(ctx: ?*anyopaque, record: RecordView) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            try self.sequences.append(self.allocator, record.commit_sequence);
            try self.payloads.append(self.allocator, try self.allocator.dupe(u8, record.payload));
        }
    };
    var collected: Collected = .{ .allocator = testing.allocator };
    defer {
        collected.sequences.deinit(testing.allocator);
        for (collected.payloads.items) |p| testing.allocator.free(p);
        collected.payloads.deinit(testing.allocator);
    }

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, Collected.onRecord, &collected);
    try testing.expectEqual(@as(u64, 4), outcome.valid_count);
    try testing.expectEqual(@as(u64, 4), outcome.last_sequence.?);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
    try testing.expectEqual(writer.tail, outcome.valid_bytes);

    try testing.expectEqualSlices(u64, &[_]u64{ 1, 2, 3, 4 }, collected.sequences.items);
    try testing.expectEqualStrings("alpha", collected.payloads.items[0]);
    try testing.expectEqualStrings("beta-beta", collected.payloads.items[1]);
    try testing.expectEqualStrings("", collected.payloads.items[2]);
    try testing.expectEqualStrings("gamma-gamma-gamma", collected.payloads.items[3]);
}

test "replay resumes continuity from a checkpoint baseline" {
    const io = testing.io;
    const capacity: u64 = 2048;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "x");
    try writer.append(12, "y");
    try writer.sync();

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, 10, null, null);
    try testing.expectEqual(@as(u64, 2), outcome.valid_count);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);

    // A baseline that disagrees with the first record's sequence must be
    // rejected as corrupt (continuity against the checkpoint, not just
    // continuity between records).
    const bad = try replay(testing.allocator, region.file, io, 0, capacity, 999, null, null);
    try testing.expectEqual(@as(u64, 0), bad.valid_count);
    try testing.expectEqual(StopReason.corrupt_record, bad.stop_reason);
}

test "writer rejects out-of-order sequence" {
    const io = testing.io;
    const capacity: u64 = 1024;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(5, "a");
    try testing.expectError(error.SequenceOutOfOrder, writer.append(5, "dup"));
    try testing.expectError(error.SequenceOutOfOrder, writer.append(7, "gap"));
}

test "writer rejects a record that would overflow the region" {
    const io = testing.io;
    const capacity: u64 = header_len + 4; // room for exactly one 4-byte payload.
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "abcd");
    try testing.expectError(error.LogFull, writer.append(2, "e"));
}

test "replay stops at a torn tail and reports the valid prefix" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "one");
    try writer.append(2, "two");
    try writer.sync();
    const good_tail = writer.tail;

    // Simulate a crash mid-append of a third record whose header landed
    // (the magic/version/header_len fields are intact and well-formed) but
    // whose claimed payload length is physically impossible for the
    // remaining region -- the one way `replay` can tell "this header is
    // real but the record behind it never finished landing" apart from
    // "this header is garbage" without being able to checksum bytes that
    // were never written. A real torn write can produce exactly this: the
    // header is a single small write that lands atomically more often than
    // not, while the much larger payload write that follows it is what the
    // crash interrupts.
    var header_buf: [header_len]u8 = undefined;
    const torn: RecordHeader = .{ .payload_len = @intCast(capacity), .commit_sequence = 3, .crc32 = 0 };
    torn.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, good_tail);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 2), outcome.valid_count);
    try testing.expectEqual(@as(u64, 2), outcome.last_sequence.?);
    try testing.expectEqual(good_tail, outcome.valid_bytes);
    try testing.expectEqual(StopReason.torn_tail, outcome.stop_reason);

    // A writer resuming from this outcome must be able to append right
    // where the valid prefix ended, overwriting the torn record.
    var resumed = Writer.initAt(region.file, io, 0, capacity, outcome.valid_bytes, outcome.last_sequence);
    try resumed.append(3, "three");
    try resumed.sync();
    const resumed_outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 3), resumed_outcome.valid_count);
    try testing.expectEqual(@as(u64, 3), resumed_outcome.last_sequence.?);
    try testing.expectEqual(StopReason.end_of_region, resumed_outcome.stop_reason);
}

test "replay stops at a torn tail when the file is physically shorter than the region" {
    // The log region's nominal `capacity` is bookkeeping (the page range
    // reserved for it, §9.1); the backing file can legitimately be shorter
    // than that while the region's tail extent hasn't grown that far yet,
    // or because a crash interrupted that growth. `replay` must treat a
    // genuine short read the same way `lsm_backend/wal.zig` treats running
    // out of file bytes: a benign torn tail, not corruption.
    const io = testing.io;
    const capacity: u64 = 256;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "hello");
    try writer.sync();
    const good_tail = writer.tail;

    // Shrink the file to end partway through where a second record's
    // header would start -- fewer than `header_len` bytes available past
    // the first valid record, a real short read.
    try region.file.setLength(io, good_tail + 3);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(good_tail, outcome.valid_bytes);
    try testing.expectEqual(StopReason.torn_tail, outcome.stop_reason);
}

test "replay detects payload corruption via checksum mismatch" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "unchanged");
    const second_offset = writer.tail;
    try writer.append(2, "payload-before-corruption");
    try writer.append(3, "trailing-after-corruption");
    try writer.sync();

    // Flip one byte inside record 2's payload (not its header), simulating
    // a media fault on an already-synced page -- the header's checksum no
    // longer matches.
    const corrupt_at = second_offset + header_len + 3;
    var one_byte: [1]u8 = undefined;
    _ = try region.file.readPositionalAll(io, &one_byte, corrupt_at);
    one_byte[0] ^= 0xFF;
    try region.file.writePositionalAll(io, &one_byte, corrupt_at);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(@as(u64, 1), outcome.last_sequence.?);
    try testing.expectEqual(second_offset, outcome.valid_bytes);
    try testing.expectEqual(StopReason.corrupt_record, outcome.stop_reason);
}

test "replay detects a sequence gap as corruption, not a torn tail" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    // Hand-write three structurally valid, correctly checksummed records
    // whose sequences skip 2 (1, then 3, then 4) -- every individual
    // record is well-formed, so only continuity checking catches this.
    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "first");
    // Bypass the writer's own continuity check to construct the gap
    // directly, the way independent corruption (not a local bug in this
    // writer) would produce it.
    var header_buf: [header_len]u8 = undefined;
    const skip: RecordHeader = .{ .payload_len = 4, .commit_sequence = 3, .crc32 = Crc32.hash("skip") };
    skip.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, writer.tail);
    try region.file.writePositionalAll(io, "skip", writer.tail + header_len);
    try writer.file.sync(io);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(@as(u64, 1), outcome.last_sequence.?);
    try testing.expectEqual(StopReason.corrupt_record, outcome.stop_reason);
}

test "replay of a never-written (empty) region is a clean end, not corruption" {
    const io = testing.io;
    const capacity: u64 = 1024;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 0), outcome.valid_count);
    try testing.expectEqual(@as(u64, 0), outcome.valid_bytes);
    try testing.expectEqual(@as(?u64, null), outcome.last_sequence);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
}

test "replay rejects an unsupported format version as corrupt, not benign" {
    const io = testing.io;
    const capacity: u64 = 1024;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "ok");
    try writer.sync();

    var header_buf: [header_len]u8 = undefined;
    var bad: RecordHeader = .{ .payload_len = 2, .commit_sequence = 2, .crc32 = Crc32.hash("hi") };
    bad.format_version = format_version + 1;
    bad.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, writer.tail);
    try region.file.writePositionalAll(io, "hi", writer.tail + header_len);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(StopReason.corrupt_record, outcome.stop_reason);
}
