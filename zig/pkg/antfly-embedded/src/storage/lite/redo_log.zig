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
    /// Number of records passed to `on_record` -- i.e. records in the
    /// valid prefix that were *not* superseded by `start_after_sequence`.
    /// A superseded record is still part of the valid prefix (counted in
    /// `valid_bytes`, required to pass every structural check), it is
    /// just not re-delivered to the caller, since the checkpoint it names
    /// already reflects it.
    valid_count: u64,
    /// Bytes consumed by the valid prefix, relative to `base_offset` --
    /// where a `Writer` should resume appending. Includes any leading
    /// superseded records' own bytes (they are not re-validated bytes to
    /// discard, just records the caller doesn't need delivered again).
    valid_bytes: u64,
    /// The last valid record's sequence (superseded or not), or the
    /// baseline passed in if no record was replayed.
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
/// calling `on_record` (if given) for each valid, non-superseded record in
/// order, and stopping at the first torn or invalid record (§9.5). A
/// record whose sequence is covered by `start_after_sequence` is skipped
/// (not delivered to `on_record`, not counted in `valid_count`) but still
/// structurally validated (magic/version/length/CRC) and still advances
/// the scan -- so a leading run of such superseded records does not stop
/// the scan early, and genuinely new records physically following them
/// (left behind when a checkpoint landed but the log's stale prefix
/// hasn't been zeroed/reset yet -- see §9.3b's gated reclaim) are still
/// found and delivered.
///
/// `start_after_sequence`, if given, is the last sequence already covered
/// by a durable checkpoint (or `null` for a fresh/empty region). Sequence
/// continuity is enforced in two different ways depending on where a
/// record falls:
///
/// - **Among superseded records** (sequence `<= start_after_sequence`,
///   including the very first one scanned): not enforced at all, not even
///   "monotonic." A real checkpoint commit consumes its own
///   `commit_sequence` without ever appending a log record for it
///   (`native.zig`'s `checkpointRedoLog`, its `checkpointing = true`
///   branch), and §9.3b's gated reclaim can leave more than one such gap
///   physically present at once if reclaim stayed deferred across several
///   checkpoints behind a slow pinned reader. This is safe to tolerate,
///   not just convenient: a superseded record is never delivered, so a
///   gap among them has no observable effect -- the real checkpoint that
///   makes them superseded already carries the authoritative state for
///   everything up to the baseline.
/// - **At the transition into genuinely new records** (the first record
///   whose sequence is `> start_after_sequence`, right after a superseded
///   run, or the very first record scanned when none was superseded):
///   must be strictly greater than the last record scanned (tolerating
///   however many checkpoint-consumed gaps accumulated), *or*, when
///   nothing has been superseded at all, must equal exactly
///   `start_after_sequence + 1` (an unexplained gap here is real
///   corruption, not a checkpoint artifact -- see "replay detects a
///   sequence gap as corruption, not a torn tail").
/// - **Among genuinely new records thereafter**: strict `+1`, since every
///   one of them was actually logged by a live `Writer.append`, which
///   always enforces `+1` for anything it accepts -- a missing one here
///   is a real, unexplained gap.
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
    // True once at least one superseded record has been scanned and we
    // have not yet transitioned into genuinely new records. Continuity
    // among superseded records is never checked (see below); this flag
    // exists only to relax the *one* comparison right at the transition
    // out of the superseded run, then switch back to strict `+1`
    // continuity for everything after. Starts `false`: the very first
    // record, if *not* superseded, still gets the ordinary strict check
    // against the baseline (the ordinary "gap before anything superseded
    // happened" corruption case).
    var in_superseded_run = false;
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

        // Superseded, not corrupt: a structurally valid, correctly
        // checksummed record whose sequence the checkpoint baseline
        // (`start_after_sequence`, never mutated by this loop) already
        // covers. This is exactly what a crash strictly between a
        // checkpoint's new slot becoming durable and this log's own
        // now-stale prefix being zeroed leaves behind (the checkpoint
        // already reflects this record; only its bytes in the log are
        // stale) -- and, since §9.3b's reclaim gates the physical zero on
        // the region's minimum pinned sequence, not every superseded
        // record is necessarily the *last* one in the region either: a
        // superseded run can be followed by genuinely new records a
        // pinned cross-process reader hasn't caught up to yet (see the
        // test by that name). Checked against the *original* baseline,
        // not `last_sequence` (which advances through superseded records
        // too, for chain-continuity purposes), so a run of several
        // leading superseded records is recognized throughout, not just
        // at `pos == 0`.
        const superseded = start_after_sequence != null and header.commit_sequence <= start_after_sequence.?;

        if (superseded) {
            // No continuity check among superseded records at all, not
            // even "monotonic": a real checkpoint commit consumes its own
            // `commit_sequence` without ever appending a log record for
            // it (`checkpointRedoLog`'s own `checkpointing` branch), so a
            // superseded run can legitimately contain any number of gaps
            // -- one per checkpoint that landed while reclaim stayed
            // gated behind a pinned reader (§9.3b). This is safe, not
            // just convenient: a superseded record is never delivered to
            // `on_record`, so whether its neighbors in the superseded run
            // are contiguous has no observable effect -- the real
            // checkpoint that makes it superseded already carries the
            // authoritative state for everything up to the baseline.
            // Structural validity (magic/version/header_len, length,
            // CRC) is still fully checked above, unconditionally.
            in_superseded_run = true;
        } else if (in_superseded_run) {
            // The first genuinely new record right after a superseded
            // run: only required to be strictly greater than whatever
            // was last scanned, tolerating however many checkpoint-
            // consumed-but-unlogged sequence numbers accumulated while
            // reclaim was gated. Strict `+1` resumes from here on (the
            // `else` branch below), since every record from here on was
            // actually logged by a live `Writer.append`, which always
            // enforces `+1` for anything it accepts.
            if (last_sequence) |last| {
                if (header.commit_sequence <= last) {
                    return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .corrupt_record };
                }
            }
            in_superseded_run = false;
        } else if (last_sequence) |last| {
            if (header.commit_sequence != last + 1) {
                return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .stop_reason = .corrupt_record };
            }
        }

        if (!superseded) {
            if (on_record) |cb| {
                try cb(ctx, .{ .commit_sequence = header.commit_sequence, .payload = payload_buf.items });
            }
            count += 1;
        }

        pos += header_len + payload_len;
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

    // A baseline that leaves a genuine *gap* before the first record
    // (expects 6, finds 11 -- records 6..10 are simply missing, not
    // explained by any checkpoint) is still rejected as corrupt:
    // continuity against the checkpoint still matters, just not in the
    // direction the next case covers.
    const gap = try replay(testing.allocator, region.file, io, 0, capacity, 5, null, null);
    try testing.expectEqual(@as(u64, 0), gap.valid_count);
    try testing.expectEqual(StopReason.corrupt_record, gap.stop_reason);

    // A baseline *at or ahead of* the first record's sequence, by
    // contrast, is the superseded-not-corrupt case this module's own
    // design doc calls out: exactly what a crash between a checkpoint's
    // new slot becoming durable and this log's own stale prefix being
    // zeroed leaves behind. The checkpoint already covers this record;
    // only its bytes are stale. Benign, not corrupt -- no valid record
    // past the checkpoint, but an ordinary clean end, not a failure.
    const superseded = try replay(testing.allocator, region.file, io, 0, capacity, 12, null, null);
    try testing.expectEqual(@as(u64, 0), superseded.valid_count);
    try testing.expectEqual(StopReason.end_of_region, superseded.stop_reason);
}

test "a superseded run can be followed by genuinely new records" {
    // §9.3b: the checkpointer's physical zero/reset of a log's stale
    // prefix is gated on the region's minimum pinned sequence (a live
    // cross-process reader may still need those bytes). While gated, the
    // writer keeps appending *after* the stale prefix instead of
    // resetting over it -- so a crash (or a lagging reader's own replay)
    // can see a leading run of superseded records immediately followed
    // by records the checkpoint does *not* yet cover. Both must survive
    // the scan: the superseded ones skipped but not stopping it, the new
    // ones delivered.
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "a"); // superseded by baseline 12
    try writer.append(12, "b"); // superseded by baseline 12 (the checkpoint's own record)
    try writer.append(13, "c"); // genuinely new
    try writer.append(14, "d"); // genuinely new
    try writer.sync();

    const Collected = struct {
        sequences: std.ArrayList(u64) = .empty,
        fn onRecord(ctx: ?*anyopaque, record: RecordView) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            try self.sequences.append(testing.allocator, record.commit_sequence);
        }
    };
    var collected: Collected = .{};
    defer collected.sequences.deinit(testing.allocator);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, 12, Collected.onRecord, &collected);
    try testing.expectEqual(@as(u64, 2), outcome.valid_count);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
    try testing.expectEqual(@as(u64, 14), outcome.last_sequence.?);
    try testing.expectEqualSlices(u64, &.{ 13, 14 }, collected.sequences.items);

    // A baseline of 10 (nothing superseded) must still see all four,
    // confirming the superseded classification -- not the scan itself --
    // is what changed.
    var collected_all: Collected = .{};
    defer collected_all.sequences.deinit(testing.allocator);
    const outcome_all = try replay(testing.allocator, region.file, io, 0, capacity, 10, Collected.onRecord, &collected_all);
    try testing.expectEqual(@as(u64, 4), outcome_all.valid_count);
    try testing.expectEqualSlices(u64, &.{ 11, 12, 13, 14 }, collected_all.sequences.items);
}

test "a gap among superseded records is tolerated -- a real checkpoint can consume a sequence without logging it" {
    // Deliberate relaxation, not an oversight: a real checkpoint commit
    // (`native.zig`'s `checkpointRedoLog`, `checkpointing = true` branch)
    // consumes its own `commit_sequence` without ever appending a log
    // record for it, and §9.3b's gated reclaim can leave more than one
    // such checkpoint's worth of gap physically present in the log at
    // once (reclaim stayed deferred across several checkpoints behind a
    // slow pinned reader). A gap among records that are *all* superseded
    // has no observable effect either way -- none of them is ever
    // delivered to `on_record`, and the real checkpoint that makes them
    // superseded already carries the authoritative state for everything
    // up to the baseline -- so this is safe to tolerate rather than flag.
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "a");
    try writer.sync();
    // Hand-write a record at sequence 13 (skipping 12) directly after
    // it, bypassing the writer's own continuity check the same way
    // "replay detects a sequence gap as corruption" does above --
    // standing in for "checkpoint sequence 12 landed but was never
    // logged."
    var header_buf: [header_len]u8 = undefined;
    const skip: RecordHeader = .{ .payload_len = 1, .commit_sequence = 13, .crc32 = Crc32.hash("b") };
    skip.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, writer.tail);
    try region.file.writePositionalAll(io, "b", writer.tail + header_len);
    try writer.file.sync(io);

    // Baseline 20 makes both records superseded; the gap between 11 and
    // 13 is tolerated, not corruption.
    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, 20, null, null);
    try testing.expectEqual(@as(u64, 0), outcome.valid_count);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
}

test "a superseded run followed by new records tolerates exactly the checkpoint's own consumed sequence, but still catches a real gap after the transition" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(2, "beta"); // superseded by baseline 3 (checkpoint's own consumed sequence)
    try writer.sync();
    // Sequence 3 (the checkpoint itself) is never logged -- the next
    // physical record jumps straight to 4, tolerated as the one
    // transition-point gap. `initAt` with `last_sequence = 3` stands in
    // for what `checkpointRedoLog` does to its own live `redo.writer`
    // when deferring reclaim: advance past the checkpoint's own
    // consumed sequence without resetting `tail`.
    writer = Writer.initAt(region.file, io, 0, capacity, writer.tail, 3);
    try writer.append(4, "gamma");
    try writer.sync();
    const Collected = struct {
        sequences: std.ArrayList(u64) = .empty,
        fn onRecord(ctx: ?*anyopaque, record: RecordView) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            try self.sequences.append(testing.allocator, record.commit_sequence);
        }
    };
    var collected: Collected = .{};
    defer collected.sequences.deinit(testing.allocator);
    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, 3, Collected.onRecord, &collected);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqualSlices(u64, &.{4}, collected.sequences.items);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);

    // But once past the transition, strict `+1` continuity resumes: a
    // second, hand-written record that skips a sequence among the
    // genuinely-new records *is* corruption.
    var region2 = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region2.file, region2.path[0..region2.path_len]);
    var writer2 = Writer.init(region2.file, io, 0, capacity);
    try writer2.append(2, "beta");
    writer2 = Writer.initAt(region2.file, io, 0, capacity, writer2.tail, 3);
    try writer2.append(4, "gamma");
    try writer2.sync();
    var header_buf: [header_len]u8 = undefined;
    const skip: RecordHeader = .{ .payload_len = 1, .commit_sequence = 6, .crc32 = Crc32.hash("d") };
    skip.encode(&header_buf);
    try region2.file.writePositionalAll(io, &header_buf, writer2.tail);
    try region2.file.writePositionalAll(io, "d", writer2.tail + header_len);
    try writer2.file.sync(io);
    const outcome2 = try replay(testing.allocator, region2.file, io, 0, capacity, 3, null, null);
    try testing.expectEqual(@as(u64, 1), outcome2.valid_count);
    try testing.expectEqual(StopReason.corrupt_record, outcome2.stop_reason);
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
