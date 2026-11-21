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
//! Record layout (§9.2), 32-byte header plus payload:
//!
//! ```
//! offset  bytes  field
//! 0       4      magic ("ARDO")
//! 4       2      format_version (2; version 1 records stop at offset 24)
//! 6       2      header_len (32 for version 2, 24 for version 1; present
//!                for forward compatibility the same way
//!                lsm_backend/wal.zig's own record header carries one)
//! 8       4      payload_len
//! 12      8      commit_sequence
//! 20      4      crc32: version 2 covers payload_len, commit_sequence,
//!                salt and the payload (`RecordHeader.checksum`); version 1
//!                covers the payload only
//! 24      8      salt (version 2): chosen at random each time the log
//!                restarts at its first byte, and repeated in every record
//!                until the next restart, like SQLite's WAL salts. A
//!                version 1 record's salt is 0.
//! 32      -      payload
//! ```
//!
//! A log restart zeroes only the first header, so the retired log's bytes
//! stay past the live tail. A crash can keep a record and lose its end
//! marker, exposing them; replay ends the log at the first header whose
//! salt differs from the log's first record, which retired bytes (written
//! before the salt was chosen) cannot match.
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
const builtin = @import("builtin");
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
pub const format_version: u16 = 2;

pub const header_len: usize = 32;

/// A version 1 header: the first 24 bytes, with no salt.
pub const legacy_format_version: u16 = 1;
pub const legacy_header_len: usize = 24;

/// Two preallocated WAL extents. A checkpoint publishes the selected
/// extent and the sequence that retired the previous one together.
pub const SegmentRotation = struct {
    active: u1 = 0,
    retired_through: ?u64 = null,

    /// Select the other extent only if the checkpoint covers the extent
    /// being retired and no reader still needs the destination extent.
    /// The caller publishes the result before appending to the new extent.
    pub fn afterCheckpoint(self: SegmentRotation, checkpoint_sequence: u64, active_end_sequence: u64, oldest_reader: ?u64) ?SegmentRotation {
        if (active_end_sequence > checkpoint_sequence) return null;
        if (self.retired_through) |retired| {
            if (retired > checkpoint_sequence) return null;
            if (oldest_reader) |reader| {
                if (reader < retired) return null;
            }
        }
        return .{ .active = self.active ^ 1, .retired_through = checkpoint_sequence };
    }

    pub fn offset(self: SegmentRotation, base_offset: u64, extent_capacity: u64) !u64 {
        return std.math.add(u64, base_offset, if (self.active == 0) 0 else extent_capacity);
    }
};

test "redo segment rotation leaves the retired extent available to pinned readers" {
    const initial: SegmentRotation = .{};
    const rotated = initial.afterCheckpoint(12, 11, 4).?;
    try std.testing.expectEqual(@as(u64, 8192), try rotated.offset(4096, 4096));
    try std.testing.expect(rotated.afterCheckpoint(20, 19, 4) == null);
    try std.testing.expect(rotated.afterCheckpoint(20, 21, null) == null);
    const reused = rotated.afterCheckpoint(20, 19, 12).?;
    try std.testing.expectEqual(@as(u64, 4096), try reused.offset(4096, 4096));
    try std.testing.expectEqual(@as(?u64, 20), reused.retired_through);
}

/// A durable group frontier, stored separately from the records it covers.
pub const Frontier = struct {
    generation: u64,
    sequence: u64,
    end_offset: u64,

    pub const encoded_len = 40;
    const signature = "AFRF\x01\x00\x00\x00";

    pub fn encode(self: Frontier) [encoded_len]u8 {
        var raw: [encoded_len]u8 = @splat(0);
        raw[0..8].* = signature.*;
        std.mem.writeInt(u64, raw[8..16], self.generation, .little);
        std.mem.writeInt(u64, raw[16..24], self.sequence, .little);
        std.mem.writeInt(u64, raw[24..32], self.end_offset, .little);
        std.mem.writeInt(u32, raw[36..40], Crc32.hash(raw[0..36]), .little);
        return raw;
    }

    pub fn decode(raw: *const [encoded_len]u8) ?Frontier {
        if (!std.mem.eql(u8, raw[0..8], signature)) return null;
        if (std.mem.readInt(u32, raw[36..40], .little) != Crc32.hash(raw[0..36])) return null;
        return .{
            .generation = std.mem.readInt(u64, raw[8..16], .little),
            .sequence = std.mem.readInt(u64, raw[16..24], .little),
            .end_offset = std.mem.readInt(u64, raw[24..32], .little),
        };
    }
};

/// The two offsets must lie in separate failure units outside the log.
pub const FrontierSlots = struct {
    offsets: [2]u64,

    pub const Published = struct {
        slot: u1,
        frontier: Frontier,
    };

    pub fn readLatest(self: FrontierSlots, file: File, io: Io) !?Published {
        var latest: ?Published = null;
        for (self.offsets, 0..) |offset, page| {
            var raw: [2 * Frontier.encoded_len]u8 = undefined;
            const len = try file.readPositionalAll(io, &raw, offset);
            for (0..len / Frontier.encoded_len) |copy| {
                const record = raw[copy * Frontier.encoded_len ..][0..Frontier.encoded_len];
                const frontier = Frontier.decode(record) orelse continue;
                if (latest) |prior| {
                    if (frontier.sequence < prior.frontier.sequence) continue;
                    if (frontier.sequence == prior.frontier.sequence) {
                        if (!std.meta.eql(frontier, prior.frontier)) return error.ConflictingRedoFrontiers;
                        continue;
                    }
                }
                latest = .{ .slot = @intCast(page ^ copy), .frontier = frontier };
            }
        }
        return latest;
    }

    /// Each logical slot has one copy on each frontier page. Updating it
    /// leaves the other logical slot intact on both pages.
    pub fn writeSlot(self: FrontierSlots, file: File, io: Io, publication: Published) !void {
        const raw = publication.frontier.encode();
        try file.writePositionalAll(io, &raw, self.offsets[publication.slot]);
        try file.writePositionalAll(io, &raw, self.offsets[publication.slot ^ 1] + Frontier.encoded_len);
    }

    /// The caller must sync the redo bytes before calling this under writer
    /// authority and sync again after writing the slot. After an I/O error
    /// the covered redo bytes must be retained until reconciliation.
    pub fn writeNext(self: FrontierSlots, file: File, io: Io, previous: ?Published, frontier: Frontier) !Published {
        const slot: u1 = if (previous) |prior| next: {
            if (frontier.sequence <= prior.frontier.sequence) return error.RedoFrontierOutOfOrder;
            break :next prior.slot ^ 1;
        } else 0;
        const next: Published = .{ .slot = slot, .frontier = frontier };
        try self.writeSlot(file, io, next);
        return next;
    }
};

test "redo frontier slots retain the preceding publication after a torn slot write" {
    const io = std.testing.io;
    var region = try makeTempRegion(io, 8192);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);
    const slots: FrontierSlots = .{ .offsets = .{ 0, 4096 } };
    try std.testing.expect(try slots.readLatest(region.file, io) == null);
    const first = try slots.writeNext(region.file, io, null, .{ .generation = 12, .sequence = 97, .end_offset = 60 });
    try region.file.sync(io);
    const second = try slots.writeNext(region.file, io, first, .{ .generation = 12, .sequence = 101, .end_offset = 92 });
    try region.file.sync(io);
    try std.testing.expect(first.slot != second.slot);
    try std.testing.expectEqual(second, (try slots.readLatest(region.file, io)).?);

    try region.file.writePositionalAll(io, &[_]u8{0}, slots.offsets[second.slot]);
    try std.testing.expectEqual(second, (try slots.readLatest(region.file, io)).?);
    try region.file.writePositionalAll(io, &[_]u8{0}, slots.offsets[second.slot ^ 1] + Frontier.encoded_len);
    try std.testing.expectEqual(first, (try slots.readLatest(region.file, io)).?);
}

test "redo frontier rejects a torn publication" {
    const durable: Frontier = .{ .generation = 12, .sequence = 97, .end_offset = 4096 };
    const next: Frontier = .{ .generation = 12, .sequence = 98, .end_offset = 4198 };
    const raw = durable.encode();
    try std.testing.expectEqual(durable, Frontier.decode(&raw).?);
    for (raw, 0..) |_, i| {
        var damaged = raw;
        damaged[i] ^= 1;
        try std.testing.expect(Frontier.decode(&damaged) == null);
    }
    var torn = raw;
    const next_raw = next.encode();
    torn[0..32].* = next_raw[0..32].*;
    try std.testing.expect(Frontier.decode(&torn) == null);
}

pub const RecordHeader = struct {
    magic: u32 = magic,
    format_version: u16 = format_version,
    header_len: u16 = header_len,
    payload_len: u32,
    commit_sequence: u64,
    crc32: u32,
    salt: u64 = 0,

    pub fn encode(self: RecordHeader, out: *[header_len]u8) void {
        std.mem.writeInt(u32, out[0..4], self.magic, .little);
        std.mem.writeInt(u16, out[4..6], self.format_version, .little);
        std.mem.writeInt(u16, out[6..8], @intCast(self.header_len), .little);
        std.mem.writeInt(u32, out[8..12], self.payload_len, .little);
        std.mem.writeInt(u64, out[12..20], self.commit_sequence, .little);
        std.mem.writeInt(u32, out[20..24], self.crc32, .little);
        std.mem.writeInt(u64, out[24..32], self.salt, .little);
    }

    /// Decodes a header from exactly `header_len` bytes. Does not validate
    /// magic/version/length against policy -- that is `Reader.replay`'s job,
    /// since a plain decode needs to be callable on untrusted bytes without
    /// deciding what to do about them.
    pub fn decode(bytes: *const [header_len]u8) RecordHeader {
        var header = decodeLegacy(bytes[0..legacy_header_len]);
        if (header.format_version != legacy_format_version) header.salt = std.mem.readInt(u64, bytes[24..32], .little);
        return header;
    }

    /// The checksum this header's record must carry. Version 2 covers the
    /// header's own fields with the payload, so damage to a committed
    /// record's salt or sequence fails it rather than reading as another
    /// run's record.
    pub fn checksum(self: RecordHeader, payload: []const u8) u32 {
        if (self.format_version == legacy_format_version) return Crc32.hash(payload);
        var fields: [20]u8 = undefined;
        std.mem.writeInt(u32, fields[0..4], self.payload_len, .little);
        std.mem.writeInt(u64, fields[4..12], self.commit_sequence, .little);
        std.mem.writeInt(u64, fields[12..20], self.salt, .little);
        var crc = Crc32.init();
        crc.update(&fields);
        crc.update(payload);
        return crc.final();
    }

    /// This header with the checksum `payload` gives it.
    pub fn sealed(self: RecordHeader, payload: []const u8) RecordHeader {
        var header = self;
        header.crc32 = header.checksum(payload);
        return header;
    }

    fn decodeLegacy(bytes: *const [legacy_header_len]u8) RecordHeader {
        return .{
            .magic = std.mem.readInt(u32, bytes[0..4], .little),
            .format_version = std.mem.readInt(u16, bytes[4..6], .little),
            .header_len = std.mem.readInt(u16, bytes[6..8], .little),
            .payload_len = std.mem.readInt(u32, bytes[8..12], .little),
            .commit_sequence = std.mem.readInt(u64, bytes[12..20], .little),
            .crc32 = std.mem.readInt(u32, bytes[20..24], .little),
        };
    }

    /// The header's length when it is a well-formed header of a known
    /// version, else null.
    fn knownLength(self: RecordHeader) ?usize {
        if (self.magic != magic) return null;
        if (self.format_version == format_version and self.header_len == header_len) return header_len;
        if (self.format_version == legacy_format_version and self.header_len == legacy_header_len) return legacy_header_len;
        return null;
    }
};

/// "ARDE" read as a little-endian u32: an end trailer.
pub const trailer_magic: u32 = 0x45445241;

/// What every append writes right after its record, where the next record's
/// header will go: the run's salt and the record's sequence, under their own
/// checksum. It is a durable end boundary apart from the record itself, so
/// damage to a run's last record (which leaves nothing of the run after it)
/// is still told apart from the run's end: the damaged record's header
/// still leads to its trailer.
pub const Trailer = struct {
    sequence: u64,
    salt: u64,

    pub fn encode(self: Trailer, out: *[header_len]u8) void {
        @memset(out, 0);
        std.mem.writeInt(u32, out[0..4], trailer_magic, .little);
        std.mem.writeInt(u16, out[4..6], format_version, .little);
        std.mem.writeInt(u16, out[6..8], @intCast(header_len), .little);
        std.mem.writeInt(u64, out[12..20], self.sequence, .little);
        std.mem.writeInt(u64, out[24..32], self.salt, .little);
        std.mem.writeInt(u32, out[20..24], trailerChecksum(out), .little);
    }

    fn decode(bytes: *const [header_len]u8) ?Trailer {
        if (std.mem.readInt(u32, bytes[0..4], .little) != trailer_magic) return null;
        if (std.mem.readInt(u16, bytes[4..6], .little) != format_version) return null;
        if (std.mem.readInt(u16, bytes[6..8], .little) != header_len) return null;
        if (!isAllZero(bytes[8..12])) return null;
        if (std.mem.readInt(u32, bytes[20..24], .little) != trailerChecksum(bytes)) return null;
        return .{ .sequence = std.mem.readInt(u64, bytes[12..20], .little), .salt = std.mem.readInt(u64, bytes[24..32], .little) };
    }

    fn trailerChecksum(bytes: *const [header_len]u8) u32 {
        var crc = Crc32.init();
        crc.update(bytes[12..20]);
        crc.update(bytes[24..32]);
        return crc.final();
    }
};

/// Reads the header at `offset` (`available` bytes remain there). Version
/// 1 headers are 24 bytes and carry salt 0.
const ReadHeader = union(enum) {
    /// Fewer bytes than a header needs.
    short,
    /// An all-zero header: the log's end (a version 1 end marker, or space
    /// never written).
    zero,
    /// An end trailer.
    trailer: Trailer,
    /// Not a header of any known version.
    garbage,
    record: struct { header: RecordHeader, len: usize },
};

fn readHeaderAt(file: File, io: Io, offset: u64, available: u64) !ReadHeader {
    if (available < legacy_header_len) return .short;
    var buf: [header_len]u8 = @splat(0);
    const want: usize = @intCast(@min(available, header_len));
    const got = try file.readPositionalAll(io, buf[0..want], offset);
    if (got < legacy_header_len) return .short;
    if (isAllZero(buf[0..legacy_header_len])) return .zero;
    if (std.mem.readInt(u32, buf[0..4], .little) == trailer_magic) {
        if (got < header_len) return .short;
        return if (Trailer.decode(&buf)) |trailer| .{ .trailer = trailer } else .garbage;
    }
    var header = RecordHeader.decodeLegacy(buf[0..legacy_header_len]);
    const len = header.knownLength() orelse return .garbage;
    if (len == header_len) {
        if (got < header_len) return .short;
        header.salt = std.mem.readInt(u64, buf[24..32], .little);
    }
    return .{ .record = .{ .header = header, .len = len } };
}

pub const AppendError = error{
    /// The record (header + payload) would not fit in
    /// `[base_offset, base_offset + capacity)`.
    LogFull,
    /// Reading the log's first header (for its salt) failed.
    ReadFailed,
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
    /// This run of the log's salt; learned on the first append (see
    /// `currentSalt`).
    salt: ?u64 = null,

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

    /// The salt every record of this run of the log carries: a fresh random
    /// one when the log restarts at its first byte, else the salt of the
    /// record there (0 for a version 1 log), whose run this writer resumes.
    fn currentSalt(self: *Writer) error{ReadFailed}!u64 {
        if (self.salt) |salt| return salt;
        const salt: u64 = if (self.tail == 0) self.freshSalt() else switch (readHeaderAt(self.file, self.io, self.base_offset, self.capacity) catch return error.ReadFailed) {
            .record => |first| first.header.salt,
            // A resumed log always starts with a record; without one,
            // nothing can share this run.
            .short, .zero, .trailer, .garbage => self.freshSalt(),
        };
        self.salt = salt;
        return salt;
    }

    fn freshSalt(self: *Writer) u64 {
        while (true) {
            var bytes: [8]u8 = undefined;
            osRandom(&bytes, self);
            const salt = std.mem.readInt(u64, &bytes, .little);
            if (salt != 0) return salt;
        }
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
        if (record_len + header_len > self.remaining()) return error.LogFull;

        var header_buf: [header_len]u8 = undefined;
        const header = (RecordHeader{
            .payload_len = @intCast(payload.len),
            .commit_sequence = commit_sequence,
            .crc32 = 0,
            .salt = try self.currentSalt(),
        }).sealed(payload);
        header.encode(&header_buf);

        const offset = self.base_offset + self.tail;
        // Writer-race investigation (docs/design/lite-performance/
        // lite-single-commit-write-path.md §8/§9): the payload is written
        // *before* the header, not after. These are two separate
        // `writePositionalAll` calls -- not atomic with each other -- and
        // this region is a fixed, preallocated extent (§9.1), so "ran out
        // of bytes" never naturally happens; the only way `replay` can
        // tell a torn write apart from real corruption is by looking at
        // whether a *complete, valid* header exists here at all
        // (`isAllZero`/magic-mismatch -> torn; a complete header whose
        // payload fails its own CRC -> corrupt, a hard stop). Writing the
        // header last means every torn write -- whether it's the payload
        // call that fails, or the header call itself only partially lands
        // -- leaves this record's own header incomplete or still
        // all-zero, which `replay` already treats as a benign end of the
        // valid prefix. The old order (header first) could leave a
        // *complete, correctly-formed* header whose claimed CRC covers a
        // payload that a subsequent failed write never actually wrote --
        // indistinguishable from genuine corruption, since the
        // preallocated region never reads back short. Reproduced via
        // "crash sweep: full text recovers from one transient io fault
        // with no crash": a `.write`-kind fault landing on the payload
        // call, after the header call already succeeded, surfaced as
        // `error.NativeRedoLogCorrupt` on reopen rather than the clean,
        // retryable torn-tail recovery this design promises for exactly
        // this case.
        //
        // A header-sized end trailer goes right after the record,
        // in the same write as the payload. A failed earlier append may
        // have left payload bytes past this record's end (recovery clears
        // only a torn header), and without the marker replay would read
        // them as the next header whenever this record is shorter.
        var end_marker: [header_len]u8 = undefined;
        (Trailer{ .sequence = commit_sequence, .salt = header.salt }).encode(&end_marker);
        try writePositionalPairAll(self.file, self.io, payload, &end_marker, offset + header_len);
        try self.file.writePositionalAll(self.io, &header_buf, offset);

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

    /// A saved writer position, captured before one or more `append` calls
    /// so a caller whose own durability barrier (`sync()`) fails afterward
    /// can undo them via `rollbackTo`. See that function's own doc comment
    /// for why this matters.
    pub const Mark = struct { tail: u64, last_sequence: ?u64 };

    /// Captures the writer's current position.
    pub fn mark(self: *const Writer) Mark {
        return .{ .tail = self.tail, .last_sequence = self.last_sequence };
    }

    /// Undoes every `append` made since `m` was captured: zeroes the
    /// *complete* byte range those appends wrote -- every rolled-back
    /// record's payload as well as its header, not just the header -- so a
    /// later `replay` sees `m.tail` onward as untouched (`isAllZero` on
    /// every header it meets there -> benign `end_of_region`, never
    /// `corrupt_record`), then resets `tail`/`last_sequence` back to `m` so
    /// the next real `append` reuses this same byte range and sequence
    /// number instead of leaving a gap. Zeroing only each record's own
    /// header is not enough: a later retry that writes a *shorter* record
    /// at `m.tail` would leave the old, longer record's own stale payload
    /// tail sitting right after the new one's declared length -- exactly
    /// where `replay` next expects either a valid header or untouched
    /// zero-fill, and neither is what it would find.
    ///
    /// Why this is needed at all: `append` physically writes a record's
    /// payload and header *before* the caller's own `sync()` ever runs
    /// (§9.3's "one append, one fsync" batches many appends behind one
    /// barrier) -- so the moment it returns, the record is already
    /// indistinguishable, on disk, from a durably-synced one: a later
    /// `replay` has no way to tell "written but this writer's own barrier
    /// never confirmed it" apart from "written and confirmed," since both
    /// look like a complete, correctly-sequenced, checksummed record.
    /// Without rolling back here, a `sync()` failure right after a
    /// successful `append` leaves `tail`/`last_sequence` permanently ahead
    /// of whatever the caller's own logical state (`self.header`,
    /// `durable_sequence`) rolls back to on that same failure -- every
    /// later commit then computes its next `commit_sequence` from that
    /// unrolled-back logical state, colliding with this writer's
    /// already-advanced `last_sequence` and failing `append`'s own
    /// continuity check forever (`error.SequenceOutOfOrder`, surfacing as
    /// the equally far less specific `error.OutcomeUnknown`) -- a one-way
    /// lockout exactly like the one `RedoCommitState`'s own ride-along
    /// append-ordering fix (`native.zig`) already closed for `append`
    /// itself failing outright. This closes the matching gap for `append`
    /// succeeding but the barrier right after it failing.
    pub fn rollbackTo(self: *Writer, m: Mark) void {
        if (m.tail >= self.tail) return;
        const zeros: [4096]u8 = @splat(0);
        // The record's trailer lies just outside the writer cursor.
        const erase_end = @min(self.capacity, self.tail +| header_len);
        var pos = m.tail;
        while (pos < erase_end) {
            const chunk = @min(zeros.len, erase_end - pos);
            // Best-effort, like every other write into this fixed,
            // preallocated extent (§9.1): a failure here leaves some stale
            // bytes behind, no worse off than not rolling back at all, so
            // it must never stop `tail`/`last_sequence` from being reset
            // below regardless.
            self.file.writePositionalAll(self.io, zeros[0..chunk], self.base_offset + pos) catch break;
            pos += chunk;
        }
        // The record before the mark ends in a trailer again.
        if (m.last_sequence) |sequence| if (self.salt) |salt| if (m.tail != 0 and m.tail + header_len <= self.capacity) {
            var trailer: [header_len]u8 = undefined;
            (Trailer{ .sequence = sequence, .salt = salt }).encode(&trailer);
            self.file.writePositionalAll(self.io, &trailer, self.base_offset + m.tail) catch {};
        };
        self.tail = m.tail;
        self.last_sequence = m.last_sequence;
    }
};

/// Fills `out` from the OS without `Io.random`, which takes the `Io`'s own
/// mutex and so must not run under every caller this writer has.
fn osRandom(out: []u8, writer: *const Writer) void {
    switch (builtin.os.tag) {
        .linux => {
            var filled: usize = 0;
            while (filled < out.len) {
                const rc = std.os.linux.getrandom(out[filled..].ptr, out.len - filled, 0);
                if (std.os.linux.errno(rc) != .SUCCESS) break;
                filled += rc;
            }
            if (filled == out.len) return;
        },
        .macos, .ios, .tvos, .watchos, .visionos, .maccatalyst, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => {
            std.c.arc4random_buf(out.ptr, out.len);
            return;
        },
        else => {},
    }
    // No OS source: still distinct per run (time, process, position).
    const Local = struct {
        var counter = std.atomic.Value(u64).init(0);
    };
    var hasher = std.hash.Wyhash.init(Local.counter.fetchAdd(1, .monotonic));
    hasher.update(std.mem.asBytes(&writer.base_offset));
    hasher.update(std.mem.asBytes(&writer.last_sequence));
    const address = @intFromPtr(writer);
    hasher.update(std.mem.asBytes(&address));
    var instant: std.c.timespec = undefined;
    if (builtin.link_libc and std.c.clock_gettime(.MONOTONIC, &instant) == 0) hasher.update(std.mem.asBytes(&instant));
    var value = hasher.final();
    var i: usize = 0;
    while (i < out.len) : (i += 8) {
        const chunk = std.mem.asBytes(&value);
        const n = @min(8, out.len - i);
        @memcpy(out[i..][0..n], chunk[0..n]);
        value = std.hash.Wyhash.hash(value, chunk);
    }
}

/// Writes `first` then `second` contiguously at `offset`, in one vectored
/// write when the kernel takes it all.
fn writePositionalPairAll(file: File, io: Io, first: []const u8, second: []const u8, offset: u64) !void {
    const total = first.len + second.len;
    var written: usize = 0;
    while (written < total) {
        if (written < first.len) {
            written += try file.writePositional(io, &.{ first[written..], second }, offset + written);
        } else {
            const at = written - first.len;
            written += try file.writePositional(io, &.{second[at..]}, offset + written);
        }
    }
}

/// One decoded, checksum-and-sequence-validated record, handed to a
/// `Reader.replay` callback. `payload` aliases the caller-supplied scratch
/// buffer and is only valid for the duration of the callback.
pub const RecordView = struct {
    commit_sequence: u64,
    payload: []const u8,
    /// The record's offset relative to the replay's `base_offset`.
    offset: u64 = 0,
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
    /// The next record is valid so far but lies past the caller's
    /// `max_sequence`: not yet published as durable, so not delivered.
    /// The valid prefix ends before it.
    durable_ceiling,
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
    generation: ?u64 = null,
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
    return replayBounded(allocator, file, io, base_offset, capacity, start_after_sequence, null, null, on_record, ctx);
}

/// `replay`, stopping (`durable_ceiling`) before any new record whose
/// sequence exceeds `max_sequence`. A reader that is not the writer bounds
/// its scan by the writer's published durable sequence: records past it may
/// still be unsynced, and a crash can lose them.
///
/// `unlogged_through` is the checkpoint slot's own commit sequence: a slot
/// can publish commits that never reach the log (a ledger-only reclaim, a
/// capacity shrink), so a jump between logged records over sequences no
/// later than it is not a gap. Continuity past it stays strict.
pub fn replayBounded(
    allocator: Allocator,
    file: File,
    io: Io,
    base_offset: u64,
    capacity: u64,
    start_after_sequence: ?u64,
    max_sequence: ?u64,
    unlogged_through: ?u64,
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

    // The salt of the first record this scan reads; see the module doc.
    var run_salt: ?u64 = null;
    // Only a folded log is ever restarted, so only one can hold a retired
    // run's bytes past its live tail.
    const restartable = (start_after_sequence orelse 0) != 0;

    while (true) {
        const remaining = capacity - pos;
        const header, const record_header_len = switch (try readHeaderAt(file, io, base_offset + pos, remaining)) {
            // Too little room left for any header: the region's end.
            .short => return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = if (remaining < legacy_header_len) .end_of_region else .torn_tail },
            // The append writes the payload and trailer before its
            // header. At a zero header those bytes cannot distinguish an
            // interrupted append from an erased record. The durable
            // sequence promised by the caller is checked at recovery.
            .zero => return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .end_of_region },
            // This run's end. A retired run's trailer (another salt) past a
            // lost one is the end too, after a restart.
            .trailer => |trailer| return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = stop: {
                const salt = run_salt orelse break :stop .end_of_region;
                if (trailer.salt == salt) break :stop if (trailer.sequence == (last_sequence orelse 0)) .end_of_region else try unclearStop(file, io, base_offset, capacity, pos, restartable, run_salt, last_sequence);
                break :stop if (restartable) .end_of_region else .corrupt_record;
            } },
            // A log restart zeroes only the first header, so a restarted
            // log keeps the retired log's bytes past its live tail; a crash
            // that keeps a record but loses its end marker exposes them.
            // Unparseable bytes after the first record of a log that has
            // been folded (the only kind ever restarted) may be the middle
            // of a retired record: the end, as in point-in-time recovery.
            .garbage => return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = try unclearStop(file, io, base_offset, capacity, pos, restartable, run_salt, last_sequence) },
            .record => |record| .{ record.header, record.len },
        };
        // A retired record, or retired payload bytes that happen to read as
        // a header, never carries this run's salt (chosen after they were
        // written): the log ends here. And a retired sequence is at or
        // below the fold that retired it, below every live record, so after
        // a live record a header that does not move forward is the end too.
        // What retired bytes cannot present stays corrupt below: this
        // run's salt with the next sequence and a bad checksum, or a gap.
        if (run_salt) |salt| if (header.salt != salt) {
            // A retired run's record is whole under its own salt: the end.
            // Anything else here (a committed record with a damaged salt,
            // retired payload bytes that read as a header) is unclear.
            const complete = try readRecordPayload(allocator, file, io, base_offset + pos, remaining, header, record_header_len, &payload_buf);
            const retired = complete and header.checksum(payload_buf.items) == header.crc32;
            // The older writer could leave no trailer at the extent edge.
            // A checksum that matches only with this run's salt provides
            // evidence that its last record is damaged, not retired bytes.
            if (complete and !retired and record_header_len == header_len and
                remaining - record_header_len - header.payload_len < header_len and
                header.commit_sequence > (last_sequence orelse 0))
            {
                var corrected = header;
                corrected.salt = salt;
                if (corrected.checksum(payload_buf.items) == header.crc32) {
                    return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .corrupt_record };
                }
            }
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = if (retired and restartable) .end_of_region else try unclearStop(file, io, base_offset, capacity, pos, restartable, run_salt, last_sequence) };
        };

        if (max_sequence) |ceiling| {
            const new_record = start_after_sequence == null or header.commit_sequence > start_after_sequence.?;
            if (new_record and header.commit_sequence > ceiling) {
                return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .durable_ceiling };
            }
        }

        const payload_len: u64 = header.payload_len;
        if (record_header_len + payload_len > remaining) {
            // This framing can be left by an interrupted header write.
            // Protected recovery independently verifies the promised end.
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .torn_tail };
        }

        payload_buf.clearRetainingCapacity();
        try payload_buf.resize(allocator, payload_len);
        if (payload_len != 0) {
            const payload_got = try file.readPositionalAll(io, payload_buf.items, base_offset + pos + record_header_len);
            if (payload_got < payload_len) {
                return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .torn_tail };
            }
        }

        if (header.checksum(payload_buf.items) != header.crc32) {
            return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .corrupt_record };
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
            // run. Gaps among the superseded records are the checkpoints'
            // own consumed sequences, but the baseline covers everything
            // through itself: the first new record must follow it exactly,
            // or jump only over sequences the checkpoint slot covers
            // without a log record (`unlogged_through`). Anything else is a
            // missing commit. Strict `+1` resumes from here on (the `else`
            // branch below), since every record from here on was actually
            // logged by a live `Writer.append`, which always enforces `+1`
            // for anything it accepts.
            const baseline = start_after_sequence.?;
            const covered_jump = header.commit_sequence > baseline + 1 and unlogged_through != null and header.commit_sequence - 1 <= unlogged_through.?;
            if (header.commit_sequence != baseline + 1 and !covered_jump) {
                return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .corrupt_record };
            }
            in_superseded_run = false;
        } else if (last_sequence) |last| {
            const covered_jump = header.commit_sequence > last + 1 and unlogged_through != null and header.commit_sequence - 1 <= unlogged_through.?;
            if (header.commit_sequence != last + 1 and !covered_jump) {
                return .{ .valid_count = count, .valid_bytes = pos, .last_sequence = last_sequence, .generation = run_salt, .stop_reason = .corrupt_record };
            }
        }

        if (!superseded) {
            if (on_record) |cb| {
                try cb(ctx, .{ .commit_sequence = header.commit_sequence, .payload = payload_buf.items, .offset = pos });
            }
            count += 1;
        }

        if (run_salt == null) run_salt = header.salt;
        pos += record_header_len + payload_len;
        last_sequence = header.commit_sequence;
    }
}

/// The sequence of a complete, checksummed record at the very start of the
/// log that is newer than `floor`, or null: an ended log (zero header), a
/// torn or garbage first header, or a superseded record. Lets writer
/// authority tell "another writer reset this log and appended" apart from
/// "the old log is still here" without scanning it.
pub fn firstRecordNewerThan(allocator: Allocator, file: File, io: Io, base_offset: u64, capacity: u64, floor: u64) !?u64 {
    const header, const len = switch (try readHeaderAt(file, io, base_offset, capacity)) {
        .record => |record| .{ record.header, record.len },
        .short, .zero, .trailer, .garbage => return null,
    };
    if (header.commit_sequence <= floor) return null;
    if (len + @as(u64, header.payload_len) > capacity) return null;
    const payload = try allocator.alloc(u8, header.payload_len);
    defer allocator.free(payload);
    if (try file.readPositionalAll(io, payload, base_offset + len) < payload.len) return null;
    if (header.checksum(payload) != header.crc32) return null;
    return header.commit_sequence;
}

/// Reads a complete record payload at `offset`. Reuses `buf`.
fn readRecordPayload(allocator: Allocator, file: File, io: Io, offset: u64, available: u64, header: RecordHeader, header_bytes: usize, buf: *std.ArrayList(u8)) !bool {
    const payload_len: u64 = header.payload_len;
    if (header_bytes + payload_len > available) return false;
    buf.clearRetainingCapacity();
    try buf.resize(allocator, payload_len);
    return try file.readPositionalAll(io, buf.items, offset + header_bytes) == payload_len;
}

/// How a scan stops at `pos` on bytes that are neither this run's next
/// record nor its end. After a restart they may be the retired run's
/// (exposed by a lost end marker); or a committed record may be damaged.
/// The retired run's bytes were written before this run's salt was chosen,
/// so any later header carrying it means the run continues past `pos`:
/// damage, corrupt. With none, `pos` is the run's end. A log that was never
/// restarted (or a version 1 run, salt 0) holds no retired bytes: corrupt.
fn unclearStop(file: File, io: Io, base_offset: u64, capacity: u64, pos: u64, restartable: bool, run_salt: ?u64, last_sequence: ?u64) !StopReason {
    const salt = run_salt orelse return .corrupt_record;
    if (!restartable or salt == 0 or pos == 0) return .corrupt_record;
    if (try damagedRecordAt(file, io, base_offset, capacity, pos, salt, last_sequence orelse 0)) return .corrupt_record;
    return if (try laterRunHeader(file, io, base_offset, capacity, pos + 1, salt, last_sequence orelse 0)) .corrupt_record else .end_of_region;
}

/// Whether the bytes at `pos`, read as a header whatever its damage, lead
/// to this run's trailer for the next record: a committed record whose
/// header was damaged, even when it is the run's last.
fn damagedRecordAt(file: File, io: Io, base_offset: u64, capacity: u64, pos: u64, salt: u64, after: u64) !bool {
    if (pos + header_len > capacity) return false;
    var buf: [header_len]u8 = undefined;
    if (try file.readPositionalAll(io, &buf, base_offset + pos) < header_len) return false;
    const header = RecordHeader.decode(&buf);
    if (header.commit_sequence <= after) return false;
    const trailer_at = pos + header_len + @as(u64, header.payload_len);
    if (trailer_at + header_len > capacity) return false;
    if (try file.readPositionalAll(io, &buf, base_offset + trailer_at) < header_len) return false;
    const trailer = Trailer.decode(&buf) orelse return false;
    return trailer.salt == salt and trailer.sequence == header.commit_sequence;
}

/// Whether a version 2 header with `salt` and a sequence past `after`
/// appears anywhere in `[from, capacity)`.
fn laterRunHeader(file: File, io: Io, base_offset: u64, capacity: u64, from: u64, salt: u64, after: u64) !bool {
    var prefix: [8]u8 = undefined;
    std.mem.writeInt(u32, prefix[0..4], magic, .little);
    std.mem.writeInt(u16, prefix[4..6], format_version, .little);
    std.mem.writeInt(u16, prefix[6..8], @intCast(header_len), .little);
    var chunk: [64 * 1024]u8 = undefined;
    var start = from;
    while (start + header_len <= capacity) {
        const want: usize = @intCast(@min(chunk.len, capacity - start));
        const got = try file.readPositionalAll(io, chunk[0..want], base_offset + start);
        if (got < header_len) return false;
        var at: usize = 0;
        while (std.mem.indexOfPos(u8, chunk[0..got], at, &prefix)) |found| : (at = found + 1) {
            if (found + header_len > got) break;
            const header = RecordHeader.decode(chunk[found..][0..header_len]);
            if (header.salt == salt and header.commit_sequence > after) return true;
        }
        if (got < want) return false;
        // Overlap so a header straddling the chunk boundary is seen.
        start += got - (header_len - 1);
    }
    return false;
}

/// The salt of the record at the log's start (0 for a version 1 record),
/// or null when the log does not start with a record.
pub fn firstRecordSalt(file: File, io: Io, base_offset: u64, capacity: u64) !?u64 {
    return switch (try readHeaderAt(file, io, base_offset, capacity)) {
        .record => |record| record.header.salt,
        .short, .zero, .trailer, .garbage => null,
    };
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

test "an append ends the log even over a failed longer append's payload" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "x");
    // A failed longer append: its payload landed, its header did not.
    const abandoned: [256]u8 = @splat(0xC3);
    try region.file.writePositionalAll(io, &abandoned, writer.tail + header_len);
    try writer.append(12, "y");
    try writer.sync();

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, 10, null, null);
    try testing.expectEqual(@as(u64, 2), outcome.valid_count);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
}

test "replay accepts a jump over sequences the checkpoint slot covers" {
    const io = testing.io;
    const capacity: u64 = 2048;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    // The slot published 11 without logging it; the log resumes at 12.
    var writer = Writer.initAt(region.file, io, 0, capacity, 0, 11);
    try writer.append(12, "x");
    try writer.append(13, "y");
    try writer.sync();

    const covered = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, 11, null, null);
    try testing.expectEqual(@as(u64, 2), covered.valid_count);
    try testing.expectEqual(StopReason.end_of_region, covered.stop_reason);

    // Without the slot's coverage it is a gap.
    const uncovered = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.corrupt_record, uncovered.stop_reason);
    // Coverage never excuses a gap past the slot.
    const past = try replayBounded(testing.allocator, region.file, io, 0, capacity, 9, null, 10, null, null);
    try testing.expectEqual(StopReason.corrupt_record, past.stop_reason);
}

test "replay rejects a missing commit right after a superseded run" {
    const io = testing.io;
    const capacity: u64 = 2048;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    // A record a pinned reader kept past the checkpoint at 10, then 12:
    // commit 11 never reached the log.
    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(9, "old");
    writer.last_sequence = 11;
    try writer.append(12, "new");
    try writer.sync();

    const missing = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, 10, null, null);
    try testing.expectEqual(StopReason.corrupt_record, missing.stop_reason);
    try testing.expectEqual(@as(u64, 0), missing.valid_count);
    // The slot covering 11 without a record explains the jump.
    const covered = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, 11, null, null);
    try testing.expectEqual(StopReason.end_of_region, covered.stop_reason);
    try testing.expectEqual(@as(u64, 1), covered.valid_count);
}

test "bounded replay stops before records past the durable ceiling" {
    const io = testing.io;
    const capacity: u64 = 2048;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "x");
    try writer.append(12, "y");
    try writer.append(13, "z");
    try writer.sync();

    const bounded = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, 12, null, null, null);
    try testing.expectEqual(@as(u64, 2), bounded.valid_count);
    try testing.expectEqual(@as(?u64, 12), bounded.last_sequence);
    try testing.expectEqual(StopReason.durable_ceiling, bounded.stop_reason);

    // Superseded records past the ceiling never stop the scan early.
    const past = try replayBounded(testing.allocator, region.file, io, 0, capacity, 12, 12, null, null, null);
    try testing.expectEqual(@as(u64, 0), past.valid_count);
    try testing.expectEqual(StopReason.durable_ceiling, past.stop_reason);
    try testing.expectEqual(@as(?u64, 12), past.last_sequence);

    const unbounded = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(@as(u64, 3), unbounded.valid_count);
}

/// Retires the log's first `retired` bytes as a reset does (first header
/// zeroed), appends `payload` at `sequence`, then puts the retired bytes
/// back where the new record's end marker went: a crash that kept the
/// record but not its marker.
fn reuseLosingMarker(io: Io, file: File, capacity: u64, sequence: u64, payload: []const u8) !u64 {
    const zeros: [header_len]u8 = @splat(0);
    const end = header_len + payload.len;
    var retired: [header_len]u8 = undefined;
    _ = try file.readPositionalAll(io, &retired, end);
    try file.writePositionalAll(io, &zeros, 0);
    var writer = Writer.initAt(file, io, 0, capacity, 0, sequence - 1);
    try writer.append(sequence, payload);
    try file.writePositionalAll(io, &retired, end);
    return end;
}

test "replay ends a reused log at a retired record past a lost end marker" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    // Retired records of one length, so the new record ends exactly where
    // a retired header begins.
    var writer = Writer.init(region.file, io, 0, capacity);
    for (1..6) |sequence| try writer.append(sequence, "retired!");
    // Folded through 5 (and a bare commit took 6); the reset log starts at 7.
    const end = try reuseLosingMarker(io, region.file, capacity, 7, "live-one");

    const outcome = try replayBounded(testing.allocator, region.file, io, 0, capacity, 5, null, 6, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(@as(?u64, 7), outcome.last_sequence);
    try testing.expectEqual(end, outcome.valid_bytes);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
}

test "replay ends a reused log at retired bytes mid-record past a lost end marker" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    for (1..4) |sequence| try writer.append(sequence, "a retired payload, long enough that the new record ends inside it, with no header there");
    const end = try reuseLosingMarker(io, region.file, capacity, 4, "short");

    const outcome = try replayBounded(testing.allocator, region.file, io, 0, capacity, 3, null, 3, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(end, outcome.valid_bytes);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);

    // A log never folded was never reset: the same bytes are corruption.
    const fresh = try replayBounded(testing.allocator, region.file, io, 0, capacity, null, null, null, null, null);
    try testing.expectEqual(StopReason.corrupt_record, fresh.stop_reason);
}

test "replay ends a reused log at a forged header inside a retired payload" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    // A retired payload holding what reads as the next live record: a
    // well-formed header with a forward sequence over a payload whose
    // checksum matches. Only the salt tells it apart.
    const live = "live";
    const forged_payload = "zz";
    const forged = (RecordHeader{ .payload_len = forged_payload.len, .commit_sequence = 8, .crc32 = 0 }).sealed(forged_payload);
    var retired_payload: [live.len + header_len + forged_payload.len]u8 = undefined;
    @memset(retired_payload[0..live.len], 'x');
    forged.encode(retired_payload[live.len..][0..header_len]);
    @memcpy(retired_payload[live.len + header_len ..], forged_payload);
    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, &retired_payload);

    const end = try reuseLosingMarker(io, region.file, capacity, 7, live);
    const outcome = try replayBounded(testing.allocator, region.file, io, 0, capacity, 5, null, 6, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(@as(?u64, 7), outcome.last_sequence);
    try testing.expectEqual(end, outcome.valid_bytes);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
}

test "replay reads a version 1 log and its writer continues it" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var pos: u64 = 0;
    for ([_][]const u8{ "one", "two" }, 1..) |payload, sequence| {
        var buf: [header_len]u8 = undefined;
        const legacy: RecordHeader = .{ .format_version = legacy_format_version, .header_len = legacy_header_len, .payload_len = @intCast(payload.len), .commit_sequence = sequence, .crc32 = Crc32.hash(payload) };
        legacy.encode(&buf);
        try region.file.writePositionalAll(io, buf[0..legacy_header_len], pos);
        try region.file.writePositionalAll(io, payload, pos + legacy_header_len);
        pos += legacy_header_len + payload.len;
    }
    const legacy_outcome = try replayBounded(testing.allocator, region.file, io, 0, capacity, null, null, null, null, null);
    try testing.expectEqual(@as(u64, 2), legacy_outcome.valid_count);
    try testing.expectEqual(pos, legacy_outcome.valid_bytes);

    // Resuming a version 1 log continues its run: salt 0.
    var writer = Writer.initAt(region.file, io, 0, capacity, pos, 2);
    try writer.append(3, "three");
    try testing.expectEqual(@as(?u64, 0), writer.salt);
    const outcome = try replayBounded(testing.allocator, region.file, io, 0, capacity, null, null, null, null, null);
    try testing.expectEqual(@as(u64, 3), outcome.valid_count);
    try testing.expectEqual(@as(?u64, 3), outcome.last_sequence);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
}

test "replay rejects a damaged salt inside a run, not a short log" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "a");
    const second = writer.tail;
    try writer.append(12, "b");
    const third = writer.tail;
    try writer.append(13, "c");

    // One flipped salt byte in the committed middle record: it fails its
    // own checksum, and the run's next record follows it.
    var byte: [1]u8 = undefined;
    _ = try region.file.readPositionalAll(io, &byte, second + 24);
    byte[0] ^= 0x5a;
    try region.file.writePositionalAll(io, &byte, second + 24);
    const damaged = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.corrupt_record, damaged.stop_reason);
    try testing.expectEqual(second, damaged.valid_bytes);

    // Damage to the last record's salt: its trailer, written after it, still
    // carries the run's salt and its sequence.
    byte[0] ^= 0x5a;
    try region.file.writePositionalAll(io, &byte, second + 24);
    _ = try region.file.readPositionalAll(io, &byte, third + 24);
    byte[0] ^= 0x5a;
    try region.file.writePositionalAll(io, &byte, third + 24);
    const last = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.corrupt_record, last.stop_reason);
    try testing.expectEqual(third, last.valid_bytes);

    // The legacy capacity rule could omit the trailer at the extent edge.
    const trailerless = try replayBounded(testing.allocator, region.file, io, 0, writer.tail, 10, null, null, null, null);
    try testing.expectEqual(StopReason.corrupt_record, trailerless.stop_reason);
    try testing.expectEqual(third, trailerless.valid_bytes);

    // A run that really ends there stops cleanly at the trailer.
    var clean = Writer.init(region.file, io, 0, capacity);
    try clean.append(11, "a");
    const ended = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.end_of_region, ended.stop_reason);
    try testing.expectEqual(clean.tail, ended.valid_bytes);
}

test "replay accepts an interrupted first append with a prepared trailer" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var trailer: [header_len]u8 = undefined;
    (Trailer{ .sequence = 11, .salt = 123 }).encode(&trailer);
    try writePositionalPairAll(region.file, io, "prepared", &trailer, header_len);
    const outcome = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
    try testing.expectEqual(@as(u64, 0), outcome.valid_bytes);
}

test "rollback of the first append erases its prepared trailer" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);
    var writer = Writer.initAt(region.file, io, 0, capacity, 0, 10);
    const mark = writer.mark();
    try writer.append(11, "rolled back");
    const trailer_at = writer.tail;
    writer.rollbackTo(mark);
    var trailer: [header_len]u8 = undefined;
    try testing.expectEqual(header_len, try region.file.readPositionalAll(io, &trailer, trailer_at));
    try testing.expect(isAllZero(&trailer));
}

test "replay reports a short prefix when a record length exceeds the extent" {
    const io = testing.io;
    const capacity: u64 = 2 * (header_len + 1) + header_len;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);
    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "a");
    const last = writer.tail;
    try writer.append(12, "b");

    // Older writers could fill the extent without writing a trailer.
    try region.file.writePositionalAll(io, &.{0x80}, last + 11);
    const outcome = try replayBounded(testing.allocator, region.file, io, 0, capacity - header_len, 10, null, null, null, null);
    try testing.expectEqual(StopReason.torn_tail, outcome.stop_reason);
    try testing.expectEqual(last, outcome.valid_bytes);

    try region.file.writePositionalAll(io, &.{0x80}, 11);
    const first = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.torn_tail, first.stop_reason);
    try testing.expectEqual(@as(u64, 0), first.valid_bytes);

    var single = Writer.init(region.file, io, 0, capacity);
    try single.append(11, "a");
    try region.file.writePositionalAll(io, &.{0x80}, 11);
    const only = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.torn_tail, only.stop_reason);
}

test "replay keeps rejecting what retired bytes can never be" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(11, "a");
    try writer.append(12, "b");
    // The next sequence with a bad checksum, after a folded baseline.
    const bad: RecordHeader = .{ .payload_len = 1, .commit_sequence = 13, .crc32 = 0xdeadbeef, .salt = writer.salt.? };
    var header_buf: [header_len]u8 = undefined;
    bad.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, writer.tail);
    try region.file.writePositionalAll(io, "c", writer.tail + header_len);
    const next = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(@as(u64, 2), next.valid_count);
    try testing.expectEqual(StopReason.corrupt_record, next.stop_reason);

    // A gap past the live tail.
    const gap = (RecordHeader{ .payload_len = 1, .commit_sequence = 15, .crc32 = 0, .salt = writer.salt.? }).sealed("c");
    gap.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, writer.tail);
    const gapped = try replayBounded(testing.allocator, region.file, io, 0, capacity, 10, null, null, null, null);
    try testing.expectEqual(StopReason.corrupt_record, gapped.stop_reason);
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
    const skip = (RecordHeader{ .payload_len = 1, .commit_sequence = 13, .crc32 = 0, .salt = writer.salt.? }).sealed("b");
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
    const skip = (RecordHeader{ .payload_len = 1, .commit_sequence = 6, .crc32 = 0, .salt = writer2.salt.? }).sealed("d");
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

test "rollbackTo undoes an append so a shorter retry at the same sequence does not leave stale payload garbage" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "one");
    try writer.sync();
    const mark_before_long = writer.mark();

    // A long record "appended" (as `RedoCommitState.appendRecord` does
    // before its own caller's `sync()` ever runs) then rolled back, as if
    // that `sync()` had failed.
    try writer.append(2, "a much longer payload than the retry below");
    writer.rollbackTo(mark_before_long);
    try testing.expectEqual(mark_before_long.tail, writer.tail);
    try testing.expectEqual(mark_before_long.last_sequence, writer.last_sequence);

    // The retry, at the same sequence, with a *shorter* payload: without
    // zeroing the long record's own payload tail (not just its header),
    // the long record's leftover bytes past the short one's own declared
    // length would sit exactly where `replay` next expects either a valid
    // header or untouched zero-fill.
    try writer.append(2, "short");
    try writer.sync();

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 2), outcome.valid_count);
    try testing.expectEqual(@as(u64, 2), outcome.last_sequence.?);
    try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);
}

test "rollbackTo is a no-op when nothing was appended since the mark" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "one");
    const m = writer.mark();
    writer.rollbackTo(m);
    try testing.expectEqual(m.tail, writer.tail);
    try testing.expectEqual(m.last_sequence, writer.last_sequence);
}

test "writer rejects a record that would overflow the region" {
    const io = testing.io;
    const capacity: u64 = 2 * header_len + 4;
    var short_region = try makeTempRegion(io, capacity - 1);
    defer cleanupTempRegion(io, &short_region.file, short_region.path[0..short_region.path_len]);
    var short_writer = Writer.init(short_region.file, io, 0, capacity - 1);
    try testing.expectError(error.LogFull, short_writer.append(1, "abcd"));
    try testing.expectEqual(@as(u64, 0), short_writer.tail);
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "abcd");
    try testing.expectError(error.LogFull, writer.append(2, "e"));
}

test "replay reports the valid prefix before an impossible header" {
    const io = testing.io;
    const capacity: u64 = 4096;
    var region = try makeTempRegion(io, capacity);
    defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);

    var writer = Writer.init(region.file, io, 0, capacity);
    try writer.append(1, "one");
    try writer.append(2, "two");
    try writer.sync();
    const good_tail = writer.tail;

    // Recovery needs a durable watermark to tell whether this short
    // prefix covers every acknowledged commit.
    var header_buf: [header_len]u8 = undefined;
    const torn: RecordHeader = .{ .payload_len = @intCast(capacity), .commit_sequence = 3, .crc32 = 0, .salt = writer.salt.? };
    torn.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, good_tail);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 2), outcome.valid_count);
    try testing.expectEqual(@as(u64, 2), outcome.last_sequence.?);
    try testing.expectEqual(good_tail, outcome.valid_bytes);
    try testing.expectEqual(StopReason.torn_tail, outcome.stop_reason);
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

// Writer-race investigation (docs/design/lite-performance/
// lite-single-commit-write-path.md §8/§9). `Writer.append` writes the
// payload *before* the header -- two separate `writePositionalAll` calls,
// not atomic with each other. This test proves why the order matters by
// constructing both halves of a torn append directly (the way a real
// transient I/O fault, interrupting one of the two calls, would leave the
// bytes) and checking `replay`'s verdict on each:
//
// - payload landed, header did not (what the *current*, fixed order
//   produces when the second call -- now the header -- fails): the header
//   bytes are still the region's untouched zero-fill, so `replay` sees a
//   benign end of the valid prefix (`end_of_region`), and a resumed
//   `Writer` can append right over it.
// - header landed, payload did not (what the *old*, header-first order
//   produced when the second call -- then the payload -- failed): a
//   complete, well-formed header whose claimed CRC covers payload bytes
//   that were never written reads back as a hard, unrecoverable
//   `corrupt_record` -- indistinguishable from real corruption, because
//   this region is a fixed, preallocated extent (§9.1) that never reads
//   back short. This is the exact failure `recoverRedoLog` surfaced as
//   `error.NativeRedoLogCorrupt` from "crash sweep: full text recovers
//   from one transient io fault with no crash" (a `.write`-kind fault
//   landing on the payload call after the header call had already
//   succeeded) before `Writer.append`'s write order was fixed.
test "a torn append leaves a benign end of region, never a false corrupt_record, regardless of which of its two writes failed" {
    const io = testing.io;
    const capacity: u64 = 4096;

    {
        var region = try makeTempRegion(io, capacity);
        defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);
        var writer = Writer.init(region.file, io, 0, capacity);
        try writer.append(1, "one");
        try writer.sync();
        const good_tail = writer.tail;

        // Simulate the payload call succeeding and the header call (the
        // second write `Writer.append` issues, after the fix) never
        // landing: write only the payload for a would-be record 2: header
        // position stays the region's untouched zero-fill.
        try region.file.writePositionalAll(io, "two", good_tail + header_len);

        const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
        try testing.expectEqual(@as(u64, 1), outcome.valid_count);
        try testing.expectEqual(@as(u64, 1), outcome.last_sequence.?);
        try testing.expectEqual(good_tail, outcome.valid_bytes);
        try testing.expectEqual(StopReason.end_of_region, outcome.stop_reason);

        // A writer resuming from this outcome appends right over the
        // orphaned payload bytes -- no manual cleanup needed.
        var resumed = Writer.initAt(region.file, io, 0, capacity, outcome.valid_bytes, outcome.last_sequence);
        try resumed.append(2, "two");
        try resumed.sync();
        const resumed_outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
        try testing.expectEqual(@as(u64, 2), resumed_outcome.valid_count);
        try testing.expectEqual(StopReason.end_of_region, resumed_outcome.stop_reason);
    }

    {
        var region = try makeTempRegion(io, capacity);
        defer cleanupTempRegion(io, &region.file, region.path[0..region.path_len]);
        var writer = Writer.init(region.file, io, 0, capacity);
        try writer.append(1, "one");
        try writer.sync();
        const good_tail = writer.tail;

        // The old, header-first order's own failure mode, reproduced
        // directly: a complete, well-formed header for record 2, correct
        // CRC and all, with the payload bytes it claims never written
        // (left as the region's untouched zero-fill). This is what the
        // old `Writer.append` order could produce; `Writer` itself can no
        // longer produce this shape, but `replay` must still call it what
        // it is -- a real, unrecoverable corruption -- since nothing else
        // can tell this apart from genuine on-disk damage.
        var header_buf: [header_len]u8 = undefined;
        const header = (RecordHeader{ .payload_len = 3, .commit_sequence = 2, .crc32 = 0, .salt = writer.salt.? }).sealed("two");
        header.encode(&header_buf);
        try region.file.writePositionalAll(io, &header_buf, good_tail);

        const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
        try testing.expectEqual(@as(u64, 1), outcome.valid_count);
        try testing.expectEqual(good_tail, outcome.valid_bytes);
        try testing.expectEqual(StopReason.corrupt_record, outcome.stop_reason);
    }
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
    const skip = (RecordHeader{ .payload_len = 4, .commit_sequence = 3, .crc32 = 0, .salt = writer.salt.? }).sealed("skip");
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
    var bad = (RecordHeader{ .payload_len = 2, .commit_sequence = 2, .crc32 = 0, .salt = writer.salt.? }).sealed("hi");
    bad.format_version = format_version + 1;
    bad.encode(&header_buf);
    try region.file.writePositionalAll(io, &header_buf, writer.tail);
    try region.file.writePositionalAll(io, "hi", writer.tail + header_len);

    const outcome = try replay(testing.allocator, region.file, io, 0, capacity, null, null, null);
    try testing.expectEqual(@as(u64, 1), outcome.valid_count);
    try testing.expectEqual(StopReason.corrupt_record, outcome.stop_reason);
}
