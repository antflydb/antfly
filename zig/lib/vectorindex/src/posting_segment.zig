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

//! Immutable posting-family segment container.
//!
//! This is the physical file-format foundation for a future
//! vector posting store. Payloads are deliberately opaque: packed posting
//! snapshots, quantized payload checkpoints, and mutation records can evolve
//! independently of this physical container. This module changes the physical
//! layout from one LSM key/value per record to one posting-local indexed
//! segment blob.

const std = @import("std");
const Allocator = std.mem.Allocator;
const posting = @import("posting.zig");

pub const PostingId = posting.PostingId;

const magic: [4]u8 = "AFPS".*;
const version: u16 = 3;
const min_supported_version: u16 = 2;
const index_entry_size: usize = 8 + 1 + 8 + 8 + 8 + 4;
const footer_size: usize = 8 + 8 + 4 + 2 + 2 + 4 + 4;

pub const EntryKind = enum(u8) {
    base = 1,
    delta = 2,
    centroid_directory = 3,
    quantized_checkpoint = 4,
    tombstone = 5,
    posting_state = 6,
    node_range = 7,
    vector_leaf = 8,
    vector_metadata = 9,
    index_metadata = 10,
    vector_directory = 11,
};

pub const DeltaValue = struct {
    sequence: u64,
    value: []const u8,
};

pub const SequencedValue = DeltaValue;

const PendingEntry = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
    value: []u8,
};

const EntryKey = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
};

const IndexEntry = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
    offset: usize,
    len: usize,
    checksum: u32,

    fn value(self: IndexEntry, data: []const u8) ![]const u8 {
        const bytes = try self.rawValue(data);
        if (std.hash.Crc32.hash(bytes) != self.checksum) return error.PostingSegmentChecksumMismatch;
        return bytes;
    }

    fn rawValue(self: IndexEntry, data: []const u8) ![]const u8 {
        const end = std.math.add(usize, self.offset, self.len) catch return error.CorruptedPostingSegment;
        if (end > data.len) return error.CorruptedPostingSegment;
        return data[self.offset..end];
    }
};

pub const Writer = struct {
    alloc: Allocator,
    entries: std.ArrayListUnmanaged(PendingEntry) = .empty,
    finished: bool = false,

    pub fn init(alloc: Allocator) Writer {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Writer) void {
        for (self.entries.items) |entry| self.alloc.free(entry.value);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendBase(self: *Writer, posting_id: PostingId, value: []const u8) !void {
        try self.appendBaseAt(posting_id, 0, value);
    }

    pub fn appendBaseAt(self: *Writer, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .base,
            .sequence = sequence,
        }, value);
    }

    /// Transfers ownership of `value`, including on error.
    pub fn appendBaseOwned(self: *Writer, posting_id: PostingId, value: []u8) !void {
        try self.appendBaseOwnedAt(posting_id, 0, value);
    }

    /// Transfers ownership of `value`, including on error.
    pub fn appendBaseOwnedAt(self: *Writer, posting_id: PostingId, sequence: u64, value: []u8) !void {
        try self.appendEntryOwned(.{
            .posting_id = posting_id,
            .kind = .base,
            .sequence = sequence,
        }, value);
    }

    pub fn appendCentroidDirectory(self: *Writer, posting_id: PostingId, value: []const u8) !void {
        try self.appendCentroidDirectoryAt(posting_id, 0, value);
    }

    pub fn appendCentroidDirectoryAt(self: *Writer, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .centroid_directory,
            .sequence = sequence,
        }, value);
    }

    pub fn appendQuantizedCheckpoint(self: *Writer, posting_id: PostingId, value: []const u8) !void {
        try self.appendQuantizedCheckpointAt(posting_id, 0, value);
    }

    pub fn appendQuantizedCheckpointAt(self: *Writer, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .quantized_checkpoint,
            .sequence = sequence,
        }, value);
    }

    /// Transfers ownership of `value`, including on error.
    pub fn appendQuantizedCheckpointOwned(self: *Writer, posting_id: PostingId, value: []u8) !void {
        try self.appendQuantizedCheckpointOwnedAt(posting_id, 0, value);
    }

    /// Transfers ownership of `value`, including on error.
    pub fn appendQuantizedCheckpointOwnedAt(self: *Writer, posting_id: PostingId, sequence: u64, value: []u8) !void {
        try self.appendEntryOwned(.{
            .posting_id = posting_id,
            .kind = .quantized_checkpoint,
            .sequence = sequence,
        }, value);
    }

    pub fn appendPostingStateAt(self: *Writer, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .posting_state,
            .sequence = sequence,
        }, value);
    }

    /// Appends any current HBC-derived value. The physical container is keyed
    /// by a generic u64 identity even though the historical API calls it a
    /// posting id; vector ids and the singleton metadata id therefore share
    /// the same compact index without key encoding overhead.
    pub fn appendValueAt(self: *Writer, id: PostingId, kind: EntryKind, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{ .posting_id = id, .kind = kind, .sequence = sequence }, value);
    }

    /// Transfers ownership of `value`, including on error.
    pub fn appendValueOwnedAt(self: *Writer, id: PostingId, kind: EntryKind, sequence: u64, value: []u8) !void {
        try self.appendEntryOwned(.{ .posting_id = id, .kind = kind, .sequence = sequence }, value);
    }

    pub fn appendTombstone(self: *Writer, posting_id: PostingId, sequence: u64) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .tombstone,
            .sequence = sequence,
        }, &.{});
    }

    pub fn appendDelta(self: *Writer, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .delta,
            .sequence = sequence,
        }, value);
    }

    fn appendEntry(self: *Writer, key: EntryKey, value: []const u8) !void {
        const owned = try self.alloc.dupe(u8, value);
        try self.appendEntryOwned(key, owned);
    }

    fn appendEntryOwned(self: *Writer, key: EntryKey, owned: []u8) !void {
        errdefer self.alloc.free(owned);
        if (self.finished) return error.PostingSegmentWriterFinished;
        try self.entries.append(self.alloc, .{
            .posting_id = key.posting_id,
            .kind = key.kind,
            .sequence = key.sequence,
            .value = owned,
        });
    }

    pub fn build(self: *Writer) ![]u8 {
        if (self.finished) return error.PostingSegmentWriterFinished;
        std.mem.sort(PendingEntry, self.entries.items, {}, pendingEntryLessThan);
        try rejectDuplicateEntries(self.entries.items);

        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(self.alloc);
        var index_entries = try std.ArrayListUnmanaged(IndexEntry).initCapacity(self.alloc, self.entries.items.len);
        defer index_entries.deinit(self.alloc);

        var output_len = footer_size;
        output_len = std.math.add(usize, output_len, std.math.mul(usize, self.entries.items.len, index_entry_size) catch return error.PostingSegmentTooLarge) catch return error.PostingSegmentTooLarge;
        for (self.entries.items) |entry| {
            output_len = std.math.add(usize, output_len, entry.value.len) catch return error.PostingSegmentTooLarge;
        }
        try out.ensureTotalCapacity(self.alloc, output_len);
        self.finished = true;

        // Consume each owned value after copying it into the final allocation.
        // With exact capacity reserved above, checkpoint construction retains
        // roughly one payload copy instead of holding complete input and output
        // payloads concurrently.
        for (self.entries.items) |*entry| {
            const offset = out.items.len;
            try out.appendSlice(self.alloc, entry.value);
            const value_len = entry.value.len;
            const checksum = std.hash.Crc32.hash(entry.value);
            self.alloc.free(entry.value);
            entry.value = &.{};
            index_entries.appendAssumeCapacity(.{
                .posting_id = entry.posting_id,
                .kind = entry.kind,
                .sequence = entry.sequence,
                .offset = offset,
                .len = value_len,
                .checksum = checksum,
            });
        }

        const index_offset = out.items.len;
        for (index_entries.items) |entry| try appendIndexEntry(self.alloc, &out, entry);
        const index_bytes = out.items[index_offset..];
        try appendU64(self.alloc, &out, @intCast(index_offset));
        try appendU64(self.alloc, &out, @intCast(index_entries.items.len));
        try appendU32(self.alloc, &out, std.hash.Crc32.hash(index_bytes));
        try appendU16(self.alloc, &out, version);
        try appendU16(self.alloc, &out, 0);
        const footer_without_checksum = out.items[out.items.len - 24 ..];
        try appendU32(self.alloc, &out, std.hash.Crc32.hash(footer_without_checksum));
        try out.appendSlice(self.alloc, &magic);
        std.debug.assert(out.items.len == output_len);
        return try out.toOwnedSlice(self.alloc);
    }
};

pub const Reader = struct {
    data: []const u8,
    index_offset: usize,
    entry_count: usize,

    pub fn init(data: []const u8) !Reader {
        if (data.len < footer_size) return error.CorruptedPostingSegment;
        const footer = data[data.len - footer_size ..];
        if (!std.mem.eql(u8, footer[footer_size - magic.len ..], &magic)) return error.BadPostingSegmentMagic;
        const segment_version = readU16(footer[20..22]);
        if (segment_version < min_supported_version or segment_version > version) return error.UnsupportedPostingSegmentVersion;
        if (readU16(footer[22..24]) != 0) return error.UnsupportedPostingSegmentFlags;
        if (readU32(footer[24..28]) != std.hash.Crc32.hash(footer[0..24])) return error.PostingSegmentChecksumMismatch;
        const index_offset_u64 = readU64(footer[0..8]);
        const entry_count_u64 = readU64(footer[8..16]);
        const index_offset = std.math.cast(usize, index_offset_u64) orelse return error.CorruptedPostingSegment;
        const entry_count = std.math.cast(usize, entry_count_u64) orelse return error.CorruptedPostingSegment;
        const index_bytes = std.math.mul(usize, entry_count, index_entry_size) catch return error.CorruptedPostingSegment;
        const index_end = std.math.add(usize, index_offset, index_bytes) catch return error.CorruptedPostingSegment;
        if (index_offset > data.len - footer_size or index_end != data.len - footer_size) return error.CorruptedPostingSegment;
        if (readU32(footer[16..20]) != std.hash.Crc32.hash(data[index_offset..index_end])) return error.PostingSegmentChecksumMismatch;
        const reader: Reader = .{
            .data = data,
            .index_offset = index_offset,
            .entry_count = entry_count,
        };
        try reader.validateIndex();
        return reader;
    }

    pub fn getBase(self: Reader, posting_id: PostingId) !?[]const u8 {
        const result = try self.getLatest(posting_id, .base);
        return if (result) |found| found.value else null;
    }

    pub fn getBaseValue(self: Reader, posting_id: PostingId) !?SequencedValue {
        return try self.getLatest(posting_id, .base);
    }

    pub fn getCentroidDirectory(self: Reader, posting_id: PostingId) !?[]const u8 {
        const result = try self.getLatest(posting_id, .centroid_directory);
        return if (result) |found| found.value else null;
    }

    pub fn getCentroidDirectoryValue(self: Reader, posting_id: PostingId) !?SequencedValue {
        return try self.getLatest(posting_id, .centroid_directory);
    }

    pub fn getQuantizedCheckpoint(self: Reader, posting_id: PostingId) !?[]const u8 {
        const result = try self.getLatest(posting_id, .quantized_checkpoint);
        return if (result) |found| found.value else null;
    }

    pub fn getQuantizedCheckpointValue(self: Reader, posting_id: PostingId) !?SequencedValue {
        return try self.getLatest(posting_id, .quantized_checkpoint);
    }

    pub fn getPostingState(self: Reader, posting_id: PostingId) !?[]const u8 {
        const result = try self.getLatest(posting_id, .posting_state);
        return if (result) |found| found.value else null;
    }

    pub fn getValue(self: Reader, id: PostingId, kind: EntryKind) !?[]const u8 {
        const result = try self.getLatest(id, kind);
        return if (result) |found| found.value else null;
    }

    /// Fingerprints the eagerly validated index and footer without touching
    /// opaque payload pages. `init` has already checked their shape and the
    /// index carries a checksum for every payload value.
    pub fn admissionChecksum(self: Reader) u32 {
        return std.hash.Crc32.hash(self.data[self.index_offset..]);
    }

    pub fn deltas(self: Reader, posting_id: PostingId) DeltaIterator {
        return .{
            .reader = self,
            .posting_id = posting_id,
            .index = self.lowerBound(posting_id, .delta, 0),
        };
    }

    fn getLatest(self: Reader, posting_id: PostingId, kind: EntryKind) !?SequencedValue {
        const index = (try self.latestIndex(posting_id, kind)) orelse return null;
        const found = try self.indexEntry(index);
        return .{ .sequence = found.sequence, .value = try found.value(self.data) };
    }

    fn latestIndex(self: Reader, posting_id: PostingId, kind: EntryKind) !?usize {
        var index = self.lowerBound(posting_id, kind, 0);
        var latest: ?usize = null;
        while (index < self.entry_count) : (index += 1) {
            const entry = try self.indexEntry(index);
            if (entry.posting_id != posting_id or entry.kind != kind) break;
            latest = index;
        }
        return latest;
    }

    fn lowerBound(self: Reader, posting_id: PostingId, kind: EntryKind, sequence: u64) usize {
        var lo: usize = 0;
        var hi: usize = self.entry_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const entry = self.indexEntry(mid) catch {
                hi = mid;
                continue;
            };
            if (compareEntryKey(entry.posting_id, entry.kind, entry.sequence, posting_id, kind, sequence) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    fn indexEntry(self: Reader, index: usize) !IndexEntry {
        if (index >= self.entry_count) return error.CorruptedPostingSegment;
        const pos = self.index_offset + index * index_entry_size;
        const raw = self.data[pos .. pos + index_entry_size];
        const kind: EntryKind = switch (raw[8]) {
            @intFromEnum(EntryKind.base) => .base,
            @intFromEnum(EntryKind.delta) => .delta,
            @intFromEnum(EntryKind.centroid_directory) => .centroid_directory,
            @intFromEnum(EntryKind.quantized_checkpoint) => .quantized_checkpoint,
            @intFromEnum(EntryKind.tombstone) => .tombstone,
            @intFromEnum(EntryKind.posting_state) => .posting_state,
            @intFromEnum(EntryKind.node_range) => .node_range,
            @intFromEnum(EntryKind.vector_leaf) => .vector_leaf,
            @intFromEnum(EntryKind.vector_metadata) => .vector_metadata,
            @intFromEnum(EntryKind.index_metadata) => .index_metadata,
            @intFromEnum(EntryKind.vector_directory) => .vector_directory,
            else => return error.CorruptedPostingSegment,
        };
        const offset = std.math.cast(usize, readU64(raw[17..25])) orelse return error.CorruptedPostingSegment;
        const len = std.math.cast(usize, readU64(raw[25..33])) orelse return error.CorruptedPostingSegment;
        return .{
            .posting_id = readU64(raw[0..8]),
            .kind = kind,
            .sequence = readU64(raw[9..17]),
            .offset = offset,
            .len = len,
            .checksum = readU32(raw[33..37]),
        };
    }

    fn validateIndex(self: Reader) !void {
        var previous: ?IndexEntry = null;
        var index: usize = 0;
        while (index < self.entry_count) : (index += 1) {
            const entry = try self.indexEntry(index);
            const end = std.math.add(usize, entry.offset, entry.len) catch return error.CorruptedPostingSegment;
            if (end > self.index_offset) return error.CorruptedPostingSegment;
            if (previous) |prev| {
                if (compareEntryKey(prev.posting_id, prev.kind, prev.sequence, entry.posting_id, entry.kind, entry.sequence) != .lt) {
                    return error.CorruptedPostingSegment;
                }
            }
            previous = entry;
        }
    }
};

/// Checksums the eagerly validated footer and index, excluding opaque payload
/// bytes that already carry lazy per-entry checksums. A durable publication
/// pointer can use this fingerprint to admit an mmap without faulting every
/// payload page at startup.
pub fn admissionChecksum(data: []const u8) !u32 {
    const reader = try Reader.init(data);
    return reader.admissionChecksum();
}

pub const VerifiedReader = struct {
    const Verification = std.atomic.Value(u8);
    const unknown: u8 = 0;
    const valid: u8 = 1;
    const corrupt: u8 = 2;

    alloc: Allocator,
    reader: Reader,
    verification: []Verification,

    pub fn init(alloc: Allocator, data: []const u8) !VerifiedReader {
        const reader = try Reader.init(data);
        const verification = try alloc.alloc(Verification, reader.entry_count);
        for (verification) |*state| state.* = Verification.init(unknown);
        return .{ .alloc = alloc, .reader = reader, .verification = verification };
    }

    pub fn deinit(self: *VerifiedReader) void {
        self.alloc.free(self.verification);
        self.* = undefined;
    }

    pub fn getBase(self: *VerifiedReader, posting_id: PostingId) !?[]const u8 {
        const result = try self.getLatest(posting_id, .base);
        return if (result) |found| found.value else null;
    }

    pub fn getQuantizedCheckpoint(self: *VerifiedReader, posting_id: PostingId) !?[]const u8 {
        const result = try self.getLatest(posting_id, .quantized_checkpoint);
        return if (result) |found| found.value else null;
    }

    pub fn getPostingState(self: *VerifiedReader, posting_id: PostingId) !?[]const u8 {
        const result = try self.getLatest(posting_id, .posting_state);
        return if (result) |found| found.value else null;
    }

    pub fn getValue(self: *VerifiedReader, id: PostingId, kind: EntryKind) !?[]const u8 {
        const result = try self.getLatest(id, kind);
        return if (result) |found| found.value else null;
    }

    /// Returns a nested container without checksumming its complete payload.
    /// The posting segment index still authenticates the container bounds;
    /// the nested format must validate its own header/index and lazily verify
    /// each value. Restrict this API so ordinary posting values cannot bypass
    /// their payload checksum by accident.
    pub fn getNestedContainer(self: *VerifiedReader, id: PostingId, kind: EntryKind) !?[]const u8 {
        if (kind != .vector_directory) return error.NotNestedPostingContainer;
        return try self.reader.getValue(id, kind);
    }

    fn getLatest(self: *VerifiedReader, posting_id: PostingId, kind: EntryKind) !?SequencedValue {
        const index = (try self.reader.latestIndex(posting_id, kind)) orelse return null;
        const entry = try self.reader.indexEntry(index);
        return .{ .sequence = entry.sequence, .value = try self.verifiedValue(index, entry) };
    }

    fn verifiedValue(self: *VerifiedReader, index: usize, entry: IndexEntry) ![]const u8 {
        const bytes = try entry.rawValue(self.reader.data);
        switch (self.verification[index].load(.acquire)) {
            valid => return bytes,
            corrupt => return error.PostingSegmentChecksumMismatch,
            else => {},
        }
        if (std.hash.Crc32.hash(bytes) != entry.checksum) {
            self.verification[index].store(corrupt, .release);
            return error.PostingSegmentChecksumMismatch;
        }
        self.verification[index].store(valid, .release);
        return bytes;
    }
};

pub const DeltaIterator = struct {
    reader: Reader,
    posting_id: PostingId,
    index: usize,

    pub fn next(self: *DeltaIterator) !?DeltaValue {
        if (self.index >= self.reader.entry_count) return null;
        const entry = try self.reader.indexEntry(self.index);
        if (entry.posting_id != self.posting_id or entry.kind != .delta) return null;
        self.index += 1;
        return .{
            .sequence = entry.sequence,
            .value = try entry.value(self.reader.data),
        };
    }
};

fn appendIndexEntry(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), entry: IndexEntry) !void {
    try appendU64(alloc, out, entry.posting_id);
    try out.append(alloc, @intFromEnum(entry.kind));
    try appendU64(alloc, out, entry.sequence);
    try appendU64(alloc, out, @intCast(entry.offset));
    try appendU64(alloc, out, @intCast(entry.len));
    try appendU32(alloc, out, entry.checksum);
}

fn rejectDuplicateEntries(entries: []const PendingEntry) !void {
    if (entries.len < 2) return;
    var i: usize = 1;
    while (i < entries.len) : (i += 1) {
        const prev = entries[i - 1];
        const cur = entries[i];
        if (prev.posting_id == cur.posting_id and prev.kind == cur.kind and prev.sequence == cur.sequence) {
            return error.DuplicatePostingSegmentEntry;
        }
    }
}

fn pendingEntryLessThan(_: void, lhs: PendingEntry, rhs: PendingEntry) bool {
    return compareEntryKey(lhs.posting_id, lhs.kind, lhs.sequence, rhs.posting_id, rhs.kind, rhs.sequence) == .lt;
}

fn compareEntryKey(lhs_posting_id: PostingId, lhs_kind: EntryKind, lhs_sequence: u64, rhs_posting_id: PostingId, rhs_kind: EntryKind, rhs_sequence: u64) std.math.Order {
    if (lhs_posting_id < rhs_posting_id) return .lt;
    if (lhs_posting_id > rhs_posting_id) return .gt;
    if (@intFromEnum(lhs_kind) < @intFromEnum(rhs_kind)) return .lt;
    if (@intFromEnum(lhs_kind) > @intFromEnum(rhs_kind)) return .gt;
    if (lhs_sequence < rhs_sequence) return .lt;
    if (lhs_sequence > rhs_sequence) return .gt;
    return .eq;
}

fn appendU16(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn readU16(bytes: *const [2]u8) u16 {
    return std.mem.readInt(u16, bytes, .big);
}

fn readU32(bytes: *const [4]u8) u32 {
    return std.mem.readInt(u32, bytes, .big);
}

fn readU64(bytes: *const [8]u8) u64 {
    return std.mem.readInt(u64, bytes, .big);
}

pub fn testStoresPointAndOrderedDeltaValues() !void {
    const alloc = std.testing.allocator;
    const base = "packed-posting-v1";
    const base_v2 = "packed-posting-v2";
    const delta_4 = "insert:40";
    const delta_5 = "delete:20";
    const centroid = "centroid-directory-v1";
    const quantized = "rabitq-checkpoint-v1";
    const posting_state = "posting-state-v1";

    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendDelta(7, 5, delta_5);
    try writer.appendCentroidDirectory(7, centroid);
    try writer.appendBaseAt(7, 10, base);
    try writer.appendBaseAt(7, 12, base_v2);
    try writer.appendQuantizedCheckpoint(7, quantized);
    try writer.appendPostingStateAt(7, 12, posting_state);
    try writer.appendDelta(7, 4, delta_4);

    const bytes = try writer.build();
    defer alloc.free(bytes);

    const reader = try Reader.init(bytes);
    try std.testing.expectEqualSlices(u8, base_v2, (try reader.getBase(7)).?);
    const latest_base = (try reader.getBaseValue(7)).?;
    try std.testing.expectEqual(@as(u64, 12), latest_base.sequence);
    try std.testing.expectEqualSlices(u8, centroid, (try reader.getCentroidDirectory(7)).?);
    try std.testing.expectEqualSlices(u8, quantized, (try reader.getQuantizedCheckpoint(7)).?);
    try std.testing.expectEqualSlices(u8, posting_state, (try reader.getPostingState(7)).?);
    try std.testing.expect(try reader.getBase(8) == null);

    var iter = reader.deltas(7);
    const first = (try iter.next()).?;
    try std.testing.expectEqual(@as(u64, 4), first.sequence);
    try std.testing.expectEqualSlices(u8, delta_4, first.value);
    const second = (try iter.next()).?;
    try std.testing.expectEqual(@as(u64, 5), second.sequence);
    try std.testing.expectEqualSlices(u8, delta_5, second.value);
    try std.testing.expect(try iter.next() == null);
}

pub fn testRejectsDuplicateLogicalEntries() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(1, "a");
    try writer.appendBase(1, "b");
    try std.testing.expectError(error.DuplicatePostingSegmentEntry, writer.build());
}

pub fn testValidatesFooterAndVersion() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(1, "a");
    const bytes = try writer.build();
    defer alloc.free(bytes);

    var bad_magic = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_magic);
    bad_magic[bad_magic.len - 1] = 'x';
    try std.testing.expectError(error.BadPostingSegmentMagic, Reader.init(bad_magic));

    var bad_version = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_version);
    bad_version[bad_version.len - magic.len - 7] = 1;
    try std.testing.expectError(error.UnsupportedPostingSegmentVersion, Reader.init(bad_version));
}

pub fn testValidatesIndexAndPayloadChecksums() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(1, "payload");
    const bytes = try writer.build();
    defer alloc.free(bytes);

    var bad_index = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_index);
    const index_offset: usize = @intCast(readU64(bad_index[bad_index.len - footer_size ..][0..8]));
    bad_index[index_offset] ^= 1;
    try std.testing.expectError(error.PostingSegmentChecksumMismatch, Reader.init(bad_index));

    var bad_payload = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_payload);
    bad_payload[0] ^= 1;
    const reader = try Reader.init(bad_payload);
    try std.testing.expectError(error.PostingSegmentChecksumMismatch, reader.getBase(1));
}

pub fn testVerifiedReaderMemoizesPayloadStatus() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(1, "payload");
    const bytes = try writer.build();
    defer alloc.free(bytes);

    var verified = try VerifiedReader.init(alloc, bytes);
    defer verified.deinit();
    try std.testing.expectEqualSlices(u8, "payload", (try verified.getBase(1)).?);
    try std.testing.expectEqualSlices(u8, "payload", (try verified.getBase(1)).?);
    const base_index = (try verified.reader.latestIndex(1, .base)).?;
    try std.testing.expectEqual(VerifiedReader.valid, verified.verification[base_index].load(.acquire));

    var corrupt_bytes = try alloc.dupe(u8, bytes);
    defer alloc.free(corrupt_bytes);
    corrupt_bytes[0] ^= 1;
    var corrupt_reader = try VerifiedReader.init(alloc, corrupt_bytes);
    defer corrupt_reader.deinit();
    try std.testing.expectError(error.PostingSegmentChecksumMismatch, corrupt_reader.getBase(1));
    try std.testing.expectError(error.PostingSegmentChecksumMismatch, corrupt_reader.getBase(1));
    const corrupt_index = (try corrupt_reader.reader.latestIndex(1, .base)).?;
    try std.testing.expectEqual(VerifiedReader.corrupt, corrupt_reader.verification[corrupt_index].load(.acquire));
}

test "posting segment stores base centroid and ordered delta values" {
    try testStoresPointAndOrderedDeltaValues();
}

test "posting segment rejects duplicate logical entries" {
    try testRejectsDuplicateLogicalEntries();
}

test "posting segment validates footer and version" {
    try testValidatesFooterAndVersion();
}

test "posting segment validates index and payload checksums" {
    try testValidatesIndexAndPayloadChecksums();
}

test "posting segment verified reader memoizes payload status" {
    try testVerifiedReaderMemoizesPayloadStatus();
}

test "posting segment stores compact HBC derived families" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendValueAt(7, .node_range, 10, "range");
    try writer.appendValueAt(42, .vector_leaf, 10, "leaf");
    try writer.appendValueAt(42, .vector_metadata, 10, "metadata");
    try writer.appendValueAt(0, .index_metadata, 10, "index");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    try std.testing.expect((try admissionChecksum(bytes)) != 0);
    var reader = try VerifiedReader.init(alloc, bytes);
    defer reader.deinit();
    try std.testing.expectEqualStrings("range", (try reader.getValue(7, .node_range)).?);
    try std.testing.expectEqualStrings("leaf", (try reader.getValue(42, .vector_leaf)).?);
    try std.testing.expectEqualStrings("metadata", (try reader.getValue(42, .vector_metadata)).?);
    try std.testing.expectEqualStrings("index", (try reader.getValue(0, .index_metadata)).?);
}

test "posting segment nested container avoids eager payload verification" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendValueAt(0, .vector_directory, 1, "nested payload");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    var reader = try VerifiedReader.init(alloc, bytes);
    defer reader.deinit();
    try std.testing.expectEqualStrings("nested payload", (try reader.getNestedContainer(0, .vector_directory)).?);
    try std.testing.expectError(error.NotNestedPostingContainer, reader.getNestedContainer(0, .base));
}
