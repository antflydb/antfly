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
const version: u16 = 1;
const index_entry_size: usize = 8 + 1 + 8 + 8 + 8;
const footer_size: usize = 8 + 8 + 2 + 4;

pub const EntryKind = enum(u8) {
    base = 1,
    delta = 2,
    centroid_directory = 3,
};

pub const DeltaValue = struct {
    sequence: u64,
    value: []const u8,
};

const PendingEntry = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
    value: []u8,
};

const IndexEntry = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
    offset: usize,
    len: usize,

    fn value(self: IndexEntry, data: []const u8) ![]const u8 {
        const end = std.math.add(usize, self.offset, self.len) catch return error.CorruptedPostingSegment;
        if (end > data.len) return error.CorruptedPostingSegment;
        return data[self.offset..end];
    }
};

pub const Writer = struct {
    alloc: Allocator,
    entries: std.ArrayListUnmanaged(PendingEntry) = .empty,

    pub fn init(alloc: Allocator) Writer {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Writer) void {
        for (self.entries.items) |entry| self.alloc.free(entry.value);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendBase(self: *Writer, posting_id: PostingId, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .base,
            .sequence = 0,
        }, value);
    }

    pub fn appendCentroidDirectory(self: *Writer, posting_id: PostingId, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .centroid_directory,
            .sequence = 0,
        }, value);
    }

    pub fn appendDelta(self: *Writer, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .delta,
            .sequence = sequence,
        }, value);
    }

    fn appendEntry(self: *Writer, key: struct {
        posting_id: PostingId,
        kind: EntryKind,
        sequence: u64,
    }, value: []const u8) !void {
        const owned = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned);
        try self.entries.append(self.alloc, .{
            .posting_id = key.posting_id,
            .kind = key.kind,
            .sequence = key.sequence,
            .value = owned,
        });
    }

    pub fn build(self: *Writer) ![]u8 {
        std.mem.sort(PendingEntry, self.entries.items, {}, pendingEntryLessThan);
        try rejectDuplicateEntries(self.entries.items);

        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(self.alloc);
        var index_entries = try std.ArrayListUnmanaged(IndexEntry).initCapacity(self.alloc, self.entries.items.len);
        defer index_entries.deinit(self.alloc);

        for (self.entries.items) |entry| {
            const offset = out.items.len;
            try out.appendSlice(self.alloc, entry.value);
            index_entries.appendAssumeCapacity(.{
                .posting_id = entry.posting_id,
                .kind = entry.kind,
                .sequence = entry.sequence,
                .offset = offset,
                .len = entry.value.len,
            });
        }

        const index_offset = out.items.len;
        for (index_entries.items) |entry| try appendIndexEntry(self.alloc, &out, entry);
        try appendU64(self.alloc, &out, @intCast(index_offset));
        try appendU64(self.alloc, &out, @intCast(index_entries.items.len));
        try appendU16(self.alloc, &out, version);
        try out.appendSlice(self.alloc, &magic);
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
        const segment_version = readU16(footer[16..18]);
        if (segment_version != version) return error.UnsupportedPostingSegmentVersion;
        const index_offset_u64 = readU64(footer[0..8]);
        const entry_count_u64 = readU64(footer[8..16]);
        const index_offset = std.math.cast(usize, index_offset_u64) orelse return error.CorruptedPostingSegment;
        const entry_count = std.math.cast(usize, entry_count_u64) orelse return error.CorruptedPostingSegment;
        const index_bytes = std.math.mul(usize, entry_count, index_entry_size) catch return error.CorruptedPostingSegment;
        const index_end = std.math.add(usize, index_offset, index_bytes) catch return error.CorruptedPostingSegment;
        if (index_offset > data.len - footer_size or index_end != data.len - footer_size) return error.CorruptedPostingSegment;
        return .{
            .data = data,
            .index_offset = index_offset,
            .entry_count = entry_count,
        };
    }

    pub fn getBase(self: Reader, posting_id: PostingId) !?[]const u8 {
        return try self.getExact(posting_id, .base, 0);
    }

    pub fn getCentroidDirectory(self: Reader, posting_id: PostingId) !?[]const u8 {
        return try self.getExact(posting_id, .centroid_directory, 0);
    }

    pub fn deltas(self: Reader, posting_id: PostingId) DeltaIterator {
        return .{
            .reader = self,
            .posting_id = posting_id,
            .index = self.lowerBound(posting_id, .delta, 0),
        };
    }

    fn getExact(self: Reader, posting_id: PostingId, kind: EntryKind, sequence: u64) !?[]const u8 {
        const index = self.lowerBound(posting_id, kind, sequence);
        if (index >= self.entry_count) return null;
        const entry = try self.indexEntry(index);
        if (entry.posting_id != posting_id or entry.kind != kind or entry.sequence != sequence) return null;
        return try entry.value(self.data);
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
        };
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

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn readU16(bytes: *const [2]u8) u16 {
    return std.mem.readInt(u16, bytes, .big);
}

fn readU64(bytes: *const [8]u8) u64 {
    return std.mem.readInt(u64, bytes, .big);
}

pub fn testStoresPointAndOrderedDeltaValues() !void {
    const alloc = std.testing.allocator;
    const base = "packed-posting-v1";
    const delta_4 = "insert:40";
    const delta_5 = "delete:20";
    const centroid = "centroid-directory-v1";

    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendDelta(7, 5, delta_5);
    try writer.appendCentroidDirectory(7, centroid);
    try writer.appendBase(7, base);
    try writer.appendDelta(7, 4, delta_4);

    const bytes = try writer.build();
    defer alloc.free(bytes);

    const reader = try Reader.init(bytes);
    try std.testing.expectEqualSlices(u8, base, (try reader.getBase(7)).?);
    try std.testing.expectEqualSlices(u8, centroid, (try reader.getCentroidDirectory(7)).?);
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
    bad_version[bad_version.len - magic.len - 1] = 2;
    try std.testing.expectError(error.UnsupportedPostingSegmentVersion, Reader.init(bad_version));
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
