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
//! `backend = segments, format = base_delta` vector posting store. The payloads
//! remain the existing logical posting values (`PostingFormat` base/delta and
//! `CentroidDirectoryFormat` records); this module only changes the physical
//! container from one LSM key/value per record to one posting-local indexed
//! segment blob.

const std = @import("std");
const Allocator = std.mem.Allocator;
const posting = @import("posting.zig");

pub const PostingId = posting.PostingId;

const magic: [4]u8 = "AFPS".*;
const manifest_magic: [4]u8 = "AFPM".*;
const version: u16 = 1;
const index_entry_size: usize = 8 + 1 + 8 + 8 + 8;
const footer_size: usize = 8 + 8 + 2 + 4;
const manifest_header_size: usize = 4 + 2 + 4 + 8;

pub const EntryKind = enum(u8) {
    base = 1,
    delta = 2,
    centroid_directory = 3,
};

pub const DeltaValue = struct {
    sequence: u64,
    value: []const u8,
};

pub const SegmentMeta = struct {
    segment_id: u64,
    min_posting_id: PostingId = 0,
    max_posting_id: PostingId = 0,
    min_delta_sequence: u64 = 0,
    max_delta_sequence: u64 = 0,
    byte_len: usize = 0,
    entry_count: usize = 0,

    pub fn mayContainPosting(self: SegmentMeta, posting_id: PostingId) bool {
        return self.entry_count != 0 and posting_id >= self.min_posting_id and posting_id <= self.max_posting_id;
    }
};

pub const SegmentBlob = struct {
    meta: SegmentMeta,
    data: []const u8,
};

pub const ManifestEntry = struct {
    meta: SegmentMeta,
    path: []const u8,
};

pub const Manifest = struct {
    next_segment_id: u64,
    segments: []const ManifestEntry,
};

pub const OwnedManifestEntry = struct {
    meta: SegmentMeta,
    path: []u8,
};

pub const OwnedManifest = struct {
    next_segment_id: u64,
    segments: []OwnedManifestEntry,

    pub fn deinit(self: *OwnedManifest, alloc: Allocator) void {
        for (self.segments) |entry| alloc.free(entry.path);
        alloc.free(self.segments);
        self.* = .{
            .next_segment_id = 0,
            .segments = &.{},
        };
    }
};

pub const OwnedSegmentStore = struct {
    manifest: OwnedManifest,
    owned_data: [][]u8,
    segments: []SegmentBlob,

    pub fn deinit(self: *OwnedSegmentStore, alloc: Allocator) void {
        for (self.owned_data) |data| alloc.free(data);
        alloc.free(self.owned_data);
        alloc.free(self.segments);
        self.manifest.deinit(alloc);
        self.* = .{
            .manifest = .{
                .next_segment_id = 0,
                .segments = &.{},
            },
            .owned_data = &.{},
            .segments = &.{},
        };
    }

    pub fn catalog(self: *const OwnedSegmentStore) Catalog {
        return .{ .segments = self.segments };
    }

    pub fn snapshot(self: *const OwnedSegmentStore) Snapshot {
        return .{ .catalog = self.catalog() };
    }
};

pub const Catalog = struct {
    segments: []const SegmentBlob,

    pub fn getBase(self: Catalog, posting_id: PostingId) !?[]const u8 {
        return try self.getLatestExact(posting_id, .base);
    }

    pub fn getCentroidDirectory(self: Catalog, posting_id: PostingId) !?[]const u8 {
        return try self.getLatestExact(posting_id, .centroid_directory);
    }

    pub fn collectDeltas(self: Catalog, alloc: Allocator, posting_id: PostingId) ![]DeltaValue {
        var deltas = std.ArrayListUnmanaged(DeltaValue).empty;
        errdefer deltas.deinit(alloc);
        for (self.segments) |segment| {
            if (!segment.meta.mayContainPosting(posting_id)) continue;
            var reader = try Reader.init(segment.data);
            var iter = reader.deltas(posting_id);
            while (try iter.next()) |delta| {
                try deltas.append(alloc, delta);
            }
        }
        std.mem.sort(DeltaValue, deltas.items, {}, deltaValueLessThan);
        return try deltas.toOwnedSlice(alloc);
    }

    fn getLatestExact(self: Catalog, posting_id: PostingId, kind: EntryKind) !?[]const u8 {
        var best_segment_id: u64 = 0;
        var best: ?[]const u8 = null;
        for (self.segments) |segment| {
            if (!segment.meta.mayContainPosting(posting_id)) continue;
            if (best != null and segment.meta.segment_id <= best_segment_id) continue;
            var reader = try Reader.init(segment.data);
            const value = switch (kind) {
                .base => try reader.getBase(posting_id),
                .centroid_directory => try reader.getCentroidDirectory(posting_id),
                .delta => null,
            };
            if (value) |found| {
                best_segment_id = segment.meta.segment_id;
                best = found;
            }
        }
        return best;
    }
};

pub const Snapshot = struct {
    catalog: Catalog,

    pub fn getBaseBytes(self: Snapshot, posting_id: PostingId) !?[]const u8 {
        return try self.catalog.getBase(posting_id);
    }

    pub fn loadBaseHeader(self: Snapshot, posting_id: PostingId) !?posting.PostingBaseHeader {
        const base_data = (try self.getBaseBytes(posting_id)) orelse return null;
        return try posting.PostingFormat.decodeBaseHeader(base_data);
    }

    pub fn loadBase(self: Snapshot, alloc: Allocator, posting_id: PostingId) !?posting.OwnedPostingBase {
        const base_data = (try self.getBaseBytes(posting_id)) orelse return null;
        return try posting.PostingFormat.decodeBase(alloc, base_data);
    }

    pub fn loadCentroidDirectoryRecord(self: Snapshot, alloc: Allocator, posting_id: PostingId) !?posting.OwnedCentroidDirectoryRecord {
        const centroid_data = (try self.catalog.getCentroidDirectory(posting_id)) orelse return null;
        return try posting.CentroidDirectoryFormat.decode(alloc, centroid_data);
    }

    pub fn loadDeltaTail(self: Snapshot, alloc: Allocator, posting_id: PostingId) ![]posting.PostingDeltaRecord {
        const delta_values = try self.catalog.collectDeltas(alloc, posting_id);
        defer alloc.free(delta_values);

        var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
        errdefer records.deinit(alloc);
        for (delta_values) |delta_value| {
            const decoded = try posting.PostingFormat.decodeDeltaTail(alloc, delta_value.value);
            defer alloc.free(decoded);
            try records.appendSlice(alloc, decoded);
        }
        std.mem.sort(posting.PostingDeltaRecord, records.items, {}, postingDeltaRecordLessThan);
        return try records.toOwnedSlice(alloc);
    }
};

pub fn encodeManifestAlloc(alloc: Allocator, manifest: Manifest) ![]u8 {
    if (manifest.segments.len > std.math.maxInt(u32)) return error.PostingSegmentManifestTooLarge;
    try validateManifest(manifest);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, &manifest_magic);
    try appendU16(alloc, &out, version);
    try appendU32(alloc, &out, @intCast(manifest.segments.len));
    try appendU64(alloc, &out, manifest.next_segment_id);
    for (manifest.segments) |entry| {
        try appendManifestEntry(alloc, &out, entry);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeManifestAlloc(alloc: Allocator, data: []const u8) !OwnedManifest {
    if (data.len < manifest_header_size) return error.CorruptedPostingSegmentManifest;
    if (!std.mem.eql(u8, data[0..4], &manifest_magic)) return error.BadPostingSegmentManifestMagic;
    if (readU16(data[4..6]) != version) return error.UnsupportedPostingSegmentManifestVersion;
    const entry_count_u32 = readU32(data[6..10]);
    const next_segment_id = readU64(data[10..18]);
    const entry_count = std.math.cast(usize, entry_count_u32) orelse return error.CorruptedPostingSegmentManifest;
    var pos: usize = manifest_header_size;
    const entries = try alloc.alloc(OwnedManifestEntry, entry_count);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |entry| alloc.free(entry.path);
        alloc.free(entries);
    }
    while (initialized < entries.len) {
        const entry = try readManifestEntry(alloc, data, &pos);
        if (initialized != 0 and entries[initialized - 1].meta.segment_id >= entry.meta.segment_id) {
            alloc.free(entry.path);
            return error.InvalidPostingSegmentManifest;
        }
        entries[initialized] = entry;
        initialized += 1;
    }
    if (pos != data.len) return error.CorruptedPostingSegmentManifest;
    return .{
        .next_segment_id = next_segment_id,
        .segments = entries,
    };
}

pub fn openStoreAlloc(alloc: Allocator, manifest_data: []const u8, context: anytype, comptime readSegmentAlloc: anytype) !OwnedSegmentStore {
    var manifest = try decodeManifestAlloc(alloc, manifest_data);
    errdefer manifest.deinit(alloc);

    const owned_data = try alloc.alloc([]u8, manifest.segments.len);
    var owned_count: usize = 0;
    errdefer {
        for (owned_data[0..owned_count]) |data| alloc.free(data);
        alloc.free(owned_data);
    }

    const segments = try alloc.alloc(SegmentBlob, manifest.segments.len);
    errdefer alloc.free(segments);

    for (manifest.segments, 0..) |entry, i| {
        const data = try readSegmentAlloc(context, alloc, entry.path, entry.meta.byte_len);
        errdefer alloc.free(data);
        try validateSegmentDataMatchesMeta(data, entry.meta);
        owned_data[i] = data;
        owned_count += 1;
        segments[i] = .{
            .meta = entry.meta,
            .data = data,
        };
    }

    return .{
        .manifest = manifest,
        .owned_data = owned_data,
        .segments = segments,
    };
}

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

    pub fn metadata(self: Reader, segment_id: u64) !SegmentMeta {
        var meta = SegmentMeta{
            .segment_id = segment_id,
            .byte_len = self.data.len,
            .entry_count = self.entry_count,
        };
        if (self.entry_count == 0) return meta;

        var i: usize = 0;
        while (i < self.entry_count) : (i += 1) {
            const entry = try self.indexEntry(i);
            if (i == 0) {
                meta.min_posting_id = entry.posting_id;
                meta.max_posting_id = entry.posting_id;
            } else {
                meta.min_posting_id = @min(meta.min_posting_id, entry.posting_id);
                meta.max_posting_id = @max(meta.max_posting_id, entry.posting_id);
            }
            if (entry.kind == .delta) {
                if (meta.min_delta_sequence == 0 or entry.sequence < meta.min_delta_sequence) {
                    meta.min_delta_sequence = entry.sequence;
                }
                meta.max_delta_sequence = @max(meta.max_delta_sequence, entry.sequence);
            }
        }
        return meta;
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

fn validateManifest(manifest: Manifest) !void {
    var previous_segment_id: ?u64 = null;
    for (manifest.segments) |entry| {
        try validateManifestEntry(entry);
        if (entry.meta.segment_id >= manifest.next_segment_id) return error.InvalidPostingSegmentManifest;
        if (previous_segment_id) |previous| {
            if (previous >= entry.meta.segment_id) return error.InvalidPostingSegmentManifest;
        }
        previous_segment_id = entry.meta.segment_id;
    }
}

fn validateManifestEntry(entry: ManifestEntry) !void {
    if (entry.path.len == 0 or entry.path.len > std.math.maxInt(u32)) return error.InvalidPostingSegmentManifest;
    if (entry.meta.entry_count == 0) return error.InvalidPostingSegmentManifest;
    if (entry.meta.byte_len == 0) return error.InvalidPostingSegmentManifest;
    if (entry.meta.min_posting_id > entry.meta.max_posting_id) return error.InvalidPostingSegmentManifest;
    if (entry.meta.min_delta_sequence != 0 and entry.meta.max_delta_sequence < entry.meta.min_delta_sequence) return error.InvalidPostingSegmentManifest;
}

fn validateSegmentDataMatchesMeta(data: []const u8, expected: SegmentMeta) !void {
    if (data.len != expected.byte_len) return error.InvalidPostingSegment;
    const reader = try Reader.init(data);
    const actual = try reader.metadata(expected.segment_id);
    if (!segmentMetaEql(actual, expected)) return error.InvalidPostingSegment;
}

fn segmentMetaEql(lhs: SegmentMeta, rhs: SegmentMeta) bool {
    return lhs.segment_id == rhs.segment_id and
        lhs.min_posting_id == rhs.min_posting_id and
        lhs.max_posting_id == rhs.max_posting_id and
        lhs.min_delta_sequence == rhs.min_delta_sequence and
        lhs.max_delta_sequence == rhs.max_delta_sequence and
        lhs.byte_len == rhs.byte_len and
        lhs.entry_count == rhs.entry_count;
}

fn appendManifestEntry(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), entry: ManifestEntry) !void {
    try appendU64(alloc, out, entry.meta.segment_id);
    try appendU64(alloc, out, entry.meta.min_posting_id);
    try appendU64(alloc, out, entry.meta.max_posting_id);
    try appendU64(alloc, out, entry.meta.min_delta_sequence);
    try appendU64(alloc, out, entry.meta.max_delta_sequence);
    try appendU64(alloc, out, @intCast(entry.meta.byte_len));
    try appendU64(alloc, out, @intCast(entry.meta.entry_count));
    try appendU32(alloc, out, @intCast(entry.path.len));
    try out.appendSlice(alloc, entry.path);
}

fn readManifestEntry(alloc: Allocator, data: []const u8, pos: *usize) !OwnedManifestEntry {
    const fixed_size = 7 * @sizeOf(u64) + @sizeOf(u32);
    if (pos.* > data.len or data.len - pos.* < fixed_size) return error.CorruptedPostingSegmentManifest;
    const segment_id = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const min_posting_id = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const max_posting_id = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const min_delta_sequence = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const max_delta_sequence = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const byte_len_u64 = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const entry_count_u64 = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const path_len_u32 = readU32(data[pos.*..][0..4]);
    pos.* += 4;
    const path_len = std.math.cast(usize, path_len_u32) orelse return error.CorruptedPostingSegmentManifest;
    if (path_len == 0 or pos.* > data.len or data.len - pos.* < path_len) return error.CorruptedPostingSegmentManifest;
    const path = try alloc.dupe(u8, data[pos.* .. pos.* + path_len]);
    errdefer alloc.free(path);
    pos.* += path_len;
    const meta = SegmentMeta{
        .segment_id = segment_id,
        .min_posting_id = min_posting_id,
        .max_posting_id = max_posting_id,
        .min_delta_sequence = min_delta_sequence,
        .max_delta_sequence = max_delta_sequence,
        .byte_len = std.math.cast(usize, byte_len_u64) orelse return error.CorruptedPostingSegmentManifest,
        .entry_count = std.math.cast(usize, entry_count_u64) orelse return error.CorruptedPostingSegmentManifest,
    };
    try validateManifestEntry(.{ .meta = meta, .path = path });
    return .{
        .meta = meta,
        .path = path,
    };
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

fn deltaValueLessThan(_: void, lhs: DeltaValue, rhs: DeltaValue) bool {
    return lhs.sequence < rhs.sequence;
}

fn postingDeltaRecordLessThan(_: void, lhs: posting.PostingDeltaRecord, rhs: posting.PostingDeltaRecord) bool {
    return lhs.sequence < rhs.sequence;
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

pub fn testStoresBaseCentroidAndOrderedDeltaValues() !void {
    const alloc = std.testing.allocator;
    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base);
    const delta_4 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 4, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta_4);
    const delta_5 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 5, .op = .tombstone, .vector_id = 20 },
    });
    defer alloc.free(delta_5);
    const centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .mutation_version = 5,
        .payload_version = 3,
        .flags = 0,
        .parent = 1,
        .level = 0,
        .member_count = 3,
        .bounds_radius = 1.5,
        .centroid = &.{ 1.0, 2.0 },
    });
    defer alloc.free(centroid);

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

pub fn testCatalogLooksUpNewestPointRecordsAndMergedDeltas() !void {
    const alloc = std.testing.allocator;
    const old_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(old_base);
    const new_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(new_base);
    const delta_10 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 10, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta_10);
    const delta_8 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 8, .op = .tombstone, .vector_id = 20 },
    });
    defer alloc.free(delta_8);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, old_base);
    try writer_1.appendDelta(7, 10, delta_10);
    const segment_1 = try writer_1.build();
    defer alloc.free(segment_1);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendBase(7, new_base);
    try writer_2.appendDelta(7, 8, delta_8);
    const segment_2 = try writer_2.build();
    defer alloc.free(segment_2);

    const reader_1 = try Reader.init(segment_1);
    const reader_2 = try Reader.init(segment_2);
    const meta_1 = try reader_1.metadata(1);
    const meta_2 = try reader_2.metadata(2);
    try std.testing.expect(meta_1.mayContainPosting(7));
    try std.testing.expectEqual(@as(usize, 2), meta_1.entry_count);
    try std.testing.expectEqual(@as(u64, 10), meta_1.min_delta_sequence);
    try std.testing.expectEqual(@as(u64, 10), meta_1.max_delta_sequence);
    try std.testing.expectEqual(segment_1.len, meta_1.byte_len);

    const blobs = [_]SegmentBlob{
        .{ .meta = meta_1, .data = segment_1 },
        .{ .meta = meta_2, .data = segment_2 },
    };
    const catalog = Catalog{ .segments = blobs[0..] };
    try std.testing.expectEqualSlices(u8, new_base, (try catalog.getBase(7)).?);
    try std.testing.expect(try catalog.getBase(8) == null);

    const deltas = try catalog.collectDeltas(alloc, 7);
    defer alloc.free(deltas);
    try std.testing.expectEqual(@as(usize, 2), deltas.len);
    try std.testing.expectEqual(@as(u64, 8), deltas[0].sequence);
    try std.testing.expectEqualSlices(u8, delta_8, deltas[0].value);
    try std.testing.expectEqual(@as(u64, 10), deltas[1].sequence);
    try std.testing.expectEqualSlices(u8, delta_10, deltas[1].value);
}

pub fn testSnapshotLoadsTypedPostingValues() !void {
    const alloc = std.testing.allocator;
    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base);
    const centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .mutation_version = 11,
        .payload_version = 9,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 3,
        .bounds_radius = 2.5,
        .centroid = &.{ 1.0, 2.0, 3.0 },
    });
    defer alloc.free(centroid);
    const delta_20 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 20, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta_20);
    const delta_10 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 10, .op = .tombstone, .vector_id = 20 },
        .{ .sequence = 12, .op = .replace, .vector_id = 30 },
    });
    defer alloc.free(delta_10);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, base);
    try writer_1.appendCentroidDirectory(7, centroid);
    try writer_1.appendDelta(7, 20, delta_20);
    const segment_1 = try writer_1.build();
    defer alloc.free(segment_1);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendDelta(7, 10, delta_10);
    const segment_2 = try writer_2.build();
    defer alloc.free(segment_2);

    const reader_1 = try Reader.init(segment_1);
    const reader_2 = try Reader.init(segment_2);
    const meta_1 = try reader_1.metadata(1);
    const meta_2 = try reader_2.metadata(2);
    const blobs = [_]SegmentBlob{
        .{ .meta = meta_1, .data = segment_1 },
        .{ .meta = meta_2, .data = segment_2 },
    };
    const snapshot = Snapshot{ .catalog = .{ .segments = blobs[0..] } };

    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(PostingId, 7), header.posting_id);
    try std.testing.expectEqual(@as(u64, 3), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);
    try std.testing.expect(try snapshot.loadBaseHeader(8) == null);

    var loaded_base = (try snapshot.loadBase(alloc, 7)).?;
    defer loaded_base.deinit(alloc);
    try std.testing.expectEqual(@as(PostingId, 7), loaded_base.posting_id);
    try std.testing.expectEqual(@as(u64, 3), loaded_base.generation);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, loaded_base.members);
    try std.testing.expect(try snapshot.loadBase(alloc, 8) == null);

    var loaded_centroid = (try snapshot.loadCentroidDirectoryRecord(alloc, 7)).?;
    defer loaded_centroid.deinit(alloc);
    try std.testing.expectEqual(@as(PostingId, 7), loaded_centroid.posting_id);
    try std.testing.expectEqual(@as(u64, 3), loaded_centroid.generation);
    try std.testing.expectEqual(@as(u64, 11), loaded_centroid.mutation_version);
    try std.testing.expectEqual(@as(u64, 9), loaded_centroid.payload_version);
    try std.testing.expectEqual(posting.CentroidDirectoryFormat.dirty_flag, loaded_centroid.flags);
    try std.testing.expectEqual(@as(PostingId, 1), loaded_centroid.parent);
    try std.testing.expectEqual(@as(u16, 0), loaded_centroid.level);
    try std.testing.expectEqual(@as(u64, 3), loaded_centroid.member_count);
    try std.testing.expectEqual(@as(f32, 2.5), loaded_centroid.bounds_radius);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, loaded_centroid.centroid);
    try std.testing.expect(try snapshot.loadCentroidDirectoryRecord(alloc, 8) == null);

    const records = try snapshot.loadDeltaTail(alloc, 7);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 3), records.len);
    try std.testing.expectEqual(@as(u64, 10), records[0].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.tombstone, records[0].op);
    try std.testing.expectEqual(@as(posting.VectorId, 20), records[0].vector_id);
    try std.testing.expectEqual(@as(u64, 12), records[1].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.replace, records[1].op);
    try std.testing.expectEqual(@as(posting.VectorId, 30), records[1].vector_id);
    try std.testing.expectEqual(@as(u64, 20), records[2].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.insert, records[2].op);
    try std.testing.expectEqual(@as(posting.VectorId, 40), records[2].vector_id);

    const missing_records = try snapshot.loadDeltaTail(alloc, 8);
    defer alloc.free(missing_records);
    try std.testing.expectEqual(@as(usize, 0), missing_records.len);
}

pub fn testManifestCodecRoundTripsSegmentMetadata() !void {
    const alloc = std.testing.allocator;
    const entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 7,
                .max_posting_id = 9,
                .min_delta_sequence = 10,
                .max_delta_sequence = 20,
                .byte_len = 4096,
                .entry_count = 12,
            },
            .path = "postings/000001.afps",
        },
        .{
            .meta = .{
                .segment_id = 2,
                .min_posting_id = 10,
                .max_posting_id = 12,
                .byte_len = 2048,
                .entry_count = 3,
            },
            .path = "postings/000002.afps",
        },
    };
    const encoded = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 3,
        .segments = entries[0..],
    });
    defer alloc.free(encoded);

    var decoded = try decodeManifestAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), decoded.next_segment_id);
    try std.testing.expectEqual(@as(usize, 2), decoded.segments.len);
    for (entries, decoded.segments) |expected, actual| {
        try std.testing.expectEqual(expected.meta.segment_id, actual.meta.segment_id);
        try std.testing.expectEqual(expected.meta.min_posting_id, actual.meta.min_posting_id);
        try std.testing.expectEqual(expected.meta.max_posting_id, actual.meta.max_posting_id);
        try std.testing.expectEqual(expected.meta.min_delta_sequence, actual.meta.min_delta_sequence);
        try std.testing.expectEqual(expected.meta.max_delta_sequence, actual.meta.max_delta_sequence);
        try std.testing.expectEqual(expected.meta.byte_len, actual.meta.byte_len);
        try std.testing.expectEqual(expected.meta.entry_count, actual.meta.entry_count);
        try std.testing.expectEqualStrings(expected.path, actual.path);
    }
}

pub fn testManifestCodecRejectsInvalidData() !void {
    const alloc = std.testing.allocator;
    const entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 2,
                .min_posting_id = 7,
                .max_posting_id = 7,
                .byte_len = 128,
                .entry_count = 1,
            },
            .path = "postings/000002.afps",
        },
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 8,
                .max_posting_id = 8,
                .byte_len = 128,
                .entry_count = 1,
            },
            .path = "postings/000001.afps",
        },
    };
    try std.testing.expectError(error.InvalidPostingSegmentManifest, encodeManifestAlloc(alloc, .{
        .next_segment_id = 3,
        .segments = entries[0..],
    }));

    const valid_entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 7,
                .max_posting_id = 7,
                .byte_len = 128,
                .entry_count = 1,
            },
            .path = "postings/000001.afps",
        },
    };
    const encoded = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 2,
        .segments = valid_entries[0..],
    });
    defer alloc.free(encoded);

    var bad_magic = try alloc.dupe(u8, encoded);
    defer alloc.free(bad_magic);
    bad_magic[0] = 'x';
    try std.testing.expectError(error.BadPostingSegmentManifestMagic, decodeManifestAlloc(alloc, bad_magic));

    var bad_version = try alloc.dupe(u8, encoded);
    defer alloc.free(bad_version);
    bad_version[5] = 2;
    try std.testing.expectError(error.UnsupportedPostingSegmentManifestVersion, decodeManifestAlloc(alloc, bad_version));

    try std.testing.expectError(error.CorruptedPostingSegmentManifest, decodeManifestAlloc(alloc, encoded[0 .. encoded.len - 1]));
}

const TestSegmentFile = struct {
    path: []const u8,
    data: []const u8,
};

const TestSegmentLoader = struct {
    files: []const TestSegmentFile,

    fn read(self: *const TestSegmentLoader, alloc: Allocator, path: []const u8, byte_limit: usize) ![]u8 {
        for (self.files) |file| {
            if (!std.mem.eql(u8, file.path, path)) continue;
            if (file.data.len > byte_limit) return error.PostingSegmentTooLarge;
            return try alloc.dupe(u8, file.data);
        }
        return error.FileNotFound;
    }
};

pub fn testOpenStoreValidatesManifestBackedSegments() !void {
    const alloc = std.testing.allocator;
    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base);
    const delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 20, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta);

    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(7, base);
    try writer.appendDelta(7, 20, delta);
    const segment = try writer.build();
    defer alloc.free(segment);

    const reader = try Reader.init(segment);
    const meta = try reader.metadata(1);
    const entries = [_]ManifestEntry{.{
        .meta = meta,
        .path = "postings/000001.afps",
    }};
    const manifest_data = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 2,
        .segments = entries[0..],
    });
    defer alloc.free(manifest_data);

    const files = [_]TestSegmentFile{.{
        .path = "postings/000001.afps",
        .data = segment,
    }};
    const loader = TestSegmentLoader{ .files = files[0..] };
    var store = try openStoreAlloc(alloc, manifest_data, &loader, TestSegmentLoader.read);
    defer store.deinit(alloc);

    try std.testing.expectEqual(@as(u64, 2), store.manifest.next_segment_id);
    try std.testing.expectEqual(@as(usize, 1), store.segments.len);
    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(PostingId, 7), header.posting_id);
    try std.testing.expectEqual(@as(u64, 3), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);

    const records = try snapshot.loadDeltaTail(alloc, 7);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 20), records[0].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.insert, records[0].op);
    try std.testing.expectEqual(@as(posting.VectorId, 40), records[0].vector_id);

    var stale_entries = [_]ManifestEntry{.{
        .meta = meta,
        .path = "postings/000001.afps",
    }};
    stale_entries[0].meta.max_posting_id = 8;
    const stale_manifest_data = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 2,
        .segments = stale_entries[0..],
    });
    defer alloc.free(stale_manifest_data);
    try std.testing.expectError(error.InvalidPostingSegment, openStoreAlloc(alloc, stale_manifest_data, &loader, TestSegmentLoader.read));
}

test "posting segment stores base centroid and ordered delta values" {
    try testStoresBaseCentroidAndOrderedDeltaValues();
}

test "posting segment rejects duplicate logical entries" {
    try testRejectsDuplicateLogicalEntries();
}

test "posting segment validates footer and version" {
    try testValidatesFooterAndVersion();
}

test "posting segment catalog looks up newest point records and merged deltas" {
    try testCatalogLooksUpNewestPointRecordsAndMergedDeltas();
}

test "posting segment snapshot loads typed posting values" {
    try testSnapshotLoadsTypedPostingValues();
}

test "posting segment manifest codec round trips segment metadata" {
    try testManifestCodecRoundTripsSegmentMetadata();
}

test "posting segment manifest codec rejects invalid data" {
    try testManifestCodecRejectsInvalidData();
}

test "posting segment store validates manifest backed segments" {
    try testOpenStoreValidatesManifestBackedSegments();
}
