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
const version: u16 = 5;
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
    base_tombstone = 7,
    quantized_checkpoint_tombstone = 8,
    posting_state_tombstone = 9,
    /// Advances source coverage when a journal batch has no posting changes.
    coverage = 10,
    node_range = 11,
    vector_leaf = 12,
    vector_metadata = 13,
    index_metadata = 14,
    node_range_tombstone = 15,
    vector_leaf_tombstone = 16,
    vector_metadata_tombstone = 17,
    index_metadata_tombstone = 18,
    commit = 255,
};

const replacement_patch_magic: [4]u8 = "AFPD".*;
const replacement_patch_version: u8 = 2;
const replacement_patch_header_len: usize = 32;
const replacement_patch_anchor_len: usize = 8;
const replacement_patch_anchor_stride: usize = 16;
const replacement_patch_min_copy_len: usize = 16;
const replacement_patch_copy_op: u8 = 0;
const replacement_patch_literal_op: u8 = 1;

/// Encodes a posting-local replacement as copy and literal operations. Copy
/// operations may reference any base offset, so an insertion or deletion in a
/// packed id/code array does not turn the shifted suffix into a large literal.
/// The base and result checksums make a patch fail closed if a WAL generation
/// is paired with the wrong segment or an earlier delta is missing.
pub fn encodeReplacementPatchAlloc(
    alloc: Allocator,
    target: RecordKind,
    base: []const u8,
    replacement: []const u8,
) ![]u8 {
    if (!isLookupValueKind(target)) return error.InvalidPostingPatchTarget;
    if (base.len > std.math.maxInt(u32) or replacement.len > std.math.maxInt(u32)) {
        return error.PostingWalRecordTooLarge;
    }

    // A point insertion or deletion in a packed posting preserves one long
    // prefix and suffix. This is the overwhelmingly common mutation shape,
    // and encoding it directly avoids allocating and populating an anchor
    // hash table for every changed node. More fragmented replacements fall
    // through to the general shifted-run matcher below.
    if (try encodePrefixSuffixReplacementPatchAlloc(alloc, target, base, replacement)) |patch| {
        return patch;
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.resize(alloc, replacement_patch_header_len);

    // Index sparse base anchors. Replacement scanning remains byte-aligned, so
    // shifted runs still find the next aligned anchor while the per-mutation
    // hash table is four times smaller than the original four-byte sampling.
    // Runs too short to contain an aligned anchor are cheaper as literals.
    var anchors = std.AutoHashMapUnmanaged(u64, u32).empty;
    defer anchors.deinit(alloc);
    if (base.len >= replacement_patch_anchor_len) {
        const anchor_count = (base.len - replacement_patch_anchor_len) / replacement_patch_anchor_stride + 1;
        try anchors.ensureTotalCapacity(alloc, @intCast(anchor_count));
        var base_offset: usize = 0;
        while (base_offset + replacement_patch_anchor_len <= base.len) : (base_offset += replacement_patch_anchor_stride) {
            const anchor = std.mem.readInt(u64, base[base_offset..][0..replacement_patch_anchor_len], .little);
            const gop = anchors.getOrPutAssumeCapacity(anchor);
            if (!gop.found_existing) gop.value_ptr.* = @intCast(base_offset);
        }
    }

    var op_count: u32 = 0;
    var replacement_offset: usize = 0;
    var literal_start: usize = 0;
    while (replacement_offset < replacement.len) {
        var copy_offset: usize = 0;
        var copy_len: usize = 0;
        if (replacement_offset + replacement_patch_anchor_len <= replacement.len) {
            const anchor = std.mem.readInt(u64, replacement[replacement_offset..][0..replacement_patch_anchor_len], .little);
            if (anchors.get(anchor)) |candidate_u32| {
                const candidate: usize = @intCast(candidate_u32);
                var matched = replacement_patch_anchor_len;
                while (candidate + matched < base.len and replacement_offset + matched < replacement.len and
                    base[candidate + matched] == replacement[replacement_offset + matched]) : (matched += 1)
                {}
                if (matched >= replacement_patch_min_copy_len) {
                    copy_offset = candidate;
                    copy_len = matched;
                }
            }
        }
        if (copy_len == 0) {
            replacement_offset += 1;
            continue;
        }

        if (literal_start < replacement_offset) {
            try appendReplacementPatchLiteral(alloc, &out, replacement[literal_start..replacement_offset]);
            op_count += 1;
        }
        try appendReplacementPatchCopy(alloc, &out, copy_offset, copy_len);
        op_count += 1;
        replacement_offset += copy_len;
        literal_start = replacement_offset;
    }
    if (literal_start < replacement.len) {
        try appendReplacementPatchLiteral(alloc, &out, replacement[literal_start..]);
        op_count += 1;
    }

    finishReplacementPatchHeader(out.items[0..replacement_patch_header_len], target, base, replacement, op_count);
    return try out.toOwnedSlice(alloc);
}

fn encodePrefixSuffixReplacementPatchAlloc(
    alloc: Allocator,
    target: RecordKind,
    base: []const u8,
    replacement: []const u8,
) !?[]u8 {
    const common_len = @min(base.len, replacement.len);
    const prefix_len = std.mem.indexOfDiff(u8, base[0..common_len], replacement[0..common_len]) orelse common_len;

    // Do not let the suffix overlap the already-accounted prefix. Comparing
    // backwards is cheap for point edits and stops immediately for unrelated
    // replacements, which are delegated to the general encoder.
    var suffix_len: usize = 0;
    const max_suffix_len = common_len - prefix_len;
    while (suffix_len < max_suffix_len and
        base[base.len - suffix_len - 1] == replacement[replacement.len - suffix_len - 1])
    {
        suffix_len += 1;
    }

    const prefix_copy_len = if (prefix_len >= replacement_patch_min_copy_len) prefix_len else 0;
    const suffix_copy_len = if (suffix_len >= replacement_patch_min_copy_len) suffix_len else 0;
    const literal_len = replacement.len - prefix_copy_len - suffix_copy_len;
    const copy_count: usize = @as(usize, @intFromBool(prefix_copy_len != 0)) +
        @as(usize, @intFromBool(suffix_copy_len != 0));
    const literal_count: usize = @intFromBool(literal_len != 0);
    const encoded_len = replacement_patch_header_len + copy_count * 9 + literal_count * 5 + literal_len;
    // Keep the general matcher for fragmented changes where it can recover
    // several shifted runs. The direct path is selected only when the one-edit
    // representation is already decisively compact.
    if (encoded_len >= replacement.len / 8) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.ensureTotalCapacity(alloc, encoded_len);
    try out.resize(alloc, replacement_patch_header_len);
    var op_count: u32 = 0;
    if (prefix_copy_len != 0) {
        try appendReplacementPatchCopy(alloc, &out, 0, prefix_copy_len);
        op_count += 1;
    }
    if (literal_len != 0) {
        try appendReplacementPatchLiteral(
            alloc,
            &out,
            replacement[prefix_copy_len .. replacement.len - suffix_copy_len],
        );
        op_count += 1;
    }
    if (suffix_copy_len != 0) {
        try appendReplacementPatchCopy(alloc, &out, base.len - suffix_copy_len, suffix_copy_len);
        op_count += 1;
    }
    std.debug.assert(out.items.len == encoded_len);
    finishReplacementPatchHeader(out.items[0..replacement_patch_header_len], target, base, replacement, op_count);
    return try out.toOwnedSlice(alloc);
}

fn appendReplacementPatchCopy(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    copy_offset: usize,
    copy_len: usize,
) !void {
    if (copy_offset > std.math.maxInt(u32) or copy_len > std.math.maxInt(u32)) {
        return error.PostingWalRecordTooLarge;
    }
    try out.append(alloc, replacement_patch_copy_op);
    var copy_header: [8]u8 = undefined;
    std.mem.writeInt(u32, copy_header[0..4], @intCast(copy_offset), .big);
    std.mem.writeInt(u32, copy_header[4..8], @intCast(copy_len), .big);
    try out.appendSlice(alloc, &copy_header);
}

fn finishReplacementPatchHeader(
    header: []u8,
    target: RecordKind,
    base: []const u8,
    replacement: []const u8,
    op_count: u32,
) void {
    std.debug.assert(header.len == replacement_patch_header_len);
    @memcpy(header[0..4], &replacement_patch_magic);
    header[4] = replacement_patch_version;
    header[5] = @intFromEnum(target);
    @memset(header[6..8], 0);
    std.mem.writeInt(u32, header[8..12], @intCast(base.len), .big);
    std.mem.writeInt(u32, header[12..16], @intCast(replacement.len), .big);
    std.mem.writeInt(u32, header[16..20], op_count, .big);
    @memset(header[20..24], 0);
    std.mem.writeInt(u32, header[24..28], std.hash.Crc32.hash(base), .big);
    std.mem.writeInt(u32, header[28..32], std.hash.Crc32.hash(replacement), .big);
}

fn appendReplacementPatchLiteral(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    if (bytes.len > std.math.maxInt(u32)) return error.PostingWalRecordTooLarge;
    try out.append(alloc, replacement_patch_literal_op);
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, @intCast(bytes.len), .big);
    try out.appendSlice(alloc, &len_buf);
    try out.appendSlice(alloc, bytes);
}

pub const DecodedReplacementPatch = struct {
    target: RecordKind,
    replacement: []u8,
};

pub fn applyReplacementPatchAlloc(
    alloc: Allocator,
    payload: []const u8,
    base: []const u8,
) !DecodedReplacementPatch {
    if (payload.len < replacement_patch_header_len) return error.CorruptedPostingPatch;
    if (!std.mem.eql(u8, payload[0..4], &replacement_patch_magic)) return error.BadPostingPatchMagic;
    if (payload[4] != replacement_patch_version) return error.UnsupportedPostingPatchVersion;
    if (payload[6] != 0 or payload[7] != 0) return error.UnsupportedPostingPatchFlags;
    const target: RecordKind = switch (payload[5]) {
        @intFromEnum(RecordKind.base) => .base,
        @intFromEnum(RecordKind.quantized_checkpoint) => .quantized_checkpoint,
        @intFromEnum(RecordKind.posting_state) => .posting_state,
        @intFromEnum(RecordKind.node_range) => .node_range,
        @intFromEnum(RecordKind.vector_leaf) => .vector_leaf,
        @intFromEnum(RecordKind.vector_metadata) => .vector_metadata,
        @intFromEnum(RecordKind.index_metadata) => .index_metadata,
        else => return error.InvalidPostingPatchTarget,
    };
    const base_len: usize = @intCast(std.mem.readInt(u32, payload[8..12], .big));
    const replacement_len: usize = @intCast(std.mem.readInt(u32, payload[12..16], .big));
    const op_count: usize = @intCast(std.mem.readInt(u32, payload[16..20], .big));
    if (payload[20] != 0 or payload[21] != 0 or payload[22] != 0 or payload[23] != 0) return error.UnsupportedPostingPatchFlags;
    if (base.len != base_len or std.hash.Crc32.hash(base) != std.mem.readInt(u32, payload[24..28], .big)) {
        return error.PostingPatchBaseMismatch;
    }
    const replacement = try alloc.alloc(u8, replacement_len);
    errdefer alloc.free(replacement);
    var payload_offset: usize = replacement_patch_header_len;
    var replacement_offset: usize = 0;
    for (0..op_count) |_| {
        if (payload_offset >= payload.len) return error.CorruptedPostingPatch;
        const op = payload[payload_offset];
        payload_offset += 1;
        switch (op) {
            replacement_patch_copy_op => {
                if (payload.len - payload_offset < 8) return error.CorruptedPostingPatch;
                const copy_offset: usize = @intCast(std.mem.readInt(u32, payload[payload_offset..][0..4], .big));
                const copy_len: usize = @intCast(std.mem.readInt(u32, payload[payload_offset + 4 ..][0..4], .big));
                payload_offset += 8;
                if (copy_offset > base.len or copy_len > base.len - copy_offset or
                    replacement_offset > replacement.len or copy_len > replacement.len - replacement_offset)
                {
                    return error.CorruptedPostingPatch;
                }
                @memcpy(replacement[replacement_offset..][0..copy_len], base[copy_offset..][0..copy_len]);
                replacement_offset += copy_len;
            },
            replacement_patch_literal_op => {
                if (payload.len - payload_offset < 4) return error.CorruptedPostingPatch;
                const literal_len: usize = @intCast(std.mem.readInt(u32, payload[payload_offset..][0..4], .big));
                payload_offset += 4;
                if (literal_len > payload.len - payload_offset or replacement_offset > replacement.len or
                    literal_len > replacement.len - replacement_offset)
                {
                    return error.CorruptedPostingPatch;
                }
                @memcpy(replacement[replacement_offset..][0..literal_len], payload[payload_offset..][0..literal_len]);
                payload_offset += literal_len;
                replacement_offset += literal_len;
            },
            else => return error.CorruptedPostingPatch,
        }
    }
    if (payload_offset != payload.len or replacement_offset != replacement.len) return error.CorruptedPostingPatch;
    if (std.hash.Crc32.hash(replacement) != std.mem.readInt(u32, payload[28..32], .big)) {
        return error.PostingPatchResultMismatch;
    }
    return .{ .target = target, .replacement = replacement };
}

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
        if ((isTombstone(kind) or kind == .coverage) and payload.len != 0) return error.InvalidPostingWalRecord;
        if (kind == .coverage and posting_id != 0) return error.InvalidPostingWalRecord;
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
                if ((isTombstone(record.kind) or record.kind == .coverage) and record.payload.len != 0) return error.InvalidPostingWalRecord;
                if (record.kind == .coverage and record.posting_id != 0) return error.InvalidPostingWalRecord;
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
            if (record.kind != .coverage) self.posting_order.appendAssumeCapacity(index);
            if (record.kind == .tombstone) {
                inline for (lookup_record_kinds) |kind| {
                    try self.latest_by_kind.put(self.alloc, postingKindKey(record.posting_id, kind), .tombstone);
                }
            } else if (tombstoneTarget(record.kind)) |target| {
                try self.latest_by_kind.put(self.alloc, postingKindKey(record.posting_id, target), .tombstone);
            } else if (record.kind != .coverage) {
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
    .node_range,
    .vector_leaf,
    .vector_metadata,
    .index_metadata,
};

fn isLookupValueKind(kind: RecordKind) bool {
    return switch (kind) {
        .base, .quantized_checkpoint, .posting_state, .node_range, .vector_leaf, .vector_metadata, .index_metadata => true,
        else => false,
    };
}

fn postingKindKey(posting_id: PostingId, kind: RecordKind) u128 {
    return (@as(u128, posting_id) << 8) | @intFromEnum(kind);
}

fn tombstoneTarget(kind: RecordKind) ?RecordKind {
    return switch (kind) {
        .base_tombstone => .base,
        .quantized_checkpoint_tombstone => .quantized_checkpoint,
        .posting_state_tombstone => .posting_state,
        .node_range_tombstone => .node_range,
        .vector_leaf_tombstone => .vector_leaf,
        .vector_metadata_tombstone => .vector_metadata,
        .index_metadata_tombstone => .index_metadata,
        else => null,
    };
}

fn isTombstone(kind: RecordKind) bool {
    return kind == .tombstone or tombstoneTarget(kind) != null;
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
        @intFromEnum(RecordKind.base_tombstone) => .base_tombstone,
        @intFromEnum(RecordKind.quantized_checkpoint_tombstone) => .quantized_checkpoint_tombstone,
        @intFromEnum(RecordKind.posting_state_tombstone) => .posting_state_tombstone,
        @intFromEnum(RecordKind.coverage) => .coverage,
        @intFromEnum(RecordKind.node_range) => .node_range,
        @intFromEnum(RecordKind.vector_leaf) => .vector_leaf,
        @intFromEnum(RecordKind.vector_metadata) => .vector_metadata,
        @intFromEnum(RecordKind.index_metadata) => .index_metadata,
        @intFromEnum(RecordKind.node_range_tombstone) => .node_range_tombstone,
        @intFromEnum(RecordKind.vector_leaf_tombstone) => .vector_leaf_tombstone,
        @intFromEnum(RecordKind.vector_metadata_tombstone) => .vector_metadata_tombstone,
        @intFromEnum(RecordKind.index_metadata_tombstone) => .index_metadata_tombstone,
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
    pub const max_delta_segments: usize = 8;
    const legacy_encoded_len: usize = 52;
    pub const encoded_len: usize = 48 + 4 + max_delta_segments * 16 + 4;
    const checkpoint_magic: [4]u8 = "AFPC".*;
    const checkpoint_version: u16 = 3;
    const min_checkpoint_version: u16 = 1;

    pub const Segment = struct {
        generation: u64 = 0,
        checksum: u32 = 0,
        admission_checksum: u32 = 0,
    };

    segment_generation: u64,
    segment_checksum: u32,
    segment_admission_checksum: u32 = 0,
    wal_generation: u64,
    wal_committed_bytes: u64,
    covered_source_sequence: u64,
    delta_segment_count: u8 = 0,
    delta_segments: [max_delta_segments]Segment = [_]Segment{.{}} ** max_delta_segments,

    pub fn latestSegmentGeneration(self: Checkpoint) u64 {
        if (self.delta_segment_count == 0) return self.segment_generation;
        return self.delta_segments[self.delta_segment_count - 1].generation;
    }

    pub fn segmentCount(self: Checkpoint) usize {
        return 1 + @as(usize, self.delta_segment_count);
    }

    pub fn segment(self: Checkpoint, index: usize) Segment {
        std.debug.assert(index < self.segmentCount());
        if (index == 0) return .{
            .generation = self.segment_generation,
            .checksum = self.segment_checksum,
            .admission_checksum = self.segment_admission_checksum,
        };
        return self.delta_segments[index - 1];
    }

    pub fn encode(self: Checkpoint) [encoded_len]u8 {
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..4], &checkpoint_magic);
        std.mem.writeInt(u16, out[4..6], checkpoint_version, .big);
        std.mem.writeInt(u16, out[6..8], 0, .big);
        std.mem.writeInt(u64, out[8..16], self.segment_generation, .big);
        std.mem.writeInt(u32, out[16..20], self.segment_checksum, .big);
        std.mem.writeInt(u32, out[20..24], self.segment_admission_checksum, .big);
        std.mem.writeInt(u64, out[24..32], self.wal_generation, .big);
        std.mem.writeInt(u64, out[32..40], self.wal_committed_bytes, .big);
        std.mem.writeInt(u64, out[40..48], self.covered_source_sequence, .big);
        out[48] = self.delta_segment_count;
        @memset(out[49..52], 0);
        var offset: usize = 52;
        for (self.delta_segments) |segment_descriptor| {
            std.mem.writeInt(u64, out[offset..][0..8], segment_descriptor.generation, .big);
            std.mem.writeInt(u32, out[offset + 8 ..][0..4], segment_descriptor.checksum, .big);
            std.mem.writeInt(u32, out[offset + 12 ..][0..4], segment_descriptor.admission_checksum, .big);
            offset += 16;
        }
        std.mem.writeInt(u32, out[offset..][0..4], std.hash.Crc32.hash(out[0..offset]), .big);
        return out;
    }

    pub fn decode(bytes: []const u8) !Checkpoint {
        if (bytes.len != legacy_encoded_len and bytes.len != encoded_len) return error.InvalidPostingCheckpoint;
        if (!std.mem.eql(u8, bytes[0..4], &checkpoint_magic)) return error.BadPostingCheckpointMagic;
        const encoded_version = std.mem.readInt(u16, bytes[4..6], .big);
        if (encoded_version < min_checkpoint_version or encoded_version > checkpoint_version) return error.UnsupportedPostingCheckpointVersion;
        if ((encoded_version < 3 and bytes.len != legacy_encoded_len) or
            (encoded_version >= 3 and bytes.len != encoded_len)) return error.InvalidPostingCheckpoint;
        if (std.mem.readInt(u16, bytes[6..8], .big) != 0 or
            (encoded_version == 1 and std.mem.readInt(u32, bytes[20..24], .big) != 0))
        {
            return error.UnsupportedPostingCheckpointFlags;
        }
        const checkpoint_checksum_offset: usize = if (encoded_version >= 3) encoded_len - 4 else 48;
        if (std.mem.readInt(u32, bytes[checkpoint_checksum_offset..][0..4], .big) != std.hash.Crc32.hash(bytes[0..checkpoint_checksum_offset])) {
            return error.PostingCheckpointChecksumMismatch;
        }
        var checkpoint: Checkpoint = .{
            .segment_generation = std.mem.readInt(u64, bytes[8..16], .big),
            .segment_checksum = std.mem.readInt(u32, bytes[16..20], .big),
            .segment_admission_checksum = if (encoded_version >= 2) std.mem.readInt(u32, bytes[20..24], .big) else 0,
            .wal_generation = std.mem.readInt(u64, bytes[24..32], .big),
            .wal_committed_bytes = std.mem.readInt(u64, bytes[32..40], .big),
            .covered_source_sequence = std.mem.readInt(u64, bytes[40..48], .big),
        };
        if (encoded_version >= 3) {
            checkpoint.delta_segment_count = bytes[48];
            if (checkpoint.delta_segment_count > max_delta_segments or
                !std.mem.allEqual(u8, bytes[49..52], 0)) return error.UnsupportedPostingCheckpointFlags;
            var offset: usize = 52;
            for (&checkpoint.delta_segments) |*segment_descriptor| {
                segment_descriptor.* = .{
                    .generation = std.mem.readInt(u64, bytes[offset..][0..8], .big),
                    .checksum = std.mem.readInt(u32, bytes[offset + 8 ..][0..4], .big),
                    .admission_checksum = std.mem.readInt(u32, bytes[offset + 12 ..][0..4], .big),
                };
                offset += 16;
            }
            var previous_generation = checkpoint.segment_generation;
            for (checkpoint.delta_segments[0..checkpoint.delta_segment_count]) |segment_descriptor| {
                if (segment_descriptor.generation <= previous_generation) return error.InvalidPostingCheckpoint;
                previous_generation = segment_descriptor.generation;
            }
            for (checkpoint.delta_segments[checkpoint.delta_segment_count..]) |segment_descriptor| {
                if (segment_descriptor.generation != 0 or segment_descriptor.checksum != 0 or
                    segment_descriptor.admission_checksum != 0) return error.InvalidPostingCheckpoint;
            }
        }
        return checkpoint;
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
        .segment_admission_checksum = 0x87654321,
        .wal_generation = 9,
        .wal_committed_bytes = 4096,
        .covered_source_sequence = 1234,
        .delta_segment_count = 2,
        .delta_segments = .{
            .{ .generation = 9, .checksum = 1, .admission_checksum = 2 },
            .{ .generation = 11, .checksum = 3, .admission_checksum = 4 },
            .{},
            .{},
            .{},
            .{},
            .{},
            .{},
        },
    };
    var encoded = expected.encode();
    const decoded = try Checkpoint.decode(&encoded);
    try std.testing.expectEqualDeep(expected, decoded);

    // V1 used the same fixed layout but reserved bytes 20..24. It remains
    // readable and deliberately requests the legacy whole-file checksum.
    var legacy: [Checkpoint.legacy_encoded_len]u8 = undefined;
    @memcpy(legacy[0..48], encoded[0..48]);
    std.mem.writeInt(u16, legacy[4..6], 1, .big);
    @memset(legacy[20..24], 0);
    std.mem.writeInt(u32, legacy[48..52], std.hash.Crc32.hash(legacy[0..48]), .big);
    try std.testing.expectEqual(@as(u32, 0), (try Checkpoint.decode(&legacy)).segment_admission_checksum);

    var unordered_delta = expected.encode();
    std.mem.writeInt(u64, unordered_delta[68..76], 8, .big);
    std.mem.writeInt(u32, unordered_delta[Checkpoint.encoded_len - 4 ..][0..4], std.hash.Crc32.hash(unordered_delta[0 .. Checkpoint.encoded_len - 4]), .big);
    try std.testing.expectError(error.InvalidPostingCheckpoint, Checkpoint.decode(&unordered_delta));

    var dirty_unused_delta = expected.encode();
    std.mem.writeInt(u64, dirty_unused_delta[84..92], 12, .big);
    std.mem.writeInt(u32, dirty_unused_delta[Checkpoint.encoded_len - 4 ..][0..4], std.hash.Crc32.hash(dirty_unused_delta[0 .. Checkpoint.encoded_len - 4]), .big);
    try std.testing.expectError(error.InvalidPostingCheckpoint, Checkpoint.decode(&dirty_unused_delta));

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

test "posting WAL kind tombstones mask only their target" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.base, 1, 7, 100, "base-v2");
    try writer.append(.quantized_checkpoint_tombstone, 1, 7, 100, &.{});
    try writer.commit(1, 100);

    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqualStrings("base-v2", replay.resolve(7, .base).value.payload);
    try std.testing.expect(replay.resolve(7, .quantized_checkpoint) == .tombstone);
    try std.testing.expect(replay.resolve(7, .posting_state) == .missing);
}

test "posting WAL resolves compact HBC derived values and tombstones" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.vector_metadata, 1, 42, 100, "doc:42");
    try writer.append(.vector_leaf, 1, 42, 100, "leaf:7");
    try writer.commit(1, 100);
    try writer.append(.vector_metadata_tombstone, 2, 42, 101, &.{});
    try writer.commit(2, 101);

    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expect(replay.resolve(42, .vector_metadata) == .tombstone);
    try std.testing.expectEqualStrings("leaf:7", replay.resolve(42, .vector_leaf).value.payload);
}

test "posting WAL replacement patches round trip and reject the wrong base" {
    const alloc = std.testing.allocator;
    const base = "a long stable posting prefix: old-middle :and-stable-suffix";
    const replacement = "a long stable posting prefix: NEW :and-stable-suffix";
    const patch = try encodeReplacementPatchAlloc(alloc, .base, base, replacement);
    defer alloc.free(patch);

    const decoded = try applyReplacementPatchAlloc(alloc, patch, base);
    defer alloc.free(decoded.replacement);
    try std.testing.expectEqual(RecordKind.base, decoded.target);
    try std.testing.expectEqualStrings(replacement, decoded.replacement);
    try std.testing.expectError(error.PostingPatchBaseMismatch, applyReplacementPatchAlloc(alloc, patch, "wrong"));
}

test "posting WAL replacement patches preserve shifted binary runs" {
    const alloc = std.testing.allocator;
    var base: [4096]u8 = undefined;
    for (&base, 0..) |*byte, i| byte.* = @truncate(i *% 131 +% i / 7);
    var replacement: [4096]u8 = undefined;
    @memcpy(replacement[0..173], base[0..173]);
    @memset(replacement[173..189], 0xa5);
    @memcpy(replacement[189..], base[173 .. base.len - 16]);

    const patch = try encodeReplacementPatchAlloc(alloc, .quantized_checkpoint, &base, &replacement);
    defer alloc.free(patch);
    try std.testing.expect(patch.len < replacement.len / 8);
    const decoded = try applyReplacementPatchAlloc(alloc, patch, &base);
    defer alloc.free(decoded.replacement);
    try std.testing.expectEqualSlices(u8, &replacement, decoded.replacement);
}

test "posting WAL replacement patches fast path a point insertion" {
    const alloc = std.testing.allocator;
    var base: [4096]u8 = undefined;
    for (&base, 0..) |*byte, i| byte.* = @truncate(i *% 193 +% i / 11);
    var replacement: [4112]u8 = undefined;
    @memcpy(replacement[0..2048], base[0..2048]);
    @memset(replacement[2048..2064], 0x5a);
    @memcpy(replacement[2064..], base[2048..]);

    const patch = try encodeReplacementPatchAlloc(alloc, .base, &base, &replacement);
    defer alloc.free(patch);
    // Header, prefix copy, insertion literal, and suffix copy.
    try std.testing.expectEqual(@as(usize, 32 + 9 + 5 + 16 + 9), patch.len);
    try std.testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, patch[16..20], .big));
    const decoded = try applyReplacementPatchAlloc(alloc, patch, &base);
    defer alloc.free(decoded.replacement);
    try std.testing.expectEqualSlices(u8, &replacement, decoded.replacement);
}

test "posting WAL coverage records advance a committed source sequence" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.coverage, 1, 0, 42, &.{});
    try writer.commit(1, 42);

    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqual(@as(u64, 42), replay.covered_source_sequence);
    try std.testing.expectEqual(@as(usize, 1), replay.records.items.len);
    try std.testing.expectEqual(@as(usize, 0), replay.posting_order.items.len);
}
