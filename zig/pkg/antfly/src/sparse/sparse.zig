// Copyright 2026 Antfly, Inc.
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

//! Backend-backed chunked inverted index for sparse vectors.
//!
//! Matches Go antfly's lib/sparseindex/ design:
//!   - Sparse vectors: sorted (indices: []u32, values: []f32)
//!   - Posting list chunks: delta-encoded doc nums + quantized weights
//!   - DAAT (Document-At-A-Time) scoring via dot product accumulation
//!
//! Sparse layout v2 uses binary typed keys. Bulk-built inverted postings are
//! stored as sparse segment blobs; legacy-shaped chunk rows remain as the small
//! delta path for incremental writes.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const backend_erased = @import("../storage/backend_erased.zig");
const backend_types = @import("../storage/backend_types.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const platform_time = @import("../platform/time.zig");
const supports_native_sparse_lmdb = builtin.os.tag != .freestanding;
const lmdb_backend = if (supports_native_sparse_lmdb) @import("../storage/lmdb_backend.zig") else struct {
    pub const Backend = struct {
        pub fn close(_: *@This()) void {}
        pub fn sync(_: *@This(), _: bool) !void {
            return error.UnsupportedPlatform;
        }
    };
};
const mem_backend = @import("../storage/mem_backend.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");

// ============================================================================
// Types
// ============================================================================

pub const SparseVector = struct {
    indices: []const u32, // sorted dimension indices
    values: []const f32, // weights per dimension
};

pub const SparseWrite = struct {
    doc_id: []const u8,
    vec: SparseVector,
    doc_num: ?u32 = null,
};

pub const BatchOptions = struct {
    defer_term_range_updates: bool = false,
    backend_batch_options: backend_types.BatchOptions = .{},
    prefer_bulk_build: bool = false,
    assume_new_doc_ids: bool = false,
};

pub const WriteProfile = struct {
    batch_calls: u64 = 0,
    incremental_calls: u64 = 0,
    bulk_append_calls: u64 = 0,
    bulk_append_fallbacks: u64 = 0,
    writes: u64 = 0,
    deletes: u64 = 0,
    postings: u64 = 0,
    terms: u64 = 0,
    reserve_ns: u64 = 0,
    dedupe_ns: u64 = 0,
    existence_check_ns: u64 = 0,
    doc_num_ns: u64 = 0,
    fwd_rev_put_ns: u64 = 0,
    posting_collect_ns: u64 = 0,
    posting_sort_ns: u64 = 0,
    posting_write_ns: u64 = 0,
    chunk_read_ns: u64 = 0,
    chunk_encode_ns: u64 = 0,
    chunk_put_ns: u64 = 0,
    range_meta_encode_ns: u64 = 0,
    range_meta_put_ns: u64 = 0,
    term_meta_ns: u64 = 0,
    commit_ns: u64 = 0,
    incremental_delete_ns: u64 = 0,
    incremental_insert_ns: u64 = 0,
    incremental_refresh_ns: u64 = 0,
    incremental_commit_ns: u64 = 0,

    pub fn delta(after: WriteProfile, before: WriteProfile) WriteProfile {
        var out: WriteProfile = .{};
        inline for (std.meta.fields(WriteProfile)) |field| {
            @field(out, field.name) = @field(after, field.name) -| @field(before, field.name);
        }
        return out;
    }

    pub fn add(self: *WriteProfile, other: WriteProfile) void {
        inline for (std.meta.fields(WriteProfile)) |field| {
            @field(self.*, field.name) += @field(other, field.name);
        }
    }
};

pub const SearchResult = struct {
    doc_id: []u8,
    doc_num: ?u32 = null,
    score: f32,
};

const SearchCandidate = struct {
    doc_num: u32,
    score: f32,
    doc_id: ?[]u8 = null,
};

const SearchProfile = struct {
    filter_resolve_ns: u64 = 0,
    segment_seek_ns: u64 = 0,
    segment_decode_ns: u64 = 0,
    delta_chunk_ns: u64 = 0,
    score_collect_ns: u64 = 0,
    sort_ns: u64 = 0,
    hydrate_ns: u64 = 0,
    terms: usize = 0,
    segment_entries: usize = 0,
    segment_chunks: usize = 0,
    delta_chunks: usize = 0,
    scored_docs: usize = 0,
    results: usize = 0,
};

pub const SearchConstraints = struct {
    filter_doc_ids: []const []const u8 = &.{},
    exclude_doc_ids: []const []const u8 = &.{},
    filter_doc_nums: []const u32 = &.{},
    exclude_doc_nums: []const u32 = &.{},
};

const BulkPosting = struct {
    term_id: u32,
    doc_num: u32,
    weight: f32,
    doc_id: []const u8,
};

const BulkDoc = struct {
    write_idx: usize,
    doc_num: u64,
};

pub const SplitRebuildResult = struct {
    doc_ids: [][]u8,
    select_docs_ns: u64 = 0,
    terms_ns: u64 = 0,
    commit_ns: u64 = 0,

    pub fn deinit(self: *SplitRebuildResult, alloc: Allocator) void {
        for (self.doc_ids) |doc_id| alloc.free(doc_id);
        alloc.free(self.doc_ids);
        self.* = undefined;
    }
};

pub const SplitPlanningStats = struct {
    selected_docs: usize = 0,
    touched_terms: usize = 0,
    right_only_chunks: usize = 0,
    mixed_chunks: usize = 0,
    right_only_postings: usize = 0,
    mixed_right_postings: usize = 0,
};

const RetainedChunk = struct {
    chunk_bytes: []u8,
    meta_bytes: []u8,
    max_weight: f32,

    fn deinit(self: *RetainedChunk, alloc: Allocator) void {
        alloc.free(self.chunk_bytes);
        alloc.free(self.meta_bytes);
        self.* = undefined;
    }
};

// ============================================================================
// Chunk encoding (matches Go's encoding.go format v1)
// ============================================================================

const CHUNK_FORMAT_VERSION: u8 = 1;

fn encodeChunk(alloc: Allocator, doc_nums: []const u32, weights: []const f32) ![]u8 {
    const n: u32 = @intCast(doc_nums.len);
    if (n == 0) return try alloc.alloc(u8, 0);

    // Compute min/max weights
    var min_w: f32 = weights[0];
    var max_w: f32 = weights[0];
    for (weights[1..]) |w| {
        if (w < min_w) min_w = w;
        if (w > max_w) max_w = w;
    }

    // Delta-encode doc nums
    var deltas = try alloc.alloc(u32, n);
    defer alloc.free(deltas);
    deltas[0] = doc_nums[0];
    for (1..n) |i| {
        deltas[i] = doc_nums[i] - doc_nums[i - 1];
    }

    // Quantize weights to u8
    var quant = try alloc.alloc(u8, n);
    defer alloc.free(quant);
    const scale = if (max_w > min_w) max_w - min_w else 1.0;
    for (weights, 0..) |w, i| {
        quant[i] = @intFromFloat(@min(255.0, (w - min_w) / scale * 255.0));
    }

    // Encode: [version:u8][n:u32 LE][max_w:f32 LE][min_w:f32 LE][deltas:n*u32 LE][quant:n*u8]
    const size = 1 + 4 + 4 + 4 + n * 4 + n;
    var buf = try alloc.alloc(u8, size);
    var pos: usize = 0;

    buf[pos] = CHUNK_FORMAT_VERSION;
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], n, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(max_w), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(min_w), .little);
    pos += 4;

    for (deltas) |d| {
        std.mem.writeInt(u32, buf[pos..][0..4], d, .little);
        pos += 4;
    }
    @memcpy(buf[pos .. pos + n], quant);

    return buf;
}

const DecodedChunk = struct {
    doc_nums: []u32,
    weights: []f32,
};

fn decodeChunkMaxWeight(data: []const u8) !f32 {
    if (data.len < 9) return error.InvalidChunk;
    const bits = std.mem.readInt(u32, data[5..9], .little);
    return @bitCast(bits);
}

fn decodeChunk(alloc: Allocator, data: []const u8) !DecodedChunk {
    if (data.len < 13) return error.InvalidChunk;
    var pos: usize = 0;

    const version = data[pos];
    pos += 1;
    if (version != CHUNK_FORMAT_VERSION) return error.InvalidChunk;

    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const max_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const max_w: f32 = @bitCast(max_w_bits);
    pos += 4;
    const min_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const min_w: f32 = @bitCast(min_w_bits);
    pos += 4;

    // Delta-decode doc nums
    var doc_nums = try alloc.alloc(u32, n);
    errdefer alloc.free(doc_nums);
    for (0..n) |i| {
        doc_nums[i] = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
    }
    // Undo deltas
    for (1..n) |i| {
        doc_nums[i] += doc_nums[i - 1];
    }

    // Dequantize weights
    var weights = try alloc.alloc(f32, n);
    errdefer alloc.free(weights);
    const scale = if (max_w > min_w) max_w - min_w else 1.0;
    for (0..n) |i| {
        const q: f32 = @floatFromInt(data[pos + i]);
        weights[i] = min_w + q * (scale / 255.0);
    }

    return .{ .doc_nums = doc_nums, .weights = weights };
}

fn collectSelectedChunkEntries(
    alloc: Allocator,
    data: []const u8,
    selected_docs: *const SelectedDocLookup,
    out_doc_nums: *std.ArrayListUnmanaged(u32),
    out_weights: *std.ArrayListUnmanaged(f32),
    out_min_doc_id: *?[]const u8,
    out_max_doc_id: *?[]const u8,
    out_max_weight: *f32,
) !void {
    if (data.len < 13) return error.InvalidChunk;
    var pos: usize = 0;

    const version = data[pos];
    pos += 1;
    if (version != CHUNK_FORMAT_VERSION) return error.InvalidChunk;

    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const max_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const max_w: f32 = @bitCast(max_w_bits);
    pos += 4;
    const min_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const min_w: f32 = @bitCast(min_w_bits);
    pos += 4;

    const delta_start = pos;
    const delta_bytes = @as(usize, n) * 4;
    const quant_start = delta_start + delta_bytes;
    if (data.len < quant_start + n) return error.InvalidChunk;

    try out_doc_nums.ensureTotalCapacity(alloc, n);
    try out_weights.ensureTotalCapacity(alloc, n);

    const scale = if (max_w > min_w) max_w - min_w else 1.0;
    var current_doc_num: u32 = 0;
    var first = true;
    for (0..n) |i| {
        const delta = std.mem.readInt(u32, data[delta_start + i * 4 ..][0..4], .little);
        current_doc_num = if (first) blk: {
            first = false;
            break :blk delta;
        } else current_doc_num + delta;

        const doc_id = selected_docs.get(current_doc_num) orelse continue;
        const q: f32 = @floatFromInt(data[quant_start + i]);
        const weight = min_w + q * (scale / 255.0);
        try out_doc_nums.append(alloc, current_doc_num);
        try out_weights.append(alloc, weight);
        out_max_weight.* = if (out_doc_nums.items.len == 1) weight else @max(out_max_weight.*, weight);
        updateBorrowedRangeBounds(out_min_doc_id, out_max_doc_id, doc_id, doc_id);
    }
}

// ============================================================================
// Forward index entry encoding
// ============================================================================

fn encodeFwdEntry(alloc: Allocator, doc_num: u64, term_ids: []const u32, weights: []const f32) ![]u8 {
    const n: u32 = @intCast(term_ids.len);
    const size = 8 + 4 + n * 4 + n * 4;
    var buf = try alloc.alloc(u8, size);
    var pos: usize = 0;

    std.mem.writeInt(u64, buf[pos..][0..8], doc_num, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], n, .little);
    pos += 4;
    for (term_ids) |tid| {
        std.mem.writeInt(u32, buf[pos..][0..4], tid, .little);
        pos += 4;
    }
    for (weights) |w| {
        std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(w), .little);
        pos += 4;
    }
    return buf;
}

fn encodedFwdEntryLen(term_ids: []const u32) usize {
    return 8 + 4 + term_ids.len * 4 + term_ids.len * 4;
}

fn appendFwdEntry(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), doc_num: u64, term_ids: []const u32, weights: []const f32) !void {
    try appendU64Le(alloc, out, doc_num);
    try appendU32Le(alloc, out, @intCast(term_ids.len));
    for (term_ids) |tid| try appendU32Le(alloc, out, tid);
    for (weights) |weight| try appendU32Le(alloc, out, @bitCast(weight));
}

const DecodedFwdEntry = struct {
    doc_num: u64,
    term_ids: []u32,
    weights: []f32,
};

fn decodeFwdDocNum(data: []const u8) !u64 {
    if (data.len < 8) return error.InvalidChunk;
    return std.mem.readInt(u64, data[0..8], .little);
}

fn decodeFwdEntry(alloc: Allocator, data: []const u8) !DecodedFwdEntry {
    var pos: usize = 0;
    const doc_num = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;

    var term_ids = try alloc.alloc(u32, n);
    errdefer alloc.free(term_ids);
    for (0..n) |i| {
        term_ids[i] = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
    }

    var weights = try alloc.alloc(f32, n);
    errdefer alloc.free(weights);
    for (0..n) |i| {
        const bits = std.mem.readInt(u32, data[pos..][0..4], .little);
        weights[i] = @bitCast(bits);
        pos += 4;
    }

    return .{ .doc_num = doc_num, .term_ids = term_ids, .weights = weights };
}

fn parseFwdDocNumAndTermCount(data: []const u8) !struct { doc_num: u64, term_count: u32, terms_start: usize } {
    if (data.len < 12) return error.InvalidChunk;
    var pos: usize = 0;
    const doc_num = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (data.len < 12 + @as(usize, n) * 4) return error.InvalidChunk;
    return .{ .doc_num = doc_num, .term_count = n, .terms_start = pos };
}

fn forEachFwdTermId(data: []const u8, comptime Context: type, context: *Context, comptime func: fn (*Context, u32) anyerror!void) !u64 {
    const parsed = try parseFwdDocNumAndTermCount(data);
    var pos = parsed.terms_start;
    for (0..parsed.term_count) |_| {
        const term_id = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        try func(context, term_id);
    }
    return parsed.doc_num;
}

// ============================================================================
// Term metadata encoding
// ============================================================================

fn encodeTermMeta(max_weight: f32, chunk_count: u32) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], @bitCast(max_weight), .little);
    std.mem.writeInt(u32, buf[4..8], chunk_count, .little);
    return buf;
}

fn decodeTermMeta(data: []const u8) struct { max_weight: f32, chunk_count: u32 } {
    const mw_bits = std.mem.readInt(u32, data[0..4], .little);
    const cc = std.mem.readInt(u32, data[4..8], .little);
    return .{ .max_weight = @bitCast(mw_bits), .chunk_count = cc };
}

const ChunkRangeMeta = struct {
    min_doc_id: []const u8,
    max_doc_id: []const u8,
};

const SEGMENT_FORMAT_VERSION: u32 = 1;
const segment_magic = "ASPSSEG1";
const segment_header_len: usize = segment_magic.len + 8;
const segment_dir_entry_len: usize = 20;
const docmap_magic = "ASPSMAP1";
const docmap_header_len: usize = docmap_magic.len + 8;

const SegmentTermPayload = struct {
    term_id: u32,
    bytes: []u8,

    fn deinit(self: *SegmentTermPayload, alloc: Allocator) void {
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

const DocMapLookup = struct {
    doc_num: u64,
    doc_id: []const u8,
    fwd_data: []const u8,
};

fn appendU32Le(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendU64Le(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn encodeSegmentFromSortedPostings(
    alloc: Allocator,
    postings: []const BulkPosting,
    chunk_size: u32,
) ![]u8 {
    var payloads = std.ArrayListUnmanaged(SegmentTermPayload).empty;
    defer {
        for (payloads.items) |*payload| payload.deinit(alloc);
        payloads.deinit(alloc);
    }

    var start: usize = 0;
    while (start < postings.len) {
        const term_id = postings[start].term_id;
        var end = start + 1;
        while (end < postings.len and postings[end].term_id == term_id) : (end += 1) {}

        var term_payload = std.ArrayListUnmanaged(u8).empty;
        errdefer term_payload.deinit(alloc);
        var cursor = start;
        while (cursor < end) {
            const take = @min(@as(usize, @intCast(chunk_size)), end - cursor);
            var doc_nums = try alloc.alloc(u32, take);
            defer alloc.free(doc_nums);
            var weights = try alloc.alloc(f32, take);
            defer alloc.free(weights);
            var min_doc_id: ?[]const u8 = null;
            var max_doc_id: ?[]const u8 = null;

            for (postings[cursor .. cursor + take], 0..) |posting, i| {
                doc_nums[i] = posting.doc_num;
                weights[i] = posting.weight;
                updateBorrowedRangeBounds(&min_doc_id, &max_doc_id, posting.doc_id, posting.doc_id);
            }

            const chunk = try encodeChunk(alloc, doc_nums, weights);
            defer alloc.free(chunk);
            const range = try encodeChunkRangeMeta(alloc, min_doc_id.?, max_doc_id.?);
            defer alloc.free(range);

            try appendU32Le(alloc, &term_payload, @intCast(chunk.len));
            try appendU32Le(alloc, &term_payload, @intCast(range.len));
            try term_payload.appendSlice(alloc, chunk);
            try term_payload.appendSlice(alloc, range);
            cursor += take;
        }

        try payloads.append(alloc, .{
            .term_id = term_id,
            .bytes = try term_payload.toOwnedSlice(alloc),
        });
        start = end;
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, segment_magic);
    try appendU32Le(alloc, &out, SEGMENT_FORMAT_VERSION);
    try appendU32Le(alloc, &out, @intCast(payloads.items.len));

    var payload_offset: u64 = segment_header_len + @as(u64, @intCast(payloads.items.len)) * segment_dir_entry_len;
    for (payloads.items) |payload| {
        try appendU32Le(alloc, &out, payload.term_id);
        try appendU32Le(alloc, &out, countSegmentPayloadChunks(payload.bytes));
        try appendU64Le(alloc, &out, payload_offset);
        try appendU32Le(alloc, &out, @intCast(payload.bytes.len));
        payload_offset += payload.bytes.len;
    }
    for (payloads.items) |payload| try out.appendSlice(alloc, payload.bytes);
    return try out.toOwnedSlice(alloc);
}

fn countSegmentPayloadChunks(payload: []const u8) u32 {
    var count: u32 = 0;
    var pos: usize = 0;
    while (pos + 8 <= payload.len) : (count += 1) {
        const chunk_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        const range_len = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little);
        pos += 8 + @as(usize, chunk_len) + @as(usize, range_len);
        if (pos > payload.len) return count;
    }
    return count;
}

fn segmentTermPayload(data: []const u8, term_id: u32) !?[]const u8 {
    if (data.len < segment_header_len) return error.InvalidSparseSegment;
    if (!std.mem.eql(u8, data[0..segment_magic.len], segment_magic)) return error.InvalidSparseSegment;
    const version = std.mem.readInt(u32, data[segment_magic.len..][0..4], .little);
    if (version != SEGMENT_FORMAT_VERSION) return error.InvalidSparseSegment;
    const term_count = std.mem.readInt(u32, data[segment_magic.len + 4 ..][0..4], .little);
    const dir_start = segment_header_len;
    const dir_len = @as(usize, term_count) * segment_dir_entry_len;
    if (dir_start + dir_len > data.len) return error.InvalidSparseSegment;

    var pos = dir_start;
    for (0..term_count) |_| {
        const current_term = std.mem.readInt(u32, data[pos..][0..4], .little);
        const offset = std.mem.readInt(u64, data[pos + 8 ..][0..8], .little);
        const len = std.mem.readInt(u32, data[pos + 16 ..][0..4], .little);
        if (current_term == term_id) {
            const start: usize = @intCast(offset);
            const end = start + @as(usize, len);
            if (end > data.len) return error.InvalidSparseSegment;
            return data[start..end];
        }
        pos += segment_dir_entry_len;
    }
    return null;
}

fn forEachSegmentChunk(
    alloc: Allocator,
    segment: []const u8,
    term_id: u32,
    context: anytype,
    comptime func: fn (@TypeOf(context), DecodedChunk) anyerror!void,
) !void {
    const payload = (try segmentTermPayload(segment, term_id)) orelse return;
    var pos: usize = 0;
    while (pos < payload.len) {
        if (pos + 8 > payload.len) return error.InvalidSparseSegment;
        const chunk_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        const range_len = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little);
        pos += 8;
        const chunk_end = pos + @as(usize, chunk_len);
        const range_end = chunk_end + @as(usize, range_len);
        if (range_end > payload.len) return error.InvalidSparseSegment;
        const decoded = try decodeChunk(alloc, payload[pos..chunk_end]);
        defer alloc.free(decoded.doc_nums);
        defer alloc.free(decoded.weights);
        try func(context, decoded);
        pos = range_end;
    }
}

fn encodeDocMapSegment(alloc: Allocator, writes: []const SparseWrite, docs: []const BulkDoc) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, docmap_magic);
    try appendU32Le(alloc, &out, SEGMENT_FORMAT_VERSION);
    try appendU32Le(alloc, &out, @intCast(docs.len));
    for (docs) |doc| {
        const write = writes[doc.write_idx];
        const fwd_len = encodedFwdEntryLen(write.vec.indices);
        try appendU64Le(alloc, &out, doc.doc_num);
        try appendU32Le(alloc, &out, @intCast(write.doc_id.len));
        try appendU32Le(alloc, &out, @intCast(fwd_len));
        try out.appendSlice(alloc, write.doc_id);
        try appendFwdEntry(alloc, &out, doc.doc_num, write.vec.indices, write.vec.values);
    }
    return try out.toOwnedSlice(alloc);
}

fn forEachDocMapEntry(
    data: []const u8,
    context: anytype,
    comptime func: fn (@TypeOf(context), DocMapLookup) anyerror!bool,
) !bool {
    if (data.len < docmap_header_len) return error.InvalidSparseDocMapSegment;
    if (!std.mem.eql(u8, data[0..docmap_magic.len], docmap_magic)) return error.InvalidSparseDocMapSegment;
    const version = std.mem.readInt(u32, data[docmap_magic.len..][0..4], .little);
    if (version != SEGMENT_FORMAT_VERSION) return error.InvalidSparseDocMapSegment;
    const count = std.mem.readInt(u32, data[docmap_magic.len + 4 ..][0..4], .little);
    var pos: usize = docmap_header_len;
    for (0..count) |_| {
        if (pos + 16 > data.len) return error.InvalidSparseDocMapSegment;
        const doc_num = std.mem.readInt(u64, data[pos..][0..8], .little);
        const doc_id_len = std.mem.readInt(u32, data[pos + 8 ..][0..4], .little);
        const fwd_len = std.mem.readInt(u32, data[pos + 12 ..][0..4], .little);
        pos += 16;
        const doc_id_end = pos + @as(usize, doc_id_len);
        const fwd_end = doc_id_end + @as(usize, fwd_len);
        if (fwd_end > data.len) return error.InvalidSparseDocMapSegment;
        if (try func(context, .{
            .doc_num = doc_num,
            .doc_id = data[pos..doc_id_end],
            .fwd_data = data[doc_id_end..fwd_end],
        })) return true;
        pos = fwd_end;
    }
    return false;
}

const SelectedDocLookup = struct {
    map: ?*const std.AutoHashMapUnmanaged(u32, []u8) = null,
    owned_map: std.AutoHashMapUnmanaged(u32, []const u8) = .empty,
    dense_min_doc_num: u32 = 0,
    dense_doc_ids: []?[]const u8 = &.{},

    fn init(alloc: Allocator, map: *const std.AutoHashMapUnmanaged(u32, []u8)) !SelectedDocLookup {
        var lookup: SelectedDocLookup = .{ .map = map };
        if (map.count() == 0) return lookup;

        var min_doc_num: u32 = std.math.maxInt(u32);
        var max_doc_num: u32 = 0;
        var it = map.iterator();
        while (it.next()) |entry| {
            const doc_num = entry.key_ptr.*;
            min_doc_num = @min(min_doc_num, doc_num);
            max_doc_num = @max(max_doc_num, doc_num);
        }

        const span = @as(usize, max_doc_num) - @as(usize, min_doc_num) + 1;
        if (span > map.count() * 8) return lookup;

        const dense_doc_ids = try alloc.alloc(?[]const u8, span);
        errdefer alloc.free(dense_doc_ids);
        @memset(dense_doc_ids, null);

        it = map.iterator();
        while (it.next()) |entry| {
            dense_doc_ids[@as(usize, entry.key_ptr.*) - min_doc_num] = entry.value_ptr.*;
        }

        lookup.dense_min_doc_num = min_doc_num;
        lookup.dense_doc_ids = dense_doc_ids;
        return lookup;
    }

    fn initFromPairs(alloc: Allocator, doc_nums: []const u32, doc_ids: []const []const u8) !SelectedDocLookup {
        std.debug.assert(doc_nums.len == doc_ids.len);
        var lookup: SelectedDocLookup = .{};
        if (doc_nums.len == 0) return lookup;

        var min_doc_num: u32 = std.math.maxInt(u32);
        var max_doc_num: u32 = 0;
        for (doc_nums) |doc_num| {
            min_doc_num = @min(min_doc_num, doc_num);
            max_doc_num = @max(max_doc_num, doc_num);
        }

        const span = @as(usize, max_doc_num) - @as(usize, min_doc_num) + 1;
        if (span <= doc_nums.len * 8) {
            const dense_doc_ids = try alloc.alloc(?[]const u8, span);
            errdefer alloc.free(dense_doc_ids);
            @memset(dense_doc_ids, null);
            for (doc_nums, doc_ids) |doc_num, doc_id| {
                dense_doc_ids[@as(usize, doc_num) - min_doc_num] = doc_id;
            }
            lookup.dense_min_doc_num = min_doc_num;
            lookup.dense_doc_ids = dense_doc_ids;
            return lookup;
        }

        try lookup.owned_map.ensureTotalCapacity(alloc, @intCast(doc_nums.len));
        for (doc_nums, doc_ids) |doc_num, doc_id| {
            lookup.owned_map.putAssumeCapacity(doc_num, doc_id);
        }
        return lookup;
    }

    fn deinit(self: *SelectedDocLookup, alloc: Allocator) void {
        if (self.dense_doc_ids.len > 0) alloc.free(self.dense_doc_ids);
        self.owned_map.deinit(alloc);
        self.* = undefined;
    }

    fn get(self: *const SelectedDocLookup, doc_num: u32) ?[]const u8 {
        if (self.dense_doc_ids.len > 0) {
            if (doc_num < self.dense_min_doc_num) return null;
            const idx = @as(usize, doc_num) - self.dense_min_doc_num;
            if (idx >= self.dense_doc_ids.len) return null;
            return self.dense_doc_ids[idx];
        }
        if (self.owned_map.count() > 0) return self.owned_map.get(doc_num);
        if (self.map) |map| return map.get(doc_num);
        return null;
    }
};

fn encodeChunkRangeMeta(alloc: Allocator, min_doc_id: []const u8, max_doc_id: []const u8) ![]u8 {
    const total = 8 + min_doc_id.len + max_doc_id.len;
    var buf = try alloc.alloc(u8, total);
    std.mem.writeInt(u32, buf[0..4], @intCast(min_doc_id.len), .little);
    std.mem.writeInt(u32, buf[4..8], @intCast(max_doc_id.len), .little);
    @memcpy(buf[8 .. 8 + min_doc_id.len], min_doc_id);
    @memcpy(buf[8 + min_doc_id.len ..][0..max_doc_id.len], max_doc_id);
    return buf;
}

fn decodeChunkRangeMeta(data: []const u8) !ChunkRangeMeta {
    if (data.len < 8) return error.InvalidChunk;
    const min_len = std.mem.readInt(u32, data[0..4], .little);
    const max_len = std.mem.readInt(u32, data[4..8], .little);
    const min_start: usize = 8;
    const min_end = min_start + min_len;
    const max_end = min_end + max_len;
    if (max_end > data.len) return error.InvalidChunk;
    return .{
        .min_doc_id = data[min_start..min_end],
        .max_doc_id = data[min_end..max_end],
    };
}

fn updateOwnedRangeBounds(
    alloc: Allocator,
    min_out: *?[]u8,
    max_out: *?[]u8,
    min_doc_id: []const u8,
    max_doc_id: []const u8,
) !void {
    if (min_out.* == null or std.mem.order(u8, min_doc_id, min_out.*.?) == .lt) {
        if (min_out.*) |existing| alloc.free(existing);
        min_out.* = try alloc.dupe(u8, min_doc_id);
    }
    if (max_out.* == null or std.mem.order(u8, max_doc_id, max_out.*.?) == .gt) {
        if (max_out.*) |existing| alloc.free(existing);
        max_out.* = try alloc.dupe(u8, max_doc_id);
    }
}

fn updateBorrowedRangeBounds(
    min_out: *?[]const u8,
    max_out: *?[]const u8,
    min_doc_id: []const u8,
    max_doc_id: []const u8,
) void {
    if (min_out.* == null or std.mem.order(u8, min_doc_id, min_out.*.?) == .lt) {
        min_out.* = min_doc_id;
    }
    if (max_out.* == null or std.mem.order(u8, max_doc_id, max_out.*.?) == .gt) {
        max_out.* = max_doc_id;
    }
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

var sparse_search_profile_enabled_cache: std.atomic.Value(u8) = .init(0);

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

fn envBoolEnabled(raw_z: [*:0]const u8) bool {
    const raw = std.mem.span(raw_z);
    return !(std.mem.eql(u8, raw, "0") or
        std.ascii.eqlIgnoreCase(raw, "false") or
        std.ascii.eqlIgnoreCase(raw, "no"));
}

fn sparseSearchProfileEnabled() bool {
    const cached = sparse_search_profile_enabled_cache.load(.monotonic);
    if (cached != 0) return cached == 2;
    if (comptime builtin.os.tag == .freestanding) {
        sparse_search_profile_enabled_cache.store(1, .monotonic);
        return false;
    }
    const raw_z = getenv("ANTFLY_BENCH_SPARSE_SEARCH_PROFILE") orelse
        getenv("ANTFLY_BENCH_METRICS") orelse {
        sparse_search_profile_enabled_cache.store(1, .monotonic);
        return false;
    };
    const enabled = envBoolEnabled(raw_z);
    sparse_search_profile_enabled_cache.store(if (enabled) 2 else 1, .monotonic);
    return enabled;
}

fn sortAndDedupU32(items: []u32) []u32 {
    if (items.len <= 1) return items;
    std.mem.sort(u32, items, {}, struct {
        fn lessThan(_: void, a: u32, b: u32) bool {
            return a < b;
        }
    }.lessThan);
    var out_len: usize = 1;
    for (items[1..]) |item| {
        if (item == items[out_len - 1]) continue;
        items[out_len] = item;
        out_len += 1;
    }
    return items[0..out_len];
}

fn sortDocNumsAndWeights(doc_nums: []u32, weights: []f32) void {
    std.debug.assert(doc_nums.len == weights.len);
    if (doc_nums.len <= 1) return;
    for (1..doc_nums.len) |i| {
        var j = i;
        while (j > 0 and doc_nums[j] < doc_nums[j - 1]) : (j -= 1) {
            std.mem.swap(u32, &doc_nums[j], &doc_nums[j - 1]);
            std.mem.swap(f32, &weights[j], &weights[j - 1]);
        }
    }
}

// ============================================================================
// Key builders
// ============================================================================

const key_fwd: u8 = 0x01;
const key_rev: u8 = 0x02;
const key_segment: u8 = 0x03;
const key_meta: u8 = 0x04;
const key_term_catalog: u8 = 0x05;
const key_docmap_segment: u8 = 0x06;
const key_doc_tombstone: u8 = 0x07;
const key_inv: u8 = 0x10;

const meta_next_doc_num: u8 = 0x01;
const meta_doc_count: u8 = 0x02;
const meta_term_count: u8 = 0x03;
const meta_next_segment_id: u8 = 0x04;

const inv_kind_meta: u8 = 0x01;
const inv_kind_chunk: u8 = 0x02;
const inv_kind_chunk_meta: u8 = 0x03;
const inv_kind_term_range: u8 = 0x04;

fn taggedPrefix(comptime tag: u8) *const [1]u8 {
    return &.{tag};
}

fn fwdKey(buf: []u8, doc_id: []const u8) []const u8 {
    std.debug.assert(buf.len >= 1 + doc_id.len);
    buf[0] = key_fwd;
    @memcpy(buf[1..][0..doc_id.len], doc_id);
    return buf[0 .. 1 + doc_id.len];
}

fn fwdKeyAlloc(alloc: Allocator, doc_id: []const u8) ![]u8 {
    const key = try alloc.alloc(u8, 1 + doc_id.len);
    key[0] = key_fwd;
    @memcpy(key[1..], doc_id);
    return key;
}

fn fwdDocIdFromKey(key: []const u8) ?[]const u8 {
    if (key.len == 0 or key[0] != key_fwd) return null;
    return key[1..];
}

fn revKey(buf: []u8, doc_num: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_rev;
    std.mem.writeInt(u64, buf[1..][0..8], doc_num, .big);
    return buf[0..9];
}

fn invMetaKey(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 6);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_meta;
    return buf[0..6];
}

fn invChunkKey(buf: []u8, term_id: u32, chunk_num: u32) []const u8 {
    std.debug.assert(buf.len >= 10);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_chunk;
    std.mem.writeInt(u32, buf[6..][0..4], chunk_num, .big);
    return buf[0..10];
}

fn invChunkMetaKey(buf: []u8, term_id: u32, chunk_num: u32) []const u8 {
    std.debug.assert(buf.len >= 10);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_chunk_meta;
    std.mem.writeInt(u32, buf[6..][0..4], chunk_num, .big);
    return buf[0..10];
}

fn termRangeKey(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 6);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_term_range;
    return buf[0..6];
}

fn invChunkPrefix(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 6);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_chunk;
    return buf[0..6];
}

fn segmentKey(buf: []u8, segment_id: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_segment;
    std.mem.writeInt(u64, buf[1..][0..8], segment_id, .big);
    return buf[0..9];
}

fn docMapSegmentKey(buf: []u8, segment_id: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_docmap_segment;
    std.mem.writeInt(u64, buf[1..][0..8], segment_id, .big);
    return buf[0..9];
}

fn docTombstoneKey(buf: []u8, doc_num: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_doc_tombstone;
    std.mem.writeInt(u64, buf[1..][0..8], doc_num, .big);
    return buf[0..9];
}

fn metaKey(kind: u8) *const [2]u8 {
    return switch (kind) {
        meta_next_doc_num => &.{ key_meta, meta_next_doc_num },
        meta_doc_count => &.{ key_meta, meta_doc_count },
        meta_term_count => &.{ key_meta, meta_term_count },
        meta_next_segment_id => &.{ key_meta, meta_next_segment_id },
        else => unreachable,
    };
}

fn termCatalogKey(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 5);
    buf[0] = key_term_catalog;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    return buf[0..5];
}

fn parseTermRangeKey(key: []const u8) ?u32 {
    if (key.len != 6 or key[0] != key_inv or key[5] != inv_kind_term_range) return null;
    return std.mem.readInt(u32, key[1..][0..4], .big);
}

// ============================================================================
// SparseIndex
// ============================================================================

pub const SparseIndexOptions = struct {
    map_size: usize = 256 * 1024 * 1024,
    chunk_size: u32 = 1024,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    backend: SparseBackend = .lsm,
    lsm_storage: ?lsm_backend.Storage = null,
    lsm_cache: ?*lsm_backend.Cache = null,
    lsm_options: lsm_backend.Options = .{ .flush_threshold = 1 },
    lsm_root_generation: u64 = 0,
};

pub const SparseBackend = enum {
    lmdb,
    mem,
    lsm_memory,
    lsm,
};

pub const SparseIndex = struct {
    alloc: Allocator,
    store: backend_erased.Store,
    owner: StoreOwner,
    dbi: void = {},
    chunk_size: u32,
    next_doc_num: u64,
    next_segment_id: u64,
    doc_count: u64,
    term_count: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    write_profile: WriteProfile = .{},

    const StoreOwner = union(enum) {
        none,
        lmdb: *lmdb_backend.Backend,
        mem: *mem_backend.Backend,
        lsm: lsm_backend.BackendHandle,

        fn close(self: *StoreOwner, alloc: Allocator) void {
            switch (self.*) {
                .none => {},
                .lmdb => |backend| {
                    backend.close();
                    alloc.destroy(backend);
                },
                .mem => |backend| {
                    backend.close();
                    alloc.destroy(backend);
                },
                .lsm => |*handle| handle.close(),
            }
            self.* = .none;
        }

        fn sync(self: *StoreOwner, force: bool) !void {
            switch (self.*) {
                .none, .mem => {},
                .lmdb => |backend| try backend.sync(force),
                .lsm => |*handle| try handle.backend.sync(force),
            }
        }
    };

    const OpenedStore = struct {
        store: backend_erased.Store,
        owner: StoreOwner,
    };

    fn resolvedLsmOptions(opts: SparseIndexOptions, memory_only: bool) lsm_backend.Options {
        var lsm_options = opts.lsm_options;
        lsm_options.backend.durability = if (memory_only or opts.no_sync) .none else lsm_options.backend.durability;
        if (!memory_only) lsm_options.storage = opts.lsm_storage orelse lsm_options.storage;
        lsm_options.cache = opts.lsm_cache orelse lsm_options.cache;
        if (opts.lsm_root_generation != 0 and lsm_options.root_generation == 0) {
            lsm_options.root_generation = opts.lsm_root_generation;
        }
        return lsm_options;
    }

    fn openStore(alloc: Allocator, path: [*:0]const u8, opts: SparseIndexOptions) !OpenedStore {
        switch (opts.backend) {
            .lmdb => {
                if (!supports_native_sparse_lmdb) return error.UnsupportedPlatform;
                const backend = try alloc.create(lmdb_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = try lmdb_backend.Backend.open(alloc, path, .{
                    .backend = .{
                        .durability = if (opts.no_sync) .none else .full,
                    },
                    .env = .{
                        .map_size = opts.map_size,
                        .no_sync = opts.no_sync,
                        .no_meta_sync = opts.no_meta_sync,
                        .no_tls = true,
                        .max_dbs = 1,
                    },
                });
                errdefer backend.close();

                var runtime = try backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{ .store = runtime, .owner = .{ .lmdb = backend } };
            },
            .mem => {
                const backend = try alloc.create(mem_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = mem_backend.Backend.init(alloc, .{});
                errdefer backend.close();

                var runtime = try backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{ .store = runtime, .owner = .{ .mem = backend } };
            },
            .lsm_memory => {
                var handle = try lsm_backend.BackendHandle.init(alloc, resolvedLsmOptions(opts, true));
                errdefer handle.close();

                var runtime = try handle.backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{ .store = runtime, .owner = .{ .lsm = handle } };
            },
            .lsm => {
                var handle = try lsm_backend.BackendHandle.open(alloc, std.mem.span(path), resolvedLsmOptions(opts, false));
                errdefer handle.close();

                var runtime = try handle.backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{ .store = runtime, .owner = .{ .lsm = handle } };
            },
        }
    }

    fn beginReadTxn(self: *SparseIndex) !backend_erased.ReadTxn {
        return try self.store.beginRead();
    }

    fn beginWriteTxn(self: *SparseIndex) !backend_erased.WriteTxn {
        return try self.store.beginWrite();
    }

    fn beginBatchTxn(self: *SparseIndex, options: backend_types.BatchOptions) !backend_erased.Batch {
        return try self.store.beginBatchWithOptions(options);
    }

    pub fn backendStore(self: *SparseIndex) *backend_erased.Store {
        return &self.store;
    }

    pub fn attachResourceManager(self: *SparseIndex, manager: *resource_manager_mod.ResourceManager) void {
        self.resource_manager = manager;
    }

    pub fn getWriteProfile(self: *SparseIndex) WriteProfile {
        return self.write_profile;
    }

    pub fn beginBulkIngestSession(self: *SparseIndex) !void {
        try self.store.beginBulkIngestSession();
    }

    pub fn finishBulkIngestSessionWithOptions(self: *SparseIndex, options: backend_types.BulkIngestFinishOptions) !void {
        try self.store.finishBulkIngestSessionWithOptions(options);
    }

    pub fn abortBulkIngestSession(self: *SparseIndex) void {
        self.store.abortBulkIngestSession();
    }

    pub fn open(alloc: Allocator, path: [*:0]const u8, opts: SparseIndexOptions) !SparseIndex {
        var opened = try openStore(alloc, path, opts);
        errdefer {
            opened.store.deinit();
            opened.owner.close(alloc);
        }

        var next_doc_num: u64 = 0;
        var next_segment_id: u64 = 1;
        var doc_count: u64 = 0;
        var term_count: u64 = 0;
        if (opts.lsm_options.backend.read_only) {
            var txn = try opened.store.beginRead();
            defer txn.abort();
            const ndn_data = txn.get(metaKey(meta_next_doc_num)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (ndn_data) |d| {
                next_doc_num = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_next_segment_id)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                next_segment_id = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_doc_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                doc_count = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_term_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                term_count = std.mem.readInt(u64, d[0..8], .little);
            }
        } else {
            var txn = try opened.store.beginWrite();
            errdefer txn.abort();

            const ndn_data = txn.get(metaKey(meta_next_doc_num)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (ndn_data) |d| {
                next_doc_num = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_next_segment_id)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                next_segment_id = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_doc_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                doc_count = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_term_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                term_count = std.mem.readInt(u64, d[0..8], .little);
            }

            try txn.commit();
        }

        return .{
            .alloc = alloc,
            .store = opened.store,
            .owner = opened.owner,
            .chunk_size = opts.chunk_size,
            .next_doc_num = next_doc_num,
            .next_segment_id = next_segment_id,
            .doc_count = doc_count,
            .term_count = term_count,
        };
    }

    pub fn close(self: *SparseIndex) void {
        self.store.deinit();
        self.owner.close(self.alloc);
        self.* = undefined;
    }

    pub fn sync(self: *SparseIndex, force: bool) !void {
        try self.owner.sync(force);
    }

    pub fn syncReplayState(self: *SparseIndex) !void {
        try self.owner.sync(false);
    }

    pub const Stats = struct {
        doc_count: u64 = 0,
        term_count: u64 = 0,
    };

    pub fn stats(self: *SparseIndex) Stats {
        if (self.doc_count == 0 and self.term_count == 0) {
            if (self.loadPersistedStats() catch null) |persisted| {
                if (persisted.doc_count != 0 or persisted.term_count != 0) {
                    self.doc_count = persisted.doc_count;
                    self.term_count = persisted.term_count;
                    return persisted;
                }
            }
        }
        return .{
            .doc_count = self.doc_count,
            .term_count = self.term_count,
        };
    }

    fn loadPersistedStats(self: *SparseIndex) !Stats {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var out = Stats{};
        if (txn.get(metaKey(meta_doc_count)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            if (raw.len >= 8) out.doc_count = std.mem.readInt(u64, raw[0..8], .little);
        }
        if (txn.get(metaKey(meta_term_count)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            if (raw.len >= 8) out.term_count = std.mem.readInt(u64, raw[0..8], .little);
        }
        return out;
    }

    pub fn scanStats(self: *SparseIndex) !Stats {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        var out = Stats{};
        var maybe_entry = try cur.first();
        while (maybe_entry) |entry| {
            if (entry.key.len > 0 and entry.key[0] == key_rev) out.doc_count += 1;
            if (entry.key.len > 0 and entry.key[0] == key_term_catalog) out.term_count += 1;
            maybe_entry = try cur.next();
        }
        return out;
    }

    pub fn refreshPersistedStatsFromScan(self: *SparseIndex) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        const scanned = try scanStatsInTxn(&txn, self.dbi);
        self.doc_count = scanned.doc_count;
        self.term_count = scanned.term_count;
        try persistSparseCounters(self, &txn);
        try txn.commit();
    }

    pub fn persistBackfillDocCount(self: *SparseIndex, doc_count: u64) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        self.doc_count = doc_count;
        try persistSparseCounters(self, &txn);
        try txn.commit();
    }

    /// Batch insert and delete sparse vectors.
    pub fn batch(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8) !void {
        return try self.batchWithOptions(writes, deletes, .{});
    }

    pub fn batchWithOptions(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !void {
        self.write_profile.batch_calls += 1;
        self.write_profile.writes += writes.len;
        self.write_profile.deletes += deletes.len;
        if (options.prefer_bulk_build and writes.len > 0) {
            if (try self.tryBulkAppend(writes, deletes, options)) return;
            self.write_profile.bulk_append_fallbacks += 1;
        }
        try self.batchIncrementalWithOptions(writes, deletes, options);
    }

    fn batchIncrementalWithOptions(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !void {
        self.write_profile.incremental_calls += 1;
        if (options.backend_batch_options.mode == .bulk_ingest) {
            var txn = try self.beginBatchTxn(options.backend_batch_options);
            try self.batchIncrementalTxn(&txn, writes, deletes, options);
            return;
        }

        var txn = try self.beginWriteTxn();
        try self.batchIncrementalTxn(&txn, writes, deletes, options);
    }

    fn batchIncrementalTxn(self: *SparseIndex, txn: anytype, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !void {
        errdefer txn.abort();
        const prev_next_doc_num = self.next_doc_num;
        const prev_next_segment_id = self.next_segment_id;
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.next_doc_num = prev_next_doc_num;
            self.next_segment_id = prev_next_segment_id;
            self.doc_count = prev_doc_count;
            self.term_count = prev_term_count;
        }
        var scratch_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer scratch_arena.deinit();
        const scratch = scratch_arena.allocator();
        var touched_terms = std.AutoHashMapUnmanaged(u32, void).empty;
        defer touched_terms.deinit(self.alloc);
        const touched_terms_ptr = if (options.defer_term_range_updates) &touched_terms else null;

        // Process deletes
        var phase_start_ns = nowNs();
        for (deletes) |doc_id| {
            const effect = try self.processDelete(scratch, txn, doc_id, touched_terms_ptr);
            self.applyDeleteEffect(effect);
            _ = scratch_arena.reset(.retain_capacity);
        }
        self.write_profile.incremental_delete_ns += elapsedSince(phase_start_ns);

        // Process inserts
        phase_start_ns = nowNs();
        for (writes) |w| {
            try self.processInsert(scratch, txn, w.doc_id, w.vec, w.doc_num, touched_terms_ptr);
            _ = scratch_arena.reset(.retain_capacity);
        }
        self.write_profile.incremental_insert_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        if (touched_terms_ptr) |map| {
            var it = map.iterator();
            while (it.next()) |entry| {
                try self.refreshTermRangeMeta(scratch, txn, entry.key_ptr.*);
                _ = scratch_arena.reset(.retain_capacity);
            }
        }
        self.write_profile.incremental_refresh_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        try persistSparseCounters(self, txn);

        try txn.commit();
        self.write_profile.incremental_commit_ns += elapsedSince(phase_start_ns);
    }

    fn estimateSparseBulkWorkingBytes(writes: []const SparseWrite) u64 {
        var total: u64 = 0;
        for (writes) |write| {
            total +|= write.doc_id.len;
            total +|= @as(u64, @intCast(write.vec.indices.len)) * (@sizeOf(BulkPosting) + @sizeOf(u32) + @sizeOf(f32));
        }
        return total;
    }

    fn tryReserveSparseBulkWorkingSet(self: *SparseIndex, writes: []const SparseWrite) ?resource_manager_mod.Reservation {
        const manager = self.resource_manager orelse return null;
        const estimated = estimateSparseBulkWorkingBytes(writes);
        return manager.reserve(.sparse_apply_working_set, estimated) catch null;
    }

    fn tryBulkAppend(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !bool {
        if (deletes.len != 0) return false;

        var phase_start_ns = nowNs();
        var reservation = self.tryReserveSparseBulkWorkingSet(writes);
        self.write_profile.reserve_ns += elapsedSince(phase_start_ns);
        defer if (reservation) |*held| held.release();

        const bulk_batch_options: backend_types.BatchOptions = if (options.backend_batch_options.mode == .bulk_ingest)
            options.backend_batch_options
        else
            .{ .mode = .bulk_ingest };
        var txn = try self.beginBatchTxn(bulk_batch_options);
        errdefer txn.abort();

        var last_index_by_doc = std.StringHashMapUnmanaged(usize).empty;
        defer last_index_by_doc.deinit(self.alloc);
        phase_start_ns = nowNs();
        try last_index_by_doc.ensureTotalCapacity(self.alloc, @intCast(writes.len));
        for (writes, 0..) |write, i| {
            try last_index_by_doc.put(self.alloc, write.doc_id, i);
        }
        self.write_profile.dedupe_ns += elapsedSince(phase_start_ns);

        var active_indices = std.ArrayListUnmanaged(usize).empty;
        defer active_indices.deinit(self.alloc);
        try active_indices.ensureTotalCapacity(self.alloc, writes.len);

        var posting_count: usize = 0;
        phase_start_ns = nowNs();
        for (writes, 0..) |write, i| {
            if (last_index_by_doc.get(write.doc_id).? != i) continue;
            if (write.vec.indices.len != write.vec.values.len) return error.InvalidSparseVector;
            if (!options.assume_new_doc_ids) {
                var fwd_key_buf: [256]u8 = undefined;
                const existing = txn.get(fwdKey(&fwd_key_buf, write.doc_id)) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (existing != null) {
                    self.write_profile.existence_check_ns += elapsedSince(phase_start_ns);
                    txn.abort();
                    return false;
                }
            }
            try active_indices.append(self.alloc, i);
            posting_count += write.vec.indices.len;
        }
        self.write_profile.existence_check_ns += elapsedSince(phase_start_ns);
        if (active_indices.items.len == 0) {
            phase_start_ns = nowNs();
            try txn.commit();
            self.write_profile.commit_ns += elapsedSince(phase_start_ns);
            self.write_profile.bulk_append_calls += 1;
            return true;
        }

        const prev_next_doc_num = self.next_doc_num;
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.next_doc_num = prev_next_doc_num;
            self.doc_count = prev_doc_count;
            self.term_count = prev_term_count;
        }

        var occupied_doc_nums = std.AutoHashMapUnmanaged(u64, []const u8).empty;
        defer occupied_doc_nums.deinit(self.alloc);
        try occupied_doc_nums.ensureTotalCapacity(self.alloc, @intCast(active_indices.items.len));

        var bulk_docs = std.ArrayListUnmanaged(BulkDoc).empty;
        defer bulk_docs.deinit(self.alloc);
        try bulk_docs.ensureTotalCapacity(self.alloc, active_indices.items.len);

        var postings = std.ArrayListUnmanaged(BulkPosting).empty;
        defer postings.deinit(self.alloc);
        try postings.ensureTotalCapacity(self.alloc, posting_count);

        for (active_indices.items) |write_idx| {
            const write = writes[write_idx];
            phase_start_ns = nowNs();
            const doc_num = try self.allocateBulkDocNum(write.doc_id, write.doc_num, &occupied_doc_nums);
            if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
            self.doc_count += 1;
            self.write_profile.doc_num_ns += elapsedSince(phase_start_ns);

            try bulk_docs.append(self.alloc, .{ .write_idx = write_idx, .doc_num = doc_num });

            const doc_num_u32: u32 = @intCast(doc_num);
            phase_start_ns = nowNs();
            for (write.vec.indices, 0..) |term_id, term_idx| {
                try postings.append(self.alloc, .{
                    .term_id = term_id,
                    .doc_num = doc_num_u32,
                    .weight = write.vec.values[term_idx],
                    .doc_id = write.doc_id,
                });
            }
            self.write_profile.posting_collect_ns += elapsedSince(phase_start_ns);
        }

        const segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        phase_start_ns = nowNs();
        const docmap_data = try encodeDocMapSegment(self.alloc, writes, bulk_docs.items);
        defer self.alloc.free(docmap_data);
        var docmap_key_buf: [16]u8 = undefined;
        try txnAppendPut(&txn, self.dbi, docMapSegmentKey(&docmap_key_buf, segment_id), docmap_data);
        self.write_profile.fwd_rev_put_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        std.mem.sort(BulkPosting, postings.items, {}, struct {
            fn lessThan(_: void, a: BulkPosting, b: BulkPosting) bool {
                if (a.term_id != b.term_id) return a.term_id < b.term_id;
                return a.doc_num < b.doc_num;
            }
        }.lessThan);
        self.write_profile.posting_sort_ns += elapsedSince(phase_start_ns);

        var start: usize = 0;
        var term_groups: u64 = 0;
        phase_start_ns = nowNs();
        while (start < postings.items.len) {
            var end = start + 1;
            while (end < postings.items.len and postings.items[end].term_id == postings.items[start].term_id) : (end += 1) {}
            term_groups += 1;
            if (try self.ensureTermCatalogEntry(&txn, postings.items[start].term_id)) self.term_count += 1;
            start = end;
        }
        if (postings.items.len > 0) {
            const segment_data = try encodeSegmentFromSortedPostings(self.alloc, postings.items, self.chunk_size);
            defer self.alloc.free(segment_data);
            var segment_key_buf: [16]u8 = undefined;
            try txnAppendPut(&txn, self.dbi, segmentKey(&segment_key_buf, segment_id), segment_data);
        }
        self.write_profile.posting_write_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        try persistSparseCounters(self, &txn);
        try txn.commit();
        self.write_profile.commit_ns += elapsedSince(phase_start_ns);
        self.write_profile.bulk_append_calls += 1;
        self.write_profile.postings += posting_count;
        self.write_profile.terms += term_groups;
        return true;
    }

    fn allocateBulkDocNum(
        self: *SparseIndex,
        doc_id: []const u8,
        preferred_doc_num: ?u32,
        occupied_doc_nums: *std.AutoHashMapUnmanaged(u64, []const u8),
    ) !u64 {
        if (preferred_doc_num) |doc_num_u32| {
            const doc_num: u64 = doc_num_u32;
            if (occupied_doc_nums.get(doc_num)) |existing_doc_id| {
                if (std.mem.eql(u8, existing_doc_id, doc_id)) return doc_num;
            } else {
                try occupied_doc_nums.put(self.alloc, doc_num, doc_id);
                if (doc_num >= self.next_doc_num) self.next_doc_num = doc_num + 1;
                return doc_num;
            }
        }

        var doc_num = self.next_doc_num;
        while (occupied_doc_nums.contains(doc_num)) : (doc_num += 1) {}
        self.next_doc_num = doc_num + 1;
        try occupied_doc_nums.put(self.alloc, doc_num, doc_id);
        return doc_num;
    }

    fn ensureTermCatalogEntry(self: *SparseIndex, txn: anytype, term_id: u32) !bool {
        var key_buf: [16]u8 = undefined;
        const key = termCatalogKey(&key_buf, term_id);
        _ = txnGet(txn, self.dbi, key) catch |err| switch (err) {
            error.NotFound => {
                try txnPut(txn, self.dbi, key, &.{});
                return true;
            },
            else => return err,
        };
        return false;
    }

    fn bulkAppendTermPostings(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        postings: []const BulkPosting,
    ) !bool {
        if (postings.len == 0) return false;
        const term_id = postings[0].term_id;
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);
        var chunk_count: u32 = 0;
        var max_weight: f32 = postings[0].weight;
        var created_term = false;
        var term_min_doc_id: ?[]const u8 = null;
        var term_max_doc_id: ?[]const u8 = null;

        var phase_start_ns = nowNs();
        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (meta_data) |data| {
            const tm = decodeTermMeta(data);
            chunk_count = tm.chunk_count;
            max_weight = tm.max_weight;
        } else {
            created_term = true;
        }
        for (postings) |posting| max_weight = @max(max_weight, posting.weight);
        var range_key_buf: [256]u8 = undefined;
        const existing_range_data = txnGet(txn, self.dbi, termRangeKey(&range_key_buf, term_id)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing_range_data) |data| {
            const range = try decodeChunkRangeMeta(data);
            updateBorrowedRangeBounds(&term_min_doc_id, &term_max_doc_id, range.min_doc_id, range.max_doc_id);
        }
        self.write_profile.term_meta_ns += elapsedSince(phase_start_ns);

        var cursor: usize = 0;
        if (chunk_count > 0 and cursor < postings.len) {
            const last_chunk_idx = chunk_count - 1;
            var ck_buf: [256]u8 = undefined;
            phase_start_ns = nowNs();
            const chunk_data = txnGet(txn, self.dbi, invChunkKey(&ck_buf, term_id, last_chunk_idx)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (chunk_data) |data| {
                const decoded = try decodeChunk(alloc, data);
                self.write_profile.chunk_read_ns += elapsedSince(phase_start_ns);
                defer alloc.free(decoded.doc_nums);
                defer alloc.free(decoded.weights);
                if (decoded.doc_nums.len < self.chunk_size) {
                    const available = @as(usize, @intCast(self.chunk_size)) - decoded.doc_nums.len;
                    const take = @min(available, postings.len - cursor);
                    var doc_nums = try alloc.alloc(u32, decoded.doc_nums.len + take);
                    defer alloc.free(doc_nums);
                    var weights = try alloc.alloc(f32, decoded.weights.len + take);
                    defer alloc.free(weights);
                    @memcpy(doc_nums[0..decoded.doc_nums.len], decoded.doc_nums);
                    @memcpy(weights[0..decoded.weights.len], decoded.weights);
                    for (postings[cursor .. cursor + take], 0..) |posting, i| {
                        doc_nums[decoded.doc_nums.len + i] = posting.doc_num;
                        weights[decoded.weights.len + i] = posting.weight;
                    }
                    sortDocNumsAndWeights(doc_nums, weights);
                    var chunk_min_doc_id: ?[]const u8 = null;
                    var chunk_max_doc_id: ?[]const u8 = null;
                    var meta_ck_buf: [256]u8 = undefined;
                    const existing_chunk_range_data = txnGet(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, last_chunk_idx)) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (existing_chunk_range_data) |range_data| {
                        const range = try decodeChunkRangeMeta(range_data);
                        updateBorrowedRangeBounds(&chunk_min_doc_id, &chunk_max_doc_id, range.min_doc_id, range.max_doc_id);
                    }
                    for (postings[cursor .. cursor + take]) |posting| {
                        updateBorrowedRangeBounds(&chunk_min_doc_id, &chunk_max_doc_id, posting.doc_id, posting.doc_id);
                        updateBorrowedRangeBounds(&term_min_doc_id, &term_max_doc_id, posting.doc_id, posting.doc_id);
                    }
                    try self.writeChunkWithRangeMetaProfiled(alloc, txn, term_id, last_chunk_idx, doc_nums, weights, chunk_min_doc_id.?, chunk_max_doc_id.?, &self.write_profile);
                    cursor += take;
                }
            } else {
                self.write_profile.chunk_read_ns += elapsedSince(phase_start_ns);
            }
        }

        while (cursor < postings.len) {
            const take = @min(@as(usize, @intCast(self.chunk_size)), postings.len - cursor);
            var doc_nums = try alloc.alloc(u32, take);
            defer alloc.free(doc_nums);
            var weights = try alloc.alloc(f32, take);
            defer alloc.free(weights);
            var min_doc_id: ?[]const u8 = null;
            var max_doc_id: ?[]const u8 = null;
            for (postings[cursor .. cursor + take], 0..) |posting, i| {
                doc_nums[i] = posting.doc_num;
                weights[i] = posting.weight;
                updateBorrowedRangeBounds(&min_doc_id, &max_doc_id, posting.doc_id, posting.doc_id);
                updateBorrowedRangeBounds(&term_min_doc_id, &term_max_doc_id, posting.doc_id, posting.doc_id);
            }
            try self.writeChunkWithRangeMetaProfiled(alloc, txn, term_id, chunk_count, doc_nums, weights, min_doc_id.?, max_doc_id.?, &self.write_profile);
            chunk_count += 1;
            cursor += take;
        }

        phase_start_ns = nowNs();
        const meta = encodeTermMeta(max_weight, chunk_count);
        try txnAppendPut(txn, self.dbi, mk, &meta);
        if (term_min_doc_id != null and term_max_doc_id != null) {
            try self.writeTermRangeMeta(alloc, txn, term_id, term_min_doc_id.?, term_max_doc_id.?);
        }
        self.write_profile.term_meta_ns += elapsedSince(phase_start_ns);
        return created_term;
    }

    const DeleteEffect = struct {
        deleted_doc: bool = false,
        removed_terms: u64 = 0,
    };

    fn applyDeleteEffect(self: *SparseIndex, effect: DeleteEffect) void {
        if (effect.deleted_doc and self.doc_count > 0) self.doc_count -= 1;
        self.term_count = if (effect.removed_terms > self.term_count) 0 else self.term_count - effect.removed_terms;
    }

    fn processDelete(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_id: []const u8,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !DeleteEffect {
        // Read forward entry
        var key_buf: [256]u8 = undefined;
        const fk = fwdKey(&key_buf, doc_id);
        const fwd = self.lookupFwdEntryByDocIdAlloc(alloc, txn, doc_id) catch |err| switch (err) {
            error.NotFound => return .{}, // doc not found, nothing to delete
            else => return err,
        };
        defer alloc.free(fwd.term_ids);
        defer alloc.free(fwd.weights);

        var effect = DeleteEffect{ .deleted_doc = true };
        // Remove from posting chunks for each term
        for (fwd.term_ids) |term_id| {
            if (try self.removeFromPostings(alloc, txn, term_id, @intCast(fwd.doc_num), touched_terms)) {
                effect.removed_terms += 1;
            }
        }

        // Delete forward and reverse entries
        txnDelete(txn, self.dbi, fk) catch {};
        var rev_buf: [256]u8 = undefined;
        const rk = revKey(&rev_buf, fwd.doc_num);
        txnDelete(txn, self.dbi, rk) catch {};
        var tombstone_buf: [16]u8 = undefined;
        try txnPut(txn, self.dbi, docTombstoneKey(&tombstone_buf, fwd.doc_num), &.{});
        return effect;
    }

    fn processInsert(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_id: []const u8,
        vec: SparseVector,
        preferred_doc_num: ?u32,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !void {
        // If doc_id already exists, delete the old mapping first to avoid orphaning
        self.applyDeleteEffect(try self.processDelete(alloc, txn, doc_id, touched_terms));

        const doc_num = try self.allocateDocNumForInsert(txn, doc_id, preferred_doc_num);
        self.doc_count += 1;
        var tombstone_buf: [16]u8 = undefined;
        txnDelete(txn, self.dbi, docTombstoneKey(&tombstone_buf, doc_num)) catch {};

        // Write forward entry
        const fwd_data = try encodeFwdEntry(alloc, doc_num, vec.indices, vec.values);
        defer alloc.free(fwd_data);
        var fwd_key_buf: [256]u8 = undefined;
        const fk = fwdKey(&fwd_key_buf, doc_id);
        try txnPut(txn, self.dbi, fk, fwd_data);

        // Write reverse entry (docNum → docID)
        var rev_key_buf: [256]u8 = undefined;
        const rk = revKey(&rev_key_buf, doc_num);
        try txnPut(txn, self.dbi, rk, doc_id);

        // Update posting chunks for each term
        if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
        const doc_num_u32: u32 = @intCast(doc_num);
        for (vec.indices, 0..) |term_id, i| {
            if (try self.addToPostings(alloc, txn, term_id, doc_id, doc_num_u32, vec.values[i], touched_terms)) {
                self.term_count += 1;
            }
        }
    }

    fn allocateDocNumForInsert(self: *SparseIndex, txn: anytype, doc_id: []const u8, preferred_doc_num: ?u32) !u64 {
        if (preferred_doc_num) |doc_num_u32| {
            const doc_num: u64 = doc_num_u32;
            var rev_buf: [256]u8 = undefined;
            const existing = txn.get(revKey(&rev_buf, doc_num)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (existing == null or std.mem.eql(u8, existing.?, doc_id)) {
                if (doc_num >= self.next_doc_num) self.next_doc_num = doc_num + 1;
                return doc_num;
            }
        }

        const doc_num = self.next_doc_num;
        self.next_doc_num += 1;
        return doc_num;
    }

    fn addToPostings(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        doc_id: []const u8,
        doc_num: u32,
        weight: f32,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !bool {
        // Read term metadata
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);
        var chunk_count: u32 = 0;
        var max_weight: f32 = weight;
        var created_term = false;

        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (meta_data) |d| {
            const tm = decodeTermMeta(d);
            chunk_count = tm.chunk_count;
            max_weight = @max(tm.max_weight, weight);
        } else {
            created_term = true;
        }

        // Find the right chunk to insert into (last chunk or create new one)
        if (chunk_count == 0) {
            // Create first chunk
            try self.writeChunkWithRangeMeta(alloc, txn, term_id, 0, &.{doc_num}, &.{weight}, doc_id, doc_id);
            chunk_count = 1;
        } else {
            // Append to last chunk
            const last_chunk_idx = chunk_count - 1;
            var ck_buf: [256]u8 = undefined;
            const ck = invChunkKey(&ck_buf, term_id, last_chunk_idx);
            const chunk_data = try txnGet(txn, self.dbi, ck);

            const decoded = try decodeChunk(alloc, chunk_data);
            defer alloc.free(decoded.doc_nums);
            defer alloc.free(decoded.weights);

            if (decoded.doc_nums.len >= self.chunk_size) {
                // Create new chunk
                try self.writeChunkWithRangeMeta(alloc, txn, term_id, chunk_count, &.{doc_num}, &.{weight}, doc_id, doc_id);
                chunk_count += 1;
            } else {
                // Append to existing chunk
                const new_len = decoded.doc_nums.len + 1;
                var new_doc_nums = try alloc.alloc(u32, new_len);
                defer alloc.free(new_doc_nums);
                var new_weights = try alloc.alloc(f32, new_len);
                defer alloc.free(new_weights);

                @memcpy(new_doc_nums[0..decoded.doc_nums.len], decoded.doc_nums);
                new_doc_nums[decoded.doc_nums.len] = doc_num;
                @memcpy(new_weights[0..decoded.weights.len], decoded.weights);
                new_weights[decoded.weights.len] = weight;
                sortDocNumsAndWeights(new_doc_nums, new_weights);

                const existing_range = self.readChunkRangeMeta(txn, term_id, last_chunk_idx) catch null;
                const min_doc_id = if (existing_range) |range|
                    if (std.mem.order(u8, doc_id, range.min_doc_id) == .lt) doc_id else range.min_doc_id
                else
                    doc_id;
                const max_doc_id = if (existing_range) |range|
                    if (std.mem.order(u8, doc_id, range.max_doc_id) == .gt) doc_id else range.max_doc_id
                else
                    doc_id;
                try self.writeChunkWithRangeMeta(alloc, txn, term_id, last_chunk_idx, new_doc_nums, new_weights, min_doc_id, max_doc_id);
            }
        }

        // Update term metadata
        const meta = encodeTermMeta(max_weight, chunk_count);
        try txnPut(txn, self.dbi, mk, &meta);
        if (created_term) _ = try self.ensureTermCatalogEntry(txn, term_id);
        if (touched_terms) |map| {
            try map.put(self.alloc, term_id, {});
        } else {
            try self.updateTermRangeMetaOnInsert(alloc, txn, term_id, doc_id);
        }
        return created_term;
    }

    fn removeFromPostings(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        doc_num: u32,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !bool {
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);

        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const tm = decodeTermMeta(meta_data);

        for (0..tm.chunk_count) |ci| {
            var ck_buf: [256]u8 = undefined;
            const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
            const chunk_data = txnGet(txn, self.dbi, ck) catch continue;

            const decoded = try decodeChunk(alloc, chunk_data);
            defer alloc.free(decoded.doc_nums);
            defer alloc.free(decoded.weights);

            // Find and remove doc_num
            var found: ?usize = null;
            for (decoded.doc_nums, 0..) |dn, i| {
                if (dn == doc_num) {
                    found = i;
                    break;
                }
            }
            if (found) |idx| {
                if (decoded.doc_nums.len == 1) {
                    // Remove empty chunk
                    txnDelete(txn, self.dbi, ck) catch {};
                    var meta_ck_buf: [256]u8 = undefined;
                    txnDelete(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch {};
                    // Update metadata chunk_count
                    const new_meta = encodeTermMeta(tm.max_weight, tm.chunk_count - 1);
                    var term_range_buf: [256]u8 = undefined;
                    if (tm.chunk_count - 1 == 0) {
                        txnDelete(txn, self.dbi, mk) catch {};
                        txnDelete(txn, self.dbi, termRangeKey(&term_range_buf, term_id)) catch {};
                        var catalog_buf: [16]u8 = undefined;
                        txnDelete(txn, self.dbi, termCatalogKey(&catalog_buf, term_id)) catch {};
                        return true;
                    } else {
                        try txnPut(txn, self.dbi, mk, &new_meta);
                        if (touched_terms) |map| {
                            try map.put(self.alloc, term_id, {});
                        } else {
                            try self.recomputeTermRangeMeta(alloc, txn, term_id, tm.chunk_count - 1);
                        }
                    }
                } else {
                    // Remove entry and re-encode
                    const new_len = decoded.doc_nums.len - 1;
                    var new_dns = try alloc.alloc(u32, new_len);
                    defer alloc.free(new_dns);
                    var new_ws = try alloc.alloc(f32, new_len);
                    defer alloc.free(new_ws);

                    var wi: usize = 0;
                    for (0..decoded.doc_nums.len) |i| {
                        if (i != idx) {
                            new_dns[wi] = decoded.doc_nums[i];
                            new_ws[wi] = decoded.weights[i];
                            wi += 1;
                        }
                    }

                    const range = try self.computeChunkRangeMeta(alloc, txn, new_dns);
                    defer {
                        alloc.free(range.min_doc_id);
                        alloc.free(range.max_doc_id);
                    }
                    try self.writeChunkWithRangeMeta(alloc, txn, term_id, @intCast(ci), new_dns, new_ws, range.min_doc_id, range.max_doc_id);
                    if (touched_terms) |map| {
                        try map.put(self.alloc, term_id, {});
                    } else {
                        try self.recomputeTermRangeMeta(alloc, txn, term_id, tm.chunk_count);
                    }
                }
                return false;
            }
        }
        return false;
    }

    fn writeChunkWithRangeMeta(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        chunk_idx: u32,
        doc_nums: []const u32,
        weights: []const f32,
        min_doc_id: []const u8,
        max_doc_id: []const u8,
    ) !void {
        try self.writeChunkWithRangeMetaProfiled(alloc, txn, term_id, chunk_idx, doc_nums, weights, min_doc_id, max_doc_id, null);
    }

    fn writeChunkWithRangeMetaProfiled(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        chunk_idx: u32,
        doc_nums: []const u32,
        weights: []const f32,
        min_doc_id: []const u8,
        max_doc_id: []const u8,
        profile: ?*WriteProfile,
    ) !void {
        var phase_start_ns = nowNs();
        const encoded = try encodeChunk(alloc, doc_nums, weights);
        if (profile) |active_profile| active_profile.chunk_encode_ns += elapsedSince(phase_start_ns);
        defer alloc.free(encoded);
        var ck_buf: [256]u8 = undefined;
        phase_start_ns = nowNs();
        try txnAppendPut(txn, self.dbi, invChunkKey(&ck_buf, term_id, chunk_idx), encoded);
        if (profile) |active_profile| active_profile.chunk_put_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        const meta = try encodeChunkRangeMeta(alloc, min_doc_id, max_doc_id);
        if (profile) |active_profile| active_profile.range_meta_encode_ns += elapsedSince(phase_start_ns);
        defer alloc.free(meta);
        var meta_ck_buf: [256]u8 = undefined;
        phase_start_ns = nowNs();
        try txnAppendPut(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, chunk_idx), meta);
        if (profile) |active_profile| active_profile.range_meta_put_ns += elapsedSince(phase_start_ns);
    }

    fn writeTermRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32, min_doc_id: []const u8, max_doc_id: []const u8) !void {
        const meta = try encodeChunkRangeMeta(alloc, min_doc_id, max_doc_id);
        defer alloc.free(meta);
        var key_buf: [256]u8 = undefined;
        try txnAppendPut(txn, self.dbi, termRangeKey(&key_buf, term_id), meta);
    }

    fn updateTermRangeMetaOnInsert(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32, doc_id: []const u8) !void {
        var key_buf: [256]u8 = undefined;
        const existing = txnGet(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch null;
        if (existing) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            const min_doc_id = if (std.mem.order(u8, doc_id, range.min_doc_id) == .lt) doc_id else range.min_doc_id;
            const max_doc_id = if (std.mem.order(u8, doc_id, range.max_doc_id) == .gt) doc_id else range.max_doc_id;
            try self.writeTermRangeMeta(alloc, txn, term_id, min_doc_id, max_doc_id);
            return;
        }

        try self.writeTermRangeMeta(alloc, txn, term_id, doc_id, doc_id);
    }

    fn recomputeTermRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32, chunk_count: u32) !void {
        var min_doc_id: ?[]const u8 = null;
        var max_doc_id: ?[]const u8 = null;

        for (0..chunk_count) |ci| {
            const range = self.readChunkRangeMeta(txn, term_id, @intCast(ci)) catch continue;
            if (min_doc_id == null or std.mem.order(u8, range.min_doc_id, min_doc_id.?) == .lt) {
                min_doc_id = range.min_doc_id;
            }
            if (max_doc_id == null or std.mem.order(u8, range.max_doc_id, max_doc_id.?) == .gt) {
                max_doc_id = range.max_doc_id;
            }
        }

        var key_buf: [256]u8 = undefined;
        if (min_doc_id == null or max_doc_id == null) {
            txnDelete(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch {};
            return;
        }

        try self.writeTermRangeMeta(alloc, txn, term_id, min_doc_id.?, max_doc_id.?);
    }

    fn refreshTermRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32) !void {
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);
        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => {
                var key_buf: [256]u8 = undefined;
                txnDelete(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch {};
                return;
            },
            else => return err,
        };
        const tm = decodeTermMeta(meta_data);
        if (tm.chunk_count == 0) {
            var key_buf: [256]u8 = undefined;
            txnDelete(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch {};
            return;
        }
        try self.recomputeTermRangeMeta(alloc, txn, term_id, tm.chunk_count);
    }

    fn computeChunkRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, doc_nums: []const u32) !struct { min_doc_id: []u8, max_doc_id: []u8 } {
        std.debug.assert(doc_nums.len > 0);
        var min_doc_id: ?[]u8 = null;
        var max_doc_id: ?[]u8 = null;
        errdefer {
            if (min_doc_id) |doc_id| alloc.free(doc_id);
            if (max_doc_id) |doc_id| alloc.free(doc_id);
        }

        for (doc_nums) |doc_num| {
            const doc_id = try self.resolveDocIdByDocNum(alloc, txn, doc_num);
            defer alloc.free(doc_id);
            if (min_doc_id == null or std.mem.order(u8, doc_id, min_doc_id.?) == .lt) {
                if (min_doc_id) |existing| alloc.free(existing);
                min_doc_id = try alloc.dupe(u8, doc_id);
            }
            if (max_doc_id == null or std.mem.order(u8, doc_id, max_doc_id.?) == .gt) {
                if (max_doc_id) |existing| alloc.free(existing);
                max_doc_id = try alloc.dupe(u8, doc_id);
            }
        }

        return .{
            .min_doc_id = min_doc_id.?,
            .max_doc_id = max_doc_id.?,
        };
    }

    fn resolveDocIdByDocNum(self: *SparseIndex, alloc: Allocator, txn: anytype, doc_num: u32) ![]u8 {
        if (self.docNumDeleted(txn, doc_num)) return error.NotFound;
        var rev_buf: [256]u8 = undefined;
        if (txnGet(txn, self.dbi, revKey(&rev_buf, doc_num))) |doc_id| {
            return alloc.dupe(u8, doc_id);
        } else |_| {}

        const LookupContext = struct {
            wanted_doc_num: u64,
            found: ?[]const u8 = null,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (entry.doc_num == ctx.wanted_doc_num) {
                    ctx.found = entry.doc_id;
                    return true;
                }
                return false;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{ .wanted_doc_num = doc_num };
            if (try forEachDocMapEntry(entry.value, &ctx, LookupContext.visit)) {
                return alloc.dupe(u8, ctx.found.?);
            }
            maybe_entry = try cur.next();
        }
        return error.NotFound;
    }

    fn docNumDeleted(self: *SparseIndex, txn: anytype, doc_num: u64) bool {
        _ = self;
        var tombstone_buf: [16]u8 = undefined;
        _ = txnGet(txn, {}, docTombstoneKey(&tombstone_buf, doc_num)) catch return false;
        return true;
    }

    fn resolveSearchCandidateDocIds(self: *SparseIndex, alloc: Allocator, txn: anytype, candidates: []SearchCandidate) !void {
        var wanted = std.AutoHashMapUnmanaged(u32, usize).empty;
        defer wanted.deinit(alloc);

        for (candidates, 0..) |candidate, i| {
            if (self.docNumDeleted(txn, candidate.doc_num)) continue;
            try wanted.put(alloc, candidate.doc_num, i);
        }
        if (wanted.count() == 0) return;

        const LookupContext = struct {
            alloc: Allocator,
            wanted: *std.AutoHashMapUnmanaged(u32, usize),
            candidates: []SearchCandidate,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (entry.doc_num > std.math.maxInt(u32)) return false;
                const doc_num: u32 = @intCast(entry.doc_num);
                const idx = ctx.wanted.get(doc_num) orelse return false;
                if (ctx.candidates[idx].doc_id == null) {
                    ctx.candidates[idx].doc_id = try ctx.alloc.dupe(u8, entry.doc_id);
                }
                _ = ctx.wanted.remove(doc_num);
                return ctx.wanted.count() == 0;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{
                .alloc = alloc,
                .wanted = &wanted,
                .candidates = candidates,
            };
            if (try forEachDocMapEntry(entry.value, &ctx, LookupContext.visit)) return;
            maybe_entry = try cur.next();
        }

        for (candidates) |*candidate| {
            if (candidate.doc_id != null) continue;
            if (!wanted.contains(candidate.doc_num)) continue;
            var rev_buf: [256]u8 = undefined;
            const doc_id = txnGet(txn, self.dbi, revKey(&rev_buf, candidate.doc_num)) catch continue;
            candidate.doc_id = try alloc.dupe(u8, doc_id);
        }
    }

    fn lookupDocNumByDocId(self: *SparseIndex, txn: anytype, doc_id: []const u8) !u64 {
        var key_buf: [256]u8 = undefined;
        if (txnGet(txn, self.dbi, fwdKey(&key_buf, doc_id))) |fwd_data| {
            const doc_num = try decodeFwdDocNum(fwd_data);
            if (self.docNumDeleted(txn, doc_num)) return error.NotFound;
            return doc_num;
        } else |_| {}

        const LookupContext = struct {
            wanted_doc_id: []const u8,
            index: *SparseIndex,
            txn: @TypeOf(txn),
            found: ?u64 = null,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (std.mem.eql(u8, entry.doc_id, ctx.wanted_doc_id)) {
                    if (ctx.index.docNumDeleted(ctx.txn, entry.doc_num)) return false;
                    ctx.found = entry.doc_num;
                    return true;
                }
                return false;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{ .wanted_doc_id = doc_id, .index = self, .txn = txn };
            if (try forEachDocMapEntry(entry.value, &ctx, LookupContext.visit)) return ctx.found.?;
            maybe_entry = try cur.next();
        }
        return error.NotFound;
    }

    fn lookupFwdEntryByDocIdAlloc(self: *SparseIndex, alloc: Allocator, txn: anytype, doc_id: []const u8) !DecodedFwdEntry {
        var key_buf: [256]u8 = undefined;
        if (txnGet(txn, self.dbi, fwdKey(&key_buf, doc_id))) |fwd_data| {
            const doc_num = try decodeFwdDocNum(fwd_data);
            if (self.docNumDeleted(txn, doc_num)) return error.NotFound;
            return try decodeFwdEntry(alloc, fwd_data);
        } else |_| {}

        const LookupContext = struct {
            alloc: Allocator,
            wanted_doc_id: []const u8,
            index: *SparseIndex,
            txn: @TypeOf(txn),
            found: ?DecodedFwdEntry = null,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (std.mem.eql(u8, entry.doc_id, ctx.wanted_doc_id)) {
                    if (ctx.index.docNumDeleted(ctx.txn, entry.doc_num)) return false;
                    ctx.found = try decodeFwdEntry(ctx.alloc, entry.fwd_data);
                    return true;
                }
                return false;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{ .alloc = alloc, .wanted_doc_id = doc_id, .index = self, .txn = txn };
            if (try forEachDocMapEntry(entry.value, &ctx, LookupContext.visit)) return ctx.found.?;
            maybe_entry = try cur.next();
        }
        return error.NotFound;
    }

    fn readChunkRangeMeta(self: *SparseIndex, txn: anytype, term_id: u32, chunk_idx: u32) !ChunkRangeMeta {
        var meta_ck_buf: [256]u8 = undefined;
        const data = try txnGet(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, chunk_idx));
        return decodeChunkRangeMeta(data);
    }

    /// Search for top-k documents matching a sparse query vector.
    /// DAAT: accumulate scores per doc, then extract top-k.
    pub fn search(self: *SparseIndex, alloc: Allocator, query_vec: *const SparseVector, k: u32) ![]SearchResult {
        return try self.searchConstrained(alloc, query_vec, k, .{});
    }

    pub fn searchConstrained(
        self: *SparseIndex,
        alloc: Allocator,
        query_vec: *const SparseVector,
        k: u32,
        constraints: SearchConstraints,
    ) ![]SearchResult {
        const profile_enabled = sparseSearchProfileEnabled();
        const total_start_ns = if (profile_enabled) nowNs() else 0;
        var profile: SearchProfile = .{};
        var txn = try self.beginReadTxn();
        defer txn.abort();

        const filter_start_ns = if (profile_enabled) nowNs() else 0;
        var filter_doc_nums = try self.resolveDocNumSetAlloc(alloc, &txn, constraints.filter_doc_ids);
        defer filter_doc_nums.deinit(alloc);
        if (constraints.filter_doc_ids.len > 0 and filter_doc_nums.count() == 0) {
            return try alloc.alloc(SearchResult, 0);
        }
        var direct_filter_doc_nums = try self.docNumSetFromSliceAlloc(alloc, constraints.filter_doc_nums);
        defer direct_filter_doc_nums.deinit(alloc);
        if (constraints.filter_doc_nums.len > 0 and direct_filter_doc_nums.count() == 0) {
            return try alloc.alloc(SearchResult, 0);
        }

        var exclude_doc_nums = try self.resolveDocNumSetAlloc(alloc, &txn, constraints.exclude_doc_ids);
        defer exclude_doc_nums.deinit(alloc);
        var direct_exclude_doc_nums = try self.docNumSetFromSliceAlloc(alloc, constraints.exclude_doc_nums);
        defer direct_exclude_doc_nums.deinit(alloc);
        if (profile_enabled) profile.filter_resolve_ns = nowNs() - filter_start_ns;

        // Accumulate scores: docNum → score
        var scores = std.AutoHashMapUnmanaged(u32, f32).empty;
        defer scores.deinit(alloc);

        const ScoreSource = enum { segment, delta };
        const AccumulateContext = struct {
            alloc: Allocator,
            query_weight: f32,
            scores: *std.AutoHashMapUnmanaged(u32, f32),
            filter_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            direct_filter_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            exclude_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            direct_exclude_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            profile: ?*SearchProfile,
            source: ScoreSource,

            fn visit(ctx: *@This(), decoded: DecodedChunk) !void {
                const collect_start_ns = if (ctx.profile != null) nowNs() else 0;
                for (decoded.doc_nums, 0..) |doc_num, di| {
                    if (ctx.filter_doc_nums.count() > 0 and !ctx.filter_doc_nums.contains(doc_num)) continue;
                    if (ctx.direct_filter_doc_nums.count() > 0 and !ctx.direct_filter_doc_nums.contains(doc_num)) continue;
                    if (ctx.exclude_doc_nums.contains(doc_num)) continue;
                    if (ctx.direct_exclude_doc_nums.contains(doc_num)) continue;
                    const doc_weight = decoded.weights[di];
                    const gop = try ctx.scores.getOrPut(ctx.alloc, doc_num);
                    if (!gop.found_existing) gop.value_ptr.* = 0;
                    gop.value_ptr.* += ctx.query_weight * doc_weight;
                }
                if (ctx.profile) |p| {
                    p.score_collect_ns += nowNs() - collect_start_ns;
                    switch (ctx.source) {
                        .segment => p.segment_chunks += 1,
                        .delta => p.delta_chunks += 1,
                    }
                }
            }
        };

        for (query_vec.indices, 0..) |term_id, qi| {
            if (profile_enabled) profile.terms += 1;
            const query_weight = query_vec.values[qi];
            const segment_seek_start_ns = if (profile_enabled) nowNs() else 0;
            var segment_cur = try txn.openCursor();
            defer segment_cur.close();
            var maybe_segment = try segment_cur.seekAtOrAfter(taggedPrefix(key_segment));
            if (profile_enabled) profile.segment_seek_ns += nowNs() - segment_seek_start_ns;
            while (maybe_segment) |segment_entry| {
                if (segment_entry.key.len == 0 or segment_entry.key[0] != key_segment) break;
                if (profile_enabled) profile.segment_entries += 1;
                var ctx = AccumulateContext{
                    .alloc = alloc,
                    .query_weight = query_weight,
                    .scores = &scores,
                    .filter_doc_nums = &filter_doc_nums,
                    .direct_filter_doc_nums = &direct_filter_doc_nums,
                    .exclude_doc_nums = &exclude_doc_nums,
                    .direct_exclude_doc_nums = &direct_exclude_doc_nums,
                    .profile = if (profile_enabled) &profile else null,
                    .source = .segment,
                };
                const segment_decode_start_ns = if (profile_enabled) nowNs() else 0;
                try forEachSegmentChunk(alloc, segment_entry.value, term_id, &ctx, AccumulateContext.visit);
                if (profile_enabled) profile.segment_decode_ns += nowNs() - segment_decode_start_ns;
                const segment_next_start_ns = if (profile_enabled) nowNs() else 0;
                maybe_segment = try segment_cur.next();
                if (profile_enabled) profile.segment_seek_ns += nowNs() - segment_next_start_ns;
            }

            // Check term metadata
            var meta_key_buf: [256]u8 = undefined;
            const mk = invMetaKey(&meta_key_buf, term_id);
            const meta_data = txn.get(mk) catch continue;
            const tm = decodeTermMeta(meta_data);

            // Scan all chunks for this term
            for (0..tm.chunk_count) |ci| {
                var ck_buf: [256]u8 = undefined;
                const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
                const chunk_data = txn.get(ck) catch continue;

                const delta_start_ns = if (profile_enabled) nowNs() else 0;
                const decoded = try decodeChunk(alloc, chunk_data);
                defer alloc.free(decoded.doc_nums);
                defer alloc.free(decoded.weights);

                var ctx = AccumulateContext{
                    .alloc = alloc,
                    .query_weight = query_weight,
                    .scores = &scores,
                    .filter_doc_nums = &filter_doc_nums,
                    .direct_filter_doc_nums = &direct_filter_doc_nums,
                    .exclude_doc_nums = &exclude_doc_nums,
                    .direct_exclude_doc_nums = &direct_exclude_doc_nums,
                    .profile = if (profile_enabled) &profile else null,
                    .source = .delta,
                };
                try AccumulateContext.visit(&ctx, decoded);
                if (profile_enabled) profile.delta_chunk_ns += nowNs() - delta_start_ns;
            }
        }

        // Extract top-k using a simple sort (fine for reasonable result sizes)
        const ScoreEntry = struct { doc_num: u32, score: f32 };
        var entries = std.ArrayListUnmanaged(ScoreEntry).empty;
        defer entries.deinit(alloc);

        var it = scores.iterator();
        while (it.next()) |e| {
            try entries.append(alloc, .{ .doc_num = e.key_ptr.*, .score = e.value_ptr.* });
        }
        if (profile_enabled) profile.scored_docs = entries.items.len;

        const sort_start_ns = if (profile_enabled) nowNs() else 0;
        std.mem.sort(ScoreEntry, entries.items, {}, struct {
            fn cmp(_: void, a: ScoreEntry, b: ScoreEntry) bool {
                return a.score > b.score; // descending
            }
        }.cmp);
        if (profile_enabled) profile.sort_ns = nowNs() - sort_start_ns;

        const n = @min(k, @as(u32, @intCast(entries.items.len)));
        if (n == 0) return try alloc.alloc(SearchResult, 0);

        // Resolve docNums to docIDs
        var results = try alloc.alloc(SearchResult, n);
        errdefer alloc.free(results);
        var valid: usize = 0;
        errdefer {
            for (results[0..valid]) |result| alloc.free(result.doc_id);
        }

        const candidate_batch_size = @max(@as(usize, 256), @as(usize, n) * 8);
        var cursor: usize = 0;
        const hydrate_start_ns = if (profile_enabled) nowNs() else 0;
        while (cursor < entries.items.len and valid < n) {
            const batch_len = @min(candidate_batch_size, entries.items.len - cursor);
            var candidates = try alloc.alloc(SearchCandidate, batch_len);
            defer {
                for (candidates) |candidate| {
                    if (candidate.doc_id) |doc_id| alloc.free(doc_id);
                }
                alloc.free(candidates);
            }

            for (entries.items[cursor .. cursor + batch_len], 0..) |entry, i| {
                candidates[i] = .{
                    .doc_num = entry.doc_num,
                    .score = entry.score,
                };
            }
            try self.resolveSearchCandidateDocIds(alloc, &txn, candidates);

            for (candidates) |*candidate| {
                if (valid >= n) break;
                const doc_id = candidate.doc_id orelse continue;
                candidate.doc_id = null;
                results[valid] = .{
                    .doc_id = doc_id,
                    .doc_num = candidate.doc_num,
                    .score = candidate.score,
                };
                valid += 1;
            }
            cursor += batch_len;
        }
        if (profile_enabled) {
            profile.hydrate_ns = nowNs() - hydrate_start_ns;
            profile.results = valid;
        }

        if (valid < n) {
            // Shrink if some doc nums couldn't be resolved
            return try alloc.realloc(results, valid);
        }
        if (profile_enabled) {
            std.log.info(
                "antfly_bench_sparse_search k={d} terms={d} results={d} scored_docs={d} total_ms={d} filter_ms={d} segment_seek_ms={d} segment_decode_ms={d} delta_chunk_ms={d} score_collect_ms={d} sort_ms={d} hydrate_ms={d} segment_entries={d} segment_chunks={d} delta_chunks={d}",
                .{
                    k,
                    profile.terms,
                    profile.results,
                    profile.scored_docs,
                    (nowNs() - total_start_ns) / std.time.ns_per_ms,
                    profile.filter_resolve_ns / std.time.ns_per_ms,
                    profile.segment_seek_ns / std.time.ns_per_ms,
                    profile.segment_decode_ns / std.time.ns_per_ms,
                    profile.delta_chunk_ns / std.time.ns_per_ms,
                    profile.score_collect_ns / std.time.ns_per_ms,
                    profile.sort_ns / std.time.ns_per_ms,
                    profile.hydrate_ns / std.time.ns_per_ms,
                    profile.segment_entries,
                    profile.segment_chunks,
                    profile.delta_chunks,
                },
            );
        }
        return results;
    }

    fn resolveDocNumSetAlloc(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_ids: []const []const u8,
    ) !std.AutoHashMapUnmanaged(u32, void) {
        var out = std.AutoHashMapUnmanaged(u32, void).empty;
        errdefer out.deinit(alloc);
        for (doc_ids) |doc_id| {
            const doc_num = self.lookupDocNumByDocId(txn, doc_id) catch continue;
            if (doc_num > std.math.maxInt(u32)) continue;
            try out.put(alloc, @intCast(doc_num), {});
        }
        return out;
    }

    fn docNumSetFromSliceAlloc(
        self: *SparseIndex,
        alloc: Allocator,
        doc_nums: []const u32,
    ) !std.AutoHashMapUnmanaged(u32, void) {
        _ = self;
        var out = std.AutoHashMapUnmanaged(u32, void).empty;
        errdefer out.deinit(alloc);
        for (doc_nums) |doc_num| try out.put(alloc, doc_num, {});
        return out;
    }

    pub fn debugDocNumForDocId(self: *SparseIndex, doc_id: []const u8) !?u32 {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        const doc_num = self.lookupDocNumByDocId(&txn, doc_id) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
        return @intCast(doc_num);
    }

    /// Free search results.
    pub fn freeResults(alloc: Allocator, results: []SearchResult) void {
        for (results) |r| alloc.free(r.doc_id);
        alloc.free(results);
    }

    pub fn rebuildRangeInto(self: *SparseIndex, dest: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !SplitRebuildResult {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        var writes = std.ArrayListUnmanaged(SparseWrite).empty;
        defer {
            for (writes.items) |write| {
                alloc.free(@constCast(write.doc_id));
                alloc.free(@constCast(write.vec.indices));
                alloc.free(@constCast(write.vec.values));
            }
            writes.deinit(alloc);
        }

        var doc_ids = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (doc_ids.items) |doc_id| alloc.free(doc_id);
            doc_ids.deinit(alloc);
        }

        const first = (try cur.seekAtOrAfter(taggedPrefix(key_fwd))) orelse return .{ .doc_ids = try doc_ids.toOwnedSlice(alloc) };

        var entry = first;
        while (true) {
            const doc_id = fwdDocIdFromKey(entry.key) orelse break;
            if (std.mem.order(u8, doc_id, lower) != .lt and (upper.len == 0 or std.mem.order(u8, doc_id, upper) == .lt)) {
                const decoded = try decodeFwdEntry(alloc, entry.value);
                defer alloc.free(decoded.term_ids);
                defer alloc.free(decoded.weights);

                const owned_doc_id = try alloc.dupe(u8, doc_id);
                errdefer alloc.free(owned_doc_id);
                const owned_indices = try alloc.dupe(u32, decoded.term_ids);
                errdefer alloc.free(owned_indices);
                const owned_values = try alloc.dupe(f32, decoded.weights);
                errdefer alloc.free(owned_values);

                try writes.append(alloc, .{
                    .doc_id = owned_doc_id,
                    .vec = .{
                        .indices = owned_indices,
                        .values = owned_values,
                    },
                });
                try doc_ids.append(alloc, try alloc.dupe(u8, doc_id));
            }

            entry = (try cur.next()) orelse break;
        }

        if (writes.items.len > 0) {
            const max_sparse_writes_per_txn = 16;
            var start: usize = 0;
            while (start < writes.items.len) {
                const end = @min(start + max_sparse_writes_per_txn, writes.items.len);
                try dest.batch(writes.items[start..end], &.{});
                start = end;
            }
        }
        return .{ .doc_ids = try doc_ids.toOwnedSlice(alloc) };
    }

    pub fn handoffRangeInto(self: *SparseIndex, dest: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8, collect_doc_ids: bool) !SplitRebuildResult {
        var src_txn = try self.beginReadTxn();
        defer src_txn.abort();

        var src_cur = try src_txn.openCursor();
        defer src_cur.close();

        var dest_txn = try dest.beginWriteTxn();
        errdefer dest_txn.abort();
        const prev_dest_next_doc_num = dest.next_doc_num;
        const prev_dest_doc_count = dest.doc_count;
        const prev_dest_term_count = dest.term_count;
        errdefer {
            dest.next_doc_num = prev_dest_next_doc_num;
            dest.doc_count = prev_dest_doc_count;
            dest.term_count = prev_dest_term_count;
        }

        var doc_ids = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (doc_ids.items) |doc_id| alloc.free(doc_id);
            doc_ids.deinit(alloc);
        }
        var selected_docs = std.AutoHashMapUnmanaged(u32, []u8).empty;
        defer {
            var selected_it = selected_docs.iterator();
            while (selected_it.next()) |selected| alloc.free(selected.value_ptr.*);
            selected_docs.deinit(alloc);
        }
        var touched_terms = std.ArrayListUnmanaged(u32).empty;
        defer touched_terms.deinit(alloc);
        const TouchedTermsContext = struct {
            alloc: Allocator,
            terms: *std.ArrayListUnmanaged(u32),

            fn visit(ctx: *@This(), term_id: u32) !void {
                try ctx.terms.append(ctx.alloc, term_id);
            }
        };
        var touched_terms_ctx = TouchedTermsContext{ .alloc = alloc, .terms = &touched_terms };

        var max_doc_num: u64 = dest.next_doc_num;
        const select_started = nowNs();

        const first = if (lower.len == 0)
            (try src_cur.seekAtOrAfter(taggedPrefix(key_fwd))) orelse {
                try persistSparseCounters(dest, &dest_txn);
                try dest_txn.commit();
                return .{
                    .doc_ids = try doc_ids.toOwnedSlice(alloc),
                    .select_docs_ns = elapsedSince(select_started),
                };
            }
        else blk: {
            const start_key = try fwdKeyAlloc(alloc, lower);
            defer alloc.free(start_key);
            break :blk (try src_cur.seekAtOrAfter(start_key)) orelse {
                try persistSparseCounters(dest, &dest_txn);
                try dest_txn.commit();
                return .{
                    .doc_ids = try doc_ids.toOwnedSlice(alloc),
                    .select_docs_ns = elapsedSince(select_started),
                };
            };
        };

        var entry = first;
        while (true) {
            const doc_id = fwdDocIdFromKey(entry.key) orelse break;
            if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) break;
            if (std.mem.order(u8, doc_id, lower) != .lt and (upper.len == 0 or std.mem.order(u8, doc_id, upper) == .lt)) {
                const doc_num = try forEachFwdTermId(entry.value, TouchedTermsContext, &touched_terms_ctx, TouchedTermsContext.visit);
                if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
                const owned_doc_id = try alloc.dupe(u8, doc_id);
                errdefer alloc.free(owned_doc_id);
                const gop = try selected_docs.getOrPut(alloc, @intCast(doc_num));
                if (gop.found_existing) {
                    alloc.free(owned_doc_id);
                } else {
                    gop.value_ptr.* = owned_doc_id;
                }
                if (collect_doc_ids) {
                    try doc_ids.append(alloc, try alloc.dupe(u8, doc_id));
                }

                try txnPut(&dest_txn, dest.dbi, entry.key, entry.value);
                var rev_buf: [256]u8 = undefined;
                const rk = revKey(&rev_buf, doc_num);
                try txnPut(&dest_txn, dest.dbi, rk, gop.value_ptr.*);
                max_doc_num = @max(max_doc_num, doc_num + 1);
                dest.doc_count += 1;
            }

            entry = (try src_cur.next()) orelse break;
        }

        const select_docs_ns = elapsedSince(select_started);
        const terms_started = nowNs();
        if (touched_terms.items.len > 0) {
            var selected_lookup = try SelectedDocLookup.init(alloc, &selected_docs);
            defer selected_lookup.deinit(alloc);
            const term_ids = sortAndDedupU32(touched_terms.items);

            for (term_ids) |term_id| {
                var range_key_buf: [256]u8 = undefined;
                const range_data = txnGet(&src_txn, self.dbi, termRangeKey(&range_key_buf, term_id)) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                const range = try decodeChunkRangeMeta(range_data);
                switch (classifyChunkRange(range, lower, upper)) {
                    .outside => {},
                    .right_only => try handoffWholeTermPostings(&src_txn, self.dbi, &dest_txn, dest.dbi, term_id, range_data),
                    .mixed => try handoffTermPostings(alloc, &src_txn, self.dbi, &dest_txn, dest.dbi, term_id, lower, upper, &selected_lookup),
                }
            }
        }
        const terms_ns = elapsedSince(terms_started);

        dest.next_doc_num = @max(dest.next_doc_num, max_doc_num);
        const commit_started = nowNs();
        const stats_after = try scanStatsInTxn(&dest_txn, dest.dbi);
        dest.doc_count = stats_after.doc_count;
        dest.term_count = stats_after.term_count;
        try persistSparseCounters(dest, &dest_txn);
        try dest_txn.commit();
        const commit_ns = elapsedSince(commit_started);

        return .{
            .doc_ids = try doc_ids.toOwnedSlice(alloc),
            .select_docs_ns = select_docs_ns,
            .terms_ns = terms_ns,
            .commit_ns = commit_ns,
        };
    }

    pub fn handoffPreparedDocIdsInto(self: *SparseIndex, dest: *SparseIndex, alloc: Allocator, doc_ids_in: []const []const u8, lower: []const u8, upper: []const u8, collect_doc_ids: bool) !SplitRebuildResult {
        var src_txn = try self.beginReadTxn();
        defer src_txn.abort();

        var dest_txn = try dest.beginWriteTxn();
        errdefer dest_txn.abort();
        const prev_dest_next_doc_num = dest.next_doc_num;
        const prev_dest_doc_count = dest.doc_count;
        const prev_dest_term_count = dest.term_count;
        errdefer {
            dest.next_doc_num = prev_dest_next_doc_num;
            dest.doc_count = prev_dest_doc_count;
            dest.term_count = prev_dest_term_count;
        }

        var doc_ids = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (doc_ids.items) |doc_id| alloc.free(doc_id);
            doc_ids.deinit(alloc);
        }
        var selected_doc_nums = std.ArrayListUnmanaged(u32).empty;
        defer selected_doc_nums.deinit(alloc);
        var selected_doc_ids = std.ArrayListUnmanaged([]const u8).empty;
        defer selected_doc_ids.deinit(alloc);
        var touched_terms = std.ArrayListUnmanaged(u32).empty;
        defer touched_terms.deinit(alloc);
        const TouchedTermsContext = struct {
            alloc: Allocator,
            terms: *std.ArrayListUnmanaged(u32),

            fn visit(ctx: *@This(), term_id: u32) !void {
                try ctx.terms.append(ctx.alloc, term_id);
            }
        };
        var touched_terms_ctx = TouchedTermsContext{ .alloc = alloc, .terms = &touched_terms };

        var max_doc_num: u64 = dest.next_doc_num;
        const select_started = nowNs();
        var key_buf = std.ArrayListUnmanaged(u8).empty;
        defer key_buf.deinit(alloc);

        for (doc_ids_in) |doc_id| {
            key_buf.clearRetainingCapacity();
            try key_buf.append(alloc, key_fwd);
            try key_buf.appendSlice(alloc, doc_id);
            const fwd_value = txnGet(&src_txn, self.dbi, key_buf.items) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };

            const doc_num = try forEachFwdTermId(fwd_value, TouchedTermsContext, &touched_terms_ctx, TouchedTermsContext.visit);
            if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
            try selected_doc_nums.append(alloc, @intCast(doc_num));
            try selected_doc_ids.append(alloc, doc_id);
            if (collect_doc_ids) {
                try doc_ids.append(alloc, try alloc.dupe(u8, doc_id));
            }

            try txnPut(&dest_txn, dest.dbi, key_buf.items, fwd_value);
            var rev_buf: [256]u8 = undefined;
            const rk = revKey(&rev_buf, doc_num);
            try txnPut(&dest_txn, dest.dbi, rk, doc_id);
            max_doc_num = @max(max_doc_num, doc_num + 1);
        }

        const select_docs_ns = elapsedSince(select_started);
        const terms_started = nowNs();
        if (touched_terms.items.len > 0) {
            const selected_doc_ids_const: []const []const u8 = @ptrCast(selected_doc_ids.items);
            var selected_lookup = try SelectedDocLookup.initFromPairs(alloc, selected_doc_nums.items, selected_doc_ids_const);
            defer selected_lookup.deinit(alloc);
            const term_ids = sortAndDedupU32(touched_terms.items);

            for (term_ids) |term_id| {
                var range_key_buf: [256]u8 = undefined;
                const range_data = txnGet(&src_txn, self.dbi, termRangeKey(&range_key_buf, term_id)) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                const range = try decodeChunkRangeMeta(range_data);
                switch (classifyChunkRange(range, lower, upper)) {
                    .outside => {},
                    .right_only => try handoffWholeTermPostings(&src_txn, self.dbi, &dest_txn, dest.dbi, term_id, range_data),
                    .mixed => try handoffTermPostings(alloc, &src_txn, self.dbi, &dest_txn, dest.dbi, term_id, lower, upper, &selected_lookup),
                }
            }
        }
        const terms_ns = elapsedSince(terms_started);

        dest.next_doc_num = @max(dest.next_doc_num, max_doc_num);
        const commit_started = nowNs();
        const stats_after = try scanStatsInTxn(&dest_txn, dest.dbi);
        dest.doc_count = stats_after.doc_count;
        dest.term_count = stats_after.term_count;
        try persistSparseCounters(dest, &dest_txn);
        try dest_txn.commit();
        const commit_ns = elapsedSince(commit_started);

        return .{
            .doc_ids = try doc_ids.toOwnedSlice(alloc),
            .select_docs_ns = select_docs_ns,
            .terms_ns = terms_ns,
            .commit_ns = commit_ns,
        };
    }

    pub fn splitPlanningStats(self: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !SplitPlanningStats {
        var src_txn = try self.beginReadTxn();
        defer src_txn.abort();

        var src_cur = try src_txn.openCursor();
        defer src_cur.close();

        var selected_doc_nums = std.AutoHashMapUnmanaged(u32, void).empty;
        defer selected_doc_nums.deinit(alloc);

        const first = if (lower.len == 0)
            (try src_cur.seekAtOrAfter(taggedPrefix(key_fwd))) orelse return .{}
        else blk: {
            const start_key = try fwdKeyAlloc(alloc, lower);
            defer alloc.free(start_key);
            break :blk (try src_cur.seekAtOrAfter(start_key)) orelse return .{};
        };

        var entry = first;
        while (true) {
            const doc_id = fwdDocIdFromKey(entry.key) orelse break;
            if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) break;
            if (std.mem.order(u8, doc_id, lower) != .lt and (upper.len == 0 or std.mem.order(u8, doc_id, upper) == .lt)) {
                const doc_num = try decodeFwdDocNum(entry.value);
                if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
                try selected_doc_nums.put(alloc, @intCast(doc_num), {});
            }

            entry = (try src_cur.next()) orelse break;
        }

        var out: SplitPlanningStats = .{
            .selected_docs = selected_doc_nums.size,
        };

        const first_term = (try src_cur.seekAtOrAfter(taggedPrefix(key_inv))) orelse return out;
        entry = first_term;
        while (true) {
            if (entry.key.len == 0 or entry.key[0] != key_inv) break;
            const term_id = parseTermRangeKey(entry.key) orelse {
                entry = (try src_cur.next()) orelse break;
                continue;
            };
            const range = try decodeChunkRangeMeta(entry.value);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => {},
                .right_only, .mixed => {
                    out.touched_terms += 1;
                    try accumulateSplitPlanningStats(alloc, &src_txn, self.dbi, term_id, lower, upper, &selected_doc_nums, &out);
                },
            }
            entry = (try src_cur.next()) orelse break;
        }

        return out;
    }

    pub fn pruneRange(self: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !void {
        var read_txn = try self.beginReadTxn();
        defer read_txn.abort();

        var cur = try read_txn.openCursor();
        defer cur.close();

        const PrunedDoc = struct {
            doc_num: u64,
            doc_id: []u8,

            fn deinit(doc: *@This(), allocator: Allocator) void {
                allocator.free(doc.doc_id);
                doc.* = undefined;
            }
        };

        var term_ids = std.ArrayListUnmanaged(u32).empty;
        defer term_ids.deinit(alloc);
        var pruned_docs = std.ArrayListUnmanaged(PrunedDoc).empty;
        defer {
            for (pruned_docs.items) |*doc| doc.deinit(alloc);
            pruned_docs.deinit(alloc);
        }
        var seen_doc_nums = std.AutoHashMapUnmanaged(u64, void).empty;
        defer seen_doc_nums.deinit(alloc);

        const CollectTermsContext = struct {
            alloc: Allocator,
            terms: *std.ArrayListUnmanaged(u32),

            fn visit(ctx: *@This(), term_id: u32) !void {
                try ctx.terms.append(ctx.alloc, term_id);
            }
        };
        var collect_terms_ctx = CollectTermsContext{ .alloc = alloc, .terms = &term_ids };

        const appendPrunedDoc = struct {
            fn run(
                allocator: Allocator,
                docs: *std.ArrayListUnmanaged(PrunedDoc),
                seen: *std.AutoHashMapUnmanaged(u64, void),
                doc_num: u64,
                doc_id: []const u8,
            ) !void {
                const gop = try seen.getOrPut(allocator, doc_num);
                if (gop.found_existing) return;
                errdefer _ = seen.remove(doc_num);
                try docs.append(allocator, .{
                    .doc_num = doc_num,
                    .doc_id = try allocator.dupe(u8, doc_id),
                });
            }
        }.run;

        const fwd_first = if (lower.len == 0)
            (try cur.seekAtOrAfter(taggedPrefix(key_fwd)))
        else blk: {
            const start_key = try fwdKeyAlloc(alloc, lower);
            defer alloc.free(start_key);
            break :blk try cur.seekAtOrAfter(start_key);
        };
        var maybe_fwd_entry = fwd_first;
        while (maybe_fwd_entry) |entry| {
            const doc_id = fwdDocIdFromKey(entry.key) orelse break;
            if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) break;
            if (docIdInOwnedRange(doc_id, lower, upper)) {
                const doc_num = try forEachFwdTermId(entry.value, CollectTermsContext, &collect_terms_ctx, CollectTermsContext.visit);
                try appendPrunedDoc(alloc, &pruned_docs, &seen_doc_nums, doc_num, doc_id);
            }
            maybe_fwd_entry = try cur.next();
        }

        const DocMapPruneContext = struct {
            alloc: Allocator,
            lower: []const u8,
            upper: []const u8,
            terms_ctx: *CollectTermsContext,
            docs: *std.ArrayListUnmanaged(PrunedDoc),
            seen: *std.AutoHashMapUnmanaged(u64, void),

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (!docIdInOwnedRange(entry.doc_id, ctx.lower, ctx.upper)) return false;
                _ = try forEachFwdTermId(entry.fwd_data, CollectTermsContext, ctx.terms_ctx, CollectTermsContext.visit);
                try appendPrunedDoc(ctx.alloc, ctx.docs, ctx.seen, entry.doc_num, entry.doc_id);
                return false;
            }
        };
        var docmap_ctx = DocMapPruneContext{
            .alloc = alloc,
            .lower = lower,
            .upper = upper,
            .terms_ctx = &collect_terms_ctx,
            .docs = &pruned_docs,
            .seen = &seen_doc_nums,
        };
        var maybe_docmap = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_docmap) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            _ = try forEachDocMapEntry(entry.value, &docmap_ctx, DocMapPruneContext.visit);
            maybe_docmap = try cur.next();
        }

        if (try cur.seekAtOrAfter(taggedPrefix(key_inv))) |first| {
            var entry = first;
            while (true) {
                if (entry.key.len == 0 or entry.key[0] != key_inv) break;
                const term_id = parseTermRangeKey(entry.key) orelse {
                    entry = (try cur.next()) orelse break;
                    continue;
                };
                const range = try decodeChunkRangeMeta(entry.value);
                switch (classifyChunkRange(range, lower, upper)) {
                    .outside => {},
                    .right_only, .mixed => try term_ids.append(alloc, term_id),
                }
                entry = (try cur.next()) orelse break;
            }
        }

        if (term_ids.items.len == 0 and pruned_docs.items.len == 0) return;

        var write_txn = try self.beginWriteTxn();
        errdefer write_txn.abort();
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.doc_count = prev_doc_count;
            self.term_count = prev_term_count;
        }
        const pruned_term_ids = sortAndDedupU32(term_ids.items);
        for (pruned_term_ids) |term_id| {
            try pruneTermPostings(alloc, &write_txn, self.dbi, term_id, lower, upper);
        }
        for (pruned_docs.items) |doc| {
            const fwd_key = try fwdKeyAlloc(alloc, doc.doc_id);
            defer alloc.free(fwd_key);
            txnDelete(&write_txn, self.dbi, fwd_key) catch {};
            var rev_key_buf: [256]u8 = undefined;
            txnDelete(&write_txn, self.dbi, revKey(&rev_key_buf, doc.doc_num)) catch {};
            var tombstone_key_buf: [16]u8 = undefined;
            try txnPut(&write_txn, self.dbi, docTombstoneKey(&tombstone_key_buf, doc.doc_num), &.{});
        }
        const stats_after = try scanStatsInTxn(&write_txn, self.dbi);
        self.doc_count = stats_after.doc_count;
        self.term_count = stats_after.term_count;
        try persistSparseCounters(self, &write_txn);
        try write_txn.commit();
    }
};

fn txnGet(txn: anytype, dbi: anytype, key: []const u8) ![]const u8 {
    _ = dbi;
    return try txn.get(key);
}

fn txnPut(txn: anytype, dbi: anytype, key: []const u8, value: []const u8) !void {
    _ = dbi;
    try txn.put(key, value);
}

fn txnTypeSupportsAppendPut(comptime T: type) bool {
    const base = switch (@typeInfo(T)) {
        .pointer => |ptr| ptr.child,
        else => T,
    };
    return @hasDecl(base, "appendPut");
}

fn txnAppendPut(txn: anytype, dbi: anytype, key: []const u8, value: []const u8) !void {
    if (comptime txnTypeSupportsAppendPut(@TypeOf(txn))) {
        txn.appendPut(key, value) catch |err| switch (err) {
            error.Unsupported => return try txnPut(txn, dbi, key, value),
            else => return err,
        };
        return;
    }
    try txnPut(txn, dbi, key, value);
}

fn txnDelete(txn: anytype, dbi: anytype, key: []const u8) !void {
    _ = dbi;
    try txn.delete(key);
}

fn persistNextDocNum(idx: *SparseIndex, txn: anytype) !void {
    var ndn_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &ndn_buf, idx.next_doc_num, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_next_doc_num), &ndn_buf);
}

fn persistNextSegmentId(idx: *SparseIndex, txn: anytype) !void {
    var segment_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &segment_buf, idx.next_segment_id, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_next_segment_id), &segment_buf);
}

fn persistSparseCounters(idx: *SparseIndex, txn: anytype) !void {
    try persistNextDocNum(idx, txn);
    try persistNextSegmentId(idx, txn);
    var doc_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &doc_buf, idx.doc_count, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_doc_count), &doc_buf);
    var term_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &term_buf, idx.term_count, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_term_count), &term_buf);
}

fn scanStatsInTxn(txn: anytype, dbi: anytype) !SparseIndex.Stats {
    _ = dbi;
    var cur = try txn.openCursor();
    defer cur.close();
    var out = SparseIndex.Stats{};
    var maybe_entry = try cur.first();
    while (maybe_entry) |entry| {
        if (entry.key.len > 0 and entry.key[0] == key_rev) out.doc_count += 1;
        if (entry.key.len > 0 and entry.key[0] == key_term_catalog) out.term_count += 1;
        maybe_entry = try cur.next();
    }
    return out;
}

fn handoffTermPostings(
    alloc: Allocator,
    src_txn: anytype,
    src_dbi: anytype,
    dest_txn: anytype,
    dest_dbi: anytype,
    term_id: u32,
    lower: []const u8,
    upper: []const u8,
    selected_docs: *const SelectedDocLookup,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(src_txn, src_dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    var out_chunk_count: u32 = 0;
    var out_max_weight: f32 = 0;
    var out_min_doc_id: ?[]const u8 = null;
    var out_max_doc_id: ?[]const u8 = null;

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(src_txn, src_dbi, ck) catch continue;

        var meta_ck_buf: [256]u8 = undefined;
        const chunk_meta_data = txnGet(src_txn, src_dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch null;
        if (chunk_meta_data) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => continue,
                .right_only => {
                    const chunk_max_weight = try decodeChunkMaxWeight(chunk_data);
                    out_max_weight = if (out_chunk_count == 0) chunk_max_weight else @max(out_max_weight, chunk_max_weight);
                    var out_ck_buf: [256]u8 = undefined;
                    const out_ck = invChunkKey(&out_ck_buf, term_id, out_chunk_count);
                    try txnPut(dest_txn, dest_dbi, out_ck, chunk_data);
                    var out_meta_ck_buf: [256]u8 = undefined;
                    const out_meta_ck = invChunkMetaKey(&out_meta_ck_buf, term_id, out_chunk_count);
                    try txnPut(dest_txn, dest_dbi, out_meta_ck, raw_meta);
                    updateBorrowedRangeBounds(&out_min_doc_id, &out_max_doc_id, range.min_doc_id, range.max_doc_id);
                    out_chunk_count += 1;
                    continue;
                },
                .mixed => {},
            }
        }

        var out_doc_nums = std.ArrayListUnmanaged(u32).empty;
        defer out_doc_nums.deinit(alloc);
        var out_weights = std.ArrayListUnmanaged(f32).empty;
        defer out_weights.deinit(alloc);
        var chunk_min_doc_id: ?[]const u8 = null;
        var chunk_max_doc_id: ?[]const u8 = null;
        try collectSelectedChunkEntries(
            alloc,
            chunk_data,
            selected_docs,
            &out_doc_nums,
            &out_weights,
            &chunk_min_doc_id,
            &chunk_max_doc_id,
            &out_max_weight,
        );
        if (out_doc_nums.items.len == 0) continue;
        try writeChunkWithRangeMetaToTxn(
            alloc,
            src_txn,
            src_dbi,
            dest_txn,
            dest_dbi,
            term_id,
            out_chunk_count,
            out_doc_nums.items,
            out_weights.items,
            chunk_min_doc_id.?,
            chunk_max_doc_id.?,
        );
        updateBorrowedRangeBounds(&out_min_doc_id, &out_max_doc_id, chunk_min_doc_id.?, chunk_max_doc_id.?);
        out_chunk_count += 1;
    }

    if (out_chunk_count == 0) return;
    const out_meta = encodeTermMeta(out_max_weight, out_chunk_count);
    try txnPut(dest_txn, dest_dbi, mk, &out_meta);
    const out_range_meta = try encodeChunkRangeMeta(alloc, out_min_doc_id.?, out_max_doc_id.?);
    defer alloc.free(out_range_meta);
    var range_key_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, termRangeKey(&range_key_buf, term_id), out_range_meta);
}

fn handoffWholeTermPostings(
    src_txn: anytype,
    src_dbi: anytype,
    dest_txn: anytype,
    dest_dbi: anytype,
    term_id: u32,
    term_range_meta: []const u8,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(src_txn, src_dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    try txnPut(dest_txn, dest_dbi, mk, meta_data);
    var range_key_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, termRangeKey(&range_key_buf, term_id), term_range_meta);

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(src_txn, src_dbi, ck) catch continue;
        try txnPut(dest_txn, dest_dbi, ck, chunk_data);

        var meta_ck_buf: [256]u8 = undefined;
        const meta_ck = invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci));
        const chunk_meta_data = txnGet(src_txn, src_dbi, meta_ck) catch continue;
        try txnPut(dest_txn, dest_dbi, meta_ck, chunk_meta_data);
    }
}

fn pruneTermPostings(
    alloc: Allocator,
    txn: anytype,
    dbi: anytype,
    term_id: u32,
    lower: []const u8,
    upper: []const u8,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(txn, dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    var kept = std.ArrayListUnmanaged(RetainedChunk).empty;
    defer {
        for (kept.items) |*chunk| chunk.deinit(alloc);
        kept.deinit(alloc);
    }

    var out_max_weight: f32 = 0;
    var out_min_doc_id: ?[]u8 = null;
    defer if (out_min_doc_id) |value| alloc.free(value);
    var out_max_doc_id: ?[]u8 = null;
    defer if (out_max_doc_id) |value| alloc.free(value);

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(txn, dbi, ck) catch continue;

        var meta_ck_buf: [256]u8 = undefined;
        const meta_ck = invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci));
        const chunk_meta_data = txnGet(txn, dbi, meta_ck) catch null;
        if (chunk_meta_data) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => {
                    const chunk_copy = try alloc.dupe(u8, chunk_data);
                    errdefer alloc.free(chunk_copy);
                    const meta_copy = try alloc.dupe(u8, raw_meta);
                    errdefer alloc.free(meta_copy);
                    try kept.append(alloc, .{
                        .chunk_bytes = chunk_copy,
                        .meta_bytes = meta_copy,
                        .max_weight = try decodeChunkMaxWeight(chunk_data),
                    });
                    out_max_weight = if (kept.items.len == 1) kept.items[0].max_weight else @max(out_max_weight, kept.items[kept.items.len - 1].max_weight);
                    try updateOwnedRangeBounds(alloc, &out_min_doc_id, &out_max_doc_id, range.min_doc_id, range.max_doc_id);
                    continue;
                },
                .right_only => continue,
                .mixed => {},
            }
        }

        const decoded = try decodeChunk(alloc, chunk_data);
        defer alloc.free(decoded.doc_nums);
        defer alloc.free(decoded.weights);

        var keep_count: usize = 0;
        for (decoded.doc_nums) |doc_num| {
            if (try shouldKeepDocNumOutsideRange(alloc, txn, dbi, doc_num, lower, upper)) keep_count += 1;
        }
        if (keep_count == 0) continue;

        var out_doc_nums = try alloc.alloc(u32, keep_count);
        defer alloc.free(out_doc_nums);
        var out_weights = try alloc.alloc(f32, keep_count);
        defer alloc.free(out_weights);

        var wi: usize = 0;
        for (decoded.doc_nums, 0..) |doc_num, i| {
            if (!(try shouldKeepDocNumOutsideRange(alloc, txn, dbi, doc_num, lower, upper))) continue;
            out_doc_nums[wi] = doc_num;
            out_weights[wi] = decoded.weights[i];
            out_max_weight = if (kept.items.len == 0 and wi == 0 and out_max_weight == 0) decoded.weights[i] else @max(out_max_weight, decoded.weights[i]);
            wi += 1;
        }

        const range = try computeChunkRangeMetaFromDocNums(alloc, txn, dbi, out_doc_nums);
        defer {
            alloc.free(range.min_doc_id);
            alloc.free(range.max_doc_id);
        }
        const encoded = try encodeChunk(alloc, out_doc_nums, out_weights);
        errdefer alloc.free(encoded);
        const encoded_meta = try encodeChunkRangeMeta(alloc, range.min_doc_id, range.max_doc_id);
        errdefer alloc.free(encoded_meta);
        try kept.append(alloc, .{
            .chunk_bytes = encoded,
            .meta_bytes = encoded_meta,
            .max_weight = try decodeChunkMaxWeight(encoded),
        });
        try updateOwnedRangeBounds(alloc, &out_min_doc_id, &out_max_doc_id, range.min_doc_id, range.max_doc_id);
    }

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        txnDelete(txn, dbi, invChunkKey(&ck_buf, term_id, @intCast(ci))) catch {};
        var meta_ck_buf: [256]u8 = undefined;
        txnDelete(txn, dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch {};
    }

    var range_key_buf: [256]u8 = undefined;
    if (kept.items.len == 0) {
        txnDelete(txn, dbi, mk) catch {};
        txnDelete(txn, dbi, termRangeKey(&range_key_buf, term_id)) catch {};
        return;
    }

    for (kept.items, 0..) |chunk, out_idx| {
        var ck_buf: [256]u8 = undefined;
        try txnPut(txn, dbi, invChunkKey(&ck_buf, term_id, @intCast(out_idx)), chunk.chunk_bytes);
        var meta_ck_buf: [256]u8 = undefined;
        try txnPut(txn, dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(out_idx)), chunk.meta_bytes);
    }

    const out_meta = encodeTermMeta(out_max_weight, @intCast(kept.items.len));
    try txnPut(txn, dbi, mk, &out_meta);
    const out_range_meta = try encodeChunkRangeMeta(alloc, out_min_doc_id.?, out_max_doc_id.?);
    defer alloc.free(out_range_meta);
    try txnPut(txn, dbi, termRangeKey(&range_key_buf, term_id), out_range_meta);
}

fn shouldKeepDocNumOutsideRange(
    alloc: Allocator,
    txn: anytype,
    dbi: anytype,
    doc_num: u32,
    lower: []const u8,
    upper: []const u8,
) !bool {
    _ = alloc;
    const doc_id = try resolveDocIdByDocNumInTxnBorrowed(txn, dbi, doc_num);
    if (std.mem.order(u8, doc_id, lower) == .lt) return true;
    if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) return true;
    return false;
}

fn docIdInOwnedRange(doc_id: []const u8, lower: []const u8, upper: []const u8) bool {
    if (std.mem.order(u8, doc_id, lower) == .lt) return false;
    if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) return false;
    return true;
}

fn accumulateSplitPlanningStats(
    alloc: Allocator,
    src_txn: anytype,
    src_dbi: anytype,
    term_id: u32,
    lower: []const u8,
    upper: []const u8,
    selected_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
    out: *SplitPlanningStats,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(src_txn, src_dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(src_txn, src_dbi, ck) catch continue;

        var meta_ck_buf: [256]u8 = undefined;
        const chunk_meta_data = txnGet(src_txn, src_dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch null;
        if (chunk_meta_data) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => continue,
                .right_only => {
                    const decoded_full = try decodeChunk(alloc, chunk_data);
                    defer alloc.free(decoded_full.doc_nums);
                    defer alloc.free(decoded_full.weights);
                    out.right_only_chunks += 1;
                    out.right_only_postings += decoded_full.doc_nums.len;
                    continue;
                },
                .mixed => {},
            }
        }

        const decoded = try decodeChunk(alloc, chunk_data);
        defer alloc.free(decoded.doc_nums);
        defer alloc.free(decoded.weights);

        var kept: usize = 0;
        for (decoded.doc_nums) |doc_num| {
            if (selected_doc_nums.contains(doc_num)) kept += 1;
        }
        if (kept == 0) continue;
        if (kept == decoded.doc_nums.len) {
            out.right_only_chunks += 1;
            out.right_only_postings += kept;
        } else {
            out.mixed_chunks += 1;
            out.mixed_right_postings += kept;
        }
    }
}

const ChunkSplitClass = enum {
    outside,
    right_only,
    mixed,
};

fn classifyChunkRange(range: ChunkRangeMeta, lower: []const u8, upper: []const u8) ChunkSplitClass {
    if (std.mem.order(u8, range.max_doc_id, lower) == .lt) return .outside;
    if (upper.len > 0 and std.mem.order(u8, range.min_doc_id, upper) != .lt) return .outside;

    const min_in = std.mem.order(u8, range.min_doc_id, lower) != .lt and (upper.len == 0 or std.mem.order(u8, range.min_doc_id, upper) == .lt);
    const max_in = std.mem.order(u8, range.max_doc_id, lower) != .lt and (upper.len == 0 or std.mem.order(u8, range.max_doc_id, upper) == .lt);
    if (min_in and max_in) return .right_only;
    return .mixed;
}

fn resolveDocIdByDocNumInTxnBorrowed(txn: anytype, dbi: anytype, doc_num: u32) ![]const u8 {
    var rev_buf: [256]u8 = undefined;
    return txnGet(txn, dbi, revKey(&rev_buf, doc_num));
}

fn resolveDocIdByDocNumInTxn(alloc: Allocator, txn: anytype, dbi: anytype, doc_num: u32) ![]u8 {
    return alloc.dupe(u8, try resolveDocIdByDocNumInTxnBorrowed(txn, dbi, doc_num));
}

fn computeChunkRangeMetaFromDocNums(
    alloc: Allocator,
    txn: anytype,
    dbi: anytype,
    doc_nums: []const u32,
) !struct { min_doc_id: []u8, max_doc_id: []u8 } {
    std.debug.assert(doc_nums.len > 0);
    var min_doc_id: ?[]u8 = null;
    var max_doc_id: ?[]u8 = null;
    errdefer {
        if (min_doc_id) |doc_id| alloc.free(doc_id);
        if (max_doc_id) |doc_id| alloc.free(doc_id);
    }

    for (doc_nums) |doc_num| {
        const doc_id = try resolveDocIdByDocNumInTxnBorrowed(txn, dbi, doc_num);
        if (min_doc_id == null or std.mem.order(u8, doc_id, min_doc_id.?) == .lt) {
            if (min_doc_id) |existing| alloc.free(existing);
            min_doc_id = try alloc.dupe(u8, doc_id);
        }
        if (max_doc_id == null or std.mem.order(u8, doc_id, max_doc_id.?) == .gt) {
            if (max_doc_id) |existing| alloc.free(existing);
            max_doc_id = try alloc.dupe(u8, doc_id);
        }
    }
    return .{ .min_doc_id = min_doc_id.?, .max_doc_id = max_doc_id.? };
}

fn writeChunkWithRangeMetaToTxn(
    alloc: Allocator,
    src_txn: anytype,
    src_dbi: anytype,
    dest_txn: anytype,
    dest_dbi: anytype,
    term_id: u32,
    chunk_idx: u32,
    doc_nums: []const u32,
    weights: []const f32,
    min_doc_id: []const u8,
    max_doc_id: []const u8,
) !void {
    _ = src_txn;
    _ = src_dbi;
    const encoded = try encodeChunk(alloc, doc_nums, weights);
    defer alloc.free(encoded);
    var ck_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, invChunkKey(&ck_buf, term_id, chunk_idx), encoded);

    const meta = try encodeChunkRangeMeta(alloc, min_doc_id, max_doc_id);
    defer alloc.free(meta);
    var meta_ck_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, invChunkMetaKey(&meta_ck_buf, term_id, chunk_idx), meta);
}

// ============================================================================
// Tests
// ============================================================================

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ts = nowNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-sparse-{s}-{d}\x00", .{ label, ts }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "sparse chunk encoding round-trip" {
    const alloc = std.testing.allocator;
    const doc_nums = [_]u32{ 10, 20, 35 };
    const weights = [_]f32{ 0.5, 0.8, 0.3 };

    const encoded = try encodeChunk(alloc, &doc_nums, &weights);
    defer alloc.free(encoded);

    const decoded = try decodeChunk(alloc, encoded);
    defer alloc.free(decoded.doc_nums);
    defer alloc.free(decoded.weights);

    try std.testing.expectEqual(@as(usize, 3), decoded.doc_nums.len);
    try std.testing.expectEqual(@as(u32, 10), decoded.doc_nums[0]);
    try std.testing.expectEqual(@as(u32, 20), decoded.doc_nums[1]);
    try std.testing.expectEqual(@as(u32, 35), decoded.doc_nums[2]);

    // Weights are quantized so check approximate equality
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.weights[0], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), decoded.weights[1], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), decoded.weights[2], 0.01);
}

test "sparse forward entry encoding round-trip" {
    const alloc = std.testing.allocator;
    const term_ids = [_]u32{ 5, 10, 42 };
    const weights = [_]f32{ 0.1, 0.9, 0.5 };

    const encoded = try encodeFwdEntry(alloc, 12345, &term_ids, &weights);
    defer alloc.free(encoded);

    const decoded = try decodeFwdEntry(alloc, encoded);
    defer alloc.free(decoded.term_ids);
    defer alloc.free(decoded.weights);

    try std.testing.expectEqual(@as(u64, 12345), decoded.doc_num);
    try std.testing.expectEqual(@as(usize, 3), decoded.term_ids.len);
    try std.testing.expectEqual(@as(u32, 5), decoded.term_ids[0]);
    try std.testing.expectEqual(@as(u32, 42), decoded.term_ids[2]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), decoded.weights[1], 0.001);
}

test "sparse insert and search single doc" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s1");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const indices = [_]u32{ 0, 5, 10 };
    const values = [_]f32{ 0.5, 0.8, 0.3 };
    const writes = [_]SparseWrite{.{
        .doc_id = "doc1",
        .vec = .{ .indices = &indices, .values = &values },
    }};
    try idx.batch(&writes, &.{});

    // Search with matching terms
    const q_indices = [_]u32{ 0, 5 };
    const q_values = [_]f32{ 1.0, 1.0 };
    const query = SparseVector{ .indices = &q_indices, .values = &q_values };

    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("doc1", results[0].doc_id);
    // Score ≈ 1.0*0.5 + 1.0*0.8 ≈ 1.3 (with quantization noise)
    try std.testing.expect(results[0].score > 1.0);
}

test "sparse multi-doc top-k search" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    // doc1: strong on term 0
    const w1_i = [_]u32{0};
    const w1_v = [_]f32{0.9};
    // doc2: strong on term 1
    const w2_i = [_]u32{1};
    const w2_v = [_]f32{0.9};
    // doc3: weak on term 0
    const w3_i = [_]u32{0};
    const w3_v = [_]f32{0.1};

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &w1_i, .values = &w1_v } },
        .{ .doc_id = "doc2", .vec = .{ .indices = &w2_i, .values = &w2_v } },
        .{ .doc_id = "doc3", .vec = .{ .indices = &w3_i, .values = &w3_v } },
    };
    try idx.batch(&writes, &.{});

    // Search for term 0 only
    const q_i = [_]u32{0};
    const q_v = [_]f32{1.0};
    const query = SparseVector{ .indices = &q_i, .values = &q_v };

    const results = try idx.search(alloc, &query, 2);
    defer SparseIndex.freeResults(alloc, results);

    // Should return doc1 and doc3 (both have term 0), doc1 ranked higher
    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("doc1", results[0].doc_id);
    try std.testing.expect(results[0].score > results[1].score);
}

test "sparse bulk append builds searchable postings" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-bulk-append");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{ .chunk_size = 2 });
    defer idx.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 1.0, 0.25 } } },
        .{ .doc_id = "doc2", .vec = .{ .indices = &.{1}, .values = &.{0.5} } },
        .{ .doc_id = "doc3", .vec = .{ .indices = &.{2}, .values = &.{0.75} } },
    };
    try idx.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .backend_batch_options = .{ .mode = .bulk_ingest },
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    const stats = idx.stats();
    try std.testing.expectEqual(@as(u64, 3), stats.doc_count);
    try std.testing.expectEqual(@as(u64, 2), stats.term_count);

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("doc1", results[0].doc_id);
}

test "sparse bulk append extends existing partial chunk" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-bulk-append-existing");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{ .chunk_size = 3 });
    defer idx.close();

    try idx.batch(&[_]SparseWrite{.{
        .doc_id = "doc1",
        .vec = .{ .indices = &.{1}, .values = &.{0.25} },
    }}, &.{});

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc2", .vec = .{ .indices = &.{1}, .values = &.{0.5} } },
        .{ .doc_id = "doc3", .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc4", .vec = .{ .indices = &.{1}, .values = &.{0.75} } },
    };
    try idx.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .backend_batch_options = .{ .mode = .bulk_ingest },
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 4), results.len);
    try std.testing.expectEqualStrings("doc3", results[0].doc_id);
}

test "sparse bulk append accounts resource working set" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-bulk-resource");
    defer cleanupTmp(path);

    var manager = resource_manager_mod.ResourceManager.init(.{});
    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();
    idx.attachResourceManager(&manager);

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &.{ 1, 2, 3 }, .values = &.{ 1.0, 0.5, 0.25 } } },
        .{ .doc_id = "doc2", .vec = .{ .indices = &.{ 1, 3 }, .values = &.{ 0.75, 0.5 } } },
    };
    try idx.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .backend_batch_options = .{ .mode = .bulk_ingest },
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    const resource_stats = manager.snapshot().slices[@intFromEnum(resource_manager_mod.Slice.sparse_apply_working_set)];
    try std.testing.expectEqual(@as(u64, 0), resource_stats.used_bytes);
    try std.testing.expect(resource_stats.peak_bytes > 0);
}

test "sparse batch delete removes from posting lists" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s3");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const idx1 = [_]u32{0};
    const val1 = [_]f32{1.0};
    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &idx1, .values = &val1 } },
    };
    try idx.batch(&writes, &.{});

    // Delete doc1
    const deletes = [_][]const u8{"doc1"};
    try idx.batch(&.{}, &deletes);

    // Search should return empty
    const q_i = [_]u32{0};
    const q_v = [_]f32{1.0};
    const query = SparseVector{ .indices = &q_i, .values = &q_v };

    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sparse constrained search filters before top-k ranking" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-constrained");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc:a", .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc:b", .vec = .{ .indices = &.{1}, .values = &.{0.1} } },
        .{ .doc_id = "doc:c", .vec = .{ .indices = &.{1}, .values = &.{0.9} } },
    };
    try idx.batch(&writes, &.{});

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const filtered = try idx.searchConstrained(alloc, &query, 1, .{
        .filter_doc_ids = &.{ "doc:b", "doc:c" },
        .exclude_doc_ids = &.{"doc:c"},
    });
    defer SparseIndex.freeResults(alloc, filtered);

    try std.testing.expectEqual(@as(usize, 1), filtered.len);
    try std.testing.expectEqualStrings("doc:b", filtered[0].doc_id);
}

test "sparse handoff range preserves doc numbers and postings" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = tmpPath(&src_buf, "split-src");
    defer cleanupTmp(src_path);
    var dest_buf: [256]u8 = undefined;
    const dest_path = tmpPath(&dest_buf, "split-dest");
    defer cleanupTmp(dest_path);

    var src = try SparseIndex.open(alloc, src_path, .{});
    defer src.close();
    var dest = try SparseIndex.open(alloc, dest_path, .{});
    defer dest.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "a", .vec = .{ .indices = &.{ 1, 3 }, .values = &.{ 0.5, 0.2 } } },
        .{ .doc_id = "b", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 0.8, 0.7 } } },
        .{ .doc_id = "c", .vec = .{ .indices = &.{ 2, 4 }, .values = &.{ 0.9, 0.4 } } },
    };
    try src.batch(&writes, &.{});

    var rebuilt = try src.handoffRangeInto(&dest, alloc, "b", "", true);
    defer rebuilt.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), rebuilt.doc_ids.len);
    try std.testing.expectEqualStrings("b", rebuilt.doc_ids[0]);
    try std.testing.expectEqualStrings("c", rebuilt.doc_ids[1]);
    try std.testing.expectEqual(@as(u64, 3), dest.next_doc_num);

    const query = SparseVector{
        .indices = &.{2},
        .values = &.{1.0},
    };
    const results = try dest.search(alloc, &query, 4);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("c", results[0].doc_id);
    try std.testing.expectEqualStrings("b", results[1].doc_id);
}

test "sparse split planning stats classify right-only and mixed chunks" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = tmpPath(&src_buf, "split-plan");
    defer cleanupTmp(src_path);

    var src = try SparseIndex.open(alloc, src_path, .{ .chunk_size = 2 });
    defer src.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "a", .vec = .{ .indices = &.{1}, .values = &.{0.1} } },
        .{ .doc_id = "b", .vec = .{ .indices = &.{1}, .values = &.{0.2} } },
        .{ .doc_id = "c", .vec = .{ .indices = &.{1}, .values = &.{0.3} } },
        .{ .doc_id = "d", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 0.4, 0.9 } } },
    };
    try src.batch(&writes, &.{});

    const stats = try src.splitPlanningStats(alloc, "b", "");
    try std.testing.expectEqual(@as(usize, 3), stats.selected_docs);
    try std.testing.expectEqual(@as(usize, 2), stats.touched_terms);
    try std.testing.expectEqual(@as(usize, 2), stats.right_only_chunks);
    try std.testing.expectEqual(@as(usize, 1), stats.mixed_chunks);
    try std.testing.expectEqual(@as(usize, 3), stats.right_only_postings);
    try std.testing.expectEqual(@as(usize, 1), stats.mixed_right_postings);
}

test "sparse empty search returns empty" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s4");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const q_i = [_]u32{0};
    const q_v = [_]f32{1.0};
    const query = SparseVector{ .indices = &q_i, .values = &q_v };

    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sparse reopen preserves data" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s5");
    defer cleanupTmp(path);

    // Insert
    {
        var idx = try SparseIndex.open(alloc, path, .{});
        defer idx.close();
        const idx1 = [_]u32{0};
        const val1 = [_]f32{0.7};
        const writes = [_]SparseWrite{
            .{ .doc_id = "persist_doc", .vec = .{ .indices = &idx1, .values = &val1 } },
        };
        try idx.batch(&writes, &.{});
    }

    // Reopen and search
    {
        var idx = try SparseIndex.open(alloc, path, .{});
        defer idx.close();

        const q_i = [_]u32{0};
        const q_v = [_]f32{1.0};
        const query = SparseVector{ .indices = &q_i, .values = &q_v };
        const results = try idx.search(alloc, &query, 10);
        defer SparseIndex.freeResults(alloc, results);

        try std.testing.expectEqual(@as(usize, 1), results.len);
        try std.testing.expectEqualStrings("persist_doc", results[0].doc_id);
    }
}

test "sparse backend adapters expose txn cursor operations" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s-adapter");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        const encoded = std.mem.toBytes(@as(u64, 7));
        try txn.put("meta:next_doc_num", &encoded);
        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("meta:next_doc_num", (try cur.first()).?.key);
        try txn.commit();
    }

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        const encoded = try txn.get("meta:next_doc_num");
        try std.testing.expectEqual(@as(u64, 7), std.mem.readInt(u64, encoded[0..8], .little));
    }
}

test "sparse backend store opens concrete txn handles" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s-store");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    var backend = idx.backendStore();
    try std.testing.expect(backend.capabilities().cursors);

    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        const encoded = std.mem.toBytes(@as(u64, 9));
        try txn.put("meta:next_doc_num", &encoded);
        try txn.commit();
    }

    {
        var txn = try backend.beginRead();
        defer txn.abort();
        const encoded = try txn.get("meta:next_doc_num");
        try std.testing.expectEqual(@as(u64, 9), std.mem.readInt(u64, encoded[0..8], .little));
    }

    {
        var batch = try backend.beginBatch();
        errdefer batch.abort();
        const encoded = std.mem.toBytes(@as(u64, 10));
        try batch.put("meta:next_doc_num", &encoded);
        try batch.commit();
    }
}
