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
//! LMDB key patterns:
//!   fwd:<docID>           → forward index entry
//!   rev:<docNum>          → reverse mapping (docNum → docID)
//!   inv:<termID>:meta     → term metadata (max_weight, chunk_count)
//!   inv:<termID>:chunk:<N>→ posting list chunk
//!   termrange:<termID>    → min/max doc key covered by the term
//!   meta:doc_count        → total document count
//!   meta:next_doc_num     → next doc number to assign

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const backend_erased = @import("../storage/backend_erased.zig");
const backend_types = @import("../storage/backend_types.zig");
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
};

pub const SearchResult = struct {
    doc_id: []u8,
    doc_num: ?u32 = null,
    score: f32,
};

pub const SearchConstraints = struct {
    filter_doc_ids: []const []const u8 = &.{},
    exclude_doc_ids: []const []const u8 = &.{},
    filter_doc_nums: []const u32 = &.{},
    exclude_doc_nums: []const u32 = &.{},
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

fn fwdKey(buf: []u8, doc_id: []const u8) []const u8 {
    return std.fmt.bufPrint(buf, "fwd:{s}", .{doc_id}) catch unreachable;
}

fn revKey(buf: []u8, doc_num: u64) []const u8 {
    return std.fmt.bufPrint(buf, "rev:{d}", .{doc_num}) catch unreachable;
}

fn invMetaKey(buf: []u8, term_id: u32) []const u8 {
    return std.fmt.bufPrint(buf, "inv:{d}:meta", .{term_id}) catch unreachable;
}

fn invChunkKey(buf: []u8, term_id: u32, chunk_num: u32) []const u8 {
    return std.fmt.bufPrint(buf, "inv:{d}:chunk:{d}", .{ term_id, chunk_num }) catch unreachable;
}

fn invChunkMetaKey(buf: []u8, term_id: u32, chunk_num: u32) []const u8 {
    return std.fmt.bufPrint(buf, "inv:{d}:chunkmeta:{d}", .{ term_id, chunk_num }) catch unreachable;
}

fn termRangeKey(buf: []u8, term_id: u32) []const u8 {
    return std.fmt.bufPrint(buf, "termrange:{d}", .{term_id}) catch unreachable;
}

fn invChunkPrefix(buf: []u8, term_id: u32) []const u8 {
    return std.fmt.bufPrint(buf, "inv:{d}:chunk:", .{term_id}) catch unreachable;
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
    doc_count: u64,
    term_count: u64,

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

    pub fn backendStore(self: *SparseIndex) *backend_erased.Store {
        return &self.store;
    }

    pub fn open(alloc: Allocator, path: [*:0]const u8, opts: SparseIndexOptions) !SparseIndex {
        var opened = try openStore(alloc, path, opts);
        errdefer {
            opened.store.deinit();
            opened.owner.close(alloc);
        }

        var next_doc_num: u64 = 0;
        var doc_count: u64 = 0;
        var term_count: u64 = 0;
        if (opts.lsm_options.backend.read_only) {
            var txn = try opened.store.beginRead();
            defer txn.abort();
            const ndn_data = txn.get("meta:next_doc_num") catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (ndn_data) |d| {
                next_doc_num = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get("meta:doc_count") catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                doc_count = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get("meta:term_count") catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                term_count = std.mem.readInt(u64, d[0..8], .little);
            }
        } else {
            var txn = try opened.store.beginWrite();
            errdefer txn.abort();

            const ndn_data = txn.get("meta:next_doc_num") catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (ndn_data) |d| {
                next_doc_num = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get("meta:doc_count") catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                doc_count = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get("meta:term_count") catch |err| switch (err) {
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
        if (txn.get("meta:doc_count") catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            if (raw.len >= 8) out.doc_count = std.mem.readInt(u64, raw[0..8], .little);
        }
        if (txn.get("meta:term_count") catch |err| switch (err) {
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
            if (std.mem.startsWith(u8, entry.key, "fwd:")) out.doc_count += 1;
            if (std.mem.startsWith(u8, entry.key, "inv:") and std.mem.endsWith(u8, entry.key, ":meta")) out.term_count += 1;
            maybe_entry = try cur.next();
        }
        return out;
    }

    /// Batch insert and delete sparse vectors.
    pub fn batch(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8) !void {
        return try self.batchWithOptions(writes, deletes, .{});
    }

    pub fn batchWithOptions(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        const prev_next_doc_num = self.next_doc_num;
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.next_doc_num = prev_next_doc_num;
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
        for (deletes) |doc_id| {
            const effect = try self.processDelete(scratch, &txn, doc_id, touched_terms_ptr);
            self.applyDeleteEffect(effect);
            _ = scratch_arena.reset(.retain_capacity);
        }

        // Process inserts
        for (writes) |w| {
            try self.processInsert(scratch, &txn, w.doc_id, w.vec, w.doc_num, touched_terms_ptr);
            _ = scratch_arena.reset(.retain_capacity);
        }

        if (touched_terms_ptr) |map| {
            var it = map.iterator();
            while (it.next()) |entry| {
                try self.refreshTermRangeMeta(scratch, &txn, entry.key_ptr.*);
                _ = scratch_arena.reset(.retain_capacity);
            }
        }

        try persistSparseCounters(self, &txn);

        try txn.commit();
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
        const fwd_data = txnGet(txn, self.dbi, fk) catch |err| switch (err) {
            error.NotFound => return .{}, // doc not found, nothing to delete
            else => return err,
        };

        const fwd = try decodeFwdEntry(alloc, fwd_data);
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
        const encoded = try encodeChunk(alloc, doc_nums, weights);
        defer alloc.free(encoded);
        var ck_buf: [256]u8 = undefined;
        try txnPut(txn, self.dbi, invChunkKey(&ck_buf, term_id, chunk_idx), encoded);

        const meta = try encodeChunkRangeMeta(alloc, min_doc_id, max_doc_id);
        defer alloc.free(meta);
        var meta_ck_buf: [256]u8 = undefined;
        try txnPut(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, chunk_idx), meta);
    }

    fn writeTermRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32, min_doc_id: []const u8, max_doc_id: []const u8) !void {
        const meta = try encodeChunkRangeMeta(alloc, min_doc_id, max_doc_id);
        defer alloc.free(meta);
        var key_buf: [256]u8 = undefined;
        try txnPut(txn, self.dbi, termRangeKey(&key_buf, term_id), meta);
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
        var rev_buf: [256]u8 = undefined;
        const doc_id = try txnGet(txn, self.dbi, revKey(&rev_buf, doc_num));
        return alloc.dupe(u8, doc_id);
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
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var filter_doc_nums = try self.resolveDocNumSetAlloc(alloc, &txn, constraints.filter_doc_ids);
        defer filter_doc_nums.deinit(alloc);
        if (constraints.filter_doc_ids.len > 0 and filter_doc_nums.count() == 0) {
            return try alloc.alloc(SearchResult, 0);
        }
        var direct_filter_doc_nums = try self.docNumSetFromSliceAlloc(alloc, constraints.filter_doc_nums);
        defer direct_filter_doc_nums.deinit(alloc);

        var exclude_doc_nums = try self.resolveDocNumSetAlloc(alloc, &txn, constraints.exclude_doc_ids);
        defer exclude_doc_nums.deinit(alloc);
        var direct_exclude_doc_nums = try self.docNumSetFromSliceAlloc(alloc, constraints.exclude_doc_nums);
        defer direct_exclude_doc_nums.deinit(alloc);

        // Accumulate scores: docNum → score
        var scores = std.AutoHashMapUnmanaged(u32, f32).empty;
        defer scores.deinit(alloc);

        for (query_vec.indices, 0..) |term_id, qi| {
            const query_weight = query_vec.values[qi];

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

                const decoded = try decodeChunk(alloc, chunk_data);
                defer alloc.free(decoded.doc_nums);
                defer alloc.free(decoded.weights);

                for (decoded.doc_nums, 0..) |doc_num, di| {
                    if (filter_doc_nums.count() > 0 and !filter_doc_nums.contains(doc_num)) continue;
                    if (direct_filter_doc_nums.count() > 0 and !direct_filter_doc_nums.contains(doc_num)) continue;
                    if (exclude_doc_nums.contains(doc_num)) continue;
                    if (direct_exclude_doc_nums.contains(doc_num)) continue;
                    const doc_weight = decoded.weights[di];
                    const gop = try scores.getOrPut(alloc, doc_num);
                    if (!gop.found_existing) gop.value_ptr.* = 0;
                    gop.value_ptr.* += query_weight * doc_weight;
                }
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

        std.mem.sort(ScoreEntry, entries.items, {}, struct {
            fn cmp(_: void, a: ScoreEntry, b: ScoreEntry) bool {
                return a.score > b.score; // descending
            }
        }.cmp);

        const n = @min(k, @as(u32, @intCast(entries.items.len)));

        // Resolve docNums to docIDs
        var results = try alloc.alloc(SearchResult, n);
        var valid: usize = 0;
        for (entries.items[0..n]) |entry| {
            var rev_buf: [256]u8 = undefined;
            const rk = revKey(&rev_buf, entry.doc_num);
            const doc_id = txn.get(rk) catch continue;
            results[valid] = .{
                .doc_id = try alloc.dupe(u8, doc_id),
                .doc_num = entry.doc_num,
                .score = entry.score,
            };
            valid += 1;
        }

        if (valid < n) {
            // Shrink if some doc nums couldn't be resolved
            return try alloc.realloc(results, valid);
        }
        return results;
    }

    fn resolveDocNumSetAlloc(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_ids: []const []const u8,
    ) !std.AutoHashMapUnmanaged(u32, void) {
        _ = self;
        var out = std.AutoHashMapUnmanaged(u32, void).empty;
        errdefer out.deinit(alloc);
        for (doc_ids) |doc_id| {
            var key_buf: [256]u8 = undefined;
            const fwd_data = txn.get(fwdKey(&key_buf, doc_id)) catch continue;
            const doc_num = try decodeFwdDocNum(fwd_data);
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
        var key_buf: [256]u8 = undefined;
        const fwd_data = txn.get(fwdKey(&key_buf, doc_id)) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const doc_num = try decodeFwdDocNum(fwd_data);
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

        const first = (try cur.seekAtOrAfter("fwd:")) orelse return .{ .doc_ids = try doc_ids.toOwnedSlice(alloc) };

        var entry = first;
        while (true) {
            if (!std.mem.startsWith(u8, entry.key, "fwd:")) break;
            const doc_id = entry.key["fwd:".len..];
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
            (try src_cur.seekAtOrAfter("fwd:")) orelse {
                try persistSparseCounters(dest, &dest_txn);
                try dest_txn.commit();
                return .{
                    .doc_ids = try doc_ids.toOwnedSlice(alloc),
                    .select_docs_ns = elapsedSince(select_started),
                };
            }
        else blk: {
            const start_key = try std.fmt.allocPrint(alloc, "fwd:{s}", .{lower});
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
            if (!std.mem.startsWith(u8, entry.key, "fwd:")) break;
            const doc_id = entry.key["fwd:".len..];
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
            try key_buf.appendSlice(alloc, "fwd:");
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
            (try src_cur.seekAtOrAfter("fwd:")) orelse return .{}
        else blk: {
            const start_key = try std.fmt.allocPrint(alloc, "fwd:{s}", .{lower});
            defer alloc.free(start_key);
            break :blk (try src_cur.seekAtOrAfter(start_key)) orelse return .{};
        };

        var entry = first;
        while (true) {
            if (!std.mem.startsWith(u8, entry.key, "fwd:")) break;
            const doc_id = entry.key["fwd:".len..];
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

        const first_term = (try src_cur.seekAtOrAfter("termrange:")) orelse return out;
        entry = first_term;
        while (true) {
            if (!std.mem.startsWith(u8, entry.key, "termrange:")) break;
            const term_id = try std.fmt.parseInt(u32, entry.key["termrange:".len..], 10);
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

        var term_ids = std.ArrayListUnmanaged(u32).empty;
        defer term_ids.deinit(alloc);

        const first = (try cur.seekAtOrAfter("termrange:")) orelse return;

        var entry = first;
        while (true) {
            if (!std.mem.startsWith(u8, entry.key, "termrange:")) break;
            const term_id = try std.fmt.parseInt(u32, entry.key["termrange:".len..], 10);
            const range = try decodeChunkRangeMeta(entry.value);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => {},
                .right_only, .mixed => try term_ids.append(alloc, term_id),
            }
            entry = (try cur.next()) orelse break;
        }

        if (term_ids.items.len == 0) return;

        var write_txn = try self.beginWriteTxn();
        errdefer write_txn.abort();
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.doc_count = prev_doc_count;
            self.term_count = prev_term_count;
        }
        for (term_ids.items) |term_id| {
            try pruneTermPostings(alloc, &write_txn, self.dbi, term_id, lower, upper);
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

fn txnDelete(txn: anytype, dbi: anytype, key: []const u8) !void {
    _ = dbi;
    try txn.delete(key);
}

fn persistNextDocNum(idx: *SparseIndex, txn: anytype) !void {
    var ndn_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &ndn_buf, idx.next_doc_num, .little);
    try txnPut(txn, idx.dbi, "meta:next_doc_num", &ndn_buf);
}

fn persistSparseCounters(idx: *SparseIndex, txn: anytype) !void {
    try persistNextDocNum(idx, txn);
    var doc_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &doc_buf, idx.doc_count, .little);
    try txnPut(txn, idx.dbi, "meta:doc_count", &doc_buf);
    var term_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &term_buf, idx.term_count, .little);
    try txnPut(txn, idx.dbi, "meta:term_count", &term_buf);
}

fn scanStatsInTxn(txn: anytype, dbi: anytype) !SparseIndex.Stats {
    _ = dbi;
    var cur = try txn.openCursor();
    defer cur.close();
    var out = SparseIndex.Stats{};
    var maybe_entry = try cur.first();
    while (maybe_entry) |entry| {
        if (std.mem.startsWith(u8, entry.key, "fwd:")) out.doc_count += 1;
        if (std.mem.startsWith(u8, entry.key, "inv:") and std.mem.endsWith(u8, entry.key, ":meta")) out.term_count += 1;
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

test "sparse search supports caller supplied ordinal doc nums" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-ordinal-doc-nums");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc:a", .doc_num = 42, .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc:b", .doc_num = 7, .vec = .{ .indices = &.{1}, .values = &.{0.5} } },
    };
    try idx.batch(&writes, &.{});

    try std.testing.expectEqual(@as(?u32, 42), try idx.debugDocNumForDocId("doc:a"));
    try std.testing.expectEqual(@as(?u32, 7), try idx.debugDocNumForDocId("doc:b"));

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const filtered = try idx.searchConstrained(alloc, &query, 10, .{
        .filter_doc_nums = &.{42},
    });
    defer SparseIndex.freeResults(alloc, filtered);

    try std.testing.expectEqual(@as(usize, 1), filtered.len);
    try std.testing.expectEqualStrings("doc:a", filtered[0].doc_id);
    try std.testing.expectEqual(@as(?u32, 42), filtered[0].doc_num);

    const excluded = try idx.searchConstrained(alloc, &query, 10, .{
        .exclude_doc_nums = &.{42},
    });
    defer SparseIndex.freeResults(alloc, excluded);

    try std.testing.expectEqual(@as(usize, 1), excluded.len);
    try std.testing.expectEqualStrings("doc:b", excluded[0].doc_id);
    try std.testing.expectEqual(@as(?u32, 7), excluded[0].doc_num);
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
