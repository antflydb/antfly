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

const std = @import("std");
const compat = @import("../io/compat.zig");

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Character-level span for one chunk within a document.
pub const FusedChunkBoundary = struct {
    start_char: u32,
    end_char: u32,
};

/// One training example: a document with pre-labeled chunk boundaries.
pub const FusedSample = struct {
    text: []const u8,
    chunk_boundaries: []FusedChunkBoundary,
    /// Optional paraphrase texts for contrastive learning.
    positive_texts: []const []const u8,
    /// Optional per-sample hard negative texts (Feature 7).
    hard_negatives: []const []const u8 = &.{},
};

/// Fixed-size batch for model training/inference.
/// All values are pre-tokenized. Dimensions: [batch_size * max_seq_len] flat.
pub const FusedBatch = struct {
    batch_size: usize,
    max_seq_len: usize,
    max_chunks: usize,

    /// Token IDs: [batch_size * max_seq_len] i32
    input_ids: []i32,
    /// Attention mask: [batch_size * max_seq_len] i32 (1=real, 0=pad)
    attention_mask: []i32,
    /// Boundary labels: [batch_size * max_seq_len] f32
    /// 1.0 at the first token of each chunk boundary after the first chunk.
    boundary_labels: []f32,
    /// Chunk start token indices: [batch_size * max_chunks] i32
    chunk_starts: []i32,
    /// Chunk end token indices (exclusive): [batch_size * max_chunks] i32
    chunk_ends: []i32,
    /// Chunk validity mask: [batch_size * max_chunks] f32 (1.0=valid, 0.0=padding)
    chunk_mask: []f32,
    /// Sample indices back into the source dataset (for debugging).
    sample_indices: []usize,

    // Hard negatives (Feature 7): null when no samples have hard_negatives.
    // hard_neg_ids: [batch_size * max_negatives * max_seq_len]
    hard_neg_ids: ?[]i32 = null,
    // hard_neg_mask: [batch_size * max_negatives * max_seq_len]
    hard_neg_mask: ?[]i32 = null,
    num_negatives: usize = 0,

    pub fn deinit(self: *FusedBatch, allocator: std.mem.Allocator) void {
        allocator.free(self.input_ids);
        allocator.free(self.attention_mask);
        allocator.free(self.boundary_labels);
        allocator.free(self.chunk_starts);
        allocator.free(self.chunk_ends);
        allocator.free(self.chunk_mask);
        allocator.free(self.sample_indices);
        if (self.hard_neg_ids) |h| allocator.free(h);
        if (self.hard_neg_mask) |h| allocator.free(h);
        self.* = undefined;
    }
};

/// Aggregate statistics over a loaded dataset.
pub const FusedDatasetStats = struct {
    num_samples: usize = 0,
    avg_text_chars: f64 = 0,
    avg_chunks_per_sample: f64 = 0,
    min_chunks: usize = 0,
    max_chunks: usize = 0,
    samples_with_positives: usize = 0,
};

pub const LoadedSamples = struct {
    arena: std.heap.ArenaAllocator,
    dataset_root: []const u8,
    samples: []FusedSample,

    pub fn deinit(self: *LoadedSamples) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

// ---------------------------------------------------------------------------
// Private parsing types (JSONL wire format)
// ---------------------------------------------------------------------------

const RawChunk = struct {
    start_char: ?u32 = null,
    end_char: ?u32 = null,
    start: ?u32 = null,
    end: ?u32 = null,
};

const RawRecord = struct {
    text: []const u8,
    chunks: ?[]const RawChunk = null,
    chunk_boundaries: ?[]const RawChunk = null,
    positives: ?[]const []const u8 = null,
    /// Feature 7: flat array of hard negative strings at the sample level.
    hard_negatives: ?[]const []const u8 = null,
};

// ---------------------------------------------------------------------------
// File resolution (mirrors reranker_data.zig exactly)
// ---------------------------------------------------------------------------

const ResolvedFiles = struct {
    arena: std.heap.ArenaAllocator,
    base_dir: []const u8,
    paths: [][]const u8,

    fn deinit(self: *ResolvedFiles) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn resolveJsonlFiles(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !ResolvedFiles {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    if (std.mem.trim(u8, path, " \t\r\n").len == 0) return error.EmptyPath;
    const stat = try compat.cwd().statFile(compat.io(), path, .{});
    if (stat.kind == .file) {
        const one = try arena_alloc.alloc([]const u8, 1);
        one[0] = try arena_alloc.dupe(u8, path);
        return .{
            .arena = arena,
            .base_dir = try arena_alloc.dupe(u8, std.fs.path.dirname(path) orelse "."),
            .paths = one,
        };
    }
    if (stat.kind != .directory) return error.UnsupportedPathType;

    var dir = try compat.cwd().openDir(compat.io(), path, .{ .iterate = true });
    defer dir.close(compat.io());
    var iter = dir.iterate();
    var paths = std.ArrayListUnmanaged([]const u8).empty;
    defer paths.deinit(arena_alloc);
    while (try iter.next(compat.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".jsonl")) continue;
        if (split) |want_split| {
            const prefix = try std.fmt.allocPrint(arena_alloc, "{s}-", .{want_split});
            if (!std.mem.startsWith(u8, entry.name, prefix)) continue;
        }
        try paths.append(arena_alloc, try std.fs.path.join(arena_alloc, &.{ path, entry.name }));
    }
    if (paths.items.len == 0) return error.NoJsonlFilesForSplit;
    std.mem.sort([]const u8, paths.items, {}, lessThanString);
    return .{
        .arena = arena,
        .base_dir = try arena_alloc.dupe(u8, path),
        .paths = try paths.toOwnedSlice(arena_alloc),
    };
}

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

// ---------------------------------------------------------------------------
// JSONL loading
// ---------------------------------------------------------------------------

fn loadSamplesFromFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    out: *std.ArrayListUnmanaged(FusedSample),
) !void {
    const data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        const rec = try std.json.parseFromSliceLeaky(RawRecord, allocator, line, .{
            .ignore_unknown_fields = true,
        });

        // Skip records with empty text.
        if (std.mem.trim(u8, rec.text, " \t\r\n").len == 0) continue;

        // Prefer `chunks`, fall back to `chunk_boundaries`.
        const raw_chunks: []const RawChunk = rec.chunks orelse rec.chunk_boundaries orelse &.{};

        // Skip records with fewer than 2 chunks.
        if (raw_chunks.len < 2) continue;

        // Map raw chunks to canonical boundaries, supporting both field-name variants.
        const boundaries = try allocator.alloc(FusedChunkBoundary, raw_chunks.len);
        for (raw_chunks, 0..) |rc, i| {
            boundaries[i] = .{
                .start_char = rc.start_char orelse rc.start orelse 0,
                .end_char = rc.end_char orelse rc.end orelse 0,
            };
        }

        // Map optional positives.
        const positives: []const []const u8 = rec.positives orelse &.{};

        // Map optional hard negatives (Feature 7).
        const hard_negs: []const []const u8 = rec.hard_negatives orelse &.{};

        try out.append(allocator, .{
            .text = rec.text,
            .chunk_boundaries = boundaries,
            .positive_texts = positives,
            .hard_negatives = hard_negs,
        });
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub fn loadSamples(
    allocator: std.mem.Allocator,
    path: []const u8,
    split: ?[]const u8,
) !LoadedSamples {
    var resolved = try resolveJsonlFiles(allocator, path, split);
    defer resolved.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    var samples = std.ArrayListUnmanaged(FusedSample).empty;
    defer samples.deinit(arena_alloc);

    for (resolved.paths) |resolved_path| {
        try loadSamplesFromFile(arena_alloc, resolved_path, &samples);
    }

    return .{
        .arena = arena,
        .dataset_root = try arena_alloc.dupe(u8, resolved.base_dir),
        .samples = try samples.toOwnedSlice(arena_alloc),
    };
}

pub fn computeStats(samples: []const FusedSample) FusedDatasetStats {
    var stats = FusedDatasetStats{ .num_samples = samples.len };
    if (samples.len == 0) return stats;

    var total_chars: usize = 0;
    var total_chunks: usize = 0;
    var with_positives: usize = 0;
    stats.min_chunks = samples[0].chunk_boundaries.len;
    stats.max_chunks = samples[0].chunk_boundaries.len;

    for (samples) |s| {
        total_chars += s.text.len;
        total_chunks += s.chunk_boundaries.len;
        stats.min_chunks = @min(stats.min_chunks, s.chunk_boundaries.len);
        stats.max_chunks = @max(stats.max_chunks, s.chunk_boundaries.len);
        if (s.positive_texts.len > 0) with_positives += 1;
    }

    const n = @as(f64, @floatFromInt(samples.len));
    stats.avg_text_chars = @as(f64, @floatFromInt(total_chars)) / n;
    stats.avg_chunks_per_sample = @as(f64, @floatFromInt(total_chunks)) / n;
    stats.samples_with_positives = with_positives;
    return stats;
}

/// Convert a character-level [start_char, end_char) span to token indices.
///
/// offsets[t] = {byte_start, byte_end} for token t.
/// Returns {start_token, end_token} where end_token is exclusive.
///
/// Tokens where offsets[t] == {0, 0} and t > 0 are treated as special/padding
/// tokens and are skipped during the search.
pub fn charToTokenBoundary(
    start_char: u32,
    end_char: u32,
    offsets: [][2]u32,
) struct { start_token: u32, end_token: u32 } {
    if (offsets.len == 0) return .{ .start_token = 0, .end_token = 0 };

    // Find the first token whose byte range contains start_char.
    var start_tok: u32 = 0;
    var found_start = false;
    for (offsets, 0..) |off, t| {
        // Skip zero-span special tokens (e.g. [CLS], [SEP]) at positions > 0.
        if (t > 0 and off[0] == 0 and off[1] == 0) continue;
        if (off[0] <= start_char and start_char < off[1]) {
            start_tok = @intCast(t);
            found_start = true;
            break;
        }
    }
    if (!found_start) {
        // start_char is beyond all tokens — return an empty range clamped to the end.
        const last: u32 = @intCast(offsets.len);
        return .{ .start_token = last, .end_token = last };
    }

    // Find the last token whose byte range contains end_char (exclusive condition:
    // off[0] < end_char <= off[1] means end_char falls within or at the end of token t).
    var end_tok: u32 = start_tok + 1; // exclusive; initialise to at least one token
    for (offsets, 0..) |off, t| {
        if (t > 0 and off[0] == 0 and off[1] == 0) continue;
        if (off[0] < end_char and end_char <= off[1]) {
            end_tok = @as(u32, @intCast(t)) + 1;
            break;
        }
    }

    // Clamp end_tok to valid range.
    const max_tok: u32 = @intCast(offsets.len);
    end_tok = @min(end_tok, max_tok);

    return .{ .start_token = start_tok, .end_token = end_tok };
}

/// Assemble a fixed-size token batch from a subset of samples addressed by `indices`.
///
/// token_fn signature:
///   fn(ctx: anytype, text: []const u8, out_ids: []i32, out_mask: []i32, out_offsets: ?[][2]u32) usize
///
/// token_fn fills the pre-allocated out_ids and out_mask slices (length max_seq_len),
/// optionally fills out_offsets with per-token byte ranges, and returns the actual
/// number of tokens produced (<= out_ids.len).
///
/// Pass `{}` as ctx for context-free (plain function) token_fn.
///
/// All output slices are allocated from `allocator`; caller owns them via FusedBatch.deinit.
pub fn assembleTokenBatch(
    allocator: std.mem.Allocator,
    samples: []const FusedSample,
    indices: []const usize,
    max_seq_len: usize,
    max_chunks: usize,
    ctx: anytype,
    token_fn: anytype,
) !FusedBatch {
    const batch_size = indices.len;
    const seq_elems = batch_size * max_seq_len;
    const chunk_elems = batch_size * max_chunks;

    const input_ids = try allocator.alloc(i32, seq_elems);
    errdefer allocator.free(input_ids);
    @memset(input_ids, 0);

    const attention_mask = try allocator.alloc(i32, seq_elems);
    errdefer allocator.free(attention_mask);
    @memset(attention_mask, 0);

    const boundary_labels = try allocator.alloc(f32, seq_elems);
    errdefer allocator.free(boundary_labels);
    @memset(boundary_labels, 0);

    const chunk_starts = try allocator.alloc(i32, chunk_elems);
    errdefer allocator.free(chunk_starts);
    @memset(chunk_starts, 0);

    const chunk_ends = try allocator.alloc(i32, chunk_elems);
    errdefer allocator.free(chunk_ends);
    @memset(chunk_ends, 0);

    const chunk_mask = try allocator.alloc(f32, chunk_elems);
    errdefer allocator.free(chunk_mask);
    @memset(chunk_mask, 0);

    const sample_indices = try allocator.alloc(usize, batch_size);
    errdefer allocator.free(sample_indices);

    // Per-sample scratch buffer for character-to-token offset mapping.
    // Reused across all samples; zeroed before each call so unused entries are {0,0}.
    const offsets_scratch = try allocator.alloc([2]u32, max_seq_len);
    defer allocator.free(offsets_scratch);

    for (indices, 0..) |sample_idx, i| {
        sample_indices[i] = sample_idx;
        const sample = samples[sample_idx];

        const ids_slice = input_ids[i * max_seq_len .. (i + 1) * max_seq_len];
        const mask_slice = attention_mask[i * max_seq_len .. (i + 1) * max_seq_len];

        @memset(offsets_scratch, .{ 0, 0 });

        const n_tokens = token_fn(ctx, sample.text, ids_slice, mask_slice, offsets_scratch);
        const active_offsets = offsets_scratch[0..@min(n_tokens, max_seq_len)];

        // Map each chunk boundary to a token span and populate batch arrays.
        const c_base = i * max_chunks;
        const n_chunks = @min(sample.chunk_boundaries.len, max_chunks);

        for (0..n_chunks) |k| {
            const boundary = sample.chunk_boundaries[k];
            const tok_span = charToTokenBoundary(
                boundary.start_char,
                boundary.end_char,
                active_offsets,
            );
            chunk_starts[c_base + k] = @intCast(tok_span.start_token);
            chunk_ends[c_base + k] = @intCast(tok_span.end_token);
            chunk_mask[c_base + k] = 1.0;

            // Set boundary label at the first token of every chunk after the first.
            // This marks the positions where chunk splits occur in the sequence.
            if (k > 0 and tok_span.start_token < max_seq_len) {
                boundary_labels[i * max_seq_len + tok_span.start_token] = 1.0;
            }
        }
    }

    // Feature 7: Hard negatives — collect up to max_negatives per sample.
    const max_negatives: usize = 7;
    var any_hard_negs = false;
    for (indices) |sample_idx| {
        if (samples[sample_idx].hard_negatives.len > 0) {
            any_hard_negs = true;
            break;
        }
    }

    var hard_neg_ids_opt: ?[]i32 = null;
    var hard_neg_mask_opt: ?[]i32 = null;
    var actual_num_negatives: usize = 0;

    if (any_hard_negs) {
        const neg_elems = batch_size * max_negatives * max_seq_len;
        const hn_ids = try allocator.alloc(i32, neg_elems);
        errdefer allocator.free(hn_ids);
        @memset(hn_ids, 0);
        const hn_mask = try allocator.alloc(i32, neg_elems);
        errdefer allocator.free(hn_mask);
        @memset(hn_mask, 0);

        for (indices, 0..) |sample_idx, i| {
            const sample = samples[sample_idx];
            const n_neg = @min(sample.hard_negatives.len, max_negatives);
            if (n_neg > actual_num_negatives) actual_num_negatives = n_neg;
            for (0..n_neg) |ni| {
                const base = (i * max_negatives + ni) * max_seq_len;
                const neg_ids_slice = hn_ids[base .. base + max_seq_len];
                const neg_mask_slice = hn_mask[base .. base + max_seq_len];
                // Temporary i32 slices for token_fn output
                _ = token_fn(ctx, sample.hard_negatives[ni], neg_ids_slice, neg_mask_slice, null);
            }
        }

        hard_neg_ids_opt = hn_ids;
        hard_neg_mask_opt = hn_mask;
    }

    return .{
        .batch_size = batch_size,
        .max_seq_len = max_seq_len,
        .max_chunks = max_chunks,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .boundary_labels = boundary_labels,
        .chunk_starts = chunk_starts,
        .chunk_ends = chunk_ends,
        .chunk_mask = chunk_mask,
        .sample_indices = sample_indices,
        .hard_neg_ids = hard_neg_ids_opt,
        .hard_neg_mask = hard_neg_mask_opt,
        .num_negatives = actual_num_negatives,
    };
}

// ---------------------------------------------------------------------------
// Length Bucketing (Feature 8)
// ---------------------------------------------------------------------------

/// Sort sample indices into buckets by text length for efficient batching.
/// Returns a new order that groups similar-length texts together.
/// Divides `indices` into windows of `bucket_size` and sorts each window
/// by `samples[idx].text.len` ascending.  This reduces padding waste by
/// keeping similar-length texts in the same batch.
pub fn sortByLength(
    allocator: std.mem.Allocator,
    samples: []const FusedSample,
    indices: []const usize,
    bucket_size: usize,
) ![]usize {
    const result = try allocator.dupe(usize, indices);
    errdefer allocator.free(result);

    const effective_bucket = @max(bucket_size, 1);
    var start: usize = 0;
    while (start < result.len) {
        const end = @min(start + effective_bucket, result.len);
        const window = result[start..end];
        std.mem.sort(usize, window, samples, struct {
            fn lessThan(ctx: []const FusedSample, a: usize, b: usize) bool {
                return ctx[a].text.len < ctx[b].text.len;
            }
        }.lessThan);
        start = end;
    }
    return result;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "charToTokenBoundary: basic span mapping" {
    // Token layout over "Hello world foo":
    //   t0: bytes [0, 5)  "Hello"
    //   t1: bytes [5, 6)  " "
    //   t2: bytes [6, 11) "world"
    //   t3: bytes [11,12) " "
    //   t4: bytes [12,15) "foo"
    //   t5: {0,0}         special/padding (t>0, skipped)
    var offsets = [_][2]u32{
        .{ 0, 5 },
        .{ 5, 6 },
        .{ 6, 11 },
        .{ 11, 12 },
        .{ 12, 15 },
        .{ 0, 0 },
    };

    // "Hello" only => [0, 1)
    {
        const r = charToTokenBoundary(0, 5, &offsets);
        try std.testing.expectEqual(@as(u32, 0), r.start_token);
        try std.testing.expectEqual(@as(u32, 1), r.end_token);
    }
    // "world" only => [2, 3)
    {
        const r = charToTokenBoundary(6, 11, &offsets);
        try std.testing.expectEqual(@as(u32, 2), r.start_token);
        try std.testing.expectEqual(@as(u32, 3), r.end_token);
    }
    // "Hello world" => [0, 3)
    {
        const r = charToTokenBoundary(0, 11, &offsets);
        try std.testing.expectEqual(@as(u32, 0), r.start_token);
        try std.testing.expectEqual(@as(u32, 3), r.end_token);
    }
    // "foo" => [4, 5)
    {
        const r = charToTokenBoundary(12, 15, &offsets);
        try std.testing.expectEqual(@as(u32, 4), r.start_token);
        try std.testing.expectEqual(@as(u32, 5), r.end_token);
    }
    // start_char beyond all tokens => clamped to end
    {
        const r = charToTokenBoundary(100, 110, &offsets);
        try std.testing.expectEqual(@as(u32, 6), r.start_token);
        try std.testing.expectEqual(@as(u32, 6), r.end_token);
    }
}

test "computeStats: hardcoded samples" {
    const boundaries_a = [_]FusedChunkBoundary{
        .{ .start_char = 0, .end_char = 10 },
        .{ .start_char = 10, .end_char = 20 },
        .{ .start_char = 20, .end_char = 30 },
    };
    const boundaries_b = [_]FusedChunkBoundary{
        .{ .start_char = 0, .end_char = 5 },
        .{ .start_char = 5, .end_char = 15 },
    };
    const positives_a = [_][]const u8{"paraphrase of sample a"};

    const samples = [_]FusedSample{
        .{
            .text = "hello world sample A", // 20 chars
            .chunk_boundaries = @constCast(&boundaries_a),
            .positive_texts = @constCast(&positives_a),
        },
        .{
            .text = "sample B text", // 13 chars
            .chunk_boundaries = @constCast(&boundaries_b),
            .positive_texts = &.{},
        },
    };

    const stats = computeStats(&samples);
    try std.testing.expectEqual(@as(usize, 2), stats.num_samples);
    try std.testing.expectEqual(@as(usize, 2), stats.min_chunks);
    try std.testing.expectEqual(@as(usize, 3), stats.max_chunks);
    try std.testing.expectEqual(@as(usize, 1), stats.samples_with_positives);
    // avg_chunks: (3 + 2) / 2 = 2.5
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), stats.avg_chunks_per_sample, 1e-6);
    // avg_text_chars: (20 + 13) / 2 = 16.5
    try std.testing.expectApproxEqAbs(@as(f64, 16.5), stats.avg_text_chars, 1e-6);
}

test "JSON parsing: both chunk field names and both coordinate field names" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    // `chunks` + `{start_char, end_char}`
    const line_a =
        \\{"text":"hello world","chunks":[{"start_char":0,"end_char":5},{"start_char":6,"end_char":11}]}
    ;
    const rec_a = try std.json.parseFromSliceLeaky(RawRecord, aa, line_a, .{
        .ignore_unknown_fields = true,
    });
    try std.testing.expectEqualStrings("hello world", rec_a.text);
    try std.testing.expect(rec_a.chunks != null);
    try std.testing.expectEqual(@as(usize, 2), rec_a.chunks.?.len);
    try std.testing.expectEqual(@as(?u32, 0), rec_a.chunks.?[0].start_char);
    try std.testing.expectEqual(@as(?u32, 5), rec_a.chunks.?[0].end_char);
    try std.testing.expectEqual(@as(?u32, null), rec_a.chunks.?[0].start);

    // `chunk_boundaries` + `{start, end}` + `positives`
    const line_b =
        \\{"text":"foo bar baz","chunk_boundaries":[{"start":0,"end":3},{"start":4,"end":7}],"positives":["alt text"]}
    ;
    const rec_b = try std.json.parseFromSliceLeaky(RawRecord, aa, line_b, .{
        .ignore_unknown_fields = true,
    });
    try std.testing.expectEqualStrings("foo bar baz", rec_b.text);
    try std.testing.expect(rec_b.chunk_boundaries != null);
    try std.testing.expectEqual(@as(usize, 2), rec_b.chunk_boundaries.?.len);
    try std.testing.expectEqual(@as(?u32, 0), rec_b.chunk_boundaries.?[0].start);
    try std.testing.expectEqual(@as(?u32, 3), rec_b.chunk_boundaries.?[0].end);
    try std.testing.expectEqual(@as(?u32, null), rec_b.chunk_boundaries.?[0].start_char);
    try std.testing.expect(rec_b.positives != null);
    try std.testing.expectEqual(@as(usize, 1), rec_b.positives.?.len);
    try std.testing.expectEqualStrings("alt text", rec_b.positives.?[0]);
}
