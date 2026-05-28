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

// Sparse embedding pipeline (SPLADE-style).
//
// Generates sparse (vocabulary-dimension) embeddings via:
// 1. Tokenize + forward pass through MLM-head model
// 2. If output is 3D [batch, seq, vocab]: max-pool over sequence dim
//    If output is 2D [batch, vocab]: use directly
// 3. Apply SPLADE activation: log(1 + relu(x))
// 4. Threshold at min_weight, take top-k by value
//
// Returns SparseVector per input text: sorted (index, value) pairs.
//
// Matches Go termite's lib/pipelines/sparse_embedding.go.

const std = @import("std");
const backends = @import("../backends/backends.zig");
const manifest_mod = @import("../models/manifest.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Tensor = backends.Tensor;

pub const SparseVector = struct {
    indices: []u32,
    values: []f32,

    pub fn deinit(self: *SparseVector, allocator: std.mem.Allocator) void {
        allocator.free(self.indices);
        allocator.free(self.values);
    }
};

pub const SparseEmbeddingConfig = struct {
    max_length: usize = 512,
    top_k: usize = 256,
    min_weight: f32 = 0.0,
    /// Standard SPLADE/MLM outputs are [batch, seq, vocab]. Backends should
    /// return concrete runtime shapes, but some imported graphs preserve
    /// symbolic/stale leading dims. Ambiguous 3D outputs require an explicit
    /// model manifest layout or a loader inference from the model structure.
    dynamic_3d_layout: ?Sparse3DLayoutKind = null,

    pub fn fromManifest(manifest: *const manifest_mod.ModelManifest) SparseEmbeddingConfig {
        return .{
            .max_length = manifest.max_position_embeddings,
            .dynamic_3d_layout = sparseLayoutFromManifest(manifest.sparse_3d_output_layout),
        };
    }
};

pub const SparseEmbeddingPipeline = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: SparseEmbeddingConfig,

    /// Generate sparse embeddings for a batch of texts.
    /// Caller owns the returned SparseVectors and must call deinit on each.
    pub fn embed(self: *SparseEmbeddingPipeline, texts: []const []const u8) ![]SparseVector {
        if (texts.len == 0) return try self.allocator.alloc(SparseVector, 0);
        const alloc = self.allocator;
        const max_len = self.config.max_length;
        const batch = texts.len;

        // Tokenize
        const all_ids = try alloc.alloc(i32, batch * max_len);
        defer alloc.free(all_ids);
        const all_mask = try alloc.alloc(i32, batch * max_len);
        defer alloc.free(all_mask);

        for (texts, 0..) |text, i| {
            var result = try self.tok.encodeForModel(alloc, text, max_len);
            defer result.deinit();
            @memcpy(all_ids[i * max_len .. (i + 1) * max_len], result.ids);
            @memcpy(all_mask[i * max_len .. (i + 1) * max_len], result.attention_mask);
        }

        // Convert to i64 for ONNX
        const ids_i64 = try alloc.alloc(i64, batch * max_len);
        defer alloc.free(ids_i64);
        const mask_i64 = try alloc.alloc(i64, batch * max_len);
        defer alloc.free(mask_i64);

        for (0..batch * max_len) |j| {
            ids_i64[j] = @intCast(all_ids[j]);
            mask_i64[j] = @intCast(all_mask[j]);
        }

        const shape = [_]i64{ @intCast(batch), @intCast(max_len) };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape, ids_i64);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape, mask_i64);
        defer attention_mask_tensor.deinit();

        // Check if model expects token_type_ids
        var token_type_tensor: ?Tensor = null;
        defer if (token_type_tensor) |*t| t.deinit();

        const input_info = self.session.inputInfo();
        var needs_token_type = false;
        for (input_info) |info| {
            if (std.mem.eql(u8, info.name, "token_type_ids")) {
                needs_token_type = true;
                break;
            }
        }

        const inputs = if (needs_token_type) blk: {
            const zeros = try alloc.alloc(i64, batch * max_len);
            defer alloc.free(zeros);
            @memset(zeros, 0);
            token_type_tensor = try Tensor.initInt64(alloc, "token_type_ids", &shape, zeros);
            break :blk &[_]Tensor{ input_ids_tensor, attention_mask_tensor, token_type_tensor.? };
        } else &[_]Tensor{ input_ids_tensor, attention_mask_tensor };

        // Run inference
        var outputs = try self.session.run(inputs, alloc);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        if (outputs.len == 0) return error.NoOutputTensors;

        const output = &outputs[0];
        const output_shape = output.shape;
        const data = output.asFloat32();

        // Get [batch, vocab] scores via max-pool if 3D, or directly if 2D
        return switch (output_shape.len) {
            3 => blk: {
                const layout = resolveSparse3DLayout(output_shape, data.len, batch, self.config.dynamic_3d_layout) orelse {
                    std.log.warn("sparse embedding unexpected 3D output shape={any} data_len={d} batch={d} configured_layout={any}", .{ output_shape, data.len, batch, self.config.dynamic_3d_layout });
                    return error.UnexpectedOutputShape;
                };
                break :blk switch (layout) {
                    .batch_seq => |dims| try self.sparseFromBatchSeq3D(data, all_mask, batch, dims.seq_len, max_len, dims.vocab),
                    .seq_batch => |dims| try self.sparseFromSeqBatch3D(data, all_mask, batch, dims.seq_len, max_len, dims.vocab),
                };
            },
            2 => try self.sparseFrom2D(data, batch, resolveSparse2DVocab(output_shape, data.len, batch) orelse {
                std.log.warn("sparse embedding unexpected 2D output shape={any} data_len={d} batch={d}", .{ output_shape, data.len, batch });
                return error.UnexpectedOutputShape;
            }),
            else => error.UnexpectedOutputShape,
        };
    }

    /// Max-pool over sequence dimension [batch, seq, vocab] → apply SPLADE activation → sparsify
    fn sparseFromBatchSeq3D(self: *SparseEmbeddingPipeline, data: []const f32, mask: []const i32, batch: usize, seq_len: usize, mask_stride: usize, vocab: usize) ![]SparseVector {
        const alloc = self.allocator;
        const results = try alloc.alloc(SparseVector, batch);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |*r| r.deinit(alloc);
            alloc.free(results);
        }

        const pooled = try alloc.alloc(f32, vocab);
        defer alloc.free(pooled);

        for (0..batch) |b| {
            // Initialize to -inf
            @memset(pooled, -std.math.inf(f32));

            // Max-pool over sequence, respecting attention mask
            for (0..seq_len) |s| {
                if (s >= mask_stride or mask[b * mask_stride + s] == 0) continue;
                const offset = (b * seq_len + s) * vocab;
                for (0..vocab) |v| {
                    if (data[offset + v] > pooled[v]) pooled[v] = data[offset + v];
                }
            }

            // Replace -inf with 0
            for (pooled) |*v| {
                if (v.* == -std.math.inf(f32)) v.* = 0;
            }

            results[b] = try spladeActivateAndSparsify(alloc, pooled, self.config.top_k, self.config.min_weight);
            initialized += 1;
        }

        return results;
    }

    /// Max-pool over sequence dimension [seq, batch, vocab] → apply SPLADE activation → sparsify.
    fn sparseFromSeqBatch3D(self: *SparseEmbeddingPipeline, data: []const f32, mask: []const i32, batch: usize, seq_len: usize, mask_stride: usize, vocab: usize) ![]SparseVector {
        const alloc = self.allocator;
        const results = try alloc.alloc(SparseVector, batch);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |*r| r.deinit(alloc);
            alloc.free(results);
        }

        const pooled = try alloc.alloc(f32, vocab);
        defer alloc.free(pooled);

        for (0..batch) |b| {
            @memset(pooled, -std.math.inf(f32));

            for (0..seq_len) |s| {
                if (s >= mask_stride or mask[b * mask_stride + s] == 0) continue;
                const offset = (s * batch + b) * vocab;
                for (0..vocab) |v| {
                    if (data[offset + v] > pooled[v]) pooled[v] = data[offset + v];
                }
            }

            for (pooled) |*v| {
                if (v.* == -std.math.inf(f32)) v.* = 0;
            }

            results[b] = try spladeActivateAndSparsify(alloc, pooled, self.config.top_k, self.config.min_weight);
            initialized += 1;
        }

        return results;
    }

    /// Apply SPLADE activation → sparsify on 2D output [batch, vocab]
    fn sparseFrom2D(self: *SparseEmbeddingPipeline, data: []const f32, batch: usize, vocab: usize) ![]SparseVector {
        const alloc = self.allocator;
        const results = try alloc.alloc(SparseVector, batch);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |*r| r.deinit(alloc);
            alloc.free(results);
        }

        for (0..batch) |b| {
            results[b] = try spladeActivateAndSparsify(alloc, data[b * vocab .. (b + 1) * vocab], self.config.top_k, self.config.min_weight);
            initialized += 1;
        }

        return results;
    }
};

const Sparse3DLayout = union(enum) {
    batch_seq: struct { seq_len: usize, vocab: usize },
    seq_batch: struct { seq_len: usize, vocab: usize },
};

pub const Sparse3DLayoutKind = enum {
    batch_seq,
    seq_batch,
};

fn sparseLayoutFromManifest(layout: ?manifest_mod.Sparse3DOutputLayout) ?Sparse3DLayoutKind {
    return switch (layout orelse return null) {
        .batch_seq => .batch_seq,
        .seq_batch => .seq_batch,
    };
}

fn positiveDim(dim: i64) ?usize {
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn resolveSparse3DLayout(shape: []const i64, data_len: usize, batch: usize, dynamic_layout: ?Sparse3DLayoutKind) ?Sparse3DLayout {
    if (shape.len != 3 or batch == 0) return null;
    const vocab = positiveDim(shape[2]) orelse return null;
    if (vocab == 0) return null;

    if (positiveDim(shape[0])) |dim0| {
        if (dim0 == batch) {
            if (positiveDim(shape[1])) |seq_len| {
                if (data_len == batch * seq_len * vocab) {
                    return .{ .batch_seq = .{ .seq_len = seq_len, .vocab = vocab } };
                }
            }
        }
    }

    if (positiveDim(shape[1])) |dim1| {
        if (dim1 == batch) {
            if (positiveDim(shape[0])) |seq_len| {
                if (data_len == seq_len * batch * vocab) {
                    return .{ .seq_batch = .{ .seq_len = seq_len, .vocab = vocab } };
                }
            }
        }
    }

    const row_width = batch * vocab;
    if (row_width == 0 or data_len % row_width != 0) return null;
    const seq_len = data_len / row_width;
    return switch (dynamic_layout orelse return null) {
        .batch_seq => blk: {
            if (!dimCanRepresentRuntimeBatch(shape[0], batch) and !dimCanRepresentFlattenedBatchSequence(shape[0], batch, seq_len, dimIsStaleRuntimeSequence(shape[1], seq_len))) return null;
            if (!dimCanRepresentRuntimeSequence(shape[1], seq_len, batch, dimIsStaleRuntimeBatch(shape[0], batch))) return null;
            break :blk .{ .batch_seq = .{ .seq_len = seq_len, .vocab = vocab } };
        },
        .seq_batch => blk: {
            if (!dimCanRepresentRuntimeSequence(shape[0], seq_len, batch, dimIsStaleRuntimeBatch(shape[1], batch))) return null;
            if (!dimCanRepresentRuntimeBatch(shape[1], batch)) return null;
            break :blk .{ .seq_batch = .{ .seq_len = seq_len, .vocab = vocab } };
        },
    };
}

fn resolveSparse2DVocab(shape: []const i64, data_len: usize, batch: usize) ?usize {
    if (shape.len != 2 or batch == 0) return null;
    if (positiveDim(shape[0])) |dim0| {
        if (dim0 == batch) {
            if (positiveDim(shape[1])) |vocab| {
                if (data_len == batch * vocab) return vocab;
                return null;
            }
            if (data_len % batch == 0) return data_len / batch;
            return null;
        }
        if (dim0 == 1 and batch != 1) {
            if (positiveDim(shape[1])) |vocab| {
                if (data_len == dim0 * vocab) return null;
            }
            if (data_len % batch == 0) return data_len / batch;
            return null;
        }
        return null;
    }
    if (data_len % batch == 0) return data_len / batch;
    return null;
}

fn dimCanRepresentRuntimeBatch(dim: i64, batch: usize) bool {
    if (dim <= 0) return true;
    if (dim == 1) return true;
    return dim == @as(i64, @intCast(batch));
}

fn dimIsStaleRuntimeBatch(dim: i64, batch: usize) bool {
    if (dim <= 0) return true;
    return dim == 1 and batch != 1;
}

fn dimIsStaleRuntimeSequence(dim: i64, seq_len: usize) bool {
    if (dim <= 0) return true;
    return dim == 1 and seq_len != 1;
}

fn dimCanRepresentFlattenedBatchSequence(dim: i64, batch: usize, seq_len: usize, sequence_axis_is_stale: bool) bool {
    if (!sequence_axis_is_stale or dim <= 0) return false;
    const flattened = std.math.mul(usize, batch, seq_len) catch return false;
    return dim == @as(i64, @intCast(flattened));
}

fn dimCanRepresentRuntimeSequence(dim: i64, seq_len: usize, batch: usize, batch_axis_is_stale: bool) bool {
    if (dim <= 0) return true;
    if (dim == 1) return true;
    if (dim == @as(i64, @intCast(seq_len))) return true;
    const flattened = std.math.mul(usize, batch, seq_len) catch return false;
    return batch_axis_is_stale and dim == @as(i64, @intCast(flattened));
}

/// Apply SPLADE activation and extract top-k sparse entries.
/// SPLADE pooling uses log(1 + relu(x)); negative logits remain zero.
fn spladeActivateAndSparsify(allocator: std.mem.Allocator, row: []const f32, top_k: usize, min_weight: f32) !SparseVector {
    if (row.len == 0) return SparseVector{ .indices = &.{}, .values = &.{} };

    const Entry = struct {
        index: u32,
        value: f32,
    };

    // Apply log1p(ReLU) and collect entries above threshold.
    var entries = std.ArrayListUnmanaged(Entry).empty;
    defer entries.deinit(allocator);

    for (row, 0..) |x, i| {
        const activated: f32 = if (x > 0.0) @log(1.0 + x) else 0.0;
        if (activated > min_weight) {
            try entries.append(allocator, .{ .index = @intCast(i), .value = activated });
        }
    }

    // Sort by value descending for top-k
    std.mem.sortUnstable(Entry, entries.items, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return a.value > b.value;
        }
    }.lessThan);

    // Take top-k
    const n = @min(entries.items.len, top_k);
    const selected = entries.items[0..n];

    // Sort by index ascending for output
    std.mem.sortUnstable(Entry, selected, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return a.index < b.index;
        }
    }.lessThan);

    // Build output arrays
    const indices = try allocator.alloc(u32, n);
    errdefer allocator.free(indices);
    const values = try allocator.alloc(f32, n);

    for (selected, 0..) |e, i| {
        indices[i] = e.index;
        values[i] = e.value;
    }

    return SparseVector{ .indices = indices, .values = values };
}

test "splade activation and sparsify" {
    const alloc = std.testing.allocator;

    const row = [_]f32{ -10.0, 0.0, 1.0, 5.0, -5.0, 25.0 };
    var result = try spladeActivateAndSparsify(alloc, &row, 3, 0.0);
    defer result.deinit(alloc);

    // Top 3 by value should be: index 5, index 3, index 2.
    // Sorted by index: 2, 3, 5
    try std.testing.expectEqual(@as(usize, 3), result.indices.len);
    try std.testing.expectEqual(@as(u32, 2), result.indices[0]);
    try std.testing.expectEqual(@as(u32, 3), result.indices[1]);
    try std.testing.expectEqual(@as(u32, 5), result.indices[2]);

    try std.testing.expectApproxEqAbs(@log(@as(f32, 2.0)), result.values[0], 1e-6);
    try std.testing.expectApproxEqAbs(@log(@as(f32, 6.0)), result.values[1], 1e-6);
    try std.testing.expectApproxEqAbs(@log(@as(f32, 26.0)), result.values[2], 1e-6);
}

test "splade activation drops non-positive logits" {
    const alloc = std.testing.allocator;

    const row = [_]f32{ -4.0, 0.0, 0.5 };
    var result = try spladeActivateAndSparsify(alloc, &row, 256, 0.0);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.indices.len);
    try std.testing.expectEqual(@as(u32, 2), result.indices[0]);
    try std.testing.expectApproxEqAbs(@log(@as(f32, 1.5)), result.values[0], 1e-6);
}

test "sparse 3d pooling supports sequence-major imported outputs" {
    const alloc = std.testing.allocator;
    var pipeline = SparseEmbeddingPipeline{
        .allocator = alloc,
        .session = undefined,
        .tok = undefined,
        .config = .{ .top_k = 4 },
    };

    const batch: usize = 2;
    const seq_len: usize = 3;
    const vocab: usize = 4;
    const mask = [_]i32{
        1, 1, 0,
        1, 1, 0,
    };
    const batch_seq = [_]f32{
        1, 0, 0, 0,
        0, 2, 0, 0,
        9, 9, 9, 9,
        1, 0, 0, 0,
        0, 2, 0, 0,
        9, 9, 9, 9,
    };
    const seq_batch = [_]f32{
        1, 0, 0, 0,
        1, 0, 0, 0,
        0, 2, 0, 0,
        0, 2, 0, 0,
        9, 9, 9, 9,
        9, 9, 9, 9,
    };

    const from_batch_seq = try pipeline.sparseFromBatchSeq3D(&batch_seq, &mask, batch, seq_len, seq_len, vocab);
    defer freeSparseVectorSlice(alloc, from_batch_seq);
    const from_seq_batch = try pipeline.sparseFromSeqBatch3D(&seq_batch, &mask, batch, seq_len, seq_len, vocab);
    defer freeSparseVectorSlice(alloc, from_seq_batch);

    try expectSparseEqual(from_batch_seq[0], from_batch_seq[1]);
    try expectSparseEqual(from_batch_seq[0], from_seq_batch[0]);
    try expectSparseEqual(from_seq_batch[0], from_seq_batch[1]);
}

test "sparse output layout uses configured dynamic batch-major fallback" {
    const layout = resolveSparse3DLayout(&.{ 1, 1, 4 }, 3 * 5 * 4, 3, .batch_seq) orelse return error.TestUnexpectedResult;
    switch (layout) {
        .batch_seq => |dims| {
            try std.testing.expectEqual(@as(usize, 5), dims.seq_len);
            try std.testing.expectEqual(@as(usize, 4), dims.vocab);
        },
        .seq_batch => return error.TestUnexpectedResult,
    }
}

test "sparse output layout rejects ambiguous dynamic 3d shape without configured fallback" {
    try std.testing.expect(resolveSparse3DLayout(&.{ 1, 1, 4 }, 3 * 5 * 4, 3, null) == null);
}

test "sparse output layout supports configured dynamic seq-major fallback" {
    const layout = resolveSparse3DLayout(&.{ 1, 1, 4 }, 3 * 5 * 4, 3, .seq_batch) orelse return error.TestUnexpectedResult;
    switch (layout) {
        .batch_seq => return error.TestUnexpectedResult,
        .seq_batch => |dims| {
            try std.testing.expectEqual(@as(usize, 5), dims.seq_len);
            try std.testing.expectEqual(@as(usize, 4), dims.vocab);
        },
    }
}

test "sparse output layout rejects concrete mismatched batch axes" {
    try std.testing.expect(resolveSparse3DLayout(&.{ 2, 5, 4 }, 3 * 5 * 4, 3, .batch_seq) == null);
    try std.testing.expect(resolveSparse3DLayout(&.{ 5, 2, 4 }, 3 * 5 * 4, 3, .seq_batch) == null);
}

test "sparse output layout accepts stale singleton batch with flattened sequence axis" {
    const layout = resolveSparse3DLayout(&.{ 1, 15, 4 }, 3 * 5 * 4, 3, .batch_seq) orelse return error.TestUnexpectedResult;
    switch (layout) {
        .batch_seq => |dims| try std.testing.expectEqual(@as(usize, 5), dims.seq_len),
        .seq_batch => return error.TestUnexpectedResult,
    }
}

test "sparse output layout accepts flattened tokens with stale singleton sequence axis" {
    const layout = resolveSparse3DLayout(&.{ 15, 1, 4 }, 3 * 5 * 4, 3, .batch_seq) orelse return error.TestUnexpectedResult;
    switch (layout) {
        .batch_seq => |dims| try std.testing.expectEqual(@as(usize, 5), dims.seq_len),
        .seq_batch => return error.TestUnexpectedResult,
    }
}

test "sparse embedding config maps manifest sparse layout" {
    var manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .max_position_embeddings = 128,
        .sparse_3d_output_layout = .batch_seq,
    };
    const config = SparseEmbeddingConfig.fromManifest(&manifest);
    try std.testing.expectEqual(@as(usize, 128), config.max_length);
    try std.testing.expectEqual(Sparse3DLayoutKind.batch_seq, config.dynamic_3d_layout.?);
}

test "sparse 2d output infers vocab from runtime element count" {
    const vocab = resolveSparse2DVocab(&.{ 1, 4 }, 3 * 4, 3) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 4), vocab);
}

test "sparse 2d output rejects concrete mismatched batch axis" {
    try std.testing.expect(resolveSparse2DVocab(&.{ 2, 6 }, 12, 3) == null);
}

test "sparse 2d output rejects concrete singleton row without stale shape evidence" {
    try std.testing.expect(resolveSparse2DVocab(&.{ 1, 6 }, 6, 3) == null);
}

test "sparse 2d output rejects concrete batch shape with mismatched element count" {
    try std.testing.expect(resolveSparse2DVocab(&.{ 3, 6 }, 12, 3) == null);
}

test "sparse ranking favors overlapping activated dimensions" {
    const alloc = std.testing.allocator;
    var pipeline = SparseEmbeddingPipeline{
        .allocator = alloc,
        .session = undefined,
        .tok = undefined,
        .config = .{ .top_k = 4 },
    };

    const batch: usize = 3;
    const vocab: usize = 8;
    const logits = [_]f32{
        0, 6, 4, 0, 0, 0, 0, 0,
        0, 5, 3, 0, 0, 0, 1, 0,
        0, 0, 0, 0, 7, 6, 0, 0,
    };
    const vectors = try pipeline.sparseFrom2D(&logits, batch, vocab);
    defer freeSparseVectorSlice(alloc, vectors);

    const related_score = sparseDot(vectors[0], vectors[1]);
    const unrelated_score = sparseDot(vectors[0], vectors[2]);
    try std.testing.expect(related_score > unrelated_score);
    try std.testing.expect(unrelated_score == 0.0);
}

fn freeSparseVectorSlice(allocator: std.mem.Allocator, vectors: []SparseVector) void {
    for (vectors) |*v| v.deinit(allocator);
    allocator.free(vectors);
}

fn sparseDot(a: SparseVector, b: SparseVector) f32 {
    var ai: usize = 0;
    var bi: usize = 0;
    var score: f32 = 0.0;
    while (ai < a.indices.len and bi < b.indices.len) {
        const av = a.indices[ai];
        const bv = b.indices[bi];
        if (av == bv) {
            score += a.values[ai] * b.values[bi];
            ai += 1;
            bi += 1;
        } else if (av < bv) {
            ai += 1;
        } else {
            bi += 1;
        }
    }
    return score;
}

fn expectSparseEqual(a: SparseVector, b: SparseVector) !void {
    try std.testing.expectEqualSlices(u32, a.indices, b.indices);
    try std.testing.expectEqual(a.values.len, b.values.len);
    for (a.values, b.values) |av, bv| {
        try std.testing.expectApproxEqAbs(av, bv, 1e-6);
    }
}
