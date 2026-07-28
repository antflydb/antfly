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

//! Result fusion for hybrid search across multiple index types.
//!
//! Matches Go antfly's remoteindex.go fusion implementation:
//!   - RRF (Reciprocal Rank Fusion): score = Σ weight * 1/(k + rank)
//!   - RSF (Relative Score Fusion): window-based min/max normalization + weighted sum
//!   - Pruner: 5 strategies (multi-index, min absolute, min ratio, std-dev, gap detection)

const std = @import("std");
const Allocator = std.mem.Allocator;

// ============================================================================
// Types
// ============================================================================

pub const FusionStrategy = enum { rrf, rsf };

pub const FusionConfig = struct {
    strategy: FusionStrategy = .rrf,
    rank_constant: f64 = 60.0,
    window_size: u32 = 0, // 0 = use result set size
    weights: []const NamedWeight = &.{},
};

pub const NamedWeight = struct {
    name: []const u8,
    weight: f64,
};

pub const RankedHit = struct {
    doc_id: []const u8,
    score: f64,
};

pub const RankedResult = struct {
    index_name: []const u8,
    hits: []const RankedHit,
    kind: SourceKind = .ranked,
};

pub const SourceKind = enum {
    /// Ordered retrieval source: contribution depends on rank or calibrated score.
    ranked,
    /// Exact membership signal: every matching candidate receives the same contribution.
    binary,
};

pub const IndexScore = struct {
    index_name: []const u8,
    /// Score reported by the source before fusion calibration.
    score: f64,
    /// One-based rank within the source's candidate window.
    rank: u32 = 0,
    /// Source score after strategy-specific calibration and before weighting.
    normalized_score: f64 = 0,
    /// Configured source weight.
    weight: f64 = 1,
    /// Additive contribution to the final fused score.
    contribution: f64 = 0,
};

pub const FusionHit = struct {
    doc_id: []const u8,
    score: f64,
    index_scores: []IndexScore,
    index_count: u32, // number of indexes this hit appeared in
};

// ============================================================================
// Fusion
// ============================================================================

/// Fuse multiple ranked result sets into a single scored list.
/// Caller owns the returned slice and all nested allocations (use freeHits).
pub fn fuse(alloc: Allocator, results: []const RankedResult, config: FusionConfig) ![]FusionHit {
    try validateConfigAndResults(results, config);
    return switch (config.strategy) {
        .rrf => rrfFuse(alloc, results, config),
        .rsf => rsfFuse(alloc, results, config),
    };
}

/// Free fusion hits returned by fuse().
pub fn freeHits(alloc: Allocator, hits: []FusionHit) void {
    for (hits) |h| {
        alloc.free(h.doc_id);
        for (h.index_scores) |is| {
            alloc.free(is.index_name);
        }
        alloc.free(h.index_scores);
    }
    alloc.free(hits);
}

fn getWeight(config: FusionConfig, index_name: []const u8) f64 {
    for (config.weights) |w| {
        if (std.mem.eql(u8, w.name, index_name)) return w.weight;
    }
    return 1.0;
}

fn validateConfigAndResults(results: []const RankedResult, config: FusionConfig) !void {
    if (!std.math.isFinite(config.rank_constant) or config.rank_constant <= 0) {
        return error.InvalidFusionConfig;
    }
    for (config.weights) |weight| {
        if (weight.name.len == 0 or !std.math.isFinite(weight.weight) or weight.weight < 0) {
            return error.InvalidFusionConfig;
        }
    }
    for (results) |result| {
        if (result.index_name.len == 0) return error.InvalidFusionInput;
        for (result.hits) |hit| {
            if (hit.doc_id.len == 0 or !std.math.isFinite(hit.score)) {
                return error.InvalidFusionInput;
            }
        }
    }
    if (results.len > 0) {
        for (results) |result| {
            if (getWeight(config, result.index_name) > 0) break;
        } else {
            return error.InvalidFusionConfig;
        }
    }
}

// ============================================================================
// RRF: Reciprocal Rank Fusion
// ============================================================================

fn rrfFuse(alloc: Allocator, results: []const RankedResult, config: FusionConfig) ![]FusionHit {
    const k = config.rank_constant;

    // Accumulate scores by doc_id
    var score_map = std.StringHashMapUnmanaged(AccumEntry).empty;
    defer {
        var it = score_map.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            for (entry.value_ptr.index_scores.items) |is| alloc.free(is.index_name);
            entry.value_ptr.index_scores.deinit(alloc);
        }
        score_map.deinit(alloc);
    }

    for (results) |result| {
        const w = getWeight(config, result.index_name);
        for (result.hits, 0..) |hit, rank| {
            const normalized_score = switch (result.kind) {
                .ranked => 1.0 / (k + @as(f64, @floatFromInt(rank)) + 1.0),
                .binary => 1.0,
            };
            const rrf_score = w * normalized_score;

            const gop = try score_map.getOrPut(alloc, hit.doc_id);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, hit.doc_id);
                gop.value_ptr.* = .{
                    .score = 0.0,
                    .index_scores = std.ArrayListUnmanaged(IndexScore).empty,
                    .index_count = 0,
                };
            }
            gop.value_ptr.score += rrf_score;
            gop.value_ptr.index_count += 1;
            try gop.value_ptr.index_scores.append(alloc, .{
                .index_name = try alloc.dupe(u8, result.index_name),
                .score = hit.score,
                .rank = @intCast(rank + 1),
                .normalized_score = normalized_score,
                .weight = w,
                .contribution = rrf_score,
            });
        }
    }

    return buildSortedHits(alloc, &score_map);
}

// ============================================================================
// RSF: Relative Score Fusion
// ============================================================================

fn rsfFuse(alloc: Allocator, results: []const RankedResult, config: FusionConfig) ![]FusionHit {
    var score_map = std.StringHashMapUnmanaged(AccumEntry).empty;
    defer {
        var it = score_map.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            for (entry.value_ptr.index_scores.items) |is| alloc.free(is.index_name);
            entry.value_ptr.index_scores.deinit(alloc);
        }
        score_map.deinit(alloc);
    }

    for (results) |result| {
        if (result.hits.len == 0) continue;

        const w = getWeight(config, result.index_name);
        const window: usize = if (config.window_size > 0)
            @min(config.window_size, result.hits.len)
        else
            result.hits.len;

        // Find min/max within window
        var min_score = result.hits[0].score;
        var max_score = result.hits[0].score;
        for (result.hits[1..@min(window, result.hits.len)]) |h| {
            if (h.score < min_score) min_score = h.score;
            if (h.score > max_score) max_score = h.score;
        }

        const score_range = max_score - min_score;

        for (result.hits, 0..) |hit, rank| {
            // Values beyond the calibration window may be below its minimum.
            // Clamp them so a source can never subtract relevance accidentally.
            const normalized = switch (result.kind) {
                .binary => 1.0,
                .ranked => if (score_range > 0)
                    std.math.clamp((hit.score - min_score) / score_range, 0.0, 1.0)
                else
                    1.0, // all same score
            };

            const contribution = w * normalized;

            const gop = try score_map.getOrPut(alloc, hit.doc_id);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, hit.doc_id);
                gop.value_ptr.* = .{
                    .score = 0.0,
                    .index_scores = std.ArrayListUnmanaged(IndexScore).empty,
                    .index_count = 0,
                };
            }
            gop.value_ptr.score += contribution;
            gop.value_ptr.index_count += 1;
            try gop.value_ptr.index_scores.append(alloc, .{
                .index_name = try alloc.dupe(u8, result.index_name),
                .score = hit.score,
                .rank = @intCast(rank + 1),
                .normalized_score = normalized,
                .weight = w,
                .contribution = contribution,
            });
        }
    }

    return buildSortedHits(alloc, &score_map);
}

// ============================================================================
// Shared helpers
// ============================================================================

const AccumEntry = struct {
    score: f64,
    index_scores: std.ArrayListUnmanaged(IndexScore),
    index_count: u32,
};

fn buildSortedHits(alloc: Allocator, score_map: *std.StringHashMapUnmanaged(AccumEntry)) ![]FusionHit {
    const count = score_map.count();
    if (count == 0) return try alloc.alloc(FusionHit, 0);

    var hits = try alloc.alloc(FusionHit, count);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| {
            alloc.free(hit.doc_id);
            for (hit.index_scores) |is| alloc.free(is.index_name);
            alloc.free(hit.index_scores);
        }
        alloc.free(hits);
    }

    var i: usize = 0;
    var it = score_map.iterator();
    while (it.next()) |entry| {
        var copied_index_scores = try alloc.alloc(IndexScore, entry.value_ptr.index_scores.items.len);
        errdefer {
            for (copied_index_scores[0..entry.value_ptr.index_scores.items.len]) |is| {
                if (is.index_name.len > 0) alloc.free(is.index_name);
            }
            alloc.free(copied_index_scores);
        }
        for (entry.value_ptr.index_scores.items, 0..) |is, j| {
            copied_index_scores[j] = .{
                .index_name = try alloc.dupe(u8, is.index_name),
                .score = is.score,
                .rank = is.rank,
                .normalized_score = is.normalized_score,
                .weight = is.weight,
                .contribution = is.contribution,
            };
        }
        hits[i] = .{
            .doc_id = try alloc.dupe(u8, entry.key_ptr.*),
            .score = entry.value_ptr.score,
            .index_scores = copied_index_scores,
            .index_count = entry.value_ptr.index_count,
        };
        i += 1;
        initialized += 1;
    }

    // Sort descending by score
    std.mem.sort(FusionHit, hits, {}, struct {
        fn cmp(_: void, a: FusionHit, b: FusionHit) bool {
            if (a.score != b.score) return a.score > b.score;
            return std.mem.order(u8, a.doc_id, b.doc_id) == .lt;
        }
    }.cmp);

    return hits;
}

// ============================================================================
// Pruner
// ============================================================================

pub const Pruner = struct {
    require_multi_index: bool = false,
    min_absolute_score: f64 = 0.0,
    min_score_ratio: f64 = 0.0,
    std_dev_threshold: f64 = 0.0, // 0 = disabled
    max_score_gap_percent: f64 = 0.0, // 0 = disabled

    /// Prune hits in-place. Returns the pruned slice (subset of input).
    /// Does NOT free removed hits — caller is responsible for the original allocation.
    pub fn prune(self: Pruner, hits: []FusionHit) []FusionHit {
        var result = hits;

        // 1. Require multi-index
        if (self.require_multi_index) {
            var write: usize = 0;
            for (result, 0..) |h, read_idx| {
                if (h.index_count >= 2) {
                    if (write != read_idx) std.mem.swap(FusionHit, &result[write], &result[read_idx]);
                    write += 1;
                }
            }
            result = result[0..write];
        }

        if (result.len == 0) return result;

        // 2. Min absolute score
        if (self.min_absolute_score > 0) {
            var write: usize = 0;
            for (result, 0..) |h, read_idx| {
                if (h.score >= self.min_absolute_score) {
                    if (write != read_idx) std.mem.swap(FusionHit, &result[write], &result[read_idx]);
                    write += 1;
                }
            }
            result = result[0..write];
        }

        if (result.len == 0) return result;

        // 3. Min score ratio
        if (self.min_score_ratio > 0) {
            const max_score = result[0].score; // already sorted desc
            const threshold = max_score * self.min_score_ratio;
            var write: usize = 0;
            for (result, 0..) |h, read_idx| {
                if (h.score >= threshold) {
                    if (write != read_idx) std.mem.swap(FusionHit, &result[write], &result[read_idx]);
                    write += 1;
                }
            }
            result = result[0..write];
        }

        if (result.len == 0) return result;

        // 4. Std-dev threshold (requires 3+ hits)
        if (self.std_dev_threshold > 0 and result.len >= 3) {
            var sum: f64 = 0;
            for (result) |h| sum += h.score;
            const mean = sum / @as(f64, @floatFromInt(result.len));

            var var_sum: f64 = 0;
            for (result) |h| {
                const diff = h.score - mean;
                var_sum += diff * diff;
            }
            const std_dev = @sqrt(var_sum / @as(f64, @floatFromInt(result.len)));
            const cutoff = mean - self.std_dev_threshold * std_dev;

            var write: usize = 0;
            for (result, 0..) |h, read_idx| {
                if (h.score >= cutoff) {
                    if (write != read_idx) std.mem.swap(FusionHit, &result[write], &result[read_idx]);
                    write += 1;
                }
            }
            result = result[0..write];
        }

        if (result.len == 0) return result;

        // 5. Max score gap percent (elbow detection)
        if (self.max_score_gap_percent > 0 and result.len >= 2) {
            var keep: usize = 1; // always keep first
            for (1..result.len) |j| {
                const prev = result[j - 1].score;
                const curr = result[j].score;
                if (prev > 0) {
                    const drop_pct = (prev - curr) / prev * 100.0;
                    if (drop_pct > self.max_score_gap_percent) break;
                }
                keep += 1;
            }
            result = result[0..keep];
        }

        return result;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "rrf basic fusion" {
    const alloc = std.testing.allocator;

    const text_hits: []const RankedHit = &.{
        .{ .doc_id = "doc1", .score = 10.0 },
        .{ .doc_id = "doc2", .score = 8.0 },
        .{ .doc_id = "doc3", .score = 5.0 },
    };
    const vec_hits: []const RankedHit = &.{
        .{ .doc_id = "doc2", .score = 0.95 },
        .{ .doc_id = "doc3", .score = 0.90 },
        .{ .doc_id = "doc4", .score = 0.85 },
    };

    const results: []const RankedResult = &.{
        .{ .index_name = "full_text", .hits = text_hits },
        .{ .index_name = "embedding", .hits = vec_hits },
    };

    const hits = try fuse(alloc, results, .{ .strategy = .rrf, .rank_constant = 60.0 });
    defer freeHits(alloc, hits);

    // doc2 appears in both — should have highest score
    try std.testing.expect(hits.len >= 4);
    try std.testing.expectEqualStrings("doc2", hits[0].doc_id);
    try std.testing.expect(hits[0].score > hits[1].score);
}

test "rsf normalization" {
    const alloc = std.testing.allocator;

    const text_hits: []const RankedHit = &.{
        .{ .doc_id = "doc1", .score = 100.0 },
        .{ .doc_id = "doc2", .score = 50.0 },
    };
    const vec_hits: []const RankedHit = &.{
        .{ .doc_id = "doc2", .score = 0.9 },
        .{ .doc_id = "doc1", .score = 0.1 },
    };

    const results: []const RankedResult = &.{
        .{ .index_name = "full_text", .hits = text_hits },
        .{ .index_name = "embedding", .hits = vec_hits },
    };

    const hits = try fuse(alloc, results, .{ .strategy = .rsf });
    defer freeHits(alloc, hits);

    // Both appear in both indexes, scores should be normalized
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    // doc1: text=1.0 (max), vec=0.0 (min) → 1.0
    // doc2: text=0.0 (min), vec=1.0 (max) → 1.0
    // Both should have similar scores
    try std.testing.expect(hits[0].score > 0.5);
    try std.testing.expect(hits[1].score > 0.5);
}

test "weighted fusion" {
    const alloc = std.testing.allocator;

    const text_hits: []const RankedHit = &.{
        .{ .doc_id = "doc1", .score = 10.0 },
    };
    const vec_hits: []const RankedHit = &.{
        .{ .doc_id = "doc2", .score = 0.9 },
    };

    const results: []const RankedResult = &.{
        .{ .index_name = "full_text", .hits = text_hits },
        .{ .index_name = "embedding", .hits = vec_hits },
    };

    const weights: []const NamedWeight = &.{
        .{ .name = "full_text", .weight = 2.0 },
        .{ .name = "embedding", .weight = 0.5 },
    };

    const hits = try fuse(alloc, results, .{
        .strategy = .rrf,
        .rank_constant = 60.0,
        .weights = weights,
    });
    defer freeHits(alloc, hits);

    try std.testing.expectEqual(@as(usize, 2), hits.len);
    // doc1 has weight 2.0 → should score higher
    try std.testing.expectEqualStrings("doc1", hits[0].doc_id);
    try std.testing.expectEqual(@as(u32, 1), hits[0].index_scores[0].rank);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0 / 61.0), hits[0].index_scores[0].contribution, 1e-12);
    try std.testing.expectApproxEqAbs(hits[0].score, hits[0].index_scores[0].contribution, 1e-12);
}

test "rsf clamps scores below the calibration window" {
    const alloc = std.testing.allocator;

    const source_hits: []const RankedHit = &.{
        .{ .doc_id = "top", .score = 10.0 },
        .{ .doc_id = "boundary", .score = 8.0 },
        .{ .doc_id = "tail", .score = 1.0 },
    };
    const results: []const RankedResult = &.{
        .{ .index_name = "semantic", .hits = source_hits },
    };

    const hits = try fuse(alloc, results, .{
        .strategy = .rsf,
        .window_size = 2,
        .weights = &.{.{ .name = "semantic", .weight = 0.5 }},
    });
    defer freeHits(alloc, hits);

    try std.testing.expectEqual(@as(usize, 3), hits.len);
    try std.testing.expectEqualStrings("top", hits[0].doc_id);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), hits[0].score, 1e-12);
    try std.testing.expectEqualStrings("boundary", hits[1].doc_id);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), hits[1].score, 1e-12);
    try std.testing.expectEqualStrings("tail", hits[2].doc_id);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), hits[2].score, 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), hits[2].index_scores[0].normalized_score, 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), hits[2].index_scores[0].contribution, 1e-12);
}

test "fusion uses document id as deterministic equal-score tie breaker" {
    const alloc = std.testing.allocator;

    const source_hits: []const RankedHit = &.{
        .{ .doc_id = "doc-b", .score = 1.0 },
        .{ .doc_id = "doc-a", .score = 1.0 },
    };
    const results: []const RankedResult = &.{
        .{ .index_name = "semantic", .hits = source_hits },
    };

    const hits = try fuse(alloc, results, .{ .strategy = .rsf });
    defer freeHits(alloc, hits);

    try std.testing.expectEqualStrings("doc-a", hits[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", hits[1].doc_id);
}

test "binary signal contributes a constant weight independent of rank" {
    const alloc = std.testing.allocator;

    const signal_hits: []const RankedHit = &.{
        .{ .doc_id = "doc-b", .score = 1.0 },
        .{ .doc_id = "doc-a", .score = 1.0 },
    };
    const results: []const RankedResult = &.{
        .{ .index_name = "in_stock", .hits = signal_hits, .kind = .binary },
    };
    const weights = [_]NamedWeight{.{ .name = "in_stock", .weight = 0.2 }};

    inline for ([_]FusionStrategy{ .rrf, .rsf }) |strategy| {
        const hits = try fuse(alloc, results, .{
            .strategy = strategy,
            .weights = &weights,
        });
        defer freeHits(alloc, hits);

        try std.testing.expectEqual(@as(usize, 2), hits.len);
        for (hits) |hit| {
            try std.testing.expectApproxEqAbs(@as(f64, 0.2), hit.score, 1e-12);
            try std.testing.expectApproxEqAbs(@as(f64, 1.0), hit.index_scores[0].normalized_score, 1e-12);
            try std.testing.expectApproxEqAbs(@as(f64, 0.2), hit.index_scores[0].contribution, 1e-12);
        }
    }
}

test "fusion rejects nonfinite scores and invalid calibration" {
    const alloc = std.testing.allocator;

    const invalid_hits: []const RankedHit = &.{
        .{ .doc_id = "doc", .score = std.math.nan(f64) },
    };
    const invalid_results: []const RankedResult = &.{
        .{ .index_name = "semantic", .hits = invalid_hits },
    };
    try std.testing.expectError(error.InvalidFusionInput, fuse(alloc, invalid_results, .{}));

    const valid_hits: []const RankedHit = &.{
        .{ .doc_id = "doc", .score = 1.0 },
    };
    const valid_results: []const RankedResult = &.{
        .{ .index_name = "semantic", .hits = valid_hits },
    };
    try std.testing.expectError(error.InvalidFusionConfig, fuse(alloc, valid_results, .{
        .rank_constant = 0,
    }));
    try std.testing.expectError(error.InvalidFusionConfig, fuse(alloc, valid_results, .{
        .weights = &.{.{ .name = "semantic", .weight = -1 }},
    }));
    try std.testing.expectError(error.InvalidFusionConfig, fuse(alloc, valid_results, .{
        .weights = &.{.{ .name = "semantic", .weight = 0 }},
    }));
}

test "pruner min_score_ratio" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(FusionHit, 3);
    defer alloc.free(hits);

    const empty_scores: []IndexScore = &.{};
    hits[0] = .{ .doc_id = "a", .score = 1.0, .index_scores = empty_scores, .index_count = 1 };
    hits[1] = .{ .doc_id = "b", .score = 0.6, .index_scores = empty_scores, .index_count = 1 };
    hits[2] = .{ .doc_id = "c", .score = 0.2, .index_scores = empty_scores, .index_count = 1 };

    const pruner = Pruner{ .min_score_ratio = 0.5 };
    const pruned = pruner.prune(hits);

    // Only a (1.0) and b (0.6 >= 0.5) should remain
    try std.testing.expectEqual(@as(usize, 2), pruned.len);
}

test "pruner std_dev_threshold" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(FusionHit, 4);
    defer alloc.free(hits);

    const empty_scores: []IndexScore = &.{};
    hits[0] = .{ .doc_id = "a", .score = 10.0, .index_scores = empty_scores, .index_count = 1 };
    hits[1] = .{ .doc_id = "b", .score = 9.0, .index_scores = empty_scores, .index_count = 1 };
    hits[2] = .{ .doc_id = "c", .score = 8.5, .index_scores = empty_scores, .index_count = 1 };
    hits[3] = .{ .doc_id = "d", .score = 1.0, .index_scores = empty_scores, .index_count = 1 }; // outlier

    const pruner = Pruner{ .std_dev_threshold = 1.0 };
    const pruned = pruner.prune(hits);

    // d is a significant outlier — should be pruned
    try std.testing.expect(pruned.len < 4);
    try std.testing.expect(pruned.len >= 2);
}

test "pruner max_score_gap_percent" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(FusionHit, 4);
    defer alloc.free(hits);

    const empty_scores: []IndexScore = &.{};
    hits[0] = .{ .doc_id = "a", .score = 1.0, .index_scores = empty_scores, .index_count = 1 };
    hits[1] = .{ .doc_id = "b", .score = 0.95, .index_scores = empty_scores, .index_count = 1 };
    hits[2] = .{ .doc_id = "c", .score = 0.3, .index_scores = empty_scores, .index_count = 1 }; // big gap
    hits[3] = .{ .doc_id = "d", .score = 0.25, .index_scores = empty_scores, .index_count = 1 };

    const pruner = Pruner{ .max_score_gap_percent = 50.0 };
    const pruned = pruner.prune(hits);

    // Gap from 0.95 to 0.3 is ~68% — should stop at c
    try std.testing.expectEqual(@as(usize, 2), pruned.len);
}

test "pruner require_multi_index" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(FusionHit, 3);
    defer alloc.free(hits);

    const empty_scores: []IndexScore = &.{};
    hits[0] = .{ .doc_id = "a", .score = 1.0, .index_scores = empty_scores, .index_count = 2 };
    hits[1] = .{ .doc_id = "b", .score = 0.8, .index_scores = empty_scores, .index_count = 1 };
    hits[2] = .{ .doc_id = "c", .score = 0.6, .index_scores = empty_scores, .index_count = 3 };

    const pruner = Pruner{ .require_multi_index = true };
    const pruned = pruner.prune(hits);

    // Only a (2 indexes) and c (3 indexes) should remain
    try std.testing.expectEqual(@as(usize, 2), pruned.len);
}

test "fusion empty results" {
    const alloc = std.testing.allocator;

    const results: []const RankedResult = &.{};
    const hits = try fuse(alloc, results, .{});
    defer freeHits(alloc, hits);

    try std.testing.expectEqual(@as(usize, 0), hits.len);
}
