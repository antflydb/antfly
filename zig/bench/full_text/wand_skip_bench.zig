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

//! WAND inner-loop profiler.
//!
//! Builds synthetic full-text indexes with controlled term distributions, runs
//! Block-Max WAND top-k queries, and reports the ratio of `iter.next()` calls
//! made while *advancing past* a non-pivoted doc vs while *fully scoring* a
//! pivoted doc. That ratio tells us whether a chunk-skipping `advanceTo()`
//! would be worth implementing — if advance dominates we'd save real wall
//! time, if score dominates the optimization is moot.
//!
//! Each scenario is run end-to-end and the counters embedded in `WANDScorer`
//! (`next_in_advance`, `next_in_score`, `pivots_*`) are read back.

const std = @import("std");
const antfly = @import("antfly-zig");
const inverted = antfly.inverted;
const scorer = antfly.scorer;
const platform_time = antfly.platform_time;

const Scenario = struct {
    name: []const u8,
    n_docs: u32,
    /// Selectivity of each term, in [0, 1]. Index aligns with `terms`.
    selectivities: []const f32,
    k: u32,
    /// Random seed — deterministic output across runs.
    seed: u64,
    /// When true, doc norms are bimodal — the first half of docs are
    /// "short" (norm 5, high BM25 TF) and the second half are "long"
    /// (norm 200, low BM25 TF). That gives Block-Max WAND an actual
    /// chunk-impact gradient to skip across once the top-k threshold
    /// rises. Uniform-random workloads don't exercise that path because
    /// every chunk has roughly the same max impact.
    skewed_norms: bool = false,
};

fn buildIndex(
    alloc: std.mem.Allocator,
    n_docs: u32,
    selectivities: []const f32,
    seed: u64,
    skewed_norms: bool,
) ![]u8 {
    var prng = std.Random.DefaultPrng.init(seed);
    const rng = prng.random();

    var builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 1024 });
    defer builder.deinit();

    // Stable term names: term0, term1, ...
    var name_buf: [16]u8 = undefined;

    var doc_id: u32 = 0;
    while (doc_id < n_docs) : (doc_id += 1) {
        // Each doc emits a vector of TermHits depending on selectivities.
        var hits = std.ArrayListUnmanaged(inverted.InvertedIndexBuilder.TermHit).empty;
        defer hits.deinit(alloc);

        // Bimodal doc-length: first half short (high TF score), second half
        // long (low TF score). Creates contiguous high-impact chunks early
        // in the doc-id space and low-impact chunks later.
        const norm: u32 = if (skewed_norms) (if (doc_id < n_docs / 2) 5 else 200) else 10;

        for (selectivities, 0..) |sel, term_idx| {
            if (rng.float(f32) >= sel) continue;
            const term = try std.fmt.bufPrint(&name_buf, "term{d}", .{term_idx});
            const owned_term = try alloc.dupe(u8, term);
            // Vary freq a little so block-max actually has work to do.
            const freq: u32 = 1 + rng.uintLessThan(u32, 5);
            try hits.append(alloc, .{
                .term = owned_term,
                .freq = freq,
                .norm = norm,
            });
        }
        try builder.addDocument(doc_id, hits.items);
        for (hits.items) |h| alloc.free(h.term);
    }

    return try builder.build();
}

const RunResult = struct {
    elapsed_ns: u64,
    next_in_advance: u64,
    next_in_score: u64,
    pivots_advanced: u64,
    pivots_scored: u64,
    chunks_skipped: u64,
    hits_returned: usize,
};

fn runScenario(alloc: std.mem.Allocator, sc: Scenario) !RunResult {
    const section = try buildIndex(alloc, sc.n_docs, sc.selectivities, sc.seed, sc.skewed_norms);
    defer alloc.free(section);

    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const avg_dl = reader.avgDocLen();

    // Build a top-k query over every term in the scenario.
    var s = scorer.WANDScorer.init(alloc, sc.k, reader.doc_count, avg_dl, .{});
    defer s.deinit();

    var name_buf: [16]u8 = undefined;
    for (sc.selectivities, 0..) |_, term_idx| {
        const term = try std.fmt.bufPrint(&name_buf, "term{d}", .{term_idx});
        const lookup = reader.lookup(term) orelse continue;
        const iter = try lookup.iterator(alloc);
        const bm = switch (lookup) {
            .postings => |p| p.block_max,
            .one_hit => null,
        };
        const cs = switch (lookup) {
            .postings => |p| p.chunk_size,
            .one_hit => 1024,
        };
        try s.addTerm(iter, lookup.docFreq(), bm, cs, 0);
    }

    const t0 = platform_time.monotonicNs();
    const results = try s.execute();
    const elapsed_ns: u64 = @intCast(platform_time.monotonicNs() - t0);
    defer alloc.free(results.hits);

    return .{
        .elapsed_ns = elapsed_ns,
        .next_in_advance = s.next_in_advance,
        .next_in_score = s.next_in_score,
        .pivots_advanced = s.pivots_advanced,
        .pivots_scored = s.pivots_scored,
        .chunks_skipped = s.chunks_skipped,
        .hits_returned = results.hits.len,
    };
}

fn fmtRatio(num: u64, denom: u64) f32 {
    if (denom == 0) return 0;
    return @as(f32, @floatFromInt(num)) / @as(f32, @floatFromInt(num + denom));
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var stdout_buf: [16 * 1024]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writerStreaming(init.io, &stdout_buf);
    defer stdout_w.interface.flush() catch {};
    const w = &stdout_w.interface;

    const scenarios = [_]Scenario{
        // Two-term, balanced — both terms in many docs. Pivots advance often.
        .{ .name = "balanced-2t-100K-k10",  .n_docs = 100_000, .selectivities = &.{ 0.30, 0.30 }, .k = 10,  .seed = 1 },
        .{ .name = "balanced-2t-100K-k1k",  .n_docs = 100_000, .selectivities = &.{ 0.30, 0.30 }, .k = 1000, .seed = 2 },
        // Two-term, skewed — one rare, one common. Classic WAND-friendly case.
        .{ .name = "skewed-2t-100K-k10",    .n_docs = 100_000, .selectivities = &.{ 0.01, 0.50 }, .k = 10,  .seed = 3 },
        .{ .name = "skewed-2t-100K-k100",   .n_docs = 100_000, .selectivities = &.{ 0.01, 0.50 }, .k = 100, .seed = 4 },
        // Three-term mix (typical "pizza near brooklyn" pattern: rare + medium + common).
        .{ .name = "mixed-3t-100K-k10",     .n_docs = 100_000, .selectivities = &.{ 0.01, 0.10, 0.50 }, .k = 10, .seed = 5 },
        // Large corpus, two-term skewed — where we'd expect chunk skipping to help most.
        .{ .name = "skewed-2t-1M-k10",      .n_docs = 1_000_000, .selectivities = &.{ 0.005, 0.30 }, .k = 10, .seed = 6 },
        // Small corpus — where the optimization shouldn't matter.
        .{ .name = "balanced-2t-5K-k10",    .n_docs = 5_000, .selectivities = &.{ 0.30, 0.30 }, .k = 10, .seed = 7 },
        // Bimodal-norm scenarios. First half of docs have norm=5 (short docs,
        // high TF), second half norm=200 (long, low TF). Once the threshold
        // rises after the first k high-impact docs, low-impact chunks
        // become skippable — this is the workload shape Block-Max chunk
        // skipping is designed for.
        .{ .name = "skewnorm-2t-100K-k10",  .n_docs = 100_000, .selectivities = &.{ 0.30, 0.30 }, .k = 10,  .seed = 8, .skewed_norms = true },
        .{ .name = "skewnorm-2t-1M-k10",    .n_docs = 1_000_000, .selectivities = &.{ 0.05, 0.30 }, .k = 10, .seed = 9, .skewed_norms = true },
    };

    try w.print("scenario                       n_docs   k    elapsed_ms  next_in_advance  next_in_score  advance%  pivots_adv  pivots_scored  chunks_skipped  hits\n", .{});
    try w.print("-----------------------------------------------------------------------------------------------------------------------------------------------------\n", .{});
    for (scenarios) |sc| {
        const r = try runScenario(alloc, sc);
        const advance_pct = fmtRatio(r.next_in_advance, r.next_in_score) * 100.0;
        try w.print("{s: <30} {d: >7}  {d: >3}  {d: >10.3}  {d: >15}  {d: >13}  {d: >7.2}%  {d: >10}  {d: >13}  {d: >14}  {d: >4}\n", .{
            sc.name,
            sc.n_docs,
            sc.k,
            @as(f64, @floatFromInt(r.elapsed_ns)) / 1_000_000.0,
            r.next_in_advance,
            r.next_in_score,
            advance_pct,
            r.pivots_advanced,
            r.pivots_scored,
            r.chunks_skipped,
            r.hits_returned,
        });
    }
}
