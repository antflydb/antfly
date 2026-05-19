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
const antfly = @import("antfly-zig");
const rabitq = antfly.rabitq;
const vec = antfly.vector;
const quantizer_mod = antfly.quantizer;
const svb = antfly.streamvbyte;
const inverted = antfly.inverted;
const scorer_mod = antfly.scorer;
const analysis_mod = antfly.analysis;
const roaring_mod = antfly.roaring;
const aggregation_mod = antfly.aggregation;
const platform_time = antfly.platform_time;

const dims = 384;
const width = rabitq.codeWidth(dims);
const num_vectors = 10_000;

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    const print = std.debug.print;

    print("RaBitQ Benchmark (dims={d}, vectors={d})\n", .{ dims, num_vectors });
    print("================================================\n\n", .{});

    var rng = std.Random.DefaultPrng.init(12345);
    const random = rng.random();

    // --- BitProduct benchmark ---
    {
        const code = try alloc.alloc(u64, width);
        const q1 = try alloc.alloc(u64, width);
        const q2 = try alloc.alloc(u64, width);
        const q3 = try alloc.alloc(u64, width);
        const q4 = try alloc.alloc(u64, width);

        for (code, q1, q2, q3, q4) |*c, *a, *b, *d, *e| {
            c.* = random.int(u64);
            a.* = random.int(u64);
            b.* = random.int(u64);
            d.* = random.int(u64);
            e.* = random.int(u64);
        }

        for (0..100) |_| {
            std.mem.doNotOptimizeAway(rabitq.bitProduct(code, q1, q2, q3, q4));
        }

        const start = nanotime();
        const bp_iters = 1000 * num_vectors;
        for (0..bp_iters) |_| {
            std.mem.doNotOptimizeAway(rabitq.bitProduct(code, q1, q2, q3, q4));
        }
        const elapsed = nanotime() - start;

        const ns_per_op = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(bp_iters));
        const ops_per_sec = @as(f64, @floatFromInt(bp_iters)) / (@as(f64, @floatFromInt(elapsed)) / 1e9);
        print("BitProduct ({d} u64s):\n", .{width});
        print("  {d:.1} ns/op, {d:.0} M ops/sec\n\n", .{ ns_per_op, ops_per_sec / 1e6 });
    }

    // --- Dot product benchmark ---
    {
        const a = try alloc.alloc(f32, dims);
        const b = try alloc.alloc(f32, dims);
        for (a, b) |*x, *y| {
            x.* = random.float(f32) * 2.0 - 1.0;
            y.* = random.float(f32) * 2.0 - 1.0;
        }

        const start = nanotime();
        const dot_iters = 1000 * num_vectors;
        for (0..dot_iters) |_| {
            std.mem.doNotOptimizeAway(vec.dot(a, b));
        }
        const elapsed = nanotime() - start;

        const ns_per_op = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(dot_iters));
        const ops_per_sec = @as(f64, @floatFromInt(dot_iters)) / (@as(f64, @floatFromInt(elapsed)) / 1e9);
        print("Dot Product ({d} dims):\n", .{dims});
        print("  {d:.1} ns/op, {d:.0} M ops/sec\n\n", .{ ns_per_op, ops_per_sec / 1e6 });
    }

    // --- Full quantize + search benchmark ---
    {
        var q = try quantizer_mod.RaBitQuantizer.init(alloc, dims, 42, .l2_squared);

        var centroid: [dims]f32 = undefined;
        @memset(&centroid, 0.0);

        const vector_data = try alloc.alloc(f32, num_vectors * dims);
        for (vector_data) |*v| {
            v.* = random.float(f32) * 2.0 - 1.0;
        }

        const quant_start = nanotime();
        var qs = try q.quantize(&centroid, vector_data, num_vectors);
        const quant_elapsed = nanotime() - quant_start;

        print("Quantize {d} vectors ({d} dims):\n", .{ num_vectors, dims });
        print("  {d:.2} ms total, {d:.1} us/vec\n\n", .{
            @as(f64, @floatFromInt(quant_elapsed)) / 1e6,
            @as(f64, @floatFromInt(quant_elapsed)) / @as(f64, @floatFromInt(num_vectors)) / 1e3,
        });

        const query = vector_data[0..dims];
        const distances = try alloc.alloc(f32, num_vectors);
        const error_bounds = try alloc.alloc(f32, num_vectors);

        for (0..10) |_| {
            try q.estimateDistances(&qs, query, distances, error_bounds);
        }

        const search_start = nanotime();
        const search_iters: usize = 1000;
        for (0..search_iters) |_| {
            try q.estimateDistances(&qs, query, distances, error_bounds);
        }
        const search_elapsed = nanotime() - search_start;

        const us_per_search = @as(f64, @floatFromInt(search_elapsed)) / @as(f64, @floatFromInt(search_iters)) / 1e3;
        print("EstimateDistances ({d} vectors, {d} dims):\n", .{ num_vectors, dims });
        print("  {d:.1} us/query ({d:.0} ns/vector)\n", .{
            us_per_search,
            us_per_search * 1e3 / @as(f64, @floatFromInt(num_vectors)),
        });

        qs.deinit(alloc);
        q.deinit();
    }

    print("\n", .{});

    // --- StreamVByte benchmark ---
    {
        const svb_count = 10_000;
        const svb_values = try alloc.alloc(u32, svb_count);

        // Mix of small and large values (typical location data)
        for (0..svb_count) |i| {
            svb_values[i] = switch (i % 5) {
                0 => random.int(u8), // small (fieldID, numAP)
                1 => random.int(u16), // medium (position)
                2, 3 => random.intRangeAtMost(u32, 0, 0xFFFFFF), // large (start/end offsets)
                else => random.int(u8),
            };
        }

        // Warmup
        for (0..10) |_| {
            const enc = try svb.encode(alloc, svb_values);
            alloc.free(enc.control);
            alloc.free(enc.data);
        }

        // Encode benchmark
        const enc_iters: usize = 10_000;
        const enc_start = nanotime();
        for (0..enc_iters) |_| {
            const enc = try svb.encode(alloc, svb_values);
            std.mem.doNotOptimizeAway(enc.control.ptr);
            alloc.free(enc.control);
            alloc.free(enc.data);
        }
        const enc_elapsed = nanotime() - enc_start;

        // One encode for decode benchmark
        const enc = try svb.encode(alloc, svb_values);

        // Decode benchmark
        const dec_dst = try alloc.alloc(u32, svb_count);
        const dec_iters: usize = 100_000;
        const dec_start = nanotime();
        for (0..dec_iters) |_| {
            std.mem.doNotOptimizeAway(svb.decodeInto(enc.control, enc.data, dec_dst));
        }
        const dec_elapsed = nanotime() - dec_start;

        const enc_ns = @as(f64, @floatFromInt(enc_elapsed)) / @as(f64, @floatFromInt(enc_iters));
        const dec_ns = @as(f64, @floatFromInt(dec_elapsed)) / @as(f64, @floatFromInt(dec_iters));
        const compression = @as(f64, @floatFromInt(svb_count * 4)) / @as(f64, @floatFromInt(enc.control.len + enc.data.len));

        print("StreamVByte ({d} uint32s):\n", .{svb_count});
        print("  Encode: {d:.1} us ({d:.1} ns/value)\n", .{ enc_ns / 1e3, enc_ns / @as(f64, @floatFromInt(svb_count)) });
        print("  Decode: {d:.1} us ({d:.1} ns/value)\n", .{ dec_ns / 1e3, dec_ns / @as(f64, @floatFromInt(svb_count)) });
        print("  Compression: {d:.2}x ({d} -> {d} bytes)\n", .{
            compression,
            svb_count * 4,
            enc.control.len + enc.data.len,
        });
    }

    print("\n", .{});
    print("Search Benchmarks\n", .{});
    print("================================================\n\n", .{});

    // --- Text Analysis benchmark ---
    {
        const sample_text =
            \\The quick brown fox jumped over the lazy dogs while running through
            \\the beautiful green meadows of the countryside where farmers were
            \\harvesting their abundant golden wheat crops during the warm autumn
            \\season before the cold winter months arrived bringing snow and ice
            \\across the entire northern hemisphere affecting millions of people
            \\who depend on agriculture for their daily sustenance and livelihood
            \\in communities scattered throughout the rural mountainous regions
        ;

        // Warmup
        for (0..100) |_| {
            const tokens = try analysis_mod.default_analyzer.analyze(alloc, sample_text);
            analysis_mod.Analyzer.freeTokens(alloc, tokens);
        }

        const ana_iters: usize = 10_000;
        const ana_start = nanotime();
        for (0..ana_iters) |_| {
            const tokens = try analysis_mod.default_analyzer.analyze(alloc, sample_text);
            std.mem.doNotOptimizeAway(tokens.ptr);
            analysis_mod.Analyzer.freeTokens(alloc, tokens);
        }
        const ana_elapsed = nanotime() - ana_start;

        const ana_us = @as(f64, @floatFromInt(ana_elapsed)) / @as(f64, @floatFromInt(ana_iters)) / 1e3;
        print("Text Analysis (English, ~70 words):\n", .{});
        print("  {d:.1} us/doc, {d:.0} K docs/sec\n\n", .{
            ana_us,
            @as(f64, @floatFromInt(ana_iters)) / (@as(f64, @floatFromInt(ana_elapsed)) / 1e9) / 1e3,
        });
    }

    // --- Roaring Bitmap benchmark ---
    {
        // Build a dense bitmap (100K elements)
        var dense = roaring_mod.RoaringBitmap.init(alloc);
        for (0..100_000) |i| {
            try dense.add(@intCast(i));
        }

        // Build a sparse bitmap (1K elements spread across range)
        var sparse = roaring_mod.RoaringBitmap.init(alloc);
        for (0..1_000) |i| {
            try sparse.add(@intCast(i * 100));
        }

        // Warmup
        for (0..100) |_| {
            var tmp = roaring_mod.RoaringBitmap.init(alloc);
            tmp.deinit();
        }

        // Union benchmark
        const union_iters: usize = 10_000;
        const union_start = nanotime();
        for (0..union_iters) |_| {
            var result = roaring_mod.RoaringBitmap.init(alloc);
            try result.orWith(&dense);
            try result.orWith(&sparse);
            std.mem.doNotOptimizeAway(result.cardinality());
            result.deinit();
        }
        const union_elapsed = nanotime() - union_start;

        // Intersection benchmark
        const isect_iters: usize = 10_000;
        const isect_start = nanotime();
        for (0..isect_iters) |_| {
            var result = roaring_mod.RoaringBitmap.init(alloc);
            try result.orWith(&dense);
            result.andWith(&sparse);
            std.mem.doNotOptimizeAway(result.cardinality());
            result.deinit();
        }
        const isect_elapsed = nanotime() - isect_start;

        // Iteration benchmark
        const iter_iters: usize = 1_000;
        const iter_start = nanotime();
        for (0..iter_iters) |_| {
            var it = dense.iterator();
            var count: u32 = 0;
            while (it.next()) |_| count += 1;
            std.mem.doNotOptimizeAway(count);
        }
        const iter_elapsed = nanotime() - iter_start;

        const union_ns = @as(f64, @floatFromInt(union_elapsed)) / @as(f64, @floatFromInt(union_iters));
        const isect_ns = @as(f64, @floatFromInt(isect_elapsed)) / @as(f64, @floatFromInt(isect_iters));
        const iter_ns_per_elem = @as(f64, @floatFromInt(iter_elapsed)) / @as(f64, @floatFromInt(iter_iters)) / 100_000.0;

        print("Roaring Bitmap (100K dense + 1K sparse):\n", .{});
        print("  Union:     {d:.1} us/op\n", .{union_ns / 1e3});
        print("  Intersect: {d:.1} us/op\n", .{isect_ns / 1e3});
        print("  Iterate:   {d:.1} ns/element ({d:.0} M elem/sec)\n\n", .{
            iter_ns_per_elem,
            1e9 / iter_ns_per_elem / 1e6,
        });

        dense.deinit();
        sparse.deinit();
    }

    // --- Aggregation benchmark ---
    {
        const agg_count: usize = 100_000;
        const agg_values = try alloc.alloc(f64, agg_count);
        for (0..agg_count) |i| {
            agg_values[i] = @as(f64, @floatFromInt(i % 1000)) + random.float(f64);
        }

        // StatsAgg collectChunk (SIMD)
        const stats_iters: usize = 1_000;
        const stats_start = nanotime();
        for (0..stats_iters) |_| {
            var stats = aggregation_mod.StatsAgg.init();
            stats.collectChunk(agg_values);
            std.mem.doNotOptimizeAway(stats.sum);
        }
        const stats_elapsed = nanotime() - stats_start;

        // HistogramAgg
        const hist_iters: usize = 100;
        const hist_start = nanotime();
        for (0..hist_iters) |_| {
            var hist = aggregation_mod.HistogramAgg.init(alloc, 10.0, 0.0);
            for (agg_values) |v| try hist.collect(v);
            std.mem.doNotOptimizeAway(hist.buckets.count());
            hist.deinit();
        }
        const hist_elapsed = nanotime() - hist_start;

        const stats_us = @as(f64, @floatFromInt(stats_elapsed)) / @as(f64, @floatFromInt(stats_iters)) / 1e3;
        const hist_us = @as(f64, @floatFromInt(hist_elapsed)) / @as(f64, @floatFromInt(hist_iters)) / 1e3;

        print("Aggregations (100K values):\n", .{});
        print("  StatsAgg (SIMD):  {d:.1} us ({d:.0} M elem/sec)\n", .{
            stats_us,
            @as(f64, @floatFromInt(agg_count)) / stats_us / 1e6 * 1e6,
        });
        print("  HistogramAgg:     {d:.1} us ({d:.0} M elem/sec)\n\n", .{
            hist_us,
            @as(f64, @floatFromInt(agg_count)) / hist_us / 1e6 * 1e6,
        });
    }

    // --- Inverted Index Read benchmark ---
    {
        const inv_doc_count: u32 = 5_000;
        var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
        defer builder.deinit();

        for (0..inv_doc_count) |i| {
            const doc_num: u32 = @intCast(i);
            const freq: u32 = @intCast((i % 10) + 1);
            const norm: u32 = @intCast((i % 50) + 5);
            try builder.addDocument(doc_num, &.{
                .{ .term = "benchmark", .freq = freq, .norm = norm },
            });
        }

        const section = try builder.build();

        var reader = try inverted.InvertedIndexReader.init(alloc, section);

        // Warmup
        for (0..10) |_| {
            const lookup = reader.lookup("benchmark") orelse continue;
            var it = try lookup.iterator(alloc);
            while (try it.next()) |hit| std.mem.doNotOptimizeAway(hit.doc_id);
            it.deinit();
        }

        const inv_iters: usize = 10_000;
        const inv_start = nanotime();
        for (0..inv_iters) |_| {
            const lookup = reader.lookup("benchmark") orelse continue;
            var it = try lookup.iterator(alloc);
            while (try it.next()) |hit| std.mem.doNotOptimizeAway(hit.doc_id);
            it.deinit();
        }
        const inv_elapsed = nanotime() - inv_start;

        const inv_us = @as(f64, @floatFromInt(inv_elapsed)) / @as(f64, @floatFromInt(inv_iters)) / 1e3;
        const ns_per_posting = inv_us * 1e3 / @as(f64, @floatFromInt(inv_doc_count));

        print("Inverted Index Read ({d} postings):\n", .{inv_doc_count});
        print("  {d:.1} us/iteration, {d:.1} ns/posting ({d:.0} M postings/sec)\n\n", .{
            inv_us,
            ns_per_posting,
            1e9 / ns_per_posting / 1e6,
        });
    }

    // --- BM25 Scorer benchmark ---
    {
        var gpa_state: std.heap.DebugAllocator(.{}) = .init;
        const gpa = gpa_state.allocator();

        const scorer_doc_count: u32 = 10_000;
        const scorer_terms = [_][]const u8{ "search", "engine", "fast" };

        var builder = inverted.InvertedIndexBuilder.init(gpa, .{});

        for (0..scorer_doc_count) |i| {
            const doc_num: u32 = @intCast(i);
            var hits_buf: [3]inverted.InvertedIndexBuilder.TermHit = undefined;
            var hit_count: usize = 0;
            // Every doc gets at least one term; some get 2 or 3
            for (scorer_terms, 0..) |term, ti| {
                if (ti == 0 or (random.int(u8) & 1) == 0) {
                    hits_buf[hit_count] = .{
                        .term = term,
                        .freq = @intCast((i % 5) + 1),
                        .norm = @intCast((i % 50) + 10),
                    };
                    hit_count += 1;
                }
            }
            try builder.addDocument(doc_num, hits_buf[0..hit_count]);
        }

        const section = try builder.build();
        builder.deinit();

        const reader = try inverted.InvertedIndexReader.init(gpa, section);
        const avg_dl = reader.avgDocLen();
        const doc_count = reader.doc_count;

        // Warmup
        for (0..10) |_| {
            var scorer = scorer_mod.WANDScorer.init(gpa, 10, doc_count, avg_dl, .{});
            for (scorer_terms) |term| {
                var lookup = reader.lookup(term) orelse continue;
                var block_max: ?inverted.BlockMaxInfo = null;
                var cs: u32 = 1024;
                switch (lookup) {
                    .postings => |p| {
                        block_max = p.block_max;
                        cs = p.chunk_size;
                    },
                    .one_hit => {},
                }
                const it = try lookup.iterator(gpa);
                try scorer.addTerm(it, lookup.docFreq(), block_max, cs, 0);
            }
            const results = try scorer.execute();
            std.mem.doNotOptimizeAway(results.hits.ptr);
            scorer.deinit();
        }

        const scorer_iters: usize = 1_000;
        const scorer_start = nanotime();
        for (0..scorer_iters) |_| {
            var scorer = scorer_mod.WANDScorer.init(gpa, 10, doc_count, avg_dl, .{});
            for (scorer_terms) |term| {
                var lookup = reader.lookup(term) orelse continue;
                var block_max: ?inverted.BlockMaxInfo = null;
                var cs: u32 = 1024;
                switch (lookup) {
                    .postings => |p| {
                        block_max = p.block_max;
                        cs = p.chunk_size;
                    },
                    .one_hit => {},
                }
                const it = try lookup.iterator(gpa);
                try scorer.addTerm(it, lookup.docFreq(), block_max, cs, 0);
            }
            const results = try scorer.execute();
            std.mem.doNotOptimizeAway(results.hits.ptr);
            scorer.deinit();
        }
        const scorer_elapsed = nanotime() - scorer_start;

        const scorer_us = @as(f64, @floatFromInt(scorer_elapsed)) / @as(f64, @floatFromInt(scorer_iters)) / 1e3;

        print("BM25 WAND Scorer ({d} docs, {d} terms, top-10):\n", .{ scorer_doc_count, scorer_terms.len });
        print("  {d:.1} us/query, {d:.0} K queries/sec\n", .{
            scorer_us,
            @as(f64, @floatFromInt(scorer_iters)) / (@as(f64, @floatFromInt(scorer_elapsed)) / 1e9) / 1e3,
        });
    }
}
