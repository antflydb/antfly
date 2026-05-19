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

//! Quickstart-shaped benchmark.
//!
//! Mirrors the per-iteration compute that the `test_text_quickstart_and_document_artifact`
//! E2E test exercises (e2e/antfly/test_quickstart.py): three documents are analyzed
//! and indexed, then a battery of full-text-search queries is run against the freshly
//! built segment.
//!
//! Stays inside the antfly-zig library: no HTTP server, no raft, no storage.
//! That intentionally undercounts the cost the real serverless API pays per
//! request (network, JSON parse/encode, planning, segment open, etc.) so the
//! numbers should be read as a *lower bound* on the per-query compute and
//! a useful comparison point for the per-primitive benchmarks in bench/bench.zig.

const std = @import("std");
const antfly = @import("antfly_quickstart_bench");

const analysis = antfly.analysis;
const inverted = antfly.inverted;
const scorer_mod = antfly.scorer;
const roaring_mod = antfly.roaring;
const platform_time = antfly.platform_time;
const vellum = antfly.vellum;

const QuickstartDoc = struct {
    doc_id: []const u8,
    body: []const u8,
};

const table_name = "docs";
const semantic_index_name = "semantic_idx";

const BenchMode = enum {
    micro,
    swarm_wiki,
};

const PublicSyncLevel = enum {
    propose,
    write,
    full_text,
    full_index,

    fn parse(raw: []const u8) ?PublicSyncLevel {
        if (std.mem.eql(u8, raw, "propose")) return .propose;
        if (std.mem.eql(u8, raw, "write")) return .write;
        if (std.mem.eql(u8, raw, "full_text")) return .full_text;
        if (std.mem.eql(u8, raw, "full_index")) return .full_index;
        return null;
    }

    fn text(self: PublicSyncLevel) []const u8 {
        return switch (self) {
            .propose => "propose",
            .write => "write",
            .full_text => "full_text",
            .full_index => "full_index",
        };
    }
};

const SwarmWikiConfig = struct {
    mode: BenchMode = .micro,
    dataset_path: []const u8 = "/Users/ajroetker/go/src/github.com/antflydb/antfly/wiki-articles-10k-v001.json",
    docs: usize = 10_000,
    batch_size: usize = 32,
    sync_level: PublicSyncLevel = .write,
    dims: usize = 512,
    model: []const u8 = "antflydb/clipclap",
    models_dir: []const u8 = "/Users/ajroetker/.termite/models",
    swarm_binary: []const u8 = "./zig-out/bin/antfly",
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 0,
    health_port: u16 = 0,
    index_ready_timeout_ms: u64 = 30 * std.time.ms_per_min,
    startup_timeout_ms: u64 = 120_000,
    poll_interval_ms: u64 = 250,
    load_progress_interval: usize = 1_000,
    query_repeats: usize = 3,
};

const WikiArticle = struct {
    url: []const u8 = "",
    title: []const u8 = "",
    body: []const u8 = "",
};

const WikiIndexStatusWire = struct {
    status: ?Status = null,

    const EnrichmentRuntime = struct {
        enabled: ?bool = null,
        target_sequence: ?u64 = null,
        applied_sequence: ?u64 = null,
        pending_sequence_count: ?u64 = null,
        processed_requests: ?u64 = null,
        retrying: ?bool = null,
        worker_failed: ?bool = null,
    };

    const Status = struct {
        doc_count: ?u64 = null,
        total_indexed: ?u64 = null,
        query_visible_doc_count: ?u64 = null,
        published_doc_count: ?u64 = null,
        replay_target_sequence: ?u64 = null,
        replay_applied_sequence: ?u64 = null,
        replay_catch_up_required: ?bool = null,
        dense_publish_pending: ?bool = null,
        runtime_fresh: ?bool = null,
        rebuilding: ?bool = null,
        backfill_active: ?bool = null,
        backfill_progress: ?f64 = null,
        enrichment_runtime: ?EnrichmentRuntime = null,
    };
};

const WikiVisibility = struct {
    doc_count: u64 = 0,
    total_indexed: u64 = 0,
    query_visible_doc_count: u64 = 0,
    published_doc_count: u64 = 0,
    replay_target_sequence: u64 = 0,
    replay_applied_sequence: u64 = 0,
    replay_catch_up_required: bool = false,
    dense_publish_pending: bool = false,
    runtime_fresh: bool = false,
    rebuilding: bool = false,
    backfill_active: bool = false,
    backfill_progress: f64 = 0,
    enrichment_enabled: bool = false,
    enrichment_target_sequence: u64 = 0,
    enrichment_applied_sequence: u64 = 0,
    enrichment_pending_sequence_count: u64 = 0,
    enrichment_processed_requests: u64 = 0,
    enrichment_retrying: bool = false,
    enrichment_worker_failed: bool = false,

    fn visibleDocs(self: WikiVisibility) u64 {
        return @max(self.query_visible_doc_count, self.published_doc_count);
    }

    fn enrichmentCaughtUp(self: WikiVisibility) bool {
        return !self.enrichment_enabled or
            (!self.enrichment_worker_failed and
                !self.enrichment_retrying and
                self.enrichment_applied_sequence >= self.enrichment_target_sequence and
                self.enrichment_pending_sequence_count == 0);
    }

    fn ready(self: WikiVisibility, expected_docs: usize) bool {
        return self.visibleDocs() >= expected_docs and
            self.total_indexed >= expected_docs and
            self.enrichmentCaughtUp() and
            !self.replay_catch_up_required and
            !self.dense_publish_pending and
            !self.rebuilding and
            !self.backfill_active;
    }
};

const WikiHttpResponse = struct {
    status: u16,
    body: []u8,

    fn deinit(self: *WikiHttpResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

const WikiHttpClient = struct {
    alloc: std.mem.Allocator,
    io_impl: *std.Io.Threaded,
    client: std.http.Client,

    fn init(alloc: std.mem.Allocator) WikiHttpClient {
        const io_impl = alloc.create(std.Io.Threaded) catch @panic("OOM");
        io_impl.* = std.Io.Threaded.init(alloc, .{ .stack_size = 1 * 1024 * 1024 });
        var out = WikiHttpClient{
            .alloc = alloc,
            .io_impl = io_impl,
            .client = undefined,
        };
        out.client = .{
            .allocator = alloc,
            .io = io_impl.io(),
            .read_buffer_size = 8 * 1024,
            .write_buffer_size = 16 * 1024,
        };
        return out;
    }

    fn deinit(self: *WikiHttpClient) void {
        self.client.deinit();
        self.io_impl.deinit();
        self.alloc.destroy(self.io_impl);
        self.* = undefined;
    }

    fn get(self: *WikiHttpClient, uri: []const u8) !WikiHttpResponse {
        return try self.request(.GET, uri, null, "");
    }

    fn postJson(self: *WikiHttpClient, uri: []const u8, body: []const u8) !WikiHttpResponse {
        return try self.request(.POST, uri, "application/json", body);
    }

    fn request(
        self: *WikiHttpClient,
        method: std.http.Method,
        uri_raw: []const u8,
        content_type: ?[]const u8,
        body: []const u8,
    ) !WikiHttpResponse {
        const uri = try std.Uri.parse(uri_raw);
        var extra_headers_buf: [1]std.http.Header = undefined;
        const extra_headers: []const std.http.Header = if (content_type) |ct| blk: {
            extra_headers_buf[0] = .{ .name = "content-type", .value = ct };
            break :blk extra_headers_buf[0..1];
        } else &.{};

        var req = try std.http.Client.request(&self.client, method, uri, .{
            .extra_headers = extra_headers,
            .keep_alive = true,
        });
        defer req.deinit();

        if (body.len > 0 or method.requestHasBody()) {
            req.transfer_encoding = .{ .content_length = body.len };
            var body_buffer: [16 * 1024]u8 = undefined;
            var body_writer = try req.sendBodyUnflushed(&body_buffer);
            if (body.len > 0) try body_writer.writer.writeAll(body);
            try body_writer.end();
            try req.connection.?.flush();
        } else {
            try req.sendBodiless();
        }

        var response = try req.receiveHead(&.{});
        var transfer_buffer: [1024]u8 = undefined;
        const response_body = try response.reader(&transfer_buffer).allocRemaining(self.alloc, .limited(16 << 20));
        return .{
            .status = @intFromEnum(response.head.status),
            .body = response_body,
        };
    }
};

// The text path of `test_text_quickstart_and_document_artifact` ingests
// these three documents (each body is the single word the test searches for).
const quickstart_docs = [_]QuickstartDoc{
    .{ .doc_id = "theory-relativity", .body = "relativity" },
    .{ .doc_id = "ancient-rome", .body = "rome" },
    .{ .doc_id = "machine-learning", .body = "learning" },
};

const Stats = struct {
    label: []const u8,
    iters: u64,
    elapsed_ns: u64,

    fn nsPerOp(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / @as(f64, @floatFromInt(self.iters));
    }

    fn opsPerSec(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.iters)) / (@as(f64, @floatFromInt(self.elapsed_ns)) / 1e9);
    }
};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

fn print(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

// Build an inverted-text section from the canned quickstart documents using the
// shared default English analyzer. Each call returns owned bytes the caller
// must free.
fn buildQuickstartSegment(alloc: std.mem.Allocator) ![]u8 {
    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    var hits_buf: std.ArrayListUnmanaged(inverted.InvertedIndexBuilder.TermHit) = .empty;
    defer hits_buf.deinit(alloc);

    for (quickstart_docs, 0..) |doc, i| {
        const tokens = try analysis.default_analyzer.analyze(alloc, doc.body);
        defer analysis.Analyzer.freeTokens(alloc, tokens);

        hits_buf.clearRetainingCapacity();
        const norm: u32 = @intCast(tokens.len);
        for (tokens) |tok| {
            try hits_buf.append(alloc, .{
                .term = tok.term,
                .freq = 1,
                .norm = norm,
            });
        }
        try builder.addDocument(@intCast(i), hits_buf.items);
    }

    return try builder.build();
}

// Execute a BM25 match query the same way the table query handler does:
// analyze the query text, then feed each analyzed term into the WAND scorer.
// Returns the number of hits returned (used to assert the fixture is
// producing the expected top hit).
fn runBm25Search(
    alloc: std.mem.Allocator,
    reader: *const inverted.InvertedIndexReader,
    query_text: []const u8,
    limit: u32,
) !usize {
    const tokens = try analysis.default_analyzer.analyze(alloc, query_text);
    defer analysis.Analyzer.freeTokens(alloc, tokens);

    var scorer = scorer_mod.WANDScorer.init(
        alloc,
        limit,
        reader.doc_count,
        reader.avgDocLen(),
        .{},
    );
    defer scorer.deinit();

    for (tokens) |tok| {
        var lookup = reader.lookup(tok.term) orelse continue;
        var block_max: ?inverted.BlockMaxInfo = null;
        var chunk_size: u32 = 1024;
        switch (lookup) {
            .postings => |p| {
                block_max = p.block_max;
                chunk_size = p.chunk_size;
            },
            .one_hit => {},
        }
        const it = try lookup.iterator(alloc);
        try scorer.addTerm(it, lookup.docFreq(), block_max, chunk_size, 0);
    }

    const results = try scorer.execute();
    defer alloc.free(results.hits);
    return results.hits.len;
}

// Doc bitmap for an analyzed multi-term query, taking the union of postings
// across all analyzed tokens. This is what the filter / exclusion / prefix
// branches of the public query path build before applying them to the scored
// hit set.
fn termsDocBitmap(
    alloc: std.mem.Allocator,
    reader: *const inverted.InvertedIndexReader,
    query_text: []const u8,
) !roaring_mod.RoaringBitmap {
    const tokens = try analysis.default_analyzer.analyze(alloc, query_text);
    defer analysis.Analyzer.freeTokens(alloc, tokens);

    var bitmap = roaring_mod.RoaringBitmap.init(alloc);
    errdefer bitmap.deinit();

    for (tokens) |tok| {
        const lookup = reader.lookup(tok.term) orelse continue;
        switch (lookup) {
            .postings => |p| {
                var bm = try p.docBitmap(alloc);
                defer bm.deinit();
                try bitmap.orWith(&bm);
            },
            .one_hit => |h| try bitmap.add(h.doc_num),
        }
    }
    return bitmap;
}

// Mirror the prefix search variant in the quickstart (`prefix:{field=body, text=rel}`):
// walk the term FST starting at `prefix`, OR the matching doc bitmaps together,
// and report the resulting cardinality.
fn runPrefixFilter(
    alloc: std.mem.Allocator,
    reader: *const inverted.InvertedIndexReader,
    prefix: []const u8,
) !usize {
    var aut = vellum.StartsWith{ .prefix = prefix };
    var term_iter = try reader.fstSearchIterator(aut.automaton());
    defer term_iter.deinit();

    var bitmap = roaring_mod.RoaringBitmap.init(alloc);
    defer bitmap.deinit();

    while (try term_iter.next()) |entry| {
        switch (entry.result) {
            .postings => |p| {
                var bm = try p.docBitmap(alloc);
                defer bm.deinit();
                try bitmap.orWith(&bm);
            },
            .one_hit => |h| try bitmap.add(h.doc_num),
        }
    }
    return bitmap.cardinality();
}

// Model the filter+exclusion search variant: score docs via BM25, then
// constrain by a filter bitmap and subtract an exclusion bitmap. This is the
// shape of `filter_query` + `exclusion_query` in the public query handler.
fn runFilteredBm25Search(
    alloc: std.mem.Allocator,
    reader: *const inverted.InvertedIndexReader,
    query_text: []const u8,
    filter_query: []const u8,
    exclusion_query: []const u8,
    limit: u32,
) !usize {
    const tokens = try analysis.default_analyzer.analyze(alloc, query_text);
    defer analysis.Analyzer.freeTokens(alloc, tokens);

    var scorer = scorer_mod.WANDScorer.init(
        alloc,
        limit,
        reader.doc_count,
        reader.avgDocLen(),
        .{},
    );
    defer scorer.deinit();

    for (tokens) |tok| {
        var lookup = reader.lookup(tok.term) orelse continue;
        var block_max: ?inverted.BlockMaxInfo = null;
        var chunk_size: u32 = 1024;
        switch (lookup) {
            .postings => |p| {
                block_max = p.block_max;
                chunk_size = p.chunk_size;
            },
            .one_hit => {},
        }
        const it = try lookup.iterator(alloc);
        try scorer.addTerm(it, lookup.docFreq(), block_max, chunk_size, 0);
    }

    const results = try scorer.execute();
    defer alloc.free(results.hits);

    var filter_bm = try termsDocBitmap(alloc, reader, filter_query);
    defer filter_bm.deinit();
    var exclude_bm = try termsDocBitmap(alloc, reader, exclusion_query);
    defer exclude_bm.deinit();

    // Apply filter ∩ ¬exclusion to the scored hits and count survivors.
    var kept: usize = 0;
    for (results.hits) |hit| {
        if (!filter_bm.contains(hit.doc_id)) continue;
        if (exclude_bm.contains(hit.doc_id)) continue;
        kept += 1;
    }
    return kept;
}

// One full quickstart "transaction": analyze + index three docs, then run all
// six query variants the e2e test issues. The numbers reflect the per-segment
// compute the public query path does *inside* the inverted-index/scorer
// stack — JSON parsing, planner, hybrid merge, doc-id projection, prefix
// filter against doc-id strings, etc. all live above this layer and aren't
// modeled.
fn runOneQuickstartIteration(alloc: std.mem.Allocator) !u64 {
    const start = nanotime();

    const section = try buildQuickstartSegment(alloc);
    defer alloc.free(section);

    var reader = try inverted.InvertedIndexReader.init(alloc, section);

    // Mirror the searches issued by `test_text_quickstart_and_document_artifact`:
    //   1. server-side BM25 search for "relativity"
    //   2. public hybrid query for body:relativity
    //   3. direct match query (body:relativity)
    //   4. direct prefix query (body:rel)
    //   5. filtered query: body:relativity OR body:rome,
    //      filter=same, exclusion=body:rome (modeled with roaring filter+exclude)
    //   6. prefix filtered search: text="relativity", filter_prefix="theory-"
    //      (modeled as BM25 + a placeholder doc-id prefix check; the actual
    //      doc-id projection happens above this layer)
    _ = try runBm25Search(alloc, &reader, "relativity", 3);
    _ = try runBm25Search(alloc, &reader, "relativity", 3);
    _ = try runBm25Search(alloc, &reader, "relativity", 3);
    _ = try runPrefixFilter(alloc, &reader, "rel");
    _ = try runFilteredBm25Search(alloc, &reader, "relativity rome", "relativity rome", "rome", 3);
    _ = try runBm25Search(alloc, &reader, "relativity", 3);

    return nanotime() - start;
}

fn warmupQuickstart(alloc: std.mem.Allocator, iters: usize) !void {
    for (0..iters) |_| {
        _ = try runOneQuickstartIteration(alloc);
    }
}

fn benchAnalyzeOnly(alloc: std.mem.Allocator, iters: usize) !Stats {
    const start = nanotime();
    var total_tokens: usize = 0;
    for (0..iters) |_| {
        for (quickstart_docs) |doc| {
            const tokens = try analysis.default_analyzer.analyze(alloc, doc.body);
            total_tokens += tokens.len;
            analysis.Analyzer.freeTokens(alloc, tokens);
        }
    }
    std.mem.doNotOptimizeAway(total_tokens);
    return .{
        .label = "analyze 3 quickstart bodies",
        .iters = @intCast(iters),
        .elapsed_ns = nanotime() - start,
    };
}

fn benchIndexBuildOnly(alloc: std.mem.Allocator, iters: usize) !Stats {
    const start = nanotime();
    for (0..iters) |_| {
        const section = try buildQuickstartSegment(alloc);
        std.mem.doNotOptimizeAway(section.ptr);
        alloc.free(section);
    }
    return .{
        .label = "build inverted segment (3 docs)",
        .iters = @intCast(iters),
        .elapsed_ns = nanotime() - start,
    };
}

fn benchFstBuilderOverhead(alloc: std.mem.Allocator, iters: usize) !void {
    // Empty FST: pure init+finish overhead at the default registry size.
    var t0 = nanotime();
    for (0..iters) |_| {
        var fst = try vellum.Builder.init(alloc, .{});
        const bytes = try fst.finish();
        std.mem.doNotOptimizeAway(bytes.ptr);
        alloc.free(bytes);
        fst.deinit();
    }
    const empty_default_ns = nanotime() - t0;

    // Empty FST with a tiny registry. Same code path; only the
    // registry_table_size differs, so any delta is the registry alloc/memset.
    t0 = nanotime();
    for (0..iters) |_| {
        var fst = try vellum.Builder.init(alloc, .{ .registry_table_size = 4 });
        const bytes = try fst.finish();
        std.mem.doNotOptimizeAway(bytes.ptr);
        alloc.free(bytes);
        fst.deinit();
    }
    const empty_tiny_ns = nanotime() - t0;

    // Three single-token quickstart inserts at the default registry size, to
    // confirm the per-key work itself is cheap.
    t0 = nanotime();
    const term1 = "learn";
    const term2 = "relativ";
    const term3 = "rome";
    for (0..iters) |_| {
        var fst = try vellum.Builder.init(alloc, .{});
        try fst.insert(term1, 0);
        try fst.insert(term2, 1);
        try fst.insert(term3, 2);
        const bytes = try fst.finish();
        std.mem.doNotOptimizeAway(bytes.ptr);
        alloc.free(bytes);
        fst.deinit();
    }
    const three_default_ns = nanotime() - t0;

    // Same three keys with the tiny registry.
    t0 = nanotime();
    for (0..iters) |_| {
        var fst = try vellum.Builder.init(alloc, .{ .registry_table_size = 4 });
        try fst.insert(term1, 0);
        try fst.insert(term2, 1);
        try fst.insert(term3, 2);
        const bytes = try fst.finish();
        std.mem.doNotOptimizeAway(bytes.ptr);
        alloc.free(bytes);
        fst.deinit();
    }
    const three_tiny_ns = nanotime() - t0;

    const n = @as(f64, @floatFromInt(iters));
    print("Vellum FST builder overhead ({d} samples each):\n", .{iters});
    print("  init+finish, no inserts, registry=10000 (default) : {d:.2} us\n", .{
        @as(f64, @floatFromInt(empty_default_ns)) / n / 1e3,
    });
    print("  init+finish, no inserts, registry=4               : {d:.2} us\n", .{
        @as(f64, @floatFromInt(empty_tiny_ns)) / n / 1e3,
    });
    print("  init+3 inserts+finish, registry=10000             : {d:.2} us\n", .{
        @as(f64, @floatFromInt(three_default_ns)) / n / 1e3,
    });
    print("  init+3 inserts+finish, registry=4                 : {d:.2} us\n\n", .{
        @as(f64, @floatFromInt(three_tiny_ns)) / n / 1e3,
    });
}

fn benchIndexBuildBreakdown(alloc: std.mem.Allocator, iters: usize) !void {
    var analyze_ns: u64 = 0;
    var add_doc_ns: u64 = 0;
    var build_ns: u64 = 0;
    var deinit_ns: u64 = 0;

    var hits_buf: std.ArrayListUnmanaged(inverted.InvertedIndexBuilder.TermHit) = .empty;
    defer hits_buf.deinit(alloc);

    for (0..iters) |_| {
        var builder = inverted.InvertedIndexBuilder.init(alloc, .{});

        for (quickstart_docs, 0..) |doc, i| {
            const t0 = nanotime();
            const tokens = try analysis.default_analyzer.analyze(alloc, doc.body);
            analyze_ns += nanotime() - t0;

            hits_buf.clearRetainingCapacity();
            const norm: u32 = @intCast(tokens.len);
            for (tokens) |tok| {
                try hits_buf.append(alloc, .{ .term = tok.term, .freq = 1, .norm = norm });
            }

            const t1 = nanotime();
            try builder.addDocument(@intCast(i), hits_buf.items);
            add_doc_ns += nanotime() - t1;

            analysis.Analyzer.freeTokens(alloc, tokens);
        }

        const t2 = nanotime();
        const section = try builder.build();
        build_ns += nanotime() - t2;

        const t3 = nanotime();
        alloc.free(section);
        builder.deinit();
        deinit_ns += nanotime() - t3;
    }

    const n = @as(f64, @floatFromInt(iters));
    print("Index build breakdown (per quickstart segment, {d} samples):\n", .{iters});
    print("  analyze        : {d:.2} us\n", .{@as(f64, @floatFromInt(analyze_ns)) / n / 1e3});
    print("  builder.add x3 : {d:.2} us\n", .{@as(f64, @floatFromInt(add_doc_ns)) / n / 1e3});
    print("  builder.build  : {d:.2} us\n", .{@as(f64, @floatFromInt(build_ns)) / n / 1e3});
    print("  free + deinit  : {d:.2} us\n\n", .{@as(f64, @floatFromInt(deinit_ns)) / n / 1e3});
}

fn benchSingleSearch(alloc: std.mem.Allocator, iters: usize) !Stats {
    const section = try buildQuickstartSegment(alloc);
    defer alloc.free(section);
    var reader = try inverted.InvertedIndexReader.init(alloc, section);

    const start = nanotime();
    for (0..iters) |_| {
        const hits = try runBm25Search(alloc, &reader, "relativity", 3);
        std.mem.doNotOptimizeAway(hits);
    }
    return .{
        .label = "BM25 single-term search ('relativity')",
        .iters = @intCast(iters),
        .elapsed_ns = nanotime() - start,
    };
}

fn benchPrefixSearch(alloc: std.mem.Allocator, iters: usize) !Stats {
    const section = try buildQuickstartSegment(alloc);
    defer alloc.free(section);
    var reader = try inverted.InvertedIndexReader.init(alloc, section);

    const start = nanotime();
    for (0..iters) |_| {
        const card = try runPrefixFilter(alloc, &reader, "rel");
        std.mem.doNotOptimizeAway(card);
    }
    return .{
        .label = "FST prefix scan ('rel')",
        .iters = @intCast(iters),
        .elapsed_ns = nanotime() - start,
    };
}

fn benchFullQuickstart(alloc: std.mem.Allocator, iters: usize) !Stats {
    var elapsed_total: u64 = 0;
    for (0..iters) |_| {
        elapsed_total += try runOneQuickstartIteration(alloc);
    }
    return .{
        .label = "quickstart inverted+scorer transaction (analyze+build+6 queries; HTTP/JSON/planner/projection NOT modeled)",
        .iters = @intCast(iters),
        .elapsed_ns = elapsed_total,
    };
}

fn printStats(s: Stats) void {
    print("{s}:\n", .{s.label});
    const ns_per_op = s.nsPerOp();
    if (ns_per_op >= 1_000_000.0) {
        print(
            "  {d:.2} ms/op, {d:.0} ops/sec\n\n",
            .{ ns_per_op / 1e6, s.opsPerSec() },
        );
    } else if (ns_per_op >= 1_000.0) {
        print(
            "  {d:.2} us/op, {d:.0} K ops/sec\n\n",
            .{ ns_per_op / 1e3, s.opsPerSec() / 1e3 },
        );
    } else {
        print(
            "  {d:.1} ns/op, {d:.0} M ops/sec\n\n",
            .{ ns_per_op, s.opsPerSec() / 1e6 },
        );
    }
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseQuickstartBenchArgs(init.minimal.args);
    if (cfg.mode == .swarm_wiki) {
        try runSwarmWikiBench(alloc, init.io, cfg);
        return;
    }

    try runMicroQuickstartBench(alloc);
}

fn runMicroQuickstartBench(alloc: std.mem.Allocator) !void {
    print("Quickstart Benchmark (mirrors test_text_quickstart_and_document_artifact)\n", .{});
    print("================================================\n", .{});
    print("Workload: 3 documents, default English analyzer, 6 query variants\n\n", .{});

    // Warm up so allocator caches and JIT-style branch predictor state aren't
    // charged against the first sample.
    try warmupQuickstart(alloc, 50);

    const analyze = try benchAnalyzeOnly(alloc, 100_000);
    printStats(analyze);

    const build_seg = try benchIndexBuildOnly(alloc, 50_000);
    printStats(build_seg);

    try benchIndexBuildBreakdown(alloc, 50_000);
    try benchFstBuilderOverhead(alloc, 50_000);

    const single = try benchSingleSearch(alloc, 200_000);
    printStats(single);

    const prefix = try benchPrefixSearch(alloc, 200_000);
    printStats(prefix);

    const full = try benchFullQuickstart(alloc, 20_000);
    printStats(full);

    // Sanity check: with the canned fixture the searches should always return
    // exactly one document for "relativity"; bail loudly if they don't, so the
    // benchmark doesn't silently regress into measuring an empty-result path.
    const section = try buildQuickstartSegment(alloc);
    defer alloc.free(section);
    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const hits = try runBm25Search(alloc, &reader, "relativity", 3);
    if (hits != 1) {
        print("warning: expected 1 hit for 'relativity', got {d}\n", .{hits});
    }
    const prefix_card = try runPrefixFilter(alloc, &reader, "rel");
    if (prefix_card != 1) {
        print("warning: expected prefix 'rel' to match 1 doc, got {d}\n", .{prefix_card});
    }
    // The filter+exclusion path: BM25 over "relativity OR rome", filtered by
    // the same set, exclusion=rome. The fixture should leave exactly one
    // document (theory-relativity). If this returns 0 or 2 the bench is
    // measuring the wrong code path.
    const filtered_kept = try runFilteredBm25Search(
        alloc,
        &reader,
        "relativity rome",
        "relativity rome",
        "rome",
        3,
    );
    if (filtered_kept != 1) {
        print("warning: expected filter+exclude to keep 1 doc, got {d}\n", .{filtered_kept});
    }

    // ------------------------------------------------------------------
    // Reference primitives at the same scales as bench/bench.zig so the
    // quickstart numbers above can be read against their constituents in
    // a single run. (The main `bench` step pulls in OpenAPI codegen and
    // doesn't build in environments without the sister antfly repo, so we
    // duplicate just the text/search primitives here.)
    // ------------------------------------------------------------------
    print("Reference Primitives (same scale as bench/bench.zig)\n", .{});
    print("================================================\n\n", .{});

    try benchTextAnalysisRef(alloc);
    try benchInvertedReadRef(alloc);
    try benchBm25WandRef(alloc);

    print("Notes:\n", .{});
    print("  * Scope: this is an inverted-index/scorer microbenchmark shaped\n", .{});
    print("    around the quickstart, NOT a full e2e replay. Specifically NOT\n", .{});
    print("    modeled: HTTP, JSON parse/encode, query planner, hybrid merge,\n", .{});
    print("    doc-id projection, doc-id prefix filter (filter_prefix=...),\n", .{});
    print("    rerankers, dense/sparse vector indexes, document_segment encoding.\n", .{});
    print("  * Modeled: text analysis, inverted-index build/read, BM25 WAND scoring,\n", .{});
    print("    FST prefix scan, and roaring filter+exclusion intersection (the\n", .{});
    print("    filter_query/exclusion_query branch of the public query handler).\n", .{});
    print("  * Reference primitives match bench/bench.zig: 70-word paragraph,\n", .{});
    print("    5K postings inverted read, 10K-doc 3-term BM25 WAND.\n", .{});
}

fn parseQuickstartBenchArgs(args_in: std.process.Args) !SwarmWikiConfig {
    var cfg = SwarmWikiConfig{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.next() orelse return cfg;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            if (std.mem.eql(u8, raw, "micro")) {
                cfg.mode = .micro;
            } else if (std.mem.eql(u8, raw, "swarm-wiki")) {
                cfg.mode = .swarm_wiki;
            } else {
                return error.InvalidArgument;
            }
        } else if (std.mem.eql(u8, arg, "--dataset")) {
            cfg.dataset_path = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--docs") or std.mem.eql(u8, arg, "--limit")) {
            cfg.docs = try parseBenchNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseBenchNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = PublicSyncLevel.parse(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--dims") or std.mem.eql(u8, arg, "--dimension")) {
            cfg.dims = try parseBenchNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--model")) {
            cfg.model = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--models-dir")) {
            cfg.models_dir = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--swarm-binary")) {
            cfg.swarm_binary = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--host")) {
            cfg.bind_host = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--port")) {
            cfg.bind_port = try parseBenchNextU16(&args, arg);
        } else if (std.mem.eql(u8, arg, "--health-port")) {
            cfg.health_port = try parseBenchNextU16(&args, arg);
        } else if (std.mem.eql(u8, arg, "--index-ready-timeout-ms")) {
            cfg.index_ready_timeout_ms = try parseBenchNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--startup-timeout-ms")) {
            cfg.startup_timeout_ms = try parseBenchNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--poll-interval-ms")) {
            cfg.poll_interval_ms = try parseBenchNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--load-progress-interval")) {
            cfg.load_progress_interval = try parseBenchNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--query-repeats")) {
            cfg.query_repeats = try parseBenchNextUsize(&args, arg);
        } else {
            return error.InvalidArgument;
        }
    }
    return cfg;
}

fn runSwarmWikiBench(alloc: std.mem.Allocator, io: std.Io, input_cfg: SwarmWikiConfig) !void {
    var cfg = input_cfg;
    if (cfg.docs == 0 or cfg.batch_size == 0 or cfg.dims == 0) return error.InvalidArgument;

    const port_base: u16 = if (cfg.bind_port != 0) cfg.bind_port else 30_000 + @as(u16, @intCast(platform_time.monotonicNs() % 10_000));
    if (cfg.bind_port == 0) cfg.bind_port = port_base;
    if (cfg.health_port == 0) cfg.health_port = port_base + 1;

    var root_buf: [256]u8 = undefined;
    const root_path = std.fmt.bufPrintZ(&root_buf, "/tmp/antfly-quickstart-wiki-{d}", .{platform_time.monotonicNs()}) catch unreachable;
    defer cleanupTempDir(root_path);

    const cwd = try std.process.currentPathAlloc(io, alloc);
    defer alloc.free(cwd);

    var child = try spawnWikiSwarm(alloc, io, cwd, cfg, root_path[0..root_path.len]);
    const child_pid = child.id orelse return error.UnexpectedProcessExit;
    var child_live = true;
    defer if (child_live) {
        terminateWikiSwarm(io, &child);
    };

    const base_uri = try std.fmt.allocPrint(alloc, "http://{s}:{d}/api/v1", .{ cfg.bind_host, cfg.bind_port });
    defer alloc.free(base_uri);
    const health_uri = try std.fmt.allocPrint(alloc, "http://{s}:{d}/healthz", .{ cfg.bind_host, cfg.health_port });
    defer alloc.free(health_uri);
    const metrics_uri = try std.fmt.allocPrint(alloc, "http://{s}:{d}/metrics", .{ cfg.bind_host, cfg.health_port });
    defer alloc.free(metrics_uri);
    const status_uri = try std.fmt.allocPrint(alloc, "{s}/tables/{s}/indexes/{s}", .{ base_uri, table_name, semantic_index_name });
    defer alloc.free(status_uri);

    try waitForHttpOk(alloc, health_uri, cfg.startup_timeout_ms);

    var client = WikiHttpClient.init(alloc);
    defer client.deinit();

    try createWikiTableAndIndex(alloc, &client, base_uri, cfg);

    const load_started = nanotime();
    const loaded = try loadWikiDataset(alloc, io, &client, base_uri, cfg, child_pid, health_uri, metrics_uri, status_uri);
    const load_ns = nanotime() - load_started;
    print(
        "quickstart_wiki_swarm load complete dataset={s} docs={d} batch_size={d} sync_level={s} load_s={d:.2}\n",
        .{
            cfg.dataset_path,
            loaded,
            cfg.batch_size,
            cfg.sync_level.text(),
            nsToSeconds(load_ns),
        },
    );

    const wait_started = nanotime();
    const visibility = try waitForWikiIndexReady(alloc, &client, base_uri, loaded, cfg.index_ready_timeout_ms);
    const wait_ns = nanotime() - wait_started;

    const query_stats = try runWikiQueries(alloc, &client, base_uri, cfg);
    const rss = sampleRssBytes(alloc, io, child_pid) catch 0;

    print(
        "quickstart_wiki_swarm dataset={s} docs={d} batch_size={d} sync_level={s} dims={d} model={s} load_s={d:.2} index_wait_s={d:.2} query_count={d} avg_query_ms={d:.2} max_query_ms={d:.2} rss_mb={d:.2} visible={d} total_indexed={d} enrichment={d}/{d} replay={d}/{d} runtime_fresh={any}\n",
        .{
            cfg.dataset_path,
            loaded,
            cfg.batch_size,
            cfg.sync_level.text(),
            cfg.dims,
            cfg.model,
            nsToSeconds(load_ns),
            nsToSeconds(wait_ns),
            query_stats.count,
            nsToMs(query_stats.avgNs()),
            nsToMs(query_stats.max_ns),
            bytesToMiB(rss),
            visibility.visibleDocs(),
            visibility.total_indexed,
            visibility.enrichment_applied_sequence,
            visibility.enrichment_target_sequence,
            visibility.replay_applied_sequence,
            visibility.replay_target_sequence,
            visibility.runtime_fresh,
        },
    );

    child_live = false;
    terminateWikiSwarm(io, &child);
}

fn terminateWikiSwarm(io: std.Io, child: *std.process.Child) void {
    const pid = child.id orelse return;
    std.posix.kill(pid, .TERM) catch {};
    _ = child.wait(io) catch {};
}

fn spawnWikiSwarm(
    alloc: std.mem.Allocator,
    io: std.Io,
    cwd: []const u8,
    cfg: SwarmWikiConfig,
    root_path: []const u8,
) !std.process.Child {
    const bind_port_arg = try std.fmt.allocPrint(alloc, "{d}", .{cfg.bind_port});
    defer alloc.free(bind_port_arg);
    const health_port_arg = try std.fmt.allocPrint(alloc, "{d}", .{cfg.health_port});
    defer alloc.free(health_port_arg);
    const replica_root = try std.fmt.allocPrint(alloc, "{s}/replicas", .{root_path});
    defer alloc.free(replica_root);
    const replica_catalog = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{root_path});
    defer alloc.free(replica_catalog);
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{root_path});
    defer alloc.free(snapshot_root);

    return try std.process.spawn(io, .{
        .argv = &.{
            cfg.swarm_binary,
            "swarm",
            "--host",
            cfg.bind_host,
            "--port",
            bind_port_arg,
            "--health-port",
            health_port_arg,
            "--tick-ms",
            "5",
            "--models-dir",
            cfg.models_dir,
            "--replica-root-dir",
            replica_root,
            "--replica-catalog-path",
            replica_catalog,
            "--snapshot-root-dir",
            snapshot_root,
        },
        .cwd = .{ .path = cwd },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
}

fn createWikiTableAndIndex(
    alloc: std.mem.Allocator,
    client: *WikiHttpClient,
    base_uri: []const u8,
    cfg: SwarmWikiConfig,
) !void {
    const table_body = "{\"num_shards\":1,\"description\":\"quickstart wiki antfly benchmark\"}";
    const table_path = try std.fmt.allocPrint(alloc, "/tables/{s}", .{table_name});
    defer alloc.free(table_path);
    const table_resp = try postJsonExpect(alloc, client, base_uri, table_path, table_body, &.{ 200, 201 });
    defer alloc.free(table_resp);

    var index_body = std.ArrayListUnmanaged(u8).empty;
    defer index_body.deinit(alloc);
    try index_body.appendSlice(alloc, "{\"name\":");
    try appendJsonString(alloc, &index_body, semantic_index_name);
    try index_body.appendSlice(alloc, ",\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":");
    try index_body.print(alloc, "{d}", .{cfg.dims});
    try index_body.appendSlice(alloc, ",\"embedder\":{\"provider\":\"antfly\",\"model\":");
    try appendJsonString(alloc, &index_body, cfg.model);
    try index_body.appendSlice(alloc, "}}");

    const index_path = try std.fmt.allocPrint(alloc, "/tables/{s}/indexes/{s}", .{ table_name, semantic_index_name });
    defer alloc.free(index_path);
    const index_resp = try postJsonExpect(alloc, client, base_uri, index_path, index_body.items, &.{ 200, 201 });
    defer alloc.free(index_resp);
}

fn loadWikiDataset(
    alloc: std.mem.Allocator,
    io: std.Io,
    client: *WikiHttpClient,
    base_uri: []const u8,
    cfg: SwarmWikiConfig,
    pid: std.process.Child.Id,
    health_uri: []const u8,
    metrics_uri: []const u8,
    status_uri: []const u8,
) !usize {
    var file = try std.Io.Dir.openFileAbsolute(io, cfg.dataset_path, .{});
    defer file.close(io);

    const reader_buf = try alloc.alloc(u8, 16 * 1024 * 1024);
    defer alloc.free(reader_buf);
    var reader = file.reader(io, reader_buf);
    var batch = std.ArrayListUnmanaged(u8).empty;
    defer batch.deinit(alloc);

    const batch_path = try std.fmt.allocPrint(alloc, "/tables/{s}/batch", .{table_name});
    defer alloc.free(batch_path);

    var loaded: usize = 0;
    var batch_count: usize = 0;
    try startWikiBatch(alloc, &batch, cfg.sync_level);
    while (loaded < cfg.docs) {
        const raw = (try reader.interface.takeDelimiter('\n')) orelse break;
        const line = std.mem.trim(u8, raw, " \t\r\n");
        if (line.len == 0) continue;

        var parsed = std.json.parseFromSlice(WikiArticle, alloc, line, .{ .ignore_unknown_fields = true }) catch |err| {
            print("quickstart_wiki_swarm skipping malformed line={d} err={s}\n", .{ loaded + 1, @errorName(err) });
            continue;
        };
        defer parsed.deinit();

        if (batch_count != 0) try batch.append(alloc, ',');
        try appendWikiInsert(alloc, &batch, loaded, parsed.value);
        batch_count += 1;
        loaded += 1;

        if (batch_count >= cfg.batch_size) {
            try finishAndPostWikiBatch(alloc, client, base_uri, batch_path, &batch);
            batch_count = 0;
            try startWikiBatch(alloc, &batch, cfg.sync_level);
        }
        if (cfg.load_progress_interval > 0 and loaded % cfg.load_progress_interval == 0) {
            const rss = sampleRssBytes(alloc, io, pid) catch 0;
            const visibility = fetchWikiVisibility(alloc, base_uri) catch WikiVisibility{};
            print(
                "quickstart_wiki_swarm load progress docs={d}/{d} rss_mb={d:.2} visible={d} indexed={d} enrichment={d}/{d} replay={d}/{d}\n",
                .{
                    loaded,
                    cfg.docs,
                    bytesToMiB(rss),
                    visibility.visibleDocs(),
                    visibility.total_indexed,
                    visibility.enrichment_applied_sequence,
                    visibility.enrichment_target_sequence,
                    visibility.replay_applied_sequence,
                    visibility.replay_target_sequence,
                },
            );
            _ = httpGetStatus(alloc, health_uri) catch {};
            _ = httpGetStatus(alloc, metrics_uri) catch {};
            _ = httpGetStatus(alloc, status_uri) catch {};
        }
    }
    if (batch_count > 0) try finishAndPostWikiBatch(alloc, client, base_uri, batch_path, &batch);
    return loaded;
}

fn startWikiBatch(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), sync_level: PublicSyncLevel) !void {
    out.clearRetainingCapacity();
    try out.print(alloc, "{{\"sync_level\":\"{s}\",\"inserts\":{{", .{sync_level.text()});
}

fn finishAndPostWikiBatch(
    alloc: std.mem.Allocator,
    client: *WikiHttpClient,
    base_uri: []const u8,
    batch_path: []const u8,
    batch: *std.ArrayListUnmanaged(u8),
) !void {
    try batch.appendSlice(alloc, "}}");
    const resp = try postJsonExpect(alloc, client, base_uri, batch_path, batch.items, &.{ 200, 201 });
    defer alloc.free(resp);
}

fn appendWikiInsert(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), doc_idx: usize, article: WikiArticle) !void {
    try out.print(alloc, "\"doc:{d:0>8}\":{{\"url\":", .{doc_idx});
    try appendJsonString(alloc, out, article.url);
    try out.appendSlice(alloc, ",\"title\":");
    try appendJsonString(alloc, out, article.title);
    try out.appendSlice(alloc, ",\"body\":");
    try appendJsonString(alloc, out, article.body);
    try out.append(alloc, '}');
}

fn waitForWikiIndexReady(
    alloc: std.mem.Allocator,
    client: *WikiHttpClient,
    base_uri: []const u8,
    expected_docs: usize,
    timeout_ms: u64,
) !WikiVisibility {
    const deadline = nanotime() + timeout_ms * std.time.ns_per_ms;
    var last = WikiVisibility{};
    while (nanotime() < deadline) {
        last = try fetchWikiVisibilityWithClient(alloc, client, base_uri);
        if (last.ready(expected_docs)) return last;
        sleepMs(500);
    }
    print(
        "quickstart_wiki_swarm index wait timeout visible={d} indexed={d} enrichment={d}/{d} enrichment_pending={d} enrichment_retrying={any} enrichment_failed={any} replay={d}/{d} catch_up={any} publish_pending={any} backfill_active={any} progress={d:.3}\n",
        .{
            last.visibleDocs(),
            last.total_indexed,
            last.enrichment_applied_sequence,
            last.enrichment_target_sequence,
            last.enrichment_pending_sequence_count,
            last.enrichment_retrying,
            last.enrichment_worker_failed,
            last.replay_applied_sequence,
            last.replay_target_sequence,
            last.replay_catch_up_required,
            last.dense_publish_pending,
            last.backfill_active,
            last.backfill_progress,
        },
    );
    return error.Timeout;
}

fn fetchWikiVisibility(alloc: std.mem.Allocator, base_uri: []const u8) !WikiVisibility {
    var client = WikiHttpClient.init(alloc);
    defer client.deinit();
    return try fetchWikiVisibilityWithClient(alloc, &client, base_uri);
}

fn fetchWikiVisibilityWithClient(alloc: std.mem.Allocator, client: *WikiHttpClient, base_uri: []const u8) !WikiVisibility {
    const uri = try std.fmt.allocPrint(alloc, "{s}/tables/{s}/indexes/{s}", .{ base_uri, table_name, semantic_index_name });
    defer alloc.free(uri);
    var detail = try client.get(uri);
    defer detail.deinit(alloc);
    var parsed = try std.json.parseFromSlice(WikiIndexStatusWire, alloc, detail.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const status = parsed.value.status orelse return .{};
    const total_indexed = status.total_indexed orelse 0;
    const doc_count = status.doc_count orelse total_indexed;
    const query_visible = status.query_visible_doc_count orelse doc_count;
    const enrichment = status.enrichment_runtime;
    const enrichment_enabled = if (enrichment) |stats| stats.enabled orelse false else false;
    const enrichment_target = if (enrichment) |stats| stats.target_sequence orelse 0 else 0;
    const enrichment_applied = if (enrichment) |stats| stats.applied_sequence orelse 0 else 0;
    return .{
        .doc_count = doc_count,
        .total_indexed = total_indexed,
        .query_visible_doc_count = query_visible,
        .published_doc_count = status.published_doc_count orelse query_visible,
        .replay_target_sequence = status.replay_target_sequence orelse 0,
        .replay_applied_sequence = status.replay_applied_sequence orelse 0,
        .replay_catch_up_required = status.replay_catch_up_required orelse false,
        .dense_publish_pending = status.dense_publish_pending orelse false,
        .runtime_fresh = status.runtime_fresh orelse false,
        .rebuilding = status.rebuilding orelse false,
        .backfill_active = status.backfill_active orelse false,
        .backfill_progress = status.backfill_progress orelse 0,
        .enrichment_enabled = enrichment_enabled,
        .enrichment_target_sequence = enrichment_target,
        .enrichment_applied_sequence = enrichment_applied,
        .enrichment_pending_sequence_count = if (enrichment) |stats| stats.pending_sequence_count orelse (enrichment_target -| enrichment_applied) else 0,
        .enrichment_processed_requests = if (enrichment) |stats| stats.processed_requests orelse 0 else 0,
        .enrichment_retrying = if (enrichment) |stats| stats.retrying orelse false else false,
        .enrichment_worker_failed = if (enrichment) |stats| stats.worker_failed orelse false else false,
    };
}

const WikiQueryStats = struct {
    count: usize = 0,
    total_ns: u64 = 0,
    max_ns: u64 = 0,

    fn avgNs(self: WikiQueryStats) u64 {
        if (self.count == 0) return 0;
        return self.total_ns / self.count;
    }
};

fn runWikiQueries(alloc: std.mem.Allocator, client: *WikiHttpClient, base_uri: []const u8, cfg: SwarmWikiConfig) !WikiQueryStats {
    const queries = [_][]const u8{
        "history of science and mathematics",
        "ancient cities and architecture",
        "machine learning and computer systems",
    };
    var stats = WikiQueryStats{};
    for (0..cfg.query_repeats) |_| {
        for (queries) |query_text| {
            const body = try makeWikiSemanticQueryBody(alloc, query_text);
            defer alloc.free(body);
            const started = nanotime();
            const uri = try std.fmt.allocPrint(alloc, "{s}/tables/{s}/query", .{ base_uri, table_name });
            defer alloc.free(uri);
            var resp = try client.postJson(uri, body);
            defer resp.deinit(alloc);
            if (resp.status != 200) {
                print("quickstart_wiki_swarm query status={d} body={s}\n", .{ resp.status, resp.body });
                return error.UnexpectedHttpStatus;
            }
            const elapsed = nanotime() - started;
            stats.count += 1;
            stats.total_ns += elapsed;
            stats.max_ns = @max(stats.max_ns, elapsed);
        }
    }
    return stats;
}

fn makeWikiSemanticQueryBody(alloc: std.mem.Allocator, query_text: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"semantic_search\":");
    try appendJsonString(alloc, &out, query_text);
    try out.appendSlice(alloc, ",\"indexes\":[");
    try appendJsonString(alloc, &out, semantic_index_name);
    try out.appendSlice(alloc, "],\"limit\":10,\"profile\":true}");
    return try out.toOwnedSlice(alloc);
}

fn postJsonExpect(
    alloc: std.mem.Allocator,
    client: *WikiHttpClient,
    base_uri: []const u8,
    path: []const u8,
    body: []const u8,
    expected_statuses: []const u16,
) ![]u8 {
    const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_uri, path });
    defer alloc.free(uri);
    var resp = try client.postJson(uri, body);
    defer resp.deinit(alloc);
    for (expected_statuses) |status| {
        if (resp.status == status) return try alloc.dupe(u8, resp.body);
    }
    print("quickstart_wiki_swarm unexpected status={d} uri={s} body={s}\n", .{ resp.status, uri, resp.body });
    return error.UnexpectedHttpStatus;
}

fn waitForHttpOk(alloc: std.mem.Allocator, uri: []const u8, timeout_ms: u64) !void {
    var client = WikiHttpClient.init(alloc);
    defer client.deinit();
    const deadline = nanotime() + timeout_ms * std.time.ns_per_ms;
    var successes: usize = 0;
    while (nanotime() < deadline) {
        const status = httpGetStatusWithClient(&client, uri) catch {
            successes = 0;
            sleepMs(100);
            continue;
        };
        if (status == 200) {
            successes += 1;
            if (successes >= 2) return;
        } else {
            successes = 0;
        }
        sleepMs(100);
    }
    return error.Timeout;
}

fn httpGetStatus(alloc: std.mem.Allocator, uri: []const u8) !u16 {
    var client = WikiHttpClient.init(alloc);
    defer client.deinit();
    return try httpGetStatusWithClient(&client, uri);
}

fn httpGetStatusWithClient(client: *WikiHttpClient, uri: []const u8) !u16 {
    var resp = try client.get(uri);
    defer resp.deinit(client.alloc);
    return resp.status;
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try out.append(alloc, '"');
    for (value) |c| {
        switch (c) {
            '"' => try out.appendSlice(alloc, "\\\""),
            '\\' => try out.appendSlice(alloc, "\\\\"),
            '\n' => try out.appendSlice(alloc, "\\n"),
            '\r' => try out.appendSlice(alloc, "\\r"),
            '\t' => try out.appendSlice(alloc, "\\t"),
            0x08 => try out.appendSlice(alloc, "\\b"),
            0x0c => try out.appendSlice(alloc, "\\f"),
            else => if (c < 0x20) {
                try out.print(alloc, "\\u{x:0>4}", .{c});
            } else {
                try out.append(alloc, c);
            },
        }
    }
    try out.append(alloc, '"');
}

fn parseBenchNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseBenchNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn parseBenchNextU16(args: *std.process.Args.Iterator, flag: []const u8) !u16 {
    const raw = args.next() orelse {
        print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u16, raw, 10);
}

fn sampleRssBytes(alloc: std.mem.Allocator, io: std.Io, pid: std.process.Child.Id) !usize {
    const pid_arg = try std.fmt.allocPrint(alloc, "{d}", .{@as(i64, @intCast(pid))});
    defer alloc.free(pid_arg);
    const result = try std.process.run(alloc, io, .{
        .argv = &.{ "ps", "-p", pid_arg, "-o", "rss=" },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.UnexpectedProcessExit,
        else => return error.UnexpectedProcessExit,
    }
    const trimmed = std.mem.trim(u8, result.stdout, " \t\r\n");
    if (trimmed.len == 0) return 0;
    return (try std.fmt.parseInt(usize, trimmed, 10)) * 1024;
}

fn cleanupTempDir(path: [:0]u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const raw = path[0..path.len];
    const parent_path = std.fs.path.dirname(raw) orelse return;
    const name = std.fs.path.basename(raw);
    var parent = std.Io.Dir.openDirAbsolute(io_impl.io(), parent_path, .{}) catch return;
    defer parent.close(io_impl.io());
    parent.deleteTree(io_impl.io(), name) catch {};
}

fn sleepMs(ms: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(ms / std.time.ms_per_s),
        .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}

fn nsToSeconds(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e9;
}

fn bytesToMiB(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

const reference_text =
    \\The quick brown fox jumped over the lazy dogs while running through
    \\the beautiful green meadows of the countryside where farmers were
    \\harvesting their abundant golden wheat crops during the warm autumn
    \\season before the cold winter months arrived bringing snow and ice
    \\across the entire northern hemisphere affecting millions of people
    \\who depend on agriculture for their daily sustenance and livelihood
    \\in communities scattered throughout the rural mountainous regions
;

fn benchTextAnalysisRef(alloc: std.mem.Allocator) !void {
    for (0..100) |_| {
        const tokens = try analysis.default_analyzer.analyze(alloc, reference_text);
        analysis.Analyzer.freeTokens(alloc, tokens);
    }
    const iters: usize = 10_000;
    const start = nanotime();
    for (0..iters) |_| {
        const tokens = try analysis.default_analyzer.analyze(alloc, reference_text);
        std.mem.doNotOptimizeAway(tokens.ptr);
        analysis.Analyzer.freeTokens(alloc, tokens);
    }
    const elapsed = nanotime() - start;
    const us_per_doc = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters)) / 1e3;
    print("Text Analysis (English, ~70 words):\n", .{});
    print("  {d:.1} us/doc, {d:.0} K docs/sec\n\n", .{
        us_per_doc,
        @as(f64, @floatFromInt(iters)) / (@as(f64, @floatFromInt(elapsed)) / 1e9) / 1e3,
    });
}

fn benchInvertedReadRef(alloc: std.mem.Allocator) !void {
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
    defer alloc.free(section);

    var reader = try inverted.InvertedIndexReader.init(alloc, section);

    for (0..10) |_| {
        const lookup = reader.lookup("benchmark") orelse continue;
        var it = try lookup.iterator(alloc);
        while (try it.next()) |hit| std.mem.doNotOptimizeAway(hit.doc_id);
        it.deinit();
    }

    const iters: usize = 10_000;
    const start = nanotime();
    for (0..iters) |_| {
        const lookup = reader.lookup("benchmark") orelse continue;
        var it = try lookup.iterator(alloc);
        while (try it.next()) |hit| std.mem.doNotOptimizeAway(hit.doc_id);
        it.deinit();
    }
    const elapsed = nanotime() - start;
    const us = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters)) / 1e3;
    const ns_per_posting = us * 1e3 / @as(f64, @floatFromInt(inv_doc_count));
    print("Inverted Index Read ({d} postings):\n", .{inv_doc_count});
    print("  {d:.1} us/iteration, {d:.1} ns/posting ({d:.0} M postings/sec)\n\n", .{
        us, ns_per_posting, 1e9 / ns_per_posting / 1e6,
    });
}

fn benchBm25WandRef(alloc: std.mem.Allocator) !void {
    const scorer_doc_count: u32 = 10_000;
    const terms = [_][]const u8{ "search", "engine", "fast" };

    var rng = std.Random.DefaultPrng.init(12345);
    const random = rng.random();

    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    for (0..scorer_doc_count) |i| {
        const doc_num: u32 = @intCast(i);
        var hits_buf: [3]inverted.InvertedIndexBuilder.TermHit = undefined;
        var hit_count: usize = 0;
        for (terms, 0..) |term, ti| {
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
    defer alloc.free(section);

    const reader = try inverted.InvertedIndexReader.init(alloc, section);
    const avg_dl = reader.avgDocLen();
    const doc_count = reader.doc_count;

    for (0..10) |_| {
        var scorer = scorer_mod.WANDScorer.init(alloc, 10, doc_count, avg_dl, .{});
        for (terms) |term| {
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
            const it = try lookup.iterator(alloc);
            try scorer.addTerm(it, lookup.docFreq(), block_max, cs, 0);
        }
        const results = try scorer.execute();
        std.mem.doNotOptimizeAway(results.hits.ptr);
        alloc.free(results.hits);
        scorer.deinit();
    }

    const iters: usize = 1_000;
    const start = nanotime();
    for (0..iters) |_| {
        var scorer = scorer_mod.WANDScorer.init(alloc, 10, doc_count, avg_dl, .{});
        for (terms) |term| {
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
            const it = try lookup.iterator(alloc);
            try scorer.addTerm(it, lookup.docFreq(), block_max, cs, 0);
        }
        const results = try scorer.execute();
        std.mem.doNotOptimizeAway(results.hits.ptr);
        alloc.free(results.hits);
        scorer.deinit();
    }
    const elapsed = nanotime() - start;
    const us = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters)) / 1e3;
    print("BM25 WAND Scorer ({d} docs, 3 terms, top-10):\n", .{scorer_doc_count});
    print("  {d:.1} us/query, {d:.0} K queries/sec\n\n", .{
        us,
        @as(f64, @floatFromInt(iters)) / (@as(f64, @floatFromInt(elapsed)) / 1e9) / 1e3,
    });
}
