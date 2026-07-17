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
pub const antfly = @import("antfly-zig");

pub const types = antfly.db.types;

pub const index_name = "search_benchmark";
pub const text_field = "text";
pub const analyzer_name = "simple";
// Manifest values use f64 so the declared decimal configuration remains
// readable; the production scorer consumes the equivalent f32 defaults.
pub const default_bm25_k1: f64 = 1.2;
pub const default_bm25_b: f64 = 0.75;

pub const index_config_json =
    "{\"type\":\"full_text\",\"analysis_config\":{\"field_analyzers\":{\"text\":\"simple\"}}}";

pub const ParsedArgs = struct {
    db_path: []const u8,
    max_text_bytes: ?usize = null,
    segment_mode: SegmentMode = .production,
    allow_legacy_query_syntax: bool = false,
    manifest_path: ?[]const u8 = null,
    bm25_k1: f64 = default_bm25_k1,
    bm25_b: f64 = default_bm25_b,
    merge_max_segments_per_tier: ?u32 = null,
    merge_max_at_once: ?u32 = null,
    postings_chunk_size: ?u32 = null,
};

pub const SegmentMode = enum {
    production,
    single,
};

pub fn parseArgs(args_in: std.process.Args) !ParsedArgs {
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    const db_path = args.next() orelse return error.MissingDatabasePath;
    var parsed = ParsedArgs{ .db_path = db_path };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--max-text-bytes")) {
            parsed.max_text_bytes = try std.fmt.parseInt(usize, args.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--segment-mode")) {
            const value = args.next() orelse return error.MissingArgument;
            parsed.segment_mode = if (std.mem.eql(u8, value, "production"))
                .production
            else if (std.mem.eql(u8, value, "single"))
                .single
            else
                return error.InvalidSegmentMode;
        } else if (std.mem.eql(u8, arg, "--allow-legacy-query-syntax")) {
            parsed.allow_legacy_query_syntax = true;
        } else if (std.mem.eql(u8, arg, "--manifest")) {
            parsed.manifest_path = args.next() orelse return error.MissingArgument;
        } else if (std.mem.eql(u8, arg, "--bm25-k1")) {
            parsed.bm25_k1 = try std.fmt.parseFloat(f64, args.next() orelse return error.MissingArgument);
            if (!std.math.isFinite(parsed.bm25_k1) or parsed.bm25_k1 < 0) return error.InvalidBM25Config;
        } else if (std.mem.eql(u8, arg, "--bm25-b")) {
            parsed.bm25_b = try std.fmt.parseFloat(f64, args.next() orelse return error.MissingArgument);
            if (!std.math.isFinite(parsed.bm25_b) or parsed.bm25_b < 0 or parsed.bm25_b > 1) return error.InvalidBM25Config;
        } else if (std.mem.eql(u8, arg, "--merge-max-segments-per-tier")) {
            parsed.merge_max_segments_per_tier = try std.fmt.parseInt(u32, args.next() orelse return error.MissingArgument, 10);
            if (parsed.merge_max_segments_per_tier.? < 2) return error.InvalidMergePolicy;
        } else if (std.mem.eql(u8, arg, "--merge-max-at-once")) {
            parsed.merge_max_at_once = try std.fmt.parseInt(u32, args.next() orelse return error.MissingArgument, 10);
            if (parsed.merge_max_at_once.? < 2) return error.InvalidMergePolicy;
        } else if (std.mem.eql(u8, arg, "--postings-chunk-size")) {
            parsed.postings_chunk_size = try std.fmt.parseInt(u32, args.next() orelse return error.MissingArgument, 10);
            if (parsed.postings_chunk_size.? < 64 or !std.math.isPowerOfTwo(parsed.postings_chunk_size.?)) return error.InvalidPostingsChunkSize;
        } else {
            return error.UnknownArgument;
        }
    }
    return parsed;
}

pub fn configureBenchmarkIndexFormat(args: ParsedArgs) void {
    antfly.inverted.setBenchmarkChunkSizeOverride(args.postings_chunk_size);
}

pub fn configureBenchmarkMergePolicy(args: ParsedArgs) void {
    if (args.merge_max_segments_per_tier == null and args.merge_max_at_once == null) {
        antfly.db.setBenchmarkTextMergePolicyOverride(null);
        return;
    }
    var policy = antfly.merger.MergePolicy{};
    if (args.merge_max_segments_per_tier) |value| policy.max_segments_per_tier = value;
    if (args.merge_max_at_once) |value| policy.max_merge_at_once = value;
    antfly.db.setBenchmarkTextMergePolicyOverride(policy);
}

pub fn openDb(alloc: std.mem.Allocator, db_path: []const u8) !antfly.db.DB {
    return try antfly.db.DB.open(alloc, db_path, .{
        .map_size = 64 * 1024 * 1024 * 1024,
        .no_sync = true,
        .start_index_workers = false,
        // Kernel indexing drains the same production merge policy
        // synchronously after bulk ingest. A background worker would race the
        // explicit drain and make the declared final segment state timing
        // dependent.
        .text_merge = .{ .enabled = false },
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
    });
}

pub fn ensureIndex(db: *antfly.db.DB) !void {
    db.addIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = index_config_json,
    }) catch |err| switch (err) {
        error.IndexAlreadyExists => {},
        else => return err,
    };
}

pub const QueryCommand = union(enum) {
    analyze: void,
    layout: void,
    resources: void,
    compact: void,
    count: void,
    top: u32,
    top_count: u32,
    verify_top: u32,
    verify_top_count: u32,
    profile_top: u32,
    explain: u32,
    unsupported: void,
};

pub fn parseCommand(raw: []const u8) QueryCommand {
    if (std.mem.eql(u8, raw, "ANALYZE")) return .{ .analyze = {} };
    if (std.mem.eql(u8, raw, "LAYOUT")) return .{ .layout = {} };
    if (std.mem.eql(u8, raw, "RESOURCES")) return .{ .resources = {} };
    if (std.mem.eql(u8, raw, "COMPACT")) return .{ .compact = {} };
    if (std.mem.eql(u8, raw, "COUNT")) return .{ .count = {} };
    if (std.mem.startsWith(u8, raw, "EXPLAIN_")) {
        const ordinal = std.fmt.parseInt(u32, raw["EXPLAIN_".len..], 10) catch return .{ .unsupported = {} };
        return .{ .explain = ordinal };
    }
    if (std.mem.startsWith(u8, raw, "VERIFY_TOP_")) {
        const rest = raw["VERIFY_TOP_".len..];
        if (std.mem.endsWith(u8, rest, "_COUNT")) {
            const n_raw = rest[0 .. rest.len - "_COUNT".len];
            const n = std.fmt.parseInt(u32, n_raw, 10) catch return .{ .unsupported = {} };
            if (n == 0) return .{ .unsupported = {} };
            return .{ .verify_top_count = n };
        }
        const n = std.fmt.parseInt(u32, rest, 10) catch return .{ .unsupported = {} };
        if (n == 0) return .{ .unsupported = {} };
        return .{ .verify_top = n };
    }
    if (std.mem.startsWith(u8, raw, "PROFILE_TOP_")) {
        const n = std.fmt.parseInt(u32, raw["PROFILE_TOP_".len..], 10) catch return .{ .unsupported = {} };
        if (n == 0) return .{ .unsupported = {} };
        return .{ .profile_top = n };
    }
    if (std.mem.startsWith(u8, raw, "TOP_")) {
        const rest = raw["TOP_".len..];
        if (std.mem.endsWith(u8, rest, "_COUNT")) {
            const n_raw = rest[0 .. rest.len - "_COUNT".len];
            const n = std.fmt.parseInt(u32, n_raw, 10) catch return .{ .unsupported = {} };
            if (n == 0) return .{ .unsupported = {} };
            return .{ .top_count = n };
        }
        const n = std.fmt.parseInt(u32, rest, 10) catch return .{ .unsupported = {} };
        if (n == 0) return .{ .unsupported = {} };
        return .{ .top = n };
    }
    return .{ .unsupported = {} };
}

pub fn explainTerm(
    db: *antfly.db.DB,
    alloc: std.mem.Allocator,
    query: types.TextQuery,
    external_ordinal: u32,
) !?antfly.index.TextTermStats {
    const term_query = switch (query) {
        .term => |term| term,
        else => return error.UnsupportedBenchmarkOperation,
    };
    const native_ordinal = std.math.add(u32, external_ordinal, 1) catch return error.InvalidBenchmarkDocumentOrdinal;
    return try db.textKernelTermStats(
        alloc,
        index_name,
        term_query.field,
        term_query.term,
        native_ordinal,
    );
}

pub const AnalyzerToken = struct {
    term: []const u8,
    position: u32,
    start_byte: u32,
    end_byte: u32,
};

pub const AnalyzerResult = struct {
    schema_version: u16 = 1,
    analyzer: []const u8 = analyzer_name,
    tokens: []AnalyzerToken,
};

pub fn analyzeAlloc(alloc: std.mem.Allocator, text: []const u8) !AnalyzerResult {
    const analyzed = try antfly.analysis.simple_analyzer.analyze(alloc, text);
    defer antfly.analysis.Analyzer.freeTokens(alloc, analyzed);
    const tokens = try alloc.alloc(AnalyzerToken, analyzed.len);
    errdefer alloc.free(tokens);
    for (analyzed, 0..) |token, i| {
        tokens[i] = .{
            .term = try alloc.dupe(u8, token.term),
            .position = token.position,
            .start_byte = token.start_byte,
            .end_byte = token.end_byte,
        };
    }
    return .{ .tokens = tokens };
}

pub fn freeAnalyzerResult(alloc: std.mem.Allocator, result: AnalyzerResult) void {
    for (result.tokens) |token| alloc.free(@constCast(token.term));
    if (result.tokens.len > 0) alloc.free(result.tokens);
}

pub const query_grammar_version = "V1";
pub const benchmark_schema_version: u16 = 1;
pub const result_schema_version: u16 = 1;

/// Parse the correctness-gated benchmark grammar documented in
/// FULL_TEXT_PERFORMANCE.md:
///
///   V1 TERM <field> <term>
///   V1 UNION <field> <term>...
///   V1 INTERSECTION <field> <term>...
///   V1 PHRASE <field> <term>...
///
/// Terms are already analyzed tokens. The grammar deliberately has no quoting,
/// escaping, implicit operators, or analyzer-dependent expansion. A workload
/// that needs arbitrary text must normalize it into tokens before timing.
pub fn parseBenchmarkQuery(alloc: std.mem.Allocator, raw_query: []const u8) !types.TextQuery {
    var tokens = std.mem.tokenizeAny(u8, raw_query, " \t\r\n");
    const version = tokens.next() orelse return error.InvalidBenchmarkQuery;
    if (!std.mem.eql(u8, version, query_grammar_version)) return error.UnsupportedQueryGrammar;

    const operation = tokens.next() orelse return error.InvalidBenchmarkQuery;
    const field = tokens.next() orelse return error.InvalidBenchmarkQuery;
    if (!std.mem.eql(u8, field, text_field)) return error.UnsupportedBenchmarkField;

    var terms = std.ArrayListUnmanaged([]const u8).empty;
    defer terms.deinit(alloc);
    while (tokens.next()) |term| {
        if (term.len == 0) continue;
        try terms.append(alloc, term);
    }
    if (terms.items.len == 0) return error.InvalidBenchmarkQuery;

    if (std.mem.eql(u8, operation, "TERM")) {
        if (terms.items.len != 1) return error.InvalidBenchmarkQuery;
        return .{ .term = .{ .field = field, .term = terms.items[0] } };
    }

    if (std.mem.eql(u8, operation, "PHRASE")) {
        return .{ .phrase = .{
            .field = field,
            .terms = try terms.toOwnedSlice(alloc),
        } };
    }

    const clause_kind: enum { must, should } = if (std.mem.eql(u8, operation, "UNION"))
        .should
    else if (std.mem.eql(u8, operation, "INTERSECTION"))
        .must
    else
        return error.UnsupportedBenchmarkOperation;

    const clauses = try alloc.alloc(types.TextQuery, terms.items.len);
    for (terms.items, 0..) |term, i| {
        clauses[i] = .{ .term = .{ .field = field, .term = term } };
    }
    return switch (clause_kind) {
        .must => .{ .bool_query = .{ .must = clauses } },
        .should => .{ .bool_query = .{ .should = clauses, .min_should = 1 } },
    };
}

pub fn parseQuery(alloc: std.mem.Allocator, raw_query: []const u8, allow_legacy: bool) !types.TextQuery {
    if (std.mem.startsWith(u8, raw_query, query_grammar_version ++ " ")) {
        return try parseBenchmarkQuery(alloc, raw_query);
    }
    if (!allow_legacy) return error.UnsupportedQueryGrammar;
    return try parseLegacyLuceneQuery(alloc, raw_query);
}

const ClauseKind = enum {
    should,
    must,
    must_not,
};

const Clause = struct {
    kind: ClauseKind,
    text: []const u8,
    phrase: bool,
};

pub fn parseLegacyLuceneQuery(alloc: std.mem.Allocator, raw_query: []const u8) !types.TextQuery {
    var clauses = std.ArrayListUnmanaged(Clause).empty;
    defer clauses.deinit(alloc);

    var i: usize = 0;
    while (i < raw_query.len) {
        while (i < raw_query.len and std.ascii.isWhitespace(raw_query[i])) : (i += 1) {}
        if (i >= raw_query.len) break;

        var kind: ClauseKind = .should;
        if (raw_query[i] == '+') {
            kind = .must;
            i += 1;
        } else if (raw_query[i] == '-') {
            kind = .must_not;
            i += 1;
        }

        if (i >= raw_query.len) break;

        var phrase = false;
        const start: usize = i;
        var end: usize = i;
        if (raw_query[i] == '"') {
            phrase = true;
            i += 1;
            const phrase_start = i;
            while (i < raw_query.len and raw_query[i] != '"') : (i += 1) {}
            end = i;
            if (i < raw_query.len and raw_query[i] == '"') i += 1;
            if (end > phrase_start) {
                try clauses.append(alloc, .{ .kind = kind, .text = raw_query[phrase_start..end], .phrase = true });
            }
            continue;
        }

        while (i < raw_query.len and !std.ascii.isWhitespace(raw_query[i])) : (i += 1) {}
        end = i;
        if (end > start) {
            try clauses.append(alloc, .{ .kind = kind, .text = raw_query[start..end], .phrase = phrase });
        }
    }

    if (clauses.items.len == 0) return .{ .match_none = {} };

    var must = std.ArrayListUnmanaged(types.TextQuery).empty;
    errdefer must.deinit(alloc);
    var should = std.ArrayListUnmanaged(types.TextQuery).empty;
    errdefer should.deinit(alloc);
    var must_not = std.ArrayListUnmanaged(types.TextQuery).empty;
    errdefer must_not.deinit(alloc);

    var should_terms = std.ArrayListUnmanaged([]const u8).empty;
    defer should_terms.deinit(alloc);

    for (clauses.items) |clause| {
        const query = if (clause.phrase)
            types.TextQuery{ .match_phrase = .{
                .field = text_field,
                .text = clause.text,
                .analyzer = "simple",
            } }
        else
            types.TextQuery{ .match = .{
                .field = text_field,
                .text = clause.text,
                .analyzer = "simple",
            } };

        switch (clause.kind) {
            .must => try must.append(alloc, query),
            .must_not => try must_not.append(alloc, query),
            .should => if (!clause.phrase) {
                try should_terms.append(alloc, clause.text);
            } else {
                try should.append(alloc, query);
            },
        }
    }

    if (should_terms.items.len > 0) {
        const joined = try joinTerms(alloc, should_terms.items);
        try should.append(alloc, .{ .match = .{
            .field = text_field,
            .text = joined,
            .analyzer = "simple",
        } });
    }

    const must_slice = try must.toOwnedSlice(alloc);
    const should_slice = try should.toOwnedSlice(alloc);
    const must_not_slice = try must_not.toOwnedSlice(alloc);

    if (must_slice.len == 0 and should_slice.len == 1 and must_not_slice.len == 0) {
        const out = should_slice[0];
        alloc.free(should_slice);
        return out;
    }
    if (must_slice.len == 1 and should_slice.len == 0 and must_not_slice.len == 0) {
        const out = must_slice[0];
        alloc.free(must_slice);
        return out;
    }

    return .{ .bool_query = .{
        .must = must_slice,
        .should = should_slice,
        .must_not = must_not_slice,
        .min_should = if (should_slice.len > 0 and must_slice.len == 0) 1 else 0,
    } };
}

fn joinTerms(alloc: std.mem.Allocator, terms: []const []const u8) ![]const u8 {
    if (terms.len == 0) return "";
    var total: usize = 0;
    for (terms) |term| total += term.len;
    total += terms.len - 1;

    const out = try alloc.alloc(u8, total);
    var pos: usize = 0;
    for (terms, 0..) |term, i| {
        if (i > 0) {
            out[pos] = ' ';
            pos += 1;
        }
        @memcpy(out[pos..][0..term.len], term);
        pos += term.len;
    }
    return out;
}

pub fn countQuery(db: *antfly.db.DB, alloc: std.mem.Allocator, query: types.TextQuery) !u32 {
    return try db.countTextKernel(alloc, index_name, query);
}

pub fn topQuery(
    db: *antfly.db.DB,
    alloc: std.mem.Allocator,
    query: types.TextQuery,
    limit: u32,
    k1: f64,
    b: f64,
) !types.TextKernelResult {
    return try db.searchTextKernel(alloc, index_name, query, .{
        .limit = limit,
        .bm25 = .{ .k1 = @floatCast(k1), .b = @floatCast(b) },
    });
}

pub fn profileTopQuery(
    db: *antfly.db.DB,
    alloc: std.mem.Allocator,
    query: types.TextQuery,
    limit: u32,
    k1: f64,
    b: f64,
) !types.TextKernelResult {
    return try db.searchTextKernel(alloc, index_name, query, .{
        .limit = limit,
        .bm25 = .{ .k1 = @floatCast(k1), .b = @floatCast(b) },
        .collect_diagnostics = true,
    });
}

pub const VerificationHit = struct {
    id: u32,
    score: f32,
};

pub const VerificationResult = struct {
    schema_version: u16 = result_schema_version,
    query_grammar: []const u8 = query_grammar_version,
    total_hits: u32,
    relation: types.TotalHitsRelation,
    hits: []VerificationHit,
};

pub fn verificationResultAlloc(
    alloc: std.mem.Allocator,
    result: types.TextKernelResult,
    exact_total_override: ?u32,
) !VerificationResult {
    const hits = try alloc.alloc(VerificationHit, result.hits.len);
    errdefer alloc.free(hits);
    for (result.hits, 0..) |hit, i| {
        if (hit.doc_ordinal == 0) return error.InvalidBenchmarkDocumentOrdinal;
        hits[i] = .{
            .id = hit.doc_ordinal - 1,
            .score = hit.score,
        };
    }
    return .{
        .total_hits = exact_total_override orelse result.total_hits,
        .relation = if (exact_total_override != null) .exact else result.total_hits_relation,
        .hits = hits,
    };
}

pub fn corpusDocumentId(buf: []u8, ordinal: usize) ![]const u8 {
    return try std.fmt.bufPrint(buf, "benchmark:{d}", .{ordinal});
}

pub fn corpusOrdinalFromId(id: []const u8) !u32 {
    const prefix = "benchmark:";
    if (!std.mem.startsWith(u8, id, prefix)) return error.InvalidBenchmarkDocumentId;
    return try std.fmt.parseInt(u32, id[prefix.len..], 10);
}

test "benchmark command parser distinguishes timed and verification operations" {
    try std.testing.expect(parseCommand("ANALYZE") == .analyze);
    try std.testing.expect(parseCommand("LAYOUT") == .layout);
    try std.testing.expect(parseCommand("RESOURCES") == .resources);
    try std.testing.expect(parseCommand("COMPACT") == .compact);
    try std.testing.expect(parseCommand("COUNT") == .count);
    try std.testing.expectEqual(@as(u32, 10), parseCommand("TOP_10").top);
    try std.testing.expectEqual(@as(u32, 10), parseCommand("TOP_10_COUNT").top_count);
    try std.testing.expectEqual(@as(u32, 10), parseCommand("VERIFY_TOP_10").verify_top);
    try std.testing.expectEqual(@as(u32, 10), parseCommand("VERIFY_TOP_10_COUNT").verify_top_count);
    try std.testing.expectEqual(@as(u32, 10), parseCommand("PROFILE_TOP_10").profile_top);
    try std.testing.expectEqual(@as(u32, 0), parseCommand("EXPLAIN_0").explain);
    try std.testing.expectEqual(@as(u32, 719), parseCommand("EXPLAIN_719").explain);
    try std.testing.expect(parseCommand("VERIFY_TOP_0") == .unsupported);
}

test "benchmark analyzer fixture exposes terms positions and byte offsets" {
    const alloc = std.testing.allocator;
    const result = try analyzeAlloc(alloc, "Hello, CAFÉ 42");
    defer freeAnalyzerResult(alloc, result);
    try std.testing.expectEqual(@as(usize, 3), result.tokens.len);
    try std.testing.expectEqualStrings("hello", result.tokens[0].term);
    // The current production lowercase filter is deliberately captured as
    // ASCII-only. Cross-engine preflight must either configure the comparator
    // identically or fail until Antfly gains a versioned Unicode lowercaser.
    try std.testing.expectEqualStrings("cafÉ", result.tokens[1].term);
    try std.testing.expectEqualStrings("42", result.tokens[2].term);
    try std.testing.expectEqual(@as(u32, 0), result.tokens[0].position);
    try std.testing.expectEqual(@as(u32, 1), result.tokens[1].position);
    try std.testing.expectEqual(@as(u32, 0), result.tokens[0].start_byte);
    try std.testing.expectEqual(@as(u32, 5), result.tokens[0].end_byte);
}

test "V1 benchmark grammar lowers exact term union intersection and phrase queries" {
    const alloc = std.testing.allocator;

    const term = try parseBenchmarkQuery(alloc, "V1 TERM text alpha");
    try std.testing.expect(term == .term);
    try std.testing.expectEqualStrings("alpha", term.term.term);

    const union_query = try parseBenchmarkQuery(alloc, "V1 UNION text alpha beta");
    defer alloc.free(union_query.bool_query.should);
    try std.testing.expect(union_query == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), union_query.bool_query.should.len);
    try std.testing.expectEqual(@as(u32, 1), union_query.bool_query.min_should);

    const intersection = try parseBenchmarkQuery(alloc, "V1 INTERSECTION text alpha beta");
    defer alloc.free(intersection.bool_query.must);
    try std.testing.expect(intersection == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), intersection.bool_query.must.len);

    const phrase = try parseBenchmarkQuery(alloc, "V1 PHRASE text alpha beta");
    defer alloc.free(phrase.phrase.terms);
    try std.testing.expect(phrase == .phrase);
    try std.testing.expectEqualStrings("beta", phrase.phrase.terms[1]);
}

test "V1 benchmark grammar fails closed" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedQueryGrammar, parseBenchmarkQuery(alloc, "V2 TERM text alpha"));
    try std.testing.expectError(error.UnsupportedBenchmarkField, parseBenchmarkQuery(alloc, "V1 TERM title alpha"));
    try std.testing.expectError(error.UnsupportedBenchmarkOperation, parseBenchmarkQuery(alloc, "V1 PREFIX text alpha"));
    try std.testing.expectError(error.InvalidBenchmarkQuery, parseBenchmarkQuery(alloc, "V1 TERM text alpha beta"));
    try std.testing.expectError(error.UnsupportedQueryGrammar, parseQuery(alloc, "alpha beta", false));
}

test "benchmark document ids round trip corpus ordinals" {
    var buf: [64]u8 = undefined;
    const id = try corpusDocumentId(&buf, 42);
    try std.testing.expectEqualStrings("benchmark:42", id);
    try std.testing.expectEqual(@as(u32, 42), try corpusOrdinalFromId(id));
    try std.testing.expectError(error.InvalidBenchmarkDocumentId, corpusOrdinalFromId("doc:42"));
}

test "verification result normalizes ordinals and exact-count relation" {
    const alloc = std.testing.allocator;
    const kernel_hits = [_]types.TextKernelHit{
        .{ .doc_ordinal = 1, .score = 3.5 },
        .{ .doc_ordinal = 3, .score = 1.25 },
    };
    const result = types.TextKernelResult{
        .hits = @constCast(&kernel_hits),
        .total_hits = 2,
        .total_hits_relation = .gte,
    };
    const verification = try verificationResultAlloc(alloc, result, 7);
    defer alloc.free(verification.hits);
    try std.testing.expectEqual(benchmark_schema_version, verification.schema_version);
    try std.testing.expectEqualStrings(query_grammar_version, verification.query_grammar);
    try std.testing.expectEqual(@as(u32, 7), verification.total_hits);
    try std.testing.expectEqual(types.TotalHitsRelation.exact, verification.relation);
    try std.testing.expectEqual(@as(u32, 0), verification.hits[0].id);
    try std.testing.expectEqual(@as(u32, 2), verification.hits[1].id);

    const invalid_hits = [_]types.TextKernelHit{.{ .doc_ordinal = 0, .score = 1 }};
    const invalid = types.TextKernelResult{ .hits = @constCast(&invalid_hits), .total_hits = 1, .total_hits_relation = .exact };
    try std.testing.expectError(error.InvalidBenchmarkDocumentOrdinal, verificationResultAlloc(alloc, invalid, null));
}
